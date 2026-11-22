//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/broadcast_test.go
//! - github.com/grafana/dskit/kv/memberlist/kv_init_service_test.go
//! - github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go
//! - github.com/grafana/dskit/kv/memberlist/node_meta_test.go
//! - github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing_test.go
//! - github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go
//! - github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{start_and_await_running, stop_and_await_terminated, ServiceContext};
use mimir_config::server::{
    MemberlistConfig, PropagationDelayTrackerConfig, ZoneAwareRoutingConfig,
};
use mimir_config::GoDuration;
use mimir_proto::memberlistpb::{BeaconDesc, KeyValuePair, PropagationDelayTrackerDesc};
use mimir_proto::ringpb::{Desc as RingDesc, InstanceDesc, InstanceState};
use mimir_ring::kv::memberlist::kv_init_service::KvInitService;
use mimir_ring::kv::memberlist::gofmt::GoJson;
use mimir_ring::kv::memberlist::mergeable::Mergeable;
use mimir_ring::kv::memberlist::node_meta::{
    encode_node_metadata, EncodedNodeMetadata, NodeRole, MAX_ZONE_NAME_LENGTH,
};
use mimir_ring::kv::memberlist::propagation_tracker::{
    get_or_create_propagation_delay_tracker_desc, merge_propagation_tracker_with_time,
    propagation_delay_tracker_codec, PropagationDelayTracker, PROPAGATION_DELAY_TRACKER_CODEC_ID,
    PROPAGATION_DELAY_TRACKER_KEY,
};
use mimir_ring::kv::memberlist::queue::Broadcast;

use mimir_ring::kv::memberlist::wire::NodeStateType;
use mimir_ring::kv::memberlist::zone_aware::ZoneAwareNodeSelection;
use mimir_ring::kv::memberlist::{
    MemberlistKv, MemberlistKvClient, NodeState, RingBroadcast, TcpTransport,
};
use mimir_ring::kv::{get_typed, CasOutcome, Codec, KvClient, KvError};
use mimir_ring::ring::model::ring_desc_codec;
use prometheus::Registry;

// ============================================================================
// broadcast_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/broadcast_test.go::TestInvalidates
#[test]
fn test_invalidates() {
    const KEY: &str = "ring";
    let msgs: HashMap<&str, RingBroadcast> = [
        ("b1", RingBroadcast::new(KEY.into(), vec!["A".into(), "B".into(), "C".into()], 1, vec![])),
        ("b2", RingBroadcast::new(KEY.into(), vec!["A".into(), "B".into(), "C".into()], 2, vec![])),
        ("b3", RingBroadcast::new(KEY.into(), vec!["A".into()], 3, vec![])),
        ("b4", RingBroadcast::new(KEY.into(), vec!["A".into(), "B".into()], 4, vec![])),
        ("b5", RingBroadcast::new(KEY.into(), vec!["A".into(), "B".into(), "D".into()], 5, vec![])),
        ("b6", RingBroadcast::new(KEY.into(), vec!["A".into(), "B".into(), "C".into(), "D".into()], 6, vec![])),
    ]
    .into_iter()
    .collect();

    let check = |k1: &str, k2: &str, first_inv_second: bool, second_inv_first: bool| {
        let b1 = &msgs[k1];
        let b2 = &msgs[k2];
        assert_eq!(first_inv_second, b1.invalidates(b2), "{k1}.invalidates({k2})");
        assert_eq!(second_inv_first, b2.invalidates(b1), "{k2}.invalidates({k1})");
    };

    check("b2", "b1", true, false);
    check("b3", "b1", false, false);
    check("b3", "b2", false, false);
    check("b4", "b1", false, false);
    check("b4", "b2", false, false);
    check("b4", "b3", true, false);
    check("b5", "b1", false, false);
    check("b5", "b2", false, false);
    check("b5", "b3", true, false);
    check("b5", "b4", true, false);
    check("b6", "b1", true, false);
    check("b6", "b2", true, false);
    check("b6", "b3", true, false);
    check("b6", "b4", true, false);
    check("b6", "b5", true, false);
}

// ============================================================================
// kv_init_service_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/kv_init_service_test.go::TestStop
#[tokio::test]
async fn test_stop() {
    let cfg = MemberlistConfig::default();
    let kvinit = KvInitService::new(cfg, Vec::new(), None, Some(Registry::new()));
    kvinit.stopping().await.unwrap();
}

// ============================================================================
// tcp_transport_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go::TestFinalAdvertiseAddr
#[tokio::test]
async fn test_final_advertise_addr() {
    let cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        ..Default::default()
    };
    let (transport, _pkt_rx, _stream_rx) = TcpTransport::new(&cfg, "", Some(&Registry::new())).await.unwrap();
    let (ip, port) = transport.final_advertise_addr("127.0.0.1", 0).unwrap();
    assert_eq!(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), ip);
    assert_eq!(0, port);
    transport.shutdown();
}

// ============================================================================
// node_meta_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/node_meta_test.go::TestEncodedNodeMetadata_RoleAndZone
#[test]
fn test_encoded_node_metadata_role_and_zone() {
    let cases = [
        ("member with zone", NodeRole::Member, "us-east-1a"),
        ("bridge with zone", NodeRole::Bridge, "eu-west-1b"),
        ("member with empty zone", NodeRole::Member, ""),
        ("member with max length zone", NodeRole::Member, "0123456789abcdef"),
    ];

    for (name, role, zone) in cases {
        let encoded = encode_node_metadata(role, zone).unwrap_or_else(|e| panic!("{name}: {e}"));
        let meta = EncodedNodeMetadata(&encoded);
        assert_eq!(role, meta.role(), "{name}");
        assert_eq!(zone, meta.zone(), "{name}");
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/node_meta_test.go::TestEncodeNodeMetadata_ValidationErrors
#[test]
fn test_encode_node_metadata_validation_errors() {
    let err = encode_node_metadata(NodeRole::Member, "0123456789abcdef0").unwrap_err();
    assert!(err.contains("zone name too long"), "got: {err}");
}

// Upstream: github.com/grafana/dskit/kv/memberlist/node_meta_test.go::TestEncodedNodeMetadata_ZeroAllocations
#[test]
fn test_encoded_node_metadata_zero_allocations() {
    let encoded = encode_node_metadata(NodeRole::Bridge, "zone-a").unwrap();
    let meta = EncodedNodeMetadata(&encoded);
    for _ in 0..100 {
        let r = meta.role();
        let z = meta.zone();
        assert_eq!(NodeRole::Bridge, r);
        assert_eq!("zone-a", z);
        // Zone slice borrows directly from encoded buffer without allocation.
        let base = encoded.as_ptr() as usize;
        let end = base + encoded.len();
        let z_ptr = z.as_ptr() as usize;
        assert!(z_ptr >= base && z_ptr < end);
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/node_meta_test.go::TestEncodedNodeMetadata_InvalidData
#[test]
fn test_encoded_node_metadata_invalid_data() {
    let cases: [(&str, &[u8], NodeRole, &str); 4] = [
        ("empty data", &[], NodeRole::Member, ""),
        ("too short", &[1, 0], NodeRole::Member, ""),
        ("unknown version", &[99, 0, 0], NodeRole::Member, ""),
        ("truncated zone data", &[1, 2, 5, b'a', b'b'], NodeRole::Bridge, ""),
    ];
    for (name, data, exp_role, exp_zone) in cases {
        let meta = EncodedNodeMetadata(data);
        assert_eq!(exp_role, meta.role(), "{name}");
        assert_eq!(exp_zone, meta.zone(), "{name}");
    }
}

// ============================================================================
// node_zone_aware_routing_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing_test.go::TestNodeRoleString
#[test]
fn test_node_role_string() {
    assert_eq!("member", NodeRole::Member.to_string());
    assert_eq!("bridge", NodeRole::Bridge.to_string());
    assert_eq!("unknown(99)", NodeRole(99).to_string());
}

// Upstream: github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing_test.go::TestZoneAwareRoutingConfig_Validate
#[test]
fn test_zone_aware_routing_config_validate() {
    let cases = [
        (
            "valid config - member role",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: "us-east-1a".into(),
                role: "member".into(),
            },
            "",
        ),
        (
            "valid config - bridge role",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: "eu-west-1b".into(),
                role: "bridge".into(),
            },
            "",
        ),
        (
            "valid config - max zone length",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: "a".repeat(MAX_ZONE_NAME_LENGTH),
                role: "member".into(),
            },
            "",
        ),
        (
            "invalid - no zone",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: String::new(),
                role: "member".into(),
            },
            "zone-aware routing is enabled but zone is not set",
        ),
        (
            "invalid - zone too long",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: "a".repeat(MAX_ZONE_NAME_LENGTH + 1),
                role: "member".into(),
            },
            "zone name too long",
        ),
        (
            "invalid - invalid role",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: "us-east-1a".into(),
                role: "invalid".into(),
            },
            "invalid role: invalid",
        ),
        (
            "invalid - empty role",
            ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: "us-east-1a".into(),
                role: String::new(),
            },
            "invalid role",
        ),
    ];

    for (name, cfg, want_err) in cases {
        let res = cfg.validate();
        if want_err.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            let err = res.unwrap_err().to_string();
            assert!(err.contains(want_err), "{name}: got {err}");
        }
    }
}

fn make_node_state(name: &str, role: NodeRole, zone: &str, state: NodeStateType) -> NodeState {
    let meta = encode_node_metadata(role, zone).unwrap();
    NodeState {
        name: name.to_string(),
        addr: vec![127, 0, 0, 1],
        port: 7946,
        meta: Some(meta),
        incarnation: 1,
        state,
        state_change: SystemTime::now(),
        vsn: vec![1, 5, 2, 1, 5, 4],
    }
}

fn selected_names<'a>(nodes: &'a [NodeState], idxs: &[usize]) -> Vec<&'a str> {
    idxs.iter().map(|&i| nodes[i].name.as_str()).collect()
}

// Upstream: github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing_test.go::TestZoneAwareNodeSelectionDelegate_SelectNodes
#[test]
fn test_zone_aware_node_selection_delegate_select_nodes() {
    let new_joined = |role: NodeRole, zone: &str, reg: &Registry| {
        let d = ZoneAwareNodeSelection::new(role, zone, Some(reg));
        d.mark_joined();
        d
    };

    // local node is member
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Member, "zone-a", &reg);
        let nodes = vec![
            make_node_state("member-zone-a", NodeRole::Member, "zone-a", NodeStateType::Alive),
            make_node_state("bridge-zone-a", NodeRole::Bridge, "zone-a", NodeStateType::Alive),
            make_node_state("member-zone-b", NodeRole::Member, "zone-b", NodeStateType::Alive),
            make_node_state("bridge-zone-b", NodeRole::Bridge, "zone-b", NodeStateType::Alive),
            make_node_state("member-no-zone", NodeRole::Member, "", NodeStateType::Alive),
        ];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        let names = selected_names(&nodes, &sel);
        assert_eq!(3, names.len());
        assert!(names.contains(&"member-zone-a"));
        assert!(names.contains(&"bridge-zone-a"));
        assert!(names.contains(&"member-no-zone"));
        assert!(pref.is_none());
        assert_eq!(1.0, delegate.select_nodes_calls());
        assert_eq!(0.0, delegate.select_nodes_calls_skipped());
    }

    // local node is bridge
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Bridge, "zone-a", &reg);
        let nodes = vec![
            make_node_state("member-zone-a", NodeRole::Member, "zone-a", NodeStateType::Alive),
            make_node_state("bridge-zone-a", NodeRole::Bridge, "zone-a", NodeStateType::Alive),
            make_node_state("member-zone-b", NodeRole::Member, "zone-b", NodeStateType::Alive),
            make_node_state("bridge-zone-b", NodeRole::Bridge, "zone-b", NodeStateType::Alive),
            make_node_state("bridge-zone-c", NodeRole::Bridge, "zone-c", NodeStateType::Alive),
            make_node_state("member-no-zone", NodeRole::Member, "", NodeStateType::Alive),
        ];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        let names = selected_names(&nodes, &sel);
        assert_eq!(5, names.len());
        assert!(names.contains(&"member-zone-a"));
        assert!(names.contains(&"bridge-zone-a"));
        assert!(names.contains(&"bridge-zone-b"));
        assert!(names.contains(&"bridge-zone-c"));
        assert!(names.contains(&"member-no-zone"));
        assert!(!names.contains(&"member-zone-b"));
        let pref_name = nodes[pref.unwrap()].name.as_str();
        assert!(pref_name == "bridge-zone-b" || pref_name == "bridge-zone-c");
        assert_eq!(1.0, delegate.select_nodes_calls());
        assert_eq!(0.0, delegate.select_nodes_calls_skipped());
    }

    // local node has empty zone
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Member, "", &reg);
        let nodes = [
            make_node_state("member-zone-a", NodeRole::Member, "zone-a", NodeStateType::Alive),
            make_node_state("bridge-zone-b", NodeRole::Bridge, "zone-b", NodeStateType::Alive),
        ];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        assert_eq!(2, sel.len());
        assert!(pref.is_none());
        assert_eq!(1.0, delegate.select_nodes_calls());
        assert_eq!(1.0, delegate.select_nodes_calls_skipped());
    }

    // node with empty metadata
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Member, "zone-a", &reg);
        let mut n = make_node_state("node-no-meta", NodeRole::Member, "", NodeStateType::Alive);
        n.meta = None;
        let nodes = vec![n];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        assert_eq!(vec!["node-no-meta"], selected_names(&nodes, &sel));
        assert!(pref.is_none());
        assert_eq!(1.0, delegate.select_nodes_calls());
        assert_eq!(0.0, delegate.select_nodes_calls_skipped());
    }

    // node with invalid metadata
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Member, "zone-a", &reg);
        let mut n = make_node_state("node-invalid-meta", NodeRole::Member, "", NodeStateType::Alive);
        n.meta = Some(vec![1, 2]);
        let nodes = vec![n];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        assert_eq!(vec!["node-invalid-meta"], selected_names(&nodes, &sel));
        assert!(pref.is_none());
    }

    // empty input
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Bridge, "zone-a", &reg);
        let (sel, pref) = delegate.select_nodes(&[]);
        assert!(sel.is_empty());
        assert!(pref.is_none());
        let (sel2, pref2) = delegate.select_nodes(&[]);
        assert!(sel2.is_empty());
        assert!(pref2.is_none());
        assert_eq!(2.0, delegate.select_nodes_calls());
        assert_eq!(0.0, delegate.select_nodes_calls_skipped());
    }

    // skip zone-aware routing when zone has no alive bridge / no bridge / suspect bridge
    for (label, b_state) in [
        ("dead bridge", Some(NodeStateType::Dead)),
        ("no bridge", None),
        ("suspect bridge", Some(NodeStateType::Suspect)),
    ] {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Member, "zone-a", &reg);
        let mut nodes = vec![
            make_node_state("member-zone-a", NodeRole::Member, "zone-a", NodeStateType::Alive),
            make_node_state("bridge-zone-a", NodeRole::Bridge, "zone-a", NodeStateType::Alive),
            make_node_state("member-zone-b", NodeRole::Member, "zone-b", NodeStateType::Alive),
        ];
        if let Some(st) = b_state {
            nodes.push(make_node_state("bridge-zone-b", NodeRole::Bridge, "zone-b", st));
        }
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        assert_eq!(nodes.len(), sel.len(), "{label}");
        assert!(pref.is_none(), "{label}");
        assert_eq!(1.0, delegate.select_nodes_calls(), "{label}");
        assert_eq!(1.0, delegate.select_nodes_calls_skipped(), "{label}");
    }

    // do not track skipped while joining
    {
        let reg = Registry::new();
        let delegate = ZoneAwareNodeSelection::new(NodeRole::Member, "zone-a", Some(&reg));
        let nodes = [
            make_node_state("member-zone-a", NodeRole::Member, "zone-a", NodeStateType::Alive),
            make_node_state("bridge-zone-a", NodeRole::Bridge, "zone-a", NodeStateType::Alive),
            make_node_state("member-zone-b", NodeRole::Member, "zone-b", NodeStateType::Alive),
        ];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        assert_eq!(3, sel.len());
        assert!(pref.is_none());
        assert_eq!(1.0, delegate.select_nodes_calls());
        assert_eq!(0.0, delegate.select_nodes_calls_skipped());

        delegate.mark_joined();
        delegate.select_nodes(&refs);
        assert_eq!(2.0, delegate.select_nodes_calls());
        assert_eq!(1.0, delegate.select_nodes_calls_skipped());
    }

    // zone names ending with digits
    {
        let reg = Registry::new();
        let delegate = new_joined(NodeRole::Member, "us-east-1", &reg);
        let nodes = vec![
            make_node_state("member-us-east-1", NodeRole::Member, "us-east-1", NodeStateType::Alive),
            make_node_state("bridge-us-east-1", NodeRole::Bridge, "us-east-1", NodeStateType::Alive),
            make_node_state("member-us-east-2", NodeRole::Member, "us-east-2", NodeStateType::Alive),
            make_node_state("bridge-us-east-2", NodeRole::Bridge, "us-east-2", NodeStateType::Alive),
        ];
        let refs: Vec<&NodeState> = nodes.iter().collect();
        let (sel, pref) = delegate.select_nodes(&refs);
        let names = selected_names(&nodes, &sel);
        assert_eq!(2, names.len());
        assert!(names.contains(&"member-us-east-1"));
        assert!(names.contains(&"bridge-us-east-1"));
        assert!(pref.is_none());
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing_test.go::TestZoneAwareRouting_EndToEnd
#[tokio::test]
async fn test_zone_aware_routing_end_to_end() {
    let codec = ring_desc_codec();
    for (name, gossip_ms, push_pull_ms) in [
        ("state sync via broadcast updates", 50i64, 0i64),
        ("state sync via push/pull", 3_600_000i64, 50i64),
    ] {
        let make_cfg = |seeds: Vec<String>, zone: &str, role: &str| {
            let mut cfg = MemberlistConfig {
                node_name: format!("{zone}-{role}"),
                randomize_node_name: true,
                bind_addr: vec!["127.0.0.1".to_string()],
                bind_port: 0,
                gossip_interval: GoDuration::from_millis(gossip_ms),
                pull_push_interval: GoDuration::from_millis(push_pull_ms),
                rejoin_interval: GoDuration::ZERO,
                leave_timeout: GoDuration::from_millis(100),
                broadcast_timeout_for_local_updates_on_shutdown: GoDuration::from_millis(100),
                zone_aware_routing: ZoneAwareRoutingConfig {
                    enabled: true,
                    instance_availability_zone: zone.to_string(),
                    role: role.to_string(),
                },
                ..Default::default()
            };
            cfg.join_members.0 = seeds;
            cfg
        };

        let bridge_a = MemberlistKv::start(
            make_cfg(vec![], "zone-a", "bridge"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();
        let seed_addr = vec![format!("127.0.0.1:{}", bridge_a.listening_port())];

        let member_b1 = MemberlistKv::start(
            make_cfg(seed_addr.clone(), "zone-b", "member"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();
        let member_b2 = MemberlistKv::start(
            make_cfg(seed_addr.clone(), "zone-b", "member"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();
        let member_a1 = MemberlistKv::start(
            make_cfg(seed_addr.clone(), "zone-a", "member"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();
        let member_a2 = MemberlistKv::start(
            make_cfg(seed_addr.clone(), "zone-a", "member"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();
        let bridge_b = MemberlistKv::start(
            make_cfg(seed_addr, "zone-b", "bridge"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();

        let all_nodes = vec![
            Arc::clone(&bridge_a),
            Arc::clone(&member_a1),
            Arc::clone(&member_a2),
            Arc::clone(&bridge_b),
            Arc::clone(&member_b1),
            Arc::clone(&member_b2),
        ];

        // Wait until all 6 nodes see all 6 members.
        for node in &all_nodes {
            let mut ok = false;
            for _ in 0..200 {
                if node.num_members() == 6 {
                    ok = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(ok, "{name}: node {} only saw {} members", node.node_name(), node.num_members());
        }

        let client_a1 = MemberlistKvClient::new(Arc::clone(&member_a1), Arc::clone(&codec)).unwrap();
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        client_a1
            .cas(
                "test-key",
                Box::new(move |_| {
                    let mut d = RingDesc::default();
                    d.ingesters.insert(
                        "test-member".to_string(),
                        InstanceDesc {
                            timestamp: now_s,
                            tokens: vec![1, 2, 3],
                            state: InstanceState::Active as i32,
                            ..Default::default()
                        },
                    );
                    Ok(CasOutcome::Update(Box::new(d)))
                }),
            )
            .await
            .unwrap();

        for node in &all_nodes {
            let c = MemberlistKvClient::new(Arc::clone(node), Arc::clone(&codec)).unwrap();
            let mut synced = false;
            for _ in 0..200 {
                if let Some(d) = get_typed::<RingDesc>(&c, "test-key").await.unwrap() {
                    if d.ingesters.get("test-member").is_some_and(|m| m.state == InstanceState::Active as i32) {
                        synced = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(synced, "{name}: node {} did not receive update", node.node_name());
        }

        for node in all_nodes {
            node.shutdown().await;
        }
    }
}

// ============================================================================
// propagation_tracker_model_test.go
// ============================================================================

fn mk_pt(entries: &[(u64, i64, i64)]) -> PropagationDelayTrackerDesc {
    let mut d = PropagationDelayTrackerDesc::default();
    for &(id, pub_at, del_at) in entries {
        d.beacons.insert(
            id,
            BeaconDesc {
                published_by: String::new(),
                published_at: pub_at,
                deleted_at: del_at,
            },
        );
    }
    d
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_Merge_AddBeacon
#[test]
fn test_propagation_delay_tracker_desc_merge_add_beacon() {
    let cases = [
        (
            "the first beacon is added",
            mk_pt(&[]),
            mk_pt(&[(1, 10, 0)]),
            mk_pt(&[(1, 10, 0)]),
            Some(mk_pt(&[(1, 10, 0)])),
        ),
        (
            "a new beacon is added to existing beacons",
            mk_pt(&[(1, 10, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            Some(mk_pt(&[(2, 20, 0)])),
        ),
        (
            "multiple new beacons are added",
            mk_pt(&[(1, 10, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0), (3, 30, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0), (3, 30, 0)]),
            Some(mk_pt(&[(2, 20, 0), (3, 30, 0)])),
        ),
    ];

    for (name, local, incoming, exp_local, exp_change) in cases {
        for local_cas in [true, false] {
            let mut l = local.clone();
            let mut inc = incoming.clone();
            let ch = l.merge(&mut inc, local_cas).unwrap();
            assert_eq!(exp_local, l, "{name} localCAS={local_cas}");
            let got_ch = ch.map(|b| *b.into_any().downcast::<PropagationDelayTrackerDesc>().unwrap());
            assert_eq!(exp_change, got_ch, "{name} localCAS={local_cas}");
        }
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_Merge_UpdateBeacon
#[test]
fn test_propagation_delay_tracker_desc_merge_update_beacon() {
    let cases = [
        (
            "beacon updated with newer timestamp",
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 30, 0)]),
            mk_pt(&[(1, 10, 0), (2, 30, 0)]),
            Some(mk_pt(&[(2, 30, 0)])),
        ),
        (
            "beacon not updated with older timestamp",
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 15, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            None,
        ),
        (
            "beacon not updated with equal timestamp",
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            None,
        ),
    ];

    for (name, local, incoming, exp_local, exp_change) in cases {
        for local_cas in [true, false] {
            let mut l = local.clone();
            let mut inc = incoming.clone();
            let ch = l.merge(&mut inc, local_cas).unwrap();
            assert_eq!(exp_local, l, "{name} localCAS={local_cas}");
            let got_ch = ch.map(|b| *b.into_any().downcast::<PropagationDelayTrackerDesc>().unwrap());
            assert_eq!(exp_change, got_ch, "{name} localCAS={local_cas}");
        }
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_Merge_DeleteBeacon
#[test]
fn test_propagation_delay_tracker_desc_merge_delete_beacon() {
    let now = UNIX_EPOCH + Duration::from_secs(10_000);
    let now_ms = 10_000 * 1000i64;

    let cases = [
        (
            "local change: beacon removed and local beacon is not deleted yet",
            true,
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, now_ms)]),
            Some(mk_pt(&[(2, 20, now_ms)])),
        ),
        (
            "local change: beacon removed and local beacon is already deleted",
            true,
            mk_pt(&[(1, 10, 0), (2, 20, 100)]),
            mk_pt(&[(1, 10, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 100)]),
            None,
        ),
        (
            "incoming: beacon deleted with newer timestamp",
            false,
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 30, 30)]),
            mk_pt(&[(1, 10, 0), (2, 30, 30)]),
            Some(mk_pt(&[(2, 30, 30)])),
        ),
        (
            "incoming: beacon deleted with equal timestamp, deletion should win",
            false,
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 20, 20)]),
            mk_pt(&[(1, 10, 0), (2, 20, 20)]),
            Some(mk_pt(&[(2, 20, 20)])),
        ),
        (
            "incoming: beacon deleted with older timestamp, should be ignored",
            false,
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            mk_pt(&[(1, 10, 0), (2, 10, 10)]),
            mk_pt(&[(1, 10, 0), (2, 20, 0)]),
            None,
        ),
        (
            "incoming: active beacon does not override tombstone with equal timestamp",
            false,
            mk_pt(&[(1, 20, 20)]),
            mk_pt(&[(1, 20, 0)]),
            mk_pt(&[(1, 20, 20)]),
            None,
        ),
        (
            "incoming: active beacon with newer timestamp overrides tombstone",
            false,
            mk_pt(&[(1, 20, 20)]),
            mk_pt(&[(1, 30, 0)]),
            mk_pt(&[(1, 30, 0)]),
            Some(mk_pt(&[(1, 30, 0)])),
        ),
        (
            "incoming: dual-tombstone with later DeletedAt wins",
            false,
            mk_pt(&[(1, 20, 100)]),
            mk_pt(&[(1, 20, 200)]),
            mk_pt(&[(1, 20, 200)]),
            Some(mk_pt(&[(1, 20, 200)])),
        ),
        (
            "incoming: dual-tombstone with earlier DeletedAt is ignored",
            false,
            mk_pt(&[(1, 20, 200)]),
            mk_pt(&[(1, 20, 100)]),
            mk_pt(&[(1, 20, 200)]),
            None,
        ),
    ];

    for (name, local_cas, mut local, incoming, exp_local, exp_change) in cases {
        let ch = merge_propagation_tracker_with_time(&mut local, &incoming, local_cas, now);
        assert_eq!(exp_local, local, "{name}");
        assert_eq!(exp_change, ch, "{name}");
    }
}

struct DummyMergeable;
impl Mergeable for DummyMergeable {
    fn merge(&mut self, _: &mut dyn Mergeable, _: bool) -> Result<Option<Box<dyn Mergeable>>, String> {
        Ok(None)
    }
    fn merge_content(&self) -> Vec<String> {
        Vec::new()
    }
    fn remove_tombstones(&mut self, _: Option<SystemTime>) -> (usize, usize) {
        (0, 0)
    }
    fn clone_mergeable(&self) -> Box<dyn Mergeable> {
        Box::new(DummyMergeable)
    }
    fn eq_mergeable(&self, _: &dyn Mergeable) -> bool {
        false
    }
    fn to_go_json(&self) -> GoJson {
        GoJson::Null
    }
    fn go_string(&self) -> String {
        String::new()
    }
    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_Merge_EdgeCases
#[test]
fn test_propagation_delay_tracker_desc_merge_edge_cases() {
    let mut local = mk_pt(&[]);
    let mut wrong = DummyMergeable;
    let err = match local.merge(&mut wrong, false) { Err(e) => e, Ok(_) => panic!("expected merge error") };
    assert!(err.contains("expected *PropagationDelayTrackerDesc"), "got: {err}");

    let mut local2 = mk_pt(&[(1, 10, 0)]);
    let mut empty = mk_pt(&[]);
    let ch = local2.merge(&mut empty, false).unwrap();
    assert!(ch.is_none());
    assert_eq!(mk_pt(&[(1, 10, 0)]), local2);
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_MergeContent
#[test]
fn test_propagation_delay_tracker_desc_merge_content() {
    let d = mk_pt(&[(1000, 1000, 0), (2000, 2000, 0), (3000, 3000, 0)]);
    let content = d.merge_content();
    assert_eq!(3, content.len());
    assert!(content.contains(&"1000".to_string()));
    assert!(content.contains(&"2000".to_string()));
    assert!(content.contains(&"3000".to_string()));
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_RemoveTombstones
#[test]
fn test_propagation_delay_tracker_desc_remove_tombstones() {
    let now = SystemTime::now();
    let now_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let old_ms = (now - Duration::from_secs(600)).duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let limit = now - Duration::from_secs(300);

    // zero limit removes all tombstones
    {
        let mut d = mk_pt(&[(1, now_ms, 0), (2, old_ms, 0), (3, now_ms, now_ms), (4, now_ms, old_ms)]);
        let (total, removed) = d.remove_tombstones(None);
        assert_eq!((0, 2), (total, removed));
        assert_eq!(mk_pt(&[(1, now_ms, 0), (2, old_ms, 0)]), d);
    }

    // non-zero limit removes old tombstones only
    {
        let mut d = mk_pt(&[(1, now_ms, 0), (2, old_ms, 0), (3, now_ms, now_ms), (4, now_ms, old_ms)]);
        let (total, removed) = d.remove_tombstones(Some(limit));
        assert_eq!((1, 1), (total, removed));
        assert_eq!(mk_pt(&[(1, now_ms, 0), (2, old_ms, 0), (3, now_ms, now_ms)]), d);
    }

    // no tombstones
    {
        let mut d = mk_pt(&[(1, now_ms, 0), (2, old_ms, 0)]);
        let (total, removed) = d.remove_tombstones(Some(limit));
        assert_eq!((0, 0), (total, removed));
        assert_eq!(mk_pt(&[(1, now_ms, 0), (2, old_ms, 0)]), d);
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestPropagationDelayTrackerDesc_Clone
#[test]
fn test_propagation_delay_tracker_desc_clone() {
    let mut d = mk_pt(&[(1000, 1000, 0), (2000, 2000, 100)]);
    let cloned = *d
        .clone_mergeable()
        .into_any()
        .downcast::<PropagationDelayTrackerDesc>()
        .unwrap();
    assert_eq!(d.beacons.len(), cloned.beacons.len());
    assert_eq!(d.beacons[&1000], cloned.beacons[&1000]);
    assert_eq!(d.beacons[&2000], cloned.beacons[&2000]);
    d.beacons.insert(1000, BeaconDesc { published_at: 9999, ..Default::default() });
    assert_ne!(d.beacons[&1000], cloned.beacons[&1000]);
    assert_eq!(1000, cloned.beacons[&1000].published_at);
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestGetOrCreatePropagationDelayTrackerDesc
#[test]
fn test_get_or_create_propagation_delay_tracker_desc() {
    let desc = get_or_create_propagation_delay_tracker_desc(None);
    assert!(desc.beacons.is_empty());

    let mut existing = PropagationDelayTrackerDesc::default();
    existing.beacons.insert(1000, BeaconDesc { published_at: 1000, ..Default::default() });
    let desc2 = get_or_create_propagation_delay_tracker_desc(Some(&existing));
    assert_eq!(1, desc2.beacons.len());
    assert_eq!(1000, desc2.beacons[&1000].published_at);
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_model_test.go::TestGetPropagationDelayTrackerCodec
#[test]
fn test_get_propagation_delay_tracker_codec() {
    let codec = propagation_delay_tracker_codec();
    assert_eq!(PROPAGATION_DELAY_TRACKER_CODEC_ID, codec.codec_id());

    let mut original = PropagationDelayTrackerDesc::default();
    original.beacons.insert(
        1234567890,
        BeaconDesc {
            published_by: String::new(),
            published_at: 1234567890,
            deleted_at: 9876543210,
        },
    );
    let encoded = codec.encode(&original).unwrap();
    let decoded = codec.decode(&encoded).unwrap();
    let decoded_desc = decoded.downcast_ref::<PropagationDelayTrackerDesc>().unwrap();
    assert_eq!(original.beacons[&1234567890], decoded_desc.beacons[&1234567890]);
}

// ============================================================================
// propagation_tracker_test.go
// ============================================================================

async fn create_pt_test_kv(left_timeout: Option<Duration>) -> Arc<MemberlistKv> {
    let mut cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        ..Default::default()
    };
    if let Some(t) = left_timeout {
        cfg.left_ingesters_timeout = GoDuration(t.as_nanos() as i64);
    }
    MemberlistKv::start(cfg, vec![propagation_delay_tracker_codec()], "", None)
        .await
        .unwrap()
}

fn find_delay_histogram(reg: &Registry) -> Option<(u64, f64)> {
    for mf in reg.gather() {
        if mf.get_name() == "memberlist_propagation_tracker_delay_seconds" {
            if let Some(m) = mf.get_metric().first() {
                let h = m.get_histogram();
                return Some((h.get_sample_count(), h.get_sample_sum()));
            }
        }
    }
    None
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_TracksPropagationDelay
#[tokio::test]
async fn test_propagation_delay_tracker_tracks_propagation_delay() {
    let kv = create_pt_test_kv(None).await;
    let reg1 = Registry::new();
    let reg2 = Registry::new();
    let cfg = PropagationDelayTrackerConfig {
        enabled: true,
        beacon_interval: GoDuration::from_millis(100),
        beacon_lifetime: GoDuration::from_secs(3600),
        log_beacons_latency_longer_than: GoDuration::ZERO,
    };

    let t1 = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg.clone(), "test-node-1".into(), Some(&reg1));
    let t2 = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, "test-node-2".into(), Some(&reg2));
    start_and_await_running(ServiceContext::background(), &*t1.service()).await.unwrap();
    start_and_await_running(ServiceContext::background(), &*t2.service()).await.unwrap();

    let mut passed = false;
    for _ in 0..200 {
        if let (Some((c1, s1)), Some((c2, s2))) = (find_delay_histogram(&reg1), find_delay_histogram(&reg2)) {
            if c1 > 0 && c2 > 0 {
                assert!(s1 / (c1 as f64) < 5.0);
                assert!(s2 / (c2 as f64) < 5.0);
                passed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(passed, "both trackers should receive beacons from each other: reg1={:?} reg2={:?}", find_delay_histogram(&reg1), find_delay_histogram(&reg2));

    stop_and_await_terminated(ServiceContext::background(), &*t1.service()).await.unwrap();
    stop_and_await_terminated(ServiceContext::background(), &*t2.service()).await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_SkipsTrackingPropagationDelayOnInitialSync
#[tokio::test]
async fn test_propagation_delay_tracker_skips_tracking_on_initial_sync() {
    let kv = create_pt_test_kv(None).await;
    let codec = propagation_delay_tracker_codec();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    let cfg = PropagationDelayTrackerConfig {
        enabled: true,
        beacon_interval: GoDuration::from_millis(100),
        beacon_lifetime: GoDuration::from_secs(3600),
        log_beacons_latency_longer_than: GoDuration::ZERO,
    };

    let publisher = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg.clone(), "publisher-node".into(), None);
    start_and_await_running(ServiceContext::background(), &*publisher.service()).await.unwrap();

    let mut pre_existing = 0usize;
    for _ in 0..100 {
        if let Some(d) = get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY).await.unwrap() {
            if d.beacons.len() >= 2 {
                pre_existing = d.beacons.len();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(pre_existing >= 2);
    stop_and_await_terminated(ServiceContext::background(), &*publisher.service()).await.unwrap();

    let reg = Registry::new();
    let tracker = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, "test-node".into(), Some(&reg));
    start_and_await_running(ServiceContext::background(), &*tracker.service()).await.unwrap();

    let mut published_more = false;
    for _ in 0..100 {
        if let Some(d) = get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY).await.unwrap() {
            if d.beacons.len() > pre_existing {
                published_more = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(published_more);

    let (count, _) = find_delay_histogram(&reg).unwrap_or((0, 0.0));
    assert_eq!(0, count, "pre-existing and own beacons must not record delay");

    stop_and_await_terminated(ServiceContext::background(), &*tracker.service()).await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_PublishesFirstBeacon
#[tokio::test]
async fn test_propagation_delay_tracker_publishes_first_beacon() {
    let kv = create_pt_test_kv(None).await;
    let codec = propagation_delay_tracker_codec();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();
    assert!(get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY).await.unwrap().is_none());

    let cfg = PropagationDelayTrackerConfig {
        enabled: true,
        beacon_interval: GoDuration::from_millis(100),
        beacon_lifetime: GoDuration::from_secs(3600),
        log_beacons_latency_longer_than: GoDuration::ZERO,
    };
    let tracker = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, "test-node".into(), None);
    start_and_await_running(ServiceContext::background(), &*tracker.service()).await.unwrap();

    let mut found = None;
    for _ in 0..100 {
        if let Some(d) = get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY).await.unwrap() {
            if !d.beacons.is_empty() {
                found = Some(d);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let desc = found.expect("expected beacon to be published");
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    for b in desc.beacons.values() {
        assert!((now_ms - b.published_at).abs() < 10_000);
    }

    stop_and_await_terminated(ServiceContext::background(), &*tracker.service()).await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_OneBeaconPerIntervalPerCluster
#[tokio::test]
async fn test_propagation_delay_tracker_one_beacon_per_interval_per_cluster() {
    let kv = create_pt_test_kv(None).await;
    let codec = propagation_delay_tracker_codec();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    let beacon_interval = Duration::from_millis(100);
    let mut trackers = Vec::new();
    for i in 0..10 {
        let cfg = PropagationDelayTrackerConfig {
            enabled: true,
            beacon_interval: GoDuration(beacon_interval.as_nanos() as i64),
            beacon_lifetime: GoDuration::from_secs(3600),
            log_beacons_latency_longer_than: GoDuration::ZERO,
        };
        let t = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, format!("test-node-{i}"), None);
        start_and_await_running(ServiceContext::background(), &*t.service()).await.unwrap();
        trackers.push(t);
    }

    tokio::time::sleep(beacon_interval * 10).await;
    for t in &trackers {
        stop_and_await_terminated(ServiceContext::background(), &*t.service()).await.unwrap();
    }

    let desc = get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY)
        .await
        .unwrap()
        .unwrap();
    let beacon_count = desc.beacons.len();
    assert!(beacon_count > 0);
    let min_ts = desc.beacons.values().map(|b| b.published_at).min().unwrap();
    let max_ts = desc.beacons.values().map(|b| b.published_at).max().unwrap();
    let actual_dur_ms = (max_ts - min_ts).max(0) as u64;
    let expected = (actual_dur_ms / beacon_interval.as_millis() as u64) as usize + 1;
    assert!(
        beacon_count <= expected * 2,
        "expected at most {} beacons, got {}",
        expected * 2,
        beacon_count
    );

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_BeaconLifetimeGarbageCollection
#[tokio::test]
async fn test_propagation_delay_tracker_beacon_lifetime_garbage_collection() {
    let kv = create_pt_test_kv(Some(Duration::from_millis(100))).await;
    let codec = propagation_delay_tracker_codec();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    let cfg = PropagationDelayTrackerConfig {
        enabled: true,
        beacon_interval: GoDuration::from_millis(50),
        beacon_lifetime: GoDuration::from_millis(200),
        log_beacons_latency_longer_than: GoDuration::ZERO,
    };
    let tracker = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, "test-node".into(), None);
    start_and_await_running(ServiceContext::background(), &*tracker.service()).await.unwrap();

    let mut initial_id = None;
    for _ in 0..200 {
        if let Some(d) = get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY).await.unwrap() {
            if let Some((&id, _)) = d.beacons.iter().find(|(_, b)| b.deleted_at == 0) {
                initial_id = Some(id);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let initial_id = initial_id.expect("beacon should be published");

    let mut gc_done = false;
    for _ in 0..500 {
        if let Some(d) = get_typed::<PropagationDelayTrackerDesc>(&client, PROPAGATION_DELAY_TRACKER_KEY).await.unwrap() {
            if !d.beacons.contains_key(&initial_id) {
                gc_done = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(gc_done, "beacon {initial_id} should be garbage collected");

    stop_and_await_terminated(ServiceContext::background(), &*tracker.service()).await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_SkipsNegativeDelay
#[tokio::test]
async fn test_propagation_delay_tracker_skips_negative_delay() {
    let kv = create_pt_test_kv(None).await;
    let codec = propagation_delay_tracker_codec();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    let reg = Registry::new();
    let cfg = PropagationDelayTrackerConfig {
        enabled: true,
        beacon_interval: GoDuration::from_secs(3600),
        beacon_lifetime: GoDuration::from_secs(3600),
        log_beacons_latency_longer_than: GoDuration::ZERO,
    };
    let tracker = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, "test-node".into(), Some(&reg));
    start_and_await_running(ServiceContext::background(), &*tracker.service()).await.unwrap();

    const FUTURE_ID: u64 = 1;
    let mut seen = false;
    for _ in 0..200 {
        let future_ms = (SystemTime::now() + Duration::from_secs(10))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let _ = client
            .cas(
                PROPAGATION_DELAY_TRACKER_KEY,
                Box::new(move |in_val| {
                    let mut desc = get_or_create_propagation_delay_tracker_desc(in_val);
                    desc.beacons.insert(
                        FUTURE_ID,
                        BeaconDesc {
                            published_at: future_ms,
                            ..Default::default()
                        },
                    );
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await;
        if tracker.already_seen(FUTURE_ID) {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(seen, "tracker should have seen the beacon");
    let (count, _) = find_delay_histogram(&reg).unwrap_or((0, 0.0));
    assert_eq!(0, count, "negative delay must not be recorded");

    stop_and_await_terminated(ServiceContext::background(), &*tracker.service()).await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTracker_LogsHighLatencyBeacons
#[tokio::test(flavor = "current_thread")]
async fn test_propagation_delay_tracker_logs_high_latency_beacons() {
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<String>>>);
    struct Visitor(String);
    impl tracing::field::Visit for Visitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, " {}={:?}", field.name(), value);
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            let _ = write!(self.0, " {}={}", field.name(), value);
        }
    }
    struct Sub(Logs);
    impl tracing::Subscriber for Sub {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut v = Visitor(format!("level={}", event.metadata().level()));
            event.record(&mut v);
            self.0 .0.lock().unwrap().push(v.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    let logs = Logs::default();
    let _guard = tracing::subscriber::set_default(Sub(logs.clone()));

    let kv = create_pt_test_kv(None).await;
    let codec = propagation_delay_tracker_codec();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    let cfg = PropagationDelayTrackerConfig {
        enabled: true,
        beacon_interval: GoDuration::from_secs(3600),
        beacon_lifetime: GoDuration::from_secs(3600),
        log_beacons_latency_longer_than: GoDuration::from_millis(1),
    };
    let tracker = PropagationDelayTracker::new(Arc::downgrade(&kv), cfg, "test-node".into(), None);
    start_and_await_running(ServiceContext::background(), &*tracker.service()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    const HIGH_LATENCY_ID: u64 = 1;
    let mut logged = false;
    for _ in 0..200 {
        let past_ms = (SystemTime::now() - Duration::from_millis(10))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let _ = client
            .cas(
                PROPAGATION_DELAY_TRACKER_KEY,
                Box::new(move |in_val| {
                    let mut desc = get_or_create_propagation_delay_tracker_desc(in_val);
                    desc.beacons.insert(
                        HIGH_LATENCY_ID,
                        BeaconDesc {
                            published_at: past_ms,
                            published_by: "other-node".to_string(),
                            deleted_at: 0,
                        },
                    );
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await;
        if tracker.already_seen(HIGH_LATENCY_ID)
            && logs.0.lock().unwrap().iter().any(|l| l.contains("high latency beacon detected"))
        {
            logged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(logged, "expected high latency warning log");

    let joined = logs.0.lock().unwrap().join("\n");
    assert!(joined.contains("high latency beacon detected"));
    assert!(joined.contains("beacon_id=1"));
    assert!(joined.contains("published_by=other-node"));
    assert!(joined.contains("delay="));

    stop_and_await_terminated(ServiceContext::background(), &*tracker.service()).await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/propagation_tracker_test.go::TestPropagationDelayTrackerConfig_Validate
#[test]
fn test_propagation_delay_tracker_config_validate() {
    let cases = [
        (
            "disabled config is always valid",
            PropagationDelayTrackerConfig {
                enabled: false,
                beacon_interval: GoDuration::ZERO,
                beacon_lifetime: GoDuration::ZERO,
                log_beacons_latency_longer_than: GoDuration::ZERO,
            },
            "",
        ),
        (
            "valid config",
            PropagationDelayTrackerConfig {
                enabled: true,
                beacon_interval: GoDuration::from_secs(60),
                beacon_lifetime: GoDuration::from_secs(600),
                log_beacons_latency_longer_than: GoDuration::ZERO,
            },
            "",
        ),
        (
            "invalid - zero beacon interval",
            PropagationDelayTrackerConfig {
                enabled: true,
                beacon_interval: GoDuration::ZERO,
                beacon_lifetime: GoDuration::from_secs(600),
                log_beacons_latency_longer_than: GoDuration::ZERO,
            },
            "propagation delay tracker beacon interval must be greater than 0",
        ),
        (
            "invalid - negative beacon interval",
            PropagationDelayTrackerConfig {
                enabled: true,
                beacon_interval: GoDuration::from_secs(-60),
                beacon_lifetime: GoDuration::from_secs(600),
                log_beacons_latency_longer_than: GoDuration::ZERO,
            },
            "propagation delay tracker beacon interval must be greater than 0",
        ),
        (
            "invalid - zero beacon lifetime",
            PropagationDelayTrackerConfig {
                enabled: true,
                beacon_interval: GoDuration::from_secs(60),
                beacon_lifetime: GoDuration::ZERO,
                log_beacons_latency_longer_than: GoDuration::ZERO,
            },
            "propagation delay tracker beacon lifetime must be greater than 0",
        ),
    ];

    for (name, cfg, want_err) in cases {
        let res = cfg.validate();
        if want_err.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            let err = res.unwrap_err().to_string();
            assert!(err.contains(want_err), "{name}: got {err}");
        }
    }
}

// ============================================================================
// memberlist_client_test.go (local state cache tests)
// ============================================================================

const LSC_KEY: &str = "test";

async fn cas_ring_lsc<F>(client: &MemberlistKvClient, key: &str, mut f: F) -> Result<(), KvError>
where
    F: FnMut(Option<&RingDesc>) -> Result<Option<(RingDesc, bool)>, mimir_ring::kv::CasCallbackError>
        + Send
        + 'static,
{
    client
        .cas(
            key,
            Box::new(move |cur| {
                let typed = cur.and_then(|a| a.downcast_ref::<RingDesc>());
                match f(typed)? {
                    None => Ok(CasOutcome::NoChange),
                    Some((v, true)) => Ok(CasOutcome::Update(Box::new(v))),
                    Some((v, false)) => Ok(CasOutcome::UpdateOnce(Box::new(v))),
                }
            }),
        )
        .await
}

fn parse_local_state(payload: &[u8], codec: &dyn Codec) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    let mut rem = payload;
    while !rem.is_empty() {
        assert!(rem.len() >= 4, "truncated length prefix");
        let len = u32::from_be_bytes([rem[0], rem[1], rem[2], rem[3]]) as usize;
        rem = &rem[4..];
        assert!(rem.len() >= len, "truncated kv pair");
        let pair = <KeyValuePair as mimir_proto::prost::Message>::decode(&rem[..len]).unwrap();
        rem = &rem[len..];
        let decoded = codec.decode(&pair.value).unwrap();
        let desc = decoded.downcast_ref::<RingDesc>().unwrap();
        let mut names: Vec<String> = desc.ingesters.keys().cloned().collect();
        names.sort();
        out.insert(pair.key, names);
    }
    out
}

async fn new_kv_for_local_state_cache(
    left_timeout: Option<Duration>,
    history_bytes: Option<i64>,
) -> Arc<MemberlistKv> {
    let mut cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: "lsc".to_string(),
        local_state_cache_enabled: true,
        ..Default::default()
    };
    if let Some(t) = left_timeout {
        cfg.left_ingesters_timeout = GoDuration(t.as_nanos() as i64);
    }
    if let Some(b) = history_bytes {
        cfg.message_history_buffer_bytes = b;
    }
    let reg = Registry::new();
    let kv = MemberlistKv::start(cfg, vec![ring_desc_codec()], "", Some(&reg))
        .await
        .unwrap();
    kv.set_delegate_ready(true);
    kv
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestLocalStateCacheServesUnchangedEntriesFromCache
#[tokio::test]
async fn test_local_state_cache_serves_unchanged_entries_from_cache() {
    let codec = ring_desc_codec();
    let kv = new_kv_for_local_state_cache(None, None).await;
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    cas_ring_lsc(&client, "ring-a", |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();
    cas_ring_lsc(&client, "ring-b", |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("b".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    let first = kv.local_state(false);
    for k in ["ring-a", "ring-b"] {
        assert_eq!((0.0, 1.0), kv.local_state_cache_counts(k));
    }

    let second = kv.local_state(false);
    for k in ["ring-a", "ring-b"] {
        assert_eq!((1.0, 1.0), kv.local_state_cache_counts(k));
    }
    assert_eq!(parse_local_state(&first, codec.as_ref()), parse_local_state(&second, codec.as_ref()));

    cas_ring_lsc(&client, "ring-a", |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 2, state: InstanceState::Active as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();
    let _ = kv.local_state(false);
    assert_eq!((1.0, 2.0), kv.local_state_cache_counts("ring-a"));
    assert_eq!((2.0, 1.0), kv.local_state_cache_counts("ring-b"));

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestLocalStateCacheInvalidatedOnInPlaceMerge
#[tokio::test]
async fn test_local_state_cache_invalidated_on_in_place_merge() {
    let codec = ring_desc_codec();
    let kv = new_kv_for_local_state_cache(None, None).await;
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();
    cas_ring_lsc(&client, LSC_KEY, |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 1, state: InstanceState::Active as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();
    assert_eq!(vec!["a".to_string()], parse_local_state(&kv.local_state(false), codec.as_ref())[LSC_KEY]);

    let peer = new_kv_for_local_state_cache(None, None).await;
    let peer_client = MemberlistKvClient::new(Arc::clone(&peer), Arc::clone(&codec)).unwrap();
    cas_ring_lsc(&peer_client, LSC_KEY, |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("c".into(), InstanceDesc { timestamp: 5, state: InstanceState::Active as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    kv.merge_remote_state(&peer.local_state(false), false);
    assert_eq!(
        vec!["a".to_string(), "c".to_string()],
        parse_local_state(&kv.local_state(false), codec.as_ref())[LSC_KEY]
    );

    peer.shutdown().await;
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestLocalStateCacheInvalidatedOnTombstoneRemoval
#[tokio::test]
async fn test_local_state_cache_invalidated_on_tombstone_removal() {
    let codec = ring_desc_codec();
    let kv = new_kv_for_local_state_cache(Some(Duration::from_secs(60)), None).await;
    let expired = (SystemTime::now() - Duration::from_secs(3600))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let mut seeded = RingDesc::default();
    seeded.ingesters.insert("a".into(), InstanceDesc { timestamp: expired, state: InstanceState::Active as i32, ..Default::default() });
    seeded.ingesters.insert("left".into(), InstanceDesc { timestamp: expired, state: InstanceState::Left as i32, ..Default::default() });
    kv.store_insert_for_test(LSC_KEY, Box::new(seeded), 1, codec.codec_id(), false, Some(SystemTime::now()));

    assert_eq!(
        vec!["a".to_string(), "left".to_string()],
        parse_local_state(&kv.local_state(false), codec.as_ref())[LSC_KEY]
    );

    let mut incoming = RingDesc::default();
    incoming.ingesters.insert("left".into(), InstanceDesc { timestamp: expired + 1, state: InstanceState::Left as i32, ..Default::default() });
    let mut m: Box<dyn Mergeable> = Box::new(incoming);
    let ver = kv
        .merge_value_for_key_for_test(LSC_KEY, m.as_mut(), 0, codec.codec_id(), false, None)
        .unwrap();
    assert_eq!(0, ver);
    assert_eq!(
        vec!["a".to_string()],
        parse_local_state(&kv.local_state(false), codec.as_ref())[LSC_KEY]
    );

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestLocalStateCacheEvictedWithObsoleteEntries
#[tokio::test]
async fn test_local_state_cache_evicted_with_obsolete_entries() {
    let codec = ring_desc_codec();
    let kv = new_kv_for_local_state_cache(None, None).await;
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();
    cas_ring_lsc(&client, LSC_KEY, |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 1, state: InstanceState::Active as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    assert_eq!(1, parse_local_state(&kv.local_state(false), codec.as_ref()).len());
    assert!(kv.local_state_cache_contains(LSC_KEY));

    kv.delete_key(LSC_KEY).await.unwrap();
    let obsolete = kv.config().obsolete_entries_timeout.as_std().unwrap();
    kv.store_age_entry_for_test(LSC_KEY, SystemTime::now() - obsolete * 2);
    kv.cleanup_obsolete_entries();

    assert!(!kv.local_state_cache_contains(LSC_KEY));
    assert_eq!(0, kv.local_state_cache_len());
    assert!(parse_local_state(&kv.local_state(false), codec.as_ref()).is_empty());
    assert_eq!((0, 0), kv.local_state_cache_metric_counts());

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestLocalStateCacheMessageHistory
#[tokio::test]
async fn test_local_state_cache_message_history() {
    let codec = ring_desc_codec();
    let kv = new_kv_for_local_state_cache(None, Some(1024 * 1024)).await;
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();
    cas_ring_lsc(&client, LSC_KEY, |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 1, state: InstanceState::Active as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    let (before, _) = kv.sent_and_received_messages();
    let payload = kv.local_state(false);
    let _ = kv.local_state(false);

    let (sent, _) = kv.sent_and_received_messages();
    assert_eq!(before.len() + 2, sent.len());
    for msg in &sent[before.len()..] {
        assert_eq!(LSC_KEY, msg.pair.key);
        assert_eq!(codec.codec_id(), msg.pair.codec);
        assert_eq!(payload.len() - 4, msg.size);
        assert_eq!(1, msg.version);
        let decoded = codec.decode(&msg.pair.value).unwrap();
        let desc = decoded.downcast_ref::<RingDesc>().unwrap();
        assert_eq!(vec!["a"], desc.ingesters.keys().collect::<Vec<_>>());
    }

    kv.shutdown().await;
}

