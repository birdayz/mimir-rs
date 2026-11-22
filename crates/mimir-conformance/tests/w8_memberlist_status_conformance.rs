//! Tier-1 differential conformance for the memberlist status page (`GET /memberlist`).
//!
//! Compares, byte for byte, against `target/go-oracle`:
//! - `json.NewEncoder(w).Encode(memberlist.StatusPageData)` (dskit
//!   kv/memberlist/http_status_handler.go, `Accept: application/json`)
//! - Mimir's pkg/api/memberlist_status.gohtml (+ pkg/api/handlers.go template funcs) and dskit's
//!   kv/memberlist/status.gohtml rendered by Go `html/template`
//! - dskit `formatValue` for `?viewKey=` / `?viewMsg=` (`format=json`, `json-pretty`, `%#v`) of
//!   ring, partition ring and HA tracker values decoded by Mimir's memberlist codecs.

use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::SystemTime;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset};
use mimir_cmd::memberlist_status::render_memberlist_status_page;
use mimir_config::server::ZoneAwareRoutingConfig;
use mimir_conformance::oracle::{invoke_oracle, workspace_root};
use mimir_proto::memberlistpb::{BeaconDesc, KeyValuePair, PropagationDelayTrackerDesc};
use mimir_proto::ringpb::{Desc, InstanceDesc, OwnerDesc, PartitionDesc, PartitionRingDesc};
use mimir_ring::kv::ha_replica::{replica_desc_codec, ReplicaDesc};
use mimir_ring::kv::memberlist::http_status::{
    render_default_page, StatusMessage, StatusPageData, StatusValueDesc,
};
use mimir_ring::kv::memberlist::node_meta::{encode_node_metadata, NodeRole};
use mimir_ring::kv::memberlist::wire::NodeStateType;
use mimir_ring::kv::memberlist::{propagation_delay_tracker_codec, NodeState};
use mimir_ring::kv::Codec;
use mimir_ring::partition_ring::model::partition_ring_desc_codec;
use mimir_ring::ring::model::ring_desc_codec;
use serde::Deserialize;
use serde_json::{json, Value};

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// A time at `unix_nano` in a fixed zone, mirrored to the oracle as `{unix_nano, offset_secs}`.
#[derive(Clone, Copy)]
struct T {
    unix_nano: i64,
    offset_secs: i32,
}

impl T {
    fn dt(self) -> DateTime<FixedOffset> {
        DateTime::from_timestamp_nanos(self.unix_nano)
            .with_timezone(&FixedOffset::east_opt(self.offset_secs).unwrap())
    }
    fn json(self) -> Value {
        json!({"unix_nano": self.unix_nano, "offset_secs": self.offset_secs})
    }
}

struct Member {
    name: &'static str,
    addr: Vec<u8>,
    port: u16,
    meta: Option<Vec<u8>>,
    state: NodeStateType,
    vsn: [u8; 6],
}

struct Store {
    key: &'static str,
    version: usize,
    codec: &'static str,
    deleted: bool,
    update_time: Option<T>,
    size: Option<usize>,
}

struct Msg {
    id: u64,
    time: T,
    size: usize,
    key: &'static str,
    value: Vec<u8>,
    codec: &'static str,
    version: usize,
    changes: Vec<&'static str>,
}

struct Scenario {
    name: &'static str,
    now: T,
    health_score: i32,
    num_members: usize,
    members: Vec<Member>,
    store: Vec<Store>,
    total_store_size: usize,
    history_bytes: i64,
    sent: Vec<Msg>,
    received: Vec<Msg>,
    zone_aware: ZoneAwareRoutingConfig,
}

#[derive(Deserialize)]
struct PageResp {
    json: String,
    mimir_html: String,
    dskit_html: String,
}

fn rust_messages(msgs: &[Msg]) -> Vec<StatusMessage> {
    msgs.iter()
        .map(|m| StatusMessage {
            id: m.id,
            time: m.time.dt(),
            size: m.size,
            pair: KeyValuePair {
                key: m.key.into(),
                value: Bytes::from(m.value.clone()),
                codec: m.codec.into(),
                ..Default::default()
            },
            version: m.version,
            changes: m.changes.iter().map(|s| s.to_string()).collect(),
        })
        .collect()
}

fn oracle_messages(msgs: &[Msg]) -> Value {
    Value::Array(
        msgs.iter()
            .map(|m| {
                json!({
                    "id": m.id, "time": m.time.json(), "size": m.size, "key": m.key,
                    "value_b64": b64(&m.value), "codec": m.codec, "version": m.version,
                    "changes": if m.changes.is_empty() { Value::Null } else { json!(m.changes) },
                })
            })
            .collect(),
    )
}

fn check(s: &Scenario) {
    let rust = StatusPageData {
        now: s.now.dt(),
        health_score: s.health_score,
        num_members: s.num_members,
        sorted_members: s
            .members
            .iter()
            .map(|m| NodeState {
                name: m.name.into(),
                addr: m.addr.clone(),
                port: m.port,
                meta: m.meta.clone(),
                incarnation: 1,
                state: m.state,
                state_change: SystemTime::now(),
                vsn: m.vsn.to_vec(),
            })
            .collect(),
        store: s
            .store
            .iter()
            .map(|v| {
                (
                    v.key.to_string(),
                    StatusValueDesc {
                        version: v.version,
                        codec_id: v.codec.into(),
                        deleted: v.deleted,
                        update_time: v
                            .update_time
                            .map(T::dt)
                            .unwrap_or_else(mimir_ring::kv::memberlist::gofmt::go_zero_time),
                    },
                )
            })
            .collect(),
        store_sizes: s
            .store
            .iter()
            .filter_map(|v| v.size.map(|sz| (v.key.to_string(), sz)))
            .collect::<BTreeMap<_, _>>(),
        total_store_size: s.total_store_size,
        message_history_buffer_bytes: s.history_bytes,
        sent_messages: rust_messages(&s.sent),
        received_messages: rust_messages(&s.received),
        zone_aware_routing: s.zone_aware.clone(),
    };

    let root = workspace_root();
    let req = json!({
        "now": s.now.json(),
        "health_score": s.health_score,
        "num_members": s.num_members,
        "members": s.members.iter().map(|m| json!({
            "name": m.name, "addr_b64": b64(&m.addr), "port": m.port,
            "meta_b64": m.meta.as_deref().map(b64), "state": m.state as u8, "vsn": m.vsn,
        })).collect::<Vec<_>>(),
        "store": s.store.iter().map(|v| (v.key.to_string(), json!({
            "version": v.version, "codec_id": v.codec, "deleted": v.deleted,
            "update_time": v.update_time.map(T::json),
        }))).collect::<serde_json::Map<_, _>>(),
        "store_sizes": s.store.iter().filter_map(|v| v.size.map(|sz| (v.key.to_string(), json!(sz))))
            .collect::<serde_json::Map<_, _>>(),
        "total_store_size": s.total_store_size,
        "message_history_buffer_bytes": s.history_bytes,
        "sent": oracle_messages(&s.sent),
        "received": oracle_messages(&s.received),
        "zone_aware_enabled": s.zone_aware.enabled,
        "zone_aware_zone": s.zone_aware.instance_availability_zone,
        "zone_aware_role": s.zone_aware.role,
        "mimir_template_path": root.join(".ref/mimir/pkg/api/memberlist_status.gohtml"),
        "dskit_template_path": root.join(".ref/mimir/vendor/github.com/grafana/dskit/kv/memberlist/status.gohtml"),
    });
    let go: PageResp = invoke_oracle("memberlist-status-page", &req).expect("oracle");

    assert_eq!(rust.to_go_json().unwrap(), go.json, "{}: StatusPageData JSON", s.name);
    assert_eq!(
        render_memberlist_status_page(&rust),
        go.mimir_html,
        "{}: Mimir memberlist_status.gohtml",
        s.name
    );
    assert_eq!(render_default_page(&rust), go.dskit_html, "{}: dskit status.gohtml", s.name);
}

fn mapped(a: u8, b: u8, c: u8, d: u8) -> Vec<u8> {
    Ipv4Addr::new(a, b, c, d).to_ipv6_mapped().octets().to_vec()
}

#[test]
fn w8_memberlist_status_page_matches_go() {
    let utc = |unix_nano| T { unix_nano, offset_secs: 0 };
    let ist = |unix_nano| T { unix_nano, offset_secs: 5 * 3600 + 1800 };
    let vsn = [1, 5, 2, 0, 0, 0];

    // Healthy, no store, history disabled (what a fresh ingester shows).
    check(&Scenario {
        name: "minimal",
        now: utc(1_763_342_601_214_840_496),
        health_score: 0,
        num_members: 1,
        members: vec![Member {
            name: "mimir-ingester-zone-a-0-52c36f2b",
            addr: mapped(10, 42, 3, 4),
            port: 7946,
            meta: None,
            state: NodeStateType::Alive,
            vsn,
        }],
        store: vec![],
        total_store_size: 0,
        history_bytes: 0,
        sent: vec![],
        received: vec![],
        zone_aware: ZoneAwareRoutingConfig::default(),
    });

    // The k3d shape: several ring keys, sizes, Go zero UpdateTime.
    check(&Scenario {
        name: "k3d-like",
        now: utc(1_763_342_601_000_000_000),
        health_score: 0,
        num_members: 3,
        members: vec![
            Member { name: "mimir-compactor-0-348a3ca2", addr: mapped(10, 42, 1, 7), port: 7946, meta: None, state: NodeStateType::Alive, vsn },
            Member { name: "mimir-distributor-x-68caec66", addr: vec![10, 42, 0, 3], port: 7946, meta: None, state: NodeStateType::Alive, vsn },
            Member { name: "mimir-store-gateway-0-d1a70766", addr: mapped(10, 42, 0, 10), port: 7946, meta: None, state: NodeStateType::Alive, vsn },
        ],
        store: vec![
            Store { key: "collectors/compactor", version: 2, codec: "ringDesc", deleted: false, update_time: None, size: Some(2602) },
            Store { key: "collectors/ingester-partitions", version: 4, codec: "partitionRingDesc", deleted: false, update_time: None, size: Some(2111) },
            Store { key: "collectors/ring", version: 6, codec: "ringDesc", deleted: false, update_time: None, size: Some(7854) },
        ],
        total_store_size: 12567,
        history_bytes: 0,
        sent: vec![],
        received: vec![],
        zone_aware: ZoneAwareRoutingConfig::default(),
    });

    // Unhealthy, zone-aware, all member states, IPv6, escaping, deleted key, missing size,
    // message history with and without changes, non-UTC zone.
    let meta = encode_node_metadata(NodeRole::Bridge, "zone-<b>").unwrap();
    check(&Scenario {
        name: "full",
        now: ist(1_700_000_000_123_456_789),
        health_score: 3,
        num_members: 4,
        members: vec![
            Member { name: "a-alive", addr: mapped(10, 0, 0, 1), port: 7946, meta: Some(meta.clone()), state: NodeStateType::Alive, vsn },
            Member { name: "b-suspect", addr: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1).octets().to_vec(), port: 7947, meta: Some(encode_node_metadata(NodeRole::Member, "zone-a").unwrap()), state: NodeStateType::Suspect, vsn },
            Member { name: "c-dead&<x>'\"+", addr: vec![192, 168, 0, 1], port: 1, meta: None, state: NodeStateType::Dead, vsn: [0; 6] },
            Member { name: "d-alive", addr: mapped(10, 0, 0, 4), port: 65535, meta: Some(vec![9, 9]), state: NodeStateType::Alive, vsn: [1, 5, 2, 1, 2, 3] },
        ],
        store: vec![
            Store { key: "collectors/ring", version: 12, codec: "ringDesc", deleted: false, update_time: Some(ist(1_700_000_000_500_000_000)), size: Some(1_048_576) },
            Store { key: "ha/a<b>&c+d ü", version: 1, codec: "replicaDesc", deleted: true, update_time: Some(utc(1_699_999_999_000_000_001)), size: Some(9) },
            Store { key: "no-size", version: 0, codec: "unknown", deleted: false, update_time: None, size: None },
        ],
        total_store_size: 1_048_585,
        history_bytes: 1 << 20,
        sent: vec![
            Msg { id: 2, time: ist(1_700_000_000_007_000_000), size: 120, key: "collectors/ring", value: vec![1, 2, 3], codec: "ringDesc", version: 12, changes: vec!["ing-1", "ing<2>"] },
            Msg { id: 4, time: ist(1_700_000_001_999_999_999), size: 0, key: "collectors/ring", value: vec![], codec: "ringDesc", version: 0, changes: vec![] },
        ],
        received: vec![Msg { id: 3, time: utc(1_700_000_000_000_000_000), size: 77, key: "ha/x", value: vec![0; 77], codec: "replicaDesc", version: 0, changes: vec!["r1"] }],
        zone_aware: ZoneAwareRoutingConfig { enabled: true, instance_availability_zone: "zone-<b>".into(), role: "bridge".into() },
    });

    // A left member: Mimir's template fails on `{{ else if eq.State 3}}` (partial page + error).
    check(&Scenario {
        name: "left-member",
        now: utc(1_700_000_000_000_000_000),
        health_score: 0,
        num_members: 1,
        members: vec![
            Member { name: "a", addr: mapped(10, 0, 0, 1), port: 7946, meta: None, state: NodeStateType::Alive, vsn },
            Member { name: "b-left", addr: mapped(10, 0, 0, 2), port: 7946, meta: None, state: NodeStateType::Left, vsn },
        ],
        store: vec![],
        total_store_size: 0,
        history_bytes: 0,
        sent: vec![],
        received: vec![],
        zone_aware: ZoneAwareRoutingConfig::default(),
    });

    // History enabled but empty.
    check(&Scenario {
        name: "history-empty",
        now: utc(1_700_000_000_000_000_000),
        health_score: 1,
        num_members: 0,
        members: vec![],
        store: vec![],
        total_store_size: 0,
        history_bytes: 4096,
        sent: vec![],
        received: vec![],
        zone_aware: ZoneAwareRoutingConfig { enabled: false, instance_availability_zone: String::new(), role: "member".into() },
    });
}

#[derive(Deserialize)]
struct FormatResp {
    body: String,
}

fn check_format_value(codec: &dyn Codec, encoded: &[u8], what: &str) {
    let val = codec.decode_mergeable(encoded).unwrap();
    for format in ["json", "json-pretty", "struct", ""] {
        let go: FormatResp = invoke_oracle(
            "memberlist-format-value",
            &json!({"codec": codec.codec_id(), "value_b64": b64(encoded), "format": format}),
        )
        .expect("oracle");
        let rust = match format {
            "json" => val.to_go_json().encode(),
            "json-pretty" => val.to_go_json().encode_indent("    "),
            _ => val.go_string(),
        };
        assert_eq!(rust, go.body, "{what} format={format:?}");
    }
}

#[test]
fn w8_memberlist_format_value_matches_go() {
    let ring = ring_desc_codec();
    let mut desc = Desc::default();
    desc.ingesters.insert(
        "ingester-zone-a-0".into(),
        InstanceDesc {
            addr: "10.42.3.4:9095".into(),
            timestamp: 1_763_342_601,
            state: 0,
            tokens: vec![1, 4_294_967_295, 77],
            zone: "zone-a".into(),
            registered_timestamp: 1_763_340_000,
            id: "ingester-zone-a-0".into(),
            read_only_updated_timestamp: 5,
            read_only: true,
            versions: HashMap::from([(10, 2), (2, 0), (3, u64::MAX)]),
        },
    );
    desc.ingesters.insert(
        "b<&>\"ü".into(),
        InstanceDesc { state: 2, ..Default::default() },
    );
    check_format_value(ring.as_ref(), &ring.encode_mergeable(&desc).unwrap(), "ring.Desc");
    check_format_value(ring.as_ref(), &ring.encode_mergeable(&Desc::default()).unwrap(), "empty ring.Desc");

    let prc = partition_ring_desc_codec();
    let mut prd = PartitionRingDesc::default();
    for id in [1, 2, 10] {
        prd.partitions.insert(
            id,
            PartitionDesc {
                id,
                tokens: if id == 2 { vec![] } else { vec![id as u32, 99] },
                state: id % 3,
                state_timestamp: 1_700_000_000 + i64::from(id),
                state_change_locked: id == 10,
                state_change_locked_timestamp: if id == 10 { 7 } else { 0 },
                token_scheme: i32::from(id == 1),
            },
        );
    }
    prd.owners.insert("ingester-zone-b-0".into(), OwnerDesc { owned_partition: 0, state: 1, updated_timestamp: 1_700_000_000 });
    prd.owners.insert("ingester-zone-a-0".into(), OwnerDesc { owned_partition: 10, state: 2, updated_timestamp: 0 });
    check_format_value(prc.as_ref(), &prc.encode_mergeable(&prd).unwrap(), "ring.PartitionRingDesc");

    let rdc = replica_desc_codec();
    let rd = ReplicaDesc {
        replica: "replica-\"1\"".into(),
        received_at: 1_700_000_000_000,
        deleted_at: 0,
        elected_at: 1_699_999_999_000,
        elected_changes: 3,
    };
    check_format_value(rdc.as_ref(), &rdc.encode_mergeable(&rd).unwrap(), "distributor.ReplicaDesc");
}

#[derive(Deserialize)]
struct FormatPropTrackerResp {
    go_string: String,
    json_encode: String,
    json_encode_indent: String,
}

#[test]
fn w8_propagation_tracker_format_value_matches_go() {
    let codec = propagation_delay_tracker_codec();

    let empty = PropagationDelayTrackerDesc::default();
    let mut single = PropagationDelayTrackerDesc::default();
    single.beacons.insert(
        2,
        BeaconDesc {
            published_by: "node-a".into(),
            published_at: 1000,
            deleted_at: 0,
        },
    );
    let mut multi = single.clone();
    multi.beacons.insert(
        10,
        BeaconDesc {
            published_by: "node-b<&>\"ü".into(),
            published_at: 2000,
            deleted_at: 3000,
        },
    );
    multi.beacons.insert(
        0xdead_beef,
        BeaconDesc {
            published_by: "node-c".into(),
            published_at: 4000,
            deleted_at: 0,
        },
    );

    for (what, desc) in [("empty", &empty), ("single", &single), ("multi", &multi)] {
        let encoded = codec.encode_mergeable(desc).unwrap();
        let val = codec.decode_mergeable(&encoded).unwrap();
        let go: FormatPropTrackerResp = invoke_oracle(
            "format-propagation-tracker",
            &json!({ "value_b64": b64(&encoded) }),
        )
        .expect("oracle format-propagation-tracker");

        assert_eq!(val.to_go_json().encode(), go.json_encode, "{what} format=json");
        assert_eq!(
            val.to_go_json().encode_indent("    "),
            go.json_encode_indent,
            "{what} format=json-pretty"
        );
        assert_eq!(val.go_string(), go.go_string, "{what} format=struct");
    }
}
