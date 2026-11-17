//! Tests ported from github.com/grafana/mimir/pkg/distributor/ha_tracker_test.go
//! (ReplicaDesc merge) plus codec/wire coverage for the `replicaDesc` codec.

use mimir_ring::kv::ha_replica::{replica_desc_codec, ReplicaDesc};
use mimir_ring::kv::memberlist::Mergeable;
use prost::Message;

/// Go test helper `merge(r1, r2)`.
fn merge(mut r1: ReplicaDesc, r2: Option<ReplicaDesc>) -> (ReplicaDesc, Option<ReplicaDesc>) {
    let change = match r2 {
        None => None,
        Some(mut r2) => Mergeable::merge(&mut r1, &mut r2, false)
            .unwrap()
            .map(|c| c.into_any().downcast::<ReplicaDesc>().map(|b| *b).unwrap()),
    };
    (r1, change)
}

fn rd(replica: &str, received_at: i64, elected_at: i64, elected_changes: i64) -> ReplicaDesc {
    ReplicaDesc {
        replica: replica.to_string(),
        received_at,
        deleted_at: 0,
        elected_at,
        elected_changes,
    }
}

// Upstream: pkg/distributor/ha_tracker_test.go::TestReplicaDescMerge
#[test]
fn test_replica_desc_merge() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let (replica1, replica2, replica3) = ("r1", "r2", "r3");

    let first_replica = || rd(replica1, now, now, 1);
    let first_replica_with_higher_received_at = || rd(replica1, now + 5, now, 1);
    let second_replica = || rd(replica2, now, now + 5, 2);
    let third_replica = || rd(replica3, now, now + 10, 3);
    let expected_first_and_first_higher_received_at_merge = || rd(replica1, now + 5, now, 1);
    let expected_first_and_second_merge = || rd(replica2, now, now + 5, 2);

    struct Case {
        name: &'static str,
        r_desc1: ReplicaDesc,
        r_desc2: Option<ReplicaDesc>,
        expected_r_desc: ReplicaDesc,
        expected_change: Option<ReplicaDesc>,
    }

    let tests = vec![
        Case {
            name: "Merge ReplicaDesc: Same replica name, different receivedAt should return ReplicaDesc with most recent receivedAt timestamp",
            r_desc1: first_replica(),
            r_desc2: Some(first_replica_with_higher_received_at()),
            expected_r_desc: expected_first_and_first_higher_received_at_merge(),
            expected_change: Some(expected_first_and_first_higher_received_at_merge()),
        },
        Case {
            name: "Merge ReplicaDesc: Different replica name, different electedAt should return ReplicaDesc with most recent electedAt timestamp",
            r_desc1: first_replica(),
            r_desc2: Some(second_replica()),
            expected_r_desc: expected_first_and_second_merge(),
            expected_change: Some(expected_first_and_second_merge()),
        },
        Case {
            name: "idempotency: no change after applying same ReplicaDesc again.",
            r_desc1: merge(first_replica(), Some(second_replica())).0,
            r_desc2: Some(first_replica()),
            expected_r_desc: expected_first_and_second_merge(),
            expected_change: None,
        },
        Case {
            name: "commutativity: Merge(first, second) == Merge(second, first)",
            r_desc1: first_replica(),
            r_desc2: Some(second_replica()),
            expected_r_desc: merge(second_replica(), Some(first_replica())).0,
            expected_change: Some(expected_first_and_second_merge()),
        },
        Case {
            name: "associativity: Merge(Merge(first, second), third) == Merge(first, Merge(second, third))",
            r_desc1: {
                let (ours1, _) = merge(first_replica(), Some(second_replica()));
                merge(ours1, Some(third_replica())).0
            },
            r_desc2: None,
            expected_r_desc: {
                let (ours2, _) = merge(second_replica(), Some(third_replica()));
                merge(first_replica(), Some(ours2)).0
            },
            expected_change: None,
        },
        Case {
            name: "Merge should return no change when replica is the same",
            r_desc1: first_replica(),
            r_desc2: Some(first_replica()),
            expected_r_desc: first_replica(),
            expected_change: None,
        },
    ];

    for tt in tests {
        let (r_desc, ch) = merge(tt.r_desc1, tt.r_desc2);
        assert_eq!(r_desc, tt.expected_r_desc, "{}", tt.name);
        assert_eq!(ch, tt.expected_change, "{}", tt.name);
    }
}

/// Same replica, same receivedAt: a deletion marker wins (ha_tracker.go mergeWithTime).
#[test]
fn deleted_marker_wins_on_equal_received_at() {
    let mut a = rd("r1", 10, 5, 1);
    let mut b = a.clone();
    b.deleted_at = 20;
    let ch = Mergeable::merge(&mut a, &mut b, false).unwrap();
    assert!(ch.is_some());
    assert_eq!(a.deleted_at, 20);
    // And not the other way round.
    let mut c = rd("r1", 10, 5, 1);
    assert!(Mergeable::merge(&mut a, &mut c, false).unwrap().is_none());
    assert_eq!(a.deleted_at, 20);
}

/// Different replicas with equal electedAt: the most recent receivedAt wins.
#[test]
fn equal_elected_at_compares_received_at() {
    let mut a = rd("r1", 10, 5, 1);
    let mut b = rd("r2", 11, 5, 2);
    assert!(Mergeable::merge(&mut a, &mut b, false).unwrap().is_some());
    assert_eq!(a.replica, "r2");
    let mut c = rd("r3", 11, 5, 3);
    assert!(Mergeable::merge(&mut a, &mut c, false).unwrap().is_none());
}

#[test]
fn merge_content_remove_tombstones_and_clone() {
    let r = rd("r1", 1, 2, 3);
    assert_eq!(
        r.merge_content(),
        vec![
            "&ReplicaDesc{Replica:r1,ReceivedAt:1,DeletedAt:0,ElectedAt:2,ElectedChanges:3,}"
                .to_string()
        ]
    );
    assert!(ReplicaDesc::default().merge_content().is_empty());
    let mut r2 = r.clone();
    assert_eq!(r2.remove_tombstones(None), (0, 0));
    let cl = r.clone_mergeable();
    assert_eq!(cl.as_any().downcast_ref::<ReplicaDesc>(), Some(&r));
}

/// `replicaDesc` codec: gogo-compatible proto3 bytes, snappy block compressed.
#[test]
fn replica_desc_codec_wire_format() {
    let codec = replica_desc_codec();
    assert_eq!(codec.codec_id(), "replicaDesc");
    let r = ReplicaDesc {
        replica: "r1".to_string(),
        received_at: 1,
        deleted_at: 0,
        elected_at: 300,
        elected_changes: 2,
    };
    // Field order and zero-value omission as gogo's MarshalToSizedBuffer.
    let raw = r.encode_to_vec();
    assert_eq!(
        raw,
        vec![0x0a, 0x02, b'r', b'1', 0x10, 0x01, 0x20, 0xac, 0x02, 0x28, 0x02]
    );
    let enc = codec.encode_mergeable(&r).unwrap();
    assert_eq!(snap::raw::Decoder::new().decompress_vec(&enc).unwrap(), raw);
    let dec = codec.decode_mergeable(&enc).unwrap();
    assert_eq!(dec.as_any().downcast_ref::<ReplicaDesc>(), Some(&r));
    let dec_any = codec.decode(&enc).unwrap();
    assert_eq!(dec_any.downcast_ref::<ReplicaDesc>(), Some(&r));
}
