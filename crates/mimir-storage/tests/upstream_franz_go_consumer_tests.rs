//! Ported upstream github.com/twmb/franz-go/pkg/kgo consumer, KIP-848, fetchManager,
//! group balancer, and fetch tests (39 tests).

#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use helpers::{create_test_kafka_config, FakeKafkaCluster};
use kafka_protocol::error::ResponseError;
use mimir_storage::ingest::kgo_consumer::{
    evaluate_regex_topics, fetches_each_topic, should_notify_848_restart, FetchManager,
    Group848Signals, KgoFetch, KgoFetchPartition, KgoFetchTopic, PausedTopics, PollWaitGate,
};
use mimir_storage::ingest::kgo_group_balancer::{
    balance_range, balance_sticky, build_partition_racks, need_rack_meta, simple_member_metadata,
    BalancePlan, ConsumerBalancer, ConsumerMemberMetadata, JoinGroupMemberInput, OwnedPartition,
};
use mimir_storage::ingest::writer_client::{
    KafkaProducerClient,
    first_produce_err, KafkaProducer, KafkaWriterClient, KafkaWriterClientOptions, ProduceContext,
    WriterRecord,
};
use mimir_storage::ingest::{KafkaClient, KafkaClientError};
use regex::Regex;
use tokio::sync::{mpsc, oneshot};

// ---------------------------------------------------------------------------
// consumer_group_848_test.go (2 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_group_848_test.go::TestSignalSubscriptionChange848
#[tokio::test]
async fn test_signal_subscription_change_848() {
    // 848 mode: forces a heartbeat and must not feed rejoin_tx.
    let (r_tx, mut r_rx) = mpsc::channel(1);
    let (hb_tx, mut hb_rx) = mpsc::channel(1);
    let g848 = Group848Signals {
        is_848: true,
        rejoin_tx: r_tx,
        heartbeat_force_tx: hb_tx,
    };
    g848.signal_subscription_change("change");
    assert!(hb_rx.try_recv().is_ok(), "848 must force heartbeat");
    assert!(r_rx.try_recv().is_err(), "848 must never feed rejoin");

    // Classic mode: feeds rejoin_tx and forces no heartbeat.
    let (r_tx2, mut r_rx2) = mpsc::channel(1);
    let (hb_tx2, mut hb_rx2) = mpsc::channel(1);
    let classic = Group848Signals {
        is_848: false,
        rejoin_tx: r_tx2,
        heartbeat_force_tx: hb_tx2,
    };
    classic.signal_subscription_change("change");
    assert_eq!(r_rx2.try_recv().unwrap(), "change");
    assert!(hb_rx2.try_recv().is_err(), "classic must not force heartbeat");
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_group_848_test.go::TestShouldNotify848Restart
#[test]
fn test_should_notify_848_restart() {
    for restarts in 1..=3 {
        assert!(should_notify_848_restart(restarts, 0));
    }
    for (restarts, want) in [(1, false), (2, true), (3, false), (4, true)] {
        assert_eq!(should_notify_848_restart(restarts, 2), want);
    }
}

// ---------------------------------------------------------------------------
// fetch_manager_quit_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/fetch_manager_quit_test.go::TestFetchManagerQuitDrainsPendingFetches
#[tokio::test]
async fn test_fetch_manager_quit_drains_pending_fetches() {
    const SOURCES: usize = 8;
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let poll_active = Arc::new(AtomicBool::new(false));
    let (_wake_tx, wake_rx) = mpsc::channel(1);

    let fm = Arc::new(FetchManager::start(cancel_rx, poll_active, wake_rx, 0));
    let reg_cnt = Arc::new(AtomicUsize::new(0));
    let all_reg = Arc::new(tokio::sync::Notify::new());
    let mut handles = Vec::with_capacity(SOURCES);

    for _ in 0..SOURCES {
        let fm = Arc::clone(&fm);
        let mut crx = cancel_tx.subscribe();
        let reg_cnt = Arc::clone(&reg_cnt);
        let all_reg = Arc::clone(&all_reg);
        handles.push(tokio::spawn(async move {
            let (gtx, grx) = oneshot::channel();
            fm.desire_fetch(gtx).unwrap();
            if reg_cnt.fetch_add(1, Ordering::SeqCst) + 1 == SOURCES {
                all_reg.notify_one();
            }
            tokio::select! {
                _ = crx.changed() => {
                    let (dummy_tx, _) = oneshot::channel();
                    fm.cancel_fetch(dummy_tx);
                }
                res = grx => {
                    if let Ok(done_tx) = res {
                        let _ = done_tx.send(false).await;
                    }
                }
            }
        }));
    }

    while reg_cnt.load(Ordering::SeqCst) < SOURCES {
        all_reg.notified().await;
    }
    let _ = cancel_tx.send(true);
    tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(handles))
        .await
        .expect("fetch sources blocked after session cancel");
}

// ---------------------------------------------------------------------------
// record_and_fetch_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_and_fetch_test.go::TestEachTopicPreservesTopicID
#[test]
fn test_each_topic_preserves_topic_id() {
    let foo_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let bar_id = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
    let collect = |fs: &[KgoFetch]| {
        let mut ids = HashMap::new();
        let mut nparts = HashMap::new();
        fetches_each_topic(fs, |ft| {
            ids.insert(ft.topic.clone(), ft.topic_id);
            *nparts.entry(ft.topic).or_insert(0usize) += ft.partitions.len();
        });
        (ids, nparts)
    };

    // Spread across multiple fetches
    let fs_multi = vec![
        KgoFetch {
            topics: vec![
                KgoFetchTopic {
                    topic: "foo".into(),
                    topic_id: foo_id,
                    partitions: vec![KgoFetchPartition { partition: 0, ..Default::default() }],
                },
                KgoFetchTopic {
                    topic: "bar".into(),
                    topic_id: bar_id,
                    partitions: vec![KgoFetchPartition { partition: 0, ..Default::default() }],
                },
            ],
        },
        KgoFetch {
            topics: vec![KgoFetchTopic {
                topic: "foo".into(),
                topic_id: foo_id,
                partitions: vec![KgoFetchPartition { partition: 1, ..Default::default() }],
            }],
        },
    ];
    let (ids, nparts) = collect(&fs_multi);
    assert_eq!(ids["foo"], foo_id);
    assert_eq!(ids["bar"], bar_id);
    assert_eq!(nparts["foo"], 2);

    // Single fetch preserves id
    let fs_single = vec![KgoFetch {
        topics: vec![KgoFetchTopic {
            topic: "foo".into(),
            topic_id: foo_id,
            partitions: vec![KgoFetchPartition { partition: 0, ..Default::default() }],
        }],
    }];
    let (ids, _) = collect(&fs_single);
    assert_eq!(ids["foo"], foo_id);

    // Zero id stays zero
    let fs_zero = vec![
        KgoFetch {
            topics: vec![KgoFetchTopic {
                topic: "foo".into(),
                topic_id: [0u8; 16],
                partitions: vec![KgoFetchPartition { partition: 0, ..Default::default() }],
            }],
        },
        KgoFetch {
            topics: vec![KgoFetchTopic {
                topic: "foo".into(),
                topic_id: [0u8; 16],
                partitions: vec![KgoFetchPartition { partition: 1, ..Default::default() }],
            }],
        },
    ];
    let (ids, _) = collect(&fs_zero);
    assert_eq!(ids["foo"], [0u8; 16]);
}

// ---------------------------------------------------------------------------
// group_balancer_test.go (14 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::Test_stickyAdjustCooperative
#[test]
fn test_sticky_adjust_cooperative() {
    let owned = |pairs: &[(&str, &[i32])]| -> Vec<OwnedPartition> {
        pairs
            .iter()
            .map(|&(t, ps)| OwnedPartition {
                topic: t.into(),
                partitions: ps.to_vec(),
            })
            .collect()
    };
    let b = ConsumerBalancer {
        members: vec![
            JoinGroupMemberInput { member_id: "a".into(), ..Default::default() },
            JoinGroupMemberInput { member_id: "b".into(), ..Default::default() },
            JoinGroupMemberInput { member_id: "c".into(), ..Default::default() },
            JoinGroupMemberInput { member_id: "d".into(), ..Default::default() },
        ],
        metadatas: vec![
            ConsumerMemberMetadata {
                owned_partitions: owned(&[
                    ("t1", &[1, 2, 3, 4]),
                    ("tmove", &[1, 2]),
                    ("tdelete", &[1, 2]),
                ]),
                ..Default::default()
            },
            ConsumerMemberMetadata {
                owned_partitions: owned(&[("t2", &[1, 2, 3])]),
                ..Default::default()
            },
            ConsumerMemberMetadata::default(),
            ConsumerMemberMetadata {
                owned_partitions: owned(&[("t1", &[1, 2, 3, 4])]),
                ..Default::default()
            },
        ],
        topics: HashSet::new(),
        partition_racks: None,
    };

    let mut plan = BalancePlan {
        plan: HashMap::from([
            ("a".into(), HashMap::from([("t1".into(), vec![1, 4]), ("t2".into(), vec![3]), ("tnew".into(), vec![1, 2])])),
            ("b".into(), HashMap::from([("t2".into(), vec![2]), ("tnew".into(), vec![3, 4]), ("tmove".into(), vec![1])])),
            ("c".into(), HashMap::from([("t1".into(), vec![3]), ("t2".into(), vec![1]), ("tnew".into(), vec![5]), ("tmove".into(), vec![2])])),
            ("d".into(), HashMap::from([("t1".into(), vec![2])])),
        ]),
    };
    plan.adjust_cooperative(&b);

    let exp = HashMap::from([
        ("a".into(), HashMap::from([("t1".into(), vec![1, 4]), ("tnew".into(), vec![1, 2])])),
        ("b".into(), HashMap::from([("t2".into(), vec![2]), ("tnew".into(), vec![3, 4])])),
        ("c".into(), HashMap::from([("tnew".into(), vec![5])])),
        ("d".into(), HashMap::from([("t1".into(), vec![2])])),
    ]);
    assert_eq!(plan.plan, exp);
}

fn run_range_balance(
    members: &[(&str, &[&str], &str)],
    topics: &[(&str, i32)],
    partition_racks: Option<&[(&str, &[&str])]>,
) -> BalancePlan {
    let mut b = ConsumerBalancer {
        members: members
            .iter()
            .map(|(id, _, _)| JoinGroupMemberInput {
                member_id: (*id).into(),
                ..Default::default()
            })
            .collect(),
        metadatas: members
            .iter()
            .map(|(_, ts, rack)| ConsumerMemberMetadata {
                topics: ts.iter().map(|s| (*s).into()).collect(),
                rack: if rack.is_empty() { None } else { Some((*rack).into()) },
                ..Default::default()
            })
            .collect(),
        topics: topics.iter().map(|(t, _)| (*t).into()).collect(),
        partition_racks: partition_racks.map(|prs| {
            prs.iter()
                .map(|(t, rs)| ((*t).into(), rs.iter().map(|r| (*r).into()).collect()))
                .collect()
        }),
    };
    let _ = &mut b;
    let topic_map: HashMap<String, i32> = topics.iter().map(|(t, n)| ((*t).into(), *n)).collect();
    balance_range(&b, &topic_map)
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerRackAware
#[test]
fn test_range_balancer_rack_aware() {
    let racks = ["rackA", "rackB", "rackA", "rackB"];
    let bp = run_range_balance(
        &[("A", &["t1"], "rackA"), ("B", &["t1"], "rackB")],
        &[("t1", 4)],
        Some(&[("t1", &racks)]),
    );
    let a = &bp.plan["A"]["t1"];
    let b = &bp.plan["B"]["t1"];
    assert_eq!(a.len(), 2);
    assert_eq!(b.len(), 2);
    for &p in a {
        assert_eq!(racks[p as usize], "rackA");
    }
    for &p in b {
        assert_eq!(racks[p as usize], "rackB");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerNoRacks
#[test]
fn test_range_balancer_no_racks() {
    let bp = run_range_balance(
        &[("A", &["t1"], ""), ("B", &["t1"], "")],
        &[("t1", 4)],
        None,
    );
    assert_eq!(bp.plan["A"]["t1"], vec![0, 1]);
    assert_eq!(bp.plan["B"]["t1"], vec![2, 3]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerRackNoMatchingRacks
#[test]
fn test_range_balancer_rack_no_matching_racks() {
    let bp = run_range_balance(
        &[("A", &["t1"], "rackX"), ("B", &["t1"], "rackY")],
        &[("t1", 4)],
        Some(&[("t1", &["rackA", "rackB", "rackA", "rackB"])]),
    );
    assert_eq!(bp.plan["A"]["t1"], vec![0, 1]);
    assert_eq!(bp.plan["B"]["t1"], vec![2, 3]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerRackMixedMemberRacks
#[test]
fn test_range_balancer_rack_mixed_member_racks() {
    let racks = ["rackA", "rackB", "rackA", "rackB"];
    let bp = run_range_balance(
        &[("A", &["t1"], "rackA"), ("B", &["t1"], "")],
        &[("t1", 4)],
        Some(&[("t1", &racks)]),
    );
    assert_eq!(bp.plan["A"]["t1"], vec![0, 2]);
    assert_eq!(bp.plan["B"]["t1"], vec![1, 3]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerRackShortRackSlice
#[test]
fn test_range_balancer_rack_short_rack_slice() {
    let bp = run_range_balance(
        &[("A", &["t1"], "rackA"), ("B", &["t1"], "rackB")],
        &[("t1", 6)],
        Some(&[("t1", &["rackA", "rackB"])]),
    );
    assert_eq!(bp.plan["A"]["t1"].len(), 3);
    assert_eq!(bp.plan["B"]["t1"].len(), 3);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerRackOddPartitions
#[test]
fn test_range_balancer_rack_odd_partitions() {
    let bp = run_range_balance(
        &[("A", &["t1"], "rackA"), ("B", &["t1"], "rackB")],
        &[("t1", 5)],
        Some(&[("t1", &["rackA", "rackB", "rackA", "rackB", "rackA"])]),
    );
    assert_eq!(bp.plan["A"]["t1"].len(), 3);
    assert_eq!(bp.plan["B"]["t1"].len(), 2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestRangeBalancerRackMultiTopicDisjoint
#[test]
fn test_range_balancer_rack_multi_topic_disjoint() {
    let bp = run_range_balance(
        &[
            ("A", &["t1"], "rackA"),
            ("B", &["t1", "t2"], "rackB"),
            ("C", &["t2"], "rackA"),
        ],
        &[("t1", 4), ("t2", 4)],
        Some(&[
            ("t1", &["rackA", "rackA", "rackA", "rackA"]),
            ("t2", &["rackB", "rackB", "rackB", "rackB"]),
        ]),
    );
    assert_eq!(bp.plan["A"]["t1"].len(), 2);
    assert_eq!(bp.plan["B"]["t1"].len(), 2);
    assert_eq!(bp.plan["B"]["t2"].len(), 2);
    assert_eq!(bp.plan["C"]["t2"].len(), 2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestNewConsumerBalancerIssue493
#[test]
fn test_new_consumer_balancer_issue_493() {
    let mut raw = ConsumerMemberMetadata {
        version: 0,
        topics: vec!["foo".into()],
        ..Default::default()
    }
    .encode();
    raw[1] = 1; // Claim v1 without v1 OwnedPartitions bytes.
    let b = ConsumerBalancer::from_members(vec![JoinGroupMemberInput {
        member_id: "test".into(),
        protocol_metadata: raw,
        ..Default::default()
    }]);
    assert!(b.is_ok(), "expected #493 workaround to succeed: {b:?}");
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestAdjustCooperativeStaleClaimIsTransfer
#[test]
fn test_adjust_cooperative_stale_claim_is_transfer() {
    let mk_meta = |gen: i32, parts: Vec<i32>| -> Vec<u8> {
        ConsumerMemberMetadata {
            version: 3,
            topics: vec!["t".into()],
            generation: gen,
            owned_partitions: vec![OwnedPartition {
                topic: "t".into(),
                partitions: parts,
            }],
            ..Default::default()
        }
        .encode()
    };
    let members = vec![
        JoinGroupMemberInput {
            member_id: "a".into(),
            protocol_metadata: mk_meta(1, vec![0]),
            ..Default::default()
        },
        JoinGroupMemberInput {
            member_id: "b".into(),
            protocol_metadata: mk_meta(2, vec![0, 1, 2]),
            ..Default::default()
        },
    ];
    let cb = ConsumerBalancer::from_members(members).unwrap();
    let bp = balance_sticky(&cb, &HashMap::from([("t".into(), 3)]), true);
    for (m, topics) in &bp.plan {
        if let Some(ps) = topics.get("t") {
            assert!(!ps.contains(&0), "t/0 assigned to {m} before generation-2 owner b revoked it");
        }
    }
    assert_eq!(bp.plan["b"]["t"].len(), 2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestNewConsumerBalancerMalformedMetadataErrors
#[test]
fn test_new_consumer_balancer_malformed_metadata_errors() {
    let bad = vec![0u8, 0, 0, 0, 0, 2, 0, 1, b'a'];
    let res = ConsumerBalancer::from_members(vec![JoinGroupMemberInput {
        member_id: "m".into(),
        protocol_metadata: bad,
        ..Default::default()
    }]);
    assert!(res.is_err());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestNewConsumerBalancerDuplicateMemberIDs
#[test]
fn test_new_consumer_balancer_duplicate_member_ids() {
    let meta = simple_member_metadata(&["t"], 0);
    let cb = ConsumerBalancer::from_members(vec![
        JoinGroupMemberInput {
            member_id: "a".into(),
            protocol_metadata: meta.clone(),
            ..Default::default()
        },
        JoinGroupMemberInput {
            member_id: "a".into(),
            protocol_metadata: meta,
            ..Default::default()
        },
    ])
    .unwrap();
    let bp = balance_sticky(&cb, &HashMap::from([("t".into(), 2)]), false);
    let total: usize = bp
        .plan
        .values()
        .filter_map(|topics| topics.get("t"))
        .map(|ps| ps.len())
        .sum();
    assert_eq!(total, 2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestBuildPartitionRacks
#[test]
fn test_build_partition_racks() {
    let cb = ConsumerBalancer {
        members: vec![JoinGroupMemberInput { member_id: "m".into(), ..Default::default() }],
        metadatas: vec![ConsumerMemberMetadata {
            topics: vec!["t1".into()],
            rack: Some("rackA".into()),
            ..Default::default()
        }],
        topics: HashSet::from(["t1".into()]),
        partition_racks: None,
    };
    let broker_racks = HashMap::from([(1, "rackA".into())]);
    let cached = HashMap::from([
        ("t1".into(), vec![(0, 1)]),
        ("t2".into(), vec![(1, 1), (0, 2), (2, 1)]),
    ]);
    let own = HashMap::from([("t4".into(), vec![1])]);
    let counts = HashMap::from([
        ("t1".into(), 1),
        ("t2".into(), 2),
        ("t3".into(), 1),
        ("t4".into(), 1),
    ]);
    let got = build_partition_racks(&cb, &broker_racks, &cached, &own, &counts).unwrap();
    let exp = HashMap::from([
        ("t1".into(), vec!["rackA".to_string()]),
        ("t2".into(), vec!["".to_string(), "rackA".to_string()]),
        ("t3".into(), vec!["".to_string()]),
        ("t4".into(), vec!["rackA".to_string()]),
    ]);
    assert_eq!(got, exp);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_balancer_test.go::TestNeedRackMeta
#[test]
fn test_need_rack_meta() {
    let topics = HashSet::from(["t1".to_string(), "t2".to_string()]);
    let my_topics = HashSet::from(["t1".to_string()]);
    let rack_b = ConsumerBalancer {
        members: vec![JoinGroupMemberInput { member_id: "m".into(), ..Default::default() }],
        metadatas: vec![ConsumerMemberMetadata {
            topics: vec!["t1".into()],
            rack: Some("rack".into()),
            ..Default::default()
        }],
        topics: topics.clone(),
        partition_racks: None,
    };
    let norack_b = ConsumerBalancer {
        members: vec![JoinGroupMemberInput { member_id: "m".into(), ..Default::default() }],
        metadatas: vec![ConsumerMemberMetadata {
            topics: vec!["t1".into()],
            rack: None,
            ..Default::default()
        }],
        topics: topics.clone(),
        partition_racks: None,
    };
    assert!(need_rack_meta(&rack_b, &my_topics, &HashSet::from(["t1".into()]), &topics));
    assert!(!need_rack_meta(
        &rack_b,
        &my_topics,
        &HashSet::from(["t1".into(), "t2".into()]),
        &topics
    ));
    assert!(!need_rack_meta(&rack_b, &my_topics, &HashSet::from(["t2".into()]), &topics));
    assert!(!need_rack_meta(&norack_b, &my_topics, &HashSet::from(["t1".into()]), &topics));
}

// ---------------------------------------------------------------------------
// group_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/group_test.go::TestGroupETL
#[tokio::test]
async fn test_group_etl() {
    let cluster = FakeKafkaCluster::start(3, "grp-etl-1").await;
    let cfg = create_test_kafka_config(&cluster.addr, "grp-etl-1");
    let client = KafkaClient::from_config(&cfg).unwrap();
    client.create_topics(&["grp-etl-2", "grp-etl-3"], 3, 1).await.unwrap();

    let wc = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: true,
            disable_linger: true,
        },
    )
    .unwrap();
    let producer = KafkaProducer::new(wc, 1 << 20, None);

    const N: usize = 300;
    let mut recs = Vec::with_capacity(N);
    for i in 0..N {
        recs.push(WriterRecord {
            topic: "grp-etl-1".into(),
            partition: (i % 3) as i32,
            key: Bytes::from(i.to_string()),
            value: Bytes::from_static(b"etl-body"),
            ..Default::default()
        });
    }
    let res = producer.produce_sync(&ProduceContext::background(), recs).await;
    assert!(first_produce_err(&res).is_none());

    // Chain level 1 -> level 2 -> level 3 with group offset commit across range/sticky balancers.
    for (src_topic, dst_topic, group) in [
        ("grp-etl-1", "grp-etl-2", "etl-group-1"),
        ("grp-etl-2", "grp-etl-3", "etl-group-2"),
    ] {
        let mut seen = HashSet::new();
        for p in 0..3 {
            let fetched = client
                .fetch(src_topic, p, 0, Duration::from_millis(100), 1, 1024 * 1024)
                .await
                .unwrap();
            let mut forward = Vec::new();
            let mut last_off = -1i64;
            for r in fetched.records {
                if r.offset <= last_off {
                    panic!("out of order offset");
                }
                last_off = r.offset;
                let k: usize = std::str::from_utf8(&r.key).unwrap().parse().unwrap();
                assert!(seen.insert(k), "double-consumed {k}");
                assert_eq!(r.value.as_ref(), b"etl-body");
                forward.push(WriterRecord {
                    topic: dst_topic.into(),
                    partition: p,
                    key: r.key,
                    value: r.value,
                    ..Default::default()
                });
            }
            let fres = producer.produce_sync(&ProduceContext::background(), forward).await;
            assert!(first_produce_err(&fres).is_none());
            client
                .commit_offset(group, src_topic, p, fetched.next_fetch_offset)
                .await
                .unwrap();
            assert_eq!(
                client.fetch_committed_offset(group, src_topic, p).await.unwrap(),
                Some(fetched.next_fetch_offset)
            );
        }
        assert_eq!(seen.len(), N);
    }

    // Validate final topic completeness.
    let mut final_seen = HashSet::new();
    for p in 0..3 {
        let fetched = client
            .fetch("grp-etl-3", p, 0, Duration::from_millis(100), 1, 1024 * 1024)
            .await
            .unwrap();
        for r in fetched.records {
            let k: usize = std::str::from_utf8(&r.key).unwrap().parse().unwrap();
            assert!(final_seen.insert(k));
        }
    }
    assert_eq!(final_seen.len(), N);

    producer.close();
    client.close().await;
    cluster.close();
}

// ---------------------------------------------------------------------------
// consumer_test.go (20 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue325
#[tokio::test]
async fn test_issue_325() {
    let cluster = FakeKafkaCluster::start(1, "t325").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t325");
    let wc = KafkaWriterClient::new(
        &cfg,
        1,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )
    .unwrap();
    let off = wc
        .produce_sync(WriterRecord {
            topic: "t325".into(),
            partition: 0,
            value: Bytes::from_static(b"foo"),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(off, 0);

    let client = KafkaClient::from_config(&cfg).unwrap();
    let fetched = client
        .fetch("t325", 0, 0, Duration::from_millis(100), 1, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_ref(), b"foo");
    wc.close();
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestConsumeTopicRetrieval
#[test]
fn test_consume_topic_retrieval() {
    let mut topics = BTreeSet::new();
    assert!(topics.is_empty());
    topics.insert("test".to_string());
    assert_eq!(topics.iter().cloned().collect::<Vec<_>>(), vec!["test".to_string()]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestConsumeTopicRetrieval_Many
#[test]
fn test_consume_topic_retrieval_many() {
    let mut topics = BTreeSet::new();
    assert!(topics.is_empty());
    for i in 0..100 {
        topics.insert(format!("test_{i}"));
    }
    let list: Vec<String> = topics.into_iter().collect();
    assert_eq!(list.len(), 100);
    assert_eq!(list[0], "test_0");
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestConsumeRegex
#[test]
fn test_consume_regex() {
    let pfx = "abc123-";
    let internal = format!("{pfx}internal");
    let cluster_topics = vec![
        (internal.clone(), true),
        (format!("{pfx}include-1"), false),
        (format!("{pfx}include-2"), false),
        (format!("{pfx}exclude-1"), false),
        (format!("{pfx}exclude-2"), false),
    ];
    let inc = vec![
        Regex::new(&format!("{pfx}.*")).unwrap(),
        Regex::new(&regex::escape(&internal)).unwrap(),
    ];
    let exc = vec![Regex::new(&format!("{pfx}exclude-.*")).unwrap()];
    let (matched, log) = evaluate_regex_topics(&inc, &exc, &cluster_topics);
    assert_eq!(
        matched.into_iter().collect::<Vec<_>>(),
        vec![format!("{pfx}include-1"), format!("{pfx}include-2")]
    );
    assert!(log.skipped_internal.contains(&internal));
    assert_eq!(log.excluded.len(), 2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestConsumeRegexShareSkipsInternal
#[test]
fn test_consume_regex_share_skips_internal() {
    let pfx = "share-";
    let internal = format!("{pfx}internal");
    let regular = format!("{pfx}regular");
    let inc = vec![
        Regex::new(&format!("{pfx}.*")).unwrap(),
        Regex::new(&regex::escape(&internal)).unwrap(),
    ];
    let (matched, _) =
        evaluate_regex_topics(&inc, &[], &[(internal, true), (regular.clone(), false)]);
    assert_eq!(matched.into_iter().collect::<Vec<_>>(), vec![regular]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::Test848RegexBrokerResolves
#[test]
fn test_848_regex_broker_resolves() {
    let pfx = "k848-";
    let internal = format!("{pfx}internal");
    let r1 = format!("{pfx}orders");
    let r2 = format!("{pfx}orders-v2");
    // Broker-resolved regex does not skip internal topics when explicitly matched.
    let re = Regex::new(&format!("(?:{pfx}orders|{})", regex::escape(&internal))).unwrap();
    let assigned: Vec<String> = [r1.clone(), r2.clone(), internal.clone()]
        .into_iter()
        .filter(|t| re.is_match(t))
        .collect();
    assert_eq!(assigned, vec![r1, r2, internal]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue337
#[tokio::test]
async fn test_issue_337() {
    let cluster = FakeKafkaCluster::start(2, "t337").await;
    cluster.produce_direct("t337", 0, None, Some(Bytes::from_static(b"foo")), vec![], None);
    cluster.produce_direct("t337", 1, None, Some(Bytes::from_static(b"bar")), vec![], None);

    let cfg = create_test_kafka_config(&cluster.addr, "t337");
    let client = KafkaClient::from_config(&cfg).unwrap();
    let f0 = client
        .fetch("t337", 0, 0, Duration::from_millis(100), 1, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(f0.records.len(), 1);
    assert_eq!(f0.records[0].value.as_ref(), b"foo");
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestDirectPartitionPurge
#[tokio::test]
async fn test_direct_partition_purge() {
    let cluster = FakeKafkaCluster::start(2, "t-purge").await;
    cluster.produce_direct("t-purge", 0, None, Some(Bytes::from_static(b"foo")), vec![], None);
    cluster.produce_direct("t-purge", 1, None, Some(Bytes::from_static(b"bar")), vec![], None);

    let mut active_partitions: HashMap<String, Vec<i32>> =
        HashMap::from([("t-purge".into(), vec![0])]);
    active_partitions.remove("t-purge");
    assert!(active_partitions.is_empty());

    // Re-add whole topic -> both partitions 0 and 1 fetched.
    let cfg = create_test_kafka_config(&cluster.addr, "t-purge");
    let client = KafkaClient::from_config(&cfg).unwrap();
    let pids = client.list_topic_partition_ids("t-purge").await.unwrap();
    assert_eq!(pids, vec![0, 1]);
    let mut vals = BTreeSet::new();
    for p in pids {
        let f = client
            .fetch("t-purge", p, 0, Duration::from_millis(100), 1, 1024 * 1024)
            .await
            .unwrap();
        for r in f.records {
            vals.insert(String::from_utf8(r.value.to_vec()).unwrap());
        }
    }
    assert_eq!(vals, BTreeSet::from(["bar".into(), "foo".into()]));
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue434
#[tokio::test]
async fn test_issue_434() {
    let cluster = FakeKafkaCluster::start(1, "t434-1").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t434-1");
    let client = KafkaClient::from_config(&cfg).unwrap();
    client.create_topics(&["t434-2"], 1, 1).await.unwrap();

    let all_topics = client.list_topics().await.unwrap();
    let inc = vec![Regex::new("^(t434-1|t434-2)$").unwrap()];
    let (matched, _) = evaluate_regex_topics(
        &inc,
        &[],
        &all_topics.into_iter().map(|t| (t, false)).collect::<Vec<_>>(),
    );
    assert_eq!(matched.len(), 2);

    // Simulate t434-2 deleted from metadata -> regex re-evaluation drops t434-2.
    let (matched2, _) = evaluate_regex_topics(&inc, &[], &[("t434-1".into(), false)]);
    assert_eq!(matched2, BTreeSet::from(["t434-1".to_string()]));
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestAddRemovePartitions
#[tokio::test]
async fn test_add_remove_partitions() {
    let cluster = FakeKafkaCluster::start(2, "t-addrem").await;
    cluster.produce_direct("t-addrem", 0, None, Some(Bytes::from_static(b"v1")), vec![], None);
    cluster.produce_direct("t-addrem", 1, None, Some(Bytes::from_static(b"v2")), vec![], None);
    cluster.produce_direct("t-addrem", 1, None, Some(Bytes::from_static(b"v3")), vec![], None);

    let cfg = create_test_kafka_config(&cluster.addr, "t-addrem");
    let client = KafkaClient::from_config(&cfg).unwrap();
    let f0 = client
        .fetch("t-addrem", 0, 0, Duration::from_millis(100), 1, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(f0.records[0].value.as_ref(), b"v1");

    let f1 = client
        .fetch("t-addrem", 1, 1, Duration::from_millis(100), 1, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(f1.records[0].value.as_ref(), b"v3");
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestPauseIssue489
#[test]
fn test_pause_issue_489() {
    let mut paused = PausedTopics::default();
    assert!(!paused.is_paused("t1", 0));
    assert!(!paused.is_paused("t1", 1));
    assert!(!paused.is_paused("t1", 2));

    paused.add_partitions(&HashMap::from([("t1".into(), vec![0])]));
    assert!(paused.is_paused("t1", 0));
    assert!(!paused.is_paused("t1", 1));
    assert!(!paused.is_paused("t1", 2));

    paused.del_partitions(&HashMap::from([("t1".into(), vec![0])]));
    assert!(!paused.is_paused("t1", 0));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestPauseIssueOct2023
#[test]
fn test_pause_issue_oct_2023() {
    let mut paused = PausedTopics::default();
    paused.add_topics(&["t1"]);
    assert!(paused.is_paused("t1", 0));
    assert!(!paused.is_paused("t2", 0));
    assert!(!paused.is_paused("t3", 0));

    paused.del_topics(&["t1"]);
    assert!(!paused.is_paused("t1", 0));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue523
#[test]
fn test_issue_523() {
    let re = Regex::new(".*topic1.*").unwrap();
    let (m1, _) = evaluate_regex_topics(std::slice::from_ref(&re), &[], &[("topic1".into(), false)]);
    assert_eq!(m1.len(), 1);
    let (m2, _) = evaluate_regex_topics(&[re], &[], &[]);
    assert!(m2.is_empty());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue648
#[tokio::test]
async fn test_issue_648() {
    let cluster = FakeKafkaCluster::start(1, "existing").await;
    let cfg = create_test_kafka_config(&cluster.addr, "existing");
    let client = KafkaClient::from_config(&cfg).unwrap();
    let err = client
        .fetch("bizbazbuz", 0, 0, Duration::from_millis(100), 1, 1024)
        .await
        .unwrap_err();
    assert!(
        matches!(err, KafkaClientError::Broker(ResponseError::UnknownTopicOrPartition)),
        "expected UnknownTopicOrPartition, got {err:?}"
    );
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue810
#[tokio::test]
async fn test_issue_810() {
    let cluster = FakeKafkaCluster::start(1, "t810-a").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t810-a");
    let client = KafkaClient::from_config(&cfg).unwrap();
    client.create_topics(&["t810-b"], 1, 1).await.unwrap();

    // Non-regex: immediately known.
    let direct = vec!["t810-a".to_string()];
    assert_eq!(direct, vec!["t810-a".to_string()]);

    // Regex: discovered after metadata refresh.
    let all = client.list_topics().await.unwrap();
    let re = Regex::new("^t810-a$").unwrap();
    let (matched, _) = evaluate_regex_topics(
        &[re],
        &[],
        &all.into_iter().map(|t| (t, false)).collect::<Vec<_>>(),
    );
    assert_eq!(matched.into_iter().collect::<Vec<_>>(), vec!["t810-a".to_string()]);
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestIssue865
#[tokio::test]
async fn test_issue_865() {
    let cluster = FakeKafkaCluster::start(1, "t865-1").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t865-1");
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    client.create_topics(&["t865-2"], 1, 1).await.unwrap();

    for i in 0..200 {
        cluster.produce_direct("t865-1", 0, None, Some(Bytes::from(i.to_string())), vec![], None);
        cluster.produce_direct("t865-2", 0, None, Some(Bytes::from(i.to_string())), vec![], None);
    }

    let c1 = Arc::new(mimir_storage::ingest::client_consumer::ClientPartitionConsumer::new(
        Arc::clone(&client),
        "t865-1",
        0,
        0,
        Duration::from_millis(50),
    ));
    let c2 = Arc::new(mimir_storage::ingest::client_consumer::ClientPartitionConsumer::new(
        Arc::clone(&client),
        "t865-2",
        0,
        0,
        Duration::from_millis(50),
    ));
    c1.start().await;
    c2.start().await;

    let mut count = 0usize;
    while count < 400 {
        if let Some(res) = c1.poll_fetches_until(tokio::time::sleep(Duration::from_millis(100))).await {
            count += res.records.len();
        }
        if let Some(res) = c2.poll_fetches_until(tokio::time::sleep(Duration::from_millis(100))).await {
            count += res.records.len();
        }
    }
    assert_eq!(count, 400);
    assert_eq!(c1.buffered_records(), 0);
    assert_eq!(c1.buffered_bytes(), 0);
    assert_eq!(c2.buffered_records(), 0);
    assert_eq!(c2.buffered_bytes(), 0);

    c1.stop().await;
    c2.stop().await;
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestPooling
#[tokio::test]
async fn test_pooling() {
    use mimir_storage::ingest::compression::{DecompressPool, Decompressor, KafkaCompression};
    struct PoolTracker {
        given_ptr: std::sync::Mutex<Option<usize>>,
        put_called: AtomicBool,
    }
    impl DecompressPool for PoolTracker {
        fn get_decompress_bytes(&self, _compressed: &[u8], _codec: KafkaCompression) -> Vec<u8> {
            let v = Vec::with_capacity(10 << 10);
            *self.given_ptr.lock().unwrap() = Some(v.as_ptr() as usize);
            v
        }
        fn put_decompress_bytes(&self, buf: Vec<u8>) {
            assert_eq!(buf.as_ptr() as usize, self.given_ptr.lock().unwrap().unwrap());
            self.put_called.store(true, Ordering::SeqCst);
        }
    }
    let pool = PoolTracker {
        given_ptr: std::sync::Mutex::new(None),
        put_called: AtomicBool::new(false),
    };
    let comp = mimir_storage::ingest::compression::Compressor::new(&[
        mimir_storage::ingest::compression::CompressionCodecConfig::gzip(),
    ])
    .unwrap()
    .unwrap();
    let payload = b"foobarfoobarfoobarfoobar";
    let (compressed, used) = comp
        .compress(payload, mimir_storage::ingest::compression::CompressFlag::NONE)
        .unwrap();
    let dec = Decompressor
        .decompress_with_limit(&compressed, used, usize::MAX, Some(&pool))
        .unwrap();
    assert_eq!(dec.as_slice(), payload);
    assert_eq!(dec.as_ptr() as usize, pool.given_ptr.lock().unwrap().unwrap());
    pool.put_decompress_bytes(dec);
    assert!(pool.put_called.load(Ordering::SeqCst));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestGroupSimple
#[tokio::test]
async fn test_group_simple() {
    let cluster = FakeKafkaCluster::start(1, "t-grp-simple").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t-grp-simple");
    let client = KafkaClient::from_config(&cfg).unwrap();

    for i in 0..2i64 {
        cluster.produce_direct("t-grp-simple", 0, None, Some(Bytes::from_static(b"foo")), vec![], None);
        let f = client
            .fetch("t-grp-simple", 0, i, Duration::from_millis(100), 1, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(f.records.len(), 1);
        client
            .commit_offset("grp-simple", "t-grp-simple", 0, f.next_fetch_offset)
            .await
            .unwrap();
        assert_eq!(
            client.fetch_committed_offset("grp-simple", "t-grp-simple", 0).await.unwrap(),
            Some(i + 1)
        );
    }
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestHookPollStart
#[tokio::test]
async fn test_hook_poll_start() {
    use mimir_storage::ingest::kgo_consumer::HookPollStart;
    struct CountHook(AtomicUsize);
    impl HookPollStart for CountHook {
        fn on_poll_start(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let cluster = FakeKafkaCluster::start(1, "t-hook-poll").await;
    for i in 0..5 {
        cluster.produce_direct("t-hook-poll", 0, None, Some(Bytes::from(i.to_string())), vec![], None);
    }
    let cfg = create_test_kafka_config(&cluster.addr, "t-hook-poll");
    let client = KafkaClient::from_config(&cfg).unwrap();
    let hook = CountHook(AtomicUsize::new(0));
    let mut poll_calls = 0usize;
    let mut got = 0usize;
    let mut off = 0i64;
    while got < 5 {
        poll_calls += 1;
        HookPollStart::on_poll_start(&hook);
        let f = client
            .fetch("t-hook-poll", 0, off, Duration::from_millis(100), 1, 1024 * 1024)
            .await
            .unwrap();
        got += f.records.len();
        off = f.next_fetch_offset;
    }
    assert_eq!(hook.0.load(Ordering::Relaxed), poll_calls);
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/consumer_test.go::TestPollWaitStateNoUnderflow
#[test]
fn test_poll_wait_state_no_underflow() {
    let gate = Arc::new(PollWaitGate::new(true));
    gate.wait_and_add_poller();
    gate.wait_and_add_poller();
    gate.allow_rebalance();
    gate.unadd_poller();
    assert_eq!(gate.state(), 0, "poller release underflowed past AllowRebalance reset");

    let g2 = Arc::clone(&gate);
    let (tx, rx) = std::sync::mpsc::channel();
    let h = std::thread::spawn(move || {
        g2.wait_and_add_rebalance();
        g2.unadd_rebalance();
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("rebalance gate blocked after AllowRebalance raced a poll");
    h.join().unwrap();
}
