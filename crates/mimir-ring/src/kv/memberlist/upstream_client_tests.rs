//! Ported from github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go (subset: join,
//! fast-join, rejoin, abort-on-join-failure, cluster label, metrics) and
//! github.com/hashicorp/memberlist/state_test.go (failure detection).
//!
//! These tests exercise the startup/membership behaviour that the ingester relies on in
//! Kubernetes (Helm sets `join_members: [dns+...]`, `rejoin_interval: 60s`; jsonnet sets
//! `-memberlist.abort-if-fast-join-fails=true`, `-memberlist.rejoin-interval`).
//! dskit injects a `DNSProvider` mock; mimir-rs has no such seam, so `dns+` cases resolve
//! `localhost` through the system resolver instead.

use super::*;
use crate::ring::model::ring_desc_codec;
use mimir_config::duration::GoDuration;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use std::future::Future;

const KEY: &str = "ring";

fn test_cfg(name: &str) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: name.to_string(),
        randomize_node_name: false,
        gossip_interval: GoDuration::from_millis(100),
        ..Default::default()
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn eventually<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn add_ring_member(kv: &MemberlistKv, id: &str) {
    let id = id.to_string();
    kv.cas_with_codec(
        KEY,
        ring_desc_codec().as_ref(),
        Box::new(move |cur| {
            let mut desc = cur
                .and_then(|v| v.downcast_ref::<Desc>())
                .cloned()
                .unwrap_or_default();
            desc.ingesters.insert(
                id.clone(),
                InstanceDesc {
                    addr: id.clone(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                    state: InstanceState::Joining as i32,
                    tokens: vec![],
                    ..Default::default()
                },
            );
            Ok(CasOutcome::Update(Box::new(desc)))
        }),
    )
    .await
    .unwrap();
}

fn ring_members(kv: &MemberlistKv) -> usize {
    kv.get_value(KEY)
        .and_then(|(v, _)| v.downcast_ref::<Desc>().map(|d| d.ingesters.len()))
        .unwrap_or(0)
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestKVConfig_Validate
#[test]
fn test_kv_config_validate() {
    type Modify = fn(&mut MemberlistConfig);
    let cases: Vec<(&str, Modify, &str)> = vec![
        ("default config is valid", |_| {}, ""),
        ("empty compression algorithm is valid", |c| c.compression_algorithm = String::new(), ""),
        ("compression algorithm lzw is valid", |c| c.compression_algorithm = "lzw".into(), ""),
        ("compression algorithm snappy is valid", |c| c.compression_algorithm = "snappy".into(), ""),
        (
            "unsupported compression algorithm is rejected",
            |c| c.compression_algorithm = "gzip".into(),
            "memberlist compression algorithm \"gzip\" is not supported",
        ),
        (
            "compression algorithm is ignored when compression is disabled",
            |c| {
                c.compression_enabled = false;
                c.compression_algorithm = "snappy".into();
            },
            "",
        ),
        (
            "zero received messages queue size is rejected",
            |c| c.received_messages_queue_size = 0,
            "memberlist received messages queue size must be greater than 0",
        ),
        (
            "negative received messages queue size is rejected",
            |c| c.received_messages_queue_size = -1,
            "memberlist received messages queue size must be greater than 0",
        ),
        (
            "zero processed messages queue size is rejected",
            |c| c.processed_messages_queue_size = 0,
            "memberlist processed messages queue size must be greater than 0",
        ),
        (
            "negative processed messages queue size is rejected",
            |c| c.processed_messages_queue_size = -1,
            "memberlist processed messages queue size must be greater than 0",
        ),
    ];
    for (name, modify, want) in cases {
        let mut cfg = MemberlistConfig::default();
        modify(&mut cfg);
        let res = cfg.validate();
        if want.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            let err = res.expect_err(name).to_string();
            assert!(err.contains(want), "{name}: {err}");
        }
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMemberlist_AbortIfFastJoinFailsAtStartup
#[tokio::test]
async fn test_memberlist_abort_if_fast_join_fails_at_startup() {
    // "default config with 1 min required node"
    let mut cfg = test_cfg("abort-fast-1");
    cfg.min_join_backoff = GoDuration::from_millis(100);
    cfg.max_join_backoff = GoDuration::from_millis(100);
    cfg.max_join_retries = 2;
    cfg.abort_if_cluster_fast_join_fails = true;
    cfg.join_members.0 = vec![format!("127.0.0.1:{}", free_port())];
    let err = MemberlistKv::start(cfg, vec![ring_desc_codec()], "", None)
        .await
        .err()
        .expect("start must fail when no seed node is reachable");
    assert!(
        err.to_string()
            .contains("fast-join failed to reach minimum required seed nodes: joined 0, required 1"),
        "{err}"
    );

    // "custom config with 2 min required nodes but only 1 reachable"
    let seed = MemberlistKv::start(test_cfg("abort-fast-seed"), vec![ring_desc_codec()], "", None)
        .await
        .unwrap();
    let mut cfg2 = test_cfg("abort-fast-2");
    cfg2.min_join_backoff = GoDuration::from_millis(100);
    cfg2.max_join_backoff = GoDuration::from_millis(100);
    cfg2.max_join_retries = 2;
    cfg2.abort_if_cluster_fast_join_fails = true;
    cfg2.abort_if_cluster_fast_join_fails_min_nodes = 2;
    cfg2.join_members.0 = vec![format!("127.0.0.1:{}", seed.listening_port())];
    let err = MemberlistKv::start(cfg2, vec![ring_desc_codec()], "", None)
        .await
        .err()
        .expect("start must fail when fewer than min nodes are joined");
    assert!(
        err.to_string()
            .contains("fast-join failed to reach minimum required seed nodes: joined 1, required 2"),
        "{err}"
    );
    seed.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMemberlist_AbortIfJoinFailsAtStartup
#[tokio::test]
async fn test_memberlist_abort_if_join_fails_at_startup() {
    // dskit: StartAndAwaitRunning succeeds (the full join runs in running()), then the service
    // fails with errFailedToJoinCluster. Startup itself must not block on the join retries.
    let mut cfg = test_cfg("abort-join");
    cfg.min_join_backoff = GoDuration::from_millis(100);
    cfg.max_join_backoff = GoDuration::from_millis(100);
    cfg.max_join_retries = 2;
    cfg.abort_if_cluster_join_fails = true;
    cfg.join_members.0 = vec![format!("127.0.0.1:{}", free_port())];
    let res = MemberlistKv::start(cfg, vec![ring_desc_codec()], "", None).await;
    assert!(
        res.is_ok(),
        "dskit reaches Running before the full join and fails the service afterwards \
         (errFailedToJoinCluster); mimir-rs fails start() instead: {:?}",
        res.err()
    );
    // mimir-rs has no equivalent of services.FailureCase() for MemberlistKv to assert
    // `failed to join memberlist cluster on startup` afterwards.
    res.unwrap().shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestFastJoin
#[tokio::test]
async fn test_fast_join() {
    let mkv1 = MemberlistKv::start(test_cfg("fast-1"), vec![ring_desc_codec()], "", None)
        .await
        .unwrap();
    add_ring_member(&mkv1, "entry").await;

    let mut cfg = test_cfg("fast-2");
    cfg.join_members.0 = vec![format!("127.0.0.1:{}", mkv1.listening_port())];
    let mkv2 = MemberlistKv::start(cfg, vec![ring_desc_codec()], "", None)
        .await
        .unwrap();
    // Value must be present as soon as start() returned (fast-join runs in Starting).
    assert_eq!(ring_members(&mkv2), 1);
    mkv2.shutdown().await;
    mkv1.shutdown().await;
}

async fn join_members_with_retry_backoff(query_type: &str, start_delay_for_rest: Duration) {
    const MEMBERS: usize = 3;
    let ports: Vec<u16> = (0..MEMBERS).map(|_| free_port()).collect();
    let mut handles = Vec::new();
    for (i, port) in ports.iter().enumerate() {
        let mut cfg = test_cfg(&format!("Member-{i}"));
        cfg.bind_port = *port as i64;
        cfg.advertise_port = *port as i64;
        cfg.gossip_nodes = 3;
        cfg.pull_push_interval = GoDuration::from_secs(5);
        cfg.min_join_backoff = GoDuration::from_millis(100);
        cfg.max_join_backoff = GoDuration::from_mins(1);
        cfg.max_join_retries = 10;
        cfg.abort_if_cluster_join_fails = true;
        if i == 0 {
            cfg.join_members.0 = vec![format!("{query_type}localhost:{}", ports[1])];
        } else {
            tokio::time::sleep(start_delay_for_rest).await;
            if i >= 2 {
                cfg.join_members.0 = vec![format!("127.0.0.1:{}", ports[i - 1])];
            }
        }
        let id = format!("Member-{i}");
        handles.push(tokio::spawn(async move {
            let kv = MemberlistKv::start(cfg, vec![ring_desc_codec()], "", None)
                .await
                .expect("failed to start KV");
            add_ring_member(&kv, &id).await;
            kv
        }));
    }
    let mut kvs = Vec::new();
    for h in handles {
        kvs.push(h.await.unwrap());
    }
    let first = Arc::clone(&kvs[0]);
    let ok = eventually(Duration::from_secs(10), || {
        let first = Arc::clone(&first);
        async move { ring_members(&first) == MEMBERS }
    })
    .await;
    let seen = ring_members(&first);
    for kv in kvs {
        kv.shutdown().await;
    }
    assert!(ok, "expected to see at least {MEMBERS} but saw {seen}");
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestJoinMembersWithRetryBackoff
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_join_members_with_retry_backoff() {
    // "Test late start of members with fast join"
    join_members_with_retry_backoff("", Duration::from_secs(1)).await;
    // "Test late start of members with DNS lookup"
    join_members_with_retry_backoff("dns+", Duration::from_secs(1)).await;
    // "Test late start of DNS service" needs an injectable DNS provider (not portable).
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestRejoin
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rejoin() {
    let p1 = free_port();
    let p2 = free_port();
    let mk = |name: &str, port: u16| {
        let mut c = test_cfg(name);
        c.bind_port = port as i64;
        c.advertise_port = port as i64;
        c.randomize_node_name = true;
        c.abort_if_cluster_join_fails = false;
        c
    };
    let cfg1 = mk("rejoin-1", p1);
    let mut cfg2 = mk("rejoin-2", p2);
    cfg2.join_members.0 = vec![format!("localhost:{p1}")];
    cfg2.rejoin_interval = GoDuration::from_secs(1);

    let mkv1 = MemberlistKv::start(cfg1.clone(), vec![ring_desc_codec()], "", None)
        .await
        .unwrap();
    let mkv2 = MemberlistKv::start(cfg2, vec![ring_desc_codec()], "", None)
        .await
        .unwrap();

    let m2 = Arc::clone(&mkv2);
    let members = move || m2.members().len();
    let m = members.clone();
    assert!(
        eventually(Duration::from_secs(10), || {
            let n = m();
            async move { n == 2 }
        })
        .await,
        "expected 2 members in the cluster"
    );

    mkv1.shutdown().await;
    drop(mkv1);
    let m = members.clone();
    assert!(
        eventually(Duration::from_secs(10), || {
            let n = m();
            async move { n == 1 }
        })
        .await,
        "expected 1 member in the cluster"
    );

    // Restart the first KV. It does not join anyone; KV2 must find it via periodic rejoin.
    let mut restarted = None;
    for _ in 0..50 {
        match MemberlistKv::start(cfg1.clone(), vec![ring_desc_codec()], "", None).await {
            Ok(kv) => {
                restarted = Some(kv);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let restarted = restarted.expect("restart KV1 on the same port");
    let m = members.clone();
    let ok = eventually(Duration::from_secs(10), || {
        let n = m();
        async move { n == 2 }
    })
    .await;
    restarted.shutdown().await;
    mkv2.shutdown().await;
    // Note: passes on mimir-rs even with rejoin_interval=0 (KV2 learns about the restarted
    // node without a rejoin), so it does not detect the missing rejoin loop; see
    // test_rejoin_with_different_seed_nodes.
    assert!(ok, "expected 2 members in the cluster after rejoin (rejoin_interval=1s)");
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestRejoinWithDifferentSeedNodes
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rejoin_with_different_seed_nodes() {
    let (p1, p2, p3) = (free_port(), free_port(), free_port());
    let mk = |name: &str, port: u16| {
        let mut c = test_cfg(name);
        c.bind_port = port as i64;
        c.advertise_port = port as i64;
        c.randomize_node_name = true;
        c
    };
    let cfg1 = mk("seed-1", p1);
    let mut cfg2 = mk("seed-2", p2);
    cfg2.join_members.0 = vec![format!("localhost:{p1}")];
    cfg2.rejoin_seed_nodes.0 = vec![format!("localhost:{p3}")];
    cfg2.rejoin_interval = GoDuration::from_secs(1);
    let cfg3 = mk("seed-3", p3);

    let mkv1 = MemberlistKv::start(cfg1, vec![ring_desc_codec()], "", None).await.unwrap();
    let mkv2 = MemberlistKv::start(cfg2, vec![ring_desc_codec()], "", None).await.unwrap();
    let k2 = Arc::clone(&mkv2);
    assert!(
        eventually(Duration::from_secs(10), || {
            let n = k2.members().len();
            async move { n == 2 }
        })
        .await
    );
    let mkv3 = MemberlistKv::start(cfg3, vec![ring_desc_codec()], "", None).await.unwrap();
    let mut ok = true;
    for kv in [&mkv1, &mkv2, &mkv3] {
        let k = Arc::clone(kv);
        ok &= eventually(Duration::from_secs(10), || {
            let n = k.members().len();
            async move { n == 3 }
        })
        .await;
    }
    for kv in [mkv1, mkv2, mkv3] {
        kv.shutdown().await;
    }
    assert!(ok, "all three nodes must see 3 members via rejoin_seed_nodes");
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMultipleClientsWithMixedLabelsAndExpectFailure
#[tokio::test]
async fn test_multiple_clients_with_mixed_labels_and_expect_failure() {
    // Simplified to two members: a node with a different cluster label must not be joinable.
    let mut cfg1 = test_cfg("label-1");
    cfg1.cluster_label = String::new();
    let mkv1 = MemberlistKv::start(cfg1, vec![ring_desc_codec()], "", None).await.unwrap();
    let mut cfg2 = test_cfg("label-2");
    cfg2.cluster_label = "label1".to_string();
    cfg2.join_members.0 = vec![format!("127.0.0.1:{}", mkv1.listening_port())];
    cfg2.max_join_retries = 1;
    let mkv2 = MemberlistKv::start(cfg2, vec![ring_desc_codec()], "", None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(mkv1.members().len(), 1, "expected to see at least 2 members, got 1");
    assert_eq!(mkv2.members().len(), 1, "expected to see at least 2 members, got 1");
    mkv2.shutdown().await;
    mkv1.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMetricsRegistration
#[tokio::test]
async fn test_metrics_registration() {
    let reg = Registry::new();
    let kv = MemberlistKv::start(test_cfg("metrics"), vec![ring_desc_codec()], "", Some(&reg))
        .await
        .unwrap();
    add_ring_member(&kv, "member").await;
    let found = reg
        .gather()
        .into_iter()
        .find(|mf| mf.get_name() == "memberlist_client_kv_store_count")
        .map(|mf| mf.get_metric()[0].get_gauge().get_value());
    kv.shutdown().await;
    assert_eq!(found, Some(1.0), "memberlist_client_kv_store_count must be 1");
}

/// dskit `metrics.go` metric names exposed by every Go Mimir process (namespace "" in Mimir).
// Not an upstream test: checks Go metric names from dskit kv/memberlist/metrics.go.
#[tokio::test]
async fn test_memberlist_client_metric_names_match_dskit() {
    let reg = Registry::new();
    // mimir-cmd passes "cortex" as the namespace; Mimir's KVConfig.MetricsNamespace is "".
    let kv = MemberlistKv::start(test_cfg("names"), vec![ring_desc_codec()], "cortex", Some(&reg))
        .await
        .unwrap();
    add_ring_member(&kv, "member").await;
    let names: HashSet<String> = reg.gather().iter().map(|mf| mf.get_name().to_string()).collect();
    kv.shutdown().await;
    let want = [
        "memberlist_client_cluster_members_count",
        "memberlist_client_kv_store_count",
        "memberlist_client_cas_attempt_total",
        "memberlist_client_cas_success_total",
        "memberlist_client_cas_failure_total",
        "memberlist_client_received_broadcasts_total",
        "memberlist_client_messages_in_broadcast_queue",
        "memberlist_client_state_pulls_total",
        "memberlist_client_state_pushes_total",
        "memberlist_client_kv_store_value_tombstones",
    ];
    let missing: Vec<&str> = want.iter().copied().filter(|n| !names.contains(*n)).collect();
    assert!(missing.is_empty(), "missing dskit metrics {missing:?}; registered: {names:?}");
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Suspect
#[tokio::test]
async fn test_member_list_probe_node_suspect() {
    let m1 = MemberlistKv::start(test_cfg("probe-1"), vec![ring_desc_codec()], "", None)
        .await
        .unwrap();
    // A peer that was alive and then crashed (kill -9 / OOM / node loss): nothing listens.
    let dead_port = free_port();
    m1.handle_alive(&Alive {
        incarnation: 1,
        node: "crashed".to_string(),
        addr: vec![127, 0, 0, 1],
        port: dead_port,
        meta: None,
        vsn: vec![
            PROTOCOL_VERSION_MIN,
            PROTOCOL_VERSION_MAX,
            PROTOCOL_VERSION_2_COMPATIBLE,
            0,
            0,
            0,
        ],
    });
    m1.probe().await;
    let state = m1
        .nodes
        .read()
        .iter()
        .find(|n| n.name == "crashed")
        .map(|n| n.state);
    m1.shutdown().await;
    assert_eq!(
        state,
        Some(NodeStateType::Suspect),
        "a node that fails its probe must be marked suspect"
    );
}
