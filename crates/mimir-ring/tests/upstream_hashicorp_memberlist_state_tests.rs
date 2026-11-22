//! Ported upstream hashicorp/memberlist tests: state_test.go (44 tests).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mimir_config::server::MemberlistConfig;
use mimir_config::GoDuration;
use mimir_ring::kv::memberlist::client::EventDelegate;
use mimir_ring::kv::memberlist::security::{failed_remote, NetOpError};
use mimir_ring::kv::memberlist::wire::{
    decode_ping, encode_ack_resp, AckResp, Alive, Dead, MessageType, NodeStateType, PushNodeState,
    Suspect, PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MIN,
};
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv, NodeState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

fn host_cfg(addr: &str, name: &str) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec![addr.to_string()],
        bind_port: 0,
        node_name: name.to_string(),
        randomize_node_name: false,
        gossip_to_dead_nodes_time: GoDuration::from_secs(30),
        ..Default::default()
    }
}

async fn host_memberlist(addr: &str, name: &str, mut cfg: MemberlistConfig, opts: KvOptions) -> Arc<MemberlistKv> {
    cfg.bind_addr = vec![addr.to_string()];
    cfg.node_name = name.to_string();
    cfg.randomize_node_name = false;
    MemberlistKv::start_with_options(
        cfg,
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            skip_set_alive: true,
            skip_schedule: true,
            ..opts
        },
    )
    .await
    .unwrap()
}

fn ip4(a: u8, b: u8, c: u8, d: u8) -> Vec<u8> {
    vec![a, b, c, d]
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum EvKind {
    Join,
    Leave,
    Update,
}

struct ChanEvents {
    tx: mpsc::UnboundedSender<(EvKind, NodeState)>,
    enabled: AtomicBool,
}

impl ChanEvents {
    fn new(enabled: bool) -> (Arc<Self>, mpsc::UnboundedReceiver<(EvKind, NodeState)>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                tx,
                enabled: AtomicBool::new(enabled),
            }),
            rx,
        )
    }
    fn toggle(&self, on: bool) {
        self.enabled.store(on, Ordering::Release);
    }
}

impl EventDelegate for ChanEvents {
    fn notify_join(&self, node: &NodeState) {
        if self.enabled.load(Ordering::Acquire) {
            let _ = self.tx.send((EvKind::Join, node.clone()));
        }
    }
    fn notify_leave(&self, node: &NodeState) {
        if self.enabled.load(Ordering::Acquire) {
            let _ = self.tx.send((EvKind::Leave, node.clone()));
        }
    }
    fn notify_update(&self, node: &NodeState) {
        if self.enabled.load(Ordering::Acquire) {
            let _ = self.tx.send((EvKind::Update, node.clone()));
        }
    }
}

async fn spawn_tcp_fallback_only_server(addr: &str) -> (u16, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let ln = TcpListener::bind(format!("{addr}:0")).await.unwrap();
    let port = ln.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let h = tokio::spawn(async move {
        loop {
            if stop2.load(Ordering::Acquire) {
                break;
            }
            let Ok(Ok((mut sock, _))) = tokio::time::timeout(Duration::from_millis(50), ln.accept()).await else {
                continue;
            };
            tokio::spawn(async move {
                let mut kind = [0u8; 1];
                if sock.read_exact(&mut kind).await.is_err() {
                    return;
                }
                if kind[0] == 1 {
                    // UDP/packet ping: simulate UDP isolation by dropping silently
                    return;
                }
                if kind[0] == 2 {
                    let mut buf = [0u8; 256];
                    if let Ok(n) = sock.read(&mut buf).await {
                        if n > 1 && buf[0] == MessageType::Ping as u8 {
                            if let Ok(p) = decode_ping(&buf[1..n]) {
                                let ack = encode_ack_resp(&AckResp { seq_no: p.seq_no, payload: None });
                                let _ = sock.write_all(&ack).await;
                                let _ = sock.flush().await;
                            }
                        }
                    }
                }
            });
        }
    });
    (port, stop, h)
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_Probe
#[tokio::test]
async fn test_member_list_probe() {
    let m1 = host_memberlist(
        "127.0.12.1",
        "127.0.12.1",
        host_cfg("127.0.12.1", "127.0.12.1"),
        KvOptions {
            probe_interval: Some(Duration::from_millis(100)),
            probe_timeout: Some(Duration::from_millis(25)),
            ..Default::default()
        },
    )
    .await;
    let m2 = host_memberlist("127.0.12.2", "127.0.12.2", host_cfg("127.0.12.2", "127.0.12.2"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.12.1".into(), addr: ip4(127, 0, 12, 1), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.12.2".into(), addr: ip4(127, 0, 12, 2), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.probe_for_test().await;
    assert_eq!(m1.get_node_state("127.0.12.2").unwrap().state, NodeStateType::Alive);
    assert_eq!(m1.sequence_num_for_test(), 1);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Suspect_Dogpile
#[tokio::test]
async fn test_member_list_probe_node_suspect_dogpile() {
    let cases = [
        (1usize, 0usize, 500u64),
        (2, 0, 500),
        (3, 0, 500),
        (4, 0, 1000),
        (5, 0, 1000),
        (5, 1, 750),
        (5, 2, 604),
        (5, 3, 500),
        (5, 4, 500),
    ];
    for (i, (num_peers, confirmations, expected_ms)) in cases.into_iter().enumerate() {
        let addr = format!("127.0.13.{}", 10 + i * 10);
        let m = host_memberlist(
            &addr,
            &addr,
            host_cfg(&addr, &addr),
            KvOptions {
                probe_interval: Some(Duration::from_millis(100)),
                probe_timeout: Some(Duration::from_millis(1)),
                suspicion_mult: Some(5),
                suspicion_max_timeout_mult: Some(2),
                ..Default::default()
            },
        )
        .await;
        let p = m.listening_port();
        let ip: Vec<u8> = addr.split('.').map(|s| s.parse().unwrap()).collect();
        m.alive_node_for_test(&Alive { incarnation: 1, node: addr.clone(), addr: ip, port: p, meta: None, vsn: m.build_vsn_array() }, true);

        let mut peers = Vec::new();
        for j in 0..num_peers.saturating_sub(1) {
            let pa = format!("127.0.13.{}", 10 + i * 10 + j + 1);
            let peer = host_memberlist(&pa, &pa, host_cfg(&pa, &pa), KvOptions::default()).await;
            let pip: Vec<u8> = pa.split('.').map(|s| s.parse().unwrap()).collect();
            m.alive_node_for_test(&Alive { incarnation: 1, node: pa, addr: pip, port: peer.listening_port(), meta: None, vsn: m.build_vsn_array() }, false);
            peers.push(peer);
        }
        let bad = format!("127.0.13.{}", 10 + i * 10 + 9);
        let bip: Vec<u8> = bad.split('.').map(|s| s.parse().unwrap()).collect();
        m.alive_node_for_test(&Alive { incarnation: 1, node: bad.clone(), addr: bip, port: p, meta: None, vsn: m.build_vsn_array() }, false);
        m.probe_node_by_name_for_test(&bad).await;
        assert_eq!(m.get_node_state(&bad).unwrap().state, NodeStateType::Suspect, "case {i}");
        for j in 0..confirmations {
            m.suspect_node_for_test(&Suspect { incarnation: 1, node: bad.clone(), from: format!("peer{j}") });
        }
        let fudge = Duration::from_millis(25);
        tokio::time::sleep(Duration::from_millis(expected_ms) - fudge).await;
        assert_eq!(m.get_node_state(&bad).unwrap().state, NodeStateType::Suspect, "case {i} early");
        tokio::time::sleep(fudge * 2).await;
        assert_eq!(m.get_node_state(&bad).unwrap().state, NodeStateType::Dead, "case {i} dead");
        for peer in peers {
            peer.shutdown().await;
        }
        m.shutdown().await;
    }
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_FallbackTCP
#[tokio::test]
async fn test_member_list_probe_node_fallback_tcp() {
    let m1 = host_memberlist("127.0.12.11", "m1", host_cfg("127.0.12.11", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), disable_tcp_pings: false, ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.12", "m2", host_cfg("127.0.12.12", "m2"), KvOptions::default()).await;
    let m3 = host_memberlist("127.0.12.13", "m3", host_cfg("127.0.12.13", "m3"), KvOptions::default()).await;
    let (p4, stop4, h4) = spawn_tcp_fallback_only_server("127.0.12.14").await;

    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 11), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 12), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m3".into(), addr: ip4(127, 0, 12, 13), port: m3.listening_port(), meta: None, vsn: m3.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 14), port: p4, meta: None, vsn: m1.build_vsn_array() }, false);

    m1.probe_node_by_name_for_test("m4").await;
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Alive);

    stop4.store(true, Ordering::Release);
    let _ = h4.await;
    m1.probe_node_by_name_for_test("m4").await;
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Suspect);

    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_FallbackTCP_Disabled
#[tokio::test]
async fn test_member_list_probe_node_fallback_tcp_disabled() {
    let m1 = host_memberlist("127.0.12.15", "m1", host_cfg("127.0.12.15", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), disable_tcp_pings: true, ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.16", "m2", host_cfg("127.0.12.16", "m2"), KvOptions::default()).await;
    let m3 = host_memberlist("127.0.12.17", "m3", host_cfg("127.0.12.17", "m3"), KvOptions::default()).await;
    let (p4, stop4, h4) = spawn_tcp_fallback_only_server("127.0.12.18").await;

    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 15), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 16), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m3".into(), addr: ip4(127, 0, 12, 17), port: m3.listening_port(), meta: None, vsn: m3.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 18), port: p4, meta: None, vsn: m1.build_vsn_array() }, false);

    m1.probe_node_by_name_for_test("m4").await;
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Suspect);

    stop4.store(true, Ordering::Release);
    let _ = h4.await;
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_FallbackTCP_OldProtocol
#[tokio::test]
async fn test_member_list_probe_node_fallback_tcp_old_protocol() {
    let m1 = host_memberlist("127.0.12.19", "m1", host_cfg("127.0.12.19", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), disable_tcp_pings: false, ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.20", "m2", host_cfg("127.0.12.20", "m2"), KvOptions::default()).await;
    let m3 = host_memberlist("127.0.12.21", "m3", host_cfg("127.0.12.21", "m3"), KvOptions::default()).await;
    let (p4, stop4, h4) = spawn_tcp_fallback_only_server("127.0.12.22").await;

    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 19), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 20), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m3".into(), addr: ip4(127, 0, 12, 21), port: m3.listening_port(), meta: None, vsn: m3.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 22), port: p4, meta: None, vsn: vec![PROTOCOL_VERSION_MIN, PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_2_COMPATIBLE, 0, 0, 0] }, false);

    m1.probe_node_by_name_for_test("m4").await;
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Suspect);

    stop4.store(true, Ordering::Release);
    let _ = h4.await;
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Awareness_Degraded
#[tokio::test]
async fn test_member_list_probe_node_awareness_degraded() {
    let m1 = host_memberlist("127.0.12.23", "m1", host_cfg("127.0.12.23", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.24", "m2", host_cfg("127.0.12.24", "m2"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;
    let m3 = host_memberlist("127.0.12.25", "m3", host_cfg("127.0.12.25", "m3"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;

    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 23), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 24), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m3".into(), addr: ip4(127, 0, 12, 25), port: m3.listening_port(), meta: None, vsn: m3.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 26), port: 7946, meta: None, vsn: vec![1, 5, 1, 1, 1, 1] }, false);

    m1.awareness_apply_delta(1);
    assert_eq!(m1.health_score_pub(), 1);
    let started = Instant::now();
    m1.probe_node_by_name_for_test("m4").await;
    let elapsed = started.elapsed();
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Suspect);
    assert!(elapsed >= Duration::from_millis(350), "elapsed {elapsed:?}");
    assert!(m2.sequence_num_for_test() == 1 || m3.sequence_num_for_test() == 1);
    assert_eq!(m1.health_score_pub(), 1);

    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Wrong_VSN
#[tokio::test]
async fn test_member_list_probe_node_wrong_vsn() {
    let m1 = host_memberlist("127.0.12.27", "m1", host_cfg("127.0.12.27", "m1"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 28), port: 7946, meta: None, vsn: vec![0; 6] }, false);
    assert!(m1.get_node_state("m4").is_none());
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Awareness_Improved
#[tokio::test]
async fn test_member_list_probe_node_awareness_improved() {
    let m1 = host_memberlist("127.0.12.29", "m1", host_cfg("127.0.12.29", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.30", "m2", host_cfg("127.0.12.30", "m2"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 29), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 30), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.awareness_apply_delta(1);
    assert_eq!(m1.health_score_pub(), 1);
    m1.probe_node_by_name_for_test("m2").await;
    assert_eq!(m1.get_node_state("m2").unwrap().state, NodeStateType::Alive);
    assert_eq!(m1.health_score_pub(), 0);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Awareness_MissedNack
#[tokio::test]
async fn test_member_list_probe_node_awareness_missed_nack() {
    let m1 = host_memberlist("127.0.12.31", "m1", host_cfg("127.0.12.31", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.32", "m2", host_cfg("127.0.12.32", "m2"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;
    let vsn = m1.build_vsn_array();
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 31), port: m1.listening_port(), meta: None, vsn: vsn.clone() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 32), port: m2.listening_port(), meta: None, vsn: vsn.clone() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m3".into(), addr: ip4(127, 0, 12, 33), port: 7946, meta: None, vsn: vsn.clone() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 34), port: 7946, meta: None, vsn }, false);
    assert_eq!(m1.health_score_pub(), 0);
    let started = Instant::now();
    m1.probe_node_by_name_for_test("m4").await;
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Suspect);
    assert!(started.elapsed() <= Duration::from_millis(350));
    assert_eq!(m1.health_score_pub(), 1);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Awareness_OldProtocol
#[tokio::test]
async fn test_member_list_probe_node_awareness_old_protocol() {
    let m1 = host_memberlist("127.0.12.35", "m1", host_cfg("127.0.12.35", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(200)), probe_timeout: Some(Duration::from_millis(10)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.36", "m2", host_cfg("127.0.12.36", "m2"), KvOptions::default()).await;
    let m3 = host_memberlist("127.0.12.37", "m3", host_cfg("127.0.12.37", "m3"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 35), port: m1.listening_port(), meta: None, vsn: vec![] }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 36), port: m2.listening_port(), meta: None, vsn: vec![] }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m3".into(), addr: ip4(127, 0, 12, 37), port: m3.listening_port(), meta: None, vsn: vec![] }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m4".into(), addr: ip4(127, 0, 12, 38), port: 7946, meta: None, vsn: vec![] }, false);
    m1.probe_node_by_name_for_test("m4").await;
    assert_eq!(m1.get_node_state("m4").unwrap().state, NodeStateType::Suspect);
    assert_eq!(m1.health_score_pub(), 1);
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode_Buddy
#[tokio::test]
async fn test_member_list_probe_node_buddy() {
    let m1 = host_memberlist("127.0.12.39", "m1", host_cfg("127.0.12.39", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(100)), probe_timeout: Some(Duration::from_millis(25)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.40", "m2", host_cfg("127.0.12.40", "m2"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 39), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 40), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m2.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 40), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, true);
    m1.change_node_state_for_test("m2", |s| s.state = NodeStateType::Suspect);
    m1.probe_node_by_name_for_test("m2").await;
    assert_eq!(m1.sequence_num_for_test(), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(m2.ml_broadcasts_num_queued(), 1);
    assert_eq!(m2.ml_broadcasts_ordered()[0].1[0], MessageType::Alive as u8);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ProbeNode
#[tokio::test]
async fn test_member_list_probe_node() {
    let m1 = host_memberlist("127.0.12.41", "m1", host_cfg("127.0.12.41", "m1"), KvOptions { probe_interval: Some(Duration::from_millis(100)), probe_timeout: Some(Duration::from_millis(25)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.42", "m2", host_cfg("127.0.12.42", "m2"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 41), port: m1.listening_port(), meta: None, vsn: vec![] }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 42), port: m2.listening_port(), meta: None, vsn: vec![] }, false);
    m1.probe_node_by_name_for_test("m2").await;
    assert_eq!(m1.get_node_state("m2").unwrap().state, NodeStateType::Alive);
    assert_eq!(m1.sequence_num_for_test(), 1);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_Ping
#[tokio::test]
async fn test_member_list_ping() {
    let m1 = host_memberlist("127.0.12.43", "m1", host_cfg("127.0.12.43", "m1"), KvOptions { probe_interval: Some(Duration::from_secs(10)), probe_timeout: Some(Duration::from_millis(150)), ..Default::default() }).await;
    let m2 = host_memberlist("127.0.12.44", "m2", host_cfg("127.0.12.44", "m2"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m1".into(), addr: ip4(127, 0, 12, 43), port: m1.listening_port(), meta: None, vsn: vec![] }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "m2".into(), addr: ip4(127, 0, 12, 44), port: m2.listening_port(), meta: None, vsn: vec![] }, false);
    let addr2 = format!("127.0.12.44:{}", m2.listening_port());
    let rtt = m1.ping("m2", &addr2).await.unwrap();
    assert!(rtt > Duration::ZERO);
    let err = m1.ping("bad", &addr2).await.unwrap_err();
    assert!(err.contains("No response from node bad"), "{err}");
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_ResetNodes
#[tokio::test]
async fn test_member_list_reset_nodes() {
    let mut cfg = host_cfg("127.0.12.45", "m1");
    cfg.gossip_to_dead_nodes_time = GoDuration::from_millis(100);
    let m = host_memberlist("127.0.12.45", "m1", cfg, KvOptions::default()).await;
    for (i, n) in ["test1", "test2", "test3"].iter().enumerate() {
        m.alive_node_for_test(&Alive { incarnation: 1, node: (*n).into(), addr: ip4(127, 0, 0, (i + 1) as u8), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    }
    m.dead_node_for_test(&Dead { incarnation: 1, node: "test2".into(), from: String::new() });
    m.reset_nodes_for_test();
    assert_eq!(m.all_nodes().len(), 3);
    assert!(m.get_node_state("test2").is_some());
    tokio::time::sleep(Duration::from_millis(150)).await;
    m.reset_nodes_for_test();
    assert_eq!(m.all_nodes().len(), 2);
    assert!(m.get_node_state("test2").is_none());
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_NextSeq
#[test]
fn test_member_list_next_seq() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "n"), vec![], "", None, KvOptions::default()).unwrap();
    assert_eq!(m.next_seq_no_for_test(), 1);
    assert_eq!(m.next_seq_no_for_test(), 2);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_setProbeChannels
#[tokio::test]
async fn test_member_list_set_probe_channels() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "n"), vec![], "", None, KvOptions::default()).unwrap();
    let (tx, _rx) = mpsc::channel(1);
    m.set_probe_channels_for_test(0, tx, None, Duration::from_millis(10));
    assert!(m.has_ack_handler_for_test(0));
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!m.has_ack_handler_for_test(0));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_setAckHandler
#[tokio::test]
async fn test_member_list_set_ack_handler() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "n"), vec![], "", None, KvOptions::default()).unwrap();
    m.set_ack_handler_for_test(0, Box::new(|_, _| {}), Duration::from_millis(10));
    assert!(m.has_ack_handler_for_test(0));
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!m.has_ack_handler_for_test(0));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_invokeAckHandler
#[tokio::test]
async fn test_member_list_invoke_ack_handler() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "n"), vec![], "", None, KvOptions::default()).unwrap();
    m.invoke_ack_handler_for_test(&AckResp { seq_no: 0, payload: None });
    let b = Arc::new(AtomicBool::new(false));
    let b2 = Arc::clone(&b);
    m.set_ack_handler_for_test(0, Box::new(move |_, _| b2.store(true, Ordering::Release)), Duration::from_millis(10));
    m.invoke_ack_handler_for_test(&AckResp { seq_no: 0, payload: None });
    assert!(b.load(Ordering::Acquire));
    assert!(!m.has_ack_handler_for_test(0));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_invokeAckHandler_Channel_Ack
#[tokio::test]
async fn test_member_list_invoke_ack_handler_channel_ack() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "n"), vec![], "", None, KvOptions::default()).unwrap();
    let ack = AckResp { seq_no: 0, payload: Some(vec![0, 0, 0]) };
    m.invoke_ack_handler_for_test(&ack);
    let (ack_tx, mut ack_rx) = mpsc::channel(1);
    let (nack_tx, mut nack_rx) = mpsc::channel(1);
    m.set_probe_channels_for_test(0, ack_tx, Some(nack_tx), Duration::from_millis(20));
    m.invoke_ack_handler_for_test(&ack);
    let (complete, payload) = tokio::time::timeout(Duration::from_millis(50), ack_rx.recv()).await.unwrap().unwrap();
    assert!(complete);
    assert_eq!(payload, vec![0, 0, 0]);
    assert!(nack_rx.try_recv().is_err());
    assert!(!m.has_ack_handler_for_test(0));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_invokeAckHandler_Channel_Nack
#[tokio::test]
async fn test_member_list_invoke_ack_handler_channel_nack() {
    use mimir_ring::kv::memberlist::wire::NackResp;
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "n"), vec![], "", None, KvOptions::default()).unwrap();
    m.invoke_nack_handler_for_test(&NackResp { seq_no: 0 });
    let (ack_tx, mut ack_rx) = mpsc::channel(1);
    let (nack_tx, mut nack_rx) = mpsc::channel(1);
    m.set_probe_channels_for_test(0, ack_tx, Some(nack_tx), Duration::from_millis(50));
    m.invoke_nack_handler_for_test(&NackResp { seq_no: 0 });
    assert!(nack_rx.try_recv().is_ok());
    assert!(ack_rx.try_recv().is_err());
    assert!(m.has_ack_handler_for_test(0));
    let ack = AckResp { seq_no: 0, payload: Some(vec![0, 0, 0]) };
    m.invoke_ack_handler_for_test(&ack);
    let (complete, payload) = tokio::time::timeout(Duration::from_millis(50), ack_rx.recv()).await.unwrap().unwrap();
    assert!(complete);
    assert_eq!(payload, vec![0, 0, 0]);
    assert!(!m.has_ack_handler_for_test(0));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_AliveNode_NewNode
#[test]
fn test_member_list_alive_node_new_node() {
    let (ev, mut rx) = ChanEvents::new(true);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { event_delegate: Some(ev), ..Default::default() }).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    assert_eq!(m.all_nodes().len(), 1);
    let st = m.get_node_state("test").unwrap();
    assert_eq!(st.incarnation, 1);
    assert_eq!(st.state, NodeStateType::Alive);
    let (kind, n) = rx.try_recv().unwrap();
    assert_eq!(kind, EvKind::Join);
    assert_eq!(n.name, "test");
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_AliveNode_SuspectNode
#[test]
fn test_member_list_alive_node_suspect_node() {
    let (ev, mut rx) = ChanEvents::new(false);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { event_delegate: Some(ev.clone()), ..Default::default() }).unwrap();
    let mut a = Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() };
    m.alive_node_for_test(&a, false);
    ev.toggle(true);
    m.change_node_state_for_test("test", |s| {
        s.state = NodeStateType::Suspect;
        s.state_change = SystemTime::now() - Duration::from_secs(3600);
    });
    m.alive_node_for_test(&a, false);
    assert_eq!(m.get_node_state("test").unwrap().state, NodeStateType::Suspect);
    a.incarnation = 2;
    m.alive_node_for_test(&a, false);
    assert_eq!(m.get_node_state("test").unwrap().state, NodeStateType::Alive);
    assert!(rx.try_recv().is_err());
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_AliveNode_Idempotent
#[test]
fn test_member_list_alive_node_idempotent() {
    let (ev, mut rx) = ChanEvents::new(false);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { event_delegate: Some(ev.clone()), ..Default::default() }).unwrap();
    let mut a = Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() };
    m.alive_node_for_test(&a, false);
    ev.toggle(true);
    let t0 = m.get_node_state("test").unwrap().state_change;
    a.incarnation = 2;
    m.alive_node_for_test(&a, false);
    let st = m.get_node_state("test").unwrap();
    assert_eq!(st.state, NodeStateType::Alive);
    assert_eq!(st.state_change, t0);
    assert!(rx.try_recv().is_err());
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_AliveNode_ChangeMeta
#[test]
fn test_member_list_alive_node_change_meta() {
    let (ev, mut rx) = ChanEvents::new(false);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { event_delegate: Some(ev.clone()), ..Default::default() }).unwrap();
    let mut a = Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: Some(b"val1".to_vec()), vsn: m.build_vsn_array() };
    m.alive_node_for_test(&a, false);
    ev.toggle(true);
    a.incarnation = 2;
    a.meta = Some(b"val2".to_vec());
    m.alive_node_for_test(&a, false);
    let st = m.get_node_state("test").unwrap();
    assert_eq!(st.meta.as_deref(), Some(b"val2".as_slice()));
    let (kind, n) = rx.try_recv().unwrap();
    assert_eq!(kind, EvKind::Update);
    assert_eq!(n.meta.as_deref(), Some(b"val2".as_slice()));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_AliveNode_Refute
#[test]
fn test_member_list_alive_node_refute() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "m".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, true);
    m.ml_broadcasts_reset();
    m.alive_node_for_test(&Alive { incarnation: 2, node: "m".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: Some(b"foo".to_vec()), vsn: m.build_vsn_array() }, false);
    let st = m.get_node_state("m").unwrap();
    assert_eq!(st.state, NodeStateType::Alive);
    assert!(st.meta.is_none());
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Alive as u8);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_AliveNode_Conflict
#[test]
fn test_member_list_alive_node_conflict() {
    let mut cfg = host_cfg("127.0.0.1", "m");
    cfg.dead_node_reclaim_time = GoDuration::from_millis(10);
    let m = MemberlistKv::new(cfg, vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 8000, meta: None, vsn: m.build_vsn_array() }, true);
    m.ml_broadcasts_reset();
    m.alive_node_for_test(&Alive { incarnation: 2, node: "test".into(), addr: ip4(127, 0, 0, 2), port: 9000, meta: Some(b"foo".to_vec()), vsn: m.build_vsn_array() }, false);
    let st = m.get_node_state("test").unwrap();
    assert_eq!(st.addr, ip4(127, 0, 0, 1));
    assert_eq!(st.port, 8000);
    assert_eq!(m.ml_broadcasts_num_queued(), 0);
    m.dead_node_for_test(&Dead { incarnation: 2, node: "test".into(), from: String::new() });
    m.ml_broadcasts_reset();
    std::thread::sleep(Duration::from_millis(15));
    m.alive_node_for_test(&Alive { incarnation: 3, node: "test".into(), addr: ip4(127, 0, 0, 2), port: 9000, meta: Some(b"foo".to_vec()), vsn: m.build_vsn_array() }, false);
    let st = m.get_node_state("test").unwrap();
    assert_eq!(st.state, NodeStateType::Alive);
    assert_eq!(st.addr, ip4(127, 0, 0, 2));
    assert_eq!(st.port, 9000);
    assert_eq!(st.meta.as_deref(), Some(b"foo".as_slice()));
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_SuspectNode_NoNode
#[test]
fn test_member_list_suspect_node_no_node() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.suspect_node_for_test(&Suspect { incarnation: 1, node: "test".into(), from: String::new() });
    assert_eq!(m.all_nodes().len(), 0);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_SuspectNode
#[tokio::test]
async fn test_member_list_suspect_node() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { probe_interval: Some(Duration::from_millis(1)), suspicion_mult: Some(1), ..Default::default() }).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.change_node_state_for_test("test", |s| s.state_change = SystemTime::now() - Duration::from_secs(3600));
    m.suspect_node_for_test(&Suspect { incarnation: 1, node: "test".into(), from: String::new() });
    let c1 = m.get_node_state("test").unwrap();
    assert_eq!(c1.state, NodeStateType::Suspect);
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Suspect as u8);
    tokio::time::sleep(Duration::from_millis(25)).await;
    let c2 = m.get_node_state("test").unwrap();
    assert_eq!(c2.state, NodeStateType::Dead);
    assert!(c2.state_change > c1.state_change);
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Dead as u8);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_SuspectNode_DoubleSuspect
#[tokio::test]
async fn test_member_list_suspect_node_double_suspect() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.change_node_state_for_test("test", |s| s.state_change = SystemTime::now() - Duration::from_secs(3600));
    let s = Suspect { incarnation: 1, node: "test".into(), from: String::new() };
    m.suspect_node_for_test(&s);
    let c1 = m.get_node_state("test").unwrap();
    assert_eq!(c1.state, NodeStateType::Suspect);
    m.ml_broadcasts_reset();
    m.suspect_node_for_test(&s);
    assert_eq!(m.get_node_state("test").unwrap().state_change, c1.state_change);
    assert_eq!(m.ml_broadcasts_num_queued(), 0);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_SuspectNode_OldSuspect
#[test]
fn test_member_list_suspect_node_old_suspect() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 10, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.ml_broadcasts_reset();
    m.suspect_node_for_test(&Suspect { incarnation: 1, node: "test".into(), from: String::new() });
    assert_eq!(m.get_node_state("test").unwrap().state, NodeStateType::Alive);
    assert_eq!(m.ml_broadcasts_num_queued(), 0);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_SuspectNode_Refute
#[test]
fn test_member_list_suspect_node_refute() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "m".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, true);
    m.ml_broadcasts_reset();
    assert_eq!(m.health_score_pub(), 0);
    m.suspect_node_for_test(&Suspect { incarnation: 1, node: "m".into(), from: String::new() });
    assert_eq!(m.get_node_state("m").unwrap().state, NodeStateType::Alive);
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Alive as u8);
    assert_eq!(m.health_score_pub(), 1);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNode_NoNode
#[test]
fn test_member_list_dead_node_no_node() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.dead_node_for_test(&Dead { incarnation: 1, node: "test".into(), from: String::new() });
    assert_eq!(m.all_nodes().len(), 0);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNodeLeft
#[test]
fn test_member_list_dead_node_left() {
    let (ev, mut rx) = ChanEvents::new(true);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { event_delegate: Some(ev), ..Default::default() }).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "node1".into(), addr: ip4(127, 0, 0, 1), port: 8000, meta: None, vsn: m.build_vsn_array() }, false);
    assert_eq!(rx.try_recv().unwrap().0, EvKind::Join);
    m.dead_node_for_test(&Dead { incarnation: 1, node: "node1".into(), from: "node1".into() });
    assert_eq!(rx.try_recv().unwrap().0, EvKind::Leave);
    assert_eq!(m.get_node_state("node1").unwrap().state, NodeStateType::Left);
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Dead as u8);
    m.alive_node_for_test(&Alive { incarnation: 3, node: "node1".into(), addr: ip4(127, 0, 0, 2), port: 9000, meta: Some(b"foo".to_vec()), vsn: m.build_vsn_array() }, false);
    assert_eq!(rx.try_recv().unwrap().0, EvKind::Join);
    let st = m.get_node_state("node1").unwrap();
    assert_eq!(st.state, NodeStateType::Alive);
    assert_eq!(st.meta.as_deref(), Some(b"foo".as_slice()));
    assert_eq!(st.addr, ip4(127, 0, 0, 2));
    assert_eq!(st.port, 9000);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNode
#[test]
fn test_member_list_dead_node() {
    let (ev, mut rx) = ChanEvents::new(true);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions { event_delegate: Some(ev), ..Default::default() }).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    let _ = rx.try_recv().unwrap();
    m.dead_node_for_test(&Dead { incarnation: 1, node: "test".into(), from: String::new() });
    assert_eq!(m.get_node_state("test").unwrap().state, NodeStateType::Dead);
    let (k, n) = rx.try_recv().unwrap();
    assert_eq!(k, EvKind::Leave);
    assert_eq!(n.name, "test");
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Dead as u8);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNode_Double
#[test]
fn test_member_list_dead_node_double() {
    let (ev, mut rx) = ChanEvents::new(true);
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.dead_node_for_test(&Dead { incarnation: 1, node: "test".into(), from: String::new() });
    m.ml_broadcasts_reset();
    m.set_event_delegate(Some(ev));
    m.dead_node_for_test(&Dead { incarnation: 2, node: "test".into(), from: String::new() });
    assert!(rx.try_recv().is_err());
    assert_eq!(m.ml_broadcasts_num_queued(), 0);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNode_OldDead
#[test]
fn test_member_list_dead_node_old_dead() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 10, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.dead_node_for_test(&Dead { incarnation: 1, node: "test".into(), from: String::new() });
    assert_eq!(m.get_node_state("test").unwrap().state, NodeStateType::Alive);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNode_AliveReplay
#[test]
fn test_member_list_dead_node_alive_replay() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    let a = Alive { incarnation: 10, node: "test".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() };
    m.alive_node_for_test(&a, false);
    m.dead_node_for_test(&Dead { incarnation: 10, node: "test".into(), from: String::new() });
    m.alive_node_for_test(&a, false);
    assert_eq!(m.get_node_state("test").unwrap().state, NodeStateType::Dead);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_DeadNode_Refute
#[test]
fn test_member_list_dead_node_refute() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "m".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, true);
    m.ml_broadcasts_reset();
    assert_eq!(m.health_score_pub(), 0);
    m.dead_node_for_test(&Dead { incarnation: 1, node: "m".into(), from: String::new() });
    assert_eq!(m.get_node_state("m").unwrap().state, NodeStateType::Alive);
    assert_eq!(m.ml_broadcasts_num_queued(), 1);
    assert_eq!(m.ml_broadcasts_ordered()[0].1[0], MessageType::Alive as u8);
    assert_eq!(m.health_score_pub(), 1);
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberList_MergeState
#[tokio::test]
async fn test_member_list_merge_state() {
    let m = MemberlistKv::new(host_cfg("127.0.0.1", "m"), vec![], "", None, KvOptions::default()).unwrap();
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test1".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test2".into(), addr: ip4(127, 0, 0, 2), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.alive_node_for_test(&Alive { incarnation: 1, node: "test3".into(), addr: ip4(127, 0, 0, 3), port: 7946, meta: None, vsn: m.build_vsn_array() }, false);
    m.suspect_node_for_test(&Suspect { incarnation: 1, node: "test1".into(), from: String::new() });
    let (ev, mut rx) = ChanEvents::new(true);
    m.set_event_delegate(Some(ev));
    let remote = vec![
        PushNodeState { name: "test1".into(), addr: ip4(127, 0, 0, 1), port: 7946, meta: None, incarnation: 2, state: NodeStateType::Alive, vsn: vec![] },
        PushNodeState { name: "test2".into(), addr: ip4(127, 0, 0, 2), port: 7946, meta: None, incarnation: 1, state: NodeStateType::Suspect, vsn: vec![] },
        PushNodeState { name: "test3".into(), addr: ip4(127, 0, 0, 3), port: 7946, meta: None, incarnation: 1, state: NodeStateType::Dead, vsn: vec![] },
        PushNodeState { name: "test4".into(), addr: ip4(127, 0, 0, 4), port: 7946, meta: None, incarnation: 2, state: NodeStateType::Alive, vsn: vec![] },
    ];
    m.merge_state_for_test(&remote);
    assert_eq!(m.get_node_state("test1").unwrap().state, NodeStateType::Alive);
    assert_eq!(m.get_node_state("test1").unwrap().incarnation, 2);
    assert_eq!(m.get_node_state("test2").unwrap().state, NodeStateType::Suspect);
    assert_eq!(m.get_node_state("test3").unwrap().state, NodeStateType::Suspect);
    assert_eq!(m.get_node_state("test4").unwrap().state, NodeStateType::Alive);
    let (k, n) = rx.try_recv().unwrap();
    assert_eq!(k, EvKind::Join);
    assert_eq!(n.name, "test4");
    assert!(rx.try_recv().is_err());
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberlist_Gossip
#[tokio::test]
async fn test_memberlist_gossip() {
    let (ev, mut rx) = ChanEvents::new(true);
    let mut c1 = host_cfg("127.0.14.1", "127.0.14.1");
    c1.gossip_interval = GoDuration::from_millis(10);
    let m1 = host_memberlist("127.0.14.1", "127.0.14.1", c1, KvOptions::default()).await;
    let mut c2 = host_cfg("127.0.14.2", "127.0.14.2");
    c2.gossip_interval = GoDuration::from_millis(10);
    let m2 = host_memberlist("127.0.14.2", "127.0.14.2", c2, KvOptions { event_delegate: Some(ev), ..Default::default() }).await;
    let m3 = host_memberlist("127.0.14.3", "127.0.14.3", host_cfg("127.0.14.3", "127.0.14.3"), KvOptions::default()).await;
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.14.1".into(), addr: ip4(127, 0, 14, 1), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.14.2".into(), addr: ip4(127, 0, 14, 2), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.14.3".into(), addr: ip4(127, 0, 14, 3), port: m3.listening_port(), meta: None, vsn: m3.build_vsn_array() }, false);

    let mut received = 0usize;
    for _ in 0..20 {
        m1.gossip_for_test().await;
        tokio::time::sleep(Duration::from_millis(15)).await;
        while rx.try_recv().is_ok() {
            received += 1;
        }
        if received >= 3 {
            break;
        }
    }
    assert!(received >= 3, "expected 3 messages from gossip, got {received}");
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberlist_GossipToDead
#[tokio::test]
async fn test_memberlist_gossip_to_dead() {
    let (ev, mut rx) = ChanEvents::new(true);
    let mut c1 = host_cfg("127.0.14.4", "127.0.14.4");
    c1.gossip_interval = GoDuration::from_millis(1);
    c1.gossip_to_dead_nodes_time = GoDuration::from_millis(100);
    let m1 = host_memberlist("127.0.14.4", "127.0.14.4", c1, KvOptions::default()).await;
    let m2 = host_memberlist("127.0.14.5", "127.0.14.5", host_cfg("127.0.14.5", "127.0.14.5"), KvOptions { event_delegate: Some(ev), ..Default::default() }).await;

    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.14.4".into(), addr: ip4(127, 0, 14, 4), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
    m1.alive_node_for_test(&Alive { incarnation: 1, node: "127.0.14.5".into(), addr: ip4(127, 0, 14, 5), port: m2.listening_port(), meta: None, vsn: m2.build_vsn_array() }, false);
    m1.change_node_state_for_test("127.0.14.5", |s| {
        s.state = NodeStateType::Dead;
        s.state_change = SystemTime::now() - Duration::from_millis(200);
    });
    m1.gossip_for_test().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(rx.try_recv().is_err(), "should not get gossip past GossipToTheDeadTime");

    m1.change_node_state_for_test("127.0.14.5", |s| {
        s.state_change = SystemTime::now() - Duration::from_millis(20);
    });
    let mut got = 0usize;
    for _ in 0..10 {
        m1.gossip_for_test().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        if got >= 2 {
            break;
        }
    }
    assert!(got >= 2, "expected 2 gossip events, got {got}");
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberlist_FailedRemote
#[test]
fn test_memberlist_failed_remote() {
    assert!(!failed_remote(None));
    let io_err = std::io::Error::other("");
    assert!(!failed_remote(Some(&io_err)));
    let cases = [
        ("file", "", false),
        ("udp", "", false),
        ("udp4", "", false),
        ("udp6", "", false),
        ("tcp", "", false),
        ("tcp4", "", false),
        ("tcp6", "", false),
        ("tcp", "dial", true),
        ("tcp", "write", true),
        ("tcp", "read", true),
        ("udp", "write", true),
        ("udp", "read", false),
    ];
    for (net, op, expected) in cases {
        let e = NetOpError { net: net.into(), op: op.into() };
        assert_eq!(failed_remote(Some(&e)), expected, "{net}/{op}");
    }
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestMemberlist_PushPull
#[tokio::test]
async fn test_memberlist_push_pull() {
    for (ppn, num_peers) in [(1usize, 1usize), (2, 2)] {
        let mut c1 = host_cfg("127.0.14.10", "node1");
        c1.gossip_interval = GoDuration::from_secs(10);
        c1.pull_push_interval = GoDuration::from_millis(1);
        let m1 = host_memberlist("127.0.14.10", "node1", c1, KvOptions { push_pull_nodes: Some(ppn), ..Default::default() }).await;

        let mut peers = Vec::new();
        let mut counts = Vec::new();
        for i in 0..num_peers {
            let name = format!("node{}", i + 2);
            let cnt = Arc::new(AtomicUsize::new(0));
            struct CountEv(Arc<AtomicUsize>);
            impl EventDelegate for CountEv {
                fn notify_join(&self, _: &NodeState) { self.0.fetch_add(1, Ordering::Relaxed); }
                fn notify_leave(&self, _: &NodeState) {}
                fn notify_update(&self, _: &NodeState) {}
            }
            let mut pc = host_cfg("127.0.14.10", &name);
            pc.gossip_interval = GoDuration::from_secs(10);
            let p = host_memberlist("127.0.14.10", &name, pc, KvOptions { event_delegate: Some(Arc::new(CountEv(cnt.clone()))), ..Default::default() }).await;
            counts.push(cnt);
            peers.push(p);
        }
        m1.alive_node_for_test(&Alive { incarnation: 1, node: "node1".into(), addr: ip4(127, 0, 14, 10), port: m1.listening_port(), meta: None, vsn: m1.build_vsn_array() }, true);
        for p in &peers {
            m1.alive_node_for_test(&Alive { incarnation: 1, node: p.node_name().to_string(), addr: ip4(127, 0, 14, 10), port: p.listening_port(), meta: None, vsn: p.build_vsn_array() }, false);
        }
        m1.push_pull_for_test().await;
        let expected_events = num_peers + 1;
        let mut ok = false;
        for _ in 0..10 {
            let participating = counts.iter().filter(|c| c.load(Ordering::Relaxed) >= expected_events).count();
            if participating >= ppn {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        assert!(ok, "ppn={ppn} peers={num_peers} failed to deliver events");
        assert!(m1.go_metrics_has_gauge("gauge_memberlist_size_local", &[]));
        for st in ["alive", "dead", "left", "suspect"] {
            assert!(m1.go_metrics_has_gauge("gauge_memberlist_node_instances", &[("node_state", st)]));
        }
        for p in peers {
            p.shutdown().await;
        }
        m1.shutdown().await;
    }
}

// Upstream: github.com/hashicorp/memberlist/state_test.go::TestVerifyProtocol
#[test]
fn test_verify_protocol() {
    type Case<'a> = (&'a [[u8; 3]], &'a [[u8; 3]], bool);
    let cases: [Case<'_>; 5] = [
        (&[[0, 0, 0]], &[[0, 0, 0]], true),
        (&[[0, 0, 0]], &[[0, 1, 0]], true),
        (&[[0, 0, 0]], &[[1, 1, 1]], false),
        (&[[0, 1, 0], [0, 2, 1]], &[[1, 3, 1]], false),
        (&[[0, 3, 2], [0, 2, 0]], &[[0, 2, 1], [0, 5, 0]], true),
    ];
    let check_pair = |a: &[[u8; 6]], b: &[[u8; 6]], expect: bool| {
        let m = MemberlistKv::new(host_cfg("127.0.0.1", "vp"), vec![], "", None, KvOptions::default()).unwrap();
        for (i, v) in a.iter().enumerate() {
            m.insert_raw_node_for_test(NodeState {
                name: format!("a{i}"),
                addr: ip4(127, 0, 0, 1),
                port: 7946 + i as u16,
                meta: None,
                incarnation: 1,
                state: NodeStateType::Alive,
                state_change: SystemTime::now(),
                vsn: v.to_vec(),
            });
        }
        let remote: Vec<PushNodeState> = b.iter().enumerate().map(|(i, v)| PushNodeState {
            name: format!("node {i}"),
            addr: ip4(127, 0, 0, 2),
            port: 7946,
            meta: None,
            incarnation: 1,
            state: NodeStateType::Alive,
            vsn: v.to_vec(),
        }).collect();
        assert_eq!(m.verify_protocol_for_test(&remote).is_ok(), expect);
    };
    for (an, bn, expect) in cases {
        let ac: Vec<[u8; 6]> = an.iter().map(|n| [n[0], n[1], n[2], 0, 0, 0]).collect();
        let aa: Vec<[u8; 6]> = an.iter().map(|n| [0, 0, 0, n[0], n[1], n[2]]).collect();
        let bc: Vec<[u8; 6]> = bn.iter().map(|n| [n[0], n[1], n[2], 0, 0, 0]).collect();
        let ba: Vec<[u8; 6]> = bn.iter().map(|n| [0, 0, 0, n[0], n[1], n[2]]).collect();
        check_pair(&ac, &bc, expect);
        check_pair(&bc, &ac, expect);
        check_pair(&aa, &ba, expect);
        check_pair(&ba, &aa, expect);
    }
}

