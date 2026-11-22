//! Ported upstream hashicorp/memberlist tests: memberlist_test.go (35 tests) + integ_test.go (1 test).

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_config::server::MemberlistConfig;
use mimir_config::GoDuration;
use mimir_ring::kv::memberlist::client::{
    AliveDelegate, ConflictDelegate, EventDelegate, IpPort, MemberlistDelegate, MergeDelegate,
    PingDelegate,
};
use mimir_ring::kv::memberlist::security::{parse_cidrs, Keyring};
use mimir_ring::kv::memberlist::wire::{
    CompressionType, NodeStateType, PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX,
    PROTOCOL_VERSION_MIN,
};
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv, NodeState};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn test_keys() -> Vec<Vec<u8>> {
    vec![(0u8..16).collect()]
}

fn cfg(addr: &str, name: &str, port: u16) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec![addr.to_string()],
        bind_port: i64::from(port),
        node_name: name.to_string(),
        randomize_node_name: false,
        gossip_to_dead_nodes_time: GoDuration::from_secs(30),
        gossip_interval: GoDuration::from_millis(200),
        pull_push_interval: GoDuration::from_secs(30),
        stream_timeout: GoDuration::from_secs(10),
        ..Default::default()
    }
}

async fn create_ml(addr: &str, name: &str, port: u16, opts: KvOptions) -> Arc<MemberlistKv> {
    MemberlistKv::start_with_options(
        cfg(addr, name, port),
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            ..opts
        },
    )
    .await
    .unwrap()
}

async fn create_ml_with_cfg(c: MemberlistConfig, opts: KvOptions) -> Arc<MemberlistKv> {
    MemberlistKv::start_with_options(
        c,
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            ..opts
        },
    )
    .await
    .unwrap()
}

async fn wait_for(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let dl = Instant::now() + timeout;
    while Instant::now() < dl {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f()
}

struct MockDelegate {
    meta: Mutex<Vec<u8>>,
    msgs: Mutex<Vec<Vec<u8>>>,
    broadcasts: Mutex<Vec<Vec<u8>>>,
    state: Mutex<Vec<u8>>,
    remote_state: Mutex<Vec<u8>>,
}

impl MockDelegate {
    fn new(meta: &[u8]) -> Arc<Self> {
        Arc::new(Self {
            meta: Mutex::new(meta.to_vec()),
            msgs: Mutex::new(Vec::new()),
            broadcasts: Mutex::new(Vec::new()),
            state: Mutex::new(Vec::new()),
            remote_state: Mutex::new(Vec::new()),
        })
    }
}

impl MemberlistDelegate for MockDelegate {
    fn node_meta(&self, _limit: usize) -> Vec<u8> {
        self.meta.lock().clone()
    }
    fn notify_msg(&self, msg: &[u8]) {
        self.msgs.lock().push(msg.to_vec());
    }
    fn get_broadcasts(&self, _overhead: usize, limit: usize) -> Vec<Vec<u8>> {
        let mut b = self.broadcasts.lock();
        let mut out = Vec::new();
        let mut used = 0usize;
        while let Some(item) = b.first() {
            if used + item.len() + 3 > limit {
                break;
            }
            let item = b.remove(0);
            used += item.len() + 3;
            out.push(item);
        }
        out
    }
    fn local_state(&self, _join: bool) -> Vec<u8> {
        self.state.lock().clone()
    }
    fn merge_remote_state(&self, buf: &[u8], _join: bool) {
        *self.remote_state.lock() = buf.to_vec();
    }
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestDefaultLANConfig_protocolVersion
#[test]
fn test_default_lan_config_protocol_version() {
    let m = MemberlistKv::new(cfg("127.0.0.1", "n", 0), vec![], "", None, KvOptions::default()).unwrap();
    assert_eq!(m.protocol_version(), PROTOCOL_VERSION_2_COMPATIBLE);
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_protocolVersion
#[tokio::test]
async fn test_create_protocol_version() {
    let cases = [
        (PROTOCOL_VERSION_MIN, false),
        (PROTOCOL_VERSION_MAX, false),
        (PROTOCOL_VERSION_MAX + 1, true),
        (PROTOCOL_VERSION_MAX - 1, false),
    ];
    for (vsn, expect_err) in cases {
        let res = MemberlistKv::start_with_options(
            cfg("127.0.15.1", "pv", 0),
            vec![],
            "",
            None,
            KvOptions {
                protocol_version: Some(vsn),
                use_transport_bind_port: true,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(res.is_err(), expect_err, "vsn={vsn}");
        if let Ok(m) = res {
            m.shutdown().await;
        }
    }
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_secretKey
#[tokio::test]
async fn test_create_secret_key() {
    let cases: [(&[u8], bool); 4] = [
        (&[], false),
        (b"abc", true),
        (&[0u8; 16], false),
        (&[0u8; 38], true),
    ];
    for (i, (key, expect_err)) in cases.into_iter().enumerate() {
        let res = MemberlistKv::start_with_options(
            cfg(&format!("127.0.15.{}", 10 + i), "sk", 0),
            vec![],
            "",
            None,
            KvOptions {
                secret_key: Some(key.to_vec()),
                use_transport_bind_port: true,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(res.is_err(), expect_err, "case {i}");
        if let Ok(m) = res {
            m.shutdown().await;
        }
    }
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_secretKeyEmpty
#[tokio::test]
async fn test_create_secret_key_empty() {
    let m = create_ml(
        "127.0.15.20",
        "ske",
        0,
        KvOptions {
            secret_key: Some(vec![]),
            ..Default::default()
        },
    )
    .await;
    assert!(!m.encryption_enabled());
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_checkBroadcastQueueMetrics
#[tokio::test]
async fn test_create_check_broadcast_queue_metrics() {
    let m = create_ml(
        "127.0.15.21",
        "qmetrics",
        0,
        KvOptions {
            queue_check_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        },
    )
    .await;
    assert!(wait_for(Duration::from_secs(1), || m.go_metrics_has_sample("sample_memberlist_queue_broadcasts")).await);
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_keyringOnly
#[tokio::test]
async fn test_create_keyring_only() {
    let kr = Arc::new(Keyring::new(&[], Some(&[0u8; 16])).unwrap());
    let m = create_ml(
        "127.0.15.22",
        "kro",
        0,
        KvOptions {
            keyring: Some(kr),
            ..Default::default()
        },
    )
    .await;
    assert!(m.encryption_enabled());
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_keyringAndSecretKey
#[tokio::test]
async fn test_create_keyring_and_secret_key() {
    let kr = Arc::new(Keyring::new(&[], Some(&[0u8; 16])).unwrap());
    let sk = vec![1u8; 16];
    let m = create_ml(
        "127.0.15.23",
        "krsk",
        0,
        KvOptions {
            keyring: Some(kr.clone()),
            secret_key: Some(sk.clone()),
            ..Default::default()
        },
    )
    .await;
    assert!(m.encryption_enabled());
    assert_eq!(kr.get_primary_key().as_deref(), Some(sk.as_slice()));
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate_invalidLoggerSettings
#[test]
fn test_create_invalid_logger_settings() {
    let res = MemberlistKv::new(
        cfg("127.0.0.1", "log", 0),
        vec![],
        "",
        None,
        KvOptions {
            logger_configured: true,
            log_output_configured: true,
            ..Default::default()
        },
    );
    assert!(res.is_err());
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestCreate
#[tokio::test]
async fn test_create() {
    // happy path
    let m = create_ml(
        "127.0.15.24",
        "create",
        0,
        KvOptions {
            protocol_version: Some(PROTOCOL_VERSION_MIN),
            delegate_protocol_version: 13,
            delegate_protocol_min: 12,
            delegate_protocol_max: 24,
            ..Default::default()
        },
    )
    .await;
    let members = m.members();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].vsn, vec![PROTOCOL_VERSION_MIN, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN, 12, 24, 13]);
    m.shutdown().await;

    // compression algorithm validation
    for (algo, want, want_err) in [
        ("", Some(CompressionType::Lzw), false),
        ("lzw", Some(CompressionType::Lzw), false),
        ("snappy", Some(CompressionType::Snappy), false),
        ("zstd", None, true),
        ("LZW", None, true),
    ] {
        let mut c = cfg("127.0.15.25", "comp", 0);
        c.compression_algorithm = algo.to_string();
        let res = MemberlistKv::start_with_options(
            c,
            vec![],
            "",
            None,
            KvOptions {
                validate_compression_algo: true,
                use_transport_bind_port: true,
                ..Default::default()
            },
        )
        .await;
        if want_err {
            assert!(res.is_err(), "algo={algo}");
        } else {
            let m = res.unwrap();
            assert_eq!(Some(m.compression_type()), want);
            m.shutdown().await;
        }
    }
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberList_CreateShutdown
#[tokio::test]
async fn test_member_list_create_shutdown() {
    let m = create_ml("127.0.15.26", "cs", 0, KvOptions::default()).await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberList_ResolveAddr
#[tokio::test]
async fn test_member_list_resolve_addr() {
    let m = create_ml("127.0.15.27", "res", 0, KvOptions::default()).await;
    let dp = m.listening_port();
    let v6_ex = IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0x0a0b, 0x12f0, 0, 0, 0, 1));
    let base: [(&str, bool, bool, Option<IpAddr>, u16); 11] = [
        ("localhost", false, true, None, dp),
        ("[::1]:80", false, false, Some(IpAddr::V6(Ipv6Addr::LOCALHOST)), 80),
        ("[::1]", false, false, Some(IpAddr::V6(Ipv6Addr::LOCALHOST)), dp),
        (":80", true, false, None, 0),
        ("localhost:80", false, true, None, 80),
        ("localhost:80000", true, false, None, 0),
        ("127.0.0.1:80", false, false, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), 80),
        ("[2001:db8:a0b:12f0::1]:80", false, false, Some(v6_ex), 80),
        ("/127.0.0.1:80", true, false, None, 0),
        ("127.0.0.1", false, false, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), dp),
        ("[2001:db8:a0b:12f0::1]", false, false, Some(v6_ex), dp),
    ];
    for (input, expect_err, ignore_ip, exp_ip, exp_port) in base {
        for tagged in [false, true] {
            if tagged && input.contains('/') {
                continue;
            }
            let arg = if tagged { format!("foo.bar/{input}") } else { input.to_string() };
            let got = m.resolve_addr_ip_ports(&arg).await;
            if expect_err {
                assert!(got.is_err(), "expected error for {arg}");
            } else {
                let got = got.unwrap();
                assert!(!got.is_empty(), "empty for {arg}");
                let exp_name = if tagged { "foo.bar" } else { "" };
                let check = if ignore_ip { &got[0..1] } else { &got[..] };
                for item in check {
                    assert_eq!(item.port, exp_port, "{arg}");
                    assert_eq!(item.node_name, exp_name, "{arg}");
                    if !ignore_ip {
                        assert_eq!(item.ip, exp_ip, "{arg}");
                    }
                }
            }
        }
    }
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberList_ResolveAddr_TCP_First
#[tokio::test]
async fn test_member_list_resolve_addr_tcp_first() {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dns_addr = ln.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let srv = tokio::spawn(async move {
        while !stop2.load(Ordering::Acquire) {
            let Ok(Ok((mut sock, _))) = tokio::time::timeout(Duration::from_millis(50), ln.accept()).await else {
                continue;
            };
            let mut lbuf = [0u8; 2];
            if sock.read_exact(&mut lbuf).await.is_err() {
                continue;
            }
            let qlen = u16::from_be_bytes(lbuf) as usize;
            let mut q = vec![0u8; qlen];
            sock.read_exact(&mut q).await.unwrap();
            // Build DNS reply with A (127.0.0.1) and AAAA (2001:db8:a0b:12f0::1)
            let q_section = &q[12..];
            let mut resp = vec![q[0], q[1], 0x84, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00];
            resp.extend_from_slice(q_section);
            // RR 1: pointer to offset 12 (0xc0, 0x0c), Type A (1), Class IN (1), TTL 60, RDLEN 4, 127.0.0.1
            resp.extend_from_slice(&[0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0, 0, 0, 60, 0x00, 0x04, 127, 0, 0, 1]);
            // RR 2: pointer to offset 12, Type AAAA (28), Class IN (1), TTL 60, RDLEN 16
            resp.extend_from_slice(&[
                0xc0, 0x0c, 0x00, 0x1c, 0x00, 0x01, 0, 0, 0, 60, 0x00, 0x10,
                0x20, 0x01, 0x0d, 0xb8, 0x0a, 0x0b, 0x12, 0xf0, 0, 0, 0, 0, 0, 0, 0, 1,
            ]);
            let mut frame = Vec::with_capacity(2 + resp.len());
            frame.extend_from_slice(&(resp.len() as u16).to_be_bytes());
            frame.extend_from_slice(&resp);
            let _ = sock.write_all(&frame).await;
            let _ = sock.flush().await;
        }
    });

    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), format!("nameserver {dns_addr}
")).unwrap();

    let m = create_ml(
        "127.0.15.28",
        "dnstcp",
        0,
        KvOptions {
            dns_config_path: Some(tmp.path().to_string_lossy().into_owned()),
            ..Default::default()
        },
    )
    .await;
    let port = m.listening_port();
    let expected = vec![
        IpPort { ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), port, node_name: String::new() },
        IpPort {
            ip: Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0x0a0b, 0x12f0, 0, 0, 0, 1))),
            port,
            node_name: String::new(),
        },
    ];
    for host in ["join.service.consul.", "join.service.consul"] {
        assert_eq!(m.resolve_addr_ip_ports(host).await.unwrap(), expected);
    }
    stop.store(true, Ordering::Release);
    let _ = srv.await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberList_Members
#[test]
fn test_member_list_members() {
    let m = MemberlistKv::new(cfg("127.0.0.1", "test", 0), vec![], "", None, KvOptions::default()).unwrap();
    for (name, st) in [
        ("test", NodeStateType::Alive),
        ("test2", NodeStateType::Dead),
        ("test3", NodeStateType::Suspect),
    ] {
        m.insert_raw_node_for_test(NodeState {
            name: name.into(),
            addr: vec![127, 0, 0, 1],
            port: 7946,
            meta: None,
            incarnation: 1,
            state: st,
            state_change: std::time::SystemTime::now(),
            vsn: vec![],
        });
    }
    let names: Vec<String> = m.members().into_iter().map(|n| n.name).collect();
    assert_eq!(names, vec!["test".to_string(), "test3".to_string()]);
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join
#[tokio::test]
async fn test_memberlist_join() {
    let m1 = create_ml("127.0.15.30", "127.0.15.30", 0, KvOptions::default()).await;
    let m2 = create_ml("127.0.15.31", "127.0.15.31", m1.listening_port(), KvOptions::default()).await;
    let num = m2.join_members(&["127.0.15.30/127.0.15.30".to_string()]).await.unwrap();
    assert_eq!(num, 1);
    assert_eq!(m2.members().len(), 2);
    assert_eq!(m2.est_num_nodes_pub(), 2);
    m2.shutdown().await;
    m1.shutdown().await;
}

async fn run_join_with_labels(secret: Option<Vec<u8>>) {
    let mut c1 = cfg("127.0.15.32", "l1", 0);
    c1.cluster_label = "blah".into();
    let m1 = create_ml_with_cfg(c1, KvOptions { secret_key: secret.clone(), ..Default::default() }).await;
    let port = m1.listening_port();

    let mut c2 = cfg("127.0.15.33", "l2", port);
    c2.cluster_label = "blah".into();
    let m2 = create_ml_with_cfg(c2, KvOptions { secret_key: secret.clone(), ..Default::default() }).await;
    assert_eq!(m2.join_members(&["l1/127.0.15.32".into()]).await.unwrap(), 1);
    assert_eq!(m2.members().len(), 2);
    assert_eq!(m1.members().len(), 2);

    let c3 = cfg("127.0.15.34", "l3", port);
    let m3 = create_ml_with_cfg(c3, KvOptions { secret_key: secret.clone(), ..Default::default() }).await;
    assert!(m3.join_members(&["l1/127.0.15.32".into()]).await.is_err());
    assert_eq!(m3.members().len(), 1);

    let mut c4 = cfg("127.0.15.35", "l4", port);
    c4.cluster_label = "not-blah".into();
    let m4 = create_ml_with_cfg(c4, KvOptions { secret_key: secret, ..Default::default() }).await;
    assert!(m4.join_members(&["l1/127.0.15.32".into()]).await.is_err());
    assert_eq!(m4.members().len(), 1);
    assert_eq!(m2.members().len(), 2);

    m4.shutdown().await;
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_with_Labels
#[tokio::test]
async fn test_memberlist_join_with_labels() {
    run_join_with_labels(None).await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_with_Labels_and_Encryption
#[tokio::test]
async fn test_memberlist_join_with_labels_and_encryption() {
    run_join_with_labels(Some(test_keys()[0].clone())).await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_JoinDifferentNetworksUniqueMask
#[tokio::test]
async fn test_memberlist_join_different_networks_unique_mask() {
    let cidrs = Some(parse_cidrs(&["127.0.0.0/8"]).unwrap());
    let m1 = create_ml("127.0.15.40", "n1", 0, KvOptions { cidrs_allowed: cidrs.clone(), ..Default::default() }).await;
    let m2 = create_ml("127.1.15.40", "n2", m1.listening_port(), KvOptions { cidrs_allowed: cidrs, ..Default::default() }).await;
    assert_eq!(m2.join_members(&["n1/127.0.15.40".into()]).await.unwrap(), 1);
    assert_eq!(m2.members().len(), 2);
    assert_eq!(m2.est_num_nodes_pub(), 2);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_JoinDifferentNetworksMultiMasks
#[tokio::test]
async fn test_memberlist_join_different_networks_multi_masks() {
    let masks = Some(parse_cidrs(&["127.0.0.0/24", "127.0.1.0/24"]).unwrap());
    let all = Some(parse_cidrs(&["127.0.0.0/8"]).unwrap());
    let m1 = create_ml("127.0.0.50", "n1", 0, KvOptions { cidrs_allowed: masks.clone(), ..Default::default() }).await;
    let p = m1.listening_port();
    let m2 = create_ml("127.0.1.50", "n2", p, KvOptions { cidrs_allowed: masks.clone(), ..Default::default() }).await;
    assert_eq!(m2.join_members(&["n1/127.0.0.50".into()]).await.unwrap(), 1);
    assert_eq!(m2.members().len(), 2);

    let m3 = create_ml("127.0.2.50", "n3", p, KvOptions { cidrs_allowed: all, ..Default::default() }).await;
    assert_eq!(m3.join_members(&["n1/127.0.0.50".into()]).await.unwrap(), 1);
    assert_eq!(m3.members().len(), 3);
    assert_eq!(m1.members().len(), 2);
    assert_eq!(m2.members().len(), 2);

    let m4 = create_ml("127.0.2.51", "n4", p, KvOptions { cidrs_allowed: masks, ..Default::default() }).await;
    assert_eq!(m4.join_members(&["127.0.0.50".into(), "127.0.1.50".into()]).await.unwrap(), 2);
    assert_eq!(m4.members().len(), 2);
    assert_eq!(m1.members().len(), 2);
    assert_eq!(m2.members().len(), 2);

    m4.shutdown().await;
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_Cancel
#[tokio::test]
async fn test_memberlist_join_cancel() {
    struct CancelMerge(AtomicBool);
    impl MergeDelegate for CancelMerge {
        fn notify_merge(&self, _peers: &[NodeState]) -> Result<(), String> {
            self.0.store(true, Ordering::Release);
            Err("Custom merge canceled".to_string())
        }
    }
    let d1 = Arc::new(CancelMerge(AtomicBool::new(false)));
    let d2 = Arc::new(CancelMerge(AtomicBool::new(false)));
    let m1 = create_ml("127.0.15.60", "c1", 0, KvOptions { merge_delegate: Some(d1.clone()), ..Default::default() }).await;
    let m2 = create_ml("127.0.15.61", "c2", m1.listening_port(), KvOptions { merge_delegate: Some(d2.clone()), ..Default::default() }).await;
    let err = m2.join_members(&["c1/127.0.15.60".into()]).await.unwrap_err().to_string();
    assert!(err.contains("Custom merge canceled"), "{err}");
    assert_eq!(m2.members().len(), 1);
    assert_eq!(m1.members().len(), 1);
    assert!(d1.0.load(Ordering::Acquire));
    assert!(d2.0.load(Ordering::Acquire));
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_Cancel_Passive
#[tokio::test]
async fn test_memberlist_join_cancel_passive() {
    struct CancelAlive {
        ignore: String,
        count: AtomicUsize,
    }
    impl AliveDelegate for CancelAlive {
        fn notify_alive(&self, peer: &NodeState) -> Result<(), String> {
            self.count.fetch_add(1, Ordering::Relaxed);
            if peer.name == self.ignore {
                Ok(())
            } else {
                Err("Custom alive canceled".to_string())
            }
        }
    }
    let a1 = Arc::new(CancelAlive { ignore: "p1".into(), count: AtomicUsize::new(0) });
    let a2 = Arc::new(CancelAlive { ignore: "p2".into(), count: AtomicUsize::new(0) });
    let m1 = create_ml("127.0.15.62", "p1", 0, KvOptions { alive_delegate: Some(a1.clone()), ..Default::default() }).await;
    let m2 = create_ml("127.0.15.63", "p2", m1.listening_port(), KvOptions { alive_delegate: Some(a2.clone()), ..Default::default() }).await;
    assert_eq!(m2.join_members(&["p1/127.0.15.62".into()]).await.unwrap(), 1);
    assert_eq!(m2.members().len(), 1);
    assert_eq!(m1.members().len(), 1);
    assert!(a1.count.load(Ordering::Relaxed) > 0);
    assert!(a2.count.load(Ordering::Relaxed) > 0);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_protocolVersions
#[tokio::test]
async fn test_memberlist_join_protocol_versions() {
    let m1 = create_ml("127.0.15.64", "pv1", 0, KvOptions::default()).await;
    let p = m1.listening_port();
    let m2 = create_ml("127.0.15.65", "pv2", p, KvOptions::default()).await;
    let m3 = create_ml("127.0.15.66", "pv3", p, KvOptions { protocol_version: Some(PROTOCOL_VERSION_MAX), ..Default::default() }).await;
    m1.join_members(&["pv2/127.0.15.65".into()]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    m1.join_members(&["pv3/127.0.15.66".into()]).await.unwrap();
    m3.shutdown().await;
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Leave
#[tokio::test]
async fn test_memberlist_leave() {
    let mut c1 = cfg("127.0.15.70", "lv1", 0);
    c1.gossip_interval = GoDuration::from_millis(1);
    let m1 = create_ml_with_cfg(c1, KvOptions::default()).await;
    let mut c2 = cfg("127.0.15.71", "lv2", m1.listening_port());
    c2.gossip_interval = GoDuration::from_millis(1);
    let m2 = create_ml_with_cfg(c2, KvOptions::default()).await;
    assert_eq!(m2.join_members(&["lv1/127.0.15.70".into()]).await.unwrap(), 1);
    assert_eq!(m2.members().len(), 2);
    m1.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || m2.members().len() == 1).await);
    assert_eq!(m1.members().len(), 1);
    assert_eq!(m2.get_node_state("lv1").unwrap().state, NodeStateType::Left);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_JoinShutdown
#[tokio::test]
async fn test_memberlist_join_shutdown() {
    let opts = KvOptions {
        probe_interval: Some(Duration::from_millis(5)),
        probe_timeout: Some(Duration::from_millis(1)),
        suspicion_mult: Some(1),
        suspicion_max_timeout_mult: Some(1),
        ..Default::default()
    };
    let m1 = create_ml("127.0.15.72", "js1", 0, opts.clone()).await;
    let m2 = create_ml("127.0.15.73", "js2", m1.listening_port(), opts).await;
    assert_eq!(m2.join_members(&["js1/127.0.15.72".into()]).await.unwrap(), 1);
    assert_eq!(m2.members().len(), 2);
    m1.shutdown().await;
    assert!(wait_for(Duration::from_secs(5), || m2.members().len() == 1).await);
    m2.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_delegateMeta
#[tokio::test]
async fn test_memberlist_delegate_meta() {
    let d1 = MockDelegate::new(b"web");
    let d2 = MockDelegate::new(b"lb");
    let m1 = create_ml("127.0.15.74", "dm1", 0, KvOptions { memberlist_delegate: Some(d1), ..Default::default() }).await;
    let m2 = create_ml("127.0.15.75", "dm2", m1.listening_port(), KvOptions { memberlist_delegate: Some(d2), ..Default::default() }).await;
    m1.join_members(&["dm2/127.0.15.75".into()]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    for m in [&m1, &m2] {
        let map: HashMap<String, String> = m.members().into_iter().map(|n| (n.name, String::from_utf8(n.meta.unwrap_or_default()).unwrap())).collect();
        assert_eq!(map.get("dm1").map(String::as_str), Some("web"));
        assert_eq!(map.get("dm2").map(String::as_str), Some("lb"));
    }
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_delegateMeta_Update
#[tokio::test]
async fn test_memberlist_delegate_meta_update() {
    let d1 = MockDelegate::new(b"web");
    let d2 = MockDelegate::new(b"lb");
    let m1 = create_ml("127.0.15.76", "mu1", 0, KvOptions { memberlist_delegate: Some(d1.clone()), ..Default::default() }).await;
    let m2 = create_ml("127.0.15.77", "mu2", m1.listening_port(), KvOptions { memberlist_delegate: Some(d2.clone()), ..Default::default() }).await;
    m1.join_members(&["mu2/127.0.15.77".into()]).await.unwrap();
    *d1.meta.lock() = b"api".to_vec();
    *d2.meta.lock() = b"db".to_vec();
    m1.update_node(Duration::from_secs(1)).await.unwrap();
    m2.update_node(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || {
        m1.get_node_state("mu2").and_then(|n| n.meta).as_deref() == Some(b"db")
            && m2.get_node_state("mu1").and_then(|n| n.meta).as_deref() == Some(b"api")
    }).await);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_UserData
#[tokio::test]
async fn test_memberlist_user_data() {
    let d1 = MockDelegate::new(b"");
    *d1.state.lock() = b"something".to_vec();
    let d2 = MockDelegate::new(b"");
    *d2.state.lock() = b"my state".to_vec();
    let bcasts: Vec<Vec<u8>> = (0..128).map(|i| format!("{i}").into_bytes()).collect();
    *d2.broadcasts.lock() = bcasts.clone();

    let mut c1 = cfg("127.0.15.78", "ud1", 0);
    c1.gossip_interval = GoDuration::from_millis(20);
    c1.pull_push_interval = GoDuration::from_millis(50);
    let m1 = create_ml_with_cfg(c1, KvOptions { memberlist_delegate: Some(d1.clone()), ..Default::default() }).await;
    let mut c2 = cfg("127.0.15.79", "ud2", m1.listening_port());
    c2.gossip_interval = GoDuration::from_millis(20);
    c2.pull_push_interval = GoDuration::from_millis(50);
    let m2 = create_ml_with_cfg(c2, KvOptions { memberlist_delegate: Some(d2.clone()), ..Default::default() }).await;
    m2.join_members(&["ud1/127.0.15.78".into()]).await.unwrap();

    let ok = wait_for(Duration::from_secs(10), || {
        let msgs: HashSet<Vec<u8>> = d1.msgs.lock().iter().cloned().collect();
        bcasts.iter().all(|b| msgs.contains(b))
            && d1.remote_state.lock().as_slice() == b"my state"
            && d2.remote_state.lock().as_slice() == b"something"
    })
    .await;
    assert!(ok, "user data broadcasts/state did not converge");
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_SendTo
#[tokio::test]
async fn test_memberlist_send_to() {
    let d1 = MockDelegate::new(b"");
    let d2 = MockDelegate::new(b"");
    let m1 = create_ml("127.0.15.80", "st1", 0, KvOptions { memberlist_delegate: Some(d1.clone()), ..Default::default() }).await;
    let p = m1.listening_port();
    let m2 = create_ml("127.0.15.81", "st2", p, KvOptions { memberlist_delegate: Some(d2.clone()), ..Default::default() }).await;
    m2.join_members(&["st1/127.0.15.80".into()]).await.unwrap();
    m1.send_to_address(&format!("127.0.15.81:{p}"), "st2", b"ping").await.unwrap();
    m2.send_to_address(&format!("127.0.15.80:{p}"), "st1", b"pong").await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || d1.msgs.lock().len() == 1 && d2.msgs.lock().len() == 1).await);
    assert_eq!(d1.msgs.lock()[0], b"pong");
    assert_eq!(d2.msgs.lock()[0], b"ping");
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlistProtocolVersion
#[tokio::test]
async fn test_memberlist_protocol_version() {
    let m = create_ml("127.0.15.82", "mpv", 0, KvOptions { protocol_version: Some(PROTOCOL_VERSION_MAX), ..Default::default() }).await;
    assert_eq!(m.protocol_version(), PROTOCOL_VERSION_MAX);
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_DeadNode
#[tokio::test]
async fn test_memberlist_join_dead_node() {
    let mut c1 = cfg("127.0.15.83", "jdn", 0);
    c1.stream_timeout = GoDuration::from_millis(50);
    let m1 = create_ml_with_cfg(c1, KvOptions::default()).await;
    let ln = TcpListener::bind(format!("127.0.15.84:{}", m1.listening_port())).await.unwrap();
    let start = Instant::now();
    let res = m1.join_members(&["fake/127.0.15.84".into()]).await;
    assert!(res.is_err());
    assert!(start.elapsed() < Duration::from_millis(250));
    drop(ln);
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_Protocol_Compatibility
#[tokio::test]
async fn test_memberlist_join_protocol_compatibility() {
    for (idx, (pv1, pv2)) in [(2u8, 1u8), (2, 3), (3, 2), (3, 1)].into_iter().enumerate() {
        let a1 = format!("127.0.16.{}", 10 + idx * 2);
        let a2 = format!("127.0.16.{}", 11 + idx * 2);
        let m1 = create_ml(&a1, &a1, 0, KvOptions { protocol_version: Some(pv1), ..Default::default() }).await;
        let m2 = create_ml(&a2, &a2, m1.listening_port(), KvOptions { protocol_version: Some(pv2), ..Default::default() }).await;
        assert_eq!(m2.join_members(&[format!("{a1}/{a1}")]).await.unwrap(), 1);
        assert_eq!(m2.members().len(), 2);
        assert_eq!(m1.members().len(), 2);
        m2.shutdown().await;
        m1.shutdown().await;
    }
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_Join_IPv6
#[tokio::test]
async fn test_memberlist_join_ipv6() {
    let m1 = create_ml("[::1]", "A", 0, KvOptions::default()).await;
    let m2 = create_ml("[::1]", "B", 0, KvOptions::default()).await;
    let num = m2.join_members(&[format!("A/[::1]:{}", m1.listening_port())]).await.unwrap();
    assert_eq!(num, 1);
    assert_eq!(m2.members().len(), 2);
    assert_eq!(m1.members().len(), 2);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestAdvertiseAddr
#[tokio::test]
async fn test_advertise_addr() {
    let mut c = cfg("127.0.16.30", "127.0.16.30", 0);
    c.advertise_addr = "127.0.16.31".into();
    c.advertise_port = 41234;
    let m = create_ml_with_cfg(c, KvOptions::default()).await;
    let members = m.members();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].addr, vec![127, 0, 16, 31]);
    assert_eq!(members[0].port, 41234);
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_conflictDelegate
#[tokio::test]
async fn test_memberlist_conflict_delegate() {
    struct MockConflict(Mutex<Option<(String, String)>>);
    impl ConflictDelegate for MockConflict {
        fn notify_conflict(&self, existing: &NodeState, other: &NodeState) {
            *self.0.lock() = Some((existing.name.clone(), other.name.clone()));
        }
    }
    let cd = Arc::new(MockConflict(Mutex::new(None)));
    let m1 = create_ml("127.0.16.32", "same-name", 0, KvOptions { conflict_delegate: Some(cd.clone()), ..Default::default() }).await;
    let m2 = create_ml("127.0.16.33", "same-name", m1.listening_port(), KvOptions::default()).await;
    m1.join_members(&["same-name/127.0.16.33".into()]).await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || cd.0.lock().is_some()).await);
    let (e, o) = cd.0.lock().clone().unwrap();
    assert_eq!(e, o);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_PingDelegate
#[tokio::test]
async fn test_memberlist_ping_delegate() {
    struct MockPing(Mutex<Option<(NodeState, Duration, Vec<u8>)>>);
    impl PingDelegate for MockPing {
        fn ack_payload(&self) -> Vec<u8> {
            b"whatever".to_vec()
        }
        fn notify_ping_complete(&self, other: &NodeState, rtt: Duration, payload: &[u8]) {
            *self.0.lock() = Some((other.clone(), rtt, payload.to_vec()));
        }
    }
    let p1 = Arc::new(MockPing(Mutex::new(None)));
    let p2 = Arc::new(MockPing(Mutex::new(None)));
    let m1 = create_ml("127.0.16.34", "pd1", 0, KvOptions { probe_interval: Some(Duration::from_millis(80)), probe_timeout: Some(Duration::from_millis(40)), ping_delegate: Some(p1), ..Default::default() }).await;
    let m2 = create_ml("127.0.16.35", "pd2", m1.listening_port(), KvOptions { probe_interval: Some(Duration::from_millis(80)), probe_timeout: Some(Duration::from_millis(40)), ping_delegate: Some(p2.clone()), ..Default::default() }).await;
    m2.join_members(&["pd1/127.0.16.34".into()]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || p2.0.lock().is_some()).await);
    let (other, rtt, payload) = p2.0.lock().clone().unwrap();
    assert_eq!(other.name, m1.local_node().unwrap().name);
    assert!(rtt > Duration::ZERO);
    assert_eq!(payload, b"whatever");
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/memberlist_test.go::TestMemberlist_EncryptedGossipTransition
#[tokio::test]
async fn test_memberlist_encrypted_gossip_transition() {
    let key = b"Hi16ZXu2lNCRVwtr20khAg==".to_vec();
    let mk_opts = |sk: Option<Vec<u8>>, vin: bool, vout: bool| KvOptions {
        secret_key: sk,
        gossip_verify_incoming: vin,
        gossip_verify_outgoing: vout,
        probe_interval: Some(Duration::from_millis(50)),
        probe_timeout: Some(Duration::from_millis(25)),
        ..Default::default()
    };
    let a0 = "127.0.16.40";
    let a1 = "127.0.16.41";
    let mut c0 = cfg(a0, a0, 0);
    c0.gossip_interval = GoDuration::from_millis(25);
    c0.leave_timeout = GoDuration::from_secs(1);
    let m0 = create_ml_with_cfg(c0, mk_opts(None, true, true)).await;
    let port = m0.listening_port();

    let mut c1 = cfg(a1, a1, port);
    c1.gossip_interval = GoDuration::from_millis(25);
    c1.leave_timeout = GoDuration::from_secs(1);
    let m1 = create_ml_with_cfg(c1, mk_opts(None, true, true)).await;
    m1.join_members(&[format!("{a0}/{a0}")]).await.unwrap();
    assert_eq!(m0.members().len(), 2);

    // Step 1: key configured, verify_in=false, verify_out=false
    m0.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m1.members().len() == 1).await);
    m0.shutdown().await;
    let mut c0 = cfg(a0, a0, port);
    c0.gossip_interval = GoDuration::from_millis(25);
    c0.leave_timeout = GoDuration::from_secs(1);
    let m0 = create_ml_with_cfg(c0, mk_opts(Some(key.clone()), false, false)).await;
    m0.join_members(&[format!("{a1}/{a1}")]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 2 && m1.members().len() == 2).await);

    m1.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 1).await);
    m1.shutdown().await;
    let mut c1 = cfg(a1, a1, port);
    c1.gossip_interval = GoDuration::from_millis(25);
    c1.leave_timeout = GoDuration::from_secs(1);
    let m1 = create_ml_with_cfg(c1, mk_opts(Some(key.clone()), false, false)).await;
    m1.join_members(&[format!("{a0}/{a0}")]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 2 && m1.members().len() == 2).await);

    // Step 2: verify_in=false, verify_out=true
    m0.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m1.members().len() == 1).await);
    m0.shutdown().await;
    let mut c0 = cfg(a0, a0, port);
    c0.gossip_interval = GoDuration::from_millis(25);
    c0.leave_timeout = GoDuration::from_secs(1);
    let m0 = create_ml_with_cfg(c0, mk_opts(Some(key.clone()), false, true)).await;
    m0.join_members(&[format!("{a1}/{a1}")]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 2 && m1.members().len() == 2).await);

    m1.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 1).await);
    m1.shutdown().await;
    let mut c1 = cfg(a1, a1, port);
    c1.gossip_interval = GoDuration::from_millis(25);
    c1.leave_timeout = GoDuration::from_secs(1);
    let m1 = create_ml_with_cfg(c1, mk_opts(Some(key.clone()), false, true)).await;
    m1.join_members(&[format!("{a0}/{a0}")]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 2 && m1.members().len() == 2).await);

    // Step 3: verify_in=true, verify_out=true
    m0.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m1.members().len() == 1).await);
    m0.shutdown().await;
    let mut c0 = cfg(a0, a0, port);
    c0.gossip_interval = GoDuration::from_millis(25);
    c0.leave_timeout = GoDuration::from_secs(1);
    let m0 = create_ml_with_cfg(c0, mk_opts(Some(key.clone()), true, true)).await;
    m0.join_members(&[format!("{a1}/{a1}")]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 2 && m1.members().len() == 2).await);

    m1.leave_cluster(Duration::from_secs(1)).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 1).await);
    m1.shutdown().await;
    let mut c1 = cfg(a1, a1, port);
    c1.gossip_interval = GoDuration::from_millis(25);
    c1.leave_timeout = GoDuration::from_secs(1);
    let m1 = create_ml_with_cfg(c1, mk_opts(Some(key), true, true)).await;
    m1.join_members(&[format!("{a0}/{a0}")]).await.unwrap();
    assert!(wait_for(Duration::from_secs(2), || m0.members().len() == 2 && m1.members().len() == 2).await);

    m1.shutdown().await;
    m0.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/integ_test.go::TestMemberlist_Integ
#[tokio::test]
async fn test_memberlist_integ() {
    struct NoopEvents(AtomicUsize);
    impl EventDelegate for NoopEvents {
        fn notify_join(&self, _: &NodeState) { self.0.fetch_add(1, Ordering::Relaxed); }
        fn notify_leave(&self, _: &NodeState) {}
        fn notify_update(&self, _: &NodeState) {}
    }
    const NUM: usize = 16;
    let secret: Vec<u8> = (0u8..16).collect();
    let joins = Arc::new(NoopEvents(AtomicUsize::new(0)));
    let mut members = Vec::with_capacity(NUM);
    for i in 0..NUM {
        let mut c = cfg("127.0.0.1", &format!("integ-{i}"), 0);
        c.gossip_interval = GoDuration::from_millis(20);
        c.pull_push_interval = GoDuration::from_millis(200);
        let m = create_ml_with_cfg(
            c,
            KvOptions {
                secret_key: Some(secret.clone()),
                probe_interval: Some(Duration::from_millis(20)),
                probe_timeout: Some(Duration::from_millis(100)),
                event_delegate: (i == 0).then(|| joins.clone() as Arc<dyn EventDelegate>),
                ..Default::default()
            },
        )
        .await;
        if let Some(prev) = members.last() {
            let prev: &Arc<MemberlistKv> = prev;
            let n = m
                .join_members(&[format!("{}/127.0.0.1:{}", prev.node_name(), prev.listening_port())])
                .await
                .unwrap();
            assert!(n > 0);
        }
        members.push(m);
    }
    let converged = wait_for(Duration::from_secs(5), || {
        members.iter().all(|m| m.num_members() == NUM)
    })
    .await;
    for m in members {
        m.shutdown().await;
    }
    assert!(converged, "16-node encrypted cluster did not converge");
    assert!(joins.0.load(Ordering::Relaxed) >= NUM);
}
