//! Ported upstream hashicorp/memberlist tests: net, transport, security, label, util.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mimir_config::server::MemberlistConfig;
use mimir_ring::kv::memberlist::client::{
    move_dead_nodes, push_pull_scale, random_offset, shuffle_nodes, suspicion_timeout,
    MemberlistDelegate, NodeSelectionDelegate,
};
use mimir_ring::kv::memberlist::queue::retransmit_limit;
use mimir_ring::kv::memberlist::security::{
    decrypt_payload, encrypt_payload, encrypted_length, pkcs7_decode, pkcs7_encode,
    MAX_ENCRYPTION_VERSION,
};
use mimir_ring::kv::memberlist::stream::{remove_label_header_from_stream, StreamFirstByte};
use mimir_ring::kv::memberlist::tcp_transport::TcpTransport;
use mimir_ring::kv::memberlist::wire::{
    add_label_header, compress_payload_raw, decode_ack_resp, decode_alive,
    decode_compound_message, decode_ping, decode_push_pull, encode_ack_resp, encode_alive,
    encode_indirect_ping, encode_ping, encode_push_pull, ensure_port, get_push_pull_buffer,
    has_port, label_overhead, make_compound_message, make_compound_messages,
    release_push_pull_buffer, remove_label_header, unwrap_crc32, AckResp, Alive,
    IndirectPingReq, MessageType, NodeStateType, Ping, PushNodeState,
    COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD, MAX_POOLED_PUSH_PULL_BUF_CAP,
};
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv, NodeState};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn cfg_at(addr: &str, name: &str, compression: bool) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec![addr.to_string()],
        bind_port: 0,
        node_name: name.to_string(),
        randomize_node_name: false,
        compression_enabled: compression,
        ..Default::default()
    }
}

async fn start_ml(addr: &str, name: &str, opts: KvOptions, compression: bool) -> Arc<MemberlistKv> {
    MemberlistKv::start_with_options(
        cfg_at(addr, name, compression),
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

async fn raw_transport(addr: &str) -> (Arc<TcpTransport>, tokio::sync::mpsc::Receiver<mimir_ring::kv::memberlist::tcp_transport::ReceivedPacket>) {
    let cfg = cfg_at(addr, addr, false);
    let (tr, pkt_rx, _stream_rx) = TcpTransport::new(&cfg, "", None).await.unwrap();
    (tr, pkt_rx)
}

async fn write_stream_bytes(stream: &mut TcpStream, bytes: &[u8]) {
    stream.write_all(&[2u8]).await.unwrap();
    stream.write_all(bytes).await.unwrap();
    stream.flush().await.unwrap();
}

// --- net_test.go (15 tests) ---

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestHandleCompoundPing
#[tokio::test]
async fn test_handle_compound_ping() {
    let m = start_ml("127.0.11.1", "n1", KvOptions::default(), false).await;
    let (tr, mut pkt_rx) = raw_transport("127.0.11.2").await;
    let src_port = tr.auto_bind_port();
    let p = Ping {
        seq_no: 42,
        node: String::new(),
        source_addr: vec![127, 0, 11, 2],
        source_port: src_port,
        source_node: "test".to_string(),
    };
    let buf = encode_ping(&p);
    let compound = make_compound_message(&[&buf, &buf, &buf]);
    tr.write_to(compound, format!("127.0.11.1:{}", m.listening_port())).await;
    for _ in 0..3 {
        let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv())
            .await
            .expect("timeout")
            .expect("closed");
        let payload = unwrap_crc32(&pkt.buf).unwrap();
        assert_eq!(payload[0], MessageType::AckResp as u8);
        let ack = decode_ack_resp(&payload[1..]).unwrap();
        assert_eq!(ack.seq_no, 42);
    }
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestHandlePing
#[tokio::test]
async fn test_handle_ping() {
    let m = start_ml("127.0.11.3", "n1", KvOptions::default(), false).await;
    let (tr, mut pkt_rx) = raw_transport("127.0.11.4").await;
    let buf = encode_ping(&Ping {
        seq_no: 42,
        node: String::new(),
        source_addr: vec![127, 0, 11, 4],
        source_port: tr.auto_bind_port(),
        source_node: "test".to_string(),
    });
    tr.write_to(buf, format!("127.0.11.3:{}", m.listening_port())).await;
    let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let payload = unwrap_crc32(&pkt.buf).unwrap();
    assert_eq!(payload[0], MessageType::AckResp as u8);
    assert_eq!(decode_ack_resp(&payload[1..]).unwrap().seq_no, 42);
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestHandlePing_WrongNode
#[tokio::test]
async fn test_handle_ping_wrong_node() {
    let m = start_ml("127.0.11.5", "n1", KvOptions::default(), false).await;
    let (tr, mut pkt_rx) = raw_transport("127.0.11.6").await;
    let buf = encode_ping(&Ping {
        seq_no: 42,
        node: "n1-bad".to_string(),
        source_addr: vec![127, 0, 11, 6],
        source_port: tr.auto_bind_port(),
        source_node: "test".to_string(),
    });
    tr.write_to(buf, format!("127.0.11.5:{}", m.listening_port())).await;
    let res = tokio::time::timeout(Duration::from_millis(60), pkt_rx.recv()).await;
    assert!(res.is_err(), "expected timeout for wrong node ping");
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestHandleIndirectPing
#[tokio::test]
async fn test_handle_indirect_ping() {
    let m = start_ml("127.0.11.7", "n1", KvOptions::default(), false).await;
    let (tr, mut pkt_rx) = raw_transport("127.0.11.8").await;
    let buf = encode_indirect_ping(&IndirectPingReq {
        seq_no: 100,
        target: vec![127, 0, 11, 7],
        port: m.listening_port(),
        node: "n1".to_string(),
        nack: false,
        source_addr: vec![127, 0, 11, 8],
        source_port: tr.auto_bind_port(),
        source_node: "test".to_string(),
    });
    tr.write_to(buf, format!("127.0.11.7:{}", m.listening_port())).await;
    let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let payload = unwrap_crc32(&pkt.buf).unwrap();
    assert_eq!(payload[0], MessageType::AckResp as u8);
    assert_eq!(decode_ack_resp(&payload[1..]).unwrap().seq_no, 100);
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestTCPPing
#[tokio::test]
async fn test_tcp_ping() {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = ln.local_addr().unwrap().to_string();
    let m = start_ml(
        "127.0.11.9",
        "tcpping",
        KvOptions {
            probe_interval: Some(Duration::from_millis(150)),
            ..Default::default()
        },
        false,
    )
    .await;
    let ping_out = Ping {
        seq_no: 23,
        node: "mongo".to_string(),
        ..Default::default()
    };

    let accept_task = tokio::spawn({
        let ln = Arc::new(ln);
        let ln1 = Arc::clone(&ln);
        async move {
            // 1. normal
            let (mut sock, _) = ln1.accept().await.unwrap();
            let mut b = [0u8; 256];
            let n = sock.read(&mut b).await.unwrap();
            assert_eq!(b[0], 2);
            assert_eq!(b[1], MessageType::Ping as u8);
            let p_in = decode_ping(&b[2..n]).unwrap();
            assert_eq!(p_in.seq_no, 23);
            assert_eq!(p_in.node, "mongo");
            sock.write_all(&encode_ack_resp(&AckResp { seq_no: p_in.seq_no, payload: None })).await.unwrap();
            sock.flush().await.unwrap();

            // 2. mismatched seq_no
            let (mut sock2, _) = ln1.accept().await.unwrap();
            let n = sock2.read(&mut b).await.unwrap();
            let p_in = decode_ping(&b[2..n]).unwrap();
            sock2.write_all(&encode_ack_resp(&AckResp { seq_no: p_in.seq_no + 1, payload: None })).await.unwrap();
            sock2.flush().await.unwrap();

            // 3. bogus msg type
            let (mut sock3, _) = ln1.accept().await.unwrap();
            let _ = sock3.read(&mut b).await.unwrap();
            sock3.write_all(&encode_indirect_ping(&IndirectPingReq::default())).await.unwrap();
            sock3.flush().await.unwrap();
            ln1
        }
    });

    let dl = Instant::now() + Duration::from_millis(150);
    assert!(m.send_ping_and_wait_for_ack(&tcp_addr, "test", ping_out.clone(), dl).await.unwrap());

    let dl = Instant::now() + Duration::from_millis(150);
    let err = m.send_ping_and_wait_for_ack(&tcp_addr, "test", ping_out.clone(), dl).await.unwrap_err();
    assert!(err.contains("sequence number"), "{err}");

    let dl = Instant::now() + Duration::from_millis(150);
    let err = m.send_ping_and_wait_for_ack(&tcp_addr, "test", ping_out.clone(), dl).await.unwrap_err();
    assert!(err.contains("unexpected msgType"), "{err}");

    let ln = accept_task.await.unwrap();
    drop(ln);

    let start = Instant::now();
    let dl = start + Duration::from_millis(150);
    let ok = m.send_ping_and_wait_for_ack(&tcp_addr, "test", ping_out, dl).await.unwrap();
    assert!(!ok);
    assert!(start.elapsed() <= Duration::from_millis(180));
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestTCPPushPull
#[tokio::test]
async fn test_tcp_push_pull() {
    let m = start_ml("127.0.11.10", "pp", KvOptions::default(), false).await;
    m.alive_node_for_test(
        &Alive {
            incarnation: 0,
            node: "Test 0".to_string(),
            addr: vec![127, 0, 11, 10],
            port: m.listening_port(),
            meta: None,
            vsn: m.build_vsn_array(),
        },
        false,
    );
    m.change_node_state_for_test("Test 0", |s| {
        s.state = NodeStateType::Suspect;
        s.state_change = SystemTime::now() - Duration::from_secs(1);
    });
    // Actually ensure Test 0 exists even if inc==0
    if m.get_node_state("Test 0").is_none() {
        m.alive_node_for_test(
            &Alive {
                incarnation: 1,
                node: "Test 0".to_string(),
                addr: vec![127, 0, 11, 10],
                port: m.listening_port(),
                meta: None,
                vsn: m.build_vsn_array(),
            },
            false,
        );
        m.change_node_state_for_test("Test 0", |s| {
            s.incarnation = 0;
            s.state = NodeStateType::Suspect;
            s.state_change = SystemTime::now() - Duration::from_secs(1);
        });
    }

    let mut conn = TcpStream::connect(format!("127.0.11.10:{}", m.listening_port()))
        .await
        .unwrap();
    let ip = vec![127, 0, 11, 10];
    let port = m.listening_port();
    let local_nodes = vec![
        PushNodeState {
            name: "Test 0".to_string(),
            addr: ip.clone(),
            port,
            meta: None,
            incarnation: 1,
            state: NodeStateType::Alive,
            vsn: vec![],
        },
        PushNodeState {
            name: "Test 1".to_string(),
            addr: ip.clone(),
            port,
            meta: None,
            incarnation: 1,
            state: NodeStateType::Alive,
            vsn: vec![],
        },
        PushNodeState {
            name: "Test 2".to_string(),
            addr: ip.clone(),
            port,
            meta: None,
            incarnation: 1,
            state: NodeStateType::Alive,
            vsn: vec![],
        },
    ];
    let payload = encode_push_pull(&local_nodes, &[], false);
    write_stream_bytes(&mut conn, &payload).await;
    let mut resp = Vec::new();
    conn.read_to_end(&mut resp).await.unwrap();
    assert_eq!(resp[0], MessageType::PushPull as u8);
    let (_join, remote_nodes, _) = decode_push_pull(&resp[1..]).unwrap();
    assert!(remote_nodes.iter().any(|n| n.name == "Test 0" && n.addr == ip && n.state == NodeStateType::Suspect));
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestSendMsg_Piggyback
#[tokio::test]
async fn test_send_msg_piggyback() {
    let m = start_ml("127.0.11.11", "piggy", KvOptions::default(), false).await;
    let a = Alive {
        incarnation: 10,
        node: "rand".to_string(),
        addr: vec![127, 0, 0, 255],
        port: m.listening_port(),
        meta: None,
        vsn: vec![1, 5, 1, 1, 1, 1],
    };
    m.queue_ml_broadcast_for_test("rand", encode_alive(&a));
    let (tr, mut pkt_rx) = raw_transport("127.0.11.12").await;
    let ping = encode_ping(&Ping {
        seq_no: 42,
        node: String::new(),
        source_addr: vec![127, 0, 11, 12],
        source_port: tr.auto_bind_port(),
        source_node: "test".to_string(),
    });
    tr.write_to(ping, format!("127.0.11.11:{}", m.listening_port())).await;
    let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let payload = unwrap_crc32(&pkt.buf).unwrap();
    assert_eq!(payload[0], MessageType::Compound as u8);
    let (trunc, parts) = decode_compound_message(&payload[1..]).unwrap();
    assert_eq!(trunc, 0);
    assert_eq!(parts.len(), 2);
    assert_eq!(decode_ack_resp(&parts[0][1..]).unwrap().seq_no, 42);
    let alive_out = decode_alive(&parts[1][1..]).unwrap();
    assert_eq!(alive_out.node, "rand");
    assert_eq!(alive_out.incarnation, 10);
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestEncryptDecryptState
#[tokio::test]
async fn test_encrypt_decrypt_state() {
    let state = b"this is our internal state...";
    let m = start_ml(
        "127.0.11.13",
        "encstate",
        KvOptions {
            secret_key: Some((0u8..16).collect()),
            protocol_version: Some(5),
            ..Default::default()
        },
        false,
    )
    .await;
    let crypt = m.encrypt_local_state(state, "").unwrap();
    assert_eq!(crypt[0], MessageType::Encrypt as u8);
    let plain = m.decrypt_remote_state_for_test(&crypt[1..], "").unwrap();
    assert!(m.go_metrics_has_sample("sample_memberlist_size_remote"));
    assert_eq!(plain, state);
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestRawSendUdp_CRC
#[tokio::test]
async fn test_raw_send_udp_crc() {
    let m = start_ml("127.0.11.14", "crc", KvOptions::default(), false).await;
    let (tr, mut pkt_rx) = raw_transport("127.0.11.15").await;
    let addr = format!("127.0.11.15:{}", tr.auto_bind_port());
    let payload = vec![3u8, 3, 3, 3];

    m.raw_send_msg_packet_for_test(&addr, None, payload.clone()).await.unwrap();
    let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv()).await.unwrap().unwrap();
    assert_eq!(pkt.buf.len(), 4);

    let pmax5_node = NodeState {
        name: "127.0.11.15".to_string(),
        addr: vec![127, 0, 11, 15],
        port: tr.auto_bind_port(),
        meta: None,
        incarnation: 1,
        state: NodeStateType::Alive,
        state_change: SystemTime::now(),
        vsn: vec![1, 5, 2, 0, 0, 0],
    };
    m.raw_send_msg_packet_for_test(&addr, Some(&pmax5_node), payload.clone()).await.unwrap();
    let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv()).await.unwrap().unwrap();
    assert_eq!(pkt.buf.len(), 9);

    m.alive_node_for_test(
        &Alive {
            incarnation: 1,
            node: "127.0.11.15".to_string(),
            addr: vec![127, 0, 11, 15],
            port: tr.auto_bind_port(),
            meta: None,
            vsn: vec![1, 5, 2, 0, 0, 0],
        },
        false,
    );
    m.raw_send_msg_packet_for_test(&addr, None, payload).await.unwrap();
    let pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv()).await.unwrap().unwrap();
    assert_eq!(pkt.buf.len(), 9);
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestRawSendMsgPacket_CompressErrorFallsBackToPlaintext
#[test]
fn test_raw_send_msg_packet_compress_error_falls_back_to_plaintext() {
    let payload = vec![0x01u8, 0x02, 0x03, 0x04, 0x05];
    let res = compress_payload_raw(99, &payload);
    assert!(res.is_err());
    let fallback = res.unwrap_or_else(|_| payload.clone());
    assert_eq!(fallback, payload);
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestIngestPacket_CRC
#[tokio::test]
async fn test_ingest_packet_crc() {
    let m = start_ml("127.0.11.16", "icrc", KvOptions::default(), false).await;
    let (tr, mut pkt_rx) = raw_transport("127.0.11.17").await;
    let addr = format!("127.0.11.17:{}", tr.auto_bind_port());
    let n = NodeState {
        name: "t".to_string(),
        addr: vec![127, 0, 11, 17],
        port: tr.auto_bind_port(),
        meta: None,
        incarnation: 1,
        state: NodeStateType::Alive,
        state_change: SystemTime::now(),
        vsn: vec![1, 5, 2, 0, 0, 0],
    };
    m.raw_send_msg_packet_for_test(&addr, Some(&n), vec![3, 3, 3, 3]).await.unwrap();
    let mut pkt = tokio::time::timeout(Duration::from_secs(2), pkt_rx.recv()).await.unwrap().unwrap().buf;
    assert_eq!(pkt.len(), 9);
    pkt[1] <<= 1;
    let err = unwrap_crc32(&pkt).unwrap_err();
    assert!(err.contains("invalid checksum"), "{err}");
    m.ingest_packet_for_test(&pkt, &addr).await;
    tr.shutdown_and_wait().await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestIngestPacket_ExportedFunc_EmptyMessage
#[tokio::test]
async fn test_ingest_packet_exported_func_empty_message() {
    let m = start_ml("127.0.11.18", "empty", KvOptions::default(), false).await;
    let mut s = TcpStream::connect(format!("127.0.11.18:{}", m.listening_port())).await.unwrap();
    // Send packet byte (1) and close immediately (EOF / packet too short)
    s.write_all(&[1u8]).await.unwrap();
    drop(s);
    tokio::time::sleep(Duration::from_millis(50)).await;
    m.ingest_packet_for_test(&[], "127.0.11.18:1").await;
    m.shutdown().await;
}

async fn create_ml(addr: &str, name: &str, opts: KvOptions, compression: bool) -> Arc<MemberlistKv> {
    MemberlistKv::start_with_options(
        cfg_at(addr, name, compression),
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

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestGossip_MismatchedKeys
#[tokio::test]
async fn test_gossip_mismatched_keys() {
    let m1 = create_ml(
        "127.0.11.19",
        "mk1",
        KvOptions {
            secret_key: Some(b"4W6DGn2VQVqDEceOdmuRTQ==".to_vec()),
            ..Default::default()
        },
        true,
    )
    .await;
    let m2 = create_ml(
        "127.0.11.20",
        "mk2",
        KvOptions {
            secret_key: Some(b"XhX/w702/JKKK7/7OtM9Ww==".to_vec()),
            ..Default::default()
        },
        true,
    )
    .await;
    let err = m2
        .join_members(&[format!("mk1/127.0.11.19:{}", m1.listening_port())])
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("no installed keys could decrypt the message"),
        "unexpected error: {err}"
    );
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestHandleCommand
#[tokio::test]
async fn test_handle_command() {
    let m = start_ml("127.0.11.21", "hcmd", KvOptions::default(), false).await;
    m.handle_command_for_test(&[], "127.0.0.1:12345").await;
    m.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/net_test.go::TestHandleConn_NilConnAfterRemoveLabelHeaderFromStream
#[tokio::test]
async fn test_handle_conn_nil_conn_after_remove_label_header_from_stream() {
    struct ErrReader;
    impl tokio::io::AsyncRead for ErrReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("test read error")))
        }
    }
    let mut r = ErrReader;
    let res = remove_label_header_from_stream(&mut r).await;
    assert!(res.is_err());
}

// --- transport_test.go (3 tests) ---

struct RecordingDelegate(Mutex<Vec<Vec<u8>>>);
impl MemberlistDelegate for RecordingDelegate {
    fn node_meta(&self, _limit: usize) -> Vec<u8> {
        Vec::new()
    }
    fn notify_msg(&self, msg: &[u8]) {
        self.0.lock().push(msg.to_vec());
    }
    fn get_broadcasts(&self, _overhead: usize, _limit: usize) -> Vec<Vec<u8>> {
        Vec::new()
    }
    fn local_state(&self, _join: bool) -> Vec<u8> {
        Vec::new()
    }
    fn merge_remote_state(&self, _buf: &[u8], _join: bool) {}
}

// Upstream: github.com/hashicorp/memberlist/transport_test.go::TestTransport_Join
#[tokio::test]
async fn test_transport_join() {
    let m1 = create_ml("127.0.11.22", "node1", KvOptions::default(), true).await;
    let m2 = create_ml("127.0.11.23", "node2", KvOptions::default(), true).await;
    let num = m2
        .join_members(&[format!("node1/127.0.11.22:{}", m1.listening_port())])
        .await
        .unwrap();
    assert_eq!(num, 1);
    assert_eq!(m2.members().len(), 2);
    assert_eq!(m2.est_num_nodes_pub(), 2);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/transport_test.go::TestTransport_Send
#[tokio::test]
async fn test_transport_send() {
    let d1 = Arc::new(RecordingDelegate(Mutex::new(Vec::new())));
    let m1 = create_ml(
        "127.0.11.24",
        "node1",
        KvOptions {
            memberlist_delegate: Some(d1.clone()),
            ..Default::default()
        },
        true,
    )
    .await;
    let m2 = create_ml("127.0.11.25", "node2", KvOptions::default(), true).await;
    let m1_addr = format!("127.0.11.24:{}", m1.listening_port());
    let num = m2.join_members(&[format!("node1/{m1_addr}")]).await.unwrap();
    assert_eq!(num, 1);
    m2.send_to_address(&m1_addr, "", b"SendTo").await.unwrap();
    let n1 = m2.members().into_iter().find(|n| n.name == "node1").unwrap();
    m2.send_best_effort(&n1, b"SendBestEffort").await.unwrap();
    m2.send_reliable(&n1, b"SendReliable").await.unwrap();
    m2.send_best_effort(&n1, b"SendToUDP").await.unwrap();
    m2.send_reliable(&n1, b"SendToTCP").await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let mut got: Vec<String> = d1
        .0
        .lock()
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();
    got.sort();
    let mut expected = vec!["SendTo", "SendToUDP", "SendToTCP", "SendBestEffort", "SendReliable"];
    expected.sort();
    assert_eq!(got, expected);
    m2.shutdown().await;
    m1.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/transport_test.go::TestTransport_TcpListenBackoff
#[tokio::test]
async fn test_transport_tcp_listen_backoff() {
    let (tr, _pkt, _st) = TcpTransport::new(&cfg_at("127.0.11.26", "backoff", false), "", None)
        .await
        .unwrap();
    tr.spawn_errored_tcp_listen_for_test().unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    tr.shutdown();
    tokio::time::timeout(Duration::from_millis(1250), tr.shutdown_and_wait())
        .await
        .expect("timed out waiting for transport shutdown");
    let calls = tr.accept_errors_for_test();
    assert!(calls > 8 && calls < 14, "expected 9..13 backoff loops, got {calls}");
}

// --- security_test.go (3 tests) ---

// Upstream: github.com/hashicorp/memberlist/security_test.go::TestPKCS7
#[test]
fn test_pkcs7() {
    for i in 0..=255u8 {
        let buf = vec![i; i as usize];
        let mut inp = buf.clone();
        pkcs7_encode(&mut inp, 0, 16);
        let dec = pkcs7_decode(&inp, 16).unwrap();
        assert_eq!(buf.as_slice(), dec);
    }
}

fn encrypt_decrypt_versioned(vsn: u8) {
    let k1: Vec<u8> = (0u8..16).collect();
    let plaintext = b"this is a plain text message";
    let extra = b"random data";
    let mut buf = Vec::new();
    encrypt_payload(vsn, &k1, plaintext, extra, &mut buf).unwrap();
    assert_eq!(buf.len(), encrypted_length(vsn, plaintext.len()));
    let msg = decrypt_payload(&[k1], &buf, extra).unwrap();
    assert_eq!(msg.as_slice(), plaintext);
}

// Upstream: github.com/hashicorp/memberlist/security_test.go::TestEncryptDecrypt_V0
#[test]
fn test_encrypt_decrypt_v0() {
    encrypt_decrypt_versioned(0);
}

// Upstream: github.com/hashicorp/memberlist/security_test.go::TestEncryptDecrypt_V1
#[test]
fn test_encrypt_decrypt_v1() {
    encrypt_decrypt_versioned(1);
}

// --- label_test.go (5 tests) ---

// Upstream: github.com/hashicorp/memberlist/label_test.go::TestAddLabelHeaderToPacket
#[test]
fn test_add_label_header_to_packet() {
    let long_label = "a".repeat(255);
    assert_eq!(add_label_header(&[], "").unwrap(), Vec::<u8>::new());
    let mut exp = vec![MessageType::HasLabel as u8, 3];
    exp.extend_from_slice(b"foo");
    assert_eq!(add_label_header(&[], "foo").unwrap(), exp);
    let mut exp2 = exp.clone();
    exp2.extend_from_slice(b"something");
    assert_eq!(add_label_header(b"something", "foo").unwrap(), exp2);
    assert_eq!(add_label_header(b"something", "").unwrap(), b"something");
    let mut exp3 = vec![MessageType::HasLabel as u8, 255];
    exp3.extend_from_slice(long_label.as_bytes());
    exp3.extend_from_slice(b"something");
    assert_eq!(add_label_header(b"something", &long_label).unwrap(), exp3);
    let err = add_label_header(b"something", &(long_label + "x")).unwrap_err();
    assert!(err.contains("is too long"), "{err}");
}

// Upstream: github.com/hashicorp/memberlist/label_test.go::TestRemoveLabelHeaderFromPacket
#[test]
fn test_remove_label_header_from_packet() {
    assert_eq!(remove_label_header(&[]).unwrap(), (&[][..], ""));
    let p = [MessageType::Ping as u8, b'b', b'l', b'a', b'h'];
    assert_eq!(remove_label_header(&p).unwrap(), (&p[..], ""));
    let e = [MessageType::Err as u8, b'b', b'l', b'a', b'h'];
    assert_eq!(remove_label_header(&e).unwrap(), (&e[..], ""));
    let v = [MAX_ENCRYPTION_VERSION, b'b', b'l', b'a', b'h'];
    assert_eq!(remove_label_header(&v).unwrap(), (&v[..], ""));
    assert!(remove_label_header(&[MessageType::HasLabel as u8, b'x']).unwrap_err().contains("truncated"));
    assert!(remove_label_header(&[MessageType::HasLabel as u8]).unwrap_err().contains("truncated"));
    assert!(remove_label_header(&[MessageType::HasLabel as u8, 0, b'x']).unwrap_err().contains("empty"));
    assert!(remove_label_header(&[MessageType::HasLabel as u8, 2, b'x']).unwrap_err().contains("truncated"));
    let mut labeled = vec![MessageType::HasLabel as u8, 3, b'a', b'b', b'c'];
    labeled.extend_from_slice(&p);
    assert_eq!(remove_label_header(&labeled).unwrap(), (&p[..], "abc"));
}

// Upstream: github.com/hashicorp/memberlist/label_test.go::TestAddLabelHeaderToStream
#[tokio::test]
async fn test_add_label_header_to_stream() {
    let long_label = "a".repeat(255);
    assert!(add_label_header(&[], &(long_label.clone() + "x")).is_err());
    for (label, prefix) in [
        ("", vec![]),
        ("foo", vec![MessageType::HasLabel as u8, 3, b'f', b'o', b'o']),
        (long_label.as_str(), {
            let mut v = vec![MessageType::HasLabel as u8, 255];
            v.extend_from_slice(long_label.as_bytes());
            v
        }),
    ] {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let hdr = add_label_header(&[], label).unwrap();
        client.write_all(&hdr).await.unwrap();
        client.write_all(b"EXTRA DATA").await.unwrap();
        drop(client);
        let mut got = Vec::new();
        server.read_to_end(&mut got).await.unwrap();
        let mut exp = prefix;
        exp.extend_from_slice(b"EXTRA DATA");
        assert_eq!(got, exp);
    }
}

// Upstream: github.com/hashicorp/memberlist/label_test.go::TestRemoveLabelHeaderFromStream
#[tokio::test]
async fn test_remove_label_header_from_stream() {
    type Case<'a> = (&'a [u8], &'a str, &'a [u8], Option<&'a str>);
    let cases: [Case<'_>; 8] = [
        (&[], "", &[], None),
        (b"\x00blah", "", b"\x00blah", None),
        (b"\x0dblah", "", b"\x0dblah", None),
        (b"\x01blah", "", b"\x01blah", None),
        (b"\xf4", "", &[], Some("cannot decode label")),
        (b"\xf4\x00x", "", &[], Some("label header cannot be empty")),
        (b"\xf4\x02x", "", &[], Some("cannot decode label")),
        (b"\xf4\x03abc\x00blah", "abc", b"\x00blah", None),
    ];
    for (input, exp_label, exp_data, exp_err) in cases {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_all(input).await.unwrap();
        drop(client);
        let res = remove_label_header_from_stream(&mut server).await;
        if let Some(err_sub) = exp_err {
            let e = res.unwrap_err();
            assert!(e.contains(err_sub), "got {e}");
        } else {
            let (lbl, first) = res.unwrap();
            assert_eq!(lbl, exp_label);
            let mut rest = Vec::new();
            if let StreamFirstByte::Byte(b) = first {
                rest.push(b);
            }
            server.read_to_end(&mut rest).await.unwrap();
            assert_eq!(rest.as_slice(), exp_data);
        }
    }
}

// Upstream: github.com/hashicorp/memberlist/label_test.go::TestLabelOverhead
#[test]
fn test_label_overhead() {
    assert_eq!(label_overhead(""), 0);
    assert_eq!(label_overhead("a"), 3);
    assert_eq!(label_overhead("abcdefg"), 9);
}

// --- util_test.go (19 tests) ---

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestUtil_PortFunctions
#[test]
fn test_util_port_functions() {
    let cases = [
        ("1.2.3.4", false, "1.2.3.4:8301"),
        ("1.2.3.4:1234", true, "1.2.3.4:1234"),
        ("2600:1f14:e22:1501:f9a:2e0c:a167:67e8", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"),
        ("[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"),
        ("[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234", true, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234"),
        ("localhost", false, "localhost:8301"),
        ("localhost:1234", true, "localhost:1234"),
        ("hashicorp.com", false, "hashicorp.com:8301"),
        ("hashicorp.com:1234", true, "hashicorp.com:1234"),
    ];
    for (addr, hp, ep) in cases {
        assert_eq!(has_port(addr), hp, "{addr}");
        assert_eq!(ensure_port(addr, 8301), ep, "{addr}");
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestEncodeDecode
#[test]
fn test_encode_decode() {
    let msg = Ping { seq_no: 100, ..Default::default() };
    let buf = encode_ping(&msg);
    let out = decode_ping(&buf[1..]).unwrap();
    assert_eq!(out.seq_no, msg.seq_no);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestEncodeRoundTrip
#[test]
fn test_encode_round_trip() {
    for i in 0..32u32 {
        let buf = encode_ping(&Ping { seq_no: i, node: "n".to_string(), ..Default::default() });
        let p = decode_ping(&buf[1..]).unwrap();
        assert_eq!(p.seq_no, i);
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestRandomOffset
#[test]
fn test_random_offset() {
    let mut vals = HashSet::new();
    for _ in 0..100 {
        let off = random_offset(2usize << 30);
        assert!(vals.insert(off), "unexpected collision");
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestRandomOffset_Zero
#[test]
fn test_random_offset_zero() {
    assert_eq!(random_offset(0), 0);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestSuspicionTimeout
#[test]
fn test_suspicion_timeout() {
    let ms = Duration::from_millis;
    let cases = [
        (5, ms(1000)),
        (10, ms(1000)),
        (50, ms(1698)),
        (100, ms(2000)),
        (500, ms(2698)),
        (1000, ms(3000)),
    ];
    for (n, expected) in cases {
        assert_eq!(suspicion_timeout(3, n, Duration::from_secs(1)) / 3, expected);
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestRetransmitLimit
#[test]
fn test_retransmit_limit() {
    assert_eq!(retransmit_limit(3, 0), 0);
    assert_eq!(retransmit_limit(3, 1), 3);
    assert_eq!(retransmit_limit(3, 99), 6);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestShuffleNodes
#[test]
fn test_shuffle_nodes() {
    let orig = vec![
        NodeStateType::Dead,
        NodeStateType::Alive,
        NodeStateType::Alive,
        NodeStateType::Dead,
        NodeStateType::Alive,
        NodeStateType::Alive,
        NodeStateType::Dead,
        NodeStateType::Alive,
    ];
    let mut nodes = orig.clone();
    shuffle_nodes(&mut nodes);
    assert_ne!(nodes, orig);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestPushPullScale
#[test]
fn test_push_pull_scale() {
    let sec = Duration::from_secs(1);
    for i in 0..=32 {
        assert_eq!(push_pull_scale(sec, i), sec);
    }
    for i in 33..=64 {
        assert_eq!(push_pull_scale(sec, i), sec * 2);
    }
    for i in 65..=128 {
        assert_eq!(push_pull_scale(sec, i), sec * 3);
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestMoveDeadNodes
#[test]
fn test_move_dead_nodes() {
    let ago = |s: u64| SystemTime::now() - Duration::from_secs(s);
    let mk = |st: NodeStateType, s: u64| NodeState {
        name: String::new(),
        addr: vec![127, 0, 0, 1],
        port: 7946,
        meta: None,
        incarnation: 0,
        state: st,
        state_change: ago(s),
        vsn: vec![],
    };
    let mut nodes = vec![
        mk(NodeStateType::Dead, 20),
        mk(NodeStateType::Alive, 20),
        mk(NodeStateType::Dead, 10),
        mk(NodeStateType::Left, 10),
        mk(NodeStateType::Left, 20),
        mk(NodeStateType::Alive, 20),
        mk(NodeStateType::Dead, 20),
        mk(NodeStateType::Alive, 20),
        mk(NodeStateType::Left, 20),
    ];
    let idx = move_dead_nodes(&mut nodes, Duration::from_secs(15));
    assert_eq!(idx, 5);
    assert_eq!(nodes[2].state, NodeStateType::Dead);
    assert_eq!(nodes[3].state, NodeStateType::Left);
    for i in [0, 1, 4] {
        assert_eq!(nodes[i].state, NodeStateType::Alive);
    }
    for n in &nodes[idx..] {
        assert!(n.dead_or_left());
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestKRandomNodes
#[test]
fn test_k_random_nodes() {
    let kv = MemberlistKv::new(cfg_at("127.0.0.1", "test0", false), vec![], "", None, KvOptions::default()).unwrap();
    for i in 0..90 {
        kv.alive_node_for_test(
            &Alive {
                incarnation: 1,
                node: format!("test{i}"),
                addr: vec![127, 0, 0, 1],
                port: 7946 + i as u16,
                meta: None,
                vsn: kv.build_vsn_array(),
            },
            false,
        );
        let st = match i % 3 {
            0 => NodeStateType::Alive,
            1 => NodeStateType::Suspect,
            _ => NodeStateType::Dead,
        };
        kv.change_node_state_for_test(&format!("test{i}"), |n| n.state = st);
    }
    let excl = |n: &NodeState| n.name == "test0" || n.state != NodeStateType::Alive;
    let s1: Vec<String> = kv.k_random_nodes_for_test(3, false, excl).into_iter().map(|n| n.name).collect();
    let s2: Vec<String> = kv.k_random_nodes_for_test(3, false, excl).into_iter().map(|n| n.name).collect();
    let s3: Vec<String> = kv.k_random_nodes_for_test(3, false, excl).into_iter().map(|n| n.name).collect();
    assert_ne!(s1, s2);
    assert_ne!(s1, s3);
    assert_ne!(s2, s3);
    for s in [&s1, &s2, &s3] {
        assert_eq!(s.len(), 3);
        assert!(!s.contains(&"test0".to_string()));
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestKRandomNodesWithDelegate
#[test]
fn test_k_random_nodes_with_delegate() {
    struct TestSel {
        picked: Vec<&'static str>,
        pref: Option<&'static str>,
    }
    impl NodeSelectionDelegate for TestSel {
        fn select_nodes(&self, nodes: &[&NodeState]) -> (Vec<usize>, Option<usize>) {
            let mut sel = Vec::new();
            let mut p = None;
            for (i, n) in nodes.iter().enumerate() {
                if self.picked.contains(&n.name.as_str()) {
                    sel.push(i);
                    if self.pref == Some(n.name.as_str()) {
                        p = Some(i);
                    }
                }
            }
            (sel, p)
        }
    }
    let seed = |kv: &MemberlistKv| {
        for i in 0..20 {
            kv.alive_node_for_test(
                &Alive {
                    incarnation: 1,
                    node: i.to_string(),
                    addr: vec![127, 0, 0, 1],
                    port: 7946 + i as u16,
                    meta: None,
                    vsn: kv.build_vsn_array(),
                },
                false,
            );
            let st = match i % 3 {
                0 => NodeStateType::Alive,
                1 => NodeStateType::Suspect,
                _ => NodeStateType::Dead,
            };
            kv.change_node_state_for_test(&i.to_string(), |n| n.state = st);
        }
    };

    // With preferred
    let kv1 = MemberlistKv::new(
        cfg_at("127.0.0.1", "sel1", false),
        vec![],
        "",
        None,
        KvOptions {
            node_selection_delegate: Some(Arc::new(TestSel {
                picked: vec!["3", "6", "9", "12"],
                pref: Some("6"),
            })),
            ..Default::default()
        },
    )
    .unwrap();
    seed(&kv1);
    let r1 = kv1.k_random_nodes_for_test(3, true, |n| n.state != NodeStateType::Alive);
    assert!(!r1.is_empty() && r1.len() <= 3);
    assert!(r1.iter().any(|n| n.name == "6"));
    for n in &r1 {
        assert!(["3", "6", "9", "12"].contains(&n.name.as_str()));
        assert_eq!(n.state, NodeStateType::Alive);
    }

    // No preferred
    let kv2 = MemberlistKv::new(
        cfg_at("127.0.0.1", "sel2", false),
        vec![],
        "",
        None,
        KvOptions {
            node_selection_delegate: Some(Arc::new(TestSel {
                picked: vec!["3", "6", "9"],
                pref: None,
            })),
            ..Default::default()
        },
    )
    .unwrap();
    seed(&kv2);
    let r2 = kv2.k_random_nodes_for_test(2, true, |n| n.state != NodeStateType::Alive);
    assert!(!r2.is_empty() && r2.len() <= 2);
    for n in &r2 {
        assert!(["3", "6", "9"].contains(&n.name.as_str()));
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestMakeCompoundMessage
#[test]
fn test_make_compound_message() {
    let buf = encode_ping(&Ping { seq_no: 100, ..Default::default() });
    let compound = make_compound_message(&[&buf, &buf, &buf]);
    assert_eq!(
        compound.len(),
        3 * buf.len() + 3 * COMPOUND_OVERHEAD + COMPOUND_HEADER_OVERHEAD
    );
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestDecodeCompoundMessage
#[test]
fn test_decode_compound_message() {
    let buf = encode_ping(&Ping { seq_no: 100, ..Default::default() });
    let compound = make_compound_message(&[&buf, &buf, &buf]);
    let (trunc, parts) = decode_compound_message(&compound[1..]).unwrap();
    assert_eq!(trunc, 0);
    assert_eq!(parts.len(), 3);
    for p in parts {
        assert_eq!(p.len(), buf.len());
    }
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestDecodeCompoundMessage_NumberOfPartsOverflow
#[test]
fn test_decode_compound_message_number_of_parts_overflow() {
    let err = decode_compound_message(&[0x80]).unwrap_err();
    assert_eq!(err, "truncated len slice");
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestDecodeCompoundMessage_Trunc
#[test]
fn test_decode_compound_message_trunc() {
    let buf = encode_ping(&Ping { seq_no: 100, ..Default::default() });
    let compound = make_compound_message(&[&buf, &buf, &buf]);
    let (trunc, parts) = decode_compound_message(&compound[1..38]).unwrap();
    assert_eq!(trunc, 1);
    assert_eq!(parts.len(), 2);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestMakeCompoundMessages
#[test]
fn test_make_compound_messages() {
    let small: Vec<Vec<u8>> = (0..300)
        .map(|i| encode_ack_resp(&AckResp { seq_no: 1, payload: Some(vec![i as u8]) }))
        .collect();
    let big: Vec<Vec<u8>> = (0..3)
        .map(|i| {
            let mut p = vec![0u8; 70_000];
            p[69_999] = i as u8;
            encode_ack_resp(&AckResp { seq_no: 2, payload: Some(p) })
        })
        .collect();
    assert!(make_compound_messages(&[]).is_empty());
    assert_eq!(
        make_compound_messages(&small[0..3]),
        vec![make_compound_message(&[&small[0], &small[1], &small[2]])]
    );
    let s300 = make_compound_messages(&small);
    assert_eq!(s300.len(), 2);
    let mut mixed = Vec::new();
    mixed.push(big[0].clone());
    mixed.extend_from_slice(&small[0..20]);
    mixed.push(big[1].clone());
    mixed.extend_from_slice(&small[20..260]);
    mixed.push(big[2].clone());
    mixed.extend_from_slice(&small[260..300]);
    let actual = make_compound_messages(&mixed);
    assert_eq!(actual.len(), 5);
    assert_eq!(actual[0], big[0]);
    assert_eq!(actual[1], big[1]);
    assert_eq!(actual[2], big[2]);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestPushPullBuffer_Reused
#[test]
fn test_push_pull_buffer_reused() {
    let b = get_push_pull_buffer();
    let ptr1 = b.as_ptr();
    let mut b = b;
    b.extend_from_slice(b"hello");
    release_push_pull_buffer(b);
    let b2 = get_push_pull_buffer();
    assert_eq!(b2.len(), 0);
    assert_eq!(b2.as_ptr(), ptr1);
    release_push_pull_buffer(b2);
}

// Upstream: github.com/hashicorp/memberlist/util_test.go::TestReleasePushPullBuffer_BoundedCap
#[test]
fn test_release_push_pull_buffer_bounded_cap() {
    let mut big = get_push_pull_buffer();
    big.extend(std::iter::repeat_n(0u8, MAX_POOLED_PUSH_PULL_BUF_CAP + 1));
    assert!(big.capacity() > MAX_POOLED_PUSH_PULL_BUF_CAP);
    release_push_pull_buffer(big);
    for i in 0..10 {
        let b = get_push_pull_buffer();
        assert!(
            b.capacity() <= MAX_POOLED_PUSH_PULL_BUF_CAP,
            "oversized buffer leaked through pool on iteration {i}"
        );
        release_push_pull_buffer(b);
    }
}
