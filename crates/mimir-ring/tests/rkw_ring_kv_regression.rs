//! Regression tests for Wave 15 Track 4 (RKW-04, RKW-05, RKW-06, RKW-07).

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use mimir_common::services::{Service, ServiceContext};
use mimir_config::ingester::EtcdConfig;
use mimir_config::server::MemberlistConfig;
use mimir_config::{GoDuration, RingConfig};
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::etcd::EtcdKv;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::memberlist::client::{MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::memberlist::tcp_transport::TcpTransport;
use mimir_ring::kv::memberlist::wire::{make_compound_message, MessageType};
use mimir_ring::kv::{BoxFuture, CasFn, CasOutcome, KvClient, KvError, StringCodec};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig, NoopFlushTransferer};
use mimir_ring::ring::model::ring_desc_codec;
use md5::{Digest, Md5};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const RING_KEY: &str = "ring";
const INSTANCE_ID: &str = "rkw-ing-1";

fn test_lifecycler_config(id: &str) -> LifecyclerConfig {
    let ring_cfg = RingConfig {
        instance_id: id.to_string(),
        instance_addr: "127.0.0.1".to_string(),
        instance_port: 9095,
        num_tokens: 2,
        instance_availability_zone: "zone-a".to_string(),
        final_sleep: GoDuration::ZERO,
        heartbeat_period: GoDuration::from_millis(80),
        observe_period: GoDuration::ZERO,
        min_ready_duration: GoDuration::ZERO,
        ..Default::default()
    };
    LifecyclerConfig::from_ring_config(&ring_cfg, 0).unwrap()
}

async fn get_inst(kv: &dyn KvClient, id: &str) -> Option<InstanceDesc> {
    let v = kv.get(RING_KEY).await.unwrap()?;
    v.downcast_ref::<Desc>()?.ingesters.get(id).cloned()
}

// ============================================================================
// RKW-04: Lifecycler registered_at persistence across 2+ heartbeats & init_ring CAS retry
// ============================================================================

#[tokio::test]
async fn rkw04_lifecycler_persists_registered_at_across_multiple_heartbeats_after_kv_reset() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let cfg = test_lifecycler_config(INSTANCE_ID);
    let lc = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.clone().into_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    // Wait until initial ACTIVE registration.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(inst) = get_inst(kv.as_ref(), INSTANCE_ID).await {
            if inst.state == InstanceState::Active as i32 && inst.tokens.len() == 2 {
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for ACTIVE");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = get_inst(kv.as_ref(), INSTANCE_ID).await.unwrap();

    // Timestamp resolution is 1s: sleep >1s so the re-registration timestamp strictly advances.
    tokio::time::sleep(Duration::from_millis(1150)).await;

    // Reset ring KV (forgetting instance).
    kv.cas(
        RING_KEY,
        Box::new(|_| Ok(CasOutcome::Update(Box::new(Desc::default())))),
    )
    .await
    .unwrap();

    // Wait for heartbeat 1 to re-add the instance.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let after_hb1 = loop {
        if let Some(inst) = get_inst(kv.as_ref(), INSTANCE_ID).await {
            break inst;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for hb1");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        after_hb1.registered_timestamp > before.registered_timestamp,
        "heartbeat 1 must bump registered_timestamp: before={}, hb1={}",
        before.registered_timestamp,
        after_hb1.registered_timestamp
    );

    // Wait across 3+ subsequent heartbeats (heartbeat_period = 80ms):
    // registered_timestamp MUST NOT revert back to `before.registered_timestamp`.
    tokio::time::sleep(Duration::from_millis(350)).await;
    let after_hb_n = get_inst(kv.as_ref(), INSTANCE_ID).await.unwrap();
    assert_eq!(
        after_hb_n.registered_timestamp, after_hb1.registered_timestamp,
        "subsequent heartbeats must preserve new registered_timestamp (got {}, expected {})",
        after_hb_n.registered_timestamp, after_hb1.registered_timestamp
    );

    svc.stop_async();
    svc.await_terminated().await.unwrap();
}

#[tokio::test]
async fn rkw04_init_ring_does_not_leak_tokens_across_cas_retries() {
    struct ConflictOnceKv {
        attempts: AtomicUsize,
    }
    impl KvClient for ConflictOnceKv {
        fn list<'a>(&'a self, _prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn get<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
            Box::pin(async { Ok(None) })
        }
        fn delete<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
            Box::pin(async { Ok(()) })
        }
        fn cas<'a>(&'a self, _key: &'a str, mut f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
            Box::pin(async move {
                let mut desc1 = Desc::default();
                desc1.ingesters.insert(
                    INSTANCE_ID.to_string(),
                    InstanceDesc {
                        id: INSTANCE_ID.to_string(),
                        addr: "127.0.0.1:9095".to_string(),
                        zone: "zone-a".to_string(),
                        state: InstanceState::Active as i32,
                        tokens: vec![111, 222],
                        ..Default::default()
                    },
                );
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    // Attempt 1 sees ACTIVE with stale tokens [111, 222], then retry triggers.
                    let _ = f(Some(&desc1));
                    // Attempt 2 sees JOINING (which resets to PENDING without setting tokens).
                    let mut desc2 = Desc::default();
                    desc2.ingesters.insert(
                        INSTANCE_ID.to_string(),
                        InstanceDesc {
                            id: INSTANCE_ID.to_string(),
                            addr: "127.0.0.1:9095".to_string(),
                            zone: "zone-a".to_string(),
                            state: InstanceState::Joining as i32,
                            tokens: vec![999],
                            ..Default::default()
                        },
                    );
                    let _ = f(Some(&desc2));
                } else {
                    let _ = f(None);
                }
                Ok(())
            })
        }
        fn watch_key<'a>(
            &'a self,
            _key: &'a str,
            _cancel: ServiceContext,
            _f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
        fn watch_prefix<'a>(
            &'a self,
            _prefix: &'a str,
            _cancel: ServiceContext,
            _f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    let kv: Arc<dyn KvClient> = Arc::new(ConflictOnceKv {
        attempts: AtomicUsize::new(0),
    });
    let mut cfg = test_lifecycler_config(INSTANCE_ID);
    cfg.join_after = Duration::from_secs(60);
    let lc = Lifecycler::with_store(
        cfg,
        kv,
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        false,
        None,
    );
    let svc = lc.clone().into_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    assert_eq!(lc.get_state(), InstanceState::Pending);
    assert!(
        lc.get_tokens().is_empty(),
        "tokens from aborted CAS attempt 1 must NOT leak into winning JOINING->PENDING attempt: {:?}",
        lc.get_tokens()
    );

    svc.stop_async();
    svc.await_terminated().await.unwrap();
}

// ============================================================================
// RKW-05: MemberlistKv Tombstone Merge & Go 1:1 UpdateTime/Deleted Parity
// ============================================================================

#[tokio::test]
async fn rkw05_memberlist_tombstone_cannot_be_resurrected_and_matches_go_get_list() {
    let codec = ring_desc_codec();
    let cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: "rkw05-node".to_string(),
        randomize_node_name: false,
        obsolete_entries_timeout: GoDuration::from_secs(60),
        message_history_buffer_bytes: 64 * 1024,
        ..Default::default()
    };
    let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "test", None)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    // 1. Create a ring entry via CAS -> update_time stays None (0 on wire), deleted = false.
    client
        .cas(
            "ring",
            Box::new(|_| {
                let mut d = Desc::default();
                d.ingesters.insert(
                    "ing-a".to_string(),
                    InstanceDesc {
                        id: "ing-a".to_string(),
                        timestamp: 100,
                        state: InstanceState::Active as i32,
                        tokens: vec![1],
                        ..Default::default()
                    },
                );
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();

    let (sent_msgs, _) = kv.sent_and_received_messages();
    assert_eq!(sent_msgs.last().unwrap().pair.update_time_millis, 0);
    assert!(!sent_msgs.last().unwrap().pair.deleted);

    // 2. Delete the key -> deleted = true, update_time != 0.
    client.delete("ring").await.unwrap();
    assert!(
        kv.list_keys("").contains(&"ring".to_string()),
        "dskit List returns tombstoned keys until ObsoleteEntriesTimeout"
    );
    assert!(
        kv.get_value("ring").is_some(),
        "dskit get returns tombstoned value until ObsoleteEntriesTimeout"
    );

    let t_del = kv.store_copy().get("ring").unwrap().update_time.unwrap();

    // 3. Simulate a concurrent normal CAS update from a peer with +500ms clock skew
    //    (deleted = false, update_time > t_del): must NOT flip deleted back to false!
    let mut peer_desc = Desc::default();
    peer_desc.ingesters.insert(
        "ing-b".to_string(),
        InstanceDesc {
            id: "ing-b".to_string(),
            timestamp: 200,
            state: InstanceState::Active as i32,
            tokens: vec![2],
            ..Default::default()
        },
    );
    let pair = KeyValuePair {
        key: "ring".to_string(),
        value: codec.encode(&peer_desc).unwrap().into(),
        codec: codec.codec_id().to_string(),
        deleted: false,
        update_time_millis: (t_del + Duration::from_millis(500))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    };
    let ser = pair.encode_to_vec();
    let mut framed = Vec::with_capacity(4 + ser.len());
    framed.extend_from_slice(&(ser.len() as u32).to_be_bytes());
    framed.extend_from_slice(&ser);
    kv.merge_remote_state(&framed, false);

    let stored = kv.store_copy().get("ring").unwrap().clone();
    assert!(stored.deleted, "deleted tombstone MUST NOT be resurrected by deleted=false update");
    assert_eq!(stored.update_time, Some(t_del));

    // 4. Local CAS on tombstoned key also keeps deleted=true until obsolete_entries_timeout.
    client
        .cas(
            "ring",
            Box::new(|_| {
                let mut d = Desc::default();
                d.ingesters.insert(
                    "ing-c".to_string(),
                    InstanceDesc {
                        id: "ing-c".to_string(),
                        timestamp: 300,
                        state: InstanceState::Active as i32,
                        tokens: vec![3],
                        ..Default::default()
                    },
                );
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();
    let stored2 = kv.store_copy().get("ring").unwrap().clone();
    assert!(stored2.deleted);
    assert_eq!(stored2.update_time, Some(t_del));

    kv.shutdown().await;
}

// ============================================================================
// RKW-06: Nested Compound Packet Depth Cap & TcpTransport Half-Open Timeout
// ============================================================================

#[tokio::test]
async fn rkw06_deeply_nested_compound_packet_does_not_stack_overflow() {
    let cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: "depth-node".to_string(),
        randomize_node_name: false,
        cluster_label_verification_disabled: true,
        ..Default::default()
    };
    let kv = MemberlistKv::start(cfg, vec![ring_desc_codec()], "test", None)
        .await
        .unwrap();
    let port = kv.listening_port();

    // Build 5,000 nested Compound envelopes around a minimal User byte.
    let mut payload = vec![MessageType::User as u8, b'x'];
    for _ in 0..5_000 {
        payload = make_compound_message(&[&payload]);
    }
    let digest = Md5::digest(&payload);
    let our_addr = "127.0.0.1:9999";
    let mut frame = Vec::with_capacity(2 + our_addr.len() + payload.len() + 16);
    frame.push(1u8); // PACKET_BYTE
    frame.push(our_addr.len() as u8);
    frame.extend_from_slice(our_addr.as_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&digest);

    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.shutdown().await.unwrap();

    // Give the packet handler time to process and verify the KV still works normally.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(kv.members().len(), 1);

    kv.shutdown().await;
}

#[tokio::test]
async fn rkw06_tcp_transport_closes_half_open_peer_within_timeout() {
    let mut cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        stream_timeout: GoDuration::from_millis(120),
        packet_write_timeout: GoDuration::from_millis(120),
        ..Default::default()
    };
    cfg.randomize_node_name = false;
    let (tr, _packets, _streams) = TcpTransport::new(&cfg, "rkw06", None).await.unwrap();
    let port = tr.auto_bind_port();

    // Case 1: Peer connects and sends nothing at all (half-open before msg_type).
    let mut s1 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut buf1 = [0u8; 8];
    let n1 = tokio::time::timeout(Duration::from_secs(2), s1.read(&mut buf1))
        .await
        .expect("half-open connection must be closed by transport timeout")
        .unwrap_or(0);
    assert_eq!(n1, 0);

    // Case 2: Peer sends PACKET_BYTE and stalls mid-packet.
    let mut s2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s2.write_all(&[1u8, 10u8]).await.unwrap(); // PACKET_BYTE, addr_len=10, then stall
    let mut buf2 = [0u8; 8];
    let n2 = tokio::time::timeout(Duration::from_secs(2), s2.read(&mut buf2))
        .await
        .expect("stalled packet reader must be closed by packet_write_timeout")
        .unwrap_or(0);
    assert_eq!(n2, 0);

    tr.shutdown_and_wait().await;
}

// ============================================================================
// RKW-07: EtcdKv max_retries == 0 and Unary Request Timeout Failover
// ============================================================================

#[tokio::test]
async fn rkw07_etcd_max_retries_zero_performs_zero_cas_attempts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_srv = Arc::clone(&hits);
    let srv = tokio::spawn(async move {
        while let Ok((_sock, _)) = listener.accept().await {
            hits_srv.fetch_add(1, Ordering::SeqCst);
        }
    });

    let cfg = EtcdConfig {
        endpoints: vec![format!("http://{addr}")],
        dial_timeout: GoDuration::from_millis(200),
        max_retries: 0,
        ..Default::default()
    };
    let kv = EtcdKv::new(cfg, Arc::new(StringCodec)).unwrap();
    let err = kv
        .cas("k", Box::new(|_| Ok(CasOutcome::Update(Box::new("v".to_string())))))
        .await
        .unwrap_err();
    assert!(matches!(err, KvError::CasFailed { .. }), "got: {err:?}");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    srv.abort();
}

#[tokio::test]
async fn rkw07_etcd_unary_request_times_out_on_blackholed_endpoint_and_fails_over() {
    // Endpoint 0: accepts TCP connections and never sends any HTTP response (blackhole).
    let blackhole = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let blackhole_addr = blackhole.local_addr().unwrap();
    let blackhole_task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = blackhole.accept().await {
            held.push(sock);
        }
    });

    // Endpoint 1: responds with valid etcd RangeResponse JSON for key "k" -> "ok".
    let good = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good_addr = good.local_addr().unwrap();
    let good_task = tokio::spawn(async move {
        use base64::Engine as _;
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(b"k");
        let val_b64 = base64::engine::general_purpose::STANDARD.encode(b"ok");
        let body = format!(r#"{{"kvs":[{{"key":"{key_b64}","value":"{val_b64}","version":"1"}}]}}"#);
        while let Ok((mut sock, _)) = good.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut req_buf = [0u8; 4096];
                let _ = sock.read(&mut req_buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });

    let cfg = EtcdConfig {
        endpoints: vec![
            format!("http://{blackhole_addr}"),
            format!("http://{good_addr}"),
        ],
        dial_timeout: GoDuration::from_millis(150),
        max_retries: 2,
        ..Default::default()
    };
    let kv = EtcdKv::new(cfg, Arc::new(StringCodec)).unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), kv.get("k"))
        .await
        .expect("EtcdKv::get must fail over from blackholed endpoint within bounded timeout")
        .unwrap()
        .unwrap();
    assert_eq!(got.downcast_ref::<String>().map(String::as_str), Some("ok"));

    blackhole_task.abort();
    good_task.abort();
}
