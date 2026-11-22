//! Wave 34 hardening regression tests for `mimir-ring`.

use std::any::Any;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use bytes::Bytes;
use mimir_common::services::{Service, ServiceContext};
use mimir_config::{EtcdConfig, GoDuration, MemberlistConfig, RingConfig};
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use mimir_proto::ringpb::{
    Desc, InstanceDesc, InstanceState, PartitionDesc, PartitionRingDesc, PartitionState,
    PartitionTokenScheme,
};
use mimir_ring::kv::etcd::EtcdKv;
use mimir_ring::kv::memberlist::client::{KvOptions, MemberlistDelegate, MemberlistKv};
use mimir_ring::kv::memberlist::security::{
    encrypt_overhead, parse_cidrs, try_encrypt_overhead,
};
use mimir_ring::kv::memberlist::stream::{
    decrypt_remote_state_for_test, read_msgpack_map_bytes_for_test,
    read_push_pull_from_stream_for_test, read_user_msg_from_stream_for_test,
    skip_sync_val_for_test,
};
use mimir_ring::kv::memberlist::wire::{
    compress_payload, make_compound_message, Alive, CompressionType, MessageType, MAX_DECOMPRESS_BYTES,
    META_MAX_SIZE,
};
use mimir_ring::kv::memberlist::TcpTransport;
use mimir_ring::kv::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError, ProtoSnappyCodec, StringCodec, WatchKeyFn, WatchPrefixFn};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig};
use mimir_ring::partition_ring::PartitionRing;
use mimir_ring::ring::model::{ring_desc_codec, CompareResult, DescExt};
use mimir_ring::ring::ticker::new_disableable_ticker;
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{Ring, RingSettings};
use mimir_ring::token_gen::{SpreadMinimizingTokenGenerator, TokenGenerator};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn make_kv_with_opts(mut cfg: MemberlistConfig, opts: KvOptions) -> Arc<MemberlistKv> {
    if cfg.bind_addr.is_empty() {
        cfg.bind_addr = vec!["127.0.0.1".to_string()];
    }
    if cfg.node_name.is_empty() {
        cfg.node_name = "test-node".to_string();
    }
    MemberlistKv::new(
        cfg,
        vec![Arc::new(StringCodec) as Arc<dyn Codec>],
        "test",
        None,
        opts,
    )
    .expect("MemberlistKv::new")
}

fn encode_user_cmd(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(MessageType::User as u8);
    out.extend_from_slice(payload);
    out
}

#[tokio::test]
async fn w34_memberlist_cumulative_decompress_budget_nested_compound_and_part_cap() {
    let cfg = MemberlistConfig {
        received_messages_queue_size: 1024,
        ..Default::default()
    };
    let kv = make_kv_with_opts(cfg, KvOptions::default());

    // 1. Cumulative decompressed byte budget across Compound -> Compress parts:
    // Each part decompresses to 1 MiB (Snappy compressed ~49 KiB, fits in u16 compound part len);
    // 22 parts = 22 MiB > MAX_DECOMPRESS_BYTES (21 MiB), so only 21 fit in the per-packet budget.
    let chunk = encode_user_cmd(&vec![b'A'; 1024 * 1024 - 1]);
    let comp_part = compress_payload(CompressionType::Snappy, &chunk).unwrap();
    assert!(comp_part.len() <= u16::MAX as usize);
    let comp_parts: Vec<&[u8]> = vec![comp_part.as_slice(); 22];
    let compound_22 = make_compound_message(&comp_parts);
    kv.handle_command_for_test(&compound_22, "127.0.0.1:7946").await;
    let (high, low) = kv.handoff_counts_for_test();
    assert_eq!(high, 0);
    assert_eq!(
        low, 21,
        "only 21 of 22 1 MiB compressed parts (21 MiB <= MAX_DECOMPRESS_BYTES={MAX_DECOMPRESS_BYTES}) should fit in per-packet budget"
    );
    let _ = kv.drain_handoff_for_test();

    // 2. Disallow nested Compound inside Compound (both direct and via Compress).
    let inner_user = encode_user_cmd(b"inner");
    let inner_compound = make_compound_message(&[&inner_user]);
    let outer_compound = make_compound_message(&[&inner_compound]);
    kv.handle_command_for_test(&outer_compound, "127.0.0.1:7946").await;
    assert_eq!(kv.handoff_counts_for_test(), (0, 0), "nested Compound must be rejected");

    let comp_inner_compound = compress_payload(CompressionType::Snappy, &inner_compound).unwrap();
    let outer_comp_compound = make_compound_message(&[&comp_inner_compound]);
    kv.handle_command_for_test(&outer_comp_compound, "127.0.0.1:7946").await;
    assert_eq!(
        kv.handoff_counts_for_test(),
        (0, 0),
        "nested Compound inside Compress inside Compound must be rejected"
    );

    // 3. Single Compound up to u8::MAX (255 <= MAX_COMPOUND_PARTS=256) succeeds.
    let part = encode_user_cmd(b"p");
    let parts_255: Vec<&[u8]> = vec![part.as_slice(); 255];
    let max_compound = make_compound_message(&parts_255);
    kv.handle_command_for_test(&max_compound, "127.0.0.1:7946").await;
    assert_eq!(
        kv.handoff_counts_for_test().1,
        255,
        "255-part Compound within MAX_COMPOUND_PARTS=256 must succeed"
    );
}

#[tokio::test]
async fn w34_memberlist_handoff_64mib_byte_ceiling() {
    let cfg = MemberlistConfig {
        received_messages_queue_size: 100_000,
        ..Default::default()
    };
    let kv = make_kv_with_opts(cfg, KvOptions::default());

    let msg_1mib = encode_user_cmd(&vec![b'Z'; 1024 * 1024]);
    for _ in 0..70 {
        kv.handle_command_for_test(&msg_1mib, "127.0.0.1:7946").await;
    }
    assert!(
        kv.handoff_total_bytes_for_test() <= 64 * 1024 * 1024,
        "handoff queue bytes {} exceeded 64 MiB",
        kv.handoff_total_bytes_for_test()
    );
    let (_, low) = kv.handoff_counts_for_test();
    assert!((63..=64).contains(&low), "unexpected accepted count {low}");

    let drained = kv.drain_handoff_for_test();
    assert_eq!(drained, low);
    assert_eq!(kv.handoff_total_bytes_for_test(), 0);

    // After draining, new messages are accepted again.
    kv.handle_command_for_test(&msg_1mib, "127.0.0.1:7946").await;
    assert_eq!(kv.handoff_counts_for_test().1, 1);
}

#[tokio::test]
async fn w34_memberlist_key_workers_capped_at_4096_and_node_meta_truncated() {
    struct OversizedMetaDelegate;
    impl MemberlistDelegate for OversizedMetaDelegate {
        fn node_meta(&self, _limit: usize) -> Vec<u8> {
            vec![b'm'; META_MAX_SIZE + 250]
        }
        fn notify_msg(&self, _msg: &[u8]) {}
        fn get_broadcasts(&self, _overhead: usize, _limit: usize) -> Vec<Vec<u8>> {
            Vec::new()
        }
        fn local_state(&self, _join: bool) -> Vec<u8> {
            Vec::new()
        }
        fn merge_remote_state(&self, _buf: &[u8], _join: bool) {}
    }

    let kv = make_kv_with_opts(
        MemberlistConfig::default(),
        KvOptions {
            memberlist_delegate: Some(Arc::new(OversizedMetaDelegate)),
            ..Default::default()
        },
    );

    // Oversized node_meta must truncate cleanly instead of panicking.
    let meta = kv.current_node_meta_for_test().unwrap();
    assert_eq!(meta.len(), META_MAX_SIZE);

    // Enqueue 4096 distinct keys -> succeeds; 4097th -> rejected (false); existing key -> still true.
    let codec: Arc<dyn Codec> = Arc::new(StringCodec);
    for i in 0..4096 {
        assert!(
            kv.enqueue_key_update_for_test(&format!("k-{i}"), Bytes::from_static(b"v"), Arc::clone(&codec), false),
            "key {i} within 4096 cap must succeed"
        );
    }
    assert!(
        !kv.enqueue_key_update_for_test("k-4096", Bytes::from_static(b"v"), Arc::clone(&codec), false),
        "4097th distinct key worker must be rejected"
    );
    assert!(
        kv.enqueue_key_update_for_test("k-0", Bytes::from_static(b"v2"), Arc::clone(&codec), false),
        "existing key worker must remain usable"
    );
}

#[tokio::test]
async fn w34_stream_incremental_bounded_reads_and_skip_sync_val() {
    // 1. decrypt_remote_state claiming 20 MiB with 0 payload bytes fails cleanly without OOM.
    let len_20mib = (20u32 * 1024 * 1024).to_be_bytes();
    let mut cur = Cursor::new(len_20mib.to_vec());
    assert!(decrypt_remote_state_for_test(&mut cur, "").await.is_err());

    // 2. read_push_pull_from_stream claiming UserStateLen=20 MiB with EOF fails cleanly.
    let mut pp_hdr = vec![0x83];
    pp_hdr.extend_from_slice(b"\xa5Nodes\x00\xacUserStateLen\xce");
    pp_hdr.extend_from_slice(&(20u32 * 1024 * 1024).to_be_bytes());
    pp_hdr.extend_from_slice(b"\xa4Join\xc2");
    let mut pp_cur = Cursor::new(pp_hdr);
    assert!(read_push_pull_from_stream_for_test(&mut pp_cur).await.is_err());

    // 3. read_user_msg_from_stream claiming UserMsgLen=20 MiB with EOF fails cleanly.
    let mut um_hdr = vec![0x81];
    um_hdr.extend_from_slice(b"\xaaUserMsgLen\xce");
    um_hdr.extend_from_slice(&(20u32 * 1024 * 1024).to_be_bytes());
    let mut um_cur = Cursor::new(um_hdr);
    assert!(read_user_msg_from_stream_for_test(&mut um_cur).await.is_err());

    // 4. read_msgpack_map_bytes (read_append) claiming 10 MiB bin32 with EOF fails cleanly.
    let mut bin32_eof = vec![0xc6];
    bin32_eof.extend_from_slice(&(10u32 * 1024 * 1024).to_be_bytes());
    let mut bin_cur = Cursor::new(bin32_eof);
    assert!(read_msgpack_map_bytes_for_test(&mut bin_cur).await.is_err());

    // 5. skip_sync_val skips nested MessagePack arrays, maps, and ext items, and rejects depth >= 32.
    let mut nested_item = vec![
        0x83, // map(3): array, map, ext
        0xa1, b'a', 0x92, 0x01, 0xc7, 0x02, 0x05, 0xaa, 0xbb, // fixarray[1, ext8(type=5, 2 bytes)]
        0xa1, b'b', 0xde, 0x00, 0x01, 0xa1, b'k', 0xd5, 0x01, 0x11, 0x22, // map16{k: fixext2}
        0xa1, b'c', 0xc9, 0x00, 0x00, 0x00, 0x02, 0x09, 0xde, 0xad, // ext32(type=9, 2 bytes)
    ];
    let expected_end = nested_item.len();
    nested_item.push(0x42);
    let mut pos = 0usize;
    skip_sync_val_for_test(&nested_item, &mut pos).expect("should skip nested map/array/ext");
    assert_eq!(pos, expected_end);

    // Depth >= 32 is rejected.
    let mut deep = vec![0x91u8; 32];
    deep.push(0x00);
    let mut dpos = 0usize;
    assert!(skip_sync_val_for_test(&deep, &mut dpos).is_err());

    // Truncated huge array32 count is rejected without looping 4B times.
    let huge_arr = [0xdd, 0xff, 0xff, 0xff, 0xff, 0x00];
    let mut hpos = 0usize;
    assert!(skip_sync_val_for_test(&huge_arr, &mut hpos).is_err());
}

#[tokio::test]
async fn w34_tcp_transport_256_concurrent_conn_semaphore() {
    let cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        stream_timeout: GoDuration::from_secs(5),
        ..Default::default()
    };
    let (tr, _pkt_rx, _stream_rx) = TcpTransport::new(&cfg, "test_sem", None).await.unwrap();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], tr.auto_bind_port()));
    assert_eq!(tr.available_conn_permits_for_test(), 256);

    // Open 256 connections that stall before sending the 1-byte message type.
    let mut conns = Vec::with_capacity(256);
    for _ in 0..256 {
        conns.push(TcpStream::connect(addr).await.unwrap());
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        while tr.available_conn_permits_for_test() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("256 connections should consume all 256 permits");

    // 257th connection cannot consume another permit while all 256 are held.
    let _extra = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(tr.available_conn_permits_for_test(), 0);

    // Dropping one connection frees a permit (immediately taken by _extra); dropping _extra + all conns returns all 256.
    drop(_extra);
    conns.pop();
    drop(conns);
    tokio::time::timeout(Duration::from_secs(5), async {
        while tr.available_conn_permits_for_test() < 256 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("permits should return after connections close");

    tr.shutdown();
}

#[test]
fn w34_proto_snappy_codec_enforces_64mib_absolute_ceiling() {
    let codec = ProtoSnappyCodec::<Desc>::new("ring");
    // Construct a compressed buffer of 2.5 MiB (so 32x ratio = 80 MiB, which passes the 32x check!),
    // with a snappy header claiming 65 MiB uncompressed (> 64 MiB ceiling).
    let mut payload = Vec::with_capacity(2_600_000);
    // Varint encoding of 65 * 1024 * 1024 = 68_157_440:
    let mut v = 65u64 * 1024 * 1024;
    while v >= 0x80 {
        payload.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    payload.push(v as u8);
    payload.resize(2_600_000, 0);

    let err = codec.decode_msg(&payload).unwrap_err();
    assert!(
        err.to_string().contains("exceeds max bound"),
        "expected 64 MiB absolute bound rejection, got: {err}"
    );
}

#[tokio::test]
async fn w34_cidrs_allowed_fails_closed_and_encrypt_overhead_no_panic() {
    // 1. encrypt_overhead never panics for vsn > 1.
    assert_eq!(encrypt_overhead(0), 45);
    assert_eq!(encrypt_overhead(1), 29);
    assert_eq!(encrypt_overhead(2), 45);
    assert_eq!(encrypt_overhead(255), 45);
    assert!(try_encrypt_overhead(2).is_err());

    // 2. CIDRsAllowed fails CLOSED on unparseable msg.from (except "pipe") and invalid a.addr.
    let cidrs = parse_cidrs(&["10.0.0.0/8"]).unwrap();
    let kv = make_kv_with_opts(
        MemberlistConfig::default(),
        KvOptions {
            cidrs_allowed: Some(cidrs),
            ..Default::default()
        },
    );

    let encode_alive = |name: &str, addr: Vec<u8>| -> Vec<u8> {
        let a = Alive {
            incarnation: 1,
            node: name.to_string(),
            addr,
            port: 7946,
            meta: None,
            vsn: vec![1, 5, 2, 1, 1, 1],
        };
        mimir_ring::kv::memberlist::wire::encode_alive(&a)
    };

    // Unparseable msg.from with allowed inner IP -> must be blocked!
    let valid_inner = encode_alive("bad-from", vec![10, 1, 2, 3]);
    kv.handle_command_for_test(&valid_inner, "unparseable-host:notaport").await;
    kv.drain_handoff_for_test();
    assert!(
        !kv.members().iter().any(|n| n.name == "bad-from"),
        "unparseable msg.from must fail closed"
    );

    // Valid msg.from with invalid a.addr (bytes_to_ip == None) -> must be blocked!
    let bad_inner = encode_alive("bad-inner", vec![1, 2, 3]);
    kv.handle_command_for_test(&bad_inner, "10.1.2.3:7946").await;
    kv.drain_handoff_for_test();
    assert!(
        !kv.members().iter().any(|n| n.name == "bad-inner"),
        "invalid Alive.addr must fail closed"
    );

    // Direct alive_node (state.rs) with invalid a.addr -> must be blocked!
    kv.alive_node_for_test(
        &Alive {
            incarnation: 1,
            node: "direct-bad-addr".to_string(),
            addr: vec![9, 9],
            port: 7946,
            meta: None,
            vsn: vec![1, 5, 2, 1, 1, 1],
        },
        false,
    );
    assert!(!kv.members().iter().any(|n| n.name == "direct-bad-addr"));

    // "pipe" with valid allowed inner IP -> must be accepted!
    let pipe_ok = encode_alive("pipe-ok", vec![10, 1, 2, 4]);
    kv.handle_command_for_test(&pipe_ok, "pipe").await;
    kv.drain_handoff_for_test();
    assert!(
        kv.members().iter().any(|n| n.name == "pipe-ok"),
        "pipe with valid allowed CIDR addr must succeed"
    );

    let _ = KeyValuePair::default().encode_to_vec();
}

#[tokio::test]
async fn w34_ticker_stop_watch_no_lost_wakeup_and_drop_aborts() {
    // Immediate stop() before task gets a chance to wait must still close rx promptly.
    let (stop, rx) = new_disableable_ticker(Duration::from_secs(60));
    let mut rx = rx.unwrap();
    stop.stop();
    let res = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("ticker should stop immediately without lost wakeup");
    assert!(res.is_none());

    // Dropping TickerStop without calling stop() must also abort the task and close rx.
    let (stop2, rx2) = new_disableable_ticker(Duration::from_secs(60));
    let mut rx2 = rx2.unwrap();
    drop(stop2);
    let res2 = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
        .await
        .expect("dropping TickerStop must abort task and close receiver");
    assert!(res2.is_none());
}

#[test]
fn w34_ring_update_ring_state_and_partition_ring_token_dedup() {
    // 1. Ring::update_ring_state updates topology cleanly even across state/topology transitions.
    let ring = Ring::with_optional_store_and_strategy(
        RingSettings {
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            zone_awareness_enabled: false,
            excluded_zones: Vec::new(),
            subring_cache_disabled: false,
        },
        "ingester",
        "ring",
        None,
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .unwrap();

    let mut d1 = Desc::default();
    d1.ingesters.insert(
        "ing-1".to_string(),
        InstanceDesc {
            addr: "127.0.0.1:9095".to_string(),
            timestamp: 100,
            state: InstanceState::Active as i32,
            tokens: vec![1000, 2000],
            ..Default::default()
        },
    );
    ring.update_ring_state(d1.clone());

    let mut d2 = d1.clone();
    d2.ingesters.insert(
        "ing-2".to_string(),
        InstanceDesc {
            addr: "127.0.0.2:9095".to_string(),
            timestamp: 101,
            state: InstanceState::Active as i32,
            tokens: vec![3000],
            ..Default::default()
        },
    );
    assert_eq!(d1.ring_compare(&d2), CompareResult::Different);
    ring.update_ring_state(d2);
    assert_eq!(ring.desc_snapshot().ingesters.len(), 2);

    // 2. PartitionRing::new iterates sorted partition IDs and deduplicates colliding tokens.
    let mut prd = PartitionRingDesc::default();
    prd.partitions.insert(
        2,
        PartitionDesc {
            id: 2,
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
            tokens: vec![1000, 3000],
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 10,
            ..Default::default()
        },
    );
    prd.partitions.insert(
        1,
        PartitionDesc {
            id: 1,
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
            tokens: vec![1000, 2000],
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 10,
            ..Default::default()
        },
    );
    let pr = PartitionRing::new(prd).unwrap();
    assert_eq!(pr.active_partitions_count(), 2);
    assert_eq!(pr.ring_tokens(), &[1000, 2000, 3000]);
    assert_eq!(pr.partition_by_token().get(&1000), Some(&1));
    assert_eq!(pr.partition_by_token().get(&2000), Some(&1));
    assert_eq!(pr.partition_by_token().get(&3000), Some(&2));
}

#[tokio::test]
async fn w34_etcd_watch_64mib_cap_and_multi_line_cursor_drain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let watch_calls = Arc::new(AtomicUsize::new(0));
    let wc = Arc::clone(&watch_calls);

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let wc = Arc::clone(&wc);
            tokio::spawn(async move {
                let mut req = [0u8; 4096];
                let _ = sock.read(&mut req).await;
                let call = wc.fetch_add(1, Ordering::SeqCst);
                let hdr = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n";
                let _ = sock.write_all(hdr.as_bytes()).await;

                if call == 0 {
                    // First connection: send > 64 MiB without any newline to trigger line buffer cap & reset.
                    let big = vec![b'x'; 1024 * 1024];
                    let chunk_hdr = format!("{:x}\r\n", big.len());
                    for _ in 0..66 {
                        if sock.write_all(chunk_hdr.as_bytes()).await.is_err() {
                            return;
                        }
                        if sock.write_all(&big).await.is_err() {
                            return;
                        }
                        if sock.write_all(b"\r\n").await.is_err() {
                            return;
                        }
                    }
                } else {
                    // Reconnect: send 2 JSON events in a single chunk (including an empty line).
                    let b64 = base64::engine::general_purpose::STANDARD;
                    let k = b64.encode(b"k1");
                    let v1 = b64.encode(b"v1");
                    let v2 = b64.encode(b"v2");
                    let body = format!(
                        "{{\"result\":{{\"header\":{{\"revision\":\"2\"}},\"events\":[{{\"type\":\"PUT\",\"kv\":{{\"key\":\"{k}\",\"value\":\"{v1}\",\"mod_revision\":\"2\"}}}}]}}}}\n\n{{\"result\":{{\"header\":{{\"revision\":\"3\"}},\"events\":[{{\"type\":\"PUT\",\"kv\":{{\"key\":\"{k}\",\"value\":\"{v2}\",\"mod_revision\":\"3\"}}}}]}}}}\n"
                    );
                    let chunk = format!("{:x}\r\n{body}\r\n0\r\n\r\n", body.len());
                    let _ = sock.write_all(chunk.as_bytes()).await;
                }
                let _ = sock.shutdown().await;
            });
        }
    });

    let kv = EtcdKv::new(
        EtcdConfig {
            endpoints: vec![format!("http://{addr}")],
            dial_timeout: GoDuration::from_secs(5),
            ..Default::default()
        },
        Arc::new(StringCodec),
    )
    .unwrap();

    let ctx = ServiceContext::background();
    let seen = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let seen_cb = Arc::clone(&seen);
    let ctx_watch = ctx.clone();

    let h = tokio::spawn(async move {
        kv.watch_key(
            "k1",
            ctx_watch,
            Box::new(move |any| {
                if let Some(s) = any.downcast_ref::<String>() {
                    seen_cb.lock().push(s.clone());
                    return seen_cb.lock().len() < 2;
                }
                true
            }),
        )
        .await;
    });

    tokio::time::timeout(Duration::from_secs(15), h)
        .await
        .expect("etcd watch must reset oversized line and recover on reconnect")
        .unwrap();
    assert_eq!(*seen.lock(), vec!["v1".to_string(), "v2".to_string()]);
    assert!(watch_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn w34_lifecycler_rolls_back_local_state_on_cas_failure_and_token_gen_no_panic() {
    struct ToggleFailKv {
        fail_cas: AtomicBool,
        data: parking_lot::Mutex<HashMap<String, Desc>>,
    }
    impl KvClient for ToggleFailKv {
        fn list<'a>(&'a self, _prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
            let v = self.data.lock().get(key).cloned();
            Box::pin(async move { Ok(v.map(|d| Box::new(d) as Box<dyn Any + Send + Sync>)) })
        }
        fn delete<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
            Box::pin(async { Ok(()) })
        }
        fn cas<'a>(&'a self, key: &'a str, mut f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
            if self.fail_cas.load(Ordering::SeqCst) {
                return Box::pin(async { Err(KvError::Other("injected cas failure".to_string())) });
            }
            let cur = self.data.lock().get(key).cloned().map(|d| Box::new(d) as Box<dyn Any + Send + Sync>);
            let res = f(cur.as_deref());
            Box::pin(async move {
                match res {
                    Ok(CasOutcome::Update(next) | CasOutcome::UpdateOnce(next)) => {
                        if let Some(d) = next.downcast_ref::<Desc>() {
                            self.data.lock().insert(key.to_string(), d.clone());
                        }
                        Ok(())
                    }
                    Ok(CasOutcome::NoChange) => Ok(()),
                    Err(e) => Err(e.err),
                }
            })
        }
        fn watch_key<'a>(&'a self, _k: &'a str, _c: ServiceContext, _f: WatchKeyFn) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
        fn watch_prefix<'a>(&'a self, _p: &'a str, _c: ServiceContext, _f: WatchPrefixFn) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    let kv = Arc::new(ToggleFailKv {
        fail_cas: AtomicBool::new(false),
        data: parking_lot::Mutex::new(HashMap::new()),
    });

    let ring_cfg = RingConfig {
        instance_id: "ing-0".to_string(),
        instance_addr: "127.0.0.1".to_string(),
        instance_port: 9095,
        num_tokens: 4,
        heartbeat_period: GoDuration::from_secs(60),
        observe_period: GoDuration::ZERO,
        ..Default::default()
    };
    let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
    let lc = Lifecycler::with_store(
        lc_cfg,
        kv.clone(),
        None,
        "ingester",
        "ring",
        false,
        None,
    );

    let svc = Arc::clone(&lc).into_service();
    let ctx = ServiceContext::background();
    svc.start_async(ctx.child()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while lc.get_state() != InstanceState::Active {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // Per W18-SR-02 / dskit lifecycler.go, CAS failure returns Err while keeping target state for heartbeat retry.
    kv.fail_cas.store(true, Ordering::SeqCst);
    let err_ro = lc.change_read_only_state(true).await.unwrap_err();
    assert!(err_ro.to_string().contains("injected cas failure"));
    let err_st = lc.change_state(InstanceState::Leaving).await.unwrap_err();
    assert!(err_st.to_string().contains("injected cas failure"));

    ctx.cancel();
    svc.stop_async();
    let _ = svc.await_terminated().await;

    // token_gen::generate_tokens does not panic even on degenerate instance_id.
    let bad_tg = SpreadMinimizingTokenGenerator::for_instance_and_zone_id("bad-", usize::MAX / 2, 0, false);
    assert!(bad_tg.generate_all_tokens().is_err());
    assert!(bad_tg.generate_tokens(4, &[]).is_empty());

    let _ = ring_desc_codec();
    let _ = UNIX_EPOCH;
    let _ = SystemTime::now();
}
