//! Ported upstream hashicorp/memberlist tests: queue, broadcast, config, keyring, compress, logging, lzw.

use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::Arc;

use mimir_config::server::MemberlistConfig;
use mimir_ring::kv::memberlist::client::MemberlistBroadcast;
use mimir_ring::kv::memberlist::lzw::{
    get_lzw_buffer, lzw_compress, lzw_decompress, release_lzw_buffer, MAX_POOLED_LZW_BUFFER_CAP,
};
use mimir_ring::kv::memberlist::queue::{Broadcast, TransmitLimitedQueue};
use mimir_ring::kv::memberlist::security::{
    decrypt_payload, encrypt_payload, ip_allowed, log_address, log_conn, parse_cidrs, Keyring,
};
use mimir_ring::kv::memberlist::wire::{
    compress_payload, compress_payload_raw, compression_type_label, decode_compressed_payload,
    decompress_buffer, decompress_payload, resolve_compression_type, CompressionType,
    MAX_DECOMPRESS_BYTES,
};
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv};

fn test_keys() -> Vec<Vec<u8>> {
    vec![
        (0u8..16).collect(),
        (0u8..16).rev().collect(),
        vec![8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7],
    ]
}

fn rand_bytes(n: usize) -> Vec<u8> {
    let mut state = (n as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
    (0..n)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 32) as u8
        })
        .collect()
}

fn ml_cfg(addr: &str, name: &str, bind_port: u16) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec![addr.to_string()],
        bind_port: i64::from(bind_port),
        node_name: name.to_string(),
        randomize_node_name: false,
        ..Default::default()
    }
}

struct NamedMsg {
    name: String,
    msg: Vec<u8>,
}

impl Broadcast for NamedMsg {
    fn invalidates(&self, _other: &dyn Broadcast) -> bool {
        false
    }
    fn message(&self) -> &[u8] {
        &self.msg
    }
    fn finished(&self) {}
    fn name(&self) -> Option<&str> {
        Some(&self.name)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Upstream: github.com/hashicorp/memberlist/broadcast_test.go::TestMemberlistBroadcast_Invalidates
#[test]
fn test_memberlist_broadcast_invalidates() {
    let m1 = MemberlistBroadcast {
        node: "test".to_string(),
        msg: Vec::new(),
        notify: None,
    };
    let m2 = MemberlistBroadcast {
        node: "foo".to_string(),
        msg: Vec::new(),
        notify: None,
    };
    assert!(!m1.invalidates(&m2) && !m2.invalidates(&m1), "unexpected invalidation");
    assert!(m1.invalidates(&m1), "expected invalidation");
}

// Upstream: github.com/hashicorp/memberlist/broadcast_test.go::TestMemberlistBroadcast_Message
#[test]
fn test_memberlist_broadcast_message() {
    let m1 = MemberlistBroadcast {
        node: "test".to_string(),
        msg: b"test".to_vec(),
        notify: None,
    };
    assert_eq!(m1.message(), b"test");
}

// Upstream: github.com/hashicorp/memberlist/queue_test.go::TestLimitedBroadcastLess
#[test]
fn test_limited_broadcast_less() {
    let q = TransmitLimitedQueue::new(3, Box::new(|| 10));
    let a = Arc::new(NamedMsg {
        name: "a".to_string(),
        msg: vec![0u8; 10],
    });
    let b = Arc::new(NamedMsg {
        name: "b".to_string(),
        msg: vec![0u8; 10],
    });
    q.queue_broadcast_with_transmits(b, 1);
    q.queue_broadcast_with_transmits(a, 0);
    let view = q.ordered_view(false);
    assert_eq!(view[0].0, 0);
    assert_eq!(view[1].0, 1);

    let q2 = TransmitLimitedQueue::new(3, Box::new(|| 10));
    q2.queue_broadcast_with_transmits(
        Arc::new(NamedMsg {
            name: "smaller".to_string(),
            msg: vec![0u8; 10],
        }),
        0,
    );
    q2.queue_broadcast_with_transmits(
        Arc::new(NamedMsg {
            name: "larger".to_string(),
            msg: vec![0u8; 12],
        }),
        0,
    );
    let view2 = q2.ordered_view(false);
    assert_eq!(view2[0].1.len(), 12);
    assert_eq!(view2[1].1.len(), 10);

    let q3 = TransmitLimitedQueue::new(3, Box::new(|| 10));
    q3.queue_broadcast_with_transmits(
        Arc::new(NamedMsg {
            name: "older".to_string(),
            msg: b"old123456789".to_vec(),
        }),
        0,
    );
    q3.queue_broadcast_with_transmits(
        Arc::new(NamedMsg {
            name: "newer".to_string(),
            msg: b"new123456789".to_vec(),
        }),
        0,
    );
    let view3 = q3.ordered_view(false);
    assert_eq!(view3[0].1, b"new123456789");
    assert_eq!(view3[1].1, b"old123456789");
}

// Upstream: github.com/hashicorp/memberlist/queue_test.go::TestTransmitLimited_ordering
#[test]
fn test_transmit_limited_ordering() {
    let q = TransmitLimitedQueue::new(1, Box::new(|| 10));
    for (name, tr) in [
        ("node0", 0),
        ("node1", 10),
        ("node2", 3),
        ("node3", 4),
        ("node4", 7),
    ] {
        q.queue_broadcast_with_transmits(
            Arc::new(NamedMsg {
                name: name.to_string(),
                msg: name.as_bytes().to_vec(),
            }),
            tr,
        );
    }
    let dump = q.ordered_view(true);
    assert_eq!(dump[0].0, 10);
    assert_eq!(dump[1].0, 7);
    assert_eq!(dump[2].0, 4);
    assert_eq!(dump[3].0, 3);
    assert_eq!(dump[4].0, 0);
}

// Upstream: github.com/hashicorp/memberlist/queue_test.go::TestTransmitLimitedQueue_GenIdConflict
#[test]
fn test_transmit_limited_queue_gen_id_conflict() {
    let broadcasts = TransmitLimitedQueue::new(3, Box::new(|| 10));
    broadcasts.queue_broadcast(Arc::new(NamedMsg {
        name: "A".to_string(),
        msg: b"A timestamp update".to_vec(),
    }));
    broadcasts.queue_broadcast(Arc::new(NamedMsg {
        name: "A".to_string(),
        msg: b"A left".to_vec(),
    }));
    broadcasts.queue_broadcast(Arc::new(NamedMsg {
        name: "B".to_string(),
        msg: b"B timestamp update".to_vec(),
    }));
    broadcasts.queue_broadcast(Arc::new(NamedMsg {
        name: "C".to_string(),
        msg: b"C left".to_vec(),
    }));
    assert_eq!(broadcasts.num_queued(), 3);
    let msgs = broadcasts.get_broadcasts(0, 1024);
    let joined: Vec<&str> = msgs
        .iter()
        .map(|m| std::str::from_utf8(m).unwrap())
        .collect();
    assert_eq!(joined.join(", "), "B timestamp update, C left, A left");
}

// Upstream: github.com/hashicorp/memberlist/config_test.go::Test_IsValidAddressDefaults
#[test]
fn test_is_valid_address_defaults() {
    let tests = [
        "127.0.0.1",
        "127.0.0.5",
        "10.0.0.9",
        "172.16.0.7",
        "192.168.2.1",
        "fe80::aede:48ff:fe00:1122",
        "::1",
    ];
    for ip_str in tests {
        let ip: IpAddr = ip_str.parse().unwrap();
        assert!(ip_allowed(None, ip).is_ok(), "IP {ip_str} should be allowed by default");
    }
}

// Upstream: github.com/hashicorp/memberlist/config_test.go::Test_IsValidAddressOverride
#[test]
fn test_is_valid_address_override() {
    type Case<'a> = (&'a str, Option<&'a [&'a str]>, &'a [&'a str], &'a [&'a str]);
    let cases: [Case<'_>; 4] = [
        (
            "Default, nil allows all",
            None,
            &["127.0.0.5", "10.0.0.9", "192.168.1.7", "::1"],
            &[],
        ),
        (
            "Only IPv4",
            Some(&["0.0.0.0/0"]),
            &["127.0.0.5", "10.0.0.9", "192.168.1.7"],
            &["fe80::38bc:4dff:fe62:b1ae", "::1"],
        ),
        (
            "Only IPv6",
            Some(&["::0/0"]),
            &["fe80::38bc:4dff:fe62:b1ae", "::1"],
            &["127.0.0.5", "10.0.0.9", "192.168.1.7"],
        ),
        (
            "Only 127.0.0.0/8 and ::1",
            Some(&["::1/128", "127.0.0.0/8"]),
            &["127.0.0.5", "::1"],
            &["::2", "178.250.0.187", "10.0.0.9", "192.168.1.7", "fe80::38bc:4dff:fe62:b1ae"],
        ),
    ];
    for (name, allow, success, fail) in cases {
        let nets = allow.map(|a| parse_cidrs(a).unwrap());
        for s in success {
            let ip: IpAddr = s.parse().unwrap();
            assert!(ip_allowed(nets.as_deref(), ip).is_ok(), "{name}: {s} should pass");
        }
        for f in fail {
            let ip: IpAddr = f.parse().unwrap();
            assert!(ip_allowed(nets.as_deref(), ip).is_err(), "{name}: {f} should fail");
        }
    }
}

// Upstream: github.com/hashicorp/memberlist/keyring_test.go::TestKeyring_EmptyRing
#[test]
fn test_keyring_empty_ring() {
    let kr = Keyring::new(&[], None).unwrap();
    assert_eq!(kr.get_keys().len(), 0);
}

// Upstream: github.com/hashicorp/memberlist/keyring_test.go::TestKeyring_PrimaryOnly
#[test]
fn test_keyring_primary_only() {
    let keys = test_keys();
    let kr = Keyring::new(&[], Some(&keys[0])).unwrap();
    assert_eq!(kr.get_keys().len(), 1);
}

// Upstream: github.com/hashicorp/memberlist/keyring_test.go::TestKeyring_GetPrimaryKey
#[test]
fn test_keyring_get_primary_key() {
    let keys = test_keys();
    let kr = Keyring::new(&keys, Some(&keys[1])).unwrap();
    assert_eq!(kr.get_primary_key().as_deref(), Some(keys[1].as_slice()));
}

// Upstream: github.com/hashicorp/memberlist/keyring_test.go::TestKeyring_AddRemoveUse
#[test]
fn test_keyring_add_remove_use() {
    let keys = test_keys();
    let kr = Keyring::new(&[], Some(&keys[1])).unwrap();
    assert!(kr.use_key(&keys[2]).is_err());
    kr.add_key(&keys[2]).unwrap();
    let got = kr.get_keys();
    assert_eq!(got[0], keys[1]);
    assert_eq!(got.len(), 2);
    kr.use_key(&keys[2]).unwrap();
    assert_eq!(kr.get_primary_key().as_deref(), Some(keys[2].as_slice()));
    assert!(kr.remove_key(&keys[2]).is_err());
    kr.remove_key(&keys[1]).unwrap();
    assert_eq!(kr.get_keys().len(), 1);
}

// Upstream: github.com/hashicorp/memberlist/keyring_test.go::TestKeyRing_MultiKeyEncryptDecrypt
#[test]
fn test_key_ring_multi_key_encrypt_decrypt() {
    let keys = test_keys();
    let plaintext = b"this is a plain text message";
    let extra = b"random data";
    let kr = Keyring::new(&keys, Some(&keys[0])).unwrap();

    let mut buf = Vec::new();
    encrypt_payload(1, &keys[0], plaintext, extra, &mut buf).unwrap();
    let msg = decrypt_payload(&kr.get_keys(), &buf, extra).unwrap();
    assert_eq!(msg, plaintext);

    buf.clear();
    encrypt_payload(1, &keys[2], plaintext, extra, &mut buf).unwrap();
    let msg = decrypt_payload(&kr.get_keys(), &buf, extra).unwrap();
    assert_eq!(msg, plaintext);

    kr.remove_key(&keys[2]).unwrap();
    assert!(decrypt_payload(&kr.get_keys(), &buf, extra).is_err());
}

// Upstream: github.com/hashicorp/memberlist/compress_test.go::TestCompressDecompress
#[test]
fn test_compress_decompress() {
    let types = [CompressionType::Lzw, CompressionType::Snappy];
    let sizes = [0usize, 1, 100, 100 * 1024, 1024 * 1024];
    for typ in types {
        for size in sizes {
            let input = rand_bytes(size);
            let buf = compress_payload(typ, &input).unwrap();
            let (got_type, decoded) = decompress_payload(&buf[1..]);
            assert_eq!(got_type, Some(typ));
            assert_eq!(decoded.unwrap(), input);
        }
    }
    // mixed codec receiver
    for sender in types {
        let input = b"the quick brown fox jumps over the lazy dog";
        let buf = compress_payload(sender, input).unwrap();
        let (got_type, decoded) = decompress_payload(&buf[1..]);
        assert_eq!(got_type, Some(sender));
        assert_eq!(decoded.unwrap(), input);
    }
    // concurrent race + pool reset hygiene + independent output
    let handles: Vec<_> = (0..8)
        .map(|w| {
            std::thread::spawn(move || {
                for i in 0..100 {
                    let typ = types[(w + i) % 2];
                    let input = rand_bytes((i * 37) % 2048);
                    let buf = compress_payload(typ, &input).unwrap();
                    let (gt, dec) = decompress_payload(&buf[1..]);
                    assert_eq!(gt, Some(typ));
                    assert_eq!(dec.unwrap(), input);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    for typ in types {
        let long = vec![b'A'; 16 * 1024];
        let short = b"BB";
        let b_long = compress_payload(typ, &long).unwrap();
        assert_eq!(decompress_payload(&b_long[1..]).1.unwrap(), long);
        let b_short = compress_payload(typ, short).unwrap();
        assert_eq!(decompress_payload(&b_short[1..]).1.unwrap(), short);
        let snap = b_long.clone();
        for _ in 0..5 {
            let _ = compress_payload(typ, &vec![b'z'; 1024]).unwrap();
        }
        assert_eq!(snap, b_long);
    }
}

// Upstream: github.com/hashicorp/memberlist/compress_test.go::TestMemberlist_initCompressionMetricLabels
#[test]
fn test_memberlist_init_compression_metric_labels() {
    assert_eq!(compression_type_label(Some(CompressionType::Lzw)), "lzw");
    assert_eq!(compression_type_label(Some(CompressionType::Snappy)), "snappy");
    assert_eq!(compression_type_label(None), "unknown");
    assert_eq!(resolve_compression_type("").unwrap(), CompressionType::Lzw);
    assert_eq!(resolve_compression_type("lzw").unwrap(), CompressionType::Lzw);
    assert_eq!(resolve_compression_type("snappy").unwrap(), CompressionType::Snappy);
    assert!(resolve_compression_type("zstd").is_err());
}

// Upstream: github.com/hashicorp/memberlist/compress_test.go::TestDecompressErrors
#[test]
fn test_decompress_errors() {
    // lzw cap exceeded
    let plain = vec![b'A'; MAX_DECOMPRESS_BYTES + 1];
    let comp = lzw_compress(&plain);
    let err = lzw_decompress(&comp, MAX_DECOMPRESS_BYTES).unwrap_err();
    assert_eq!(
        err,
        format!("memberlist: LZW-decompressed payload exceeds {MAX_DECOMPRESS_BYTES} bytes")
    );

    // snappy varint header claiming MAX_DECOMPRESS_BYTES + 1
    let mut hdr = Vec::new();
    let mut v = (MAX_DECOMPRESS_BYTES + 1) as u64;
    while v >= 0x80 {
        hdr.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    hdr.push(v as u8);
    let err = decompress_buffer(CompressionType::Snappy, &hdr).unwrap_err();
    assert_eq!(
        err,
        format!(
            "memberlist: snappy-decompressed payload would exceed {MAX_DECOMPRESS_BYTES} bytes (claimed {})",
            MAX_DECOMPRESS_BYTES + 1
        )
    );

    // wrapper decode error
    let (got_type, res) = decompress_payload(&[0xff, 0xff, 0xff, 0xff]);
    let err = res.unwrap_err();
    assert!(err.contains("msgpack decode error"), "got {err}");
    assert_eq!(got_type, None);
    assert_eq!(compression_type_label(got_type), "unknown");

    // unknown algorithm
    let bad_frame = [0x82u8, 0xa4, b'A', b'l', b'g', b'o', 99, 0xa3, b'B', b'u', b'f', 0xc4, 0x00];
    let err = decode_compressed_payload(&bad_frame).unwrap_err();
    assert_eq!(err, "cannot decompress unknown algorithm 99");
}

// Upstream: github.com/hashicorp/memberlist/compress_test.go::TestCompressPayload_UnknownAlgo
#[test]
fn test_compress_payload_unknown_algo() {
    let err = compress_payload_raw(255, b"data").unwrap_err();
    assert_eq!(err, "memberlist: cannot compress with unknown algorithm 255");
}

// Upstream: github.com/hashicorp/memberlist/compress_test.go::TestDecompressBuffer_MalformedBody
#[test]
fn test_decompress_buffer_malformed_body() {
    for (typ, substr) in [
        (CompressionType::Lzw, "lzw"),
        (CompressionType::Snappy, "snappy"),
    ] {
        let garbage = vec![0xffu8; 32];
        let err = decompress_buffer(typ, &garbage).unwrap_err();
        assert!(err.contains(substr), "{err}");

        let input = b"the quick brown fox ".repeat(100);
        let wrapped = compress_payload(typ, &input).unwrap();
        let cp = decode_compressed_payload(&wrapped[1..]).unwrap();
        let half = &cp.buf[..cp.buf.len() / 2];
        let err = decompress_buffer(typ, half).unwrap_err();
        assert!(err.contains(substr), "{err}");
    }
}

// Upstream: github.com/hashicorp/memberlist/compress_test.go::TestMemberlistCompression
#[tokio::test]
async fn test_memberlist_compression() {
    // snappy only
    let mut c1 = ml_cfg("127.0.10.1", "snappy-1", 0);
    c1.compression_enabled = true;
    c1.compression_algorithm = "snappy".to_string();
    let m1 = MemberlistKv::start_with_options(
        c1,
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let port = m1.listening_port();

    let mut c2 = ml_cfg("127.0.10.2", "snappy-2", port);
    c2.compression_enabled = true;
    c2.compression_algorithm = "snappy".to_string();
    let m2 = MemberlistKv::start_with_options(
        c2,
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let num = m2.join_members(&["snappy-1/127.0.10.1".to_string()]).await.unwrap();
    assert_eq!(num, 1);
    assert_eq!(m2.members().len(), 2);
    m2.shutdown().await;
    m1.shutdown().await;

    // mixed rollout: m3 emits Snappy, m4 emits LZW
    let mut c3 = ml_cfg("127.0.10.3", "mixed-1", 0);
    c3.compression_enabled = true;
    c3.compression_algorithm = "snappy".to_string();
    let m3 = MemberlistKv::start_with_options(
        c3,
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let port2 = m3.listening_port();

    let mut c4 = ml_cfg("127.0.10.4", "mixed-2", port2);
    c4.compression_enabled = true;
    c4.compression_algorithm = "lzw".to_string();
    let m4 = MemberlistKv::start_with_options(
        c4,
        vec![],
        "",
        None,
        KvOptions {
            use_transport_bind_port: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let num2 = m4.join_members(&["mixed-1/127.0.10.3".to_string()]).await.unwrap();
    assert_eq!(num2, 1);
    assert_eq!(m4.members().len(), 2);
    assert_eq!(m3.members().len(), 2);
    m4.shutdown().await;
    m3.shutdown().await;
}

// Upstream: github.com/hashicorp/memberlist/logging_test.go::TestLogging_Address
#[test]
fn test_logging_address() {
    assert_eq!(log_address(None), "from=<unknown address>");
    assert_eq!(log_address(Some("127.0.0.1")), "from=127.0.0.1");
}

// Upstream: github.com/hashicorp/memberlist/logging_test.go::TestLogging_Conn
#[test]
fn test_logging_conn() {
    assert_eq!(log_conn(None), "from=<unknown address>");
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    let conn = TcpStream::connect(addr).unwrap();
    let _accepted = ln.accept().unwrap();
    assert_eq!(
        log_conn(conn.peer_addr().ok()),
        format!("from={}", conn.peer_addr().unwrap())
    );
}

// Upstream: github.com/hashicorp/memberlist/lzw_test.go::TestReleaseLZWBuffer_BoundedCap
#[test]
fn test_release_lzw_buffer_bounded_cap() {
    let mut big = get_lzw_buffer();
    big.extend(std::iter::repeat_n(0u8, MAX_POOLED_LZW_BUFFER_CAP + 1));
    assert!(big.capacity() > MAX_POOLED_LZW_BUFFER_CAP);
    release_lzw_buffer(big);
    for i in 0..10 {
        let b = get_lzw_buffer();
        assert!(
            b.capacity() <= MAX_POOLED_LZW_BUFFER_CAP,
            "oversized buffer leaked through pool on iteration {i}"
        );
        release_lzw_buffer(b);
    }
}
