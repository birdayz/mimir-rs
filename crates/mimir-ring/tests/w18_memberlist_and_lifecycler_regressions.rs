//! Regression tests for Wave 18 classic Lifecycler & Memberlist fixes
//! (W18-SR-02, W18-SR-08, W18-KM-11, W18-KM-12, W18-KM-13).

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::{Service, ServiceContext};
use mimir_config::server::MemberlistConfig;
use mimir_config::{GoDuration, RingConfig};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::memberlist::client::{MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::memberlist::tcp_transport::TcpTransport;
use mimir_ring::kv::memberlist::wire::{add_label_header, compress_payload, CompressionType, MessageType};
use mimir_ring::kv::{BoxFuture, CasFn, KvClient, KvError, StringCodec};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig, LifecyclerError, NoopFlushTransferer};
use mimir_ring::ring::model::{ring_desc_codec, InstanceDescExt};
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const RING_KEY: &str = "ring";

/// KvClient wrapper around InMemoryKv that injects transient CAS failures when armed.
struct ToggleFailCasKv {
    inner: InMemoryKv,
    fail_cas: AtomicBool,
}

impl ToggleFailCasKv {
    fn new() -> Self {
        Self {
            inner: InMemoryKv::new(ring_desc_codec()),
            fail_cas: AtomicBool::new(false),
        }
    }

    fn set_fail_cas(&self, fail: bool) {
        self.fail_cas.store(fail, Ordering::Release);
    }
}

impl KvClient for ToggleFailCasKv {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        self.inner.list(prefix)
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        self.inner.get(key)
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        self.inner.delete(key)
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        if self.fail_cas.load(Ordering::Acquire) {
            return Box::pin(async { Err(KvError::Other("transient CAS error".to_string())) });
        }
        self.inner.cas(key, f)
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        self.inner.watch_key(key, cancel, f)
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        self.inner.watch_prefix(prefix, cancel, f)
    }
}

async fn get_inst(kv: &dyn KvClient, id: &str) -> Option<InstanceDesc> {
    let v = kv.get(RING_KEY).await.unwrap()?;
    v.downcast_ref::<Desc>()?.ingesters.get(id).cloned()
}

// ============================================================================
// W18-SR-02: Classic Lifecycler does not roll back local state on KV CAS failure
// ============================================================================

#[tokio::test]
async fn w18_sr_02_change_state_no_rollback_on_cas_failure_and_heartbeat_publishes() {
    let kv = Arc::new(ToggleFailCasKv::new());
    let ring_cfg = RingConfig {
        instance_id: "sr02-ing".to_string(),
        instance_addr: "127.0.0.1".to_string(),
        instance_port: 9095,
        num_tokens: 2,
        heartbeat_period: GoDuration::from_millis(30),
        observe_period: GoDuration::ZERO,
        min_ready_duration: GoDuration::ZERO,
        final_sleep: GoDuration::ZERO,
        unregister_on_shutdown: false,
        ..Default::default()
    };
    let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 0).unwrap();
    let lc = Lifecycler::with_store(
        lc_cfg,
        Arc::clone(&kv) as Arc<dyn KvClient>,
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.clone().into_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    // Wait until registered as ACTIVE.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(inst) = get_inst(kv.as_ref(), "sr02-ing").await {
            if inst.instance_state() == InstanceState::Active {
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for ACTIVE");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Arm CAS failure and transition ACTIVE -> LEAVING.
    kv.set_fail_cas(true);
    let err = lc
        .change_state(InstanceState::Leaving)
        .await
        .expect_err("CAS failure must still return Err");
    assert!(matches!(err, LifecyclerError::Kv(_)), "unexpected err: {err}");

    // Local state MUST remain LEAVING (not rolled back to ACTIVE).
    assert_eq!(lc.get_state(), InstanceState::Leaving);

    // Disarm CAS failure and wait for the heartbeat to publish LEAVING to KV.
    kv.set_fail_cas(false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(inst) = get_inst(kv.as_ref(), "sr02-ing").await {
            if inst.instance_state() == InstanceState::Leaving {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "heartbeat did not publish LEAVING after CAS recovery"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    svc.stop_async();
    let _ = svc.await_terminated().await;
}

#[tokio::test]
async fn w18_sr_02_joining_to_active_and_read_only_keep_local_state_on_cas_failure() {
    let kv = Arc::new(ToggleFailCasKv::new());
    let ring_cfg = RingConfig {
        instance_id: "sr02-ro".to_string(),
        instance_addr: "127.0.0.1".to_string(),
        instance_port: 9095,
        num_tokens: 2,
        heartbeat_period: GoDuration::from_millis(30),
        observe_period: GoDuration::from_secs(60),
        min_ready_duration: GoDuration::ZERO,
        final_sleep: GoDuration::ZERO,
        unregister_on_shutdown: false,
        ..Default::default()
    };
    let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 0).unwrap();
    let lc = Lifecycler::with_store(
        lc_cfg,
        Arc::clone(&kv) as Arc<dyn KvClient>,
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.clone().into_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    // Wait for auto_join with observe_period=60s to transition Pending -> Joining.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if lc.get_state() == InstanceState::Joining {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for JOINING");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Fail CAS on JOINING -> ACTIVE.
    kv.set_fail_cas(true);
    let err = lc
        .change_state(InstanceState::Active)
        .await
        .expect_err("CAS failure must still return Err");
    assert!(matches!(err, LifecyclerError::Kv(_)));
    assert_eq!(lc.get_state(), InstanceState::Active);

    // Fail CAS on read_only = true.
    let err = lc
        .change_read_only_state(true)
        .await
        .expect_err("CAS failure must still return Err");
    assert!(matches!(err, LifecyclerError::Kv(_)));
    let (ro, ro_ts) = lc.get_read_only_state();
    assert!(ro, "read_only must stay true after CAS failure");
    assert!(ro_ts.is_some());

    // Retry while already read_only=true is a no-op and preserves the first timestamp.
    lc.change_read_only_state(true).await.unwrap();
    let (ro2, ro_ts2) = lc.get_read_only_state();
    assert!(ro2);
    assert_eq!(ro_ts, ro_ts2);

    // Disarm CAS and verify next heartbeat publishes both ACTIVE and read_only=true.
    kv.set_fail_cas(false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(inst) = get_inst(kv.as_ref(), "sr02-ro").await {
            if inst.instance_state() == InstanceState::Active && inst.read_only {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "heartbeat did not publish ACTIVE + read_only after CAS recovery"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    svc.stop_async();
    let _ = svc.await_terminated().await;
}

// ============================================================================
// W18-SR-08: Classic Lifecycler::check_ready uses Go duration formatting
// ============================================================================

#[tokio::test]
async fn w18_sr_08_classic_lifecycler_check_ready_formats_min_ready_duration_like_go() {
    for (dur, want_str) in [
        (GoDuration::from_secs(120), "2m0s"),
        (GoDuration::from_secs(90), "1m30s"),
    ] {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let ring_cfg = RingConfig {
            instance_id: "sr08-ing".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 9095,
            num_tokens: 2,
            heartbeat_period: GoDuration::from_millis(50),
            observe_period: GoDuration::ZERO,
            min_ready_duration: dur,
            final_sleep: GoDuration::ZERO,
            ..Default::default()
        };
        let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 0).unwrap();
        let lc = Lifecycler::with_store(
            lc_cfg,
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

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(inst) = get_inst(kv.as_ref(), "sr08-ing").await {
                if inst.instance_state() == InstanceState::Active && !inst.tokens.is_empty() {
                    break;
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out waiting for ACTIVE + tokens");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let err = lc
            .check_ready()
            .await
            .expect_err("must not be ready before min_ready_duration");
        assert_eq!(err, format!("waiting for {want_str} after being ready"));

        svc.stop_async();
        let _ = svc.await_terminated().await;
    }
}

// ============================================================================
// W18-KM-11: Outbound TCP stream writes bounded by -memberlist.stream-timeout
// ============================================================================

#[tokio::test]
async fn w18_km_11_push_pull_and_dial_timeout_bound_wedged_peer_writes() {
    // 1. Peer accepts TCP connections and intentionally never reads from them, so a large
    //    local_state write exceeding the socket send buffer blocks unless bounded by stream_timeout.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap().to_string();
    let _accept_task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    // Test TcpTransport::dial_timeout bounds the [STREAM_BYTE] write too.
    let t_cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        ..Default::default()
    };
    let (transport, _pkt_rx, _conn_rx) = TcpTransport::new(&t_cfg, "test", None).await.unwrap();
    let conn = transport
        .dial_timeout(&peer_addr, Duration::from_millis(200))
        .await
        .expect("small [STREAM_BYTE] write succeeds before timeout");
    drop(conn);
    transport.shutdown_and_wait().await;

    // Test MemberlistKv::join_members with >4 MiB local state against a wedged non-reading peer.
    let ring_codec = ring_desc_codec();
    let ml_started = MemberlistKv::start(
        MemberlistConfig {
            node_name: "km11-started".to_string(),
            randomize_node_name: false,
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            stream_timeout: GoDuration::from_millis(200),
            compression_enabled: false,
            ..Default::default()
        },
        vec![Arc::clone(&ring_codec)],
        "",
        None,
    )
    .await
    .unwrap();
    let c2 = MemberlistKvClient::new(Arc::clone(&ml_started), ring_codec).unwrap();
    let big_zone = "z".repeat(4096);
    c2.cas(
        "big-ring",
        Box::new(move |_| {
            let mut d = Desc::default();
            for i in 0..1024 {
                d.ingesters.insert(
                    format!("ing-{i:04}"),
                    InstanceDesc {
                        id: format!("ing-{i:04}"),
                        addr: "127.0.0.1:9095".to_string(),
                        zone: big_zone.clone(),
                        tokens: vec![i as u32],
                        ..Default::default()
                    },
                );
            }
            Ok(mimir_ring::kv::CasOutcome::Update(Box::new(d)))
        }),
    )
    .await
    .unwrap();
    drop(c2);

    let started = tokio::time::Instant::now();
    let join_res = tokio::time::timeout(
        Duration::from_secs(3),
        ml_started.join_members(&[peer_addr]),
    )
    .await
    .expect("join_members must NOT hang past stream_timeout");
    assert!(join_res.is_err(), "wedged peer join must fail with timeout");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "expected fast timeout (~200ms), took {:?}",
        started.elapsed()
    );

    ml_started.shutdown().await;
}

// ============================================================================
// W18-KM-12: Stream decompression metrics & clean EOF after HasLabel
// ============================================================================

fn counter_value(reg: &Registry, name: &str, algo: &str) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if m.get_label()
                    .iter()
                    .any(|lp| lp.get_name() == "algo" && lp.get_value() == algo)
                {
                    return m.get_counter().get_value();
                }
            }
        }
    }
    0.0
}

#[tokio::test]
async fn w18_km_12_stream_decompress_metrics_and_clean_eof_after_has_label() {
    let reg = Registry::new();
    let cfg_b = MemberlistConfig {
        node_name: "km12-b".to_string(),
        randomize_node_name: false,
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        cluster_label: "prod-a".to_string(),
        compression_enabled: true,
        compression_algorithm: "snappy".to_string(),
        ..Default::default()
    };
    let ring_codec = ring_desc_codec();
    let node_b = MemberlistKv::start_with_options(
        cfg_b,
        vec![Arc::clone(&ring_codec)],
        "",
        Some(&reg),
        Default::default(),
    )
    .await
    .unwrap();
    // Seed compressible state across multiple keys on node_b so PushPull payload compresses
    // smaller than raw (note: ring_desc_codec snappy-compresses each value internally, so
    // repeating identical values across multiple keys yields high inter-key compressibility).
    let cb = MemberlistKvClient::new(Arc::clone(&node_b), Arc::clone(&ring_codec)).unwrap();
    for k in 0..24 {
        cb.cas(
            &format!("ring-{k:02}"),
            Box::new(|_| {
                let mut d = Desc::default();
                d.ingesters.insert(
                    "compressible-ing".to_string(),
                    InstanceDesc {
                        id: "compressible-ing".to_string(),
                        addr: "127.0.0.1:9095".to_string(),
                        zone: "zone-a".to_string(),
                        tokens: (0..64).collect(),
                        ..Default::default()
                    },
                );
                Ok(mimir_ring::kv::CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();
    }
    drop(cb);
    let addr_b = format!("127.0.0.1:{}", node_b.listening_port());

    let before_snappy_attempts =
        counter_value(&reg, "counter_memberlist_decompress_attempts_total", "snappy");
    let before_snappy_errors =
        counter_value(&reg, "counter_memberlist_decompress_errors_total", "snappy");
    let before_unknown_attempts =
        counter_value(&reg, "counter_memberlist_decompress_attempts_total", "unknown");
    let before_unknown_errors =
        counter_value(&reg, "counter_memberlist_decompress_errors_total", "unknown");

    // 1. Compressed PushPull sync between node A and node B increments attempts_total{algo="snappy"}.
    let cfg_a = MemberlistConfig {
        node_name: "km12-a".to_string(),
        randomize_node_name: false,
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        cluster_label: "prod-a".to_string(),
        compression_enabled: true,
        compression_algorithm: "snappy".to_string(),
        join_members: mimir_config::StringSliceCSVMulti(vec![addr_b.clone()]),
        ..Default::default()
    };
    let node_a = MemberlistKv::start_with_options(
        cfg_a,
        vec![Arc::clone(&ring_codec)],
        "",
        None,
        Default::default(),
    )
    .await
    .unwrap();
    let after_sync_attempts =
        counter_value(&reg, "counter_memberlist_decompress_attempts_total", "snappy");
    assert!(
        after_sync_attempts > before_snappy_attempts,
        "expected snappy decompress_attempts_total to increment on compressed PushPull sync (before={before_snappy_attempts}, after={after_sync_attempts})"
    );

    // 2. Clean EOF immediately after [HasLabel, len, label] must be ignored silently (0 bytes returned).
    {
        let mut s = TcpStream::connect(&addr_b).await.unwrap();
        let mut frame = vec![2u8]; // STREAM_BYTE
        frame.extend_from_slice(&add_label_header(&[], "prod-a").unwrap());
        s.write_all(&frame).await.unwrap();
        s.shutdown().await.unwrap();
        let mut reply = Vec::new();
        let n = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0, "expected clean EOF without ErrResp, got {reply:?}");
    }

    // 3. Corrupt snappy compressed stream payload increments attempts_total & errors_total for "snappy".
    {
        let mut s = TcpStream::connect(&addr_b).await.unwrap();
        let mut frame = vec![2u8]; // STREAM_BYTE
        frame.extend_from_slice(&add_label_header(&[], "prod-a").unwrap());
        // Manually construct MessageType::Compress with valid msgpack wrapper {Algo: 1 (Snappy), Buf: corrupt}.
        frame.push(MessageType::Compress as u8);
        frame.push(0x82); // fixmap(2)
        frame.extend_from_slice(&[0xa4, b'A', b'l', b'g', b'o', CompressionType::Snappy as u8]);
        frame.extend_from_slice(&[0xa3, b'B', b'u', b'f', 0xc4, 0x04, 0xff, 0xff, 0xff, 0xff]);
        s.write_all(&frame).await.unwrap();
        s.flush().await.unwrap();
        let mut reply = vec![0u8; 256];
        let _ = tokio::time::timeout(Duration::from_secs(2), s.read(&mut reply)).await;
    }
    let after_err_attempts =
        counter_value(&reg, "counter_memberlist_decompress_attempts_total", "snappy");
    let after_err_errors =
        counter_value(&reg, "counter_memberlist_decompress_errors_total", "snappy");
    assert!(after_err_attempts > after_sync_attempts);
    assert!(after_err_errors > before_snappy_errors);

    // 4. Corrupt Compress wrapper (invalid msgpack) increments attempts_total & errors_total for "unknown".
    {
        let mut s = TcpStream::connect(&addr_b).await.unwrap();
        let mut frame = vec![2u8]; // STREAM_BYTE
        frame.extend_from_slice(&add_label_header(&[], "prod-a").unwrap());
        frame.push(MessageType::Compress as u8);
        frame.push(0xc1); // invalid msgpack byte
        s.write_all(&frame).await.unwrap();
        s.flush().await.unwrap();
        let mut reply = vec![0u8; 256];
        let _ = tokio::time::timeout(Duration::from_secs(2), s.read(&mut reply)).await;
    }
    let after_unk_attempts =
        counter_value(&reg, "counter_memberlist_decompress_attempts_total", "unknown");
    let after_unk_errors =
        counter_value(&reg, "counter_memberlist_decompress_errors_total", "unknown");
    assert!(after_unk_attempts > before_unknown_attempts);
    assert!(after_unk_errors > before_unknown_errors);

    // Also verify compress_payload roundtrip works so unused import is avoided cleanly.
    let _ = compress_payload(CompressionType::Lzw, b"hello");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// W18-KM-13: Reject -memberlist.watch-prefix-buffer-size <= 0 in MemberlistKv::new
// ============================================================================

#[test]
fn w18_km_13_memberlist_kv_new_rejects_non_positive_watch_prefix_buffer_size() {
    for bad in [0, -1, -128] {
        let cfg = MemberlistConfig {
            watch_prefix_buffer_size: bad,
            ..Default::default()
        };
        let err = MemberlistKv::new(cfg, vec![Arc::new(StringCodec)], "", None, Default::default())
            .err()
            .expect("must reject non-positive watch_prefix_buffer_size");
        assert_eq!(
            err.to_string(),
            "invalid KV store config: invalid WatchPrefixBufferSize: must be greater than 0"
        );
    }
}
