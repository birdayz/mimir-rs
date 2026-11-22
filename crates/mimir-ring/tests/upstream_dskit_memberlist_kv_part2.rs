//! Ported from github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go (remaining 28 tests).

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use mimir_common::services::{start_and_await_running, ServiceContext};
use mimir_config::server::{MemberlistConfig, ZoneAwareRoutingConfig};
use mimir_config::GoDuration;
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use mimir_proto::ringpb::{Desc as RingDesc, InstanceDesc, InstanceState};
use mimir_ring::dns::{DnsError, DnsProvider};
use mimir_ring::kv::memberlist::gofmt::GoJson;
use mimir_ring::kv::memberlist::kv_init_service::KvInitService;
use mimir_ring::kv::memberlist::mergeable::Mergeable;
use mimir_ring::kv::memberlist::wire::CompressionType;
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::{get_typed, CasOutcome, Codec, KvClient, KvError};
use mimir_ring::ring::model::ring_desc_codec;
use parking_lot::{Mutex, RwLock};
use prometheus::{Encoder, Registry, TextEncoder};
use rand::Rng;

const KEY: &str = "test";

fn localhost_cfg(name: &str) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: name.to_string(),
        ..Default::default()
    }
}

fn default_kv_cfg(i: usize) -> MemberlistConfig {
    MemberlistConfig {
        node_name: format!("Member-{i}"),
        randomize_node_name: true,
        gossip_interval: GoDuration::from_millis(100),
        gossip_nodes: 10,
        pull_push_interval: GoDuration::from_secs(1),
        obsolete_entries_timeout: GoDuration::from_secs(5),
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        ..Default::default()
    }
}

fn generate_tokens(n: usize) -> Vec<u32> {
    let mut rng = rand::thread_rng();
    (0..n).map(|_| rng.gen()).collect()
}

fn add_or_activate_member(name: &str, d: Option<&RingDesc>) -> RingDesc {
    let mut r = d.cloned().unwrap_or_default();
    let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let entry = r.ingesters.entry(name.to_string()).or_insert_with(|| InstanceDesc {
        timestamp: now_s,
        tokens: generate_tokens(128),
        state: InstanceState::Joining as i32,
        ..Default::default()
    });
    if entry.state == InstanceState::Joining as i32 && !entry.tokens.is_empty() {
        entry.state = InstanceState::Active as i32;
    }
    entry.timestamp = now_s;
    r
}

async fn cas_ring<F>(client: &MemberlistKvClient, key: &str, mut f: F) -> Result<(), KvError>
where
    F: FnMut(Option<&RingDesc>) -> Result<Option<(RingDesc, bool)>, mimir_ring::kv::CasCallbackError>
        + Send
        + 'static,
{
    client
        .cas(
            key,
            Box::new(move |cur| {
                let typed = cur.and_then(|a| a.downcast_ref::<RingDesc>());
                match f(typed)? {
                    None => Ok(CasOutcome::NoChange),
                    Some((v, true)) => Ok(CasOutcome::Update(Box::new(v))),
                    Some((v, false)) => Ok(CasOutcome::UpdateOnce(Box::new(v))),
                }
            }),
        )
        .await
}

fn marshal_kvp(key: &str, codec: &dyn Codec, value: &RingDesc) -> Vec<u8> {
    let encoded = codec.encode(value).unwrap();
    KeyValuePair {
        key: key.to_string(),
        value: Bytes::from(encoded),
        codec: codec.codec_id().to_string(),
        deleted: false,
        update_time_millis: 0,
    }
    .encode_to_vec()
}

fn decode_kvp_ring(bytes: &[u8], expected_key: &str, codec: &dyn Codec) -> RingDesc {
    let kvp = KeyValuePair::decode(bytes).unwrap();
    assert_eq!(expected_key, kvp.key);
    let any = codec.decode(&kvp.value).unwrap();
    any.downcast_ref::<RingDesc>().unwrap().clone()
}

#[derive(Clone, Default)]
struct MockDns {
    mapping: Arc<RwLock<HashMap<String, Vec<String>>>>,
    resolved: Arc<Mutex<Vec<String>>>,
}

impl MockDns {
    fn set(&self, key: &str, addrs: Vec<String>) {
        self.mapping.write().insert(key.to_string(), addrs);
    }

    fn clear(&self) {
        self.mapping.write().clear();
        self.resolved.lock().clear();
    }
}

impl DnsProvider for MockDns {
    fn resolve<'a>(&'a self, addrs: &'a [String]) -> BoxFuture<'a, Result<(), DnsError>> {
        async move {
            let map = self.mapping.read();
            let mut out = Vec::new();
            for a in addrs {
                if let Some(list) = map.get(a) {
                    out.extend(list.iter().cloned());
                }
            }
            *self.resolved.lock() = out;
            Ok(())
        }
        .boxed()
    }

    fn addresses(&self) -> Vec<String> {
        self.resolved.lock().clone()
    }
}

fn free_ports(count: usize) -> Vec<i64> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port() as i64)
        .collect()
}

// ============================================================================
// Tests 1..10
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestKV_BuildMemberlistConfig_CompressionAlgorithm
#[test]
fn test_kv_build_memberlist_config_compression_algorithm() {
    let cases = [
        ("empty propagates literally", "", "", CompressionType::Lzw),
        ("lzw propagates", "lzw", "lzw", CompressionType::Lzw),
        ("snappy propagates", "snappy", "snappy", CompressionType::Snappy),
    ];
    for (name, configured, expected_cfg, expected_wire) in cases {
        let mut cfg = localhost_cfg("comp-test");
        cfg.compression_algorithm = configured.to_string();
        let kv = MemberlistKv::new(cfg, Vec::new(), "", None, KvOptions::default()).unwrap();
        assert_eq!(expected_cfg, kv.configured_compression_algorithm(), "{name}");
        assert_eq!(expected_wire, kv.wire_compression_type(), "{name}");
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestBasicGetAndCas
#[tokio::test]
async fn test_basic_get_and_cas() {
    let codec = ring_desc_codec();
    let kv = MemberlistKv::start(localhost_cfg("basic"), vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    assert!(client.get(KEY).await.unwrap().is_none());

    let name = "Ing 1";
    cas_ring(&client, KEY, move |d| {
        let mut r = d.cloned().unwrap_or_default();
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        r.ingesters.insert(
            name.to_string(),
            InstanceDesc {
                timestamp: now_s,
                tokens: generate_tokens(128),
                state: InstanceState::Pending as i32,
                ..Default::default()
            },
        );
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    let r = get_typed::<RingDesc>(&client, KEY).await.unwrap().unwrap();
    assert!(r.ingesters[name].timestamp != 0);
    assert!(!r.ingesters[name].tokens.is_empty());
    assert!(client.get("other key").await.unwrap().is_none());

    cas_ring(&client, KEY, move |d| {
        let mut r = d.cloned().unwrap();
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let e = r.ingesters.get_mut(name).unwrap();
        e.state = InstanceState::Active as i32;
        e.timestamp = now_s;
        Ok(Some((r, true)))
    })
    .await
    .unwrap();
    let r = get_typed::<RingDesc>(&client, KEY).await.unwrap().unwrap();
    assert_eq!(InstanceState::Active as i32, r.ingesters[name].state);

    cas_ring(&client, KEY, move |d| {
        let mut r = d.cloned().unwrap();
        r.ingesters.remove(name);
        Ok(Some((r, true)))
    })
    .await
    .unwrap();
    let (m, _) = kv.get_mergeable(KEY).unwrap();
    let r = m.as_any().downcast_ref::<RingDesc>().unwrap();
    assert_eq!(InstanceState::Left as i32, r.ingesters[name].state);

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestCASNoChangeShortTimeout
#[tokio::test]
async fn test_cas_no_change_short_timeout() {
    let codec = ring_desc_codec();
    let kv = MemberlistKv::start(localhost_cfg("nochange-to"), vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    cas_ring(&client, KEY, |d| {
        let mut r = d.cloned().unwrap_or_default();
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        r.ingesters.insert(
            "hello".to_string(),
            InstanceDesc {
                timestamp: now_s,
                tokens: generate_tokens(128),
                state: InstanceState::Joining as i32,
                ..Default::default()
            },
        );
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = Arc::clone(&calls);
    let res = tokio::time::timeout(
        Duration::from_millis(500),
        cas_ring(&client, KEY, move |d| {
            calls_c.fetch_add(1, Ordering::SeqCst);
            Ok(Some((d.cloned().unwrap(), true)))
        }),
    )
    .await;
    assert!(res.is_err(), "expected timeout while retrying no-change");
    assert_eq!(1, calls.load(Ordering::SeqCst));

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestCASFailedBecauseOfVersionChanges
#[tokio::test]
async fn test_cas_failed_because_of_version_changes() {
    let codec = ring_desc_codec();
    let kv = MemberlistKv::start(localhost_cfg("ver-chg"), vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());

    cas_ring(&client, KEY, |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("nonempty".into(), InstanceDesc { timestamp: 1, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = Arc::clone(&calls);
    let inner_kv = Arc::clone(&kv);
    let inner_codec = Arc::clone(&codec);
    let err = kv
        .cas_with_codec(
            KEY,
            codec.as_ref(),
            Box::new(move |cur| {
                let n = calls_c.fetch_add(1, Ordering::SeqCst) + 1;
                let mut r = cur.unwrap().downcast_ref::<RingDesc>().unwrap().clone();
                let mut d_inner = r.clone();
                d_inner.ingesters.insert(
                    format!("{n}"),
                    InstanceDesc { timestamp: n as i64, ..Default::default() },
                );
                let mut m: Box<dyn Mergeable> = Box::new(d_inner);
                let _ = inner_kv.merge_value_for_key_for_test(
                    KEY,
                    m.as_mut(),
                    0,
                    inner_codec.codec_id(),
                    false,
                    Some(SystemTime::now()),
                );
                r.ingesters.insert("world".into(), InstanceDesc { timestamp: 99, ..Default::default() });
                Ok(CasOutcome::Update(Box::new(r)))
            }),
        )
        .await
        .unwrap_err();

    assert_eq!(
        "failed to CAS-update key test: too many retries: version mismatch",
        err.to_string()
    );
    assert_eq!(10, calls.load(Ordering::SeqCst));

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestCASRetryBackoff
#[tokio::test]
async fn test_cas_retry_backoff() {
    let codec = ring_desc_codec();
    let mut cfg = localhost_cfg("retry-backoff");
    cfg.cas_retry_min_backoff = GoDuration::from_millis(5);
    cfg.cas_retry_max_backoff = GoDuration::from_millis(20);
    let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    cas_ring(&client, KEY, |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("nonempty".into(), InstanceDesc { timestamp: 1, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = Arc::clone(&calls);
    let inner_kv = Arc::clone(&kv);
    let inner_codec = Arc::clone(&codec);
    let start = Instant::now();
    let err = kv
        .cas_with_codec(
            KEY,
            codec.as_ref(),
            Box::new(move |cur| {
                let n = calls_c.fetch_add(1, Ordering::SeqCst) + 1;
                let mut r = cur.unwrap().downcast_ref::<RingDesc>().unwrap().clone();
                let mut d_inner = r.clone();
                d_inner.ingesters.insert(
                    format!("{n}"),
                    InstanceDesc { timestamp: n as i64, ..Default::default() },
                );
                let mut m: Box<dyn Mergeable> = Box::new(d_inner);
                let _ = inner_kv.merge_value_for_key_for_test(
                    KEY,
                    m.as_mut(),
                    0,
                    inner_codec.codec_id(),
                    false,
                    Some(SystemTime::now()),
                );
                r.ingesters.insert("world".into(), InstanceDesc { timestamp: 99, ..Default::default() });
                Ok(CasOutcome::Update(Box::new(r)))
            }),
        )
        .await
        .unwrap_err();
    let elapsed = start.elapsed();

    assert_eq!(
        "failed to CAS-update key test: too many retries: version mismatch",
        err.to_string()
    );
    assert_eq!(10, calls.load(Ordering::SeqCst));
    assert!(
        elapsed >= Duration::from_millis(9 * 5),
        "expected >= 45ms backoff, got {elapsed:?}"
    );

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMultipleCAS
#[tokio::test]
async fn test_multiple_cas() {
    let codec = ring_desc_codec();
    let opts = KvOptions {
        max_cas_retries: Some(20),
        ..Default::default()
    };
    let kv = MemberlistKv::start_with_options(
        localhost_cfg("multi-cas"),
        vec![Arc::clone(&codec)],
        "",
        None,
        opts,
    )
    .await
    .unwrap();
    let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());

    const MEMBERS: usize = 10;
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut handles = Vec::new();
    for i in 0..MEMBERS {
        let c = Arc::clone(&client);
        let n = Arc::clone(&notify);
        let name = format!("Member-{i}");
        handles.push(tokio::spawn(async move {
            n.notified().await;
            let n1 = name.clone();
            cas_ring(&c, KEY, move |d| {
                let mut r = d.cloned().unwrap_or_default();
                let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                r.ingesters.insert(
                    n1.clone(),
                    InstanceDesc {
                        timestamp: now_s,
                        tokens: generate_tokens(128),
                        state: InstanceState::Joining as i32,
                        ..Default::default()
                    },
                );
                Ok(Some((r, true)))
            })
            .await
            .unwrap();

            let n2 = name.clone();
            cas_ring(&c, KEY, move |d| {
                let mut r = d.cloned().unwrap();
                let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                if let Some(e) = r.ingesters.get_mut(&n2) {
                    e.state = InstanceState::Active as i32;
                    e.timestamp = now_s;
                }
                Ok(Some((r, true)))
            })
            .await
            .unwrap();
        }));
    }
    tokio::task::yield_now().await;
    notify.notify_waiters();
    for h in handles {
        h.await.unwrap();
    }

    let r = get_typed::<RingDesc>(&*client, KEY).await.unwrap().unwrap();
    for i in 0..MEMBERS {
        let name = format!("Member-{i}");
        assert_eq!(InstanceState::Active as i32, r.ingesters[&name].state);
    }

    let notify2 = Arc::new(tokio::sync::Notify::new());
    let mut handles2 = Vec::new();
    for i in 0..MEMBERS {
        let c = Arc::clone(&client);
        let n = Arc::clone(&notify2);
        let name = format!("Member-{i}");
        handles2.push(tokio::spawn(async move {
            n.notified().await;
            let n1 = name.clone();
            cas_ring(&c, KEY, move |d| {
                let mut r = d.cloned().unwrap();
                r.ingesters.remove(&n1);
                Ok(Some((r, true)))
            })
            .await
            .unwrap();
        }));
    }
    tokio::task::yield_now().await;
    notify2.notify_waiters();
    for h in handles2 {
        h.await.unwrap();
    }

    let (m, _) = kv.get_mergeable(KEY).unwrap();
    let r = m.as_any().downcast_ref::<RingDesc>().unwrap();
    for i in 0..MEMBERS {
        let name = format!("Member-{i}");
        assert_eq!(InstanceState::Left as i32, r.ingesters[&name].state);
    }

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestDeleteMultipleClients
#[tokio::test]
async fn test_delete_multiple_clients() {
    let delete_time = Duration::from_millis(400);
    let codec = ring_desc_codec();
    let mut cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        gossip_nodes: 1,
        gossip_interval: GoDuration::from_millis(50),
        pull_push_interval: GoDuration(delete_time.as_nanos() as i64),
        obsolete_entries_timeout: GoDuration(delete_time.as_nanos() as i64),
        ..Default::default()
    };

    let kv1 = MemberlistKv::start(cfg.clone(), vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let c1 = MemberlistKvClient::new(Arc::clone(&kv1), Arc::clone(&codec)).unwrap();
    cas_ring(&c1, KEY, |d| Ok(Some((add_or_activate_member("entry", d), true))))
        .await
        .unwrap();

    cfg.join_members.0 = vec![format!("127.0.0.1:{}", kv1.listening_port())];
    let kv2 = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let c2 = MemberlistKvClient::new(Arc::clone(&kv2), Arc::clone(&codec)).unwrap();

    assert!(c2.get(KEY).await.unwrap().is_some());
    c1.delete(KEY).await.unwrap();

    for c in [&c1, &c2] {
        let mut gone = false;
        for _ in 0..200 {
            if c.get(KEY).await.unwrap().is_none() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(gone, "expected key to be deleted on both clients");
    }

    kv2.shutdown().await;
    kv1.shutdown().await;
}

async fn run_multi_clients_cluster<F>(members: usize, mut config_gen: F) -> Result<(), String>
where
    F: FnMut(usize) -> MemberlistConfig,
{
    let codec = ring_desc_codec();
    let mut clients = Vec::new();
    let mut kvs = Vec::new();
    let mut prev_port = 0u16;

    for i in 0..members {
        let cfg = config_gen(i);
        let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
            .await
            .map_err(|e| e.to_string())?;
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());
        if prev_port > 0 {
            let _ = kv.join_members(&[format!("127.0.0.1:{prev_port}")]).await;
        }
        prev_port = kv.listening_port();
        kvs.push(kv);
        clients.push(client);
    }

    let stop = ServiceContext::background();
    let mut worker_handles = Vec::new();
    for (i, client) in clients.iter().enumerate() {
        let c = Arc::clone(client);
        let s = stop.clone();
        let name = format!("Member-{i}");
        worker_handles.push(tokio::spawn(async move {
            for _ in 0..3 {
                if s.is_canceled() {
                    break;
                }
                let n = name.clone();
                let _ = cas_ring(&c, "ring", move |d| {
                    Ok(Some((add_or_activate_member(&n, d), true)))
                })
                .await;
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }));
    }

    // Observe ring via WatchKey on first client for up to 1.5s.
    let joined_members = Arc::new(AtomicUsize::new(0));
    let jm_c = Arc::clone(&joined_members);
    let w_ctx = ServiceContext::background();
    let w_ctx_c = w_ctx.clone();
    let first_c = Arc::clone(&clients[0]);
    let watch_h = tokio::spawn(async move {
        first_c
            .watch_key(
                "ring",
                w_ctx_c,
                Box::new(move |val| {
                    if let Some(d) = val.downcast_ref::<RingDesc>() {
                        jm_c.store(d.ingesters.len(), Ordering::SeqCst);
                    }
                    true
                }),
            )
            .await;
    });
    tokio::time::sleep(Duration::from_millis(900)).await;
    w_ctx.cancel();
    let _ = watch_h.await;
    stop.cancel();
    for h in worker_handles {
        let _ = h.await;
    }

    let seen = joined_members.load(Ordering::SeqCst);
    let res = if seen <= 1 {
        Err(format!("expected to see at least 2 members, got {seen}"))
    } else {
        let mut last_err = Ok(());
        for _ in 0..80 {
            let mut ok = true;
            let mut ref_tokens: Option<Vec<u32>> = None;
            for (i, c) in clients.iter().enumerate() {
                let Some(r) = get_typed::<RingDesc>(c.as_ref(), "ring").await.unwrap() else {
                    last_err = Err(format!("Member {i} missing ring"));
                    ok = false;
                    break;
                };
                if r.ingesters.len() != members {
                    last_err = Err(format!(
                        "Member {i} has only {} members in the ring",
                        r.ingesters.len()
                    ));
                    ok = false;
                    break;
                }
                if !r.ingesters.values().all(|m| m.state == InstanceState::Active as i32) {
                    last_err = Err(format!("Member {i} has non-active members"));
                    ok = false;
                    break;
                }
                let mut toks: Vec<u32> = r
                    .ingesters
                    .values()
                    .flat_map(|m| m.tokens.iter().copied())
                    .collect();
                toks.sort_unstable();
                match &ref_tokens {
                    None => ref_tokens = Some(toks),
                    Some(exp) if exp != &toks => {
                        last_err = Err(format!("Member {i} tokens mismatch"));
                        ok = false;
                        break;
                    }
                    _ => {}
                }
            }
            if ok {
                last_err = Ok(());
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        last_err
    };

    for kv in kvs {
        kv.shutdown().await;
    }
    res
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMultipleClients
#[tokio::test]
async fn test_multiple_clients() {
    run_multi_clients_cluster(10, default_kv_cfg).await.unwrap();
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMultipleClientsWithMixedLabelsAndClusterLabelVerificationDisabled
#[tokio::test]
async fn test_multiple_clients_with_mixed_labels_and_cluster_label_verification_disabled() {
    let labels = ["", "label1", "label2"];
    run_multi_clients_cluster(labels.len(), |i| {
        let mut cfg = default_kv_cfg(i);
        cfg.cluster_label_verification_disabled = true;
        cfg.cluster_label = labels[i].to_string();
        cfg
    })
    .await
    .unwrap();
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMultipleClientsWithSameLabelWithClusterLabelVerification
#[tokio::test]
async fn test_multiple_clients_with_same_label_with_cluster_label_verification() {
    run_multi_clients_cluster(10, |i| {
        let mut cfg = default_kv_cfg(i);
        cfg.cluster_label = "myTestLabel".to_string();
        cfg
    })
    .await
    .unwrap();
}

// ============================================================================
// Tests 11..20
// ============================================================================

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct DistributedCounter(BTreeMap<String, i64>);

impl Mergeable for DistributedCounter {
    fn merge(
        &mut self,
        other: &mut dyn Mergeable,
        _local_cas: bool,
    ) -> Result<Option<Box<dyn Mergeable>>, String> {
        let Some(odc) = other.as_any().downcast_ref::<DistributedCounter>() else {
            return Err("invalid thing to merge".to_string());
        };
        let mut updated = DistributedCounter::default();
        for (k, &v) in &odc.0 {
            let cur = self.0.get(k).copied().unwrap_or(0);
            if v > cur {
                self.0.insert(k.clone(), v);
                updated.0.insert(k.clone(), v);
            }
        }
        if updated.0.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Box::new(updated)))
        }
    }
    fn merge_content(&self) -> Vec<String> {
        self.0.keys().cloned().collect()
    }
    fn remove_tombstones(&mut self, _: Option<SystemTime>) -> (usize, usize) {
        (0, 0)
    }
    fn clone_mergeable(&self) -> Box<dyn Mergeable> {
        Box::new(self.clone())
    }
    fn eq_mergeable(&self, other: &dyn Mergeable) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
    fn to_go_json(&self) -> GoJson {
        GoJson::Null
    }
    fn go_string(&self) -> String {
        format!("{self:?}")
    }
    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}

struct DistributedCounterCodec;
impl Codec for DistributedCounterCodec {
    fn codec_id(&self) -> &str {
        "distributedCounter"
    }
    fn decode(&self, bytes: &[u8]) -> Result<Box<dyn Any + Send + Sync>, KvError> {
        let dc: DistributedCounter =
            serde_json::from_slice(bytes).map_err(|e| KvError::Codec(e.to_string()))?;
        Ok(Box::new(dc))
    }
    fn encode(&self, msg: &(dyn Any + Send + Sync)) -> Result<Vec<u8>, KvError> {
        let dc = msg
            .downcast_ref::<DistributedCounter>()
            .ok_or_else(|| KvError::InvalidType("DistributedCounter".into()))?;
        serde_json::to_vec(dc).map_err(|e| KvError::Codec(e.to_string()))
    }
    fn decode_mergeable(&self, bytes: &[u8]) -> Result<Box<dyn Mergeable>, KvError> {
        let dc: DistributedCounter =
            serde_json::from_slice(bytes).map_err(|e| KvError::Codec(e.to_string()))?;
        Ok(Box::new(dc))
    }
    fn encode_mergeable(&self, msg: &dyn Mergeable) -> Result<Vec<u8>, KvError> {
        let dc = msg
            .as_any()
            .downcast_ref::<DistributedCounter>()
            .ok_or_else(|| KvError::InvalidType("DistributedCounter".into()))?;
        serde_json::to_vec(dc).map_err(|e| KvError::Codec(e.to_string()))
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMultipleCodecs
#[tokio::test]
async fn test_multiple_codecs() {
    let c1_codec = ring_desc_codec();
    let c2_codec: Arc<dyn Codec> = Arc::new(DistributedCounterCodec);
    let kv = MemberlistKv::start(
        localhost_cfg("multi-codecs"),
        vec![Arc::clone(&c1_codec), Arc::clone(&c2_codec)],
        "",
        None,
    )
    .await
    .unwrap();
    let kv1 = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&c1_codec)).unwrap();
    let kv2 = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&c2_codec)).unwrap();

    cas_ring(&kv1, "data", |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert(
            "test".into(),
            InstanceDesc {
                timestamp: 100,
                state: InstanceState::Active as i32,
                ..Default::default()
            },
        );
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    kv2.cas(
        "counter",
        Box::new(|cur| {
            let mut dc = cur
                .and_then(|a| a.downcast_ref::<DistributedCounter>())
                .cloned()
                .unwrap_or_default();
            dc.0.insert("test".into(), 5);
            Ok(CasOutcome::Update(Box::new(dc)))
        }),
    )
    .await
    .unwrap();

    let got_data = get_typed::<RingDesc>(&kv1, "data").await.unwrap().unwrap();
    assert_eq!(InstanceState::Active as i32, got_data.ingesters["test"].state);
    let got_counter = get_typed::<DistributedCounter>(&kv2, "counter")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(5, got_counter.0["test"]);

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestNotifyMsgResendsOnlyChanges
#[tokio::test]
async fn test_notify_msg_resends_only_changes() {
    let codec = ring_desc_codec();
    let mut cfg = localhost_cfg("notify-resends");
    cfg.retransmit_factor = 1;
    let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();

    assert_eq!(0, kv.delegate_get_broadcasts(0, i32::MAX as usize).len());

    let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    cas_ring(&client, KEY, move |_| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: now_s, state: InstanceState::Joining as i32, ..Default::default() });
        r.ingesters.insert("b".into(), InstanceDesc { timestamp: now_s, state: InstanceState::Joining as i32, ..Default::default() });
        Ok(Some((r, true)))
    })
    .await
    .unwrap();

    assert_eq!(1, kv.delegate_get_broadcasts(0, i32::MAX as usize).len());
    assert_eq!(0, kv.delegate_get_broadcasts(0, i32::MAX as usize).len());

    let mut incoming = RingDesc::default();
    incoming.ingesters.insert("a".into(), InstanceDesc { timestamp: now_s - 5, state: InstanceState::Active as i32, ..Default::default() });
    incoming.ingesters.insert("b".into(), InstanceDesc { timestamp: now_s + 5, state: InstanceState::Active as i32, tokens: vec![1, 2, 3], ..Default::default() });
    incoming.ingesters.insert("c".into(), InstanceDesc { timestamp: now_s, state: InstanceState::Active as i32, ..Default::default() });
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &incoming));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let d = get_typed::<RingDesc>(&client, KEY).await.unwrap().unwrap();
    assert_eq!(InstanceState::Joining as i32, d.ingesters["a"].state);
    assert_eq!(vec![1, 2, 3], d.ingesters["b"].tokens);
    assert_eq!(InstanceState::Active as i32, d.ingesters["c"].state);

    let bs = kv.delegate_get_broadcasts(0, i32::MAX as usize);
    assert_eq!(1, bs.len());
    let b_desc = decode_kvp_ring(&bs[0], KEY, codec.as_ref());
    assert!(!b_desc.ingesters.contains_key("a"));
    assert_eq!(vec![1, 2, 3], b_desc.ingesters["b"].tokens);
    assert_eq!(InstanceState::Active as i32, b_desc.ingesters["c"].state);

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestSendingOldTombstoneShouldNotForwardMessage
#[tokio::test]
async fn test_sending_old_tombstone_should_not_forward_message() {
    let codec = ring_desc_codec();
    let mut cfg = localhost_cfg("old-tomb");
    cfg.retransmit_factor = 1;
    cfg.left_ingesters_timeout = GoDuration::from_secs(300);
    let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap();
    assert_eq!(0, kv.delegate_get_broadcasts(0, i32::MAX as usize).len());

    let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let mk = |ts: i64| {
        let mut r = RingDesc::default();
        r.ingesters.insert("instance".into(), InstanceDesc { timestamp: ts, state: InstanceState::Left as i32, ..Default::default() });
        r
    };

    // 1) old tombstone, empty KV -> ignored
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &mk(now_s - 600)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(kv.delegate_get_broadcasts(0, i32::MAX as usize).is_empty());
    let d = get_typed::<RingDesc>(&client, KEY).await.unwrap();
    assert!(d.is_none_or(|x| x.ingesters.is_empty()));

    // 2) recent tombstone, empty KV -> stored and forwarded
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &mk(now_s)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let bs = kv.delegate_get_broadcasts(0, i32::MAX as usize);
    assert_eq!(1, bs.len());
    assert_eq!(now_s, decode_kvp_ring(&bs[0], KEY, codec.as_ref()).ingesters["instance"].timestamp);

    // 3) old tombstone, KV contains tombstone -> ignored
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &mk(now_s - 10)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(kv.delegate_get_broadcasts(0, i32::MAX as usize).is_empty());

    // 4) fresh tombstone, KV contains tombstone -> updated and forwarded
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &mk(now_s + 10)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let bs = kv.delegate_get_broadcasts(0, i32::MAX as usize);
    assert_eq!(1, bs.len());
    assert_eq!(now_s + 10, decode_kvp_ring(&bs[0], KEY, codec.as_ref()).ingesters["instance"].timestamp);

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestDelegateMethodsDontCrashBeforeKVStarts
#[tokio::test]
async fn test_delegate_methods_dont_crash_before_kv_starts() {
    let codec = ring_desc_codec();
    let kv = MemberlistKv::new(
        localhost_cfg("before-start"),
        vec![Arc::clone(&codec)],
        "",
        None,
        KvOptions::default(),
    )
    .unwrap();

    let _ = kv.local_state(true);
    kv.merge_remote_state(&[], true);
    let _ = kv.delegate_get_broadcasts(100, 100);

    let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let mut msg = RingDesc::default();
    msg.ingesters.insert("a".into(), InstanceDesc { timestamp: now_s - 5, state: InstanceState::Active as i32, ..Default::default() });
    msg.ingesters.insert("b".into(), InstanceDesc { timestamp: now_s + 5, state: InstanceState::Active as i32, tokens: vec![1, 2, 3], ..Default::default() });
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &msg));

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(kv.get_value(KEY).is_none());

    start_and_await_running(ServiceContext::background(), &*kv.service())
        .await
        .unwrap();
    kv.notify_msg(&marshal_kvp(KEY, codec.as_ref(), &msg));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (got, _) = kv.get_value(KEY).unwrap();
    let got_desc = got.downcast_ref::<RingDesc>().unwrap();
    assert_eq!(vec![1, 2, 3], got_desc.ingesters["b"].tokens);

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestCleanupObsoleteEntriesStopsKeyWorkers
#[tokio::test]
async fn test_cleanup_obsolete_entries_stops_key_workers() {
    let codec = ring_desc_codec();
    let mut cfg = localhost_cfg("cleanup-workers");
    cfg.obsolete_entries_timeout = GoDuration::from_millis(100);
    let kv = MemberlistKv::new(cfg, vec![Arc::clone(&codec)], "", None, KvOptions::default()).unwrap();

    let enqueue = |k: &str, deleted: bool| {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        let bytes = Bytes::from(codec.encode(&r).unwrap());
        assert!(kv.enqueue_key_update_for_test(k, bytes, Arc::clone(&codec), deleted));
    };

    enqueue(KEY, false);
    for _ in 0..50 {
        if kv.num_key_workers() == 1 && kv.get_value(KEY).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(1, kv.num_key_workers());
    assert!(kv.get_value(KEY).is_some());

    kv.delete_key(KEY).await.unwrap();
    kv.store_age_entry_for_test(KEY, SystemTime::now() - Duration::from_millis(200));
    kv.cleanup_obsolete_entries();
    tokio::task::yield_now().await;
    assert_eq!(0, kv.num_key_workers());

    enqueue(KEY, false);
    for _ in 0..50 {
        if kv.num_key_workers() == 1 && kv.get_value(KEY).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(1, kv.num_key_workers());

    enqueue("already-purged-key", true);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(1, kv.num_key_workers());

    kv.shutdown_workers_for_test();
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestGetBroadcastsPrefersLocalUpdates
#[tokio::test]
async fn test_get_broadcasts_prefers_local_updates() {
    let codec = ring_desc_codec();
    let mut cfg = localhost_cfg("prefer-local");
    cfg.retransmit_factor = 1;
    let reg = Registry::new();
    let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", Some(&reg))
        .await
        .unwrap();

    let small = {
        let mut r = RingDesc::default();
        r.ingesters.insert("a".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        r
    };
    let medium = {
        let mut r = RingDesc::default();
        r.ingesters.insert("d".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        r.ingesters.insert("e".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        r
    };
    let big = {
        let mut r = RingDesc::default();
        r.ingesters.insert("b".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        r.ingesters.insert("c".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        r.ingesters.insert("d".into(), InstanceDesc { timestamp: 1, state: InstanceState::Joining as i32, ..Default::default() });
        r
    };

    assert_eq!(0, kv.delegate_get_broadcasts(0, i32::MAX as usize).len());

    kv.broadcast_new_value("non-local", &small, 1, codec.as_ref(), false, false, Some(SystemTime::now()));
    kv.broadcast_new_value("non-local", &big, 2, codec.as_ref(), false, false, Some(SystemTime::now()));
    kv.broadcast_new_value("local", &small, 1, codec.as_ref(), true, false, Some(SystemTime::now()));
    kv.broadcast_new_value("local", &big, 2, codec.as_ref(), true, false, Some(SystemTime::now()));
    kv.broadcast_new_value("local", &medium, 3, codec.as_ref(), true, false, Some(SystemTime::now()));

    let mut buf = Vec::new();
    TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains(r#"memberlist_client_messages_in_broadcast_queue{queue="gossip"} 2"#), "{text}");
    assert!(text.contains(r#"memberlist_client_messages_in_broadcast_queue{queue="local"} 3"#), "{text}");

    let msgs = kv.delegate_get_broadcasts(0, 10000);
    assert_eq!(5, msgs.len());
    let key_of = |b: &[u8]| KeyValuePair::decode(b).unwrap().key;
    assert_eq!("local", key_of(&msgs[0]));
    assert_eq!("local", key_of(&msgs[1]));
    assert_eq!("local", key_of(&msgs[2]));
    assert_eq!("non-local", key_of(&msgs[3]));
    assert_eq!("non-local", key_of(&msgs[4]));
    assert!(msgs[0].len() > msgs[1].len());
    assert!(msgs[1].len() > msgs[2].len());
    assert!(msgs[3].len() > msgs[4].len());

    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestRaceBetweenStoringNewValueForKeyAndUpdatingIt
#[tokio::test]
async fn test_race_between_storing_new_value_for_key_and_updating_it() {
    let codec = ring_desc_codec();
    let kv = MemberlistKv::start(localhost_cfg("race-store"), vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<i64>(100);
    let kv_cas = Arc::clone(&kv);
    let c_cas = Arc::clone(&codec);
    let stop = ServiceContext::background();
    let stop_c = stop.clone();

    let h = tokio::spawn(async move {
        let mut d = RingDesc::default();
        for i in 0..100 {
            d.ingesters.insert(
                format!("member_{i}"),
                InstanceDesc { timestamp: 1, state: i % 3, ..Default::default() },
            );
        }
        let d_clone = d.clone();
        kv_cas
            .cas_with_codec(KEY, c_cas.as_ref(), Box::new(move |_| Ok(CasOutcome::Update(Box::new(d_clone.clone())))))
            .await
            .unwrap();
        while !stop_c.is_canceled() {
            let sum: i64 = d.ingesters.iter().map(|(k, v)| k.len() as i64 + v.timestamp + v.tokens.len() as i64).sum();
            let _ = tx.try_send(sum);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let _ = rx.recv().await;
    for s in 0..20 {
        let mut d = RingDesc::default();
        for i in 0..100 {
            d.ingesters.insert(
                format!("member_{i}"),
                InstanceDesc { timestamp: 2, state: (i + s) % 3, ..Default::default() },
            );
        }
        let encoded = codec.encode(&d).unwrap();
        let ser = KeyValuePair {
            key: KEY.to_string(),
            value: Bytes::from(encoded),
            codec: codec.codec_id().to_string(),
            deleted: false,
            update_time_millis: 0,
        }
        .encode_to_vec();
        let mut payload = Vec::with_capacity(4 + ser.len());
        payload.extend_from_slice(&(ser.len() as u32).to_be_bytes());
        payload.extend_from_slice(&ser);
        kv.merge_remote_state(&payload, false);
        tokio::time::sleep(Duration::from_millis(10)).await;
        while rx.try_recv().is_ok() {}
    }

    stop.cancel();
    h.await.unwrap();
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestNotificationDelay
#[tokio::test]
async fn test_notification_delay() {
    let mut cfg = localhost_cfg("notif-delay");
    cfg.notify_interval = GoDuration::from_secs(100 * 365 * 24 * 3600);
    let kv = MemberlistKv::new(cfg, Vec::new(), "", None, KvOptions::default()).unwrap();

    let n123 = kv.add_test_key_watcher("foo_123");
    let n124 = kv.add_test_key_watcher("foo_124");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();

    let tx1 = tx.clone();
    let n123_c = Arc::clone(&n123);
    let h1 = tokio::spawn(async move {
        loop {
            n123_c.notified().await;
            if tx1.send("foo_123").is_err() {
                break;
            }
        }
    });
    let tx2 = tx;
    let n124_c = Arc::clone(&n124);
    let h2 = tokio::spawn(async move {
        loop {
            n124_c.notified().await;
            if tx2.send("foo_124").is_err() {
                break;
            }
        }
    });
    tokio::task::yield_now().await;

    let drain = |r: &mut tokio::sync::mpsc::UnboundedReceiver<&'static str>| {
        let mut counts: HashMap<&'static str, usize> = [("foo_123", 0), ("foo_124", 0)].into_iter().collect();
        while let Ok(k) = r.try_recv() {
            *counts.get_mut(k).unwrap() += 1;
        }
        counts
    };

    kv.notify_watchers("foo_123");
    kv.send_key_notifications();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(1, drain(&mut rx)["foo_123"]);

    // Coalescing
    for _ in 0..6 {
        kv.notify_watchers("foo_123");
        tokio::task::yield_now().await;
        assert_eq!(0, drain(&mut rx)["foo_123"]);
    }
    kv.send_key_notifications();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(1, drain(&mut rx)["foo_123"]);

    // Two buffered updates
    kv.notify_watchers("foo_123");
    kv.send_key_notifications();
    tokio::time::sleep(Duration::from_millis(10)).await;
    kv.notify_watchers("foo_123");
    kv.send_key_notifications();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(2, drain(&mut rx)["foo_123"]);

    // Multiple keys
    kv.notify_watchers("foo_123");
    kv.notify_watchers("foo_124");
    kv.send_key_notifications();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let c = drain(&mut rx);
    assert_eq!(1, c["foo_123"]);
    assert_eq!(1, c["foo_124"]);

    kv.send_key_notifications();
    kv.send_key_notifications();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let c2 = drain(&mut rx);
    assert_eq!(0, c2["foo_123"]);
    assert_eq!(0, c2["foo_124"]);

    h1.abort();
    h2.abort();
    kv.remove_test_key_watcher("foo_123", &n123);
    kv.remove_test_key_watcher("foo_124", &n124);
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMemberlist_WatchPrefix
#[tokio::test]
async fn test_memberlist_watch_prefix() {
    // 1) buffer size config & invalid rejection
    {
        let mut cfg = localhost_cfg("wp-cfg");
        cfg.watch_prefix_buffer_size = 10;
        let kv = MemberlistKv::new(cfg, Vec::new(), "", None, KvOptions::default()).unwrap();
        assert_eq!(10, kv.config().watch_prefix_buffer_size);

        for bad in [-3, 0] {
            let mut bad_cfg = localhost_cfg("wp-bad");
            bad_cfg.watch_prefix_buffer_size = bad;
            let init = KvInitService::new(bad_cfg, Vec::new(), None, None);
            let err = init.get_memberlist_kv().await.err().unwrap().to_string();
            assert!(err.contains("invalid WatchPrefixBufferSize"), "got: {err}");
        }
    }

    // 2) buffering behavior
    {
        let codec = ring_desc_codec();
        let mut cfg = localhost_cfg("wp-buf");
        cfg.watch_prefix_buffer_size = 2;
        cfg.notify_interval = GoDuration::ZERO;
        let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
            .await
            .unwrap();
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());

        const PREFIX: &str = "test_prefix_";
        let keys = vec![
            format!("{PREFIX}1"),
            format!("{PREFIX}2"),
            format!("{PREFIX}3"),
        ];
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let ctx = ServiceContext::background();
        let ctx_c = ctx.clone();
        let c_watch = Arc::clone(&client);
        let wh = tokio::spawn(async move {
            c_watch
                .watch_prefix(
                    PREFIX,
                    ctx_c,
                    Box::new(move |k, _| {
                        let _ = tx.send(k.to_string());
                        true
                    }),
                )
                .await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        for k in &keys {
            cas_ring(&client, k, |d| Ok(Some((add_or_activate_member("test", d), true))))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let mut received = Vec::new();
        for _ in 0..3 {
            let k = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            received.push(k);
        }
        ctx.cancel();
        let _ = wh.await;

        for k in &keys {
            assert!(received.contains(k), "missing {k} in {received:?}");
        }
        kv.shutdown().await;
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMemberlist_WatchKey_ShouldStartNotifyingChangesBeforeFullJoinIsCompleted
#[tokio::test]
async fn test_memberlist_watch_key_notifies_before_full_join_completed() {
    let codec = ring_desc_codec();
    for notify_ms in [0i64, 50] {
        let producer = MemberlistKv::start(
            localhost_cfg("producer"),
            vec![Arc::clone(&codec)],
            "",
            None,
        )
        .await
        .unwrap();
        let p_client = Arc::new(MemberlistKvClient::new(Arc::clone(&producer), Arc::clone(&codec)).unwrap());
        let p_stop = ServiceContext::background();
        let p_stop_c = p_stop.clone();
        let pc = Arc::clone(&p_client);
        let p_handle = tokio::spawn(async move {
            let mut seq = 1i64;
            while !p_stop_c.is_canceled() {
                let s = seq;
                seq += 1;
                let _ = cas_ring(&pc, KEY, move |_| {
                    let mut r = RingDesc::default();
                    r.ingesters.insert("test".into(), InstanceDesc { timestamp: s, ..Default::default() });
                    Ok(Some((r, true)))
                })
                .await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let producer_addr = format!("127.0.0.1:{}", producer.listening_port());
        let dns = Arc::new(MockDns::default());
        dns.set(&producer_addr, vec![producer_addr.clone()]);
        let mut consumer_cfg = localhost_cfg("consumer");
        consumer_cfg.notify_interval = GoDuration::from_millis(notify_ms);
        consumer_cfg.join_members.0 = vec![producer_addr];

        let opts = KvOptions {
            dns_provider: Some(dns),
            before_join_members_on_startup_hook: Some(Arc::new(|ctx| {
                async move {
                    ctx.canceled().await;
                }
                .boxed()
            })),
            ..Default::default()
        };
        let consumer = MemberlistKv::start_with_options(
            consumer_cfg,
            vec![Arc::clone(&codec)],
            "",
            None,
            opts,
        )
        .await
        .unwrap();
        let c_client = Arc::new(MemberlistKvClient::new(Arc::clone(&consumer), Arc::clone(&codec)).unwrap());

        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Mutex::new(Some(tx));
        let w_ctx = ServiceContext::background();
        let w_ctx_c = w_ctx.clone();
        let cw = Arc::clone(&c_client);
        let wh = tokio::spawn(async move {
            cw.watch_key(
                KEY,
                w_ctx_c,
                Box::new(move |_| {
                    if let Some(s) = tx.lock().take() {
                        let _ = s.send(());
                    }
                    false
                }),
            )
            .await;
        });

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("WatchKey should notify before full join completes")
            .unwrap();

        w_ctx.cancel();
        let _ = wh.await;
        p_stop.cancel();
        let _ = p_handle.await;
        consumer.shutdown().await;
        producer.shutdown().await;
    }
}

// ============================================================================
// Tests 21..28
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestMemberlist_WatchKey_ShouldNotifyCurrentValueOnStart
#[tokio::test]
async fn test_memberlist_watch_key_notifies_current_value_on_start() {
    let codec = ring_desc_codec();
    for set_before_watch in [true, false] {
        let kv = MemberlistKv::start(localhost_cfg("wk-cur"), vec![Arc::clone(&codec)], "", None)
            .await
            .unwrap();
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());

        if set_before_watch {
            cas_ring(&client, KEY, |d| Ok(Some((add_or_activate_member("Ing 1", d), true))))
                .await
                .unwrap();
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Mutex::new(Some(tx));
        let w_ctx = ServiceContext::background();
        let w_ctx_c = w_ctx.clone();
        let cw = Arc::clone(&client);
        let wh = tokio::spawn(async move {
            cw.watch_key(
                KEY,
                w_ctx_c,
                Box::new(move |val| {
                    let d = val.downcast_ref::<RingDesc>().unwrap().clone();
                    if let Some(s) = tx.lock().take() {
                        let _ = s.send(d);
                    }
                    false
                }),
            )
            .await;
        });

        if !set_before_watch {
            cas_ring(&client, KEY, |d| Ok(Some((add_or_activate_member("Ing 1", d), true))))
                .await
                .unwrap();
        }

        let got = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .unwrap();
        assert!(got.ingesters.contains_key("Ing 1"));
        w_ctx.cancel();
        let _ = wh.await;
        kv.shutdown().await;
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestWatchPrefixHandlesGracefullyNotificationForAlreadyDeletedEntry
#[tokio::test]
async fn test_watch_prefix_handles_gracefully_notification_for_already_deleted_entry() {
    let codec = ring_desc_codec();
    let mut cfg = localhost_cfg("wp-deleted");
    cfg.cluster_label_verification_disabled = true;
    cfg.obsolete_entries_timeout = GoDuration::from_millis(100);
    cfg.notify_interval = GoDuration::from_millis(500);
    let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "", None)
        .await
        .unwrap();
    let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let w_ctx = ServiceContext::background();
    let w_ctx_c = w_ctx.clone();
    let cw = Arc::clone(&client);
    let wh = tokio::spawn(async move {
        cw.watch_prefix(
            "test",
            w_ctx_c,
            Box::new(move |k, val| {
                assert!(val.downcast_ref::<RingDesc>().is_some(), "unexpected non-RingDesc/nil for {k}");
                let _ = tx.send(k.to_string());
                true
            }),
        )
        .await;
    });

    cas_ring(&client, KEY, |d| Ok(Some((add_or_activate_member("test", d), true))))
        .await
        .unwrap();
    client.delete(KEY).await.unwrap();

    let _ = tokio::time::timeout(Duration::from_millis(700), rx.recv()).await;
    w_ctx.cancel();
    let _ = wh.await;
    kv.shutdown().await;
}

// Upstream: github.com/grafana/dskit/kv/memberlist/memberlist_client_test.go::TestNetworkPartition_RecoveryViaRejoin
#[tokio::test]
async fn test_network_partition_recovery_via_rejoin() {
    let codec = ring_desc_codec();
    let dns = Arc::new(MockDns::default());

    let make_cfg = |name: &str, zone: &str, role: &str, port: i64| {
        let mut cfg = MemberlistConfig {
            node_name: name.to_string(),
            randomize_node_name: false,
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: port,
            abort_if_cluster_join_fails: role == "member",
            rejoin_interval: GoDuration::from_millis(100),
            pull_push_interval: GoDuration::ZERO,
            gossip_interval: GoDuration::from_millis(50),
            leave_timeout: GoDuration::from_millis(100),
            zone_aware_routing: ZoneAwareRoutingConfig {
                enabled: true,
                instance_availability_zone: zone.to_string(),
                role: role.to_string(),
            },
            ..Default::default()
        };
        cfg.join_members.0 = vec![format!("dns+{name}-join")];
        cfg.rejoin_seed_nodes.0 = vec![format!("dns+{name}-rejoin")];
        cfg
    };

    let start_node = |cfg: MemberlistConfig| {
        let c = Arc::clone(&codec);
        let d = Arc::clone(&dns);
        async move {
            MemberlistKv::start_with_options(
                cfg,
                vec![c],
                "",
                None,
                KvOptions {
                    dns_provider: Some(d),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        }
    };

    let wait_members = |kv: &Arc<MemberlistKv>, expected: usize| {
        let kv = Arc::clone(kv);
        async move {
            for _ in 0..100 {
                if kv.num_members() == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("node {} expected {expected} members, got {}", kv.node_name(), kv.num_members());
        }
    };

    let wait_data = |c: &Arc<MemberlistKvClient>, expected: Vec<&'static str>| {
        let c = Arc::clone(c);
        async move {
            let mut exp: Vec<String> = expected.into_iter().map(String::from).collect();
            exp.sort();
            for _ in 0..100 {
                if let Some(d) = get_typed::<RingDesc>(c.as_ref(), KEY).await.unwrap() {
                    let mut got: Vec<String> = d.ingesters.keys().cloned().collect();
                    got.sort();
                    if got == exp {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("expected ring members {exp:?}");
        }
    };

    // Subtest 1: zones isolated at startup, then recover via rejoin
    {
        dns.clear();
        let ports = free_ports(3);
        let a_addr = format!("127.0.0.1:{}", ports[0]);
        let b_addr = format!("127.0.0.1:{}", ports[1]);

        let mkv_a = start_node(make_cfg("bridge-zone-a", "zone-a", "bridge", ports[0])).await;
        let mkv_b = start_node(make_cfg("bridge-zone-b", "zone-b", "bridge", ports[1])).await;
        let c_a = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_a), Arc::clone(&codec)).unwrap());
        let c_b = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_b), Arc::clone(&codec)).unwrap());

        cas_ring(&c_a, KEY, |d| Ok(Some((add_or_activate_member("member-A", d), true)))).await.unwrap();

        dns.set("dns+member-zone-b-join", vec![b_addr.clone()]);
        dns.set("dns+member-zone-b-rejoin", vec![b_addr.clone()]);
        let mkv_mb = start_node(make_cfg("member-zone-b", "zone-b", "member", ports[2])).await;
        let c_mb = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_mb), Arc::clone(&codec)).unwrap());
        cas_ring(&c_mb, KEY, |d| Ok(Some((add_or_activate_member("member-B", d), true)))).await.unwrap();

        wait_members(&mkv_a, 1).await;
        wait_members(&mkv_b, 2).await;
        wait_members(&mkv_mb, 2).await;
        wait_data(&c_a, vec!["member-A"]).await;
        wait_data(&c_b, vec!["member-B"]).await;

        dns.set("dns+bridge-zone-b-join", vec![a_addr.clone()]);
        dns.set("dns+bridge-zone-b-rejoin", vec![a_addr]);

        wait_members(&mkv_a, 3).await;
        wait_members(&mkv_b, 3).await;
        wait_members(&mkv_mb, 3).await;
        wait_data(&c_a, vec!["member-A", "member-B"]).await;
        wait_data(&c_b, vec!["member-A", "member-B"]).await;
        wait_data(&c_mb, vec!["member-A", "member-B"]).await;

        mkv_mb.shutdown().await;
        mkv_b.shutdown().await;
        mkv_a.shutdown().await;
    }

    // Subtest 2: zones isolated at startup, then recover via new non-isolated bridge
    {
        dns.clear();
        let ports = free_ports(3);
        let a_addr = format!("127.0.0.1:{}", ports[0]);
        let b_addr = format!("127.0.0.1:{}", ports[1]);

        let mkv_a = start_node(make_cfg("bridge-zone-a", "zone-a", "bridge", ports[0])).await;
        let mkv_b = start_node(make_cfg("bridge-zone-b", "zone-b", "bridge", ports[1])).await;
        let c_a = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_a), Arc::clone(&codec)).unwrap());
        cas_ring(&c_a, KEY, |d| Ok(Some((add_or_activate_member("member-A", d), true)))).await.unwrap();

        dns.set("dns+member-zone-b-join", vec![b_addr.clone()]);
        dns.set("dns+member-zone-b-rejoin", vec![b_addr.clone()]);
        let mkv_mb = start_node(make_cfg("member-zone-b", "zone-b", "member", ports[2])).await;
        let c_mb = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_mb), Arc::clone(&codec)).unwrap());
        cas_ring(&c_mb, KEY, |d| Ok(Some((add_or_activate_member("member-B", d), true)))).await.unwrap();

        wait_members(&mkv_a, 1).await;
        wait_members(&mkv_b, 2).await;
        mkv_b.shutdown().await;

        dns.set("dns+bridge-zone-b-join", vec![a_addr.clone()]);
        dns.set("dns+bridge-zone-b-rejoin", vec![a_addr]);
        let mkv_b_new = start_node(make_cfg("bridge-zone-b", "zone-b", "bridge", ports[1])).await;
        let c_b_new = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_b_new), Arc::clone(&codec)).unwrap());

        wait_members(&mkv_a, 3).await;
        wait_members(&mkv_b_new, 3).await;
        wait_members(&mkv_mb, 3).await;
        wait_data(&c_a, vec!["member-A", "member-B"]).await;
        wait_data(&c_b_new, vec!["member-A", "member-B"]).await;
        wait_data(&c_mb, vec!["member-A", "member-B"]).await;

        mkv_mb.shutdown().await;
        mkv_b_new.shutdown().await;
        mkv_a.shutdown().await;
    }

    // Subtest 3: all bridges lost, then state recovery via existing members rejoin
    {
        dns.clear();
        let ports = free_ports(4);
        let a_addr = format!("127.0.0.1:{}", ports[0]);
        let b_addr = format!("127.0.0.1:{}", ports[1]);

        dns.set("dns+bridge-zone-a-join", vec![a_addr.clone(), b_addr.clone()]);
        dns.set("dns+bridge-zone-a-rejoin", vec![a_addr.clone(), b_addr.clone()]);
        dns.set("dns+bridge-zone-b-join", vec![a_addr.clone(), b_addr.clone()]);
        dns.set("dns+bridge-zone-b-rejoin", vec![a_addr.clone(), b_addr.clone()]);

        let mkv_a = start_node(make_cfg("bridge-zone-a", "zone-a", "bridge", ports[0])).await;
        let mkv_b = start_node(make_cfg("bridge-zone-b", "zone-b", "bridge", ports[1])).await;
        wait_members(&mkv_a, 2).await;
        wait_members(&mkv_b, 2).await;

        dns.set("dns+member-zone-a-join", vec![a_addr.clone()]);
        dns.set("dns+member-zone-a-rejoin", vec![a_addr]);
        let mkv_ma = start_node(make_cfg("member-zone-a", "zone-a", "member", ports[2])).await;
        let c_ma = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_ma), Arc::clone(&codec)).unwrap());

        dns.set("dns+member-zone-b-join", vec![b_addr.clone()]);
        dns.set("dns+member-zone-b-rejoin", vec![b_addr]);
        let mkv_mb = start_node(make_cfg("member-zone-b", "zone-b", "member", ports[3])).await;
        let c_mb = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_mb), Arc::clone(&codec)).unwrap());

        cas_ring(&c_ma, KEY, |d| Ok(Some((add_or_activate_member("member-A", d), true)))).await.unwrap();
        cas_ring(&c_mb, KEY, |d| Ok(Some((add_or_activate_member("member-B", d), true)))).await.unwrap();

        wait_members(&mkv_a, 4).await;
        wait_members(&mkv_b, 4).await;
        wait_data(&c_ma, vec!["member-A", "member-B"]).await;
        wait_data(&c_mb, vec!["member-A", "member-B"]).await;

        mkv_a.shutdown().await;
        mkv_b.shutdown().await;

        let mkv_a_new = start_node(make_cfg("bridge-zone-a", "zone-a", "bridge", ports[0])).await;
        let mkv_b_new = start_node(make_cfg("bridge-zone-b", "zone-b", "bridge", ports[1])).await;
        let c_a_new = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_a_new), Arc::clone(&codec)).unwrap());
        let c_b_new = Arc::new(MemberlistKvClient::new(Arc::clone(&mkv_b_new), Arc::clone(&codec)).unwrap());

        wait_members(&mkv_a_new, 4).await;
        wait_members(&mkv_b_new, 4).await;
        wait_members(&mkv_ma, 4).await;
        wait_members(&mkv_mb, 4).await;
        wait_data(&c_a_new, vec!["member-A", "member-B"]).await;
        wait_data(&c_b_new, vec!["member-A", "member-B"]).await;
        wait_data(&c_ma, vec!["member-A", "member-B"]).await;
        wait_data(&c_mb, vec!["member-A", "member-B"]).await;

        mkv_ma.shutdown().await;
        mkv_mb.shutdown().await;
        mkv_a_new.shutdown().await;
        mkv_b_new.shutdown().await;
    }
}

