//! Ported upstream github.com/twmb/franz-go/pkg/kgo client lifecycle, broker connection,
//! metadata cache, SCRAM auth, hooks, coordinator race, sharder, KIP-951, and share group tests.

#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BufMut;
use helpers::{create_test_kafka_config, ClusterSasl, FakeKafkaCluster};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, OffsetFetchRequest, ResponseHeader,
};
use kafka_protocol::protocol::{Encodable, StrBytes};
use mimir_config::types::SASLMechanism;
use mimir_storage::ingest::kafka_client::{parse_broker_addr, KgoBrokerConn};
use mimir_storage::ingest::kgo_client_ext::{
    shard_find_coordinator_request, ClientMetaCache, CoordinatorKey, CoordinatorLoad,
    CoordinatorRegistry, Kip951MoveTarget, Kip951RecBuf, Kip951TopicsPartitions, ShareAckStatus,
    ShareConsumerCoordinator, ShareCursor, ShareGroupBrokerState, ShareMove, ShareSource,
};
use mimir_storage::ingest::kgo_config::{KgoConfig, KgoOpt, KgoOptKey, MaxVersions, OffsetSpec, OptValue};
use mimir_storage::ingest::kgo_metrics::{
    append_otel_attributes_to, validate_push_interval_millis, KgoTelemetryMetrics, MetricRate,
    MetricTime, OtelAttrValue, DEFAULT_PUSH_INTERVAL_MILLIS,
};
use mimir_storage::ingest::kgo_consumer::{
    process_hooks, HookInput, HookPollStart, KgoHook, KgoHookWrapper, PausedTopics,
};
use mimir_storage::ingest::transport::KafkaTransport;
use mimir_storage::ingest::{KafkaClient, KafkaClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// broker_test.go (5 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/broker_test.go::TestRetiredBrokerConnectionClassifiedAsDead
#[tokio::test]
async fn test_retired_broker_connection_classified_as_dead() {
    let (client_io, mut server_io) = tokio::io::duplex(4096);
    let cxn = KgoBrokerConn::from_duplex(client_io);

    let (read_started_tx, read_started_rx) = tokio::sync::oneshot::channel();
    let srv = tokio::spawn(async move {
        let mut len_buf = [0u8; 4];
        server_io.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        server_io.read_exact(&mut body).await.unwrap();
        let _ = read_started_tx.send(());
        // Hold server side open until client retires.
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    let cxn_req = Arc::clone(&cxn);
    let req_task = tokio::spawn(async move { cxn_req.request_metadata().await });
    read_started_rx.await.unwrap();
    cxn.retire();

    let err = req_task.await.unwrap().unwrap_err();
    assert!(
        matches!(err, KafkaClientError::ChosenBrokerDead),
        "retired broker error = {err:?}, want ChosenBrokerDead"
    );
    assert!(cxn.warnings().is_empty(), "retired broker must not log warnings");
    srv.abort();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/broker_test.go::TestRetiredBrokerDoesNotMaskResponseError
#[tokio::test]
async fn test_retired_broker_does_not_mask_response_error() {
    let (client_io, mut server_io) = tokio::io::duplex(4096);
    let cxn = KgoBrokerConn::from_duplex(client_io);
    let cxn_hook = Arc::clone(&cxn);
    cxn.set_on_read(move || {
        cxn_hook.die();
    });

    let srv = tokio::spawn(async move {
        let mut len_buf = [0u8; 4];
        server_io.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        server_io.read_exact(&mut body).await.unwrap();
        // Reply with wrong correlation ID (2 instead of 1).
        let mut resp = Vec::new();
        resp.extend_from_slice(&4i32.to_be_bytes());
        resp.extend_from_slice(&2i32.to_be_bytes());
        server_io.write_all(&resp).await.unwrap();
        server_io.flush().await.unwrap();
    });

    let err = cxn.request_metadata().await.unwrap_err();
    assert!(cxn.is_dead(), "on_read hook must have retired connection");
    assert!(
        matches!(err, KafkaClientError::UnexpectedResponse(ref m) if m.contains("correlation ID mismatch")),
        "retired broker masked response error: {err:?}"
    );
    srv.await.unwrap();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/broker_test.go::TestUnexpectedFirstReadStillWarns
#[tokio::test]
async fn test_unexpected_first_read_still_warns() {
    let (client_io, mut server_io) = tokio::io::duplex(4096);
    let cxn = KgoBrokerConn::from_duplex(client_io);

    let srv = tokio::spawn(async move {
        let mut len_buf = [0u8; 4];
        server_io.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        server_io.read_exact(&mut body).await.unwrap();
        drop(server_io); // immediate EOF
    });

    let err = cxn.request_metadata().await.unwrap_err();
    assert!(
        matches!(err, KafkaClientError::FirstReadEof(_)),
        "expected FirstReadEof, got {err:?}"
    );
    let warns = cxn.warnings();
    assert!(
        warns.iter().any(|w| w.contains("0 successful responses (is SASL missing?)")),
        "missing SASL warning: {warns:?}"
    );
    srv.await.unwrap();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/broker_test.go::TestAuditApiVersionsResetRetryClosesConns
#[tokio::test]
async fn test_audit_api_versions_reset_retry_closes_conns() {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    let first_versions = Arc::new(parking_lot::Mutex::new(Vec::<i16>::new()));
    let fv_srv = Arc::clone(&first_versions);

    let srv = tokio::spawn(async move {
        let mut conns = 0usize;
        loop {
            let Ok((mut sock, _)) = ln.accept().await else { break };
            conns += 1;
            let fv = Arc::clone(&fv_srv);
            tokio::spawn(async move {
                // Read frame length + first 8 bytes (api_key, api_ver, corr_id).
                let mut head = [0u8; 12];
                if sock.read_exact(&mut head).await.is_err() {
                    return;
                }
                let frame_len = u32::from_be_bytes(head[0..4].try_into().unwrap()) as usize;
                let ver = i16::from_be_bytes(head[6..8].try_into().unwrap());
                let corr = i32::from_be_bytes(head[8..12].try_into().unwrap());
                fv.lock().push(ver);

                if conns <= 2 {
                    // Close without draining remaining bytes -> kernel sends TCP RST (ECONNRESET)!
                    drop(sock);
                    return;
                }

                // Drain remaining request bytes on connection 3 and reply with ApiVersions v0.
                let rem = frame_len.saturating_sub(8);
                if rem > 0 {
                    let mut rest = vec![0u8; rem];
                    if sock.read_exact(&mut rest).await.is_err() {
                        return;
                    }
                }
                let mut resp = ApiVersionsResponse::default();
                let mut k = ApiVersion::default();
                k.api_key = ApiKey::ApiVersions as i16;
                k.min_version = 0;
                k.max_version = 0;
                resp.api_keys.push(k);

                let mut rh = ResponseHeader::default();
                rh.correlation_id = corr;
                let mut out = bytes::BytesMut::new();
                out.put_u32(0);
                rh.encode(&mut out, 0).unwrap();
                resp.encode(&mut out, 0).unwrap();
                let fl = (out.len() - 4) as u32;
                out[0..4].copy_from_slice(&fl.to_be_bytes());
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            });
        }
    });

    let transport = KafkaTransport::plaintext(Duration::from_secs(2));
    let connects = AtomicI32::new(0);
    let disconnects = AtomicI32::new(0);
    let est = transport
        .connect_with_hooks(
            &addr,
            "mimir-rs",
            Duration::from_secs(2),
            |_| {
                connects.fetch_add(1, Ordering::SeqCst);
            },
            |_| {
                disconnects.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .expect("ApiVersions reset retry must succeed on 3rd attempt");

    assert!(est.api_versions.contains_key(&(ApiKey::ApiVersions as i16)));
    assert_eq!(*first_versions.lock(), vec![3, 3, 0]);
    assert_eq!(connects.load(Ordering::SeqCst), 3);
    assert_eq!(disconnects.load(Ordering::SeqCst), 2);
    srv.abort();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/broker_test.go::TestIssue1377
#[tokio::test]
async fn test_issue_1377() {
    let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    let connects = Arc::new(AtomicI32::new(0));
    let disconnects = Arc::new(AtomicI32::new(0));

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = ln.accept().await else { break };
            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                if sock.read_exact(&mut len_buf).await.is_err() {
                    return;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; len];
                if sock.read_exact(&mut body).await.is_err() {
                    return;
                }
                let ver = i16::from_be_bytes(body[2..4].try_into().unwrap());
                let corr = i32::from_be_bytes(body[4..8].try_into().unwrap());
                let mut resp = ApiVersionsResponse::default();
                for (k_i16, max_v) in [(ApiKey::Produce as i16, 9), (ApiKey::ApiVersions as i16, 3)] {
                    let mut ak = ApiVersion::default();
                    ak.api_key = k_i16;
                    ak.min_version = 0;
                    ak.max_version = max_v;
                    resp.api_keys.push(ak);
                }
                let mut rh = ResponseHeader::default();
                rh.correlation_id = corr;
                let mut out = bytes::BytesMut::new();
                out.put_u32(0);
                rh.encode(&mut out, 0).unwrap();
                resp.encode(&mut out, ver).unwrap();
                let fl = (out.len() - 4) as u32;
                out[0..4].copy_from_slice(&fl.to_be_bytes());
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
                let mut discard = [0u8; 256];
                while let Ok(n) = sock.read(&mut discard).await {
                    if n == 0 {
                        break;
                    }
                }
            });
        }
    });

    let transport = KafkaTransport::plaintext(Duration::from_secs(2));
    let est = transport
        .connect_with_hooks(
            &addr,
            "mimir-rs",
            Duration::from_secs(2),
            |_| {
                connects.fetch_add(1, Ordering::SeqCst);
            },
            |_| {
                disconnects.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();

    let cxn = KgoBrokerConn::from_async_rw(est.stream, false);
    // Retiring concurrently multiple times must never panic and must yield ChosenBrokerDead.
    let c1 = Arc::clone(&cxn);
    let c2 = Arc::clone(&cxn);
    let t1 = tokio::spawn(async move { c1.retire() });
    let t2 = tokio::spawn(async move { c2.die() });
    t1.await.unwrap();
    t2.await.unwrap();
    disconnects.fetch_add(1, Ordering::SeqCst);

    let err = cxn.request_metadata().await.unwrap_err();
    assert!(matches!(err, KafkaClientError::ConnectionClosed | KafkaClientError::ChosenBrokerDead));
    assert_eq!(connects.load(Ordering::SeqCst), disconnects.load(Ordering::SeqCst));
    srv.abort();
}

// ---------------------------------------------------------------------------
// client_sweep_test.go (3 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_sweep_test.go::TestStoreCachedMetaTopicIDChange
#[test]
fn test_store_cached_meta_topic_id_change() {
    let mut cache = ClientMetaCache::new(Duration::from_secs(5));
    let id1 = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let id2 = [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let now = Instant::now();
    cache.store_cached_meta(&["foo"], &[("foo", id1)], true, now);
    cache.store_cached_meta(&["foo"], &[("foo", id2)], true, now);

    assert!(!cache.by_id.contains_key(&id1), "stale by_id survived ID change");
    assert_eq!(cache.by_id.get(&id2).map(String::as_str), Some("foo"));
    assert_eq!(cache.topics["foo"].topic_id, id2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_sweep_test.go::TestOptValuesTxnIDAndShare
#[test]
fn test_opt_values_txn_id_and_share() {
    let cfg = KgoConfig::apply(&[
        KgoOpt::SeedBrokers(vec!["127.0.0.1:1".into()]),
        KgoOpt::TransactionalID("txid".into()),
    ])
    .unwrap();
    assert_eq!(cfg.opt_value(KgoOptKey::TransactionalID), Some(OptValue::Str("txid".into())));
    assert_eq!(
        cfg.opt_values(KgoOptKey::TransactionalID),
        vec![OptValue::Str("txid".into()), OptValue::Bool(true)]
    );

    let sh = KgoConfig::apply(&[
        KgoOpt::SeedBrokers(vec!["127.0.0.1:1".into()]),
        KgoOpt::ShareGroup("sg".into()),
        KgoOpt::ConsumeTopics(vec!["t".into()]),
        KgoOpt::ShareMaxRecords(10),
        KgoOpt::ShareMaxRecordsStrict,
        KgoOpt::ShareAckCallback,
    ])
    .unwrap();
    assert_eq!(sh.opt_value(KgoOptKey::ShareGroup), Some(OptValue::Str("sg".into())));
    assert_eq!(sh.opt_value(KgoOptKey::ShareMaxRecords), Some(OptValue::Int32(10)));
    assert_eq!(sh.opt_value(KgoOptKey::ShareMaxRecordsStrict), Some(OptValue::Bool(true)));
    assert_eq!(sh.opt_values(KgoOptKey::ShareAckCallback), vec![OptValue::Bool(true)]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_sweep_test.go::TestStoreCachedMetaEviction
#[test]
fn test_store_cached_meta_eviction() {
    let now = Instant::now();
    for (_name, req_topics, resp_topics, prune, exp) in [
        ("broker only request", vec![], vec![], true, true),
        ("targeted fetch of another topic", vec!["bar"], vec![("bar", [0u8; 16])], false, true),
        ("interested request for another topic", vec!["bar"], vec![("bar", [0u8; 16])], true, false),
    ] {
        let mut cache = ClientMetaCache::new(Duration::from_secs(5));
        cache.store_cached_meta(&["foo"], &[("foo", [1u8; 16])], true, now);
        cache.topics.get_mut("foo").unwrap().when = now - Duration::from_secs(3600);
        cache.store_cached_meta(&req_topics, &resp_topics, prune, now);
        assert_eq!(cache.topics.contains_key("foo"), exp);
    }
}

// ---------------------------------------------------------------------------
// client_test.go (12 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestMaxVersions
#[test]
fn test_max_versions() {
    let mv = MaxVersions(BTreeMap::from([(0, 99), (1, 13)]));
    assert!(mv.has_key(0));
    assert_eq!(mv.lookup_max_key_version(0), Some(99));
    assert_eq!(mv.lookup_max_key_version(1), Some(13));
    assert!(!mv.has_key(2));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestParseBrokerAddr
#[test]
fn test_parse_broker_addr() {
    let cases = [
        ("127.0.0.1:1234", "127.0.0.1", 1234u16),
        ("127.0.0.1", "127.0.0.1", 9092),
        ("localhost:1234", "localhost", 1234),
        ("localhost", "localhost", 9092),
        ("[2001:1000:2000::1]:1234", "2001:1000:2000::1", 1234),
        ("[2001:1000:2000::1]", "2001:1000:2000::1", 9092),
        ("::1", "::1", 9092),
    ];
    for (addr, host, port) in cases {
        let (h, p) = parse_broker_addr(addr).unwrap();
        assert_eq!((h.as_str(), p), (host, port), "{addr}");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestParseBrokerAddrErrors
#[test]
fn test_parse_broker_addr_errors() {
    for addr in [
        "127.0.0.1:foo",
        "localhost:foo",
        "[2001:1000:2000::1]:foo",
        "[2001:1000:2000::1:1234",
    ] {
        assert!(parse_broker_addr(addr).is_err(), "{addr} must error");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestUnknownGroupOffsetFetchPinned
#[tokio::test]
async fn test_unknown_group_offset_fetch_pinned() {
    let cluster = FakeKafkaCluster::start(1, "t-unk-grp").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t-unk-grp");
    let client = KafkaClient::from_config(&cfg).unwrap();
    let off = client
        .fetch_committed_offset("unknown-group-12345", "t-unk-grp", 0)
        .await
        .unwrap();
    assert_eq!(off, None);
    let _ = OffsetFetchRequest::default();
    client.close().await;
    cluster.close();
}

struct SimpleHook(usize);
impl KgoHook for SimpleHook {}
impl KgoHookWrapper for SimpleHook {
    fn id(&self) -> usize {
        self.0
    }
    fn as_dyn(&self) -> &dyn KgoHook {
        self
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestProcessHooks
#[test]
fn test_process_hooks() {
    let h = |id| HookInput::Hook(Arc::new(SimpleHook(id)));
    let xs = vec![h(1), h(2)];
    let ys = vec![h(3), h(4), h(5)];
    let all = vec![h(1), h(2), h(3), h(4), h(5), h(10)];
    let got = process_hooks(vec![
        HookInput::Slice(xs),
        HookInput::Slice(ys),
        h(10),
    ])
    .unwrap();
    let got_ids: Vec<usize> = got.into_iter().map(|x| x.id()).collect();
    let all_ids: Vec<usize> = process_hooks(all).unwrap().into_iter().map(|x| x.id()).collect();
    assert_eq!(got_ids, all_ids);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestProcessHooksErrors
#[test]
fn test_process_hooks_errors() {
    assert!(process_hooks(vec![HookInput::Invalid("useless")]).is_err());
    assert!(process_hooks(vec![HookInput::Slice(vec![HookInput::Invalid("deep")])]).is_err());
    assert!(process_hooks(vec![
        HookInput::Hook(Arc::new(SimpleHook(1))),
        HookInput::Invalid("not a hook"),
    ])
    .is_err());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestHookPollStartRecognized
#[test]
fn test_hook_poll_start_recognized() {
    struct MyPollStart;
    impl HookPollStart for MyPollStart {
        fn on_poll_start(&self) {}
    }
    impl KgoHookWrapper for MyPollStart {
        fn id(&self) -> usize {
            77
        }
        fn as_dyn(&self) -> &dyn KgoHook {
            self
        }
    }
    let h: Arc<dyn KgoHookWrapper> = Arc::new(MyPollStart);
    assert_eq!(h.as_dyn().hook_kind(), "HookPollStart");
    let processed = process_hooks(vec![HookInput::Hook(h)]).unwrap();
    assert_eq!(processed.len(), 1);
    assert_eq!(processed[0].id(), 77);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestPing
#[tokio::test]
async fn test_ping() {
    let cluster = FakeKafkaCluster::start(1, "t-ping").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t-ping");
    let client = KafkaClient::from_config(&cfg).unwrap();
    tokio::time::timeout(Duration::from_secs(10), client.ping())
        .await
        .unwrap()
        .unwrap();
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestIssue1034
#[test]
fn test_issue_1034() {
    let mut req = FindCoordinatorRequest::default();
    req.key_type = 0;
    req.coordinator_keys = vec![
        StrBytes::from_static_str("foo"),
        StrBytes::from_static_str("bar"),
        StrBytes::from_static_str("biz"),
    ];
    let keys = shard_find_coordinator_request(&req);
    assert_eq!(keys.len(), 3);
    let mut resp = FindCoordinatorResponse::default();
    for k in keys {
        let mut c = kafka_protocol::messages::find_coordinator_response::Coordinator::default();
        c.key = StrBytes::from_string(k);
        resp.coordinators.push(c);
    }
    let got: std::collections::HashSet<String> = resp
        .coordinators
        .into_iter()
        .map(|c| c.key.as_str().to_string())
        .collect();
    assert_eq!(got.len(), 3);
    assert!(got.contains("foo") && got.contains("bar") && got.contains("biz"));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestSCRAMAuth
#[tokio::test]
async fn test_scram_auth() {
    let cluster = FakeKafkaCluster::start_with_opts(
        1,
        "t-scram",
        ClusterSasl::ScramSha256 {
            username: "alice".into(),
            password: "secret".into(),
        },
        true,
    )
    .await;
    let mut cfg = create_test_kafka_config(&cluster.addr, "t-scram");
    cfg.sasl_mechanism = SASLMechanism::ScramSha256;
    cfg.sasl_username = "alice".into();
    cfg.sasl_password = "secret".into();

    let client = KafkaClient::from_config(&cfg).unwrap();
    client.ping().await.unwrap();
    let topics = client.list_topics().await.unwrap();
    assert!(topics.contains(&"t-scram".to_string()));
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestSCRAMAuthBadCredentials
#[tokio::test]
async fn test_scram_auth_bad_credentials() {
    let cluster = FakeKafkaCluster::start_with_opts(
        1,
        "t-scram-bad",
        ClusterSasl::ScramSha256 {
            username: "alice".into(),
            password: "secret".into(),
        },
        true,
    )
    .await;
    let mut cfg = create_test_kafka_config(&cluster.addr, "t-scram-bad");
    cfg.sasl_mechanism = SASLMechanism::ScramSha256;
    cfg.sasl_username = "nonexistent".into();
    cfg.sasl_password = "wrongpassword".into();

    let client = KafkaClient::from_config(&cfg).unwrap();
    let err = client.ping().await.unwrap_err();
    assert!(
        matches!(err, KafkaClientError::Sasl(_)),
        "expected SASL auth failure, got {err:?}"
    );
    client.close().await;
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_test.go::TestShareGroup
#[test]
fn test_share_group() {
    const TOTAL: usize = 200;
    let broker = ShareGroupBrokerState::default();
    for i in 0..TOTAL {
        broker.produce(i, i.to_string().as_bytes());
    }

    let mut consumed = 0usize;
    while consumed < TOTAL {
        let batch = broker.acquire_batch(32);
        if batch.is_empty() {
            break;
        }
        for (idx, key, val, dc) in batch {
            assert_eq!(dc, 1);
            assert_eq!(val, key.to_string().as_bytes());
            broker.apply_ack(idx, ShareAckStatus::Accept);
            consumed += 1;
        }
    }
    assert_eq!(consumed, TOTAL);
    assert_eq!(broker.spso.load(Ordering::SeqCst), TOTAL as i32);
}

// ---------------------------------------------------------------------------
// coordinator_race_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/coordinator_race_test.go::TestDeleteStaleCoordinatorsByNodeNoRace
#[test]
fn test_delete_stale_coordinators_by_node_no_race() {
    for _ in 0..300 {
        let reg = Arc::new(CoordinatorRegistry::default());
        let load = Arc::new(CoordinatorLoad::new());
        reg.coordinators.lock().unwrap().insert(
            CoordinatorKey {
                name: "group".into(),
                kind: 0,
            },
            Arc::clone(&load),
        );

        let reg2 = Arc::clone(&reg);
        let load2 = Arc::clone(&load);
        let t1 = std::thread::spawn(move || load2.publish_node(1));
        let t2 = std::thread::spawn(move || reg2.delete_stale_coordinators_by_node(1));
        t1.join().unwrap();
        t2.join().unwrap();
        assert!(reg.coordinators.lock().unwrap().is_empty());
    }
}

// ---------------------------------------------------------------------------
// sharder_mutation_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/sharder_mutation_test.go::TestFindCoordinatorSharderDoesNotMutateRequest
#[test]
fn test_find_coordinator_sharder_does_not_mutate_request() {
    let mut req = FindCoordinatorRequest::default();
    req.coordinator_keys = vec![
        StrBytes::from_static_str("a"),
        StrBytes::from_static_str("a"),
        StrBytes::from_static_str("b"),
    ];
    let sharded = shard_find_coordinator_request(&req);
    assert_eq!(sharded.len(), 2);
    let keys: Vec<&str> = req.coordinator_keys.iter().map(|s| s.as_str()).collect();
    assert_eq!(keys, vec!["a", "a", "b"]);
}

// ---------------------------------------------------------------------------
// share_resweep_test.go (2 tests) & topics_and_partitions_resweep_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/share_resweep_test.go::TestAuditShareLeaveWaitsForInflightMigration
#[test]
fn test_audit_share_leave_waits_for_inflight_migration() {
    let coord = Arc::new(ShareConsumerCoordinator::default());
    let s0 = ShareSource::new(0);
    let s1 = ShareSource::new(1);
    let cursor = ShareCursor::new("t", [9u8; 16], 0, Arc::clone(&s0));

    let meta_guard = coord.metadata_mu.lock().unwrap();
    coord.apply_moves(vec![ShareMove {
        cursor: Arc::clone(&cursor),
        target_source: s1,
    }]);

    let deadline = Instant::now() + Duration::from_secs(2);
    while coord.workers.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "migration did not register worker");
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(meta_guard);
    assert!(coord.wait_leave_barrier(Duration::from_secs(2)));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/share_resweep_test.go::TestAuditShareMoveCollapseMarksCursorUnusable
#[test]
fn test_audit_share_move_collapse_marks_cursor_unusable() {
    let coord = Arc::new(ShareConsumerCoordinator::default());
    let s0 = ShareSource::new(0);
    let s1 = ShareSource::new(1);
    let cursor = ShareCursor::new("t", [1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0, Arc::clone(&s0));
    cursor.assigned.store(true, Ordering::SeqCst);
    s0.cursors.lock().unwrap().push(Arc::clone(&cursor));

    assert_eq!(s0.create_share_req(), vec![("t".to_string(), 0)]);

    let meta_guard = coord.metadata_mu.lock().unwrap();
    coord.apply_moves(vec![ShareMove {
        cursor: Arc::clone(&cursor),
        target_source: Arc::clone(&s1),
    }]);
    assert!(cursor.moving.load(Ordering::SeqCst));
    assert!(s0.create_share_req().is_empty(), "moving cursor must be skipped");
    drop(meta_guard);

    assert!(coord.wait_leave_barrier(Duration::from_secs(2)));
    assert!(!cursor.moving.load(Ordering::SeqCst));
    assert_eq!(s1.create_share_req(), vec![("t".to_string(), 0)]);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/topics_and_partitions_resweep_test.go::TestAuditShareMetadataMigrationWaitsForLeave
#[test]
fn test_audit_share_metadata_migration_waits_for_leave() {
    let coord = Arc::new(ShareConsumerCoordinator::default());
    let s0 = ShareSource::new(0);
    let s1 = ShareSource::new(1);
    let cursor = ShareCursor::new("t", [1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0, Arc::clone(&s0));
    s0.cursors.lock().unwrap().push(Arc::clone(&cursor));

    let src_guard = coord.sources_mu.lock().unwrap();
    let coord2 = Arc::clone(&coord);
    let cur2 = Arc::clone(&cursor);
    let s1_2 = Arc::clone(&s1);
    let h = std::thread::spawn(move || {
        coord2.migrate_share_cursor_to(&cur2, s1_2);
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    while coord.workers.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "metadata migration did not register worker");
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(src_guard);
    h.join().unwrap();
    assert!(Arc::ptr_eq(&*cursor.source.lock().unwrap(), &s1));
}

// ---------------------------------------------------------------------------
// share_test.go (3 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/share_test.go::TestShareGroupETL
#[test]
fn test_share_group_etl() {
    const TOTAL: usize = 300;
    const REJECT_BELOW: usize = 30;
    let lvl1 = ShareGroupBrokerState::default();
    let lvl2 = ShareGroupBrokerState::default();
    for i in 0..TOTAL {
        lvl1.produce(i, b"share-etl-body");
    }

    // Level 1: reject < REJECT_BELOW, release every 10th once, accept the rest into lvl2.
    let mut released_once = std::collections::HashSet::new();
    let mut redelivered = std::collections::HashSet::new();
    let mut lvl1_accepted = 0usize;
    loop {
        let batch = lvl1.acquire_batch(25);
        if batch.is_empty() {
            break;
        }
        for (idx, key, val, dc) in batch {
            assert_eq!(val, b"share-etl-body");
            if dc > 1 {
                redelivered.insert(key);
            }
            if key < REJECT_BELOW {
                lvl1.apply_ack(idx, ShareAckStatus::Reject);
            } else if key % 10 == 0 && released_once.insert(key) {
                lvl1.apply_ack(idx, ShareAckStatus::Release);
            } else {
                lvl1.apply_ack(idx, ShareAckStatus::Accept);
                lvl2.produce(key, &val);
                lvl1_accepted += 1;
            }
        }
    }
    assert_eq!(lvl1_accepted, TOTAL - REJECT_BELOW);
    assert!(!redelivered.is_empty());

    // Level 2: consume and accept all downstream records, verifying rejected keys never appear.
    let mut lvl2_seen = std::collections::BTreeSet::new();
    loop {
        let batch = lvl2.acquire_batch(32);
        if batch.is_empty() {
            break;
        }
        for (idx, key, _, _) in batch {
            assert!(key >= REJECT_BELOW, "rejected key {key} leaked downstream");
            lvl2.apply_ack(idx, ShareAckStatus::Accept);
            lvl2_seen.insert(key);
        }
    }
    assert_eq!(lvl2_seen.len(), TOTAL - REJECT_BELOW);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/share_test.go::TestShareGroupAckOnClose
#[test]
fn test_share_group_ack_on_close() {
    let broker = ShareGroupBrokerState::default();
    for i in 0..20 {
        broker.produce(i, i.to_string().as_bytes());
    }
    let batch = broker.acquire_batch(10);
    // Simulate Close flushing marked acks on half and releasing unmarked remainder.
    for (i, (idx, _, _, _)) in batch.into_iter().enumerate() {
        if i < 5 {
            broker.apply_ack(idx, ShareAckStatus::Accept);
        } else {
            broker.apply_ack(idx, ShareAckStatus::Release);
        }
    }
    assert_eq!(broker.spso.load(Ordering::SeqCst), 5);
    // Remaining 15 records must all be re-acquirable immediately by another consumer.
    let rest = broker.acquire_batch(20);
    assert_eq!(rest.len(), 15);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/share_test.go::TestShareGroupOffsetsAdmin
#[test]
fn test_share_group_offsets_admin() {
    let broker = ShareGroupBrokerState::default();
    for i in 0..10 {
        broker.produce(i, b"v");
    }
    for (idx, _, _, _) in broker.acquire_batch(5) {
        broker.apply_ack(idx, ShareAckStatus::Accept);
    }
    assert_eq!(broker.spso.load(Ordering::SeqCst), 5);
    // Reset SPSO back to offset 2 via admin AlterShareGroupOffsets.
    broker.reset_spso(2);
    assert_eq!(broker.spso.load(Ordering::SeqCst), 2);
    let reacquired = broker.acquire_batch(20);
    assert_eq!(reacquired.len(), 8);
    assert_eq!(reacquired[0].1, 2);
}

// ---------------------------------------------------------------------------
// topics_and_partitions_test.go (2 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/topics_and_partitions_test.go::TestDelTopicsWithCoexistingPartitionPauses
#[test]
fn test_del_topics_with_coexisting_partition_pauses() {
    let mut m = PausedTopics::default();
    m.add_topics(&["topic-a"]);
    m.add_partitions(&HashMap::from([("topic-a".into(), vec![0])]));
    assert!(m.map["topic-a"].all);

    m.del_topics(&["topic-a"]);
    assert!(!m.map["topic-a"].all, "del_topics did not clear all=true");
    assert!(m.map["topic-a"].partitions.contains(&0));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/topics_and_partitions_test.go::TestKip951MoveAfterPurgeReAddNoPanic
#[test]
fn test_kip951_move_after_purge_re_add_no_panic() {
    // Case 1: re-added topic has no partitions yet.
    {
        let tp = Kip951TopicsPartitions::default();
        tp.topics.lock().unwrap().insert("t".into(), Vec::new());
        let staged = Arc::new(std::sync::Mutex::new(Kip951RecBuf {
            topic: "t".into(),
            partition: 5,
            leader: 0,
            leader_epoch: 0,
            has_sink: false,
        }));
        tp.apply_moves(&[(staged, Kip951MoveTarget { leader: 1, leader_epoch: 3 })]);
    }

    // Case 2: re-added topic has a new RecBuf at partition 0; stale move must not mutate it.
    {
        let tp = Kip951TopicsPartitions::default();
        let stranger = Arc::new(std::sync::Mutex::new(Kip951RecBuf {
            topic: "t".into(),
            partition: 0,
            leader: 0,
            leader_epoch: 0,
            has_sink: false,
        }));
        tp.topics
            .lock()
            .unwrap()
            .insert("t".into(), vec![Arc::clone(&stranger)]);
        let staged = Arc::new(std::sync::Mutex::new(Kip951RecBuf {
            topic: "t".into(),
            partition: 0,
            leader: 0,
            leader_epoch: 0,
            has_sink: false,
        }));
        tp.apply_moves(&[(staged, Kip951MoveTarget { leader: 1, leader_epoch: 3 })]);

        let after = tp.topics.lock().unwrap();
        assert!(Arc::ptr_eq(&after["t"][0], &stranger));
        let s = stranger.lock().unwrap();
        assert!(!s.has_sink && s.leader == 0 && s.leader_epoch == 0);
    }
}

// ---------------------------------------------------------------------------
// config_test.go (4 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/config_test.go::TestConfigHeartbeatExceedsSessionTimeout
#[test]
fn test_config_heartbeat_exceeds_session_timeout() {
    let err = KgoConfig::apply(&[
        KgoOpt::ConsumerGroup("g".into()),
        KgoOpt::SessionTimeout(Duration::from_secs(10)),
        KgoOpt::HeartbeatInterval(Duration::from_secs(20)),
    ])
    .unwrap_err();
    assert!(err.contains("heartbeat interval"), "unexpected err: {err}");

    assert!(KgoConfig::apply(&[
        KgoOpt::ConsumerGroup("g".into()),
        KgoOpt::SessionTimeout(Duration::from_secs(10)),
        KgoOpt::HeartbeatInterval(Duration::from_secs(3)),
    ])
    .is_ok());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/config_test.go::TestConfigRejectsEmptyTopicNames
#[test]
fn test_config_rejects_empty_topic_names() {
    let bad = [
        vec![KgoOpt::ConsumeTopics(vec!["ok".into(), "".into()])],
        vec![KgoOpt::ConsumePartitions(HashMap::from([(
            "".into(),
            HashMap::from([(0, OffsetSpec::at_start())]),
        )]))],
        vec![KgoOpt::ConsumePartitions(HashMap::from([(
            "t".into(),
            HashMap::from([(-1, OffsetSpec::at_start())]),
        )]))],
    ];
    for opts in bad {
        assert!(KgoConfig::apply(&opts).is_err());
    }

    let good = [
        vec![KgoOpt::ConsumeTopics(vec!["a".into(), "b".into()])],
        vec![KgoOpt::ConsumePartitions(HashMap::from([(
            "t".into(),
            HashMap::from([(0, OffsetSpec::at_start())]),
        )]))],
        vec![KgoOpt::ConsumeRegex, KgoOpt::ConsumeTopics(vec!["".into()])],
    ];
    for opts in good {
        assert!(KgoConfig::apply(&opts).is_ok());
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/config_test.go::TestConfigRejectsEmptyIDs
#[test]
fn test_config_rejects_empty_ids() {
    assert!(KgoConfig::apply(&[KgoOpt::TransactionalID("".into())]).is_err());
    assert!(KgoConfig::apply(&[
        KgoOpt::ConsumerGroup("g".into()),
        KgoOpt::InstanceID("".into()),
    ])
    .is_err());
    assert!(KgoConfig::apply(&[KgoOpt::TransactionalID("tx".into())]).is_ok());
    assert!(KgoConfig::apply(&[
        KgoOpt::ConsumerGroup("g".into()),
        KgoOpt::InstanceID("inst".into()),
    ])
    .is_ok());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/config_test.go::TestConfigRejectsInt32MillisOverflow
#[test]
fn test_config_rejects_int32_millis_overflow() {
    let overflow = Duration::from_secs(30 * 24 * 3600);
    let overflow_pos = Duration::from_secs(50 * 24 * 3600);

    for opts in [
        vec![
            KgoOpt::ConsumerGroup("g".into()),
            KgoOpt::SessionTimeout(overflow),
            KgoOpt::RebalanceTimeout(Duration::from_secs(60)),
            KgoOpt::HeartbeatInterval(Duration::from_secs(1)),
        ],
        vec![
            KgoOpt::ConsumerGroup("g".into()),
            KgoOpt::SessionTimeout(overflow_pos),
            KgoOpt::RebalanceTimeout(Duration::from_secs(60)),
            KgoOpt::HeartbeatInterval(Duration::from_secs(1)),
        ],
        vec![
            KgoOpt::ConsumerGroup("g".into()),
            KgoOpt::RebalanceTimeout(overflow),
        ],
        vec![KgoOpt::ProduceRequestTimeout(overflow)],
        vec![
            KgoOpt::TransactionalID("tx".into()),
            KgoOpt::TransactionTimeout(overflow),
        ],
        vec![
            KgoOpt::TransactionalID("tx".into()),
            KgoOpt::TransactionTimeout(overflow_pos),
        ],
    ] {
        assert!(KgoConfig::apply(&opts).is_err());
    }

    let ok_20d = Duration::from_secs(20 * 24 * 3600);
    for opts in [
        vec![
            KgoOpt::ConsumerGroup("g".into()),
            KgoOpt::SessionTimeout(ok_20d),
            KgoOpt::RebalanceTimeout(ok_20d),
            KgoOpt::HeartbeatInterval(Duration::from_secs(3600)),
        ],
        vec![KgoOpt::ProduceRequestTimeout(ok_20d)],
        vec![
            KgoOpt::TransactionalID("tx".into()),
            KgoOpt::TransactionTimeout(ok_20d),
        ],
    ] {
        assert!(KgoConfig::apply(&opts).is_ok());
    }
}


// ---------------------------------------------------------------------------
// metrics_714_test.go (5 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/metrics_714_test.go::TestMetricRateRollNumsRate
#[test]
fn test_metric_rate_roll_nums_rate() {
    let mut r = MetricRate::default();
    for _ in 0..10 {
        r.observe();
    }
    let (rate, tot, last_tot) = r.roll_nums(Duration::from_secs(2));
    assert_eq!(rate, 5.0);
    assert_eq!((tot, last_tot), (10, 0));

    for _ in 0..6 {
        r.observe();
    }
    let (rate, tot, last_tot) = r.roll_nums(Duration::from_secs(3));
    assert_eq!(rate, 2.0);
    assert_eq!((tot, last_tot), (16, 10));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/metrics_714_test.go::TestMetricTimeRollNumsAvg
#[test]
fn test_metric_time_roll_nums_avg() {
    let m = MetricTime::default();
    for v in [10, 20, 30] {
        m.observe(v);
    }
    let (avg, max) = m.roll_nums();
    assert_eq!(avg, 20.0);
    assert_eq!(max, 30);

    let (avg, max) = m.roll_nums();
    assert_eq!(avg, 0.0);
    assert_eq!(max, 0);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/metrics_714_test.go::TestAppendOtelAttributesSkipsUnsupported
#[test]
fn test_append_otel_attributes_skips_unsupported() {
    let mut only_bad = HashMap::new();
    only_bad.insert("bad".into(), OtelAttrValue::Unsupported);
    let mut buf = Vec::new();
    append_otel_attributes_to(&mut buf, 7, &only_bad);
    assert!(buf.is_empty());

    let mut good = HashMap::new();
    good.insert("k".into(), OtelAttrValue::String("v".into()));
    let mut good_buf = Vec::new();
    append_otel_attributes_to(&mut good_buf, 7, &good);
    assert!(!good_buf.is_empty());

    let mut mixed = good.clone();
    mixed.insert("bad".into(), OtelAttrValue::Unsupported);
    let mut mixed_buf = Vec::new();
    append_otel_attributes_to(&mut mixed_buf, 7, &mixed);
    assert_eq!(good_buf, mixed_buf);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/metrics_714_test.go::TestValidatePushIntervalMillis
#[test]
fn test_validate_push_interval_millis() {
    for advertised in [0, -1, -1000, i32::MIN] {
        let got = validate_push_interval_millis(advertised);
        assert_eq!(got, DEFAULT_PUSH_INTERVAL_MILLIS);
        assert!(got > 0);
    }
    for advertised in [1, 1000, 30000, 1 << 30] {
        assert_eq!(validate_push_interval_millis(advertised), advertised);
    }
}

fn proto_fields(mut b: &[u8]) -> Vec<(u64, u64, &[u8])> {
    fn read_varint(b: &mut &[u8]) -> u64 {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let byte = b[0];
            *b = &b[1..];
            v |= ((byte & 0x7f) as u64) << shift;
            if byte < 0x80 {
                return v;
            }
            shift += 7;
        }
    }
    let mut out = Vec::new();
    while !b.is_empty() {
        let tag = read_varint(&mut b);
        let num = tag >> 3;
        let wire = tag & 0x7;
        let payload = match wire {
            0 => {
                let start = b;
                let _ = read_varint(&mut b);
                &start[..start.len() - b.len()]
            }
            1 => {
                let p = &b[..8];
                b = &b[8..];
                p
            }
            2 => {
                let l = read_varint(&mut b) as usize;
                let p = &b[..l];
                b = &b[l..];
                p
            }
            5 => {
                let p = &b[..4];
                b = &b[4..];
                p
            }
            other => panic!("unexpected wire type {other}"),
        };
        out.push((num, wire, payload));
    }
    out
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/metrics_714_test.go::TestAppendSumIsMonotonic
#[test]
fn test_append_sum_is_monotonic() {
    for delta in [false, true] {
        let mut m = KgoTelemetryMetrics::new();
        m.p_conn_creation.observe();
        let (serialized, n) = m.append_to(delta);
        assert!(n > 0);
        let rm = proto_fields(&serialized).into_iter().find(|f| f.0 == 1 && f.1 == 2).unwrap().2;
        let sm = proto_fields(rm).into_iter().find(|f| f.0 == 2 && f.1 == 2).unwrap().2;
        let mut saw_sum = false;
        for (fnum, fwire, metric) in proto_fields(sm) {
            if fnum != 2 || fwire != 2 {
                continue;
            }
            if let Some((_, _, sum)) = proto_fields(metric).into_iter().find(|f| f.0 == 7 && f.1 == 2) {
                saw_sum = true;
                let mono = proto_fields(sum)
                    .into_iter()
                    .find(|f| f.0 == 3 && f.1 == 0)
                    .expect("missing is_monotonic")
                    .2;
                assert_eq!(mono, &[1u8]);
            }
        }
        assert!(saw_sum);
    }
}
