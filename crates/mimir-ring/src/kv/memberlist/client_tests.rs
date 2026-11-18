//! Rust-specific regression tests for the memberlist KV client.

    use super::*;
    use crate::kv::StringCodec;

    fn local_cfg(name: &str) -> MemberlistConfig {
        MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            node_name: name.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_fast_join_resolves_dns_plus_members() {
        let codec: Arc<dyn Codec> = Arc::new(StringCodec);
        let seed = MemberlistKv::start(local_cfg("seed"), vec![Arc::clone(&codec)], "test", None)
            .await
            .unwrap();

        let mut cfg = local_cfg("joiner");
        cfg.join_members.0 = vec![format!("dns+localhost:{}", seed.listening_port())];
        cfg.abort_if_cluster_fast_join_fails = true;
        let joiner = MemberlistKv::start(cfg, vec![codec], "test", None)
            .await
            .expect("fast-join via dns+ must reach the seed");

        assert!(joiner.members().iter().any(|n| n.name == seed.node_name));
        joiner.shutdown().await;
        seed.shutdown().await;
    }

    #[tokio::test]
    async fn test_start_does_not_block_on_join_retries() {
        // Before the fix, start() ran all join retries (1s..1m backoff, 10 attempts) inline,
        // so nothing after it (including the HTTP server) came up while seeds were unreachable.
        let mut cfg = local_cfg("lonely");
        cfg.join_members.0 = vec!["127.0.0.1:1".to_string()];
        let kv = tokio::time::timeout(
            Duration::from_secs(3),
            MemberlistKv::start(cfg, vec![Arc::new(StringCodec) as Arc<dyn Codec>], "test", None),
        )
        .await
        .expect("start() must return while join retries continue in the background")
        .unwrap();
        kv.shutdown().await;
    }

    #[tokio::test]
    async fn test_dropped_watchers_are_unregistered() {
        let cfg = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            node_name: "watcher-test-node".to_string(),
            ..Default::default()
        };

        let codec: Arc<dyn Codec> = Arc::new(StringCodec);
        let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "test", None)
            .await
            .unwrap();
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), codec).unwrap());

        let ctx1 = ServiceContext::background();
        let ctx1_c = ctx1.clone();
        let c1 = Arc::clone(&client);
        let h1 = tokio::spawn(async move {
            c1.watch_key("k1", ctx1_c, Box::new(|_| true)).await;
        });

        let ctx2 = ServiceContext::background();
        let ctx2_c = ctx2.clone();
        let c2 = Arc::clone(&client);
        let h2 = tokio::spawn(async move {
            c2.watch_prefix("pref/", ctx2_c, Box::new(|_, _| true)).await;
        });

        // Wait for both watchers to register
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let wk = kv.watchers.lock().get("k1").map_or(0, Vec::len);
                let pw = kv.prefix_watchers.lock().get("pref/").map_or(0, Vec::len);
                if wk == 1 && pw == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        // Cancel both watchers and verify both maps clean up completely
        ctx1.cancel();
        ctx2.cancel();
        h1.await.unwrap();
        h2.await.unwrap();

        assert!(kv.watchers.lock().is_empty());
        assert!(kv.prefix_watchers.lock().is_empty());

        kv.shutdown().await;
    }

    #[tokio::test]
    async fn test_merge_stores_result_and_notifies_watchers_when_change_tombstones_expire() {
        use crate::ring::model::ring_desc_codec;
        use mimir_config::GoDuration;
        use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};

        let mut cfg = local_cfg("p0-1-node");
        cfg.left_ingesters_timeout = GoDuration::from_secs(60);
        let codec = ring_desc_codec();
        let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "test", None)
            .await
            .unwrap();
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), Arc::clone(&codec)).unwrap());

        let old_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 120;

        // Seed ACTIVE ingester-1 with old timestamp.
        client
            .cas(
                "ring",
                Box::new(move |_| {
                    let mut d = Desc::default();
                    d.ingesters.insert(
                        "ingester-1".to_string(),
                        InstanceDesc {
                            id: "ingester-1".to_string(),
                            addr: "127.0.0.1:9095".to_string(),
                            timestamp: old_ts,
                            state: InstanceState::Active as i32,
                            tokens: vec![100, 200],
                            ..Default::default()
                        },
                    );
                    Ok(CasOutcome::Update(Box::new(d)))
                }),
            )
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<String>>();
        let ctx = ServiceContext::background();
        let ctx_w = ctx.clone();
        let c_w = Arc::clone(&client);
        let watcher = tokio::spawn(async move {
            c_w.watch_key(
                "ring",
                ctx_w,
                Box::new(move |any| {
                    if let Ok(d) = any.downcast::<Desc>() {
                        let ids: Vec<String> = d.ingesters.keys().cloned().collect();
                        let _ = tx.send(ids);
                    }
                    true
                }),
            )
            .await;
        });

        let initial = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(initial, vec!["ingester-1".to_string()]);

        // Simulate incoming gossip marking ingester-1 as LEFT with an expired timestamp (old_ts + 1 < now - 60s).
        let mut incoming = Desc::default();
        incoming.ingesters.insert(
            "ingester-1".to_string(),
            InstanceDesc {
                id: "ingester-1".to_string(),
                addr: "127.0.0.1:9095".to_string(),
                timestamp: old_ts + 1,
                state: InstanceState::Left as i32,
                ..Default::default()
            },
        );
        let encoded = codec.encode(&incoming).unwrap();
        let (ch, new_ver, _, _) = kv
            .merge_bytes_value_for_key("ring", &encoded, codec.as_ref(), false, None)
            .unwrap();
        assert!(ch.is_none(), "expired tombstone should not be re-gossiped");
        assert!(new_ver > 0, "local store & version must update");
        kv.notify_watchers("ring");

        let updated = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            updated.is_empty(),
            "ingester-1 must be removed from store & local watchers, got {updated:?}"
        );

        let cur = kv
            .get_value("ring")
            .unwrap()
            .0
            .downcast::<Desc>()
            .unwrap();
        assert!(cur.ingesters.is_empty());

        ctx.cancel();
        watcher.await.unwrap();
        kv.shutdown().await;
    }

    #[tokio::test]
    async fn test_watch_key_notifies_delete_and_allows_recreate() {
        use crate::kv::is_key_deleted;
        use crate::ring::model::ring_desc_codec;
        use mimir_config::GoDuration;
        use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};

        let mut cfg = local_cfg("p1-7-node");
        // Keep obsolete entries around long enough so re-create happens while tombstoned.
        cfg.obsolete_entries_timeout = GoDuration::from_secs(60);
        let codec = ring_desc_codec();
        let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "test", None)
            .await
            .unwrap();
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), codec).unwrap());

        client
            .cas(
                "ring",
                Box::new(|_| {
                    let mut d = Desc::default();
                    d.ingesters.insert(
                        "old-ing".to_string(),
                        InstanceDesc {
                            id: "old-ing".to_string(),
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

        let (tx, mut rx) = mpsc::unbounded_channel::<Result<Vec<String>, ()>>();
        let ctx = ServiceContext::background();
        let ctx_w = ctx.clone();
        let c_w = Arc::clone(&client);
        let watcher = tokio::spawn(async move {
            c_w.watch_key(
                "ring",
                ctx_w,
                Box::new(move |any| {
                    if is_key_deleted(any.as_ref()) {
                        let _ = tx.send(Err(()));
                    } else if let Ok(d) = any.downcast::<Desc>() {
                        let _ = tx.send(Ok(d.ingesters.keys().cloned().collect()));
                    }
                    true
                }),
            )
            .await;
        });

        assert_eq!(rx.recv().await.unwrap().unwrap(), vec!["old-ing".to_string()]);

        client.delete("ring").await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), Err(()), "watch_key must notify on delete");
        assert!(client.get("ring").await.unwrap().is_none());

        // Re-create before obsolete_entries_timeout expires; old-ing must NOT come back.
        tokio::time::sleep(Duration::from_millis(5)).await;
        client
            .cas(
                "ring",
                Box::new(|cur| {
                    assert!(cur.is_none());
                    let mut d = Desc::default();
                    d.ingesters.insert(
                        "new-ing".to_string(),
                        InstanceDesc {
                            id: "new-ing".to_string(),
                            timestamp: 200,
                            state: InstanceState::Active as i32,
                            tokens: vec![2],
                            ..Default::default()
                        },
                    );
                    Ok(CasOutcome::Update(Box::new(d)))
                }),
            )
            .await
            .unwrap();

        let recreated = rx.recv().await.unwrap().unwrap();
        assert_eq!(recreated, vec!["new-ing".to_string()]);

        ctx.cancel();
        watcher.await.unwrap();
        kv.shutdown().await;
    }

    #[tokio::test]
    async fn test_cluster_label_over_255_rejected_at_startup() {
        let mut cfg = local_cfg("too-long-label");
        cfg.cluster_label = "a".repeat(256);
        let err = MemberlistKv::start(cfg, vec![], "test", None)
            .await
            .err()
            .expect("label > 255 must fail startup");
        assert!(err.to_string().contains("too long"), "got: {err}");
    }

    #[tokio::test]
    async fn test_alive_node_empty_meta_none_vs_some_empty_does_not_refute() {
        let cfg = local_cfg("local-node");
        let kv = MemberlistKv::start(cfg, vec![], "test", None)
            .await
            .unwrap();
        let (ip, port) = kv.advertise();
        let inc_before = kv.incarnation.load(Ordering::Acquire);
        let alive = super::super::wire::Alive {
            incarnation: inc_before,
            node: "local-node".to_string(),
            addr: super::ip_to_bytes(ip),
            port,
            meta: Some(Vec::new()),
            vsn: vec![
                super::super::wire::PROTOCOL_VERSION_MIN,
                super::super::wire::PROTOCOL_VERSION_MAX,
                super::super::wire::PROTOCOL_VERSION_MAX,
                1,
                2,
                1,
            ],
        };
        kv.alive_node(&alive, None, false);
        assert_eq!(
            kv.incarnation.load(Ordering::Acquire),
            inc_before,
            "Some(&[]) meta must equal None without refutation"
        );
        kv.shutdown().await;
    }

    #[test]
    fn test_decode_push_pull_normalizes_port_zero_to_7946() {
        let ns = super::super::wire::PushNodeState {
            name: "p0".to_string(),
            addr: vec![127, 0, 0, 1],
            port: 0,
            meta: None,
            incarnation: 1,
            state: super::super::wire::NodeStateType::Alive,
            vsn: vec![1, 5, 2, 0, 0, 0],
        };
        let enc = super::super::wire::encode_push_pull(&[ns], b"", true);
        let (_join, nodes, _user) = super::super::wire::decode_push_pull(&enc[1..]).unwrap();
        assert_eq!(nodes[0].port, 7946);
    }

    #[tokio::test]
    async fn test_handle_conn_discards_bad_label_before_decompress_and_server_reply_has_no_label() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut cfg = local_cfg("srv-label");
        cfg.cluster_label = "prod-a".to_string();
        let kv = MemberlistKv::start(cfg, vec![], "test", None)
            .await
            .unwrap();
        let port = kv.listening_port();

        // 1. Send wrong label followed by invalid Compress payload; server must reject before decompressing.
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let mut frame = vec![2u8]; // STREAM_BYTE
        frame.extend_from_slice(&super::super::wire::add_label_header(&[super::super::wire::MessageType::Compress as u8, 0xff], "wrong").unwrap());
        s.write_all(&frame).await.unwrap();
        let mut resp = Vec::new();
        let _ = s.read_to_end(&mut resp).await;
        assert!(resp.is_empty(), "wrong label must be dropped before reading/decompressing");

        // 2. Send valid PushPull with label "prod-a"; reply must start with PushPull (or Compress wrapping PushPull), NEVER HasLabel (244).
        let mut s2 = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let pp = super::super::wire::encode_push_pull(&[], b"", true);
        let labeled = super::super::wire::add_label_header(&pp, "prod-a").unwrap();
        s2.write_all(&[2u8]).await.unwrap();
        s2.write_all(&labeled).await.unwrap();
        let mut first = [0u8; 1];
        s2.read_exact(&mut first).await.unwrap();
        assert_ne!(
            first[0],
            super::super::wire::MessageType::HasLabel as u8,
            "server reply must NOT prepend HasLabel (244)"
        );

        kv.shutdown().await;
    }
