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
