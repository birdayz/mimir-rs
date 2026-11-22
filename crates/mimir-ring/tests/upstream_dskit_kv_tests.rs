//! Ported from:
//! - github.com/grafana/dskit/kv/client_test.go
//! - github.com/grafana/dskit/kv/kv_test.go
//! - github.com/grafana/dskit/kv/multi_test.go
//! - github.com/grafana/dskit/kv/etcd/mock_test.go

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::ServiceContext;
use mimir_config::ingester::{KVStoreConfig, MultiKVConfig};
use mimir_config::yaml_v3;
use mimir_config::{
    parse_cli_and_load_config_test_mode, parse_runtime_config_yaml, FlagSet, InstanceLimits,
    LimitsConfig,
};
use mimir_ring::kv::etcd::EtcdKv;
use mimir_ring::kv::etcd_mock::{
    CmpOp, CmpTarget, MockCmp, MockEtcdKv, MockEventType, MockKeyValue, MockOp,
};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::mock::MockKv;
use mimir_ring::kv::multi::{
    MultiConfigProvider, MultiKv, MultiRuntimeConfig, NamedKvClient,
};
use mimir_ring::kv::{
    build_kv_client, get_typed, CasOutcome, Codec, KvClient, KvError, StringCodec,
};
use prometheus::Registry;
use tokio::sync::mpsc;

// ============================================================================
// github.com/grafana/dskit/kv/client_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/client_test.go::TestParseConfig
#[test]
fn test_parse_config() {
    // Verify YAML parsing via Mimir's yaml_v3 + full config loader.
    let conf_yaml = r#"
ingester:
  ring:
    kvstore:
      store: consul
      consul:
        host: "consul:8500"
        consistent_reads: true
      prefix: "test/"
      multi:
        primary: consul
        secondary: etcd
"#;
    let dir = tempfile::TempDir::new().unwrap();
    let cfg_path = dir.path().join("mimir.yaml");
    std::fs::write(&cfg_path, conf_yaml).unwrap();
    let loaded = parse_cli_and_load_config_test_mode(
        "mimir",
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", cfg_path.display()),
        ],
    )
    .unwrap();
    let kv = &loaded.config.ingester.ring.kvstore;
    assert_eq!("consul", kv.store);
    assert_eq!("test/", kv.prefix);
    assert_eq!("consul:8500", kv.consul.host);
    assert!(kv.consul.consistent_reads);
    assert_eq!("consul", kv.multi.primary);
    assert_eq!("etcd", kv.multi.secondary);

    // Also parse the raw dskit yaml.v3 document tree directly.
    let raw_dskit_yaml = r#"
store: consul
consul:
  host: "consul:8500"
  consistentreads: true
prefix: "test/"
multi:
  primary: consul
  secondary: etcd
"#;
    let doc = yaml_v3::parse_first_document(raw_dskit_yaml.as_bytes())
        .unwrap()
        .unwrap();
    let val = yaml_v3::doc_to_serde_value(&doc).unwrap();
    assert_eq!("consul", val["store"].as_str().unwrap());
    assert_eq!("test/", val["prefix"].as_str().unwrap());
    assert_eq!("consul:8500", val["consul"]["host"].as_str().unwrap());
    assert_eq!("consul", val["multi"]["primary"].as_str().unwrap());
    assert_eq!("etcd", val["multi"]["secondary"].as_str().unwrap());
}

// Upstream: github.com/grafana/dskit/kv/client_test.go::TestDefaultStoreValue
#[test]
fn test_default_store_value() {
    // With default store = "consul", flag registration keeps "consul".
    let mut cfg1 = KVStoreConfig {
        store: "consul".to_string(),
        ..KVStoreConfig::default()
    };
    let mut fs1 = FlagSet::<KVStoreConfig>::new();
    cfg1.register_flags_into("", "", &mut fs1, |c| c, |c| c);
    fs1.parse(&mut cfg1, &[]).unwrap();
    assert_eq!("consul", cfg1.store);

    // Pre-set store = "memberlist" is preserved by RegisterFlagsWithPrefix.
    let mut cfg2 = KVStoreConfig {
        store: "memberlist".to_string(),
        ..KVStoreConfig::default()
    };
    let mut fs2 = FlagSet::<KVStoreConfig>::new();
    cfg2.register_flags_into("", "", &mut fs2, |c| c, |c| c);
    fs2.parse(&mut cfg2, &[]).unwrap();
    assert_eq!("memberlist", cfg2.store);
}

struct FixedStringCodec {
    value: &'static str,
}

impl Codec for FixedStringCodec {
    fn codec_id(&self) -> &str {
        self.value
    }

    fn decode(&self, _bytes: &[u8]) -> Result<Box<dyn Any + Send + Sync>, KvError> {
        Ok(Box::new(self.value.to_string()))
    }

    fn encode(&self, _msg: &(dyn Any + Send + Sync)) -> Result<Vec<u8>, KvError> {
        Ok(self.value.as_bytes().to_vec())
    }
}

// Upstream: github.com/grafana/dskit/kv/client_test.go::TestMultipleInMemoryClient
#[tokio::test]
async fn test_multiple_in_memory_client() {
    let cfg = KVStoreConfig {
        store: "inmemory".to_string(),
        prefix: String::new(),
        ..KVStoreConfig::default()
    };
    let foo = build_kv_client(
        &cfg,
        Arc::new(FixedStringCodec { value: "foo" }),
        None,
        Some(&Registry::new()),
    )
    .unwrap();
    let bar = build_kv_client(
        &cfg,
        Arc::new(FixedStringCodec { value: "bar" }),
        None,
        Some(&Registry::new()),
    )
    .unwrap();

    foo.cas(
        "foo",
        Box::new(|_| Ok(CasOutcome::UpdateOnce(Box::new("foo".to_string())))),
    )
    .await
    .unwrap();
    let foo_key = get_typed::<String>(foo.as_ref(), "foo").await.unwrap();
    assert_eq!(Some("foo".to_string()), foo_key);

    bar.cas(
        "bar",
        Box::new(|_| Ok(CasOutcome::UpdateOnce(Box::new("bar".to_string())))),
    )
    .await
    .unwrap();
    let bar_key = get_typed::<String>(bar.as_ref(), "bar").await.unwrap();
    assert_eq!(Some("bar".to_string()), bar_key);
}

// ============================================================================
// github.com/grafana/dskit/kv/kv_test.go
// ============================================================================

struct FixtureGuard {
    _mock_etcd: Option<Arc<MockEtcdKv>>,
}

impl Drop for FixtureGuard {
    fn drop(&mut self) {
        if let Some(m) = &self._mock_etcd {
            m.close();
        }
    }
}

fn make_fixtures() -> Vec<(&'static str, Arc<dyn KvClient>, FixtureGuard)> {
    let consul_client: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(Arc::new(StringCodec)));
    let (etcd_client, etcd_mock) = EtcdKv::new_in_memory_client(Arc::new(StringCodec));
    vec![
        (
            "consul",
            consul_client,
            FixtureGuard { _mock_etcd: None },
        ),
        (
            "etcd",
            Arc::new(etcd_client),
            FixtureGuard {
                _mock_etcd: Some(etcd_mock),
            },
        ),
    ]
}

// Upstream: github.com/grafana/dskit/kv/kv_test.go::TestCAS
#[tokio::test]
async fn test_cas() {
    const KEY: &str = "/key";
    for (name, client, _guard) in make_fixtures() {
        client
            .cas(
                KEY,
                Box::new(|_| Ok(CasOutcome::Update(Box::new("0".to_string())))),
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: initial CAS failed: {e}"));

        for i in 0..10 {
            let expected = i.to_string();
            let next = (i + 1).to_string();
            client
                .cas(
                    KEY,
                    Box::new(move |cur| {
                        let got = cur
                            .and_then(|a| a.downcast_ref::<String>())
                            .expect("expected String in CAS");
                        assert_eq!(&expected, got);
                        Ok(CasOutcome::Update(Box::new(next.clone())))
                    }),
                )
                .await
                .unwrap_or_else(|e| panic!("{name}: CAS step {i} failed: {e}"));
        }

        let val = get_typed::<String>(client.as_ref(), KEY).await.unwrap();
        assert_eq!(Some("10".to_string()), val, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/kv/kv_test.go::TestNilCAS
#[tokio::test]
async fn test_nil_cas() {
    const KEY: &str = "/key";
    for (name, client, _guard) in make_fixtures() {
        client
            .cas(
                KEY,
                Box::new(|_| Ok(CasOutcome::Update(Box::new("0".to_string())))),
            )
            .await
            .unwrap();

        client
            .cas(
                KEY,
                Box::new(|cur| {
                    let got = cur
                        .and_then(|a| a.downcast_ref::<String>())
                        .expect("expected String");
                    assert_eq!("0", got);
                    Ok(CasOutcome::NoChange)
                }),
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: nil CAS failed: {e}"));

        let val = get_typed::<String>(client.as_ref(), KEY).await.unwrap();
        assert_eq!(Some("0".to_string()), val, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/kv/kv_test.go::TestWatchKey
#[tokio::test]
async fn test_watch_key() {
    const KEY: &str = "test";
    const MAX: usize = 100;
    const SLEEP: Duration = Duration::from_millis(15);
    const TOTAL_TIMEOUT: Duration = Duration::from_millis(3 * (MAX as u64) * 15);
    const EXPECTED_FACTOR: f64 = 0.75;

    for (name, client, _guard) in make_fixtures() {
        let (tx, mut rx) = mpsc::channel::<String>(MAX);
        let ctx = ServiceContext::background();

        let client_watch = client.clone();
        let ctx_watch = ctx.clone();
        let watch_handle = tokio::spawn(async move {
            client_watch
                .watch_key(
                    KEY,
                    ctx_watch,
                    Box::new(move |val| {
                        let s = val.downcast_ref::<String>().unwrap().clone();
                        let _ = tx.try_send(s);
                        true
                    }),
                )
                .await;
        });

        let client_gen = client.clone();
        let ctx_gen = ctx.clone();
        let gen_handle = tokio::spawn(async move {
            for i in 0..MAX {
                tokio::time::sleep(SLEEP).await;
                if ctx_gen.is_canceled() {
                    break;
                }
                let val_str = i.to_string();
                let res = client_gen
                    .cas(
                        KEY,
                        Box::new(move |_| Ok(CasOutcome::Update(Box::new(val_str.clone())))),
                    )
                    .await;
                if ctx_gen.is_canceled() {
                    break;
                }
                res.unwrap();
            }
        });

        let mut last_observed: i64 = -1;
        let mut observed_count: usize = 0;
        let deadline = tokio::time::sleep(TOTAL_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => break,
                maybe_val = rx.recv() => {
                    let Some(val_str) = maybe_val else { break };
                    let val: i64 = val_str.parse().unwrap_or_else(|_| panic!("{name}: unexpected value: {val_str}"));
                    assert!(val > last_observed, "{name}: non-increasing {val} <= {last_observed}");
                    last_observed = val;
                    observed_count += 1;
                    if (observed_count as f64) >= EXPECTED_FACTOR * (MAX as f64) {
                        break;
                    }
                }
            }
        }

        ctx.cancel();
        let _ = tokio::join!(watch_handle, gen_handle);

        assert!(
            (observed_count as f64) >= EXPECTED_FACTOR * (MAX as f64),
            "{name}: expected at least 75% observed values, got {observed_count}/{MAX}"
        );
    }
}

// Upstream: github.com/grafana/dskit/kv/kv_test.go::TestWatchPrefix
#[tokio::test]
async fn test_watch_prefix() {
    const PREFIX: &str = "test/";
    const PREFIX2: &str = "ignore/";
    const MAX: usize = 100;
    const SLEEP: Duration = Duration::from_millis(10);
    const TOTAL_TIMEOUT: Duration = Duration::from_secs(10);

    for (name, client, _guard) in make_fixtures() {
        let (tx, mut rx) = mpsc::channel::<String>(MAX * 2);
        let ctx = ServiceContext::background();

        let client_watch = client.clone();
        let ctx_watch = ctx.clone();
        let watch_handle = tokio::spawn(async move {
            client_watch
                .watch_prefix(
                    PREFIX,
                    ctx_watch,
                    Box::new(move |key, _| {
                        let _ = tx.try_send(key.to_string());
                        true
                    }),
                )
                .await;
        });

        let spawn_gen = |p: &'static str, c: Arc<dyn KvClient>, cx: ServiceContext| {
            tokio::spawn(async move {
                for i in 0..MAX {
                    if cx.is_canceled() {
                        break;
                    }
                    tokio::time::sleep(SLEEP).await;
                    if cx.is_canceled() {
                        break;
                    }
                    let key = format!("{p}{i}");
                    let val = key.clone();
                    let res = c
                        .cas(
                            &key,
                            Box::new(move |_| Ok(CasOutcome::Update(Box::new(val.clone())))),
                        )
                        .await;
                    if cx.is_canceled() {
                        break;
                    }
                    res.unwrap();
                }
            })
        };

        let gen1 = spawn_gen(PREFIX, client.clone(), ctx.clone());
        let gen2 = spawn_gen(PREFIX2, client.clone(), ctx.clone());

        let mut observed: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let deadline = tokio::time::sleep(TOTAL_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => break,
                maybe_key = rx.recv() => {
                    let Some(k) = maybe_key else { break };
                    *observed.entry(k).or_default() += 1;
                    if observed.len() == MAX {
                        break;
                    }
                }
            }
        }

        ctx.cancel();
        let _ = tokio::join!(watch_handle, gen1, gen2);

        for i in 0..MAX {
            let key = format!("{PREFIX}{i}");
            let count = observed.remove(&key).unwrap_or(0);
            assert_eq!(1, count, "{name}: key {key} count {count}");
        }
        assert!(
            observed.is_empty(),
            "{name}: unexpected keys reported: {observed:?}"
        );
    }
}

// Upstream: github.com/grafana/dskit/kv/kv_test.go::TestList
#[tokio::test]
async fn test_list() {
    let keys_to_create = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    for (name, client, _guard) in make_fixtures() {
        for key in &keys_to_create {
            let v = key.clone();
            client
                .cas(
                    key,
                    Box::new(move |_| Ok(CasOutcome::UpdateOnce(Box::new(v.clone())))),
                )
                .await
                .unwrap();
        }
        let mut stored = client.list("").await.unwrap();
        stored.sort();
        assert_eq!(keys_to_create, stored, "{name}");
    }
}

// ============================================================================
// github.com/grafana/dskit/kv/multi_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/kv/multi_test.go::TestMultiRuntimeConfigWithVariousEnabledValues
#[test]
fn test_multi_runtime_config_with_various_enabled_values() {
    let cases = [
        ("nil", "multi_kv_config:
  primary: test
", None),
        (
            "true",
            "multi_kv_config:
  primary: test
  mirror_enabled: true
",
            Some(true),
        ),
        (
            "false",
            "multi_kv_config:
  mirror_enabled: false
",
            Some(false),
        ),
    ];

    for (name, yaml, expected) in cases {
        let parsed = parse_runtime_config_yaml(
            yaml.as_bytes(),
            &LimitsConfig::default(),
            &InstanceLimits::default(),
        )
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(expected, parsed.multi_kv_config.mirror_enabled, "{name}");
        let rt = MultiRuntimeConfig::from(&parsed.multi_kv_config);
        assert_eq!(expected, rt.mirroring, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/kv/multi_test.go::TestNewMulti
#[tokio::test]
async fn test_new_multi() {
    let provider: MultiConfigProvider = Arc::new(|| {
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.try_send(MultiRuntimeConfig {
            primary_store: "b".to_string(),
            mirroring: None,
        });
        rx
    });

    let clients = vec![
        NamedKvClient {
            name: "a".to_string(),
            client: Arc::new(MockKv::new()),
        },
        NamedKvClient {
            name: "b".to_string(),
            client: Arc::new(MockKv::new()),
        },
    ];

    let mc = MultiKv::new_with_options(
        MultiKVConfig::default(),
        clients,
        None::<&Registry>,
        None,
        Some(provider),
    );
    drop(mc);
}

// ============================================================================
// github.com/grafana/dskit/kv/etcd/mock_test.go
// ============================================================================

fn mk_kv(key: &str, val: &str) -> MockKeyValue {
    MockKeyValue {
        key: key.as_bytes().to_vec(),
        value: val.as_bytes().to_vec(),
        ..Default::default()
    }
}

// Upstream: github.com/grafana/dskit/kv/etcd/mock_test.go::TestMockKv_Get
#[test]
fn test_mock_kv_get() {
    // exact match
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw("/foo", mk_kv("/foo", "1"));
        let res = kv.get("/foo", false);
        assert_eq!(1, res.kvs.len());
        assert_eq!(b"/foo", res.kvs[0].key.as_slice());
    }

    // not exact match
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw("/foo", mk_kv("/foo", "1"));
        let res = kv.get("/bar", false);
        assert!(res.kvs.is_empty());
    }

    // prefix match
    {
        let kv = MockEtcdKv::new();
        let foo = mk_kv("/foo", "1");
        let baz = mk_kv("/baz", "2");
        let first = mk_kv("/first", "3");
        kv.insert_raw("/foo", foo.clone());
        kv.insert_raw("/baz", baz);
        kv.insert_raw("/first", first.clone());
        let mut res = kv.get("/f", true);
        res.kvs.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(vec![first, foo], res.kvs);
    }

    // empty prefix
    {
        let kv = MockEtcdKv::new();
        let foo = mk_kv("/foo", "1");
        let baz = mk_kv("/baz", "2");
        let first = mk_kv("/first", "3");
        kv.insert_raw("/foo", foo.clone());
        kv.insert_raw("/baz", baz.clone());
        kv.insert_raw("/first", first.clone());
        let mut res = kv.get("", true);
        res.kvs.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(vec![baz, first, foo], res.kvs);
    }
}

// Upstream: github.com/grafana/dskit/kv/etcd/mock_test.go::TestMockKV_Put
#[test]
fn test_mock_kv_put() {
    // new key
    {
        let kv = MockEtcdKv::new();
        kv.put("/foo", b"1".to_vec());
        let got = kv.get_raw("/foo").unwrap();
        assert_eq!(1, got.version);
        assert_eq!(b"1", got.value.as_slice());
    }

    // existing key
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw(
            "/foo",
            MockKeyValue {
                key: b"/foo".to_vec(),
                create_revision: 1,
                mod_revision: 2,
                version: 2,
                value: b"1".to_vec(),
                lease: 0,
            },
        );
        kv.put("/foo", b"2".to_vec());
        let got = kv.get_raw("/foo").unwrap();
        assert_eq!(3, got.version);
        assert_eq!(b"2", got.value.as_slice());
    }
}

// Upstream: github.com/grafana/dskit/kv/etcd/mock_test.go::TestMockKV_Delete
#[test]
fn test_mock_kv_delete() {
    // exact match
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw("/foo", mk_kv("/foo", "1"));
        let res = kv.delete("/foo", false);
        assert_eq!(1, res.deleted);
        assert!(kv.is_empty());
    }

    // prefix match
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw("/foo", mk_kv("/foo", "1"));
        kv.insert_raw("/baz", mk_kv("/baz", "2"));
        kv.insert_raw("/first", mk_kv("/first", "3"));
        let res = kv.delete("/f", true);
        assert_eq!(2, res.deleted);
        assert!(!kv.contains_key("/foo"));
        assert!(!kv.contains_key("/first"));
    }

    // empty prefix
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw("/foo", mk_kv("/foo", "1"));
        kv.insert_raw("/baz", mk_kv("/baz", "2"));
        kv.insert_raw("/first", mk_kv("/first", "3"));
        let res = kv.delete("", true);
        assert_eq!(3, res.deleted);
        assert!(!kv.contains_key("/foo"));
        assert!(!kv.contains_key("/baz"));
        assert!(!kv.contains_key("/first"));
    }
}

// Upstream: github.com/grafana/dskit/kv/etcd/mock_test.go::TestMockKV_EvalCmp
#[test]
fn test_mock_kv_eval_cmp() {
    const KEY: &str = "/foo";
    let kv = MockEtcdKv::new();
    kv.insert_raw(
        KEY,
        MockKeyValue {
            key: KEY.as_bytes().to_vec(),
            value: b"m".to_vec(),
            create_revision: 1,
            lease: 2,
            version: 3,
            mod_revision: 4,
        },
    );

    let tests = [
        ("value equal", CmpOp::Equal, CmpTarget::Value(b"m".to_vec()), true),
        ("value not equal", CmpOp::Equal, CmpTarget::Value(b"n".to_vec()), false),
        ("create revision equal", CmpOp::Equal, CmpTarget::CreateRevision(1), true),
        ("create revision not equal", CmpOp::Equal, CmpTarget::CreateRevision(2), false),
        ("lease equal", CmpOp::Equal, CmpTarget::Lease(2), true),
        ("lease not equal", CmpOp::Equal, CmpTarget::Lease(3), false),
        ("version equal", CmpOp::Equal, CmpTarget::Version(3), true),
        ("version not equal", CmpOp::Equal, CmpTarget::Version(4), false),
        ("mod revision equal", CmpOp::Equal, CmpTarget::ModRevision(4), true),
        ("mod revision not equal", CmpOp::Equal, CmpTarget::ModRevision(5), false),
        ("value unequal true", CmpOp::NotEqual, CmpTarget::Value(b"a".to_vec()), true),
        ("value unequal false", CmpOp::NotEqual, CmpTarget::Value(b"m".to_vec()), false),
        ("value greater true", CmpOp::Greater, CmpTarget::Value(b"a".to_vec()), true),
        ("value greater false", CmpOp::Greater, CmpTarget::Value(b"z".to_vec()), false),
        ("value less true", CmpOp::Less, CmpTarget::Value(b"z".to_vec()), true),
        ("value less false", CmpOp::Less, CmpTarget::Value(b"a".to_vec()), false),
        ("version unequal true", CmpOp::NotEqual, CmpTarget::Version(2), true),
        ("version unequal false", CmpOp::NotEqual, CmpTarget::Version(3), false),
        ("version greater true", CmpOp::Greater, CmpTarget::Version(2), true),
        ("version greater false", CmpOp::Greater, CmpTarget::Version(4), false),
        ("version less true", CmpOp::Less, CmpTarget::Version(4), true),
        ("version less false", CmpOp::Less, CmpTarget::Version(2), false),
    ];

    for (name, op, target, want) in tests {
        let cmp = MockCmp {
            key: KEY.to_string(),
            op,
            target,
        };
        assert_eq!(want, kv.eval_cmp(&cmp), "{name}");
    }
}

// Upstream: github.com/grafana/dskit/kv/etcd/mock_test.go::TestMockKV_Txn
#[test]
fn test_mock_kv_txn() {
    // success compare value
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw(
            "/foo",
            MockKeyValue {
                key: b"/foo".to_vec(),
                create_revision: 1,
                mod_revision: 3,
                version: 3,
                value: b"1".to_vec(),
                lease: 0,
            },
        );
        let res = kv.txn(
            &[MockCmp {
                key: "/foo".to_string(),
                op: CmpOp::Equal,
                target: CmpTarget::Value(b"1".to_vec()),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"2".to_vec(),
            }],
            &[],
        );
        assert!(res.succeeded);
        assert_eq!(b"2", kv.get_raw("/foo").unwrap().value.as_slice());
    }

    // failure compare value
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw(
            "/foo",
            MockKeyValue {
                key: b"/foo".to_vec(),
                create_revision: 1,
                mod_revision: 3,
                version: 3,
                value: b"2".to_vec(),
                lease: 0,
            },
        );
        let res = kv.txn(
            &[MockCmp {
                key: "/foo".to_string(),
                op: CmpOp::Equal,
                target: CmpTarget::Value(b"3".to_vec()),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"4".to_vec(),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"-1".to_vec(),
            }],
        );
        assert!(!res.succeeded);
        assert_eq!(b"-1", kv.get_raw("/foo").unwrap().value.as_slice());
    }

    // success compare version exists
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw(
            "/foo",
            MockKeyValue {
                key: b"/foo".to_vec(),
                create_revision: 1,
                mod_revision: 3,
                version: 3,
                value: b"1".to_vec(),
                lease: 0,
            },
        );
        let res = kv.txn(
            &[MockCmp {
                key: "/foo".to_string(),
                op: CmpOp::Equal,
                target: CmpTarget::Version(3),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"2".to_vec(),
            }],
            &[],
        );
        assert!(res.succeeded);
        assert_eq!(b"2", kv.get_raw("/foo").unwrap().value.as_slice());
    }

    // failure compare version exists
    {
        let kv = MockEtcdKv::new();
        kv.insert_raw(
            "/foo",
            MockKeyValue {
                key: b"/foo".to_vec(),
                create_revision: 1,
                mod_revision: 3,
                version: 3,
                value: b"1".to_vec(),
                lease: 0,
            },
        );
        let res = kv.txn(
            &[MockCmp {
                key: "/foo".to_string(),
                op: CmpOp::Equal,
                target: CmpTarget::Version(2),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"2".to_vec(),
            }],
            &[],
        );
        assert!(!res.succeeded);
        assert_eq!(b"1", kv.get_raw("/foo").unwrap().value.as_slice());
    }

    // success compare version does not exist
    {
        let kv = MockEtcdKv::new();
        let res = kv.txn(
            &[MockCmp {
                key: "/foo".to_string(),
                op: CmpOp::Equal,
                target: CmpTarget::Version(0),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"1".to_vec(),
            }],
            &[],
        );
        assert!(res.succeeded);
        assert_eq!(b"1", kv.get_raw("/foo").unwrap().value.as_slice());
    }

    // failure compare version does not exist
    {
        let kv = MockEtcdKv::new();
        let res = kv.txn(
            &[MockCmp {
                key: "/foo".to_string(),
                op: CmpOp::Equal,
                target: CmpTarget::Version(1),
            }],
            &[MockOp::Put {
                key: "/foo".to_string(),
                value: b"2".to_vec(),
            }],
            &[],
        );
        assert!(!res.succeeded);
        assert!(!kv.contains_key("/foo"));
    }
}

// Upstream: github.com/grafana/dskit/kv/etcd/mock_test.go::TestMockKV_Watch
#[tokio::test]
async fn test_mock_kv_watch() {
    // watch stopped by context
    {
        let kv = MockEtcdKv::new();
        let ctx = ServiceContext::background();
        let mut rx = kv.watch(ctx.clone(), "/bar", false);
        ctx.cancel();
        let resp = rx.recv().await.unwrap();
        assert!(resp.canceled);
        assert!(rx.recv().await.is_none());
    }

    // watch stopped by close
    {
        let kv = MockEtcdKv::new();
        let ctx = ServiceContext::background();
        let mut rx = kv.watch(ctx, "/bar", false);
        kv.close();
        let resp = rx.recv().await.unwrap();
        assert!(resp.canceled);
        assert!(rx.recv().await.is_none());
    }

    // watch exact key
    {
        let kv = MockEtcdKv::new();
        let ctx = ServiceContext::background();
        let mut rx = kv.watch(ctx.clone(), "/bar", false);
        kv.put("/foo", b"1".to_vec());
        kv.put("/bar", b"1".to_vec());
        let resp = rx.recv().await.unwrap();
        assert_eq!(1, resp.events.len());
        assert_eq!(MockEventType::Put, resp.events[0].event_type);
        assert_eq!(b"/bar", resp.events[0].kv.key.as_slice());
        ctx.cancel();
    }

    // watch prefix match
    {
        let kv = MockEtcdKv::new();
        let ctx = ServiceContext::background();
        let mut rx = kv.watch(ctx.clone(), "/b", true);
        let _ = kv.delete("/foo", false);
        kv.put("/bar", b"1".to_vec());
        let resp = rx.recv().await.unwrap();
        assert_eq!(1, resp.events.len());
        assert_eq!(MockEventType::Put, resp.events[0].event_type);
        assert_eq!(b"/bar", resp.events[0].kv.key.as_slice());
        ctx.cancel();
    }
}
