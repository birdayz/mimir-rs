//! Multi KV client runtime configuration (primary switch / mirroring) and mirror writes.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/multi.go (`watchConfigChannel`, `setNewPrimaryClient`, `CAS`,
//!   `writeToSecondary`) and the behaviour covered by dskit's kv/multi_test.go
//!   (`TestMultiRuntimeConfigWithVariousEnabledValues`, primary switch tests)
//! - github.com/grafana/mimir/pkg/mimir/runtime_config.go (`multiClientRuntimeConfigChannel`)

use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::ServiceContext;
use mimir_config::ingester::{KVStoreConfig, MultiKVConfig};
use mimir_config::{
    GoDuration, InstanceLimits, LimitsConfig, RuntimeConfigConfig, RuntimeConfigManager,
    StringSliceCSV,
};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::multi::{
    runtime_config_provider, MultiConfigProvider, MultiKv, MultiRuntimeConfig, NamedKvClient,
};
use mimir_ring::kv::{
    build_kv_client_with_multi_config_provider, get_typed, CasOutcome, Codec, KvClient,
    StringCodec,
};
use prometheus::Registry;
use tokio::sync::mpsc;

fn codec() -> Arc<dyn Codec> {
    Arc::new(StringCodec)
}

struct Setup {
    multi: Arc<MultiKv>,
    a: Arc<InMemoryKv>,
    b: Arc<InMemoryKv>,
    reg: Registry,
}

fn setup(mirror_enabled: bool, provider: Option<MultiConfigProvider>) -> Setup {
    let a = Arc::new(InMemoryKv::new(codec()));
    let b = Arc::new(InMemoryKv::new(codec()));
    let reg = Registry::new();
    let cfg = MultiKVConfig {
        primary: "a".to_string(),
        secondary: "b".to_string(),
        mirror_enabled,
        mirror_timeout: GoDuration::from_secs(2),
    };
    let multi = MultiKv::new_with_options(
        cfg,
        vec![
            NamedKvClient {
                name: "a".to_string(),
                client: a.clone() as Arc<dyn KvClient>,
            },
            NamedKvClient {
                name: "b".to_string(),
                client: b.clone() as Arc<dyn KvClient>,
            },
        ],
        Some(&reg),
        Some(codec()),
        provider,
    );
    Setup { multi, a, b, reg }
}

fn channel_provider() -> (mpsc::Sender<MultiRuntimeConfig>, MultiConfigProvider) {
    let (tx, rx) = mpsc::channel(1);
    let rx = parking_lot::Mutex::new(Some(rx));
    let provider: MultiConfigProvider =
        Arc::new(move || rx.lock().take().expect("provider called once"));
    (tx, provider)
}

async fn put(kv: &dyn KvClient, key: &str, val: &str) {
    let v = val.to_string();
    kv.cas(key, Box::new(move |_| Ok(CasOutcome::Update(Box::new(v.clone())))))
        .await
        .unwrap();
}

async fn get(kv: &dyn KvClient, key: &str) -> Option<String> {
    get_typed::<String>(kv, key).await.unwrap()
}

fn gauge(reg: &Registry, name: &str, label: Option<(&str, &str)>) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let matches = match label {
                None => true,
                Some((k, v)) => m
                    .get_label()
                    .iter()
                    .any(|l| l.get_name() == k && l.get_value() == v),
            };
            if matches {
                return if m.has_gauge() {
                    m.get_gauge().get_value()
                } else {
                    m.get_counter().get_value()
                };
            }
        }
    }
    panic!("metric {name} {label:?} not found");
}

async fn eventually(mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met in time");
}

#[tokio::test]
async fn runtime_config_switches_primary_and_toggles_mirroring() {
    let (tx, provider) = channel_provider();
    let s = setup(false, Some(provider));
    assert_eq!(s.multi.primary_store(), "a");
    assert_eq!(gauge(&s.reg, "multikv_primary_store", Some(("store", "a"))), 1.0);
    assert_eq!(gauge(&s.reg, "multikv_mirror_enabled", None), 0.0);

    put(s.multi.as_ref(), "k", "v1").await;
    assert_eq!(get(s.a.as_ref(), "k").await.as_deref(), Some("v1"));
    assert_eq!(get(s.b.as_ref(), "k").await, None);

    // Empty primary and nil mirroring are ignored.
    tx.send(MultiRuntimeConfig::default()).await.unwrap();
    // Switch primary to "b".
    tx.send(MultiRuntimeConfig {
        primary_store: "b".to_string(),
        mirroring: None,
    })
    .await
    .unwrap();
    let m = s.multi.clone();
    eventually(move || m.primary_store() == "b").await;
    assert_eq!(gauge(&s.reg, "multikv_primary_store", Some(("store", "a"))), 0.0);
    assert_eq!(gauge(&s.reg, "multikv_primary_store", Some(("store", "b"))), 1.0);
    assert_eq!(get(s.multi.as_ref(), "k").await, None, "reads go to the new primary");
    put(s.multi.as_ref(), "k", "v2").await;
    assert_eq!(get(s.b.as_ref(), "k").await.as_deref(), Some("v2"));
    assert_eq!(get(s.a.as_ref(), "k").await.as_deref(), Some("v1"));

    // Enable mirroring: writes to primary "b" are mirrored to "a".
    tx.send(MultiRuntimeConfig {
        primary_store: String::new(),
        mirroring: Some(true),
    })
    .await
    .unwrap();
    let m = s.multi.clone();
    eventually(move || m.mirroring_enabled()).await;
    assert_eq!(gauge(&s.reg, "multikv_mirror_enabled", None), 1.0);
    put(s.multi.as_ref(), "k", "v3").await;
    assert_eq!(get(s.a.as_ref(), "k").await.as_deref(), Some("v3"));
    assert_eq!(gauge(&s.reg, "multikv_mirror_writes_total", None), 1.0);
    assert_eq!(gauge(&s.reg, "multikv_mirror_write_errors_total", None), 0.0);

    // Unknown store: primary unchanged, but the mirroring value is still applied.
    tx.send(MultiRuntimeConfig {
        primary_store: "consul".to_string(),
        mirroring: Some(false),
    })
    .await
    .unwrap();
    let m = s.multi.clone();
    eventually(move || !m.mirroring_enabled()).await;
    assert_eq!(s.multi.primary_store(), "b");
    put(s.multi.as_ref(), "k", "v4").await;
    assert_eq!(get(s.a.as_ref(), "k").await.as_deref(), Some("v3"));
}

#[tokio::test]
async fn mirroring_follows_cas_result() {
    let s = setup(true, None);
    // No change -> nothing mirrored.
    s.multi
        .cas("k", Box::new(|_| Ok(CasOutcome::NoChange)))
        .await
        .unwrap();
    assert_eq!(gauge(&s.reg, "multikv_mirror_writes_total", None), 0.0);

    // Update -> value stored in both.
    put(s.multi.as_ref(), "k", "x").await;
    assert_eq!(get(s.a.as_ref(), "k").await.as_deref(), Some("x"));
    assert_eq!(get(s.b.as_ref(), "k").await.as_deref(), Some("x"));
    assert_eq!(gauge(&s.reg, "multikv_mirror_writes_total", None), 1.0);
}

#[tokio::test]
async fn watch_moves_to_new_primary_on_switch() {
    let (tx, provider) = channel_provider();
    let s = setup(false, Some(provider));
    let (wtx, mut wrx) = mpsc::unbounded_channel::<String>();
    let ctx = ServiceContext::background();
    let m = s.multi.clone();
    let wctx = ctx.clone();
    let handle = tokio::spawn(async move {
        m.watch_key(
            "k",
            wctx,
            Box::new(move |v| {
                let _ = wtx.send(*v.downcast::<String>().unwrap());
                true
            }),
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    put(s.a.as_ref(), "k", "from-a").await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), wrx.recv()).await.unwrap().as_deref(),
        Some("from-a")
    );

    tx.send(MultiRuntimeConfig {
        primary_store: "b".to_string(),
        mirroring: None,
    })
    .await
    .unwrap();
    let m = s.multi.clone();
    eventually(move || m.primary_store() == "b").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    put(s.b.as_ref(), "k", "from-b").await;
    let mut got = Vec::new();
    while let Ok(Some(v)) = tokio::time::timeout(Duration::from_secs(5), wrx.recv()).await {
        got.push(v.clone());
        if v == "from-b" {
            break;
        }
    }
    assert_eq!(got.last().map(String::as_str), Some("from-b"));

    ctx.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

/// End-to-end: `multi_kv_config` in the runtime config file flips the primary of a `multi`
/// KV client built from `KVStoreConfig`, as modules.go wires `multiClientRuntimeConfigChannel`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_config_file_flips_primary_of_multi_store() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("runtime.yaml");
    std::fs::write(&file, "multi_kv_config:\n  mirror_enabled: true\n").unwrap();

    let mgr = RuntimeConfigManager::new(
        RuntimeConfigConfig {
            file: StringSliceCSV(vec![file.to_str().unwrap().to_string()]),
            period: GoDuration::from_secs(3600),
            ..Default::default()
        },
        LimitsConfig::default(),
        InstanceLimits::default(),
    )
    .unwrap();

    // Two backends that need no external service: an inmemory primary and a consul secondary
    // (the consul client does not connect until used).
    let kv_cfg = KVStoreConfig {
        store: "multi".to_string(),
        multi: MultiKVConfig {
            primary: "inmemory".to_string(),
            secondary: "consul".to_string(),
            mirror_enabled: false,
            mirror_timeout: GoDuration::from_millis(200),
        },
        ..Default::default()
    };
    let reg = Registry::new();
    let client = build_kv_client_with_multi_config_provider(
        &kv_cfg,
        codec(),
        None,
        Some(&reg),
        Some(runtime_config_provider(&mgr)),
    )
    .unwrap();

    // Initial runtime value (mirror_enabled: true) is pushed on creation.
    let r = reg.clone();
    eventually(move || gauge(&r, "multikv_mirror_enabled", None) == 1.0).await;
    assert_eq!(gauge(&reg, "multikv_primary_store", Some(("store", "inmemory"))), 1.0);

    std::fs::write(
        &file,
        "multi_kv_config:\n  primary: consul\n  mirror_enabled: false\n",
    )
    .unwrap();
    mgr.reload().unwrap();
    let r = reg.clone();
    eventually(move || {
        gauge(&r, "multikv_primary_store", Some(("store", "consul"))) == 1.0
            && gauge(&r, "multikv_mirror_enabled", None) == 0.0
    })
    .await;
    assert_eq!(gauge(&reg, "multikv_primary_store", Some(("store", "inmemory"))), 0.0);

    // And back.
    std::fs::write(&file, "multi_kv_config:\n  primary: inmemory\n").unwrap();
    mgr.reload().unwrap();
    let r = reg.clone();
    eventually(move || gauge(&r, "multikv_primary_store", Some(("store", "inmemory"))) == 1.0)
        .await;
    drop(client);
}
