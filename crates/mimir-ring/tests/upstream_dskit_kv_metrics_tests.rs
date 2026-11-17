//! Tests ported from github.com/grafana/dskit/kv/client_test.go (metrics /
//! role / type labels of `kv_request_duration_seconds`) plus Rust-side
//! coverage of github.com/grafana/dskit/kv/metrics.go and the Mimir
//! `-target=ingester` KV registerer wiring.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use mimir_common::services::ServiceContext;
use mimir_config::duration::GoDuration;
use mimir_config::ingester::{KVStoreConfig, MultiKVConfig, RingConfig};
use mimir_ring::kv::metrics::{error_code, get_cas_error_code, new_metrics_client};
use mimir_ring::kv::{
    build_kv_client, create_client, registerer_with_kv_name, BoxFuture, CasFn, CasOutcome,
    KvClient, KvError, Registerer, Role, StringCodec, WatchKeyFn, WatchPrefixFn,
};
use mimir_ring::ring::Ring;
use prometheus::{Encoder, Registry, TextEncoder};

/// Go `newConfigsForTest`.
fn new_configs_for_test() -> KVStoreConfig {
    KVStoreConfig {
        store: String::new(),
        prefix: String::new(),
        multi: MultiKVConfig {
            primary: "inmemory".to_string(),
            secondary: "mock".to_string(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cas_fn(value: &str) -> CasFn {
    let value = value.to_string();
    Box::new(move |_| Ok(CasOutcome::Update(Box::new(value.clone()))))
}

/// Go `typeToRoleMapHistogramLabels`.
fn type_to_role_map_histogram_labels(reg: &Registry, name: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let mut backend_type = String::new();
            let mut role = String::new();
            for l in m.get_label() {
                if l.get_name() == "role" {
                    role = l.get_value().to_string();
                } else if l.get_name() == "type" {
                    backend_type = l.get_value().to_string();
                }
            }
            assert!(!backend_type.is_empty());
            assert!(!role.is_empty());
            result.insert(backend_type, role);
        }
    }
    result
}

// Upstream: github.com/grafana/dskit/kv/client_test.go::Test_createClient_multiBackend_withSingleRing
#[tokio::test]
async fn test_create_client_multi_backend_with_single_ring() {
    let cfg = new_configs_for_test();
    let reg = Registry::new();
    create_client(
        "multi",
        "/collector",
        &cfg,
        Arc::new(StringCodec),
        None,
        Role::Primary,
        Some(&Registerer::new(&reg)),
    )
    .unwrap();
}

// Upstream: github.com/grafana/dskit/kv/client_test.go::Test_createClient_multiBackend_withMultiRing
#[tokio::test]
async fn test_create_client_multi_backend_with_multi_ring() {
    let cfg1 = new_configs_for_test();
    let cfg2 = KVStoreConfig::default();
    let reg = Registerer::new(&Registry::new());
    create_client(
        "multi",
        "/test",
        &cfg1,
        Arc::new(StringCodec),
        None,
        Role::Primary,
        Some(&reg),
    )
    .expect("First client for KV store must not fail");
    create_client(
        "mock",
        "/test",
        &cfg2,
        Arc::new(StringCodec),
        None,
        Role::Primary,
        Some(&reg),
    )
    .expect("Second client for KV store must not fail");
}

// Upstream: github.com/grafana/dskit/kv/client_test.go::Test_createClient_singleBackend_mustContainRoleAndTypeLabels
#[tokio::test]
async fn test_create_client_single_backend_must_contain_role_and_type_labels() {
    let cfg = new_configs_for_test();
    let reg = Registry::new();
    let client = create_client(
        "mock",
        "/test1",
        &cfg,
        Arc::new(StringCodec),
        None,
        Role::Primary,
        Some(&Registerer::new(&reg)),
    )
    .unwrap();
    client.cas("/test", cas_fn("inCAS")).await.unwrap();

    let actual = type_to_role_map_histogram_labels(&reg, "kv_request_duration_seconds");
    assert_eq!(actual.len(), 1);
    assert_eq!(actual["mock"], "primary");
}

// Upstream: github.com/grafana/dskit/kv/client_test.go::Test_createClient_multiBackend_mustContainRoleAndTypeLabels
#[tokio::test]
async fn test_create_client_multi_backend_must_contain_role_and_type_labels() {
    let mut cfg = new_configs_for_test();
    cfg.multi.mirror_enabled = true;
    cfg.multi.mirror_timeout = GoDuration::from_secs(10);
    let reg = Registry::new();
    let client = create_client(
        "multi",
        "/test1",
        &cfg,
        Arc::new(StringCodec),
        None,
        Role::Primary,
        Some(&Registerer::new(&reg)),
    )
    .unwrap();
    client.cas("/test", cas_fn("inCAS")).await.unwrap();

    let actual = type_to_role_map_histogram_labels(&reg, "kv_request_duration_seconds");
    // expected multi-primary, inmemory-primary and mock-secondary
    assert_eq!(actual.len(), 3, "{actual:?}");
    assert_eq!(actual["multi"], "primary");
    assert_eq!(actual["inmemory"], "primary");
    assert_eq!(actual["mock"], "secondary");
}

// ---------------------------------------------------------------------------
// Rust-side coverage of kv/metrics.go and Mimir's ingester wiring.
// ---------------------------------------------------------------------------

fn gather_text(reg: &Registry) -> String {
    let mut buf = Vec::new();
    TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

fn inmemory_cfg() -> KVStoreConfig {
    KVStoreConfig {
        store: "inmemory".to_string(),
        prefix: String::new(),
        ..Default::default()
    }
}

/// Mimir `pkg/ingester/ingester.go`: the lifecycler KV client is created with
/// `kv.RegistererWithKVName(cortexPrefixedRegisterer, "ingester-lifecycler")`.
#[tokio::test]
async fn ingester_lifecycler_kv_series_are_byte_identical_to_go() {
    let reg = Registry::new();
    let cortex = Registerer::new(&reg).wrap_with_prefix("cortex_");
    let kv_reg = registerer_with_kv_name(Some(&cortex), "ingester-lifecycler");
    let client = build_kv_client(
        &inmemory_cfg(),
        Arc::new(StringCodec),
        None,
        kv_reg.as_ref(),
    )
    .unwrap();
    client
        .cas("ingester-lifecycler-test-key", cas_fn("v"))
        .await
        .unwrap();
    client.get("ingester-lifecycler-test-key").await.unwrap();

    let text = gather_text(&reg);
    assert!(
        text.contains(
            "# HELP cortex_kv_request_duration_seconds Time spent on kv store requests.\n"
        ),
        "{text}"
    );
    assert!(
        text.contains("# TYPE cortex_kv_request_duration_seconds histogram\n"),
        "{text}"
    );
    assert!(
        text.contains(
            "cortex_kv_request_duration_seconds_count{kv_name=\"ingester-lifecycler\",operation=\"CAS\",role=\"primary\",status_code=\"200\",type=\"inmemory\"} 1\n"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "cortex_kv_request_duration_seconds_count{kv_name=\"ingester-lifecycler\",operation=\"GET\",role=\"primary\",status_code=\"200\",type=\"inmemory\"} 1\n"
        ),
        "{text}"
    );
    // Classic buckets are prometheus.DefBuckets.
    let les: Vec<&str> = text
        .lines()
        .filter(|l| {
            l.starts_with("cortex_kv_request_duration_seconds_bucket")
                && l.contains("operation=\"CAS\"")
        })
        .map(|l| {
            let i = l.find(",le=\"").or_else(|| l.find("{le=\"")).unwrap() + 5;
            let j = l[i..].find('"').unwrap();
            &l[i..i + j]
        })
        .collect();
    assert_eq!(
        les,
        vec!["0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"]
    );
}

/// Mimir `pkg/mimir/modules.go` initIngesterPartitionRing and
/// `pkg/ingester/ingester.go`: the partition ring KV clients use the
/// unprefixed registerer with `kv_name="ingester-partitions-{watcher,lifecycler}"`.
#[tokio::test]
async fn partition_ring_kv_series_are_unprefixed_like_go() {
    let reg = Registry::new();
    let base = Registerer::new(&reg);
    for name in [
        "ingester-partitions-watcher",
        "ingester-partitions-lifecycler",
    ] {
        let kv_reg = registerer_with_kv_name(Some(&base), name);
        let c = build_kv_client(
            &inmemory_cfg(),
            Arc::new(StringCodec),
            None,
            kv_reg.as_ref(),
        )
        .unwrap();
        c.list("").await.unwrap();
    }
    let text = gather_text(&reg);
    for name in [
        "ingester-partitions-watcher",
        "ingester-partitions-lifecycler",
    ] {
        let line = format!(
            "kv_request_duration_seconds_count{{kv_name=\"{name}\",operation=\"List\",role=\"primary\",status_code=\"200\",type=\"inmemory\"}} 1\n"
        );
        assert!(text.contains(&line), "{text}");
    }
}

/// dskit `ring.New` creates its KV client with `kv_name="<name>-ring"` on the
/// registerer it receives (Mimir: `cortex_`-prefixed for the ingester ring).
#[tokio::test]
async fn ring_new_registers_kv_metrics_with_ring_kv_name() {
    let reg = Registry::new();
    let cortex = Registerer::new(&reg).wrap_with_prefix("cortex_");
    let cfg = RingConfig {
        kvstore: inmemory_cfg(),
        ..Default::default()
    };
    Ring::new(&cfg, "ingester", "ring", None, Some(&cortex)).unwrap();
    // A second client with the same registerer/kv_name/type/role is a
    // duplicate registration (Go promauto panics).
    let kv_reg = registerer_with_kv_name(Some(&cortex), "ingester-ring");
    let dup = build_kv_client(
        &inmemory_cfg(),
        Arc::new(StringCodec),
        None,
        kv_reg.as_ref(),
    );
    assert!(dup.is_err());
    // A different kv_name is fine.
    let kv_reg = registerer_with_kv_name(Some(&cortex), "ingester-lifecycler");
    build_kv_client(
        &inmemory_cfg(),
        Arc::new(StringCodec),
        None,
        kv_reg.as_ref(),
    )
    .unwrap();
}

/// Without a registerer the raw client is returned and nothing is registered.
#[tokio::test]
async fn no_registerer_means_no_metrics() {
    let reg = Registry::new();
    let c =
        build_kv_client::<Registry>(&inmemory_cfg(), Arc::new(StringCodec), None, None).unwrap();
    c.cas("k", cas_fn("v")).await.unwrap();
    assert!(reg.gather().is_empty());
}

#[test]
fn cas_error_code_matches_go() {
    assert_eq!(get_cas_error_code(None), "200");
    assert_eq!(
        get_cas_error_code(Some(&KvError::HttpGrpc {
            code: 429,
            body: "too many".into()
        })),
        "429"
    );
    assert_eq!(
        get_cas_error_code(Some(&KvError::OperationAborted("aborted".into()))),
        "200"
    );
    assert_eq!(
        get_cas_error_code(Some(&KvError::Other("boom".into()))),
        "500"
    );
    assert_eq!(error_code(None), "200");
    // instrument.ErrorCode does not special-case httpgrpc errors.
    assert_eq!(
        error_code(Some(&KvError::HttpGrpc {
            code: 429,
            body: String::new()
        })),
        "500"
    );
}

/// A client failing every operation.
struct FailingKv(fn() -> KvError);

impl KvClient for FailingKv {
    fn list<'a>(&'a self, _p: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move { Err((self.0)()) })
    }
    fn get<'a>(
        &'a self,
        _k: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move { Err((self.0)()) })
    }
    fn delete<'a>(&'a self, _k: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move { Err((self.0)()) })
    }
    fn cas<'a>(&'a self, _k: &'a str, _f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move { Err((self.0)()) })
    }
    fn watch_key<'a>(
        &'a self,
        _k: &'a str,
        _c: ServiceContext,
        _f: WatchKeyFn,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
    fn watch_prefix<'a>(
        &'a self,
        _p: &'a str,
        _c: ServiceContext,
        _f: WatchPrefixFn,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

#[tokio::test]
async fn operations_and_status_codes_match_go() {
    let reg = Registry::new();
    let r = Registerer::new(&reg).wrap_with(Role::Primary.labels());
    let c = new_metrics_client(
        "consul",
        Arc::new(FailingKv(|| KvError::Other("x".into()))),
        &r,
    )
    .unwrap();
    assert!(c.list("p").await.is_err());
    assert!(c.get("k").await.is_err());
    assert!(c.delete("k").await.is_err());
    assert!(c.cas("k", cas_fn("v")).await.is_err());
    c.watch_key("k", ServiceContext::default(), Box::new(|_| false))
        .await;
    c.watch_prefix("p", ServiceContext::default(), Box::new(|_, _| false))
        .await;

    let reg2 = Registry::new();
    let r2 = Registerer::new(&reg2).wrap_with(Role::Secondary.labels());
    let c2 = new_metrics_client(
        "consul",
        Arc::new(FailingKv(|| KvError::HttpGrpc {
            code: 409,
            body: "conflict".into(),
        })),
        &r2,
    )
    .unwrap();
    assert!(c2.cas("k", cas_fn("v")).await.is_err());
    let text2 = gather_text(&reg2);
    assert!(text2.contains(
        "kv_request_duration_seconds_count{operation=\"CAS\",role=\"secondary\",status_code=\"409\",type=\"consul\"} 1\n"
    ), "{text2}");

    let text = gather_text(&reg);
    for (op, code) in [
        ("List", "500"),
        ("GET", "500"),
        ("Delete", "500"),
        ("CAS", "500"),
        ("WatchKey", "200"),
        ("WatchPrefix", "200"),
    ] {
        let line = format!(
            "kv_request_duration_seconds_count{{operation=\"{op}\",role=\"primary\",status_code=\"{code}\",type=\"consul\"}} 1\n"
        );
        assert!(text.contains(&line), "missing {line}\n{text}");
    }
}

/// prometheus.WrapRegistererWithPrefix / WrapRegistererWith semantics.
#[test]
fn registerer_wrapping_matches_client_golang() {
    let reg = Registry::new();
    // WrapRegistererWithPrefix("a_", WrapRegistererWithPrefix("b_", reg)) => b_a_<name>
    let r = Registerer::new(&reg)
        .wrap_with_prefix("b_")
        .wrap_with_prefix("a_");
    let c = prometheus::IntCounter::with_opts(
        prometheus::Opts::new("x_total", "help").const_label("z", "1"),
    )
    .unwrap();
    r.wrap_with([("k", "v")])
        .register(Box::new(c.clone()))
        .unwrap();
    c.inc();
    assert_eq!(
        gather_text(&reg),
        "# HELP b_a_x_total help\n# TYPE b_a_x_total counter\nb_a_x_total{k=\"v\",z=\"1\"} 1\n"
    );

    // Wrapping with a label the collector already has fails, like Go.
    let c2 = prometheus::IntCounter::with_opts(
        prometheus::Opts::new("y_total", "help").const_label("k", "1"),
    )
    .unwrap();
    assert!(Registerer::new(&reg)
        .wrap_with([("k", "v")])
        .register(Box::new(c2))
        .is_err());
    // Wrapping twice with the same label name fails, like Go.
    let c3 = prometheus::IntCounter::new("w_total", "help").unwrap();
    assert!(Registerer::new(&reg)
        .wrap_with([("k", "v")])
        .wrap_with([("k", "w")])
        .register(Box::new(c3))
        .is_err());
}
