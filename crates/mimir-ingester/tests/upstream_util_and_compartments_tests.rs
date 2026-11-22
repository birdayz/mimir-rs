//! 1:1 upstream-ported tests for `pkg/compartments/*` (14) and `pkg/usagestats/{reporter,seed}_test.go` (9).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::http::{header, Request, StatusCode};
use axum::routing::get;
use axum::Router as AxumRouter;
use mimir_common::compartments::{
    base_tenant_id, index_page_html, parse_tenant_id_with_metadata,
    parse_tenant_id_with_metadata_bytes, replace_read_compartment, replace_write_compartment,
    tenant_id_with_read_compartment, with_read_compartment_suffix, CompartmentMatcher, Router,
    TopicRouter, MAX_READ_COMPARTMENTS,
};
use mimir_common::services::{start_and_await_running, stop_and_await_terminated, ServiceContext};
use mimir_common::tenant::{valid_metadata_bytes, valid_tenant_id, Metadata};
use mimir_common::usagestats::{
    build_report, init_seed_file, new_cluster_seed, read_seed_file, wait_seed_file_stability,
    write_seed_file, BoxFuture, ClusterSeed, Report, Reporter, ReporterConfig, SeedBucket,
    SeedBucketError, SeedError, CLUSTER_SEED_FILE_NAME,
};
use mimir_config::{
    BucketConfig, CompartmentsConfig, CompartmentsReadConfig, CompartmentsWriteConfig,
    FilesystemConfig, StorageBackendConfig, UsageStatsConfig,
};
use mimir_storage::bucket::{BucketClient, MIMIR_INTERNALS_PREFIX};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::Registry;
use rand::Rng;
use tempfile::TempDir;
use tower::ServiceExt;

// ============================================================================
// pkg/compartments/config_test.go
// ============================================================================

// Upstream: pkg/compartments/config_test.go::TestConfig_Validate
#[test]
fn test_compartments_config_validate() {
    struct Case<'a> {
        name: &'a str,
        cfg: CompartmentsConfig,
        expected_err: Option<&'a str>,
    }

    let cases = [
        Case {
            name: "disabled passes regardless of other values",
            cfg: CompartmentsConfig {
                enabled: false,
                read: CompartmentsReadConfig { num_compartments: 0 },
                write: CompartmentsWriteConfig { num_compartments: 0 },
            },
            expected_err: None,
        },
        Case {
            name: "enabled with valid config passes",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig { num_compartments: 2 },
                write: CompartmentsWriteConfig { num_compartments: 3 },
            },
            expected_err: None,
        },
        Case {
            name: "enabled with zero read compartments is rejected",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig { num_compartments: 0 },
                write: CompartmentsWriteConfig { num_compartments: 3 },
            },
            expected_err: Some("compartments read.num-compartments must be greater than 0 when compartments are enabled"),
        },
        Case {
            name: "enabled with negative read compartments is rejected",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig { num_compartments: -1 },
                write: CompartmentsWriteConfig { num_compartments: 3 },
            },
            expected_err: Some("compartments read.num-compartments must be greater than 0 when compartments are enabled"),
        },
        Case {
            name: "enabled with the maximum number of read compartments passes",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig {
                    num_compartments: MAX_READ_COMPARTMENTS as i64,
                },
                write: CompartmentsWriteConfig { num_compartments: 3 },
            },
            expected_err: None,
        },
        Case {
            name: "enabled with too many read compartments is rejected",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig {
                    num_compartments: MAX_READ_COMPARTMENTS as i64 + 1,
                },
                write: CompartmentsWriteConfig { num_compartments: 3 },
            },
            expected_err: Some("compartments read.num-compartments must not be greater than 256"),
        },
        Case {
            name: "enabled with zero write compartments is rejected",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig { num_compartments: 2 },
                write: CompartmentsWriteConfig { num_compartments: 0 },
            },
            expected_err: Some("compartments write.num-compartments must be greater than 0 when compartments are enabled"),
        },
        Case {
            name: "enabled with negative write compartments is rejected",
            cfg: CompartmentsConfig {
                enabled: true,
                read: CompartmentsReadConfig { num_compartments: 2 },
                write: CompartmentsWriteConfig { num_compartments: -1 },
            },
            expected_err: Some("compartments write.num-compartments must be greater than 0 when compartments are enabled"),
        },
    ];

    for tc in cases {
        let res = tc.cfg.validate();
        match tc.expected_err {
            Some(expected) => {
                let err = res.expect_err(tc.name).to_string();
                assert!(err.contains(expected), "{}: got {err:?}", tc.name);
            }
            None => res.expect(tc.name),
        }
    }
}

// Upstream: pkg/compartments/config_test.go::TestReplaceReadCompartment
#[test]
fn test_replace_read_compartment() {
    assert_eq!(
        replace_read_compartment("ingest-rc-<read-compartment-id>", 0),
        "ingest-rc-0"
    );
    assert_eq!(
        replace_read_compartment("ingest-rc-<read-compartment-id>", 3),
        "ingest-rc-3"
    );
    // A template without the placeholder is returned unchanged.
    assert_eq!(replace_read_compartment("ingest", 2), "ingest");
}

// Upstream: pkg/compartments/config_test.go::TestReplaceWriteCompartment
#[test]
fn test_replace_write_compartment() {
    assert_eq!(
        replace_write_compartment("kafka-wc-<write-compartment-id>:9092", 0),
        "kafka-wc-0:9092"
    );
    assert_eq!(
        replace_write_compartment("kafka-wc-<write-compartment-id>:9092", 5),
        "kafka-wc-5:9092"
    );
    // A value without the placeholder is returned unchanged.
    assert_eq!(replace_write_compartment("kafka:9092", 1), "kafka:9092");
}

// Upstream: pkg/compartments/config_test.go::TestWithReadCompartmentSuffix
#[test]
fn test_with_read_compartment_suffix() {
    assert_eq!(
        with_read_compartment_suffix("ingester-partitions", 0),
        "ingester-partitions-rc-0"
    );
    assert_eq!(
        with_read_compartment_suffix("ingester-partitions", 3),
        "ingester-partitions-rc-3"
    );
    assert_eq!(with_read_compartment_suffix("blocks", 2), "blocks-rc-2");
}

// ============================================================================
// pkg/compartments/index_page_test.go
// ============================================================================

// Upstream: pkg/compartments/index_page_test.go::TestNewIndexPageHandler
#[tokio::test]
async fn test_new_index_page_handler() {
    const NUM_COMPARTMENTS: usize = 3;

    let html = index_page_html(
        "Partitions Ring Status",
        "partition-ring/compartment-<read-compartment-id>",
        NUM_COMPARTMENTS,
    );
    let app = AxumRouter::new().route(
        "/",
        get(move || {
            let body = html.clone();
            async move { ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body) }
        }),
    );

    let resp = app
        .oneshot(Request::builder().uri("/").body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/html; charset=utf-8"
    );
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(body.contains("Partitions Ring Status"));
    assert!(body.contains(r#"<a href="partition-ring/compartment-0">Compartment 0</a>"#));
    assert!(body.contains(r#"<a href="partition-ring/compartment-1">Compartment 1</a>"#));
    assert!(body.contains(r#"<a href="partition-ring/compartment-2">Compartment 2</a>"#));
    assert!(!body.contains("compartment-3"));
}

// ============================================================================
// pkg/compartments/router_test.go
// ============================================================================

fn new_test_router(num_compartments: usize) -> TopicRouter {
    TopicRouter::new(num_compartments, "mimir-rc-<read-compartment-id>")
}

fn to_compartment_matchers(matchers: &[LabelMatcher]) -> Vec<CompartmentMatcher<'_>> {
    matchers
        .iter()
        .map(|m| match m.match_type {
            MatchType::Equal => CompartmentMatcher::Equal {
                name: &m.name,
                value: &m.value,
            },
            MatchType::RegexMatch => CompartmentMatcher::Regexp {
                name: &m.name,
                set_matches: m.set_matches(),
            },
            _ => CompartmentMatcher::Other { name: &m.name },
        })
        .collect()
}

// Upstream: pkg/compartments/router_test.go::TestTopicRouter_TopicForCompartment
#[test]
fn test_topic_router_topic_for_compartment() {
    let r = new_test_router(3);
    assert_eq!(r.num_compartments(), 3);
    assert_eq!(r.topic_for_compartment(0), "mimir-rc-0");
    assert_eq!(r.topic_for_compartment(1), "mimir-rc-1");
    assert_eq!(r.topic_for_compartment(2), "mimir-rc-2");
}

// Upstream: pkg/compartments/router_test.go::TestTopicRouter_Topics
#[test]
fn test_topic_router_topics() {
    let r = new_test_router(3);
    assert_eq!(r.topics(), vec!["mimir-rc-0", "mimir-rc-1", "mimir-rc-2"]);

    // The returned slice is a copy: mutating it must not affect the router.
    let mut cloned = r.topics();
    cloned[0] = "mutated".to_string();
    assert_eq!(r.topic_for_compartment(0), "mimir-rc-0");
}

// Upstream: pkg/compartments/router_test.go::TestRouter_CompartmentForMetric
#[test]
fn test_router_compartment_for_metric() {
    let r = new_test_router(4);

    // deterministic
    let first = r.compartment_for_metric("user-1", "metric_name");
    for _ in 0..100 {
        assert_eq!(r.compartment_for_metric("user-1", "metric_name"), first);
    }

    // always within bounds
    for i in 0..1000 {
        let id = r.compartment_for_metric("user-1", &format!("metric_{i}"));
        assert!(id < r.num_compartments());
    }

    // consistent with TopicForMetric
    for i in 0..100 {
        let name = format!("metric_{i}");
        assert_eq!(
            r.topic_for_compartment(r.compartment_for_metric("user-1", &name)),
            r.topic_for_metric("user-1", &name)
        );
    }
}

// Upstream: pkg/compartments/router_test.go::TestRouter_CompartmentsForMatchers
#[test]
fn test_router_compartments_for_matchers() {
    const NUM_COMPARTMENTS: usize = 4;
    const USER_ID: &str = "user-1";
    let r = new_test_router(NUM_COMPARTMENTS);

    let eq = |name: &str, value: &str| LabelMatcher::new(MatchType::Equal, name, value).unwrap();
    let re = |name: &str, value: &str| LabelMatcher::new(MatchType::RegexMatch, name, value).unwrap();
    let neq =
        |name: &str, value: &str| LabelMatcher::new(MatchType::NotEqual, name, value).unwrap();

    // exact __name__ matcher pins to the routing compartment
    {
        let want = r.compartment_for_metric(USER_ID, "my_metric");
        let ms = [eq("__name__", "my_metric"), eq("job", "x")];
        let got = r.compartments_for_matchers(USER_ID, to_compartment_matchers(&ms));
        assert_eq!(got, vec![want]);
    }

    // enumerable regexp __name__ targets the union of the matched names' compartments
    {
        let names = ["metric_a", "metric_b", "metric_c", "metric_d", "metric_e"];
        let want: HashSet<usize> = names
            .iter()
            .map(|n| r.compartment_for_metric(USER_ID, n))
            .collect();
        let ms = [re("__name__", &names.join("|"))];
        let got = r.compartments_for_matchers(USER_ID, to_compartment_matchers(&ms));
        assert_eq!(got.len(), want.len());
        let got_set: HashSet<usize> = got.into_iter().collect();
        assert_eq!(got_set, want);
    }

    // exact matcher wins over a regexp set
    {
        let want = r.compartment_for_metric(USER_ID, "metric_a");
        let ms = [
            eq("__name__", "metric_a"),
            re("__name__", "metric_a|metric_b|metric_c"),
        ];
        let got = r.compartments_for_matchers(USER_ID, to_compartment_matchers(&ms));
        assert_eq!(got, vec![want]);
    }

    // conflicting exact names pin to the first one
    {
        let want = r.compartment_for_metric(USER_ID, "metric_a");
        let ms = [eq("__name__", "metric_a"), eq("__name__", "metric_b")];
        let got = r.compartments_for_matchers(USER_ID, to_compartment_matchers(&ms));
        assert_eq!(got, vec![want]);
    }

    // queries that cannot be restricted return all compartments
    {
        let all = vec![0, 1, 2, 3];
        let cases: Vec<(&str, Vec<LabelMatcher>)> = vec![
            ("no matchers", vec![]),
            ("no __name__ matcher", vec![eq("job", "x")]),
            ("unbounded regex", vec![re("__name__", "foo.*")]),
            ("negative __name__", vec![neq("__name__", "x")]),
        ];
        for (name, ms) in cases {
            let got = r.compartments_for_matchers(USER_ID, to_compartment_matchers(&ms));
            assert_eq!(got, all, "{name}");
        }
    }
}

// Upstream: pkg/compartments/router_test.go::TestRouter_TopicForMetric
#[test]
fn test_router_topic_for_metric() {
    // the same metric may map to different compartments for different tenants
    {
        let r = new_test_router(16);
        let mut differ = false;
        for i in 0..100 {
            let a = r.compartment_for_metric(&format!("user-a-{i}"), "the_metric");
            let b = r.compartment_for_metric(&format!("user-b-{i}"), "the_metric");
            if a != b {
                differ = true;
                break;
            }
        }
        assert!(differ, "expected the tenant to affect the compartment assignment");
    }

    // all compartments are used given enough distinct metrics
    {
        const NUM_COMPARTMENTS: usize = 8;
        let r = new_test_router(NUM_COMPARTMENTS);
        let mut seen = HashSet::new();
        for i in 0..10_000 {
            if seen.len() >= NUM_COMPARTMENTS {
                break;
            }
            seen.insert(r.compartment_for_metric("user-1", &format!("metric_{i}")));
        }
        assert_eq!(seen.len(), NUM_COMPARTMENTS);
    }
}

// Upstream: pkg/compartments/router_test.go::TestNewRouter
#[test]
fn test_new_router() {
    const NUM_COMPARTMENTS: usize = 4;
    let r = Router::new(NUM_COMPARTMENTS);
    let with_topics = TopicRouter::new(NUM_COMPARTMENTS, "mimir-rc-<read-compartment-id>");

    assert_eq!(r.num_compartments(), NUM_COMPARTMENTS);

    for i in 0..1000 {
        let name = format!("metric_{i}");
        assert_eq!(
            r.compartment_for_metric("user-1", &name),
            with_topics.compartment_for_metric("user-1", &name)
        );
    }

    let exact = [LabelMatcher::new(MatchType::Equal, "__name__", "the_metric").unwrap()];
    assert_eq!(
        r.compartments_for_matchers("user-1", to_compartment_matchers(&exact)),
        vec![r.compartment_for_metric("user-1", "the_metric")]
    );

    let none = [LabelMatcher::new(MatchType::Equal, "job", "test").unwrap()];
    let mut got = r.compartments_for_matchers("user-1", to_compartment_matchers(&none));
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 2, 3]);
}

// ============================================================================
// pkg/compartments/tenant_id_test.go
// ============================================================================

// Upstream: pkg/compartments/tenant_id_test.go::TestTenantIDWithReadCompartment
#[test]
fn test_tenant_id_with_read_compartment() {
    assert_eq!(tenant_id_with_read_compartment("123", 2), "123/rc/2");
    assert_eq!(tenant_id_with_read_compartment("user", 0), "user/rc/0");
}

// Upstream: pkg/compartments/tenant_id_test.go::TestBaseTenantID
#[test]
fn test_base_tenant_id() {
    let cases = [
        ("read compartment without metadata", "tenant/rc/1", "tenant"),
        ("read compartment with metadata", "tenant/rc/1:source=x", "tenant"),
        ("compartment zero", "user/rc/0", "user"),
        ("no read compartment", "tenant", "tenant"),
        ("metadata only", "tenant:source=x", "tenant"),
        ("malformed non-digit compartment", "tenant/rc/x", "tenant/rc/x"),
        ("malformed empty compartment", "tenant/rc/", "tenant/rc/"),
        ("malformed wrong infix", "tenant/foo/1", "tenant/foo/1"),
        ("malformed non-digit compartment with metadata", "tenant/rc/x:source=x", "tenant/rc/x"),
    ];

    for (name, input, expected) in cases {
        assert_eq!(base_tenant_id(input), expected, "{name}");
    }
}

// Upstream: pkg/compartments/tenant_id_test.go::TestParseTenantIDWithMetadata
#[test]
fn test_parse_tenant_id_with_metadata() {
    let ok_cases = [
        ("without read compartment", "tenant", "tenant", ""),
        ("without read compartment, with metadata", "tenant:source=x", "tenant", ":source=x"),
        ("with read compartment", "tenant/rc/1", "tenant/rc/1", ""),
        ("with read compartment and metadata", "tenant/rc/1:source=x", "tenant/rc/1", ":source=x"),
    ];
    for (name, input, exp_id, exp_meta) in ok_cases {
        let (tid, meta) = parse_tenant_id_with_metadata(input).expect(name);
        assert_eq!(tid, exp_id, "{name}");
        assert_eq!(meta, Metadata::new(exp_meta), "{name}");
    }

    let err_cases: Vec<(&str, &[u8], String)> = vec![
        (
            "malformed non-digit compartment",
            b"tenant/rc/x",
            valid_tenant_id("tenant/rc/x").unwrap_err().to_string(),
        ),
        (
            "malformed wrong infix",
            b"tenant/foo/1",
            valid_tenant_id("tenant/foo/1").unwrap_err().to_string(),
        ),
        (
            "invalid tenant ID with read compartment",
            b"bad tenant/rc/1",
            valid_tenant_id("bad tenant").unwrap_err().to_string(),
        ),
        (
            "invalid metadata with read compartment",
            b"tenant/rc/1:source=\xb5",
            valid_metadata_bytes(b":source=\xb5").unwrap_err().to_string(),
        ),
    ];
    for (name, input, expected_err) in err_cases {
        let err = parse_tenant_id_with_metadata_bytes(input)
            .expect_err(name)
            .to_string();
        assert_eq!(err, expected_err, "{name}");
    }
}

// ============================================================================
// pkg/usagestats/{reporter,seed}_test.go
// ============================================================================

struct StorageSeedBucket(BucketClient);

impl SeedBucket for StorageSeedBucket {
    fn get_object<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, SeedBucketError>> {
        Box::pin(async move {
            self.0
                .with_expected_not_found()
                .download(name)
                .await
                .map(|b| b.to_vec())
                .map_err(|e| {
                    if e.is_not_found() {
                        SeedBucketError::NotFound
                    } else {
                        SeedBucketError::Other(e.to_string())
                    }
                })
        })
    }

    fn put_object<'a>(&'a self, name: &'a str, data: Vec<u8>) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.0
                .upload(name, data)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

struct DelayedSeedBucket<T> {
    inner: T,
    min_delay: Duration,
    max_delay: Duration,
}

impl<T: SeedBucket> SeedBucket for DelayedSeedBucket<T> {
    fn get_object<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, SeedBucketError>> {
        Box::pin(async move {
            let ms = rand::thread_rng().gen_range(self.min_delay..=self.max_delay);
            tokio::time::sleep(ms).await;
            self.inner.get_object(name).await
        })
    }

    fn put_object<'a>(&'a self, name: &'a str, data: Vec<u8>) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let ms = rand::thread_rng().gen_range(self.min_delay..=self.max_delay);
            tokio::time::sleep(ms).await;
            self.inner.put_object(name, data).await
        })
    }
}

#[derive(Default)]
struct MockSeedBucket {
    get_res: Mutex<Option<Result<Vec<u8>, SeedBucketError>>>,
    put_res: Mutex<Option<Result<(), String>>>,
}

impl MockSeedBucket {
    fn mock_get(&self, data: &str, err: Option<SeedBucketError>) {
        let res = match err {
            Some(e) => Err(e),
            None => Ok(data.as_bytes().to_vec()),
        };
        *self.get_res.lock().unwrap() = Some(res);
    }

    fn mock_upload(&self, err: Option<&str>) {
        let res = match err {
            Some(e) => Err(e.to_string()),
            None => Ok(()),
        };
        *self.put_res.lock().unwrap() = Some(res);
    }
}

impl SeedBucket for MockSeedBucket {
    fn get_object<'a>(&'a self, _name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, SeedBucketError>> {
        Box::pin(async move {
            self.get_res
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(Err(SeedBucketError::NotFound))
        })
    }

    fn put_object<'a>(&'a self, _name: &'a str, _data: Vec<u8>) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move { self.put_res.lock().unwrap().clone().unwrap_or(Ok(())) })
    }
}

fn prepare_local_bucket_client() -> (TempDir, BucketClient) {
    let dir = TempDir::new().unwrap();
    let client = BucketClient::from_config(&BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "filesystem".to_string(),
            filesystem: FilesystemConfig {
                dir: dir.path().to_string_lossy().to_string(),
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    (dir, client)
}

// Upstream: pkg/usagestats/reporter_test.go::TestConfigValidation
#[test]
fn test_usagestats_config_validation() {
    let cases = [
        (
            "valid config",
            UsageStatsConfig {
                enabled: true,
                installation_mode: "custom".to_string(),
            },
            None,
        ),
        (
            "valid config with helm installation mode",
            UsageStatsConfig {
                enabled: true,
                installation_mode: "helm".to_string(),
            },
            None,
        ),
        (
            "valid config with jsonnet installation mode",
            UsageStatsConfig {
                enabled: true,
                installation_mode: "jsonnet".to_string(),
            },
            None,
        ),
        (
            "invalid config with unknown installation mode",
            UsageStatsConfig {
                enabled: true,
                installation_mode: "unknown".to_string(),
            },
            Some(r#"unsupported installation mode: "unknown""#),
        ),
    ];

    for (name, cfg, expected_err) in cases {
        let res = cfg.validate();
        match expected_err {
            Some(exp) => {
                let err = res.expect_err(name).to_string();
                assert!(err.contains(exp), "{name}: got {err:?}");
            }
            None => res.expect(name),
        }
    }
}

// Upstream: pkg/usagestats/reporter_test.go::TestSendReport
#[tokio::test]
async fn test_usagestats_send_report() {
    let server_invoked = Arc::new(AtomicBool::new(false));
    let si1 = Arc::clone(&server_invoked);
    let si2 = Arc::clone(&server_invoked);

    let app = AxumRouter::new()
        .route(
            "/success",
            axum::routing::post(move || {
                si1.store(true, Ordering::SeqCst);
                async { StatusCode::OK }
            }),
        )
        .route(
            "/failure",
            axum::routing::post(move || {
                si2.store(true, Ordering::SeqCst);
                async { StatusCode::SERVICE_UNAVAILABLE }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // server returns 2xx
    {
        server_invoked.store(false, Ordering::SeqCst);
        let reg = Registry::new();
        let (_dir, client) = prepare_local_bucket_client();
        let reporter = Reporter::new(
            ReporterConfig {
                server_url: format!("http://{addr}/success"),
                ..Default::default()
            },
            Arc::new(StorageSeedBucket(client.with_prefix(MIMIR_INTERNALS_PREFIX))),
            Some(&reg),
        );
        let report = build_report(&new_cluster_seed(), SystemTime::now(), Duration::from_secs(3600));
        reporter.send_report(&report).await.unwrap();
        assert!(server_invoked.load(Ordering::SeqCst));
        assert_eq!(reporter.requests_total.get(), 1.0);
        assert_eq!(reporter.requests_failed_total.get(), 0.0);
    }

    // server returns 5xx
    {
        server_invoked.store(false, Ordering::SeqCst);
        let reg = Registry::new();
        let (_dir, client) = prepare_local_bucket_client();
        let reporter = Reporter::new(
            ReporterConfig {
                server_url: format!("http://{addr}/failure"),
                ..Default::default()
            },
            Arc::new(StorageSeedBucket(client.with_prefix(MIMIR_INTERNALS_PREFIX))),
            Some(&reg),
        );
        let report = build_report(&new_cluster_seed(), SystemTime::now(), Duration::from_secs(3600));
        let err = reporter.send_report(&report).await.unwrap_err();
        assert!(err.contains("received status code: 503"), "got: {err}");
        assert!(server_invoked.load(Ordering::SeqCst));
        assert_eq!(reporter.requests_total.get(), 1.0);
        assert_eq!(reporter.requests_failed_total.get(), 1.0);
    }

    // server is not running
    {
        server_invoked.store(false, Ordering::SeqCst);
        // Bind then drop an ephemeral port so connection is reliably refused.
        let closed_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let reg = Registry::new();
        let (_dir, client) = prepare_local_bucket_client();
        let reporter = Reporter::new(
            ReporterConfig {
                server_url: format!("http://{closed_addr}"),
                ..Default::default()
            },
            Arc::new(StorageSeedBucket(client.with_prefix(MIMIR_INTERNALS_PREFIX))),
            Some(&reg),
        );
        let report = build_report(&new_cluster_seed(), SystemTime::now(), Duration::from_secs(3600));
        let err = reporter.send_report(&report).await.unwrap_err();
        assert!(err.contains("connection refused"), "got: {err}");
        assert!(!server_invoked.load(Ordering::SeqCst));
        assert_eq!(reporter.requests_total.get(), 1.0);
        assert_eq!(reporter.requests_failed_total.get(), 1.0);
    }

    server_task.abort();
}

// Upstream: pkg/usagestats/reporter_test.go::TestReporter_SendReportPeriodically
#[tokio::test]
async fn test_reporter_send_report_periodically() {
    #[derive(Clone)]
    struct ReceivedReport {
        report: Report,
        failed: bool,
    }

    for fail_one_every in [0i32, 2i32] {
        let (_dir, bucket_client) = prepare_local_bucket_client();
        let bucket = Arc::new(StorageSeedBucket(
            bucket_client.with_prefix(MIMIR_INTERNALS_PREFIX),
        ));
        let reports_received: Arc<Mutex<Vec<ReceivedReport>>> = Arc::new(Mutex::new(Vec::new()));
        let reports_accepted: Arc<Mutex<Vec<Report>>> = Arc::new(Mutex::new(Vec::new()));
        let requests_count = Arc::new(AtomicI32::new(0));

        let rr = Arc::clone(&reports_received);
        let ra = Arc::clone(&reports_accepted);
        let rc = Arc::clone(&requests_count);

        let app = AxumRouter::new().route(
            "/",
            axum::routing::post(move |body: Bytes| {
                let rr = Arc::clone(&rr);
                let ra = Arc::clone(&ra);
                let rc = Arc::clone(&rc);
                async move {
                    let count = rc.fetch_add(1, Ordering::SeqCst) + 1;
                    let report: Report = serde_json::from_slice(&body).unwrap();
                    let should_fail = fail_one_every > 0 && count % fail_one_every == 0;
                    rr.lock().unwrap().push(ReceivedReport {
                        report: report.clone(),
                        failed: should_fail,
                    });
                    if should_fail {
                        return StatusCode::SERVICE_UNAVAILABLE;
                    }
                    ra.lock().unwrap().push(report);
                    StatusCode::OK
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut seed = new_cluster_seed();
        seed.created_at = SystemTime::now() - Duration::from_secs(3600);
        write_seed_file(&*bucket, &seed).await.unwrap();

        let reg = Registry::new();
        let reporter = Reporter::new(
            ReporterConfig {
                server_url: format!("http://{addr}"),
                report_check_interval: Duration::from_millis(100),
                report_send_interval: Duration::from_secs(1),
                seed_file_min_stability: Duration::from_secs(5 * 60),
                cpu_usage_sample_interval: Duration::from_secs(60),
            },
            bucket,
            Some(&reg),
        );
        let svc = reporter.into_service();
        start_and_await_running(ServiceContext::background(), &*svc).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if reports_accepted.lock().unwrap().len() >= 4 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for 4 accepted reports (fail_one_every={fail_one_every})"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        stop_and_await_terminated(ServiceContext::background(), &*svc).await.unwrap();
        server_task.abort();

        let accepted = reports_accepted.lock().unwrap();
        for i in 1..accepted.len() {
            assert_eq!(
                accepted[i - 1].interval + Duration::from_secs(1),
                accepted[i].interval
            );
        }

        if fail_one_every > 0 {
            let received = reports_received.lock().unwrap();
            for (i, r) in received.iter().enumerate() {
                if r.failed {
                    assert!(received.len() > i + 1);
                    assert!(!received[i + 1].failed);
                    assert_eq!(r.report, received[i + 1].report);
                }
            }
        }
    }
}

// Upstream: pkg/usagestats/reporter_test.go::TestReporter_SendReportShouldSkipToNextReportOnLongFailure
#[tokio::test]
async fn test_reporter_send_report_should_skip_to_next_report_on_long_failure() {
    let (_dir, bucket_client) = prepare_local_bucket_client();
    let bucket = Arc::new(StorageSeedBucket(
        bucket_client.with_prefix(MIMIR_INTERNALS_PREFIX),
    ));
    let reports: Arc<Mutex<Vec<Report>>> = Arc::new(Mutex::new(Vec::new()));
    let requests_count = Arc::new(AtomicI32::new(0));

    let rep = Arc::clone(&reports);
    let rc = Arc::clone(&requests_count);
    let app = AxumRouter::new().route(
        "/",
        axum::routing::post(move |body: Bytes| {
            let rep = Arc::clone(&rep);
            let rc = Arc::clone(&rc);
            async move {
                let count = rc.fetch_add(1, Ordering::SeqCst) + 1;
                if count > 1 && count <= 11 {
                    return StatusCode::SERVICE_UNAVAILABLE;
                }
                let report: Report = serde_json::from_slice(&body).unwrap();
                rep.lock().unwrap().push(report);
                StatusCode::OK
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut seed = new_cluster_seed();
    seed.created_at = SystemTime::now() - Duration::from_secs(3600);
    write_seed_file(&*bucket, &seed).await.unwrap();

    let reg = Registry::new();
    let reporter = Reporter::new(
        ReporterConfig {
            server_url: format!("http://{addr}"),
            report_check_interval: Duration::from_millis(100),
            report_send_interval: Duration::from_secs(1),
            seed_file_min_stability: Duration::from_secs(5 * 60),
            cpu_usage_sample_interval: Duration::from_secs(60),
        },
        bucket,
        Some(&reg),
    );
    let svc = reporter.into_service();
    start_and_await_running(ServiceContext::background(), &*svc).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if reports.lock().unwrap().len() >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for 2 reports"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    stop_and_await_terminated(ServiceContext::background(), &*svc).await.unwrap();
    server_task.abort();

    let got = reports.lock().unwrap();
    assert!(
        got[1].interval >= got[0].interval + Duration::from_secs(2),
        "expected {:?} >= {:?}",
        got[1].interval,
        got[0].interval + Duration::from_secs(2)
    );
}

// Upstream: pkg/usagestats/seed_test.go::TestReadSeedFile
#[tokio::test]
async fn test_read_seed_file() {
    let expected_ts = mimir_common::usagestats::parse_rfc3339("2006-01-02T15:04:05.999999999Z").unwrap();

    // the seed file does not exist
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_get("", Some(SeedBucketError::NotFound));
        let err = read_seed_file(&bucket).await.unwrap_err();
        assert_eq!(err, SeedError::NotFound);
    }

    // an error occurred while reading the seed file
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_get("{}", Some(SeedBucketError::Other("read failure".to_string())));
        let err = read_seed_file(&bucket).await.unwrap_err().to_string();
        assert!(err.contains("read failure"), "got: {err}");
    }

    // the seed file is corrupted
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_get("xxx", None);
        let err = read_seed_file(&bucket).await.unwrap_err().to_string();
        assert!(err.contains("the cluster seed file is corrupted"), "got: {err}");
    }

    // the seed file is read successfully
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_get(
            r#"{"UID":"xxx","created_at":"2006-01-02T15:04:05.999999999Z"}"#,
            None,
        );
        let seed = read_seed_file(&bucket).await.unwrap();
        assert_eq!(
            seed,
            ClusterSeed {
                uid: "xxx".to_string(),
                created_at: expected_ts,
            }
        );
    }
}

// Upstream: pkg/usagestats/seed_test.go::TestWriteSeedFile
#[tokio::test]
async fn test_write_seed_file() {
    let seed = ClusterSeed {
        uid: "xxx".to_string(),
        created_at: SystemTime::now(),
    };

    // error writing
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_upload(Some("write failure"));
        let err = write_seed_file(&bucket, &seed).await.unwrap_err();
        assert!(err.contains("write failure"), "got: {err}");
    }

    // success
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_upload(None);
        write_seed_file(&bucket, &seed).await.unwrap();
    }
}

// Upstream: pkg/usagestats/seed_test.go::TestWaitSeedFileStability
#[tokio::test]
async fn test_wait_seed_file_stability() {
    const MIN_STABILITY: Duration = Duration::from_secs(2);
    let ctx = ServiceContext::background();

    // should immediately return if seed file does not exist
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_get("", Some(SeedBucketError::NotFound));
        let err = wait_seed_file_stability(&ctx, &bucket, MIN_STABILITY)
            .await
            .unwrap_err();
        assert_eq!(err, SeedError::NotFound);
    }

    // should immediately return if seed file is corrupted
    {
        let bucket = MockSeedBucket::default();
        bucket.mock_get("xxx", None);
        let err = wait_seed_file_stability(&ctx, &bucket, MIN_STABILITY)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("the cluster seed file is corrupted"));
    }

    // should immediately return if seed file was created more than min stability ago
    {
        let old_seed = ClusterSeed {
            uid: "old".to_string(),
            created_at: SystemTime::now() - MIN_STABILITY * 2,
        };
        let bucket = MockSeedBucket::default();
        bucket.mock_get(&serde_json::to_string(&old_seed).unwrap(), None);
        let actual = wait_seed_file_stability(&ctx, &bucket, MIN_STABILITY)
            .await
            .unwrap();
        assert_eq!(actual.uid, old_seed.uid);
        assert_eq!(
            actual.created_at.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            old_seed.created_at.duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
    }

    // should wait for min stability and return the seed file if created recently
    {
        let new_seed = ClusterSeed {
            uid: "new".to_string(),
            created_at: SystemTime::now(),
        };
        let bucket = MockSeedBucket::default();
        bucket.mock_get(&serde_json::to_string(&new_seed).unwrap(), None);
        let start = tokio::time::Instant::now();
        let actual = wait_seed_file_stability(&ctx, &bucket, MIN_STABILITY)
            .await
            .unwrap();
        assert_eq!(actual.uid, new_seed.uid);
        assert!(start.elapsed() >= MIN_STABILITY - Duration::from_millis(50));
    }
}

// Upstream: pkg/usagestats/seed_test.go::TestInitSeedFile
#[tokio::test]
async fn test_init_seed_file() {
    const MIN_STABILITY: Duration = Duration::from_secs(2);
    let ctx = ServiceContext::background();

    // exists and older than min stability -> immediate
    {
        let (_dir, client) = prepare_local_bucket_client();
        let bucket = StorageSeedBucket(client);
        let old_seed = ClusterSeed {
            uid: "old".to_string(),
            created_at: SystemTime::now() - MIN_STABILITY * 2,
        };
        write_seed_file(&bucket, &old_seed).await.unwrap();
        let actual = init_seed_file(&ctx, &bucket, MIN_STABILITY).await.unwrap();
        let expected = read_seed_file(&bucket).await.unwrap();
        assert_eq!(actual.uid, expected.uid);
    }

    // exists and newer than min stability -> waits
    {
        let (_dir, client) = prepare_local_bucket_client();
        let bucket = StorageSeedBucket(client);
        let new_seed = ClusterSeed {
            uid: "new".to_string(),
            created_at: SystemTime::now(),
        };
        write_seed_file(&bucket, &new_seed).await.unwrap();
        let start = tokio::time::Instant::now();
        let actual = init_seed_file(&ctx, &bucket, MIN_STABILITY).await.unwrap();
        assert!(start.elapsed() >= MIN_STABILITY - Duration::from_millis(50));
        let expected = read_seed_file(&bucket).await.unwrap();
        assert_eq!(actual.uid, expected.uid);
    }

    // doesn't exist -> creates and waits
    {
        let (_dir, client) = prepare_local_bucket_client();
        let bucket = StorageSeedBucket(client);
        let start = tokio::time::Instant::now();
        let actual = init_seed_file(&ctx, &bucket, MIN_STABILITY).await.unwrap();
        assert!(start.elapsed() >= MIN_STABILITY - Duration::from_millis(50));
        let expected = read_seed_file(&bucket).await.unwrap();
        assert_eq!(actual.uid, expected.uid);
    }

    // exists and corrupted -> recreates and waits
    {
        let (_dir, client) = prepare_local_bucket_client();
        client.upload(CLUSTER_SEED_FILE_NAME, b"xxx".to_vec()).await.unwrap();
        let bucket = StorageSeedBucket(client);
        let start = tokio::time::Instant::now();
        let actual = init_seed_file(&ctx, &bucket, MIN_STABILITY).await.unwrap();
        assert!(start.elapsed() >= MIN_STABILITY - Duration::from_millis(50));
        let expected = read_seed_file(&bucket).await.unwrap();
        assert_eq!(actual.uid, expected.uid);
    }
}

// Upstream: pkg/usagestats/seed_test.go::TestInitSeedFile_CreatingConcurrency
#[tokio::test]
async fn test_init_seed_file_creating_concurrency() {
    const NUM_REPLICAS: usize = 100;
    const MIN_STABILITY: Duration = Duration::from_secs(5);

    let (_dir, client) = prepare_local_bucket_client();
    let bucket = Arc::new(DelayedSeedBucket {
        inner: StorageSeedBucket(client),
        min_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    });

    let barrier = Arc::new(tokio::sync::Barrier::new(NUM_REPLICAS));
    let mut handles = Vec::with_capacity(NUM_REPLICAS);
    for _ in 0..NUM_REPLICAS {
        let b = Arc::clone(&bucket);
        let bar = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let ctx = ServiceContext::background();
            init_seed_file(&ctx, &*b, MIN_STABILITY).await.unwrap()
        }));
    }

    let mut seeds = Vec::with_capacity(NUM_REPLICAS);
    for h in handles {
        seeds.push(h.await.unwrap());
    }

    assert_eq!(seeds.len(), NUM_REPLICAS);
    for seed in &seeds[1..] {
        assert_eq!(seed, &seeds[0]);
    }
}
