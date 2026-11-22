//! 1:1 upstream-ported tests for `dskit/{backoff,multierror,timeutil,user,tenant,netutil,instrument}`.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::net::IpAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_common::backoff::{Backoff, BackoffConfig, BackoffError};
use mimir_common::instrument::{
    collected_request, Collector, HistogramCollector, HISTOGRAM_COLLECTOR_BUCKETS,
};
use mimir_common::multierror::{BoxError, MultiError};
use mimir_common::netutil::{
    filter_best_ip, get_first_address_of_with, go_addr_string, parse_cidr_ip,
    private_network_interfaces_from, Interface,
};
use mimir_common::tenant::{
    extract_org_id_from_grpc_metadata, inject_org_id_into_grpc_metadata, parse_single_tenant_id,
    parse_tenant_ids, parse_with_metadata, valid_tenant_id, Metadata, TenantError,
    MAX_METADATA_LENGTH, MAX_TENANT_ID_LENGTH,
};
use mimir_common::timeutil::new_variable_ticker;
use parking_lot::Mutex;
use prometheus::{HistogramOpts, HistogramVec, Registry};
use tonic::metadata::MetadataMap;

// ============================================================================
// github.com/grafana/dskit/backoff/backoff_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/backoff/backoff_test.go::TestBackoff_NextDelay
#[test]
fn test_backoff_next_delay() {
    let cases = [
        (
            "exponential backoff with jitter honoring min and max",
            Duration::from_millis(100),
            Duration::from_secs(10),
            vec![
                (Duration::from_millis(100), Duration::from_millis(200)),
                (Duration::from_millis(200), Duration::from_millis(400)),
                (Duration::from_millis(400), Duration::from_millis(800)),
                (Duration::from_millis(800), Duration::from_millis(1600)),
                (Duration::from_millis(1600), Duration::from_millis(3200)),
                (Duration::from_millis(3200), Duration::from_millis(6400)),
                (Duration::from_millis(6400), Duration::from_millis(10000)),
                (Duration::from_millis(6400), Duration::from_millis(10000)),
            ],
        ),
        (
            "exponential backoff with max equal to the end of a range",
            Duration::from_millis(100),
            Duration::from_millis(800),
            vec![
                (Duration::from_millis(100), Duration::from_millis(200)),
                (Duration::from_millis(200), Duration::from_millis(400)),
                (Duration::from_millis(400), Duration::from_millis(800)),
                (Duration::from_millis(400), Duration::from_millis(800)),
            ],
        ),
        (
            "exponential backoff with max equal to the end of a range + 1",
            Duration::from_millis(100),
            Duration::from_millis(801),
            vec![
                (Duration::from_millis(100), Duration::from_millis(200)),
                (Duration::from_millis(200), Duration::from_millis(400)),
                (Duration::from_millis(400), Duration::from_millis(800)),
                (Duration::from_millis(800), Duration::from_millis(801)),
                (Duration::from_millis(800), Duration::from_millis(801)),
            ],
        ),
        (
            "exponential backoff with max equal to the end of a range - 1",
            Duration::from_millis(100),
            Duration::from_millis(799),
            vec![
                (Duration::from_millis(100), Duration::from_millis(200)),
                (Duration::from_millis(200), Duration::from_millis(400)),
                (Duration::from_millis(400), Duration::from_millis(799)),
                (Duration::from_millis(400), Duration::from_millis(799)),
            ],
        ),
        (
            "min backoff is equal to max",
            Duration::from_millis(100),
            Duration::from_millis(100),
            vec![
                (Duration::from_millis(100), Duration::from_millis(100)),
                (Duration::from_millis(100), Duration::from_millis(100)),
                (Duration::from_millis(100), Duration::from_millis(100)),
            ],
        ),
        (
            "min backoff is greater than max",
            Duration::from_millis(200),
            Duration::from_millis(100),
            vec![
                (Duration::from_millis(200), Duration::from_millis(200)),
                (Duration::from_millis(200), Duration::from_millis(200)),
                (Duration::from_millis(200), Duration::from_millis(200)),
            ],
        ),
    ];

    for (name, min_backoff, max_backoff, expected_ranges) in cases {
        let mut b = Backoff::new(BackoffConfig {
            min_backoff,
            max_backoff,
            max_retries: expected_ranges.len(),
        });
        for (lo, hi) in expected_ranges {
            let delay = b.next_delay();
            assert!(
                delay >= lo && delay <= hi,
                "{name}: {delay:?} not within [{lo:?}, {hi:?}]"
            );
        }
    }
}

// Upstream: github.com/grafana/dskit/backoff/backoff_test.go::TestBackoff_ErrAndErrCause
#[test]
fn test_backoff_err_and_err_cause() {
    // context deadline exceeded without cause
    {
        let b = Backoff::with_deadline(BackoffConfig::default(), Instant::now(), None, None);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(b.err(), Some(BackoffError::DeadlineExceeded));
        assert_eq!(b.err_cause(), Some(BackoffError::DeadlineExceeded));
    }

    // context deadline exceeded with cause
    {
        let cause = Arc::new(Mutex::new(Some("my cause".to_string())));
        let b = Backoff::with_deadline(BackoffConfig::default(), Instant::now(), None, Some(cause));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(b.err(), Some(BackoffError::DeadlineExceeded));
        assert_eq!(b.err_cause(), Some(BackoffError::Cause("my cause".to_string())));
    }

    // context is canceled without cause
    {
        let flag = Arc::new(AtomicBool::new(true));
        let b = Backoff::with_cancel_flag(BackoffConfig::default(), flag);
        assert_eq!(b.err(), Some(BackoffError::Canceled));
        assert_eq!(b.err_cause(), Some(BackoffError::Canceled));
    }

    // context is canceled with cause
    {
        let flag = Arc::new(AtomicBool::new(true));
        let cause = Arc::new(Mutex::new(Some("my cause".to_string())));
        let b = Backoff::with_cancel_cause(BackoffConfig::default(), flag, cause);
        assert_eq!(b.err(), Some(BackoffError::Canceled));
        assert_eq!(b.err_cause(), Some(BackoffError::Cause("my cause".to_string())));
    }
}

// ============================================================================
// github.com/grafana/dskit/multierror/multierror_test.go
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct SentinelCanceled;

impl fmt::Display for SentinelCanceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context canceled")
    }
}

impl StdError for SentinelCanceled {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SentinelDeadlineExceeded;

impl fmt::Display for SentinelDeadlineExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context deadline exceeded")
    }
}

impl StdError for SentinelDeadlineExceeded {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CustomError(&'static str);

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for CustomError {}

#[derive(Debug)]
struct WrappedBoxError {
    msg: &'static str,
    source: BoxError,
}

impl fmt::Display for WrappedBoxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.msg, self.source)
    }
}

impl StdError for WrappedBoxError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestError {
    cause: &'static str,
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "testError[cause: {}]", self.cause)
    }
}

impl StdError for TestError {}

// Upstream: github.com/grafana/dskit/multierror/multierror_test.go::TestMultiError_Is
#[test]
fn test_multi_error_is() {
    let canceled: BoxError = Arc::new(SentinelCanceled);
    let some_err: BoxError = Arc::new(CustomError("some error"));
    let wrapped_canceled: BoxError = Arc::new(WrappedBoxError {
        msg: "some message",
        source: canceled.clone(),
    });
    let nested_multi = Arc::new(
        MultiError::new([
            Some(Arc::new(CustomError("my error")) as BoxError),
            Some(Arc::new(WrappedBoxError {
                msg: "wrapped",
                source: canceled.clone(),
            }) as BoxError),
        ])
        .err()
        .unwrap(),
    );

    let cases: Vec<(&str, Vec<Option<BoxError>>, bool, &str)> = vec![
        (
            "adding a context cancellation doesn't lose the information",
            vec![Some(canceled.clone())],
            true,
            "canceled",
        ),
        (
            "adding multiple context cancellations doesn't lose the information",
            vec![Some(canceled.clone()), Some(canceled.clone())],
            true,
            "canceled",
        ),
        (
            "adding wrapped context cancellations doesn't lose the information",
            vec![Some(some_err.clone()), Some(wrapped_canceled.clone())],
            true,
            "canceled",
        ),
        (
            "adding a nil error doesn't lose the information",
            vec![Some(some_err.clone()), Some(wrapped_canceled.clone()), None],
            true,
            "canceled",
        ),
        (
            "errors with no context cancellation error are not a context canceled error",
            vec![
                Some(Arc::new(CustomError("first error"))),
                Some(Arc::new(CustomError("second error"))),
            ],
            false,
            "canceled",
        ),
        (
            "no errors are not a context canceled error",
            vec![],
            false,
            "canceled",
        ),
        ("no errors are a nil error", vec![], true, "nil"),
        (
            "nested multi-error contains assert.AnError",
            vec![
                Some(Arc::new(CustomError("assert.AnError"))),
                Some(nested_multi.clone()),
            ],
            true,
            "anerror",
        ),
        (
            "nested multi-error contains custom error",
            vec![
                Some(Arc::new(CustomError("assert.AnError"))),
                Some(nested_multi.clone()),
            ],
            true,
            "myerror",
        ),
        (
            "nested multi-error contains wrapped context.Canceled",
            vec![
                Some(Arc::new(CustomError("assert.AnError"))),
                Some(nested_multi.clone()),
            ],
            true,
            "canceled",
        ),
        (
            "nested multi-error does not contain context.DeadlineExceeded",
            vec![
                Some(Arc::new(CustomError("assert.AnError"))),
                Some(nested_multi),
            ],
            false,
            "deadline",
        ),
    ];

    for (name, source_errors, expected, target_kind) in cases {
        let m_err = MultiError::new(source_errors);
        let err = m_err.err();
        let matched = match (err, target_kind) {
            (None, "nil") => true,
            (None, _) => false,
            (Some(_), "nil") => false,
            (Some(e), "canceled") => e.is_type::<SentinelCanceled>(),
            (Some(e), "deadline") => e.is_type::<SentinelDeadlineExceeded>(),
            (Some(e), "anerror") => e.is_matching(&|inner| {
                inner
                    .downcast_ref::<CustomError>()
                    .is_some_and(|c| c.0 == "assert.AnError")
            }),
            (Some(e), "myerror") => e.is_matching(&|inner| {
                inner
                    .downcast_ref::<CustomError>()
                    .is_some_and(|c| c.0 == "my error")
            }),
            _ => unreachable!(),
        };
        assert_eq!(matched, expected, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/multierror/multierror_test.go::TestMultiError_As
#[test]
fn test_multi_error_as() {
    let te1 = TestError {
        cause: "error cause 1",
    };
    let te2 = TestError {
        cause: "error cause 2",
    };

    let cases: Vec<(&str, Vec<BoxError>, Option<TestError>)> = vec![
        (
            "MultiError containing only a testError can be cast to that testError",
            vec![Arc::new(te1.clone())],
            Some(te1.clone()),
        ),
        (
            "MultiError containing multiple testErrors can be cast to the first testError added",
            vec![Arc::new(te1.clone()), Arc::new(te2.clone())],
            Some(te1.clone()),
        ),
        (
            "MultiError containing multiple errors can be cast to the first testError added",
            vec![
                Arc::new(SentinelCanceled),
                Arc::new(te1.clone()),
                Arc::new(SentinelDeadlineExceeded),
                Arc::new(te2),
            ],
            Some(te1),
        ),
        (
            "MultiError non containing a testError cannot be cast to a testError",
            vec![
                Arc::new(SentinelCanceled),
                Arc::new(SentinelDeadlineExceeded),
            ],
            None,
        ),
    ];

    for (name, errs, expected) in cases {
        let m_err = MultiError::new(errs.into_iter().map(Some)).err().unwrap();
        let got = m_err.downcast_first::<TestError>().cloned();
        assert_eq!(got, expected, "{name}");
    }
}

// ============================================================================
// github.com/grafana/dskit/timeutil/variable_ticker_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/timeutil/variable_ticker_test.go::TestVariableTicker
#[tokio::test]
async fn test_variable_ticker() {
    // Run the 3 subcases concurrently just like t.Parallel() in Go so wall-clock is ~2.5s.
    let f1 = async {
        let start = Instant::now();
        let (stop, mut rx) = new_variable_ticker(&[
            Duration::from_millis(100),
            Duration::from_millis(200),
        ]);
        let mut ticks = Vec::new();
        while ticks.len() < 3 {
            ticks.push(rx.recv().await.expect("tick"));
        }
        stop.stop();
        let tol = 0.08;
        assert!((ticks[0].duration_since(start).as_secs_f64() - 0.1).abs() < tol);
        assert!((ticks[1].duration_since(start).as_secs_f64() - 0.3).abs() < tol);
        assert!((ticks[2].duration_since(start).as_secs_f64() - 0.5).abs() < tol);
    };

    let f2 = async {
        for durations in [
            vec![Duration::from_millis(150)],
            vec![Duration::from_millis(150), Duration::from_millis(300)],
        ] {
            let (stop, mut rx) = new_variable_ticker(&durations);
            stop.stop();
            let res = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await;
            assert!(
                res.is_err(),
                "should not close the channel and not send any further tick"
            );
        }
    };

    let f3 = async {
        for durations in [
            vec![Duration::from_millis(150)],
            vec![Duration::from_millis(150), Duration::from_millis(300)],
        ] {
            let (stop, mut rx) = new_variable_ticker(&durations);
            stop.stop();
            stop.stop();
            let res = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await;
            assert!(
                res.is_err(),
                "should not close the channel and not send any further tick"
            );
        }
    };

    tokio::join!(f1, f2, f3);
}

// ============================================================================
// github.com/grafana/dskit/user/grpc_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/user/grpc_test.go::TestExtractFromGRPCRequest
#[test]
fn test_extract_from_grpc_request() {
    let empty_md = MetadataMap::new();
    assert_eq!(
        extract_org_id_from_grpc_metadata(&empty_md).unwrap_err(),
        TenantError::NoOrgID
    );

    let mut md = MetadataMap::new();
    md.insert("x-scope-orgid", "user-1".parse().unwrap());
    let actual_org_id = extract_org_id_from_grpc_metadata(&md).unwrap();
    assert_eq!(actual_org_id, "user-1");

    let mut out_md = MetadataMap::new();
    inject_org_id_into_grpc_metadata(&mut out_md, &actual_org_id).unwrap();
    assert_eq!(
        extract_org_id_from_grpc_metadata(&out_md).unwrap(),
        "user-1"
    );
}

// ============================================================================
// github.com/grafana/dskit/tenant/metadata_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/tenant/metadata_test.go::TestMetadata_WithTenant
#[test]
fn test_metadata_with_tenant() {
    let cases = [
        (
            "single pair",
            Metadata::default().with("key", "value"),
            "my-tenant",
            "my-tenant:key=value",
        ),
        (
            "multiple pairs sorted by key",
            Metadata::default().with("product", "k6").with("env", "prod"),
            "123456",
            "123456:env=prod:product=k6",
        ),
        (
            "empty metadata",
            Metadata::default(),
            "tenant-a",
            "tenant-a",
        ),
    ];
    for (name, md, tenant, expected) in cases {
        assert_eq!(md.with_tenant(tenant), expected, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/tenant/metadata_test.go::TestMetadata_IsEmpty
#[test]
fn test_metadata_is_empty() {
    assert!(Metadata::new("").is_empty());
    assert!(!Metadata::new(":a=b").is_empty());
    assert!(!Metadata::new(":a=b:b=c").is_empty());
}

// Upstream: github.com/grafana/dskit/tenant/metadata_test.go::TestMetadata_Has
#[test]
fn test_metadata_has() {
    let mut md = Metadata::default();
    md.set("key", "value");
    assert!(md.has("key"));
    assert!(!md.has("missing"));
}

// Upstream: github.com/grafana/dskit/tenant/metadata_test.go::TestMetadata_Get
#[test]
fn test_metadata_get() {
    let mut md = Metadata::default();
    md.set("key", "value");
    assert_eq!(md.get("key"), Some("value"));
    assert_eq!(md.get("missing"), None);
}

// Upstream: github.com/grafana/dskit/tenant/metadata_test.go::TestMetadata_DivideIter
#[test]
fn test_metadata_divide_iter() {
    let cases = [
        ("empty input", "", vec![]),
        (
            "multiple pairs",
            ":env=prod:product=k6",
            vec![("env", "prod"), ("product", "k6")],
        ),
    ];
    for (name, input, expected) in cases {
        let md = Metadata::new(input);
        let got: Vec<(&str, &str)> = md.iter().collect();
        assert_eq!(got, expected, "{name}");

        let expected_subs: Vec<Metadata> = expected
            .iter()
            .map(|(k, v)| Metadata::default().with(k, v))
            .collect();
        let got_subs: Vec<Metadata> = md.divide().collect();
        assert_eq!(got_subs, expected_subs, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/tenant/metadata_test.go::TestMetadata_Set
#[test]
fn test_metadata_set() {
    let mut md = Metadata::default();
    md.set("product", "k6");
    assert_eq!(md.encode(), ":product=k6");

    md.set("env", "prod");
    assert_eq!(md.encode(), ":env=prod:product=k6");

    md.set("env", "dev");
    assert_eq!(md.encode(), ":env=dev:product=k6");

    md.set("foo", "bar");
    assert_eq!(md.encode(), ":env=dev:foo=bar:product=k6");
}

// ============================================================================
// github.com/grafana/dskit/tenant/resolver_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/tenant/resolver_test.go::TestTenantIDs
#[test]
fn test_tenant_ids() {
    // no-header tested via Missing / NoOrgID
    let too_long_str = "123".repeat(MAX_TENANT_ID_LENGTH);
    let meta_too_long = format!("tenant-a:{}", "x".repeat(MAX_METADATA_LENGTH + 1));

    struct Case<'a> {
        name: &'a str,
        header: Option<&'a str>,
        err_tenant_id: Option<TenantError>,
        err_tenant_ids: Option<TenantError>,
        err_metadata: Option<TenantError>,
        err_metadata_contain: Option<&'a str>,
        tenant_id: &'a str,
        tenant_ids: Vec<&'a str>,
        metadata: Metadata,
    }

    let cases = [
        Case {
            name: "empty",
            header: Some(""),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![""],
            metadata: Metadata::default(),
        },
        Case {
            name: "single-tenant",
            header: Some("tenant-a"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "parent-dir",
            header: Some(".."),
            err_tenant_id: Some(TenantError::UnsafeTenantId),
            err_tenant_ids: Some(TenantError::UnsafeTenantId),
            err_metadata: Some(TenantError::UnsafeTenantId),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![],
            metadata: Metadata::default(),
        },
        Case {
            name: "current-dir",
            header: Some("."),
            err_tenant_id: Some(TenantError::UnsafeTenantId),
            err_tenant_ids: Some(TenantError::UnsafeTenantId),
            err_metadata: Some(TenantError::UnsafeTenantId),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant",
            header: Some("tenant-a|tenant-b"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["tenant-a", "tenant-b"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-wrong-order",
            header: Some("tenant-b|tenant-a"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["tenant-a", "tenant-b"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-duplicate-order",
            header: Some("tenant-b|tenant-b|tenant-a"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["tenant-a", "tenant-b"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-same-tenant-duplicated",
            header: Some("tenant-a|tenant-a"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-same-tenant-with-metadata-duplicated",
            header: Some("tenant-a:key=value|tenant-a:key=value"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default().with("key", "value"),
        },
        Case {
            name: "multi-tenant-with-relative-path",
            header: Some("tenant-a|tenant-b|.."),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: Some(TenantError::UnsafeTenantId),
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![],
            metadata: Metadata::default(),
        },
        Case {
            name: "containing-forward-slash",
            header: Some("forward/slash"),
            err_tenant_id: Some(TenantError::UnsupportedCharacter {
                tenant_id: "forward/slash".to_string(),
                ch: '/',
            }),
            err_tenant_ids: Some(TenantError::UnsupportedCharacter {
                tenant_id: "forward/slash".to_string(),
                ch: '/',
            }),
            err_metadata: Some(TenantError::UnsupportedCharacter {
                tenant_id: "forward/slash".to_string(),
                ch: '/',
            }),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![],
            metadata: Metadata::default(),
        },
        Case {
            name: "containing-backward-slash",
            header: Some(r"backward\slash"),
            err_tenant_id: Some(TenantError::UnsupportedCharacter {
                tenant_id: r"backward\slash".to_string(),
                ch: '\\',
            }),
            err_tenant_ids: Some(TenantError::UnsupportedCharacter {
                tenant_id: r"backward\slash".to_string(),
                ch: '\\',
            }),
            err_metadata: Some(TenantError::UnsupportedCharacter {
                tenant_id: r"backward\slash".to_string(),
                ch: '\\',
            }),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![],
            metadata: Metadata::default(),
        },
        Case {
            name: "too-long",
            header: Some(&too_long_str),
            err_tenant_id: Some(TenantError::TooLong),
            err_tenant_ids: Some(TenantError::TooLong),
            err_metadata: Some(TenantError::TooLong),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec![],
            metadata: Metadata::default(),
        },
        Case {
            name: "tenant-with-metadata",
            header: Some("123456:key=value"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "123456",
            tenant_ids: vec!["123456"],
            metadata: Metadata::default().with("key", "value"),
        },
        Case {
            name: "tenant-with-metadata-complex",
            header: Some("my-tenant-id:product=k6"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "my-tenant-id",
            tenant_ids: vec!["my-tenant-id"],
            metadata: Metadata::default().with("product", "k6"),
        },
        Case {
            name: "tenant-with-empty-metadata",
            header: Some("tenant-a:"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: Some("no key-value separator '='"),
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "invalid-metadata-with-slash",
            header: Some("tenant-a:key/value"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: Some(TenantError::MetadataUnsupportedCharacter {
                metadata: ":key/value".to_string(),
                ch: '/',
            }),
            err_metadata_contain: None,
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "invalid-metadata-too-long",
            header: Some(&meta_too_long),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: Some("metadata too long"),
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "invalid-metadata-no-equals",
            header: Some("tenant-a:keyvalue"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: Some("malformed tenant metadata"),
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "invalid-metadata-not-sorted",
            header: Some("tenant-a:z=foo:a=bar"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: Some("keys must be unique and sorted"),
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "invalid-metadata-not-unique",
            header: Some("tenant-a:a=foo:z=bar:a=bar"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: Some("keys must be unique and sorted"),
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-with-metadata",
            header: Some("tenant-a|tenant-b:key=value"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["tenant-a", "tenant-b"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-with-same-metadata",
            header: Some("123123:key=k6|1234515:key=k6"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["123123", "1234515"],
            metadata: Metadata::default(),
        },
        Case {
            name: "multi-tenant-with-different-metadata",
            header: Some("123123:key=k6|1234515:key=k7"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["123123", "1234515"],
            metadata: Metadata::default(),
        },
        Case {
            name: "tenant-with-multiple-metadata-pairs",
            header: Some("tenant-a:env=prod:product=k6"),
            err_tenant_id: None,
            err_tenant_ids: None,
            err_metadata: None,
            err_metadata_contain: None,
            tenant_id: "tenant-a",
            tenant_ids: vec!["tenant-a"],
            metadata: Metadata::default().with("env", "prod").with("product", "k6"),
        },
        Case {
            name: "multi-tenant-each-with-same-metadata",
            header: Some("tenant-a:key=k6|tenant-b:key=k6|tenant-c:key=k6"),
            err_tenant_id: Some(TenantError::TooManyOrgIDs),
            err_tenant_ids: None,
            err_metadata: Some(TenantError::TooManyOrgIDs),
            err_metadata_contain: None,
            tenant_id: "",
            tenant_ids: vec!["tenant-a", "tenant-b", "tenant-c"],
            metadata: Metadata::default(),
        },
    ];

    for tc in cases {
        let header = tc.header.unwrap();
        match (parse_single_tenant_id(header), &tc.err_tenant_id) {
            (Ok(id), None) => assert_eq!(id, tc.tenant_id, "{}", tc.name),
            (Err(e), Some(exp)) => assert_eq!(&e, exp, "{}", tc.name),
            (res, exp) => panic!("{}: tenant_id mismatch: {res:?} vs {exp:?}", tc.name),
        }
        match (parse_tenant_ids(header), &tc.err_tenant_ids) {
            (Ok(ids), None) => assert_eq!(ids, tc.tenant_ids, "{}", tc.name),
            (Err(e), Some(exp)) => assert_eq!(&e, exp, "{}", tc.name),
            (res, exp) => panic!("{}: tenant_ids mismatch: {res:?} vs {exp:?}", tc.name),
        }
        match (parse_with_metadata(header), &tc.err_metadata, tc.err_metadata_contain) {
            (Ok((id, md)), None, None) => {
                assert_eq!(id, tc.tenant_id, "{}", tc.name);
                assert_eq!(md, tc.metadata, "{}", tc.name);
            }
            (Err(e), Some(exp), _) => assert_eq!(&e, exp, "{}", tc.name),
            (Err(e), None, Some(substr)) => {
                assert!(e.to_string().contains(substr), "{}: {e}", tc.name);
            }
            (res, exp, sub) => panic!("{}: metadata mismatch: {res:?} vs {exp:?}/{sub:?}", tc.name),
        }
    }
}

// ============================================================================
// github.com/grafana/dskit/tenant/tenant_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/tenant/tenant_test.go::TestValidTenantIDs
#[test]
fn test_valid_tenant_ids() {
    let max_ok = "a".repeat(150);
    let too_long = "a".repeat(151);
    let cases = [
        ("tenant-a", None),
        (
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz_0987654321!.*'()",
            None,
        ),
        (
            "invalid|",
            Some("tenant ID 'invalid|' contains unsupported character '|'"),
        ),
        (max_ok.as_str(), None),
        (
            too_long.as_str(),
            Some("tenant ID is too long: max 150 characters"),
        ),
        (".", Some("tenant ID is '.' or '..'")),
        ("..", Some("tenant ID is '.' or '..'")),
    ];
    for (name, expected_err) in cases {
        match (valid_tenant_id(name), expected_err) {
            (Ok(()), None) => {}
            (Err(e), Some(exp)) => assert_eq!(e.to_string(), exp, "{name}"),
            (res, exp) => panic!("{name}: {res:?} vs {exp:?}"),
        }
    }
}

// ============================================================================
// github.com/grafana/dskit/netutil/netutil_test.go
// ============================================================================

fn svec(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn gen_test_interfaces(names: &[&str]) -> Vec<Interface> {
    names
        .iter()
        .enumerate()
        .map(|(i, n)| Interface {
            index: i as u32 + 1,
            name: n.to_string(),
            flags: 0,
        })
        .collect()
}

// Upstream: github.com/grafana/dskit/netutil/netutil_test.go::TestPrivateInterface
#[test]
fn test_private_interface() {
    let test_ints_addrs: HashMap<&str, Vec<&str>> = HashMap::from([
        ("privNetA", vec!["10.6.19.34/8"]),
        ("privNetB", vec!["172.16.0.7/12"]),
        ("privNetC", vec!["192.168.3.29/24"]),
        ("pubNet", vec!["34.120.177.193/24"]),
        ("multiPriv", vec!["10.6.19.34/8", "172.16.0.7/12"]),
        ("multiMix", vec!["1.1.1.1/24", "192.168.0.42/24"]),
        ("multiPub", vec!["1.1.1.1/24", "34.120.177.193/24"]),
    ]);
    let default_output = svec(&["eth0", "en0"]);
    let scenarios = [
        ("empty interface list", vec![], default_output.clone()),
        ("single private interface", vec!["privNetA"], svec(&["privNetA"])),
        ("single public interface", vec!["pubNet"], default_output.clone()),
        (
            "single interface multi address private",
            vec!["multiPriv"],
            svec(&["multiPriv"]),
        ),
        (
            "single interface multi address mix",
            vec!["multiMix"],
            svec(&["multiMix"]),
        ),
        (
            "single interface multi address public",
            vec!["multiPub"],
            default_output.clone(),
        ),
        (
            "all private interfaces",
            vec!["privNetA", "privNetB", "privNetC"],
            svec(&["privNetA", "privNetB", "privNetC"]),
        ),
        (
            "mix of public and private interfaces",
            vec![
                "pubNet",
                "privNetA",
                "privNetB",
                "privNetC",
                "multiPriv",
                "multiMix",
                "multiPub",
            ],
            svec(&["privNetA", "privNetB", "privNetC", "multiPriv", "multiMix"]),
        ),
    ];
    for (desc, ints, expected) in scenarios {
        let got = private_network_interfaces_from(
            &gen_test_interfaces(&ints),
            &default_output,
            |i| {
                Ok(test_ints_addrs
                    .get(i.name.as_str())
                    .map(|v| v.iter().map(|x| x.to_string()).collect())
                    .unwrap_or_default())
            },
        );
        assert_eq!(got, expected, "{desc}");
    }
}

// Upstream: github.com/grafana/dskit/netutil/netutil_test.go::TestPrivateInterfaceError
#[test]
fn test_private_interface_error() {
    let interfaces = gen_test_interfaces(&["eth9"]);
    let got = private_network_interfaces_from(&interfaces, &[], |_| {
        Ok(vec!["not_a_parseable_ip_string".to_string()])
    });
    assert_eq!(got, Vec::<String>::new());
}

fn mock_interface_addresses(name: &str) -> Result<Vec<IpAddr>, String> {
    let to_addr = |a: &str| parse_cidr_ip(a).unwrap();
    let interfaces: HashMap<&str, Vec<IpAddr>> = HashMap::from([
        (
            "wlan0",
            vec![
                to_addr("172.16.16.51/24"),
                to_addr("fc16::c9a7:1d89:2dd8:f59a/64"),
                to_addr("fe80::ec62:ed39:f931:bf6d/64"),
            ],
        ),
        (
            "em0",
            vec![
                to_addr("fe80::ec62:ed39:f931:bf6d/64"),
                to_addr("fc99::9a/64"),
            ],
        ),
        (
            "em1",
            vec![
                to_addr("10.54.99.53/24"),
                to_addr("fcca::e6:9274:f580:7a1b:c335/64"),
                to_addr("fe80::38f8:7eff:fe7c:6e5d/64"),
            ],
        ),
        (
            "em2",
            vec![
                to_addr("10.54.99.53/24"),
                to_addr("fe80::38f8:7eff:fe7c:6e56/64"),
                to_addr("fe80::38f8:7eff:fe7c:6e5d/64"),
            ],
        ),
        ("lo1", vec![to_addr("fe80::ec62:ed39:f931:bf6d/64")]),
        ("lo2", vec![to_addr("169.254.1.1/16")]),
        ("lo9", vec![]),
        ("lo10", vec![]),
        ("enp0s31f6", vec![to_addr("1.1.1.1/31")]),
        ("enp0s31f7", vec![to_addr("2001::1111/120")]),
    ]);
    interfaces
        .get(name)
        .cloned()
        .ok_or_else(|| "no such network interface".to_string())
}

// Upstream: github.com/grafana/dskit/netutil/netutil_test.go::TestGetFirstAddressOf
#[test]
fn test_get_first_address_of() {
    let cases = [
        (vec!["wlan0"], "172.16.16.51", None, false),
        (
            vec!["em0"],
            "",
            Some("no useable address found for interfaces [em0]"),
            false,
        ),
        (vec!["em0"], "fc99::9a", None, true),
        (
            vec!["lo1"],
            "",
            Some("no useable address found for interfaces [lo1]"),
            false,
        ),
        (vec!["lo1"], "fe80::ec62:ed39:f931:bf6d", None, true),
        (vec!["lo2"], "169.254.1.1", None, false),
        (
            vec!["lo9"],
            "",
            Some("no useable address found for interfaces [lo9]"),
            false,
        ),
        (
            vec!["lo9", "lo10"],
            "",
            Some("no useable address found for interfaces [lo9 lo10]"),
            false,
        ),
        (vec!["lo1", "lo2", "enp0s31f6"], "1.1.1.1", None, false),
        (vec!["lo1", "lo2", "enp0s31f7"], "169.254.1.1", None, false),
        (
            vec!["lo1", "lo2", "enp0s31f7", "enp0s31f6"],
            "2001::1111",
            None,
            true,
        ),
        (
            vec!["lo1", "lo2", "enp0s31f6", "enp0s31f7"],
            "1.1.1.1",
            None,
            false,
        ),
        (
            vec!["lo1", "lo2", "enp0s31f6", "enp0s31f7"],
            "2001::1111",
            None,
            true,
        ),
        (vec!["em0", "em1"], "fc99::9a", None, true),
        (vec!["em1"], "fcca::e6:9274:f580:7a1b:c335", None, true),
        (vec!["lo1", "enp0s31f6"], "1.1.1.1", None, true),
        (vec!["lo2", "enp0s31f6"], "1.1.1.1", None, true),
        (vec!["em2"], "10.54.99.53", None, true),
    ];

    for (names, expected_addr, expected_err, enable_inet6) in cases {
        let names = svec(&names);
        let res = get_first_address_of_with(&names, mock_interface_addresses, enable_inet6, || {
            Ok(vec![])
        });
        match expected_err {
            Some(err_msg) => {
                assert_eq!(res.unwrap_err().to_string(), err_msg);
            }
            None => {
                assert_eq!(res.unwrap(), expected_addr);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/netutil/netutil_test.go::TestFilterBestIP
#[test]
fn test_filter_best_ip() {
    let to_addr = |s: &str| -> IpAddr { s.parse().unwrap() };
    let cases = [
        (vec!["169.254.1.1", "127.0.0.1"], "169.254.1.1", false),
        (vec!["1.1.1.1"], "1.1.1.1", false),
        (vec!["10.54.99.53", "169.254.1.1"], "10.54.99.53", false),
        (vec!["169.254.1.1", "fe80::1"], "169.254.1.1", false),
        (vec!["169.254.1.1", "fe80::1"], "fe80::1", true),
        (vec!["fc99::1", "fe80::1"], "invalid IP", false),
        (
            vec!["169.254.1.1", "10.54.99.53", "fc99::1", "fe80::1"],
            "fc99::1",
            true,
        ),
        (
            vec!["169.254.1.1", "10.54.99.53", "fc99::1", "fe80::1"],
            "10.54.99.53",
            false,
        ),
    ];
    for (addrs, expected, enable_inet6) in cases {
        let parsed: Vec<IpAddr> = addrs.into_iter().map(to_addr).collect();
        let best = filter_best_ip(&parsed, enable_inet6);
        assert_eq!(go_addr_string(best), expected);
    }
}

// ============================================================================
// github.com/grafana/dskit/instrument/instrument_test.go
// ============================================================================

#[derive(Default)]
struct SpyCollector {
    before: Mutex<bool>,
    after: Mutex<bool>,
    after_code: Mutex<String>,
}

impl Collector for SpyCollector {
    fn register(&self, _registry: &Registry) {}

    fn before(&self, _method: &str, _start: Instant) {
        *self.before.lock() = true;
    }

    fn after(&self, _method: &str, status_code: &str, _start: Instant) {
        *self.after.lock() = true;
        *self.after_code.lock() = status_code.to_string();
    }
}

// Upstream: github.com/grafana/dskit/instrument/instrument_test.go::TestNewHistogramCollector
#[test]
fn test_new_histogram_collector() {
    let m = HistogramVec::new(
        HistogramOpts::new("foo", "help").namespace("test").subsystem("instrumentation"),
        &HISTOGRAM_COLLECTOR_BUCKETS,
    )
    .unwrap();
    let c = HistogramCollector::new(m);
    assert!(c.metric().is_some());
}

// Upstream: github.com/grafana/dskit/instrument/instrument_test.go::TestCollectedRequest
#[test]
fn test_collected_request() {
    let c = SpyCollector::default();
    let mut f_called = false;
    let res: Result<(), CustomError> = collected_request("test", &c, None, || {
        f_called = true;
        Ok(())
    });
    assert!(res.is_ok());
    assert!(f_called);
    assert!(*c.before.lock());
    assert!(*c.after.lock());
    assert_eq!(*c.after_code.lock(), "200");
}

// Upstream: github.com/grafana/dskit/instrument/instrument_test.go::TestCollectedRequest_Error
#[test]
fn test_collected_request_error() {
    let c = SpyCollector::default();
    let res = collected_request("test", &c, None, || Err(CustomError("boom")));
    assert_eq!(res.unwrap_err().to_string(), "boom");
    assert!(*c.before.lock());
    assert!(*c.after.lock());
    assert_eq!(*c.after_code.lock(), "500");
}
