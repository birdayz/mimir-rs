//! 1:1 upstream-ported tests for `dskit/grpcencoding/snappy`, `dskit/grpcutil`, and `dskit/grpcclient`.

use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_proto::grpcclient::{
    CallOption, Config as GrpcClientConfig, DialOption, GrpcRateLimiter, InterceptorKind,
    DEFAULT_KEEPALIVE_TIME, DEFAULT_KEEPALIVE_TIMEOUT,
};
use mimir_proto::grpcutil::{
    append_message_size_to_outgoing_context, compose_tap_handles, error_to_status,
    error_to_status_code, is_canceled, status, with_service_states, with_shutdown_requested,
    ContextCanceledError, HealthCheck, RawRpcStatusError, ServiceHealthState,
    ServiceStatesSummary, ServingStatus, Sizer, TapContext, TapHandle, TapInfo,
    METADATA_MESSAGE_SIZE,
};
use mimir_proto::grpcutilpb::{
    extract_grpcutil_error_details, google_rpc, ErrorCause, ErrorDetails,
    GRPCUTIL_ERROR_DETAILS_TYPE_URL,
};
use mimir_proto::snappy::SnappyCompressor;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::Code;

// ============================================================================
// github.com/grafana/dskit/grpcencoding/snappy/snappy_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/grpcencoding/snappy/snappy_test.go::TestSnappy
#[test]
fn test_snappy() {
    let c = SnappyCompressor::new();
    assert_eq!(c.name(), "snappy");

    let tests = [
        ("empty", String::new()),
        ("short", "hello world".to_string()),
        ("long", "123456789".repeat(1024)),
    ];
    for (name, input) in tests {
        let compressed = c.compress(input.as_bytes()).expect(name);
        let decompressed = c.decompress(&compressed).expect(name);
        assert_eq!(String::from_utf8(decompressed).unwrap(), input, "{name}");
    }
}

// ============================================================================
// github.com/grafana/dskit/grpcutil/health_check_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/grpcutil/health_check_test.go::TestHealthCheck_Check_ServiceManager
#[test]
fn test_health_check_check_service_manager() {
    let cases = [
        (
            "all services are new",
            vec![ServiceHealthState::New, ServiceHealthState::New],
            ServingStatus::NotServing,
        ),
        (
            "all services are starting",
            vec![ServiceHealthState::Starting, ServiceHealthState::Starting],
            ServingStatus::NotServing,
        ),
        (
            "some services are starting and some running",
            vec![ServiceHealthState::Starting, ServiceHealthState::Running],
            ServingStatus::NotServing,
        ),
        (
            "all services are running",
            vec![ServiceHealthState::Running, ServiceHealthState::Running],
            ServingStatus::Serving,
        ),
        (
            "some services are stopping",
            vec![ServiceHealthState::Running, ServiceHealthState::Stopping],
            ServingStatus::Serving,
        ),
        (
            "some services are terminated while others running",
            vec![ServiceHealthState::Running, ServiceHealthState::Terminated],
            ServingStatus::Serving,
        ),
        (
            "all services are stopping",
            vec![ServiceHealthState::Stopping, ServiceHealthState::Stopping],
            ServingStatus::Serving,
        ),
        (
            "some services are terminated while others stopping",
            vec![ServiceHealthState::Stopping, ServiceHealthState::Terminated],
            ServingStatus::Serving,
        ),
        (
            "a service has failed while others are running",
            vec![ServiceHealthState::Running, ServiceHealthState::Failed],
            ServingStatus::NotServing,
        ),
        (
            "all services are terminated",
            vec![ServiceHealthState::Terminated, ServiceHealthState::Terminated],
            ServingStatus::NotServing,
        ),
    ];

    for (name, states, expected) in cases {
        let h = HealthCheck::new_from(vec![with_service_states(move || {
            ServiceStatesSummary::from_states(&states)
        })]);
        let res = h.check().expect(name);
        assert_eq!(res.status, expected, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/grpcutil/health_check_test.go::TestHealthCheck_List_ServiceManager
#[test]
fn test_health_check_list_service_manager() {
    let h = HealthCheck::new_from(vec![with_service_states(|| {
        ServiceStatesSummary::from_states(&[ServiceHealthState::New, ServiceHealthState::New])
    })]);
    let res = h.list().unwrap();
    assert_eq!(res.statuses.len(), 1);
    assert_eq!(
        res.statuses.get("server").map(|r| r.status),
        Some(ServingStatus::NotServing)
    );
}

// Upstream: github.com/grafana/dskit/grpcutil/health_check_test.go::TestHealthCheck_Check_ShutdownRequested
#[test]
fn test_health_check_check_shutdown_requested() {
    for (name, requested, expected) in [
        ("shutdown not requested", false, ServingStatus::Serving),
        ("shutdown is requested", true, ServingStatus::NotServing),
    ] {
        let flag = Arc::new(AtomicBool::new(requested));
        let h = HealthCheck::new_from(vec![with_shutdown_requested(flag)]);
        let res = h.check().expect(name);
        assert_eq!(res.status, expected, "{name}");
    }
}

// ============================================================================
// github.com/grafana/dskit/grpcutil/metadata_test.go
// ============================================================================

struct FakeSizer(usize);

impl Sizer for FakeSizer {
    fn size(&self) -> usize {
        self.0
    }
}

// Upstream: github.com/grafana/dskit/grpcutil/metadata_test.go::TestAppendMessageSizeToOutgoingContext
#[test]
fn test_append_message_size_to_outgoing_context() {
    let mut md = MetadataMap::new();
    let sizer = FakeSizer(5);
    append_message_size_to_outgoing_context(&mut md, &sizer);
    let vals: Vec<_> = md.get_all(METADATA_MESSAGE_SIZE).iter().collect();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].to_str().unwrap(), "5");
}

// ============================================================================
// github.com/grafana/dskit/grpcutil/status_test.go
// ============================================================================

#[derive(Debug)]
struct PlainError(&'static str);

impl fmt::Display for PlainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for PlainError {}

#[derive(Debug)]
struct WrappedError {
    msg: &'static str,
    source: Box<dyn StdError + 'static>,
}

impl fmt::Display for WrappedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.msg, self.source)
    }
}

impl StdError for WrappedError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

const MSG_ERR: &str = "this is an error";

// Upstream: github.com/grafana/dskit/grpcutil/status_test.go::TestErrorToStatus
#[test]
fn test_error_to_status() {
    let none_err: Option<&(dyn StdError + 'static)> = None;
    assert!(error_to_status(none_err).is_none());

    let rand_err = PlainError(MSG_ERR);
    assert!(error_to_status(Some(&rand_err)).is_none());

    let wrapped_rand = WrappedError {
        msg: "wrapped",
        source: Box::new(PlainError(MSG_ERR)),
    };
    assert!(error_to_status(Some(&wrapped_rand)).is_none());

    let grpc_err = tonic::Status::internal(MSG_ERR);
    let st = error_to_status(Some(&grpc_err)).expect("grpc status");
    assert_eq!(st.code, Code::Internal as i32);
    assert_eq!(st.message, MSG_ERR);

    let wrapped_grpc = WrappedError {
        msg: "wrapped",
        source: Box::new(tonic::Status::internal(MSG_ERR)),
    };
    let st = error_to_status(Some(&wrapped_grpc)).expect("wrapped grpc status");
    assert_eq!(st.code, Code::Internal as i32);
    assert_eq!(st.message, MSG_ERR);
}

// Upstream: github.com/grafana/dskit/grpcutil/status_test.go::TestErrorToStatusCode
#[test]
fn test_error_to_status_code() {
    assert_eq!(error_to_status_code(None), Code::Ok as i32);

    let rand_err = PlainError(MSG_ERR);
    assert_eq!(error_to_status_code(Some(&rand_err)), Code::Unknown as i32);

    let wrapped_rand = WrappedError {
        msg: "wrapped",
        source: Box::new(PlainError(MSG_ERR)),
    };
    assert_eq!(error_to_status_code(Some(&wrapped_rand)), Code::Unknown as i32);

    let grpc_internal = tonic::Status::internal(MSG_ERR);
    assert_eq!(
        error_to_status_code(Some(&grpc_internal)),
        Code::Internal as i32
    );

    let wrapped_unavail = WrappedError {
        msg: "wrapped",
        source: Box::new(tonic::Status::unavailable(MSG_ERR)),
    };
    assert_eq!(
        error_to_status_code(Some(&wrapped_unavail)),
        Code::Unavailable as i32
    );

    let grpc_fp = tonic::Status::failed_precondition(MSG_ERR);
    assert_eq!(
        error_to_status_code(Some(&grpc_fp)),
        Code::FailedPrecondition as i32
    );

    let wrapped_re = WrappedError {
        msg: "wrapped",
        source: Box::new(tonic::Status::resource_exhausted(MSG_ERR)),
    };
    assert_eq!(
        error_to_status_code(Some(&wrapped_re)),
        Code::ResourceExhausted as i32
    );

    let http_400 = RawRpcStatusError::new(400, MSG_ERR);
    assert_eq!(error_to_status_code(Some(&http_400)), 400);

    let wrapped_http_503 = WrappedError {
        msg: "wrapped",
        source: Box::new(RawRpcStatusError::new(503, MSG_ERR)),
    };
    assert_eq!(error_to_status_code(Some(&wrapped_http_503)), 503);
}

// Upstream: github.com/grafana/dskit/grpcutil/status_test.go::TestStatus
#[test]
fn test_status() {
    let stat = status::<ErrorDetails>(
        Code::FailedPrecondition,
        "bad data",
        GRPCUTIL_ERROR_DETAILS_TYPE_URL,
        &[],
    );
    assert_eq!(stat.code(), Code::FailedPrecondition);
    assert_eq!(stat.message(), "bad data");
    assert!(stat.details().is_empty());

    let d1 = ErrorDetails {
        cause: ErrorCause::WrongClusterValidationLabel as i32,
    };
    let d2 = ErrorDetails {
        cause: ErrorCause::UnknownCause as i32,
    };
    let stat = status(
        Code::FailedPrecondition,
        "bad data",
        GRPCUTIL_ERROR_DETAILS_TYPE_URL,
        &[Some(&d1), Some(&d2)],
    );
    assert_eq!(stat.code(), Code::FailedPrecondition);
    assert_eq!(stat.message(), "bad data");
    let decoded = google_rpc::Status::decode(stat.details()).unwrap();
    assert_eq!(decoded.details.len(), 2);
    assert_eq!(
        extract_grpcutil_error_details(&stat),
        Some(d1)
    );
    let d2_decoded = ErrorDetails::decode(decoded.details[1].value.as_slice()).unwrap();
    assert_eq!(d2_decoded, d2);

    let stat = status::<ErrorDetails>(
        Code::Internal,
        "bad details",
        GRPCUTIL_ERROR_DETAILS_TYPE_URL,
        &[None],
    );
    assert_eq!(stat.code(), Code::InvalidArgument);
    assert_eq!(
        stat.message(),
        r#"error while creating details for a Status with code Internal and error message "bad details": proto: Marshal called with nil"#
    );
}

// Upstream: github.com/grafana/dskit/grpcutil/status_test.go::TestIsCanceled
#[test]
fn test_is_canceled() {
    assert!(is_canceled(Some(&ContextCanceledError)));
    assert!(is_canceled(Some(&tonic::Status::cancelled("context canceled"))));

    let wrapped_canceled = WrappedError {
        msg: "wrapped",
        source: Box::new(tonic::Status::cancelled("context canceled")),
    };
    assert!(is_canceled(Some(&wrapped_canceled)));

    let rand_err = PlainError(MSG_ERR);
    assert!(!is_canceled(Some(&rand_err)));

    let wrapped_rand = WrappedError {
        msg: "wrapped",
        source: Box::new(PlainError(MSG_ERR)),
    };
    assert!(!is_canceled(Some(&wrapped_rand)));

    let internal_err = tonic::Status::internal(MSG_ERR);
    assert!(!is_canceled(Some(&internal_err)));

    let wrapped_deadline = WrappedError {
        msg: "wrapped",
        source: Box::new(tonic::Status::deadline_exceeded("deadline exceeded")),
    };
    assert!(!is_canceled(Some(&wrapped_deadline)));
}

// ============================================================================
// github.com/grafana/dskit/grpcutil/taphandle_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/grpcutil/taphandle_test.go::TestComposeTapHandles
#[test]
fn test_compose_tap_handles() {
    // single handle is not composed
    {
        let called = Arc::new(AtomicBool::new(false));
        let called_c = Arc::clone(&called);
        let h: TapHandle = Arc::new(move |ctx, _info| {
            called_c.store(true, Ordering::SeqCst);
            Ok(ctx)
        });
        let composed = compose_tap_handles(vec![h]);
        let ctx = composed(TapContext::new(), &TapInfo::default()).unwrap();
        assert!(ctx.value::<bool>("missing").is_none());
        assert!(called.load(Ordering::SeqCst));
    }

    // multiple handles are composed
    {
        let results = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let r1 = Arc::clone(&results);
        let h1: TapHandle = Arc::new(move |ctx, _info| {
            r1.lock().push(1);
            Ok(ctx.with_value("k1", true))
        });
        let r2 = Arc::clone(&results);
        let h2: TapHandle = Arc::new(move |ctx, _info| {
            r2.lock().push(2);
            Ok(ctx.with_value("k2", true))
        });
        let composed = compose_tap_handles(vec![h1, h2]);
        let ctx = composed(TapContext::new(), &TapInfo::default()).unwrap();
        assert_eq!(ctx.value::<bool>("k1"), Some(&true));
        assert_eq!(ctx.value::<bool>("k2"), Some(&true));
        assert_eq!(&*results.lock(), &[1, 2]);
    }

    // error short-circuits execution
    {
        let results = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let r1 = Arc::clone(&results);
        let h1: TapHandle = Arc::new(move |_ctx, _info| {
            r1.lock().push(1);
            Err("boom".into())
        });
        let r2 = Arc::clone(&results);
        let h2: TapHandle = Arc::new(move |ctx, _info| {
            r2.lock().push(2);
            Ok(ctx)
        });
        let composed = compose_tap_handles(vec![h1, h2]);
        let err = composed(TapContext::new(), &TapInfo::default()).unwrap_err();
        assert_eq!(err.to_string(), "boom");
        assert_eq!(&*results.lock(), &[1]);
    }
}

// ============================================================================
// github.com/grafana/dskit/grpcclient/grpcclient_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/grpcclient/grpcclient_test.go::TestConfig
#[test]
fn test_config() {
    // custom compressors
    {
        let comp = "custom";
        let mut cfg = GrpcClientConfig {
            custom_compressors: vec![comp.to_string()],
            ..Default::default()
        };
        let specs = cfg.register_flags_with_prefix("test");
        let f = specs
            .iter()
            .find(|s| s.name == "test.grpc-compression")
            .unwrap();
        assert_eq!(
            f.usage,
            "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 'custom' and '' (disable compression)"
        );

        // valid compressor
        cfg.grpc_compression = comp.to_string();
        assert!(cfg.validate().is_ok());
        let opts = cfg.call_options();
        assert!(opts
            .iter()
            .any(|o| matches!(o, CallOption::UseCompressor(c) if c == comp)));

        // invalid compressor
        cfg.grpc_compression = "invalid".to_string();
        assert_eq!(
            cfg.validate().unwrap_err(),
            r#"unsupported compression type: "invalid""#
        );
    }

    // keepalive defaults
    {
        let mut cfg = GrpcClientConfig::default();
        cfg.register_flags_with_prefix("test");
        assert_eq!(cfg.keepalive_time, DEFAULT_KEEPALIVE_TIME);
        assert_eq!(cfg.keepalive_timeout, DEFAULT_KEEPALIVE_TIMEOUT);
    }

    // keepalive params
    {
        let cfg = GrpcClientConfig {
            keepalive_time: Duration::from_secs(5),
            keepalive_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let kp = cfg.keepalive_params();
        assert_eq!(kp.time, Duration::from_secs(5));
        assert_eq!(kp.timeout, Duration::from_secs(2));
        assert!(kp.permit_without_stream);

        let unset = GrpcClientConfig {
            keepalive_time: Duration::ZERO,
            keepalive_timeout: Duration::ZERO,
            ..Default::default()
        };
        let kp = unset.keepalive_params();
        assert_eq!(kp.time, DEFAULT_KEEPALIVE_TIME);
        assert_eq!(kp.timeout, DEFAULT_KEEPALIVE_TIMEOUT);
        assert!(kp.permit_without_stream);
    }
}

// Upstream: github.com/grafana/dskit/grpcclient/grpcclient_test.go::TestDialOptionWithClusterValidation
#[test]
fn test_dial_option_with_cluster_validation() {
    let input_unary = vec!["instrument_unary".to_string()];
    let cases = [
        ("", 0.0, input_unary.clone(), input_unary.len(), false),
        ("cluster", 0.0, vec![], 1, true),
        ("cluster", 10.0, vec![], 2, true),
        (
            "cluster",
            0.0,
            input_unary.clone(),
            input_unary.len() + 1,
            true,
        ),
        (
            "cluster",
            10.0,
            input_unary.clone(),
            input_unary.len() + 2,
            true,
        ),
    ];

    for (cluster_label, rate_limit, unary, expected_len, expect_cluster_last) in cases {
        let cfg = GrpcClientConfig {
            cluster_validation_label: cluster_label.to_string(),
            rate_limit,
            ..Default::default()
        };
        let opts = cfg.dial_option(unary, vec![]).unwrap();
        let chain = opts
            .iter()
            .find_map(|o| match o {
                DialOption::ChainUnaryInterceptors(c) => Some(c),
                _ => None,
            })
            .expect("ChainUnaryInterceptors option");
        assert_eq!(chain.len(), expected_len);
        if expect_cluster_last {
            assert_eq!(
                chain.last(),
                Some(&InterceptorKind::ClusterValidation {
                    label: cluster_label.to_string(),
                })
            );
        } else {
            assert!(!chain
                .iter()
                .any(|k| matches!(k, InterceptorKind::ClusterValidation { .. })));
        }
    }
}

// ============================================================================
// github.com/grafana/dskit/grpcclient/ratelimit_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/grpcclient/ratelimit_test.go::TestRateLimiterFailureResultsInResourceExhaustedError
#[tokio::test]
async fn test_rate_limiter_failure_results_in_resource_exhausted_error() {
    let cfg = GrpcClientConfig {
        rate_limit_burst: 0,
        rate_limit: 0.0,
        ..Default::default()
    };
    let limiter = GrpcRateLimiter::new(&cfg);
    #[allow(clippy::result_large_err)]
    let ok_fn = || Ok(());
    let err = limiter.invoke(ok_fn).await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert_eq!(err.message(), "rate: Wait(n=1) exceeds limiter's burst 0");
}
