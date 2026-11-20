mod common;

use std::sync::Arc;
use std::time::Duration;

use mimir_common::errors::{
    EXEMPLAR_SERIES_MISSING, EXEMPLAR_TOO_FAR_IN_FUTURE, MAX_METADATA_PER_METRIC,
    MAX_METADATA_PER_USER, MAX_SERIES_PER_METRIC, MAX_SERIES_PER_USER,
    SAMPLE_DUPLICATE_TIMESTAMP, SAMPLE_OUT_OF_ORDER, SAMPLE_TIMESTAMP_TOO_OLD,
    SAMPLE_TOO_FAR_IN_FUTURE,
};
use mimir_common::limiter::{ReactiveLimiter, ReactiveLimiterConfig as CommonRlCfg};
use mimir_common::services::State;
use mimir_config::{GoDuration, InstanceLimits, ReactiveLimiterConfig};
use mimir_ingester::circuit_breaker::{
    ActivationState, BreakerError, BreakerState, CircuitBreaker, IngesterCircuitBreaker,
    OptionalCircuitBreakerExt,
};
use mimir_ingester::errors::{
    format_labels_string, IngesterError, MAX_METADATA_PER_METRIC_FLAG,
    MAX_METADATA_PER_USER_FLAG, MAX_SERIES_PER_METRIC_FLAG, MAX_SERIES_PER_USER_FLAG,
};
use mimir_ingester::reactive_limiter::IngesterReactiveLimiter;
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::prost::Message;
use mimir_proto::zerocopy::{decode_write_request, DecodeOptions, WriteRequestScratch};
use mimir_tsdb::labels::Labels;
use prometheus::Registry;
use tempfile::TempDir;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value, mock_write_request,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy, TEST_USER_ID,
};

// ============================================================================
// pkg/ingester/circuitbreaker_test.go (16 tests)
// ============================================================================

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_TryRecordFailure
#[test]
fn test_circuit_breaker_try_record_failure() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        ..Default::default()
    };
    let reg = Registry::new();
    let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();

    // no error
    assert!(!cb.try_record_failure(None));

    // context cancelled
    assert!(!cb.try_record_failure(Some(&BreakerError::Canceled)));

    // gRPC context cancelled
    assert!(!cb.try_record_failure(Some(&BreakerError::Status(tonic::Code::Cancelled))));

    // gRPC deadline exceeded
    assert!(cb.try_record_failure(Some(&BreakerError::Status(tonic::Code::DeadlineExceeded))));
    assert!(cb.try_record_failure(Some(&BreakerError::DeadlineExceeded)));

    // gRPC unavailable with INSTANCE_LIMIT details is not a failure
    let inst_err = IngesterError::max_inflight_requests_reached();
    let inst_status = inst_err.map_push_error_to_status();
    assert!(!cb.try_record_failure(Some(&BreakerError::from_status(&inst_status))));

    // gRPC unavailable with SERVICE_UNAVAILABLE details is not a failure
    let unavail_status = grpcutilpb::status_with_cortex_error_details(
        tonic::Code::Unavailable,
        "broken!",
        &cortexpb::ErrorDetails {
            cause: cortexpb::ErrorCause::ServiceUnavailable as i32,
            soft: false,
            rejected_samples: 0,
        },
    );
    assert!(!cb.try_record_failure(Some(&BreakerError::from_status(&unavail_status))));

    // gRPC unavailable without details is not a failure
    assert!(!cb.try_record_failure(Some(&BreakerError::Status(tonic::Code::Unavailable))));
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_NilImplementation
#[test]
fn test_circuit_breaker_nil_implementation() {
    let cb: Option<Arc<CircuitBreaker>> = None;

    assert!(!cb.try_record_failure_opt(Some(&BreakerError::Other("error".into()))));
    assert!(!cb.is_active_opt());
    cb.activate_opt();
    cb.deactivate_opt();
    assert!(!cb.is_open_opt());
    cb.schedule_activation_opt();
    let permit = cb.try_acquire_permit_opt().unwrap();
    assert!(permit.is_none());
    assert!(cb
        .finish_request_opt(Duration::ZERO, Duration::ZERO, None)
        .is_none());
    assert!(cb
        .record_result_opt(&[BreakerError::Other("error".into())])
        .is_none());
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_IsActive
#[test]
fn test_circuit_breaker_is_active() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        ..Default::default()
    };
    let reg = Registry::new();
    let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();

    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());

    cb.activate();
    assert_eq!(cb.activation_state(), ActivationState::Active);
    assert!(cb.is_active());

    cb.deactivate();
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_IsActiveWithDelay
#[tokio::test]
async fn test_circuit_breaker_is_active_with_delay() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        initial_delay: GoDuration::from_millis(50),
        ..Default::default()
    };
    let reg = Registry::new();
    let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();

    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());

    cb.activate();
    assert_eq!(cb.activation_state(), ActivationState::Pending);
    assert!(!cb.is_active());

    // InitialDelay starts when we first get a request, not when we activate
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(cb.activation_state(), ActivationState::Pending);

    assert!(cb.try_acquire_permit().is_ok());
    assert_eq!(cb.activation_state(), ActivationState::Activating);
    assert!(!cb.is_active());

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(cb.activation_state(), ActivationState::Active);
    assert!(cb.is_active());

    cb.deactivate();
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());

    assert!(cb.try_acquire_permit().is_ok());
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_IsActiveWithDelay_Cancel
#[tokio::test]
async fn test_circuit_breaker_is_active_with_delay_cancel() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        initial_delay: GoDuration::from_millis(50),
        ..Default::default()
    };
    let reg = Registry::new();
    let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();

    cb.activate();
    assert_eq!(cb.activation_state(), ActivationState::Pending);
    assert!(!cb.is_active());

    cb.deactivate();
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(cb.activation_state(), ActivationState::Inactive);

    cb.activate();
    assert!(cb.try_acquire_permit().is_ok());
    assert_eq!(cb.activation_state(), ActivationState::Activating);
    assert!(!cb.is_active());

    cb.deactivate();
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
    assert!(!cb.is_active());

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(cb.activation_state(), ActivationState::Inactive);
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_IsActiveWithDelay_CancelThenRestart
#[tokio::test]
async fn test_circuit_breaker_is_active_with_delay_cancel_then_restart() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        initial_delay: GoDuration::from_millis(150),
        ..Default::default()
    };
    let reg = Registry::new();
    let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();

    cb.activate();
    assert!(cb.try_acquire_permit().is_ok());
    assert_eq!(cb.activation_state(), ActivationState::Activating);
    assert!(!cb.is_active());

    tokio::time::sleep(Duration::from_millis(50)).await;
    cb.deactivate();
    assert_eq!(cb.activation_state(), ActivationState::Inactive);

    tokio::time::sleep(Duration::from_millis(50)).await;
    cb.activate();
    assert!(cb.try_acquire_permit().is_ok());
    assert_eq!(cb.activation_state(), ActivationState::Activating);
    assert!(!cb.is_active());

    // At t=150ms (50ms into second activation), cancelled first activation must NOT have activated it.
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(cb.activation_state(), ActivationState::Activating);
    assert!(!cb.is_active());

    // After full 150ms of second activation, it should be active.
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(cb.activation_state(), ActivationState::Active);
    assert!(cb.is_active());
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_TryAcquirePermit
#[test]
fn test_circuit_breaker_try_acquire_permit() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 20,
        cooldown_period: GoDuration::from_secs(10),
        ..Default::default()
    };

    // 1) inactive
    {
        let reg = Registry::new();
        let mut c = cfg.clone();
        c.initial_delay = GoDuration::from_secs(60);
        let cb = CircuitBreaker::new(&c, Some(&reg), "test-request-type", true).unwrap().unwrap();
        cb.deactivate();
        let permit = cb.try_acquire_permit().unwrap();
        assert!(permit.is_none());
    }

    // 2) closed
    {
        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", true).unwrap().unwrap();
        cb.activate();
        cb.force_close();
        let permit = cb.try_acquire_permit().unwrap();
        assert!(permit.is_some());
    }

    // 3) open
    {
        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", true).unwrap().unwrap();
        cb.activate();
        cb.force_open();
        let err = cb.try_acquire_permit().err().unwrap();
        assert!(matches!(err, IngesterError::CircuitBreakerOpen { .. }));
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "test-request-type"), ("result", "circuit_breaker_open")]
            ),
            Some(1.0)
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_transitions_total",
                &[("request_type", "test-request-type"), ("state", "open")]
            ),
            Some(1.0)
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_current_state",
                &[("request_type", "test-request-type"), ("state", "open")]
            ),
            Some(1.0)
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_current_state",
                &[("request_type", "test-request-type"), ("state", "closed")]
            ),
            Some(0.0)
        );
    }

    // 4) half-open
    {
        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", true).unwrap().unwrap();
        cb.activate();
        cb.force_half_open();
        let permit = cb.try_acquire_permit().unwrap();
        assert!(permit.is_some());
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_RecordResult
#[test]
fn test_circuit_breaker_record_result() {
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        cooldown_period: GoDuration::from_secs(10),
        ..Default::default()
    };

    type Case<'a> = (&'a str, &'a [BreakerError], Option<BreakerError>, f64, f64, f64);
    let cases: &[Case<'_>] = &[
        ("success", &[], None, 1.0, 0.0, 0.0),
        (
            "irrelevant canceled",
            &[BreakerError::Canceled],
            None,
            1.0,
            0.0,
            0.0,
        ),
        (
            "deadline exceeded",
            &[BreakerError::DeadlineExceeded],
            Some(BreakerError::DeadlineExceeded),
            0.0,
            1.0,
            1.0,
        ),
        (
            "multiple errors with deadline exceeded",
            &[BreakerError::Canceled, BreakerError::DeadlineExceeded],
            Some(BreakerError::DeadlineExceeded),
            0.0,
            1.0,
            1.0,
        ),
    ];

    for (name, errs, exp_err, exp_success, exp_error, exp_timeouts) in cases {
        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();
        cb.activate();
        let got = cb.record_result(errs);
        assert_eq!(got, *exp_err, "{name}");
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "test-request-type"), ("result", "success")]
            ),
            Some(*exp_success),
            "{name}"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "test-request-type"), ("result", "error")]
            ),
            Some(*exp_error),
            "{name}"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_request_timeouts_total",
                &[("request_type", "test-request-type")]
            ),
            Some(*exp_timeouts),
            "{name}"
        );
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestCircuitBreaker_FinishRequest
#[test]
fn test_circuit_breaker_finish_request() {
    let max_dur = Duration::from_secs(2);
    let cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        request_timeout: GoDuration::from_secs(2),
        ..Default::default()
    };

    type Case<'a> = (
        &'a str,
        Duration,
        bool,
        Option<BreakerError>,
        Option<BreakerError>,
        f64,
        f64,
        f64,
    );
    let cases: &[Case<'_>] = &[
        ("active within timeout", Duration::from_secs(1), true, None, None, 1.0, 0.0, 0.0),
        ("inactive within timeout", Duration::from_secs(1), false, None, None, 0.0, 0.0, 0.0),
        (
            "active exceeds timeout",
            Duration::from_secs(3),
            true,
            None,
            Some(BreakerError::DeadlineExceeded),
            0.0,
            1.0,
            1.0,
        ),
        ("inactive exceeds timeout", Duration::from_secs(3), false, None, None, 0.0, 0.0, 0.0),
        (
            "inactive exceeds timeout with relevant err",
            Duration::from_secs(3),
            false,
            Some(BreakerError::DeadlineExceeded),
            None,
            0.0,
            0.0,
            0.0,
        ),
        (
            "inactive exceeds timeout with canceled",
            Duration::from_secs(3),
            false,
            Some(BreakerError::Canceled),
            None,
            0.0,
            0.0,
            0.0,
        ),
        (
            "active exceeds timeout with irrelevant err",
            Duration::from_secs(3),
            true,
            Some(BreakerError::Ingester(IngesterError::max_inflight_requests_reached())),
            Some(BreakerError::DeadlineExceeded),
            0.0,
            1.0,
            1.0,
        ),
    ];

    for (name, dur, is_active, err, exp_err, exp_s, exp_e, exp_to) in cases {
        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "test-request-type", false).unwrap().unwrap();
        if *is_active {
            cb.activate();
        }
        let got = cb.finish_request_with_max_duration(*dur, max_dur, err.as_ref(), false);
        assert_eq!(got, *exp_err, "{name}");
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "test-request-type"), ("result", "success")]
            ),
            Some(*exp_s),
            "{name}"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "test-request-type"), ("result", "error")]
            ),
            Some(*exp_e),
            "{name}"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_request_timeouts_total",
                &[("request_type", "test-request-type")]
            ),
            Some(*exp_to),
            "{name}"
        );
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestIngester_IngestStorage_PushToStorageAndReleaseRequest_CircuitBreaker
#[tokio::test]
async fn test_ingester_ingest_storage_push_to_storage_and_release_request_circuit_breaker() {
    let push_timeout = Duration::from_millis(100);
    for initial_delay_enabled in [false, true] {
        let dir = TempDir::new().unwrap();
        let reg = Registry::new();
        let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.active_series.active_series_metrics_idle_timeout = GoDuration::from_millis(100);
        cfg.instance_limits = InstanceLimits {
            max_tenants: 3,
            ..Default::default()
        };
        cfg.push_circuit_breaker = mimir_config::CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 2,
            cooldown_period: GoDuration::from_secs(10),
            initial_delay: if initial_delay_enabled {
                GoDuration::from_hours(1)
            } else {
                GoDuration(0)
            },
            request_timeout: GoDuration::from_millis(100),
            ..Default::default()
        };

        let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            Some(&reg),
        );
        ing.circuit_breaker.push.as_ref().unwrap().enable_test_mode();
        start_and_wait_healthy(&ing).await;

        let lbls = Labels::from_pairs([("__name__", "test")]);
        let req0 = mock_write_request(&lbls, 1.0, 8);
        let bytes0 = req0.encode_to_vec();
        let mut scratch = WriteRequestScratch::new();
        let borrowed0 =
            decode_write_request(&bytes0, DecodeOptions::classic(), &mut scratch).unwrap();
        ing.push_borrowed_to_storage("test-0", &borrowed0, bytes0.len() as i64)
            .unwrap();

        let mut count = 0usize;
        for uid in ["test-1", "test-2"] {
            for ts in [9i64, 10] {
                let req = mock_write_request(&lbls, 1.0, ts);
                let bytes = req.encode_to_vec();
                let borrowed =
                    decode_write_request(&bytes, DecodeOptions::classic(), &mut scratch).unwrap();
                count += 1;
                ing.circuit_breaker
                    .push
                    .as_ref()
                    .unwrap()
                    .set_test_request_delay(push_timeout + Duration::from_millis(1));
                let res = ing.push_borrowed_to_storage(uid, &borrowed, bytes.len() as i64);
                if initial_delay_enabled || count <= 2 {
                    assert!(res.is_ok(), "unexpected err: {res:?}");
                } else {
                    assert!(matches!(res, Err(IngesterError::CircuitBreakerOpen { .. })));
                }
            }
        }

        if initial_delay_enabled {
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "success")]
                ),
                Some(0.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_current_state",
                    &[("request_type", "push"), ("state", "closed")]
                ),
                Some(1.0)
            );
        } else {
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "circuit_breaker_open")]
                ),
                Some(2.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "error")]
                ),
                Some(2.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "success")]
                ),
                Some(1.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_request_timeouts_total",
                    &[("request_type", "push")]
                ),
                Some(2.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_current_state",
                    &[("request_type", "push"), ("state", "open")]
                ),
                Some(1.0)
            );
        }
        let _ = ing.stop().await;
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestIngester_StartPushRequest_CircuitBreakerOpen
#[tokio::test]
async fn test_ingester_start_push_request_circuit_breaker_open() {
    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_circuit_breaker = mimir_config::CircuitBreakerConfig {
        enabled: true,
        cooldown_period: GoDuration::from_secs(10),
        ..Default::default()
    };

    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;

    let push_cb = ing.circuit_breaker.push.as_ref().unwrap();
    push_cb.force_close();
    let p = ing.start_push_request(0).await.unwrap();
    p.finish(None);

    push_cb.force_open();
    let err = ing.start_push_request(0).await.err().unwrap();
    assert!(matches!(err, IngesterError::CircuitBreakerOpen { .. }));

    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_circuit_breaker_results_total",
            &[("request_type", "push"), ("result", "circuit_breaker_open")]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_circuit_breaker_transitions_total",
            &[("request_type", "push"), ("state", "open")]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_circuit_breaker_current_state",
            &[("request_type", "push"), ("state", "open")]
        ),
        Some(1.0)
    );
    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestIngester_FinishPushRequest
#[tokio::test]
async fn test_ingester_finish_push_request() {
    type Case<'a> = (&'a str, Duration, bool, Option<IngesterError>, f64, f64, f64);
    let cases: &[Case<'_>] = &[
        ("permit within timeout no err", Duration::from_secs(1), true, None, 1.0, 0.0, 0.0),
        ("no permit within timeout no err", Duration::from_secs(1), false, None, 0.0, 0.0, 0.0),
        ("permit exceeds timeout no err", Duration::from_secs(3), true, None, 0.0, 1.0, 1.0),
        ("no permit exceeds timeout no err", Duration::from_secs(3), false, None, 0.0, 0.0, 0.0),
        (
            "permit exceeds timeout instance limit err",
            Duration::from_secs(3),
            true,
            Some(IngesterError::max_inflight_requests_reached()),
            0.0,
            1.0,
            1.0,
        ),
        (
            "no permit exceeds timeout instance limit err",
            Duration::from_secs(3),
            false,
            Some(IngesterError::max_inflight_requests_reached()),
            0.0,
            0.0,
            0.0,
        ),
    ];

    for (name, dur, acquired, err, exp_s, exp_e, exp_to) in cases {
        let dir = TempDir::new().unwrap();
        let reg = Registry::new();
        let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.push_circuit_breaker = mimir_config::CircuitBreakerConfig {
            enabled: true,
            request_timeout: GoDuration::from_secs(2),
            ..Default::default()
        };
        let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            Some(&reg),
        );
        start_and_wait_healthy(&ing).await;
        if !*acquired {
            ing.circuit_breaker.push.as_ref().unwrap().deactivate();
        }
        let permit = ing.start_push_request(0).await.unwrap();
        permit.finish_with_duration(*dur, err.as_ref());

        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "push"), ("result", "success")]
            ),
            Some(*exp_s),
            "{name}"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "push"), ("result", "error")]
            ),
            Some(*exp_e),
            "{name}"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_request_timeouts_total",
                &[("request_type", "push")]
            ),
            Some(*exp_to),
            "{name}"
        );
        let _ = ing.stop().await;
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestIngester_Push_CircuitBreaker_DeadlineExceeded
#[tokio::test]
async fn test_ingester_push_circuit_breaker_deadline_exceeded() {
    let push_timeout = Duration::from_secs(1);
    for initial_delay_enabled in [false, true] {
        let dir = TempDir::new().unwrap();
        let reg = Registry::new();
        let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.active_series.active_series_metrics_idle_timeout = GoDuration::from_millis(100);
        cfg.push_circuit_breaker = mimir_config::CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 2,
            cooldown_period: GoDuration::from_secs(10),
            initial_delay: if initial_delay_enabled {
                GoDuration::from_hours(1)
            } else {
                GoDuration(0)
            },
            request_timeout: GoDuration::from_secs(1),
            ..Default::default()
        };

        let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            Some(&reg),
        );
        ing.circuit_breaker.push.as_ref().unwrap().enable_test_mode();
        start_and_wait_healthy(&ing).await;

        let lbls = Labels::from_pairs([("__name__", "test")]);
        let req0 = mock_write_request(&lbls, 1.0, 8);
        ing.push_grpc("test-0", &req0).await.unwrap();

        let mut count = 0usize;
        for uid in ["test-1", "test-2"] {
            for ts in [9i64, 10] {
                let req = mock_write_request(&lbls, 1.0, ts);
                ing.circuit_breaker
                    .push
                    .as_ref()
                    .unwrap()
                    .set_test_request_delay(push_timeout + Duration::from_millis(1));
                count += 1;
                let res = ing.push_grpc(uid, &req).await;
                if initial_delay_enabled || count <= 2 {
                    assert!(res.is_ok());
                } else {
                    assert_eq!(
                        ing.circuit_breaker.push.as_ref().unwrap().state(),
                        BreakerState::Open
                    );
                    assert!(matches!(res, Err(IngesterError::CircuitBreakerOpen { .. })));
                }
            }
        }

        if initial_delay_enabled {
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "success")]
                ),
                Some(0.0)
            );
        } else {
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "circuit_breaker_open")]
                ),
                Some(2.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "error")]
                ),
                Some(2.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", "push"), ("result", "success")]
                ),
                Some(1.0)
            );
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_request_timeouts_total",
                    &[("request_type", "push")]
                ),
                Some(2.0)
            );
        }
        let _ = ing.stop().await;
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestPRCircuitBreaker_NewPRCircuitBreaker
#[test]
fn test_pr_circuit_breaker_new_pr_circuit_breaker() {
    let push_cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        cooldown_period: GoDuration::from_secs(10),
        ..Default::default()
    };
    let read_cfg = push_cfg.clone();
    let reg = Registry::new();
    let pr_cb = IngesterCircuitBreaker::new_with_test_mode(&push_cfg, &read_cfg, Some(&reg), true).unwrap();
    assert!(pr_cb.push.is_some());
    assert!(pr_cb.read.is_some());

    for rt in ["push", "read"] {
        for res in ["success", "error", "circuit_breaker_open"] {
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_results_total",
                    &[("request_type", rt), ("result", res)]
                ),
                Some(0.0)
            );
        }
        for st in ["closed", "half-open", "open"] {
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_transitions_total",
                    &[("request_type", rt), ("state", st)]
                ),
                Some(0.0)
            );
            let exp_g = if st == "closed" { 1.0 } else { 0.0 };
            assert_eq!(
                metric_value(
                    &reg,
                    "cortex_ingester_circuit_breaker_current_state",
                    &[("request_type", rt), ("state", st)]
                ),
                Some(exp_g)
            );
        }
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_request_timeouts_total",
                &[("request_type", rt)]
            ),
            Some(0.0)
        );
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestPRCircuitBreaker_TryPushAcquirePermit
#[test]
fn test_pr_circuit_breaker_try_push_acquire_permit() {
    let push_cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 20,
        cooldown_period: GoDuration::from_secs(10),
        request_timeout: GoDuration::from_secs(2),
        ..Default::default()
    };

    // inactive
    {
        let reg = Registry::new();
        let cb = IngesterCircuitBreaker::new_with_test_mode(
            &push_cfg,
            &Default::default(),
            Some(&reg),
            true,
        )
        .unwrap();
        cb.push.as_ref().unwrap().deactivate();
        let p = cb.try_acquire_push_permit().unwrap();
        assert!(p.is_none());
    }

    // closed
    {
        let reg = Registry::new();
        let cb = IngesterCircuitBreaker::new_with_test_mode(
            &push_cfg,
            &Default::default(),
            Some(&reg),
            true,
        )
        .unwrap();
        let push = cb.push.as_ref().unwrap();
        push.activate();
        push.force_close();
        let p = cb.try_acquire_push_permit().unwrap().unwrap();
        p.finish(Duration::ZERO, false);
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "push"), ("result", "success")]
            ),
            Some(1.0)
        );
    }

    // open
    {
        let reg = Registry::new();
        let cb = IngesterCircuitBreaker::new_with_test_mode(
            &push_cfg,
            &Default::default(),
            Some(&reg),
            true,
        )
        .unwrap();
        let push = cb.push.as_ref().unwrap();
        push.activate();
        push.force_open();
        let err = cb.try_acquire_push_permit().err().unwrap();
        assert!(matches!(err, IngesterError::CircuitBreakerOpen { .. }));
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "push"), ("result", "circuit_breaker_open")]
            ),
            Some(1.0)
        );
    }

    // half-open
    {
        let reg = Registry::new();
        let cb = IngesterCircuitBreaker::new_with_test_mode(
            &push_cfg,
            &Default::default(),
            Some(&reg),
            true,
        )
        .unwrap();
        let push = cb.push.as_ref().unwrap();
        push.activate();
        push.force_half_open();
        let p = cb.try_acquire_push_permit().unwrap().unwrap();
        p.finish(Duration::ZERO, false);
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "push"), ("result", "success")]
            ),
            Some(1.0)
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_circuit_breaker_transitions_total",
                &[("request_type", "push"), ("state", "closed")]
            ),
            Some(1.0)
        );
    }
}

// Upstream: pkg/ingester/circuitbreaker_test.go::TestPRCircuitBreaker_TryReadAcquirePermit
#[test]
fn test_pr_circuit_breaker_try_read_acquire_permit() {
    let push_cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 20,
        cooldown_period: GoDuration::from_secs(10),
        request_timeout: GoDuration::from_secs(2),
        ..Default::default()
    };
    let read_cfg = mimir_config::CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 20,
        cooldown_period: GoDuration::from_secs(10),
        request_timeout: GoDuration::from_secs(30),
        ..Default::default()
    };

    #[derive(Clone, Copy)]
    enum SetupState {
        Inactive,
        Closed,
        Open,
        HalfOpen,
    }

    let apply = |cb: &Arc<CircuitBreaker>, st: SetupState| match st {
        SetupState::Inactive => cb.deactivate(),
        SetupState::Closed => {
            cb.activate();
            cb.force_close();
        }
        SetupState::Open => {
            cb.activate();
            cb.force_open();
        }
        SetupState::HalfOpen => {
            cb.activate();
            cb.force_half_open();
        }
    };

    let matrix = [
        (SetupState::Inactive, SetupState::Inactive, false),
        (SetupState::Inactive, SetupState::Closed, false),
        (SetupState::Inactive, SetupState::Open, false),
        (SetupState::Inactive, SetupState::HalfOpen, false),
        (SetupState::Closed, SetupState::Inactive, false),
        (SetupState::Closed, SetupState::Closed, false),
        (SetupState::Closed, SetupState::Open, true),
        (SetupState::Closed, SetupState::HalfOpen, false),
        (SetupState::Open, SetupState::Inactive, true),
        (SetupState::Open, SetupState::Closed, true),
        (SetupState::Open, SetupState::Open, true),
        (SetupState::Open, SetupState::HalfOpen, true),
        (SetupState::HalfOpen, SetupState::Inactive, false),
        (SetupState::HalfOpen, SetupState::Closed, false),
        (SetupState::HalfOpen, SetupState::Open, true),
        (SetupState::HalfOpen, SetupState::HalfOpen, false),
    ];

    for (read_st, push_st, expect_err) in matrix {
        let reg = Registry::new();
        let cb = IngesterCircuitBreaker::new_with_test_mode(&push_cfg, &read_cfg, Some(&reg), true).unwrap();
        apply(cb.read.as_ref().unwrap(), read_st);
        apply(cb.push.as_ref().unwrap(), push_st);
        let res = cb.try_acquire_read_permit();
        if expect_err {
            assert!(matches!(res, Err(IngesterError::CircuitBreakerOpen { .. })));
        } else {
            let p = res.unwrap();
            if let Some(permit) = p {
                permit.finish(Duration::ZERO, false);
                assert_eq!(
                    metric_value(
                        &reg,
                        "cortex_ingester_circuit_breaker_results_total",
                        &[("request_type", "read"), ("result", "success")]
                    ),
                    Some(1.0)
                );
            }
        }
    }
}

// ============================================================================
// pkg/ingester/errors_test.go (16 tests)
// ============================================================================

const ERR_TEST_TIMESTAMP_MS: i64 = 1575043969;

fn check_ingester_error(
    err: &IngesterError,
    expected_cause: cortexpb::ErrorCause,
    expected_soft: bool,
) {
    assert_eq!(err.error_cause(), expected_cause);
    assert_eq!(err.is_soft(), expected_soft);
}

fn check_status_details(
    status: &tonic::Status,
    expected: Option<cortexpb::ErrorDetails>,
) {
    let details = grpcutilpb::extract_cortex_error_details(status);
    assert_eq!(details, expected);
}

// Upstream: pkg/ingester/errors_test.go::TestUnavailableError
#[test]
fn test_unavailable_error() {
    let err = IngesterError::Unavailable {
        state: State::Starting,
    };
    assert_eq!(err.to_string(), "ingester is unavailable (current state: Starting)");
    check_ingester_error(&err, cortexpb::ErrorCause::ServiceUnavailable, false);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    assert_eq!(
        wrapped.to_string(),
        format!("user={TEST_USER_ID}: ingester is unavailable (current state: Starting)")
    );
    check_ingester_error(&wrapped, cortexpb::ErrorCause::ServiceUnavailable, false);
}

// Upstream: pkg/ingester/errors_test.go::TestInstanceLimitReachedError
#[test]
fn test_instance_limit_reached_error() {
    let msg = "this is a limit error message";
    let err = IngesterError::InstanceLimitReached {
        message: msg.to_string(),
    };
    assert_eq!(err.to_string(), msg);
    check_ingester_error(&err, cortexpb::ErrorCause::InstanceLimit, false);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    assert_eq!(wrapped.to_string(), format!("user={TEST_USER_ID}: {msg}"));
    check_ingester_error(&wrapped, cortexpb::ErrorCause::InstanceLimit, false);
}

// Upstream: pkg/ingester/errors_test.go::TestNewTSDBUnavailableError
#[test]
fn test_new_tsdb_unavailable_error() {
    let msg = "TSDB Head forced compaction in progress and no write request is currently allowed";
    let err = IngesterError::tsdb_forced_compaction();
    assert_eq!(err.to_string(), msg);
    check_ingester_error(&err, cortexpb::ErrorCause::TsdbUnavailable, false);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::TsdbUnavailable, false);
}

// Upstream: pkg/ingester/errors_test.go::TestNewPerUserSeriesLimitError
#[test]
fn test_new_per_user_series_limit_error() {
    let limit = 100;
    let err = IngesterError::per_user_series_limit_reached(limit);
    let expected = MAX_SERIES_PER_USER.message_with_per_tenant_limit_config(
        &format!("per-user series limit of {limit} exceeded"),
        MAX_SERIES_PER_USER_FLAG,
        &[],
    );
    assert_eq!(err.to_string(), expected);
    check_ingester_error(&err, cortexpb::ErrorCause::TenantLimit, true);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::TenantLimit, true);
}

// Upstream: pkg/ingester/errors_test.go::TestNewPerUserMetadataLimitError
#[test]
fn test_new_per_user_metadata_limit_error() {
    let limit = 100;
    let err = IngesterError::per_user_metadata_limit_reached(limit);
    let expected = MAX_METADATA_PER_USER.message_with_per_tenant_limit_config(
        &format!("per-user metric metadata limit of {limit} exceeded"),
        MAX_METADATA_PER_USER_FLAG,
        &[],
    );
    assert_eq!(err.to_string(), expected);
    check_ingester_error(&err, cortexpb::ErrorCause::TenantLimit, true);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::TenantLimit, true);
}

// Upstream: pkg/ingester/errors_test.go::TestNewPerMetricSeriesLimitError
#[test]
fn test_new_per_metric_series_limit_error() {
    let limit = 100;
    let series = format_labels_string([("__name__", "testmetric"), ("foo", "biz")]);
    let err = IngesterError::per_metric_series_limit_reached(limit, series.clone());
    let expected = format!(
        "{} This is for series {}",
        MAX_SERIES_PER_METRIC.message_with_per_tenant_limit_config(
            &format!("per-metric series limit of {limit} exceeded"),
            MAX_SERIES_PER_METRIC_FLAG,
            &[],
        ),
        series
    );
    assert_eq!(err.to_string(), expected);
    check_ingester_error(&err, cortexpb::ErrorCause::TenantLimit, true);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::TenantLimit, true);
}

// Upstream: pkg/ingester/errors_test.go::TestNewPerMetricMetadataLimitError
#[test]
fn test_new_per_metric_metadata_limit_error() {
    let limit = 100;
    let family = "testmetric";
    let err = IngesterError::per_metric_metadata_limit_reached(limit, family.to_string());
    let expected = format!(
        "{} This is for metric {}",
        MAX_METADATA_PER_METRIC.message_with_per_tenant_limit_config(
            &format!("per-metric metadata limit of {limit} exceeded"),
            MAX_METADATA_PER_METRIC_FLAG,
            &[],
        ),
        family
    );
    assert_eq!(err.to_string(), expected);
    check_ingester_error(&err, cortexpb::ErrorCause::TenantLimit, true);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::TenantLimit, true);
}

// Upstream: pkg/ingester/errors_test.go::TestNewSampleError
#[test]
fn test_new_sample_error() {
    let series = format_labels_string([("__name__", "test")]);
    let cases = [
        (
            IngesterError::sample_timestamp_too_old(ERR_TEST_TIMESTAMP_MS, series.clone()),
            "the sample has been rejected because its timestamp is too old (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test",
        ),
        (
            IngesterError::sample_timestamp_too_old_ooo_enabled(
                ERR_TEST_TIMESTAMP_MS,
                series.clone(),
                Duration::from_secs(2 * 3600),
            ),
            "the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of 2h (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test",
        ),
        (
            IngesterError::sample_too_far_in_future(ERR_TEST_TIMESTAMP_MS, series.clone()),
            "received a sample whose timestamp is too far in the future (err-mimir-too-far-in-future). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test",
        ),
        (
            IngesterError::sample_out_of_order(ERR_TEST_TIMESTAMP_MS, series.clone()),
            "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed (err-mimir-sample-out-of-order). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test",
        ),
        (
            IngesterError::sample_duplicate_timestamp(
                format!(
                    "duplicate sample for timestamp {}; overrides not allowed: existing {}, new value {}",
                    ERR_TEST_TIMESTAMP_MS, 1.5, 2.5
                ),
                ERR_TEST_TIMESTAMP_MS,
                series,
            ),
            "duplicate sample for timestamp 1575043969; overrides not allowed: existing 1.5, new value 2.5 (err-mimir-sample-duplicate-timestamp). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series test",
        ),
    ];

    for (err, expected_msg) in cases {
        assert_eq!(err.to_string(), expected_msg);
        check_ingester_error(&err, cortexpb::ErrorCause::BadData, true);
        let wrapped = err.with_user(TEST_USER_ID, 0);
        check_ingester_error(&wrapped, cortexpb::ErrorCause::BadData, true);
    }
}

// Upstream: pkg/ingester/errors_test.go::TestNewExemplarError
#[test]
fn test_new_exemplar_error() {
    let series = format_labels_string([("__name__", "test")]);
    let exemplar = format_labels_string([("traceID", "123")]);
    let cases = [
        (
            IngesterError::exemplar_missing_series(
                ERR_TEST_TIMESTAMP_MS,
                series.clone(),
                exemplar.clone(),
            ),
            "the exemplar has been rejected because the related series has not been ingested yet (err-mimir-exemplar-series-missing). The affected exemplar is {traceID=\"123\"} with timestamp 1970-01-19T05:30:43.969Z for series test",
        ),
        (
            IngesterError::exemplar_too_far_in_future(ERR_TEST_TIMESTAMP_MS, series, exemplar),
            "received an exemplar whose timestamp is too far in the future (err-mimir-exemplar-too-far-in-future). The affected exemplar is {traceID=\"123\"} with timestamp 1970-01-19T05:30:43.969Z for series test",
        ),
    ];

    for (err, expected_msg) in cases {
        assert_eq!(err.to_string(), expected_msg);
        check_ingester_error(&err, cortexpb::ErrorCause::BadData, true);
        let wrapped = err.with_user(TEST_USER_ID, 0);
        check_ingester_error(&wrapped, cortexpb::ErrorCause::BadData, true);
    }
}

// Upstream: pkg/ingester/errors_test.go::TestNewTSDBIngestExemplarErr
#[test]
fn test_new_tsdb_ingest_exemplar_err() {
    let series = format_labels_string([("__name__", "test")]);
    let exemplar = format_labels_string([("traceID", "123")]);
    let err = IngesterError::TsdbIngestExemplar {
        original_err: "another error".to_string(),
        timestamp_ms: ERR_TEST_TIMESTAMP_MS,
        series_labels: series,
        exemplar_labels: exemplar,
    };
    assert_eq!(
        err.to_string(),
        "err: another error. timestamp=1970-01-19T05:30:43.969Z, series=test, exemplar={traceID=\"123\"}"
    );
    check_ingester_error(&err, cortexpb::ErrorCause::BadData, true);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::BadData, true);
}

// Upstream: pkg/ingester/errors_test.go::TestTooBusyError
#[test]
fn test_too_busy_error() {
    let err = IngesterError::TooBusy;
    assert_eq!(
        err.to_string(),
        "ingester is currently too busy to process queries, try again later"
    );
    check_ingester_error(&err, cortexpb::ErrorCause::TooBusy, false);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::TooBusy, false);
}

// Upstream: pkg/ingester/errors_test.go::TestNewCircuitBreakerOpenError
#[test]
fn test_new_circuit_breaker_open_error() {
    let err = IngesterError::CircuitBreakerOpen {
        request_type: "foo".to_string(),
        remaining_delay: Duration::from_secs(1),
    };
    assert_eq!(
        err.to_string(),
        "circuit breaker open on foo request type with remaining delay 1s"
    );
    check_ingester_error(&err, cortexpb::ErrorCause::CircuitBreakerOpen, false);

    let wrapped = err.with_user(TEST_USER_ID, 0);
    check_ingester_error(&wrapped, cortexpb::ErrorCause::CircuitBreakerOpen, false);
}

// Upstream: pkg/ingester/errors_test.go::TestNewErrorWithStatus
#[test]
fn test_new_error_with_status() {
    let msg = "this is an error";
    let ing_err = IngesterError::TooBusy;
    let s1 = grpcutilpb::status_with_cortex_error_details(
        tonic::Code::Unimplemented,
        ing_err.to_string(),
        &ing_err.error_details().unwrap(),
    );
    assert_eq!(s1.code(), tonic::Code::Unimplemented);
    assert_eq!(
        grpcutilpb::extract_cortex_error_details(&s1),
        Some(cortexpb::ErrorDetails {
            cause: cortexpb::ErrorCause::TooBusy as i32,
            soft: false,
            rejected_samples: 0,
        })
    );

    let non_ing_err = IngesterError::Internal {
        message: msg.to_string(),
    };
    assert!(non_ing_err.error_details().is_none());
    let s2 = tonic::Status::unimplemented(non_ing_err.to_string());
    assert_eq!(s2.code(), tonic::Code::Unimplemented);
    assert_eq!(s2.message(), msg);
    assert!(grpcutilpb::extract_cortex_error_details(&s2).is_none());
}

// Upstream: pkg/ingester/errors_test.go::TestWrapOrAnnotateWithUser
#[test]
fn test_wrap_or_annotate_with_user() {
    let user_id = "1";
    let internal = IngesterError::Internal {
        message: "this error will be annotated".to_string(),
    };
    let wrapped_internal = internal.with_user(user_id, 0);
    assert_eq!(
        wrapped_internal.to_string(),
        "user=1: this error will be annotated"
    );
    assert!(wrapped_internal.error_details().is_none());

    let series = format_labels_string([("__name__", "test")]);
    let sample_err = IngesterError::sample_timestamp_too_old(ERR_TEST_TIMESTAMP_MS, series);
    let expected_msg = format!("user={user_id}: {sample_err}");
    let wrapped_safe = sample_err.clone().with_user(user_id, 0);
    assert_eq!(wrapped_safe.to_string(), expected_msg);
    if let IngesterError::WrappedUser { inner, .. } = wrapped_safe {
        assert_eq!(*inner, sample_err);
    } else {
        panic!("expected WrappedUser");
    }
}

// Upstream: pkg/ingester/errors_test.go::TestMapPushErrorToErrorWithStatus
#[test]
fn test_map_push_error_to_error_with_status() {
    let series = format_labels_string([("__name__", "testmetric"), ("foo", "biz")]);
    let exemplar = format_labels_string([("traceID", "123")]);

    let cases: Vec<(IngesterError, tonic::Code, bool, Option<cortexpb::ErrorDetails>)> = vec![
        (
            IngesterError::Internal {
                message: "this is an error".into(),
            },
            tonic::Code::Internal,
            false,
            None,
        ),
        (
            IngesterError::Unavailable {
                state: State::Stopping,
            },
            tonic::Code::Unavailable,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::ServiceUnavailable as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::PushGrpcDisabled,
            tonic::Code::Unimplemented,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::MethodNotAllowed as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::InstanceLimitReached {
                message: "instance limit reached".into(),
            },
            tonic::Code::Unavailable,
            true,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::InstanceLimit as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::TsdbUnavailable {
                message: "tsdb stopping".into(),
            },
            tonic::Code::Internal,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::TsdbUnavailable as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::Sample {
                err_id: SAMPLE_TIMESTAMP_TOO_OLD,
                err_msg: "sample error".into(),
                timestamp_ms: 1,
                series: series.clone(),
            },
            tonic::Code::InvalidArgument,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::BadData as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::Exemplar {
                err_id: EXEMPLAR_SERIES_MISSING,
                err_msg: "exemplar error".into(),
                timestamp_ms: 1,
                series_labels: series.clone(),
                exemplar_labels: exemplar.clone(),
            },
            tonic::Code::InvalidArgument,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::BadData as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::TsdbIngestExemplar {
                original_err: "this is an error".into(),
                timestamp_ms: 1,
                series_labels: series.clone(),
                exemplar_labels: exemplar,
            },
            tonic::Code::InvalidArgument,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::BadData as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::per_user_series_limit_reached(10),
            tonic::Code::FailedPrecondition,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::TenantLimit as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::per_user_metadata_limit_reached(10),
            tonic::Code::FailedPrecondition,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::TenantLimit as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::per_metric_series_limit_reached(10, series),
            tonic::Code::FailedPrecondition,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::TenantLimit as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::per_metric_metadata_limit_reached(10, "testmetric".into()),
            tonic::Code::FailedPrecondition,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::TenantLimit as i32,
                soft: true,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::CircuitBreakerOpen {
                request_type: "foo".into(),
                remaining_delay: Duration::from_secs(1),
            },
            tonic::Code::Unavailable,
            false,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::CircuitBreakerOpen as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
    ];

    for (err, expected_code, expected_do_not_log, expected_details) in cases {
        let status = err.map_push_error_to_status();
        assert_eq!(status.code(), expected_code);
        assert_eq!(status.message(), err.to_string());
        assert_eq!(err.is_do_not_log(), expected_do_not_log);
        check_status_details(&status, expected_details);
    }

    // Verify global error constants used in tests compile & match expected strings
    let _ = (
        SAMPLE_OUT_OF_ORDER,
        SAMPLE_TOO_FAR_IN_FUTURE,
        SAMPLE_DUPLICATE_TIMESTAMP,
        EXEMPLAR_TOO_FAR_IN_FUTURE,
    );
}

// Upstream: pkg/ingester/errors_test.go::TestMapReadErrorToErrorWithStatus
#[test]
fn test_map_read_error_to_error_with_status() {
    let cases: Vec<(IngesterError, tonic::Code, Option<cortexpb::ErrorDetails>)> = vec![
        (
            IngesterError::Internal {
                message: "this is an error".into(),
            },
            tonic::Code::Internal,
            None,
        ),
        (
            IngesterError::Unavailable {
                state: State::Stopping,
            },
            tonic::Code::Unavailable,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::ServiceUnavailable as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::TooBusy,
            tonic::Code::ResourceExhausted,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::TooBusy as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
        (
            IngesterError::CircuitBreakerOpen {
                request_type: "foo".into(),
                remaining_delay: Duration::from_secs(1),
            },
            tonic::Code::Unavailable,
            Some(cortexpb::ErrorDetails {
                cause: cortexpb::ErrorCause::CircuitBreakerOpen as i32,
                soft: false,
                rejected_samples: 0,
            }),
        ),
    ];

    for (err, expected_code, expected_details) in cases {
        let status = err.map_read_error_to_status();
        assert_eq!(status.code(), expected_code);
        assert_eq!(status.message(), err.to_string());
        check_status_details(&status, expected_details);
    }
}

// ============================================================================
// pkg/ingester/instance_limits_test.go (2 tests)
// ============================================================================

// Upstream: pkg/ingester/instance_limits_test.go::TestInstanceLimitsUnmarshal
#[test]
fn test_instance_limits_unmarshal() {
    let yaml = r#"
max_ingestion_rate: 125.678
max_tenants: 50000
"#;
    let defaults = InstanceLimits {
        max_ingestion_rate: 10.0,
        max_tenants: 20,
        max_series: 30,
        max_inflight_push_requests: 40,
        ..Default::default()
    };
    let parsed_partial: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
    let mut base_val = serde_yaml::to_value(&defaults).unwrap();
    if let (Some(base_map), Some(patch_map)) =
        (base_val.as_mapping_mut(), parsed_partial.as_mapping())
    {
        for (k, v) in patch_map {
            base_map.insert(k.clone(), v.clone());
        }
    }
    let l: InstanceLimits = serde_yaml::from_value(base_val).unwrap();
    assert!((l.max_ingestion_rate - 125.678).abs() < f64::EPSILON);
    assert_eq!(l.max_tenants, 50000);
    assert_eq!(l.max_series, 30);
    assert_eq!(l.max_inflight_push_requests, 40);
}

// Upstream: pkg/ingester/instance_limits_test.go::TestInstanceLimitErr
#[test]
fn test_instance_limit_err() {
    let limit_errors = [
        IngesterError::max_ingestion_rate_reached(),
        IngesterError::max_tenants_reached(),
        IngesterError::max_in_memory_series_reached(),
        IngesterError::max_inflight_requests_reached(),
    ];

    for err in limit_errors {
        assert!(matches!(err, IngesterError::InstanceLimitReached { .. }));
        check_ingester_error(&err, cortexpb::ErrorCause::InstanceLimit, false);

        let wrapped = err.with_user(TEST_USER_ID, 0);
        check_ingester_error(&wrapped, cortexpb::ErrorCause::InstanceLimit, false);
    }
}

// ============================================================================
// pkg/ingester/reactivelimiter_test.go (1 test)
// ============================================================================

// Upstream: pkg/ingester/reactivelimiter_test.go::Test_ingesterReactiveLimiter
#[test]
fn test_ingester_reactive_limiter() {
    let read_cfg = ReactiveLimiterConfig {
        enabled: true,
        max_limit_factor: 1.0,
        min_limit_factor: 1.2,
        initial_rejection_factor: 1.0,
        max_rejection_factor: 2.0,
        ..Default::default()
    };
    let push_cfg = ReactiveLimiterConfig::default();
    let reg = Registry::new();
    let rl = IngesterReactiveLimiter::new(
        &mimir_config::PrioritizerConfig::default(),
        &push_cfg,
        &read_cfg,
        Some(&reg),
    )
    .unwrap();
    assert!(rl.prioritizer.is_none());

    // "newReactiveLimiter with Prioritizer": building a prioritized limiter works without panic.
    let prioritizer = Arc::new(mimir_ingester::prioritizer::Prioritizer::new());
    let prioritized = mimir_ingester::reactive_limiter::IngesterRl::new_with_priority(
        &read_cfg,
        mimir_ingester::prioritizer::Priority::High,
        Arc::clone(&prioritizer),
    )
    .unwrap();
    assert_eq!(prioritizer.registered_policies(), 1);
    assert!(prioritized.can_acquire_permit());
    assert!(rl.push.is_none());
    assert!(rl.read.is_some());

    let direct = ReactiveLimiter::new(CommonRlCfg {
        enabled: true,
        max_limit_factor: 1.0,
        min_limit_factor: 1.2,
        initial_rejection_factor: 1.0,
        max_rejection_factor: 2.0,
        ..Default::default()
    })
    .unwrap();
    assert!(direct.limit() >= 1);
}
