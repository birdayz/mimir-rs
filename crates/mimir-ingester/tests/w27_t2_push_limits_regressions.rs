mod common;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use common::default_ingester_test_config;
use mimir_common::services::State;
use mimir_config::{LimitsConfig, Overrides};
use mimir_ingester::errors::IngesterError;
use mimir_ingester::ingest_storage::ingester_err_to_pusher_err;
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb::extract_cortex_error_details;
use mimir_proto::prost::Message;
use mimir_proto::zerocopy::{decode_write_request, DecodeOptions, WriteRequestScratch};
use tempfile::TempDir;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn new_ingester_with_ingest_storage(
    dir: &std::path::Path,
    ingest_enabled: bool,
    limits: LimitsConfig,
) -> Arc<Ingester> {
    let (mut cfg, blocks_cfg, mut ingest_cfg) = default_ingester_test_config(dir);
    cfg.ring.instance_id = "ingester-0".to_string();
    ingest_cfg.enabled = ingest_enabled;
    Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(limits),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("create ingester")
}

// ===========================================================================
// W27-A-03 & W27-A-04: BadRequest classification & bare PusherError formatting
// ===========================================================================
#[test]
fn w27_a_03_and_04_bad_request_maps_to_server_error_and_bare_message() {
    let err = IngesterError::BadRequest {
        message: "invalid RW2 frame".to_string(),
    };

    // Go errors.go:599-625: non-ingesterError maps to codes.Internal without ErrorDetails.
    let status = err.map_push_error_to_status();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "invalid RW2 frame");
    assert!(extract_cortex_error_details(&status).is_none());

    let wrapped_status = err.clone().with_do_not_log().map_push_error_to_grpc_server_status();
    assert_eq!(wrapped_status.code(), tonic::Code::Internal);
    assert!(extract_cortex_error_details(&wrapped_status).is_none());

    // HTTP-status table stays 400.
    assert_eq!(err.http_status_code(), 400);

    // ingest_storage: BadRequest has no ErrorDetails -> cause: None -> server error, bare message.
    let pusher_err = ingester_err_to_pusher_err(err);
    assert_eq!(pusher_err.cause, None);
    assert!(!pusher_err.is_client_error());
    assert_eq!(pusher_err.grpc_code.as_deref(), Some("Internal"));
    assert_eq!(pusher_err.to_string(), "invalid RW2 frame");

    // Client error with ErrorDetails also displays bare message (no "rpc error: code = ... desc = ").
    let client_err = IngesterError::LabelsNotSorted {
        series: r#"{b="2", a="1"}"#.to_string(),
    }
    .with_user("t1", 1);
    let client_pusher_err = ingester_err_to_pusher_err(client_err);
    assert!(client_pusher_err.is_client_error());
    assert!(!client_pusher_err.to_string().contains("rpc error: code ="));
    assert!(client_pusher_err.to_string().starts_with("user=t1: "));
}

// ===========================================================================
// W27-A-04: push_to_storage / push_borrowed_to_storage wrap start_push_request_sync
// errors in DoNotLog when ingest_storage is enabled
// ===========================================================================
#[test]
fn w27_a_04_push_storage_wraps_admission_errors_with_do_not_log_when_ingest_storage_enabled() {
    let ts = now_ms();
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "m_do_not_log")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let wire = req.encode_to_vec();

    // 1. ingest_storage enabled -> State::New Unavailable is wrapped in DoNotLog.
    let dir_enabled = TempDir::new().unwrap();
    let ing_enabled = new_ingester_with_ingest_storage(
        dir_enabled.path(),
        true,
        LimitsConfig::default(),
    );

    let err_owned = ing_enabled.push_to_storage("t1", &req).unwrap_err();
    assert!(err_owned.is_do_not_log(), "expected DoNotLog, got {err_owned:?}");
    assert!(matches!(err_owned, IngesterError::DoNotLog { .. }));
    assert_eq!(err_owned.map_push_error_to_status().code(), tonic::Code::Unavailable);

    let mut scratch = WriteRequestScratch::new();
    let borrowed = decode_write_request(&wire, DecodeOptions::classic(), &mut scratch).unwrap();
    let err_borrowed = ing_enabled
        .push_borrowed_to_storage("t1", &borrowed, wire.len() as i64)
        .unwrap_err();
    assert!(err_borrowed.is_do_not_log(), "expected DoNotLog, got {err_borrowed:?}");
    assert!(matches!(err_borrowed, IngesterError::DoNotLog { .. }));
    assert_eq!(err_borrowed.map_push_error_to_status().code(), tonic::Code::Unavailable);

    // 2. ingest_storage disabled -> State::New Unavailable is NOT wrapped in DoNotLog.
    let dir_disabled = TempDir::new().unwrap();
    let ing_disabled = new_ingester_with_ingest_storage(
        dir_disabled.path(),
        false,
        LimitsConfig::default(),
    );
    let err_disabled = ing_disabled.push_to_storage("t1", &req).unwrap_err();
    assert!(!err_disabled.is_do_not_log(), "unexpected DoNotLog: {err_disabled:?}");
    let err_borrowed_disabled = ing_disabled
        .push_borrowed_to_storage("t1", &borrowed, wire.len() as i64)
        .unwrap_err();
    assert!(
        !err_borrowed_disabled.is_do_not_log(),
        "unexpected DoNotLog: {err_borrowed_disabled:?}"
    );
}

// ===========================================================================
// W27-A-06: Exemplar validation checks duplicate label names BEFORE 128-rune limit
// ===========================================================================
#[test]
fn w27_a_06_exemplar_duplicate_label_checked_before_128_rune_limit() {
    let dir = TempDir::new().unwrap();
    let limits = LimitsConfig {
        max_global_exemplars_per_user: 10,
        ..Default::default()
    };
    let ing = new_ingester_with_ingest_storage(dir.path(), false, limits);
    ing.set_state_for_test(State::Running);

    let ts = now_ms();
    let long_val = "x".repeat(130);

    // Case 1: {a="<130 chars>", b="1", b="2"} -> duplicate "b" comes after over-limit "a".
    let req_dup_b = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_dup_order_1")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![
                    cortexpb::LabelPair::from_strs("a", &long_val),
                    cortexpb::LabelPair::from_strs("b", "1"),
                    cortexpb::LabelPair::from_strs("b", "2"),
                ],
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let err1 = ing.push_to_storage("t1", &req_dup_b).unwrap_err();
    let msg1 = err1.to_string();
    assert!(
        msg1.contains(r#"label name "b" is not unique: invalid exemplar"#),
        "expected duplicate label error before rune-length error, got: {msg1}"
    );

    // Same on zero-copy BorrowedWriteRequest path.
    let wire1 = req_dup_b.encode_to_vec();
    let mut scratch = WriteRequestScratch::new();
    let borrowed1 = decode_write_request(&wire1, DecodeOptions::classic(), &mut scratch).unwrap();
    let err1_b = ing
        .push_borrowed_to_storage("t1", &borrowed1, wire1.len() as i64)
        .unwrap_err();
    assert!(
        err1_b
            .to_string()
            .contains(r#"label name "b" is not unique: invalid exemplar"#),
        "borrowed path got: {err1_b}"
    );

    // Case 2: {a="<130 chars>", a="1"} -> duplicate "a" itself.
    let req_dup_a = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_dup_order_2")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![
                    cortexpb::LabelPair::from_strs("a", &long_val),
                    cortexpb::LabelPair::from_strs("a", "1"),
                ],
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let err2 = ing.push_to_storage("t1", &req_dup_a).unwrap_err();
    assert!(
        err2.to_string()
            .contains(r#"label name "a" is not unique: invalid exemplar"#),
        "got: {err2}"
    );

    // Case 3: {a="<130 chars>", b="1"} (no duplicates) -> still returns 128-rune limit error.
    let req_len_only = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_dup_order_3")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![
                    cortexpb::LabelPair::from_strs("a", &long_val),
                    cortexpb::LabelPair::from_strs("b", "1"),
                ],
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let err3 = ing.push_to_storage("t1", &req_len_only).unwrap_err();
    assert!(
        err3.to_string()
            .contains("label length for exemplar exceeds maximum of 128 UTF-8 characters"),
        "got: {err3}"
    );
}
