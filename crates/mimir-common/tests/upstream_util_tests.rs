//! 1:1 upstream-ported tests for `mimir-common` (`activitytracker`, `globalerror`, `shutdownmarker`).

use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, SystemTime};

use mimir_common::activitytracker::{
    load_unfinished_entries, trim_entry_to_size, ActivityTracker, ActivityTrackerConfig,
    ActivityTrackerExt,
};
use mimir_common::errors::{
    error_has_context_error, extract_status_from_error_chain,
    wrap_do_not_log_error_with_grpc_status, wrap_error_with_grpc_status,
    wrap_grpc_error_with_context_error, Code, ContextError, DoNotLogError, ErrorCause,
    ErrorDetails, ErrorWithStatus, Status, GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR,
    MISSING_METRIC_NAME,
};
use mimir_common::shutdownmarker::{create, exists, get_path, remove, SHUTDOWN_MARKER_FILENAME};
use mimir_proto::grpcutilpb::{extract_cortex_error_details, extract_http_response};
use prometheus::{Encoder, Registry, TextEncoder};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

// ============================================================================
// pkg/util/activitytracker/activity_tracker_test.go
// ============================================================================

fn check_activities_in_file(
    file: &std::path::Path,
    expected_activities: &[String],
    reference_time: SystemTime,
    time_delta: Duration,
) {
    let entries = load_unfinished_entries(file).unwrap();
    let mut activities = Vec::with_capacity(entries.len());
    for entry in &entries {
        let diff = entry
            .timestamp
            .duration_since(reference_time)
            .or_else(|_| reference_time.duration_since(entry.timestamp))
            .unwrap();
        assert!(
            diff <= time_delta,
            "timestamp {:?} not within {:?} of {:?}",
            entry.timestamp,
            time_delta,
            reference_time
        );
        activities.push(entry.activity.clone());
    }
    let mut sorted_expected = expected_activities.to_vec();
    sorted_expected.sort();
    activities.sort();
    assert_eq!(activities, sorted_expected);
}

// Upstream: pkg/util/activitytracker/activity_tracker_test.go::TestActivityTracker
#[test]
fn test_activity_tracker() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("activity");

    const MAX_ENTRIES: usize = 5;
    let tr = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: file.clone(),
            max_entries: MAX_ENTRIES,
        },
        None,
    )
    .unwrap()
    .unwrap();

    let now = SystemTime::now();

    let mut inserted_activities = Vec::new();
    for i in 0..MAX_ENTRIES {
        let a = format!("activity-{i}");
        let aix = tr.insert_static(&a);
        assert!(aix >= 0 && (aix as usize) < MAX_ENTRIES);
        inserted_activities.push(a);
        assert_eq!(tr.free_slots_count(), (MAX_ENTRIES - i - 1) as f64);
    }

    check_activities_in_file(&file, &inserted_activities, now, Duration::from_secs(5));

    // Test inserting another activity that doesn't fit anymore.
    assert_eq!(tr.failed_full_count(), 0.0);
    let aix = tr.insert_static("extra");
    assert!(aix < 0);
    assert_eq!(tr.failed_full_count(), 1.0);

    // Test delete and verify that other entries are still there.
    for i in 0..MAX_ENTRIES {
        tr.delete(i as isize);
        inserted_activities.remove(0);
        check_activities_in_file(&file, &inserted_activities, now, Duration::from_secs(5));
        assert_eq!(tr.free_slots_count(), (i + 1) as f64);
    }

    // Test inserting empty activity.
    assert_eq!(tr.failed_empty_count(), 0.0);
    let aix = tr.insert_static("");
    assert!(aix < 0);
    assert_eq!(tr.failed_empty_count(), 1.0);

    tr.close().unwrap();
}

// Upstream: pkg/util/activitytracker/activity_tracker_test.go::TestNilActivityTracker
#[test]
fn test_nil_activity_tracker() {
    let tr = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: PathBuf::new(),
            max_entries: 5,
        },
        None,
    )
    .unwrap();
    assert!(tr.is_none());

    let ix = tr.insert_static_or_noop("test");
    assert_eq!(ix, -1);
    tr.delete_or_noop(ix);
    tr.close_or_noop().unwrap();
}

// Upstream: pkg/util/activitytracker/activity_tracker_test.go::TestLoadedActivitiesOnStartupMetric
#[test]
fn test_loaded_activities_on_startup_metric() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("activity");

    // First tracker: insert 3 activities without deleting, simulating a crash.
    let tr1 = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: file.clone(),
            max_entries: 5,
        },
        None,
    )
    .unwrap()
    .unwrap();
    tr1.insert_static("activity-1");
    tr1.insert_static("activity-2");
    tr1.insert_static("activity-3");
    tr1.close().unwrap();

    // Second tracker on the same file: should report 3 loaded activities.
    let reg = Registry::new();
    let tr2 = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: file,
            max_entries: 5,
        },
        Some(&reg),
    )
    .unwrap()
    .unwrap();

    assert_eq!(tr2.loaded_activities_on_startup_count(), 3.0);

    let mut buf = Vec::new();
    TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("# HELP activity_tracker_unfinished_activities_loaded Number of unfinished activities loaded from the activity tracker file on startup."));
    assert!(text.contains("# TYPE activity_tracker_unfinished_activities_loaded gauge"));
    assert!(text.contains("activity_tracker_unfinished_activities_loaded 3"));

    tr2.close().unwrap();
}

// Upstream: pkg/util/activitytracker/activity_tracker_test.go::TestLoadedActivitiesOnStartupMetricFirstRun
#[test]
fn test_loaded_activities_on_startup_metric_first_run() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("activity");

    let reg = Registry::new();
    let tr = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: file,
            max_entries: 5,
        },
        Some(&reg),
    )
    .unwrap()
    .unwrap();

    assert_eq!(tr.loaded_activities_on_startup_count(), 0.0);
    tr.close().unwrap();
}

// Upstream: pkg/util/activitytracker/activity_tracker_test.go::TestTrimEntrySize
#[test]
fn test_trim_entry_size() {
    let cases = [
        ("hello world", 100usize, "hello world"),
        ("hello world", 10, "hello worl"),
        ("hello world", 5, "hello"),
        ("\u{1f389}\u{1f384}\u{1f385}", 5, "\u{1f389}"),
        (
            "\u{1f389}\u{1f384}\u{1f385}",
            8,
            "\u{1f389}\u{1f384}",
        ),
        (
            "\u{1f389}\u{1f384}\u{1f385}",
            9,
            "\u{1f389}\u{1f384}",
        ),
        (
            "\u{1f389}\u{1f384}\u{1f385}",
            10,
            "\u{1f389}\u{1f384}",
        ),
        (
            "\u{1f389}\u{1f384}\u{1f385}",
            11,
            "\u{1f389}\u{1f384}",
        ),
        (
            "\u{1f389}\u{1f384}\u{1f385}",
            12,
            "\u{1f389}\u{1f384}\u{1f385}",
        ),
    ];

    for (input, size, expected) in cases {
        assert_eq!(
            trim_entry_to_size(input, size),
            expected,
            "input={input:?}, size={size}"
        );
    }
}

// ============================================================================
// pkg/util/globalerror/grpc_test.go
// ============================================================================

#[derive(Debug)]
struct SimpleErr(&'static str);

impl fmt::Display for SimpleErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for SimpleErr {}

#[derive(Debug)]
struct CustomWrappedStatus {
    msg: &'static str,
    inner: Status,
}

impl fmt::Display for CustomWrappedStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.msg, self.inner)
    }
}

impl StdError for CustomWrappedStatus {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.inner)
    }
}

// Upstream: pkg/util/globalerror/grpc_test.go::TestWrapContextError
#[test]
fn test_wrap_context_error() {
    // Subtest 1: should wrap gRPC context errors
    enum OrigErr {
        Raw(Status),
        Wrapped(CustomWrappedStatus),
        WithStatus(ErrorWithStatus),
    }

    struct Case {
        name: &'static str,
        ctx_err: Option<ContextError>,
        orig_err: OrigErr,
        expected_code: Code,
        expected_ctx_err: Option<ContextError>,
    }

    let cases = vec![
        Case {
            name: "gRPC Canceled error",
            ctx_err: Some(ContextError::Canceled),
            orig_err: OrigErr::Raw(Status::cancelled("context canceled")),
            expected_code: Code::Cancelled,
            expected_ctx_err: Some(ContextError::Canceled),
        },
        Case {
            name: "wrapped gRPC Canceled error",
            ctx_err: Some(ContextError::Canceled),
            orig_err: OrigErr::Wrapped(CustomWrappedStatus {
                msg: "custom message",
                inner: Status::cancelled("context canceled"),
            }),
            expected_code: Code::Cancelled,
            expected_ctx_err: Some(ContextError::Canceled),
        },
        Case {
            name: "gRPC DeadlineExceeded error",
            ctx_err: Some(ContextError::DeadlineExceeded),
            orig_err: OrigErr::Raw(Status::deadline_exceeded("context deadline exceeded")),
            expected_code: Code::DeadlineExceeded,
            expected_ctx_err: Some(ContextError::DeadlineExceeded),
        },
        Case {
            name: "wrapped gRPC DeadlineExceeded error",
            ctx_err: Some(ContextError::DeadlineExceeded),
            orig_err: OrigErr::Wrapped(CustomWrappedStatus {
                msg: "custom message",
                inner: Status::deadline_exceeded("context deadline exceeded"),
            }),
            expected_code: Code::DeadlineExceeded,
            expected_ctx_err: Some(ContextError::DeadlineExceeded),
        },
        Case {
            name: "ErrorWithStatus with Canceled status and with canceled context",
            ctx_err: Some(ContextError::Canceled),
            orig_err: OrigErr::WithStatus(wrap_error_with_grpc_status(
                SimpleErr("cancel error"),
                Code::Cancelled,
                None,
            )),
            expected_code: Code::Cancelled,
            expected_ctx_err: Some(ContextError::Canceled),
        },
        Case {
            name: "ErrorWithStatus with Canceled status and without canceled context",
            ctx_err: None,
            orig_err: OrigErr::WithStatus(wrap_error_with_grpc_status(
                SimpleErr("cancel error"),
                Code::Cancelled,
                None,
            )),
            expected_code: Code::Cancelled,
            expected_ctx_err: None,
        },
        Case {
            name: "ErrorWithStatus with DeadlineExceeded status and with deadline exceeded",
            ctx_err: Some(ContextError::DeadlineExceeded),
            orig_err: OrigErr::WithStatus(wrap_error_with_grpc_status(
                SimpleErr("timeout error"),
                Code::DeadlineExceeded,
                None,
            )),
            expected_code: Code::DeadlineExceeded,
            expected_ctx_err: Some(ContextError::DeadlineExceeded),
        },
        Case {
            name: "ErrorWithStatus with DeadlineExceeded status and without deadline exceeded",
            ctx_err: None,
            orig_err: OrigErr::WithStatus(wrap_error_with_grpc_status(
                SimpleErr("timeout error"),
                Code::DeadlineExceeded,
                None,
            )),
            expected_code: Code::DeadlineExceeded,
            expected_ctx_err: None,
        },
        Case {
            name: "grpc: the client connection is closing",
            ctx_err: None,
            orig_err: OrigErr::Raw(Status::cancelled(GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR)),
            expected_code: Code::Cancelled,
            expected_ctx_err: None,
        },
        Case {
            name: "grpc: the client connection is closing even with canceled context",
            ctx_err: Some(ContextError::Canceled),
            orig_err: OrigErr::Raw(Status::cancelled(GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR)),
            expected_code: Code::Cancelled,
            expected_ctx_err: None,
        },
    ];

    for tc in cases {
        let wrapped_dyn: Box<dyn StdError> = match tc.orig_err {
            OrigErr::Raw(s) => match wrap_grpc_error_with_context_error(tc.ctx_err, s) {
                Ok(orig) => Box::new(orig),
                Err(ews) => Box::new(ews),
            },
            OrigErr::Wrapped(ws) => {
                match wrap_grpc_error_with_context_error(tc.ctx_err, ws) {
                    Ok(orig) => Box::new(orig),
                    Err(ews) => Box::new(ews),
                }
            }
            OrigErr::WithStatus(ews) => {
                match wrap_grpc_error_with_context_error(tc.ctx_err, ews) {
                    Ok(orig) => Box::new(orig),
                    Err(ews) => Box::new(ews),
                }
            }
        };

        if let Some(expected_ctx) = tc.expected_ctx_err {
            assert!(
                error_has_context_error(wrapped_dyn.as_ref(), expected_ctx),
                "case {}: expected context error {:?}",
                tc.name,
                expected_ctx
            );
            assert!(
                wrapped_dyn.source().is_some(),
                "case {}: wrapped error must preserve original error as source",
                tc.name
            );
        } else {
            assert!(
                !error_has_context_error(wrapped_dyn.as_ref(), ContextError::Canceled),
                "case {}: unexpected Canceled",
                tc.name
            );
            assert!(
                !error_has_context_error(wrapped_dyn.as_ref(), ContextError::DeadlineExceeded),
                "case {}: unexpected DeadlineExceeded",
                tc.name
            );
        }

        let extracted = extract_status_from_error_chain(wrapped_dyn.as_ref())
            .unwrap_or_else(|| panic!("case {}: missing gRPC status", tc.name));
        assert_eq!(extracted.code(), tc.expected_code, "case {}", tc.name);
    }

    // Subtest 2: should return the input error on a non-gRPC error
    let orig = SimpleErr("mock error");
    let res = wrap_grpc_error_with_context_error(None, orig).unwrap();
    assert_eq!(res.to_string(), "mock error");

    let res = wrap_grpc_error_with_context_error(None, ContextError::Canceled).unwrap();
    assert_eq!(res, ContextError::Canceled);

    let res = wrap_grpc_error_with_context_error(None, ContextError::DeadlineExceeded).unwrap();
    assert_eq!(res, ContextError::DeadlineExceeded);

    let eof = io::Error::new(io::ErrorKind::UnexpectedEof, "EOF");
    let res = wrap_grpc_error_with_context_error(None, eof).unwrap();
    assert_eq!(res.kind(), io::ErrorKind::UnexpectedEof);
}

fn check_error_with_status_details(status: &Status, expected: Option<ErrorDetails>) {
    let extracted = extract_cortex_error_details(status);
    match expected {
        None => assert!(extracted.is_none(), "expected no details, got {extracted:?}"),
        Some(exp) => {
            let det = extracted.expect("expected cortexpb.ErrorDetails");
            assert_eq!(det.cause, exp.cause as i32);
            assert_eq!(det.soft, exp.soft);
            assert_eq!(det.rejected_samples, exp.rejected_samples);
        }
    }
}

// Upstream: pkg/util/globalerror/grpc_test.go::TestWrapErrorWithGRPCStatus
#[test]
fn test_wrap_error_with_grpc_status() {
    let generic_err_msg = "this is an error";

    let bad_data = ErrorDetails {
        cause: ErrorCause::BadData,
        soft: false,
        rejected_samples: 0,
    };

    struct Case {
        name: &'static str,
        details: Option<ErrorDetails>,
        do_not_log: bool,
        expected_msg: &'static str,
    }

    let cases = [
        Case {
            name: "new ErrorWithStatus backed by genericErr contains ErrorDetails",
            details: Some(bad_data),
            do_not_log: false,
            expected_msg: generic_err_msg,
        },
        Case {
            name: "new ErrorWithStatus backed by DoNotLog error contains ErrorDetails",
            details: Some(bad_data),
            do_not_log: true,
            expected_msg: generic_err_msg,
        },
        Case {
            name: "new ErrorWithStatus without ErrorDetails backed by DoNotLog error",
            details: None,
            do_not_log: true,
            expected_msg: generic_err_msg,
        },
        Case {
            name: "new ErrorWithStatus without ErrorDetails",
            details: None,
            do_not_log: false,
            expected_msg: generic_err_msg,
        },
    ];

    for tc in cases {
        let status_code = Code::Unimplemented;
        let err_with_status = if tc.do_not_log {
            wrap_do_not_log_error_with_grpc_status(
                DoNotLogError(SimpleErr(generic_err_msg)),
                status_code,
                tc.details,
            )
        } else {
            wrap_error_with_grpc_status(SimpleErr(generic_err_msg), status_code, tc.details)
        };

        assert_eq!(err_with_status.to_string(), tc.expected_msg, "{}", tc.name);
        assert!(err_with_status.underlying().is_some(), "{}", tc.name);
        assert_eq!(
            err_with_status.underlying().unwrap().to_string(),
            generic_err_msg
        );

        let stat = err_with_status.to_tonic_status();
        assert_eq!(stat.code(), status_code, "{}", tc.name);
        assert_eq!(stat.message(), tc.expected_msg, "{}", tc.name);
        check_error_with_status_details(&stat, tc.details);

        // Ensure httpgrpc's extract_http_response does not recognize it.
        assert!(extract_http_response(&stat).is_none(), "{}", tc.name);

        if tc.do_not_log {
            assert!(!err_with_status.should_log(), "{}", tc.name);
        } else {
            assert!(err_with_status.should_log(), "{}", tc.name);
        }
    }
}

// Upstream: pkg/util/globalerror/grpc_test.go::TestErrorWithStatus_Err
#[test]
fn test_error_with_status_err() {
    let generic_err_msg = "this is an error";
    let bad_data = ErrorDetails {
        cause: ErrorCause::BadData,
        soft: false,
        rejected_samples: 0,
    };

    for (do_not_log, details) in [
        (false, Some(bad_data)),
        (true, Some(bad_data)),
        (true, None),
        (false, None),
    ] {
        let err_with_status = if do_not_log {
            wrap_do_not_log_error_with_grpc_status(
                DoNotLogError(SimpleErr(generic_err_msg)),
                Code::Unimplemented,
                details,
            )
        } else {
            wrap_error_with_grpc_status(SimpleErr(generic_err_msg), Code::Unimplemented, details)
        };

        let status = err_with_status.err().expect("expected non-None Status");
        assert_eq!(status.code(), Code::Unimplemented);
        assert_eq!(status.message(), generic_err_msg);
        // Immutable Status does not wrap the original Rust error chain.
        assert!(StdError::source(&status).is_none());
        check_error_with_status_details(&status, details);
        assert!(extract_http_response(&status).is_none());
    }

    // OK status or missing underlying returns None.
    let ok_ews = wrap_error_with_grpc_status(SimpleErr("ok"), Code::Ok, None);
    assert!(ok_ews.err().is_none());
    let no_underlying = ErrorWithStatus::new("msg", Code::Internal, None);
    assert!(no_underlying.err().is_none());
}

#[derive(Clone, Default)]
struct MockGrpcSvc;

impl tower::Service<http::Request<tonic::body::BoxBody>> for MockGrpcSvc {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        Box::pin(async move {
            struct SucceedSvc;
            impl tonic::server::UnaryService<()> for SucceedSvc {
                type Response = ();
                type Future = std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = Result<tonic::Response<()>, Status>>
                            + Send,
                    >,
                >;
                fn call(&mut self, _request: tonic::Request<()>) -> Self::Future {
                    Box::pin(async { Ok(tonic::Response::new(())) })
                }
            }
            let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
            Ok(grpc.unary(SucceedSvc, req).await)
        })
    }
}

impl tonic::server::NamedService for MockGrpcSvc {
    const NAME: &'static str = "dskit.FakeServer";
}

// Upstream: pkg/util/globalerror/grpc_test.go::TestGRPCClientClosingConnectionError_IsNotContextCanceled
#[tokio::test]
async fn test_grpc_client_closing_connection_error_is_not_context_canceled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let incoming = TcpListenerStream::new(listener);
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(MockGrpcSvc)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let endpoint = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();

    // Call succeeds when server & connection are open.
    let mut client = tonic::client::Grpc::new(channel.clone());
    client.ready().await.unwrap();
    let req = tonic::Request::new(());
    let res: Result<tonic::Response<()>, Status> = client
        .unary(
            req,
            http::uri::PathAndQuery::from_static("/dskit.FakeServer/Succeed"),
            tonic::codec::ProstCodec::default(),
        )
        .await;
    assert!(res.is_ok(), "initial RPC should succeed: {res:?}");

    // Shut down the server / close the connection.
    let _ = shutdown_tx.send(());
    let _ = server_handle.await;

    // Wait briefly for the transport close to propagate, then invoke again.
    let mut err_opt = None;
    for _ in 0..20 {
        let mut c = tonic::client::Grpc::new(channel.clone());
        if let Err(e) = c.ready().await {
            err_opt = Some(Status::unavailable(e.to_string()));
            break;
        }
        match c
            .unary::<(), (), _>(
                tonic::Request::new(()),
                http::uri::PathAndQuery::from_static("/dskit.FakeServer/Succeed"),
                tonic::codec::ProstCodec::default(),
            )
            .await
        {
            Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(status) => {
                err_opt = Some(status);
                break;
            }
        }
    }
    let closed_err = err_opt.expect("call on closed connection must fail");
    let wrapped = wrap_grpc_error_with_context_error(Some(ContextError::Canceled), closed_err);
    let err_ref: &dyn StdError = match &wrapped {
        Ok(s) => s,
        Err(ews) => ews,
    };
    assert!(!error_has_context_error(err_ref, ContextError::Canceled));

    // Also explicitly verify the exact Go gRPC closing connection error string is never wrapped as ContextError::Canceled.
    let closing_status = Status::cancelled(GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR);
    let kept = wrap_grpc_error_with_context_error(Some(ContextError::Canceled), closing_status)
        .expect("closing connection status must not be wrapped as ContextError::Canceled");
    assert_eq!(kept.code(), Code::Cancelled);
    assert_eq!(kept.message(), GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR);
}

// ============================================================================
// pkg/util/globalerror/user_test.go
// ============================================================================

// Upstream: pkg/util/globalerror/user_test.go::TestID_Message
#[test]
fn test_id_message() {
    assert_eq!(
        "an error (err-mimir-missing-metric-name)",
        MISSING_METRIC_NAME.message("an error")
    );
}

// Upstream: pkg/util/globalerror/user_test.go::TestID_MessageWithPerInstanceLimitConfig
#[test]
fn test_id_message_with_per_instance_limit_config() {
    let cases = [
        (
            "an error (err-mimir-missing-metric-name). To adjust the related limit, configure -my-flag1, or contact your service administrator.",
            MISSING_METRIC_NAME.message_with_per_instance_limit_config("an error", "my-flag1", &[]),
        ),
        (
            "an error (err-mimir-missing-metric-name). To adjust the related limits, configure -my-flag1 and -my-flag2, or contact your service administrator.",
            MISSING_METRIC_NAME.message_with_per_instance_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2"],
            ),
        ),
        (
            "an error (err-mimir-missing-metric-name). To adjust the related limits, configure -my-flag1, -my-flag2 and -my-flag3, or contact your service administrator.",
            MISSING_METRIC_NAME.message_with_per_instance_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2", "my-flag3"],
            ),
        ),
    ];

    for (expected, actual) in cases {
        assert_eq!(expected, actual);
    }
}

// Upstream: pkg/util/globalerror/user_test.go::TestID_MessageWithPerTenantLimitConfig
#[test]
fn test_id_message_with_per_tenant_limit_config() {
    let cases = [
        (
            "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limit, configure -my-flag1, or contact your service administrator.",
            MISSING_METRIC_NAME.message_with_per_tenant_limit_config("an error", "my-flag1", &[]),
        ),
        (
            "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limits, configure -my-flag1 and -my-flag2, or contact your service administrator.",
            MISSING_METRIC_NAME.message_with_per_tenant_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2"],
            ),
        ),
        (
            "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limits, configure -my-flag1, -my-flag2 and -my-flag3, or contact your service administrator.",
            MISSING_METRIC_NAME.message_with_per_tenant_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2", "my-flag3"],
            ),
        ),
    ];

    for (expected, actual) in cases {
        assert_eq!(expected, actual);
    }
}

// ============================================================================
// pkg/util/shutdownmarker/shutdown_marker_test.go
// ============================================================================

// Upstream: pkg/util/shutdownmarker/shutdown_marker_test.go::TestShutdownMarker_GetPath
#[test]
fn test_shutdown_marker_get_path() {
    let dir = PathBuf::from("/a/b/c");
    let expected = dir.join(SHUTDOWN_MARKER_FILENAME);
    assert_eq!(expected, get_path(&dir));
}

// Upstream: pkg/util/shutdownmarker/shutdown_marker_test.go::TestShutdownMarker_Create
#[test]
fn test_shutdown_marker_create() {
    let dir = tempdir().unwrap();
    let shutdown_marker_path = get_path(dir.path());
    assert!(!exists(&shutdown_marker_path).unwrap());

    create(&shutdown_marker_path).unwrap();
    assert!(exists(&shutdown_marker_path).unwrap());
}

// Upstream: pkg/util/shutdownmarker/shutdown_marker_test.go::TestShutdownMarker_Remove
#[test]
fn test_shutdown_marker_remove() {
    let dir = tempdir().unwrap();
    let shutdown_marker_path = get_path(dir.path());
    assert!(!exists(&shutdown_marker_path).unwrap());

    create(&shutdown_marker_path).unwrap();
    assert!(exists(&shutdown_marker_path).unwrap());

    remove(&shutdown_marker_path).unwrap();
    assert!(!exists(&shutdown_marker_path).unwrap());
}

// Upstream: pkg/util/shutdownmarker/shutdown_marker_test.go::TestShutdownMarker_Remove_WhenParentDirectoryDoesNotExist
#[test]
fn test_shutdown_marker_remove_when_parent_directory_does_not_exist() {
    let dir = tempdir().unwrap();
    let shutdown_marker_path = get_path(dir.path().join("missing"));
    remove(&shutdown_marker_path).unwrap();
}

// ============================================================================
// pkg/util/atomicfs/fsync_test.go
// ============================================================================

// Upstream: pkg/util/atomicfs/fsync_test.go::TestCreateFile
#[test]
fn test_atomicfs_create_file() {
    use std::io::Cursor;

    let dir = tempdir().unwrap();
    let path = dir.path().join("TestCreateFile");
    mimir_common::atomicfs::create_file(&path, Cursor::new(b"test")).unwrap();

    // Ensure the temporary file created by create_file has been removed.
    let tmp_err = std::fs::metadata(mimir_common::atomicfs::temp_path(&path)).unwrap_err();
    assert_eq!(tmp_err.kind(), io::ErrorKind::NotFound);

    // Ensure the directory entry for the file exists.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(entries.iter().any(|n| n == "TestCreateFile"));

    // Check the contents of the file.
    let contents = std::fs::read_to_string(&path).unwrap();
    assert_eq!(contents, "test");
}

// Upstream: pkg/util/atomicfs/fsync_test.go::TestCreate
#[test]
fn test_atomicfs_create() {
    // success
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("TestCreate");
        let mut f = mimir_common::atomicfs::create(&path).unwrap();
        assert_eq!(f.write_str("test").unwrap(), 4);
        f.close().unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(entries.iter().any(|n| n == "TestCreate"));

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "test");
    }

    // duplicate close
    {
        let dir = tempdir().unwrap();
        let path = dir.path().join("TestCreate");
        let mut f = mimir_common::atomicfs::create(&path).unwrap();
        assert_eq!(f.write_str("test").unwrap(), 4);
        f.close().unwrap();

        let dup_err = f.close().unwrap_err();
        assert!(mimir_common::atomicfs::is_closed_err(&dup_err));

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "test");
    }
}

// ============================================================================
// pkg/util/log/rate_limit_test.go
// ============================================================================

type LoggedCall = (mimir_common::logging::LogLevel, Vec<(String, String)>);

#[derive(Default)]
struct RecordingLogger {
    calls: parking_lot::Mutex<Vec<LoggedCall>>,
}

impl RecordingLogger {
    fn count(&self) -> usize {
        self.calls.lock().len()
    }

    fn take(&self) -> Vec<LoggedCall> {
        std::mem::take(&mut *self.calls.lock())
    }
}

impl mimir_common::logging::Logger for RecordingLogger {
    fn log(&self, level: mimir_common::logging::LogLevel, kvs: &[(&str, &str)]) {
        self.calls.lock().push((
            level,
            kvs.iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        ));
    }
}

// Upstream: pkg/util/log/rate_limit_test.go::TestRateLimitedLogger
#[test]
fn test_rate_limited_logger() {
    use std::sync::Arc;
    use std::time::Instant;

    let limit_interval = Duration::from_secs(1);
    let test_duration = Duration::from_secs(10);
    let attempted_calls = 1000u32;
    let expected_actual_calls = 10usize;

    let test_start = Instant::now();
    let fake_now = Arc::new(parking_lot::Mutex::new(test_start));
    let clock = {
        let fake_now = Arc::clone(&fake_now);
        Arc::new(move || *fake_now.lock())
    };

    let mock_logger = Arc::new(RecordingLogger::default());
    let rl = mimir_common::logging::IntervalRateLimitedLogger::new_with_clock(
        limit_interval,
        mock_logger.clone(),
        clock,
    );

    let time_step = test_duration / attempted_calls;
    let test_end = test_start + test_duration + Duration::from_nanos(1);
    while *fake_now.lock() < test_end {
        mimir_common::logging::Logger::log(&rl, mimir_common::logging::LogLevel::Info, &[]);
        *fake_now.lock() += time_step;
    }

    assert_eq!(mock_logger.count(), expected_actual_calls);
}

// Upstream: pkg/util/log/rate_limit_test.go::TestRateLimitedLogger_Burst
#[test]
fn test_rate_limited_logger_burst() {
    use std::sync::Arc;
    use std::time::Instant;

    let limit_interval = Duration::from_secs(1);
    let expected_actual_calls = 2usize;

    let test_start = Instant::now();
    let fake_now = Arc::new(parking_lot::Mutex::new(test_start));
    let clock = {
        let fake_now = Arc::clone(&fake_now);
        Arc::new(move || *fake_now.lock())
    };

    let mock_logger = Arc::new(RecordingLogger::default());
    let rl = mimir_common::logging::IntervalRateLimitedLogger::new_with_clock(
        limit_interval,
        mock_logger.clone(),
        clock,
    );

    *fake_now.lock() = test_start + Duration::from_nanos(1);
    mimir_common::logging::Logger::log(&rl, mimir_common::logging::LogLevel::Info, &[]);

    // Only one of these should go through because we don't have a leaky bucket.
    *fake_now.lock() = test_start + limit_interval * 3;
    mimir_common::logging::Logger::log(&rl, mimir_common::logging::LogLevel::Info, &[]);
    mimir_common::logging::Logger::log(&rl, mimir_common::logging::LogLevel::Info, &[]);
    mimir_common::logging::Logger::log(&rl, mimir_common::logging::LogLevel::Info, &[]);

    assert_eq!(mock_logger.count(), expected_actual_calls);
}

// ============================================================================
// pkg/util/log/sampler_test.go
// ============================================================================

#[derive(Debug)]
struct PlainError(String);

impl fmt::Display for PlainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for PlainError {}

// Upstream: pkg/util/log/sampler_test.go::TestSampler_Sample
#[test]
fn test_sampler_sample() {
    const ERROR_SAMPLE_RATE: i64 = 5;
    let sampler = mimir_common::logging::Sampler::new(ERROR_SAMPLE_RATE).unwrap();
    assert!(sampler.sample());
    for _ in 1..ERROR_SAMPLE_RATE {
        assert!(!sampler.sample());
    }
    assert!(sampler.sample());
}

// Upstream: pkg/util/log/sampler_test.go::TestSampledError_Error
#[test]
fn test_sampled_error_error() {
    const ERROR_SAMPLE_RATE: i64 = 5;
    let sampler = mimir_common::logging::Sampler::new(ERROR_SAMPLE_RATE).unwrap();
    let err = PlainError("this is an occurrence of error with id 1".to_string());
    let sampled_err = sampler.wrap_error(err);
    assert_eq!(
        sampled_err.to_string(),
        "this is an occurrence of error with id 1"
    );
}

// Upstream: pkg/util/log/sampler_test.go::TestSampledError_ShouldLog
#[test]
fn test_sampled_error_should_log() {
    use mimir_common::logging::OptionalLogging;

    const ERROR_SAMPLE_RATE: i64 = 5;
    let sampler = mimir_common::logging::Sampler::new(ERROR_SAMPLE_RATE).unwrap();
    let err = PlainError("this is an occurrence of error with id 1".to_string());
    let sampled_err = sampler.wrap_error(err);

    let (should_log, reason) = sampled_err.should_log();
    assert!(should_log);
    assert_eq!(reason, format!("sampled 1/{ERROR_SAMPLE_RATE}"));

    for _ in 1..ERROR_SAMPLE_RATE {
        let (should_log, reason) = sampled_err.should_log();
        assert!(!should_log);
        assert_eq!(reason, format!("sampled 1/{ERROR_SAMPLE_RATE}"));
    }

    let (should_log, reason) = sampled_err.should_log();
    assert!(should_log);
    assert_eq!(reason, format!("sampled 1/{ERROR_SAMPLE_RATE}"));
}

// Upstream: pkg/util/log/sampler_test.go::TestSampledError_ShouldImplementOptionalLoggingInterface
#[test]
fn test_sampled_error_should_implement_optional_logging_interface() {
    use mimir_common::logging::{OptionalLogging, SampledError, Sampler};

    fn assert_optional_logging<T: OptionalLogging + StdError>(_t: &T) {}

    let sampler = Sampler::new(5).unwrap();
    let err = PlainError("this is an occurrence of error with id 1".to_string());
    let sampled_err: SampledError<PlainError> = sampler.wrap_error(err);
    assert_optional_logging(&sampled_err);
}

// Upstream: pkg/util/log/sampler_test.go::TestNilSampler
#[test]
fn test_nil_sampler() {
    use mimir_common::logging::{wrap_error_with_sampler, SampledError, Sampler};

    let sampler = Sampler::new(0);
    assert!(sampler.is_none());
    let wrapped = wrap_error_with_sampler(sampler.as_ref(), PlainError("error".to_string()));
    assert_eq!(wrapped.to_string(), "error");
    assert!(wrapped.downcast_ref::<SampledError<PlainError>>().is_none());
    assert!(wrapped.downcast_ref::<PlainError>().is_some());
}

// ============================================================================
// pkg/util/log/slogadapter_test.go
// ============================================================================

// Upstream: pkg/util/log/slogadapter_test.go::TestSlogFromGoKit
#[test]
fn test_slog_from_gokit() {
    use std::sync::Arc;
    use mimir_common::logging::{slog_from_gokit, LevelFilterLogger, LogLevel};

    let levels = [
        LogLevel::Debug,
        LogLevel::Info,
        LogLevel::Warn,
        LogLevel::Error,
    ];

    // enabled for the right slog levels when go-kit level configured
    for (i, &lvl) in levels.iter().enumerate() {
        let m_logger = Arc::new(RecordingLogger::default());
        let filtered = LevelFilterLogger::new(m_logger.clone(), lvl);
        let slogger = slog_from_gokit(filtered);

        for (j, &sl) in levels.iter().enumerate() {
            if j >= i {
                assert!(slogger.enabled(sl), "expected {sl:?} enabled for {lvl:?}");
            } else {
                assert!(!slogger.enabled(sl), "expected {sl:?} disabled for {lvl:?}");
            }
        }
        assert_eq!(m_logger.count(), 0, "probe should be intercepted by LevelFilterLogger");
    }

    // enabled for the right slog levels when go-kit level not configured
    {
        let m_logger = Arc::new(RecordingLogger::default());
        let slogger = slog_from_gokit(m_logger.clone());
        for &sl in &levels {
            assert!(slogger.enabled(sl), "expected {sl:?} enabled");
        }
        let calls = m_logger.take();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1[0].0, "probe");
    }

    // wraps go-kit logger
    {
        let m_logger = Arc::new(RecordingLogger::default());
        let slogger = slog_from_gokit(m_logger.clone());
        let _ = m_logger.take(); // consume probe call

        slogger.debug("test", &[("attr", "value")]);
        slogger.info("test", &[("attr", "value")]);
        slogger.warn("test", &[("attr", "value")]);
        slogger.error("test", &[("attr", "value")]);

        let calls = m_logger.take();
        assert_eq!(calls.len(), 4);
        for (idx, &expected_lvl) in levels.iter().enumerate() {
            assert_eq!(calls[idx].0, expected_lvl);
            assert_eq!(
                calls[idx].1,
                vec![
                    ("msg".to_string(), "test".to_string()),
                    ("attr".to_string(), "value".to_string()),
                ]
            );
        }
    }
}

// ============================================================================
// pkg/util/limiter/memory_consumption_test.go (tracing panic)
// ============================================================================

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionTracker_NegativeMemoryConsumptionPanicWithTracing
#[tokio::test]
async fn test_memory_consumption_tracker_negative_memory_consumption_panic_with_tracing() {
    use mimir_common::tracing::{current_trace_id, parse_traceparent, with_trace_context};
    use mimir_proto::client::streaming::{MemoryConsumptionSource, MemoryConsumptionTracker};

    let ctx = parse_traceparent("00-00000000000000010000000000000002-0000000000000003-01")
        .unwrap();

    with_trace_context(ctx, async {
        let trace_id = current_trace_id();
        assert_eq!(trace_id.as_deref(), Some("00000000000000010000000000000002"));
        let tracker = MemoryConsumptionTracker::new_with_details(0, None, "foo + bar", trace_id);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracker.decrease_memory_consumption_by_source(10, MemoryConsumptionSource::IngesterChunks);
        }));
        let msg = res.unwrap_err().downcast::<String>().unwrap();
        assert_eq!(
            *msg,
            "Estimated memory consumption of all instances of ingester chunks in this query is 0 bytes when trying to return 10 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar (trace ID: 00000000000000010000000000000002)"
        );
    })
    .await;
}

// ============================================================================
// pkg/usagestats/{stats,report,reporter}_test.go
// ============================================================================

// Upstream: pkg/usagestats/stats_test.go::TestCounter
#[test]
fn test_usagestats_counter() {
    let c = mimir_common::usagestats::CounterStat::default();
    c.inc(100);
    c.inc(200);
    c.inc(300);
    std::thread::sleep(Duration::from_secs(1));
    c.update_rate();
    let v = c.value();
    assert_eq!(v["total"], serde_json::json!(600));
    let rate = v["rate"].as_f64().unwrap();
    assert!(rate >= 550.0, "expected rate >= 550, got {rate}");
    c.reset();
    assert_eq!(c.value()["total"], serde_json::json!(0));
    assert_eq!(c.value()["rate"], serde_json::json!(0.0));
}

// Upstream: pkg/usagestats/report_test.go::TestBuildReport
#[test]
fn test_usagestats_build_report() {
    use mimir_common::usagestats::{
        build_report_with_version, get_and_reset_counter, get_int, get_string, set_target,
        ClusterSeed, PrometheusVersion,
    };

    let cluster_created_at = SystemTime::now() - Duration::from_secs(3600);
    let seed = ClusterSeed {
        uid: "test".to_string(),
        created_at: cluster_created_at,
    };
    let report_at = SystemTime::now();
    let report_interval = Duration::from_secs(3600);

    set_target("all");
    let version = PrometheusVersion {
        version: "dev-version".to_string(),
        branch: "dev-branch".to_string(),
        revision: "dev-revision".to_string(),
        build_user: String::new(),
        build_date: String::new(),
        go_version: "go1.23".to_string(),
    };

    get_string("custom_string").set("my_value");
    get_int("custom_int").set(111);
    get_and_reset_counter("custom_counter").inc(222);

    let report = build_report_with_version(&seed, report_at, report_interval, version);
    assert_eq!(report.cluster_id, "test");
    assert_eq!(report.created_at, cluster_created_at);
    assert_eq!(report.interval, report_at);
    assert_eq!(report.interval_period, report_interval.as_secs_f64());
    assert_eq!(report.target, "all");
    assert_eq!(report.edition, "oss");
    assert_eq!(report.mode, "classic");
    assert!(report.metrics["memstats"].is_object());
    assert_eq!(report.version.version, "dev-version");
    assert_eq!(report.version.branch, "dev-branch");
    assert_eq!(report.version.revision, "dev-revision");
    assert_eq!(report.version.go_version, "go1.23");
    assert_eq!(report.metrics["custom_string"], "my_value");
    assert_eq!(report.metrics["custom_int"], 111);
    assert!(report.metrics["custom_counter"].is_object());
    assert_eq!(report.metrics["custom_counter"]["total"], 222);
}

// Upstream: pkg/usagestats/reporter_test.go::TestGetNextReportAt
#[test]
fn test_usagestats_get_next_report_at() {
    use std::time::UNIX_EPOCH;
    use mimir_common::usagestats::get_next_report_at;

    let at = |ns: u64| UNIX_EPOCH + Duration::from_nanos(ns);
    const H: u64 = 3_600_000_000_000;
    const M: u64 = 60_000_000_000;
    const MS: u64 = 1_000_000;
    let hour = Duration::from_secs(3600);

    // createdAt aligned with interval and now
    assert_eq!(get_next_report_at(hour, at(H), at(2 * H)), at(2 * H));
    // createdAt aligned with interval
    assert_eq!(get_next_report_at(hour, at(H), at(2 * H + 1)), at(3 * H));
    // createdAt not aligned
    assert_eq!(
        get_next_report_at(hour, at(H + 18 * M + 20 * MS), at(2 * H + 1)),
        at(2 * H + 18 * M + 20 * MS)
    );
}

