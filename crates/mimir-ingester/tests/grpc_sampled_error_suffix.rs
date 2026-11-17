//! Go: vendor/github.com/grafana/dskit/middleware/grpc_logging.go (GRPCServerLog appends
//! " (sampled 1/N)" to sampled errors returned by gRPC handlers) + pkg/util/log/sampler.go.

use mimir_ingester::IngesterError;
use tonic::Code;

#[test]
fn sampled_push_error_gets_dskit_suffix_on_grpc_server_status() {
    let base = IngesterError::per_user_series_limit_reached(1);
    let plain = base.to_string();

    let sampled = IngesterError::per_user_series_limit_reached(1)
        .with_sampling_rate(true, 10)
        .with_user("user-1", 1);
    let st = sampled.map_push_error_to_grpc_server_status();
    assert_eq!(st.code(), Code::FailedPrecondition);
    assert_eq!(st.message(), format!("user=user-1: {plain} (sampled 1/10)"));
    // Handler-level mapping (what Go unit tests see when calling ingester.Push directly) has no suffix.
    assert_eq!(
        sampled.map_push_error_to_status().message(),
        format!("user=user-1: {plain}")
    );
    // The suffix doesn't depend on whether this occurrence was logged.
    let not_logged = IngesterError::per_user_series_limit_reached(1)
        .with_sampling_rate(false, 10)
        .with_user("user-1", 1);
    assert!(not_logged
        .map_push_error_to_grpc_server_status()
        .message()
        .ends_with(" (sampled 1/10)"));

    // error_sample_rate = 0: Go's NewSampler(0) returns nil, so no wrapping and no suffix.
    let unsampled = IngesterError::per_user_series_limit_reached(1)
        .with_sampling_rate(true, 0)
        .with_user("user-1", 1);
    assert_eq!(
        unsampled.map_push_error_to_grpc_server_status().message(),
        format!("user=user-1: {plain}")
    );
}
