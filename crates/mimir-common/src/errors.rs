//! Global error codes, Prometheus discard reason constants, and gRPC/HTTP status wrappers.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/globalerror/user.go
//! - github.com/grafana/mimir/pkg/util/globalerror/grpc.go
//! - github.com/grafana/mimir/pkg/util/globalerror/histogram.go
//! - github.com/grafana/mimir/pkg/distributor/validate.go
//! - github.com/grafana/mimir/pkg/ingester/ingester.go
//! - github.com/grafana/mimir/pkg/mimirpb/mimir.proto (ErrorCause, ErrorDetails)

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use thiserror::Error;
pub use tonic::{Code, Status};

pub const GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR: &str = "grpc: the client connection is closing";

const ERR_PREFIX: &str = "err-mimir-";

/// Immutable user-facing error IDs (`pkg/util/globalerror/user.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlobalErrorId(&'static str);

impl GlobalErrorId {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    pub const fn as_str(&self) -> &'static str {
        self.0
    }

    /// Returns the error ID formatted for Prometheus `reason` label values (`-` replaced by `_`).
    pub fn label_value(&self) -> String {
        self.0.replace('-', "_")
    }

    /// Returns `"{msg} (err-mimir-{id})"`.
    pub fn message(&self, msg: &str) -> String {
        format!("{msg} ({ERR_PREFIX}{})", self.0)
    }

    /// Returns the message with error ID and per-instance limit config flag hints.
    pub fn message_with_per_instance_limit_config(
        &self,
        msg: &str,
        flag: &str,
        add_flags: &[&str],
    ) -> String {
        let (flags_list, plural) = build_flags_list(flag, add_flags);
        format!(
            "{msg} ({ERR_PREFIX}{}). To adjust the related limit{plural}, configure {flags_list}, or contact your service administrator.",
            self.0
        )
    }

    /// Returns the message with error ID and per-tenant limit config flag hints.
    pub fn message_with_per_tenant_limit_config(
        &self,
        msg: &str,
        flag: &str,
        add_flags: &[&str],
    ) -> String {
        let (flags_list, plural) = build_flags_list(flag, add_flags);
        format!(
            "{msg} ({ERR_PREFIX}{}). To adjust the related per-tenant limit{plural}, configure {flags_list}, or contact your service administrator.",
            self.0
        )
    }

    /// Returns the message with error ID, remediation strategy, and per-tenant limit config flag hints.
    pub fn message_with_strategy_and_per_tenant_limit_config(
        &self,
        msg: &str,
        strategy: &str,
        flag: &str,
        add_flags: &[&str],
    ) -> String {
        let (flags_list, plural) = build_flags_list(flag, add_flags);
        format!(
            "{msg} ({ERR_PREFIX}{}). {strategy}. Otherwise, to adjust the related per-tenant limit{plural}, configure {flags_list}, or contact your service administrator.",
            self.0
        )
    }
}

impl fmt::Display for GlobalErrorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

fn build_flags_list(flag: &str, add_flags: &[&str]) -> (String, &'static str) {
    let mut sb = String::with_capacity(flag.len() + add_flags.len() * 16 + 8);
    sb.push('-');
    sb.push_str(flag);
    let plural = if add_flags.is_empty() {
        ""
    } else {
        for add_flag in &add_flags[..add_flags.len() - 1] {
            sb.push_str(", -");
            sb.push_str(add_flag);
        }
        sb.push_str(" and -");
        sb.push_str(add_flags[add_flags.len() - 1]);
        "s"
    };
    (sb, plural)
}

// All global error IDs from pkg/util/globalerror/user.go (verbatim).
pub const MISSING_METRIC_NAME: GlobalErrorId = GlobalErrorId::new("missing-metric-name");
pub const INVALID_METRIC_NAME: GlobalErrorId = GlobalErrorId::new("metric-name-invalid");
pub const MAX_LABEL_NAMES_PER_SERIES: GlobalErrorId =
    GlobalErrorId::new("max-label-names-per-series");
pub const MAX_LABEL_NAMES_PER_INFO_SERIES: GlobalErrorId =
    GlobalErrorId::new("max-label-names-per-info-series");
pub const MAX_NATIVE_HISTOGRAM_BUCKETS: GlobalErrorId =
    GlobalErrorId::new("max-native-histogram-buckets");
pub const NOT_REDUCIBLE_NATIVE_HISTOGRAM: GlobalErrorId =
    GlobalErrorId::new("not-reducible-native-histogram");
pub const INVALID_SCHEMA_NATIVE_HISTOGRAM: GlobalErrorId =
    GlobalErrorId::new("invalid-native-histogram-schema");
pub const SERIES_INVALID_LABEL: GlobalErrorId = GlobalErrorId::new("label-invalid");
pub const SERIES_INVALID_LABEL_VALUE: GlobalErrorId = GlobalErrorId::new("label-value-invalid");
pub const SERIES_LABEL_NAME_TOO_LONG: GlobalErrorId = GlobalErrorId::new("label-name-too-long");
pub const SERIES_LABEL_VALUE_TOO_LONG: GlobalErrorId = GlobalErrorId::new("label-value-too-long");
pub const SERIES_BLOCKED_LABEL_VALUE_BYTES: GlobalErrorId =
    GlobalErrorId::new("label-value-bytes-blocked-label");
pub const SERIES_WITH_DUPLICATE_LABEL_NAMES: GlobalErrorId =
    GlobalErrorId::new("duplicate-label-names");
pub const SERIES_LABELS_NOT_SORTED: GlobalErrorId = GlobalErrorId::new("labels-not-sorted");
pub const SAMPLE_TOO_FAR_IN_FUTURE: GlobalErrorId = GlobalErrorId::new("too-far-in-future");
pub const SAMPLE_TOO_FAR_IN_PAST: GlobalErrorId = GlobalErrorId::new("too-far-in-past");
pub const MAX_SERIES_PER_METRIC: GlobalErrorId = GlobalErrorId::new("max-series-per-metric");
pub const MAX_METADATA_PER_METRIC: GlobalErrorId = GlobalErrorId::new("max-metadata-per-metric");
pub const MAX_SERIES_PER_USER: GlobalErrorId = GlobalErrorId::new("max-series-per-user");
pub const MAX_METADATA_PER_USER: GlobalErrorId = GlobalErrorId::new("max-metadata-per-user");
pub const MAX_CHUNKS_PER_QUERY: GlobalErrorId = GlobalErrorId::new("max-chunks-per-query");
pub const MAX_SERIES_PER_QUERY: GlobalErrorId = GlobalErrorId::new("max-series-per-query");
pub const MAX_CHUNK_BYTES_PER_QUERY: GlobalErrorId =
    GlobalErrorId::new("max-chunks-bytes-per-query");
pub const MAX_ESTIMATED_CHUNKS_PER_QUERY: GlobalErrorId =
    GlobalErrorId::new("max-estimated-chunks-per-query");
pub const MAX_ESTIMATED_MEMORY_CONSUMPTION_PER_QUERY: GlobalErrorId =
    GlobalErrorId::new("max-estimated-memory-consumption-per-query");

pub const DISTRIBUTOR_MAX_INGESTION_RATE: GlobalErrorId =
    GlobalErrorId::new("distributor-max-ingestion-rate");
pub const DISTRIBUTOR_MAX_INFLIGHT_PUSH_REQUESTS: GlobalErrorId =
    GlobalErrorId::new("distributor-max-inflight-push-requests");
pub const DISTRIBUTOR_MAX_INFLIGHT_PUSH_REQUESTS_BYTES: GlobalErrorId =
    GlobalErrorId::new("distributor-max-inflight-push-requests-bytes");

pub const INGESTER_MAX_INGESTION_RATE: GlobalErrorId =
    GlobalErrorId::new("ingester-max-ingestion-rate");
pub const INGESTER_MAX_TENANTS: GlobalErrorId = GlobalErrorId::new("ingester-max-tenants");
pub const INGESTER_MAX_IN_MEMORY_SERIES: GlobalErrorId = GlobalErrorId::new("ingester-max-series");
pub const INGESTER_MAX_INFLIGHT_PUSH_REQUESTS: GlobalErrorId =
    GlobalErrorId::new("ingester-max-inflight-push-requests");
pub const INGESTER_MAX_INFLIGHT_PUSH_REQUESTS_BYTES: GlobalErrorId =
    GlobalErrorId::new("ingester-max-inflight-push-requests-bytes");
pub const INGESTER_MAX_INFLIGHT_READ_REQUESTS: GlobalErrorId =
    GlobalErrorId::new("ingester-max-inflight-read-requests");

pub const EXEMPLAR_LABELS_MISSING: GlobalErrorId = GlobalErrorId::new("exemplar-labels-missing");
pub const EXEMPLAR_LABELS_TOO_LONG: GlobalErrorId = GlobalErrorId::new("exemplar-labels-too-long");
pub const EXEMPLAR_TIMESTAMP_INVALID: GlobalErrorId =
    GlobalErrorId::new("exemplar-timestamp-invalid");

pub const METRIC_METADATA_MISSING_METRIC_NAME: GlobalErrorId =
    GlobalErrorId::new("metadata-missing-metric-name");
pub const METRIC_METADATA_METRIC_NAME_TOO_LONG: GlobalErrorId =
    GlobalErrorId::new("metric-name-too-long");
pub const METRIC_METADATA_HELP_TOO_LONG: GlobalErrorId = GlobalErrorId::new("help-too-long");
pub const METRIC_METADATA_UNIT_TOO_LONG: GlobalErrorId = GlobalErrorId::new("unit-too-long");

pub const MAX_QUERY_LENGTH: GlobalErrorId = GlobalErrorId::new("max-query-length");
pub const MAX_TOTAL_QUERY_LENGTH: GlobalErrorId = GlobalErrorId::new("max-total-query-length");
pub const MAX_QUERY_EXPRESSION_SIZE_BYTES: GlobalErrorId =
    GlobalErrorId::new("max-query-expression-size-bytes");
pub const REQUEST_RATE_LIMITED: GlobalErrorId = GlobalErrorId::new("tenant-max-request-rate");
pub const INGESTION_RATE_LIMITED: GlobalErrorId = GlobalErrorId::new("tenant-max-ingestion-rate");
pub const TOO_MANY_HA_CLUSTERS: GlobalErrorId = GlobalErrorId::new("tenant-too-many-ha-clusters");
pub const QUERY_BLOCKED: GlobalErrorId = GlobalErrorId::new("query-blocked");
pub const REQUEST_BLOCKED: GlobalErrorId = GlobalErrorId::new("request-blocked");
pub const QUERY_LIMITED: GlobalErrorId = GlobalErrorId::new("query-limited");

pub const SAMPLE_TIMESTAMP_TOO_OLD: GlobalErrorId = GlobalErrorId::new("sample-timestamp-too-old");
pub const SAMPLE_OUT_OF_ORDER: GlobalErrorId = GlobalErrorId::new("sample-out-of-order");
pub const SAMPLE_DUPLICATE_TIMESTAMP: GlobalErrorId =
    GlobalErrorId::new("sample-duplicate-timestamp");
pub const EXEMPLAR_SERIES_MISSING: GlobalErrorId = GlobalErrorId::new("exemplar-series-missing");
pub const EXEMPLAR_TOO_FAR_IN_FUTURE: GlobalErrorId =
    GlobalErrorId::new("exemplar-too-far-in-future");
pub const EXEMPLAR_TOO_FAR_IN_PAST: GlobalErrorId = GlobalErrorId::new("exemplar-too-far-in-past");

pub const STORE_CONSISTENCY_CHECK_FAILED: GlobalErrorId =
    GlobalErrorId::new("store-consistency-check-failed");
pub const BUCKET_INDEX_TOO_OLD: GlobalErrorId = GlobalErrorId::new("bucket-index-too-old");

pub const MAX_ACTIVE_SERIES: GlobalErrorId = GlobalErrorId::new("max-active-series");

pub const DISTRIBUTOR_MAX_WRITE_MESSAGE_SIZE: GlobalErrorId =
    GlobalErrorId::new("distributor-max-write-message-size");
pub const DISTRIBUTOR_MAX_OTLP_REQUEST_SIZE: GlobalErrorId =
    GlobalErrorId::new("distributor-max-otlp-request-size");
pub const DISTRIBUTOR_MAX_WRITE_REQUEST_DATA_ITEM_SIZE: GlobalErrorId =
    GlobalErrorId::new("distributor-max-write-request-data-item-size");

pub const NATIVE_HISTOGRAM_COUNT_MISMATCH: GlobalErrorId =
    GlobalErrorId::new("native-histogram-count-mismatch");
pub const NATIVE_HISTOGRAM_COUNT_NOT_BIG_ENOUGH: GlobalErrorId =
    GlobalErrorId::new("native-histogram-count-not-big-enough");
pub const NATIVE_HISTOGRAM_NEGATIVE_BUCKET_COUNT: GlobalErrorId =
    GlobalErrorId::new("native-histogram-negative-bucket-count");
pub const NATIVE_HISTOGRAM_SPAN_NEGATIVE_OFFSET: GlobalErrorId =
    GlobalErrorId::new("native-histogram-span-negative-offset");
pub const NATIVE_HISTOGRAM_SPANS_BUCKETS_MISMATCH: GlobalErrorId =
    GlobalErrorId::new("native-histogram-spans-buckets-mismatch");
pub const NATIVE_HISTOGRAM_CUSTOM_BUCKETS_MISMATCH: GlobalErrorId =
    GlobalErrorId::new("native-histogram-custom-buckets-mismatch");
pub const NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INVALID: GlobalErrorId =
    GlobalErrorId::new("native-histogram-custom-buckets-invalid");
pub const NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INFINITE: GlobalErrorId =
    GlobalErrorId::new("native-histogram-custom-buckets-infinite");
pub const NATIVE_HISTOGRAM_CUSTOM_BUCKETS_NOT_REDUCIBLE: GlobalErrorId =
    GlobalErrorId::new("native-histogram-custom-buckets-not-reducible");

/// Prometheus discard reason constants used in `cortex_discarded_*_total` and `cortex_ingester_*`.
pub mod discard_reasons {
    pub const EXEMPLAR_LABELS_BLANK: &str = "exemplar_labels_blank";
    pub const EXEMPLAR_TOO_OLD: &str = "exemplar_too_old";
    pub const EXEMPLAR_TOO_FAR_IN_FUTURE: &str = "exemplar_too_far_in_future";
    pub const TOO_MANY_EXEMPLARS_PER_SERIES_PER_REQUEST: &str =
        "too_many_exemplars_per_series_per_request";

    pub const RATE_LIMITED: &str = "rate_limited";
    pub const TOO_MANY_HA_CLUSTERS: &str = "too_many_ha_clusters";
    pub const PER_USER_ACTIVE_SERIES_LIMIT: &str = "per_user_active_series_limit";

    // Ingester sample / series discard reasons (pkg/ingester/ingester.go)
    pub const SAMPLE_OUT_OF_ORDER: &str = "sample-out-of-order";
    pub const SAMPLE_TOO_OLD: &str = "sample-too-old";
    pub const SAMPLE_TOO_FAR_IN_FUTURE: &str = "sample-too-far-in-future";
    pub const NEW_VALUE_FOR_TIMESTAMP: &str = "new-value-for-timestamp";
    pub const SAMPLE_TIMESTAMP_TOO_OLD: &str = "sample-timestamp-too-old";
    pub const PER_USER_SERIES_LIMIT: &str = "per_user_series_limit";
    pub const PER_METRIC_SERIES_LIMIT: &str = "per_metric_series_limit";
    pub const INVALID_NATIVE_HISTOGRAM: &str = "invalid-native-histogram";
    pub const LABELS_NOT_SORTED: &str = "labels-not-sorted";
}

/// Cause enum matching `mimirpb.ErrorCause` in `pkg/mimirpb/mimir.proto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i32)]
pub enum ErrorCause {
    #[default]
    Unknown = 0,
    ReplicasDidNotMatch = 1,
    TooManyClusters = 2,
    BadData = 3,
    IngestionRateLimited = 4,
    RequestRateLimited = 5,
    InstanceLimit = 6,
    ServiceUnavailable = 7,
    TsdbUnavailable = 8,
    TooBusy = 9,
    CircuitBreakerOpen = 10,
    MethodNotAllowed = 11,
    TenantLimit = 12,
    ActiveSeriesLimited = 13,
}

/// Structured error details matching `mimirpb.ErrorDetails`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ErrorDetails {
    pub cause: ErrorCause,
    pub soft: bool,
    pub rejected_samples: i64,
}

/// Context cancellation / deadline error (`context.Canceled` / `context.DeadlineExceeded`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum ContextError {
    #[error("context canceled")]
    Canceled,
    #[error("context deadline exceeded")]
    DeadlineExceeded,
}

/// Marker error matching `dskit/middleware.DoNotLogError`.
#[derive(Debug)]
pub struct DoNotLogError<E>(pub E);

impl<E: fmt::Display> fmt::Display for DoNotLogError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<E: StdError + 'static> StdError for DoNotLogError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.0)
    }
}

/// Error wrapper carrying gRPC status code, message, optional `ErrorDetails`,
/// optional underlying error cause, optional `ContextError`, and optional HTTP status code.
///
/// Ported from `github.com/grafana/mimir/pkg/util/globalerror/grpc.go:ErrorWithStatus`.
#[derive(Clone)]
pub struct ErrorWithStatus {
    pub message: String,
    pub grpc_code: Code,
    pub http_status: Option<u16>,
    pub details: Option<ErrorDetails>,
    pub do_not_log: bool,
    pub ctx_err: Option<ContextError>,
    underlying_err: Option<Arc<dyn StdError + Send + Sync>>,
}

impl fmt::Debug for ErrorWithStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorWithStatus")
            .field("message", &self.message)
            .field("grpc_code", &self.grpc_code)
            .field("http_status", &self.http_status)
            .field("details", &self.details)
            .field("do_not_log", &self.do_not_log)
            .field("ctx_err", &self.ctx_err)
            .finish()
    }
}

impl PartialEq for ErrorWithStatus {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
            && self.grpc_code == other.grpc_code
            && self.http_status == other.http_status
            && self.details == other.details
            && self.do_not_log == other.do_not_log
            && self.ctx_err == other.ctx_err
    }
}

impl Eq for ErrorWithStatus {}

impl fmt::Display for ErrorWithStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for ErrorWithStatus {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        if let Some(ref u) = self.underlying_err {
            return Some(u.as_ref());
        }
        if let Some(ref c) = self.ctx_err {
            return Some(c);
        }
        None
    }
}

impl ErrorWithStatus {
    pub fn new(
        message: impl Into<String>,
        grpc_code: Code,
        details: Option<ErrorDetails>,
    ) -> Self {
        Self {
            message: message.into(),
            grpc_code,
            http_status: None,
            details,
            do_not_log: false,
            ctx_err: None,
            underlying_err: None,
        }
    }

    pub fn with_underlying<E: StdError + Send + Sync + 'static>(mut self, err: E) -> Self {
        self.underlying_err = Some(Arc::new(err));
        self
    }

    pub fn with_underlying_arc(mut self, err: Arc<dyn StdError + Send + Sync>) -> Self {
        self.underlying_err = Some(err);
        self
    }

    pub fn with_ctx_err(mut self, ctx_err: ContextError) -> Self {
        self.ctx_err = Some(ctx_err);
        self
    }

    pub fn with_http_status(mut self, status: u16) -> Self {
        self.http_status = Some(status);
        self
    }

    pub fn with_do_not_log(mut self, do_not_log: bool) -> Self {
        self.do_not_log = do_not_log;
        self
    }

    pub fn underlying(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.underlying_err.as_deref()
    }

    pub fn should_log(&self) -> bool {
        !self.do_not_log
    }

    /// Returns an immutable `tonic::Status` representing this `ErrorWithStatus` without the
    /// wrapped underlying error chain, or `None` if `grpc_code == Code::Ok` or no underlying error
    /// was attached (`ErrorWithStatus.Err()` in `globalerror/grpc.go`).
    pub fn err(&self) -> Option<Status> {
        if self.underlying_err.is_none() || self.grpc_code == Code::Ok {
            return None;
        }
        Some(self.to_tonic_status())
    }

    /// Encodes this error as a `tonic::Status`, embedding `cortexpb.ErrorDetails` into
    /// `grpc-status-details-bin` if present.
    pub fn to_tonic_status(&self) -> Status {
        if let Some(details) = self.details {
            let pb = cortexpb::ErrorDetails {
                cause: details.cause as i32,
                soft: details.soft,
                rejected_samples: details.rejected_samples,
            };
            grpcutilpb::status_with_cortex_error_details(self.grpc_code, &self.message, &pb)
        } else {
            Status::new(self.grpc_code, self.message.clone())
        }
    }

    /// Returns true if `other` has the same gRPC status code, message, and `ErrorDetails.cause`.
    pub fn equals(&self, other: &Self) -> bool {
        if self.grpc_code != other.grpc_code || self.message != other.message {
            return false;
        }
        match (self.details, other.details) {
            (None, None) => true,
            (Some(a), Some(b)) => a.cause == b.cause,
            _ => false,
        }
    }
}

impl From<ErrorWithStatus> for Status {
    fn from(err: ErrorWithStatus) -> Self {
        err.to_tonic_status()
    }
}

/// Wraps `original_err` with a gRPC status code and optional `ErrorDetails`, matching
/// `pkg/util/globalerror/grpc.go:WrapErrorWithGRPCStatus`.
pub fn wrap_error_with_grpc_status<E: StdError + Send + Sync + 'static>(
    original_err: E,
    err_code: Code,
    err_details: Option<ErrorDetails>,
) -> ErrorWithStatus {
    let message = original_err.to_string();
    ErrorWithStatus::new(message, err_code, err_details).with_underlying(original_err)
}

/// Wraps a `DoNotLogError` with a gRPC status code and optional `ErrorDetails`.
pub fn wrap_do_not_log_error_with_grpc_status<E: StdError + Send + Sync + 'static>(
    original_err: DoNotLogError<E>,
    err_code: Code,
    err_details: Option<ErrorDetails>,
) -> ErrorWithStatus {
    let message = original_err.to_string();
    ErrorWithStatus::new(message, err_code, err_details)
        .with_underlying(original_err)
        .with_do_not_log(true)
}

/// Checks if `err` is a gRPC error (`Status` or `ErrorWithStatus`) corresponding to `ctx_err`
/// (`ContextError::Canceled` or `ContextError::DeadlineExceeded`), and if so wraps it with `ctx_err`.
///
/// Ported from `pkg/util/globalerror/grpc.go:WrapGRPCErrorWithContextError`.
pub fn wrap_grpc_error_with_context_error<E: StdError + Send + Sync + 'static>(
    ctx_err: Option<ContextError>,
    err: E,
) -> Result<E, ErrorWithStatus> {
    let Some(ctx_err) = ctx_err else {
        return Ok(err);
    };

    let any: &dyn StdError = &err;
    if let Some(status) = extract_status_from_error_chain(any) {
        let code = status.code();
        let msg = status.message().to_string();
        let details = grpcutilpb::extract_cortex_error_details(&status).map(|d| ErrorDetails {
            cause: error_cause_from_i32(d.cause),
            soft: d.soft,
            rejected_samples: d.rejected_samples,
        });
        match code {
            Code::Cancelled => {
                if msg != GRPC_CLIENT_CONNECTION_IS_CLOSING_ERR {
                    return Err(ErrorWithStatus::new(msg, code, details)
                        .with_underlying(err)
                        .with_ctx_err(ContextError::Canceled));
                }
            }
            Code::DeadlineExceeded => {
                return Err(ErrorWithStatus::new(msg, code, details)
                    .with_underlying(err)
                    .with_ctx_err(ContextError::DeadlineExceeded));
            }
            _ => return Ok(err),
        }
    }
    let _ = ctx_err;
    Ok(err)
}

/// Extracts a `tonic::Status` from an error or its `source()` chain.
pub fn extract_status_from_error_chain(err: &(dyn StdError + 'static)) -> Option<Status> {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(ews) = e.downcast_ref::<ErrorWithStatus>() {
            return Some(ews.to_tonic_status());
        }
        if let Some(st) = e.downcast_ref::<Status>() {
            return Some(st.clone());
        }
        cur = e.source();
    }
    None
}

/// Checks whether `err` or any error in its `source()` chain (including `ErrorWithStatus::ctx_err`)
/// matches the given `ContextError`.
pub fn error_has_context_error(err: &(dyn StdError + 'static), target: ContextError) -> bool {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(ce) = e.downcast_ref::<ContextError>() {
            if *ce == target {
                return true;
            }
        }
        if let Some(ews) = e.downcast_ref::<ErrorWithStatus>() {
            if ews.ctx_err == Some(target) {
                return true;
            }
        }
        cur = e.source();
    }
    false
}

fn error_cause_from_i32(v: i32) -> ErrorCause {
    match v {
        1 => ErrorCause::ReplicasDidNotMatch,
        2 => ErrorCause::TooManyClusters,
        3 => ErrorCause::BadData,
        4 => ErrorCause::IngestionRateLimited,
        5 => ErrorCause::RequestRateLimited,
        6 => ErrorCause::InstanceLimit,
        7 => ErrorCause::ServiceUnavailable,
        8 => ErrorCause::TsdbUnavailable,
        9 => ErrorCause::TooBusy,
        10 => ErrorCause::CircuitBreakerOpen,
        11 => ErrorCause::MethodNotAllowed,
        12 => ErrorCause::TenantLimit,
        13 => ErrorCause::ActiveSeriesLimited,
        _ => ErrorCause::Unknown,
    }
}

/// Native histogram validation errors mapped by `MapNativeHistogramErr` in `globalerror/histogram.go`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NativeHistogramValidationError {
    #[error("histogram count mismatch")]
    CountMismatch,
    #[error("histogram count not big enough")]
    CountNotBigEnough,
    #[error("histogram negative bucket count")]
    NegativeBucketCount,
    #[error("histogram span negative offset")]
    SpanNegativeOffset,
    #[error("histogram spans buckets mismatch")]
    SpansBucketsMismatch,
    #[error("histogram custom buckets mismatch")]
    CustomBucketsMismatch,
    #[error("histogram custom buckets invalid")]
    CustomBucketsInvalid,
    #[error("histogram custom buckets infinite")]
    CustomBucketsInfinite,
}

/// Maps a [`NativeHistogramValidationError`] to its [`GlobalErrorId`].
pub fn map_native_histogram_err(err: NativeHistogramValidationError) -> GlobalErrorId {
    match err {
        NativeHistogramValidationError::CountMismatch => NATIVE_HISTOGRAM_COUNT_MISMATCH,
        NativeHistogramValidationError::CountNotBigEnough => NATIVE_HISTOGRAM_COUNT_NOT_BIG_ENOUGH,
        NativeHistogramValidationError::NegativeBucketCount => {
            NATIVE_HISTOGRAM_NEGATIVE_BUCKET_COUNT
        }
        NativeHistogramValidationError::SpanNegativeOffset => NATIVE_HISTOGRAM_SPAN_NEGATIVE_OFFSET,
        NativeHistogramValidationError::SpansBucketsMismatch => {
            NATIVE_HISTOGRAM_SPANS_BUCKETS_MISMATCH
        }
        NativeHistogramValidationError::CustomBucketsMismatch => {
            NATIVE_HISTOGRAM_CUSTOM_BUCKETS_MISMATCH
        }
        NativeHistogramValidationError::CustomBucketsInvalid => {
            NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INVALID
        }
        NativeHistogramValidationError::CustomBucketsInfinite => {
            NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INFINITE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_error_messages() {
        assert_eq!(
            MISSING_METRIC_NAME.message("an error"),
            "an error (err-mimir-missing-metric-name)"
        );
        assert_eq!(MISSING_METRIC_NAME.label_value(), "missing_metric_name");
        assert_eq!(
            MISSING_METRIC_NAME.message_with_per_instance_limit_config("an error", "my-flag1", &[]),
            "an error (err-mimir-missing-metric-name). To adjust the related limit, configure -my-flag1, or contact your service administrator."
        );
        assert_eq!(
            MISSING_METRIC_NAME.message_with_per_instance_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2"]
            ),
            "an error (err-mimir-missing-metric-name). To adjust the related limits, configure -my-flag1 and -my-flag2, or contact your service administrator."
        );
        assert_eq!(
            MISSING_METRIC_NAME.message_with_per_instance_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2", "my-flag3"]
            ),
            "an error (err-mimir-missing-metric-name). To adjust the related limits, configure -my-flag1, -my-flag2 and -my-flag3, or contact your service administrator."
        );
        assert_eq!(
            MISSING_METRIC_NAME.message_with_per_tenant_limit_config("an error", "my-flag1", &[]),
            "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limit, configure -my-flag1, or contact your service administrator."
        );
        assert_eq!(
            MISSING_METRIC_NAME.message_with_per_tenant_limit_config(
                "an error",
                "my-flag1",
                &["my-flag2", "my-flag3"]
            ),
            "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limits, configure -my-flag1, -my-flag2 and -my-flag3, or contact your service administrator."
        );
    }
}
