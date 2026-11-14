//! Ingester error types, ErrorDetails mapping, gRPC/HTTP status wrapping, and error samplers.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/errors.go
//! - github.com/grafana/mimir/pkg/ingester/instance_limits.go
//! - github.com/grafana/mimir/pkg/storage/soft_append_error_processor.go

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use mimir_common::errors::{
    GlobalErrorId, EXEMPLAR_SERIES_MISSING, EXEMPLAR_TOO_FAR_IN_FUTURE, EXEMPLAR_TOO_FAR_IN_PAST,
    INGESTER_MAX_INFLIGHT_PUSH_REQUESTS, INGESTER_MAX_INFLIGHT_PUSH_REQUESTS_BYTES,
    INGESTER_MAX_INGESTION_RATE, INGESTER_MAX_IN_MEMORY_SERIES, INGESTER_MAX_TENANTS,
    INVALID_SCHEMA_NATIVE_HISTOGRAM, MAX_METADATA_PER_METRIC, MAX_METADATA_PER_USER,
    MAX_NATIVE_HISTOGRAM_BUCKETS, MAX_SERIES_PER_METRIC, MAX_SERIES_PER_USER,
    NATIVE_HISTOGRAM_COUNT_MISMATCH, NATIVE_HISTOGRAM_COUNT_NOT_BIG_ENOUGH,
    NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INFINITE, NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INVALID,
    NATIVE_HISTOGRAM_CUSTOM_BUCKETS_MISMATCH, NATIVE_HISTOGRAM_CUSTOM_BUCKETS_NOT_REDUCIBLE,
    NATIVE_HISTOGRAM_NEGATIVE_BUCKET_COUNT, NATIVE_HISTOGRAM_SPANS_BUCKETS_MISMATCH,
    NATIVE_HISTOGRAM_SPAN_NEGATIVE_OFFSET, NOT_REDUCIBLE_NATIVE_HISTOGRAM,
    SAMPLE_DUPLICATE_TIMESTAMP, SAMPLE_OUT_OF_ORDER, SAMPLE_TIMESTAMP_TOO_OLD,
    SAMPLE_TOO_FAR_IN_FUTURE, SAMPLE_TOO_FAR_IN_PAST, SERIES_INVALID_LABEL,
    SERIES_INVALID_LABEL_VALUE, SERIES_LABELS_NOT_SORTED, SERIES_LABEL_NAME_TOO_LONG,
    SERIES_LABEL_VALUE_TOO_LONG, MAX_LABEL_NAMES_PER_SERIES,
};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_common::services::State;
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::httpgrpcpb;

pub const INGESTER_TOO_BUSY_MSG: &str =
    "ingester is currently too busy to process queries, try again later";
pub const INGESTER_PUSH_GRPC_DISABLED_MSG: &str =
    "ingester is configured with Push gRPC method disabled";

pub const MAX_INGESTION_RATE_FLAG: &str = "ingester.instance-limits.max-ingestion-rate";
pub const MAX_IN_MEMORY_TENANTS_FLAG: &str = "ingester.instance-limits.max-tenants";
pub const MAX_IN_MEMORY_SERIES_FLAG: &str = "ingester.instance-limits.max-series";
pub const MAX_INFLIGHT_PUSH_REQUESTS_FLAG: &str =
    "ingester.instance-limits.max-inflight-push-requests";
pub const MAX_INFLIGHT_PUSH_REQUESTS_BYTES_FLAG: &str =
    "ingester.instance-limits.max-inflight-push-requests-bytes";

pub const MAX_SERIES_PER_USER_FLAG: &str = "ingester.max-global-series-per-user";
pub const MAX_SERIES_PER_METRIC_FLAG: &str = "ingester.max-global-series-per-metric";
pub const MAX_METADATA_PER_USER_FLAG: &str = "ingester.max-global-metadata-per-user";
pub const MAX_METADATA_PER_METRIC_FLAG: &str = "ingester.max-global-metadata-per-metric";
pub const MAX_LABEL_NAMES_PER_SERIES_FLAG: &str = "validation.max-label-names-per-series";
pub const MAX_LENGTH_LABEL_NAME_FLAG: &str = "validation.max-length-label-name";
pub const MAX_LENGTH_LABEL_VALUE_FLAG: &str = "validation.max-length-label-value";
pub const MAX_NATIVE_HISTOGRAM_BUCKETS_FLAG: &str = "validation.max-native-histogram-buckets";

/// Formats `(&str, &str)` pairs matching Go `mimirpb.FromLabelAdaptersToString`:
/// prints `__name__` unquoted first, then `{k1="v1", k2="v2"}` only if additional labels exist.
pub fn format_labels_string<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
    let collected: Vec<(&'a str, &'a str)> = pairs.into_iter().collect();
    let mut out = String::new();
    let mut metric_name_idx = None;
    for (i, &(k, v)) in collected.iter().enumerate() {
        if k == "__name__" {
            out.push_str(v);
            metric_name_idx = Some(i);
            break;
        }
    }

    let mut count = 0usize;
    for (i, &(k, v)) in collected.iter().enumerate() {
        if Some(i) == metric_name_idx {
            continue;
        }
        if count == 0 {
            out.push('{');
        } else {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push('=');
        out.push('"');
        for ch in v.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c => out.push(c),
            }
        }
        out.push('"');
        count += 1;
    }
    if count > 0 {
        out.push('}');
    }
    out
}

pub fn format_timestamp_ms_rfc3339(ts_ms: i64) -> String {
    if ts_ms >= 0 {
        format_rfc3339_nano_utc(UNIX_EPOCH + Duration::from_millis(ts_ms as u64))
    } else {
        let abs = ts_ms.unsigned_abs();
        let secs_total = abs.div_ceil(1000);
        let rem_ms = (secs_total * 1000 - abs) as u32;
        if let Some(t) = UNIX_EPOCH
            .checked_sub(Duration::from_secs(secs_total))
            .map(|t| t + Duration::from_millis(rem_ms as u64))
        {
            format_rfc3339_nano_utc(t)
        } else {
            format!("{ts_ms}ms")
        }
    }
}

/// All structured ingester error variants matching `pkg/ingester/errors.go`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngesterError {
    Sample {
        err_id: GlobalErrorId,
        err_msg: String,
        timestamp_ms: i64,
        series: String,
    },
    Exemplar {
        err_id: GlobalErrorId,
        err_msg: String,
        timestamp_ms: i64,
        series_labels: String,
        exemplar_labels: String,
    },
    TsdbIngestExemplar {
        original_err: String,
        timestamp_ms: i64,
        series_labels: String,
        exemplar_labels: String,
    },
    PerUserSeriesLimitReached {
        limit: usize,
    },
    PerUserMetadataLimitReached {
        limit: usize,
    },
    PerMetricSeriesLimitReached {
        limit: usize,
        series: String,
    },
    PerMetricMetadataLimitReached {
        limit: usize,
        family: String,
    },
    LabelsNotSorted {
        series: String,
    },
    Validation {
        err_id: GlobalErrorId,
        message: String,
        series: String,
    },
    NativeHistogramValidation {
        err_id: GlobalErrorId,
        original_err: String,
        series: String,
        timestamp_ms: i64,
    },
    Unavailable {
        state: State,
    },
    InstanceLimitReached {
        message: String,
    },
    TsdbUnavailable {
        message: String,
    },
    TooBusy,
    PushGrpcDisabled,
    CircuitBreakerOpen {
        request_type: String,
        remaining_delay: Duration,
    },
    ReactiveLimiterExceeded {
        message: String,
    },
    BadRequest {
        message: String,
    },
    Internal {
        message: String,
    },
    WrappedUser {
        user_id: String,
        inner: Box<IngesterError>,
        rejected_samples: i64,
    },
    Sampled {
        inner: Box<IngesterError>,
        should_log: bool,
    },
    DoNotLog {
        inner: Box<IngesterError>,
    },
}

impl IngesterError {
    pub fn sample_timestamp_too_old(timestamp_ms: i64, series: String) -> Self {
        Self::Sample {
            err_id: SAMPLE_TIMESTAMP_TOO_OLD,
            err_msg: "the sample has been rejected because its timestamp is too old".to_string(),
            timestamp_ms,
            series,
        }
    }

    pub fn sample_timestamp_too_old_ooo_enabled(
        timestamp_ms: i64,
        series: String,
        ooo_window: Duration,
    ) -> Self {
        let dur_str = mimir_config::ModelDuration::from_nanos(ooo_window.as_nanos() as i64).to_string();
        Self::Sample {
            err_id: SAMPLE_TIMESTAMP_TOO_OLD,
            err_msg: format!(
                "the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of {}",
                dur_str
            ),
            timestamp_ms,
            series,
        }
    }

    pub fn sample_too_far_in_future(timestamp_ms: i64, series: String) -> Self {
        Self::Sample {
            err_id: SAMPLE_TOO_FAR_IN_FUTURE,
            err_msg: "received a sample whose timestamp is too far in the future".to_string(),
            timestamp_ms,
            series,
        }
    }

    pub fn sample_too_far_in_past(timestamp_ms: i64, series: String) -> Self {
        Self::Sample {
            err_id: SAMPLE_TOO_FAR_IN_PAST,
            err_msg: "received a sample whose timestamp is too far in the past".to_string(),
            timestamp_ms,
            series,
        }
    }

    pub fn sample_out_of_order(timestamp_ms: i64, series: String) -> Self {
        Self::Sample {
            err_id: SAMPLE_OUT_OF_ORDER,
            err_msg: "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed".to_string(),
            timestamp_ms,
            series,
        }
    }

    pub fn sample_duplicate_timestamp(
        err_msg: impl Into<String>,
        timestamp_ms: i64,
        series: String,
    ) -> Self {
        Self::Sample {
            err_id: SAMPLE_DUPLICATE_TIMESTAMP,
            err_msg: err_msg.into(),
            timestamp_ms,
            series,
        }
    }

    pub fn exemplar_missing_series(
        timestamp_ms: i64,
        series_labels: String,
        exemplar_labels: String,
    ) -> Self {
        Self::Exemplar {
            err_id: EXEMPLAR_SERIES_MISSING,
            err_msg: "the exemplar has been rejected because the related series has not been ingested yet".to_string(),
            timestamp_ms,
            series_labels,
            exemplar_labels,
        }
    }

    pub fn exemplar_too_far_in_future(
        timestamp_ms: i64,
        series_labels: String,
        exemplar_labels: String,
    ) -> Self {
        Self::Exemplar {
            err_id: EXEMPLAR_TOO_FAR_IN_FUTURE,
            err_msg: "received an exemplar whose timestamp is too far in the future".to_string(),
            timestamp_ms,
            series_labels,
            exemplar_labels,
        }
    }

    pub fn exemplar_too_far_in_past(
        timestamp_ms: i64,
        series_labels: String,
        exemplar_labels: String,
    ) -> Self {
        Self::Exemplar {
            err_id: EXEMPLAR_TOO_FAR_IN_PAST,
            err_msg: "received an exemplar whose timestamp is too far in the past".to_string(),
            timestamp_ms,
            series_labels,
            exemplar_labels,
        }
    }

    pub fn per_user_series_limit_reached(limit: usize) -> Self {
        Self::PerUserSeriesLimitReached { limit }
    }

    pub fn per_user_metadata_limit_reached(limit: usize) -> Self {
        Self::PerUserMetadataLimitReached { limit }
    }

    pub fn per_metric_series_limit_reached(limit: usize, series: String) -> Self {
        Self::PerMetricSeriesLimitReached { limit, series }
    }

    pub fn per_metric_metadata_limit_reached(limit: usize, family: String) -> Self {
        Self::PerMetricMetadataLimitReached { limit, family }
    }

    pub fn labels_not_sorted(series: String) -> Self {
        Self::LabelsNotSorted { series }
    }

    pub fn invalid_label_name(name: &str, series: String) -> Self {
        Self::Validation {
            err_id: SERIES_INVALID_LABEL,
            message: SERIES_INVALID_LABEL.message(&format!("label name {name:?} is not valid")),
            series,
        }
    }

    pub fn invalid_label_value(name: &str, value: &str, series: String) -> Self {
        Self::Validation {
            err_id: SERIES_INVALID_LABEL_VALUE,
            message: SERIES_INVALID_LABEL_VALUE
                .message(&format!("invalid label value {value:?} for label {name:?}")),
            series,
        }
    }

    pub fn label_name_too_long(name: &str, max_len: usize, series: String) -> Self {
        Self::Validation {
            err_id: SERIES_LABEL_NAME_TOO_LONG,
            message: SERIES_LABEL_NAME_TOO_LONG.message_with_per_tenant_limit_config(
                &format!("label name too long: {name:?} (limit: {max_len})"),
                MAX_LENGTH_LABEL_NAME_FLAG,
                &[],
            ),
            series,
        }
    }

    pub fn label_value_too_long(value: &str, max_len: usize, series: String) -> Self {
        Self::Validation {
            err_id: SERIES_LABEL_VALUE_TOO_LONG,
            message: SERIES_LABEL_VALUE_TOO_LONG.message_with_per_tenant_limit_config(
                &format!("label value too long (len: {}, limit: {max_len}): {value:?}", value.len()),
                MAX_LENGTH_LABEL_VALUE_FLAG,
                &[],
            ),
            series,
        }
    }

    pub fn max_label_names_per_series(count: usize, limit: usize, series: String) -> Self {
        Self::Validation {
            err_id: MAX_LABEL_NAMES_PER_SERIES,
            message: MAX_LABEL_NAMES_PER_SERIES.message_with_per_tenant_limit_config(
                &format!("series has too many label names: {count} > {limit}"),
                MAX_LABEL_NAMES_PER_SERIES_FLAG,
                &[],
            ),
            series,
        }
    }

    pub fn max_native_histogram_buckets(buckets: usize, limit: usize, series: String) -> Self {
        Self::Validation {
            err_id: MAX_NATIVE_HISTOGRAM_BUCKETS,
            message: MAX_NATIVE_HISTOGRAM_BUCKETS.message_with_per_tenant_limit_config(
                &format!("native histogram has too many buckets: {buckets} > {limit}"),
                MAX_NATIVE_HISTOGRAM_BUCKETS_FLAG,
                &[],
            ),
            series,
        }
    }

    pub fn max_ingestion_rate_reached() -> Self {
        Self::InstanceLimitReached {
            message: INGESTER_MAX_INGESTION_RATE.message_with_per_instance_limit_config(
                "the write request has been rejected because the ingester exceeded the samples ingestion rate limit",
                MAX_INGESTION_RATE_FLAG,
                &[],
            ),
        }
    }

    pub fn max_tenants_reached() -> Self {
        Self::InstanceLimitReached {
            message: INGESTER_MAX_TENANTS.message_with_per_instance_limit_config(
                "the write request has been rejected because the ingester exceeded the allowed number of tenants",
                MAX_IN_MEMORY_TENANTS_FLAG,
                &[],
            ),
        }
    }

    pub fn max_in_memory_series_reached() -> Self {
        Self::InstanceLimitReached {
            message: INGESTER_MAX_IN_MEMORY_SERIES.message_with_per_instance_limit_config(
                "the write request has been rejected because the ingester exceeded the allowed number of in-memory series",
                MAX_IN_MEMORY_SERIES_FLAG,
                &[],
            ),
        }
    }

    pub fn max_inflight_requests_reached() -> Self {
        Self::InstanceLimitReached {
            message: INGESTER_MAX_INFLIGHT_PUSH_REQUESTS.message_with_per_instance_limit_config(
                "the write request has been rejected because the ingester exceeded the allowed number of inflight push requests",
                MAX_INFLIGHT_PUSH_REQUESTS_FLAG,
                &[],
            ),
        }
    }

    pub fn max_inflight_requests_bytes_reached() -> Self {
        Self::InstanceLimitReached {
            message: INGESTER_MAX_INFLIGHT_PUSH_REQUESTS_BYTES
                .message_with_per_instance_limit_config(
                    "the write request has been rejected because the ingester exceeded the allowed total size in bytes of inflight push requests",
                    MAX_INFLIGHT_PUSH_REQUESTS_BYTES_FLAG,
                    &[],
                ),
        }
    }

    pub fn tsdb_forced_compaction() -> Self {
        Self::TsdbUnavailable {
            message: "TSDB Head forced compaction in progress and no write request is currently allowed".to_string(),
        }
    }

    pub fn tsdb_early_compaction(request_min_ts: i64, allowed_min_ts: i64) -> Self {
        Self::TsdbUnavailable {
            message: format!(
                "TSDB Head early compaction in progress and the write request contains samples overlapping with it: request_min_timestamp: {} allowed_min_timestamp: {}",
                format_timestamp_ms_rfc3339(request_min_ts),
                format_timestamp_ms_rfc3339(allowed_min_ts),
            ),
        }
    }

    pub fn tsdb_closing() -> Self {
        Self::TsdbUnavailable {
            message: "TSDB is closing".to_string(),
        }
    }

    pub fn tsdb_not_active() -> Self {
        Self::TsdbUnavailable {
            message: "TSDB is not active".to_string(),
        }
    }

    pub fn with_user(self, user_id: impl Into<String>, rejected_samples: i64) -> Self {
        Self::WrappedUser {
            user_id: user_id.into(),
            inner: Box::new(self),
            rejected_samples,
        }
    }

    pub fn with_sampling(self, should_log: bool) -> Self {
        Self::Sampled {
            inner: Box::new(self),
            should_log,
        }
    }

    pub fn with_do_not_log(self) -> Self {
        Self::DoNotLog {
            inner: Box::new(self),
        }
    }

    pub fn should_log_error(&self) -> bool {
        match self {
            Self::Sampled { should_log, .. } => *should_log,
            Self::DoNotLog { .. } => false,
            Self::WrappedUser { inner, .. } => inner.should_log_error(),
            _ => true,
        }
    }

    pub fn is_optional_logging(&self) -> bool {
        match self {
            Self::Sampled { .. } | Self::DoNotLog { .. } => true,
            Self::WrappedUser { inner, .. } => inner.is_optional_logging(),
            _ => false,
        }
    }

    pub fn error_cause(&self) -> cortexpb::ErrorCause {
        match self {
            Self::Sample { .. }
            | Self::Exemplar { .. }
            | Self::TsdbIngestExemplar { .. }
            | Self::LabelsNotSorted { .. }
            | Self::Validation { .. }
            | Self::NativeHistogramValidation { .. } => cortexpb::ErrorCause::BadData,
            Self::PerUserSeriesLimitReached { .. }
            | Self::PerUserMetadataLimitReached { .. }
            | Self::PerMetricSeriesLimitReached { .. }
            | Self::PerMetricMetadataLimitReached { .. } => cortexpb::ErrorCause::TenantLimit,
            Self::Unavailable { .. } => cortexpb::ErrorCause::ServiceUnavailable,
            Self::InstanceLimitReached { .. } => cortexpb::ErrorCause::InstanceLimit,
            Self::TsdbUnavailable { .. } => cortexpb::ErrorCause::TsdbUnavailable,
            Self::TooBusy => cortexpb::ErrorCause::TooBusy,
            Self::PushGrpcDisabled => cortexpb::ErrorCause::MethodNotAllowed,
            Self::CircuitBreakerOpen { .. } => cortexpb::ErrorCause::CircuitBreakerOpen,
            Self::ReactiveLimiterExceeded { .. } => cortexpb::ErrorCause::RequestRateLimited,
            Self::BadRequest { .. } => cortexpb::ErrorCause::BadData,
            Self::Internal { .. } => cortexpb::ErrorCause::Unknown,
            Self::WrappedUser { inner, .. } | Self::Sampled { inner, .. } | Self::DoNotLog { inner } => inner.error_cause(),
        }
    }

    pub fn has_ingester_error_details(&self) -> bool {
        !matches!(self, Self::Internal { .. } | Self::BadRequest { .. })
            && match self {
                Self::WrappedUser { inner, .. } | Self::Sampled { inner, .. } | Self::DoNotLog { inner } => {
                    inner.has_ingester_error_details()
                }
                _ => true,
            }
    }

    pub fn is_soft(&self) -> bool {
        match self {
            Self::Sample { .. }
            | Self::Exemplar { .. }
            | Self::TsdbIngestExemplar { .. }
            | Self::PerUserSeriesLimitReached { .. }
            | Self::PerUserMetadataLimitReached { .. }
            | Self::PerMetricSeriesLimitReached { .. }
            | Self::PerMetricMetadataLimitReached { .. }
            | Self::LabelsNotSorted { .. }
            | Self::Validation { .. }
            | Self::NativeHistogramValidation { .. } => true,
            Self::WrappedUser { inner, .. } | Self::Sampled { inner, .. } | Self::DoNotLog { inner } => inner.is_soft(),
            _ => false,
        }
    }

    pub fn is_do_not_log(&self) -> bool {
        match self {
            Self::InstanceLimitReached { .. } => true,
            Self::WrappedUser { inner, .. } => inner.is_do_not_log(),
            _ => false,
        }
    }

    pub fn rejected_samples(&self) -> i64 {
        match self {
            Self::WrappedUser {
                rejected_samples, ..
            } => *rejected_samples,
            Self::Sampled { inner, .. } | Self::DoNotLog { inner } => inner.rejected_samples(),
            _ => 0,
        }
    }

    pub fn error_details(&self) -> Option<cortexpb::ErrorDetails> {
        if !self.has_ingester_error_details() {
            return None;
        }
        Some(cortexpb::ErrorDetails {
            cause: self.error_cause() as i32,
            soft: self.is_soft(),
            rejected_samples: self.rejected_samples(),
        })
    }

    pub fn map_push_error_to_status(&self) -> tonic::Status {
        if let Self::BadRequest { message } = self {
            return tonic::Status::invalid_argument(message.clone());
        }
        let code = match self.error_cause() {
            cortexpb::ErrorCause::BadData => tonic::Code::InvalidArgument,
            cortexpb::ErrorCause::TenantLimit => tonic::Code::FailedPrecondition,
            cortexpb::ErrorCause::ServiceUnavailable => tonic::Code::Unavailable,
            cortexpb::ErrorCause::InstanceLimit => tonic::Code::Unavailable,
            cortexpb::ErrorCause::TsdbUnavailable => tonic::Code::Internal,
            cortexpb::ErrorCause::MethodNotAllowed => tonic::Code::Unimplemented,
            cortexpb::ErrorCause::CircuitBreakerOpen => tonic::Code::Unavailable,
            _ => tonic::Code::Internal,
        };
        match self.error_details() {
            Some(details) => {
                grpcutilpb::status_with_cortex_error_details(code, self.to_string(), &details)
            }
            None => tonic::Status::new(code, self.to_string()),
        }
    }

    pub fn map_read_error_to_status(&self) -> tonic::Status {
        if let Self::BadRequest { message } = self {
            return tonic::Status::invalid_argument(message.clone());
        }
        let code = match self.error_cause() {
            cortexpb::ErrorCause::TooBusy => tonic::Code::ResourceExhausted,
            cortexpb::ErrorCause::ServiceUnavailable => tonic::Code::Unavailable,
            cortexpb::ErrorCause::MethodNotAllowed => tonic::Code::Unimplemented,
            cortexpb::ErrorCause::CircuitBreakerOpen => tonic::Code::Unavailable,
            _ => tonic::Code::Internal,
        };
        match self.error_details() {
            Some(details) => {
                grpcutilpb::status_with_cortex_error_details(code, self.to_string(), &details)
            }
            None => tonic::Status::new(code, self.to_string()),
        }
    }

    /// Maps an ingester error to its equivalent `httpgrpc` HTTP status code (`400`/`429`/`500`/`501`/`503`).
    pub fn http_status_code(&self) -> i32 {
        match self.error_cause() {
            cortexpb::ErrorCause::BadData | cortexpb::ErrorCause::TenantLimit => 400,
            cortexpb::ErrorCause::RequestRateLimited
            | cortexpb::ErrorCause::IngestionRateLimited
            | cortexpb::ErrorCause::TooBusy => 429,
            cortexpb::ErrorCause::MethodNotAllowed => 501,
            cortexpb::ErrorCause::ServiceUnavailable
            | cortexpb::ErrorCause::InstanceLimit
            | cortexpb::ErrorCause::CircuitBreakerOpen => 503,
            _ => 500,
        }
    }

    /// Builds an `httpgrpc.HTTPResponse`-wrapped `tonic::Status` for HTTP-over-gRPC callers.
    pub fn to_httpgrpc_status(&self) -> tonic::Status {
        let code = self.http_status_code();
        let msg = self.to_string();
        grpcutilpb::status_with_http_response(
            &httpgrpcpb::HttpResponse {
                code,
                headers: Vec::new(),
                body: msg.clone().into_bytes().into(),
            },
            msg,
        )
    }
}

impl fmt::Display for IngesterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sample {
                err_id,
                err_msg,
                timestamp_ms,
                series,
            } => write!(
                f,
                "{}. The affected sample has timestamp {} and is from series {}",
                err_id.message(err_msg),
                format_timestamp_ms_rfc3339(*timestamp_ms),
                series
            ),
            Self::Exemplar {
                err_id,
                err_msg,
                timestamp_ms,
                series_labels,
                exemplar_labels,
            } => write!(
                f,
                "{}. The affected exemplar is {} with timestamp {} for series {}",
                err_id.message(err_msg),
                exemplar_labels,
                format_timestamp_ms_rfc3339(*timestamp_ms),
                series_labels
            ),
            Self::TsdbIngestExemplar {
                original_err,
                timestamp_ms,
                series_labels,
                exemplar_labels,
            } => write!(
                f,
                "err: {}. timestamp={}, series={}, exemplar={}",
                original_err,
                format_timestamp_ms_rfc3339(*timestamp_ms),
                series_labels,
                exemplar_labels
            ),
            Self::PerUserSeriesLimitReached { limit } => f.write_str(
                &MAX_SERIES_PER_USER.message_with_per_tenant_limit_config(
                    &format!("per-user series limit of {limit} exceeded"),
                    MAX_SERIES_PER_USER_FLAG,
                    &[],
                ),
            ),
            Self::PerUserMetadataLimitReached { limit } => f.write_str(
                &MAX_METADATA_PER_USER.message_with_per_tenant_limit_config(
                    &format!("per-user metric metadata limit of {limit} exceeded"),
                    MAX_METADATA_PER_USER_FLAG,
                    &[],
                ),
            ),
            Self::PerMetricSeriesLimitReached { limit, series } => write!(
                f,
                "{} This is for series {}",
                MAX_SERIES_PER_METRIC.message_with_per_tenant_limit_config(
                    &format!("per-metric series limit of {limit} exceeded"),
                    MAX_SERIES_PER_METRIC_FLAG,
                    &[],
                ),
                series
            ),
            Self::PerMetricMetadataLimitReached { limit, family } => write!(
                f,
                "{} This is for metric {}",
                MAX_METADATA_PER_METRIC.message_with_per_tenant_limit_config(
                    &format!("per-metric metadata limit of {limit} exceeded"),
                    MAX_METADATA_PER_METRIC_FLAG,
                    &[],
                ),
                family
            ),
            Self::LabelsNotSorted { series } => write!(
                f,
                "{} This is for series {}",
                SERIES_LABELS_NOT_SORTED
                    .message("received a series with out-of-order or non-unique label names"),
                series
            ),
            Self::Validation {
                message, series, ..
            } => write!(f, "{message} This is for series {series}"),
            Self::NativeHistogramValidation {
                err_id,
                original_err,
                series,
                timestamp_ms,
            } => f.write_str(&err_id.message(&format!(
                "err: {}. timestamp={}, series={}",
                original_err,
                format_timestamp_ms_rfc3339(*timestamp_ms),
                series
            ))),
            Self::Unavailable { state } => {
                write!(f, "ingester is unavailable (current state: {state})")
            }
            Self::InstanceLimitReached { message } => f.write_str(message),
            Self::TsdbUnavailable { message } => f.write_str(message),
            Self::TooBusy => f.write_str(INGESTER_TOO_BUSY_MSG),
            Self::PushGrpcDisabled => f.write_str(INGESTER_PUSH_GRPC_DISABLED_MSG),
            Self::CircuitBreakerOpen {
                request_type,
                remaining_delay,
            } => write!(
                f,
                "circuit breaker open on {request_type} request type with remaining delay {}",
                mimir_config::GoDuration::from_nanos(remaining_delay.as_nanos() as i64)
            ),
            Self::ReactiveLimiterExceeded { message } => f.write_str(message),
            Self::BadRequest { message } => f.write_str(message),
            Self::Internal { message } => f.write_str(message),
            Self::WrappedUser { user_id, inner, .. } => write!(f, "user={user_id}: {inner}"),
            Self::Sampled { inner, .. } | Self::DoNotLog { inner } => write!(f, "{inner}"),
        }
    }
}

impl std::error::Error for IngesterError {}

/// Maps native histogram validation error strings to their exact `GlobalErrorId`.
pub fn map_native_histogram_err_id(msg: &str) -> GlobalErrorId {
    if msg.contains("count mismatch") || msg.contains("positive/negative") {
        NATIVE_HISTOGRAM_COUNT_MISMATCH
    } else if msg.contains("not big enough") {
        NATIVE_HISTOGRAM_COUNT_NOT_BIG_ENOUGH
    } else if msg.contains("negative bucket count") {
        NATIVE_HISTOGRAM_NEGATIVE_BUCKET_COUNT
    } else if msg.contains("negative offset") {
        NATIVE_HISTOGRAM_SPAN_NEGATIVE_OFFSET
    } else if msg.contains("spans") && msg.contains("buckets") {
        NATIVE_HISTOGRAM_SPANS_BUCKETS_MISMATCH
    } else if msg.contains("custom buckets") && msg.contains("infinite") {
        NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INFINITE
    } else if msg.contains("custom buckets") && msg.contains("reducible") {
        NATIVE_HISTOGRAM_CUSTOM_BUCKETS_NOT_REDUCIBLE
    } else if msg.contains("custom buckets") && msg.contains("mismatch") {
        NATIVE_HISTOGRAM_CUSTOM_BUCKETS_MISMATCH
    } else if msg.contains("custom buckets") {
        NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INVALID
    } else if msg.contains("reducible") {
        NOT_REDUCIBLE_NATIVE_HISTOGRAM
    } else {
        INVALID_SCHEMA_NATIVE_HISTOGRAM
    }
}

/// Rate-limited sampler matching `pkg/util/log.Sampler`.
#[derive(Debug)]
pub struct ErrorSampler {
    rate: i64,
    counter: AtomicI64,
}

impl ErrorSampler {
    pub fn new(rate: i64) -> Self {
        Self {
            rate,
            counter: AtomicI64::new(0),
        }
    }

    /// Returns `true` if this error occurrence should be logged.
    pub fn should_log(&self) -> bool {
        if self.rate == 0 {
            return true;
        }
        if self.rate < 0 {
            return false;
        }
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        n % self.rate == 0
    }
}

#[derive(Debug)]
pub struct ErrorSamplers {
    pub sample_timestamp_too_old: ErrorSampler,
    pub sample_timestamp_too_old_ooo_enabled: ErrorSampler,
    pub sample_timestamp_too_far_in_future: ErrorSampler,
    pub sample_out_of_order: ErrorSampler,
    pub sample_duplicate_timestamp: ErrorSampler,
    pub max_series_per_metric_limit_exceeded: ErrorSampler,
    pub max_metadata_per_metric_limit_exceeded: ErrorSampler,
    pub max_series_per_user_limit_exceeded: ErrorSampler,
    pub max_metadata_per_user_limit_exceeded: ErrorSampler,
    pub native_histogram_validation_error: ErrorSampler,
    pub labels_not_sorted: ErrorSampler,
}

impl ErrorSamplers {
    pub fn new(freq: i64) -> Self {
        Self {
            sample_timestamp_too_old: ErrorSampler::new(freq),
            sample_timestamp_too_old_ooo_enabled: ErrorSampler::new(freq),
            sample_timestamp_too_far_in_future: ErrorSampler::new(freq),
            sample_out_of_order: ErrorSampler::new(freq),
            sample_duplicate_timestamp: ErrorSampler::new(freq),
            max_series_per_metric_limit_exceeded: ErrorSampler::new(freq),
            max_metadata_per_metric_limit_exceeded: ErrorSampler::new(freq),
            max_series_per_user_limit_exceeded: ErrorSampler::new(freq),
            max_metadata_per_user_limit_exceeded: ErrorSampler::new(freq),
            native_histogram_validation_error: ErrorSampler::new(freq),
            labels_not_sorted: ErrorSampler::new(freq),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_mapping_matches_upstream_go_tests() {
        let e1 = IngesterError::Unavailable {
            state: State::Stopping,
        };
        let s1 = e1.map_push_error_to_status();
        assert_eq!(s1.code(), tonic::Code::Unavailable);
        let d1 = grpcutilpb::extract_cortex_error_details(&s1).unwrap();
        assert_eq!(d1.cause, cortexpb::ErrorCause::ServiceUnavailable as i32);
        assert!(!d1.soft);

        let e2 = IngesterError::per_user_series_limit_reached(100).with_user("u1", 5);
        let s2 = e2.map_push_error_to_status();
        assert_eq!(s2.code(), tonic::Code::FailedPrecondition);
        let d2 = grpcutilpb::extract_cortex_error_details(&s2).unwrap();
        assert_eq!(d2.cause, cortexpb::ErrorCause::TenantLimit as i32);
        assert!(d2.soft);
        assert_eq!(d2.rejected_samples, 5);
        assert_eq!(e2.http_status_code(), 400);

        let e3 = IngesterError::LabelsNotSorted {
            series: "{b=\"2\", a=\"1\"}".to_string(),
        }
        .with_user("u1", 3);
        let s3 = e3.map_push_error_to_status();
        assert_eq!(s3.code(), tonic::Code::InvalidArgument);
        let d3 = grpcutilpb::extract_cortex_error_details(&s3).unwrap();
        assert_eq!(d3.cause, cortexpb::ErrorCause::BadData as i32);
        assert!(d3.soft);
        assert_eq!(d3.rejected_samples, 3);

        let e4 = IngesterError::PushGrpcDisabled;
        let s4 = e4.map_push_error_to_status();
        assert_eq!(s4.code(), tonic::Code::Unimplemented);
        let d4 = grpcutilpb::extract_cortex_error_details(&s4).unwrap();
        assert_eq!(d4.cause, cortexpb::ErrorCause::MethodNotAllowed as i32);
        assert_eq!(e4.http_status_code(), 501);

        let e5 = IngesterError::TooBusy;
        let s5 = e5.map_read_error_to_status();
        assert_eq!(s5.code(), tonic::Code::ResourceExhausted);
        let d5 = grpcutilpb::extract_cortex_error_details(&s5).unwrap();
        assert_eq!(d5.cause, cortexpb::ErrorCause::TooBusy as i32);
    }
}
