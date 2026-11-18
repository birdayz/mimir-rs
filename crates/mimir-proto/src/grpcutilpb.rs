//! Ported from github.com/grafana/dskit/grpcutil/error_details.proto,
//! github.com/grafana/dskit/grpcutil/status.go,
//! github.com/grafana/dskit/httpgrpc/httpgrpc.go, and google/rpc/status.proto.

use prost::Message;

use crate::{cortexpb, httpgrpcpb};

pub const CORTEXPB_ERROR_DETAILS_TYPE_URL: &str = "type.googleapis.com/cortexpb.ErrorDetails";
pub const HTTPGRPC_HTTP_RESPONSE_TYPE_URL: &str = "type.googleapis.com/httpgrpc.HTTPResponse";
pub const GRPCUTIL_ERROR_DETAILS_TYPE_URL: &str = "type.googleapis.com/grpcutil.ErrorDetails";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum ErrorCause {
    UnknownCause = 0,
    WrongClusterValidationLabel = 1,
}

impl ErrorCause {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ErrorCause::UnknownCause => "UNKNOWN_CAUSE",
            ErrorCause::WrongClusterValidationLabel => "WRONG_CLUSTER_VALIDATION_LABEL",
        }
    }

    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "UNKNOWN_CAUSE" => Some(Self::UnknownCause),
            "WRONG_CLUSTER_VALIDATION_LABEL" => Some(Self::WrongClusterValidationLabel),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct ErrorDetails {
    #[prost(enumeration = "ErrorCause", tag = "1")]
    pub cause: i32,
}

/// Wire-compatible representation of `google.rpc.Status`.
pub mod google_rpc {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Status {
        #[prost(int32, tag = "1")]
        pub code: i32,
        #[prost(string, tag = "2")]
        pub message: String,
        #[prost(message, repeated, tag = "3")]
        pub details: Vec<prost_types::Any>,
    }
}

/// Pack a `prost::Message` into `prost_types::Any` with the given `type_url`.
pub fn pack_any<M: Message>(type_url: &str, msg: &M) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.to_string(),
        value: msg.encode_to_vec(),
    }
}

/// Unpack a `prost_types::Any` if its `type_url` matches `expected_type_url` (or suffix after `'/'`).
pub fn unpack_any<M: Message + Default>(
    any: &prost_types::Any,
    expected_type_url: &str,
) -> Option<Result<M, prost::DecodeError>> {
    let expected_name = expected_type_url
        .rsplit_once('/')
        .map_or(expected_type_url, |(_, name)| name);
    let matches = any.type_url == expected_type_url
        || any
            .type_url
            .rsplit_once('/')
            .is_some_and(|(_, actual_name)| !actual_name.is_empty() && actual_name == expected_name);
    if matches {
        Some(M::decode(any.value.as_slice()))
    } else {
        None
    }
}

/// Build a `tonic::Status` carrying serialized `google.rpc.Status` details in `grpc-status-details-bin`.
pub fn status_with_details(
    code: tonic::Code,
    message: impl Into<String>,
    details: Vec<prost_types::Any>,
) -> tonic::Status {
    let message = message.into();
    let rpc_status = google_rpc::Status {
        code: code as i32,
        message: message.clone(),
        details,
    };
    tonic::Status::with_details(code, message, rpc_status.encode_to_vec().into())
}

/// Decode `google.rpc.Status` from `tonic::Status::details()`.
pub fn decode_status_details(status: &tonic::Status) -> Result<google_rpc::Status, prost::DecodeError> {
    google_rpc::Status::decode(status.details())
}

/// Build a `tonic::Status` carrying `cortexpb::ErrorDetails`.
pub fn status_with_cortex_error_details(
    code: tonic::Code,
    message: impl Into<String>,
    details: &cortexpb::ErrorDetails,
) -> tonic::Status {
    let any = pack_any(CORTEXPB_ERROR_DETAILS_TYPE_URL, details);
    status_with_details(code, message, vec![any])
}

/// Extract the first `cortexpb::ErrorDetails` from a `tonic::Status`, if present.
pub fn extract_cortex_error_details(status: &tonic::Status) -> Option<cortexpb::ErrorDetails> {
    let rpc_status = decode_status_details(status).ok()?;
    for any in &rpc_status.details {
        if let Some(Ok(d)) = unpack_any::<cortexpb::ErrorDetails>(any, CORTEXPB_ERROR_DETAILS_TYPE_URL) {
            return Some(d);
        }
    }
    None
}

/// Build a `tonic::Status` carrying `grpcutil::ErrorDetails`.
pub fn status_with_grpcutil_error_details(
    code: tonic::Code,
    message: impl Into<String>,
    details: &ErrorDetails,
) -> tonic::Status {
    let any = pack_any(GRPCUTIL_ERROR_DETAILS_TYPE_URL, details);
    status_with_details(code, message, vec![any])
}

/// Extract the first `grpcutil::ErrorDetails` from a `tonic::Status`, if present.
pub fn extract_grpcutil_error_details(status: &tonic::Status) -> Option<ErrorDetails> {
    let rpc_status = decode_status_details(status).ok()?;
    for any in &rpc_status.details {
        if let Some(Ok(d)) = unpack_any::<ErrorDetails>(any, GRPCUTIL_ERROR_DETAILS_TYPE_URL) {
            return Some(d);
        }
    }
    None
}

/// Build a `tonic::Status` wrapping an `httpgrpc.HTTPResponse` (matching `dskit/httpgrpc.ErrorFromHTTPResponseWithMessage`).
///
/// Note: `google.rpc.Status.code` stores `resp.code` (the HTTP status code) verbatim,
/// matching `httpgrpc.go`.
pub fn status_with_http_response(
    resp: &httpgrpcpb::HttpResponse,
    message: impl Into<String>,
) -> tonic::Status {
    let message = message.into();
    let any = pack_any(HTTPGRPC_HTTP_RESPONSE_TYPE_URL, resp);
    let rpc_status = google_rpc::Status {
        code: resp.code,
        message: message.clone(),
        details: vec![any],
    };
    let tonic_code = http_code_to_grpc_code(resp.code);
    tonic::Status::with_details(tonic_code, message, rpc_status.encode_to_vec().into())
}

/// Extract `httpgrpcpb::HttpResponse` from a `tonic::Status` (matching `dskit/httpgrpc.HTTPResponseFromError`).
pub fn extract_http_response(status: &tonic::Status) -> Option<httpgrpcpb::HttpResponse> {
    if status.details().is_empty() {
        return None;
    }
    let rpc_status = decode_status_details(status).ok()?;
    if rpc_status.details.len() != 1 {
        return None;
    }
    unpack_any::<httpgrpcpb::HttpResponse>(&rpc_status.details[0], HTTPGRPC_HTTP_RESPONSE_TYPE_URL)?
        .ok()
}

/// Best-effort mapping from HTTP status code to `tonic::Code` for the HTTP/2 `grpc-status` header.
/// Note that the exact HTTP status code is always preserved inside `google.rpc.Status.code` and
/// `httpgrpc.HTTPResponse.Code`.
pub fn http_code_to_grpc_code(http_code: i32) -> tonic::Code {
    match http_code {
        200..=299 => tonic::Code::Ok,
        400 => tonic::Code::InvalidArgument,
        401 => tonic::Code::Unauthenticated,
        403 => tonic::Code::PermissionDenied,
        404 => tonic::Code::NotFound,
        409 => tonic::Code::AlreadyExists,
        429 => tonic::Code::ResourceExhausted,
        499 => tonic::Code::Cancelled,
        501 => tonic::Code::Unimplemented,
        503 => tonic::Code::Unavailable,
        504 => tonic::Code::DeadlineExceeded,
        500..=599 => tonic::Code::Internal,
        _ => tonic::Code::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cortex_error_details_roundtrip() {
        let details = cortexpb::ErrorDetails {
            cause: cortexpb::ErrorCause::IngestionRateLimited as i32,
            soft: true,
            rejected_samples: 42,
        };
        let status = status_with_cortex_error_details(
            tonic::Code::ResourceExhausted,
            "rate limit exceeded",
            &details,
        );
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(status.message(), "rate limit exceeded");
        let extracted = extract_cortex_error_details(&status).expect("extract details");
        assert_eq!(extracted, details);
    }

    #[test]
    fn test_http_response_status_roundtrip() {
        let resp = httpgrpcpb::HttpResponse {
            code: 429,
            headers: vec![httpgrpcpb::Header {
                key: "Retry-After".to_string(),
                values: vec!["5".to_string()],
            }],
            body: bytes::Bytes::from_static(b"too many requests"),
        };
        let status = status_with_http_response(&resp, "too many requests");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let rpc_status = decode_status_details(&status).unwrap();
        assert_eq!(rpc_status.code, 429);
        let extracted = extract_http_response(&status).expect("extract http response");
        assert_eq!(extracted, resp);
    }

    #[test]
    fn test_grpcutil_error_details_roundtrip() {
        let details = ErrorDetails {
            cause: ErrorCause::WrongClusterValidationLabel as i32,
        };
        let status = status_with_grpcutil_error_details(
            tonic::Code::InvalidArgument,
            "wrong cluster",
            &details,
        );
        let extracted = extract_grpcutil_error_details(&status).expect("extract grpcutil details");
        assert_eq!(extracted, details);
    }

    #[test]
    fn test_unpack_any_suffix_and_detail_free_4xx_is_client_error() {
        let details = cortexpb::ErrorDetails {
            cause: cortexpb::ErrorCause::BadData as i32,
            soft: false,
            rejected_samples: 1,
        };
        let any_custom_prefix = pack_any("https://example.com/custom/cortexpb.ErrorDetails", &details);
        let unpacked = unpack_any::<cortexpb::ErrorDetails>(
            &any_custom_prefix,
            CORTEXPB_ERROR_DETAILS_TYPE_URL,
        )
        .expect("suffix match")
        .expect("decode");
        assert_eq!(unpacked, details);

        let any_short_prefix = pack_any("/cortexpb.ErrorDetails", &details);
        assert!(unpack_any::<cortexpb::ErrorDetails>(
            &any_short_prefix,
            CORTEXPB_ERROR_DETAILS_TYPE_URL,
        )
        .is_some());

        let any_wrong_suffix = pack_any("type.googleapis.com/other.ErrorDetails", &details);
        assert!(unpack_any::<cortexpb::ErrorDetails>(
            &any_wrong_suffix,
            CORTEXPB_ERROR_DETAILS_TYPE_URL,
        )
        .is_none());

        let mut detail_free_400 = tonic::Status::unknown("bad request");
        detail_free_400
            .metadata_mut()
            .insert("grpc-status", "400".parse().unwrap());
        assert!(cortexpb::is_client_error(&detail_free_400));

        let mut detail_free_500 = tonic::Status::unknown("internal error");
        detail_free_500
            .metadata_mut()
            .insert("grpc-status", "500".parse().unwrap());
        assert!(!cortexpb::is_client_error(&detail_free_500));
    }
}
