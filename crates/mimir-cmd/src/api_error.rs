//! HTTP API error types, retryability classification, and Prometheus/gRPC error translation.
//!
//! Ported from github.com/grafana/mimir/pkg/api/error/error.go

use std::error::Error as StdError;
use std::fmt;

use mimir_common::errors::{extract_status_from_error_chain, ContextError};
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpResponse};
use serde::Serialize;
use tonic::Code;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorType {
    None,
    Timeout,
    Canceled,
    Exec,
    BadData,
    Internal,
    Unavailable,
    NotFound,
    TooManyRequests,
    TooLargeEntry,
    NotAcceptable,
    CostLimit,
}

impl ErrorType {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorType::None => "",
            ErrorType::Timeout => "timeout",
            ErrorType::Canceled => "canceled",
            ErrorType::Exec => "execution",
            ErrorType::BadData => "bad_data",
            ErrorType::Internal => "internal",
            ErrorType::Unavailable => "unavailable",
            ErrorType::NotFound => "not_found",
            ErrorType::TooManyRequests => "too_many_requests",
            ErrorType::TooLargeEntry => "too_large_entry",
            ErrorType::NotAcceptable => "not_acceptable",
            ErrorType::CostLimit => "cost_limit",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "" => ErrorType::None,
            "timeout" => ErrorType::Timeout,
            "canceled" => ErrorType::Canceled,
            "execution" => ErrorType::Exec,
            "bad_data" => ErrorType::BadData,
            "internal" => ErrorType::Internal,
            "unavailable" => ErrorType::Unavailable,
            "not_found" => ErrorType::NotFound,
            "too_many_requests" => ErrorType::TooManyRequests,
            "too_large_entry" => ErrorType::TooLargeEntry,
            "not_acceptable" => ErrorType::NotAcceptable,
            "cost_limit" => ErrorType::CostLimit,
            _ => ErrorType::None,
        }
    }
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct PromqlErrStorage(pub Box<dyn StdError + Send + Sync>);

impl fmt::Display for PromqlErrStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for PromqlErrStorage {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct PromqlErrQueryCanceled(pub String);

impl fmt::Display for PromqlErrQueryCanceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query processing would load too many samples or was canceled: {}", self.0)
    }
}

impl StdError for PromqlErrQueryCanceled {}

#[derive(Debug, Clone)]
pub struct PromqlErrQueryTimeout(pub String);

impl fmt::Display for PromqlErrQueryTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query timed out: {}", self.0)
    }
}

impl StdError for PromqlErrQueryTimeout {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub typ: ErrorType,
    pub message: String,
}

impl ApiError {
    pub fn new(typ: ErrorType, msg: impl Into<String>) -> Self {
        Self {
            typ,
            message: msg.into(),
        }
    }

    pub fn status_code(&self) -> u16 {
        match self.typ {
            ErrorType::BadData => 400,
            ErrorType::Exec | ErrorType::CostLimit => 422,
            ErrorType::Canceled => 499,
            ErrorType::Timeout => 503,
            ErrorType::Internal => 500,
            ErrorType::NotFound => 404,
            ErrorType::TooManyRequests => 429,
            ErrorType::TooLargeEntry => 413,
            ErrorType::NotAcceptable => 406,
            ErrorType::Unavailable => 503,
            ErrorType::None => 500,
        }
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        #[derive(Serialize)]
        struct JsonErr<'a> {
            status: &'static str,
            #[serde(rename = "errorType", skip_serializing_if = "str::is_empty")]
            error_type: &'a str,
            #[serde(skip_serializing_if = "str::is_empty")]
            error: &'a str,
        }
        serde_json::to_vec(&JsonErr {
            status: "error",
            error_type: self.typ.as_str(),
            error: &self.message,
        })
    }

    pub fn add_details(mut self, details: &str) -> Self {
        self.message = format!("{details}: {}", self.message);
        self
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for ApiError {}

pub fn is_api_error(err: &(dyn StdError + 'static)) -> bool {
    find_in_chain::<ApiError>(err).is_some()
}

pub fn http_response_from_error(err: &(dyn StdError + 'static)) -> Option<HttpResponse> {
    let api_err = find_in_chain::<ApiError>(err)?;
    let body = api_err.encode_json().ok()?;
    Some(HttpResponse {
        code: i32::from(api_err.status_code()),
        body: body.into(),
        headers: vec![HttpGrpcHeader {
            key: "Content-Type".to_string(),
            values: vec!["application/json".to_string()],
        }],
    })
}

pub fn is_retryable_api_error(err: &(dyn StdError + 'static)) -> bool {
    let Some(api_err) = find_in_chain::<ApiError>(err) else {
        return true;
    };
    is_retryable_api_error_type(api_err.typ)
}

pub fn is_retryable_api_error_type(typ: ErrorType) -> bool {
    typ == ErrorType::Internal
}

pub fn type_for_error(err: &(dyn StdError + 'static), fallback: ErrorType) -> ErrorType {
    if let Some(api_err) = find_in_chain::<ApiError>(err) {
        return api_err.typ;
    }
    if let Some(ctx_err) = find_in_chain::<ContextError>(err) {
        return match ctx_err {
            ContextError::Canceled => ErrorType::Canceled,
            ContextError::DeadlineExceeded => ErrorType::Timeout,
        };
    }
    if find_in_chain::<PromqlErrQueryCanceled>(err).is_some() {
        return ErrorType::Canceled;
    }
    if find_in_chain::<PromqlErrQueryTimeout>(err).is_some() {
        return ErrorType::Timeout;
    }
    if find_in_chain::<PromqlErrStorage>(err).is_some() {
        return ErrorType::Internal;
    }
    if let Some(status) = extract_status_from_error_chain(err) {
        return type_for_code(status.code(), &err.to_string(), fallback);
    }
    fallback
}

fn type_for_code(code: Code, err_msg: &str, fallback: ErrorType) -> ErrorType {
    match code {
        Code::Cancelled => ErrorType::Canceled,
        Code::Internal => ErrorType::Internal,
        Code::DeadlineExceeded => ErrorType::Timeout,
        Code::Unavailable => ErrorType::Unavailable,
        Code::NotFound => ErrorType::NotFound,
        Code::InvalidArgument => ErrorType::BadData,
        Code::ResourceExhausted => {
            if err_msg.contains("message larger than max") {
                ErrorType::TooLargeEntry
            } else {
                fallback
            }
        }
        _ => fallback,
    }
}

fn find_in_chain<'a, T: StdError + 'static>(mut cur: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    loop {
        if let Some(found) = cur.downcast_ref::<T>() {
            return Some(found);
        }
        cur = cur.source()?;
    }
}
