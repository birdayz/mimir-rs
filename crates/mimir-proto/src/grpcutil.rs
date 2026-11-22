//! Ported from github.com/grafana/dskit/grpcutil/{health_check,metadata,status,taphandle}.go.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};

use crate::grpcutilpb::{google_rpc, pack_any, status_with_details};

// ---------------------------------------------------------------------------
// health_check.go
// ---------------------------------------------------------------------------

/// Service lifecycle states mirrored from `dskit/services.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServiceHealthState {
    New,
    Starting,
    Running,
    Stopping,
    Terminated,
    Failed,
}

/// Summary of managed service counts by state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceStatesSummary {
    pub new: usize,
    pub starting: usize,
    pub running: usize,
    pub stopping: usize,
    pub terminated: usize,
    pub failed: usize,
}

impl ServiceStatesSummary {
    pub fn from_states(states: &[ServiceHealthState]) -> Self {
        let mut summary = Self::default();
        for s in states {
            match s {
                ServiceHealthState::New => summary.new += 1,
                ServiceHealthState::Starting => summary.starting += 1,
                ServiceHealthState::Running => summary.running += 1,
                ServiceHealthState::Stopping => summary.stopping += 1,
                ServiceHealthState::Terminated => summary.terminated += 1,
                ServiceHealthState::Failed => summary.failed += 1,
            }
        }
        summary
    }
}

/// Serving status matching `grpc.health.v1.HealthCheckResponse.ServingStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ServingStatus {
    Unknown = 0,
    Serving = 1,
    NotServing = 2,
    ServiceUnknown = 3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheckResponse {
    pub status: ServingStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthListResponse {
    pub statuses: HashMap<String, HealthCheckResponse>,
}

/// Check function that determines whether the gRPC application is healthy.
pub type HealthCheckFn = Arc<dyn Fn() -> bool + Send + Sync>;

/// Returns a health check matching `dskit/grpcutil.WithManager`:
/// healthy after all services are running and until all services terminate
/// (Stopping is still considered healthy; New/Starting/Failed are unhealthy).
pub fn with_service_states<F>(states_fn: F) -> HealthCheckFn
where
    F: Fn() -> ServiceStatesSummary + Send + Sync + 'static,
{
    Arc::new(move || {
        let states = states_fn();
        if states.new > 0 || states.starting > 0 || states.failed > 0 {
            return false;
        }
        states.running > 0 || states.stopping > 0
    })
}

/// Returns a health check that reports unhealthy when `requested` is `true`.
pub fn with_shutdown_requested(requested: Arc<AtomicBool>) -> HealthCheckFn {
    Arc::new(move || !requested.load(Ordering::Acquire))
}

/// Fulfills the `grpc.health.v1.Health` checks by ensuring every check returns `true`.
#[derive(Clone, Default)]
pub struct HealthCheck {
    checks: Vec<HealthCheckFn>,
}

impl HealthCheck {
    pub fn new_from(checks: Vec<HealthCheckFn>) -> Self {
        Self { checks }
    }

    #[allow(clippy::result_large_err)]
    pub fn check(&self) -> Result<HealthCheckResponse, tonic::Status> {
        for check in &self.checks {
            if !check() {
                return Ok(HealthCheckResponse {
                    status: ServingStatus::NotServing,
                });
            }
        }
        Ok(HealthCheckResponse {
            status: ServingStatus::Serving,
        })
    }

    pub fn watch(&self) -> tonic::Status {
        tonic::Status::unimplemented("Watching is not supported")
    }

    #[allow(clippy::result_large_err)]
    pub fn list(&self) -> Result<HealthListResponse, tonic::Status> {
        let check_resp = self.check()?;
        let mut statuses = HashMap::new();
        statuses.insert("server".to_string(), check_resp);
        Ok(HealthListResponse { statuses })
    }
}

// ---------------------------------------------------------------------------
// metadata.go
// ---------------------------------------------------------------------------

/// gRPC metadata key for message size.
pub const METADATA_MESSAGE_SIZE: &str = "message-size";

/// Trait for objects that can report their serialized size in bytes.
pub trait Sizer {
    fn size(&self) -> usize;
}

/// Appends `message-size` metadata to an outgoing `tonic::metadata::MetadataMap`.
pub fn append_message_size_to_outgoing_context<S: Sizer>(md: &mut MetadataMap, req: &S) {
    let val = req.size().to_string();
    if let Ok(mv) = MetadataValue::try_from(val.as_str()) {
        md.append(METADATA_MESSAGE_SIZE, mv);
    }
}

// ---------------------------------------------------------------------------
// status.go
// ---------------------------------------------------------------------------

/// Error sentinel representing a canceled operation (analogous to `context.Canceled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextCanceledError;

impl fmt::Display for ContextCanceledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context canceled")
    }
}

impl StdError for ContextCanceledError {}

/// Error carrying a raw `google.rpc.Status` (which may have non-standard HTTP status codes).
#[derive(Debug, Clone, PartialEq)]
pub struct RawRpcStatusError {
    pub status: google_rpc::Status,
}

impl RawRpcStatusError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            status: google_rpc::Status {
                code,
                message: message.into(),
                details: Vec::new(),
            },
        }
    }
}

impl fmt::Display for RawRpcStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rpc error: code = {} desc = {}",
            self.status.code, self.status.message
        )
    }
}

impl StdError for RawRpcStatusError {}

/// Extracts `google_rpc::Status` from an error chain if any error in the chain is a
/// `tonic::Status` or `RawRpcStatusError`.
pub fn error_to_status(err: Option<&(dyn StdError + 'static)>) -> Option<google_rpc::Status> {
    let mut cur = err;
    while let Some(e) = cur {
        if let Some(st) = e.downcast_ref::<tonic::Status>() {
            if !st.details().is_empty() {
                if let Ok(decoded) = google_rpc::Status::decode(st.details()) {
                    return Some(decoded);
                }
            }
            return Some(google_rpc::Status {
                code: st.code() as i32,
                message: st.message().to_string(),
                details: Vec::new(),
            });
        }
        if let Some(raw) = e.downcast_ref::<RawRpcStatusError>() {
            return Some(raw.status.clone());
        }
        cur = e.source();
    }
    None
}

/// Extracts gRPC status code from an error chain.
/// - `None` -> `tonic::Code::Ok as i32` (0)
/// - Error wrapping `tonic::Status` / `RawRpcStatusError` -> its numeric code
/// - Otherwise -> `tonic::Code::Unknown as i32` (2)
pub fn error_to_status_code(err: Option<&(dyn StdError + 'static)>) -> i32 {
    let Some(err) = err else {
        return tonic::Code::Ok as i32;
    };
    error_to_status(Some(err)).map_or(tonic::Code::Unknown as i32, |s| s.code)
}

/// Builds a `tonic::Status` with optional detail messages.
/// If any detail fails to encode (`Err(detail_err)`), returns `InvalidArgument` with the
/// upstream-formatted error message.
pub fn status_with_any_details(
    err_code: tonic::Code,
    err_message: &str,
    details: Vec<Result<(&'static str, Vec<u8>), String>>,
) -> tonic::Status {
    if details.is_empty() {
        return tonic::Status::new(err_code, err_message);
    }
    let mut anys = Vec::with_capacity(details.len());
    for d in details {
        match d {
            Ok((type_url, value)) => anys.push(prost_types::Any {
                type_url: type_url.to_string(),
                value,
            }),
            Err(err) => {
                let code_str = format!("{err_code:?}");
                return tonic::Status::invalid_argument(format!(
                    "error while creating details for a Status with code {code_str} and error message {err_message:?}: {err}"
                ));
            }
        }
    }
    status_with_details(err_code, err_message, anys)
}

/// Builds a `tonic::Status` carrying `Message` details.
pub fn status<M: Message>(
    err_code: tonic::Code,
    err_message: &str,
    type_url: &str,
    details: &[Option<&M>],
) -> tonic::Status {
    if details.is_empty() {
        return tonic::Status::new(err_code, err_message);
    }
    let mut anys = Vec::with_capacity(details.len());
    for d in details {
        match d {
            Some(msg) => anys.push(pack_any(type_url, *msg)),
            None => {
                let code_str = format!("{err_code:?}");
                return tonic::Status::invalid_argument(format!(
                    "error while creating details for a Status with code {code_str} and error message {err_message:?}: proto: Marshal called with nil"
                ));
            }
        }
    }
    status_with_details(err_code, err_message, anys)
}

/// Checks whether an error chain comes from a canceled operation (`ContextCanceledError` or
/// gRPC status code `Canceled`).
pub fn is_canceled(err: Option<&(dyn StdError + 'static)>) -> bool {
    let mut cur = err;
    while let Some(e) = cur {
        if e.downcast_ref::<ContextCanceledError>().is_some() {
            return true;
        }
        cur = e.source();
    }
    error_to_status_code(err) == tonic::Code::Cancelled as i32
}

// ---------------------------------------------------------------------------
// taphandle.go
// ---------------------------------------------------------------------------

/// Metadata passed to a `TapHandle`.
#[derive(Debug, Clone, Default)]
pub struct TapInfo {
    pub full_method_name: String,
}

/// Type-erased request context threaded through composed `TapHandle`s.
#[derive(Debug, Default)]
pub struct TapContext {
    values: HashMap<&'static str, Box<dyn Any + Send + Sync>>,
}

impl TapContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_value<T: Any + Send + Sync>(mut self, key: &'static str, value: T) -> Self {
        self.values.insert(key, Box::new(value));
        self
    }

    pub fn value<T: Any>(&self, key: &str) -> Option<&T> {
        self.values.get(key).and_then(|v| v.downcast_ref::<T>())
    }
}

pub type TapError = Box<dyn StdError + Send + Sync>;

pub type TapHandle = Arc<dyn Fn(TapContext, &TapInfo) -> Result<TapContext, TapError> + Send + Sync>;

/// Composes multiple `TapHandle`s into a single `TapHandle` that runs them in order and
/// short-circuits on the first error.
pub fn compose_tap_handles(handles: Vec<TapHandle>) -> TapHandle {
    if handles.len() == 1 {
        return handles.into_iter().next().expect("len == 1");
    }
    Arc::new(move |mut ctx: TapContext, info: &TapInfo| {
        for h in &handles {
            ctx = h(ctx, info)?;
        }
        Ok(ctx)
    })
}
