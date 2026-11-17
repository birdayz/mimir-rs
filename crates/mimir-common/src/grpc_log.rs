//! Per-RPC error logging decisions handed from gRPC handlers to the server's log interceptor.
//!
//! Ported from:
//! - github.com/grafana/dskit/middleware/grpc_logging.go (`GRPCServerLog`, `OptionalLogging`,
//!   `DoNotLogError`, `shouldLog`: the error is decorated as `"<err> (<reason>)"` and dropped when
//!   the handler's error says so)
//! - github.com/grafana/mimir/pkg/util/log/sampler.go (`SampledError.ShouldLog`:
//!   `sampled 1/<freq>`)
//!
//! In Go the interceptor inspects the returned `error` value; with tonic only the encoded status
//! reaches the server, so handlers record what Go's error would say in a [`GrpcErrorLog`] request
//! extension inserted by the server for every RPC.

use std::sync::Arc;

use parking_lot::Mutex;

/// What the gRPC server log interceptor does with a failed RPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcErrorLogEntry {
    /// The Go `err.Error()` text (e.g. `globalerror.ErrorWithStatus` logs just the message).
    pub err: String,
    /// `OptionalLogging.ShouldLog` result.
    pub keep: bool,
    /// `OptionalLogging.ShouldLog` reason appended as `"<err> (<reason>)"`, if any.
    pub reason: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    entry: Option<GrpcErrorLogEntry>,
    instrumentation_code: Option<i32>,
}

/// Request extension carrying the handler's logging decision back to the server.
#[derive(Clone, Debug, Default)]
pub struct GrpcErrorLog(Arc<Mutex<Inner>>);

impl GrpcErrorLog {
    pub fn set(&self, entry: GrpcErrorLogEntry) {
        self.0.lock().entry = Some(entry);
    }

    pub fn take(&self) -> Option<GrpcErrorLogEntry> {
        self.0.lock().entry.take()
    }

    /// The gRPC code dskit's instrumentation derives from the handler's Go error when it differs
    /// from the code on the wire: e.g. an error wrapping `context.DeadlineExceeded` is sent as
    /// `DeadlineExceeded` by grpc-go but recorded as `Unknown` (`grpcutil.ErrorToStatusCode`).
    pub fn set_instrumentation_code(&self, code: i32) {
        self.0.lock().instrumentation_code = Some(code);
    }

    pub fn instrumentation_code(&self) -> Option<i32> {
        self.0.lock().instrumentation_code
    }
}
