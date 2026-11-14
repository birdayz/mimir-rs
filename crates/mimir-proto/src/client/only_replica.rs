//! Ported from github.com/grafana/mimir/pkg/ingester/client/only_replica.go

use std::future::Future;

use tonic::metadata::{MetadataMap, MetadataValue};

pub const ONLY_REPLICA_GRPC_MD_KEY: &str = "__only_replica__";

tokio::task_local! {
    static AVAILABLE_REPLICAS: Option<usize>;
    static ONLY_REPLICA: bool;
}

/// Runs an async future with `available_replicas` set in task-local context
/// (analogous to `ring.ContextWithAvailableReplicas`).
pub async fn with_available_replicas<F, R>(replicas: usize, fut: F) -> R
where
    F: Future<Output = R>,
{
    AVAILABLE_REPLICAS.scope(Some(replicas), fut).await
}

pub fn get_available_replicas() -> Option<usize> {
    AVAILABLE_REPLICAS.try_with(|v| *v).ok().flatten()
}

/// Runs an async future with the `only_replica` flag set in task-local context
/// (analogous to `ContextWithOnlyReplica`).
pub async fn with_only_replica<F, R>(fut: F) -> R
where
    F: Future<Output = R>,
{
    ONLY_REPLICA.scope(true, fut).await
}

/// Returns whether the current task-local context indicates this ingester is the only replica
/// handling a request (analogous to `IsOnlyReplicaContext`).
pub fn is_only_replica_context() -> bool {
    ONLY_REPLICA.try_with(|v| *v).unwrap_or(false)
}

/// Request extension marker set by server interceptors when `__only_replica__ == "true"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OnlyReplicaMarker(pub bool);

/// Client interceptor (works for both unary and streaming tonic requests) that injects
/// `__only_replica__: "true"` into outgoing gRPC metadata when `available_replicas == Some(1)`.
#[allow(clippy::result_large_err)]
pub fn only_replica_client_interceptor<T>(mut req: tonic::Request<T>) -> Result<tonic::Request<T>, tonic::Status> {
    if get_available_replicas() == Some(1) {
        req.metadata_mut()
            .insert(ONLY_REPLICA_GRPC_MD_KEY, MetadataValue::from_static("true"));
    }
    Ok(req)
}

/// Extracts whether `__only_replica__` metadata is `"true"`.
pub fn is_only_replica_in_metadata(md: &MetadataMap) -> bool {
    md.get(ONLY_REPLICA_GRPC_MD_KEY)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "true")
}

/// Server interceptor (works for both unary and streaming tonic requests) that extracts
/// `__only_replica__` from incoming metadata and attaches `OnlyReplicaMarker(true)` to extensions.
#[allow(clippy::result_large_err)]
pub fn only_replica_server_interceptor<T>(mut req: tonic::Request<T>) -> Result<tonic::Request<T>, tonic::Status> {
    if is_only_replica_in_metadata(req.metadata()) {
        req.extensions_mut().insert(OnlyReplicaMarker(true));
    }
    Ok(req)
}
