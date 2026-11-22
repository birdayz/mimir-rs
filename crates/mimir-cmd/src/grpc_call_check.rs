//! gRPC inflight method limiter (`TapHandle` + `stats.Handler` lifecycle).
//!
//! Ported from github.com/grafana/mimir/pkg/mimir/grpc_call_check.go

use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::Status;

pub const METADATA_MESSAGE_SIZE: &str = "message-size";
pub const HTTPGRPC_METADATA_METHOD: &str = "httpgrpc-method";
pub const HTTPGRPC_METADATA_URL: &str = "httpgrpc-url";

pub const INGESTER_METHOD_PREFIX: &str = "/cortex.Ingester";
pub const INGESTER_PUSH_METHOD: &str = "/cortex.Ingester/Push";
pub const HTTPGRPC_HANDLE_METHOD: &str = "/httpgrpc.HTTP/Handle";

pub const PROMETHEUS_PUSH_ENDPOINT: &str = "/api/v1/push";
pub const OTLP_PUSH_ENDPOINT: &str = "/otlp/v1/metrics";

pub type FinishFn = Box<dyn FnOnce(Option<&Status>) + Send>;

pub trait PushReceiver: Send + Sync {
    fn start_push_request(&self, request_size: i64) -> Result<(), String>;
    fn prepare_push_request(&self) -> Result<Option<FinishFn>, String>;
    fn finish_push_request(&self);
}

pub trait IngesterReceiver: PushReceiver {
    fn start_read_request(&self) -> Result<(), String>;
    fn prepare_read_request(&self) -> Result<Option<FinishFn>, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcCallKind {
    IngesterPush,
    IngesterRead,
    DistributorPush,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CallContext {
    pub kind: Option<RpcCallKind>,
}

pub type GetIngesterFn = Arc<dyn Fn() -> Option<Arc<dyn IngesterReceiver>> + Send + Sync>;
pub type GetDistributorFn = Arc<dyn Fn() -> Option<Arc<dyn PushReceiver>> + Send + Sync>;

pub struct GrpcInflightMethodLimiter {
    get_ingester: Option<GetIngesterFn>,
    get_distributor: Option<GetDistributorFn>,
}

impl GrpcInflightMethodLimiter {
    pub fn new(
        get_ingester: Option<GetIngesterFn>,
        get_distributor: Option<GetDistributorFn>,
    ) -> Self {
        Self {
            get_ingester,
            get_distributor,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn rpc_call_starting(
        &self,
        method_name: &str,
        md: Option<&MetadataMap>,
    ) -> Result<CallContext, Status> {
        if let Some(ref get_ing) = self.get_ingester {
            if method_name.starts_with(INGESTER_METHOD_PREFIX) {
                let Some(ing) = get_ing() else {
                    return Err(Status::unavailable("no ingester"));
                };
                let (res, kind) = if method_name == INGESTER_PUSH_METHOD {
                    let size = md.map_or(0, |m| get_message_size(m, METADATA_MESSAGE_SIZE));
                    (ing.start_push_request(size), RpcCallKind::IngesterPush)
                } else {
                    (ing.start_read_request(), RpcCallKind::IngesterRead)
                };
                if let Err(err) = res {
                    return Err(Status::unavailable(err));
                }
                return Ok(CallContext { kind: Some(kind) });
            }
        }

        if let Some(ref get_dist) = self.get_distributor {
            if method_name == HTTPGRPC_HANDLE_METHOD {
                let http_method = md
                    .map(|m| get_single_metadata(m, HTTPGRPC_METADATA_METHOD))
                    .unwrap_or_default();
                let http_url = md
                    .map(|m| get_single_metadata(m, HTTPGRPC_METADATA_URL))
                    .unwrap_or_default();

                if http_method == "POST"
                    && (http_url.ends_with(PROMETHEUS_PUSH_ENDPOINT)
                        || http_url.ends_with(OTLP_PUSH_ENDPOINT))
                {
                    let Some(dist) = get_dist() else {
                        return Err(Status::unavailable("no distributor"));
                    };
                    let size = md.map_or(0, |m| get_message_size(m, METADATA_MESSAGE_SIZE));
                    if let Err(err) = dist.start_push_request(size) {
                        return Err(Status::unavailable(err));
                    }
                    return Ok(CallContext {
                        kind: Some(RpcCallKind::DistributorPush),
                    });
                }
            }
        }

        Ok(CallContext { kind: None })
    }

    #[allow(clippy::result_large_err)]
    pub fn rpc_call_processing(
        &self,
        ctx: CallContext,
        _method_name: &str,
    ) -> Result<Option<FinishFn>, Status> {
        match ctx.kind {
            Some(RpcCallKind::IngesterPush) => {
                if let Some(ing) = self.get_ingester.as_ref().and_then(|f| f()) {
                    return ing.prepare_push_request().map_err(Status::unavailable);
                }
            }
            Some(RpcCallKind::IngesterRead) => {
                if let Some(ing) = self.get_ingester.as_ref().and_then(|f| f()) {
                    return ing.prepare_read_request().map_err(Status::unavailable);
                }
            }
            _ => {}
        }
        Ok(None)
    }

    pub fn rpc_call_finished(&self, ctx: CallContext) {
        match ctx.kind {
            Some(RpcCallKind::IngesterPush) => {
                let ing = self
                    .get_ingester
                    .as_ref()
                    .and_then(|f| f())
                    .expect("ingester receiver missing on RPCCallFinished");
                ing.finish_push_request();
            }
            Some(RpcCallKind::DistributorPush) => {
                let dist = self
                    .get_distributor
                    .as_ref()
                    .and_then(|f| f())
                    .expect("distributor receiver missing on RPCCallFinished");
                dist.finish_push_request();
            }
            _ => {}
        }
    }
}

fn get_single_metadata(md: &MetadataMap, key: &str) -> String {
    let values: Vec<_> = md.get_all(key).iter().collect();
    if values.len() != 1 {
        return String::new();
    }
    values[0].to_str().unwrap_or_default().to_string()
}

fn get_message_size(md: &MetadataMap, key: &str) -> i64 {
    let val = get_single_metadata(md, key);
    if val.is_empty() {
        return 0;
    }
    val.parse::<i64>().unwrap_or(0)
}
