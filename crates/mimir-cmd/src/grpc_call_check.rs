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

/// Ported from `github.com/grafana/dskit/httpgrpc/httpgrpc.go` (`AppendRequestMetadataToContext`).
pub fn append_request_metadata_to_context(
    md: &mut MetadataMap,
    req: &mimir_proto::httpgrpcpb::HttpRequest,
) {
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(req.method.as_str()) {
        md.append(HTTPGRPC_METADATA_METHOD, val);
    }
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(req.url.as_str()) {
        md.append(HTTPGRPC_METADATA_URL, val);
    }
}

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
                // Upstream pkg/mimir/grpc_call_check.go panics on nil receiver in RPCCallFinished,
                // asserted directly by TestGrpcInflightMethodLimiter.
                let ing = self
                    .get_ingester
                    .as_ref()
                    .and_then(|f| f())
                    .expect("ingester receiver missing on RPCCallFinished");
                ing.finish_push_request();
            }
            Some(RpcCallKind::DistributorPush) => {
                // Upstream pkg/mimir/grpc_call_check.go panics on nil receiver in RPCCallFinished,
                // asserted directly by TestGrpcInflightMethodLimiter.
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

// ============================================================================
// github.com/grafana/dskit/server/limits.go (GrpcInflightLimitCheck)
// ============================================================================

pub const UNPROCESSED_REQUEST_CHECK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

/// Ported from `dskit/server/limits.go` (`GrpcInflightMethodLimiter`).
pub trait DskitGrpcMethodLimiter: Send + Sync + 'static {
    type CallCtx: Clone + Send + Sync + 'static;

    #[allow(clippy::result_large_err)]
    fn rpc_call_starting(
        &self,
        method_name: &str,
        md: &MetadataMap,
    ) -> Result<Self::CallCtx, Status>;

    #[allow(clippy::result_large_err)]
    fn rpc_call_processing(
        &self,
        ctx: &Self::CallCtx,
        method_name: &str,
    ) -> Result<Option<FinishFn>, Status>;

    fn rpc_call_finished(&self, ctx: &Self::CallCtx);
}

/// Ported from `dskit/server/limits.go` (`isMethodNameValid`).
pub fn is_method_name_valid(mut method: &str) -> bool {
    if let Some(stripped) = method.strip_prefix('/') {
        method = stripped;
    }
    method.rfind('/').is_some()
}

/// Mockable timer handle matching `dskit/server/limits.go` (`testableTimer`).
pub struct TestableTimer {
    stop_fn: Box<dyn FnOnce() -> bool + Send + Sync>,
}

impl TestableTimer {
    pub fn new(stop_fn: impl FnOnce() -> bool + Send + Sync + 'static) -> Self {
        Self {
            stop_fn: Box::new(stop_fn),
        }
    }

    pub fn stop(self) -> bool {
        (self.stop_fn)()
    }
}

pub type TimeAfterFuncMock = Arc<
    dyn Fn(std::time::Duration, Box<dyn FnOnce() + Send + Sync>) -> TestableTimer + Send + Sync,
>;

/// Per-RPC state tracked between `TapHandle`, `HandleRPC(InHeader)`, and `HandleRPC(End)`.
pub struct LimitCheckerState<C> {
    pub call_ctx: C,
    pub full_method: String,
    headers_processed: std::sync::atomic::AtomicBool,
    rpc_call_finished_once: std::sync::Once,
    non_processed_timer: parking_lot::Mutex<Option<TestableTimer>>,
}

impl<C> LimitCheckerState<C> {
    pub fn mark_headers_processed(&self) {
        self.headers_processed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn headers_were_processed(&self) -> bool {
        self.headers_processed
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Ported from `github.com/grafana/dskit/server/limits.go` (`grpcInflightLimitCheck`).
pub struct GrpcInflightLimitCheck<L: DskitGrpcMethodLimiter> {
    limiter: Arc<L>,
    pub time_after_func_mock: Option<TimeAfterFuncMock>,
}

impl<L: DskitGrpcMethodLimiter> GrpcInflightLimitCheck<L> {
    pub fn new(limiter: Arc<L>) -> Self {
        Self {
            limiter,
            time_after_func_mock: None,
        }
    }

    pub fn with_time_after_mock(mut self, mock: TimeAfterFuncMock) -> Self {
        self.time_after_func_mock = Some(mock);
        self
    }

    #[allow(clippy::result_large_err)]
    pub fn tap_handle(
        &self,
        full_method_name: &str,
        md: &MetadataMap,
        is_ctx_done: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Option<Arc<LimitCheckerState<L::CallCtx>>>, Status> {
        if !is_method_name_valid(full_method_name) {
            return Ok(None);
        }
        let call_ctx = self.limiter.rpc_call_starting(full_method_name, md)?;
        let state = Arc::new(LimitCheckerState {
            call_ctx,
            full_method: full_method_name.to_string(),
            headers_processed: std::sync::atomic::AtomicBool::new(false),
            rpc_call_finished_once: std::sync::Once::new(),
            non_processed_timer: parking_lot::Mutex::new(None),
        });

        let limiter_cb = Arc::clone(&self.limiter);
        let state_cb = Arc::clone(&state);
        let is_done_cb = Arc::clone(&is_ctx_done);
        let check_cb: Box<dyn FnOnce() + Send + Sync> = Box::new(move || {
            if state_cb.headers_were_processed() {
                return;
            }
            if is_done_cb() {
                state_cb.rpc_call_finished_once.call_once(|| {
                    limiter_cb.rpc_call_finished(&state_cb.call_ctx);
                });
            }
        });

        let timer = if let Some(ref mock) = self.time_after_func_mock {
            mock(UNPROCESSED_REQUEST_CHECK_TIMEOUT, check_cb)
        } else {
            let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let aborted_c = Arc::clone(&aborted);
            std::thread::spawn(move || {
                std::thread::sleep(UNPROCESSED_REQUEST_CHECK_TIMEOUT);
                if !aborted_c.load(std::sync::atomic::Ordering::Acquire) {
                    check_cb();
                }
            });
            TestableTimer::new(move || !aborted.swap(true, std::sync::atomic::Ordering::AcqRel))
        };
        *state.non_processed_timer.lock() = Some(timer);
        Ok(Some(state))
    }

    pub fn handle_rpc_in_header(&self, state: &LimitCheckerState<L::CallCtx>) {
        if let Some(timer) = state.non_processed_timer.lock().take() {
            if !timer.stop() {
                state.mark_headers_processed();
            }
        }
    }

    pub fn handle_rpc_end(&self, state: &LimitCheckerState<L::CallCtx>) {
        state.rpc_call_finished_once.call_once(|| {
            self.limiter.rpc_call_finished(&state.call_ctx);
        });
    }

    #[allow(clippy::result_large_err)]
    pub fn rpc_call_processing(
        &self,
        state: &LimitCheckerState<L::CallCtx>,
        method_name: &str,
    ) -> Result<Option<FinishFn>, Status> {
        self.limiter.rpc_call_processing(&state.call_ctx, method_name)
    }
}
