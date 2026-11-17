//! KV client instrumentation (`kv_request_duration_seconds`).
//!
//! Ported from github.com/grafana/dskit/kv/metrics.go

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use mimir_common::services::ServiceContext;
use prometheus::{HistogramOpts, HistogramVec, Registry};

use super::{BoxFuture, CasFn, KvClient, KvError, WatchKeyFn, WatchPrefixFn};

/// Wraps a [`KvClient`] and tracks the duration of every request
/// (`dskit/kv.newMetricsClient`).
pub struct MetricsKvClient {
    inner: Arc<dyn KvClient>,
    request_duration: HistogramVec,
}

impl MetricsKvClient {
    /// `reg` must already carry the `kv_name` / `role` labels and any name prefix (see
    /// `mimir_common::wrapped_registry`), exactly like the registerer passed to Go's
    /// `newMetricsClient`.
    pub fn new(backend: &str, inner: Arc<dyn KvClient>, reg: &Registry) -> Self {
        let request_duration = HistogramVec::new(
            HistogramOpts::new("kv_request_duration_seconds", "Time spent on kv store requests.")
                .const_label("type", backend)
                .buckets(prometheus::DEFAULT_BUCKETS.to_vec()),
            &["operation", "status_code"],
        )
        .expect("valid kv_request_duration_seconds metric");
        let _ = reg.register(Box::new(request_duration.clone()));
        Self {
            inner,
            request_duration,
        }
    }

    fn observe(&self, operation: &str, status_code: &str, start: Instant) {
        self.request_duration
            .with_label_values(&[operation, status_code])
            .observe(start.elapsed().as_secs_f64());
    }
}

/// `instrument.ErrorCode`.
fn error_code<T>(res: &Result<T, KvError>) -> &'static str {
    if res.is_ok() {
        "200"
    } else {
        "500"
    }
}

impl KvClient for MetricsKvClient {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.inner.list(prefix).await;
            self.observe("List", error_code(&res), start);
            res
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.inner.get(key).await;
            self.observe("GET", error_code(&res), start);
            res
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.inner.delete(key).await;
            self.observe("Delete", error_code(&res), start);
            res
        })
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.inner.cas(key, f).await;
            // `getCasErrorCode`: only httpgrpc errors carry their own status code, which none of
            // the Rust KV backends produce; every other error is a 500.
            self.observe("CAS", error_code(&res), start);
            res
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        f: WatchKeyFn,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let start = Instant::now();
            self.inner.watch_key(key, cancel, f).await;
            self.observe("WatchKey", "200", start);
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        f: WatchPrefixFn,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let start = Instant::now();
            self.inner.watch_prefix(prefix, cancel, f).await;
            self.observe("WatchPrefix", "200", start);
        })
    }
}
