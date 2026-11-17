//! KV client request-duration instrumentation and Go-style registerer wrapping.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/metrics.go (`RegistererWithKVName`,
//!   `getCasErrorCode`, `newMetricsClient`, `metrics.*`)
//! - github.com/grafana/dskit/kv/client.go (`role`, `Primary`, `Secondary`, `role.Labels`)
//! - github.com/grafana/dskit/instrument/instrument.go (`CollectedRequest`, `ErrorCode`)
//! - github.com/prometheus/client_golang/prometheus/wrap.go
//!   (`WrapRegistererWith`, `WrapRegistererWithPrefix`)

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use mimir_common::services::ServiceContext;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{LabelPair, MetricFamily};
use prometheus::{HistogramOpts, HistogramVec, Registry};

use super::{BoxFuture, CasFn, KvClient, KvError, WatchKeyFn, WatchPrefixFn};

/// Emulation of a Go `prometheus.Registerer` obtained by (possibly nested)
/// `prometheus.WrapRegistererWithPrefix` / `prometheus.WrapRegistererWith`
/// calls around a base registry: every collector registered through it gets
/// its metric names prefixed and the extra constant labels attached.
#[derive(Clone)]
pub struct Registerer {
    registry: Registry,
    prefix: String,
    labels: BTreeMap<String, String>,
    /// Label name wrapped twice (Go fails registration in that case).
    conflict: Option<String>,
}

impl Registerer {
    /// Unwrapped registerer writing straight into `registry`.
    pub fn new(registry: &Registry) -> Self {
        Registerer {
            registry: registry.clone(),
            prefix: String::new(),
            labels: BTreeMap::new(),
            conflict: None,
        }
    }

    /// The underlying (unwrapped) registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Accumulated metric-name prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Accumulated constant labels.
    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }

    /// Go `prometheus.WrapRegistererWithPrefix(prefix, r)`.
    pub fn wrap_with_prefix(&self, prefix: &str) -> Self {
        let mut out = self.clone();
        // The outer wrapper prefixes first, then the inner one prefixes again:
        // name = inner_prefix + outer_prefix + name.
        out.prefix = format!("{}{}", self.prefix, prefix);
        out
    }

    /// Go `prometheus.WrapRegistererWith(labels, r)`.
    pub fn wrap_with<I, K, V>(&self, labels: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut out = self.clone();
        for (k, v) in labels {
            let k = k.into();
            if out.labels.contains_key(&k) && out.conflict.is_none() {
                out.conflict = Some(k.clone());
            }
            out.labels.insert(k, v.into());
        }
        out
    }

    /// Go `Registerer.Register`.
    pub fn register(&self, c: Box<dyn Collector>) -> prometheus::Result<()> {
        if let Some(ln) = &self.conflict {
            return Err(prometheus::Error::Msg(format!(
                "attempted wrapping with already existing label name {ln:?}"
            )));
        }
        if self.prefix.is_empty() && self.labels.is_empty() {
            return self.registry.register(c);
        }
        let wrapped = WrappingCollector::new(c, &self.prefix, &self.labels)?;
        self.registry.register(Box::new(wrapped))
    }
}

/// Anything that can act as a Go `prometheus.Registerer`: a plain
/// [`Registry`] (unwrapped) or a [`Registerer`].
pub trait AsRegisterer {
    fn as_registerer(&self) -> Registerer;
}

impl AsRegisterer for Registry {
    fn as_registerer(&self) -> Registerer {
        Registerer::new(self)
    }
}

impl AsRegisterer for Registerer {
    fn as_registerer(&self) -> Registerer {
        self.clone()
    }
}

/// Collector adapter implementing Go's `wrappingCollector`.
struct WrappingCollector {
    inner: Box<dyn Collector>,
    descs: Vec<Desc>,
    prefix: String,
    labels: Vec<(String, String)>,
}

impl WrappingCollector {
    fn new(
        inner: Box<dyn Collector>,
        prefix: &str,
        labels: &BTreeMap<String, String>,
    ) -> prometheus::Result<Self> {
        let mut descs = Vec::new();
        for d in inner.desc() {
            let mut const_labels: HashMap<String, String> = d
                .const_label_pairs
                .iter()
                .map(|lp| (lp.get_name().to_string(), lp.get_value().to_string()))
                .collect();
            for (k, v) in labels {
                if const_labels.contains_key(k) {
                    return Err(prometheus::Error::Msg(format!(
                        "attempted wrapping with already existing label name {k:?}"
                    )));
                }
                const_labels.insert(k.clone(), v.clone());
            }
            descs.push(Desc::new(
                format!("{prefix}{}", d.fq_name),
                d.help.clone(),
                d.variable_labels.clone(),
                const_labels,
            )?);
        }
        Ok(WrappingCollector {
            inner,
            descs,
            prefix: prefix.to_string(),
            labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        })
    }
}

impl Collector for WrappingCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut mfs = self.inner.collect();
        for mf in &mut mfs {
            let name = format!("{}{}", self.prefix, mf.get_name());
            mf.set_name(name);
            for m in mf.mut_metric().iter_mut() {
                let mut pairs: Vec<LabelPair> = m.take_label().into_vec();
                for (k, v) in &self.labels {
                    let mut lp = LabelPair::default();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    pairs.push(lp);
                }
                pairs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                m.set_label(pairs.into());
            }
        }
        mfs
    }
}

/// Go `kv.RegistererWithKVName`: wraps the provided registerer with the
/// `kv_name` label. A `None` registerer stays `None`.
pub fn registerer_with_kv_name(reg: Option<&Registerer>, name: &str) -> Option<Registerer> {
    reg.map(|r| r.wrap_with([("kv_name", name)]))
}

/// Go `kv.role`: the role of a KV store (`role` label of
/// `kv_request_duration_seconds`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Go `kv.Primary`: a role to use KV store primarily.
    Primary,
    /// Go `kv.Secondary`: a role for KV store used by "multi" KV store.
    Secondary,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Primary => "primary",
            Role::Secondary => "secondary",
        }
    }

    /// Go `(*role).Labels()`.
    pub fn labels(&self) -> [(&'static str, &'static str); 1] {
        [("role", self.as_str())]
    }
}

/// Go `instrument.ErrorCode`.
pub fn error_code(err: Option<&KvError>) -> String {
    match err {
        None => "200".to_string(),
        Some(_) => "500".to_string(),
    }
}

/// Go `kv.getCasErrorCode`: converts the provided CAS error into the code
/// that should be used to track the operation in metrics.
pub fn get_cas_error_code(err: Option<&KvError>) -> String {
    match err {
        None => "200".to_string(),
        Some(KvError::HttpGrpc { code, .. }) => code.to_string(),
        // If the error has been returned to abort the CAS operation, then we
        // shouldn't consider it an error when tracking metrics.
        Some(KvError::OperationAborted(_)) => "200".to_string(),
        Some(_) => "500".to_string(),
    }
}

/// Go `kv.metrics`: a [`KvClient`] wrapper tracking
/// `kv_request_duration_seconds{operation,status_code}`.
pub struct MetricsKvClient {
    c: Arc<dyn KvClient>,
    request_duration: HistogramVec,
}

/// Go `newMetricsClient(backend, c, reg)`. Like `promauto`, a registration
/// failure (e.g. a duplicate) is fatal: it is returned as an error.
pub fn new_metrics_client(
    backend: &str,
    c: Arc<dyn KvClient>,
    reg: &Registerer,
) -> Result<Arc<dyn KvClient>, KvError> {
    // NativeHistogramBucketFactor 1.1 / MaxBucketNumber 100 /
    // MinResetDuration 1h are not supported by the Rust prometheus client;
    // only the classic buckets (prometheus.DefBuckets) are exposed.
    let request_duration = HistogramVec::new(
        HistogramOpts::new(
            "kv_request_duration_seconds",
            "Time spent on kv store requests.",
        )
        .buckets(prometheus::DEFAULT_BUCKETS.to_vec())
        .const_label("type", backend),
        &["operation", "status_code"],
    )
    .map_err(|e| KvError::Other(format!("creating kv_request_duration_seconds: {e}")))?;
    reg.register(Box::new(request_duration.clone()))
        .map_err(|e| KvError::Other(format!("registering kv_request_duration_seconds: {e}")))?;
    Ok(Arc::new(MetricsKvClient {
        c,
        request_duration,
    }))
}

impl MetricsKvClient {
    fn observe(&self, method: &str, code: &str, start: Instant) {
        self.request_duration
            .with_label_values(&[method, code])
            .observe(start.elapsed().as_secs_f64());
    }
}

impl KvClient for MetricsKvClient {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.c.list(prefix).await;
            self.observe("List", &error_code(res.as_ref().err()), start);
            res
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.c.get(key).await;
            self.observe("GET", &error_code(res.as_ref().err()), start);
            res
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.c.delete(key).await;
            self.observe("Delete", &error_code(res.as_ref().err()), start);
            res
        })
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.c.cas(key, f).await;
            self.observe("CAS", &get_cas_error_code(res.as_ref().err()), start);
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
            self.c.watch_key(key, cancel, f).await;
            self.observe("WatchKey", &error_code(None), start);
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
            self.c.watch_prefix(prefix, cancel, f).await;
            self.observe("WatchPrefix", &error_code(None), start);
        })
    }
}
