//! Ported from github.com/grafana/dskit/dns/provider.go

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use parking_lot::RwLock;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{Counter, Opts, Registry};

use super::error::DnsError;
use super::godns;
use super::resolver::{DnsSd, QType, Resolver};

/// Go `dns.ResolverType` (a plain string type; the zero value is `""`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ResolverType(pub Cow<'static, str>);

/// Go `dns.GolangResolverType`.
pub const GOLANG_RESOLVER_TYPE: ResolverType = ResolverType(Cow::Borrowed("golang"));
/// Go `dns.MiekgdnsResolverType`.
pub const MIEKGDNS_RESOLVER_TYPE: ResolverType = ResolverType(Cow::Borrowed("miekgdns"));

impl ResolverType {
    /// Go `(*ResolverType).Set` (flag.Value).
    pub fn set(&mut self, v: &str) -> Result<(), DnsError> {
        match v {
            "golang" | "miekgdns" => {
                self.0 = Cow::Owned(v.to_string());
                Ok(())
            }
            _ => Err(DnsError::Other(format!("unsupported resolver type {v}"))),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Go `(ResolverType).String`.
impl fmt::Display for ResolverType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for ResolverType {
    type Err = DnsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut t = ResolverType::default();
        t.set(s)?;
        Ok(t)
    }
}

/// Object-safe view of a DNS provider, mirroring dskit
/// `kv/memberlist.DNSProvider`:
///
/// ```go
/// type DNSProvider interface {
///     Resolve(ctx context.Context, addrs []string) error
///     Addresses() []string
/// }
/// ```
pub trait DnsProvider: Send + Sync {
    fn resolve<'a>(&'a self, addrs: &'a [String]) -> BoxFuture<'a, Result<(), DnsError>>;
    fn addresses(&self) -> Vec<String>;
}

type ResolvedMap = Arc<RwLock<HashMap<String, Vec<String>>>>;

/// Go `dns.Provider`: a stateful cache for asynchronous DNS resolutions.
pub struct Provider {
    resolver: Arc<dyn Resolver>,
    /// A map from domain name to a slice of resolved targets.
    resolved: ResolvedMap,
    resolver_lookups_count: Counter,
    resolver_failures_count: Counter,
}

/// Go `Provider.Describe/Collect` for `dns_provider_results`.
struct ResultsCollector {
    desc: Desc,
    resolved: ResolvedMap,
}

impl Collector for ResultsCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let resolved = self.resolved.read();
        if resolved.is_empty() {
            return Vec::new();
        }
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(MetricType::GAUGE);
        for (name, addrs) in resolved.iter() {
            let mut pairs: Vec<LabelPair> = self.desc.const_label_pairs.clone();
            let mut addr = LabelPair::default();
            addr.set_name("addr".to_string());
            addr.set_value(name.clone());
            pairs.push(addr);
            pairs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
            let mut g = Gauge::default();
            g.set_value(addrs.len() as f64);
            let mut m = Metric::default();
            m.set_label(pairs.into());
            m.set_gauge(g);
            mf.mut_metric().push(m);
        }
        vec![mf]
    }
}

fn registration_error(e: prometheus::Error) -> DnsError {
    DnsError::Other(format!("failed to register DNS provider metrics: {e}"))
}

impl Provider {
    /// Go `NewProvider(resolverType, maxIdleConnections, logger, reg)`.
    ///
    /// `metric_prefix` and `const_labels` reproduce
    /// `prometheus.WrapRegistererWithPrefix` / `prometheus.WrapRegistererWith`
    /// around `reg` (Mimir's memberlist uses prefix `cortex_` and
    /// `component="memberlist"`). As with Go `MustRegister`, registration
    /// conflicts are fatal (returned as an error here).
    ///
    /// `miekgdns` is not ported and returns an explicit error; any other
    /// unknown type falls back to the Go resolver with a warning, like dskit.
    pub fn new(
        resolver_type: &ResolverType,
        max_idle_connections: u32,
        registry: Option<&Registry>,
        metric_prefix: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Self, DnsError> {
        let _ = max_idle_connections; // only used by the miekgdns resolver.
        let backend: Arc<godns::Resolver> = match resolver_type.as_str() {
            "golang" => Arc::new(godns::Resolver::new()),
            "miekgdns" => {
                return Err(DnsError::Other(
                    "dns resolver type \"miekgdns\" is not implemented in mimir-rs (only \"golang\" is supported)".to_string(),
                ))
            }
            other => {
                tracing::warn!(r#type = %other, "no such resolver type, defaulting to golang");
                Arc::new(godns::Resolver::new())
            }
        };
        Self::with_resolver(
            Arc::new(DnsSd::new(backend)),
            registry,
            metric_prefix,
            const_labels,
        )
    }

    /// Like [`Provider::new`] but with an explicit [`Resolver`] (Go tests set
    /// `prv.resolver` directly).
    pub fn with_resolver(
        resolver: Arc<dyn Resolver>,
        registry: Option<&Registry>,
        metric_prefix: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Self, DnsError> {
        let resolved: ResolvedMap = Arc::new(RwLock::new(HashMap::new()));
        let lookups = Counter::with_opts(
            Opts::new(
                format!("{metric_prefix}dns_lookups_total"),
                "The number of DNS lookups resolutions attempts",
            )
            .const_labels(const_labels.clone()),
        )
        .map_err(registration_error)?;
        let failures = Counter::with_opts(
            Opts::new(
                format!("{metric_prefix}dns_failures_total"),
                "The number of DNS lookup failures",
            )
            .const_labels(const_labels.clone()),
        )
        .map_err(registration_error)?;
        let desc = Desc::new(
            format!("{metric_prefix}dns_provider_results"),
            "The number of resolved endpoints for each configured address".to_string(),
            vec!["addr".to_string()],
            const_labels,
        )
        .map_err(registration_error)?;

        if let Some(reg) = registry {
            reg.register(Box::new(lookups.clone()))
                .map_err(registration_error)?;
            reg.register(Box::new(failures.clone()))
                .map_err(registration_error)?;
            reg.register(Box::new(ResultsCollector {
                desc,
                resolved: resolved.clone(),
            }))
            .map_err(registration_error)?;
        }

        Ok(Provider {
            resolver,
            resolved,
            resolver_lookups_count: lookups,
            resolver_failures_count: failures,
        })
    }

    /// Go `(*Provider).Clone`: a new provider sharing resolver and counters
    /// but with an empty resolved map (not registered as a collector).
    pub fn clone_provider(&self) -> Provider {
        Provider {
            resolver: self.resolver.clone(),
            resolved: Arc::new(RwLock::new(HashMap::new())),
            resolver_lookups_count: self.resolver_lookups_count.clone(),
            resolver_failures_count: self.resolver_failures_count.clone(),
        }
    }

    /// Go `(*Provider).Resolve`: stores a list of provided addresses or their
    /// DNS records if requested. Addresses prefixed with `dns+` or `dnssrv+`
    /// will be resolved through respective DNS lookup (A/AAAA or SRV). For
    /// non-SRV records, it will return an error if a port is not supplied.
    /// On a failed lookup the previously resolved targets for that address
    /// are kept; all failures are returned as a multierror.
    pub async fn resolve(&self, addrs: &[String]) -> Result<(), DnsError> {
        let mut resolved_addrs: HashMap<String, Vec<String>> = HashMap::new();
        let mut errs: Vec<DnsError> = Vec::new();

        for addr in addrs {
            let (qtype, name) = get_qtype_name(addr);
            if qtype.is_empty() {
                resolved_addrs.insert(name.to_string(), vec![name.to_string()]);
                continue;
            }

            let qtype = QType::new(qtype.to_string());
            let res = self.resolver.resolve(name, &qtype).await;
            self.resolver_lookups_count.inc();
            let resolved = match res {
                Ok(r) => r,
                Err(err) => {
                    // Append all the failed dns resolution in the error list.
                    errs.push(err);
                    // The DNS resolution failed. Continue without modifying the old records.
                    self.resolver_failures_count.inc();
                    // Use cached values.
                    self.resolved.read().get(addr).cloned().unwrap_or_default()
                }
            };
            resolved_addrs.insert(addr.clone(), resolved);
        }

        // All addresses have been resolved. We can now take an exclusive lock
        // to update the local state.
        *self.resolved.write() = resolved_addrs;

        match DnsError::multi(errs) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Go `(*Provider).Addresses`: the latest addresses present in the Provider.
    pub fn addresses(&self) -> Vec<String> {
        let resolved = self.resolved.read();
        let mut result = Vec::new();
        for addrs in resolved.values() {
            result.extend(addrs.iter().cloned());
        }
        result
    }
}

impl DnsProvider for Provider {
    fn resolve<'a>(&'a self, addrs: &'a [String]) -> BoxFuture<'a, Result<(), DnsError>> {
        Provider::resolve(self, addrs).boxed()
    }

    fn addresses(&self) -> Vec<String> {
        Provider::addresses(self)
    }
}

/// Go `IsDynamicNode`: returns if the specified addr uses any kind of SD
/// mechanism.
pub fn is_dynamic_node(addr: &str) -> bool {
    let (qtype, _) = get_qtype_name(addr);
    !qtype.is_empty()
}

/// Go `GetQTypeName`: splits the provided addr into two parts: the QType (if
/// any) and the name.
pub fn get_qtype_name(addr: &str) -> (&str, &str) {
    match addr.split_once('+') {
        Some((qtype, name)) => (qtype, name),
        None => ("", addr),
    }
}
