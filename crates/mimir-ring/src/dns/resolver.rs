//! Ported from github.com/grafana/dskit/dns/resolver.go

use std::borrow::Cow;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;

use super::error::DnsError;
use super::net::{go_ip_string, join_host_port, split_host_port};

/// Go `dns.QType` (a plain string type: any value can be constructed, unknown
/// ones are rejected by [`DnsSd`] with `invalid lookup scheme "<q>"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QType(pub Cow<'static, str>);

/// `A` qtype performs A/AAAA lookup.
pub const A: QType = QType(Cow::Borrowed("dns"));
/// `SRV` qtype performs SRV lookup with A/AAAA lookup for each SRV result.
pub const SRV: QType = QType(Cow::Borrowed("dnssrv"));
/// `SRVNoA` qtype performs SRV lookup without any A/AAAA lookup for each SRV result.
pub const SRV_NO_A: QType = QType(Cow::Borrowed("dnssrvnoa"));

impl QType {
    pub fn new(s: impl Into<Cow<'static, str>>) -> Self {
        QType(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Go `dns.Resolver` interface.
pub trait Resolver: Send + Sync {
    /// Resolve performs a DNS lookup and returns a list of records.
    /// `name` is the domain name to be resolved.
    /// `qtype` is the query type. Accepted values are `dns` for A/AAAA lookup
    /// and `dnssrv`/`dnssrvnoa` for SRV lookup.
    /// If scheme is passed through name, it is preserved on IP results.
    fn resolve<'a>(
        &'a self,
        name: &'a str,
        qtype: &'a QType,
    ) -> BoxFuture<'a, Result<Vec<String>, DnsError>>;
}

/// Error returned by an [`IpLookupResolver`] (Go `error`).
pub type LookupError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Go `net.SRV`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Srv {
    pub target: String,
    pub port: u16,
    pub priority: u16,
    pub weight: u16,
}

/// Go `dns.ipLookupResolver` (implemented by `godns.Resolver`), injectable so
/// tests can mock DNS.
pub trait IpLookupResolver: Send + Sync {
    /// Go `LookupIPAddr(ctx, host) ([]net.IPAddr, error)`.
    fn lookup_ip_addr<'a>(
        &'a self,
        host: &'a str,
    ) -> BoxFuture<'a, Result<Vec<IpAddr>, LookupError>>;
    /// Go `LookupSRV(ctx, service, proto, name) (cname string, addrs []*net.SRV, err error)`.
    fn lookup_srv<'a>(
        &'a self,
        service: &'a str,
        proto: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<(String, Vec<Srv>), LookupError>>;
    /// Go `IsNotFound(err error) bool`.
    fn is_not_found(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool;
}

/// Go `dns.dnsSD` (created by `dns.NewResolver`).
#[derive(Clone)]
pub struct DnsSd {
    resolver: Arc<dyn IpLookupResolver>,
}

impl DnsSd {
    /// Go `NewResolver(resolver ipLookupResolver, logger log.Logger) Resolver`.
    pub fn new(resolver: Arc<dyn IpLookupResolver>) -> Self {
        DnsSd { resolver }
    }

    async fn resolve_impl(&self, name: &str, qtype: &QType) -> Result<Vec<String>, DnsError> {
        let mut res: Vec<String> = Vec::new();
        let mut scheme = "";
        let mut name = name;

        // strings.Split(name, "//")
        let mut scheme_split = name.split("//");
        let first = scheme_split.next().unwrap_or("");
        if let Some(second) = scheme_split.next() {
            scheme = first;
            name = second;
        }

        // Split the host and port if present.
        let (host, port, split_ok) = match split_host_port(name) {
            Some((h, p)) => (h, p, true),
            // The host could be missing a port.
            None => (name, "", false),
        };

        match qtype.as_str() {
            "dns" => {
                if port.is_empty() {
                    return Err(DnsError::MissingPort {
                        name: name.to_string(),
                    });
                }
                let ips = match self.resolver.lookup_ip_addr(host).await {
                    Ok(ips) => ips,
                    Err(err) => {
                        // We exclude error from std Golang resolver for the case of the domain
                        // (e.g `NXDOMAIN`) not being found by DNS server. Since `miekg` does not
                        // consider this as an error, when the host cannot be found, empty slice
                        // will be returned.
                        if !self.resolver.is_not_found(err.as_ref()) {
                            return Err(DnsError::LookupIpAddresses {
                                host: host.to_string(),
                                source: Arc::from(err),
                            });
                        }
                        tracing::error!(host = %host, err = %err, "failed to lookup IP addresses");
                        Vec::new()
                    }
                };
                for ip in &ips {
                    res.push(append_scheme(
                        scheme,
                        &join_host_port(&go_ip_string(ip), port),
                    ));
                }
            }
            "dnssrv" | "dnssrvnoa" => {
                let recs = match self.resolver.lookup_srv("", "", host).await {
                    Ok((_, recs)) => recs,
                    Err(err) => {
                        if !self.resolver.is_not_found(err.as_ref()) {
                            return Err(DnsError::LookupSrvRecords {
                                host: host.to_string(),
                                source: Arc::from(err),
                            });
                        }
                        tracing::error!(host = %host, err = %err, "failed to lookup SRV records");
                        Vec::new()
                    }
                };

                for rec in &recs {
                    // Only use port from SRV record if no explicit port was specified.
                    let rec_port;
                    let res_port = if port.is_empty() {
                        rec_port = rec.port.to_string();
                        rec_port.as_str()
                    } else {
                        port
                    };

                    if qtype.as_str() == "dnssrvnoa" {
                        res.push(append_scheme(
                            scheme,
                            &join_host_port(&rec.target, res_port),
                        ));
                        continue;
                    }
                    // Do A lookup for the domain in SRV answer.
                    let res_ips = match self.resolver.lookup_ip_addr(&rec.target).await {
                        Ok(ips) => ips,
                        Err(err) => {
                            if !self.resolver.is_not_found(err.as_ref()) {
                                return Err(DnsError::LookupIpAddresses {
                                    host: host.to_string(),
                                    source: Arc::from(err),
                                });
                            }
                            tracing::error!(srv = %host, a = %rec.target, err = %err, "failed to lookup IP addresses");
                            Vec::new()
                        }
                    };
                    for ip in &res_ips {
                        res.push(append_scheme(
                            scheme,
                            &join_host_port(&go_ip_string(ip), res_port),
                        ));
                    }
                }
            }
            other => {
                return Err(DnsError::InvalidLookupScheme {
                    qtype: other.to_string(),
                })
            }
        }

        // Go checks `res == nil && err == nil` where `err` is the
        // net.SplitHostPort error of the outer scope.
        if res.is_empty() && split_ok {
            tracing::warn!(host = %host, "IP address lookup yielded no results. No host found or no addresses found");
        }

        Ok(res)
    }
}

impl Resolver for DnsSd {
    fn resolve<'a>(
        &'a self,
        name: &'a str,
        qtype: &'a QType,
    ) -> BoxFuture<'a, Result<Vec<String>, DnsError>> {
        self.resolve_impl(name, qtype).boxed()
    }
}

fn append_scheme(scheme: &str, host: &str) -> String {
    if scheme.is_empty() {
        host.to_string()
    } else {
        format!("{scheme}//{host}")
    }
}
