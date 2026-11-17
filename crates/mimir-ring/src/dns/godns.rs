//! Go-resolver-compatible [`IpLookupResolver`] backed by hickory-resolver.
//!
//! Ported from:
//! - github.com/grafana/dskit/dns/godns/resolver.go
//! - Go standard library `net.DefaultResolver` semantics relied upon by dskit:
//!   `LookupIPAddr` returns both A and AAAA records, `/etc/hosts` is consulted
//!   before DNS, no response caching, `/etc/resolv.conf` and `/etc/hosts` are
//!   re-read when they change (checked at most every 5s), NXDOMAIN and empty
//!   answers are `*net.DNSError{IsNotFound: true}` ("no such host"), SRV
//!   records are ordered per RFC 2782 (net/dnsclient.go `byPriorityWeight`),
//!   and the default name servers when `/etc/resolv.conf` is unreadable are
//!   `127.0.0.1:53` and `[::1]:53` (net/dnsconfig.go `defaultNS`).

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures::future::BoxFuture;
use futures::FutureExt;
use hickory_resolver::config::{
    LookupIpStrategy, NameServerConfigGroup, ResolveHosts, ResolverConfig,
};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::proto::ProtoErrorKind;
use hickory_resolver::{ResolveError, TokioResolver};
use parking_lot::Mutex;
use rand::Rng;

use super::resolver::{IpLookupResolver, LookupError, Srv};

const RESOLV_CONF_PATH: &str = "/etc/resolv.conf";
const HOSTS_PATH: &str = "/etc/hosts";
/// Go re-checks resolv.conf / hosts at most every 5 seconds.
const RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Go `*net.DNSError` (subset used by dskit).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoDnsError {
    /// Description of the error.
    pub err: String,
    /// Name looked for.
    pub name: String,
    /// Server used (may be empty).
    pub server: String,
    pub is_timeout: bool,
    /// Host could not be found.
    pub is_not_found: bool,
}

/// Go `errNoSuchHost`.
pub const ERR_NO_SUCH_HOST: &str = "no such host";

impl fmt::Display for GoDnsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lookup {}", self.name)?;
        if !self.server.is_empty() {
            write!(f, " on {}", self.server)?;
        }
        write!(f, ": {}", self.err)
    }
}

impl std::error::Error for GoDnsError {}

impl GoDnsError {
    fn from_resolve_error(name: &str, e: &ResolveError) -> Self {
        if e.is_no_records_found() || e.is_nx_domain() {
            return GoDnsError {
                err: ERR_NO_SUCH_HOST.to_string(),
                name: name.to_string(),
                server: String::new(),
                is_timeout: false,
                is_not_found: true,
            };
        }
        let is_timeout = e
            .proto()
            .map(|p| matches!(p.kind(), ProtoErrorKind::Timeout))
            .unwrap_or(false);
        GoDnsError {
            err: if is_timeout {
                "i/o timeout".to_string()
            } else {
                e.to_string()
            },
            name: name.to_string(),
            server: String::new(),
            is_timeout,
            is_not_found: false,
        }
    }
}

struct State {
    resolver: Arc<TokioResolver>,
    last_checked: Instant,
    resolv_conf_mtime: Option<SystemTime>,
    hosts_mtime: Option<SystemTime>,
}

/// Go `godns.Resolver{Resolver: net.DefaultResolver}`.
pub struct Resolver {
    state: Mutex<State>,
}

impl Default for Resolver {
    fn default() -> Self {
        Self::new()
    }
}

fn mtime(path: &str) -> Option<SystemTime> {
    std::fs::metadata(Path::new(path))
        .and_then(|m| m.modified())
        .ok()
}

fn build_resolver() -> TokioResolver {
    let mut builder = match TokioResolver::builder_tokio() {
        Ok(b) => b,
        Err(err) => {
            tracing::debug!(err = %err, "failed to read system DNS configuration, using Go default name servers");
            // net/dnsconfig.go: defaultNS = []string{"127.0.0.1:53", "[::1]:53"}
            let ns = NameServerConfigGroup::from_ips_clear(
                &[
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    IpAddr::V6(Ipv6Addr::LOCALHOST),
                ],
                53,
                true,
            );
            TokioResolver::builder_with_config(
                ResolverConfig::from_parts(None, vec![], ns),
                TokioConnectionProvider::default(),
            )
        }
    };
    let opts = builder.options_mut();
    // net.Resolver.LookupIPAddr returns both A and AAAA records.
    opts.ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
    // Go's pure resolver consults /etc/hosts first ("files dns").
    opts.use_hosts_file = ResolveHosts::Always;
    // Go's resolver does not cache answers.
    opts.cache_size = 0;
    opts.positive_max_ttl = Some(Duration::ZERO);
    opts.negative_max_ttl = Some(Duration::ZERO);
    builder.build()
}

impl Resolver {
    pub fn new() -> Self {
        Resolver {
            state: Mutex::new(State {
                resolver: Arc::new(build_resolver()),
                last_checked: Instant::now(),
                resolv_conf_mtime: mtime(RESOLV_CONF_PATH),
                hosts_mtime: mtime(HOSTS_PATH),
            }),
        }
    }

    fn current(&self) -> Arc<TokioResolver> {
        let mut st = self.state.lock();
        if st.last_checked.elapsed() >= RECHECK_INTERVAL {
            st.last_checked = Instant::now();
            let rc = mtime(RESOLV_CONF_PATH);
            let hosts = mtime(HOSTS_PATH);
            if rc != st.resolv_conf_mtime || hosts != st.hosts_mtime {
                st.resolv_conf_mtime = rc;
                st.hosts_mtime = hosts;
                st.resolver = Arc::new(build_resolver());
            }
        }
        st.resolver.clone()
    }

    async fn lookup_ip_addr_impl(&self, host: &str) -> Result<Vec<IpAddr>, LookupError> {
        // net/lookup.go lookupIPAddr: make sure that no matter what we do
        // later, host=="" is rejected.
        if host.is_empty() {
            return Err(Box::new(GoDnsError {
                err: ERR_NO_SUCH_HOST.to_string(),
                name: host.to_string(),
                server: String::new(),
                is_timeout: false,
                is_not_found: true,
            }));
        }
        let resolver = self.current();
        match resolver.lookup_ip(host).await {
            Ok(lookup) => {
                let ips: Vec<IpAddr> = lookup.iter().collect();
                if ips.is_empty() {
                    return Err(Box::new(GoDnsError {
                        err: ERR_NO_SUCH_HOST.to_string(),
                        name: host.to_string(),
                        server: String::new(),
                        is_timeout: false,
                        is_not_found: true,
                    }));
                }
                Ok(ips)
            }
            Err(e) => Err(Box::new(GoDnsError::from_resolve_error(host, &e))),
        }
    }

    async fn lookup_srv_impl(
        &self,
        service: &str,
        proto: &str,
        name: &str,
    ) -> Result<(String, Vec<Srv>), LookupError> {
        // net/lookup_unix.go / dnsclient_unix.go goLookupSRV.
        let target = if service.is_empty() && proto.is_empty() {
            name.to_string()
        } else {
            format!("_{service}._{proto}.{name}")
        };
        let resolver = self.current();
        match resolver.srv_lookup(target.as_str()).await {
            Ok(lookup) => {
                let cname = lookup.query().name().to_ascii();
                let mut srvs: Vec<Srv> = lookup
                    .iter()
                    .map(|s| Srv {
                        target: s.target().to_ascii(),
                        port: s.port(),
                        priority: s.priority(),
                        weight: s.weight(),
                    })
                    .collect();
                if srvs.is_empty() {
                    return Err(Box::new(GoDnsError {
                        err: ERR_NO_SUCH_HOST.to_string(),
                        name: target,
                        server: String::new(),
                        is_timeout: false,
                        is_not_found: true,
                    }));
                }
                sort_by_priority_weight(&mut srvs, &mut rand::thread_rng());
                Ok((cname, srvs))
            }
            Err(e) => Err(Box::new(GoDnsError::from_resolve_error(&target, &e))),
        }
    }
}

impl IpLookupResolver for Resolver {
    fn lookup_ip_addr<'a>(
        &'a self,
        host: &'a str,
    ) -> BoxFuture<'a, Result<Vec<IpAddr>, LookupError>> {
        self.lookup_ip_addr_impl(host).boxed()
    }

    fn lookup_srv<'a>(
        &'a self,
        service: &'a str,
        proto: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<(String, Vec<Srv>), LookupError>> {
        self.lookup_srv_impl(service, proto, name).boxed()
    }

    /// Go `godns.(*Resolver).IsNotFound`: `errors.Cause(err).(*net.DNSError).IsNotFound`.
    fn is_not_found(&self, err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
        let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = cur {
            if let Some(dns_err) = e.downcast_ref::<GoDnsError>() {
                return dns_err.is_not_found;
            }
            cur = e.source();
        }
        false
    }
}

/// Port of Go `net.byPriorityWeight.shuffleByWeight` (RFC 2782).
fn shuffle_by_weight(addrs: &mut [Srv], rng: &mut impl Rng) {
    let mut sum: u64 = addrs.iter().map(|a| u64::from(a.weight)).sum();
    let mut addrs = addrs;
    while sum > 0 && addrs.len() > 1 {
        let mut s = 0u64;
        let n = rng.gen_range(0..sum);
        for i in 0..addrs.len() {
            s += u64::from(addrs[i].weight);
            if s > n {
                if i > 0 {
                    addrs.swap(0, i);
                }
                break;
            }
        }
        sum -= u64::from(addrs[0].weight);
        addrs = &mut addrs[1..];
    }
}

/// Port of Go `net.byPriorityWeight.sort` (RFC 2782).
fn sort_by_priority_weight(addrs: &mut [Srv], rng: &mut impl Rng) {
    addrs.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.weight.cmp(&b.weight)));
    let mut i = 0;
    for j in 1..addrs.len() {
        if addrs[i].priority != addrs[j].priority {
            shuffle_by_weight(&mut addrs[i..j], rng);
            i = j;
        }
    }
    if !addrs.is_empty() {
        shuffle_by_weight(&mut addrs[i..], rng);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn srv(target: &str, priority: u16, weight: u16) -> Srv {
        Srv {
            target: target.to_string(),
            port: 0,
            priority,
            weight,
        }
    }

    #[test]
    fn sort_by_priority_weight_orders_by_priority() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let mut addrs = vec![
                srv("c", 20, 5),
                srv("a", 10, 1),
                srv("b", 10, 9),
                srv("d", 30, 0),
                srv("e", 20, 0),
            ];
            sort_by_priority_weight(&mut addrs, &mut rng);
            let prios: Vec<u16> = addrs.iter().map(|a| a.priority).collect();
            assert_eq!(prios, vec![10, 10, 20, 20, 30]);
            // RFC 2782: within a priority, a zero-weight record is only
            // picked once all weighted records have been picked.
            assert_eq!(addrs[2].target, "c");
            assert_eq!(addrs[3].target, "e");
        }
    }

    #[test]
    fn go_dns_error_display_matches_go() {
        let e = GoDnsError {
            err: ERR_NO_SUCH_HOST.into(),
            name: "foo.invalid".into(),
            server: "127.0.0.53:53".into(),
            is_timeout: false,
            is_not_found: true,
        };
        assert_eq!(
            e.to_string(),
            "lookup foo.invalid on 127.0.0.53:53: no such host"
        );
        let e = GoDnsError {
            server: String::new(),
            ..e
        };
        assert_eq!(e.to_string(), "lookup foo.invalid: no such host");
    }

    #[test]
    fn is_not_found_matches_godns() {
        let r = Resolver::new();
        let nf: LookupError = Box::new(GoDnsError {
            err: ERR_NO_SUCH_HOST.into(),
            name: "x".into(),
            server: String::new(),
            is_timeout: false,
            is_not_found: true,
        });
        assert!(r.is_not_found(nf.as_ref()));
        let other: LookupError = Box::new(GoDnsError {
            err: "server misbehaving".into(),
            name: "x".into(),
            server: String::new(),
            is_timeout: false,
            is_not_found: false,
        });
        assert!(!r.is_not_found(other.as_ref()));
        let plain: LookupError = "boom".into();
        assert!(!r.is_not_found(plain.as_ref()));
    }

    #[tokio::test]
    async fn resolves_localhost_via_hosts_file() {
        let r = Resolver::new();
        let ips = r
            .lookup_ip_addr("localhost")
            .await
            .expect("localhost must resolve");
        assert!(ips.iter().any(|ip| ip.is_loopback()), "{ips:?}");
    }

    #[tokio::test]
    async fn resolves_ip_literals() {
        let r = Resolver::new();
        assert_eq!(
            r.lookup_ip_addr("127.0.0.1").await.unwrap(),
            vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]
        );
        assert_eq!(
            r.lookup_ip_addr("::1").await.unwrap(),
            vec![IpAddr::V6(Ipv6Addr::LOCALHOST)]
        );
    }

    /// Requires a working system DNS resolver: only runs when
    /// `MIMIR_RS_NETWORK_TESTS=1` (`#[ignore]` is forbidden in this repo).
    #[tokio::test]
    async fn network_nxdomain_is_not_found_and_real_names_resolve() {
        if std::env::var("MIMIR_RS_NETWORK_TESTS").as_deref() != Ok("1") {
            eprintln!("skipping: set MIMIR_RS_NETWORK_TESTS=1 to run");
            return;
        }
        let r = Resolver::new();
        let err = r
            .lookup_ip_addr("does-not-exist.invalid")
            .await
            .unwrap_err();
        assert!(r.is_not_found(err.as_ref()), "{err}");
        assert_eq!(
            err.to_string(),
            "lookup does-not-exist.invalid: no such host"
        );
        let err = r
            .lookup_srv("", "", "_nope._tcp.does-not-exist.invalid")
            .await
            .unwrap_err();
        assert!(r.is_not_found(err.as_ref()), "{err}");
        let ips = r.lookup_ip_addr("github.com").await.unwrap();
        assert!(!ips.is_empty());
    }

    #[tokio::test]
    async fn empty_host_is_not_found() {
        let r = Resolver::new();
        let err = r.lookup_ip_addr("").await.unwrap_err();
        assert!(r.is_not_found(err.as_ref()));
        assert_eq!(err.to_string(), "lookup : no such host");
    }
}
