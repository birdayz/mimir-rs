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
use hickory_resolver::proto::op::{Query, ResponseCode};
use hickory_resolver::proto::rr::{Name, RData, RecordType};
use hickory_resolver::proto::ProtoErrorKind;
use hickory_resolver::{Hosts, IntoName, ResolveError, TokioResolver};
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

fn is_not_found_resolve_error(e: &ResolveError) -> bool {
    match e.proto().map(|p| p.kind()) {
        Some(ProtoErrorKind::NoRecordsFound { response_code, .. }) => {
            matches!(response_code, ResponseCode::NoError | ResponseCode::NXDomain)
        }
        _ => e.is_no_records_found() || e.is_nx_domain(),
    }
}

impl GoDnsError {
    fn from_resolve_error(name: &str, e: &ResolveError) -> Self {
        if is_not_found_resolve_error(e) {
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
    hosts: Arc<Hosts>,
    custom_resolver: bool,
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

fn load_system_hosts(opts: &hickory_resolver::config::ResolverOpts) -> Hosts {
    match opts.use_hosts_file {
        ResolveHosts::Never => Hosts::default(),
        ResolveHosts::Always | ResolveHosts::Auto => Hosts::from_system().unwrap_or_default(),
    }
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
        let resolver = build_resolver();
        let hosts = load_system_hosts(resolver.options());
        Resolver {
            state: Mutex::new(State {
                resolver: Arc::new(resolver),
                hosts: Arc::new(hosts),
                custom_resolver: false,
                last_checked: Instant::now(),
                resolv_conf_mtime: mtime(RESOLV_CONF_PATH),
                hosts_mtime: mtime(HOSTS_PATH),
            }),
        }
    }

    /// Creates a [`Resolver`] backed by a caller-supplied [`TokioResolver`] (without
    /// `/etc/resolv.conf` auto-reload).
    pub fn with_resolver(resolver: TokioResolver) -> Self {
        let hosts = load_system_hosts(resolver.options());
        Self::with_resolver_and_hosts(resolver, hosts)
    }

    /// Creates a [`Resolver`] backed by caller-supplied [`TokioResolver`] and [`Hosts`].
    pub fn with_resolver_and_hosts(resolver: TokioResolver, hosts: Hosts) -> Self {
        Resolver {
            state: Mutex::new(State {
                resolver: Arc::new(resolver),
                hosts: Arc::new(hosts),
                custom_resolver: true,
                last_checked: Instant::now(),
                resolv_conf_mtime: None,
                hosts_mtime: None,
            }),
        }
    }

    fn current(&self) -> (Arc<TokioResolver>, Arc<Hosts>) {
        let mut st = self.state.lock();
        if !st.custom_resolver && st.last_checked.elapsed() >= RECHECK_INTERVAL {
            st.last_checked = Instant::now();
            let rc = mtime(RESOLV_CONF_PATH);
            let hosts = mtime(HOSTS_PATH);
            if rc != st.resolv_conf_mtime || hosts != st.hosts_mtime {
                st.resolv_conf_mtime = rc;
                st.hosts_mtime = hosts;
                let resolver = build_resolver();
                st.hosts = Arc::new(load_system_hosts(resolver.options()));
                st.resolver = Arc::new(resolver);
            }
        }
        (st.resolver.clone(), st.hosts.clone())
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
        // Go net.Resolver.lookupIPAddr short-circuits IP literals (including IPv6 %zone) before
        // sending any DNS queries (avoiding search-domain queries under Kubernetes ndots:5).
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![ip]);
        }
        if let Some((ip_part, zone)) = host.split_once('%') {
            if !zone.is_empty() {
                if let Ok(v6) = ip_part.parse::<Ipv6Addr>() {
                    return Ok(vec![IpAddr::V6(v6)]);
                }
            }
        }
        let name = match host.into_name() {
            Ok(n) => n,
            Err(e) => {
                let re = ResolveError::from(e);
                return Err(Box::new(GoDnsError::from_resolve_error(host, &re)));
            }
        };
        let (resolver, hosts) = self.current();

        // Go net.Resolver.goLookupIPCNAMEOrder consults /etc/hosts (lookupStaticHost) first for
        // the requested hostname regardless of search domains / ndots, returning immediately if
        // any A or AAAA static entry exists without issuing DNS queries for the other family.
        if resolver.options().use_hosts_file != ResolveHosts::Never {
            let mut static_ips = Vec::new();
            if let Some(lookup) = hosts.lookup_static_host(&Query::query(name.clone(), RecordType::A)) {
                for rdata in lookup.iter() {
                    if let RData::A(a) = rdata {
                        static_ips.push(IpAddr::V4(a.0));
                    }
                }
            }
            if let Some(lookup) =
                hosts.lookup_static_host(&Query::query(name.clone(), RecordType::AAAA))
            {
                for rdata in lookup.iter() {
                    if let RData::AAAA(aaaa) = rdata {
                        static_ips.push(IpAddr::V6(aaaa.0));
                    }
                }
            }
            if !static_ips.is_empty() {
                return Ok(static_ips);
            }
        }

        // Build Go net.Resolver.nameList search candidates in precedence order, marking every
        // candidate FQDN so hickory evaluates each candidate once with A+AAAA together and never
        // merges A from candidate 1 with AAAA from candidate 2.
        let candidates = build_go_search_candidates(host, &name, resolver.config(), resolver.options());
        let mut first_non_not_found: Option<ResolveError> = None;
        let mut first_not_found: Option<ResolveError> = None;

        for cand in candidates {
            let (v4_res, v6_res) =
                tokio::join!(resolver.ipv4_lookup(cand.clone()), resolver.ipv6_lookup(cand));
            let mut ips = Vec::new();
            let mut cand_errs: Vec<ResolveError> = Vec::new();
            match v4_res {
                Ok(lookup) => ips.extend(lookup.iter().map(|a| IpAddr::V4(a.0))),
                Err(e) => cand_errs.push(e),
            }
            match v6_res {
                Ok(lookup) => ips.extend(lookup.iter().map(|aaaa| IpAddr::V6(aaaa.0))),
                Err(e) => cand_errs.push(e),
            }
            if !ips.is_empty() {
                return Ok(ips);
            }
            for e in cand_errs {
                if !is_not_found_resolve_error(&e) {
                    if first_non_not_found.is_none() {
                        first_non_not_found = Some(e);
                    }
                } else if first_not_found.is_none() {
                    first_not_found = Some(e);
                }
            }
        }

        if let Some(err) = first_non_not_found.as_ref().or(first_not_found.as_ref()) {
            return Err(Box::new(GoDnsError::from_resolve_error(host, err)));
        }
        Err(Box::new(GoDnsError {
            err: ERR_NO_SUCH_HOST.to_string(),
            name: host.to_string(),
            server: String::new(),
            is_timeout: false,
            is_not_found: true,
        }))
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
        let (resolver, _) = self.current();
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
fn build_go_search_candidates(
    raw_host: &str,
    name: &Name,
    config: &ResolverConfig,
    options: &hickory_resolver::config::ResolverOpts,
) -> Vec<Name> {
    if name.is_fqdn() {
        return vec![name.clone()];
    }
    let dots = raw_host.bytes().filter(|&b| b == b'.').count();
    let mut rooted = name.clone();
    rooted.set_fqdn(true);

    let mut out = Vec::with_capacity(1 + config.search().len() + 1);
    let push_unique = |list: &mut Vec<Name>, cand: Name| {
        if !list.contains(&cand) {
            list.push(cand);
        }
    };

    if dots >= options.ndots {
        push_unique(&mut out, rooted.clone());
    }
    for search in config.search() {
        if let Ok(mut fqdn) = name.clone().append_domain(search) {
            fqdn.set_fqdn(true);
            push_unique(&mut out, fqdn);
        }
    }
    if let Some(domain) = config.domain() {
        if let Ok(mut fqdn) = name.clone().append_domain(domain) {
            fqdn.set_fqdn(true);
            push_unique(&mut out, fqdn);
        }
    }
    if dots < options.ndots {
        push_unique(&mut out, rooted);
    }
    out
}

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
    /// `MIMIR_RS_NETWORK_TESTS=1` (ignored tests are forbidden in this repo).
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

    fn parse_qname_and_qtype(pkt: &[u8]) -> Option<(String, u16, usize)> {
        if pkt.len() < 12 {
            return None;
        }
        let mut pos = 12usize;
        let mut labels = Vec::new();
        loop {
            let len = *pkt.get(pos)? as usize;
            pos += 1;
            if len == 0 {
                break;
            }
            let end = pos.checked_add(len)?;
            let label = std::str::from_utf8(pkt.get(pos..end)?).ok()?;
            labels.push(label.to_ascii_lowercase());
            pos = end;
        }
        if pos + 4 > pkt.len() {
            return None;
        }
        let qtype = u16::from_be_bytes([pkt[pos], pkt[pos + 1]]);
        let mut qname = labels.join(".");
        if !qname.is_empty() {
            qname.push('.');
        }
        Some((qname, qtype, pos + 4))
    }

    fn build_mock_dns_reply(req: &[u8], q_end: usize, rcode: u8, v4: Option<[u8; 4]>, v6: Option<[u8; 16]>) -> Vec<u8> {
        let mut resp = Vec::with_capacity(q_end + 32);
        resp.extend_from_slice(&req[..q_end]);
        let rd = req[2] & 0x01;
        resp[2] = 0x80 | rd;
        resp[3] = 0x80 | rcode;
        resp[4] = 0;
        resp[5] = 1;
        let ancount: u16 = if v4.is_some() || v6.is_some() { 1 } else { 0 };
        resp[6..8].copy_from_slice(&ancount.to_be_bytes());
        resp[8..12].fill(0);
        if let Some(ip) = v4 {
            resp.extend_from_slice(&[0xc0, 0x0c]);
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            resp.extend_from_slice(&4u16.to_be_bytes());
            resp.extend_from_slice(&ip);
        } else if let Some(ip) = v6 {
            resp.extend_from_slice(&[0xc0, 0x0c]);
            resp.extend_from_slice(&28u16.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            resp.extend_from_slice(&16u16.to_be_bytes());
            resp.extend_from_slice(&ip);
        }
        resp
    }

    #[tokio::test]
    async fn w23_csc_05_etc_hosts_and_per_candidate_search_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::net::UdpSocket;

        // (i) Non-localhost /etc/hosts entry resolves immediately with search domains + ndots:5
        // and sends ZERO network DNS packets.
        // (ii) IPv4-only /etc/hosts entry with completely unreachable (packet-dropping) DNS server
        // returns immediately without stalling on AAAA timeout.
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = sock.local_addr().unwrap().port();
        let packets = Arc::new(AtomicUsize::new(0));
        let packets_srv = Arc::clone(&packets);
        let sock_srv = Arc::clone(&sock);
        let srv_drop = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while sock_srv.recv_from(&mut buf).await.is_ok() {
                packets_srv.fetch_add(1, Ordering::SeqCst);
            }
        });

        let mut hosts = Hosts::default();
        hosts
            .read_hosts_conf("10.0.0.5 seed-0
".as_bytes())
            .unwrap();
        let ns = NameServerConfigGroup::from_ips_clear(&[IpAddr::V4(Ipv4Addr::LOCALHOST)], port, true);
        let search = vec![
            Name::from_ascii("default.svc.cluster.local.").unwrap(),
            Name::from_ascii("svc.cluster.local.").unwrap(),
        ];
        let mut builder = TokioResolver::builder_with_config(
            ResolverConfig::from_parts(None, search.clone(), ns),
            TokioConnectionProvider::default(),
        );
        builder.options_mut().use_hosts_file = ResolveHosts::Always;
        builder.options_mut().cache_size = 0;
        builder.options_mut().attempts = 1;
        builder.options_mut().timeout = Duration::from_secs(5);
        builder.options_mut().ndots = 5;

        let r_hosts = Resolver::with_resolver_and_hosts(builder.build(), hosts);
        let ips = tokio::time::timeout(Duration::from_millis(200), r_hosts.lookup_ip_addr("seed-0"))
            .await
            .expect("IPv4-only /etc/hosts entry must return immediately without DNS timeout")
            .unwrap();
        assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))]);
        assert_eq!(
            packets.load(Ordering::SeqCst),
            0,
            "/etc/hosts match must send 0 DNS queries even under search+ndots:5"
        );
        srv_drop.abort();

        // (iii) Candidate 1 has A only (AAAA NODATA) and Candidate 2 has AAAA (A NODATA):
        // must return ONLY candidate 1's A record and never merge AAAA from candidate 2!
        let sock2 = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port2 = sock2.local_addr().unwrap().port();
        let queried_cand2 = Arc::new(AtomicUsize::new(0));
        let qc2 = Arc::clone(&queried_cand2);
        let sock2_srv = Arc::clone(&sock2);
        let srv_search = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while let Ok((n, peer)) = sock2_srv.recv_from(&mut buf).await {
                let Some((qname, qtype, q_end)) = parse_qname_and_qtype(&buf[..n]) else {
                    continue;
                };
                let resp = match (qname.as_str(), qtype) {
                    ("my-svc.default.svc.cluster.local.", 1) => {
                        build_mock_dns_reply(&buf[..n], q_end, 0, Some([10, 2, 0, 1]), None)
                    }
                    ("my-svc.default.svc.cluster.local.", 28) => {
                        build_mock_dns_reply(&buf[..n], q_end, 0, None, None)
                    }
                    ("my-svc.svc.cluster.local.", 28) => {
                        qc2.fetch_add(1, Ordering::SeqCst);
                        build_mock_dns_reply(
                            &buf[..n],
                            q_end,
                            0,
                            None,
                            Some([0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9]),
                        )
                    }
                    _ => build_mock_dns_reply(&buf[..n], q_end, 3, None, None),
                };
                let _ = sock2_srv.send_to(&resp, peer).await;
            }
        });

        let ns2 = NameServerConfigGroup::from_ips_clear(&[IpAddr::V4(Ipv4Addr::LOCALHOST)], port2, true);
        let mut builder2 = TokioResolver::builder_with_config(
            ResolverConfig::from_parts(None, search, ns2),
            TokioConnectionProvider::default(),
        );
        builder2.options_mut().use_hosts_file = ResolveHosts::Never;
        builder2.options_mut().cache_size = 0;
        builder2.options_mut().attempts = 1;
        builder2.options_mut().timeout = Duration::from_millis(200);
        builder2.options_mut().ndots = 5;

        let r_search = Resolver::with_resolver(builder2.build());
        let ips2 = r_search.lookup_ip_addr("my-svc").await.unwrap();
        assert_eq!(ips2, vec![IpAddr::V4(Ipv4Addr::new(10, 2, 0, 1))]);
        assert_eq!(
            queried_cand2.load(Ordering::SeqCst),
            0,
            "candidate 2 must never be queried once candidate 1 returned an A record"
        );
        srv_search.abort();
    }
}
