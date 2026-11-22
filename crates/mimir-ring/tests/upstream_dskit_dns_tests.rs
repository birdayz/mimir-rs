//! Tests ported from github.com/grafana/dskit/dns (provider_test.go,
//! resolver_test.go) plus Rust-side metrics / caching / trait coverage.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use mimir_ring::dns::{
    is_dynamic_node, DnsError, DnsProvider, DnsSd, IpLookupResolver, LookupError, Provider, QType,
    Resolver, ResolverType, Srv, A, GOLANG_RESOLVER_TYPE, SRV, SRV_NO_A,
};
use parking_lot::Mutex;
use prometheus::{Encoder, Registry, TextEncoder};

// ---------------------------------------------------------------------------
// provider_test.go
// ---------------------------------------------------------------------------

/// Go `mockResolver` from provider_test.go.
#[derive(Default)]
struct MockResolver {
    res: HashMap<String, Vec<String>>,
    err: Mutex<Option<String>>,
}

impl Resolver for MockResolver {
    fn resolve<'a>(
        &'a self,
        name: &'a str,
        _qtype: &'a QType,
    ) -> BoxFuture<'a, Result<Vec<String>, DnsError>> {
        async move {
            if let Some(e) = self.err.lock().clone() {
                return Err(DnsError::Other(e));
            }
            Ok(self.res.get(name).cloned().unwrap_or_default())
        }
        .boxed()
    }
}

fn gather_text(reg: &Registry) -> String {
    let mut buf = Vec::new();
    TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

/// Equivalent of Go `testutil.CollectAndCompare(prv, expected)` for the
/// `dns_provider_results` family: compares HELP/TYPE and the (unordered) samples.
fn check_results_metrics(reg: &Registry, family: &str, help: &str, expected_samples: &[&str]) {
    let text = gather_text(reg);
    let mut got_samples: Vec<String> = Vec::new();
    let mut got_meta: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.starts_with(&format!("# HELP {family} "))
            || line.starts_with(&format!("# TYPE {family} "))
        {
            got_meta.push(line.to_string());
        } else if line.starts_with(&format!("{family}{{"))
            || line.starts_with(&format!("{family} "))
        {
            got_samples.push(line.to_string());
        }
    }
    let mut want: Vec<String> = expected_samples.iter().map(|s| s.to_string()).collect();
    got_samples.sort();
    want.sort();
    assert_eq!(got_samples, want, "full exposition:\n{text}");
    if !want.is_empty() {
        assert_eq!(
            got_meta,
            vec![
                format!("# HELP {family} {help}"),
                format!("# TYPE {family} gauge"),
            ]
        );
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

const RESULTS_HELP: &str = "The number of resolved endpoints for each configured address";

// Upstream: github.com/grafana/dskit/dns/provider_test.go::TestProvider
#[tokio::test]
async fn test_provider() {
    let ips = strings(&[
        "127.0.0.1:19091",
        "127.0.0.2:19092",
        "127.0.0.3:19093",
        "127.0.0.4:19094",
        "127.0.0.5:19095",
    ]);

    let mock = MockResolver {
        res: HashMap::from([
            ("a".to_string(), ips[..2].to_vec()),
            ("b".to_string(), ips[2..4].to_vec()),
            ("c".to_string(), vec![ips[4].clone()]),
        ]),
        err: Mutex::new(None),
    };
    let reg = Registry::new();
    let prv = Provider::with_resolver(Arc::new(mock), Some(&reg), "", HashMap::new()).unwrap();
    let check = |samples: &[&str]| {
        check_results_metrics(&reg, "dns_provider_results", RESULTS_HELP, samples)
    };

    prv.resolve(&strings(&["any+x"])).await.unwrap();
    assert_eq!(sorted(prv.addresses()), Vec::<String>::new());
    check(&[r#"dns_provider_results{addr="any+x"} 0"#]);

    prv.resolve(&strings(&["any+a", "any+b", "any+c"]))
        .await
        .unwrap();
    assert_eq!(sorted(prv.addresses()), ips);
    check(&[
        r#"dns_provider_results{addr="any+a"} 2"#,
        r#"dns_provider_results{addr="any+b"} 2"#,
        r#"dns_provider_results{addr="any+c"} 1"#,
    ]);

    prv.resolve(&strings(&["any+b", "any+c"])).await.unwrap();
    assert_eq!(sorted(prv.addresses()), ips[2..].to_vec());
    check(&[
        r#"dns_provider_results{addr="any+b"} 2"#,
        r#"dns_provider_results{addr="any+c"} 1"#,
    ]);

    prv.resolve(&strings(&["any+x"])).await.unwrap();
    assert_eq!(sorted(prv.addresses()), Vec::<String>::new());
    check(&[r#"dns_provider_results{addr="any+x"} 0"#]);

    prv.resolve(&strings(&["any+a", "any+b", "any+c"]))
        .await
        .unwrap();
    assert_eq!(sorted(prv.addresses()), ips);
    check(&[
        r#"dns_provider_results{addr="any+a"} 2"#,
        r#"dns_provider_results{addr="any+b"} 2"#,
        r#"dns_provider_results{addr="any+c"} 1"#,
    ]);

    prv.resolve(&strings(&["any+b", "example.com:90", "any+c"]))
        .await
        .unwrap();
    let mut want = ips[2..].to_vec();
    want.push("example.com:90".to_string());
    assert_eq!(sorted(prv.addresses()), want);
    check(&[
        r#"dns_provider_results{addr="any+b"} 2"#,
        r#"dns_provider_results{addr="example.com:90"} 1"#,
        r#"dns_provider_results{addr="any+c"} 1"#,
    ]);

    prv.resolve(&strings(&["any+b", "any+c"])).await.unwrap();
    assert_eq!(sorted(prv.addresses()), ips[2..].to_vec());
    check(&[
        r#"dns_provider_results{addr="any+b"} 2"#,
        r#"dns_provider_results{addr="any+c"} 1"#,
    ]);
}

// Upstream: github.com/grafana/dskit/dns/provider_test.go::TestIsDynamicNode
#[test]
fn test_is_dynamic_node() {
    for (node, is_dynamic) in [
        ("1.2.3.4", false),
        ("gibberish+1.1.1.1+noa", true),
        ("", false),
        ("dns+aaa", true),
        ("dnssrv+asdasdsa", true),
    ] {
        assert_eq!(
            is_dynamic_node(node),
            is_dynamic,
            "mismatch between results for {node:?}"
        );
    }
}

// Upstream: github.com/grafana/dskit/dns/provider_test.go::TestResolverType
#[test]
fn test_resolver_type() {
    // set valid types
    for val in ["golang", "miekgdns"] {
        let mut rt = ResolverType::default();
        rt.set(val).unwrap();
        assert_eq!(rt.to_string(), val);
    }
    // set invalid types
    for val in ["dns+golang", "invalid", "miekgdns27"] {
        let mut rt = ResolverType::default();
        let err = rt.set(val).unwrap_err();
        assert_eq!(err.to_string(), format!("unsupported resolver type {val}"));
    }
}

// ---------------------------------------------------------------------------
// resolver_test.go
// ---------------------------------------------------------------------------

/// Go `mockHostnameResolver` from resolver_test.go.
#[derive(Default)]
struct MockHostnameResolver {
    result_ips: HashMap<String, Vec<IpAddr>>,
    result_srvs: HashMap<String, Vec<Srv>>,
    err: Option<String>,
    not_found: bool,
}

impl IpLookupResolver for MockHostnameResolver {
    fn lookup_ip_addr<'a>(
        &'a self,
        host: &'a str,
    ) -> BoxFuture<'a, Result<Vec<IpAddr>, LookupError>> {
        async move {
            if let Some(e) = &self.err {
                return Err(e.clone().into());
            }
            Ok(self.result_ips.get(host).cloned().unwrap_or_default())
        }
        .boxed()
    }

    fn lookup_srv<'a>(
        &'a self,
        _service: &'a str,
        _proto: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<(String, Vec<Srv>), LookupError>> {
        async move {
            if let Some(e) = &self.err {
                return Err(e.clone().into());
            }
            Ok((
                String::new(),
                self.result_srvs.get(name).cloned().unwrap_or_default(),
            ))
        }
        .boxed()
    }

    fn is_not_found(&self, _err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
        self.not_found
    }
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

fn srv(target: &str, port: u16) -> Srv {
    Srv {
        target: target.to_string(),
        port,
        priority: 0,
        weight: 0,
    }
}

struct DnsSdTest {
    test_name: &'static str,
    addr: &'static str,
    qtype: QType,
    expected_result: Vec<String>,
    expected_err: Option<&'static str>,
    resolver: MockHostnameResolver,
}

const ERROR_FROM_RESOLVER: &str = "error from resolver";

fn srv_resolver_with_ips() -> MockHostnameResolver {
    MockHostnameResolver {
        result_srvs: HashMap::from([(
            "_test._tcp.mycompany.com".to_string(),
            vec![
                srv("alt1.mycompany.com.", 8080),
                srv("alt2.mycompany.com.", 8081),
            ],
        )]),
        result_ips: HashMap::from([
            ("alt1.mycompany.com.".to_string(), vec![ip("192.168.0.1")]),
            ("alt2.mycompany.com.".to_string(), vec![ip("192.168.0.2")]),
        ]),
        ..Default::default()
    }
}

fn srv_no_a_resolver() -> MockHostnameResolver {
    MockHostnameResolver {
        result_srvs: HashMap::from([(
            "_test._tcp.mycompany.com".to_string(),
            vec![srv("192.168.0.1", 8080), srv("192.168.0.2", 8081)],
        )]),
        ..Default::default()
    }
}

fn dns_sd_tests() -> Vec<DnsSdTest> {
    vec![
        DnsSdTest {
            test_name: "single ip from dns lookup of host port",
            addr: "test.mycompany.com:8080",
            qtype: A,
            expected_result: strings(&["192.168.0.1:8080"]),
            expected_err: None,
            resolver: MockHostnameResolver {
                result_ips: HashMap::from([(
                    "test.mycompany.com".to_string(),
                    vec![ip("192.168.0.1")],
                )]),
                ..Default::default()
            },
        },
        // Scheme is preserved.
        DnsSdTest {
            test_name: "single ip from dns lookup of host port with scheme",
            addr: "http://test.mycompany.com:8080",
            qtype: A,
            expected_result: strings(&["http://192.168.0.1:8080"]),
            expected_err: None,
            resolver: MockHostnameResolver {
                result_ips: HashMap::from([(
                    "test.mycompany.com".to_string(),
                    vec![ip("192.168.0.1")],
                )]),
                ..Default::default()
            },
        },
        DnsSdTest {
            test_name: "error on dns lookup when no port is specified",
            addr: "test.mycompany.com",
            qtype: A,
            expected_result: vec![],
            expected_err: Some("missing port in address given for dns lookup: test.mycompany.com"),
            resolver: MockHostnameResolver::default(),
        },
        DnsSdTest {
            test_name: "multiple SRV records from SRV lookup",
            addr: "_test._tcp.mycompany.com",
            qtype: SRV,
            expected_result: strings(&["192.168.0.1:8080", "192.168.0.2:8081"]),
            expected_err: None,
            resolver: srv_resolver_with_ips(),
        },
        DnsSdTest {
            test_name: "multiple SRV records from SRV lookup with specified port",
            addr: "_test._tcp.mycompany.com:8082",
            qtype: SRV,
            expected_result: strings(&["192.168.0.1:8082", "192.168.0.2:8082"]),
            expected_err: None,
            resolver: srv_resolver_with_ips(),
        },
        DnsSdTest {
            test_name: "error from SRV resolver",
            addr: "_test._tcp.mycompany.com",
            qtype: SRV,
            expected_result: vec![],
            expected_err: Some(
                "lookup SRV records \"_test._tcp.mycompany.com\": error from resolver",
            ),
            resolver: MockHostnameResolver {
                err: Some(ERROR_FROM_RESOLVER.to_string()),
                ..Default::default()
            },
        },
        DnsSdTest {
            test_name: "multiple SRV records from SRV no A lookup",
            addr: "_test._tcp.mycompany.com",
            qtype: SRV_NO_A,
            expected_result: strings(&["192.168.0.1:8080", "192.168.0.2:8081"]),
            expected_err: None,
            resolver: srv_no_a_resolver(),
        },
        DnsSdTest {
            test_name: "multiple SRV records from SRV no A lookup with specified port",
            addr: "_test._tcp.mycompany.com:8082",
            qtype: SRV_NO_A,
            expected_result: strings(&["192.168.0.1:8082", "192.168.0.2:8082"]),
            expected_err: None,
            resolver: srv_no_a_resolver(),
        },
        DnsSdTest {
            test_name: "error from SRV no A lookup",
            addr: "_test._tcp.mycompany.com",
            qtype: SRV,
            expected_result: vec![],
            expected_err: Some(
                "lookup SRV records \"_test._tcp.mycompany.com\": error from resolver",
            ),
            resolver: MockHostnameResolver {
                err: Some(ERROR_FROM_RESOLVER.to_string()),
                ..Default::default()
            },
        },
        DnsSdTest {
            test_name: "error on bad qtype",
            addr: "test.mycompany.com",
            qtype: QType::new("invalid"),
            expected_result: vec![],
            expected_err: Some("invalid lookup scheme \"invalid\""),
            resolver: MockHostnameResolver::default(),
        },
    ]
}

async fn test_dns_sd(tt: DnsSdTest) {
    let dns_sd = DnsSd::new(Arc::new(tt.resolver));
    let (result, err) = match dns_sd.resolve(tt.addr, &tt.qtype).await {
        Ok(r) => (r, None),
        Err(e) => (Vec::new(), Some(e)),
    };
    match (tt.expected_err, err) {
        (Some(want), Some(got)) => assert_eq!(got.to_string(), want, "{}", tt.test_name),
        (None, None) => {}
        (want, got) => panic!("{}: expected error {want:?}, got {got:?}", tt.test_name),
    }
    assert_eq!(sorted(result), tt.expected_result, "{}", tt.test_name);
}

// Upstream: github.com/grafana/dskit/dns/resolver_test.go::TestDnsSD_Resolve
#[tokio::test]
async fn test_dns_sd_resolve() {
    for tt in dns_sd_tests() {
        test_dns_sd(tt).await;
    }
}

// ---------------------------------------------------------------------------
// Rust-side coverage of behaviour without a dedicated upstream test.
// ---------------------------------------------------------------------------

/// NXDOMAIN-style errors (IsNotFound) are logged and yield no results; other
/// errors are returned (dskit resolver.go).
#[tokio::test]
async fn not_found_is_not_an_error() {
    let sd = DnsSd::new(Arc::new(MockHostnameResolver {
        err: Some("no such host".to_string()),
        not_found: true,
        ..Default::default()
    }));
    assert_eq!(
        sd.resolve("foo:80", &A).await.unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        sd.resolve("_x._tcp.foo", &SRV).await.unwrap(),
        Vec::<String>::new()
    );

    // Non-not-found errors on the per-target A lookup use the SRV host in the message.
    struct SrvOkAFails;
    impl IpLookupResolver for SrvOkAFails {
        fn lookup_ip_addr<'a>(
            &'a self,
            _h: &'a str,
        ) -> BoxFuture<'a, Result<Vec<IpAddr>, LookupError>> {
            async { Err::<Vec<IpAddr>, LookupError>("boom".into()) }.boxed()
        }
        fn lookup_srv<'a>(
            &'a self,
            _s: &'a str,
            _p: &'a str,
            _n: &'a str,
        ) -> BoxFuture<'a, Result<(String, Vec<Srv>), LookupError>> {
            async { Ok((String::new(), vec![srv("t.example.", 1)])) }.boxed()
        }
        fn is_not_found(&self, _err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
            false
        }
    }
    let sd = DnsSd::new(Arc::new(SrvOkAFails));
    let err = sd.resolve("_x._tcp.foo", &SRV).await.unwrap_err();
    assert_eq!(err.to_string(), "lookup IP addresses \"_x._tcp.foo\": boom");
    // SRVNoA does not do the A lookup.
    assert_eq!(
        sd.resolve("_x._tcp.foo", &SRV_NO_A).await.unwrap(),
        strings(&["t.example.:1"])
    );
}

/// IPv6 results are bracketed, IPv4-mapped IPv6 printed as IPv4 (Go net.IP.String).
#[tokio::test]
async fn ipv6_results_are_joined_like_go() {
    let sd = DnsSd::new(Arc::new(MockHostnameResolver {
        result_ips: HashMap::from([(
            "h".to_string(),
            vec![ip("2001:db8::1"), ip("::ffff:10.0.0.1")],
        )]),
        ..Default::default()
    }));
    assert_eq!(
        sd.resolve("h:7946", &A).await.unwrap(),
        strings(&["[2001:db8::1]:7946", "10.0.0.1:7946"])
    );
    // IPv6 literal with brackets in the input.
    let sd = DnsSd::new(Arc::new(MockHostnameResolver {
        result_ips: HashMap::from([("::1".to_string(), vec![ip("::1")])]),
        ..Default::default()
    }));
    assert_eq!(
        sd.resolve("[::1]:80", &A).await.unwrap(),
        strings(&["[::1]:80"])
    );
}

/// Provider keeps the last good result per address on failure and returns a
/// multierror of all failures; lookups/failures counters are incremented.
#[tokio::test]
async fn provider_keeps_cached_results_on_failure() {
    let mock = Arc::new(MockResolver {
        res: HashMap::from([
            ("a".to_string(), strings(&["1.1.1.1:1"])),
            ("b".to_string(), strings(&["2.2.2.2:2", "3.3.3.3:3"])),
        ]),
        err: Mutex::new(None),
    });
    let reg = Registry::new();
    let prv = Provider::with_resolver(mock.clone(), Some(&reg), "", HashMap::new()).unwrap();
    let addrs = strings(&["dns+a", "dns+b", "static:9"]);
    prv.resolve(&addrs).await.unwrap();
    assert_eq!(
        sorted(prv.addresses()),
        strings(&["1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "static:9"])
    );

    *mock.err.lock() = Some("resolver down".to_string());
    let err = prv.resolve(&addrs).await.unwrap_err();
    assert_eq!(err.to_string(), "2 errors: resolver down; resolver down");
    assert_eq!(
        sorted(prv.addresses()),
        strings(&["1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "static:9"])
    );

    // A single failure is not prefixed with the count.
    let err = prv.resolve(&strings(&["dns+a"])).await.unwrap_err();
    assert_eq!(err.to_string(), "resolver down");
    // Addresses never resolved successfully have nothing cached.
    let err = prv.resolve(&strings(&["dns+new"])).await.unwrap_err();
    assert_eq!(err.to_string(), "resolver down");
    assert_eq!(prv.addresses(), Vec::<String>::new());

    let text = gather_text(&reg);
    assert!(text.contains("dns_lookups_total 6\n"), "{text}");
    assert!(text.contains("dns_failures_total 4\n"), "{text}");
}

/// Mimir wraps the registerer with prefix `cortex_` and label
/// `component="memberlist"` (pkg/mimir/modules.go initMemberlistKV).
#[tokio::test]
async fn provider_metrics_with_mimir_memberlist_wrapping() {
    let mock = MockResolver {
        res: HashMap::from([("a".to_string(), strings(&["1.1.1.1:1", "2.2.2.2:2"]))]),
        err: Mutex::new(None),
    };
    let reg = Registry::new();
    let labels = HashMap::from([("component".to_string(), "memberlist".to_string())]);
    let prv = Provider::with_resolver(Arc::new(mock), Some(&reg), "cortex_", labels).unwrap();
    prv.resolve(&strings(&["dns+a", "x:1"])).await.unwrap();

    let text = gather_text(&reg);
    let expected = "\
# HELP cortex_dns_failures_total The number of DNS lookup failures
# TYPE cortex_dns_failures_total counter
cortex_dns_failures_total{component=\"memberlist\"} 0
# HELP cortex_dns_lookups_total The number of DNS lookups resolutions attempts
# TYPE cortex_dns_lookups_total counter
cortex_dns_lookups_total{component=\"memberlist\"} 1
# HELP cortex_dns_provider_results The number of resolved endpoints for each configured address
# TYPE cortex_dns_provider_results gauge
cortex_dns_provider_results{addr=\"dns+a\",component=\"memberlist\"} 2
cortex_dns_provider_results{addr=\"x:1\",component=\"memberlist\"} 1
";
    assert_eq!(text, expected);

    // Registering a second provider on the same registry fails like Go MustRegister.
    let dup = Provider::with_resolver(
        Arc::new(MockResolver::default()),
        Some(&reg),
        "cortex_",
        HashMap::from([("component".to_string(), "memberlist".to_string())]),
    );
    assert!(dup.is_err());
}

/// Go Provider.Clone: shares resolver, starts with an empty map.
#[tokio::test]
async fn provider_clone_starts_empty() {
    let mock = MockResolver {
        res: HashMap::from([("a".to_string(), strings(&["1.1.1.1:1"]))]),
        err: Mutex::new(None),
    };
    let prv = Provider::with_resolver(Arc::new(mock), None, "", HashMap::new()).unwrap();
    prv.resolve(&strings(&["dns+a"])).await.unwrap();
    let cl = prv.clone_provider();
    assert_eq!(cl.addresses(), Vec::<String>::new());
    cl.resolve(&strings(&["dns+a"])).await.unwrap();
    assert_eq!(cl.addresses(), strings(&["1.1.1.1:1"]));
}

/// Production Go-compatible resolver through the object-safe DnsProvider trait.
#[tokio::test]
async fn golang_provider_resolves_localhost_and_rejects_missing_port() {
    let prv: Box<dyn DnsProvider> =
        Box::new(Provider::new(&GOLANG_RESOLVER_TYPE, 0, None, "", HashMap::new()).unwrap());
    prv.resolve(&strings(&["dns+localhost:7946", "10.1.2.3:7946"]))
        .await
        .unwrap();
    let addrs = sorted(prv.addresses());
    assert!(addrs.contains(&"127.0.0.1:7946".to_string()), "{addrs:?}");
    assert!(addrs.contains(&"10.1.2.3:7946".to_string()), "{addrs:?}");

    let err = prv.resolve(&strings(&["dns+localhost"])).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "missing port in address given for dns lookup: localhost"
    );

    let err = prv
        .resolve(&strings(&["foo+localhost:1"]))
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "invalid lookup scheme \"foo\"");

    // IP literals resolve without DNS.
    prv.resolve(&strings(&["dns+127.0.0.1:1", "dns+[::1]:2"]))
        .await
        .unwrap();
    assert_eq!(
        sorted(prv.addresses()),
        strings(&["127.0.0.1:1", "[::1]:2"])
    );
}

#[test]
fn miekgdns_resolver_type_is_supported() {
    let rt: ResolverType = "miekgdns".parse().unwrap();
    assert!(Provider::new(&rt, 2, None, "", HashMap::new()).is_ok());
}
