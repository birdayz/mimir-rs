//! Wave 19 regression tests for DNS A+AAAA error masking, IP literal short-circuit,
//! and classic ring tokens_file_path Go-JSON compatibility.
//!
//! Covers:
//! - W19-RKN-01: godns::Resolver A+AAAA SERVFAIL/Timeout masking behind fast NODATA,
//!   and IP literal short-circuit (including IPv6 %zone) under ndots:5.
//! - W19-RKN-02: load_tokens_from_file accepts Go-written `{"tokens":null}`, `{}`,
//!   and `{"Tokens":[...]}`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::config::{NameServerConfigGroup, ResolveHosts, ResolverConfig};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::proto::rr::Name;
use hickory_resolver::TokioResolver;
use mimir_ring::dns::godns::Resolver as GoDnsResolver;
use mimir_ring::dns::{DnsSd, IpLookupResolver, Provider, Resolver};
use mimir_ring::token_gen::load_tokens_from_file;
use prometheus::Registry;
use tempfile::TempDir;
use tokio::net::UdpSocket;

#[derive(Clone, Copy)]
enum DnsAction {
    NoData,
    NxDomain,
    ServFail,
    Drop,
    AddrV4([u8; 4]),
    AddrV6([u8; 16]),
}

fn parse_dns_question_qtype_and_end(pkt: &[u8]) -> Option<(u16, usize)> {
    if pkt.len() < 12 {
        return None;
    }
    let mut pos = 12usize;
    loop {
        let len = *pkt.get(pos)? as usize;
        pos += 1;
        if len == 0 {
            break;
        }
        if (len & 0xc0) == 0xc0 {
            pos += 1;
            break;
        }
        pos = pos.checked_add(len)?;
        if pos > pkt.len() {
            return None;
        }
    }
    if pos + 4 > pkt.len() {
        return None;
    }
    let qtype = u16::from_be_bytes([pkt[pos], pkt[pos + 1]]);
    Some((qtype, pos + 4))
}

fn build_dns_response(req: &[u8], q_end: usize, action: DnsAction) -> Option<Vec<u8>> {
    let mut resp = Vec::with_capacity(q_end + 32);
    resp.extend_from_slice(&req[..q_end]);
    // QR=1, RD copied from request, RA=1
    let rd = req[2] & 0x01;
    resp[2] = 0x80 | rd;
    let rcode: u8 = match action {
        DnsAction::NoData | DnsAction::AddrV4(_) | DnsAction::AddrV6(_) => 0,
        DnsAction::ServFail => 2,
        DnsAction::NxDomain => 3,
        DnsAction::Drop => return None,
    };
    resp[3] = 0x80 | rcode;
    // QDCOUNT=1
    resp[4] = 0;
    resp[5] = 1;
    // ANCOUNT
    let ancount: u16 = match action {
        DnsAction::AddrV4(_) | DnsAction::AddrV6(_) => 1,
        _ => 0,
    };
    resp[6..8].copy_from_slice(&ancount.to_be_bytes());
    resp[8..12].fill(0);

    match action {
        DnsAction::AddrV4(ip) => {
            // Pointer to question name at offset 12 (0xC00C)
            resp.extend_from_slice(&[0xc0, 0x0c]);
            resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
            resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
            resp.extend_from_slice(&30u32.to_be_bytes()); // TTL
            resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH
            resp.extend_from_slice(&ip);
        }
        DnsAction::AddrV6(ip) => {
            resp.extend_from_slice(&[0xc0, 0x0c]);
            resp.extend_from_slice(&28u16.to_be_bytes()); // TYPE AAAA
            resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
            resp.extend_from_slice(&30u32.to_be_bytes()); // TTL
            resp.extend_from_slice(&16u16.to_be_bytes()); // RDLENGTH
            resp.extend_from_slice(&ip);
        }
        _ => {}
    }
    Some(resp)
}

async fn spawn_mock_dns_server<F>(handler: F) -> (u16, Arc<AtomicUsize>, tokio::task::JoinHandle<()>)
where
    F: Fn(u16) -> DnsAction + Send + Sync + 'static,
{
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let port = sock.local_addr().unwrap().port();
    let count = Arc::new(AtomicUsize::new(0));
    let count_c = Arc::clone(&count);
    let handle = tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                break;
            };
            count_c.fetch_add(1, Ordering::SeqCst);
            let Some((qtype, q_end)) = parse_dns_question_qtype_and_end(&buf[..n]) else {
                continue;
            };
            let action = handler(qtype);
            if let Some(resp) = build_dns_response(&buf[..n], q_end, action) {
                let _ = sock.send_to(&resp, peer).await;
            }
        }
    });
    (port, count, handle)
}

fn build_test_tokio_resolver(port: u16, ndots: usize, search: Vec<Name>) -> TokioResolver {
    let ns = NameServerConfigGroup::from_ips_clear(&[IpAddr::V4(Ipv4Addr::LOCALHOST)], port, true);
    let cfg = ResolverConfig::from_parts(None, search, ns);
    let mut builder = TokioResolver::builder_with_config(cfg, TokioConnectionProvider::default());
    let opts = builder.options_mut();
    opts.use_hosts_file = ResolveHosts::Never;
    opts.cache_size = 0;
    opts.attempts = 1;
    opts.timeout = Duration::from_millis(80);
    opts.ndots = ndots;
    builder.build()
}

fn counter_value(reg: &Registry, name: &str) -> f64 {
    reg.gather()
        .iter()
        .filter(|mf| mf.get_name() == name)
        .flat_map(|mf| mf.get_metric())
        .map(|m| m.get_counter().get_value())
        .sum()
}

#[tokio::test]
async fn w19_rkn_01_servfail_on_a_not_masked_by_fast_aaaa_nodata_and_preserves_cache() {
    // Phase 1: healthy A record + AAAA NODATA populates Provider cache.
    let mode = Arc::new(AtomicUsize::new(0));
    let mode_srv = Arc::clone(&mode);
    let (port, _packets, srv) = spawn_mock_dns_server(move |qtype| {
        match (mode_srv.load(Ordering::SeqCst), qtype) {
            (0, 1) => DnsAction::AddrV4([10, 1, 0, 1]),
            (0, 28) => DnsAction::NoData,
            // Phase 2: AAAA fast NODATA, A returns SERVFAIL
            (1, 28) => DnsAction::NoData,
            (1, 1) => DnsAction::ServFail,
            // Phase 3: AAAA fast NODATA, A drops (timeout)
            (2, 28) => DnsAction::NoData,
            (2, 1) => DnsAction::Drop,
            // Phase 4: both NODATA / NXDOMAIN
            (3, 28) => DnsAction::NoData,
            (3, 1) => DnsAction::NxDomain,
            // Phase 5: both A and AAAA return records
            (4, 1) => DnsAction::AddrV4([10, 1, 0, 2]),
            (4, 28) => DnsAction::AddrV6([0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]),
            _ => DnsAction::NoData,
        }
    })
    .await;

    let godns = Arc::new(GoDnsResolver::with_resolver(build_test_tokio_resolver(
        port,
        1,
        vec![],
    )));
    let sd: Arc<dyn Resolver> = Arc::new(DnsSd::new(Arc::clone(&godns) as Arc<dyn IpLookupResolver>));
    let reg = Registry::new();
    let provider = Provider::with_resolver(sd, Some(&reg), "cortex_", std::collections::HashMap::new()).unwrap();
    let addrs = vec!["dns+my-svc.cluster.local.:7946".to_string()];

    // Phase 1: healthy lookup
    provider.resolve(&addrs).await.unwrap();
    assert_eq!(provider.addresses(), vec!["10.1.0.1:7946"]);
    assert_eq!(counter_value(&reg, "cortex_dns_failures_total"), 0.0);

    // Phase 2: A=SERVFAIL + AAAA=NODATA -> must fail, increment failures, preserve cache!
    mode.store(1, Ordering::SeqCst);
    let err = godns
        .lookup_ip_addr("my-svc.cluster.local.")
        .await
        .unwrap_err();
    assert!(!godns.is_not_found(err.as_ref()), "SERVFAIL must not be NotFound: {err}");
    let prov_err = provider.resolve(&addrs).await.unwrap_err();
    assert!(!prov_err.to_string().is_empty());
    assert_eq!(counter_value(&reg, "cortex_dns_failures_total"), 1.0);
    assert_eq!(
        provider.addresses(),
        vec!["10.1.0.1:7946"],
        "Provider must keep cached peers on SERVFAIL"
    );

    // Phase 3: A=Timeout + AAAA=NODATA -> must fail, increment failures, preserve cache!
    mode.store(2, Ordering::SeqCst);
    let err = godns
        .lookup_ip_addr("my-svc.cluster.local.")
        .await
        .unwrap_err();
    assert!(!godns.is_not_found(err.as_ref()), "Timeout must not be NotFound: {err}");
    provider.resolve(&addrs).await.unwrap_err();
    assert_eq!(counter_value(&reg, "cortex_dns_failures_total"), 2.0);
    assert_eq!(
        provider.addresses(),
        vec!["10.1.0.1:7946"],
        "Provider must keep cached peers on timeout"
    );

    // Phase 4: both NODATA / NXDOMAIN -> NotFound, no failure counter bump
    mode.store(3, Ordering::SeqCst);
    let err = godns
        .lookup_ip_addr("my-svc.cluster.local.")
        .await
        .unwrap_err();
    assert!(godns.is_not_found(err.as_ref()), "NODATA+NXDOMAIN must be NotFound: {err}");
    provider.resolve(&addrs).await.unwrap();
    assert_eq!(counter_value(&reg, "cortex_dns_failures_total"), 2.0);
    assert!(provider.addresses().is_empty());

    // Phase 5: both A and AAAA merged
    mode.store(4, Ordering::SeqCst);
    let ips = godns.lookup_ip_addr("my-svc.cluster.local.").await.unwrap();
    assert_eq!(
        ips,
        vec![
            IpAddr::V4(Ipv4Addr::new(10, 1, 0, 2)),
            IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2)),
        ]
    );

    srv.abort();
}

#[tokio::test]
async fn w19_rkn_01_ip_literals_short_circuit_under_ndots_5() {
    let (port, packets, srv) = spawn_mock_dns_server(|_| DnsAction::ServFail).await;
    let search = vec![Name::from_ascii("default.svc.cluster.local.").unwrap()];
    let godns = GoDnsResolver::with_resolver(build_test_tokio_resolver(port, 5, search));

    assert_eq!(
        godns.lookup_ip_addr("10.0.0.5").await.unwrap(),
        vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))]
    );
    assert_eq!(
        godns.lookup_ip_addr("fe80::1%eth0").await.unwrap(),
        vec![IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1))]
    );
    assert_eq!(
        packets.load(Ordering::SeqCst),
        0,
        "IP literals must short-circuit without sending DNS search-domain queries"
    );

    srv.abort();
}

#[test]
fn w19_rkn_02_load_tokens_from_file_accepts_go_null_empty_and_capitalized_tokens() {
    let dir = TempDir::new().unwrap();

    let p_null = dir.path().join("null.json");
    std::fs::write(&p_null, br#"{"tokens":null}"#).unwrap();
    assert_eq!(load_tokens_from_file(&p_null).unwrap(), Vec::<u32>::new());

    let p_empty = dir.path().join("empty.json");
    std::fs::write(&p_empty, br#"{}"#).unwrap();
    assert_eq!(load_tokens_from_file(&p_empty).unwrap(), Vec::<u32>::new());

    let p_cap = dir.path().join("cap.json");
    std::fs::write(&p_cap, br#"{"Tokens":[300,100,200]}"#).unwrap();
    assert_eq!(load_tokens_from_file(&p_cap).unwrap(), vec![100, 200, 300]);

    let p_cap_null = dir.path().join("cap_null.json");
    std::fs::write(&p_cap_null, br#"{"Tokens":null}"#).unwrap();
    assert_eq!(load_tokens_from_file(&p_cap_null).unwrap(), Vec::<u32>::new());
}
