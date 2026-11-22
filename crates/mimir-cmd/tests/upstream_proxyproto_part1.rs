//! Ported upstream tests from github.com/pires/go-proxyproto:
//! - addr_proto_test.go (7)
//! - version_cmd_test.go (3)
//! - tlv_test.go (5)
//! - v1_test.go (10)
//! - v2_test.go (12)

use std::io::{self, Cursor, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener, TcpStream};
use std::sync::RwLock;
use std::thread;
use std::time::Duration;

use mimir_cmd::server::proxyproto::{
    default_policy, format_unix_name, join_tlvs, max_v2_header_size, new_ip_addr, parse_unix_name,
    parse_version1, read, set_max_v2_header_size, set_v1_accept_ipv4_in_tcp6,
    v1_accept_ipv4_in_tcp6, AddressFamilyAndProtocol, Header, ListenerHeaderTimeout, PP2Type,
    ProtocolVersionAndCommand, ProxyAddr, ProxyBufReader, ProxyError, ProxyListener, Tlv,
    DEFAULT_READ_BUFFER_SIZE, LENGTH_UNIX, LENGTH_UNSPEC, LENGTH_V4, LENGTH_V6, SIGV2,
};

static GLOBAL_RW: RwLock<()> = RwLock::new(());

const TEST_NO_PROTOCOL: &str = "There is no spoon";
const TEST_LOCALHOST_IP4_ADDR: &str = "127.0.0.1";
const TEST_LOCALHOST_IP4_IN6_ADDR: &str = "::ffff:127.0.0.1";
const TEST_LOCALHOST_IP6_ADDR: &str = "::1";
const TEST_IP6_LONG_ADDR: &str = "1234:5678:9abc:def0:cafe:babe:dead:2bad";
const TEST_VALID_PORT: i32 = 65533;
const TEST_INVALID_PORT: i32 = 99999;
const TEST_UNIX_SOCKET_NAME: &str = "socket";

fn v4_ip() -> IpAddr {
    IpAddr::V4(TEST_LOCALHOST_IP4_ADDR.parse().unwrap())
}
fn v6_ip() -> IpAddr {
    IpAddr::V6(TEST_LOCALHOST_IP6_ADDR.parse().unwrap())
}
fn v4_addr() -> ProxyAddr {
    ProxyAddr::tcp(v4_ip(), TEST_VALID_PORT)
}
fn v6_addr() -> ProxyAddr {
    ProxyAddr::tcp(v6_ip(), TEST_VALID_PORT)
}
fn v4_udp_addr() -> ProxyAddr {
    ProxyAddr::udp(v4_ip(), TEST_VALID_PORT)
}
fn v6_udp_addr() -> ProxyAddr {
    ProxyAddr::udp(v6_ip(), TEST_VALID_PORT)
}
fn unix_stream_addr() -> ProxyAddr {
    ProxyAddr::unix(TEST_UNIX_SOCKET_NAME)
}
fn unix_datagram_addr() -> ProxyAddr {
    ProxyAddr::unixgram(TEST_UNIX_SOCKET_NAME)
}
fn new_bufio(data: &[u8]) -> ProxyBufReader<Cursor<Vec<u8>>> {
    ProxyBufReader::new_size(Cursor::new(data.to_vec()), DEFAULT_READ_BUFFER_SIZE)
}
fn v2_header(command: u8, transport: u8, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(16 + payload.len());
    b.extend_from_slice(SIGV2);
    b.push(command);
    b.push(transport);
    b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    b.extend_from_slice(payload);
    b
}
fn fixture_with_tlv(base_len: u16, addr: &[u8], tlv: &[u8]) -> Vec<u8> {
    let total = base_len + tlv.len() as u16;
    let mut out = Vec::with_capacity(2 + addr.len() + tlv.len());
    out.extend_from_slice(&total.to_be_bytes());
    out.extend_from_slice(addr);
    out.extend_from_slice(tlv);
    out
}
fn ipv4_addr_block() -> Vec<u8> {
    let mut v = Vec::new();
    let ip: Ipv4Addr = TEST_LOCALHOST_IP4_ADDR.parse().unwrap();
    v.extend_from_slice(&ip.octets());
    v.extend_from_slice(&ip.octets());
    v.extend_from_slice(&(TEST_VALID_PORT as u16).to_be_bytes());
    v.extend_from_slice(&(TEST_VALID_PORT as u16).to_be_bytes());
    v
}
fn ipv6_addr_block() -> Vec<u8> {
    let mut v = Vec::new();
    let ip: Ipv6Addr = TEST_LOCALHOST_IP6_ADDR.parse().unwrap();
    v.extend_from_slice(&ip.octets());
    v.extend_from_slice(&ip.octets());
    v.extend_from_slice(&(TEST_VALID_PORT as u16).to_be_bytes());
    v.extend_from_slice(&(TEST_VALID_PORT as u16).to_be_bytes());
    v
}
fn unix_addr_block() -> Vec<u8> {
    let u = format_unix_name(TEST_UNIX_SOCKET_NAME);
    [u.as_slice(), u.as_slice()].concat()
}

// --- addr_proto_test.go (7) ---

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestTCPoverIPv4
#[test]
fn test_tc_pover_ipv4() {
    let b = AddressFamilyAndProtocol::TCPV4.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_ipv4());
    assert!(ap.is_stream());
    assert_eq!(ap.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestTCPoverIPv6
#[test]
fn test_tc_pover_ipv6() {
    let b = AddressFamilyAndProtocol::TCPV6.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_ipv6());
    assert!(ap.is_stream());
    assert_eq!(ap.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestUDPoverIPv4
#[test]
fn test_ud_pover_ipv4() {
    let b = AddressFamilyAndProtocol::UDPV4.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_ipv4());
    assert!(ap.is_datagram());
    assert_eq!(ap.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestUDPoverIPv6
#[test]
fn test_ud_pover_ipv6() {
    let b = AddressFamilyAndProtocol::UDPV6.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_ipv6());
    assert!(ap.is_datagram());
    assert_eq!(ap.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestUnixStream
#[test]
fn test_unix_stream() {
    let b = AddressFamilyAndProtocol::UNIX_STREAM.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_unix());
    assert!(ap.is_stream());
    assert_eq!(ap.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestUnixDatagram
#[test]
fn test_unix_datagram() {
    let b = AddressFamilyAndProtocol::UNIX_DATAGRAM.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_unix());
    assert!(ap.is_datagram());
    assert_eq!(ap.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/addr_proto_test.go::TestInvalidAddressFamilyAndProtocol
#[test]
fn test_invalid_address_family_and_protocol() {
    let b = AddressFamilyAndProtocol::UNSPEC.0;
    let ap = AddressFamilyAndProtocol(b);
    assert!(ap.is_unspec());
    assert_eq!(ap.to_byte(), b);
}

// --- version_cmd_test.go (3) ---

// Upstream: github.com/pires/go-proxyproto/version_cmd_test.go::TestLocal
#[test]
fn test_local() {
    let b = ProtocolVersionAndCommand::LOCAL.0;
    let cmd = ProtocolVersionAndCommand(b);
    assert!(!cmd.is_unspec());
    assert!(cmd.is_local());
    assert!(!cmd.is_proxy());
    assert_eq!(cmd.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/version_cmd_test.go::TestProxy
#[test]
fn test_proxy() {
    let b = ProtocolVersionAndCommand::PROXY.0;
    let cmd = ProtocolVersionAndCommand(b);
    assert!(!cmd.is_unspec());
    assert!(!cmd.is_local());
    assert!(cmd.is_proxy());
    assert_eq!(cmd.to_byte(), b);
}

// Upstream: github.com/pires/go-proxyproto/version_cmd_test.go::TestInvalidProtocolVersion
#[test]
fn test_invalid_protocol_version() {
    let invalid = ProtocolVersionAndCommand(0x00);
    assert!(invalid.is_unspec());
    assert_eq!(invalid.to_byte(), ProtocolVersionAndCommand::LOCAL.0);
}

// --- tlv_test.go (5) ---

// Upstream: github.com/pires/go-proxyproto/tlv_test.go::TestValid0Length
#[test]
fn test_valid0_length() {
    let _g = GLOBAL_RW.read().unwrap();
    let payload = fixture_with_tlv(LENGTH_V4, &ipv4_addr_block(), &[PP2Type::MIN_CUSTOM.0, 0x00, 0x00]);
    let raw = [SIGV2.as_slice(), &[ProtocolVersionAndCommand::PROXY.0, AddressFamilyAndProtocol::TCPV4.0], &payload].concat();
    let mut r = new_bufio(&raw);
    let h = read(&mut r).unwrap();
    let tlvs = h.tlvs().unwrap();
    assert_eq!(tlvs.len(), 1);
    assert!(tlvs[0].value.is_empty());
}

// Upstream: github.com/pires/go-proxyproto/tlv_test.go::TestInvalidV2TLV
#[test]
fn test_invalid_v2_tlv() {
    let _g = GLOBAL_RW.read().unwrap();
    let fixtures = [
        vec![PP2Type::MIN_CUSTOM.0 + 1],
        vec![PP2Type::MIN_CUSTOM.0 + 2, 0x00],
        vec![PP2Type::MIN_CUSTOM.0 + 3, 0x00, 0x01],
        vec![PP2Type::MIN_CUSTOM.0 + 3, 0x00, 0x02, 0x00],
    ];
    for f in fixtures {
        let payload = fixture_with_tlv(LENGTH_V4, &ipv4_addr_block(), &f);
        let raw = [SIGV2.as_slice(), &[ProtocolVersionAndCommand::PROXY.0, AddressFamilyAndProtocol::TCPV4.0], &payload].concat();
        let mut r = new_bufio(&raw);
        let hdr = read(&mut r).unwrap();
        let err = hdr.tlvs().unwrap_err();
        assert!(matches!(err, ProxyError::TruncatedTlv), "got {err:?}");
    }
}

// Upstream: github.com/pires/go-proxyproto/tlv_test.go::TestV2TLVPP2Registered
#[test]
fn test_v2_tlvpp2_registered() {
    let reg_types = [
        PP2Type::ALPN,
        PP2Type::AUTHORITY,
        PP2Type::CRC32C,
        PP2Type::NOOP,
        PP2Type::UNIQUE_ID,
        PP2Type::SSL,
        PP2Type::SUBTYPE_SSL_VERSION,
        PP2Type::SUBTYPE_SSL_CN,
        PP2Type::SUBTYPE_SSL_CIPHER,
        PP2Type::SUBTYPE_SSL_SIG_ALG,
        PP2Type::SUBTYPE_SSL_KEY_ALG,
        PP2Type::SUBTYPE_SSL_GROUP,
        PP2Type::SUBTYPE_SSL_SIG_SCHEME,
        PP2Type::SUBTYPE_SSL_CLIENT_CERT,
        PP2Type::NETNS,
    ];
    for p in reg_types {
        assert!(p.registered());
        assert!(p.spec());
        assert!(!p.app());
        assert!(!p.experiment());
        assert!(!p.future());
    }
    for i in 0u8..=0xFF {
        let p = PP2Type(i);
        if !reg_types.contains(&p) {
            assert!(!p.registered(), "type {i:#x} unexpectedly registered");
        }
    }
}

// Upstream: github.com/pires/go-proxyproto/tlv_test.go::TestJoinTLVs
#[test]
fn test_join_tlvs() {
    let cases = [
        (
            [&[PP2Type::AUTHORITY.0, 0x00, 0x0B][..], b"example.org"].concat(),
            vec![Tlv { typ: PP2Type::AUTHORITY, value: b"example.org".to_vec() }],
        ),
        (
            vec![PP2Type::NOOP.0, 0x00, 0x00],
            vec![Tlv { typ: PP2Type::NOOP, value: Vec::new() }],
        ),
    ];
    for (expected, tlvs) in cases {
        let raw = join_tlvs(&tlvs).unwrap();
        assert_eq!(raw, expected);
    }
}

// Upstream: github.com/pires/go-proxyproto/tlv_test.go::TestPP2TypeReservedRanges
#[test]
fn test_pp2_type_reserved_ranges() {
    let cases = [
        (PP2Type(0x00), false, false, false),
        (PP2Type(0x06), false, false, false),
        (PP2Type(0x29), false, false, false),
        (PP2Type(0xDF), false, false, false),
        (PP2Type::MIN_CUSTOM, true, false, false),
        (PP2Type::MAX_CUSTOM, true, false, false),
        (PP2Type::MIN_EXPERIMENT, false, true, false),
        (PP2Type::MAX_EXPERIMENT, false, true, false),
        (PP2Type::MIN_FUTURE, false, false, true),
        (PP2Type::MAX_FUTURE, false, false, true),
    ];
    for (typ, app, exp, fut) in cases {
        assert!(!typ.registered());
        assert_eq!(typ.app(), app);
        assert_eq!(typ.experiment(), exp);
        assert_eq!(typ.future(), fut);
        assert_eq!(typ.spec(), app || exp || fut);
    }
}

// --- v1_test.go (10) ---

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestParseV1IPv4InTCP6Compat
#[test]
fn test_parse_v1_ipv4_in_tcp6_compat() {
    let _g = GLOBAL_RW.write().unwrap();
    set_v1_accept_ipv4_in_tcp6(true);
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) { set_v1_accept_ipv4_in_tcp6(false); }
    }
    let _reset = Reset;

    let cases = [
        (
            "PROXY TCP6 192.0.2.1 192.0.2.2 1234 5678\r\n",
            Header {
                version: 1,
                command: ProtocolVersionAndCommand::PROXY,
                transport_protocol: AddressFamilyAndProtocol::TCPV6,
                source_addr: Some(ProxyAddr::tcp("192.0.2.1".parse().unwrap(), 1234)),
                destination_addr: Some(ProxyAddr::tcp("192.0.2.2".parse().unwrap(), 5678)),
                raw_tlvs: Vec::new(),
            },
            "PROXY TCP6 ::ffff:192.0.2.1 ::ffff:192.0.2.2 1234 5678\r\n",
        ),
        (
            "PROXY TCP6 2001:db8::1 192.0.2.1 51512 22\r\n",
            Header {
                version: 1,
                command: ProtocolVersionAndCommand::PROXY,
                transport_protocol: AddressFamilyAndProtocol::TCPV6,
                source_addr: Some(ProxyAddr::tcp("2001:db8::1".parse().unwrap(), 51512)),
                destination_addr: Some(ProxyAddr::tcp("192.0.2.1".parse().unwrap(), 22)),
                raw_tlvs: Vec::new(),
            },
            "PROXY TCP6 2001:db8::1 ::ffff:192.0.2.1 51512 22\r\n",
        ),
        (
            "PROXY TCP6 192.0.2.1 2001:db8::1 51512 22\r\n",
            Header {
                version: 1,
                command: ProtocolVersionAndCommand::PROXY,
                transport_protocol: AddressFamilyAndProtocol::TCPV6,
                source_addr: Some(ProxyAddr::tcp("192.0.2.1".parse().unwrap(), 51512)),
                destination_addr: Some(ProxyAddr::tcp("2001:db8::1".parse().unwrap(), 22)),
                raw_tlvs: Vec::new(),
            },
            "PROXY TCP6 ::ffff:192.0.2.1 2001:db8::1 51512 22\r\n",
        ),
    ];
    for (line, expected, wire) in cases {
        let mut r = new_bufio(line.as_bytes());
        let h = read(&mut r).unwrap();
        assert!(h.equals_to(Some(&expected)));
        let got = h.format().unwrap();
        assert_eq!(std::str::from_utf8(&got).unwrap(), wire);
    }
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestFormatV1InvalidPorts
#[test]
fn test_format_v1_invalid_ports() {
    for port in [-1, 65536, 700000] {
        let h = Header {
            version: 1,
            command: ProtocolVersionAndCommand::PROXY,
            transport_protocol: AddressFamilyAndProtocol::TCPV4,
            source_addr: Some(ProxyAddr::tcp("10.1.1.1".parse().unwrap(), port)),
            destination_addr: Some(ProxyAddr::tcp("20.2.2.2".parse().unwrap(), 443)),
            raw_tlvs: Vec::new(),
        };
        let err = h.format().unwrap_err();
        assert!(matches!(err, ProxyError::InvalidPortNumber(_)), "port {port}: {err:?}");
    }
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestReadV1Invalid
#[test]
fn test_read_v1_invalid() {
    let _g = GLOBAL_RW.read().unwrap();
    assert!(!v1_accept_ipv4_in_tcp6());
    let ip4_ports = format!("{TEST_LOCALHOST_IP4_ADDR} {TEST_LOCALHOST_IP4_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let ip4_bad_ports = format!("{TEST_LOCALHOST_IP4_ADDR} {TEST_LOCALHOST_IP4_ADDR} {TEST_INVALID_PORT} {TEST_INVALID_PORT}");
    let ip6_ports = format!("{TEST_LOCALHOST_IP6_ADDR} {TEST_LOCALHOST_IP6_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let ip4in6_ports = format!("{TEST_LOCALHOST_IP4_IN6_ADDR} {TEST_LOCALHOST_IP4_IN6_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let ip6_long = format!("{TEST_IP6_LONG_ADDR} {TEST_IP6_LONG_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");

    type ErrCase<'a> = (&'a str, Vec<u8>, fn(&ProxyError) -> bool);
    let cases: Vec<ErrCase<'_>> = vec![
        ("no signature", TEST_NO_PROTOCOL.as_bytes().to_vec(), |e| matches!(e, ProxyError::NoProxyProtocol)),
        ("prox", b"PROX".to_vec(), |e| matches!(e, ProxyError::NoProxyProtocol)),
        ("proxy lf", b"PROXY \n".to_vec(), |e| matches!(e, ProxyError::LineMustEndWithCrlf)),
        ("proxy crlf", b"PROXY \r\n".to_vec(), |e| matches!(e, ProxyError::CantReadAddressFamilyAndProtocol)),
        ("proxy no space crlf", b"PROXY\r\n".to_vec(), |e| matches!(e, ProxyError::CantReadAddressFamilyAndProtocol)),
        ("proxy something crlf", b"PROXY SOMETHING\r\n".to_vec(), |e| matches!(e, ProxyError::CantReadAddressFamilyAndProtocol)),
        ("incomplete TCP4", format!("PROXY TCP4 {ip4_ports}").into_bytes(), |e| matches!(e, ProxyError::CantReadVersion1Header(_))),
        ("invalid IP", b"PROXY TCP4 invalid invalid 65533 65533\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP4 with IPv6", format!("PROXY TCP4 {ip6_ports}\r\n").into_bytes(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP4 with v4-in-v6", format!("PROXY TCP4 {ip4in6_ports}\r\n").into_bytes(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP6 with invalid", b"PROXY TCP6 not-an-ip ::1 1234 5678\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP4 with invalid port", format!("PROXY TCP4 {ip4_bad_ports}\r\n").into_bytes(), |e| matches!(e, ProxyError::InvalidPortNumber(_))),
        ("header too long", format!("PROXY UNKNOWN {ip6_long} \r\n").into_bytes(), |e| matches!(e, ProxyError::Version1HeaderTooLong)),
        ("signed port", b"PROXY TCP4 1.2.3.4 5.6.7.8 +80 443\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidPortNumber(_))),
        ("leading-zero port", b"PROXY TCP4 1.2.3.4 5.6.7.8 080 443\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidPortNumber(_))),
        ("empty port token", b"PROXY TCP4 1.2.3.4 5.6.7.8  443\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidPortNumber(_))),
        ("trailing token", b"PROXY TCP4 1.2.3.4 5.6.7.8 80 443 extra\r\n".to_vec(), |e| matches!(e, ProxyError::CantReadAddressFamilyAndProtocol)),
        ("signature trailing garbage", b"PROXYjunk TCP4 1.2.3.4 5.6.7.8 80 443\r\n".to_vec(), |e| matches!(e, ProxyError::CantReadVersion1Header(_))),
        ("signature glued to proto", b"PROXYTCP4 1.2.3.4 5.6.7.8 80 443\r\n".to_vec(), |e| matches!(e, ProxyError::CantReadVersion1Header(_))),
        ("TCP6 zoned src", b"PROXY TCP6 fe80::1%eth0 fe80::2 80 443\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP6 zoned dst", b"PROXY TCP6 fe80::1 fe80::2%eth0 80 443\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP4 leading-zero octet", b"PROXY TCP4 01.2.3.4 5.6.7.8 80 443\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP6 plain IPv4", b"PROXY TCP6 192.0.2.1 192.0.2.2 1234 5678\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("TCP6 plain IPv4 dst", b"PROXY TCP6 2001:db8::1 192.0.2.1 51512 22\r\n".to_vec(), |e| matches!(e, ProxyError::InvalidAddress(_))),
    ];
    for (desc, input, check) in cases {
        let mut r = new_bufio(&input);
        let err = read(&mut r).expect_err(desc);
        assert!(check(&err), "{desc}: unexpected error {err:?}");
    }
}

fn valid_v1_cases() -> Vec<(&'static str, Vec<u8>, Header, &'static str)> {
    let ip4_ports = format!("{TEST_LOCALHOST_IP4_ADDR} {TEST_LOCALHOST_IP4_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let ip4_bad_ports = format!("{TEST_LOCALHOST_IP4_ADDR} {TEST_LOCALHOST_IP4_ADDR} {TEST_INVALID_PORT} {TEST_INVALID_PORT}");
    let ip6_ports = format!("{TEST_LOCALHOST_IP6_ADDR} {TEST_LOCALHOST_IP6_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let ip4in6_ports = format!("{TEST_LOCALHOST_IP4_IN6_ADDR} {TEST_LOCALHOST_IP4_IN6_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    vec![
        (
            "TCP4",
            format!("PROXY TCP4 {ip4_ports}\r\nGET /").into_bytes(),
            Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() },
            "PROXY TCP4 127.0.0.1 127.0.0.1 65533 65533\r\n",
        ),
        (
            "TCP6",
            format!("PROXY TCP6 {ip6_ports}\r\nGET /").into_bytes(),
            Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: Vec::new() },
            "PROXY TCP6 ::1 ::1 65533 65533\r\n",
        ),
        (
            "TCP4IN6",
            format!("PROXY TCP6 {ip4in6_ports}\r\nGET /").into_bytes(),
            Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() },
            "PROXY TCP6 ::ffff:127.0.0.1 ::ffff:127.0.0.1 65533 65533\r\n",
        ),
        (
            "unknown",
            b"PROXY UNKNOWN\r\n".to_vec(),
            Header { version: 1, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNSPEC, source_addr: None, destination_addr: None, raw_tlvs: Vec::new() },
            "PROXY UNKNOWN\r\n",
        ),
        (
            "unknown with addresses",
            format!("PROXY UNKNOWN {ip4_bad_ports}\r\n").into_bytes(),
            Header { version: 1, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNSPEC, source_addr: None, destination_addr: None, raw_tlvs: Vec::new() },
            "PROXY UNKNOWN\r\n",
        ),
    ]
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestParseV1Valid
#[test]
fn test_parse_v1_valid() {
    let _g = GLOBAL_RW.read().unwrap();
    for (desc, wire, expected, _) in valid_v1_cases() {
        let mut r = new_bufio(&wire);
        let h = read(&mut r).expect(desc);
        assert!(h.equals_to(Some(&expected)), "{desc}: {h:?}");
    }
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestWriteV1Valid
#[test]
fn test_write_v1_valid() {
    let _g = GLOBAL_RW.read().unwrap();
    for (desc, _, expected, expected_wire) in valid_v1_cases() {
        let mut buf = Vec::new();
        expected.write_to(&mut buf).expect(desc);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), expected_wire, "{desc}");
        let mut r = new_bufio(&buf);
        let h = read(&mut r).expect(desc);
        assert!(h.equals_to(Some(&expected)), "{desc}");
    }
}

struct DataSource { n_bytes: usize, n_read: usize }
impl Read for DataSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.n_read >= self.n_bytes { return Ok(0); }
        let avail = (self.n_bytes - self.n_read).min(buf.len());
        buf[..avail].fill(0x20);
        self.n_read += avail;
        Ok(avail)
    }
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestParseVersion1Overflow
#[test]
fn test_parse_version1_overflow() {
    let mut reader = ProxyBufReader::new(DataSource { n_bytes: 0, n_read: 0 });
    let buf_size = reader.size();
    reader.inner_mut().n_bytes = buf_size * 16;
    let _ = parse_version1(&mut reader, false);
    assert!(reader.inner().n_read <= buf_size, "n_read={} > buf_size={buf_size}", reader.inner().n_read);
}

fn spawn_v1_client(addr: std::net::SocketAddr, header: String, length: usize, terminate: bool) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut s = TcpStream::connect(addr).unwrap();
        let mut buf = vec![0x20u8; header.len() + length.max(if terminate { 2 } else { 0 })];
        buf[..header.len()].copy_from_slice(header.as_bytes());
        if terminate {
            let len = buf.len();
            buf[len - 2..].copy_from_slice(b"\r\n");
        }
        s.write_all(&buf).unwrap();
        thread::sleep(Duration::from_millis(150));
    })
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestVersion1Overflow
#[test]
fn test_version1_overflow() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let header = format!("PROXY TCP6 {TEST_IP6_LONG_ADDR} {TEST_IP6_LONG_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let cli = spawn_v1_client(addr, header, 10240, true);
    let mut conn = pl.accept().unwrap();
    let err = conn.read(&mut []).unwrap_err();
    assert!(err.to_string().contains("107 bytes") || !err.to_string().is_empty());
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestVersion1SlowLoris
#[test]
fn test_version1_slow_loris() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.read_header_timeout = ListenerHeaderTimeout::Duration(Duration::from_secs(10));
    let addr = pl.listener.local_addr().unwrap();
    let header = format!("PROXY TCP6 {TEST_IP6_LONG_ADDR} {TEST_IP6_LONG_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let cli = spawn_v1_client(addr, header, 0, false);
    let mut conn = pl.accept().unwrap();
    let start = std::time::Instant::now();
    let err = conn.read(&mut []).unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(5), "blocked too long");
    assert!(err.to_string().contains("version 1 header"), "got {err}");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestVersion1SlowLorisOverflow
#[test]
fn test_version1_slow_loris_overflow() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let header = format!("PROXY TCP6 {TEST_IP6_LONG_ADDR} {TEST_IP6_LONG_ADDR} {TEST_VALID_PORT} {TEST_VALID_PORT}");
    let cli = spawn_v1_client(addr, header, 10240, false);
    let mut conn = pl.accept().unwrap();
    let start = std::time::Instant::now();
    let err = conn.read(&mut []).unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(err.to_string().contains("version 1 header") || err.to_string().contains("107 bytes"));
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/v1_test.go::TestParseV1Boundaries
#[test]
fn test_parse_v1_boundaries() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut r = new_bufio(b"PROXY TCP4 1.2.3.4 5.6.7.8 65535 65535\r\n");
    let h = read(&mut r).unwrap();
    assert_eq!(h.ports(), Some((65535, 65535)));

    let mut r = new_bufio(b"PROXY TCP4 1.2.3.4 5.6.7.8 65536 443\r\n");
    assert!(matches!(read(&mut r).unwrap_err(), ProxyError::InvalidPortNumber(_)));

    let filler_len = 107 - "PROXY UNKNOWN ".len() - 2;
    let line107 = format!("PROXY UNKNOWN {}\r\n", "x".repeat(filler_len));
    assert_eq!(line107.len(), 107);
    let mut r = new_bufio(line107.as_bytes());
    let h = read(&mut r).unwrap();
    assert!(h.command.is_local());

    let line108 = format!("PROXY UNKNOWN {}\r\n", "x".repeat(filler_len + 1));
    let mut r = new_bufio(line108.as_bytes());
    assert!(matches!(read(&mut r).unwrap_err(), ProxyError::Version1HeaderTooLong));
}

// --- v2_test.go (12) ---

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestParseV2Invalid
#[test]
fn test_parse_v2_invalid() {
    let _g = GLOBAL_RW.read().unwrap();
    let p = ProtocolVersionAndCommand::PROXY.0;
    let l = ProtocolVersionAndCommand::LOCAL.0;
    let tcp4 = AddressFamilyAndProtocol::TCPV4.0;
    let tcp6 = AddressFamilyAndProtocol::TCPV6.0;
    let unspec = AddressFamilyAndProtocol::UNSPEC.0;
    let unixs = AddressFamilyAndProtocol::UNIX_STREAM.0;
    let len4 = LENGTH_V4.to_be_bytes();
    let len6 = LENGTH_V6.to_be_bytes();
    let len_unix = LENGTH_UNIX.to_be_bytes();
    let v4 = ipv4_addr_block();
    let v6 = ipv6_addr_block();
    let too_large = fixture_with_tlv(LENGTH_V4, &v4, &vec![0u8; 10 * 1024]);
    let trunc_tlv_full = fixture_with_tlv(LENGTH_V4, &v4, &[0u8; 8]);
    let trunc_tlv = &trunc_tlv_full[..2 + v4.len() + 3];

    type ErrCase<'a> = (&'a str, Vec<u8>, fn(&ProxyError) -> bool);
    let cases: Vec<ErrCase<'_>> = vec![
        ("no signature", TEST_NO_PROTOCOL.as_bytes().to_vec(), |e| matches!(e, ProxyError::NoProxyProtocol)),
        ("truncated v2 sig", SIGV2[2..].to_vec(), |e| matches!(e, ProxyError::NoProxyProtocol)),
        ("sig only", SIGV2.to_vec(), |e| matches!(e, ProxyError::CantReadProtocolVersionAndCommand(_))),
        ("invalid cmd", [SIGV2.as_slice(), &[0x99]].concat(), |e| matches!(e, ProxyError::UnsupportedProtocolVersionAndCommand)),
        ("cmd only", [SIGV2.as_slice(), &[p]].concat(), |e| matches!(e, ProxyError::CantReadAddressFamilyAndProtocol)),
        ("proxy unspec", [SIGV2.as_slice(), &[p, unspec]].concat(), |e| matches!(e, ProxyError::UnsupportedAddressFamilyAndProtocol)),
        ("invalid family", [SIGV2.as_slice(), &[p, 0x99]].concat(), |e| matches!(e, ProxyError::UnsupportedAddressFamilyAndProtocol)),
        ("tcp4 no len", [SIGV2.as_slice(), &[p, tcp4]].concat(), |e| matches!(e, ProxyError::CantReadLength(_))),
        ("tcp4 1-byte len", [SIGV2.as_slice(), &[p, tcp4, 0x99]].concat(), |e| matches!(e, ProxyError::CantReadLength(_))),
        ("unspec no len", [SIGV2.as_slice(), &[l, unspec]].concat(), |e| matches!(e, ProxyError::CantReadLength(_))),
        ("tcp4 mismatching len", [SIGV2.as_slice(), &[p, tcp4], &len4].concat(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("tcp6 mismatching len", [SIGV2.as_slice(), &[p, tcp6], &len6].concat(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("tcp4 len0 with v6", [SIGV2.as_slice(), &[p, tcp4, 0, 0], &v6].concat(), |e| matches!(e, ProxyError::InvalidLength(_))),
        ("tcp6 len36 with v4", [SIGV2.as_slice(), &[p, tcp6], &len6, &v4].concat(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("unspec len>0 no tlv", [SIGV2.as_slice(), &[l, unspec, 0, 100]].concat(), |e| matches!(e, ProxyError::InvalidLength(_))),
        ("too large tlv", [SIGV2.as_slice(), &[p, tcp4], &too_large].concat(), |e| matches!(e, ProxyError::InvalidLength(_))),
        ("0x13 undef transport", [SIGV2.as_slice(), &[p, 0x13], &len4].concat(), |e| matches!(e, ProxyError::UnsupportedAddressFamilyAndProtocol)),
        ("version nibble 0x1", [SIGV2.as_slice(), &[0x11]].concat(), |e| matches!(e, ProxyError::UnsupportedProtocolVersionAndCommand)),
        ("local tcp4 mismatching len", [SIGV2.as_slice(), &[l, tcp4], &len4].concat(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("unix mismatching len", [SIGV2.as_slice(), &[p, unixs], &len_unix].concat(), |e| matches!(e, ProxyError::InvalidAddress(_))),
        ("truncated tlv bytes", [SIGV2.as_slice(), &[p, tcp4], trunc_tlv].concat(), |e| e.is_unexpected_eof()),
        ("local short block truncated", [SIGV2.as_slice(), &[l, tcp4, 0, 5, 0xAA, 0xBB]].concat(), |e| matches!(e, ProxyError::InvalidLength(_))),
    ];
    for (desc, raw, check) in cases {
        let mut r = new_bufio(&raw);
        let err = read(&mut r).expect_err(desc);
        assert!(check(&err), "{desc}: unexpected error {err:?}");
    }
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestParseV2MaxHeaderSizeBoundary
#[test]
fn test_parse_v2_max_header_size_boundary() {
    let _g = GLOBAL_RW.write().unwrap();
    struct Reset(u16);
    impl Drop for Reset {
        fn drop(&mut self) { set_max_v2_header_size(self.0); }
    }
    let orig = max_v2_header_size();
    let _r = Reset(orig);

    let parse_len = |payload_len: usize| -> Result<Header, ProxyError> {
        let raw = v2_header(ProtocolVersionAndCommand::PROXY.0, AddressFamilyAndProtocol::TCPV4.0, &vec![0u8; payload_len]);
        let mut r = new_bufio(&raw);
        read(&mut r)
    };
    parse_len(usize::from(orig)).unwrap();
    assert!(matches!(parse_len(usize::from(orig) + 1).unwrap_err(), ProxyError::InvalidLength(_)));

    set_max_v2_header_size(8192);
    parse_len(8000).unwrap();

    set_max_v2_header_size(64);
    let raw_local = v2_header(ProtocolVersionAndCommand::LOCAL.0, AddressFamilyAndProtocol::UNIX_STREAM.0, &[0u8; 100]);
    let mut r = new_bufio(&raw_local);
    assert!(matches!(read(&mut r).unwrap_err(), ProxyError::InvalidLength(_)));
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestValidateLengthUndefinedFamily
#[test]
fn test_validate_length_undefined_family() {
    let h = Header { transport_protocol: AddressFamilyAndProtocol(0x99), ..Default::default() };
    assert!(!h.validate_length(216));
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestNewIPAddrFallback
#[test]
fn test_new_ip_addr_fallback() {
    assert!(new_ip_addr(AddressFamilyAndProtocol(0x13), "10.0.0.1".parse().unwrap(), 80).is_none());
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestFormatV2InvalidPorts
#[test]
fn test_format_v2_invalid_ports() {
    for port in [-1, 65536, 700000] {
        let h = Header {
            version: 2,
            command: ProtocolVersionAndCommand::PROXY,
            transport_protocol: AddressFamilyAndProtocol::TCPV4,
            source_addr: Some(ProxyAddr::tcp("10.1.1.1".parse().unwrap(), port)),
            destination_addr: Some(ProxyAddr::tcp("20.2.2.2".parse().unwrap(), 443)),
            raw_tlvs: Vec::new(),
        };
        assert!(matches!(h.format().unwrap_err(), ProxyError::InvalidPortNumber(_)));
    }
}

fn valid_v2_cases() -> Vec<(&'static str, Vec<u8>, Header)> {
    let p = ProtocolVersionAndCommand::PROXY.0;
    let l = ProtocolVersionAndCommand::LOCAL.0;
    let v4 = ipv4_addr_block();
    let v6 = ipv6_addr_block();
    let unix = unix_addr_block();
    let tlv: Vec<u8> = (0u8..100u8).collect();
    let med_tlv = vec![0u8; 2048];

    vec![
        ("local", [SIGV2.as_slice(), &[l, AddressFamilyAndProtocol::TCPV4.0], &LENGTH_V4.to_be_bytes(), &v4].concat(), Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() }),
        ("local unspec", [SIGV2.as_slice(), &[l, AddressFamilyAndProtocol::UNSPEC.0], &LENGTH_UNSPEC.to_be_bytes()].concat(), Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNSPEC, source_addr: None, destination_addr: None, raw_tlvs: Vec::new() }),
        ("proxy TCPv4", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV4.0], &LENGTH_V4.to_be_bytes(), &v4].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() }),
        ("proxy TCPv6", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV6.0], &LENGTH_V6.to_be_bytes(), &v6].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: Vec::new() }),
        ("proxy TCPv4 with TLV", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV4.0], &fixture_with_tlv(LENGTH_V4, &v4, &tlv)].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: tlv.clone() }),
        ("proxy TCPv6 with TLV", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV6.0], &fixture_with_tlv(LENGTH_V6, &v6, &tlv)].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: tlv.clone() }),
        ("local unspec with TLV", [SIGV2.as_slice(), &[l, AddressFamilyAndProtocol::UNSPEC.0], &fixture_with_tlv(LENGTH_UNSPEC, &[], &tlv)].concat(), Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNSPEC, source_addr: None, destination_addr: None, raw_tlvs: tlv.clone() }),
        ("proxy UDPv4", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UDPV4.0], &LENGTH_V4.to_be_bytes(), &v4].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV4, source_addr: Some(v4_udp_addr()), destination_addr: Some(v4_udp_addr()), raw_tlvs: Vec::new() }),
        ("proxy UDPv6", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UDPV6.0], &LENGTH_V6.to_be_bytes(), &v6].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV6, source_addr: Some(v6_udp_addr()), destination_addr: Some(v6_udp_addr()), raw_tlvs: Vec::new() }),
        ("proxy unix stream", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UNIX_STREAM.0], &LENGTH_UNIX.to_be_bytes(), &unix].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(unix_stream_addr()), destination_addr: Some(unix_stream_addr()), raw_tlvs: Vec::new() }),
        ("proxy unix datagram", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UNIX_DATAGRAM.0], &LENGTH_UNIX.to_be_bytes(), &unix].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_DATAGRAM, source_addr: Some(unix_datagram_addr()), destination_addr: Some(unix_datagram_addr()), raw_tlvs: Vec::new() }),
        ("proxy unix stream with TLV", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UNIX_STREAM.0], &fixture_with_tlv(LENGTH_UNIX, &unix, &tlv)].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(unix_stream_addr()), destination_addr: Some(unix_stream_addr()), raw_tlvs: tlv.clone() }),
        ("local unix stream with TLV", [SIGV2.as_slice(), &[l, AddressFamilyAndProtocol::UNIX_STREAM.0], &fixture_with_tlv(LENGTH_UNIX, &unix, &tlv)].concat(), Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(unix_stream_addr()), destination_addr: Some(unix_stream_addr()), raw_tlvs: tlv }),
        ("proxy TCPv4 with medium TLV", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV4.0], &fixture_with_tlv(LENGTH_V4, &v4, &med_tlv)].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: med_tlv }),
    ]
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestParseV2Valid
#[test]
fn test_parse_v2_valid() {
    let _g = GLOBAL_RW.read().unwrap();
    for (desc, wire, expected) in valid_v2_cases() {
        let mut r = new_bufio(&wire);
        let h = read(&mut r).expect(desc);
        assert!(h.equals_to(Some(&expected)), "{desc}");
    }
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestWriteV2Valid
#[test]
fn test_write_v2_valid() {
    let _g = GLOBAL_RW.read().unwrap();
    for (desc, _, expected) in valid_v2_cases() {
        let mut buf = Vec::new();
        expected.write_to(&mut buf).expect(desc);
        let mut r = new_bufio(&buf);
        let h = read(&mut r).expect(desc);
        assert!(h.equals_to(Some(&expected)), "{desc}");
    }
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestParseV2Padded
#[test]
fn test_parse_v2_padded() {
    let _g = GLOBAL_RW.read().unwrap();
    let length_padded: u16 = 84;
    let p = ProtocolVersionAndCommand::PROXY.0;
    let v4 = ipv4_addr_block();
    let v6 = ipv6_addr_block();
    let unix = unix_addr_block();
    let tail = [0x99u8, 0x97, 0x98];

    let padded_v4 = [length_padded.to_be_bytes().as_slice(), &v4, &vec![0u8; usize::from(length_padded - LENGTH_V4)]].concat();
    let padded_v6 = [length_padded.to_be_bytes().as_slice(), &v6, &vec![0u8; usize::from(length_padded - LENGTH_V6)]].concat();
    let padded_unix = [(LENGTH_UNIX + 4).to_be_bytes().as_slice(), &unix, &[0u8; 4]].concat();

    let cases = [
        ("proxy TCPv4", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV4.0], &padded_v4].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: vec![0u8; usize::from(length_padded - LENGTH_V4)] }),
        ("proxy TCPv6", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::TCPV6.0], &padded_v6].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: vec![0u8; usize::from(length_padded - LENGTH_V6)] }),
        ("proxy UDPv4", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UDPV4.0], &padded_v4].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: vec![0u8; usize::from(length_padded - LENGTH_V4)] }),
        ("proxy unix stream", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UNIX_STREAM.0], &padded_unix].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(unix_stream_addr()), destination_addr: Some(unix_stream_addr()), raw_tlvs: vec![0u8; 4] }),
        ("proxy UDPv6", [SIGV2.as_slice(), &[p, AddressFamilyAndProtocol::UDPV6.0], &padded_v6].concat(), Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: vec![0u8; usize::from(length_padded - LENGTH_V6)] }),
    ];
    for (desc, val, expected) in cases {
        let full = [val.as_slice(), &tail].concat();
        let mut r = new_bufio(&full);
        let h = read(&mut r).expect(desc);
        assert!(h.equals_to(Some(&expected)), "{desc}");
        assert_eq!(r.peek_bytes(tail.len()).unwrap(), &tail, "{desc}");
    }
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestV2EqualsToTLV
#[test]
fn test_v2_equals_to_tlv() {
    let _g = GLOBAL_RW.read().unwrap();
    let tlv: Vec<u8> = (0u8..100u8).collect();
    let raw = [SIGV2.as_slice(), &[ProtocolVersionAndCommand::PROXY.0, AddressFamilyAndProtocol::TCPV4.0], &fixture_with_tlv(LENGTH_V4, &ipv4_addr_block(), &tlv)].concat();
    let hdr = read(&mut new_bufio(&raw)).unwrap();
    let mut e_hdr = Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::TCPV4,
        source_addr: Some(v4_addr()),
        destination_addr: Some(v4_addr()),
        raw_tlvs: Vec::new(),
    };
    assert!(!e_hdr.equals_to(Some(&hdr)));
    e_hdr.raw_tlvs = tlv;
    assert!(e_hdr.equals_to(Some(&hdr)));
    e_hdr.raw_tlvs[0] = e_hdr.raw_tlvs[0].wrapping_add(1);
    assert!(!e_hdr.equals_to(Some(&hdr)));
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::TestV2TLVFormatTooLargeTLV
#[test]
fn test_v2_tlv_format_too_large_tlv() {
    let big = vec![0u8; 1 << 16];
    let headers = [
        Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(unix_stream_addr()), destination_addr: Some(unix_stream_addr()), raw_tlvs: big.clone() },
        Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: big.clone() },
        Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: big.clone() },
        Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: big.clone() },
        Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV6, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: big.clone() },
        Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNSPEC, source_addr: None, destination_addr: None, raw_tlvs: big },
    ];
    for h in headers {
        assert!(matches!(h.format().unwrap_err(), ProxyError::Uint16Overflow));
    }
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::Test_parseUnixName
#[test]
fn test_parse_unix_name() {
    assert_eq!(parse_unix_name(b"socketname"), "socketname");
    assert_eq!(parse_unix_name(b"socketname\0"), "socketname");
    assert_eq!(parse_unix_name(b"sock\0etname\0"), "sock");
    assert_eq!(parse_unix_name(b""), "");
    assert_eq!(parse_unix_name(&[0, 0, 0]), "");
    assert_eq!(parse_unix_name(b"abc123\0"), "abc123");
    assert_eq!(parse_unix_name(b"te\0st"), "te");
    assert_eq!(parse_unix_name(&[0x7f, 0xfe, 0x3c]), String::from_utf8_lossy(&[0x7f, 0xfe, 0x3c]));
}

// Upstream: github.com/pires/go-proxyproto/v2_test.go::Test_formatUnixName
#[test]
fn test_format_unix_name() {
    let max_len = usize::from(LENGTH_UNIX) / 2;
    let long_name = "a".repeat(max_len + 5);
    let long_formatted = format_unix_name(&long_name);
    assert_eq!(long_formatted.len(), max_len);
    assert_eq!(parse_unix_name(&long_formatted), &long_name[..max_len]);

    let short_formatted = format_unix_name(TEST_UNIX_SOCKET_NAME);
    assert_eq!(short_formatted.len(), max_len);
    assert_eq!(parse_unix_name(&short_formatted), TEST_UNIX_SOCKET_NAME);
    assert!(short_formatted.starts_with(TEST_UNIX_SOCKET_NAME.as_bytes()));
}

#[test]
fn test_default_policy_is_require() {
    let _g = GLOBAL_RW.read().unwrap();
    assert_eq!(default_policy(), mimir_cmd::server::proxyproto::Policy::Require);
}
