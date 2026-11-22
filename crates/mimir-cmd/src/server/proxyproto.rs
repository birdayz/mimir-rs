//! PROXY protocol (v1 and v2) parser, writer, policy engine, and stream wrappers.
//!
//! Ported from github.com/pires/go-proxyproto (v0.16.0):
//! - header.go, v1.go, v2.go, tlv.go, udp.go, policy.go, protocol.go, addr_proto.go, version_cmd.go

use std::any::Any;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Signature for PROXY protocol v1.
pub const SIGV1: &[u8; 5] = b"PROXY";
/// Signature for PROXY protocol v2.
pub const SIGV2: &[u8; 12] = b"\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A";

/// Default size used for internal header-detection read buffer (`protocol.go:readBufferSize`).
pub const DEFAULT_READ_BUFFER_SIZE: usize = 256;
/// Minimum read buffer size enforced by `ProxyBufReader` (matches Go `bufio.minReadBufferSize`).
pub const MIN_READ_BUFFER_SIZE: usize = 16;
/// Maximum v1 header length including trailing CRLF.
pub const MAX_V1_HEADER_LEN: usize = 107;
/// Length of each Unix socket name field in a v2 address block.
pub const UNIX_NAME_LEN: usize = 108;

/// Default header read timeout (`DefaultReadHeaderTimeout`).
pub const DEFAULT_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// Default max v2 header payload size (`MaxV2HeaderSize`).
pub const DEFAULT_MAX_V2_HEADER_SIZE: u16 = 4096;

pub const LENGTH_UNSPEC: u16 = 0;
pub const LENGTH_V4: u16 = 12;
pub const LENGTH_V6: u16 = 36;
pub const LENGTH_UNIX: u16 = 216;

pub const NETWORK_UNIX: &str = "unix";
pub const NETWORK_UNIXGRAM: &str = "unixgram";

static GLOBAL_DEFAULT_POLICY: AtomicU8 = AtomicU8::new(Policy::Require as u8);
static GLOBAL_DEFAULT_READ_HEADER_TIMEOUT_NANOS: AtomicU64 = AtomicU64::new(10_000_000_000);
static GLOBAL_MAX_V2_HEADER_SIZE: AtomicU16 = AtomicU16::new(DEFAULT_MAX_V2_HEADER_SIZE);
static GLOBAL_V1_ACCEPT_IPV4_IN_TCP6: AtomicBool = AtomicBool::new(false);

pub fn default_policy() -> Policy {
    match GLOBAL_DEFAULT_POLICY.load(Ordering::Acquire) {
        0 => Policy::Use,
        1 => Policy::Ignore,
        2 => Policy::Reject,
        3 => Policy::Require,
        4 => Policy::Skip,
        _ => Policy::Require,
    }
}

pub fn set_default_policy(policy: Policy) {
    GLOBAL_DEFAULT_POLICY.store(policy as u8, Ordering::Release);
}

pub fn default_read_header_timeout() -> Duration {
    Duration::from_nanos(GLOBAL_DEFAULT_READ_HEADER_TIMEOUT_NANOS.load(Ordering::Acquire))
}

pub fn set_default_read_header_timeout(timeout: Duration) {
    let nanos = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
    GLOBAL_DEFAULT_READ_HEADER_TIMEOUT_NANOS.store(nanos, Ordering::Release);
}

pub fn max_v2_header_size() -> u16 {
    GLOBAL_MAX_V2_HEADER_SIZE.load(Ordering::Acquire)
}

pub fn set_max_v2_header_size(size: u16) {
    GLOBAL_MAX_V2_HEADER_SIZE.store(size, Ordering::Release);
}

pub fn v1_accept_ipv4_in_tcp6() -> bool {
    GLOBAL_V1_ACCEPT_IPV4_IN_TCP6.load(Ordering::Acquire)
}

pub fn set_v1_accept_ipv4_in_tcp6(enabled: bool) {
    GLOBAL_V1_ACCEPT_IPV4_IN_TCP6.store(enabled, Ordering::Release);
}

/// Per-call parsing options (defaults to the global settings above).
#[derive(Debug, Clone, Copy)]
pub struct ReadOptions {
    pub max_v2_header_size: u16,
    pub v1_accept_ipv4_in_tcp6: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_v2_header_size: max_v2_header_size(),
            v1_accept_ipv4_in_tcp6: v1_accept_ipv4_in_tcp6(),
        }
    }
}

/// Errors produced by PROXY protocol parsing, formatting, and policy enforcement.
#[derive(Debug)]
pub enum ProxyError {
    CantReadVersion1Header(Option<String>),
    Version1HeaderTooLong,
    LineMustEndWithCrlf,
    CantReadProtocolVersionAndCommand(Option<String>),
    CantReadAddressFamilyAndProtocol,
    CantReadLength(Option<String>),
    CantResolveSourceUnixAddress,
    CantResolveDestinationUnixAddress,
    NoProxyProtocol,
    UnknownProxyProtocolVersion,
    UnsupportedProtocolVersionAndCommand,
    UnsupportedAddressFamilyAndProtocol,
    InvalidLength(Option<String>),
    InvalidAddress(Option<String>),
    InvalidPortNumber(Option<String>),
    SuperfluousProxyHeader,
    TruncatedTlv,
    MalformedTlv,
    IncompatibleTlv,
    InvalidUpstream(String),
    Uint16Overflow,
    CannotFormatTlv { typ: PP2Type, len: usize },
    InvalidIpRange(String),
    InvalidIpAddress(String),
    BothPoliciesSet,
    Io(io::Error),
    Custom(String),
}

impl ProxyError {
    pub fn is_no_proxy_protocol(&self) -> bool {
        matches!(self, Self::NoProxyProtocol)
    }

    pub fn is_invalid_upstream(&self) -> bool {
        matches!(self, Self::InvalidUpstream(_))
    }

    pub fn is_unexpected_eof(&self) -> bool {
        matches!(self, Self::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(
            self,
            Self::Io(e) if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock
        )
    }
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CantReadVersion1Header(None) => write!(f, "proxyproto: can't read version 1 header"),
            Self::CantReadVersion1Header(Some(e)) => write!(f, "proxyproto: can't read version 1 header: {e}"),
            Self::Version1HeaderTooLong => write!(f, "proxyproto: version 1 header must be 107 bytes or less"),
            Self::LineMustEndWithCrlf => write!(f, "proxyproto: version 1 header is invalid, must end with \\r\\n"),
            Self::CantReadProtocolVersionAndCommand(None) => {
                write!(f, "proxyproto: can't read proxy protocol version and command")
            }
            Self::CantReadProtocolVersionAndCommand(Some(e)) => {
                write!(f, "proxyproto: can't read proxy protocol version and command: {e}")
            }
            Self::CantReadAddressFamilyAndProtocol => {
                write!(f, "proxyproto: can't read address family or protocol")
            }
            Self::CantReadLength(None) => write!(f, "proxyproto: can't read length"),
            Self::CantReadLength(Some(e)) => write!(f, "proxyproto: can't read length: {e}"),
            Self::CantResolveSourceUnixAddress => write!(f, "proxyproto: can't resolve source Unix address"),
            Self::CantResolveDestinationUnixAddress => {
                write!(f, "proxyproto: can't resolve destination Unix address")
            }
            Self::NoProxyProtocol => write!(f, "proxyproto: proxy protocol signature not present"),
            Self::UnknownProxyProtocolVersion => write!(f, "proxyproto: unknown proxy protocol version"),
            Self::UnsupportedProtocolVersionAndCommand => {
                write!(f, "proxyproto: unsupported proxy protocol version and command")
            }
            Self::UnsupportedAddressFamilyAndProtocol => {
                write!(f, "proxyproto: unsupported address family and protocol")
            }
            Self::InvalidLength(None) => write!(f, "proxyproto: invalid length"),
            Self::InvalidLength(Some(e)) => write!(f, "proxyproto: invalid length: {e}"),
            Self::InvalidAddress(None) => write!(f, "proxyproto: invalid address"),
            Self::InvalidAddress(Some(e)) => write!(f, "proxyproto: invalid address: {e}"),
            Self::InvalidPortNumber(None) => write!(f, "proxyproto: invalid port number"),
            Self::InvalidPortNumber(Some(e)) => write!(f, "proxyproto: invalid port number: {e}"),
            Self::SuperfluousProxyHeader => {
                write!(f, "proxyproto: upstream connection sent PROXY header but isn't allowed to send one")
            }
            Self::TruncatedTlv => write!(f, "proxyproto: truncated TLV"),
            Self::MalformedTlv => write!(f, "proxyproto: malformed TLV Value"),
            Self::IncompatibleTlv => write!(f, "proxyproto: incompatible TLV type"),
            Self::InvalidUpstream(detail) => {
                if detail.is_empty() {
                    write!(f, "proxyproto: upstream connection address not trusted for PROXY information")
                } else {
                    write!(
                        f,
                        "proxyproto: upstream connection address not trusted for PROXY information: {detail}"
                    )
                }
            }
            Self::Uint16Overflow => write!(f, "proxyproto: uint16 overflow"),
            Self::CannotFormatTlv { typ, len } => {
                write!(f, "proxyproto: cannot format TLV {} with length {len}", typ.0)
            }
            Self::InvalidIpRange(msg) => write!(f, "proxyproto: {msg}"),
            Self::InvalidIpAddress(msg) => write!(f, "proxyproto: {msg}"),
            Self::BothPoliciesSet => write!(f, "only one of policy or connpolicy must be provided."),
            Self::Io(e) => write!(f, "{e}"),
            Self::Custom(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for ProxyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ProxyError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<ProxyError> for io::Error {
    fn from(err: ProxyError) -> Self {
        match err {
            ProxyError::Io(e) => e,
            other => io::Error::new(io::ErrorKind::InvalidData, other),
        }
    }
}

impl Clone for ProxyError {
    fn clone(&self) -> Self {
        match self {
            Self::CantReadVersion1Header(s) => Self::CantReadVersion1Header(s.clone()),
            Self::Version1HeaderTooLong => Self::Version1HeaderTooLong,
            Self::LineMustEndWithCrlf => Self::LineMustEndWithCrlf,
            Self::CantReadProtocolVersionAndCommand(s) => Self::CantReadProtocolVersionAndCommand(s.clone()),
            Self::CantReadAddressFamilyAndProtocol => Self::CantReadAddressFamilyAndProtocol,
            Self::CantReadLength(s) => Self::CantReadLength(s.clone()),
            Self::CantResolveSourceUnixAddress => Self::CantResolveSourceUnixAddress,
            Self::CantResolveDestinationUnixAddress => Self::CantResolveDestinationUnixAddress,
            Self::NoProxyProtocol => Self::NoProxyProtocol,
            Self::UnknownProxyProtocolVersion => Self::UnknownProxyProtocolVersion,
            Self::UnsupportedProtocolVersionAndCommand => Self::UnsupportedProtocolVersionAndCommand,
            Self::UnsupportedAddressFamilyAndProtocol => Self::UnsupportedAddressFamilyAndProtocol,
            Self::InvalidLength(s) => Self::InvalidLength(s.clone()),
            Self::InvalidAddress(s) => Self::InvalidAddress(s.clone()),
            Self::InvalidPortNumber(s) => Self::InvalidPortNumber(s.clone()),
            Self::SuperfluousProxyHeader => Self::SuperfluousProxyHeader,
            Self::TruncatedTlv => Self::TruncatedTlv,
            Self::MalformedTlv => Self::MalformedTlv,
            Self::IncompatibleTlv => Self::IncompatibleTlv,
            Self::InvalidUpstream(s) => Self::InvalidUpstream(s.clone()),
            Self::Uint16Overflow => Self::Uint16Overflow,
            Self::CannotFormatTlv { typ, len } => Self::CannotFormatTlv { typ: *typ, len: *len },
            Self::InvalidIpRange(s) => Self::InvalidIpRange(s.clone()),
            Self::InvalidIpAddress(s) => Self::InvalidIpAddress(s.clone()),
            Self::BothPoliciesSet => Self::BothPoliciesSet,
            Self::Io(e) => Self::Io(io::Error::new(e.kind(), e.to_string())),
            Self::Custom(s) => Self::Custom(s.clone()),
        }
    }
}

/// Command in PROXY protocol v2 (`version_cmd.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolVersionAndCommand(pub u8);

impl ProtocolVersionAndCommand {
    pub const LOCAL: Self = Self(0x20);
    pub const PROXY: Self = Self(0x21);

    pub fn is_local(self) -> bool {
        self == Self::LOCAL
    }

    pub fn is_proxy(self) -> bool {
        self == Self::PROXY
    }

    pub fn is_unspec(self) -> bool {
        !self.is_local() && !self.is_proxy()
    }

    pub fn is_supported(self) -> bool {
        self == Self::LOCAL || self == Self::PROXY
    }

    pub fn to_byte(self) -> u8 {
        if self.is_local() {
            Self::LOCAL.0
        } else if self.is_proxy() {
            Self::PROXY.0
        } else {
            Self::LOCAL.0
        }
    }
}

/// Address family and transport protocol (`addr_proto.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AddressFamilyAndProtocol(pub u8);

impl AddressFamilyAndProtocol {
    pub const UNSPEC: Self = Self(0x00);
    pub const TCPV4: Self = Self(0x11);
    pub const UDPV4: Self = Self(0x12);
    pub const TCPV6: Self = Self(0x21);
    pub const UDPV6: Self = Self(0x22);
    pub const UNIX_STREAM: Self = Self(0x31);
    pub const UNIX_DATAGRAM: Self = Self(0x32);

    pub fn is_supported(self) -> bool {
        matches!(
            self,
            Self::UNSPEC
                | Self::TCPV4
                | Self::UDPV4
                | Self::TCPV6
                | Self::UDPV6
                | Self::UNIX_STREAM
                | Self::UNIX_DATAGRAM
        )
    }

    pub fn is_ipv4(self) -> bool {
        (self.0 & 0xF0) == 0x10
    }

    pub fn is_ipv6(self) -> bool {
        (self.0 & 0xF0) == 0x20
    }

    pub fn is_unix(self) -> bool {
        (self.0 & 0xF0) == 0x30
    }

    pub fn is_stream(self) -> bool {
        (self.0 & 0x0F) == 0x01
    }

    pub fn is_datagram(self) -> bool {
        (self.0 & 0x0F) == 0x02
    }

    pub fn is_unspec(self) -> bool {
        (self.0 & 0xF0) == 0x00 || (self.0 & 0x0F) == 0x00
    }

    pub fn to_byte(self) -> u8 {
        if self.is_ipv4() && self.is_stream() {
            Self::TCPV4.0
        } else if self.is_ipv4() && self.is_datagram() {
            Self::UDPV4.0
        } else if self.is_ipv6() && self.is_stream() {
            Self::TCPV6.0
        } else if self.is_ipv6() && self.is_datagram() {
            Self::UDPV6.0
        } else if self.is_unix() && self.is_stream() {
            Self::UNIX_STREAM.0
        } else if self.is_unix() && self.is_datagram() {
            Self::UNIX_DATAGRAM.0
        } else {
            Self::UNSPEC.0
        }
    }
}

/// PROXY protocol v2 TLV type (`tlv.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PP2Type(pub u8);

impl PP2Type {
    pub const ALPN: Self = Self(0x01);
    pub const AUTHORITY: Self = Self(0x02);
    pub const CRC32C: Self = Self(0x03);
    pub const NOOP: Self = Self(0x04);
    pub const UNIQUE_ID: Self = Self(0x05);
    pub const SSL: Self = Self(0x20);
    pub const SUBTYPE_SSL_VERSION: Self = Self(0x21);
    pub const SUBTYPE_SSL_CN: Self = Self(0x22);
    pub const SUBTYPE_SSL_CIPHER: Self = Self(0x23);
    pub const SUBTYPE_SSL_SIG_ALG: Self = Self(0x24);
    pub const SUBTYPE_SSL_KEY_ALG: Self = Self(0x25);
    pub const SUBTYPE_SSL_GROUP: Self = Self(0x26);
    pub const SUBTYPE_SSL_SIG_SCHEME: Self = Self(0x27);
    pub const SUBTYPE_SSL_CLIENT_CERT: Self = Self(0x28);
    pub const NETNS: Self = Self(0x30);

    pub const MIN_CUSTOM: Self = Self(0xE0);
    pub const MAX_CUSTOM: Self = Self(0xEF);
    pub const MIN_EXPERIMENT: Self = Self(0xF0);
    pub const MAX_EXPERIMENT: Self = Self(0xF7);
    pub const MIN_FUTURE: Self = Self(0xF8);
    pub const MAX_FUTURE: Self = Self(0xFF);

    pub fn registered(self) -> bool {
        matches!(
            self,
            Self::ALPN
                | Self::AUTHORITY
                | Self::CRC32C
                | Self::NOOP
                | Self::UNIQUE_ID
                | Self::SSL
                | Self::SUBTYPE_SSL_VERSION
                | Self::SUBTYPE_SSL_CN
                | Self::SUBTYPE_SSL_CIPHER
                | Self::SUBTYPE_SSL_SIG_ALG
                | Self::SUBTYPE_SSL_KEY_ALG
                | Self::SUBTYPE_SSL_GROUP
                | Self::SUBTYPE_SSL_SIG_SCHEME
                | Self::SUBTYPE_SSL_CLIENT_CERT
                | Self::NETNS
        )
    }

    pub fn app(self) -> bool {
        self.0 >= Self::MIN_CUSTOM.0 && self.0 <= Self::MAX_CUSTOM.0
    }

    pub fn experiment(self) -> bool {
        self.0 >= Self::MIN_EXPERIMENT.0 && self.0 <= Self::MAX_EXPERIMENT.0
    }

    pub fn future(self) -> bool {
        self.0 >= Self::MIN_FUTURE.0
    }

    pub fn spec(self) -> bool {
        self.registered() || self.app() || self.experiment() || self.future()
    }
}

/// Uninterpreted Type-Length-Value for PROXY protocol v2 (`tlv.go`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tlv {
    pub typ: PP2Type,
    pub value: Vec<u8>,
}

/// Splits a raw TLV vector into `Tlv` records (`SplitTLVs`).
pub fn split_tlvs(raw: &[u8]) -> Result<Vec<Tlv>, ProxyError> {
    let mut tlvs = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        let typ = PP2Type(raw[i]);
        if raw.len() - i <= 2 {
            return Err(ProxyError::TruncatedTlv);
        }
        let tlv_len = u16::from_be_bytes([raw[i + 1], raw[i + 2]]) as usize;
        i += 3;
        if i + tlv_len > raw.len() {
            return Err(ProxyError::TruncatedTlv);
        }
        let value = if typ != PP2Type::NOOP {
            raw[i..i + tlv_len].to_vec()
        } else {
            Vec::new()
        };
        i += tlv_len;
        tlvs.push(Tlv { typ, value });
    }
    Ok(tlvs)
}

/// Joins multiple `Tlv` records into raw wire bytes (`JoinTLVs`).
pub fn join_tlvs(tlvs: &[Tlv]) -> Result<Vec<u8>, ProxyError> {
    let mut raw = Vec::new();
    for tlv in tlvs {
        let len_u16 = u16::try_from(tlv.value.len()).map_err(|_| ProxyError::CannotFormatTlv {
            typ: tlv.typ,
            len: tlv.value.len(),
        })?;
        raw.push(tlv.typ.0);
        raw.extend_from_slice(&len_u16.to_be_bytes());
        raw.extend_from_slice(&tlv.value);
    }
    Ok(raw)
}

/// Address abstraction matching Go `net.Addr` variants used by `go-proxyproto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyAddr {
    Tcp { ip: Option<IpAddr>, port: i32 },
    Udp { ip: Option<IpAddr>, port: i32 },
    Unix { net: String, name: String },
    Other { network: String, addr: String },
}

impl ProxyAddr {
    pub fn tcp(ip: IpAddr, port: i32) -> Self {
        Self::Tcp { ip: Some(ip), port }
    }

    pub fn udp(ip: IpAddr, port: i32) -> Self {
        Self::Udp { ip: Some(ip), port }
    }

    pub fn unix(name: impl Into<String>) -> Self {
        Self::Unix {
            net: NETWORK_UNIX.to_string(),
            name: name.into(),
        }
    }

    pub fn unixgram(name: impl Into<String>) -> Self {
        Self::Unix {
            net: NETWORK_UNIXGRAM.to_string(),
            name: name.into(),
        }
    }

    pub fn other(network: impl Into<String>, addr: impl Into<String>) -> Self {
        Self::Other {
            network: network.into(),
            addr: addr.into(),
        }
    }

    pub fn network(&self) -> &str {
        match self {
            Self::Tcp { .. } => "tcp",
            Self::Udp { .. } => "udp",
            Self::Unix { net, .. } => net.as_str(),
            Self::Other { network, .. } => network.as_str(),
        }
    }

    pub fn as_socket_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp { ip: Some(ip), port } | Self::Udp { ip: Some(ip), port } => {
                let port = u16::try_from(*port).ok()?;
                Some(SocketAddr::new(*ip, port))
            }
            _ => None,
        }
    }
}

impl From<SocketAddr> for ProxyAddr {
    fn from(addr: SocketAddr) -> Self {
        Self::Tcp {
            ip: Some(addr.ip()),
            port: i32::from(addr.port()),
        }
    }
}

fn format_go_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                v4.to_string()
            } else {
                v6.to_string()
            }
        }
    }
}

impl fmt::Display for ProxyAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp { ip, port } | Self::Udp { ip, port } => match ip {
                Some(ip) => {
                    let s = format_go_ip(*ip);
                    if s.contains(':') {
                        write!(f, "[{s}]:{port}")
                    } else {
                        write!(f, "{s}:{port}")
                    }
                }
                None => write!(f, "<nil>:{port}"),
            },
            Self::Unix { name, .. } => write!(f, "{name}"),
            Self::Other { addr, .. } => write!(f, "{addr}"),
        }
    }
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
    }
}

fn ip_to_v4(ip: Option<IpAddr>) -> Option<Ipv4Addr> {
    match ip? {
        IpAddr::V4(v4) => Some(v4),
        IpAddr::V6(v6) => v6.to_ipv4_mapped(),
    }
}

fn ip_to_v6(ip: Option<IpAddr>) -> Option<Ipv6Addr> {
    match ip? {
        IpAddr::V4(v4) => Some(v4.to_ipv6_mapped()),
        IpAddr::V6(v6) => Some(v6),
    }
}

fn addrs_equal(a: Option<&ProxyAddr>, b: Option<&ProxyAddr>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.to_string() == b.to_string(),
        _ => false,
    }
}

/// PROXY protocol header (`header.go`).
#[derive(Debug, Clone)]
pub struct Header {
    pub version: u8,
    pub command: ProtocolVersionAndCommand,
    pub transport_protocol: AddressFamilyAndProtocol,
    pub source_addr: Option<ProxyAddr>,
    pub destination_addr: Option<ProxyAddr>,
    pub raw_tlvs: Vec<u8>,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            version: 0,
            command: ProtocolVersionAndCommand::LOCAL,
            transport_protocol: AddressFamilyAndProtocol::UNSPEC,
            source_addr: None,
            destination_addr: None,
            raw_tlvs: Vec::new(),
        }
    }
}

impl Header {
    /// Creates a new PROXY header from source and destination addresses (`HeaderProxyFromAddrs`).
    pub fn proxy_from_addrs(version: u8, source_addr: &ProxyAddr, dest_addr: &ProxyAddr) -> Self {
        let ver = if !(1..=2).contains(&version) { 2 } else { version };
        let mut h = Self {
            version: ver,
            command: ProtocolVersionAndCommand::LOCAL,
            transport_protocol: AddressFamilyAndProtocol::UNSPEC,
            source_addr: None,
            destination_addr: None,
            raw_tlvs: Vec::new(),
        };

        match (source_addr, dest_addr) {
            (ProxyAddr::Tcp { ip: src_ip, .. }, ProxyAddr::Tcp { ip: dst_ip, .. }) => {
                if ip_to_v4(*src_ip).is_some() && ip_to_v4(*dst_ip).is_some() {
                    h.transport_protocol = AddressFamilyAndProtocol::TCPV4;
                } else if ip_to_v6(*src_ip).is_some() && ip_to_v6(*dst_ip).is_some() {
                    h.transport_protocol = AddressFamilyAndProtocol::TCPV6;
                }
            }
            (ProxyAddr::Udp { ip: src_ip, .. }, ProxyAddr::Udp { ip: dst_ip, .. }) => {
                if ip_to_v4(*src_ip).is_some() && ip_to_v4(*dst_ip).is_some() {
                    h.transport_protocol = AddressFamilyAndProtocol::UDPV4;
                } else if ip_to_v6(*src_ip).is_some() && ip_to_v6(*dst_ip).is_some() {
                    h.transport_protocol = AddressFamilyAndProtocol::UDPV6;
                }
            }
            (ProxyAddr::Unix { net: src_net, .. }, ProxyAddr::Unix { net: dst_net, .. })
                if src_net == dst_net =>
            {
                if src_net == NETWORK_UNIX {
                    h.transport_protocol = AddressFamilyAndProtocol::UNIX_STREAM;
                } else if src_net == NETWORK_UNIXGRAM {
                    h.transport_protocol = AddressFamilyAndProtocol::UNIX_DATAGRAM;
                }
            }
            _ => {}
        }

        if h.transport_protocol != AddressFamilyAndProtocol::UNSPEC {
            h.command = ProtocolVersionAndCommand::PROXY;
            h.source_addr = Some(source_addr.clone());
            h.destination_addr = Some(dest_addr.clone());
        }

        h
    }

    /// Returns TCP source/destination addresses if the header is stream-based (`TCPAddrs`).
    pub fn tcp_addrs(&self) -> Option<(&ProxyAddr, &ProxyAddr)> {
        if !self.transport_protocol.is_stream() {
            return None;
        }
        match (self.source_addr.as_ref(), self.destination_addr.as_ref()) {
            (Some(s @ ProxyAddr::Tcp { .. }), Some(d @ ProxyAddr::Tcp { .. })) => Some((s, d)),
            _ => None,
        }
    }

    /// Returns UDP source/destination addresses if the header is datagram-based (`UDPAddrs`).
    pub fn udp_addrs(&self) -> Option<(&ProxyAddr, &ProxyAddr)> {
        if !self.transport_protocol.is_datagram() {
            return None;
        }
        match (self.source_addr.as_ref(), self.destination_addr.as_ref()) {
            (Some(s @ ProxyAddr::Udp { .. }), Some(d @ ProxyAddr::Udp { .. })) => Some((s, d)),
            _ => None,
        }
    }

    /// Returns UNIX source/destination addresses if the header is UNIX-based (`UnixAddrs`).
    pub fn unix_addrs(&self) -> Option<(&ProxyAddr, &ProxyAddr)> {
        if !self.transport_protocol.is_unix() {
            return None;
        }
        match (self.source_addr.as_ref(), self.destination_addr.as_ref()) {
            (Some(s @ ProxyAddr::Unix { .. }), Some(d @ ProxyAddr::Unix { .. })) => Some((s, d)),
            _ => None,
        }
    }

    /// Returns source/destination IPs for TCP/UDP headers (`IPs`).
    pub fn ips(&self) -> Option<(Option<IpAddr>, Option<IpAddr>)> {
        if let Some((ProxyAddr::Tcp { ip: s, .. }, ProxyAddr::Tcp { ip: d, .. })) = self.tcp_addrs() {
            return Some((*s, *d));
        }
        if let Some((ProxyAddr::Udp { ip: s, .. }, ProxyAddr::Udp { ip: d, .. })) = self.udp_addrs() {
            return Some((*s, *d));
        }
        None
    }

    /// Returns source/destination ports for TCP/UDP headers (`Ports`).
    pub fn ports(&self) -> Option<(i32, i32)> {
        if let Some((ProxyAddr::Tcp { port: s, .. }, ProxyAddr::Tcp { port: d, .. })) = self.tcp_addrs() {
            return Some((*s, *d));
        }
        if let Some((ProxyAddr::Udp { port: s, .. }, ProxyAddr::Udp { port: d, .. })) = self.udp_addrs() {
            return Some((*s, *d));
        }
        None
    }

    /// Deprecated wrapper delegating to `equals_to` (`EqualTo`).
    pub fn equal_to(&self, other: Option<&Self>) -> bool {
        self.equals_to(other)
    }

    /// Returns true if headers are equivalent (`EqualsTo`).
    pub fn equals_to(&self, other: Option<&Self>) -> bool {
        let Some(other) = other else {
            return false;
        };
        if self.version != other.version
            || self.command != other.command
            || self.transport_protocol != other.transport_protocol
        {
            return false;
        }
        if self.version == 2 && self.raw_tlvs != other.raw_tlvs {
            return false;
        }
        if self.command == ProtocolVersionAndCommand::LOCAL {
            return true;
        }
        addrs_equal(self.source_addr.as_ref(), other.source_addr.as_ref())
            && addrs_equal(self.destination_addr.as_ref(), other.destination_addr.as_ref())
    }

    /// Returns the TLVs stored in this header (`TLVs`).
    pub fn tlvs(&self) -> Result<Vec<Tlv>, ProxyError> {
        split_tlvs(&self.raw_tlvs)
    }

    /// Sets the TLVs stored in this header (`SetTLVs`).
    pub fn set_tlvs(&mut self, tlvs: &[Tlv]) -> Result<(), ProxyError> {
        self.raw_tlvs = join_tlvs(tlvs)?;
        Ok(())
    }

    /// Renders a PROXY protocol header over the wire (`Format`).
    pub fn format(&self) -> Result<Vec<u8>, ProxyError> {
        match self.version {
            1 => self.format_version1(),
            2 => self.format_version2(),
            _ => Err(ProxyError::UnknownProxyProtocolVersion),
        }
    }

    /// Renders and writes the header to `w` (`WriteTo`).
    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<usize, ProxyError> {
        let buf = self.format()?;
        w.write_all(&buf)?;
        Ok(buf.len())
    }

    /// Renders the header followed by `payload` as a single UDP datagram (`FormatUDPDatagram`).
    pub fn format_udp_datagram(&self, payload: &[u8]) -> Result<Vec<u8>, ProxyError> {
        let mut buf = self.format()?;
        buf.extend_from_slice(payload);
        Ok(buf)
    }

    /// Validates v2 payload length against declared transport family (`validateLength`).
    pub fn validate_length(&self, length: u16) -> bool {
        if self.transport_protocol.is_ipv4() {
            length >= LENGTH_V4
        } else if self.transport_protocol.is_ipv6() {
            length >= LENGTH_V6
        } else if self.transport_protocol.is_unix() {
            length >= LENGTH_UNIX
        } else if self.transport_protocol.is_unspec() {
            let _ = length;
            true
        } else {
            false
        }
    }

    fn format_version1(&self) -> Result<Vec<u8>, ProxyError> {
        let proto = match self.transport_protocol {
            AddressFamilyAndProtocol::TCPV4 => "TCP4",
            AddressFamilyAndProtocol::TCPV6 => "TCP6",
            _ => return Ok(b"PROXY UNKNOWN\r\n".to_vec()),
        };

        let (
            Some(ProxyAddr::Tcp { ip: src_ip_opt, port: src_port }),
            Some(ProxyAddr::Tcp { ip: dst_ip_opt, port: dst_port }),
        ) = (self.source_addr.as_ref(), self.destination_addr.as_ref())
        else {
            return Err(ProxyError::InvalidAddress(None));
        };

        if !(0..=i32::from(u16::MAX)).contains(src_port) || !(0..=i32::from(u16::MAX)).contains(dst_port) {
            return Err(ProxyError::InvalidPortNumber(None));
        }

        let (src_str, dst_str) = match self.transport_protocol {
            AddressFamilyAndProtocol::TCPV4 => {
                let Some(s4) = ip_to_v4(*src_ip_opt) else {
                    return Err(ProxyError::InvalidAddress(None));
                };
                let Some(d4) = ip_to_v4(*dst_ip_opt) else {
                    return Err(ProxyError::InvalidAddress(None));
                };
                (s4.to_string(), d4.to_string())
            }
            AddressFamilyAndProtocol::TCPV6 => {
                let Some(s6) = ip_to_v6(*src_ip_opt) else {
                    return Err(ProxyError::InvalidAddress(None));
                };
                let Some(d6) = ip_to_v6(*dst_ip_opt) else {
                    return Err(ProxyError::InvalidAddress(None));
                };
                (s6.to_string(), d6.to_string())
            }
            _ => return Err(ProxyError::InvalidAddress(None)),
        };

        let mut out = Vec::with_capacity(108);
        out.extend_from_slice(SIGV1);
        out.push(b' ');
        out.extend_from_slice(proto.as_bytes());
        out.push(b' ');
        out.extend_from_slice(src_str.as_bytes());
        out.push(b' ');
        out.extend_from_slice(dst_str.as_bytes());
        out.push(b' ');
        out.extend_from_slice(src_port.to_string().as_bytes());
        out.push(b' ');
        out.extend_from_slice(dst_port.to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        Ok(out)
    }

    fn format_version2(&self) -> Result<Vec<u8>, ProxyError> {
        let base_len = if self.transport_protocol.is_unspec() {
            LENGTH_UNSPEC
        } else if self.transport_protocol.is_ipv4() {
            LENGTH_V4
        } else if self.transport_protocol.is_ipv6() {
            LENGTH_V6
        } else if self.transport_protocol.is_unix() {
            LENGTH_UNIX
        } else {
            LENGTH_UNSPEC
        };
        let validated_len = add_tlv_len(base_len, self.raw_tlvs.len())?;
        let mut buf = Vec::with_capacity(16 + usize::from(validated_len));
        buf.extend_from_slice(SIGV2);
        buf.push(self.command.to_byte());
        buf.push(self.transport_protocol.to_byte());

        if self.transport_protocol.is_unspec() {
            let hdr_len = add_tlv_len(LENGTH_UNSPEC, self.raw_tlvs.len())?;
            buf.extend_from_slice(&hdr_len.to_be_bytes());
        } else {
            let mut addr_src: Vec<u8> = Vec::new();
            let mut addr_dst: Vec<u8> = Vec::new();
            let mut has_addrs = false;

            if self.transport_protocol.is_ipv4() {
                let hdr_len = add_tlv_len(LENGTH_V4, self.raw_tlvs.len())?;
                buf.extend_from_slice(&hdr_len.to_be_bytes());
                if let Some((src_ip, dst_ip)) = self.ips() {
                    if let (Some(s4), Some(d4)) = (ip_to_v4(src_ip), ip_to_v4(dst_ip)) {
                        addr_src.extend_from_slice(&s4.octets());
                        addr_dst.extend_from_slice(&d4.octets());
                        has_addrs = true;
                    }
                }
            } else if self.transport_protocol.is_ipv6() {
                let hdr_len = add_tlv_len(LENGTH_V6, self.raw_tlvs.len())?;
                buf.extend_from_slice(&hdr_len.to_be_bytes());
                if let Some((src_ip, dst_ip)) = self.ips() {
                    if let (Some(s6), Some(d6)) = (ip_to_v6(src_ip), ip_to_v6(dst_ip)) {
                        addr_src.extend_from_slice(&s6.octets());
                        addr_dst.extend_from_slice(&d6.octets());
                        has_addrs = true;
                    }
                }
            } else if self.transport_protocol.is_unix() {
                let hdr_len = add_tlv_len(LENGTH_UNIX, self.raw_tlvs.len())?;
                buf.extend_from_slice(&hdr_len.to_be_bytes());
                let Some((ProxyAddr::Unix { name: src_name, .. }, ProxyAddr::Unix { name: dst_name, .. })) =
                    self.unix_addrs()
                else {
                    return Err(ProxyError::InvalidAddress(None));
                };
                addr_src = format_unix_name(src_name);
                addr_dst = format_unix_name(dst_name);
                has_addrs = true;
            }

            if !has_addrs {
                return Err(ProxyError::InvalidAddress(None));
            }
            buf.extend_from_slice(&addr_src);
            buf.extend_from_slice(&addr_dst);

            if let Some((src_port, dst_port)) = self.ports() {
                if !(0..=i32::from(u16::MAX)).contains(&src_port) || !(0..=i32::from(u16::MAX)).contains(&dst_port) {
                    return Err(ProxyError::InvalidPortNumber(None));
                }
                buf.extend_from_slice(&(src_port as u16).to_be_bytes());
                buf.extend_from_slice(&(dst_port as u16).to_be_bytes());
            }
        }

        if !self.raw_tlvs.is_empty() {
            buf.extend_from_slice(&self.raw_tlvs);
        }
        Ok(buf)
    }
}

fn add_tlv_len(base: u16, tlv_len: usize) -> Result<u16, ProxyError> {
    if tlv_len == 0 {
        return Ok(base);
    }
    let sum = usize::from(base).saturating_add(tlv_len);
    u16::try_from(sum).map_err(|_| ProxyError::Uint16Overflow)
}

pub fn new_ip_addr(transport: AddressFamilyAndProtocol, ip: IpAddr, port: u16) -> Option<ProxyAddr> {
    if transport.is_stream() {
        Some(ProxyAddr::tcp(ip, i32::from(port)))
    } else if transport.is_datagram() {
        Some(ProxyAddr::udp(ip, i32::from(port)))
    } else {
        None
    }
}

pub fn parse_unix_name(b: &[u8]) -> String {
    let slice = match b.iter().position(|&c| c == 0) {
        Some(pos) => &b[..pos],
        None => b,
    };
    String::from_utf8_lossy(slice).into_owned()
}

pub fn format_unix_name(name: &str) -> Vec<u8> {
    let bytes = name.as_bytes();
    let mut out = vec![0u8; UNIX_NAME_LEN];
    let n = bytes.len().min(UNIX_NAME_LEN);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

#[derive(Debug)]
enum PeekErr {
    Eof,
    Io(io::Error),
}

#[derive(Debug)]
enum ByteErr {
    Eof,
    Io(io::Error),
}

/// Buffered reader matching Go `bufio.Reader` behavior required by v1/v2 parsing and `ProxyConn`.
pub struct ProxyBufReader<R> {
    inner: R,
    buf: Vec<u8>,
    r: usize,
    w: usize,
    eof: bool,
    pending_err: Option<io::Error>,
}

impl<R> ProxyBufReader<R> {
    pub fn new(inner: R) -> Self {
        Self::new_size(inner, DEFAULT_READ_BUFFER_SIZE)
    }

    pub fn new_size(inner: R, size: usize) -> Self {
        Self {
            inner,
            buf: vec![0u8; size.max(MIN_READ_BUFFER_SIZE)],
            r: 0,
            w: 0,
            eof: false,
            pending_err: None,
        }
    }

    pub fn size(&self) -> usize {
        self.buf.len()
    }

    pub fn buffered(&self) -> usize {
        self.w - self.r
    }

    pub fn buffered_bytes(&self) -> &[u8] {
        &self.buf[self.r..self.w]
    }

    pub fn inner(&self) -> &R {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner_and_buffered(self) -> (R, Vec<u8>) {
        let rest = self.buf[self.r..self.w].to_vec();
        (self.inner, rest)
    }

    pub fn reset(&mut self, inner: R) {
        self.inner = inner;
        self.r = 0;
        self.w = 0;
        self.eof = false;
        self.pending_err = None;
    }

    pub fn set_pending_err(&mut self, err: io::Error) {
        self.pending_err = Some(err);
    }

    fn take_err_for_byte(&mut self) -> ByteErr {
        if let Some(e) = self.pending_err.take() {
            ByteErr::Io(e)
        } else {
            ByteErr::Eof
        }
    }

    fn take_err_for_peek(&mut self) -> PeekErr {
        if let Some(e) = self.pending_err.take() {
            PeekErr::Io(e)
        } else {
            PeekErr::Eof
        }
    }
}

impl<R: Read> ProxyBufReader<R> {
    fn fill(&mut self) {
        if self.r > 0 {
            self.buf.copy_within(self.r..self.w, 0);
            self.w -= self.r;
            self.r = 0;
        }
        if self.w >= self.buf.len() || self.pending_err.is_some() || self.eof {
            return;
        }
        match self.inner.read(&mut self.buf[self.w..]) {
            Ok(0) => {
                self.eof = true;
            }
            Ok(n) => {
                self.w += n;
            }
            Err(e) => {
                self.pending_err = Some(e);
            }
        }
    }

    pub fn peek_bytes(&mut self, n: usize) -> io::Result<&[u8]> {
        self.peek(n).map_err(|e| match e {
            PeekErr::Eof => io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"),
            PeekErr::Io(io_e) => io_e,
        })
    }

    fn peek(&mut self, n: usize) -> Result<&[u8], PeekErr> {
        while self.w - self.r < n && self.w - self.r < self.buf.len() && self.pending_err.is_none() && !self.eof {
            self.fill();
        }
        if self.w - self.r >= n {
            Ok(&self.buf[self.r..self.r + n])
        } else {
            Err(self.take_err_for_peek())
        }
    }

    fn read_byte(&mut self) -> Result<u8, ByteErr> {
        if self.r == self.w {
            self.fill();
            if self.r == self.w {
                return Err(self.take_err_for_byte());
            }
        }
        let b = self.buf[self.r];
        self.r += 1;
        Ok(b)
    }

    fn read_exact_bytes(&mut self, out: &mut [u8]) -> Result<(), ByteErr> {
        let mut pos = 0;
        while pos < out.len() {
            let n = self.read(&mut out[pos..]).map_err(ByteErr::Io)?;
            if n == 0 {
                return Err(ByteErr::Eof);
            }
            pos += n;
        }
        Ok(())
    }

    fn discard(&mut self, mut n: usize) -> Result<(), ByteErr> {
        while n > 0 {
            if self.buffered() == 0 {
                self.fill();
                if self.buffered() == 0 {
                    return Err(self.take_err_for_byte());
                }
            }
            let take = n.min(self.buffered());
            self.r += take;
            n -= take;
        }
        Ok(())
    }

    fn read_tlv_exact(&mut self, out: &mut [u8]) -> Result<(), ProxyError> {
        let mut pos = 0;
        while pos < out.len() {
            match self.read(&mut out[pos..]) {
                Ok(0) => {
                    if pos == 0 {
                        return Err(ProxyError::InvalidLength(None));
                    }
                    return Err(ProxyError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected EOF",
                    )));
                }
                Ok(n) => {
                    pos += n;
                }
                Err(e) => return Err(ProxyError::Io(e)),
            }
        }
        Ok(())
    }
}

impl<R: Read> Read for ProxyBufReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            if self.buffered() > 0 {
                return Ok(0);
            }
            if let Some(e) = self.pending_err.take() {
                return Err(e);
            }
            return Ok(0);
        }
        if self.r == self.w {
            if let Some(e) = self.pending_err.take() {
                return Err(e);
            }
            if self.eof {
                return Ok(0);
            }
            if out.len() >= self.buf.len() {
                return self.inner.read(out);
            }
            self.fill();
            if self.r == self.w {
                if let Some(e) = self.pending_err.take() {
                    return Err(e);
                }
                return Ok(0);
            }
        }
        let n = out.len().min(self.w - self.r);
        out[..n].copy_from_slice(&self.buf[self.r..self.r + n]);
        self.r += n;
        Ok(n)
    }
}

/// Async buffered reader mirroring `ProxyBufReader` for `tokio::io::AsyncRead`.
pub struct ProxyAsyncBufReader<R> {
    inner: R,
    buf: Vec<u8>,
    r: usize,
    w: usize,
    consumed: usize,
    eof: bool,
    pending_err: Option<io::Error>,
}

impl<R> ProxyAsyncBufReader<R> {
    pub fn new(inner: R) -> Self {
        Self::new_size(inner, DEFAULT_READ_BUFFER_SIZE)
    }

    pub fn new_size(inner: R, size: usize) -> Self {
        Self {
            inner,
            buf: vec![0u8; size.max(MIN_READ_BUFFER_SIZE)],
            r: 0,
            w: 0,
            consumed: 0,
            eof: false,
            pending_err: None,
        }
    }

    pub fn buffered(&self) -> usize {
        self.w - self.r
    }

    pub fn consumed_bytes(&self) -> usize {
        self.consumed
    }

    pub fn into_inner_and_buffered(self) -> (R, Vec<u8>) {
        let rest = self.buf[self.r..self.w].to_vec();
        (self.inner, rest)
    }

    fn take_err_for_byte(&mut self) -> ByteErr {
        if let Some(e) = self.pending_err.take() {
            ByteErr::Io(e)
        } else {
            ByteErr::Eof
        }
    }

    fn take_err_for_peek(&mut self) -> PeekErr {
        if let Some(e) = self.pending_err.take() {
            PeekErr::Io(e)
        } else {
            PeekErr::Eof
        }
    }
}

impl<R: AsyncRead + Unpin> ProxyAsyncBufReader<R> {
    async fn fill(&mut self) {
        if self.r > 0 {
            self.buf.copy_within(self.r..self.w, 0);
            self.w -= self.r;
            self.r = 0;
        }
        if self.w >= self.buf.len() || self.pending_err.is_some() || self.eof {
            return;
        }
        match self.inner.read(&mut self.buf[self.w..]).await {
            Ok(0) => {
                self.eof = true;
            }
            Ok(n) => {
                self.w += n;
            }
            Err(e) => {
                self.pending_err = Some(e);
            }
        }
    }

    async fn peek(&mut self, n: usize) -> Result<&[u8], PeekErr> {
        while self.w - self.r < n && self.w - self.r < self.buf.len() && self.pending_err.is_none() && !self.eof {
            self.fill().await;
        }
        if self.w - self.r >= n {
            Ok(&self.buf[self.r..self.r + n])
        } else {
            Err(self.take_err_for_peek())
        }
    }

    async fn read_byte(&mut self) -> Result<u8, ByteErr> {
        if self.r == self.w {
            self.fill().await;
            if self.r == self.w {
                return Err(self.take_err_for_byte());
            }
        }
        let b = self.buf[self.r];
        self.r += 1;
        self.consumed = self.consumed.saturating_add(1);
        Ok(b)
    }

    async fn read_chunk(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.r == self.w {
            if let Some(e) = self.pending_err.take() {
                return Err(e);
            }
            if self.eof {
                return Ok(0);
            }
            self.fill().await;
            if self.r == self.w {
                if let Some(e) = self.pending_err.take() {
                    return Err(e);
                }
                return Ok(0);
            }
        }
        let n = out.len().min(self.w - self.r);
        out[..n].copy_from_slice(&self.buf[self.r..self.r + n]);
        self.r += n;
        self.consumed = self.consumed.saturating_add(n);
        Ok(n)
    }

    async fn read_exact_bytes(&mut self, out: &mut [u8]) -> Result<(), ByteErr> {
        let mut pos = 0;
        while pos < out.len() {
            let n = self.read_chunk(&mut out[pos..]).await.map_err(ByteErr::Io)?;
            if n == 0 {
                return Err(ByteErr::Eof);
            }
            pos += n;
        }
        Ok(())
    }

    async fn discard(&mut self, mut n: usize) -> Result<(), ByteErr> {
        while n > 0 {
            if self.buffered() == 0 {
                self.fill().await;
                if self.buffered() == 0 {
                    return Err(self.take_err_for_byte());
                }
            }
            let take = n.min(self.buffered());
            self.r += take;
            self.consumed = self.consumed.saturating_add(take);
            n -= take;
        }
        Ok(())
    }

    async fn read_tlv_exact(&mut self, out: &mut [u8]) -> Result<(), ProxyError> {
        let mut pos = 0;
        while pos < out.len() {
            match self.read_chunk(&mut out[pos..]).await {
                Ok(0) => {
                    if pos == 0 {
                        return Err(ProxyError::InvalidLength(None));
                    }
                    return Err(ProxyError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected EOF",
                    )));
                }
                Ok(n) => pos += n,
                Err(e) => return Err(ProxyError::Io(e)),
            }
        }
        Ok(())
    }
}

fn parse_v1_port_number(port_str: &str) -> Result<i32, ProxyError> {
    if port_str.len() > 1 && port_str.starts_with('0') {
        return Err(ProxyError::InvalidPortNumber(None));
    }
    if !port_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ProxyError::InvalidPortNumber(Some("invalid syntax".to_string())));
    }
    let port: u16 = port_str
        .parse()
        .map_err(|e: std::num::ParseIntError| ProxyError::InvalidPortNumber(Some(e.to_string())))?;
    Ok(i32::from(port))
}

fn parse_v1_ip_address(
    protocol: AddressFamilyAndProtocol,
    addr_str: &str,
    accept_ipv4_in_tcp6: bool,
) -> Result<IpAddr, ProxyError> {
    if addr_str.contains('%') {
        return Err(ProxyError::InvalidAddress(None));
    }
    match protocol {
        AddressFamilyAndProtocol::TCPV4 => {
            let v4 = Ipv4Addr::from_str(addr_str)
                .map_err(|e| ProxyError::InvalidAddress(Some(e.to_string())))?;
            Ok(IpAddr::V4(v4))
        }
        AddressFamilyAndProtocol::TCPV6 => {
            if let Ok(v6) = Ipv6Addr::from_str(addr_str) {
                return Ok(IpAddr::V6(v6));
            }
            if accept_ipv4_in_tcp6 {
                if let Ok(v4) = Ipv4Addr::from_str(addr_str) {
                    return Ok(IpAddr::V6(v4.to_ipv6_mapped()));
                }
            }
            Err(ProxyError::InvalidAddress(None))
        }
        _ => Err(ProxyError::InvalidAddress(None)),
    }
}

fn parse_v1_line(buf: &[u8], accept_ipv4_in_tcp6: bool) -> Result<Header, ProxyError> {
    if buf.len() < 2 || buf[buf.len() - 2] != b'\r' {
        return Err(ProxyError::LineMustEndWithCrlf);
    }
    let line = std::str::from_utf8(&buf[..buf.len() - 2])
        .map_err(|_| ProxyError::CantReadVersion1Header(None))?;
    let tokens: Vec<&str> = line.split(' ').collect();
    if tokens.first().copied() != Some("PROXY") {
        return Err(ProxyError::CantReadVersion1Header(None));
    }
    let Some(&proto_tok) = tokens.get(1) else {
        return Err(ProxyError::CantReadAddressFamilyAndProtocol);
    };
    let transport_protocol = match proto_tok {
        "TCP4" => AddressFamilyAndProtocol::TCPV4,
        "TCP6" => AddressFamilyAndProtocol::TCPV6,
        "UNKNOWN" => AddressFamilyAndProtocol::UNSPEC,
        _ => return Err(ProxyError::CantReadAddressFamilyAndProtocol),
    };
    if transport_protocol != AddressFamilyAndProtocol::UNSPEC && tokens.len() != 6 {
        return Err(ProxyError::CantReadAddressFamilyAndProtocol);
    }
    if transport_protocol == AddressFamilyAndProtocol::UNSPEC {
        return Ok(Header {
            version: 1,
            command: ProtocolVersionAndCommand::LOCAL,
            transport_protocol,
            source_addr: None,
            destination_addr: None,
            raw_tlvs: Vec::new(),
        });
    }

    let source_ip = parse_v1_ip_address(transport_protocol, tokens[2], accept_ipv4_in_tcp6)?;
    let dest_ip = parse_v1_ip_address(transport_protocol, tokens[3], accept_ipv4_in_tcp6)?;
    let source_port = parse_v1_port_number(tokens[4])?;
    let dest_port = parse_v1_port_number(tokens[5])?;

    Ok(Header {
        version: 1,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol,
        source_addr: Some(ProxyAddr::tcp(source_ip, source_port)),
        destination_addr: Some(ProxyAddr::tcp(dest_ip, dest_port)),
        raw_tlvs: Vec::new(),
    })
}

fn byte_err_msg(err: ByteErr) -> String {
    match err {
        ByteErr::Eof => "EOF".to_string(),
        ByteErr::Io(e) => e.to_string(),
    }
}

/// Sync header reader (`Read`).
pub fn read<R: Read>(reader: &mut ProxyBufReader<R>) -> Result<Header, ProxyError> {
    read_with_options(reader, ReadOptions::default())
}

/// Sync header reader with explicit `ReadOptions`.
pub fn read_with_options<R: Read>(
    reader: &mut ProxyBufReader<R>,
    opts: ReadOptions,
) -> Result<Header, ProxyError> {
    let b1 = match reader.peek(1) {
        Ok(b) => b[0],
        Err(PeekErr::Eof) => return Err(ProxyError::NoProxyProtocol),
        Err(PeekErr::Io(e)) => return Err(ProxyError::Io(e)),
    };

    if b1 == SIGV1[0] || b1 == SIGV2[0] {
        let sig5 = match reader.peek(5) {
            Ok(b) => b,
            Err(PeekErr::Eof) => return Err(ProxyError::NoProxyProtocol),
            Err(PeekErr::Io(e)) => return Err(ProxyError::Io(e)),
        };
        if sig5 == SIGV1 {
            return parse_version1(reader, opts.v1_accept_ipv4_in_tcp6);
        }

        let sig12 = match reader.peek(12) {
            Ok(b) => b,
            Err(PeekErr::Eof) => return Err(ProxyError::NoProxyProtocol),
            Err(PeekErr::Io(e)) => return Err(ProxyError::Io(e)),
        };
        if sig12 == SIGV2 {
            return parse_version2(reader, opts.max_v2_header_size);
        }
    }

    Err(ProxyError::NoProxyProtocol)
}

/// Sync read with timeout (`ReadTimeout`).
pub fn read_timeout<R: Read + Send + 'static>(
    mut reader: ProxyBufReader<R>,
    timeout: Duration,
) -> (Result<Header, ProxyError>, Option<ProxyBufReader<R>>) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let res = read(&mut reader);
        let _ = tx.send((res, reader));
    });
    match rx.recv_timeout(timeout) {
        Ok((res, r)) => (res, Some(r)),
        Err(_) => (Err(ProxyError::NoProxyProtocol), None),
    }
}

/// Sync cancellable read with deadline (`ReadHeaderTimeout`).
pub fn read_header_timeout<C: StreamConn>(
    reader: &mut ProxyBufReader<C>,
    timeout: Duration,
) -> Result<Header, ProxyError> {
    if !timeout.is_zero() {
        let deadline = SystemTime::now() + timeout;
        reader.inner_mut().set_read_deadline(Some(deadline))?;
        let res = read(reader);
        let _ = reader.inner_mut().set_read_deadline(None);
        return match res {
            Err(e) if e.is_timeout() => Err(ProxyError::NoProxyProtocol),
            other => other,
        };
    }
    read(reader)
}

pub fn parse_version1<R: Read>(
    reader: &mut ProxyBufReader<R>,
    accept_ipv4_in_tcp6: bool,
) -> Result<Header, ProxyError> {
    let mut buf = Vec::with_capacity(MAX_V1_HEADER_LEN);
    loop {
        let b = reader
            .read_byte()
            .map_err(|e| ProxyError::CantReadVersion1Header(Some(byte_err_msg(e))))?;
        buf.push(b);
        if b == b'\n' {
            break;
        }
        if buf.len() == MAX_V1_HEADER_LEN {
            return Err(ProxyError::Version1HeaderTooLong);
        }
        if reader.buffered() == 0 {
            return Err(ProxyError::CantReadVersion1Header(None));
        }
    }
    parse_v1_line(&buf, accept_ipv4_in_tcp6)
}

fn decode_v2_addrs(
    transport: AddressFamilyAndProtocol,
    payload: &[u8],
) -> (Option<ProxyAddr>, Option<ProxyAddr>) {
    if transport.is_ipv4() {
        let src_ip = IpAddr::V4(Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]));
        let dst_ip = IpAddr::V4(Ipv4Addr::new(payload[4], payload[5], payload[6], payload[7]));
        let src_port = u16::from_be_bytes([payload[8], payload[9]]);
        let dst_port = u16::from_be_bytes([payload[10], payload[11]]);
        (
            new_ip_addr(transport, src_ip, src_port),
            new_ip_addr(transport, dst_ip, dst_port),
        )
    } else if transport.is_ipv6() {
        let mut s = [0u8; 16];
        s.copy_from_slice(&payload[..16]);
        let mut d = [0u8; 16];
        d.copy_from_slice(&payload[16..32]);
        let src_port = u16::from_be_bytes([payload[32], payload[33]]);
        let dst_port = u16::from_be_bytes([payload[34], payload[35]]);
        (
            new_ip_addr(transport, IpAddr::V6(Ipv6Addr::from(s)), src_port),
            new_ip_addr(transport, IpAddr::V6(Ipv6Addr::from(d)), dst_port),
        )
    } else if transport.is_unix() {
        let net = if transport.is_datagram() {
            NETWORK_UNIXGRAM
        } else {
            NETWORK_UNIX
        };
        let src = ProxyAddr::Unix {
            net: net.to_string(),
            name: parse_unix_name(&payload[..UNIX_NAME_LEN]),
        };
        let dst = ProxyAddr::Unix {
            net: net.to_string(),
            name: parse_unix_name(&payload[UNIX_NAME_LEN..UNIX_NAME_LEN * 2]),
        };
        (Some(src), Some(dst))
    } else {
        (None, None)
    }
}

fn v2_addr_block_len(transport: AddressFamilyAndProtocol) -> usize {
    if transport == AddressFamilyAndProtocol::UNSPEC {
        0
    } else if transport.is_ipv4() {
        usize::from(LENGTH_V4)
    } else if transport.is_ipv6() {
        usize::from(LENGTH_V6)
    } else if transport.is_unix() {
        usize::from(LENGTH_UNIX)
    } else {
        0
    }
}

fn parse_version2<R: Read>(
    reader: &mut ProxyBufReader<R>,
    max_v2_size: u16,
) -> Result<Header, ProxyError> {
    let mut sig = [0u8; 12];
    for byte in &mut sig {
        *byte = reader
            .read_byte()
            .map_err(|e| ProxyError::CantReadProtocolVersionAndCommand(Some(byte_err_msg(e))))?;
    }

    let b13 = reader
        .read_byte()
        .map_err(|e| ProxyError::CantReadProtocolVersionAndCommand(Some(byte_err_msg(e))))?;
    let command = ProtocolVersionAndCommand(b13);
    if !command.is_supported() {
        return Err(ProxyError::UnsupportedProtocolVersionAndCommand);
    }

    let b14 = reader
        .read_byte()
        .map_err(|_| ProxyError::CantReadAddressFamilyAndProtocol)?;
    let transport_protocol = AddressFamilyAndProtocol(b14);
    if !transport_protocol.is_supported() {
        return Err(ProxyError::UnsupportedAddressFamilyAndProtocol);
    }
    if transport_protocol == AddressFamilyAndProtocol::UNSPEC
        && command != ProtocolVersionAndCommand::LOCAL
    {
        return Err(ProxyError::UnsupportedAddressFamilyAndProtocol);
    }

    let mut len_bytes = [0u8; 2];
    reader
        .read_exact_bytes(&mut len_bytes)
        .map_err(|e| ProxyError::CantReadLength(Some(byte_err_msg(e))))?;
    let length = u16::from_be_bytes(len_bytes);

    let mut header = Header {
        version: 2,
        command,
        transport_protocol,
        source_addr: None,
        destination_addr: None,
        raw_tlvs: Vec::new(),
    };

    if !header.validate_length(length) {
        if header.command == ProtocolVersionAndCommand::LOCAL {
            if length > max_v2_size {
                return Err(ProxyError::InvalidLength(None));
            }
            reader
                .discard(usize::from(length))
                .map_err(|e| ProxyError::InvalidLength(Some(byte_err_msg(e))))?;
            header.transport_protocol = AddressFamilyAndProtocol::UNSPEC;
            return Ok(header);
        }
        return Err(ProxyError::InvalidLength(None));
    }

    if length == 0 {
        return Ok(header);
    }
    if length > max_v2_size {
        return Err(ProxyError::InvalidLength(None));
    }

    let addr_len = v2_addr_block_len(header.transport_protocol);
    if addr_len > 0 {
        let mut addr_buf = vec![0u8; addr_len];
        reader
            .read_exact_bytes(&mut addr_buf)
            .map_err(|e| ProxyError::InvalidAddress(Some(byte_err_msg(e))))?;
        let (src, dst) = decode_v2_addrs(header.transport_protocol, &addr_buf);
        header.source_addr = src;
        header.destination_addr = dst;
    }

    let tlv_len = usize::from(length) - addr_len;
    if tlv_len > 0 {
        let mut tlv_buf = vec![0u8; tlv_len];
        reader.read_tlv_exact(&mut tlv_buf)?;
        header.raw_tlvs = tlv_buf;
    }

    Ok(header)
}

/// Async header reader (for `tokio::io::AsyncRead`).
pub async fn read_async<R: AsyncRead + Unpin>(
    reader: &mut ProxyAsyncBufReader<R>,
) -> Result<Header, ProxyError> {
    read_async_with_options(reader, ReadOptions::default()).await
}

pub async fn read_async_with_options<R: AsyncRead + Unpin>(
    reader: &mut ProxyAsyncBufReader<R>,
    opts: ReadOptions,
) -> Result<Header, ProxyError> {
    let b1 = match reader.peek(1).await {
        Ok(b) => b[0],
        Err(PeekErr::Eof) => return Err(ProxyError::NoProxyProtocol),
        Err(PeekErr::Io(e)) => return Err(ProxyError::Io(e)),
    };

    if b1 == SIGV1[0] || b1 == SIGV2[0] {
        let sig5 = match reader.peek(5).await {
            Ok(b) => b,
            Err(PeekErr::Eof) => return Err(ProxyError::NoProxyProtocol),
            Err(PeekErr::Io(e)) => return Err(ProxyError::Io(e)),
        };
        if sig5 == SIGV1 {
            return parse_version1_async(reader, opts.v1_accept_ipv4_in_tcp6).await;
        }

        let sig12 = match reader.peek(12).await {
            Ok(b) => b,
            Err(PeekErr::Eof) => return Err(ProxyError::NoProxyProtocol),
            Err(PeekErr::Io(e)) => return Err(ProxyError::Io(e)),
        };
        if sig12 == SIGV2 {
            return parse_version2_async(reader, opts.max_v2_header_size).await;
        }
    }

    Err(ProxyError::NoProxyProtocol)
}

async fn parse_version1_async<R: AsyncRead + Unpin>(
    reader: &mut ProxyAsyncBufReader<R>,
    accept_ipv4_in_tcp6: bool,
) -> Result<Header, ProxyError> {
    let mut buf = Vec::with_capacity(MAX_V1_HEADER_LEN);
    loop {
        let b = reader
            .read_byte()
            .await
            .map_err(|e| ProxyError::CantReadVersion1Header(Some(byte_err_msg(e))))?;
        buf.push(b);
        if b == b'\n' {
            break;
        }
        if buf.len() == MAX_V1_HEADER_LEN {
            return Err(ProxyError::Version1HeaderTooLong);
        }
        if reader.buffered() == 0 {
            return Err(ProxyError::CantReadVersion1Header(None));
        }
    }
    parse_v1_line(&buf, accept_ipv4_in_tcp6)
}

async fn parse_version2_async<R: AsyncRead + Unpin>(
    reader: &mut ProxyAsyncBufReader<R>,
    max_v2_size: u16,
) -> Result<Header, ProxyError> {
    for _ in 0..12 {
        let _ = reader
            .read_byte()
            .await
            .map_err(|e| ProxyError::CantReadProtocolVersionAndCommand(Some(byte_err_msg(e))))?;
    }

    let b13 = reader
        .read_byte()
        .await
        .map_err(|e| ProxyError::CantReadProtocolVersionAndCommand(Some(byte_err_msg(e))))?;
    let command = ProtocolVersionAndCommand(b13);
    if !command.is_supported() {
        return Err(ProxyError::UnsupportedProtocolVersionAndCommand);
    }

    let b14 = reader
        .read_byte()
        .await
        .map_err(|_| ProxyError::CantReadAddressFamilyAndProtocol)?;
    let transport_protocol = AddressFamilyAndProtocol(b14);
    if !transport_protocol.is_supported() {
        return Err(ProxyError::UnsupportedAddressFamilyAndProtocol);
    }
    if transport_protocol == AddressFamilyAndProtocol::UNSPEC
        && command != ProtocolVersionAndCommand::LOCAL
    {
        return Err(ProxyError::UnsupportedAddressFamilyAndProtocol);
    }

    let mut len_bytes = [0u8; 2];
    reader
        .read_exact_bytes(&mut len_bytes)
        .await
        .map_err(|e| ProxyError::CantReadLength(Some(byte_err_msg(e))))?;
    let length = u16::from_be_bytes(len_bytes);

    let mut header = Header {
        version: 2,
        command,
        transport_protocol,
        source_addr: None,
        destination_addr: None,
        raw_tlvs: Vec::new(),
    };

    if !header.validate_length(length) {
        if header.command == ProtocolVersionAndCommand::LOCAL {
            if length > max_v2_size {
                return Err(ProxyError::InvalidLength(None));
            }
            reader
                .discard(usize::from(length))
                .await
                .map_err(|e| ProxyError::InvalidLength(Some(byte_err_msg(e))))?;
            header.transport_protocol = AddressFamilyAndProtocol::UNSPEC;
            return Ok(header);
        }
        return Err(ProxyError::InvalidLength(None));
    }

    if length == 0 {
        return Ok(header);
    }
    if length > max_v2_size {
        return Err(ProxyError::InvalidLength(None));
    }

    let addr_len = v2_addr_block_len(header.transport_protocol);
    if addr_len > 0 {
        let mut addr_buf = vec![0u8; addr_len];
        reader
            .read_exact_bytes(&mut addr_buf)
            .await
            .map_err(|e| ProxyError::InvalidAddress(Some(byte_err_msg(e))))?;
        let (src, dst) = decode_v2_addrs(header.transport_protocol, &addr_buf);
        header.source_addr = src;
        header.destination_addr = dst;
    }

    let tlv_len = usize::from(length) - addr_len;
    if tlv_len > 0 {
        let mut tlv_buf = vec![0u8; tlv_len];
        reader.read_tlv_exact(&mut tlv_buf).await?;
        header.raw_tlvs = tlv_buf;
    }

    Ok(header)
}

/// Async read with timeout.
///
/// Mirrors Go `go-proxyproto` `ReadHeaderTimeout`: a timeout while peeking before a PROXY
/// signature is matched maps to `ProxyError::NoProxyProtocol` (leaving any buffered raw bytes
/// intact), whereas a timeout after signature bytes have already been consumed fails the
/// connection with `TimedOut` so partial PROXY headers cannot leak stripped bytes into upper
/// protocols.
pub async fn read_header_timeout_async<R: AsyncRead + Unpin>(
    reader: &mut ProxyAsyncBufReader<R>,
    timeout: Duration,
) -> Result<Header, ProxyError> {
    if timeout.is_zero() {
        return read_async(reader).await;
    }
    let before = reader.consumed_bytes();
    match tokio::time::timeout(timeout, read_async(reader)).await {
        Ok(res) => res,
        Err(_) => {
            if reader.consumed_bytes() > before {
                Err(ProxyError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "proxyproto: timeout reading PROXY header",
                )))
            } else {
                Err(ProxyError::NoProxyProtocol)
            }
        }
    }
}

/// UDP datagram parser (`ParseUDPDatagram` in `udp.go`).
pub fn parse_udp_datagram(datagram: &[u8]) -> Result<(Header, &[u8]), ProxyError> {
    let cursor = io::Cursor::new(datagram);
    let mut reader = ProxyBufReader::new_size(cursor, datagram.len());
    let header = read(&mut reader)?;
    let buffered = reader.buffered();
    let pos = usize::try_from(reader.inner().position()).unwrap_or(datagram.len());
    let consumed = pos.saturating_sub(buffered);
    Ok((header, &datagram[consumed..]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Policy {
    Use = 0,
    Ignore = 1,
    Reject = 2,
    Require = 3,
    Skip = 4,
}

pub struct ConnPolicyOptions {
    pub upstream: ProxyAddr,
    pub downstream: ProxyAddr,
}

pub type PolicyFunc = Arc<dyn Fn(&ProxyAddr) -> Result<Policy, ProxyError> + Send + Sync>;
pub type ConnPolicyFunc = Arc<dyn Fn(&ConnPolicyOptions) -> Result<Policy, ProxyError> + Send + Sync>;
pub type Validator = Arc<dyn Fn(&Header) -> Result<(), ProxyError> + Send + Sync>;

/// IP CIDR matcher supporting both IPv4 and IPv6 (matching Go `net.IPNet.Contains`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpNet {
    V4 { network: u32, mask: u32 },
    V6 { network: u128, mask: u128 },
}

impl IpNet {
    pub fn parse(cidr: &str) -> Result<Self, ProxyError> {
        let Some((ip_part, prefix_part)) = cidr.split_once('/') else {
            return Err(ProxyError::InvalidIpRange(format!(
                "given string {cidr:?} is not a valid IP range: invalid CIDR address: {cidr}"
            )));
        };
        let ip = IpAddr::from_str(ip_part).map_err(|e| {
            ProxyError::InvalidIpRange(format!("given string {cidr:?} is not a valid IP range: {e}"))
        })?;
        let prefix: u32 = prefix_part.parse().map_err(|e| {
            ProxyError::InvalidIpRange(format!("given string {cidr:?} is not a valid IP range: {e}"))
        })?;
        match ip {
            IpAddr::V4(v4) => {
                if prefix > 32 {
                    return Err(ProxyError::InvalidIpRange(format!(
                        "given string {cidr:?} is not a valid IP range: invalid prefix {prefix}"
                    )));
                }
                let mask = if prefix == 0 { 0 } else { !0u32 << (32 - prefix) };
                Ok(Self::V4 {
                    network: u32::from(v4) & mask,
                    mask,
                })
            }
            IpAddr::V6(v6) => {
                if prefix > 128 {
                    return Err(ProxyError::InvalidIpRange(format!(
                        "given string {cidr:?} is not a valid IP range: invalid prefix {prefix}"
                    )));
                }
                let mask = if prefix == 0 { 0 } else { !0u128 << (128 - prefix) };
                Ok(Self::V6 {
                    network: u128::from(v6) & mask,
                    mask,
                })
            }
        }
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self, normalize_ip(ip)) {
            (Self::V4 { network, mask }, IpAddr::V4(v4)) => (u32::from(v4) & *mask) == *network,
            (Self::V6 { network, mask }, IpAddr::V6(v6)) => (u128::from(v6) & *mask) == *network,
            (Self::V6 { network, mask }, IpAddr::V4(v4)) => {
                (u128::from(v4.to_ipv6_mapped()) & *mask) == *network
            }
            _ => false,
        }
    }
}

#[derive(Clone)]
enum IpMatcher {
    Exact(IpAddr),
    Net(IpNet),
}

impl IpMatcher {
    fn matches(&self, ip: IpAddr) -> bool {
        match self {
            Self::Exact(target) => normalize_ip(*target) == normalize_ip(ip),
            Self::Net(net) => net.contains(ip),
        }
    }
}

fn parse_ranges(allowed: &[&str]) -> Result<Vec<IpMatcher>, ProxyError> {
    let mut matchers = Vec::with_capacity(allowed.len());
    for entry in allowed {
        if entry.rfind('/').is_some_and(|idx| idx > 0) {
            matchers.push(IpMatcher::Net(IpNet::parse(entry)?));
        } else {
            let ip = IpAddr::from_str(entry).map_err(|_| {
                ProxyError::InvalidIpAddress(format!("given string {entry:?} is not a valid IP address"))
            })?;
            matchers.push(IpMatcher::Exact(ip));
        }
    }
    Ok(matchers)
}

pub fn ip_from_addr(addr: &ProxyAddr) -> Result<IpAddr, ProxyError> {
    match addr {
        ProxyAddr::Tcp { ip: Some(ip), .. } | ProxyAddr::Udp { ip: Some(ip), .. } => Ok(*ip),
        ProxyAddr::Other { addr: s, .. } => {
            if let Ok(sock) = SocketAddr::from_str(s) {
                return Ok(sock.ip());
            }
            Err(ProxyError::InvalidIpAddress("invalid IP address".to_string()))
        }
        _ => Err(ProxyError::InvalidIpAddress(format!(
            "address {addr}: missing port in address"
        ))),
    }
}

pub fn conn_skip_proxy_header_for_cidr(
    skip_header_cidr: Option<IpNet>,
    def: Policy,
) -> ConnPolicyFunc {
    Arc::new(move |opts: &ConnPolicyOptions| {
        let ip = ip_from_addr(&opts.upstream).map_err(|e| ProxyError::InvalidUpstream(e.to_string()))?;
        if let Some(cidr) = skip_header_cidr {
            if cidr.contains(ip) {
                return Ok(Policy::Skip);
            }
        }
        Ok(def)
    })
}

pub fn skip_proxy_header_for_cidr(skip_header_cidr: Option<IpNet>, def: Policy) -> PolicyFunc {
    let conn_policy = conn_skip_proxy_header_for_cidr(skip_header_cidr, def);
    Arc::new(move |upstream: &ProxyAddr| {
        conn_policy(&ConnPolicyOptions {
            upstream: upstream.clone(),
            downstream: ProxyAddr::other("", ""),
        })
    })
}

pub fn policy_from_ranges(
    ranges: &[&str],
    matched: Policy,
    unmatched: Policy,
) -> Result<ConnPolicyFunc, ProxyError> {
    let allow_from = parse_ranges(ranges)?;
    Ok(Arc::new(move |opts: &ConnPolicyOptions| {
        let ip = ip_from_addr(&opts.upstream).map_err(|e| ProxyError::InvalidUpstream(e.to_string()))?;
        for m in &allow_from {
            if m.matches(ip) {
                return Ok(matched);
            }
        }
        Ok(unmatched)
    }))
}

pub fn conn_lax_whitelist_policy(allowed: &[&str]) -> Result<ConnPolicyFunc, ProxyError> {
    policy_from_ranges(allowed, Policy::Use, Policy::Ignore)
}

pub fn lax_whitelist_policy(allowed: &[&str]) -> Result<PolicyFunc, ProxyError> {
    let conn_policy = conn_lax_whitelist_policy(allowed)?;
    Ok(Arc::new(move |upstream: &ProxyAddr| {
        conn_policy(&ConnPolicyOptions {
            upstream: upstream.clone(),
            downstream: ProxyAddr::other("", ""),
        })
    }))
}

pub fn conn_strict_whitelist_policy(allowed: &[&str]) -> Result<ConnPolicyFunc, ProxyError> {
    policy_from_ranges(allowed, Policy::Use, Policy::Reject)
}

pub fn strict_whitelist_policy(allowed: &[&str]) -> Result<PolicyFunc, ProxyError> {
    let conn_policy = conn_strict_whitelist_policy(allowed)?;
    Ok(Arc::new(move |upstream: &ProxyAddr| {
        conn_policy(&ConnPolicyOptions {
            upstream: upstream.clone(),
            downstream: ProxyAddr::other("", ""),
        })
    }))
}

pub fn trust_proxy_header_from(trusted_ips: Vec<IpAddr>) -> ConnPolicyFunc {
    Arc::new(move |opts: &ConnPolicyOptions| {
        let ip = ip_from_addr(&opts.upstream).map_err(|e| ProxyError::InvalidUpstream(e.to_string()))?;
        for trusted in &trusted_ips {
            if normalize_ip(*trusted) == normalize_ip(ip) {
                return Ok(Policy::Require);
            }
        }
        Err(ProxyError::InvalidUpstream(format!(
            "{} is not a trusted PROXY sender",
            format_go_ip(ip)
        )))
    })
}

pub fn trust_proxy_header_from_ranges(trusted: &[&str]) -> Result<ConnPolicyFunc, ProxyError> {
    let allow_from = parse_ranges(trusted)?;
    Ok(Arc::new(move |opts: &ConnPolicyOptions| {
        let ip = ip_from_addr(&opts.upstream).map_err(|e| ProxyError::InvalidUpstream(e.to_string()))?;
        for m in &allow_from {
            if m.matches(ip) {
                return Ok(Policy::Require);
            }
        }
        Err(ProxyError::InvalidUpstream(format!(
            "{} is not a trusted PROXY sender",
            format_go_ip(ip)
        )))
    }))
}

pub fn ignore_proxy_header_not_on_interface(allowed_ip: IpAddr) -> ConnPolicyFunc {
    Arc::new(move |opts: &ConnPolicyOptions| {
        let ip = ip_from_addr(&opts.downstream).map_err(|e| ProxyError::InvalidUpstream(e.to_string()))?;
        if normalize_ip(allowed_ip) == normalize_ip(ip) {
            return Ok(Policy::Use);
        }
        Ok(Policy::Ignore)
    })
}

/// Stream connection trait mirroring Go `net.Conn`.
pub trait StreamConn: Read + Write + Send {
    fn local_addr(&self) -> io::Result<ProxyAddr>;
    fn remote_addr(&self) -> io::Result<ProxyAddr>;
    fn close(&mut self) -> io::Result<()>;
    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()>;
    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()>;
    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()>;
    fn read_from_reader(&mut self, _r: &mut dyn Read) -> Option<io::Result<u64>> {
        None
    }
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

fn deadline_to_timeout(t: Option<SystemTime>) -> Option<Duration> {
    t.map(|deadline| {
        deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::from_nanos(1))
    })
}

impl StreamConn for TcpStream {
    fn local_addr(&self) -> io::Result<ProxyAddr> {
        TcpStream::local_addr(self).map(ProxyAddr::from)
    }

    fn remote_addr(&self) -> io::Result<ProxyAddr> {
        TcpStream::peer_addr(self).map(ProxyAddr::from)
    }

    fn close(&mut self) -> io::Result<()> {
        let _ = self.shutdown(Shutdown::Both);
        Ok(())
    }

    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        let d = deadline_to_timeout(t);
        self.set_read_timeout(d)?;
        self.set_write_timeout(d)
    }

    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        self.set_read_timeout(deadline_to_timeout(t))
    }

    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        self.set_write_timeout(deadline_to_timeout(t))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(unix)]
impl StreamConn for UnixStream {
    fn local_addr(&self) -> io::Result<ProxyAddr> {
        let addr = UnixStream::local_addr(self)?;
        let name = addr
            .as_pathname()
            .and_then(|p| p.to_str())
            .unwrap_or("")
            .to_string();
        Ok(ProxyAddr::unix(name))
    }

    fn remote_addr(&self) -> io::Result<ProxyAddr> {
        let addr = UnixStream::peer_addr(self)?;
        let name = addr
            .as_pathname()
            .and_then(|p| p.to_str())
            .unwrap_or("")
            .to_string();
        Ok(ProxyAddr::unix(name))
    }

    fn close(&mut self) -> io::Result<()> {
        let _ = self.shutdown(Shutdown::Both);
        Ok(())
    }

    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        let d = deadline_to_timeout(t);
        self.set_read_timeout(d)?;
        self.set_write_timeout(d)
    }

    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        self.set_read_timeout(deadline_to_timeout(t))
    }

    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        self.set_write_timeout(deadline_to_timeout(t))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl StreamConn for Box<dyn StreamConn> {
    fn local_addr(&self) -> io::Result<ProxyAddr> {
        (**self).local_addr()
    }

    fn remote_addr(&self) -> io::Result<ProxyAddr> {
        (**self).remote_addr()
    }

    fn close(&mut self) -> io::Result<()> {
        (**self).close()
    }

    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        (**self).set_deadline(t)
    }

    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        (**self).set_read_deadline(t)
    }

    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        (**self).set_write_deadline(t)
    }

    fn read_from_reader(&mut self, r: &mut dyn Read) -> Option<io::Result<u64>> {
        (**self).read_from_reader(r)
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        (**self).as_any_mut()
    }
}

/// Listener trait mirroring Go `net.Listener`.
pub trait StreamListener: Send + Sync {
    type Conn: StreamConn + 'static;
    fn accept_stream(&self) -> io::Result<Self::Conn>;
    fn close_listener(&self) -> io::Result<()>;
    fn addr(&self) -> io::Result<ProxyAddr>;
}

impl StreamListener for TcpListener {
    type Conn = TcpStream;

    fn accept_stream(&self) -> io::Result<Self::Conn> {
        let (stream, _) = self.accept()?;
        Ok(stream)
    }

    fn close_listener(&self) -> io::Result<()> {
        Ok(())
    }

    fn addr(&self) -> io::Result<ProxyAddr> {
        self.local_addr().map(ProxyAddr::from)
    }
}

#[cfg(unix)]
impl StreamListener for UnixListener {
    type Conn = UnixStream;

    fn accept_stream(&self) -> io::Result<Self::Conn> {
        let (stream, _) = self.accept()?;
        Ok(stream)
    }

    fn close_listener(&self) -> io::Result<()> {
        Ok(())
    }

    fn addr(&self) -> io::Result<ProxyAddr> {
        let addr = self.local_addr()?;
        let name = addr
            .as_pathname()
            .and_then(|p| p.to_str())
            .unwrap_or("")
            .to_string();
        Ok(ProxyAddr::unix(name))
    }
}

pub type ConnOption<C> = Box<dyn FnOnce(&mut ProxyConn<C>)>;

pub fn with_policy<C: StreamConn>(policy: Policy) -> ConnOption<C> {
    Box::new(move |c| {
        c.proxy_header_policy = policy;
    })
}

pub fn validate_header<C: StreamConn>(validator: Option<Validator>) -> ConnOption<C> {
    Box::new(move |c| {
        if let Some(v) = validator {
            c.validate = Some(v);
        }
    })
}

pub fn set_read_header_timeout_nanos<C: StreamConn>(nanos: i64) -> ConnOption<C> {
    Box::new(move |c| {
        if nanos >= 0 {
            c.read_header_timeout = Duration::from_nanos(nanos as u64);
        }
    })
}

pub fn with_buffer_size<C: StreamConn>(length: isize) -> ConnOption<C> {
    Box::new(move |c| {
        if length <= 0 {
            return;
        }
        let sz = length as usize;
        c.buffer_size = Some(sz);
        if let Some(conn) = c.conn.take() {
            c.buf_reader = Some(ProxyBufReader::new_size(conn, sz));
        } else if let Some(br) = c.buf_reader.take() {
            let (conn, _) = br.into_inner_and_buffered();
            c.buf_reader = Some(ProxyBufReader::new_size(conn, sz));
        }
    })
}

/// Connection wrapper reading an optional/required PROXY protocol header on first I/O (`Conn`).
pub struct ProxyConn<C: StreamConn> {
    read_deadline: Mutex<Option<SystemTime>>,
    once: OnceLock<Result<(), ProxyError>>,
    buf_reader: Option<ProxyBufReader<C>>,
    conn: Option<C>,
    buffer_size: Option<usize>,
    header: Option<Header>,
    pub proxy_header_policy: Policy,
    pub validate: Option<Validator>,
    pub read_header_timeout: Duration,
}

impl<C: StreamConn> ProxyConn<C> {
    pub fn new(conn: C, opts: Vec<ConnOption<C>>) -> Self {
        let mut pc = Self {
            read_deadline: Mutex::new(None),
            once: OnceLock::new(),
            buf_reader: Some(ProxyBufReader::new(conn)),
            conn: None,
            buffer_size: None,
            header: None,
            proxy_header_policy: default_policy(),
            validate: None,
            read_header_timeout: default_read_header_timeout(),
        };
        for opt in opts {
            opt(&mut pc);
        }
        pc
    }

    pub fn from_pre_parsed(conn: C, header: Option<Header>) -> Self {
        let once = OnceLock::new();
        let _ = once.set(Ok(()));
        Self {
            read_deadline: Mutex::new(None),
            once,
            buf_reader: None,
            conn: Some(conn),
            buffer_size: None,
            header,
            proxy_header_policy: default_policy(),
            validate: None,
            read_header_timeout: default_read_header_timeout(),
        }
    }

    pub fn buffer_size(&self) -> Option<usize> {
        self.buffer_size
    }

    pub fn read_header_timeout(&self) -> Duration {
        self.read_header_timeout
    }

    pub fn has_buf_reader(&self) -> bool {
        self.buf_reader.is_some()
    }

    pub fn buf_reader_mut(&mut self) -> Option<&mut ProxyBufReader<C>> {
        self.buf_reader.as_mut()
    }

    pub fn raw(&self) -> Option<&C> {
        self.buf_reader
            .as_ref()
            .map(ProxyBufReader::inner)
            .or(self.conn.as_ref())
    }

    pub fn raw_mut(&mut self) -> Option<&mut C> {
        if let Some(br) = self.buf_reader.as_mut() {
            Some(br.inner_mut())
        } else {
            self.conn.as_mut()
        }
    }

    pub fn tcp_conn(&self) -> Option<&TcpStream> {
        self.raw()?.as_any().downcast_ref::<TcpStream>()
    }

    #[cfg(unix)]
    pub fn unix_conn(&self) -> Option<&UnixStream> {
        self.raw()?.as_any().downcast_ref::<UnixStream>()
    }

    pub fn proxy_header(&mut self) -> Option<&Header> {
        let _ = self.ensure_header_processed();
        self.header.as_ref()
    }

    pub fn local_addr_or_proxy(&mut self) -> io::Result<ProxyAddr> {
        let err = self.ensure_header_processed().is_err();
        if !err {
            if let Some(h) = &self.header {
                if !h.command.is_local() {
                    if let Some(dst) = &h.destination_addr {
                        return Ok(dst.clone());
                    }
                }
            }
        }
        match self.raw() {
            Some(c) => c.local_addr(),
            None => Err(io::Error::other("proxyproto: connection missing")),
        }
    }

    pub fn remote_addr_or_proxy(&mut self) -> io::Result<ProxyAddr> {
        let err = self.ensure_header_processed().is_err();
        if !err {
            if let Some(h) = &self.header {
                if !h.command.is_local() {
                    if let Some(src) = &h.source_addr {
                        return Ok(src.clone());
                    }
                }
            }
        }
        match self.raw() {
            Some(c) => c.remote_addr(),
            None => Err(io::Error::other("proxyproto: connection missing")),
        }
    }

    pub fn ensure_header_processed(&mut self) -> Result<(), ProxyError> {
        if let Some(res) = self.once.get() {
            return res.clone();
        }
        let res = self.read_header();
        let _ = self.once.set(res.clone());
        res
    }

    fn read_header(&mut self) -> Result<(), ProxyError> {
        let Some(br) = self.buf_reader.as_mut() else {
            return Ok(());
        };

        if !self.read_header_timeout.is_zero() {
            let stored = *self.read_deadline.lock();
            let mut header_deadline = SystemTime::now() + self.read_header_timeout;
            if let Some(sd) = stored {
                if sd < header_deadline {
                    header_deadline = sd;
                }
            }
            br.inner_mut().set_read_deadline(Some(header_deadline))?;
        }

        let mut res = read(br);

        if !self.read_header_timeout.is_zero() {
            let stored = *self.read_deadline.lock();
            let _restore_res = br.inner_mut().set_read_deadline(stored);
            if let Err(e) = &res {
                if e.is_timeout() {
                    res = Err(ProxyError::NoProxyProtocol);
                }
            }
        }

        match res {
            Err(ProxyError::NoProxyProtocol) => {
                if self.proxy_header_policy == Policy::Require {
                    return Err(ProxyError::NoProxyProtocol);
                }
                Ok(())
            }
            Ok(header) => match self.proxy_header_policy {
                Policy::Reject => Err(ProxyError::SuperfluousProxyHeader),
                Policy::Use | Policy::Require => {
                    if let Some(v) = &self.validate {
                        v(&header)?;
                    }
                    self.header = Some(header);
                    Ok(())
                }
                Policy::Ignore | Policy::Skip => Ok(()),
            },
            Err(e) => Err(e),
        }
    }

    pub fn read_conn(&mut self, buf: &mut [u8]) -> Result<usize, ProxyError> {
        self.ensure_header_processed()?;
        if let Some(br) = self.buf_reader.as_mut() {
            if br.buffered() > 0 {
                let res = br.read(buf);
                if br.buffered() == 0 {
                    if let Some(old) = self.buf_reader.take() {
                        let (c, _) = old.into_inner_and_buffered();
                        self.conn = Some(c);
                    }
                }
                return res.map_err(ProxyError::Io);
            }
            let _ = br.read(&mut [])?;
            if let Some(old) = self.buf_reader.take() {
                let (c, _) = old.into_inner_and_buffered();
                self.conn = Some(c);
            }
        }
        match self.conn.as_mut() {
            Some(c) => c.read(buf).map_err(ProxyError::Io),
            None => Err(ProxyError::Io(io::Error::other("connection closed"))),
        }
    }

    pub fn write_conn(&mut self, buf: &[u8]) -> Result<usize, ProxyError> {
        self.ensure_header_processed()?;
        match self.raw_mut() {
            Some(c) => c.write(buf).map_err(ProxyError::Io),
            None => Err(ProxyError::Io(io::Error::other("connection closed"))),
        }
    }

    pub fn read_from(&mut self, r: &mut dyn Read) -> Result<u64, ProxyError> {
        self.ensure_header_processed()?;
        let Some(c) = self.raw_mut() else {
            return Err(ProxyError::Io(io::Error::other("connection closed")));
        };
        if let Some(res) = c.read_from_reader(r) {
            return res.map_err(ProxyError::Io);
        }
        io::copy(r, c).map_err(ProxyError::Io)
    }

    pub fn write_to(&mut self, w: &mut dyn Write) -> Result<u64, ProxyError> {
        self.ensure_header_processed()?;
        if self.buf_reader.is_none() {
            let Some(c) = self.conn.as_mut() else {
                return Err(ProxyError::Io(io::Error::other("connection closed")));
            };
            return io::copy(c, w).map_err(ProxyError::Io);
        }

        let buffered_len = self.buf_reader.as_ref().map_or(0, ProxyBufReader::buffered);
        if buffered_len == 0 {
            if let Some(br) = self.buf_reader.as_mut() {
                let _ = br.read(&mut [])?;
            }
            if let Some(br) = self.buf_reader.take() {
                let (c, _) = br.into_inner_and_buffered();
                self.conn = Some(c);
            }
            let Some(c) = self.conn.as_mut() else {
                return Err(ProxyError::Io(io::Error::other("connection closed")));
            };
            return io::copy(c, w).map_err(ProxyError::Io);
        }

        let Some(br) = self.buf_reader.take() else {
            return Ok(0);
        };
        let (mut c, buffered) = br.into_inner_and_buffered();
        let mut total: u64 = 0;
        match w.write_all(&buffered) {
            Ok(()) => {
                total += buffered.len() as u64;
            }
            Err(e) => {
                self.conn = Some(c);
                return Err(ProxyError::Io(e));
            }
        }
        let copy_res = io::copy(&mut c, w);
        self.conn = Some(c);
        match copy_res {
            Ok(n) => Ok(total + n),
            Err(e) => Err(ProxyError::Io(e)),
        }
    }

    /// Like `write_to`, delegating the unbuffered copy to `dst.read_from_reader` when available.
    pub fn write_to_stream<W: StreamConn>(&mut self, dst: &mut W) -> Result<u64, ProxyError> {
        self.ensure_header_processed()?;
        if self.buf_reader.is_none() {
            let Some(c) = self.conn.as_mut() else {
                return Err(ProxyError::Io(io::Error::other("connection closed")));
            };
            return match dst.read_from_reader(c) {
                Some(res) => res.map_err(ProxyError::Io),
                None => io::copy(c, dst).map_err(ProxyError::Io),
            };
        }

        let buffered_len = self.buf_reader.as_ref().map_or(0, ProxyBufReader::buffered);
        if buffered_len == 0 {
            if let Some(br) = self.buf_reader.as_mut() {
                let _ = br.read(&mut [])?;
            }
            if let Some(br) = self.buf_reader.take() {
                let (c, _) = br.into_inner_and_buffered();
                self.conn = Some(c);
            }
            let Some(c) = self.conn.as_mut() else {
                return Err(ProxyError::Io(io::Error::other("connection closed")));
            };
            return match dst.read_from_reader(c) {
                Some(res) => res.map_err(ProxyError::Io),
                None => io::copy(c, dst).map_err(ProxyError::Io),
            };
        }

        let Some(br) = self.buf_reader.take() else {
            return Ok(0);
        };
        let (mut c, buffered) = br.into_inner_and_buffered();
        let mut total: u64 = 0;
        if let Err(e) = dst.write_all(&buffered) {
            self.conn = Some(c);
            return Err(ProxyError::Io(e));
        }
        total += buffered.len() as u64;
        let copy_res = match dst.read_from_reader(&mut c) {
            Some(res) => res,
            None => io::copy(&mut c, dst),
        };
        self.conn = Some(c);
        match copy_res {
            Ok(n) => Ok(total + n),
            Err(e) => Err(ProxyError::Io(e)),
        }
    }
}

impl<C: StreamConn> Read for ProxyConn<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_conn(buf).map_err(io::Error::from)
    }
}

impl<C: StreamConn> Write for ProxyConn<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_conn(buf).map_err(io::Error::from)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.raw_mut() {
            Some(c) => c.flush(),
            None => Ok(()),
        }
    }
}

impl<C: StreamConn + 'static> StreamConn for ProxyConn<C> {
    fn local_addr(&self) -> io::Result<ProxyAddr> {
        match self.raw() {
            Some(c) => c.local_addr(),
            None => Err(io::Error::other("connection closed")),
        }
    }

    fn remote_addr(&self) -> io::Result<ProxyAddr> {
        match self.raw() {
            Some(c) => c.remote_addr(),
            None => Err(io::Error::other("connection closed")),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        match self.raw_mut() {
            Some(c) => c.close(),
            None => Ok(()),
        }
    }

    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        *self.read_deadline.lock() = t;
        match self.raw_mut() {
            Some(c) => c.set_deadline(t),
            None => Err(io::Error::other("connection closed")),
        }
    }

    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        *self.read_deadline.lock() = t;
        match self.raw_mut() {
            Some(c) => c.set_read_deadline(t),
            None => Err(io::Error::other("connection closed")),
        }
    }

    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        match self.raw_mut() {
            Some(c) => c.set_write_deadline(t),
            None => Err(io::Error::other("connection closed")),
        }
    }

    fn read_from_reader(&mut self, r: &mut dyn Read) -> Option<io::Result<u64>> {
        Some(self.read_from(r).map_err(io::Error::from))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Result of `ProxyListener::accept`: either a wrapped `ProxyConn` or a raw `C` when `Policy::Skip` matched.
#[allow(clippy::large_enum_variant)]
pub enum AcceptedConn<C: StreamConn> {
    Proxy(ProxyConn<C>),
    Raw(C),
}

impl<C: StreamConn> AcceptedConn<C> {
    pub fn into_proxy(self) -> Option<ProxyConn<C>> {
        match self {
            Self::Proxy(p) => Some(p),
            Self::Raw(_) => None,
        }
    }

    pub fn into_raw(self) -> Option<C> {
        match self {
            Self::Raw(c) => Some(c),
            Self::Proxy(_) => None,
        }
    }

    pub fn remote_addr(&mut self) -> io::Result<ProxyAddr> {
        match self {
            Self::Proxy(p) => p.remote_addr_or_proxy(),
            Self::Raw(c) => c.remote_addr(),
        }
    }

    pub fn local_addr(&mut self) -> io::Result<ProxyAddr> {
        match self {
            Self::Proxy(p) => p.local_addr_or_proxy(),
            Self::Raw(c) => c.local_addr(),
        }
    }
}

impl<C: StreamConn> Read for AcceptedConn<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Proxy(p) => p.read(buf),
            Self::Raw(c) => c.read(buf),
        }
    }
}

impl<C: StreamConn> Write for AcceptedConn<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Proxy(p) => p.write(buf),
            Self::Raw(c) => c.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Proxy(p) => p.flush(),
            Self::Raw(c) => c.flush(),
        }
    }
}

/// Timeout configuration on `ProxyListener`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ListenerHeaderTimeout {
    #[default]
    Default,
    Disabled,
    Duration(Duration),
}

/// Listener wrapper mirroring `proxyproto.Listener` (`protocol.go`).
pub struct ProxyListener<L> {
    pub listener: L,
    pub policy: Option<PolicyFunc>,
    pub conn_policy: Option<ConnPolicyFunc>,
    pub validate_header: Option<Validator>,
    pub read_header_timeout: ListenerHeaderTimeout,
    pub read_buffer_size: usize,
}

impl<L: StreamListener> ProxyListener<L> {
    pub fn new(listener: L) -> Self {
        Self {
            listener,
            policy: None,
            conn_policy: None,
            validate_header: None,
            read_header_timeout: ListenerHeaderTimeout::Default,
            read_buffer_size: 0,
        }
    }

    pub fn accept(&self) -> Result<AcceptedConn<L::Conn>, ProxyError> {
        loop {
            let mut conn = self.listener.accept_stream()?;
            let mut proxy_header_policy = default_policy();

            if self.policy.is_some() && self.conn_policy.is_some() {
                let _ = conn.close();
                return Err(ProxyError::BothPoliciesSet);
            }

            if self.policy.is_some() || self.conn_policy.is_some() {
                let res = if let Some(p) = &self.policy {
                    let remote = conn.remote_addr()?;
                    p(&remote)
                } else if let Some(cp) = &self.conn_policy {
                    let upstream = conn.remote_addr()?;
                    let downstream = conn.local_addr()?;
                    cp(&ConnPolicyOptions {
                        upstream,
                        downstream,
                    })
                } else {
                    Ok(proxy_header_policy)
                };

                match res {
                    Ok(p) => proxy_header_policy = p,
                    Err(e) => {
                        if let Err(close_err) = conn.close() {
                            return Err(ProxyError::Io(close_err));
                        }
                        if e.is_invalid_upstream() {
                            continue;
                        }
                        return Err(e);
                    }
                }

                if proxy_header_policy == Policy::Skip {
                    return Ok(AcceptedConn::Raw(conn));
                }
            }

            let mut opts: Vec<ConnOption<L::Conn>> = vec![
                with_policy(proxy_header_policy),
                validate_header(self.validate_header.clone()),
            ];
            if self.read_buffer_size > 0 {
                opts.push(with_buffer_size(self.read_buffer_size as isize));
            }
            let mut new_conn = ProxyConn::new(conn, opts);
            let timeout = match self.read_header_timeout {
                ListenerHeaderTimeout::Default => default_read_header_timeout(),
                ListenerHeaderTimeout::Disabled => Duration::ZERO,
                ListenerHeaderTimeout::Duration(d) => d,
            };
            new_conn.read_header_timeout = timeout;
            return Ok(AcceptedConn::Proxy(new_conn));
        }
    }

    pub fn close(&self) -> io::Result<()> {
        self.listener.close_listener()
    }

    pub fn addr(&self) -> io::Result<ProxyAddr> {
        self.listener.addr()
    }
}
