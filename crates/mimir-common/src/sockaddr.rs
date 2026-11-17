//! Private IP auto-detection used by memberlist when bound to `0.0.0.0`.
//!
//! Ported from github.com/hashicorp/go-sockaddr:
//! - ifaddr.go (`GetPrivateIP`)
//! - ifaddrs.go (`GetAllInterfaces`, `GetPrivateInterfaces`, `IfByFlag("forwardable"|"up")`,
//!   `IfByRFC`, `OrderedIfAddrBy`, `AscIfDefault`, `AscIfType`, `AscIfNetworkSize`,
//!   `parseDefaultIfNameFromIPCmd`, `parseIfNameFromIPCmd`)
//! - route_info_linux.go (`NewRouteInfo`, `GetDefaultInterfaceName`)
//! - sockaddrs.go (`AscType`, `AscNetworkSize`), ipv4addr.go / ipv6addr.go (`ContainsNetwork`)
//! - rfc.go (`KnownRFCs()[6890]`, `ForwardingBlacklist`)

use std::cmp::Ordering;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::gosort;
use crate::netutil::{self, flags, Interface, InterfaceAddr};

/// go-sockaddr `IfAddr`: one interface address together with its interface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IfAddr {
    pub addr: InterfaceAddr,
    pub iface: Interface,
}

/// An IP network (go-sockaddr `IPv4Addr` / `IPv6Addr` with a mask).
#[derive(Clone, Copy, Debug)]
struct Net {
    ip: IpAddr,
    prefix_len: u8,
}

const fn v4(a: u8, b: u8, c: u8, d: u8, prefix_len: u8) -> Net {
    Net { ip: IpAddr::V4(Ipv4Addr::new(a, b, c, d)), prefix_len }
}

#[allow(clippy::too_many_arguments)]
const fn v6(s: [u16; 8], prefix_len: u8) -> Net {
    Net {
        ip: IpAddr::V6(Ipv6Addr::new(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7])),
        prefix_len,
    }
}

/// `KnownRFCs()[6890]`.
static RFC_6890: &[Net] = &[
    v4(0, 0, 0, 0, 8),
    v4(10, 0, 0, 0, 8),
    v4(100, 64, 0, 0, 10),
    v4(127, 0, 0, 0, 8),
    v4(169, 254, 0, 0, 16),
    v4(172, 16, 0, 0, 12),
    v4(192, 0, 0, 0, 24),
    v4(192, 0, 0, 0, 29),
    v4(192, 0, 2, 0, 24),
    v4(192, 88, 99, 0, 24),
    v4(192, 168, 0, 0, 16),
    v4(198, 18, 0, 0, 15),
    v4(198, 51, 100, 0, 24),
    v4(203, 0, 113, 0, 24),
    v4(240, 0, 0, 0, 4),
    v4(255, 255, 255, 255, 32),
    v6([0, 0, 0, 0, 0, 0, 0, 1], 128),
    v6([0, 0, 0, 0, 0, 0, 0, 0], 128),
    v6([0x64, 0xff9b, 0, 0, 0, 0, 0, 0], 96),
    v6([0, 0, 0, 0, 0, 0xffff, 0, 0], 96),
    v6([0x100, 0, 0, 0, 0, 0, 0, 0], 64),
    v6([0x2001, 0, 0, 0, 0, 0, 0, 0], 16),
    v6([0x2001, 0, 0, 0, 0, 0, 0, 0], 16),
    v6([0x2001, 0x2, 0, 0, 0, 0, 0, 0], 48),
    v6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0], 32),
    v6([0x2001, 0x10, 0, 0, 0, 0, 0, 0], 28),
    v6([0x2002, 0, 0, 0, 0, 0, 0, 0], 16),
    v6([0xfc00, 0, 0, 0, 0, 0, 0, 0], 7),
    v6([0xfe80, 0, 0, 0, 0, 0, 0, 0], 10),
];

/// `KnownRFCs()[ForwardingBlacklist]`.
static FORWARDING_BLACKLIST: &[Net] = &[
    v4(0, 0, 0, 0, 8),
    v4(127, 0, 0, 0, 8),
    v4(169, 254, 0, 0, 16),
    v4(192, 0, 0, 0, 24),
    v4(192, 0, 2, 0, 24),
    v4(198, 51, 100, 0, 24),
    v4(203, 0, 113, 0, 24),
    v4(240, 0, 0, 0, 4),
    v4(255, 255, 255, 255, 32),
    v6([0, 0, 0, 0, 0, 0, 0, 1], 128),
    v6([0, 0, 0, 0, 0, 0, 0, 0], 128),
    v6([0, 0, 0, 0, 0, 0xffff, 0, 0], 96),
    v6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0], 32),
    v6([0x2001, 0x10, 0, 0, 0, 0, 0, 0], 28),
    v6([0xfe80, 0, 0, 0, 0, 0, 0, 0], 10),
];

fn ip_bits(ip: IpAddr) -> (u128, u8) {
    match ip {
        IpAddr::V4(a) => (u32::from(a) as u128, 32),
        IpAddr::V6(a) => (u128::from(a), 128),
    }
}

/// `IPv4Addr.Contains` / `IPv6Addr.Contains` (i.e. `ContainsNetwork`): the address family must
/// match and the whole network of `x` (not only its address) must lie within `net`.
fn net_contains(net: &Net, x: &InterfaceAddr) -> bool {
    if net.ip.is_ipv4() != x.ip.is_ipv4() {
        return false;
    }
    if x.prefix_len < net.prefix_len {
        return false;
    }
    let (n, width) = ip_bits(net.ip);
    let (a, _) = ip_bits(x.ip);
    let shift = u32::from(width - net.prefix_len);
    if shift >= 128 {
        return true;
    }
    (n >> shift) == (a >> shift)
}

fn is_rfc(nets: &[Net], x: &InterfaceAddr) -> bool {
    nets.iter().any(|n| net_contains(n, x))
}

/// `GetAllInterfaces`.
pub fn get_all_interfaces() -> std::io::Result<Vec<IfAddr>> {
    Ok(netutil::interfaces_with_addrs()?
        .into_iter()
        .flat_map(|i| {
            let iface = i.iface;
            i.addrs
                .into_iter()
                .map(move |addr| IfAddr { addr, iface: iface.clone() })
        })
        .collect())
}

/// Linux `NewRouteInfo().GetDefaultInterfaceName()`: runs `ip route` (looked up on `$PATH`,
/// falling back to `/sbin/ip`) and parses the default route's device.
pub fn get_default_interface_name() -> Result<String, String> {
    let path = which_ip().unwrap_or_else(|| "/sbin/ip".to_string());
    let out = std::process::Command::new(&path)
        .arg("route")
        .stderr(std::process::Stdio::null())
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!("exit status {}", out.status.code().unwrap_or(-1)));
    }
    parse_default_if_name_from_ip_cmd(&String::from_utf8_lossy(&out.stdout))
        .map_err(|_| "No default interface found".to_string())
}

/// `exec.LookPath("ip")`.
fn which_ip() -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let dir = if dir.as_os_str().is_empty() { std::path::PathBuf::from(".") } else { dir };
        let cand = dir.join("ip");
        if let Ok(meta) = std::fs::metadata(&cand) {
            if meta.is_file() && meta.permissions().mode() & 0o111 != 0 {
                return Some(cand.to_string_lossy().into_owned());
            }
        }
    }
    None
}

/// `parseIfNameFromIPCmd`: splits each line on `[\s]+`, keeping lines with >= 5 fields.
fn parse_if_name_from_ip_cmd(route_out: &str) -> Vec<Vec<&str>> {
    route_out
        .split('\n')
        .filter_map(|line| {
            // Go regexp.Split with `[\s]+`: leading whitespace yields an empty first field.
            let mut kvs: Vec<&str> = Vec::new();
            let mut rest = line;
            loop {
                match rest.find(|c: char| c.is_ascii_whitespace()) {
                    Some(start) => {
                        kvs.push(&rest[..start]);
                        let after = &rest[start..];
                        let end = after
                            .find(|c: char| !c.is_ascii_whitespace())
                            .unwrap_or(after.len());
                        rest = &after[end..];
                    }
                    None => {
                        kvs.push(rest);
                        break;
                    }
                }
            }
            (kvs.len() >= 5).then_some(kvs)
        })
        .collect()
}

/// `parseDefaultIfNameFromIPCmd`.
pub fn parse_default_if_name_from_ip_cmd(route_out: &str) -> Result<String, String> {
    for l in parse_if_name_from_ip_cmd(route_out) {
        if l[0] == "default" && l[1] == "via" && l[3] == "dev" {
            return Ok(l[4].trim().to_string());
        }
    }
    Err("No default interface found".to_string())
}

/// `sortReceiverBeforeArg` / `sortDeferDecision` / `sortArgBeforeReceiver`.
fn cmp_to_i(o: Ordering) -> i32 {
    match o {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// `AscIfDefault`, with the (identical, per-comparison) `ip route` lookup memoised.
fn asc_if_default(default_if: &Option<String>, p1: &IfAddr, p2: &IfAddr) -> i32 {
    let Some(d) = default_if else { return 0 };
    match (p1.iface.name == *d, p2.iface.name == *d) {
        (true, true) => 0,
        (true, false) => -1,
        (false, true) => 1,
        (false, false) => 0,
    }
}

/// `AscIfType` (`TypeIPv4` = 0x2 sorts before `TypeIPv6` = 0x4).
fn asc_if_type(p1: &IfAddr, p2: &IfAddr) -> i32 {
    let t = |a: &IfAddr| if a.addr.ip.is_ipv4() { 2 } else { 4 };
    cmp_to_i(t(p1).cmp(&t(p2)))
}

/// `AscIfNetworkSize`: `bytes.Compare` of the network masks.
fn asc_if_network_size(p1: &IfAddr, p2: &IfAddr) -> i32 {
    let mask = |a: &IfAddr| -> Vec<u8> {
        let (bits, width) = if a.addr.ip.is_ipv4() { (a.addr.prefix_len, 4) } else { (a.addr.prefix_len, 16) };
        let mut m = vec![0u8; width];
        for (i, b) in m.iter_mut().enumerate() {
            let remaining = (bits as i32 - (i as i32) * 8).clamp(0, 8);
            *b = if remaining == 0 { 0 } else { 0xffu8 << (8 - remaining) };
        }
        m
    };
    cmp_to_i(mask(p1).cmp(&mask(p2)))
}

/// `GetPrivateInterfaces` operating on an already-enumerated address list.
pub fn private_interfaces_from(all: Vec<IfAddr>, default_if: Option<String>) -> Vec<IfAddr> {
    // FilterIfByType(TypeIP): every enumerated address is IPv4 or IPv6.
    // IfByFlag("forwardable").
    let mut ifs: Vec<IfAddr> = all
        .into_iter()
        .filter(|a| !is_rfc(FORWARDING_BLACKLIST, &a.addr))
        .collect();
    // IfByFlag("up").
    ifs.retain(|a| a.iface.flags & flags::UP == flags::UP);
    if ifs.is_empty() {
        return ifs;
    }

    // OrderedIfAddrBy(AscIfDefault, AscIfType, AscIfNetworkSize).Sort(privateIfs)
    type CmpIfAddrFunc<'a> = &'a dyn Fn(&IfAddr, &IfAddr) -> i32;
    let cmps: [CmpIfAddrFunc; 3] = [
        &|a, b| asc_if_default(&default_if, a, b),
        &asc_if_type,
        &asc_if_network_size,
    ];
    gosort::sort_by(&mut ifs, |p, q| {
        for cmp in &cmps[..cmps.len() - 1] {
            match cmp(p, q) {
                -1 => return true,
                1 => return false,
                _ => {}
            }
        }
        cmps[cmps.len() - 1](p, q) == -1
    });

    // IfByRFC("6890").
    ifs.retain(|a| is_rfc(RFC_6890, &a.addr));
    ifs
}

/// `GetPrivateInterfaces`.
pub fn get_private_interfaces() -> std::io::Result<Vec<IfAddr>> {
    let all = get_all_interfaces()?;
    if all.is_empty() {
        return Ok(all);
    }
    // AscIfDefault calls NewRouteInfo().GetDefaultInterfaceName() and defers on error; it is
    // only invoked when there is something to sort.
    let needs_sort = all.len() > 1;
    let default_if = if needs_sort { get_default_interface_name().ok() } else { None };
    Ok(private_interfaces_from(all, default_if))
}

/// `GetPrivateIP` returns a string with a single IP address that is part of RFC 6890 and has a
/// default route. If the system can't determine its IP address or find an RFC 6890 IP address,
/// an empty string will be returned instead.
pub fn get_private_ip() -> std::io::Result<String> {
    let private_ifs = get_private_interfaces()?;
    Ok(private_ifs.first().map(|a| a.addr.ip.to_string()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ifa(name: &str, flags_: u32, cidr: &str) -> IfAddr {
        let (ip, len) = cidr.split_once('/').unwrap();
        IfAddr {
            addr: InterfaceAddr { ip: ip.parse().unwrap(), prefix_len: len.parse().unwrap() },
            iface: Interface { index: 0, name: name.into(), flags: flags_ },
        }
    }

    const UP: u32 = flags::UP;

    #[test]
    fn parse_default_route() {
        let out = "default via 192.168.178.1 dev enp5s0 proto dhcp src 192.168.178.22 metric 100 \n\
                   10.88.0.0/16 dev bruh-sb0 proto kernel scope link src 10.88.0.1 \n";
        assert_eq!(parse_default_if_name_from_ip_cmd(out), Ok("enp5s0".into()));
        assert!(parse_default_if_name_from_ip_cmd("10.0.0.0/8 dev eth0 proto kernel\n").is_err());
        // Leading whitespace produces an empty first field in Go's regexp.Split.
        assert!(parse_default_if_name_from_ip_cmd("  default via 1.2.3.4 dev eth0\n").is_err());
    }

    /// Typical Kubernetes pod: lo + eth0 (no `ip` binary in distroless => no default route info).
    #[test]
    fn k8s_pod_picks_eth0() {
        let all = vec![
            ifa("lo", UP | flags::LOOPBACK, "127.0.0.1/8"),
            ifa("lo", UP | flags::LOOPBACK, "::1/128"),
            ifa("eth0", UP, "10.42.3.17/24"),
            ifa("eth0", UP, "fe80::1/64"),
        ];
        let got = private_interfaces_from(all, None);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].addr.ip.to_string(), "10.42.3.17");
    }

    #[test]
    fn excludes_down_public_and_non_forwardable() {
        let all = vec![
            ifa("down0", 0, "10.0.0.5/24"),
            ifa("pub0", UP, "8.8.4.4/24"),
            ifa("ll0", UP, "169.254.3.3/16"),
            ifa("cgnat0", UP, "100.117.159.3/32"),
        ];
        let got = private_interfaces_from(all, None);
        assert_eq!(got.iter().map(|a| a.addr.ip.to_string()).collect::<Vec<_>>(), vec!["100.117.159.3"]);
    }

    /// The interface's whole network must lie inside the RFC 6890 block (`ContainsNetwork`).
    #[test]
    fn network_containment_not_address_containment() {
        let got = private_interfaces_from(vec![ifa("x", UP, "10.1.2.3/7")], None);
        assert!(got.is_empty());
    }

    #[test]
    fn ordering_default_then_type_then_network_size() {
        let all = vec![
            ifa("br0", UP, "172.17.0.1/16"),
            ifa("wg0", UP, "fd00::1/64"),
            ifa("tun0", UP, "10.8.0.1/24"),
            ifa("eth0", UP, "192.168.1.10/24"),
            ifa("eth0", UP, "fd12::10/64"),
        ];
        let got = private_interfaces_from(all.clone(), Some("eth0".into()));
        let ips: Vec<String> = got.iter().map(|a| a.addr.ip.to_string()).collect();
        assert_eq!(ips, vec!["192.168.1.10", "fd12::10", "172.17.0.1", "10.8.0.1", "fd00::1"]);
        let got = private_interfaces_from(all, None);
        let ips: Vec<String> = got.iter().map(|a| a.addr.ip.to_string()).collect();
        // Ties keep input order (Go's pdqsort uses insertion sort for n <= 12).
        assert_eq!(ips, vec!["172.17.0.1", "10.8.0.1", "192.168.1.10", "fd00::1", "fd12::10"]);
    }
}
