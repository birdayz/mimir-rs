//! Network interface discovery used to auto-detect the address an instance advertises in the
//! ring (and the default `-*.instance-interface-names`).
//!
//! Ported from github.com/grafana/dskit/netutil/netutil.go
//! (`PrivateNetworkInterfaces`, `PrivateNetworkInterfacesWithFallback`,
//! `privateNetworkInterfaces`, `GetFirstAddressOf`, `getFirstAddressOf`,
//! `getInterfaceAddresses`, `filterBestIP`), plus the subset of Go's `net` package
//! (`net.Interfaces`, `net.InterfaceByName`, `(*net.Interface).Addrs`, `net.IP.IsPrivate`,
//! `netip.Addr` predicates) those functions rely on.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use tracing::{debug, warn};

/// Go `net.Flags` bits (`net/interface.go`), as reported by the OS.
pub mod flags {
    pub const UP: u32 = 1 << 0;
    pub const BROADCAST: u32 = 1 << 1;
    pub const LOOPBACK: u32 = 1 << 2;
    pub const POINT_TO_POINT: u32 = 1 << 3;
    pub const MULTICAST: u32 = 1 << 4;
    pub const RUNNING: u32 = 1 << 5;
}

/// Go `net.Interface` (the fields used by dskit / go-sockaddr).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    pub index: u32,
    pub name: String,
    /// Go `net.Flags` bits (see [`flags`]).
    pub flags: u32,
}

/// Go `*net.IPNet` as returned by `(*net.Interface).Addrs()` on Linux/BSD.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InterfaceAddr {
    pub ip: IpAddr,
    pub prefix_len: u8,
}

impl fmt::Display for InterfaceAddr {
    /// `net.IPNet.String()`: `<ip>/<prefix length>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.ip, self.prefix_len)
    }
}

/// An interface together with its addresses, in OS (netlink dump) order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceWithAddrs {
    pub iface: Interface,
    pub addrs: Vec<InterfaceAddr>,
}

/// Error type mirroring the Go error strings of this package.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct NetutilError(pub String);

/// Formats a `[]string` like Go's `fmt.Sprintf("%s", names)` (`[a b c]`).
pub fn go_fmt_string_slice(names: &[String]) -> String {
    format!("[{}]", names.join(" "))
}

// ---------------------------------------------------------------------------------------------
// OS interface enumeration (Go `net.Interfaces` / `net.InterfaceByName` / `Interface.Addrs`).
// ---------------------------------------------------------------------------------------------

/// Snapshot of all network interfaces with their addresses, in the order Go's
/// `net.Interfaces()` returns them (link dump order) and, per interface, in the order Go's
/// `(*net.Interface).Addrs()` returns them (address dump order).
#[cfg(unix)]
pub fn interfaces_with_addrs() -> std::io::Result<Vec<InterfaceWithAddrs>> {
    use std::ffi::CStr;

    struct IfAddrsGuard(*mut libc::ifaddrs);
    impl Drop for IfAddrsGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: `self.0` came from a successful `getifaddrs` call.
                unsafe { libc::freeifaddrs(self.0) };
            }
        }
    }

    let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs initialises `ifap` on success; `IfAddrsGuard` frees it on drop.
    if unsafe { libc::getifaddrs(&mut ifap) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let _guard = IfAddrsGuard(ifap);

    let mut out: Vec<InterfaceWithAddrs> = Vec::new();
    let mut cur = ifap;
    while !cur.is_null() {
        // SAFETY: `cur` is a valid node of the list returned by getifaddrs.
        let ifa = unsafe { &*cur };
        cur = ifa.ifa_next;
        if ifa.ifa_name.is_null() {
            continue;
        }
        // SAFETY: ifa_name is a NUL-terminated C string owned by the list.
        let raw_name = unsafe { CStr::from_ptr(ifa.ifa_name) }.to_string_lossy().into_owned();
        // IPv4 addresses carry their IFA_LABEL ("eth0:1"); Go attributes them to the link by
        // index. ':' is not a valid character in Linux link names, so strip the alias suffix.
        let link_name = match raw_name.split_once(':') {
            Some((base, _)) => base.to_string(),
            None => raw_name.clone(),
        };

        let pos = match out.iter().position(|i| i.iface.name == link_name) {
            Some(p) => p,
            None => {
                let c_name = std::ffi::CString::new(link_name.clone()).unwrap_or_default();
                // SAFETY: c_name is a valid NUL-terminated string.
                let index = unsafe { libc::if_nametoindex(c_name.as_ptr()) };
                out.push(InterfaceWithAddrs {
                    iface: Interface {
                        index,
                        name: link_name.clone(),
                        flags: go_flags(ifa.ifa_flags),
                    },
                    addrs: Vec::new(),
                });
                out.len() - 1
            }
        };

        if ifa.ifa_addr.is_null() {
            continue;
        }
        // SAFETY: ifa_addr points to a sockaddr whose family determines its concrete type.
        let family = unsafe { (*ifa.ifa_addr).sa_family } as i32;
        let (ip, max_bits) = match family {
            libc::AF_INET => {
                // SAFETY: family is AF_INET.
                let sin = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                (IpAddr::V4(Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr))), 32u8)
            }
            libc::AF_INET6 => {
                // SAFETY: family is AF_INET6.
                let sin6 = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                (IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)), 128u8)
            }
            _ => continue,
        };
        let prefix_len = if ifa.ifa_netmask.is_null() {
            max_bits
        } else {
            // SAFETY: the netmask has the same family as the address.
            unsafe { netmask_prefix_len(ifa.ifa_netmask, family) }.unwrap_or(max_bits)
        };
        out[pos].addrs.push(InterfaceAddr { ip, prefix_len });
    }
    Ok(out)
}

#[cfg(not(unix))]
pub fn interfaces_with_addrs() -> std::io::Result<Vec<InterfaceWithAddrs>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "network interface enumeration is not supported on this platform",
    ))
}

#[cfg(unix)]
fn go_flags(raw: libc::c_uint) -> u32 {
    let raw = raw as libc::c_int;
    let mut f = 0;
    if raw & libc::IFF_UP != 0 {
        f |= flags::UP;
    }
    if raw & libc::IFF_BROADCAST != 0 {
        f |= flags::BROADCAST;
    }
    if raw & libc::IFF_LOOPBACK != 0 {
        f |= flags::LOOPBACK;
    }
    if raw & libc::IFF_POINTOPOINT != 0 {
        f |= flags::POINT_TO_POINT;
    }
    if raw & libc::IFF_MULTICAST != 0 {
        f |= flags::MULTICAST;
    }
    if raw & libc::IFF_RUNNING != 0 {
        f |= flags::RUNNING;
    }
    f
}

/// # Safety
/// `mask` must be a valid non-null `sockaddr` pointer from `getifaddrs`.
#[cfg(unix)]
unsafe fn netmask_prefix_len(mask: *const libc::sockaddr, family: i32) -> Option<u8> {
    if (*mask).sa_family as i32 != family {
        return None;
    }
    match family {
        libc::AF_INET => {
            let sin = &*(mask as *const libc::sockaddr_in);
            Some(sin.sin_addr.s_addr.count_ones() as u8)
        }
        libc::AF_INET6 => {
            let sin6 = &*(mask as *const libc::sockaddr_in6);
            Some(u128::from_ne_bytes(sin6.sin6_addr.s6_addr).count_ones() as u8)
        }
        _ => None,
    }
}

/// Go `net.Interfaces()`.
pub fn interfaces() -> std::io::Result<Vec<Interface>> {
    Ok(interfaces_with_addrs()?.into_iter().map(|i| i.iface).collect())
}

/// Go `(*net.Interface).Addrs()` for the interface with the given name.
pub fn interface_addrs(iface: &Interface) -> std::io::Result<Vec<InterfaceAddr>> {
    Ok(interfaces_with_addrs()?
        .into_iter()
        .find(|i| i.iface.name == iface.name)
        .map(|i| i.addrs)
        .unwrap_or_default())
}

/// Go `getInterfaceAddresses`: `net.InterfaceByName(name)` + `Addrs()`, normalised to bare IPs.
pub fn get_interface_addresses(name: &str) -> Result<Vec<IpAddr>, String> {
    let all = interfaces_with_addrs().map_err(|e| format!("route ip+net: {e}"))?;
    match all.into_iter().find(|i| i.iface.name == name) {
        Some(i) => Ok(i.addrs.into_iter().map(|a| a.ip).collect()),
        // net.InterfaceByName -> errNoSuchInterface wrapped in an OpError.
        None => Err("route ip+net: no such network interface".to_string()),
    }
}

// ---------------------------------------------------------------------------------------------
// netip / net predicates (Go semantics).
// ---------------------------------------------------------------------------------------------

/// `netip.Addr.Unmap()` applied for predicates that unmap 4in6 first.
fn unmap(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => ip,
        },
        v4 => v4,
    }
}

/// `netip.Addr.IsLoopback()`.
pub fn is_loopback(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => v4.octets()[0] == 127,
        IpAddr::V6(v6) => v6 == Ipv6Addr::LOCALHOST,
    }
}

/// `netip.Addr.IsLinkLocalUnicast()`.
pub fn is_link_local_unicast(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => v4.octets()[0] == 169 && v4.octets()[1] == 254,
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    }
}

/// `net.IP.IsPrivate()` (RFC 1918 for IPv4, RFC 4193 `fc00::/7` for IPv6).
pub fn is_private(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            o[0] == 10 || (o[0] == 172 && o[1] & 0xf0 == 16) || (o[0] == 192 && o[1] == 168)
        }
        IpAddr::V6(v6) => v6.octets()[0] & 0xfe == 0xfc,
    }
}

/// `netip.Addr.Is4()` (true only for 4-byte addresses, never for 4in6).
fn is4(ip: IpAddr) -> bool {
    ip.is_ipv4()
}

/// `netip.Addr.Is6()`.
fn is6(ip: IpAddr) -> bool {
    ip.is_ipv6()
}

/// `net.ParseCIDR(s)`, returning only the IP.
pub fn parse_cidr_ip(s: &str) -> Result<IpAddr, String> {
    let (addr, len) = s
        .split_once('/')
        .ok_or_else(|| format!("invalid CIDR address: {s}"))?;
    let ip: IpAddr = addr.parse().map_err(|_| format!("invalid CIDR address: {s}"))?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    match len.parse::<u32>() {
        Ok(n) if n <= max && !len.starts_with('+') && !len.is_empty() => Ok(ip),
        _ => Err(format!("invalid CIDR address: {s}")),
    }
}

// ---------------------------------------------------------------------------------------------
// dskit netutil.
// ---------------------------------------------------------------------------------------------

/// `PrivateNetworkInterfaces`: lists network interfaces and returns those having an address
/// conformant to RFC1918.
pub fn private_network_interfaces() -> Vec<String> {
    private_network_interfaces_with_fallback(&[])
}

/// `PrivateNetworkInterfacesWithFallback`.
pub fn private_network_interfaces_with_fallback(fallback: &[String]) -> Vec<String> {
    let snapshot = match interfaces_with_addrs() {
        Ok(s) => s,
        Err(err) => {
            warn!(msg = "error getting network interfaces", err = %err);
            Vec::new()
        }
    };
    let ints: Vec<Interface> = snapshot.iter().map(|i| i.iface.clone()).collect();
    private_network_interfaces_from(&ints, fallback, |i: &Interface| {
        Ok(snapshot
            .iter()
            .find(|s| s.iface.name == i.name)
            .map(|s| s.addrs.iter().map(ToString::to_string).collect())
            .unwrap_or_default())
    })
}

/// `privateNetworkInterfaces`: private testable function that checks each given interface.
/// `get_interface_addrs` mirrors dskit's injectable `getInterfaceAddrs` (returning each
/// address' `String()`, i.e. CIDR notation).
pub fn private_network_interfaces_from(
    all: &[Interface],
    fallback: &[String],
    get_interface_addrs: impl Fn(&Interface) -> Result<Vec<String>, String>,
) -> Vec<String> {
    let mut priv_ints = Vec::new();
    for i in all {
        let addrs = match get_interface_addrs(i) {
            Ok(a) => a,
            Err(err) => {
                warn!(msg = "error getting addresses from network interface", interface = %i.name, err = %err);
                Vec::new()
            }
        };
        for s in &addrs {
            let ip = match parse_cidr_ip(s) {
                Ok(ip) => ip,
                Err(err) => {
                    warn!(msg = "error parsing network interface IP address", interface = %i.name, addr = %s, err = %err);
                    continue;
                }
            };
            if is_private(ip) {
                priv_ints.push(i.name.clone());
                break;
            }
        }
    }
    if priv_ints.is_empty() {
        return fallback.to_vec();
    }
    priv_ints
}

/// The default of every `-*.instance-interface-names` flag:
/// `netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)`.
pub fn default_instance_interface_names() -> Vec<String> {
    private_network_interfaces_with_fallback(&["eth0".to_string(), "en0".to_string()])
}

/// `GetFirstAddressOf` returns the first IPv4/IPV6 address of the supplied interface names,
/// omitting any link-local addresses.
pub fn get_first_address_of(names: &[String], enable_inet6: bool) -> Result<String, NetutilError> {
    get_first_address_of_with(names, get_interface_addresses, enable_inet6, || {
        interfaces().map(|ifs| ifs.into_iter().map(|i| i.name).collect())
    })
}

/// `getFirstAddressOf` with the interface address getter (`NetworkInterfaceAddressGetter`)
/// and the `net.Interfaces()` lister injectable for tests.
pub fn get_first_address_of_with(
    names: &[String],
    interface_addrs_func: impl Fn(&str) -> Result<Vec<IpAddr>, String>,
    enable_inet6: bool,
    list_interfaces: impl FnOnce() -> std::io::Result<Vec<String>>,
) -> Result<String, NetutilError> {
    let mut ip_addr: Option<IpAddr> = None;

    // When passing an empty list of interface names, we select all interfaces.
    let all_names;
    let names = if names.is_empty() {
        all_names = list_interfaces().map_err(|err| {
            NetutilError(format!(
                "failed to get interface list and no interface names supplied: {err}"
            ))
        })?;
        &all_names[..]
    } else {
        names
    };

    debug!(msg = "looking for addresses", inf = %go_fmt_string_slice(names), inet6enabled = enable_inet6);

    for name in names {
        let addrs = match interface_addrs_func(name) {
            Ok(a) => a,
            Err(err) => {
                warn!(msg = "error getting addresses for interface", inf = %name, err = %err);
                continue;
            }
        };

        if addrs.is_empty() {
            warn!(msg = "no addresses found for interface", inf = %name, err = "null");
            continue;
        }
        if let Some(ip) = filter_best_ip(&addrs, enable_inet6) {
            // Select the best between what we've received.
            let mut candidates = vec![ip];
            candidates.extend(ip_addr);
            ip_addr = filter_best_ip(&candidates, enable_inet6);
        }
        debug!(msg = "detected highest quality address", ipAddr = %go_addr_string(ip_addr), inf = %name);
        let Some(ip) = ip_addr else {
            debug!(msg = "ignoring address", ipAddr = %go_addr_string(ip_addr), inf = %name);
            continue;
        };
        if is_link_local_unicast(ip) {
            debug!(msg = "ignoring address", ipAddr = %ip, inf = %name);
            continue;
        }

        if enable_inet6 {
            if is6(ip) {
                return Ok(ip.to_string());
            }
            continue;
        }

        return Ok(ip.to_string());
    }

    debug!(msg = "detected IP address after looking up all configured interface names", ipAddr = %go_addr_string(ip_addr));
    let Some(ip) = ip_addr else {
        return Err(NetutilError(format!(
            "no useable address found for interfaces {}",
            go_fmt_string_slice(names)
        )));
    };
    if is_link_local_unicast(ip) {
        warn!(msg = "using link-local address", address = %ip);
    }
    Ok(ip.to_string())
}

/// `netip.Addr.String()` including the zero value (`"invalid IP"`).
pub fn go_addr_string(ip: Option<IpAddr>) -> String {
    match ip {
        Some(ip) => ip.to_string(),
        None => "invalid IP".to_string(),
    }
}

/// `filterBestIP` returns an opinionated "best" address from a list of addresses.
/// A high quality address is one that is considered routable, and not in the link-local
/// address space. A low quality address is a link-local address. When IPv6 is enabled using
/// `enable_inet6`, an IPv6 will be preferred over an equivalent quality IPv4 address,
/// otherwise IPv6 addresses are guaranteed to not be returned from this function.
/// Loopback addresses are never selected. `None` is Go's invalid (zero) `netip.Addr`.
pub fn filter_best_ip(addrs: &[IpAddr], enable_inet6: bool) -> Option<IpAddr> {
    let mut inet4_addr: Option<IpAddr> = None;
    let mut inet6_addr: Option<IpAddr> = None;

    for &addr in addrs {
        if is_loopback(addr) {
            continue;
        }

        if is6(addr) && !enable_inet6 {
            continue;
        }

        if is4(addr) {
            // If we have already been set, can we improve on the quality?
            if let Some(cur) = inet4_addr {
                if is_link_local_unicast(cur) && !is_link_local_unicast(addr) {
                    inet4_addr = Some(addr);
                }
                continue;
            }
            inet4_addr = Some(addr);
        }

        if is6(addr) {
            if let Some(cur) = inet6_addr {
                if is_link_local_unicast(cur) && !is_link_local_unicast(addr) {
                    inet6_addr = Some(addr);
                }
                continue;
            }
            inet6_addr = Some(addr);
        }
    }

    // If both address families have been set, compare.
    if let (Some(v4), Some(v6)) = (inet4_addr, inet6_addr) {
        if is_link_local_unicast(v4) && !is_link_local_unicast(v6) {
            return Some(v6);
        }
        if is_link_local_unicast(v6) && !is_link_local_unicast(v4) {
            return Some(v4);
        }
        if enable_inet6 {
            return Some(v6);
        }
        return Some(v4);
    }

    inet4_addr.or(inet6_addr)
}

#[cfg(test)]
mod tests {
    //! Ported from github.com/grafana/dskit/netutil/netutil_test.go.
    use super::*;
    use std::collections::HashMap;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    /// `generateTestInterfaces`.
    fn generate_test_interfaces(names: &[&str]) -> Vec<Interface> {
        names
            .iter()
            .enumerate()
            .map(|(i, n)| Interface { index: i as u32 + 1, name: n.to_string(), flags: 0 })
            .collect()
    }

    #[test]
    fn test_private_interface() {
        let test_ints_addrs: HashMap<&str, Vec<&str>> = HashMap::from([
            ("privNetA", vec!["10.6.19.34/8"]),
            ("privNetB", vec!["172.16.0.7/12"]),
            ("privNetC", vec!["192.168.3.29/24"]),
            ("pubNet", vec!["34.120.177.193/24"]),
            ("multiPriv", vec!["10.6.19.34/8", "172.16.0.7/12"]),
            ("multiMix", vec!["1.1.1.1/24", "192.168.0.42/24"]),
            ("multiPub", vec!["1.1.1.1/24", "34.120.177.193/24"]),
        ]);
        let default_output = s(&["eth0", "en0"]);
        let cases: Vec<(&str, Vec<&str>, Vec<String>)> = vec![
            ("empty interface list", vec![], default_output.clone()),
            ("single private interface", vec!["privNetA"], s(&["privNetA"])),
            ("single public interface", vec!["pubNet"], default_output.clone()),
            ("single interface multi address private", vec!["multiPriv"], s(&["multiPriv"])),
            ("single interface multi address mix", vec!["multiMix"], s(&["multiMix"])),
            ("single interface multi address public", vec!["multiPub"], default_output.clone()),
            (
                "all private interfaces",
                vec!["privNetA", "privNetB", "privNetC"],
                s(&["privNetA", "privNetB", "privNetC"]),
            ),
            (
                "mix of public and private interfaces",
                vec!["pubNet", "privNetA", "privNetB", "privNetC", "multiPriv", "multiMix", "multiPub"],
                s(&["privNetA", "privNetB", "privNetC", "multiPriv", "multiMix"]),
            ),
        ];
        for (desc, ints, expected) in cases {
            let got = private_network_interfaces_from(
                &generate_test_interfaces(&ints),
                &default_output,
                |i: &Interface| {
                    Ok(test_ints_addrs
                        .get(i.name.as_str())
                        .map(|v| v.iter().map(|x| x.to_string()).collect())
                        .unwrap_or_default())
                },
            );
            assert_eq!(got, expected, "{desc}");
        }
    }

    #[test]
    fn test_private_interface_error() {
        let interfaces = generate_test_interfaces(&["eth9"]);
        let got = private_network_interfaces_from(&interfaces, &[], |_| {
            Ok(vec!["not_a_parseable_ip_string".to_string()])
        });
        assert_eq!(got, Vec::<String>::new());
    }

    /// `getMockInterfaceAddresses`.
    fn get_mock_interface_addresses(name: &str) -> Result<Vec<IpAddr>, String> {
        let to_addr = |a: &str| parse_cidr_ip(a).unwrap();
        let interfaces: HashMap<&str, Vec<IpAddr>> = HashMap::from([
            (
                "wlan0",
                vec![
                    to_addr("172.16.16.51/24"),
                    to_addr("fc16::c9a7:1d89:2dd8:f59a/64"),
                    to_addr("fe80::ec62:ed39:f931:bf6d/64"),
                ],
            ),
            ("em0", vec![to_addr("fe80::ec62:ed39:f931:bf6d/64"), to_addr("fc99::9a/64")]),
            (
                "em1",
                vec![
                    to_addr("10.54.99.53/24"),
                    to_addr("fcca::e6:9274:f580:7a1b:c335/64"),
                    to_addr("fe80::38f8:7eff:fe7c:6e5d/64"),
                ],
            ),
            (
                "em2",
                vec![
                    to_addr("10.54.99.53/24"),
                    to_addr("fe80::38f8:7eff:fe7c:6e56/64"),
                    to_addr("fe80::38f8:7eff:fe7c:6e5d/64"),
                ],
            ),
            ("lo1", vec![to_addr("fe80::ec62:ed39:f931:bf6d/64")]),
            ("lo2", vec![to_addr("169.254.1.1/16")]),
            ("lo9", vec![]),
            ("lo10", vec![]),
            ("enp0s31f6", vec![to_addr("1.1.1.1/31")]),
            ("enp0s31f7", vec![to_addr("2001::1111/120")]),
        ]);
        interfaces
            .get(name)
            .cloned()
            .ok_or_else(|| "no such network interface".to_string())
    }

    #[test]
    fn test_get_first_address_of() {
        struct Case {
            names: Vec<&'static str>,
            addr: &'static str,
            err: Option<&'static str>,
            enable_inet6: bool,
        }
        let c = |names: Vec<&'static str>, addr, err, enable_inet6| Case { names, addr, err, enable_inet6 };
        let cases = vec![
            c(vec!["wlan0"], "172.16.16.51", None, false),
            c(vec!["em0"], "", Some("no useable address found for interfaces [em0]"), false),
            c(vec!["em0"], "fc99::9a", None, true),
            c(vec!["lo1"], "", Some("no useable address found for interfaces [lo1]"), false),
            c(vec!["lo1"], "fe80::ec62:ed39:f931:bf6d", None, true),
            c(vec!["lo2"], "169.254.1.1", None, false),
            c(vec!["lo9"], "", Some("no useable address found for interfaces [lo9]"), false),
            c(vec!["lo9", "lo10"], "", Some("no useable address found for interfaces [lo9 lo10]"), false),
            c(vec!["lo1", "lo2", "enp0s31f6"], "1.1.1.1", None, false),
            c(vec!["lo1", "lo2", "enp0s31f7"], "169.254.1.1", None, false),
            c(vec!["lo1", "lo2", "enp0s31f7", "enp0s31f6"], "2001::1111", None, true),
            c(vec!["lo1", "lo2", "enp0s31f6", "enp0s31f7"], "1.1.1.1", None, false),
            c(vec!["lo1", "lo2", "enp0s31f6", "enp0s31f7"], "2001::1111", None, true),
            c(vec!["em0", "em1"], "fc99::9a", None, true),
            c(vec!["em1"], "fcca::e6:9274:f580:7a1b:c335", None, true),
            c(vec!["lo1", "enp0s31f6"], "1.1.1.1", None, true),
            c(vec!["lo2", "enp0s31f6"], "1.1.1.1", None, true),
            c(vec!["em2"], "10.54.99.53", None, true),
        ];
        for tc in cases {
            let names = s(&tc.names);
            let res = get_first_address_of_with(
                &names,
                get_mock_interface_addresses,
                tc.enable_inet6,
                || panic!("interface listing must not be used when names are supplied"),
            );
            match tc.err {
                Some(e) => assert_eq!(res, Err(NetutilError(e.to_string())), "{:?}", tc.names),
                None => assert_eq!(res, Ok(tc.addr.to_string()), "{:?}", tc.names),
            }
        }
    }

    /// Empty name list: all interfaces from `net.Interfaces()` are considered, and the error
    /// lists them.
    #[test]
    fn test_get_first_address_of_empty_names_uses_all_interfaces() {
        let res = get_first_address_of_with(&[], get_mock_interface_addresses, false, || {
            Ok(s(&["lo9", "em0", "wlan0"]))
        });
        assert_eq!(res, Ok("172.16.16.51".to_string()));
        let res = get_first_address_of_with(&[], get_mock_interface_addresses, false, || {
            Ok(s(&["lo9", "lo10"]))
        });
        assert_eq!(
            res,
            Err(NetutilError("no useable address found for interfaces [lo9 lo10]".into()))
        );
        let res = get_first_address_of_with(&[], get_mock_interface_addresses, false, || {
            Err(std::io::Error::other("boom"))
        });
        assert_eq!(
            res,
            Err(NetutilError(
                "failed to get interface list and no interface names supplied: boom".into()
            ))
        );
    }

    #[test]
    fn test_filter_best_ip() {
        let to_addr = |a: &str| a.parse::<IpAddr>().unwrap();
        let cases: Vec<(Vec<&str>, &str, bool)> = vec![
            (vec!["169.254.1.1", "127.0.0.1"], "169.254.1.1", false),
            (vec!["1.1.1.1"], "1.1.1.1", false),
            (vec!["10.54.99.53", "169.254.1.1"], "10.54.99.53", false),
            (vec!["169.254.1.1", "fe80::1"], "169.254.1.1", false),
            (vec!["169.254.1.1", "fe80::1"], "fe80::1", true),
            (vec!["169.254.1.1", "fe80::1"], "fe80::1", true),
            (vec!["fc99::1", "fe80::1"], "invalid IP", false),
            (vec!["169.254.1.1", "10.54.99.53", "fc99::1", "fe80::1"], "fc99::1", true),
            (vec!["169.254.1.1", "10.54.99.53", "fc99::1", "fe80::1"], "10.54.99.53", false),
        ];
        for (addrs, expected, inet6) in cases {
            let addrs: Vec<IpAddr> = addrs.into_iter().map(to_addr).collect();
            assert_eq!(go_addr_string(filter_best_ip(&addrs, inet6)), expected, "{addrs:?}");
        }
    }

    /// Cases carried over from PR #2 (crates/mimir-ring/src/netutil.rs, superseded by this
    /// module): loopback is never selected; missing / loopback-only / empty interfaces are
    /// skipped in order.
    #[test]
    fn test_pr2_loopback_and_skipped_interfaces() {
        let ips = |s: &[&str]| s.iter().map(|a| a.parse::<IpAddr>().unwrap()).collect::<Vec<_>>();
        assert_eq!(filter_best_ip(&ips(&["127.0.0.1", "::1"]), true), None);
        assert_eq!(filter_best_ip(&ips(&["127.0.0.1", "::1"]), false), None);

        let lookup = |name: &str| match name {
            "lo" => Ok(ips(&["127.0.0.1", "::1"])),
            "eth0" => Ok(ips(&["fe80::42", "10.1.2.3"])),
            "empty" => Ok(vec![]),
            _ => Err("no such network interface".to_string()),
        };
        let no_listing = || -> std::io::Result<Vec<String>> { panic!("names were supplied") };
        assert_eq!(
            get_first_address_of_with(&s(&["missing", "lo", "empty", "eth0"]), lookup, false, no_listing),
            Ok("10.1.2.3".to_string())
        );
        assert_eq!(
            get_first_address_of_with(&s(&["lo"]), lookup, false, no_listing),
            Err(NetutilError("no useable address found for interfaces [lo]".into()))
        );
        // Real interfaces never yield loopback.
        if let Ok(addr) = get_first_address_of(&[], false) {
            assert!(!is_loopback(addr.parse().unwrap()), "{addr}");
        }
    }

    #[test]
    fn test_is_private_matches_go() {
        for (ip, want) in [
            ("10.0.0.1", true),
            ("172.16.0.1", true),
            ("172.31.255.255", true),
            ("172.32.0.1", false),
            ("192.168.1.1", true),
            ("100.64.0.1", false),
            ("127.0.0.1", false),
            ("fc00::1", true),
            ("fd00::1", true),
            ("fe80::1", false),
            ("::ffff:10.0.0.1", true),
        ] {
            assert_eq!(is_private(ip.parse().unwrap()), want, "{ip}");
        }
    }

    /// The real OS enumeration must at least see the loopback interface with its address.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_os_enumeration_sees_loopback() {
        let all = interfaces_with_addrs().unwrap();
        let lo = all.iter().find(|i| i.iface.name == "lo").expect("lo present");
        assert!(lo.iface.flags & flags::LOOPBACK != 0);
        assert!(lo.addrs.iter().any(|a| a.ip == IpAddr::V4(Ipv4Addr::LOCALHOST) && a.prefix_len == 8));
        assert_eq!(get_interface_addresses("does-not-exist-0"), Err("route ip+net: no such network interface".into()));
    }
}
