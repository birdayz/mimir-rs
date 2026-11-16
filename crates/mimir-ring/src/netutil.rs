//! Instance address discovery from network interfaces.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/util.go (`GetInstanceAddr`)
//! - github.com/grafana/dskit/netutil/netutil.go (`GetFirstAddressOf`, `filterBestIP`)

use std::ffi::CStr;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use tracing::{debug, warn};

/// Returns `config_addr` if set, otherwise the best address of the first usable interface in
/// `interfaces` (all interfaces when empty). Mirrors dskit `ring.GetInstanceAddr`.
pub fn get_instance_addr(
    config_addr: &str,
    interfaces: &[String],
    enable_inet6: bool,
) -> Result<String, String> {
    if !config_addr.is_empty() {
        return Ok(config_addr.to_string());
    }
    let all = interface_addrs().map_err(|e| format!("failed to get interface list: {e}"))?;
    let names: Vec<String> = if interfaces.is_empty() {
        let mut names: Vec<String> = Vec::new();
        for (name, _) in &all {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        names
    } else {
        interfaces.to_vec()
    };
    get_first_address_of(&names, enable_inet6, |name| {
        if !all.iter().any(|(n, _)| n == name) {
            return Err(format!("route ip+net: no such network interface {name:?}"));
        }
        Ok(all
            .iter()
            .filter(|(n, _)| n == name)
            .filter_map(|(_, a)| *a)
            .collect())
    })
}

fn get_first_address_of(
    names: &[String],
    enable_inet6: bool,
    addrs_of: impl Fn(&str) -> Result<Vec<IpAddr>, String>,
) -> Result<String, String> {
    let mut ip_addr: Option<IpAddr> = None;
    debug!(inf = ?names, inet6enabled = enable_inet6, "looking for addresses");

    for name in names {
        let addrs = match addrs_of(name) {
            Ok(a) => a,
            Err(err) => {
                warn!(inf = %name, %err, "error getting addresses for interface");
                continue;
            }
        };
        if addrs.is_empty() {
            warn!(inf = %name, "no addresses found for interface");
            continue;
        }
        if let Some(ip) = filter_best_ip(&addrs, enable_inet6) {
            // Select the best between what we've received.
            let candidates: Vec<IpAddr> = std::iter::once(ip).chain(ip_addr).collect();
            ip_addr = filter_best_ip(&candidates, enable_inet6);
        }
        debug!(ipAddr = ?ip_addr, inf = %name, "detected highest quality address");
        let ip = match ip_addr {
            Some(ip) if !is_link_local_unicast(ip) => ip,
            _ => {
                debug!(ipAddr = ?ip_addr, inf = %name, "ignoring address");
                continue;
            }
        };
        if enable_inet6 && !ip.is_ipv6() {
            continue;
        }
        return Ok(ip.to_string());
    }

    debug!(ipAddr = ?ip_addr, "detected IP address after looking up all configured interface names");
    let Some(ip) = ip_addr else {
        return Err(format!(
            "no useable address found for interfaces [{}]",
            names.join(" ")
        ));
    };
    if is_link_local_unicast(ip) {
        warn!(address = %ip, "using link-local address");
    }
    Ok(ip.to_string())
}

/// Opinionated "best" address: routable beats link-local; loopback is never selected; IPv6 is only
/// considered (and preferred at equal quality) when `enable_inet6` is set.
fn filter_best_ip(addrs: &[IpAddr], enable_inet6: bool) -> Option<IpAddr> {
    let (mut inet4, mut inet6): (Option<IpAddr>, Option<IpAddr>) = (None, None);
    for &addr in addrs {
        if addr.is_loopback() || (addr.is_ipv6() && !enable_inet6) {
            continue;
        }
        let slot = if addr.is_ipv4() {
            &mut inet4
        } else {
            &mut inet6
        };
        match *slot {
            // If we have already been set, can we improve on the quality?
            Some(cur) if is_link_local_unicast(cur) && !is_link_local_unicast(addr) => {
                *slot = Some(addr)
            }
            Some(_) => {}
            None => *slot = Some(addr),
        }
    }
    match (inet4, inet6) {
        (Some(v4), Some(v6)) => {
            if is_link_local_unicast(v4) && !is_link_local_unicast(v6) {
                Some(v6)
            } else if is_link_local_unicast(v6) && !is_link_local_unicast(v4) {
                Some(v4)
            } else if enable_inet6 {
                Some(v6)
            } else {
                Some(v4)
            }
        }
        (v4, v6) => v4.or(v6),
    }
}

fn is_link_local_unicast(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_unicast_link_local(),
    }
}

/// `(interface name, address)` for every entry from getifaddrs(3); `None` for non-IP families, so
/// interfaces without an IP are still listed (like Go `net.InterfaceByName` finding them).
fn interface_addrs() -> io::Result<Vec<(String, Option<IpAddr>)>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: on success getifaddrs stores a list that we free exactly once with freeifaddrs below.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let mut out = Vec::new();
    let mut cur = head;
    while !cur.is_null() {
        // SAFETY: `cur` is a live node of the list returned by getifaddrs; ifa_name is a valid
        // C string and ifa_addr, when non-null, points to a sockaddr whose family tag says
        // whether it is a sockaddr_in or sockaddr_in6.
        let ifa = unsafe { &*cur };
        let name = unsafe { CStr::from_ptr(ifa.ifa_name) }
            .to_string_lossy()
            .into_owned();
        let addr = if ifa.ifa_addr.is_null() {
            None
        } else {
            match i32::from(unsafe { (*ifa.ifa_addr).sa_family }) {
                libc::AF_INET => {
                    let sin = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                    Some(IpAddr::V4(Ipv4Addr::from(u32::from_be(
                        sin.sin_addr.s_addr,
                    ))))
                }
                libc::AF_INET6 => {
                    let sin6 = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                    Some(IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)))
                }
                _ => None,
            }
        };
        out.push((name, addr));
        cur = ifa.ifa_next;
    }
    // SAFETY: `head` came from a successful getifaddrs call and is not used after this.
    unsafe { libc::freeifaddrs(head) };
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ips(s: &[&str]) -> Vec<IpAddr> {
        s.iter().map(|a| a.parse().unwrap()).collect()
    }

    // Cases from dskit netutil_test.go TestFilterBestIP.
    #[test]
    fn filter_best_ip_matches_dskit() {
        let all4 = &["169.254.1.1", "10.54.99.53", "fc99::1", "fe80::1"][..];
        let cases: &[(&[&str], bool, Option<&str>)] = &[
            (&["169.254.1.1", "127.0.0.1"], false, Some("169.254.1.1")),
            (&["1.1.1.1"], false, Some("1.1.1.1")),
            (&["10.54.99.53", "169.254.1.1"], false, Some("10.54.99.53")),
            (&["169.254.1.1", "fe80::1"], false, Some("169.254.1.1")),
            (&["169.254.1.1", "fe80::1"], true, Some("fe80::1")),
            (&["fc99::1", "fe80::1"], false, None), // inet6 is not enabled
            (all4, true, Some("fc99::1")),
            (all4, false, Some("10.54.99.53")),
            // Not in dskit's table: loopback is never selected.
            (&["127.0.0.1", "::1"], true, None),
        ];
        for (addrs, inet6, want) in cases {
            let got = filter_best_ip(&ips(addrs), *inet6).map(|a| a.to_string());
            assert_eq!(got.as_deref(), *want, "{addrs:?} inet6={inet6}");
        }
    }

    #[test]
    fn first_address_skips_loopback_and_missing_interfaces() {
        let lookup = |name: &str| match name {
            "lo" => Ok(ips(&["127.0.0.1", "::1"])),
            "eth0" => Ok(ips(&["fe80::42", "10.1.2.3"])),
            "empty" => Ok(vec![]),
            _ => Err("no such network interface".to_string()),
        };
        let names = |n: &[&str]| n.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            get_first_address_of(&names(&["missing", "lo", "empty", "eth0"]), false, lookup),
            Ok("10.1.2.3".to_string())
        );
        assert_eq!(
            get_first_address_of(&names(&["lo"]), false, lookup),
            Err("no useable address found for interfaces [lo]".to_string())
        );
    }

    #[test]
    fn config_addr_wins_and_real_interfaces_never_yield_loopback() {
        assert_eq!(
            get_instance_addr("1.2.3.4", &[], false),
            Ok("1.2.3.4".into())
        );
        if let Ok(addr) = get_instance_addr("", &[], false) {
            assert!(!addr.parse::<IpAddr>().unwrap().is_loopback(), "{addr}");
        }
    }
}
