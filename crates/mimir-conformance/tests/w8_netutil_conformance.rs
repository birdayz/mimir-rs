//! Tier-1 differential conformance for instance address auto-detection on the live host.
//!
//! Compares `mimir_common::netutil` / `mimir_common::sockaddr` / `mimir_ring::ring::util`
//! against the upstream Go functions run by `target/go-oracle`:
//! - github.com/grafana/dskit/netutil/netutil.go (`PrivateNetworkInterfacesWithFallback`,
//!   `GetFirstAddressOf`)
//! - github.com/grafana/dskit/ring/util.go (`GetInstanceAddr`)
//! - github.com/hashicorp/go-sockaddr (`GetPrivateInterfaces`, `GetPrivateIP`)
//! - Go `sort.Sort` (pdqsort; go-sockaddr's tie order depends on it)

use mimir_common::{gosort, netutil, sockaddr};
use mimir_conformance::oracle::invoke_oracle;
use mimir_ring::ring::util::get_instance_addr;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, Debug, PartialEq)]
struct PrivIf {
    name: String,
    ip: String,
}

#[derive(Deserialize, Debug)]
struct NetAddrDetect {
    default_interface_names: Vec<String>,
    instance_addr: String,
    instance_addr_err: String,
    first_addr_all_inet6: String,
    first_addr_all_inet6_err: String,
    private_interfaces: Vec<PrivIf>,
    private_ip: String,
    private_ip_err: String,
}

fn split(r: Result<String, netutil::NetutilError>) -> (String, String) {
    match r {
        Ok(a) => (a, String::new()),
        Err(e) => (String::new(), e.to_string()),
    }
}

/// Interfaces can change while the test runs (containers starting/stopping); retry a few times
/// before declaring a divergence.
fn with_retries(mut f: impl FnMut() -> Result<(), String>) {
    let mut last = String::new();
    for _ in 0..5 {
        match f() {
            Ok(()) => return,
            Err(e) => last = e,
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("{last}");
}

#[test]
fn w8_net_addr_detect_matches_go_on_host() {
    for (names, inet6) in [
        (None, false),
        (None, true),
        (Some(vec!["lo".to_string()]), false),
        (Some(vec!["mimir-no-such-if0".to_string(), "lo".to_string()]), true),
    ] {
        with_retries(|| {
            let go: NetAddrDetect = invoke_oracle(
                "net-addr-detect",
                &json!({ "names": names, "enable_inet6": inet6 }),
            )
            .map_err(|e| format!("oracle: {e:#}"))?;

            let default_names = netutil::default_instance_interface_names();
            let used = names.clone().unwrap_or_else(|| default_names.clone());
            let (addr, addr_err) = split(get_instance_addr("", &used, inet6));
            let (all6, all6_err) = split(netutil::get_first_address_of(&[], true));
            let private: Vec<PrivIf> = sockaddr::get_private_interfaces()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|a| PrivIf { name: a.iface.name, ip: a.addr.ip.to_string() })
                .collect();
            let private_ip = sockaddr::get_private_ip().map_err(|e| e.to_string())?;

            let mut errs = Vec::new();
            if default_names != go.default_interface_names {
                errs.push(format!(
                    "default interface names: Go {:?}, Rust {default_names:?}",
                    go.default_interface_names
                ));
            }
            if (addr.as_str(), addr_err.as_str())
                != (go.instance_addr.as_str(), go.instance_addr_err.as_str())
            {
                errs.push(format!(
                    "GetInstanceAddr({used:?}, inet6={inet6}): Go ({:?}, {:?}), Rust ({addr:?}, {addr_err:?})",
                    go.instance_addr, go.instance_addr_err
                ));
            }
            if (all6.as_str(), all6_err.as_str())
                != (go.first_addr_all_inet6.as_str(), go.first_addr_all_inet6_err.as_str())
            {
                errs.push(format!(
                    "GetFirstAddressOf(nil, inet6): Go ({:?}, {:?}), Rust ({all6:?}, {all6_err:?})",
                    go.first_addr_all_inet6, go.first_addr_all_inet6_err
                ));
            }
            if private != go.private_interfaces {
                errs.push(format!(
                    "GetPrivateInterfaces: Go {:?}, Rust {private:?}",
                    go.private_interfaces
                ));
            }
            if !go.private_ip_err.is_empty() || private_ip != go.private_ip {
                errs.push(format!(
                    "GetPrivateIP: Go ({:?}, {:?}), Rust {private_ip:?}",
                    go.private_ip, go.private_ip_err
                ));
            }
            if errs.is_empty() {
                Ok(())
            } else {
                Err(errs.join("\n"))
            }
        });
    }
}

#[derive(Deserialize)]
struct GoSortResp {
    perm: Vec<usize>,
}

/// Go's `sort.Sort` is unstable for n > 12; the resulting order of equal elements must match
/// bit-for-bit (go-sockaddr picks `privateIfs[0]` after such a sort).
#[test]
fn w8_go_sort_permutation_matches_go() {
    let mut seed = 0x2545f4914f6cdd1du64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut inputs: Vec<Vec<i64>> = Vec::new();
    for n in [0usize, 1, 2, 7, 12, 13, 20, 33, 49, 50, 51, 64, 100, 257, 1000, 4096] {
        for modulus in [2u64, 3, 7, 1_000_000] {
            inputs.push((0..n).map(|_| (next() % modulus) as i64).collect());
        }
        inputs.push((0..n as i64).collect());
        inputs.push((0..n as i64).rev().collect());
        inputs.push((0..n as i64).map(|i| i % 5).collect());
        // Mostly sorted with a few swaps (exercises partialInsertionSort).
        let mut v: Vec<i64> = (0..n as i64).collect();
        if n > 4 {
            v.swap(1, n - 2);
            v.swap(n / 2, n / 3);
        }
        inputs.push(v);
    }
    for keys in inputs {
        let go: GoSortResp = invoke_oracle("go-sort", &json!({ "keys": keys })).unwrap();
        let mut items: Vec<(i64, usize)> = keys.iter().copied().zip(0..).collect();
        gosort::sort_by(&mut items, |a, b| a.0 < b.0);
        let perm: Vec<usize> = items.into_iter().map(|(_, i)| i).collect();
        assert_eq!(perm, go.perm, "n={} keys={keys:?}", keys.len());
    }
}
