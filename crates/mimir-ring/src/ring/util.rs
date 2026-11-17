//! Instance address/port resolution for ring registration.
//!
//! Ported from github.com/grafana/dskit/ring/util.go (`GetInstanceAddr`, `GetInstancePort`)
//! and Go's `net.JoinHostPort`.

use mimir_common::netutil::{self, NetutilError};

/// `GetInstanceAddr` returns the address to use to register the instance in the ring.
pub fn get_instance_addr(
    config_addr: &str,
    net_interfaces: &[String],
    enable_inet6: bool,
) -> Result<String, NetutilError> {
    get_instance_addr_with(config_addr, enable_inet6, |inet6| {
        netutil::get_first_address_of(net_interfaces, inet6)
    })
}

/// `GetInstanceAddr` with the `netutil.GetFirstAddressOf` call injectable for tests.
pub fn get_instance_addr_with(
    config_addr: &str,
    enable_inet6: bool,
    get_first_address_of: impl FnOnce(bool) -> Result<String, NetutilError>,
) -> Result<String, NetutilError> {
    if !config_addr.is_empty() {
        return Ok(config_addr.to_string());
    }
    get_first_address_of(enable_inet6)
}

/// `GetInstancePort` returns the port to use to register the instance in the ring.
pub fn get_instance_port(config_port: i64, listen_port: i64) -> i64 {
    if config_port > 0 {
        return config_port;
    }
    listen_port
}

/// Go `net.JoinHostPort`: brackets the host when it contains a colon.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_instance_addr_prefers_configured_addr() {
        let got = get_instance_addr_with("1.2.3.4", false, |_| panic!("must not auto-detect"));
        assert_eq!(got, Ok("1.2.3.4".to_string()));
        // Configured addresses are returned verbatim (no validation, like Go).
        let got = get_instance_addr_with("ingester-0.svc", true, |_| panic!("must not auto-detect"));
        assert_eq!(got, Ok("ingester-0.svc".to_string()));
    }

    #[test]
    fn get_instance_addr_auto_detects_and_propagates_errors() {
        let got = get_instance_addr_with("", true, |inet6| {
            assert!(inet6);
            Ok("fc99::9a".to_string())
        });
        assert_eq!(got, Ok("fc99::9a".to_string()));
        let err = NetutilError("no useable address found for interfaces [nope0]".into());
        let got = get_instance_addr_with("", false, |_| Err(err.clone()));
        assert_eq!(got, Err(err));
    }

    #[test]
    fn get_instance_addr_unknown_interface_errors() {
        let got = get_instance_addr("", &["mimir-no-such-if0".to_string()], false);
        assert_eq!(
            got,
            Err(NetutilError(
                "no useable address found for interfaces [mimir-no-such-if0]".into()
            ))
        );
    }

    #[test]
    fn get_instance_port_cases() {
        assert_eq!(get_instance_port(0, 9095), 9095);
        assert_eq!(get_instance_port(-1, 9095), 9095);
        assert_eq!(get_instance_port(1111, 9095), 1111);
    }

    #[test]
    fn join_host_port_cases() {
        assert_eq!(join_host_port("10.0.0.1", "9095"), "10.0.0.1:9095");
        assert_eq!(join_host_port("fc99::9a", "9095"), "[fc99::9a]:9095");
        assert_eq!(join_host_port("localhost", "0"), "localhost:0");
    }
}
