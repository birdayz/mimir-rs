//! Go `net` helpers relied upon by dskit's DNS resolver.
//!
//! Ported from the Go standard library: net/ipsock.go (`SplitHostPort`,
//! `JoinHostPort`) and net/ip.go (`IP.String`).

use std::net::IpAddr;

/// Port of Go's `net.SplitHostPort`. Returns `None` whenever Go returns an
/// error (missing port, too many colons, unbalanced brackets).
pub fn split_host_port(hostport: &str) -> Option<(&str, &str)> {
    let bytes = hostport.as_bytes();
    // The port starts after the last colon.
    let i = hostport.rfind(':')?;
    let (host, j, k);
    if bytes[0] == b'[' {
        // Expect the first ']' just before the last ':'.
        let end = hostport.find(']')?;
        if end + 1 == hostport.len() {
            // There can't be a ':' behind the ']' now.
            return None;
        } else if end + 1 != i {
            // Either ']' isn't followed by a colon, or it is followed by a
            // colon that is not the last one.
            return None;
        }
        host = &hostport[1..end];
        j = 1;
        k = end + 1;
    } else {
        host = &hostport[..i];
        if host.contains(':') {
            return None;
        }
        j = 0;
        k = 0;
    }
    if hostport[j..].contains('[') {
        return None;
    }
    if hostport[k..].contains(']') {
        return None;
    }
    Some((host, &hostport[i + 1..]))
}

/// Port of Go's `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: &str) -> String {
    // We assume that host is a literal IPv6 address if host has colons.
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Port of Go's `net.IP.String()`: IPv4-mapped IPv6 addresses are printed in
/// dotted-quad form, everything else in RFC 5952 form.
pub fn go_ip_string(ip: &IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_host_port_matches_go() {
        // Cases from Go's net/ipsock_test.go (splitJoinTests / splitFailureTests).
        let ok = [
            ("www.google.com:80", "www.google.com", "80"),
            ("127.0.0.1:1234", "127.0.0.1", "1234"),
            ("[::1]:80", "::1", "80"),
            ("[fe80::1%lo0]:80", "fe80::1%lo0", "80"),
            ("localhost%lo0:80", "localhost%lo0", "80"),
            (":80", "", "80"),
            ("host:", "host", ""),
            ("[]:0", "", "0"),
        ];
        for (in_, h, p) in ok {
            assert_eq!(split_host_port(in_), Some((h, p)), "{in_}");
        }
        for bad in [
            "www.google.com",
            "127.0.0.1",
            "[::1]",
            "[fe80::1%lo0]",
            "[localhost%lo0]",
            "localhost%lo0",
            "::1",
            "fe80::1%lo0",
            "fe80::1%lo0:80",
            "[::1]:80:80",
            "[::1]]:80",
            "[foo:bar]",
            "[foo:bar]baz",
            "[foo]bar:baz",
            "[foo]:[bar]:baz",
            "[foo]:[bar]baz",
            "foo[bar]:baz",
            "foo]bar:baz",
            "",
        ] {
            assert_eq!(split_host_port(bad), None, "{bad}");
        }
    }

    #[test]
    fn join_host_port_matches_go() {
        assert_eq!(join_host_port("::1", "80"), "[::1]:80");
        assert_eq!(join_host_port("1.2.3.4", "80"), "1.2.3.4:80");
        assert_eq!(
            join_host_port("alt1.mycompany.com.", "8080"),
            "alt1.mycompany.com.:8080"
        );
    }

    #[test]
    fn go_ip_string_prints_v4_mapped_as_v4() {
        assert_eq!(
            go_ip_string(&"::ffff:10.0.0.1".parse().unwrap()),
            "10.0.0.1"
        );
        assert_eq!(go_ip_string(&"2001:db8::1".parse().unwrap()), "2001:db8::1");
        assert_eq!(go_ip_string(&"10.0.0.1".parse().unwrap()), "10.0.0.1");
    }
}
