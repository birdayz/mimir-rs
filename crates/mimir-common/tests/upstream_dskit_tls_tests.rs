//! Ported from github.com/grafana/dskit/crypto/tls/tls_test.go (6 tests).

use std::fs;
use std::path::PathBuf;

use mimir_common::tls::{ClientConfig, TlsConfigError};
use rustls::pki_types::ServerName;
use tempfile::TempDir;

// certPEM and keyPEM are copied from the golang crypto/tls library
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----";

const KEY_PEM: &str = "-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----";

// caPEM is CA certificate of Let's Encrypt
const CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----";

struct X509Paths {
    _dir: TempDir,
    cert: PathBuf,
    key: PathBuf,
    ca: PathBuf,
}

fn new_test_x509_files(cert: Option<&[u8]>, key: Option<&[u8]>, ca: Option<&[u8]>) -> X509Paths {
    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    let ca_path = dir.path().join("ca.pem");
    if let Some(b) = cert {
        fs::write(&cert_path, b).expect("write cert");
    }
    if let Some(b) = key {
        fs::write(&key_path, b).expect("write key");
    }
    if let Some(b) = ca {
        fs::write(&ca_path, b).expect("write ca");
    }
    X509Paths {
        _dir: dir,
        cert: cert_path,
        key: key_path,
        ca: ca_path,
    }
}

// Upstream: github.com/grafana/dskit/crypto/tls/tls_test.go::TestGetTLSConfig_ClientCerts
#[test]
fn test_get_tls_config_client_certs() {
    let paths = new_test_x509_files(Some(CERT_PEM.as_bytes()), Some(KEY_PEM.as_bytes()), None);

    // Working certificate passed.
    let c = ClientConfig {
        cert_path: paths.cert.to_string_lossy().into_owned(),
        key_path: paths.key.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let tls_cfg = c.get_tls_config().expect("valid client cert config");
    assert!(
        !tls_cfg.insecure_skip_verify,
        "make sure we default to not skip verification"
    );
    assert!(
        tls_cfg.config.client_auth_cert_resolver.has_certs(),
        "ensure GetClientCertificate equivalent is set"
    );
    let resolved_client_cert = tls_cfg
        .config
        .client_auth_cert_resolver
        .resolve(&[], &[])
        .expect("ensure GetClientCertificate returns a certificate");
    let both = c
        .get_client_and_server_tls_config()
        .expect("backwards compatibility with callers using client config for servers");
    assert!(!resolved_client_cert.cert.is_empty());
    assert!(both.client.config.client_auth_cert_resolver.has_certs());

    // Expect error with key and cert swapped.
    let c = ClientConfig {
        cert_path: paths.key.to_string_lossy().into_owned(),
        key_path: paths.cert.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let err = c.get_tls_config().unwrap_err().to_string();
    assert!(
        err.contains("failed to find certificate PEM data in certificate input, but did find a private key"),
        "unexpected error: {err}"
    );

    // Expect error with only key passed along.
    let c = ClientConfig {
        key_path: paths.key.to_string_lossy().into_owned(),
        ..Default::default()
    };
    assert_eq!(c.get_tls_config().unwrap_err(), TlsConfigError::CertMissing);

    // Expect error with only cert passed along.
    let c = ClientConfig {
        cert_path: paths.cert.to_string_lossy().into_owned(),
        ..Default::default()
    };
    assert_eq!(c.get_tls_config().unwrap_err(), TlsConfigError::KeyMissing);
}

// Upstream: github.com/grafana/dskit/crypto/tls/tls_test.go::TestGetTLSConfig_CA
#[test]
fn test_get_tls_config_ca() {
    // Single CA passed.
    let paths = new_test_x509_files(None, None, Some(CERT_PEM.as_bytes()));
    let c = ClientConfig {
        ca_path: paths.ca.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let tls_cfg = c.get_tls_config().expect("single CA");
    assert_eq!(tls_cfg.ca_cert_count(), Some(1), "ensure one CA is returned");
    assert!(!tls_cfg.insecure_skip_verify);

    // Two CAs passed.
    let two_cas = format!("{CERT_PEM}\n{CA_PEM}");
    let paths = new_test_x509_files(None, None, Some(two_cas.as_bytes()));
    let c = ClientConfig {
        ca_path: paths.ca.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let tls_cfg = c.get_tls_config().expect("two CAs");
    assert_eq!(tls_cfg.ca_cert_count(), Some(2), "ensure two CAs are returned");
    assert!(!tls_cfg.insecure_skip_verify);

    // Expect error on missing CA file.
    let c = ClientConfig {
        ca_path: format!("{}-not-existing", paths.ca.to_string_lossy()),
        ..Default::default()
    };
    let err = c.get_tls_config().unwrap_err().to_string();
    assert!(
        err.contains("error loading ca cert"),
        "unexpected error: {err}"
    );
}

// Upstream: github.com/grafana/dskit/crypto/tls/tls_test.go::TestGetTLSConfig_InsecureSkipVerify
#[test]
fn test_get_tls_config_insecure_skip_verify() {
    let c = ClientConfig {
        insecure_skip_verify: true,
        ..Default::default()
    };
    let tls_cfg = c.get_tls_config().expect("insecure_skip_verify");
    assert!(tls_cfg.insecure_skip_verify);

    // Perform an in-memory TLS handshake against an untrusted self-signed cert for a mismatched hostname.
    let paths = new_test_x509_files(Some(CERT_PEM.as_bytes()), Some(KEY_PEM.as_bytes()), None);
    let srv_cfg = ClientConfig {
        cert_path: paths.cert.to_string_lossy().into_owned(),
        key_path: paths.key.to_string_lossy().into_owned(),
        ..Default::default()
    }
    .get_client_and_server_tls_config()
    .unwrap();
    let mut client = rustls::ClientConnection::new(
        tls_cfg.config,
        ServerName::try_from("untrusted.example.com").unwrap(),
    )
    .unwrap();
    let mut server = rustls::ServerConnection::new(srv_cfg.server).unwrap();
    let mut buf = Vec::new();
    while client.is_handshaking() || server.is_handshaking() {
        buf.clear();
        let _ = client.write_tls(&mut buf);
        if !buf.is_empty() {
            server.read_tls(&mut &buf[..]).unwrap();
            server.process_new_packets().unwrap();
        }
        buf.clear();
        let _ = server.write_tls(&mut buf);
        if !buf.is_empty() {
            client.read_tls(&mut &buf[..]).unwrap();
            client.process_new_packets().unwrap();
        }
    }
}

// Upstream: github.com/grafana/dskit/crypto/tls/tls_test.go::TestGetTLSConfig_ServerName
#[test]
fn test_get_tls_config_server_name() {
    let c = ClientConfig {
        server_name: "myserver.com".to_string(),
        ..Default::default()
    };
    let tls_cfg = c.get_tls_config().expect("server_name");
    assert_eq!(
        tls_cfg.server_name,
        Some(ServerName::try_from("myserver.com").unwrap())
    );
}

// Upstream: github.com/grafana/dskit/crypto/tls/tls_test.go::TestGetTLSConfig_MinVersion
#[test]
fn test_get_tls_config_min_version() {
    let cases = [
        ("", true, false, false),
        ("VersionTLS10", false, true, false),
        ("VersionTLS11", false, true, false),
        ("VersionTLS12", true, false, false),
        ("VersionTLS13", true, false, false),
        ("VersionTLS14", false, false, true),
    ];
    for (min_version, ok, expect_rustls_unsupported, expect_unknown) in cases {
        let c = ClientConfig {
            min_version: min_version.to_string(),
            ..Default::default()
        };
        let res = c.get_tls_config();
        if ok {
            let resolved = res.unwrap_or_else(|e| panic!("{min_version}: {e}"));
            let debug_str = format!("{:?}", resolved.config);
            if min_version == "VersionTLS13" {
                assert!(debug_str.contains("TLS13"), "{min_version}: {debug_str}");
            }
        } else if expect_rustls_unsupported {
            assert!(
                matches!(res, Err(TlsConfigError::NotSupportedByRustls(_))),
                "{min_version}: expected NotSupportedByRustls, got {res:?}"
            );
        } else if expect_unknown {
            assert_eq!(
                res.unwrap_err(),
                TlsConfigError::UnknownMinVersion(min_version.to_string())
            );
        }
    }
}

// Upstream: github.com/grafana/dskit/crypto/tls/tls_test.go::TestGetTLSConfig_CipherSuites
#[test]
fn test_get_tls_config_cipher_suites() {
    // 1. No cipher suites set.
    let c = ClientConfig::default();
    assert!(c.get_tls_config().is_ok());

    // 2. All rustls-supported ECDHE AEAD TLS 1.2 + TLS 1.3 cipher suites.
    let supported = [
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
    ]
    .join(",");
    let c = ClientConfig {
        cipher_suites: supported,
        ..Default::default()
    };
    let resolved = c.get_tls_config().expect("supported cipher suites");
    let suite_names: Vec<String> = resolved
        .config
        .crypto_provider()
        .cipher_suites
        .iter()
        .map(|s| format!("{:?}", s.suite()))
        .collect();
    assert_eq!(
        suite_names,
        vec![
            "TLS13_AES_256_GCM_SHA384",
            "TLS13_AES_128_GCM_SHA256",
            "TLS13_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        ]
    );

    // 3. Go legacy / non-AEAD / RSA-kx suites rejected explicitly by rustls.
    for legacy in [
        "TLS_RSA_WITH_RC4_128_SHA",
        "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA",
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
        "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    ] {
        let c = ClientConfig {
            cipher_suites: legacy.to_string(),
            ..Default::default()
        };
        assert!(
            matches!(c.get_tls_config(), Err(TlsConfigError::NotSupportedByRustls(_))),
            "{legacy} should be rejected as NotSupportedByRustls"
        );
    }

    // 4. Unknown cipher suite.
    let c = ClientConfig {
        cipher_suites: "TLS_NO_SUITE".to_string(),
        ..Default::default()
    };
    assert_eq!(
        c.get_tls_config().unwrap_err(),
        TlsConfigError::UnsupportedCipherSuite("TLS_NO_SUITE".to_string())
    );
}
