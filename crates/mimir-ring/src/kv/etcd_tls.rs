//! TLS client configuration of the etcd KV client.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/etcd/etcd.go (`Config.GetTLS`: TLS only with `tls_enabled`,
//!   errors wrapped as `unable to initialise TLS configuration for etcd`)
//! - go.etcd.io/etcd/client/pkg/v3/transport/listener.go (`TLSInfo.ClientConfig`, `baseConfig`,
//!   `cafiles`) and go.etcd.io/etcd/client/pkg/v3/tlsutil/tlsutil.go (`NewCert`, `NewCertPool`)
//!
//! Go semantics preserved (they differ from dskit `ClientConfig.GetTLSConfig`, which the etcd
//! client does not use):
//! - `tls_cert_path` / `tls_key_path`: both or neither (`KeyFile and CertFile must both be
//!   present[key: K, cert: C]`); the pair is loaded at startup (`os.ReadFile` / `tls.X509KeyPair`
//!   errors verbatim) and re-read on every handshake (`GetClientCertificate`).
//! - `tls_ca_path`: only the certificates of that file are trusted (`RootCAs`); empty: the host's
//!   root CAs. A file without certificates trusts nothing.
//! - `tls_server_name` has no effect, exactly like Go: `baseConfig` copies it into the
//!   `tls.Config`, but clientv3's resolver gives every address its endpoint host as `ServerName`
//!   and grpc-go's `tlsCreds.ClientHandshake` overwrites `cfg.ServerName` with that authority, so
//!   the certificate is always verified against (and SNI is the host of) the dialed endpoint,
//!   which is what this HTTP client does too (verified with go-mimir: an IP endpoint with a
//!   matching `tls_server_name` fails with `cannot validate certificate for 127.0.0.1 because it
//!   doesn't contain any IP SANs`).
//! - `tls_insecure_skip_verify`: any certificate and name are accepted (handshake signatures are
//!   still verified).
//! - `tls_cipher_suites` / `tls_min_version` are ignored, exactly like Go: `GetTLS` never copies
//!   them into the `transport.TLSInfo`, so the client always uses TLS >= 1.2 with the default
//!   suites.

use std::sync::Arc;

use mimir_common::tls::{go_read_err, system_roots, CachedReloadingCert};
use mimir_config::ingester::EtcdConfig;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use tokio_rustls::rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{
    CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

/// dskit `etcd.New`: `errors.Wrapf(err, "unable to initialise TLS configuration for etcd")`.
pub const TLS_INIT_ERROR_PREFIX: &str = "unable to initialise TLS configuration for etcd";

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}



/// etcd `TLSInfo.ClientConfig` for the etcd KV `Config` (only called with `tls_enabled`).
/// Errors are the Go texts, without the dskit wrapping.
pub fn client_config(cfg: &EtcdConfig) -> Result<rustls::ClientConfig, String> {
    let provider = provider();

    // `baseConfig` (only when a certificate or key is configured).
    let client_cert = if cfg.tls_cert_path.is_empty() && cfg.tls_key_path.is_empty() {
        None
    } else {
        if cfg.tls_key_path.is_empty() || cfg.tls_cert_path.is_empty() {
            return Err(format!(
                "KeyFile and CertFile must both be present[key: {}, cert: {}]",
                cfg.tls_key_path, cfg.tls_cert_path
            ));
        }
        let resolver = CachedReloadingCert::from_files(
            &cfg.tls_cert_path,
            &cfg.tls_key_path,
            Arc::clone(&provider),
        )
        .map_err(|e| e.into_reason())?;
        Some(Arc::new(resolver) as Arc<dyn ResolvesClientCert>)
    };

    // `cafiles` + `tlsutil.NewCertPool`.
    let roots = if cfg.tls_ca_path.is_empty() {
        None
    } else {
        let pem = std::fs::read(&cfg.tls_ca_path).map_err(|e| go_read_err(&cfg.tls_ca_path, &e))?;
        let mut store = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(&pem) {
            let cert = cert.map_err(|e| format!("x509: malformed certificate: {e}"))?;
            store
                .add(cert)
                .map_err(|e| format!("x509: malformed certificate: {e}"))?;
        }
        Some(Arc::new(store))
    };

    let verifier = if cfg.tls_insecure_skip_verify {
        EtcdVerifier {
            inner: None,
            reject_all: false,
            algorithms: provider.signature_verification_algorithms,
        }
    } else {
        let roots = roots.unwrap_or_else(system_roots);
        if roots.is_empty() {
            // Go: an empty pool rejects every certificate ("certificate signed by unknown
            // authority"); webpki cannot build a verifier without trust anchors.
            EtcdVerifier {
                inner: None,
                reject_all: true,
                algorithms: provider.signature_verification_algorithms,
            }
        } else {
            let inner = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
                .build()
                .map_err(|e| format!("building TLS verifier: {e}"))?;
            EtcdVerifier {
                inner: Some(inner),
                reject_all: false,
                algorithms: provider.signature_verification_algorithms,
            }
        }
    };

    // Go `baseConfig`: `MinVersion: tls.VersionTLS12` (also Go's client default without one).
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| format!("building TLS config: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));
    Ok(match client_cert {
        Some(resolver) => builder.with_client_cert_resolver(resolver),
        None => builder.with_no_client_auth(),
    })
}

/// TLS for `https://` endpoints when `tls_enabled` is false: etcd clientv3 then dials with
/// `credentials.NewTransportCredential(nil)` (host roots, no client certificate).
pub fn default_client_config() -> Result<rustls::ClientConfig, String> {
    client_config(&EtcdConfig {
        tls_cert_path: String::new(),
        tls_key_path: String::new(),
        tls_ca_path: String::new(),
        tls_server_name: String::new(),
        tls_insecure_skip_verify: false,
        ..EtcdConfig::default()
    })
}



/// Server certificate verification with Go's `InsecureSkipVerify` and empty-pool semantics.
#[derive(Debug)]
struct EtcdVerifier {
    inner: Option<Arc<WebPkiServerVerifier>>,
    reject_all: bool,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for EtcdVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.reject_all {
            return Err(rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer));
        }
        match &self.inner {
            None => Ok(ServerCertVerified::assertion()),
            Some(inner) => {
                inner.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        format!(
            "{}/../mimir-storage/tests/fixtures/kafka_tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    fn cfg() -> EtcdConfig {
        EtcdConfig {
            tls_enabled: true,
            ..EtcdConfig::default()
        }
    }

    fn err(c: &EtcdConfig) -> String {
        client_config(c).expect_err("must fail")
    }

    /// Error texts of etcd `TLSInfo.ClientConfig` (checked against go-mimir, see
    /// w8_kv_etcd_tls_testcontainers).
    #[test]
    fn go_error_texts() {
        let c = EtcdConfig {
            tls_cert_path: "/nonexistent/cert.pem".into(),
            ..cfg()
        };
        assert_eq!(err(&c), "KeyFile and CertFile must both be present[key: , cert: /nonexistent/cert.pem]");
        let c = EtcdConfig {
            tls_key_path: "/nonexistent/key.pem".into(),
            ..cfg()
        };
        assert_eq!(err(&c), "KeyFile and CertFile must both be present[key: /nonexistent/key.pem, cert: ]");
        let c = EtcdConfig {
            tls_cert_path: "/nonexistent/cert.pem".into(),
            tls_key_path: "/nonexistent/key.pem".into(),
            ..cfg()
        };
        assert_eq!(err(&c), "open /nonexistent/cert.pem: no such file or directory");
        let c = EtcdConfig {
            tls_cert_path: fixture("client.crt"),
            tls_key_path: fixture("ca.crt"),
            ..cfg()
        };
        assert_eq!(
            err(&c),
            "tls: found a certificate rather than a key in the PEM for the private key"
        );
        let c = EtcdConfig {
            tls_ca_path: "/nonexistent/ca.pem".into(),
            ..cfg()
        };
        assert_eq!(err(&c), "open /nonexistent/ca.pem: no such file or directory");
    }

    #[test]
    fn valid_configs_build() {
        let c = EtcdConfig {
            tls_cert_path: fixture("client.crt"),
            tls_key_path: fixture("client.key"),
            tls_ca_path: fixture("ca.crt"),
            // Without effect like in Go (see the module docs).
            tls_server_name: "not a valid name!".into(),
            tls_min_version: "VersionTLS10".into(),
            tls_cipher_suites: "TLS_RSA_WITH_RC4_128_SHA".into(),
            ..cfg()
        };
        let tls = client_config(&c).unwrap();
        assert!(tls.client_auth_cert_resolver.has_certs());
        let c = EtcdConfig {
            tls_insecure_skip_verify: true,
            ..cfg()
        };
        assert!(!client_config(&c).unwrap().client_auth_cert_resolver.has_certs());
        default_client_config().unwrap();
    }
}
