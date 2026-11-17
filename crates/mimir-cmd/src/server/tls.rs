//! Server-side TLS for the HTTP and gRPC servers.
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go (TLS setup: TLS enabled when a cert (path or
//!   inline) and a key (path or inline) are configured; errors wrapped as
//!   `error generating {http,grpc} tls config: ...`)
//! - github.com/grafana/dskit/server/tls_config.go (`stringToCipherSuites`,
//!   `stringToTLSVersion`, `stringToCurvePreferences`)
//! - github.com/prometheus/exporter-toolkit/web/tls_config.go (`ConfigToTLSConfig`: the key pair is
//!   validated eagerly and re-read from disk on every handshake (`GetCertificate`), client auth
//!   policies, client CA pool, "client CA's have been configured without a Client Auth Policy")
//!
//! rustls (ring provider) cannot honour some values Go accepts: TLS 1.0/1.1, CBC/RC4/3DES/RSA-kx
//! cipher suites, and the P-521 / post-quantum hybrid groups. Those are rejected at startup with
//! an explicit error instead of being silently ignored. As in Go, the cipher suite list only
//! restricts TLS 1.2 suites (TLS 1.3 suites are always enabled), and the minimum version
//! defaults to TLS 1.2 (Go's server default).

use std::fmt;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use mimir_config::server::TLSConfig;
use rustls::crypto::{CryptoProvider, SupportedKxGroup, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme, SupportedCipherSuite};

/// Server-wide TLS options (`-server.tls-cipher-suites`, `-server.tls-min-version`,
/// `-server.tls-curve-preferences`).
#[derive(Clone, Debug, Default)]
pub struct TlsOptions {
    pub cipher_suites: String,
    pub min_version: String,
    pub curve_preferences: String,
}

impl TlsOptions {
    /// dskit `stringToCipherSuites`, `stringToTLSVersion`, `stringToCurvePreferences`: parsed
    /// (and rejected) at server creation even when no listener uses TLS.
    pub fn validate(&self) -> Result<()> {
        cipher_suites(&self.cipher_suites)?;
        protocol_versions(&self.min_version)?;
        kx_groups(&self.curve_preferences)?;
        Ok(())
    }
}

/// Whether TLS is enabled for a listener (dskit: cert and key, each from path or inline).
pub fn tls_enabled(c: &TLSConfig) -> bool {
    (!c.cert_file.is_empty() || !c.cert.is_empty()) && (!c.key_file.is_empty() || !c.key.is_empty())
}

const GO_TLS13_SUITES: &[&str] = &[
    "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
];

/// Go `tls.CipherSuites()` + `tls.InsecureCipherSuites()` names rustls does not implement.
const GO_SUITES_NOT_IN_RUSTLS: &[&str] = &[
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
    "TLS_RSA_WITH_RC4_128_SHA",
    "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
    "TLS_RSA_WITH_AES_128_CBC_SHA",
    "TLS_RSA_WITH_AES_256_CBC_SHA",
    "TLS_RSA_WITH_AES_128_CBC_SHA256",
    "TLS_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
    "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
    "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
];

fn tls12_suite(name: &str) -> Option<SupportedCipherSuite> {
    use rustls::crypto::ring::cipher_suite as cs;
    Some(match name {
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => cs::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => cs::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => cs::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => cs::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => cs::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => cs::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
        _ => return None,
    })
}

fn cipher_suites(s: &str) -> Result<Option<Vec<SupportedCipherSuite>>> {
    if s.is_empty() {
        return Ok(None);
    }
    let mut tls12 = Vec::new();
    for name in s.split(',') {
        if let Some(suite) = tls12_suite(name) {
            tls12.push(suite);
        } else if GO_TLS13_SUITES.contains(&name) {
            // Accepted by Go but without effect (TLS 1.3 suites are not configurable).
        } else if GO_SUITES_NOT_IN_RUSTLS.contains(&name) {
            bail!("cipher suite {name:?} is not supported by mimir-rs (rustls implements only ECDHE AEAD TLS 1.2 suites)");
        } else {
            bail!("cipher suite {name:?} not recognized");
        }
    }
    let mut all: Vec<SupportedCipherSuite> = rustls::crypto::ring::DEFAULT_CIPHER_SUITES
        .iter()
        .copied()
        .filter(|s| matches!(s, SupportedCipherSuite::Tls13(_)))
        .collect();
    all.extend(tls12);
    Ok(Some(all))
}

fn protocol_versions(min: &str) -> Result<&'static [&'static rustls::SupportedProtocolVersion]> {
    static TLS12_AND_13: &[&rustls::SupportedProtocolVersion] =
        &[&rustls::version::TLS13, &rustls::version::TLS12];
    static TLS13_ONLY: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];
    match min {
        "" | "VersionTLS12" => Ok(TLS12_AND_13),
        "VersionTLS13" => Ok(TLS13_ONLY),
        "VersionTLS10" | "VersionTLS11" => bail!(
            "TLS version {min:?} is not supported by mimir-rs (rustls supports TLS 1.2 and 1.3 only)"
        ),
        other => bail!("TLS version {other:?} not recognized"),
    }
}

fn kx_groups(s: &str) -> Result<Option<Vec<&'static dyn SupportedKxGroup>>> {
    use rustls::crypto::ring::kx_group;
    if s.is_empty() {
        return Ok(None);
    }
    let mut out: Vec<&'static dyn SupportedKxGroup> = Vec::new();
    for name in s.split(',') {
        out.push(match name {
            "CurveP256" => kx_group::SECP256R1,
            "CurveP384" => kx_group::SECP384R1,
            "X25519" => kx_group::X25519,
            "CurveP521" | "X25519MLKEM768" | "SecP256r1MLKEM768" | "SecP384r1MLKEM1024" => bail!(
                "curve {name:?} is not supported by mimir-rs (supported: CurveP256, CurveP384, X25519)"
            ),
            other => bail!("curve {other:?} not recognized"),
        });
    }
    Ok(Some(out))
}

fn go_read_err(path: &str, err: &std::io::Error) -> String {
    match err.kind() {
        std::io::ErrorKind::NotFound => format!("open {path}: no such file or directory"),
        std::io::ErrorKind::PermissionDenied => format!("open {path}: permission denied"),
        _ => format!("open {path}: {err}"),
    }
}

/// Loads the key pair like exporter-toolkit's `loadCert` (file paths win over inline values).
fn load_cert(cfg: &TLSConfig, provider: &CryptoProvider) -> Result<Arc<CertifiedKey>> {
    let cert_data = if !cfg.cert_file.is_empty() {
        std::fs::read(&cfg.cert_file).map_err(|e| {
            anyhow!("failed to read cert_file ({}): {}", cfg.cert_file, go_read_err(&cfg.cert_file, &e))
        })?
    } else {
        cfg.cert.as_bytes().to_vec()
    };
    let key_data = if !cfg.key_file.is_empty() {
        std::fs::read(&cfg.key_file).map_err(|e| {
            anyhow!("failed to read key_file ({}): {}", cfg.key_file, go_read_err(&cfg.key_file, &e))
        })?
    } else {
        cfg.key.as_bytes().to_vec()
    };
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_data)
        .filter_map(|c| c.ok())
        .collect();
    if certs.is_empty() {
        bail!("failed to load X509KeyPair: tls: failed to find any PEM data in certificate input");
    }
    let key = PrivateKeyDer::from_pem_slice(&key_data).map_err(|_| {
        anyhow!("failed to load X509KeyPair: tls: failed to find any PEM data in key input")
    })?;
    let signing_key = provider
        .key_provider
        .load_private_key(key)
        .map_err(|e| anyhow!("failed to load X509KeyPair: {e}"))?;
    let ck = CertifiedKey::new(certs, signing_key);
    ck.keys_match()
        .map_err(|_| anyhow!("failed to load X509KeyPair: tls: private key does not match public key"))?;
    Ok(Arc::new(ck))
}

/// exporter-toolkit `GetCertificate`: re-reads the key pair on every handshake.
struct ReloadingCertResolver {
    cfg: TLSConfig,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for ReloadingCertResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReloadingCertResolver").finish_non_exhaustive()
    }
}

impl ResolvesServerCert for ReloadingCertResolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        load_cert(&self.cfg, &self.provider).ok()
    }
}

/// Go `tls.RequestClientCert` / `tls.RequireAnyClientCert`: a client certificate is requested
/// (optionally required) but never verified against a CA.
#[derive(Debug)]
struct AnyClientCert {
    mandatory: bool,
    algs: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for AnyClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        self.mandatory
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

fn client_ca_pool(cfg: &TLSConfig) -> Result<Option<RootCertStore>> {
    let pem = if !cfg.client_ca_file.is_empty() {
        Some(std::fs::read(&cfg.client_ca_file).map_err(|e| anyhow!(go_read_err(&cfg.client_ca_file, &e)))?)
    } else if !cfg.client_ca.is_empty() {
        Some(cfg.client_ca.as_bytes().to_vec())
    } else {
        None
    };
    let Some(pem) = pem else { return Ok(None) };
    let mut roots = RootCertStore::empty();
    // x509.CertPool.AppendCertsFromPEM skips unparsable blocks.
    for c in CertificateDer::pem_slice_iter(&pem).flatten() {
        let _ = roots.add(c);
    }
    Ok(Some(roots))
}

fn system_roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    for c in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(c);
    }
    roots
}

/// Builds the rustls server config for one listener (`alpn`: `["h2", "http/1.1"]` for HTTP like
/// Go's `http.Server`, `["h2"]` for gRPC like grpc-go's TLS credentials).
pub fn server_config(cfg: &TLSConfig, opts: &TlsOptions, alpn: &[&[u8]]) -> Result<Arc<rustls::ServerConfig>> {
    let mut provider = rustls::crypto::ring::default_provider();
    if let Some(suites) = cipher_suites(&opts.cipher_suites)? {
        provider.cipher_suites = suites;
    }
    let versions = protocol_versions(&opts.min_version)?;
    if let Some(groups) = kx_groups(&opts.curve_preferences)? {
        provider.kx_groups = groups;
    }
    let provider = Arc::new(provider);

    if cfg.cert_file.is_empty() && cfg.cert.is_empty() {
        bail!("missing one of cert or cert_file");
    }
    if cfg.key_file.is_empty() && cfg.key.is_empty() {
        bail!("missing one of key or key_file");
    }
    // Confirm that certificate and key paths are valid.
    load_cert(cfg, &provider)?;

    let ca = client_ca_pool(cfg)?;
    let algs = provider.signature_verification_algorithms;
    let verifier: Arc<dyn ClientCertVerifier> = match cfg.client_auth_type.as_str() {
        "" | "NoClientCert" => {
            if ca.is_some() {
                bail!("client CA's have been configured without a Client Auth Policy");
            }
            WebPkiClientVerifier::no_client_auth()
        }
        "RequestClientCert" => Arc::new(AnyClientCert { mandatory: false, algs }),
        "RequireAnyClientCert" | "RequireClientCert" => Arc::new(AnyClientCert { mandatory: true, algs }),
        mode @ ("VerifyClientCertIfGiven" | "RequireAndVerifyClientCert") => {
            // Go verifies against the system roots when no client CA is configured.
            let roots = Arc::new(ca.unwrap_or_else(system_roots));
            let mut b = WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(&provider));
            if mode == "VerifyClientCertIfGiven" {
                b = b.allow_unauthenticated();
            }
            b.build().map_err(|e| anyhow!("client CA pool: {e}"))?
        }
        other => bail!("invalid ClientAuth: {other}"),
    };

    let mut sc = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .map_err(|e| anyhow!("{e}"))?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(Arc::new(ReloadingCertResolver {
            cfg: cfg.clone(),
            provider,
        }));
    sc.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(sc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_parsing_errors_match_go() {
        assert_eq!(cipher_suites("FOO").unwrap_err().to_string(), "cipher suite \"FOO\" not recognized");
        assert!(cipher_suites("TLS_RSA_WITH_AES_128_CBC_SHA").unwrap_err().to_string().contains("not supported"));
        assert!(cipher_suites("TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256").unwrap().is_some());
        assert_eq!(protocol_versions("x").unwrap_err().to_string(), "TLS version \"x\" not recognized");
        assert!(protocol_versions("VersionTLS13").is_ok());
        assert_eq!(kx_groups("Foo").unwrap_err().to_string(), "curve \"Foo\" not recognized");
        assert!(kx_groups("X25519,CurveP256").unwrap().is_some());
    }

    #[test]
    fn tls_enabled_needs_cert_and_key() {
        let mut c = TLSConfig::default();
        assert!(!tls_enabled(&c));
        c.cert_file = "x".into();
        assert!(!tls_enabled(&c));
        c.key = "k".into();
        assert!(tls_enabled(&c));
    }

    #[test]
    fn missing_files_error_like_go() {
        let c = TLSConfig {
            cert_file: "/nonexistent/cert.pem".into(),
            key_file: "/nonexistent/key.pem".into(),
            ..Default::default()
        };
        let err = server_config(&c, &TlsOptions::default(), &[b"h2"]).unwrap_err().to_string();
        assert_eq!(
            err,
            "failed to read cert_file (/nonexistent/cert.pem): open /nonexistent/cert.pem: no such file or directory"
        );
    }
}
