//! Client-side TLS configuration (rustls translation of Go `crypto/tls` client settings).
//!
//! Ported from github.com/grafana/dskit/crypto/tls/tls.go (`ClientConfig`, `GetTLSConfig`).
//!
//! The resolved [`rustls::ClientConfig`] is built with an explicit `ring` [`CryptoProvider`]
//! (`ClientConfig::builder_with_provider`), so no process-wide default provider is ever required.
//!
//! Go semantics preserved:
//! - `tls_ca_path` set: ONLY the certificates of that PEM file are trusted (`RootCAs` = pool from
//!   file; unparsable blocks are skipped like `x509.CertPool.AppendCertsFromPEM`). Empty: the
//!   host's root CA certificates (`SSL_CERT_FILE` / `SSL_CERT_DIR` honoured, like Go on Linux).
//! - `tls_cert_path` / `tls_key_path`: both or neither; the key pair is loaded and validated
//!   eagerly (`tls.X509KeyPair`) and re-read from disk on every handshake that asks for a client
//!   certificate (`GetClientCertificate`), so rotated certificates are picked up.
//! - `tls_server_name`: overrides the name verified on the server certificate (and sent as SNI).
//!   IP addresses are verified against IP SANs.
//! - `tls_insecure_skip_verify`: any server certificate and name is accepted; handshake
//!   signatures are still verified (as Go does).
//! - `tls_min_version` / `tls_cipher_suites`: dskit names. Values Go accepts but rustls cannot
//!   honour (TLS 1.0 / 1.1, non-AEAD or non-ECDHE TLS 1.2 suites) are rejected with an explicit
//!   error instead of being silently ignored. As in Go, the cipher suite list only restricts
//!   TLS 1.0-1.2 suites; TLS 1.3 suites are always enabled.

use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{
    CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme, SupportedCipherSuite,
    SupportedProtocolVersion,
};
use thiserror::Error;

/// dskit `ClientConfig` (YAML keys `tls_cert_path`, `tls_key_path`, `tls_ca_path`,
/// `tls_server_name`, `tls_insecure_skip_verify`, `tls_cipher_suites`, `tls_min_version`).
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ClientConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_path: String,
    pub server_name: String,
    pub insecure_skip_verify: bool,
    pub cipher_suites: String,
    pub min_version: String,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field("ca_path", &self.ca_path)
            .field("server_name", &self.server_name)
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .field("cipher_suites", &self.cipher_suites)
            .field("min_version", &self.min_version)
            .finish()
    }
}

/// Errors of [`ClientConfig::get_tls_config`]. Messages follow dskit / Go `crypto/tls` wording.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TlsConfigError {
    /// dskit `errKeyMissing`.
    #[error("certificate given but no key configured")]
    KeyMissing,
    /// dskit `errCertMissing`.
    #[error("key given but no certificate configured")]
    CertMissing,
    #[error("error loading ca cert: {path}: {reason}")]
    LoadCa { path: String, reason: String },
    #[error("error loading client cert: {path}: {reason}")]
    LoadClientCert { path: String, reason: String },
    #[error("error loading client key: {path}: {reason}")]
    LoadClientKey { path: String, reason: String },
    #[error("failed to load TLS certificate {cert_path},{key_path}: {reason}")]
    KeyPair {
        cert_path: String,
        key_path: String,
        reason: String,
    },
    #[error("unknown minimum TLS version: {0:?}")]
    UnknownMinVersion(String),
    #[error("unsupported cipher suite: {0:?}")]
    UnsupportedCipherSuite(String),
    /// A value Go accepts but rustls cannot honour; rejected explicitly instead of ignored.
    #[error("{0}")]
    NotSupportedByRustls(String),
    #[error("invalid TLS server name {name:?}: {reason}")]
    InvalidServerName { name: String, reason: String },
    #[error("building TLS client config: {0}")]
    Rustls(String),
}

/// Resolved client TLS settings: the shared rustls config (build once, share via `Arc`) and the
/// optional `tls_server_name` override. When `server_name` is `None` the caller derives the name
/// from the dialed address (franz-go `kgo` sets `ServerName` to the host of `host:port`).
#[derive(Clone)]
pub struct ResolvedClientConfig {
    pub config: Arc<rustls::ClientConfig>,
    pub server_name: Option<ServerName<'static>>,
}

impl fmt::Debug for ResolvedClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedClientConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// Go `tls.CipherSuites()` / `tls.InsecureCipherSuites()` names known to dskit.
const GO_TLS13_SUITES: &[&str] = &[
    "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
];

const GO_TLS12_SUITES_NOT_IN_RUSTLS: &[&str] = &[
    // tls.CipherSuites()
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
    // tls.InsecureCipherSuites()
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

fn rustls_tls12_suite(name: &str) -> Option<SupportedCipherSuite> {
    use rustls::crypto::ring::cipher_suite as cs;
    Some(match name {
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => cs::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => cs::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => cs::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => cs::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => {
            cs::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
        }
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => {
            cs::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
        }
        _ => return None,
    })
}

/// Maps dskit `tls_min_version` to the rustls protocol versions to enable (max is always TLS 1.3,
/// Go's default). Empty means Go's client default minimum, TLS 1.2.
fn protocol_versions(
    min_version: &str,
) -> Result<&'static [&'static SupportedProtocolVersion], TlsConfigError> {
    static TLS12_AND_13: &[&SupportedProtocolVersion] =
        &[&rustls::version::TLS13, &rustls::version::TLS12];
    static TLS13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
    match min_version {
        "" | "VersionTLS12" => Ok(TLS12_AND_13),
        "VersionTLS13" => Ok(TLS13_ONLY),
        "VersionTLS10" | "VersionTLS11" => Err(TlsConfigError::NotSupportedByRustls(format!(
            "minimum TLS version {min_version:?} is not supported by mimir-rs: rustls implements only TLS 1.2 and TLS 1.3, use VersionTLS12 or VersionTLS13"
        ))),
        other => Err(TlsConfigError::UnknownMinVersion(other.to_string())),
    }
}

/// Maps dskit `tls_cipher_suites` (comma separated Go names, spaces ignored) to the rustls
/// cipher suite list. `None` keeps the provider defaults.
fn cipher_suites(names: &str) -> Result<Option<Vec<SupportedCipherSuite>>, TlsConfigError> {
    if names.is_empty() {
        return Ok(None);
    }
    let cleaned = names.replace(' ', "");
    let mut tls12 = Vec::new();
    for name in cleaned.split(',') {
        if GO_TLS13_SUITES.contains(&name) {
            // Go: "Note that TLS 1.3 ciphersuites are not configurable." Accepted, no effect.
            continue;
        }
        if let Some(suite) = rustls_tls12_suite(name) {
            tls12.push(suite);
            continue;
        }
        if GO_TLS12_SUITES_NOT_IN_RUSTLS.contains(&name) {
            return Err(TlsConfigError::NotSupportedByRustls(format!(
                "cipher suite {name:?} is not supported by mimir-rs: rustls implements only ECDHE AEAD (AES-GCM, ChaCha20-Poly1305) TLS 1.2 cipher suites"
            )));
        }
        return Err(TlsConfigError::UnsupportedCipherSuite(name.to_string()));
    }
    let mut out: Vec<SupportedCipherSuite> = rustls::crypto::ring::DEFAULT_CIPHER_SUITES
        .iter()
        .copied()
        .filter(|s| s.version() == &rustls::version::TLS13)
        .collect();
    out.extend(tls12);
    Ok(Some(out))
}

/// Formats an `os.ReadFile`-style error: `open <path>: <lowercase errno text>`.
pub fn go_read_err(path: &str, err: &std::io::Error) -> String {
    let mut msg = err.to_string();
    if let Some(idx) = msg.find(" (os error ") {
        msg.truncate(idx);
    }
    let mut chars = msg.chars();
    let msg = match chars.next() {
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
        None => msg,
    };
    if err.kind() == std::io::ErrorKind::IsADirectory {
        return format!("read {path}: {msg}");
    }
    format!("open {path}: {msg}")
}

fn read_secret(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(Path::new(path)).map_err(|e| go_read_err(path, &e))
}

/// Port of Go `tls.X509KeyPair` (PEM cert chain + PKCS#8 / PKCS#1 / SEC1 key, key must match).
pub fn x509_key_pair(
    cert_pem: &[u8],
    key_pem: &[u8],
    provider: &CryptoProvider,
) -> Result<CertifiedKey, String> {
    let mut certs = Vec::new();
    for item in CertificateDer::pem_slice_iter(cert_pem) {
        match item {
            Ok(c) => certs.push(c),
            Err(_) => break,
        }
    }
    if certs.is_empty() {
        if PrivateKeyDer::from_pem_slice(cert_pem).is_ok() {
            return Err("tls: failed to find certificate PEM data in certificate input, but did find a private key; PEM inputs may have been switched".to_string());
        }
        return Err("tls: failed to find any PEM data in certificate input".to_string());
    }
    let key = match PrivateKeyDer::from_pem_slice(key_pem) {
        Ok(k) => k,
        Err(_) => {
            if CertificateDer::pem_slice_iter(key_pem).any(|c| c.is_ok()) {
                return Err("tls: found a certificate rather than a key in the PEM for the private key".to_string());
            }
            return Err("tls: failed to find any PEM data in key input".to_string());
        }
    };
    CertifiedKey::from_der(certs, key, provider).map_err(|e| match e {
        rustls::Error::InconsistentKeys(_) => "tls: private key does not match public key".to_string(),
        other => format!("tls: failed to parse private key: {other}"),
    })
}

/// Process-wide cache of the host's root CA certificates (Go `x509.SystemCertPool` is also
/// loaded once per process).
pub fn system_roots() -> Arc<RootCertStore> {
    static ROOTS: OnceLock<Arc<RootCertStore>> = OnceLock::new();
    Arc::clone(ROOTS.get_or_init(|| {
        let loaded = rustls_native_certs::load_native_certs();
        for err in &loaded.errors {
            tracing::warn!(err = %err, "failed to load some system root CA certificates");
        }
        let mut store = RootCertStore::empty();
        let (_added, _ignored) = store.add_parsable_certificates(loaded.certs);
        Arc::new(store)
    }))
}

/// Builds the rustls `CryptoProvider` matching the configured cipher suites.
fn provider_for(cfg: &ClientConfig) -> Result<Arc<CryptoProvider>, TlsConfigError> {
    let mut provider = rustls::crypto::ring::default_provider();
    if let Some(suites) = cipher_suites(&cfg.cipher_suites)? {
        provider.cipher_suites = suites;
    }
    Ok(Arc::new(provider))
}

/// Parses `name` as a TLS server name (DNS name or IP address literal).
pub fn parse_server_name(name: &str) -> Result<ServerName<'static>, TlsConfigError> {
    if let Ok(ip) = name.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(name.to_string()).map_err(|e| TlsConfigError::InvalidServerName {
        name: name.to_string(),
        reason: e.to_string(),
    })
}

impl ClientConfig {
    /// Port of dskit `ClientConfig.GetTLSConfig`.
    pub fn get_tls_config(&self) -> Result<ResolvedClientConfig, TlsConfigError> {
        // Read CA certificates.
        let ca_roots = if self.ca_path.is_empty() {
            None
        } else {
            let pem = read_secret(&self.ca_path).map_err(|reason| TlsConfigError::LoadCa {
                path: self.ca_path.clone(),
                reason,
            })?;
            let mut store = RootCertStore::empty();
            let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
                .map_while(Result::ok)
                .collect();
            let (_added, _ignored) = store.add_parsable_certificates(certs);
            Some(Arc::new(store))
        };

        // Read client certificate (same precedence of errors as dskit).
        let client_cert = if self.cert_path.is_empty() && self.key_path.is_empty() {
            None
        } else {
            if self.cert_path.is_empty() {
                return Err(TlsConfigError::CertMissing);
            }
            if self.key_path.is_empty() {
                return Err(TlsConfigError::KeyMissing);
            }
            let resolver = ReloadingClientCert {
                cert_path: self.cert_path.clone(),
                key_path: self.key_path.clone(),
                // Key parsing does not depend on the configured cipher suites.
                provider: Arc::new(rustls::crypto::ring::default_provider()),
            };
            // Confirm that certificate and key paths are valid.
            resolver.load()?;
            Some(resolver)
        };

        let versions = protocol_versions(&self.min_version)?;
        let provider = provider_for(self)?;

        let server_name = if self.server_name.is_empty() {
            None
        } else {
            Some(parse_server_name(&self.server_name)?)
        };

        let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(versions)
            .map_err(|e| TlsConfigError::Rustls(e.to_string()))?;

        let builder = if self.insecure_skip_verify {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SignatureOnlyVerifier {
                    algorithms: provider.signature_verification_algorithms,
                    reject_all: false,
                }))
        } else {
            let roots = ca_roots.unwrap_or_else(system_roots);
            if roots.is_empty() {
                // Go: an empty pool rejects every server certificate ("certificate signed by
                // unknown authority"); webpki refuses to build a verifier without anchors.
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(SignatureOnlyVerifier {
                        algorithms: provider.signature_verification_algorithms,
                        reject_all: true,
                    }))
            } else {
                let verifier = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
                    .build()
                    .map_err(|e| TlsConfigError::Rustls(e.to_string()))?;
                builder.with_webpki_verifier(verifier)
            }
        };

        let config = match client_cert {
            Some(resolver) => builder.with_client_cert_resolver(Arc::new(resolver)),
            None => builder.with_no_client_auth(),
        };

        Ok(ResolvedClientConfig {
            config: Arc::new(config),
            server_name,
        })
    }
}

/// The single dskit `*tls.Config` resolved for both ends of a connection. dskit `kv/memberlist`
/// (`NewTCPTransport`) passes the one `GetTLSConfig` result to `tls.Listen` (server side) and to
/// `tls.DialWithDialer` (client side); rustls needs two distinct config objects for that.
#[derive(Clone)]
pub struct ResolvedClientServerConfig {
    /// Client side (`tls.DialWithDialer`), identical to [`ClientConfig::get_tls_config`].
    pub client: ResolvedClientConfig,
    /// Server side (`tls.Listen`) of the same Go `tls.Config`.
    pub server: Arc<rustls::ServerConfig>,
}

impl fmt::Debug for ResolvedClientServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedClientServerConfig")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl ClientConfig {
    /// Port of dskit `ClientConfig.GetTLSConfig` for callers that use the returned `tls.Config`
    /// also for server purposes (dskit `kv/memberlist` TCP transport). Errors are exactly those
    /// of [`ClientConfig::get_tls_config`].
    ///
    /// Go semantics of the server side of such a config:
    /// - `Certificates = []tls.Certificate{cert}`: the key pair loaded (once) at config time is
    ///   served to every client; without `tls_cert_path`/`tls_key_path` the server has no
    ///   certificate and every handshake fails (Go: `tls: no certificates configured`).
    /// - `ClientAuth` is left at `tls.NoClientCert`: client certificates are never requested and
    ///   `tls_ca_path` / `tls_server_name` / `tls_insecure_skip_verify` only affect the client.
    /// - `tls_min_version` / `tls_cipher_suites` apply to both sides.
    pub fn get_client_and_server_tls_config(
        &self,
    ) -> Result<ResolvedClientServerConfig, TlsConfigError> {
        let client = self.get_tls_config()?;

        let cert = if self.cert_path.is_empty() && self.key_path.is_empty() {
            None
        } else {
            // Both paths are set (validated by get_tls_config above).
            let loader = ReloadingClientCert {
                cert_path: self.cert_path.clone(),
                key_path: self.key_path.clone(),
                provider: Arc::new(rustls::crypto::ring::default_provider()),
            };
            Some(Arc::new(loader.load()?))
        };

        let versions = protocol_versions(&self.min_version)?;
        let provider = provider_for(self)?;
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(versions)
            .map_err(|e| TlsConfigError::Rustls(e.to_string()))?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(StaticServerCert(cert)));

        Ok(ResolvedClientServerConfig {
            client,
            server: Arc::new(server),
        })
    }
}

/// Go `tls.Config.Certificates` used by a server: one static key pair (or none, in which case
/// every handshake is aborted like Go's `tls: no certificates configured`).
#[derive(Debug)]
struct StaticServerCert(Option<Arc<CertifiedKey>>);

impl rustls::server::ResolvesServerCert for StaticServerCert {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.0.clone()
    }
}

/// Go `GetClientCertificate`: the key pair is re-read from disk on every handshake that requests
/// a client certificate.
struct ReloadingClientCert {
    cert_path: String,
    key_path: String,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for ReloadingClientCert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReloadingClientCert")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish_non_exhaustive()
    }
}

impl ReloadingClientCert {
    fn load(&self) -> Result<CertifiedKey, TlsConfigError> {
        let cert = read_secret(&self.cert_path).map_err(|reason| TlsConfigError::LoadClientCert {
            path: self.cert_path.clone(),
            reason,
        })?;
        let key = read_secret(&self.key_path).map_err(|reason| TlsConfigError::LoadClientKey {
            path: self.key_path.clone(),
            reason,
        })?;
        x509_key_pair(&cert, &key, &self.provider).map_err(|reason| TlsConfigError::KeyPair {
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            reason,
        })
    }
}

impl ResolvesClientCert for ReloadingClientCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        match self.load() {
            Ok(ck) => Some(Arc::new(ck)),
            Err(err) => {
                // Go fails the handshake with this error; rustls can only send no certificate,
                // which a server requiring client authentication rejects.
                tracing::error!(err = %err, "failed to load TLS client certificate for handshake");
                None
            }
        }
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// `InsecureSkipVerify` (accept any certificate and name, still verify handshake signatures), or
/// an empty trust pool (reject every certificate as signed by an unknown authority).
#[derive(Debug)]
struct SignatureOnlyVerifier {
    algorithms: WebPkiSupportedAlgorithms,
    reject_all: bool,
}

impl ServerCertVerifier for SignatureOnlyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.reject_all {
            return Err(rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer));
        }
        Ok(ServerCertVerified::assertion())
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

    fn cfg() -> ClientConfig {
        ClientConfig {
            ca_path: fixture("ca.crt"),
            ..Default::default()
        }
    }

    fn err(c: &ClientConfig) -> String {
        c.get_tls_config().unwrap_err().to_string()
    }

    #[test]
    fn default_config_uses_host_roots_and_no_server_name() {
        let r = ClientConfig::default().get_tls_config().unwrap();
        assert!(r.server_name.is_none());
        assert!(!r.config.client_auth_cert_resolver.has_certs());
        if Path::new("/etc/ssl/certs").exists() {
            assert!(!system_roots().is_empty(), "host root CAs must load");
        }
    }

    #[test]
    fn ca_file_errors_follow_dskit() {
        let c = ClientConfig {
            ca_path: "/does/not/exist.crt".into(),
            ..Default::default()
        };
        assert_eq!(
            err(&c),
            "error loading ca cert: /does/not/exist.crt: open /does/not/exist.crt: no such file or directory"
        );
        // A CA file without certificates is accepted (Go AppendCertsFromPEM ignores garbage);
        // every server certificate is then rejected at handshake time.
        let c = ClientConfig {
            ca_path: fixture("README.md"),
            ..Default::default()
        };
        assert!(c.get_tls_config().is_ok());
    }

    #[test]
    fn client_cert_and_key_must_be_both_set() {
        let mut c = cfg();
        c.cert_path = fixture("client.crt");
        assert_eq!(err(&c), "certificate given but no key configured");
        let mut c = cfg();
        c.key_path = fixture("client.key");
        assert_eq!(err(&c), "key given but no certificate configured");
    }

    #[test]
    fn client_key_pair_loading() {
        for (cert, key) in [
            ("client.crt", "client.key"),
            ("client.crt", "client.pkcs1.key"),
            ("client-ec.crt", "client-ec.key"),
            ("client-ec.crt", "client-ec.sec1.key"),
        ] {
            let mut c = cfg();
            c.cert_path = fixture(cert);
            c.key_path = fixture(key);
            let r = c.get_tls_config().unwrap_or_else(|e| panic!("{cert}+{key}: {e}"));
            assert!(r.config.client_auth_cert_resolver.has_certs());
        }

        let mut c = cfg();
        c.cert_path = fixture("missing.crt");
        c.key_path = fixture("client.key");
        assert!(err(&c).starts_with(&format!(
            "error loading client cert: {}: open {}",
            c.cert_path, c.cert_path
        )));

        let mut c = cfg();
        c.cert_path = fixture("client.crt");
        c.key_path = fixture("missing.key");
        assert!(err(&c).starts_with("error loading client key: "));

        let mut c = cfg();
        c.cert_path = fixture("client.key");
        c.key_path = fixture("client.crt");
        assert_eq!(
            err(&c),
            format!(
                "failed to load TLS certificate {},{}: tls: failed to find certificate PEM data in certificate input, but did find a private key; PEM inputs may have been switched",
                c.cert_path, c.key_path
            )
        );

        let mut c = cfg();
        c.cert_path = fixture("client.crt");
        c.key_path = fixture("server.key");
        assert!(err(&c).ends_with("tls: private key does not match public key"));

        let mut c = cfg();
        c.cert_path = fixture("client.crt");
        c.key_path = fixture("ca.crt");
        assert!(err(&c).ends_with(
            "tls: found a certificate rather than a key in the PEM for the private key"
        ));
    }

    #[test]
    fn min_version() {
        for v in ["", "VersionTLS12", "VersionTLS13"] {
            let mut c = cfg();
            c.min_version = v.into();
            c.get_tls_config().unwrap();
        }
        let mut c = cfg();
        c.min_version = "VersionTLS14".into();
        assert_eq!(err(&c), "unknown minimum TLS version: \"VersionTLS14\"");
        for v in ["VersionTLS10", "VersionTLS11"] {
            c.min_version = v.into();
            assert!(err(&c).contains("is not supported by mimir-rs"), "{v}");
        }
    }

    #[test]
    fn cipher_suites() {
        let mut c = cfg();
        c.cipher_suites =
            "TLS_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256".into();
        let r = c.get_tls_config().unwrap();
        let names: Vec<String> = r
            .config
            .crypto_provider()
            .cipher_suites
            .iter()
            .map(|s| format!("{:?}", s.suite()))
            .collect();
        assert_eq!(
            names,
            vec![
                "TLS13_AES_256_GCM_SHA384",
                "TLS13_AES_128_GCM_SHA256",
                "TLS13_CHACHA20_POLY1305_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            ]
        );

        c.cipher_suites = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,FOO".into();
        assert_eq!(err(&c), "unsupported cipher suite: \"FOO\"");
        c.cipher_suites = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,".into();
        assert_eq!(err(&c), "unsupported cipher suite: \"\"");
        c.cipher_suites = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA".into();
        assert!(err(&c).contains("is not supported by mimir-rs"));
        c.cipher_suites = "TLS_RSA_WITH_AES_128_GCM_SHA256".into();
        assert!(err(&c).contains("is not supported by mimir-rs"));
    }

    #[test]
    fn server_name_override() {
        let mut c = cfg();
        c.server_name = "kafka.example.com".into();
        assert_eq!(
            c.get_tls_config().unwrap().server_name,
            Some(ServerName::try_from("kafka.example.com").unwrap())
        );
        c.server_name = "10.0.0.1".into();
        assert!(matches!(
            c.get_tls_config().unwrap().server_name,
            Some(ServerName::IpAddress(_))
        ));
    }

    #[test]
    fn client_and_server_config() {
        let mut c = cfg();
        c.cert_path = fixture("server.crt");
        c.key_path = fixture("server.key");
        c.min_version = "VersionTLS13".into();
        let r = c.get_client_and_server_tls_config().unwrap();
        assert!(r.client.config.client_auth_cert_resolver.has_certs());

        // Errors are those of get_tls_config.
        let mut c = cfg();
        c.cert_path = fixture("missing.crt");
        c.key_path = fixture("server.key");
        assert_eq!(
            c.get_client_and_server_tls_config().unwrap_err(),
            c.get_tls_config().unwrap_err()
        );

        // No key pair: the server side still builds (handshakes fail, like Go).
        cfg().get_client_and_server_tls_config().unwrap();
    }

    #[test]
    fn insecure_skip_verify_without_ca() {
        let c = ClientConfig {
            insecure_skip_verify: true,
            ..Default::default()
        };
        c.get_tls_config().unwrap();
    }
}
