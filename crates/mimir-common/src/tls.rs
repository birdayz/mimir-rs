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
//!   eagerly (`tls.X509KeyPair`) and cached via [`CachedReloadingCert`] with throttled metadata
//!   checks and last-known-good fallback across transient symlink swaps.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime};

use parking_lot::{Mutex, RwLock};

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
        self.get_tls_config_with_resolver().map(|(cfg, _)| cfg)
    }

    fn get_tls_config_with_resolver(
        &self,
    ) -> Result<(ResolvedClientConfig, Option<Arc<CachedReloadingCert>>), TlsConfigError> {
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
            let resolver = CachedReloadingCert::from_files(
                &self.cert_path,
                &self.key_path,
                // Key parsing does not depend on the configured cipher suites.
                Arc::new(rustls::crypto::ring::default_provider()),
            )
            .map_err(|e| match e {
                CertLoadError::ReadCert { path, reason } => {
                    TlsConfigError::LoadClientCert { path, reason }
                }
                CertLoadError::ReadKey { path, reason } => {
                    TlsConfigError::LoadClientKey { path, reason }
                }
                CertLoadError::KeyPair(reason) => TlsConfigError::KeyPair {
                    cert_path: self.cert_path.clone(),
                    key_path: self.key_path.clone(),
                    reason,
                },
            })?;
            Some(Arc::new(resolver))
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

        let config = match &client_cert {
            Some(resolver) => {
                let r: Arc<dyn ResolvesClientCert> = Arc::clone(resolver) as Arc<dyn ResolvesClientCert>;
                builder.with_client_cert_resolver(r)
            }
            None => builder.with_no_client_auth(),
        };

        Ok((
            ResolvedClientConfig {
                config: Arc::new(config),
                server_name,
            },
            client_cert,
        ))
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
    /// Server-side semantics for callers that use one config for both ends (e.g. memberlist):
    /// - When `tls_cert_path`/`tls_key_path` are configured, the key pair is eagerly validated at
    ///   startup and reloaded live via [`CachedReloadingCert`] so rotated leaf certificates take
    ///   effect without restarting the node; without a key pair, every server handshake fails
    ///   (Go: `tls: no certificates configured`).
    /// - `ClientAuth` is left at `tls.NoClientCert`: client certificates are never requested and
    ///   `tls_ca_path` / `tls_server_name` / `tls_insecure_skip_verify` only affect the client.
    /// - `tls_min_version` / `tls_cipher_suites` apply to both sides.
    pub fn get_client_and_server_tls_config(
        &self,
    ) -> Result<ResolvedClientServerConfig, TlsConfigError> {
        let (client, resolver) = self.get_tls_config_with_resolver()?;

        let versions = protocol_versions(&self.min_version)?;
        let provider = provider_for(self)?;
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(versions)
            .map_err(|e| TlsConfigError::Rustls(e.to_string()))?
            .with_no_client_auth();
        let server = match resolver {
            Some(r) => builder.with_cert_resolver(r as Arc<dyn rustls::server::ResolvesServerCert>),
            None => builder.with_cert_resolver(Arc::new(NoServerCert)),
        };

        Ok(ResolvedClientServerConfig {
            client,
            server: Arc::new(server),
        })
    }
}

/// Server cert resolver when no key pair is configured (every handshake is aborted like Go's
/// `tls: no certificates configured`).
#[derive(Debug)]
struct NoServerCert;

impl rustls::server::ResolvesServerCert for NoServerCert {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        None
    }
}

const DEFAULT_RELOAD_CHECK_INTERVAL_MS: u64 = 200;

fn monotonic_millis() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let start = EPOCH.get_or_init(Instant::now);
    start.elapsed().as_millis().min(u64::MAX as u128) as u64
}

/// Source of a PEM-encoded certificate chain or private key.
#[derive(Clone, Debug)]
pub enum PemSource {
    File(String),
    Inline(Vec<u8>),
}

impl PemSource {
    fn display_name(&self) -> &str {
        match self {
            Self::File(p) => p.as_str(),
            Self::Inline(_) => "<inline>",
        }
    }

    fn fingerprint(&self) -> Result<SourceFingerprint, (String, String)> {
        match self {
            Self::Inline(_) => Ok(SourceFingerprint::Inline),
            Self::File(path) => {
                let meta = std::fs::metadata(Path::new(path))
                    .map_err(|e| (path.clone(), go_read_err(path, &e)))?;
                let mtime = meta.modified().ok();
                #[cfg(unix)]
                let (ino, mtime_nsec, ctime, ctime_nsec) = {
                    use std::os::unix::fs::MetadataExt;
                    (meta.ino(), meta.mtime_nsec(), meta.ctime(), meta.ctime_nsec())
                };
                #[cfg(not(unix))]
                let (ino, mtime_nsec, ctime, ctime_nsec) = (0, 0, 0, 0);
                Ok(SourceFingerprint::File(FileMeta {
                    mtime,
                    mtime_nsec,
                    ctime,
                    ctime_nsec,
                    ino,
                    len: meta.len(),
                }))
            }
        }
    }

    fn read_bytes(&self) -> Result<Vec<u8>, (String, String)> {
        match self {
            Self::Inline(b) => Ok(b.clone()),
            Self::File(path) => read_secret(path).map_err(|reason| (path.clone(), reason)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileMeta {
    mtime: Option<SystemTime>,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
    ino: u64,
    len: u64,
}

impl SourceFingerprint {
    fn within_coarse_clock_window(&self, now: SystemTime) -> bool {
        match self {
            Self::Inline => false,
            Self::File(meta) => meta
                .mtime
                .is_some_and(|mt| now.duration_since(mt).ok().is_some_and(|d| d < std::time::Duration::from_secs(2))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceFingerprint {
    File(FileMeta),
    Inline,
}

/// Structured error from eager or reloaded certificate/key loading.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CertLoadError {
    #[error("error loading cert {path}: {reason}")]
    ReadCert { path: String, reason: String },
    #[error("error loading key {path}: {reason}")]
    ReadKey { path: String, reason: String },
    #[error("{0}")]
    KeyPair(String),
}

impl CertLoadError {
    pub fn into_reason(self) -> String {
        match self {
            Self::ReadCert { reason, .. } | Self::ReadKey { reason, .. } | Self::KeyPair(reason) => {
                reason
            }
        }
    }
}

fn load_certified_key_with_hashes(
    cert_source: &PemSource,
    key_source: &PemSource,
    provider: &CryptoProvider,
) -> Result<(CertifiedKey, (u64, u64)), CertLoadError> {
    let cert_pem = cert_source
        .read_bytes()
        .map_err(|(path, reason)| CertLoadError::ReadCert { path, reason })?;
    let key_pem = key_source
        .read_bytes()
        .map_err(|(path, reason)| CertLoadError::ReadKey { path, reason })?;
    let hashes = (
        xxhash_rust::xxh3::xxh3_64(&cert_pem),
        xxhash_rust::xxh3::xxh3_64(&key_pem),
    );
    let ck = x509_key_pair(&cert_pem, &key_pem, provider).map_err(CertLoadError::KeyPair)?;
    Ok((ck, hashes))
}

struct CachedCertState {
    fingerprint: (SourceFingerprint, SourceFingerprint),
    content_hashes: (u64, u64),
    key: Arc<CertifiedKey>,
}

/// Thread-safe cached certificate+key reloader implementing both
/// [`rustls::server::ResolvesServerCert`] and [`rustls::client::ResolvesClientCert`].
///
/// - Validates and loads the initial [`CertifiedKey`] eagerly at construction time.
/// - Throttles `std::fs::metadata` checks to at most once every 200ms via monotonic millis so
///   handshake storms perform zero file syscalls and zero PEM/key parses.
/// - Stats `(mtime, ino, len)` and only re-reads/re-parses when metadata changes.
/// - On transient reload failures (e.g. Kubernetes `..data` symlink-swap cert/key mismatch or
///   `NotFound`), logs a warning without updating the cached fingerprint and returns the
///   last-known-good [`CertifiedKey`] so live handshakes are never dropped.
pub struct CachedReloadingCert {
    cert_source: PemSource,
    key_source: PemSource,
    provider: Arc<CryptoProvider>,
    check_interval_ms: u64,
    last_check_ms: AtomicU64,
    reload_lock: Mutex<()>,
    state: RwLock<CachedCertState>,
}

impl fmt::Debug for CachedReloadingCert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedReloadingCert")
            .field("cert", &self.cert_source.display_name())
            .field("key", &self.key_source.display_name())
            .finish_non_exhaustive()
    }
}

impl CachedReloadingCert {
    /// Builds a cached reloader from certificate and key file paths, validating eagerly.
    pub fn from_files(
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
        provider: Arc<CryptoProvider>,
    ) -> Result<Self, CertLoadError> {
        Self::new(
            PemSource::File(cert_path.into()),
            PemSource::File(key_path.into()),
            provider,
        )
    }

    /// Builds a cached reloader from file or inline PEM sources, validating eagerly.
    pub fn new(
        cert_source: PemSource,
        key_source: PemSource,
        provider: Arc<CryptoProvider>,
    ) -> Result<Self, CertLoadError> {
        let cert_fp = cert_source
            .fingerprint()
            .map_err(|(path, reason)| CertLoadError::ReadCert { path, reason })?;
        let key_fp = key_source
            .fingerprint()
            .map_err(|(path, reason)| CertLoadError::ReadKey { path, reason })?;
        let (ck, content_hashes) =
            load_certified_key_with_hashes(&cert_source, &key_source, &provider)?;
        Ok(Self {
            cert_source,
            key_source,
            provider,
            check_interval_ms: DEFAULT_RELOAD_CHECK_INTERVAL_MS,
            last_check_ms: AtomicU64::new(monotonic_millis()),
            reload_lock: Mutex::new(()),
            state: RwLock::new(CachedCertState {
                fingerprint: (cert_fp, key_fp),
                content_hashes,
                key: Arc::new(ck),
            }),
        })
    }

    /// Returns the active [`CertifiedKey`], reloading if the throttle window elapsed and file
    /// metadata changed, or returning the last-known-good key on transient errors.
    pub fn resolve_cert(&self) -> Arc<CertifiedKey> {
        if matches!(
            (&self.cert_source, &self.key_source),
            (PemSource::Inline(_), PemSource::Inline(_))
        ) {
            return Arc::clone(&self.state.read().key);
        }

        let now = monotonic_millis();
        let last = self.last_check_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.check_interval_ms {
            return Arc::clone(&self.state.read().key);
        }
        if self
            .last_check_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Arc::clone(&self.state.read().key);
        }

        let Some(_reload_guard) = self.reload_lock.try_lock() else {
            return Arc::clone(&self.state.read().key);
        };

        let cert_fp = match self.cert_source.fingerprint() {
            Ok(fp) => fp,
            Err((path, reason)) => {
                tracing::warn!(
                    cert = %path,
                    key = %self.key_source.display_name(),
                    err = %reason,
                    "failed to stat TLS certificate during reload check; keeping last-known-good certificate"
                );
                return Arc::clone(&self.state.read().key);
            }
        };
        let key_fp = match self.key_source.fingerprint() {
            Ok(fp) => fp,
            Err((path, reason)) => {
                tracing::warn!(
                    cert = %self.cert_source.display_name(),
                    key = %path,
                    err = %reason,
                    "failed to stat TLS key during reload check; keeping last-known-good certificate"
                );
                return Arc::clone(&self.state.read().key);
            }
        };
        let next_fp = (cert_fp, key_fp);

        let (cached_hashes, fp_matches) = {
            let guard = self.state.read();
            if guard.fingerprint == next_fp {
                let sys_now = SystemTime::now();
                let in_coarse_window = next_fp.0.within_coarse_clock_window(sys_now)
                    || next_fp.1.within_coarse_clock_window(sys_now);
                if !in_coarse_window {
                    return Arc::clone(&guard.key);
                }
                (guard.content_hashes, true)
            } else {
                (guard.content_hashes, false)
            }
        };

        if fp_matches {
            if let (Ok(cb), Ok(kb)) = (self.cert_source.read_bytes(), self.key_source.read_bytes()) {
                let h = (
                    xxhash_rust::xxh3::xxh3_64(&cb),
                    xxhash_rust::xxh3::xxh3_64(&kb),
                );
                if h == cached_hashes {
                    return Arc::clone(&self.state.read().key);
                }
            }
        }

        match load_certified_key_with_hashes(&self.cert_source, &self.key_source, &self.provider) {
            Ok((ck, content_hashes)) => {
                let new_key = Arc::new(ck);
                let mut guard = self.state.write();
                guard.fingerprint = next_fp;
                guard.content_hashes = content_hashes;
                guard.key = Arc::clone(&new_key);
                tracing::info!(
                    cert = %self.cert_source.display_name(),
                    key = %self.key_source.display_name(),
                    "reloaded TLS certificate and key"
                );
                new_key
            }
            Err(err) => {
                tracing::warn!(
                    cert = %self.cert_source.display_name(),
                    key = %self.key_source.display_name(),
                    err = %err,
                    "failed to reload TLS certificate/key (possible mid-swap); keeping last-known-good certificate"
                );
                Arc::clone(&self.state.read().key)
            }
        }
    }
}

impl rustls::server::ResolvesServerCert for CachedReloadingCert {
    fn resolve(
        &self,
        _client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.resolve_cert())
    }
}

impl ResolvesClientCert for CachedReloadingCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.resolve_cert())
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

    #[test]
    fn cached_reloading_cert_throttles_and_survives_torn_swaps() {
        use rustls::client::ResolvesClientCert;
        use rustls::server::ResolvesServerCert;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("tls.crt");
        let key_path = dir.path().join("tls.key");
        std::fs::copy(fixture("server.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("server.key"), &key_path).unwrap();

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let reloader = CachedReloadingCert::from_files(
            cert_path.to_string_lossy().into_owned(),
            key_path.to_string_lossy().into_owned(),
            provider,
        )
        .unwrap();

        let k1 = ResolvesClientCert::resolve(&reloader, &[], &[]).unwrap();
        let k1_bytes = k1.cert[0].as_ref().to_vec();

        // Within the 200ms throttle window, even removing the files returns the cached key with
        // zero syscalls.
        std::fs::remove_file(&cert_path).unwrap();
        let k_throttled = reloader.resolve_cert();
        assert!(Arc::ptr_eq(&k1, &k_throttled));

        // Wait past 200ms while cert is missing -> logs warn and falls back to last-known-good.
        std::thread::sleep(Duration::from_millis(230));
        let k_missing = reloader.resolve_cert();
        assert!(Arc::ptr_eq(&k1, &k_missing));

        // Restore original files (new inode, same contents or changed mtime) -> reloads cleanly,
        // wait: write cert #2 while key #1 is still present (torn Kubernetes ..data swap!).
        std::fs::copy(fixture("client.crt"), &cert_path).unwrap();
        std::thread::sleep(Duration::from_millis(230));
        let k_torn = reloader.resolve_cert();
        assert!(
            Arc::ptr_eq(&k1, &k_torn),
            "torn cert/key swap must keep last-known-good keypair"
        );

        // Complete the swap with matching key #2, wait past 200ms -> fingerprint was NOT updated on
        // the torn error so this check reloads cert #2!
        std::fs::copy(fixture("client.key"), &key_path).unwrap();
        std::thread::sleep(Duration::from_millis(230));
        let k2 = ResolvesClientCert::resolve(&reloader, &[], &[]).unwrap();
        assert!(!Arc::ptr_eq(&k1, &k2));
        assert_ne!(k2.cert[0].as_ref(), k1_bytes.as_slice());

        // Subsequent check after 200ms when metadata is unchanged keeps the same Arc without
        // re-reading or re-parsing.
        std::thread::sleep(Duration::from_millis(230));
        let k2_again = reloader.resolve_cert();
        assert!(Arc::ptr_eq(&k2, &k2_again));

        // Verify ResolvesServerCert trait impl returns the same updated keypair (trait check).
        let _: &dyn ResolvesServerCert = &reloader;
    }

    #[test]
    fn client_and_server_tls_config_reloads_server_cert_live() {
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("node.crt");
        let key_path = dir.path().join("node.key");
        std::fs::copy(fixture("server.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("server.key"), &key_path).unwrap();

        let mut c = cfg();
        c.cert_path = cert_path.to_string_lossy().into_owned();
        c.key_path = key_path.to_string_lossy().into_owned();
        let resolved = c.get_client_and_server_tls_config().unwrap();

        let initial_bytes = resolved
            .client
            .config
            .client_auth_cert_resolver
            .resolve(&[], &[])
            .unwrap()
            .cert[0]
            .as_ref()
            .to_vec();
        std::fs::copy(fixture("client.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("client.key"), &key_path).unwrap();
        std::thread::sleep(Duration::from_millis(230));
        let rotated = resolved
            .client
            .config
            .client_auth_cert_resolver
            .resolve(&[], &[])
            .unwrap();
        assert_ne!(rotated.cert[0].as_ref(), initial_bytes.as_slice());
    }

    #[test]
    fn cached_reloading_cert_detects_same_inode_overwrite_within_coarse_tick() {
        use rustls::client::ResolvesClientCert;
        use std::io::Write;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("ec.crt");
        let key_path = dir.path().join("ec.key");

        let cert1 = std::fs::read(fixture("client-ec.crt")).unwrap();
        let key1 = std::fs::read(fixture("client-ec.key")).unwrap();
        let cert2 = std::fs::read(fixture("server.crt")).unwrap();
        let key2 = std::fs::read(fixture("server.key")).unwrap();

        std::fs::write(&cert_path, &cert1).unwrap();
        std::fs::write(&key_path, &key1).unwrap();

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let reloader = CachedReloadingCert::from_files(
            cert_path.to_string_lossy().into_owned(),
            key_path.to_string_lossy().into_owned(),
            provider,
        )
        .unwrap();
        let k1 = ResolvesClientCert::resolve(&reloader, &[], &[]).unwrap();
        let k1_bytes = k1.cert[0].as_ref().to_vec();

        // Overwrite in-place via O_TRUNC (preserving inode!) immediately after construction.
        for (path, bytes) in [(&cert_path, &cert2), (&key_path, &key2)] {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)
                .unwrap();
            f.write_all(bytes).unwrap();
            f.flush().unwrap();
        }

        std::thread::sleep(Duration::from_millis(230));
        let k2 = ResolvesClientCert::resolve(&reloader, &[], &[]).unwrap();
        assert_ne!(k2.cert[0].as_ref(), k1_bytes.as_slice());
    }
}
