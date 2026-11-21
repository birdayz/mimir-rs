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

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use mimir_common::tls::{go_read_err, CachedReloadingCert, CertLoadError, PemSource};
use mimir_config::server::TLSConfig;
use rustls::crypto::{SupportedKxGroup, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ResolvesServerCert, WebPkiClientVerifier};
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
    let mut first_unsupported: Option<&str> = None;
    for name in s.split(',') {
        match name {
            "CurveP256" => out.push(kx_group::SECP256R1),
            "CurveP384" => out.push(kx_group::SECP384R1),
            "X25519" => out.push(kx_group::X25519),
            "CurveP521" | "X25519MLKEM768" | "SecP256r1MLKEM768" | "SecP384r1MLKEM1024" => {
                // Go crypto/tls accepts PQ hybrid groups and P-521 and negotiates any
                // remaining supported fallback curves from the list; only fail if none
                // of the configured curves are supported by rustls/ring.
                first_unsupported.get_or_insert(name);
            }
            other => bail!("curve {other:?} not recognized"),
        }
    }
    if out.is_empty() {
        let name = first_unsupported.unwrap_or(s);
        bail!(
            "curve {name:?} is not supported by mimir-rs (supported: CurveP256, CurveP384, X25519)"
        );
    }
    Ok(Some(out))
}



/// Go `tls.RequestClientCert` / `tls.RequireAnyClientCert`: a client certificate is requested
/// (optionally required) but never verified against a CA, while still advertising any
/// configured `ClientCAs` distinguished names in TLS `CertificateRequest`.
#[derive(Debug)]
struct AnyClientCert {
    mandatory: bool,
    algs: WebPkiSupportedAlgorithms,
    hint_subjects: Vec<DistinguishedName>,
}

impl ClientCertVerifier for AnyClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        self.mandatory
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.hint_subjects
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

/// Go `x509.NewCertPool()` (empty CA pool) under `VerifyClientCertIfGiven` or
/// `RequireAndVerifyClientCert`: accepts unauthenticated clients when `!mandatory`, and
/// rejects any presented client certificate at handshake time.
#[derive(Debug)]
struct EmptyPoolClientCertVerifier {
    mandatory: bool,
    algs: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for EmptyPoolClientCertVerifier {
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
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
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
    // Confirm that certificate and key paths are valid and build the cached live reloader.
    let cert_source = if !cfg.cert_file.is_empty() {
        PemSource::File(cfg.cert_file.clone())
    } else {
        PemSource::Inline(cfg.cert.as_bytes().to_vec())
    };
    let key_source = if !cfg.key_file.is_empty() {
        PemSource::File(cfg.key_file.clone())
    } else {
        PemSource::Inline(cfg.key.as_bytes().to_vec())
    };
    let cert_resolver = CachedReloadingCert::new(cert_source, key_source, Arc::clone(&provider))
        .map_err(|e| match e {
            CertLoadError::ReadCert { path, reason } => {
                anyhow!("failed to read cert_file ({path}): {reason}")
            }
            CertLoadError::ReadKey { path, reason } => {
                anyhow!("failed to read key_file ({path}): {reason}")
            }
            CertLoadError::KeyPair(reason) => anyhow!("failed to load X509KeyPair: {reason}"),
        })?;

    let ca = client_ca_pool(cfg)?;
    let algs = provider.signature_verification_algorithms;
    let verifier: Arc<dyn ClientCertVerifier> = match cfg.client_auth_type.as_str() {
        "" | "NoClientCert" => {
            if ca.is_some() {
                bail!("client CA's have been configured without a Client Auth Policy");
            }
            WebPkiClientVerifier::no_client_auth()
        }
        "RequestClientCert" => Arc::new(AnyClientCert {
            mandatory: false,
            algs,
            hint_subjects: ca.as_ref().map(|s| s.subjects()).unwrap_or_default(),
        }),
        "RequireAnyClientCert" | "RequireClientCert" => Arc::new(AnyClientCert {
            mandatory: true,
            algs,
            hint_subjects: ca.as_ref().map(|s| s.subjects()).unwrap_or_default(),
        }),
        mode @ ("VerifyClientCertIfGiven" | "RequireAndVerifyClientCert") => {
            // Go verifies against the system roots when no client CA is configured.
            let roots = Arc::new(ca.unwrap_or_else(system_roots));
            if roots.is_empty() {
                Arc::new(EmptyPoolClientCertVerifier {
                    mandatory: mode == "RequireAndVerifyClientCert",
                    algs,
                })
            } else {
                let mut b = WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(&provider));
                if mode == "VerifyClientCertIfGiven" {
                    b = b.allow_unauthenticated();
                }
                b.build().map_err(|e| anyhow!("client CA pool: {e}"))?
            }
        }
        other => bail!("invalid ClientAuth: {other}"),
    };

    let mut sc = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .map_err(|e| anyhow!("{e}"))?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(Arc::new(cert_resolver) as Arc<dyn ResolvesServerCert>);
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

    fn tls_fixture(name: &str) -> String {
        format!(
            "{}/../mimir-storage/tests/fixtures/kafka_tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[test]
    fn d_tls_06_kx_groups_filters_pq_and_p521_when_fallback_curves_remain() {
        assert_eq!(kx_groups("X25519MLKEM768,X25519").unwrap().unwrap().len(), 1);
        assert_eq!(
            kx_groups("CurveP521,SecP256r1MLKEM768,SecP384r1MLKEM1024,CurveP256,CurveP384")
                .unwrap()
                .unwrap()
                .len(),
            2
        );
        assert!(kx_groups("X25519MLKEM768")
            .unwrap_err()
            .to_string()
            .contains("not supported"));
        assert!(kx_groups("CurveP521")
            .unwrap_err()
            .to_string()
            .contains("not supported"));
    }

    #[tokio::test]
    async fn d_tls_01_empty_client_ca_verify_policies_do_not_fail_startup() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let ca_pem = std::fs::read(tls_fixture("ca.crt")).unwrap();
        let mut ca_roots = RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(&ca_pem).flatten() {
            let _ = ca_roots.add(c);
        }
        let ca_roots = Arc::new(ca_roots);
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        // 1. VerifyClientCertIfGiven with empty/comments-only client_ca succeeds at config build,
        //    accepts unauthenticated clients, and rejects clients presenting a cert.
        let cfg_if_given = TLSConfig {
            cert_file: tls_fixture("server.crt"),
            key_file: tls_fixture("server.key"),
            client_auth_type: "VerifyClientCertIfGiven".into(),
            client_ca: "# comments only
".into(),
            ..Default::default()
        };
        let sc = server_config(&cfg_if_given, &TlsOptions::default(), &[b"http/1.1"]).unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(sc);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let acc = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut tls) = acc.accept(sock).await {
                        let _ = tls.write_all(b"ok").await;
                    }
                });
            }
        });

        // Unauthenticated client succeeds.
        let client_no_cert = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(Arc::clone(&ca_roots))
            .with_no_client_auth();
        let connector_no_cert = tokio_rustls::TlsConnector::from(Arc::new(client_no_cert));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut tls = connector_no_cert
            .connect(rustls::pki_types::ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let mut buf = [0u8; 2];
        tls.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");

        // Client presenting a cert is rejected.
        let client_certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(tls_fixture("client.crt"))
                .unwrap()
                .map(Result::unwrap)
                .collect();
        let client_key =
            rustls::pki_types::PrivateKeyDer::from_pem_file(tls_fixture("client.key")).unwrap();
        let client_with_cert = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(Arc::clone(&ca_roots))
            .with_client_auth_cert(client_certs.clone(), client_key.clone_key())
            .unwrap();
        let connector_with_cert = tokio_rustls::TlsConnector::from(Arc::new(client_with_cert));
        let tcp2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut tls2 = connector_with_cert
            .connect(rustls::pki_types::ServerName::try_from("localhost").unwrap(), tcp2)
            .await
            .unwrap();
        let mut buf2 = [0u8; 2];
        assert!(tls2.read_exact(&mut buf2).await.is_err());
        srv.abort();

        // 2. RequireAndVerifyClientCert with empty/comments-only client_ca also succeeds at
        //    config build and rejects unauthenticated clients.
        let cfg_req = TLSConfig {
            cert_file: tls_fixture("server.crt"),
            key_file: tls_fixture("server.key"),
            client_auth_type: "RequireAndVerifyClientCert".into(),
            client_ca: "# comments only
".into(),
            ..Default::default()
        };
        let sc_req = server_config(&cfg_req, &TlsOptions::default(), &[b"http/1.1"]).unwrap();
        let acceptor_req = tokio_rustls::TlsAcceptor::from(sc_req);
        let listener_req = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_req = listener_req.local_addr().unwrap();
        let srv_req = tokio::spawn(async move {
            while let Ok((sock, _)) = listener_req.accept().await {
                let acc = acceptor_req.clone();
                tokio::spawn(async move {
                    let _ = acc.accept(sock).await;
                });
            }
        });
        let tcp3 = tokio::net::TcpStream::connect(addr_req).await.unwrap();
        let mut tls3 = connector_no_cert
            .connect(rustls::pki_types::ServerName::try_from("localhost").unwrap(), tcp3)
            .await
            .unwrap();
        let mut buf3 = [0u8; 2];
        assert!(tls3.read_exact(&mut buf3).await.is_err());
        srv_req.abort();
    }

    #[tokio::test]
    async fn d_tls_04_request_and_require_any_client_cert_advertise_ca_hint_subjects() {
        #[derive(Debug)]
        struct CaptureHintsResolver {
            seen: Arc<parking_lot::Mutex<Option<Vec<Vec<u8>>>>>,
            inner: Arc<rustls::sign::CertifiedKey>,
        }
        impl rustls::client::ResolvesClientCert for CaptureHintsResolver {
            fn resolve(
                &self,
                root_hint_subjects: &[&[u8]],
                _sigschemes: &[SignatureScheme],
            ) -> Option<Arc<rustls::sign::CertifiedKey>> {
                *self.seen.lock() = Some(root_hint_subjects.iter().map(|s| s.to_vec()).collect());
                Some(Arc::clone(&self.inner))
            }
            fn has_certs(&self) -> bool {
                true
            }
        }

        let ca_pem = std::fs::read(tls_fixture("ca.crt")).unwrap();
        let mut ca_roots = RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(&ca_pem).flatten() {
            let _ = ca_roots.add(c);
        }
        let expected_subjects: Vec<Vec<u8>> = ca_roots
            .subjects()
            .into_iter()
            .map(|dn| dn.as_ref().to_vec())
            .collect();
        assert!(!expected_subjects.is_empty());

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let client_ck = Arc::new(
            mimir_common::tls::x509_key_pair(
                &std::fs::read(tls_fixture("client.crt")).unwrap(),
                &std::fs::read(tls_fixture("client.key")).unwrap(),
                &provider,
            )
            .unwrap(),
        );
        let ca_roots = Arc::new(ca_roots);

        for policy in ["RequestClientCert", "RequireAnyClientCert", "RequireClientCert"] {
            let cfg = TLSConfig {
                cert_file: tls_fixture("server.crt"),
                key_file: tls_fixture("server.key"),
                client_auth_type: policy.into(),
                client_ca_file: tls_fixture("ca.crt"),
                ..Default::default()
            };
            let sc = server_config(&cfg, &TlsOptions::default(), &[b"http/1.1"]).unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(sc);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let srv = tokio::spawn(async move {
                if let Ok((sock, _)) = listener.accept().await {
                    let _ = acceptor.accept(sock).await;
                }
            });

            let seen = Arc::new(parking_lot::Mutex::new(None));
            let client_cfg = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(Arc::clone(&ca_roots))
                .with_client_cert_resolver(Arc::new(CaptureHintsResolver {
                    seen: Arc::clone(&seen),
                    inner: Arc::clone(&client_ck),
                }));
            let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let _ = connector
                .connect(rustls::pki_types::ServerName::try_from("localhost").unwrap(), tcp)
                .await
                .unwrap();

            assert_eq!(
                seen.lock().as_ref().unwrap(),
                &expected_subjects,
                "policy {policy} must advertise ClientCAs DN hints in CertificateRequest"
            );
            srv.abort();
        }
    }
}
