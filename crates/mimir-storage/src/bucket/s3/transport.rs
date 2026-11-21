//! HTTP transport for the S3 client, configured from the bucket `http.*` settings exactly like
//! thanos `exthttp.DefaultTransport` configures Go's `http.Transport`.
//!
//! Ported from:
//! - github.com/thanos-io/objstore/exthttp/transport.go (`DefaultTransport`)
//! - github.com/thanos-io/objstore/exthttp/tlsconfig.go (`NewTLSConfig`, `readCAFile`,
//!   `updateRootCA`, `getClientCertificate`)
//! - github.com/grafana/mimir/pkg/storage/bucket/common/http.go (`HTTPConfig.ToExtHTTP`)
//!
//! Mapping of Go `http.Transport` fields:
//! - `TLSClientConfig`: `tls_ca_path` replaces the root CAs (error texts of `NewTLSConfig`), client
//!   certificate/key are loaded eagerly and re-read on every handshake (`GetClientCertificate`),
//!   `tls_server_name` is the name the server certificate is verified against,
//!   `insecure_skip_verify` accepts any certificate (handshake signatures are still verified).
//!   As in Go, a custom TLS config without `ForceAttemptHTTP2` speaks HTTP/1.1 only (no ALPN).
//! - `DialContext` timeout 30s + keep-alive 30s, `TLSHandshakeTimeout`: connect timeout
//!   (dial + handshake) = 30s + `tls_handshake_timeout`.
//! - `IdleConnTimeout` (0 = no limit), `MaxIdleConnsPerHost` (0 = Go default 2) capped by
//!   `MaxIdleConns` (one host per client).
//! - `MaxConnsPerHost`: a per-client semaphore held for the whole request/response exchange.
//! - `ResponseHeaderTimeout`: measured from the end of the request body to the response headers.
//! - `Proxy: http.ProxyFromEnvironment`: reqwest's system proxy support (`HTTP(S)_PROXY`,
//!   `NO_PROXY`).
//! - `ExpectContinueTimeout` only applies to requests carrying `Expect: 100-continue`, which
//!   minio-go never sends; it is therefore inert in Go as well.
//! - No overall request timeout (Go `http.Client.Timeout` is unset) and redirects are not
//!   followed (minio-go `CheckRedirect` returns `http.ErrUseLastResponse`).

use std::sync::Arc;
use std::time::Duration;

use mimir_common::tls::CachedReloadingCert;
use mimir_config::BucketHTTPConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::sync::Semaphore;

/// Go `net.Dialer.Timeout` / `KeepAlive` used by `exthttp.DefaultTransport`.
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);
const DIAL_KEEP_ALIVE: Duration = Duration::from_secs(30);
/// Immutable per-target-host DNS resolver override so reqwest dials `target_host` while
/// sending `sni_host` in TLS `ClientHello` SNI. Each distinct target host gets its own
/// `reqwest::Client` (and thus its own keep-alive pool and resolver), eliminating both
/// pooled-connection misrouting and concurrent target-host races.
#[derive(Debug)]
struct SniResolver {
    sni_host: String,
    target_host: String,
}

impl reqwest::dns::Resolve for SniResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let lookup_host = if name.as_str().eq_ignore_ascii_case(&self.sni_host) {
            self.target_host.clone()
        } else {
            name.as_str().to_string()
        };
        Box::pin(async move {
            if let Ok(ip) = lookup_host.parse::<std::net::IpAddr>() {
                let addrs: reqwest::dns::Addrs =
                    Box::new(std::iter::once(std::net::SocketAddr::new(ip, 0)));
                return Ok(addrs);
            }
            let resolved: Vec<std::net::SocketAddr> =
                tokio::net::lookup_host((lookup_host.as_str(), 0))
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                    .collect();
            let addrs: reqwest::dns::Addrs = Box::new(resolved.into_iter());
            Ok(addrs)
        })
    }
}

/// Per-target-host `reqwest::Client` pool used when `http.tls_server_name` is a DNS name.
#[derive(Clone, Debug)]
pub(crate) struct SniClientPool {
    pub sni_host: String,
    tls: rustls::ClientConfig,
    idle_conn_timeout: Option<Duration>,
    max_idle_per_host: usize,
    connect_timeout: Option<Duration>,
    clients: Arc<parking_lot::RwLock<std::collections::HashMap<String, reqwest::Client>>>,
}

impl SniClientPool {
    fn build_client(&self, target_host: &str) -> Result<reqwest::Client, String> {
        let resolver = Arc::new(SniResolver {
            sni_host: self.sni_host.clone(),
            target_host: target_host.to_string(),
        });
        let mut builder = reqwest::Client::builder()
            .use_preconfigured_tls(self.tls.clone())
            .redirect(reqwest::redirect::Policy::none())
            .tcp_keepalive(DIAL_KEEP_ALIVE)
            .pool_idle_timeout(self.idle_conn_timeout)
            .pool_max_idle_per_host(self.max_idle_per_host)
            .http1_only()
            .dns_resolver(resolver);
        if let Some(t) = self.connect_timeout {
            builder = builder.connect_timeout(t);
        }
        builder
            .build()
            .map_err(|e| format!("unable to build HTTP client: {e}"))
    }

    pub fn client_for_host(&self, target_host: &str) -> Result<reqwest::Client, String> {
        let key = target_host.to_ascii_lowercase();
        if let Some(c) = self.clients.read().get(&key) {
            return Ok(c.clone());
        }
        let mut guard = self.clients.write();
        if let Some(c) = guard.get(&key) {
            return Ok(c.clone());
        }
        let client = self.build_client(target_host)?;
        guard.insert(key, client.clone());
        Ok(client)
    }
}

/// An HTTP client honouring the bucket `http.*` settings.
#[derive(Clone, Debug)]
pub(crate) struct HttpTransport {
    pub client: reqwest::Client,
    pub response_header_timeout: Option<Duration>,
    pub conn_limit: Option<Arc<Semaphore>>,
    pub sni: Option<SniClientPool>,
}

fn go_open_err(path: &str, err: &std::io::Error) -> String {
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

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn system_roots() -> RootCertStore {
    let loaded = rustls_native_certs::load_native_certs();
    for err in &loaded.errors {
        tracing::warn!(err = %err, "failed to load some system root CA certificates");
    }
    let mut store = RootCertStore::empty();
    store.add_parsable_certificates(loaded.certs);
    store
}



#[derive(Debug)]
enum VerifyMode {
    InsecureSkip,
    RejectAll,
    WebPki(Arc<WebPkiServerVerifier>),
}

/// Server certificate verifier implementing Go's `InsecureSkipVerify`, empty-CA reject-all,
/// and `ServerName` override.
#[derive(Debug)]
struct GoVerifier {
    mode: VerifyMode,
    server_name: Option<ServerName<'static>>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for GoVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match &self.mode {
            VerifyMode::InsecureSkip => Ok(ServerCertVerified::assertion()),
            VerifyMode::RejectAll => Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            )),
            VerifyMode::WebPki(inner) => inner.verify_server_cert(
                end_entity,
                intermediates,
                self.server_name.as_ref().unwrap_or(server_name),
                ocsp_response,
                now,
            ),
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

/// exthttp `NewTLSConfig` + `tlsConfig.InsecureSkipVerify = config.InsecureSkipVerify`.
pub(crate) fn new_tls_config(cfg: &BucketHTTPConfig) -> Result<rustls::ClientConfig, String> {
    let provider = provider();
    let roots = if cfg.tls_ca_path.is_empty() {
        system_roots()
    } else {
        let pem = std::fs::read(&cfg.tls_ca_path).map_err(|e| {
            format!(
                "unable to load specified CA cert {}: {}",
                cfg.tls_ca_path,
                go_open_err(&cfg.tls_ca_path, &e)
            )
        })?;
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
            .filter_map(Result::ok)
            .collect();
        let mut store = RootCertStore::empty();
        let (added, _) = store.add_parsable_certificates(certs);
        if added == 0 {
            return Err(format!("unable to use specified CA cert {}", cfg.tls_ca_path));
        }
        store
    };
    let server_name = if cfg.tls_server_name.is_empty() {
        None
    } else {
        let name = &cfg.tls_server_name;
        match name.parse::<std::net::IpAddr>() {
            Ok(ip) => Some(ServerName::IpAddress(ip.into())),
            Err(_) => match ServerName::try_from(name.clone()) {
                Ok(sn) => Some(sn),
                Err(_) if cfg.insecure_skip_verify => None,
                Err(e) => return Err(format!("invalid TLS server name {name:?}: {e}")),
            },
        }
    };
    let client_cert = match (cfg.tls_cert_path.is_empty(), cfg.tls_key_path.is_empty()) {
        (true, true) => None,
        (false, true) => {
            return Err(format!(
                "client cert file {:?} specified without client key file",
                cfg.tls_cert_path
            ))
        }
        (true, false) => {
            return Err(format!(
                "client key file {:?} specified without client cert file",
                cfg.tls_key_path
            ))
        }
        (false, false) => {
            let r = CachedReloadingCert::from_files(
                &cfg.tls_cert_path,
                &cfg.tls_key_path,
                Arc::clone(&provider),
            )
            .map_err(|e| {
                format!(
                    "unable to use specified client cert ({}) & key ({}): {}",
                    cfg.tls_cert_path,
                    cfg.tls_key_path,
                    e.into_reason()
                )
            })?;
            Some(Arc::new(r) as Arc<dyn ResolvesClientCert>)
        }
    };
    let mode = if cfg.insecure_skip_verify {
        VerifyMode::InsecureSkip
    } else if roots.is_empty() {
        // Go: empty root pool succeeds at startup and rejects all server certificates at handshake.
        VerifyMode::RejectAll
    } else {
        VerifyMode::WebPki(
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                .build()
                .map_err(|e| format!("unable to build TLS verifier: {e}"))?,
        )
    };
    let verifier = GoVerifier {
        mode,
        server_name,
        algorithms: provider.signature_verification_algorithms,
    };
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));
    let config = match client_cert {
        None => builder.with_no_client_auth(),
        Some(r) => builder.with_client_cert_resolver(r),
    };
    Ok(config)
}

fn duration_opt(d: Duration) -> Option<Duration> {
    if d.is_zero() {
        None
    } else {
        Some(d)
    }
}

impl HttpTransport {
    /// exthttp `DefaultTransport(config)` wrapped in minio-go's `http.Client`.
    pub fn new(cfg: &BucketHTTPConfig) -> Result<Self, String> {
        if cfg.force_attempt_http2 {
            return Err("http.force_attempt_http2 is not supported by mimir-rs for the s3 backend".to_string());
        }
        let tls = new_tls_config(cfg)?;
        let idle_conn_timeout = duration_opt(Duration::from_nanos(
            cfg.idle_conn_timeout.0.max(0) as u64,
        ));
        let tls_handshake = Duration::from_nanos(cfg.tls_handshake_timeout.0.max(0) as u64);
        let max_idle_per_host = crate::bucket::exthttp::max_idle_per_host(cfg);
        // Dial timeout (30s) plus TLS handshake timeout; a zero handshake timeout means
        // "no limit" for the handshake, so the combined connect timeout is unbounded too.
        let connect_timeout = (!tls_handshake.is_zero()).then(|| DIAL_TIMEOUT + tls_handshake);
        let sni = if cfg.tls_server_name.is_empty() {
            None
        } else {
            match cfg.tls_server_name.parse::<std::net::IpAddr>() {
                Ok(_) => None,
                Err(_) => ServerName::try_from(cfg.tls_server_name.clone())
                    .ok()
                    .and_then(|sn| match sn {
                        ServerName::DnsName(dns) => Some(SniClientPool {
                            sni_host: dns.as_ref().to_string(),
                            tls: tls.clone(),
                            idle_conn_timeout,
                            max_idle_per_host,
                            connect_timeout,
                            clients: Arc::new(parking_lot::RwLock::new(
                                std::collections::HashMap::new(),
                            )),
                        }),
                        _ => None,
                    }),
            }
        };
        let mut builder = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .redirect(reqwest::redirect::Policy::none())
            .tcp_keepalive(DIAL_KEEP_ALIVE)
            .pool_idle_timeout(idle_conn_timeout)
            .pool_max_idle_per_host(max_idle_per_host)
            .http1_only();
        if let Some(t) = connect_timeout {
            builder = builder.connect_timeout(t);
        }
        let client = builder
            .build()
            .map_err(|e| format!("unable to build HTTP client: {e}"))?;
        Ok(Self {
            client,
            response_header_timeout: duration_opt(Duration::from_nanos(
                cfg.response_header_timeout.0.max(0) as u64,
            )),
            conn_limit: (cfg.max_connections_per_host > 0)
                .then(|| Arc::new(Semaphore::new(cfg.max_connections_per_host as usize))),
            sni,
        })
    }

    /// Go `http.DefaultTransport`.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn go_default() -> Self {
        let cfg = BucketHTTPConfig {
            response_header_timeout: Default::default(),
            max_idle_connections_per_host: 0,
            ..Default::default()
        };
        Self::new(&cfg).expect("default transport")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fixture(name: &str) -> String {
        format!(
            "{}/tests/fixtures/kafka_tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    // exthttp NewTLSConfig error texts.
    #[test]
    fn tls_config_errors_follow_exthttp() {
        let err = |c: BucketHTTPConfig| new_tls_config(&c).unwrap_err();
        assert_eq!(
            err(BucketHTTPConfig {
                tls_ca_path: "/does/not/exist.crt".into(),
                ..Default::default()
            }),
            "unable to load specified CA cert /does/not/exist.crt: open /does/not/exist.crt: no such file or directory"
        );
        assert!(Path::new(&fixture("client.key")).exists());
        assert_eq!(
            err(BucketHTTPConfig {
                tls_ca_path: fixture("client.key"),
                ..Default::default()
            }),
            format!("unable to use specified CA cert {}", fixture("client.key"))
        );
        assert_eq!(
            err(BucketHTTPConfig {
                tls_cert_path: "a.crt".into(),
                ..Default::default()
            }),
            "client cert file \"a.crt\" specified without client key file"
        );
        assert_eq!(
            err(BucketHTTPConfig {
                tls_key_path: "a.key".into(),
                ..Default::default()
            }),
            "client key file \"a.key\" specified without client cert file"
        );
        let e = err(BucketHTTPConfig {
            tls_cert_path: fixture("client.crt"),
            tls_key_path: "/missing.key".into(),
            ..Default::default()
        });
        assert!(
            e.starts_with(&format!(
                "unable to use specified client cert ({}) & key (/missing.key): open /missing.key",
                fixture("client.crt")
            )),
            "{e}"
        );
    }

    #[test]
    fn tls_config_ok() {
        let c = new_tls_config(&BucketHTTPConfig {
            tls_ca_path: fixture("ca.crt"),
            tls_cert_path: fixture("client.crt"),
            tls_key_path: fixture("client.key"),
            tls_server_name: "minio.local".into(),
            ..Default::default()
        })
        .unwrap();
        assert!(c.client_auth_cert_resolver.has_certs());
        assert!(c.alpn_protocols.is_empty());
        HttpTransport::new(&BucketHTTPConfig::default()).unwrap();
        assert!(HttpTransport::new(&BucketHTTPConfig {
            force_attempt_http2: true,
            ..Default::default()
        })
        .is_err());
    }

    #[test]
    fn d_tls_01_and_03_empty_roots_reject_all_and_insecure_non_rfc_server_name() {
        let provider = provider();
        let verifier = GoVerifier {
            mode: VerifyMode::RejectAll,
            server_name: None,
            algorithms: provider.signature_verification_algorithms,
        };
        let cert = CertificateDer::pem_file_iter(fixture("server.crt"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let err = verifier
            .verify_server_cert(
                &cert,
                &[],
                &ServerName::try_from("localhost").unwrap(),
                &[],
                UnixTime::now(),
            )
            .unwrap_err();
        assert_eq!(
            err,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)
        );

        // Non-RFC-1035 tls_server_name must fail when insecure_skip_verify is false,
        // and succeed when insecure_skip_verify is true.
        let strict = new_tls_config(&BucketHTTPConfig {
            tls_server_name: "bad_host:9092".into(),
            insecure_skip_verify: false,
            ..Default::default()
        });
        assert!(strict.is_err());

        let insecure_cfg = BucketHTTPConfig {
            tls_server_name: "bad_host:9092".into(),
            insecure_skip_verify: true,
            ..Default::default()
        };
        assert!(new_tls_config(&insecure_cfg).is_ok());
        assert!(HttpTransport::new(&insecure_cfg).is_ok());
    }
}
