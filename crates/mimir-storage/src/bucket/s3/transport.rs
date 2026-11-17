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
/// Go `http.DefaultMaxIdleConnsPerHost`.
const GO_DEFAULT_MAX_IDLE_CONNS_PER_HOST: usize = 2;

/// An HTTP client honouring the bucket `http.*` settings.
#[derive(Clone, Debug)]
pub(crate) struct HttpTransport {
    pub client: reqwest::Client,
    pub response_header_timeout: Option<Duration>,
    pub conn_limit: Option<Arc<Semaphore>>,
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



/// Server certificate verifier implementing Go's `InsecureSkipVerify` and `ServerName` override.
#[derive(Debug)]
struct GoVerifier {
    inner: Option<Arc<WebPkiServerVerifier>>,
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
        match &self.inner {
            None => Ok(ServerCertVerified::assertion()),
            Some(inner) => inner.verify_server_cert(
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
        Some(match name.parse::<std::net::IpAddr>() {
            Ok(ip) => ServerName::IpAddress(ip.into()),
            Err(_) => ServerName::try_from(name.clone())
                .map_err(|e| format!("invalid TLS server name {name:?}: {e}"))?,
        })
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
    let inner = if cfg.insecure_skip_verify {
        None
    } else {
        Some(
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                .build()
                .map_err(|e| format!("unable to build TLS verifier: {e}"))?,
        )
    };
    let verifier = GoVerifier {
        inner,
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
        let tls_handshake = Duration::from_nanos(cfg.tls_handshake_timeout.0.max(0) as u64);
        let mut max_idle_per_host = if cfg.max_idle_connections_per_host > 0 {
            cfg.max_idle_connections_per_host as usize
        } else {
            GO_DEFAULT_MAX_IDLE_CONNS_PER_HOST
        };
        if cfg.max_idle_connections > 0 {
            max_idle_per_host = max_idle_per_host.min(cfg.max_idle_connections as usize);
        }
        let mut builder = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .redirect(reqwest::redirect::Policy::none())
            .tcp_keepalive(DIAL_KEEP_ALIVE)
            .pool_idle_timeout(duration_opt(Duration::from_nanos(
                cfg.idle_conn_timeout.0.max(0) as u64,
            )))
            .pool_max_idle_per_host(max_idle_per_host)
            .http1_only();
        // Dial timeout (30s) plus TLS handshake timeout; a zero handshake timeout means
        // "no limit" for the handshake, so the combined connect timeout is unbounded too.
        if !tls_handshake.is_zero() {
            builder = builder.connect_timeout(DIAL_TIMEOUT + tls_handshake);
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
        })
    }

    /// Go `http.DefaultTransport` (used by thanos for the IAM credential provider).
    pub fn go_default() -> Self {
        let cfg = BucketHTTPConfig {
            response_header_timeout: Default::default(),
            max_idle_connections_per_host: 0,
            ..Default::default()
        };
        // http.DefaultTransport has no custom TLS config; it trusts the system roots.
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
}
