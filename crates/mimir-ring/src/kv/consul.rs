//! Consul HTTP/HTTPS KV client.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/consul/client.go
//! - github.com/grafana/dskit/kv/consul/metrics.go
//! - github.com/hashicorp/consul/api/v2/api.go (`DefaultConfig`, `SetupTLSConfig`, `NewClient`, `newRequest`)
//! - github.com/hashicorp/consul/api/v2/kv.go (`KV.Get`, `KV.List`, `KV.Put`, `KV.CAS`, `KV.Delete`)
//! - github.com/hashicorp/go-rootcerts/rootcerts.go (`LoadCAFile`, `LoadCAPath`)

use std::any::Any;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::services::ServiceContext;
use mimir_common::tls::{go_read_err, parse_server_name, system_roots, x509_key_pair};
use mimir_config::ingester::ConsulConfig;
use prometheus::{HistogramOpts, HistogramVec, Registry};
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use tokio_rustls::rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::{
    CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tracing::{debug, error, warn};

use super::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError, Registerer};

const LONG_POLL_DURATION: Duration = Duration::from_secs(10);

/// `CONSUL_*` environment variables read by `hashicorp/consul/api/v2.DefaultConfig()`.
#[derive(Clone, Debug, Default)]
pub struct ConsulEnv {
    pub http_addr: String,
    pub token: String,
    pub token_file: String,
    pub http_auth: String,
    pub http_ssl: String,
    pub ca_cert: String,
    pub ca_path: String,
    pub client_cert: String,
    pub client_key: String,
    pub tls_server_name: String,
    pub http_ssl_verify: String,
    pub namespace: String,
    pub partition: String,
}

impl ConsulEnv {
    pub fn from_process_env() -> Self {
        let get = |k: &str| std::env::var(k).unwrap_or_default();
        Self {
            http_addr: get("CONSUL_HTTP_ADDR"),
            token: get("CONSUL_HTTP_TOKEN"),
            token_file: get("CONSUL_HTTP_TOKEN_FILE"),
            http_auth: get("CONSUL_HTTP_AUTH"),
            http_ssl: get("CONSUL_HTTP_SSL"),
            ca_cert: get("CONSUL_CACERT"),
            ca_path: get("CONSUL_CAPATH"),
            client_cert: get("CONSUL_CLIENT_CERT"),
            client_key: get("CONSUL_CLIENT_KEY"),
            tls_server_name: get("CONSUL_TLS_SERVER_NAME"),
            http_ssl_verify: get("CONSUL_HTTP_SSL_VERIFY"),
            namespace: get("CONSUL_NAMESPACE"),
            partition: get("CONSUL_PARTITION"),
        }
    }
}

/// Go `strconv.ParseBool`.
fn consul_parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {s:?}: invalid syntax")),
    }
}

/// Go `net/url` `escape(s, encodePath)`: escapes `#`, `?`, `%`, spaces, control/non-ASCII bytes,
/// and sub-delims outside Go's `encodePath` set while preserving `/` path separators.
fn escape_go_url_path(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(path.len());
    for &b in path.as_bytes() {
        let keep = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@'
            );
        if keep {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 0x0f) as usize]));
        }
    }
    out
}

/// Go `net/url.QueryEscape`.
fn escape_go_query_component(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b == b' ' {
            out.push('+');
        } else if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 0x0f) as usize]));
        }
    }
    out
}

/// Go `consul/api.durToMsec`: converts a duration to `"<ms>ms"`, rounding positive sub-millisecond
/// durations up to `"1ms"`.
fn dur_to_msec(dur: Duration) -> String {
    let ms = dur.as_millis();
    if !dur.is_zero() && ms == 0 {
        return "1ms".to_string();
    }
    format!("{ms}ms")
}

/// Splits `server` into host if `:` appears after any `]`, matching `SetupTLSConfig` in
/// `hashicorp/consul/api/v2/api.go`.
fn extract_tls_server_host(address: &str) -> Result<String, String> {
    let last_colon = address.rfind(':');
    let last_bracket = address.rfind(']');
    let host_part = match (last_colon, last_bracket) {
        (Some(c), Some(b)) if c > b => &address[..c],
        (Some(c), None) => {
            if address[..c].contains(':') {
                return Err(format!("address {address}: too many colons in address"));
            }
            &address[..c]
        }
        _ => address,
    };
    let unwrapped = host_part
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host_part);
    Ok(unwrapped.to_string())
}

fn append_certs_from_pem(store: &mut RootCertStore, pem: &[u8]) -> usize {
    let mut added = 0usize;
    for cert in CertificateDer::pem_slice_iter(pem).flatten() {
        if store.add(cert).is_ok() {
            added += 1;
        }
    }
    added
}

fn load_ca_file(ca_file: &str) -> Result<RootCertStore, String> {
    let pem = std::fs::read(ca_file)
        .map_err(|e| format!("Error loading CA File: {}", go_read_err(ca_file, &e)))?;
    let mut store = RootCertStore::empty();
    if append_certs_from_pem(&mut store, &pem) == 0 {
        return Err(format!(
            "Error loading CA File: Couldn't parse PEM in: {ca_file}"
        ));
    }
    Ok(store)
}

fn walk_ca_path(dir: &Path, store: &mut RootCertStore) -> Result<(), String> {
    let meta = std::fs::metadata(dir).map_err(|e| go_read_err(&dir.to_string_lossy(), &e))?;
    if !meta.is_dir() {
        let p = dir.to_string_lossy();
        let pem = std::fs::read(dir)
            .map_err(|e| format!("Error loading file from CAPath: {}", go_read_err(&p, &e)))?;
        if append_certs_from_pem(store, &pem) == 0 {
            return Err(format!(
                "Error loading CA Path: Couldn't parse PEM in: {p}"
            ));
        }
        return Ok(());
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| go_read_err(&dir.to_string_lossy(), &e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| go_read_err(&dir.to_string_lossy(), &e))?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let ft = entry
            .file_type()
            .map_err(|e| go_read_err(&path.to_string_lossy(), &e))?;
        if ft.is_dir() {
            walk_ca_path(&path, store)?;
        } else {
            let p = path.to_string_lossy();
            let pem = std::fs::read(&path)
                .map_err(|e| format!("Error loading file from CAPath: {}", go_read_err(&p, &e)))?;
            if append_certs_from_pem(store, &pem) == 0 {
                return Err(format!(
                    "Error loading CA Path: Couldn't parse PEM in: {p}"
                ));
            }
        }
    }
    Ok(())
}

fn load_ca_path(ca_path: &str) -> Result<RootCertStore, String> {
    let mut store = RootCertStore::empty();
    walk_ca_path(Path::new(ca_path), &mut store)?;
    Ok(store)
}

fn load_client_cert_key(
    cert_path: &str,
    key_path: &str,
    provider: &CryptoProvider,
) -> Result<CertifiedKey, String> {
    let cert = std::fs::read(cert_path).map_err(|e| go_read_err(cert_path, &e))?;
    let key = std::fs::read(key_path).map_err(|e| go_read_err(key_path, &e))?;
    x509_key_pair(&cert, &key, provider)
}

#[derive(Debug)]
struct ConsulClientCertResolver {
    cert_path: String,
    key_path: String,
    provider: Arc<CryptoProvider>,
    initial: Arc<CertifiedKey>,
}

impl ResolvesClientCert for ConsulClientCertResolver {
    fn resolve(&self, _: &[&[u8]], _: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        match load_client_cert_key(&self.cert_path, &self.key_path, &self.provider) {
            Ok(ck) => Some(Arc::new(ck)),
            Err(_) => Some(Arc::clone(&self.initial)),
        }
    }

    fn has_certs(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct ConsulTlsVerifier {
    inner: Option<Arc<WebPkiServerVerifier>>,
    reject_all: bool,
    override_server_name: Option<ServerName<'static>>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for ConsulTlsVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.reject_all {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ));
        }
        match &self.inner {
            None => Ok(ServerCertVerified::assertion()),
            Some(inner) => {
                let effective_name = self.override_server_name.as_ref().unwrap_or(server_name);
                inner.verify_server_cert(
                    end_entity,
                    intermediates,
                    effective_name,
                    ocsp_response,
                    now,
                )
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

fn build_consul_tls_config(env: &ConsulEnv) -> Result<rustls::ClientConfig, KvError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let override_server_name = if env.tls_server_name.is_empty() {
        None
    } else {
        let host = extract_tls_server_host(&env.tls_server_name).map_err(KvError::InvalidConfig)?;
        Some(parse_server_name(&host).map_err(|e| KvError::InvalidConfig(e.to_string()))?)
    };

    let client_cert = if env.client_cert.is_empty() && env.client_key.is_empty() {
        None
    } else if env.client_cert.is_empty() || env.client_key.is_empty() {
        return Err(KvError::InvalidConfig(
            "both client cert and client key must be provided".to_string(),
        ));
    } else {
        let initial = load_client_cert_key(&env.client_cert, &env.client_key, &provider)
            .map_err(KvError::InvalidConfig)?;
        Some(ConsulClientCertResolver {
            cert_path: env.client_cert.clone(),
            key_path: env.client_key.clone(),
            provider: Arc::clone(&provider),
            initial: Arc::new(initial),
        })
    };

    let custom_roots = if !env.ca_cert.is_empty() {
        Some(Arc::new(
            load_ca_file(&env.ca_cert).map_err(KvError::InvalidConfig)?,
        ))
    } else if !env.ca_path.is_empty() {
        Some(Arc::new(
            load_ca_path(&env.ca_path).map_err(KvError::InvalidConfig)?,
        ))
    } else {
        None
    };

    let mut insecure_skip_verify = false;
    if !env.http_ssl_verify.is_empty() {
        let do_verify = match consul_parse_bool(&env.http_ssl_verify) {
            Ok(b) => b,
            Err(err) => {
                warn!(%err, "could not parse CONSUL_HTTP_SSL_VERIFY");
                false
            }
        };
        if !do_verify {
            insecure_skip_verify = true;
        }
    }

    let verifier = if insecure_skip_verify {
        ConsulTlsVerifier {
            inner: None,
            reject_all: false,
            override_server_name,
            algorithms: provider.signature_verification_algorithms,
        }
    } else {
        let roots = custom_roots.unwrap_or_else(system_roots);
        if roots.is_empty() {
            ConsulTlsVerifier {
                inner: None,
                reject_all: true,
                override_server_name,
                algorithms: provider.signature_verification_algorithms,
            }
        } else {
            let inner = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
                .build()
                .map_err(|e| KvError::InvalidConfig(format!("building TLS verifier: {e}")))?;
            ConsulTlsVerifier {
                inner: Some(inner),
                reject_all: false,
                override_server_name,
                algorithms: provider.signature_verification_algorithms,
            }
        }
    };

    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| KvError::InvalidConfig(format!("building TLS config: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));

    Ok(match client_cert {
        Some(resolver) => builder.with_client_cert_resolver(Arc::new(resolver)),
        None => builder.with_no_client_auth(),
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ConsulKvPair {
    key: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    create_index: u64,
    #[serde(default)]
    modify_index: u64,
}

struct SimpleRateLimiter {
    rate_per_sec: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl SimpleRateLimiter {
    fn new(rate_per_sec: f64, burst: i64) -> Option<Self> {
        if rate_per_sec <= 0.0 {
            return None;
        }
        let b = (burst.max(1)) as f64;
        Some(Self {
            rate_per_sec,
            burst: b,
            state: Mutex::new((b, Instant::now())),
        })
    }

    async fn wait(&self, cancel: &ServiceContext) -> bool {
        loop {
            if cancel.is_canceled() {
                return false;
            }
            let sleep_dur = {
                let mut st = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(st.1).as_secs_f64();
                st.0 = (st.0 + elapsed * self.rate_per_sec).min(self.burst);
                st.1 = now;
                if st.0 >= 1.0 {
                    st.0 -= 1.0;
                    None
                } else {
                    let needed = (1.0 - st.0) / self.rate_per_sec;
                    Some(Duration::from_secs_f64(needed))
                }
            };
            match sleep_dur {
                None => return true,
                Some(d) => {
                    tokio::select! {
                        _ = cancel.canceled() => return false,
                        _ = tokio::time::sleep(d) => {}
                    }
                }
            }
        }
    }
}

pub struct ConsulKv {
    http: reqwest::Client,
    scheme: String,
    host_authority: String,
    path_prefix: String,
    namespace: String,
    partition: String,
    cfg: ConsulConfig,
    codec: Arc<dyn Codec>,
    limiter: Option<SimpleRateLimiter>,
    request_duration: HistogramVec,
    long_poll_duration: Duration,
}

impl ConsulKv {
    pub fn new(
        cfg: ConsulConfig,
        codec: Arc<dyn Codec>,
        reg: Option<&Registry>,
    ) -> Result<Self, KvError> {
        Self::with_registerer(cfg, codec, reg.map(Registerer::new).as_ref())
    }

    /// Go `consul.NewClient(cfg, codec, logger, registerer)` with a (possibly
    /// wrapped) Go-style registerer and `CONSUL_*` environment defaults.
    pub fn with_registerer(
        cfg: ConsulConfig,
        codec: Arc<dyn Codec>,
        reg: Option<&Registerer>,
    ) -> Result<Self, KvError> {
        let env = ConsulEnv::from_process_env();
        Self::with_registerer_and_env(cfg, codec, reg, &env)
    }

    /// Constructs [`ConsulKv`] using an explicit [`ConsulEnv`] snapshot (used by unit tests to
    /// avoid mutating process-wide environment variables concurrently).
    pub fn with_registerer_and_env(
        cfg: ConsulConfig,
        codec: Arc<dyn Codec>,
        reg: Option<&Registerer>,
        env: &ConsulEnv,
    ) -> Result<Self, KvError> {
        let mut scheme = "http".to_string();
        if !env.http_ssl.is_empty() {
            match consul_parse_bool(&env.http_ssl) {
                Ok(true) => scheme = "https".to_string(),
                Ok(false) => {}
                Err(err) => warn!(%err, "could not parse CONSUL_HTTP_SSL"),
            }
        }

        let raw_addr = if !cfg.host.is_empty() {
            cfg.host.as_str()
        } else if !env.http_addr.is_empty() {
            env.http_addr.as_str()
        } else {
            "localhost:8500"
        };

        let (host_authority, path_prefix) = match raw_addr.split_once("://") {
            Some((proto, rest)) => {
                match proto {
                    "http" => {
                        // Never revert to http if TLS was explicitly requested via CONSUL_HTTP_SSL.
                    }
                    "https" => {
                        scheme = "https".to_string();
                    }
                    other => {
                        return Err(KvError::InvalidConfig(format!(
                            "Unknown protocol scheme: {other}"
                        )));
                    }
                }
                match rest.split_once('/') {
                    Some((h, p)) => {
                        let p = p.trim_end_matches('/');
                        let prefix = if p.is_empty() {
                            String::new()
                        } else {
                            format!("/{p}")
                        };
                        (h.to_string(), prefix)
                    }
                    None => (rest.to_string(), String::new()),
                }
            }
            None => (raw_addr.trim_end_matches('/').to_string(), String::new()),
        };

        if host_authority.is_empty() {
            return Err(KvError::InvalidConfig(
                "consul host cannot be empty".to_string(),
            ));
        }

        // Token precedence (`hashicorp/consul/api/v2/api.go`):
        // 1. cfg.acl_token (YAML / CLI)
        // 2. CONSUL_HTTP_TOKEN_FILE
        // 3. CONSUL_HTTP_TOKEN
        let token = if !cfg.acl_token.is_empty() {
            cfg.acl_token.clone()
        } else if !env.token_file.is_empty() {
            let data = std::fs::read_to_string(&env.token_file).map_err(|e| {
                KvError::InvalidConfig(format!(
                    "Error loading token file {} : {}",
                    env.token_file,
                    go_read_err(&env.token_file, &e)
                ))
            })?;
            data.trim().to_string()
        } else {
            env.token.clone()
        };

        let mut headers = HeaderMap::new();
        if !token.is_empty() {
            let val = HeaderValue::from_str(&token)
                .map_err(|e| KvError::InvalidConfig(format!("invalid consul acl token: {e}")))?;
            headers.insert("X-Consul-Token", val);
        }
        if !env.http_auth.is_empty() {
            let (user, pass) = match env.http_auth.split_once(':') {
                Some((u, p)) => (u, p),
                None => (env.http_auth.as_str(), ""),
            };
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
            let val = HeaderValue::from_str(&format!("Basic {encoded}"))
                .map_err(|e| KvError::InvalidConfig(format!("invalid consul http auth: {e}")))?;
            headers.insert(AUTHORIZATION, val);
        }

        let tls_config = build_consul_tls_config(env)?;
        let timeout: Duration = cfg.http_client_timeout.as_std().unwrap_or(Duration::ZERO);
        let mut builder = reqwest::Client::builder()
            .use_preconfigured_tls(tls_config)
            .default_headers(headers);
        if !timeout.is_zero() {
            builder = builder.timeout(timeout);
        }
        let http = builder
            .build()
            .map_err(|e| KvError::Http(format!("build consul http client: {e}")))?;

        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "consul_request_duration_seconds",
                "Time spent on consul requests.",
            )
            .buckets(prometheus::DEFAULT_BUCKETS.to_vec()),
            &["operation", "status_code"],
        )
        .map_err(|e| KvError::Other(format!("creating consul_request_duration_seconds: {e}")))?;
        if let Some(r) = reg {
            let _ = r.register(Box::new(request_duration.clone()));
        }

        let limiter = SimpleRateLimiter::new(cfg.watch_rate_limit, cfg.watch_burst_size);
        Ok(Self {
            http,
            scheme,
            host_authority,
            path_prefix,
            namespace: env.namespace.clone(),
            partition: env.partition.clone(),
            cfg,
            codec,
            limiter,
            request_duration,
            long_poll_duration: LONG_POLL_DURATION,
        })
    }

    /// Put a key directly (Go `consul.Client.Put`, used primarily by tests).
    pub async fn put(&self, key: &str, value: &(dyn Any + Send + Sync)) -> Result<(), KvError> {
        let bytes = self.codec.encode(value)?;
        self.put_raw(key, bytes).await
    }

    fn observe_op(&self, operation: &str, ok: bool, start: Instant) {
        let status = if ok { "200" } else { "500" };
        self.request_duration
            .with_label_values(&[operation, status])
            .observe(start.elapsed().as_secs_f64());
    }

    fn build_kv_url(
        &self,
        key_or_prefix: &str,
        trim_leading_slash: bool,
        extra_params: &[(&str, String)],
    ) -> String {
        let key_part = if trim_leading_slash {
            key_or_prefix.strip_prefix('/').unwrap_or(key_or_prefix)
        } else {
            key_or_prefix
        };
        let raw_path = format!("{}/v1/kv/{key_part}", self.path_prefix);
        let escaped_path = escape_go_url_path(&raw_path);

        let mut params: BTreeMap<&str, String> = BTreeMap::new();
        if !self.namespace.is_empty() {
            params.insert("ns", self.namespace.clone());
        }
        if !self.partition.is_empty() {
            params.insert("partition", self.partition.clone());
        }
        for (k, v) in extra_params {
            params.insert(*k, v.clone());
        }

        if params.is_empty() {
            format!("{}://{}{}", self.scheme, self.host_authority, escaped_path)
        } else {
            let mut query = String::new();
            for (i, (k, v)) in params.iter().enumerate() {
                if i > 0 {
                    query.push('&');
                }
                query.push_str(&escape_go_query_component(k));
                query.push('=');
                query.push_str(&escape_go_query_component(v));
            }
            format!(
                "{}://{}{}?{}",
                self.scheme, self.host_authority, escaped_path, query
            )
        }
    }

    fn push_read_consistency(&self, params: &mut Vec<(&'static str, String)>) {
        if self.cfg.consistent_reads {
            params.push(("consistent", String::new()));
        } else {
            params.push(("stale", String::new()));
        }
    }

    async fn get_raw(
        &self,
        key: &str,
        wait_index: Option<u64>,
        allow_consistency: bool,
        cancel: Option<&ServiceContext>,
    ) -> Result<(Option<ConsulKvPair>, u64), KvError> {
        let start = Instant::now();
        let res = self
            .get_raw_untracked(key, wait_index, allow_consistency, cancel)
            .await;
        self.observe_op("Get", res.is_ok(), start);
        res
    }

    async fn get_raw_untracked(
        &self,
        key: &str,
        wait_index: Option<u64>,
        allow_consistency: bool,
        cancel: Option<&ServiceContext>,
    ) -> Result<(Option<ConsulKvPair>, u64), KvError> {
        let mut params = Vec::with_capacity(3);
        if let Some(idx) = wait_index {
            if idx > 0 {
                params.push(("index", idx.to_string()));
            }
            params.push(("wait", dur_to_msec(self.long_poll_duration)));
        }
        if allow_consistency {
            self.push_read_consistency(&mut params);
        }
        let url = self.build_kv_url(key, true, &params);

        let send_fut = self.http.get(&url).send();
        let resp = match cancel {
            Some(c) => {
                tokio::select! {
                    _ = c.canceled() => return Err(KvError::Canceled),
                    r = send_fut => r.map_err(|e| KvError::Http(e.to_string()))?,
                }
            }
            None => send_fut.await.map_err(|e| KvError::Http(e.to_string()))?,
        };

        let last_index = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let status = resp.status();
        if status.as_u16() == 404 {
            return Ok((None, last_index));
        }
        if status.as_u16() != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(KvError::Http(format!(
                "Unexpected response code: {} ({body})",
                status.as_u16()
            )));
        }

        let pairs: Vec<ConsulKvPair> = resp
            .json()
            .await
            .map_err(|e| KvError::Http(format!("decode consul get response: {e}")))?;
        Ok((pairs.into_iter().next(), last_index))
    }

    async fn list_raw(
        &self,
        prefix: &str,
        wait_index: Option<u64>,
        cancel: Option<&ServiceContext>,
    ) -> Result<(Vec<ConsulKvPair>, u64), KvError> {
        let start = Instant::now();
        let res = self.list_raw_untracked(prefix, wait_index, cancel).await;
        self.observe_op("List", res.is_ok(), start);
        res
    }

    async fn list_raw_untracked(
        &self,
        prefix: &str,
        wait_index: Option<u64>,
        cancel: Option<&ServiceContext>,
    ) -> Result<(Vec<ConsulKvPair>, u64), KvError> {
        let mut params = vec![("recurse", String::new())];
        if let Some(idx) = wait_index {
            if idx > 0 {
                params.push(("index", idx.to_string()));
            }
            params.push(("wait", dur_to_msec(self.long_poll_duration)));
        }
        self.push_read_consistency(&mut params);
        let url = self.build_kv_url(prefix, true, &params);

        let send_fut = self.http.get(&url).send();
        let resp = match cancel {
            Some(c) => {
                tokio::select! {
                    _ = c.canceled() => return Err(KvError::Canceled),
                    r = send_fut => r.map_err(|e| KvError::Http(e.to_string()))?,
                }
            }
            None => send_fut.await.map_err(|e| KvError::Http(e.to_string()))?,
        };

        let last_index = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let status = resp.status();
        if status.as_u16() == 404 {
            return Ok((Vec::new(), last_index));
        }
        if status.as_u16() != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(KvError::Http(format!(
                "Unexpected response code: {} ({body})",
                status.as_u16()
            )));
        }

        let pairs: Vec<ConsulKvPair> = resp
            .json()
            .await
            .map_err(|e| KvError::Http(format!("decode consul list response: {e}")))?;
        Ok((pairs, last_index))
    }

    async fn put_raw(&self, key: &str, bytes: Vec<u8>) -> Result<(), KvError> {
        let start = Instant::now();
        let res = self.put_with_params_untracked(key, &[], bytes).await.map(|_| ());
        self.observe_op("Put", res.is_ok(), start);
        res
    }

    async fn cas_raw(&self, key: &str, index: u64, bytes: Vec<u8>) -> Result<bool, KvError> {
        let start = Instant::now();
        let params = [("cas", index.to_string())];
        let res = self.put_with_params_untracked(key, &params, bytes).await;
        self.observe_op("CAS", res.is_ok(), start);
        res
    }

    async fn put_with_params_untracked(
        &self,
        key: &str,
        params: &[(&str, String)],
        bytes: Vec<u8>,
    ) -> Result<bool, KvError> {
        if key.starts_with('/') {
            return Err(KvError::Http(format!(
                "Invalid key. Key must not begin with a '/': {key}"
            )));
        }
        let url = self.build_kv_url(key, false, params);
        let resp = self
            .http
            .put(&url)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(|e| KvError::Http(e.to_string()))?;

        let status = resp.status();
        if status.as_u16() != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(KvError::Http(format!(
                "Unexpected response code: {} ({body})",
                status.as_u16()
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| KvError::Http(format!("Failed to read response: {e}")))?;
        Ok(body.contains("true"))
    }

    async fn delete_raw(&self, key: &str) -> Result<(), KvError> {
        let start = Instant::now();
        let res = self.delete_raw_untracked(key).await;
        self.observe_op("Delete", res.is_ok(), start);
        res
    }

    async fn delete_raw_untracked(&self, key: &str) -> Result<(), KvError> {
        let url = self.build_kv_url(key, true, &[]);
        let resp = self
            .http
            .delete(&url)
            .send()
            .await
            .map_err(|e| KvError::Http(e.to_string()))?;
        let status = resp.status();
        if status.as_u16() != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(KvError::Http(format!(
                "Unexpected response code: {} ({body})",
                status.as_u16()
            )));
        }
        let _ = resp
            .text()
            .await
            .map_err(|e| KvError::Http(format!("Failed to read response: {e}")))?;
        Ok(())
    }

    async fn cas_inner(&self, key: &str, mut f: CasFn) -> Result<(), KvError> {
        let retries = 10;
        let cas_delay: Duration = self.cfg.cas_retry_delay.as_std().unwrap_or(Duration::ZERO);
        let sleep_before_retry = if !cas_delay.is_zero() {
            let max_ns = cas_delay.as_nanos().min(u64::MAX as u128) as u64;
            Duration::from_nanos(rand::thread_rng().gen_range(0..max_ns.max(1)))
        } else {
            Duration::ZERO
        };

        let mut index;
        for i in 0..retries {
            if i > 0 && !sleep_before_retry.is_zero() {
                tokio::time::sleep(sleep_before_retry).await;
            }

            let (pair, _) = match self.get_raw(key, None, false, None).await {
                Ok(v) => v,
                Err(err) => {
                    error!(key, %err, "error getting key from consul");
                    continue;
                }
            };

            let decoded = match pair {
                Some(p) => {
                    index = p.modify_index;
                    match decode_consul_value(self.codec.as_ref(), p.value.as_deref()) {
                        Ok(v) => Some(v),
                        Err(err) => {
                            error!(key, %err, "error decoding consul key");
                            continue;
                        }
                    }
                }
                None => {
                    index = 0;
                    None
                }
            };

            let outcome = match f(decoded.as_deref()) {
                Ok(o) => o,
                Err(cb_err) => {
                    if !cb_err.retry {
                        return Err(cb_err.err);
                    }
                    continue;
                }
            };

            let new_val = match outcome {
                CasOutcome::NoChange => return Ok(()),
                CasOutcome::Update(v) => v,
            };

            let bytes = match self.codec.encode(new_val.as_ref()) {
                Ok(b) => b,
                Err(err) => {
                    error!(key, %err, "error serializing value");
                    continue;
                }
            };

            let ok = match self.cas_raw(key, index, bytes).await {
                Ok(ok) => ok,
                Err(err) => {
                    error!(key, %err, "error CASing consul key");
                    continue;
                }
            };
            if !ok {
                debug!(key, index, "error CASing, trying again");
                continue;
            }
            return Ok(());
        }

        Err(KvError::CasFailed {
            key: key.to_string(),
            reason: "failed to CAS".to_string(),
        })
    }
}

fn check_last_index(index: u64, meta_last_index: u64) -> (u64, bool) {
    if meta_last_index == 0 {
        return (1, false);
    }
    if meta_last_index < index {
        return (0, false);
    }
    if index == meta_last_index {
        return (meta_last_index, true);
    }
    (meta_last_index, false)
}

fn decode_consul_value(
    codec: &dyn Codec,
    val_b64: Option<&str>,
) -> Result<Box<dyn Any + Send + Sync>, KvError> {
    let raw = match val_b64 {
        Some(s) if !s.is_empty() => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| KvError::Codec(format!("base64 decode: {e}")))?,
        _ => Vec::new(),
    };
    codec.decode(&raw)
}

impl KvClient for ConsulKv {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let (pairs, _) = self.list_raw(prefix, None, None).await?;
            Ok(pairs.into_iter().map(|p| p.key).collect())
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let (pair, _) = self.get_raw(key, None, true, None).await?;
            match pair {
                None => Ok(None),
                Some(p) => Ok(Some(decode_consul_value(
                    self.codec.as_ref(),
                    p.value.as_deref(),
                )?)),
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move { self.delete_raw(key).await })
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self.cas_inner(key, f).await;
            self.observe_op("CAS loop", res.is_ok(), start);
            res
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut backoff = Backoff::new(BackoffConfig {
                min_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
                max_retries: 0,
            });
            let mut index = 0u64;

            while backoff.ongoing() && !cancel.is_canceled() {
                if let Some(lim) = &self.limiter {
                    if !lim.wait(&cancel).await {
                        return;
                    }
                }

                let res = self.get_raw(key, Some(index), true, Some(&cancel)).await;
                if cancel.is_canceled() {
                    return;
                }

                let (pair, meta_last_index) = match res {
                    Ok(v) => {
                        backoff.reset();
                        v
                    }
                    Err(err) => {
                        error!(key, %err, "error getting path from consul");
                        tokio::select! {
                            _ = cancel.canceled() => return,
                            _ = backoff.wait() => continue,
                        }
                    }
                };

                let (new_index, skip) = check_last_index(index, meta_last_index);
                index = new_index;
                if skip {
                    continue;
                }

                let Some(p) = pair else {
                    continue;
                };

                match decode_consul_value(self.codec.as_ref(), p.value.as_deref()) {
                    Ok(val) => {
                        if !f(val) {
                            return;
                        }
                    }
                    Err(err) => {
                        error!(key, %err, "error decoding consul key");
                    }
                }
            }
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut backoff = Backoff::new(BackoffConfig {
                min_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
                max_retries: 0,
            });
            let mut index = 0u64;

            while backoff.ongoing() && !cancel.is_canceled() {
                if let Some(lim) = &self.limiter {
                    if !lim.wait(&cancel).await {
                        return;
                    }
                }

                let res = self.list_raw(prefix, Some(index), Some(&cancel)).await;
                if cancel.is_canceled() {
                    return;
                }

                let (pairs, meta_last_index) = match res {
                    Ok(v) => {
                        backoff.reset();
                        v
                    }
                    Err(err) => {
                        error!(prefix, %err, "error listing prefix from consul");
                        tokio::select! {
                            _ = cancel.canceled() => return,
                            _ = backoff.wait() => continue,
                        }
                    }
                };

                let (new_index, skip) = check_last_index(index, meta_last_index);
                if skip {
                    continue;
                }

                for kvp in pairs {
                    if index > 0 && kvp.modify_index <= index && kvp.create_index <= index {
                        continue;
                    }
                    match decode_consul_value(self.codec.as_ref(), kvp.value.as_deref()) {
                        Ok(val) => {
                            if !f(&kvp.key, val) {
                                return;
                            }
                        }
                        Err(err) => {
                            error!(prefix, key = %kvp.key, %err, "error decoding value for prefix");
                        }
                    }
                }

                index = new_index;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{cas_typed, get_typed, registerer_with_kv_name, StringCodec};
    use axum::body::Bytes;
    use axum::extract::{Request, State};
    use axum::http::{HeaderMap as AxumHeaders, Method, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::Router;
    use mimir_config::duration::GoDuration;
    use parking_lot::Mutex as SyncMutex;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio_rustls::rustls::pki_types::PrivateKeyDer;
    use tokio_rustls::rustls::server::WebPkiClientVerifier;
    use tokio_rustls::TlsAcceptor;

    fn tls_fixture(name: &str) -> String {
        format!(
            "{}/../mimir-storage/tests/fixtures/kafka_tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[derive(Clone, Serialize)]
    #[serde(rename_all = "PascalCase")]
    struct MockPairOut {
        key: String,
        value: String,
        create_index: u64,
        modify_index: u64,
    }

    #[derive(Clone)]
    struct StoredEntry {
        key: String,
        value: Vec<u8>,
        create_index: u64,
        modify_index: u64,
    }

    struct MockConsulInner {
        current: u64,
        store: HashMap<String, StoredEntry>,
        required_token: Option<String>,
        required_basic_auth: Option<String>,
        fail_next_cas: bool,
        recorded: Vec<String>,
    }

    #[derive(Clone)]
    struct MockConsulState {
        inner: Arc<SyncMutex<MockConsulInner>>,
        notify: Arc<Notify>,
        max_wait: Duration,
    }

    impl MockConsulState {
        fn new() -> Self {
            Self {
                inner: Arc::new(SyncMutex::new(MockConsulInner {
                    current: 1,
                    store: HashMap::new(),
                    required_token: None,
                    required_basic_auth: None,
                    fail_next_cas: false,
                    recorded: Vec::new(),
                })),
                notify: Arc::new(Notify::new()),
                max_wait: Duration::from_millis(80),
            }
        }

        fn reset_index(&self) {
            self.inner.lock().current = 0;
            self.notify.notify_waiters();
        }

        fn reset_index_for_key(&self, key: &str) {
            if let Some(ent) = self.inner.lock().store.get_mut(key) {
                ent.modify_index = 0;
            }
            self.notify.notify_waiters();
        }

        fn recorded(&self) -> Vec<String> {
            self.inner.lock().recorded.clone()
        }
    }

    fn percent_decode_path(p: &str) -> String {
        let bytes = p.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                if let Ok(v) = u8::from_str_radix(&p[i + 1..i + 3], 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn parse_query_map(q: &str) -> HashMap<String, String> {
        let mut out = HashMap::new();
        if q.is_empty() {
            return out;
        }
        for pair in q.split('&') {
            match pair.split_once('=') {
                Some((k, v)) => {
                    out.insert(percent_decode_path(k), percent_decode_path(v));
                }
                None => {
                    out.insert(percent_decode_path(pair), String::new());
                }
            }
        }
        out
    }

    async fn mock_consul_handler(
        State(st): State<MockConsulState>,
        headers: AxumHeaders,
        req: Request,
    ) -> Response {
        let method = req.method().clone();
        let raw_path = req.uri().path().to_string();
        let raw_query = req.uri().query().unwrap_or("").to_string();
        let tok = headers
            .get("x-consul-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let auth = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        {
            let mut inner = st.inner.lock();
            inner
                .recorded
                .push(format!("{method} {raw_path}?{raw_query}"));
            if let Some(req_tok) = &inner.required_token {
                if &tok != req_tok {
                    return (StatusCode::FORBIDDEN, "Permission denied").into_response();
                }
            }
            if let Some(req_auth) = &inner.required_basic_auth {
                if &auth != req_auth {
                    return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
                }
            }
        }

        let decoded_path = percent_decode_path(&raw_path);
        let Some(key) = decoded_path
            .strip_prefix("/v1/kv/")
            .or_else(|| decoded_path.strip_prefix("/consul-proxy/v1/kv/"))
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let key = key.to_string();
        let q = parse_query_map(&raw_query);

        match method {
            Method::GET => {
                let wait_index = q
                    .get("index")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let has_wait = q.contains_key("wait");
                let has_recurse = q.contains_key("recurse");

                if has_recurse {
                    if has_wait {
                        let deadline = tokio::time::Instant::now() + st.max_wait;
                        loop {
                            let notified = st.notify.notified();
                            let cur = st.inner.lock().current;
                            if wait_index < cur || tokio::time::Instant::now() >= deadline {
                                break;
                            }
                            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                                let idx_str = wait_index.to_string();
                                return (
                                    StatusCode::NOT_FOUND,
                                    [("X-Consul-Index", idx_str)],
                                    "",
                                )
                                    .into_response();
                            }
                        }
                    }
                    let (list, cur) = {
                        let inner = st.inner.lock();
                        let mut out: Vec<MockPairOut> = inner
                            .store
                            .values()
                            .filter(|e| e.key.starts_with(&key))
                            .map(|e| MockPairOut {
                                key: e.key.clone(),
                                value: base64::engine::general_purpose::STANDARD.encode(&e.value),
                                create_index: e.create_index,
                                modify_index: e.modify_index,
                            })
                            .collect();
                        out.sort_by(|a, b| a.key.cmp(&b.key));
                        (out, inner.current)
                    };
                    let idx_str = cur.to_string();
                    if list.is_empty() {
                        return (StatusCode::NOT_FOUND, [("X-Consul-Index", idx_str)], "")
                            .into_response();
                    }
                    let body = serde_json::to_string(&list).unwrap();
                    return (
                        StatusCode::OK,
                        [
                            ("X-Consul-Index", idx_str),
                            ("Content-Type", "application/json".to_string()),
                        ],
                        body,
                    )
                        .into_response();
                }

                // Single-key Get ( mirrors dskit/kv/consul/mock.go ).
                {
                    let inner = st.inner.lock();
                    if !inner.store.contains_key(&key) && wait_index == 0 {
                        let idx_str = inner.current.to_string();
                        return (StatusCode::NOT_FOUND, [("X-Consul-Index", idx_str)], "")
                            .into_response();
                    }
                }

                let start_modify = {
                    let inner = st.inner.lock();
                    inner
                        .store
                        .get(&key)
                        .map(|e| e.modify_index)
                        .unwrap_or(inner.current)
                };

                if wait_index >= start_modify && has_wait {
                    let deadline = tokio::time::Instant::now() + st.max_wait;
                    loop {
                        let notified = st.notify.notified();
                        let now_mod = {
                            let inner = st.inner.lock();
                            inner
                                .store
                                .get(&key)
                                .map(|e| e.modify_index)
                                .unwrap_or(start_modify)
                        };
                        if now_mod != start_modify {
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline
                            || tokio::time::timeout_at(deadline, notified).await.is_err()
                        {
                            let idx_str = wait_index.to_string();
                            let maybe_entry = st.inner.lock().store.get(&key).cloned();
                            return match maybe_entry {
                                None => (
                                    StatusCode::NOT_FOUND,
                                    [("X-Consul-Index", idx_str)],
                                    "",
                                )
                                    .into_response(),
                                Some(ent) => {
                                    let body = serde_json::to_string(&[MockPairOut {
                                        key: ent.key,
                                        value: base64::engine::general_purpose::STANDARD
                                            .encode(&ent.value),
                                        create_index: ent.create_index,
                                        modify_index: ent.modify_index,
                                    }])
                                    .unwrap();
                                    (
                                        StatusCode::OK,
                                        [
                                            ("X-Consul-Index", idx_str),
                                            ("Content-Type", "application/json".to_string()),
                                        ],
                                        body,
                                    )
                                        .into_response()
                                }
                            };
                        }
                    }
                }

                let (maybe_ent, cur) = {
                    let inner = st.inner.lock();
                    (inner.store.get(&key).cloned(), inner.current)
                };
                match maybe_ent {
                    None => (
                        StatusCode::NOT_FOUND,
                        [("X-Consul-Index", cur.to_string())],
                        "",
                    )
                        .into_response(),
                    Some(ent) => {
                        let idx_str = ent.modify_index.to_string();
                        let body = serde_json::to_string(&[MockPairOut {
                            key: ent.key,
                            value: base64::engine::general_purpose::STANDARD.encode(&ent.value),
                            create_index: ent.create_index,
                            modify_index: ent.modify_index,
                        }])
                        .unwrap();
                        (
                            StatusCode::OK,
                            [
                                ("X-Consul-Index", idx_str),
                                ("Content-Type", "application/json".to_string()),
                            ],
                            body,
                        )
                            .into_response()
                    }
                }
            }
            Method::PUT => {
                let body: Bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let mut inner = st.inner.lock();
                if let Some(cas_str) = q.get("cas") {
                    let cas_idx = cas_str.parse::<u64>().unwrap_or(0);
                    if inner.fail_next_cas {
                        inner.fail_next_cas = false;
                        return (StatusCode::OK, "false").into_response();
                    }
                    let existing = inner.store.get(&key);
                    let conflict = match existing {
                        None => cas_idx != 0,
                        Some(ent) => ent.modify_index != cas_idx,
                    };
                    if conflict {
                        return (StatusCode::OK, "false").into_response();
                    }
                }
                inner.current += 1;
                let idx = inner.current;
                match inner.store.get_mut(&key) {
                    Some(ent) => {
                        ent.value = body.to_vec();
                        ent.modify_index = idx;
                    }
                    None => {
                        inner.store.insert(
                            key.clone(),
                            StoredEntry {
                                key,
                                value: body.to_vec(),
                                create_index: idx,
                                modify_index: idx,
                            },
                        );
                    }
                }
                drop(inner);
                st.notify.notify_waiters();
                (StatusCode::OK, "true").into_response()
            }
            Method::DELETE => {
                let mut inner = st.inner.lock();
                inner.store.remove(&key);
                inner.current += 1;
                drop(inner);
                st.notify.notify_waiters();
                (StatusCode::OK, "true").into_response()
            }
            _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    async fn spawn_http_mock(st: MockConsulState) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(mock_consul_handler)
            .with_state(st);
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, handle)
    }

    async fn spawn_https_mock(
        st: MockConsulState,
        cert_file: &str,
        key_file: &str,
        client_ca_file: Option<&str>,
    ) -> (SocketAddr, Vec<tokio::task::JoinHandle<()>>) {
        let (plain_addr, plain_handle) = spawn_http_mock(st).await;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let cert_pem = std::fs::read(cert_file).unwrap();
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
            .map(|c| c.unwrap())
            .collect();
        let key_pem = std::fs::read(key_file).unwrap();
        let key = PrivateKeyDer::from_pem_slice(&key_pem).unwrap();

        let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap();
        let server_cfg = if let Some(ca_path) = client_ca_file {
            let ca_store = Arc::new(load_ca_file(ca_path).unwrap());
            let verifier = WebPkiClientVerifier::builder_with_provider(ca_store, provider)
                .build()
                .unwrap();
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .unwrap()
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap()
        };

        let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let tls_handle = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls_stream) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let Ok(mut backend) = tokio::net::TcpStream::connect(plain_addr).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut tls_stream, &mut backend).await;
                });
            }
        });
        (addr, vec![plain_handle, tls_handle])
    }

    async fn observe_values_for(client: &ConsulKv, key: &str, dur: Duration) -> Vec<String> {
        let cancel = ServiceContext::background();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(dur).await;
            cancel_clone.cancel();
        });
        let observed = Arc::new(SyncMutex::new(Vec::new()));
        let obs_cb = Arc::clone(&observed);
        client
            .watch_key(
                key,
                cancel,
                Box::new(move |val| {
                    let Some(s) = val.downcast_ref::<String>() else {
                        return false;
                    };
                    obs_cb.lock().push(s.clone());
                    true
                }),
            )
            .await;
        let out = observed.lock().clone();
        out
    }

    // Upstream: github.com/grafana/dskit/kv/consul/client_test.go::TestWatchKeyWithRateLimit
    #[tokio::test]
    async fn test_watch_key_with_rate_limit() {
        let st = MockConsulState::new();
        let (addr, server) = spawn_http_mock(st).await;
        let cfg = ConsulConfig {
            host: addr.to_string(),
            watch_rate_limit: 5.0,
            watch_burst_size: 1,
            ..Default::default()
        };
        let c = Arc::new(ConsulKv::new(cfg, Arc::new(StringCodec), None).unwrap());
        let writer_client = Arc::clone(&c);
        let writer = tokio::spawn(async move {
            for i in 0..=100 {
                let v = i.to_string();
                let _ = writer_client.put("test", &v).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let observed = observe_values_for(&c, "test", Duration::from_millis(1200)).await;
        writer.await.unwrap();
        server.abort();

        assert!(
            (4..=11).contains(&observed.len()),
            "expected ~6 observed values with 5/s rate limit, got {observed:?}"
        );
        let last: i32 = observed.last().unwrap().parse().unwrap();
        assert!(
            last >= 50,
            "expected high last observed value, got {observed:?}"
        );
    }

    // Upstream: github.com/grafana/dskit/kv/consul/client_test.go::TestWatchKeyNoRateLimit
    #[tokio::test]
    async fn test_watch_key_no_rate_limit() {
        let st = MockConsulState::new();
        let (addr, server) = spawn_http_mock(st).await;
        let cfg = ConsulConfig {
            host: addr.to_string(),
            watch_rate_limit: 0.0,
            ..Default::default()
        };
        let c = Arc::new(ConsulKv::new(cfg, Arc::new(StringCodec), None).unwrap());
        let writer_client = Arc::clone(&c);
        let writer = tokio::spawn(async move {
            for i in 0..=60 {
                let v = i.to_string();
                let _ = writer_client.put("test", &v).await;
                tokio::time::sleep(Duration::from_millis(4)).await;
            }
        });

        let observed = observe_values_for(&c, "test", Duration::from_millis(600)).await;
        writer.await.unwrap();
        server.abort();

        assert!(
            observed.len() >= 25,
            "expected many observed values without rate limit, got {} ({observed:?})",
            observed.len()
        );
    }

    // Upstream: github.com/grafana/dskit/kv/consul/client_test.go::TestReset
    #[tokio::test]
    async fn test_reset() {
        let st = MockConsulState::new();
        let (addr, server) = spawn_http_mock(st.clone()).await;
        let cfg = ConsulConfig {
            host: addr.to_string(),
            watch_rate_limit: 0.0,
            ..Default::default()
        };
        let c = Arc::new(ConsulKv::new(cfg, Arc::new(StringCodec), None).unwrap());
        let writer_client = Arc::clone(&c);
        let writer_st = st.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            for i in 0..=5 {
                let v = i.to_string();
                let _ = writer_client.put("test", &v).await;
                if i == 1 {
                    writer_st.reset_index();
                }
                if i == 2 {
                    writer_st.reset_index_for_key("test");
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        });

        let observed = observe_values_for(&c, "test", Duration::from_millis(350)).await;
        writer.await.unwrap();
        server.abort();

        assert!(
            observed.len() >= 4,
            "expected updates across index resets, got {observed:?}"
        );
        assert_eq!(observed.last().map(String::as_str), Some("5"));
    }

    // Upstream: github.com/grafana/dskit/kv/consul/client_test.go::TestWatchKeyWithNoStartValue
    #[tokio::test]
    async fn test_watch_key_with_no_start_value() {
        let st = MockConsulState::new();
        let (addr, server) = spawn_http_mock(st).await;
        let cfg = ConsulConfig {
            host: addr.to_string(),
            watch_rate_limit: 0.0,
            ..Default::default()
        };
        let c = Arc::new(ConsulKv::new(cfg, Arc::new(StringCodec), None).unwrap());
        let writer_client = Arc::clone(&c);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            writer_client
                .put("test", &"start".to_string())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            writer_client.put("test", &"end".to_string()).await.unwrap();
        });

        let cancel = ServiceContext::background();
        let cancel_timer = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            cancel_timer.cancel();
        });
        let seen = Arc::new(SyncMutex::new(Vec::new()));
        let seen_cb = Arc::clone(&seen);
        c.watch_key(
            "test",
            cancel,
            Box::new(move |val| {
                let s = val.downcast_ref::<String>().unwrap().clone();
                let mut g = seen_cb.lock();
                g.push(s);
                g.len() != 2
            }),
        )
        .await;
        server.abort();

        assert_eq!(*seen.lock(), vec!["start".to_string(), "end".to_string()]);
    }

    #[tokio::test]
    async fn test_url_encoding_special_keys_subpath_and_query_params() {
        let st = MockConsulState::new();
        let (addr, server) = spawn_http_mock(st.clone()).await;

        let cfg = ConsulConfig {
            host: format!("http://{addr}/consul-proxy"),
            consistent_reads: true,
            watch_rate_limit: 0.0,
            cas_retry_delay: GoDuration::from_millis(10),
            ..Default::default()
        };
        let c = ConsulKv::new(cfg, Arc::new(StringCodec), None).unwrap();

        // Key with '#', '?', '%', and space MUST be percent-encoded so '#1/ring?x=1' is never
        // stripped as a URL fragment or query string, and '?cas=0' stays intact!
        let tricky_key = "collectors#1/ring?x=1%25 y";
        cas_typed::<String, _>(&c, tricky_key, |cur| {
            assert!(cur.is_none());
            Ok(Some(("val-1".to_string(), true)))
        })
        .await
        .unwrap();

        let got: Option<String> = get_typed(&c, tricky_key).await.unwrap();
        assert_eq!(got.as_deref(), Some("val-1"));

        let listed = c.list("collectors#1/").await.unwrap();
        assert_eq!(listed, vec![tricky_key.to_string()]);

        // Exercise initial WatchKey (index == 0 -> omitted!) + subsequent poll (index > 0 -> sent!).
        let cancel = ServiceContext::background();
        c.watch_key(
            tricky_key,
            cancel,
            Box::new(|v| {
                assert_eq!(v.downcast_ref::<String>().map(String::as_str), Some("val-1"));
                false
            }),
        )
        .await;

        let reqs = st.recorded();
        // CAS internal Get (no stale/consistent), CAS Put (?cas=0), Get (?consistent=), List (?consistent=&recurse=), WatchKey (?consistent=&wait=10000ms)
        assert_eq!(
            reqs[0],
            "GET /consul-proxy/v1/kv/collectors%231/ring%3Fx=1%2525%20y?"
        );
        assert_eq!(
            reqs[1],
            "PUT /consul-proxy/v1/kv/collectors%231/ring%3Fx=1%2525%20y?cas=0"
        );
        assert_eq!(
            reqs[2],
            "GET /consul-proxy/v1/kv/collectors%231/ring%3Fx=1%2525%20y?consistent="
        );
        assert_eq!(
            reqs[3],
            "GET /consul-proxy/v1/kv/collectors%231/?consistent=&recurse="
        );
        assert_eq!(
            reqs[4],
            "GET /consul-proxy/v1/kv/collectors%231/ring%3Fx=1%2525%20y?consistent=&wait=10000ms"
        );

        server.abort();
    }

    #[test]
    fn test_unknown_scheme_rejected_at_startup() {
        for bad in [
            "ftp://127.0.0.1:8500",
            "gopher://127.0.0.1:8500",
            "unix:///tmp/consul.sock",
        ] {
            let cfg = ConsulConfig {
                host: bad.to_string(),
                ..Default::default()
            };
            let err = match ConsulKv::new(cfg, Arc::new(StringCodec), None) {
                Ok(_) => panic!("expected invalid scheme {bad} to fail"),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains("Unknown protocol scheme:"),
                "unexpected error for {bad}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn test_consul_env_vars_and_mtls() {
        let tmp = tempfile::tempdir().unwrap();
        let tok_file = tmp.path().join("consul.token");
        std::fs::write(&tok_file, "  secret-from-file \n").unwrap();

        let st = MockConsulState::new();
        {
            let mut g = st.inner.lock();
            g.required_token = Some("secret-from-file".to_string());
            g.required_basic_auth = Some(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("alice:s3cr3t")
            ));
        }

        let (tls_addr, handles) = spawn_https_mock(
            st.clone(),
            &tls_fixture("server.crt"),
            &tls_fixture("server.key"),
            Some(&tls_fixture("ca.crt")),
        )
        .await;

        let env = ConsulEnv {
            http_addr: tls_addr.to_string(),
            token: "fallback-token".to_string(),
            token_file: tok_file.to_string_lossy().into_owned(),
            http_auth: "alice:s3cr3t".to_string(),
            http_ssl: "true".to_string(),
            ca_cert: tls_fixture("ca.crt"),
            client_cert: tls_fixture("client.crt"),
            client_key: tls_fixture("client.key"),
            namespace: "team-a".to_string(),
            partition: "part-1".to_string(),
            ..Default::default()
        };
        let cfg = ConsulConfig {
            host: String::new(), // falls back to CONSUL_HTTP_ADDR
            ..Default::default()
        };
        let c = ConsulKv::with_registerer_and_env(cfg.clone(), Arc::new(StringCodec), None, &env)
            .unwrap();
        c.put("k1", &"v1".to_string()).await.unwrap();
        let got: Option<String> = get_typed(&c, "k1").await.unwrap();
        assert_eq!(got.as_deref(), Some("v1"));

        let reqs = st.recorded();
        assert_eq!(reqs[0], "PUT /v1/kv/k1?ns=team-a&partition=part-1");
        assert_eq!(reqs[1], "GET /v1/kv/k1?ns=team-a&partition=part-1&stale=");

        // Without client cert against mTLS server -> handshake must fail.
        let mut no_client_cert_env = env.clone();
        no_client_cert_env.client_cert.clear();
        no_client_cert_env.client_key.clear();
        let c_no_mtls = ConsulKv::with_registerer_and_env(
            cfg.clone(),
            Arc::new(StringCodec),
            None,
            &no_client_cert_env,
        )
        .unwrap();
        assert!(c_no_mtls.get("k1").await.is_err());

        // Mismatched client cert/key pairing (only cert set) -> startup error!
        let mut half_cert_env = env.clone();
        half_cert_env.client_key.clear();
        let err = match ConsulKv::with_registerer_and_env(
            cfg.clone(),
            Arc::new(StringCodec),
            None,
            &half_cert_env,
        ) {
            Ok(_) => panic!("expected error when client_key is missing"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("both client cert and client key must be provided"));

        for h in handles {
            h.abort();
        }

        // Test CONSUL_TLS_SERVER_NAME + CONSUL_CAPATH + CONSUL_HTTP_SSL_VERIFY=false against sni.crt
        let ca_dir = tmp.path().join("capath");
        std::fs::create_dir_all(&ca_dir).unwrap();
        std::fs::copy(tls_fixture("ca.crt"), ca_dir.join("root.pem")).unwrap();

        let st2 = MockConsulState::new();
        let (sni_addr, sni_handles) = spawn_https_mock(
            st2,
            &tls_fixture("sni.crt"),
            &tls_fixture("sni.key"),
            None,
        )
        .await;

        // Dialing 127.0.0.1 against sni.crt (DNS:kafka.mimir-rs.test only) fails without CONSUL_TLS_SERVER_NAME:
        let env_no_sni = ConsulEnv {
            http_ssl: "true".to_string(),
            ca_path: ca_dir.to_string_lossy().into_owned(),
            ..Default::default()
        };
        let sni_cfg = ConsulConfig {
            host: sni_addr.to_string(),
            ..Default::default()
        };
        let c_no_sni = ConsulKv::with_registerer_and_env(
            sni_cfg.clone(),
            Arc::new(StringCodec),
            None,
            &env_no_sni,
        )
        .unwrap();
        assert!(c_no_sni.get("k").await.is_err());

        // Succeeds with CONSUL_TLS_SERVER_NAME=kafka.mimir-rs.test:8501:
        let mut env_with_sni = env_no_sni.clone();
        env_with_sni.tls_server_name = "kafka.mimir-rs.test:8501".to_string();
        let c_with_sni = ConsulKv::with_registerer_and_env(
            sni_cfg.clone(),
            Arc::new(StringCodec),
            None,
            &env_with_sni,
        )
        .unwrap();
        assert_eq!(get_typed::<String>(&c_with_sni, "k").await.unwrap(), None);

        // Succeeds with wrong CA (other-ca.crt) when CONSUL_HTTP_SSL_VERIFY=false:
        let env_skip_verify = ConsulEnv {
            http_ssl: "true".to_string(),
            ca_cert: tls_fixture("other-ca.crt"),
            http_ssl_verify: "false".to_string(),
            ..Default::default()
        };
        let c_skip =
            ConsulKv::with_registerer_and_env(sni_cfg, Arc::new(StringCodec), None, &env_skip_verify)
                .unwrap();
        assert_eq!(get_typed::<String>(&c_skip, "k").await.unwrap(), None);

        for h in sni_handles {
            h.abort();
        }
    }

    #[tokio::test]
    async fn test_consul_request_duration_metrics_all_operations() {
        let st = MockConsulState::new();
        st.inner.lock().fail_next_cas = true; // force 1 CAS conflict retry!
        let (addr, server) = spawn_http_mock(st).await;

        let reg = Registry::new();
        let cortex = Registerer::new(&reg).wrap_with_prefix("cortex_");
        let kv_reg = registerer_with_kv_name(Some(&cortex), "ingester-lifecycler");

        let cfg = ConsulConfig {
            host: addr.to_string(),
            watch_rate_limit: 0.0,
            cas_retry_delay: GoDuration::from_millis(5),
            ..Default::default()
        };
        let c = ConsulKv::with_registerer(cfg, Arc::new(StringCodec), kv_reg.as_ref()).unwrap();

        // 1. CAS (1 CAS loop -> 2 internal Gets + 2 CAS HTTP PUTs because first returns false)
        cas_typed::<String, _>(&c, "ring", |_| Ok(Some(("v1".to_string(), true))))
            .await
            .unwrap();
        // 2. Get (1 Get)
        assert_eq!(
            get_typed::<String>(&c, "ring").await.unwrap().as_deref(),
            Some("v1")
        );
        // 3. List (1 List)
        assert_eq!(c.list("").await.unwrap(), vec!["ring".to_string()]);
        // 4. WatchKey (1 Get, stops immediately when callback returns false)
        c.watch_key("ring", ServiceContext::background(), Box::new(|_| false))
            .await;
        // 5. WatchPrefix (1 List, stops immediately when callback returns false)
        c.watch_prefix("", ServiceContext::background(), Box::new(|_, _| false))
            .await;
        // 6. Put (1 Put) + Delete (1 Delete)
        c.put("extra", &"v2".to_string()).await.unwrap();
        c.delete("ring").await.unwrap();

        server.abort();

        let mut counts: HashMap<(String, String, String), u64> = HashMap::new();
        for mf in reg.gather() {
            if mf.get_name() != "cortex_consul_request_duration_seconds" {
                continue;
            }
            for m in mf.get_metric() {
                let mut kv_name = String::new();
                let mut op = String::new();
                let mut code = String::new();
                for lp in m.get_label() {
                    match lp.get_name() {
                        "kv_name" => kv_name = lp.get_value().to_string(),
                        "operation" => op = lp.get_value().to_string(),
                        "status_code" => code = lp.get_value().to_string(),
                        _ => {}
                    }
                }
                counts.insert((kv_name, op, code), m.get_histogram().get_sample_count());
            }
        }

        let get_count = |op: &str| {
            counts
                .get(&(
                    "ingester-lifecycler".to_string(),
                    op.to_string(),
                    "200".to_string(),
                ))
                .copied()
                .unwrap_or(0)
        };
        assert_eq!(get_count("CAS loop"), 1);
        assert_eq!(get_count("CAS"), 2);
        // 2 internal CAS Gets + 1 explicit Get + 1 WatchKey Get = 4
        assert_eq!(get_count("Get"), 4);
        // 1 explicit List + 1 WatchPrefix List = 2
        assert_eq!(get_count("List"), 2);
        assert_eq!(get_count("Put"), 1);
        assert_eq!(get_count("Delete"), 1);
    }
}
