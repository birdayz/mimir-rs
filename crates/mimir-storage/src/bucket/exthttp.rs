//! HTTP transport settings for the object storage clients (`blocks_storage.<backend>.http.*`).
//!
//! Ported from:
//! - github.com/thanos-io/objstore/exthttp/transport.go (`DefaultTransport`)
//! - github.com/thanos-io/objstore/exthttp/tlsconfig.go (`NewTLSConfig`)
//! - github.com/grafana/mimir/pkg/storage/bucket/common/http.go (`HTTPConfig.ToExtHTTP`)
//!
//! Go builds one `http.Transport` per bucket client from these settings. `object_store` 0.11
//! builds its own `reqwest` client from [`ClientOptions`], so this module maps every setting
//! that can be expressed there. Settings that cannot be honoured are rejected at config
//! validation time (`{Azure,GCS}Config::validate_mimir_rs_support`), not here.
//!
//! Mapping (Go `http.Transport` field -> reqwest/object_store):
//! - `DialContext` 30s dial timeout + `TLSHandshakeTimeout` -> one connect timeout covering TCP
//!   connect and TLS handshake (`30s + tls_handshake_timeout`; no limit when the handshake timeout
//!   is <= 0, as in Go where the dial is then still bounded by the OS connect timeout).
//! - `IdleConnTimeout` -> pool idle timeout (<= 0 means "no limit" in Go).
//! - `MaxIdleConnsPerHost` / `MaxIdleConns` -> per-host idle pool size. Go uses
//!   `DefaultMaxIdleConnsPerHost` (2) when the per-host value is 0, keeps no idle connection when it
//!   is negative, and additionally caps the idle pool across hosts by `MaxIdleConns` (0 = no cap).
//!   A bucket client talks to a single storage host, so the effective per-host limit is
//!   `min(per_host, max_idle)`.
//! - `InsecureSkipVerify` -> accept invalid certificates (rustls: any certificate and hostname,
//!   like Go).
//! - `TLSConfig.CAFile` -> replaces the system root certificates via `reqwest_client`
//!   (`.tls_built_in_root_certs(false)`, used by both GCS and Azure bucket clients, matching Go
//!   `exthttp.updateRootCA`). `client_options` is retained only for legacy `object_store` 0.11
//!   helpers where `ClientOptions::with_root_certificate` appends rather than replaces.
//! - `ForceAttemptHTTP2` -> HTTP/2 via ALPN. Go disables HTTP/2 when a custom `TLSClientConfig`
//!   is set (always the case here) unless `ForceAttemptHTTP2` is true, so the default is HTTP/1.1.
//! - `ExpectContinueTimeout` only applies to requests carrying `Expect: 100-continue`; neither the
//!   Azure nor the GCS Go client sends that header, so it has no observable effect.
//! - `ResponseHeaderTimeout` cannot be expressed with `object_store` 0.11 (no per-response-header
//!   timeout and no way to inject a custom client); tracked in PORT_STATUS.md.
//! - `Proxy: http.ProxyFromEnvironment` -> reqwest's default environment proxy handling.
//! - Go `http.Client` has no total request timeout unless the provider sets one (GCS uses
//!   `idle_conn_timeout`), so `object_store`'s default 30s request / 5s connect timeouts must
//!   never be used.

use std::time::Duration;

use mimir_config::{BucketHTTPConfig, GoDuration};
use object_store::{Certificate, ClientOptions};

use super::BucketError;

/// Go `net.Dialer.Timeout` used by `exthttp.DefaultTransport`.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `http.DefaultMaxIdleConnsPerHost`.
pub const GO_DEFAULT_MAX_IDLE_CONNS_PER_HOST: usize = 2;

/// Stand-in for "no limit" where `object_store` only accepts a concrete duration (100 years).
const NO_LIMIT: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

/// Positive Go duration as std duration; `None` for zero/negative ("no limit" in `net/http`).
fn positive(d: GoDuration) -> Option<Duration> {
    if d.0 > 0 {
        d.as_std()
    } else {
        None
    }
}

/// Connect timeout covering TCP dial (Go: 30s) and TLS handshake (Go: `tls_handshake_timeout`).
pub fn connect_timeout(cfg: &BucketHTTPConfig) -> Option<Duration> {
    positive(cfg.tls_handshake_timeout).map(|tls| DIAL_TIMEOUT + tls)
}

/// Idle connection timeout (`None` = connections never expire, Go `IdleConnTimeout <= 0`).
pub fn pool_idle_timeout(cfg: &BucketHTTPConfig) -> Option<Duration> {
    positive(cfg.idle_conn_timeout)
}

/// Effective number of idle connections kept for the (single) storage host.
pub fn max_idle_per_host(cfg: &BucketHTTPConfig) -> usize {
    let per_host = match cfg.max_idle_connections_per_host {
        0 => GO_DEFAULT_MAX_IDLE_CONNS_PER_HOST,
        n if n < 0 => 0,
        n => n as usize,
    };
    match cfg.max_idle_connections {
        0 => per_host,
        n if n < 0 => 0,
        n => per_host.min(n as usize),
    }
}

/// Reads `tls_ca_path` like Go `exthttp.NewTLSConfig` (same error texts). Returns the PEM bytes.
pub fn read_ca_file(cfg: &BucketHTTPConfig) -> Result<Option<Vec<u8>>, BucketError> {
    if cfg.tls_ca_path.is_empty() {
        return Ok(None);
    }
    let data = std::fs::read(&cfg.tls_ca_path).map_err(|e| {
        BucketError::Config(format!(
            "unable to load specified CA cert {}: {}",
            cfg.tls_ca_path, e
        ))
    })?;
    Ok(Some(data))
}

fn unusable_ca(cfg: &BucketHTTPConfig) -> BucketError {
    BucketError::Config(format!("unable to use specified CA cert {}", cfg.tls_ca_path))
}

/// Maps the bucket HTTP config onto `object_store` client options (Go `exthttp.DefaultTransport`
/// plus the provider's `http.Client`). `total_timeout` is the Go `http.Client.Timeout` of the
/// provider (`None` = no total timeout).
pub fn client_options(
    cfg: &BucketHTTPConfig,
    total_timeout: Option<Duration>,
) -> Result<ClientOptions, BucketError> {
    let mut opts = ClientOptions::new();
    opts = match total_timeout {
        Some(t) => opts.with_timeout(t),
        None => opts.with_timeout_disabled(),
    };
    opts = match connect_timeout(cfg) {
        Some(t) => opts.with_connect_timeout(t),
        None => opts.with_connect_timeout_disabled(),
    };
    opts = opts
        .with_pool_idle_timeout(pool_idle_timeout(cfg).unwrap_or(NO_LIMIT))
        .with_pool_max_idle_per_host(max_idle_per_host(cfg))
        .with_allow_invalid_certificates(cfg.insecure_skip_verify);
    if cfg.force_attempt_http2 {
        opts = opts.with_allow_http2();
    } else {
        opts = opts.with_http1_only();
    }
    if let Some(pem) = read_ca_file(cfg)? {
        let certs = Certificate::from_pem_bundle(&pem).map_err(|_| unusable_ca(cfg))?;
        if certs.is_empty() {
            return Err(unusable_ca(cfg));
        }
        for c in certs {
            opts = opts.with_root_certificate(c);
        }
    }
    Ok(opts)
}

/// Builds a `reqwest` client with the same transport settings as [`client_options`], for the few
/// requests mimir-rs issues outside of `object_store` (e.g. the Azure container probe).
pub fn reqwest_client(
    cfg: &BucketHTTPConfig,
    total_timeout: Option<Duration>,
    allow_http: bool,
) -> Result<reqwest::Client, BucketError> {
    let mut b = reqwest::Client::builder()
        .pool_idle_timeout(pool_idle_timeout(cfg))
        .pool_max_idle_per_host(max_idle_per_host(cfg))
        .danger_accept_invalid_certs(cfg.insecure_skip_verify)
        .https_only(!allow_http);
    if let Some(t) = total_timeout {
        b = b.timeout(t);
    }
    if let Some(t) = connect_timeout(cfg) {
        b = b.connect_timeout(t);
    }
    if !cfg.force_attempt_http2 {
        b = b.http1_only();
    }
    if let Some(pem) = read_ca_file(cfg)? {
        let certs = reqwest::Certificate::from_pem_bundle(&pem).map_err(|_| unusable_ca(cfg))?;
        if certs.is_empty() {
            return Err(unusable_ca(cfg));
        }
        // Replace the OS root store (matching Go exthttp.updateRootCA setting cfg.RootCAs).
        b = b.tls_built_in_root_certs(false);
        for c in certs {
            b = b.add_root_certificate(c);
        }
    }
    b.build()
        .map_err(|e| BucketError::Config(format!("building HTTP client: {e}")))
}

/// Bounded HTTP client for credential/metadata/OAuth endpoints (connect 5s, total 15s).
pub(crate) fn bounded_auth_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Parses `x-ms-retry-after-ms` or `Retry-After` (seconds or HTTP-date).
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    if let Some(ms) = headers
        .get("x-ms-retry-after-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        return Some(Duration::from_millis(ms));
    }
    let val = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(secs) = val.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(val) {
        let delta = dt.with_timezone(&chrono::Utc) - chrono::Utc::now();
        return Some(delta.to_std().unwrap_or(Duration::ZERO));
    }
    None
}

/// Parses `Content-Range: bytes <start>-<end>/<total>`.
pub(crate) fn parse_content_range(s: &str) -> Option<(std::ops::Range<usize>, usize)> {
    let rem = s.trim().strip_prefix("bytes ")?;
    let (range, size) = rem.split_once('/')?;
    let size: usize = size.parse().ok()?;
    let (start_s, end_s) = range.split_once('-')?;
    let start: usize = start_s.parse().ok()?;
    let end: usize = end_s.parse().ok()?;
    Some((start..end + 1, size))
}

pub(crate) fn map_http_error(
    store: &'static str,
    path: &str,
    status: reqwest::StatusCode,
    body: String,
) -> object_store::Error {
    let source: Box<dyn std::error::Error + Send + Sync> = if body.is_empty() {
        format!("Client/Server error with status {status}").into()
    } else {
        format!("HTTP error with status {status}: {body}").into()
    };
    match status {
        reqwest::StatusCode::NOT_FOUND => object_store::Error::NotFound {
            path: path.to_string(),
            source,
        },
        reqwest::StatusCode::NOT_MODIFIED => object_store::Error::NotModified {
            path: path.to_string(),
            source,
        },
        reqwest::StatusCode::PRECONDITION_FAILED => object_store::Error::Precondition {
            path: path.to_string(),
            source,
        },
        reqwest::StatusCode::CONFLICT => object_store::Error::AlreadyExists {
            path: path.to_string(),
            source,
        },
        reqwest::StatusCode::FORBIDDEN => object_store::Error::PermissionDenied {
            path: path.to_string(),
            source,
        },
        reqwest::StatusCode::UNAUTHORIZED => object_store::Error::Unauthenticated {
            path: path.to_string(),
            source,
        },
        _ => object_store::Error::Generic { store, source },
    }
}

/// Percent-encodes every non-ASCII-alphanumeric byte as `%XX` (matching `object_store` GCS).
pub(crate) fn percent_encode_non_alnum(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

pub(crate) fn extract_object_meta(
    store: &'static str,
    location: &object_store::path::Path,
    headers: &reqwest::header::HeaderMap,
    version_header: Option<&str>,
) -> object_store::Result<object_store::ObjectMeta> {
    use chrono::{DateTime, TimeZone, Utc};
    use reqwest::header::{CONTENT_LENGTH, ETAG, LAST_MODIFIED};
    let err = |msg: String| object_store::Error::Generic {
        store,
        source: msg.into(),
    };
    let last_modified = match headers.get(LAST_MODIFIED) {
        Some(lm) => {
            let s = lm.to_str().map_err(|e| err(e.to_string()))?;
            DateTime::parse_from_rfc2822(s)
                .map_err(|e| err(format!("Invalid last modified '{s}': {e}")))?
                .with_timezone(&Utc)
        }
        None => Utc.timestamp_nanos(0),
    };
    let e_tag = match headers.get(ETAG) {
        Some(et) => Some(et.to_str().map_err(|e| err(e.to_string()))?.to_string()),
        None => None,
    };
    let size = match headers.get(CONTENT_LENGTH) {
        Some(cl) => {
            let s = cl.to_str().map_err(|e| err(e.to_string()))?;
            s.parse::<usize>()
                .map_err(|e| err(format!("Invalid content length '{s}': {e}")))?
        }
        None => 0,
    };
    let version = match version_header.and_then(|h| headers.get(h)) {
        Some(v) => Some(v.to_str().map_err(|e| err(e.to_string()))?.to_string()),
        None => None,
    };
    Ok(object_store::ObjectMeta {
        location: location.clone(),
        last_modified,
        version,
        size,
        e_tag,
    })
}

pub(crate) fn extract_attributes(
    headers: &reqwest::header::HeaderMap,
    meta_prefix: Option<&str>,
) -> object_store::Attributes {
    use object_store::{Attribute, Attributes};
    use reqwest::header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE};
    let mut attrs = Attributes::new();
    for (hdr, attr) in [
        (CACHE_CONTROL, Attribute::CacheControl),
        (CONTENT_DISPOSITION, Attribute::ContentDisposition),
        (CONTENT_ENCODING, Attribute::ContentEncoding),
        (CONTENT_LANGUAGE, Attribute::ContentLanguage),
        (CONTENT_TYPE, Attribute::ContentType),
    ] {
        if let Some(Ok(s)) = headers.get(hdr).map(|v| v.to_str()) {
            attrs.insert(attr, s.to_string().into());
        }
    }
    if let Some(prefix) = meta_prefix {
        for (k, v) in headers {
            if let Some(suffix) = k.as_str().strip_prefix(prefix) {
                if let Ok(s) = v.to_str() {
                    attrs.insert(
                        Attribute::Metadata(suffix.to_string().into()),
                        s.to_string().into(),
                    );
                }
            }
        }
    }
    attrs
}

pub(crate) fn payload_to_body(payload: &object_store::PutPayload) -> reqwest::Body {
    let chunks: Vec<bytes::Bytes> = payload.as_ref().to_vec();
    reqwest::Body::wrap_stream(futures::stream::iter(
        chunks.into_iter().map(Ok::<_, std::io::Error>),
    ))
}

pub(crate) fn validate_get_range(
    store: &'static str,
    range: &object_store::GetRange,
) -> object_store::Result<()> {
    if let object_store::GetRange::Bounded(r) = range {
        if r.start >= r.end {
            return Err(object_store::Error::Generic {
                store,
                source: format!("Wanted range starting at {} and ending at {}, but end must be greater than start", r.start, r.end).into(),
            });
        }
    }
    Ok(())
}

pub(crate) fn resolve_get_range(
    store: &'static str,
    range: &object_store::GetRange,
    len: usize,
) -> object_store::Result<std::ops::Range<usize>> {
    use object_store::GetRange;
    match range {
        GetRange::Bounded(r) => {
            if r.start >= len {
                return Err(object_store::Error::Generic {
                    store,
                    source: format!("Wanted range starting at {}, but object was only {} bytes long", r.start, len).into(),
                });
            }
            Ok(r.start..r.end.min(len))
        }
        GetRange::Offset(start) => {
            if *start >= len {
                return Err(object_store::Error::Generic {
                    store,
                    source: format!("Wanted range starting at {}, but object was only {} bytes long", start, len).into(),
                });
            }
            Ok(*start..len)
        }
        GetRange::Suffix(n) => Ok(len.saturating_sub(*n)..len),
    }
}

pub(crate) fn apply_get_options(
    mut rb: reqwest::RequestBuilder,
    options: &object_store::GetOptions,
) -> reqwest::RequestBuilder {
    use reqwest::header::*;
    const DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";
    if let Some(ref range) = options.range {
        rb = rb.header(RANGE, range.to_string());
    }
    if let Some(ref tag) = options.if_match {
        rb = rb.header(IF_MATCH, tag);
    }
    if let Some(ref tag) = options.if_none_match {
        rb = rb.header(IF_NONE_MATCH, tag);
    }
    if let Some(date) = options.if_unmodified_since {
        rb = rb.header(IF_UNMODIFIED_SINCE, date.format(DATE_FORMAT).to_string());
    }
    if let Some(date) = options.if_modified_since {
        rb = rb.header(IF_MODIFIED_SINCE, date.format(DATE_FORMAT).to_string());
    }
    rb
}

/// Ported from `thanos-io/objstore/exthttp/parse.go` and `clientutil/parse.go`:
/// returns the content length parsed from the `Content-Length` HTTP header.
pub fn parse_content_length(headers: &reqwest::header::HeaderMap) -> Result<i64, String> {
    let val = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .ok_or_else(|| "Content-Length header not found".to_string())?;
    let s = val
        .to_str()
        .map_err(|e| format!("convert Content-Length: {e}"))?;
    if s.is_empty() {
        return Err("Content-Length header has no values".to_string());
    }
    s.parse::<i64>()
        .map_err(|e| format!("convert Content-Length: strconv.ParseInt: parsing {s:?}: {e}"))
}

/// Ported from `thanos-io/objstore/exthttp/parse.go` and `clientutil/parse.go`:
/// returns the timestamp parsed from the `Last-Modified` HTTP header.
/// If `format` is empty, RFC3339 is used as the default format.
pub fn parse_last_modified(
    headers: &reqwest::header::HeaderMap,
    format: &str,
) -> Result<chrono::DateTime<chrono::FixedOffset>, String> {
    let val = headers
        .get(reqwest::header::LAST_MODIFIED)
        .ok_or_else(|| "Last-Modified header not found".to_string())?;
    let s = val
        .to_str()
        .map_err(|e| format!("parse Last-Modified: {e}"))?;
    if s.is_empty() {
        return Err("Last-Modified header has no values".to_string());
    }
    let fmt = if format.is_empty() {
        "2006-01-02T15:04:05Z07:00"
    } else {
        format
    };
    match fmt {
        "2006-01-02T15:04:05Z07:00" | "rfc3339" | "RFC3339" => {
            chrono::DateTime::parse_from_rfc3339(s).map_err(|e| {
                format!("parse Last-Modified: parsing time {s:?} as \"2006-01-02T15:04:05Z07:00\": {e}")
            })
        }
        "Mon, 02 Jan 2006 15:04:05 MST" | "rfc1123" | "RFC1123" => {
            chrono::DateTime::parse_from_rfc2822(s).map_err(|e| {
                format!("parse Last-Modified: parsing time {s:?} as \"Mon, 02 Jan 2006 15:04:05 MST\": {e}")
            })
        }
        other => chrono::DateTime::parse_from_str(s, other)
            .map_err(|e| format!("parse Last-Modified: parsing time {s:?} as {other:?}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_go_default_transport() {
        let cfg = BucketHTTPConfig::default();
        assert_eq!(connect_timeout(&cfg), Some(Duration::from_secs(40)));
        assert_eq!(pool_idle_timeout(&cfg), Some(Duration::from_secs(90)));
        assert_eq!(max_idle_per_host(&cfg), 100);
        assert!(client_options(&cfg, None).is_ok());
        assert!(reqwest_client(&cfg, None, false).is_ok());
    }

    #[test]
    fn idle_connection_limits_follow_net_http() {
        let mut cfg = BucketHTTPConfig {
            max_idle_connections_per_host: 0,
            ..Default::default()
        };
        assert_eq!(max_idle_per_host(&cfg), GO_DEFAULT_MAX_IDLE_CONNS_PER_HOST);
        cfg.max_idle_connections_per_host = 50;
        cfg.max_idle_connections = 10;
        assert_eq!(max_idle_per_host(&cfg), 10);
        cfg.max_idle_connections = 0;
        assert_eq!(max_idle_per_host(&cfg), 50);
        cfg.max_idle_connections_per_host = -1;
        assert_eq!(max_idle_per_host(&cfg), 0);
    }

    #[test]
    fn zero_timeouts_mean_no_limit() {
        let cfg = BucketHTTPConfig {
            idle_conn_timeout: GoDuration(0),
            tls_handshake_timeout: GoDuration(0),
            ..Default::default()
        };
        assert_eq!(connect_timeout(&cfg), None);
        assert_eq!(pool_idle_timeout(&cfg), None);
        assert!(client_options(&cfg, None).is_ok());
    }

    #[test]
    fn ca_file_errors_match_go() {
        let cfg = BucketHTTPConfig {
            tls_ca_path: "/nonexistent/ca.pem".to_string(),
            ..Default::default()
        };
        let err = client_options(&cfg, None).unwrap_err().to_string();
        assert!(
            err.contains("unable to load specified CA cert /nonexistent/ca.pem: "),
            "{err}"
        );

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ca.pem");
        std::fs::write(&p, "not a pem").unwrap();
        let cfg = BucketHTTPConfig {
            tls_ca_path: p.to_string_lossy().into_owned(),
            ..Default::default()
        };
        let err = client_options(&cfg, None).unwrap_err().to_string();
        assert!(
            err.ends_with(&format!("unable to use specified CA cert {}", p.display())),
            "{err}"
        );
        let err = reqwest_client(&cfg, None, false).unwrap_err().to_string();
        assert!(err.contains("unable to use specified CA cert"), "{err}");
    }

    #[tokio::test]
    async fn d2_04_tls_ca_path_replaces_root_store_in_reqwest_client() {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/kafka_tls");
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(dir.join("server.crt"))
                .unwrap()
                .map(Result::unwrap)
                .collect();
        let key = PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
        let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        let server_cfg = std::sync::Arc::new(
            rustls::ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap(),
        );
        let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let srv = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let acc = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut tls) = acc.accept(sock).await {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 1024];
                        while let Ok(n) = tls.read(&mut tmp).await {
                            if n == 0 {
                                break;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        let _ = tls
                            .write_all(
                                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
                            )
                            .await;
                        let _ = tls.flush().await;
                        let _ = tls.shutdown().await;
                    }
                });
            }
        });

        let ok_cfg = BucketHTTPConfig {
            tls_ca_path: dir.join("ca.crt").to_string_lossy().into_owned(),
            ..Default::default()
        };
        let ok_client = reqwest_client(&ok_cfg, Some(Duration::from_secs(5)), false).unwrap();
        let resp = ok_client
            .get(format!("https://localhost:{}/", addr.port()))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let wrong_cfg = BucketHTTPConfig {
            tls_ca_path: dir.join("other-ca.crt").to_string_lossy().into_owned(),
            ..Default::default()
        };
        let wrong_client =
            reqwest_client(&wrong_cfg, Some(Duration::from_secs(5)), false).unwrap();
        let err = wrong_client
            .get(format!("https://localhost:{}/", addr.port()))
            .send()
            .await
            .unwrap_err();
        assert!(err.is_connect() || err.is_request(), "expected TLS verify error, got {err:?}");

        // Verify GCS and Azure builders propagate http.tls_ca_path via reqwest_client
        // (replacing OS roots rather than appending).
        let conn_str = format!(
            "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=https://localhost:{};",
            addr.port()
        );
        let az_wrong_cfg = mimir_config::AzureConfig {
            account_name: "devstoreaccount1".into(),
            connection_string: conn_str.clone(),
            container_name: "mimir".into(),
            max_retries: 0,
            http: wrong_cfg.clone(),
            ..Default::default()
        };
        let az_err = crate::bucket::azure::new_store_async(
            &az_wrong_cfg,
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            az_err.contains("azure") && !az_err.contains("storage_account_name"),
            "expected TLS failure with wrong tls_ca_path on Azure store, got {az_err}"
        );

        let az_ok_cfg = mimir_config::AzureConfig {
            http: ok_cfg.clone(),
            ..az_wrong_cfg
        };
        crate::bucket::azure::new_store_async(&az_ok_cfg, Some(Duration::from_secs(2)))
            .await
            .unwrap();

        let gcs_cfg = mimir_config::GCSConfig {
            bucket_name: "mimir".into(),
            service_account: r#"{"type":"authorized_user","client_id":"cid","client_secret":"sec","refresh_token":"rt","token_uri":"https://localhost/token"}"#.into(),
            http: wrong_cfg,
            ..Default::default()
        };
        assert!(crate::bucket::gcs::new_store(&gcs_cfg).is_ok());

        srv.abort();
    }
}
