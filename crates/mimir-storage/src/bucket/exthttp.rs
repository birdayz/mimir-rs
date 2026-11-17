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
//! - `TLSConfig.CAFile` -> additional trusted root certificates. NOTE: Go replaces the system
//!   roots with the CA file; `object_store` 0.11 can only add roots (documented divergence).
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
        for c in certs {
            b = b.add_root_certificate(c);
        }
    }
    b.build()
        .map_err(|e| BucketError::Config(format!("building HTTP client: {e}")))
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
}
