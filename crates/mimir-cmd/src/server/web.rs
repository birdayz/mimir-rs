//! Ported from `github.com/prometheus/exporter-toolkit/web`
//! (`cache.go`, `handler.go`, `tls_config.go`, `tls_protocols_go127.go`).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use mimir_config::server::TLSConfig;
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};

use super::tls::{server_config_with_options, TlsOptions};

// ============================================================================
// cache.go
// ============================================================================

const CACHE_SIZE: usize = 100;

/// Ported from `exporter-toolkit/web/cache.go`.
pub struct WebCache {
    map: Mutex<HashMap<String, bool>>,
}

impl Default for WebCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WebCache {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<bool> {
        self.map.lock().get(key).copied()
    }

    pub fn set(&self, key: String, value: bool) {
        let mut m = self.map.lock();
        if m.len() >= CACHE_SIZE {
            let num_to_delete = (m.len() / 10).max(1);
            let mut rng = rand::thread_rng();
            for _ in 0..=num_to_delete {
                if m.is_empty() {
                    break;
                }
                let idx = rng.gen_range(0..m.len());
                if let Some(k) = m.keys().nth(idx).cloned() {
                    m.remove(&k);
                }
            }
        }
        m.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.lock().is_empty()
    }
}

// ============================================================================
// tls_config.go (FlagConfig, IsEnabled, YAML schema, ConfigToTLSConfig)
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebFlagError {
    MissingFlag,
    NoListeners,
}

impl std::fmt::Display for WebFlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingFlag => f.write_str("missing required flag configuration"),
            Self::NoListeners => f.write_str("no web listen address or systemd socket flag specified"),
        }
    }
}

impl std::error::Error for WebFlagError {}

/// Ported from `exporter-toolkit/web/tls_config.go` (`FlagConfig`).
#[derive(Clone, Debug, Default)]
pub struct WebFlagConfig {
    pub web_listen_addresses: Option<Vec<String>>,
    pub web_systemd_socket: Option<bool>,
    pub web_config_file: Option<String>,
}

impl WebFlagConfig {
    pub fn check_flags(cfg: Option<&Self>) -> Result<(), WebFlagError> {
        let Some(c) = cfg else {
            return Err(WebFlagError::MissingFlag);
        };
        if c.web_config_file.is_none() {
            return Err(WebFlagError::MissingFlag);
        }
        if !c.web_systemd_socket.unwrap_or(false)
            && c.web_listen_addresses.as_ref().is_none_or(Vec::is_empty)
        {
            return Err(WebFlagError::NoListeners);
        }
        Ok(())
    }

    pub fn listen_and_serve_validate(cfg: Option<&Self>) -> Result<(), WebFlagError> {
        Self::check_flags(cfg)
    }
}

/// Ported from `exporter-toolkit/web/tls_config.go` (`TLSConfig.IsEnabled`).
pub fn web_tls_is_enabled(t: &TLSConfig) -> bool {
    !t.cert_file.is_empty()
        || !t.cert.is_empty()
        || !t.key_file.is_empty()
        || !t.key.is_empty()
        || !t.client_ca_file.is_empty()
        || !t.client_ca.is_empty()
        || !t.client_auth_type.is_empty()
}

/// Ported from `exporter-toolkit/web/tls_protocols_go127.go` (`tlsNextProtos`).
pub fn tls_next_protos(configured: &[&str], http2_enabled: bool) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(configured.len());
    for &p in configured {
        if !http2_enabled && p == "h2" {
            continue;
        }
        out.push(p.to_string());
    }
    out
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebYamlTlsConfig {
    pub cert: String,
    pub key: String,
    pub client_ca: String,
    pub cert_file: String,
    pub key_file: String,
    pub client_auth_type: String,
    pub client_ca_file: String,
    pub cipher_suites: Vec<String>,
    pub curve_preferences: Vec<String>,
    pub min_version: String,
    pub max_version: String,
    pub prefer_server_cipher_suites: Option<bool>,
    pub client_allowed_sans: Vec<String>,
}

impl WebYamlTlsConfig {
    pub fn to_dskit_tls(&self) -> TLSConfig {
        TLSConfig {
            cert: self.cert.clone(),
            key: self.key.clone(),
            client_ca: self.client_ca.clone(),
            cert_file: self.cert_file.clone(),
            key_file: self.key_file.clone(),
            client_auth_type: self.client_auth_type.clone(),
            client_ca_file: self.client_ca_file.clone(),
        }
    }

    pub fn set_directory(&mut self, dir: &Path) {
        let join_nonempty = |p: &mut String| {
            if !p.is_empty() && !Path::new(p.as_str()).is_absolute() {
                *p = dir.join(p.as_str()).to_string_lossy().into_owned();
            }
        };
        join_nonempty(&mut self.cert_file);
        join_nonempty(&mut self.key_file);
        join_nonempty(&mut self.client_ca_file);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebHttpConfig {
    pub http2: bool,
    pub headers: BTreeMap<String, String>,
}

impl Default for WebHttpConfig {
    fn default() -> Self {
        Self {
            http2: true,
            headers: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebRateLimitConfig {
    pub burst: u64,
    pub interval: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    pub tls_server_config: WebYamlTlsConfig,
    pub http_server_config: WebHttpConfig,
    pub rate_limit: WebRateLimitConfig,
    pub basic_auth_users: HashMap<String, String>,
}

fn validate_header_config(headers: &BTreeMap<String, String>) -> Result<(), String> {
    for (k, v) in headers {
        match k.as_str() {
            "Strict-Transport-Security" | "X-XSS-Protection" | "Content-Security-Policy" => {}
            "X-Content-Type-Options" => {
                if v != "nosniff" {
                    return Err(format!(
                        "invalid value for {k}. Expected one of: [\"nosniff\"], but got: {v:?}"
                    ));
                }
            }
            "X-Frame-Options" => {
                if v != "deny" && v != "sameorigin" {
                    return Err(format!(
                        "invalid value for {k}. Expected one of: [\"deny\" \"sameorigin\"], but got: {v:?}"
                    ));
                }
            }
            other => {
                return Err(format!("HTTP header {other:?} can not be configured"));
            }
        }
    }
    Ok(())
}

fn map_web_tls_version(v: &str) -> Result<String, String> {
    match v {
        "" => Ok(String::new()),
        "TLS13" => Ok("VersionTLS13".to_string()),
        "TLS12" => Ok("VersionTLS12".to_string()),
        "TLS11" => Ok("VersionTLS11".to_string()),
        "TLS10" => Ok("VersionTLS10".to_string()),
        other => Err(format!("unknown TLS version: {other}")),
    }
}

fn map_web_cipher_name(s: &str) -> Result<(), String> {
    const KNOWN: &[&str] = &[
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
    ];
    if KNOWN.contains(&s) {
        Ok(())
    } else {
        Err(format!("unknown cipher: {s}"))
    }
}

fn is_http2_cipher(s: &str) -> bool {
    matches!(
        s,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
            | "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
            | "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
            | "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"
            | "TLS_AES_128_GCM_SHA256"
            | "TLS_AES_256_GCM_SHA384"
            | "TLS_CHACHA20_POLY1305_SHA256"
    )
}

fn map_web_curve_name(s: &str) -> Result<(), String> {
    if matches!(s, "CurveP256" | "CurveP384" | "CurveP521" | "X25519") {
        Ok(())
    } else {
        Err(format!("unknown curve: {s}"))
    }
}

impl WebConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let mut cfg: Self = if raw.trim().is_empty() {
            Self::default()
        } else {
            serde_yaml::from_str(&raw).map_err(|e| format!("yaml: {e}"))?
        };
        validate_header_config(&cfg.http_server_config.headers)?;
        for c in &cfg.tls_server_config.cipher_suites {
            map_web_cipher_name(c)?;
        }
        for c in &cfg.tls_server_config.curve_preferences {
            map_web_curve_name(c)?;
        }
        let _ = map_web_tls_version(&cfg.tls_server_config.min_version)?;
        let _ = map_web_tls_version(&cfg.tls_server_config.max_version)?;
        if let Some(dir) = path.parent() {
            cfg.tls_server_config.set_directory(dir);
        }
        Ok(cfg)
    }

    pub fn validate(path: &Path) -> Result<(), String> {
        let cfg = Self::load(path)?;
        for hash in cfg.basic_auth_users.values() {
            bcrypt_cost(hash)?;
        }
        if web_tls_is_enabled(&cfg.tls_server_config.to_dskit_tls()) {
            let _ = cfg.build_tls_server_config()?;
        }
        Ok(())
    }

    pub fn build_tls_server_config(&self) -> Result<Arc<rustls::ServerConfig>, String> {
        let tls = self.tls_server_config.to_dskit_tls();
        if !web_tls_is_enabled(&tls) {
            return Err("TLS config is not present".to_string());
        }
        let min_v = map_web_tls_version(&self.tls_server_config.min_version)?;
        let max_v = if self.tls_server_config.max_version.is_empty() {
            None
        } else {
            Some(map_web_tls_version(&self.tls_server_config.max_version)?)
        };
        if self.http_server_config.http2
            && !self.tls_server_config.cipher_suites.is_empty()
            && max_v.as_deref() == Some("VersionTLS12")
            && !self
                .tls_server_config
                .cipher_suites
                .iter()
                .any(|c| is_http2_cipher(c))
        {
            return Err(
                "TLSConfig.CipherSuites is missing an HTTP/2-required TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 cipher"
                    .to_string(),
            );
        }
        let opts = TlsOptions {
            cipher_suites: self
                .tls_server_config
                .cipher_suites
                .iter()
                .filter(|c| !c.ends_with("_CBC_SHA"))
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
            min_version: min_v,
            curve_preferences: self.tls_server_config.curve_preferences.join(","),
        };
        let alpn_strs = tls_next_protos(&["h2", "http/1.1"], self.http_server_config.http2);
        let alpn_refs: Vec<&[u8]> = alpn_strs.iter().map(|s| s.as_bytes()).collect();
        server_config_with_options(
            &tls,
            &opts,
            &alpn_refs,
            max_v.as_deref(),
            &self.tls_server_config.client_allowed_sans,
        )
        .map_err(|e| e.to_string())
    }
}

// ============================================================================
// handler.go (WebAuthHandler + pure-Rust bcrypt verification)
// ============================================================================

const FAKE_PASSWORD_HASH: &str = "$2y$10$QOauhQNbBCuQDKes6eFzPeMqBSjb7Mr5DUmpZ/VcEd00UAV/LDeSi";

/// Ported from `exporter-toolkit/web/handler.go` (`webHandler`).
struct SimpleRateLimiter {
    interval: std::time::Duration,
    burst: f64,
    tokens: f64,
    last: std::time::Instant,
}

impl SimpleRateLimiter {
    fn new(interval: std::time::Duration, burst: u64) -> Self {
        Self {
            interval,
            burst: burst as f64,
            tokens: burst as f64,
            last: std::time::Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        if self.interval.is_zero() {
            return true;
        }
        if self.burst <= 0.0 {
            return false;
        }
        let now = std::time::Instant::now();
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        let rate_per_sec = 1.0 / self.interval.as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate_per_sec).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct WebAuthHandler {
    config_path: std::path::PathBuf,
    cache: WebCache,
    bcrypt_mtx: Mutex<()>,
    limiter: Mutex<Option<SimpleRateLimiter>>,
}

impl WebAuthHandler {
    pub fn new(config_path: std::path::PathBuf) -> Self {
        let limiter = WebConfig::load(&config_path).ok().and_then(|c| {
            let interval = c.rate_limit.interval.parse::<mimir_config::GoDuration>()
                .ok()
                .and_then(|d| d.as_std())
                .unwrap_or_default();
            (!interval.is_zero())
                .then(|| SimpleRateLimiter::new(interval, c.rate_limit.burst))
        });
        Self {
            config_path,
            cache: WebCache::new(),
            bcrypt_mtx: Mutex::new(()),
            limiter: Mutex::new(limiter),
        }
    }

    pub fn apply_headers_and_check_auth(
        &self,
        req_headers: &HeaderMap,
        resp: Response,
    ) -> Response {
        let Ok(cfg) = WebConfig::load(&self.config_path) else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response();
        };

        if let Some(lim) = self.limiter.lock().as_mut() {
            if !lim.allow() {
                return (StatusCode::TOO_MANY_REQUESTS, "Too Many Requests").into_response();
            }
        }

        let mut resp = resp;
        for (k, v) in &cfg.http_server_config.headers {
            if let (Ok(hn), Ok(hv)) = (
                HeaderName::from_bytes(k.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                resp.headers_mut().insert(hn, hv);
            }
        }

        if cfg.basic_auth_users.is_empty() {
            return resp;
        }

        if let Some((user, pass)) = parse_basic_auth(req_headers) {
            let (valid_user, hashed) = match cfg.basic_auth_users.get(&user) {
                Some(h) => (true, h.as_str()),
                None => (false, FAKE_PASSWORD_HASH),
            };
            let cache_key = format!(
                "{}:{}:{}",
                hex_encode(user.as_bytes()),
                hex_encode(hashed.as_bytes()),
                hex_encode(pass.as_bytes())
            );
            let auth_ok = match self.cache.get(&cache_key) {
                Some(cached) => cached,
                None => {
                    let _g = self.bcrypt_mtx.lock();
                    let pw_ok = bcrypt_verify(hashed, pass.as_bytes());
                    let ok = valid_user && pw_ok;
                    self.cache.set(cache_key, ok);
                    ok
                }
            };
            if auth_ok && valid_user {
                return resp;
            }
        }

        let mut unauth = (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        unauth
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic"));
        for (k, v) in &cfg.http_server_config.headers {
            if let (Ok(hn), Ok(hv)) = (
                HeaderName::from_bytes(k.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                unauth.headers_mut().insert(hn, hv);
            }
        }
        unauth
    }
}

fn parse_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    let val = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let b64 = val.strip_prefix("Basic ")?;
    let decoded = decode_std_base64(b64.as_bytes())?;
    let s = String::from_utf8(decoded).ok()?;
    let (u, p) = s.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn decode_std_base64(input: &[u8]) -> Option<Vec<u8>> {
    let val = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    if !input.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity((input.len() / 4) * 3);
    for chunk in input.as_chunks::<4>().0 {
        let a = val(chunk[0])?;
        let b = val(chunk[1])?;
        out.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            let c = val(chunk[2])?;
            out.push((b << 4) | (c >> 2));
            if chunk[3] != b'=' {
                let d = val(chunk[3])?;
                out.push((c << 6) | d);
            }
        } else if chunk[3] != b'=' {
            return None;
        }
    }
    Some(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ============================================================================
// Pure-Rust Eksblowfish bcrypt verification ($2a$, $2b$, $2y$)
// ============================================================================

pub fn bcrypt_cost(hash: &str) -> Result<u32, String> {
    if hash.len() < 29 {
        return Err("hashedSecret too short to be a bcrypted password".to_string());
    }
    let bytes = hash.as_bytes();
    if bytes[0] != b'$' || bytes[1] != b'2' || !matches!(bytes[2], b'a' | b'b' | b'y' | b'x') || bytes[3] != b'$' {
        return Err("invalid bcrypt version".to_string());
    }
    let cost_str = std::str::from_utf8(&bytes[4..6]).map_err(|_| "invalid bcrypt cost".to_string())?;
    let cost: u32 = cost_str
        .parse()
        .map_err(|_| format!("invalid bcrypt cost: {cost_str}"))?;
    if !(4..=31).contains(&cost) || bytes[6] != b'$' {
        return Err(format!("invalid bcrypt cost: {cost}"));
    }
    Ok(cost)
}

pub fn bcrypt_verify(hash: &str, password: &[u8]) -> bool {
    let Ok(cost) = bcrypt_cost(hash) else {
        return false;
    };
    if hash.len() != 60 {
        return false;
    }
    let Some(salt) = bcrypt_decode_base64(&hash.as_bytes()[7..29], 16) else {
        return false;
    };
    let Some(expected) = bcrypt_decode_base64(&hash.as_bytes()[29..60], 23) else {
        return false;
    };
    let actual = eksblowfish_crypt(cost, &salt, password);
    let mut diff = 0u8;
    for i in 0..23 {
        diff |= actual[i] ^ expected[i];
    }
    diff == 0
}

fn bcrypt_decode_base64(input: &[u8], out_len: usize) -> Option<Vec<u8>> {
    const ALPHA: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let decode_char = |c: u8| -> Option<u8> { ALPHA.iter().position(|&x| x == c).map(|i| i as u8) };
    let mut out = Vec::with_capacity(out_len);
    let mut i = 0;
    while out.len() < out_len && i + 1 < input.len() {
        let c1 = decode_char(input[i])?;
        let c2 = decode_char(input[i + 1])?;
        out.push((c1 << 2) | ((c2 & 0x30) >> 4));
        if out.len() >= out_len {
            break;
        }
        if i + 2 >= input.len() {
            return None;
        }
        let c3 = decode_char(input[i + 2])?;
        out.push(((c2 & 0x0f) << 4) | ((c3 & 0x3c) >> 2));
        if out.len() >= out_len {
            break;
        }
        if i + 3 >= input.len() {
            return None;
        }
        let c4 = decode_char(input[i + 3])?;
        out.push(((c3 & 0x03) << 6) | c4);
        i += 4;
    }
    (out.len() == out_len).then_some(out)
}

fn eksblowfish_crypt(cost: u32, salt: &[u8], password: &[u8]) -> [u8; 24] {
    let mut key: Vec<u8> = password.iter().copied().take(72).collect();
    if key.len() < 72 {
        key.push(0);
    }

    let mut state = BlowfishState {
        p: INIT_P,
        s: INIT_S,
    };
    state.expand_key(salt, &key);
    let rounds = 1u32 << cost;
    for _ in 0..rounds {
        state.expand_key(&[], &key);
        state.expand_key(&[], salt);
    }

    let mut cdata: [u32; 6] = [
        0x4f72_7068, // "Orph"
        0x6561_6e42, // "eanB"
        0x6568_6f6c, // "ehol"
        0x6465_7253, // "derS"
        0x6372_7944, // "cryD"
        0x6f75_6274, // "oubt"
    ];
    for _ in 0..64 {
        for pair in cdata.as_chunks_mut::<2>().0 {
            let (l, r) = state.encipher(pair[0], pair[1]);
            pair[0] = l;
            pair[1] = r;
        }
    }
    let mut out = [0u8; 24];
    for (idx, word) in cdata.iter().enumerate() {
        out[idx * 4..(idx + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

struct BlowfishState {
    p: [u32; 18],
    s: [[u32; 256]; 4],
}

impl BlowfishState {
    #[inline]
    fn f(&self, x: u32) -> u32 {
        let a = ((x >> 24) & 0xff) as usize;
        let b = ((x >> 16) & 0xff) as usize;
        let c = ((x >> 8) & 0xff) as usize;
        let d = (x & 0xff) as usize;
        ((self.s[0][a].wrapping_add(self.s[1][b])) ^ self.s[2][c]).wrapping_add(self.s[3][d])
    }

    #[inline]
    fn encipher(&self, mut l: u32, mut r: u32) -> (u32, u32) {
        for i in (0..16).step_by(2) {
            l ^= self.p[i];
            r ^= self.f(l);
            r ^= self.p[i + 1];
            l ^= self.f(r);
        }
        l ^= self.p[16];
        r ^= self.p[17];
        (r, l)
    }

    fn expand_key(&mut self, data: &[u8], key: &[u8]) {
        if !key.is_empty() {
            let mut j = 0usize;
            for p_elem in &mut self.p {
                let mut w = 0u32;
                for _ in 0..4 {
                    w = (w << 8) | u32::from(key[j]);
                    j = (j + 1) % key.len();
                }
                *p_elem ^= w;
            }
        }
        let mut l = 0u32;
        let mut r = 0u32;
        let mut d_idx = 0usize;
        let next_word = |idx: &mut usize| -> u32 {
            if data.is_empty() {
                return 0;
            }
            let mut w = 0u32;
            for _ in 0..4 {
                w = (w << 8) | u32::from(data[*idx]);
                *idx = (*idx + 1) % data.len();
            }
            w
        };
        for i in (0..18).step_by(2) {
            l ^= next_word(&mut d_idx);
            r ^= next_word(&mut d_idx);
            let (nl, nr) = self.encipher(l, r);
            l = nl;
            r = nr;
            self.p[i] = l;
            self.p[i + 1] = r;
        }
        for box_idx in 0..4 {
            for i in (0..256).step_by(2) {
                l ^= next_word(&mut d_idx);
                r ^= next_word(&mut d_idx);
                let (nl, nr) = self.encipher(l, r);
                l = nl;
                r = nr;
                self.s[box_idx][i] = l;
                self.s[box_idx][i + 1] = r;
            }
        }
    }
}

// Standard Blowfish initial P-array and S-boxes (fractional part of pi).
include!("blowfish_tables.in");
