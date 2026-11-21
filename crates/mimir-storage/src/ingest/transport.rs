//! Shared Kafka broker transport: TCP dial with optional TLS, `ApiVersions` discovery and SASL
//! authentication, used by every Kafka connection of the ingest storage clients.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (`commonKafkaClientOptions`,
//!   `kafkaAuthOptions`)
//! - github.com/grafana/dskit/crypto/tls/tls.go (via [`mimir_common::tls`])
//! - github.com/twmb/franz-go/pkg/kgo/client.go (`DialTLSConfig` dialer: per-connection
//!   `ServerName` = host of the broker address when not overridden)
//! - github.com/twmb/franz-go/pkg/kgo/broker.go (`brokerCxn.init`, `sasl`, `doSasl`,
//!   session lifetime / reauthentication expiry)
//! - github.com/twmb/franz-go/pkg/sasl/{plain,scram,oauth,aws}
//!
//! Connection establishment is identical for all four connection sites (`KafkaClient`,
//! `IngestKafkaClient`, the concurrent fetcher and the batched `ListOffsets` client):
//! 1. TCP connect + TLS handshake, both bounded by `dial_timeout` (Go flag doc: "including the TLS
//!    handshake when TLS is enabled. It does not include the subsequent SASL authentication
//!    handshake").
//! 2. `ApiVersions` (every new connection, like kgo).
//! 3. SASL: `SaslHandshake` (v1 when supported -> `SaslAuthenticate` framing, v0 -> raw framing)
//!    followed by the mechanism exchange, before any other request.
//!
//! Steps 2 and 3 run synchronously on the stream before it is split into the per-connection
//! reader/writer tasks, so the hot request path is unchanged.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::error::{ParseResponseErrorCode, ResponseError};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use mimir_common::tls::{parse_server_name, ResolvedClientConfig};
use mimir_config::types::SASLMechanism;
use mimir_config::KafkaConfig;
use ring::rand::SecureRandom;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use super::kafka_client::KafkaClientError;
use super::util::{
    base64_decode, base64_encode, sasl_mechanism, KafkaMskIamAuth, KafkaOauthbearerAuth,
    KafkaSaslSecretConfig, ResolvedSaslMechanism,
};

/// Default dial timeout when none is configured.
const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound for a single connection-setup response (kgo `BrokerMaxReadBytes` default).
const MAX_HANDSHAKE_RESPONSE_BYTES: usize = 100 << 20;

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// A broker connection: plain TCP or TLS over TCP.
pub enum KafkaStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl KafkaStream {
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Splits into independently owned halves. Plain TCP uses the lock-free
    /// [`TcpStream::into_split`]; TLS shares the session through [`tokio::io::split`].
    pub fn into_split(self) -> (KafkaReadHalf, KafkaWriteHalf) {
        match self {
            Self::Plain(s) => {
                let (r, w) = s.into_split();
                (KafkaReadHalf::Plain(r), KafkaWriteHalf::Plain(w))
            }
            Self::Tls(s) => {
                let (r, w) = tokio::io::split(*s);
                (KafkaReadHalf::Tls(r), KafkaWriteHalf::Tls(w))
            }
        }
    }
}

impl AsyncRead for KafkaStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for KafkaStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Read half of a [`KafkaStream`].
pub enum KafkaReadHalf {
    Plain(OwnedReadHalf),
    Tls(tokio::io::ReadHalf<TlsStream<TcpStream>>),
}

impl AsyncRead for KafkaReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

/// Write half of a [`KafkaStream`]. Writers must `flush()` after each frame: with TLS the
/// encrypted records may otherwise stay buffered in the session (a no-op for plain TCP).
pub enum KafkaWriteHalf {
    Plain(OwnedWriteHalf),
    Tls(tokio::io::WriteHalf<TlsStream<TcpStream>>),
}

impl AsyncWrite for KafkaWriteHalf {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// SASL configuration (util.go kafkaAuthOptions)
// ---------------------------------------------------------------------------

/// SASL mechanism and credentials used on every broker connection.
#[derive(Clone)]
pub enum SaslAuthConfig {
    None,
    Plain {
        username: String,
        password: String,
    },
    ScramSha256 {
        username: String,
        password: String,
    },
    ScramSha512 {
        username: String,
        password: String,
    },
    Oauthbearer(ResolvedSaslMechanism<KafkaOauthbearerAuth>),
    MskIam(ResolvedSaslMechanism<KafkaMskIamAuth>),
}

impl fmt::Debug for SaslAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Plain { username, .. }
            | Self::ScramSha256 { username, .. }
            | Self::ScramSha512 { username, .. } => f
                .debug_struct(self.mechanism_name().unwrap_or_default())
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Oauthbearer(_) | Self::MskIam(_) => {
                f.write_str(self.mechanism_name().unwrap_or_default())
            }
        }
    }
}

impl SaslAuthConfig {
    /// Port of `kafkaAuthOptions` (`pkg/storage/ingest/util.go`). The config must have passed
    /// `KafkaConfig::validate`.
    pub fn from_kafka_config(cfg: &KafkaConfig) -> Result<Self, KafkaClientError> {
        // For backwards-compatibility, PLAIN with no username nor password disables SASL.
        if cfg.sasl_mechanism == SASLMechanism::Plain && cfg.sasl_username.is_empty() {
            return Ok(Self::None);
        }
        let to_nanos = |d: mimir_config::GoDuration| Duration::from_nanos(d.as_nanos().max(0) as u64);
        Ok(match cfg.sasl_mechanism {
            SASLMechanism::Plain => Self::Plain {
                username: cfg.sasl_username.clone(),
                password: cfg.sasl_password.clone(),
            },
            SASLMechanism::ScramSha256 => Self::ScramSha256 {
                username: cfg.sasl_username.clone(),
                password: cfg.sasl_password.clone(),
            },
            SASLMechanism::ScramSha512 => Self::ScramSha512 {
                username: cfg.sasl_username.clone(),
                password: cfg.sasl_password.clone(),
            },
            SASLMechanism::Oauthbearer => Self::Oauthbearer(
                sasl_mechanism(
                    KafkaSaslSecretConfig {
                        secret: Some(KafkaOauthbearerAuth {
                            token: cfg.sasl_oauthbearer_token.clone(),
                            zid: cfg.sasl_oauthbearer_zid.clone(),
                            extensions: cfg.sasl_oauthbearer_extensions.clone(),
                        }),
                        file_path: cfg.sasl_oauthbearer_file_path.clone(),
                        http_socket_path: cfg.sasl_oauthbearer_http_socket_path.clone(),
                        http_socket_timeout: to_nanos(cfg.sasl_oauthbearer_http_socket_timeout),
                    },
                    ResolvedSaslMechanism::Callback,
                    ResolvedSaslMechanism::Static,
                )
                .map_err(KafkaClientError::Config)?,
            ),
            SASLMechanism::MskIam => Self::MskIam(
                sasl_mechanism(
                    KafkaSaslSecretConfig {
                        secret: Some(KafkaMskIamAuth {
                            access_key: cfg.sasl_msk_iam_access_key.clone(),
                            secret_key: cfg.sasl_msk_iam_secret_key.clone(),
                            session_token: cfg.sasl_msk_iam_session_token.clone(),
                            user_agent: cfg.sasl_msk_iam_user_agent.clone(),
                        }),
                        file_path: cfg.sasl_msk_iam_file_path.clone(),
                        http_socket_path: cfg.sasl_msk_iam_http_socket_path.clone(),
                        http_socket_timeout: to_nanos(cfg.sasl_msk_iam_http_socket_timeout),
                    },
                    ResolvedSaslMechanism::Callback,
                    ResolvedSaslMechanism::Static,
                )
                .map_err(KafkaClientError::Config)?,
            ),
        })
    }

    /// The SASL mechanism name sent in `SaslHandshake`, or `None` when SASL is disabled.
    pub fn mechanism_name(&self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Plain { .. } => Some("PLAIN"),
            Self::ScramSha256 { .. } => Some("SCRAM-SHA-256"),
            Self::ScramSha512 { .. } => Some("SCRAM-SHA-512"),
            Self::Oauthbearer(_) => Some("OAUTHBEARER"),
            Self::MskIam(_) => Some("AWS_MSK_IAM"),
        }
    }
}

async fn resolve_credentials<A: Clone>(m: &ResolvedSaslMechanism<A>) -> Result<A, String> {
    match m {
        ResolvedSaslMechanism::Static(a) => Ok(a.clone()),
        ResolvedSaslMechanism::Callback(cb) => cb().await,
    }
}

// ---------------------------------------------------------------------------
// SASL mechanism sessions (franz-go pkg/sasl)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScramHash {
    Sha256,
    Sha512,
}

impl ScramHash {
    fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
    fn hmac_alg(self) -> ring::hmac::Algorithm {
        match self {
            Self::Sha256 => ring::hmac::HMAC_SHA256,
            Self::Sha512 => ring::hmac::HMAC_SHA512,
        }
    }
    fn pbkdf2_alg(self) -> ring::pbkdf2::Algorithm {
        match self {
            Self::Sha256 => ring::pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => ring::pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }
    fn digest_alg(self) -> &'static ring::digest::Algorithm {
        match self {
            Self::Sha256 => &ring::digest::SHA256,
            Self::Sha512 => &ring::digest::SHA512,
        }
    }
    fn size(self) -> usize {
        self.digest_alg().output_len()
    }
    fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        ring::hmac::sign(&ring::hmac::Key::new(self.hmac_alg(), key), data)
            .as_ref()
            .to_vec()
    }
}

/// SCRAM username/authzid escaping (`strings.NewReplacer("=", "=3D", ",", "=2C")`).
fn scram_escape(s: &str) -> String {
    s.replace('=', "=3D").replace(',', "=2C")
}

/// franz-go `pkg/sasl/scram` session.
struct ScramSession {
    hash: ScramHash,
    step: u8,
    password: String,
    nonce: Vec<u8>,
    client_first_bare: Vec<u8>,
    exp_server_signature: Vec<u8>,
}

impl ScramSession {
    /// `scram.Authenticate`: returns the session and the client-first message.
    fn start(hash: ScramHash, user: &str, pass: &str, nonce_raw: Option<&[u8]>) -> Result<(Self, Vec<u8>), String> {
        if user.is_empty() || pass.is_empty() {
            return Err(format!("{} user and pass must be non-empty", hash.name()));
        }
        let nonce_raw = match nonce_raw {
            Some(n) => n.to_vec(),
            None => {
                let mut buf = [0u8; 20];
                ring::rand::SystemRandom::new()
                    .fill(&mut buf)
                    .map_err(|_| "unable to generate SCRAM nonce".to_string())?;
                buf.to_vec()
            }
        };
        // base64.RawStdEncoding
        let nonce = base64_encode(&nonce_raw).trim_end_matches('=').as_bytes().to_vec();

        let mut bare = Vec::with_capacity(100);
        bare.extend_from_slice(b"n=");
        bare.extend_from_slice(scram_escape(user).as_bytes());
        bare.extend_from_slice(b",r=");
        bare.extend_from_slice(&nonce);

        let mut first = b"n,,".to_vec();
        first.extend_from_slice(&bare);
        Ok((
            Self {
                hash,
                step: 0,
                password: pass.to_string(),
                nonce,
                client_first_bare: bare,
                exp_server_signature: Vec::new(),
            },
            first,
        ))
    }

    fn challenge(&mut self, resp: &[u8]) -> Result<(bool, Vec<u8>), String> {
        let step = self.step;
        self.step += 1;
        match step {
            0 => Ok((false, self.authenticate_client(resp)?)),
            1 => {
                self.verify_server(resp)?;
                Ok((true, Vec::new()))
            }
            _ => Err(format!(
                "challenge / response should be done, but still going at {step}"
            )),
        }
    }

    fn authenticate_client(&mut self, server_first: &[u8]) -> Result<Vec<u8>, String> {
        let kvs: Vec<&[u8]> = server_first.split(|&b| b == b',').collect();
        if kvs.len() < 3 {
            return Err(format!("got {} kvs != exp min 3", kvs.len()));
        }
        let lossy = |b: &[u8]| String::from_utf8_lossy(b).into_owned();

        let server_nonce = kvs[0]
            .strip_prefix(b"r=")
            .ok_or_else(|| format!("unexpected kv {:?} where nonce expected", lossy(kvs[0])))?;
        if !server_nonce.starts_with(&self.nonce) {
            return Err("server did not reply with nonce beginning with client nonce".to_string());
        }
        let salt_b64 = kvs[1]
            .strip_prefix(b"s=")
            .ok_or_else(|| format!("unexpected kv {:?} where salt expected", lossy(kvs[1])))?;
        let salt = base64_decode(&lossy(salt_b64))
            .map_err(|e| format!("server salt {:?} decode err: {e}", lossy(salt_b64)))?;
        let iters_raw = kvs[2]
            .strip_prefix(b"i=")
            .ok_or_else(|| format!("unexpected kv {:?} where iterations expected", lossy(kvs[2])))?;
        let iters: i64 = lossy(iters_raw)
            .parse()
            .map_err(|e| format!("server iterations {:?} parse err: {e}", lossy(iters_raw)))?;
        if iters < 4096 {
            return Err(format!("server iterations {iters} less than minimum 4096"));
        }
        let iters = NonZeroU32::new(u32::try_from(iters).map_err(|_| {
            format!("server iterations {iters} too large")
        })?)
        .expect("iters >= 4096");

        let h = self.hash;
        let mut salted = vec![0u8; h.size()];
        ring::pbkdf2::derive(h.pbkdf2_alg(), iters, &salt, self.password.as_bytes(), &mut salted);
        let client_key = h.hmac(&salted, b"Client Key");
        let stored_key = ring::digest::digest(h.digest_alg(), &client_key);

        let mut final_without_proof = b"c=biws,r=".to_vec();
        final_without_proof.extend_from_slice(server_nonce);

        let mut auth_msg = self.client_first_bare.clone();
        auth_msg.push(b',');
        auth_msg.extend_from_slice(server_first);
        auth_msg.push(b',');
        auth_msg.extend_from_slice(&final_without_proof);

        let mut proof = h.hmac(stored_key.as_ref(), &auth_msg);
        for (p, c) in proof.iter_mut().zip(client_key.iter()) {
            *p ^= c;
        }
        let server_key = h.hmac(&salted, b"Server Key");
        self.exp_server_signature = base64_encode(&h.hmac(&server_key, &auth_msg)).into_bytes();

        let mut out = final_without_proof;
        out.extend_from_slice(b",p=");
        out.extend_from_slice(base64_encode(&proof).as_bytes());
        Ok(out)
    }

    fn verify_server(&self, server_final: &[u8]) -> Result<(), String> {
        let kv = server_final.split(|&b| b == b',').next().unwrap_or_default();
        if let Some(e) = kv.strip_prefix(b"e=") {
            return Err(format!(
                "server sent authentication error {:?}",
                String::from_utf8_lossy(e)
            ));
        }
        let Some(sig) = kv.strip_prefix(b"v=") else {
            return Err(format!(
                "server sent unexpected first kv {:?}",
                String::from_utf8_lossy(kv)
            ));
        };
        if sig != self.exp_server_signature.as_slice() {
            return Err(format!(
                "server signature mismatch; got {:?} != exp {:?}",
                String::from_utf8_lossy(sig),
                String::from_utf8_lossy(&self.exp_server_signature)
            ));
        }
        Ok(())
    }
}

/// franz-go `pkg/sasl/oauth` initial client response.
fn oauthbearer_initial(auth: &KafkaOauthbearerAuth) -> Result<Vec<u8>, String> {
    if auth.token.is_empty() {
        return Err("OAUTHBEARER token must be non-empty".to_string());
    }
    let mut gs2 = String::from("n,");
    if !auth.zid.is_empty() {
        gs2.push_str("a=");
        gs2.push_str(&auth.zid);
    }
    gs2.push(',');
    let mut out = gs2.into_bytes();
    out.extend_from_slice(b"\x01auth=Bearer ");
    out.extend_from_slice(auth.token.as_bytes());
    out.push(0x01);
    // BTreeMap iterates sorted by key, like franz-go's sort.Slice on keys.
    for (k, v) in &auth.extensions {
        if k.is_empty() {
            continue;
        }
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
        out.push(0x01);
    }
    out.push(0x01);
    Ok(out)
}

// --- AWS_MSK_IAM (franz-go pkg/sasl/aws) ---

const MSK_SERVICE: &str = "kafka-cluster";

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Go `net.SplitHostPort`.
pub fn split_host_port(addr: &str) -> Result<(&str, &str), String> {
    let missing_port = || format!("address {addr}: missing port in address");
    if let Some(rest) = addr.strip_prefix('[') {
        let end = rest.find(']').ok_or_else(|| format!("address {addr}: missing ']' in address"))?;
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let port = after.strip_prefix(':').ok_or_else(missing_port)?;
        if port.contains(':') {
            return Err(format!("address {addr}: too many colons in address"));
        }
        return Ok((host, port));
    }
    let (host, port) = addr.rsplit_once(':').ok_or_else(missing_port)?;
    if host.contains(':') {
        return Err(format!("address {addr}: too many colons in address"));
    }
    Ok((host, port))
}

fn identify_region(host: &str) -> Result<String, String> {
    for suffix in [".amazonaws.com", ".amazonaws.com.cn", ".c2s.ic.gov", ".sc2s.sgov.gov"] {
        if let Some(service_region) = host.strip_suffix(suffix) {
            match service_region.rfind('.') {
                Some(dot) => return Ok(service_region[dot + 1..].to_string()),
                None => break,
            }
        }
    }
    Err(format!("cannot determine the region in {host:?}"))
}

/// Go `url.QueryEscape`.
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Go `encoding/json` string encoding (HTML-safe escaping).
fn go_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Converts days since the Unix epoch into a (year, month, day) civil date (UTC).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `time.Now().UTC().Format("20060102T150405Z")`.
fn amz_timestamp(now: SystemTime) -> String {
    let secs = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    format!(
        "{y:04}{mo:02}{d:02}T{:02}{:02}{:02}Z",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60
    )
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

/// franz-go `aws.challenge`: the signed AWS_MSK_IAM JSON payload for broker `addr` (host:port).
fn msk_iam_challenge(auth: &KafkaMskIamAuth, addr: &str, now: SystemTime) -> Result<Vec<u8>, String> {
    let (host, _) = split_host_port(addr)?;
    let region = match identify_region(host) {
        Ok(r) => r,
        Err(err) => std::env::var("AWS_REGION")
            .ok()
            .filter(|r| !r.is_empty())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok().filter(|r| !r.is_empty()))
            .ok_or(err)?,
    };
    let timestamp = amz_timestamp(now);
    let date = &timestamp[..8];
    let scope = format!("{date}/{region}/{MSK_SERVICE}/aws4_request");

    let mut v: BTreeMap<&str, String> = BTreeMap::new();
    v.insert("Action", format!("{MSK_SERVICE}:Connect"));
    v.insert("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string());
    v.insert("X-Amz-Credential", format!("{}/{scope}", auth.access_key));
    v.insert("X-Amz-Date", timestamp.clone());
    v.insert("X-Amz-Expires", "300".to_string());
    v.insert("X-Amz-SignedHeaders", "host".to_string());
    if !auth.session_token.is_empty() {
        v.insert("X-Amz-Security-Token", auth.session_token.clone());
    }
    // url.Values.Encode (sorted by key) with "+" replaced by "%20".
    let qps = v
        .iter()
        .map(|(k, val)| format!("{}={}", query_escape(k), query_escape(val)))
        .collect::<Vec<_>>()
        .join("&")
        .replace('+', "%20");

    const EMPTY_BODY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let canonical = format!("GET\n/\n{qps}\nhost:{host}\n\nhost\n{EMPTY_BODY_SHA256}");
    let canon_hash = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes());
    let sts = format!("AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}", hex_lower(canon_hash.as_ref()));

    let h = ScramHash::Sha256;
    let mut key = format!("AWS4{}", auth.secret_key).into_bytes();
    for part in [date, region.as_str(), MSK_SERVICE, "aws4_request"] {
        key = h.hmac(&key, part.as_bytes());
    }
    let signature = hex_lower(&h.hmac(&key, sts.as_bytes()));
    v.insert("X-Amz-Signature", signature);

    let mut kv: BTreeMap<String, String> = v.into_iter().map(|(k, val)| (k.to_lowercase(), val)).collect();
    kv.insert("host".to_string(), host.to_string());
    kv.insert("version".to_string(), "2020_10_22".to_string());
    let ua = if auth.user_agent.is_empty() {
        format!("franz-go/mimir-rs/{}", hostname())
    } else {
        auth.user_agent.clone()
    };
    kv.insert("user-agent".to_string(), ua);

    // json.Marshal(map[string]string): keys sorted, HTML-safe escaping.
    let mut out = String::from("{");
    for (i, (k, val)) in kv.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        go_json_string(&mut out, k);
        out.push(':');
        go_json_string(&mut out, val);
    }
    out.push('}');
    Ok(out.into_bytes())
}

enum SaslSession {
    Plain,
    Scram(Box<ScramSession>),
    Oauthbearer { error_challenge: Option<String> },
    MskIam,
}

impl SaslSession {
    /// `sasl.Mechanism.Authenticate`: the session plus the first client message.
    async fn start(sasl: &SaslAuthConfig, addr: &str) -> Result<(Self, Vec<u8>), String> {
        match sasl {
            SaslAuthConfig::None => Err("SASL is not configured".to_string()),
            SaslAuthConfig::Plain { username, password } => {
                if username.is_empty() || password.is_empty() {
                    return Err("PLAIN user and pass must be non-empty".to_string());
                }
                // Zid is always empty in Mimir: "\x00" + user + "\x00" + pass.
                let mut msg = Vec::with_capacity(username.len() + password.len() + 2);
                msg.push(0);
                msg.extend_from_slice(username.as_bytes());
                msg.push(0);
                msg.extend_from_slice(password.as_bytes());
                Ok((Self::Plain, msg))
            }
            SaslAuthConfig::ScramSha256 { username, password } => {
                let (s, m) = ScramSession::start(ScramHash::Sha256, username, password, None)?;
                Ok((Self::Scram(Box::new(s)), m))
            }
            SaslAuthConfig::ScramSha512 { username, password } => {
                let (s, m) = ScramSession::start(ScramHash::Sha512, username, password, None)?;
                Ok((Self::Scram(Box::new(s)), m))
            }
            SaslAuthConfig::Oauthbearer(m) => {
                let auth = resolve_credentials(m).await?;
                Ok((
                    Self::Oauthbearer { error_challenge: None },
                    oauthbearer_initial(&auth)?,
                ))
            }
            SaslAuthConfig::MskIam(m) => {
                let auth = resolve_credentials(m).await?;
                Ok((Self::MskIam, msk_iam_challenge(&auth, addr, SystemTime::now())?))
            }
        }
    }

    /// `sasl.Session.Challenge`: returns `(done, next client message)`.
    fn challenge(&mut self, resp: &[u8]) -> Result<(bool, Vec<u8>), String> {
        match self {
            Self::Plain => Ok((true, Vec::new())),
            Self::Scram(s) => s.challenge(resp),
            Self::Oauthbearer { error_challenge } => {
                if !resp.is_empty() {
                    let msg = String::from_utf8_lossy(resp).trim().to_string();
                    if msg.starts_with('{') {
                        *error_challenge = Some(msg);
                        return Ok((true, vec![0x01]));
                    }
                    return Err(format!("unexpected data in oauth response: {msg}"));
                }
                Ok((true, Vec::new()))
            }
            Self::MskIam => {
                if resp.is_empty() {
                    return Err("empty challenge response: failed".to_string());
                }
                Ok((true, Vec::new()))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TlsDialer {
    connector: TlsConnector,
    server_name: Option<ServerName<'static>>,
    insecure_skip_verify: bool,
}

/// Dial + TLS + SASL settings of one Kafka client, built once from [`KafkaConfig`] and shared
/// (`Arc`) by all its connections.
#[derive(Clone)]
pub struct KafkaTransport {
    dial_timeout: Duration,
    tls: Option<TlsDialer>,
    sasl: SaslAuthConfig,
}

impl fmt::Debug for KafkaTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTransport")
            .field("dial_timeout", &self.dial_timeout)
            .field("tls", &self.tls.is_some())
            .field("sasl", &self.sasl)
            .finish()
    }
}

/// A broker connection after `ApiVersions` and SASL, ready to be split into reader/writer tasks.
pub struct EstablishedConn {
    pub stream: KafkaStream,
    /// `api_key -> (min_version, max_version)` advertised by the broker.
    pub api_versions: HashMap<i16, (i16, i16)>,
    /// Next correlation ID to use on this connection.
    pub next_correlation_id: i32,
    /// When the SASL session must be re-established (broker `SessionLifetimeMs`, with kgo's
    /// pessimism). `None` when the session does not expire.
    pub sasl_expiry: Option<Instant>,
}

impl KafkaTransport {
    /// Plaintext transport without SASL.
    pub fn plaintext(dial_timeout: Duration) -> Self {
        Self {
            dial_timeout: if dial_timeout.is_zero() {
                DEFAULT_DIAL_TIMEOUT
            } else {
                dial_timeout
            },
            tls: None,
            sasl: SaslAuthConfig::None,
        }
    }

    /// Port of the dial / TLS / SASL part of `commonKafkaClientOptions`.
    pub fn from_kafka_config(cfg: &KafkaConfig) -> Result<Self, KafkaClientError> {
        let mut t = Self::plaintext(Duration::from_nanos(cfg.dial_timeout.as_nanos().max(0) as u64));
        if cfg.tls_enabled {
            let resolved = cfg
                .tls_client_config()
                .get_tls_config()
                .map_err(|e| KafkaClientError::Config(format!("invalid Kafka TLS config: {e}")))?;
            t = t.with_tls(resolved);
        }
        t.sasl = SaslAuthConfig::from_kafka_config(cfg)?;
        Ok(t)
    }

    pub fn with_tls(mut self, tls: ResolvedClientConfig) -> Self {
        self.tls = Some(TlsDialer {
            connector: TlsConnector::from(tls.config),
            server_name: tls.server_name,
            insecure_skip_verify: tls.insecure_skip_verify,
        });
        self
    }

    pub fn with_sasl(mut self, sasl: SaslAuthConfig) -> Self {
        self.sasl = sasl;
        self
    }

    pub fn dial_timeout(&self) -> Duration {
        self.dial_timeout
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    pub fn sasl(&self) -> &SaslAuthConfig {
        &self.sasl
    }

    /// TCP connect plus TLS handshake (when enabled), bounded by the dial timeout.
    pub async fn dial(&self, addr: &str) -> Result<KafkaStream, KafkaClientError> {
        let fut = async {
            let tcp = TcpStream::connect(addr).await?;
            let _ = tcp.set_nodelay(true);
            let Some(tls) = &self.tls else {
                return Ok(KafkaStream::Plain(tcp));
            };
            // kgo: when ServerName is empty, use the host part of the broker address.
            let name = match &tls.server_name {
                Some(n) => n.clone(),
                None => {
                    let (host, _) = split_host_port(addr).map_err(|e| {
                        KafkaClientError::Config(format!("unable to split host:port for dialing: {e}"))
                    })?;
                    match parse_server_name(host) {
                        Ok(n) => n,
                        Err(_) if tls.insecure_skip_verify => ServerName::IpAddress(
                            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED).into(),
                        ),
                        Err(e) => return Err(KafkaClientError::Config(e.to_string())),
                    }
                }
            };
            let stream = tls.connector.connect(name, tcp).await.map_err(|e| {
                io::Error::new(e.kind(), format!("TLS handshake with broker {addr} failed: {e}"))
            })?;
            Ok(KafkaStream::Tls(Box::new(stream)))
        };
        tokio::time::timeout(self.dial_timeout, fut)
            .await
            .map_err(|_| KafkaClientError::ConnectTimeout {
                addr: addr.to_string(),
            })?
    }

    /// Dials `addr`, discovers API versions and authenticates (kgo `brokerCxn.init`). Each setup
    /// request is bounded by `request_timeout` (minimum 2s).
    pub async fn connect(
        &self,
        addr: &str,
        client_id: &str,
        request_timeout: Duration,
    ) -> Result<EstablishedConn, KafkaClientError> {
        let stream = self.dial(addr).await?;
        let mut io = HandshakeIo {
            stream,
            next_corr_id: 0,
            client_id: Some(StrBytes::from_string(client_id.to_string())),
            timeout: request_timeout.max(Duration::from_secs(2)),
            addr,
            eof_kind: super::kgo_retry::FirstReadEofKind::Tls,
        };
        let api_versions = io.request_api_versions().await?;
        io.eof_kind = super::kgo_retry::FirstReadEofKind::Sasl;
        let sasl_expiry = self.authenticate(&mut io, &api_versions).await?;
        Ok(EstablishedConn {
            next_correlation_id: io.next_corr_id,
            stream: io.stream,
            api_versions,
            sasl_expiry,
        })
    }

    /// kgo `brokerCxn.sasl` + `doSasl`.
    async fn authenticate(
        &self,
        io: &mut HandshakeIo<'_>,
        versions: &HashMap<i16, (i16, i16)>,
    ) -> Result<Option<Instant>, KafkaClientError> {
        let Some(mech) = self.sasl.mechanism_name() else {
            return Ok(None);
        };
        let addr = io.addr;
        let sasl_err = |msg: String| {
            KafkaClientError::Sasl(format!(
                "SASL {mech} authentication with broker {addr} failed: {msg}"
            ))
        };
        let max_version = |key: ApiKey, ours: i16| -> Option<i16> {
            versions
                .get(&(key as i16))
                .map(|&(_, broker_max)| broker_max.min(ours))
                .filter(|v| *v >= 0)
        };

        let mut authenticate = false;
        if let Some(hs_ver) = max_version(ApiKey::SaslHandshake, 1) {
            let mut req = SaslHandshakeRequest::default();
            req.mechanism = StrBytes::from_static_str(mech);
            let resp: SaslHandshakeResponse =
                io.roundtrip(ApiKey::SaslHandshake, hs_ver, &req).await?;
            if let Some(err) = resp.error_code.err() {
                let supported: Vec<&str> = resp.mechanisms.iter().map(|m| m.as_str()).collect();
                return Err(sasl_err(format!(
                    "SaslHandshake: {err} (broker supported mechanisms: {supported:?})"
                )));
            }
            authenticate = hs_ver == 1;
        }

        let (mut session, mut client_write) =
            SaslSession::start(&self.sasl, addr).await.map_err(sasl_err)?;
        if client_write.is_empty() {
            return Err(sasl_err(format!("unexpected server-write sasl with mechanism {mech}")));
        }
        let auth_ver = max_version(ApiKey::SaslAuthenticate, 2).unwrap_or(0);

        let mut prereq = Instant::now();
        let mut lifetime_ms: i64 = 0;
        let mut done = false;
        while !done || !client_write.is_empty() {
            let challenge: Bytes = if !authenticate {
                io.write_raw(&client_write).await?;
                if !done {
                    io.read_frame().await?
                } else {
                    Bytes::new()
                }
            } else {
                let mut req = SaslAuthenticateRequest::default();
                req.auth_bytes = Bytes::from(std::mem::take(&mut client_write));
                prereq = Instant::now();
                let resp: SaslAuthenticateResponse =
                    io.roundtrip(ApiKey::SaslAuthenticate, auth_ver, &req).await?;
                if let Some(err) = resp.error_code.err() {
                    let detail = match resp.error_message {
                        Some(m) => format!("{err}: {}", m.as_str()),
                        None => err.to_string(),
                    };
                    let oauth_ctx = match &session {
                        SaslSession::Oauthbearer { error_challenge: Some(c) } => format!(" (oauthbearer challenge: {c})"),
                        _ => String::new(),
                    };
                    let tag = if err == ResponseError::SaslAuthenticationFailed {
                        "SASL_AUTHENTICATION_FAILED: "
                    } else {
                        ""
                    };
                    return Err(sasl_err(format!("{tag}{detail}{oauth_ctx}")));
                }
                lifetime_ms = resp.session_lifetime_ms;
                resp.auth_bytes
            };
            client_write.clear();
            if !done {
                let (d, next) = session.challenge(&challenge).map_err(sasl_err)?;
                done = d;
                client_write = next;
            }
        }

        if let SaslSession::Oauthbearer { error_challenge: Some(c) } = session {
            return Err(sasl_err(format!("server sent OAUTHBEARER error challenge: {c}")));
        }
        if lifetime_ms <= 0 {
            return Ok(None);
        }
        Ok(Some(session_expiry(lifetime_ms, prereq.elapsed())))
    }

    pub(crate) fn kip368_versions(&self, versions: &HashMap<i16, (i16, i16)>) -> Option<(i16, i16)> {
        let max_version = |key: ApiKey, ours: i16| -> Option<i16> {
            versions
                .get(&(key as i16))
                .map(|&(_, broker_max)| broker_max.min(ours))
                .filter(|v| *v >= 0)
        };
        let hs_ver = max_version(ApiKey::SaslHandshake, 1).filter(|&v| v >= 1)?;
        let auth_ver = max_version(ApiKey::SaslAuthenticate, 2).unwrap_or(0);
        Some((hs_ver, auth_ver))
    }

    pub(crate) async fn start_kip368_auth(
        &self,
        addr: &str,
        mech: &'static str,
    ) -> Result<Kip368AuthFlow, KafkaClientError> {
        let (session, client_write) = SaslSession::start(&self.sasl, addr).await.map_err(|msg| {
            KafkaClientError::Sasl(format!(
                "SASL {mech} authentication with broker {addr} failed: {msg}"
            ))
        })?;
        if client_write.is_empty() {
            return Err(KafkaClientError::Sasl(format!(
                "SASL {mech} authentication with broker {addr} failed: unexpected server-write sasl with mechanism {mech}"
            )));
        }
        Ok(Kip368AuthFlow {
            addr: addr.to_string(),
            mech,
            session,
            client_write,
            done: false,
            lifetime_ms: 0,
            prereq: Instant::now(),
        })
    }
}

pub(crate) struct Kip368AuthFlow {
    addr: String,
    mech: &'static str,
    session: SaslSession,
    client_write: Vec<u8>,
    done: bool,
    lifetime_ms: i64,
    prereq: Instant,
}

impl Kip368AuthFlow {
    pub(crate) fn next_authenticate_request(&mut self) -> Option<SaslAuthenticateRequest> {
        if self.done && self.client_write.is_empty() {
            return None;
        }
        let mut req = SaslAuthenticateRequest::default();
        req.auth_bytes = Bytes::from(std::mem::take(&mut self.client_write));
        self.prereq = Instant::now();
        Some(req)
    }

    pub(crate) fn on_authenticate_response(
        &mut self,
        resp: SaslAuthenticateResponse,
    ) -> Result<(), KafkaClientError> {
        if let Some(err) = resp.error_code.err() {
            let detail = match resp.error_message {
                Some(m) => format!("{err}: {}", m.as_str()),
                None => err.to_string(),
            };
            let oauth_ctx = match &self.session {
                SaslSession::Oauthbearer {
                    error_challenge: Some(c),
                } => format!(" (oauthbearer challenge: {c})"),
                _ => String::new(),
            };
            let tag = if err == ResponseError::SaslAuthenticationFailed {
                "SASL_AUTHENTICATION_FAILED: "
            } else {
                ""
            };
            return Err(self.sasl_err(format!("{tag}{detail}{oauth_ctx}")));
        }
        self.lifetime_ms = resp.session_lifetime_ms;
        self.client_write.clear();
        if !self.done {
            let (d, next) = self
                .session
                .challenge(&resp.auth_bytes)
                .map_err(|m| self.sasl_err(m))?;
            self.done = d;
            self.client_write = next;
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Option<Instant>, KafkaClientError> {
        if let SaslSession::Oauthbearer {
            error_challenge: Some(c),
        } = &self.session
        {
            return Err(self.sasl_err(format!(
                "server sent OAUTHBEARER error challenge: {c}"
            )));
        }
        if self.lifetime_ms <= 0 {
            return Ok(None);
        }
        Ok(Some(session_expiry(self.lifetime_ms, self.prereq.elapsed())))
    }

    fn sasl_err(&self, msg: String) -> KafkaClientError {
        KafkaClientError::Sasl(format!(
            "SASL {} authentication with broker {} failed: {msg}",
            self.mech, self.addr
        ))
    }
}

/// kgo's pessimistic SASL session expiry: lifetime minus max(1s or 1.1x auth latency,
/// 2%-5% of the lifetime), lifetime clamped to one year.
fn session_expiry(lifetime_ms: i64, latency: Duration) -> Instant {
    const MAX_LIFETIME_MS: i64 = 365 * 24 * 3600 * 1000;
    let lifetime_ms = lifetime_ms.min(MAX_LIFETIME_MS);
    let min_pessimism = (1.1 * latency.as_millis() as f64).max(1000.0);
    let mut rnd = [0u8; 8];
    let random = match ring::rand::SystemRandom::new().fill(&mut rnd) {
        Ok(()) => (u64::from_le_bytes(rnd) >> 11) as f64 / (1u64 << 53) as f64,
        Err(_) => 0.5,
    };
    let max_pessimism = lifetime_ms as f64 * (0.05 - 0.03 * random);
    let use_lifetime_ms = lifetime_ms - min_pessimism.max(max_pessimism) as i64;
    let now = Instant::now();
    if use_lifetime_ms <= 0 {
        return now;
    }
    now + Duration::from_millis(use_lifetime_ms as u64)
}

/// Synchronous request/response I/O on a connection before it is shared.
struct HandshakeIo<'a> {
    stream: KafkaStream,
    next_corr_id: i32,
    client_id: Option<StrBytes>,
    timeout: Duration,
    addr: &'a str,
    eof_kind: super::kgo_retry::FirstReadEofKind,
}

impl HandshakeIo<'_> {
    async fn write_raw(&mut self, payload: &[u8]) -> Result<(), KafkaClientError> {
        let mut frame = BytesMut::with_capacity(4 + payload.len());
        frame.put_u32(payload.len() as u32);
        frame.extend_from_slice(payload);
        let timeout = self.timeout;
        let stream = &mut self.stream;
        tokio::time::timeout(timeout, async {
            stream.write_all(&frame).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| KafkaClientError::RequestTimeout(timeout))??;
        Ok(())
    }

    /// Reads one size-prefixed frame. Detects a TLS alert sent to a plaintext client (kgo
    /// `readConn`), and an immediate EOF (often a TLS client talking to a plaintext listener or
    /// vice versa).
    async fn read_frame(&mut self) -> Result<Bytes, KafkaClientError> {
        let addr = self.addr;
        let eof_kind = self.eof_kind;
        let timeout = self.timeout;
        let stream = &mut self.stream;
        let fut = async {
            let mut size_buf = [0u8; 4];
            if let Err(e) = stream.read_exact(&mut size_buf).await {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return Err(KafkaClientError::FirstReadEof(eof_kind.to_string()));
                }
                return Err(e.into());
            }
            let size = i32::from_be_bytes(size_buf);
            if size < 0 || size as usize > MAX_HANDSHAKE_RESPONSE_BYTES {
                let tls_version = u16::from_be_bytes([size_buf[1], size_buf[2]]);
                if size_buf[0] == 21 && tls_version & 0x0300 != 0 {
                    let guess = match tls_version {
                        0x0300 => "SSL v3",
                        0x0301 => "TLS v1.0",
                        0x0302 => "TLS v1.1",
                        0x0303 => "TLS v1.2",
                        0x0304 => "TLS v1.3",
                        _ => "unknown TLS version",
                    };
                    return Err(KafkaClientError::UnexpectedResponse(format!(
                        "invalid large response size {size} > limit {MAX_HANDSHAKE_RESPONSE_BYTES}; the first three bytes received appear to be a tls alert record for {guess}; is this a plaintext connection speaking to a tls endpoint?"
                    )));
                }
                return Err(KafkaClientError::UnexpectedResponse(format!(
                    "invalid response size {size} from broker {addr}"
                )));
            }
            let mut payload = vec![0u8; size as usize];
            stream.read_exact(&mut payload).await?;
            Ok(Bytes::from(payload))
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| KafkaClientError::RequestTimeout(timeout))?
    }

    async fn roundtrip<Req, Resp>(
        &mut self,
        api_key: ApiKey,
        version: i16,
        req: &Req,
    ) -> Result<Resp, KafkaClientError>
    where
        Req: Request + Encodable,
        Resp: Decodable + HeaderVersion,
    {
        let corr_id = self.next_corr_id;
        self.next_corr_id = self.next_corr_id.wrapping_add(1);
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = version;
        header.correlation_id = corr_id;
        header.client_id = self.client_id.clone();

        let mut frame = BytesMut::with_capacity(128);
        frame.put_u32(0);
        header.encode(&mut frame, Req::header_version(version))?;
        req.encode(&mut frame, version)?;
        let len = (frame.len() - 4) as u32;
        frame[0..4].copy_from_slice(&len.to_be_bytes());

        let timeout = self.timeout;
        let stream = &mut self.stream;
        tokio::time::timeout(timeout, async {
            stream.write_all(&frame).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| KafkaClientError::RequestTimeout(timeout))??;

        let mut buf = self.read_frame().await?;
        // ApiVersions responses always use header v0 (KIP-511).
        let resp_header_ver = if api_key == ApiKey::ApiVersions {
            0
        } else {
            Resp::header_version(version)
        };
        let header = ResponseHeader::decode(&mut buf, resp_header_ver)?;
        if header.correlation_id != corr_id {
            return Err(KafkaClientError::UnexpectedResponse(format!(
                "correlation ID mismatch: got {} != exp {corr_id}",
                header.correlation_id
            )));
        }
        Ok(Resp::decode(&mut buf, version)?)
    }

    /// `ApiVersions` v3 (falls back to v0 if the broker rejects v3).
    async fn request_api_versions(&mut self) -> Result<HashMap<i16, (i16, i16)>, KafkaClientError> {
        let mut req = ApiVersionsRequest::default();
        req.client_software_name = StrBytes::from_static_str("mimir-rs");
        req.client_software_version = StrBytes::from_static_str("0.1.0");

        let resp: ApiVersionsResponse = match self.roundtrip::<_, ApiVersionsResponse>(ApiKey::ApiVersions, 3, &req).await {
            Ok(r) if r.error_code.err() != Some(ResponseError::UnsupportedVersion) => r,
            Ok(_) | Err(KafkaClientError::Protocol(_)) => {
                self.roundtrip(ApiKey::ApiVersions, 0, &ApiVersionsRequest::default())
                    .await?
            }
            Err(e) => return Err(e),
        };
        if let Some(err) = resp.error_code.err() {
            return Err(KafkaClientError::Broker(err));
        }
        Ok(resp
            .api_keys
            .into_iter()
            .map(|k| (k.api_key, (k.min_version, k.max_version)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_host_port_matches_go() {
        assert_eq!(split_host_port("localhost:9092").unwrap(), ("localhost", "9092"));
        assert_eq!(split_host_port("127.0.0.1:9092").unwrap(), ("127.0.0.1", "9092"));
        assert_eq!(split_host_port("[::1]:9092").unwrap(), ("::1", "9092"));
        assert_eq!(
            split_host_port("localhost").unwrap_err(),
            "address localhost: missing port in address"
        );
        assert_eq!(
            split_host_port("::1:9092").unwrap_err(),
            "address ::1:9092: too many colons in address"
        );
    }

    #[test]
    fn scram_client_messages_match_rfc7677_vector() {
        // RFC 7677 section 3 test vector (SCRAM-SHA-256).
        let nonce_b64 = "rOprNGfwEbeRWgbNEkqO";
        let mut s = ScramSession {
            hash: ScramHash::Sha256,
            step: 0,
            password: "pencil".into(),
            nonce: nonce_b64.as_bytes().to_vec(),
            client_first_bare: b"n=user,r=rOprNGfwEbeRWgbNEkqO".to_vec(),
            exp_server_signature: Vec::new(),
        };
        let server_first = b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let (done, client_final) = s.challenge(server_first).unwrap();
        assert!(!done);
        assert_eq!(
            String::from_utf8(client_final).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        let (done, next) = s
            .challenge(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
        assert!(done);
        assert!(next.is_empty());
    }

    #[test]
    fn scram_rejects_bad_server_messages_like_franz_go() {
        let (mut s, first) =
            ScramSession::start(ScramHash::Sha512, "u=s,er", "p", Some(b"0123456789abcdefghij")).unwrap();
        let first = String::from_utf8(first).unwrap();
        assert!(first.starts_with("n,,n=u=3Ds=2Cer,r="), "{first}");
        // RawStdEncoding: no padding.
        assert!(!first.ends_with('='));
        let nonce = String::from_utf8(s.nonce.clone()).unwrap();
        let low_iter = format!("r={nonce}x,s=c2FsdA==,i=1024");
        assert_eq!(
            s.challenge(low_iter.as_bytes()).unwrap_err(),
            "server iterations 1024 less than minimum 4096"
        );
        let (mut s, _) = ScramSession::start(ScramHash::Sha256, "u", "p", None).unwrap();
        assert_eq!(
            s.challenge(b"r=other,s=c2FsdA==,i=4096").unwrap_err(),
            "server did not reply with nonce beginning with client nonce"
        );
        assert!(ScramSession::start(ScramHash::Sha256, "", "p", None).is_err());

        let s = ScramSession {
            hash: ScramHash::Sha256,
            step: 1,
            password: String::new(),
            nonce: Vec::new(),
            client_first_bare: Vec::new(),
            exp_server_signature: b"abc".to_vec(),
        };
        assert_eq!(
            s.verify_server(b"e=invalid-proof").unwrap_err(),
            "server sent authentication error \"invalid-proof\""
        );
        assert!(s.verify_server(b"x=1").unwrap_err().starts_with("server sent unexpected first kv"));
        assert!(s.verify_server(b"v=abd").unwrap_err().starts_with("server signature mismatch"));
        assert!(s.verify_server(b"v=abc").is_ok());
    }

    #[test]
    fn oauthbearer_initial_message_matches_franz_go() {
        let mut ext = BTreeMap::new();
        ext.insert("b".to_string(), "2".to_string());
        ext.insert("a".to_string(), "1".to_string());
        ext.insert(String::new(), "skipped".to_string());
        let auth = KafkaOauthbearerAuth {
            token: "tok".into(),
            zid: "admin".into(),
            extensions: ext,
        };
        assert_eq!(
            oauthbearer_initial(&auth).unwrap(),
            b"n,a=admin,\x01auth=Bearer tok\x01a=1\x01b=2\x01\x01".to_vec()
        );
        let auth = KafkaOauthbearerAuth {
            token: String::new(),
            zid: String::new(),
            extensions: BTreeMap::new(),
        };
        assert_eq!(
            oauthbearer_initial(&auth).unwrap_err(),
            "OAUTHBEARER token must be non-empty"
        );
        // RFC 7628 §3.2.2 error challenge yields 0x01 and captures JSON.
        let mut s = SaslSession::Oauthbearer { error_challenge: None };
        let (done, reply) = s.challenge(br#"{"status":"invalid_token","scope":"kafka"}"#).unwrap();
        assert!(done);
        assert_eq!(reply, vec![0x01]);
        assert!(matches!(
            s,
            SaslSession::Oauthbearer { error_challenge: Some(ref m) } if m.contains("invalid_token")
        ));
    }

    #[test]
    fn msk_iam_challenge_is_sigv4_signed() {
        let auth = KafkaMskIamAuth {
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: String::new(),
            user_agent: "ua".into(),
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_440_938_160); // 2015-08-30T12:36:00Z
        let out = msk_iam_challenge(&auth, "b-1.cluster.kafka.us-east-1.amazonaws.com:9098", now).unwrap();
        let s = String::from_utf8(out).unwrap();
        let v: BTreeMap<String, String> = serde_json::from_str(&s).unwrap();
        assert_eq!(v["action"], "kafka-cluster:Connect");
        assert_eq!(v["host"], "b-1.cluster.kafka.us-east-1.amazonaws.com");
        assert_eq!(v["x-amz-credential"], "AKIDEXAMPLE/20150830/us-east-1/kafka-cluster/aws4_request");
        assert_eq!(v["x-amz-date"], "20150830T123600Z");
        assert_eq!(v["x-amz-expires"], "300");
        assert_eq!(v["x-amz-signedheaders"], "host");
        assert_eq!(v["version"], "2020_10_22");
        assert_eq!(v["user-agent"], "ua");
        assert_eq!(v["x-amz-signature"].len(), 64);
        // Keys are emitted sorted, like Go's json.Marshal of a map.
        let keys: Vec<&String> = v.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
        assert!(s.starts_with("{\"action\":"));

        assert!(msk_iam_challenge(&auth, "nohostport", now).is_err());
    }

    #[test]
    fn civil_dates() {
        assert_eq!(amz_timestamp(UNIX_EPOCH), "19700101T000000Z");
        assert_eq!(
            amz_timestamp(UNIX_EPOCH + Duration::from_secs(951_782_400)),
            "20000229T000000Z"
        );
    }

    #[test]
    fn go_json_escaping() {
        let mut s = String::new();
        go_json_string(&mut s, "a<b>&\"\\\n\u{1}");
        assert_eq!(s, "\"a\\u003cb\\u003e\\u0026\\\"\\\\\\n\\u0001\"");
        assert_eq!(query_escape("a b+/=~"), "a+b%2B%2F%3D~");
    }

    #[test]
    fn plain_sasl_config_with_empty_username_disables_sasl() {
        let cfg = KafkaConfig::default();
        assert!(matches!(
            SaslAuthConfig::from_kafka_config(&cfg).unwrap(),
            SaslAuthConfig::None
        ));
        let cfg = KafkaConfig {
            sasl_mechanism: SASLMechanism::ScramSha256,
            sasl_username: "u".into(),
            sasl_password: "secret".into(),
            ..Default::default()
        };
        let sasl = SaslAuthConfig::from_kafka_config(&cfg).unwrap();
        assert_eq!(sasl.mechanism_name(), Some("SCRAM-SHA-256"));
        assert!(!format!("{sasl:?}").contains("secret"));
    }

    #[tokio::test]
    async fn d1_5_immediate_eof_on_api_versions_returns_non_retryable_first_read_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                // Read the client's ApiVersions request then close cleanly (FIN -> UnexpectedEof).
                let mut tmp = [0u8; 256];
                let _ = sock.read(&mut tmp).await;
                let _ = sock.shutdown().await;
                drop(sock);
            }
        });

        let transport = KafkaTransport::plaintext(Duration::from_secs(2));
        let err = match transport.connect(&addr, "test", Duration::from_secs(2)).await {
            Ok(_) => panic!("expected FirstReadEof error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, KafkaClientError::FirstReadEof(ref m) if m == &super::super::kgo_retry::FirstReadEofKind::Tls.to_string()),
            "expected FirstReadEof(Tls), got {err:?}"
        );
        assert!(!super::super::kgo_retry::is_retryable_broker_err(&err));
        srv.abort();
    }
}
