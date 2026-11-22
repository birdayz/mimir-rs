//! Google Cloud Storage backend for the blocks-storage bucket client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/bucket_client.go (`NewBucketClient`,
//!   `retryAlwaysBucket`)
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/config.go
//! - github.com/thanos-io/objstore/providers/gcs/gcs.go (`NewBucketWithConfig`,
//!   `appendHttpOptions`, `newBucket`)
//! - cloud.google.com/go/storage invoke.go (`run`: retry policy, idempotency, max attempts,
//!   default backoff, 408/429/5xx retries + Retry-After) and storage.go
//! - golang.org/x/oauth2/google default.go / google.go (`CredentialsFromJSON`,
//!   `FindDefaultCredentials` credential file types), `jwt/jwt.go`, `google/externalaccount`,
//!   `google/internal/impersonate`, `google/internal/stsexchange`

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use mimir_config::GCSConfig;
use object_store::gcp::GcpCredential;
use object_store::path::{Path, DELIMITER};
use object_store::{
    Attribute, BackoffConfig, CredentialProvider, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, RetryConfig, UploadPart,
};
use rand::Rng;
use reqwest::header::{
    HeaderName, HeaderValue, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING,
    CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
};
use reqwest::{Method, StatusCode};
use ring::signature::RsaKeyPair;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::PrivateKeyDer;

use super::exthttp;
use super::BucketError;

const STORE: &str = "GCS";
const DEFAULT_GCS_BASE_URL: &str = "https://storage.googleapis.com";
const STORAGE_FULL_CONTROL_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.full_control";
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_SCOPES_SPACE: &str = "https://www.googleapis.com/auth/devstorage.full_control https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_SCOPES_COMMA: &str = "https://www.googleapis.com/auth/devstorage.full_control,https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_STS_TOKEN_URL: &str = "https://sts.googleapis.com/v1/token";
const DEFAULT_METADATA_HOST: &str = "metadata.google.internal";
const DEFAULT_METADATA_IP: &str = "169.254.169.254";
const VERSION_HEADER: &str = "x-goog-generation";
const VERSION_MATCH: &str = "x-goog-if-generation-match";
const META_PREFIX: &str = "x-goog-meta-";
const TOKEN_MIN_TTL: Duration = Duration::from_secs(4 * 60);
const EXPIRY_DELTA: Duration = Duration::from_secs(10);

/// Go `golang.org/x/oauth2/google.Endpoint.TokenURL`.
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Credential JSON kinds accepted by Go `google.CredentialsFromJSON`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialKind {
    ServiceAccount,
    AuthorizedUser,
    ExternalAccount,
    ImpersonatedServiceAccount,
}

/// Classifies a credentials JSON like Go `CredentialsFromJSONWithParams`, failing explicitly for
/// the kinds mimir-rs cannot use (with the same "missing 'type'" / "unknown credential type"
/// messages as Go for invalid ones).
pub fn credential_kind(json: &str) -> Result<CredentialKind, String> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    if v.get("web").is_some() || v.get("installed").is_some() {
        return Err(unsupported_kind(
            "OAuth client (client_credentials.json \"web\"/\"installed\")",
        ));
    }
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "service_account" => Ok(CredentialKind::ServiceAccount),
        "authorized_user" => Ok(CredentialKind::AuthorizedUser),
        "external_account" => Ok(CredentialKind::ExternalAccount),
        "impersonated_service_account" => Ok(CredentialKind::ImpersonatedServiceAccount),
        t @ ("external_account_authorized_user" | "gdch_service_account") => {
            Err(unsupported_kind(&format!("{t:?}")))
        }
        "" => Err("missing 'type' field in credentials".to_string()),
        t => Err(format!("unknown credential type: {t:?}")),
    }
}

fn unsupported_kind(kind: &str) -> String {
    format!("GCS credential type {kind} is not supported by mimir-rs for the gcs backend")
}

/// Retry config for the Go storage client with `WithMaxAttempts(max_attempts)`
/// (applied only when > 0) and gax default backoff (1s initial, 30s max, multiplier 2).
pub fn retry_config(max_attempts: i64, retry: bool) -> RetryConfig {
    let max_retries = if !retry {
        0
    } else if max_attempts > 0 {
        (max_attempts - 1) as usize
    } else {
        usize::MAX
    };
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            base: 2.0,
        },
        max_retries,
        retry_timeout: Duration::MAX,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CredentialSource {
    ServiceAccountKey(String),
    AuthorizedUserJson(String),
    ExternalAccountJson(String),
    ImpersonatedServiceAccountJson(String),
    ApplicationCredentialsFile(String),
    Metadata,
}

fn credential_source(
    cfg: &GCSConfig,
    env: &dyn Fn(&str) -> Option<String>,
    read: &dyn Fn(&str) -> Option<std::io::Result<String>>,
) -> Result<CredentialSource, String> {
    if !cfg.service_account.is_empty() {
        return match credential_kind(&cfg.service_account)
            .map_err(|e| format!("failed to create credentials from JSON: {e}"))?
        {
            CredentialKind::ServiceAccount => {
                Ok(CredentialSource::ServiceAccountKey(cfg.service_account.clone()))
            }
            CredentialKind::AuthorizedUser => {
                Ok(CredentialSource::AuthorizedUserJson(cfg.service_account.clone()))
            }
            CredentialKind::ExternalAccount => {
                Ok(CredentialSource::ExternalAccountJson(cfg.service_account.clone()))
            }
            CredentialKind::ImpersonatedServiceAccount => Ok(
                CredentialSource::ImpersonatedServiceAccountJson(cfg.service_account.clone()),
            ),
        };
    }
    if let Some(path) = env("GOOGLE_APPLICATION_CREDENTIALS").filter(|p| !p.is_empty()) {
        let content = read(&path)
            .unwrap_or_else(|| Err(std::io::Error::from(std::io::ErrorKind::NotFound)))
            .map_err(|e| {
                format!(
                    "google: error getting credentials using GOOGLE_APPLICATION_CREDENTIALS environment variable: open {path}: {e}"
                )
            })?;
        credential_kind(&content).map_err(|e| {
            format!(
                "google: error getting credentials using GOOGLE_APPLICATION_CREDENTIALS environment variable: {e}"
            )
        })?;
        return Ok(CredentialSource::ApplicationCredentialsFile(path));
    }
    if let Some(home) = env("HOME").filter(|h| !h.is_empty()) {
        let path = format!("{home}/.config/gcloud/application_default_credentials.json");
        if let Some(Ok(content)) = read(&path) {
            credential_kind(&content).map_err(|e| {
                format!("google: error getting credentials using well-known file ({path}): {e}")
            })?;
            return Ok(CredentialSource::ApplicationCredentialsFile(path));
        }
    }
    Ok(CredentialSource::Metadata)
}

#[async_trait]
trait GcpAuth: Send + Sync + fmt::Debug {
    async fn get_token(&self) -> object_store::Result<String>;
    async fn invalidate(&self);
}

#[derive(Debug)]
struct NoOauthAuth;

#[async_trait]
impl GcpAuth for NoOauthAuth {
    async fn get_token(&self) -> object_store::Result<String> {
        Ok(String::new())
    }
    async fn invalidate(&self) {}
}

#[derive(serde::Deserialize)]
struct ServiceAccountJson {
    #[serde(default)]
    client_email: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    private_key_id: String,
    #[serde(default)]
    token_uri: String,
    #[serde(default)]
    audience: String,
    #[serde(default)]
    gcs_base_url: Option<String>,
    #[serde(default)]
    disable_oauth: bool,
}

#[derive(serde::Serialize)]
struct JwtHeader<'a> {
    alg: &'a str,
    typ: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    kid: &'a str,
}

#[derive(serde::Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

/// Internal `PutOptions::tags` key requesting `x-goog-if-generation-not-match`.
pub(crate) const IF_GEN_NOT_MATCH_TAG: &str = "__mimir_if_generation_not_match";

struct ServiceAccountJwtProvider {
    issuer: String,
    key_id: String,
    private_key_pem: String,
    private_key: std::sync::OnceLock<Result<RsaKeyPair, String>>,
    token_uri: String,
    audience: String,
    scopes: String,
    client: reqwest::Client,
    cache: tokio::sync::Mutex<Option<(String, Option<Instant>)>>,
}

impl fmt::Debug for ServiceAccountJwtProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAccountJwtProvider")
            .field("issuer", &self.issuer)
            .field("token_uri", &self.token_uri)
            .finish()
    }
}

impl ServiceAccountJwtProvider {
    fn from_sa_json(j: &ServiceAccountJson) -> Result<Self, String> {
        let token_uri = if j.token_uri.is_empty() {
            GOOGLE_TOKEN_URL.to_string()
        } else {
            j.token_uri.clone()
        };
        Ok(Self {
            issuer: j.client_email.clone(),
            key_id: j.private_key_id.clone(),
            private_key_pem: j.private_key.clone(),
            private_key: std::sync::OnceLock::new(),
            token_uri,
            audience: j.audience.clone(),
            scopes: DEFAULT_SCOPES_SPACE.to_string(),
            client: exthttp::bounded_auth_client(),
            cache: tokio::sync::Mutex::new(None),
        })
    }

    fn key_pair(&self) -> object_store::Result<&RsaKeyPair> {
        let res = self.private_key.get_or_init(|| {
            let key_der = PrivateKeyDer::from_pem_slice(self.private_key_pem.as_bytes())
                .map_err(|e| format!("invalid service account private_key PEM: {e}"))?;
            match &key_der {
                PrivateKeyDer::Pkcs8(k) => RsaKeyPair::from_pkcs8(k.secret_pkcs8_der()),
                PrivateKeyDer::Pkcs1(k) => RsaKeyPair::from_der(k.secret_pkcs1_der()),
                _ => return Err("unsupported private key format in service account".to_string()),
            }
            .map_err(|e| format!("invalid RSA private key: {e}"))
        });
        res.as_ref().map_err(|e| object_store::Error::Generic {
            store: STORE,
            source: e.clone().into(),
        })
    }

    fn build_signed_jwt(&self) -> object_store::Result<String> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Go jws.go subtracts 10s from iat for minor clock skew.
        let iat = now_secs.saturating_sub(10);
        let exp = iat + 3600;
        let aud = if self.audience.is_empty() {
            self.token_uri.as_str()
        } else {
            self.audience.as_str()
        };
        let hdr = serde_json::to_vec(&JwtHeader {
            alg: "RS256",
            typ: "JWT",
            kid: &self.key_id,
        })
        .map_err(|e| object_store::Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;
        let claims = serde_json::to_vec(&JwtClaims {
            iss: &self.issuer,
            scope: &self.scopes,
            aud,
            iat,
            exp,
        })
        .map_err(|e| object_store::Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(hdr),
            URL_SAFE_NO_PAD.encode(claims)
        );
        let key = self.key_pair()?;
        let mut sig = vec![0u8; key.public().modulus_len()];
        key
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                signing_input.as_bytes(),
                &mut sig,
            )
            .map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: format!("JWT sign failed: {e:?}").into(),
            })?;
        Ok(format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(sig)))
    }
}

#[async_trait]
impl GcpAuth for ServiceAccountJwtProvider {
    async fn get_token(&self) -> object_store::Result<String> {
        let mut cache = self.cache.lock().await;
        if let Some((tok, exp)) = cache.as_ref() {
            if exp.is_none_or(|e| Instant::now() + EXPIRY_DELTA < e) {
                return Ok(tok.clone());
            }
        }
        let jwt = self.build_signed_jwt()?;
        let generic = |e: String| object_store::Error::Generic {
            store: STORE,
            source: format!("oauth2: cannot fetch token: {e}").into(),
        };
        let mut last_err = String::new();
        for attempt in 0..=3usize {
            let started = Instant::now();
            match self
                .client
                .post(&self.token_uri)
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                    ("assertion", jwt.as_str()),
                ])
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp.text().await.map_err(|e| generic(e.to_string()))?;
                    if status.is_success() {
                        let tok: TokenResponse =
                            serde_json::from_str(&body).map_err(|e| generic(e.to_string()))?;
                        if tok.access_token.is_empty() {
                            return Err(generic("response missing access_token".to_string()));
                        }
                        let expiry = tok
                            .expires_in
                            .filter(|s| *s > 0)
                            .map(|s| started + Duration::from_secs(s));
                        *cache = Some((tok.access_token.clone(), expiry));
                        return Ok(tok.access_token);
                    }
                    last_err = format!("{status}\nResponse: {body}");
                    if attempt < 3 && is_retryable_status(status) {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                        tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
                Err(e) => {
                    last_err = e.to_string();
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
            }
        }
        Err(generic(last_err))
    }

    async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }
}

#[derive(Debug)]
struct GceMetadataProvider {
    client: reqwest::Client,
    cache: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl GceMetadataProvider {
    fn new() -> Self {
        Self {
            client: exthttp::bounded_auth_client(),
            cache: tokio::sync::Mutex::new(None),
        }
    }

    async fn fetch_from_host(&self, host: &str) -> Result<(String, Duration), String> {
        let url = format!(
            "http://{host}/computeMetadata/v1/instance/service-accounts/default/token"
        );
        let mut last_err = String::new();
        for attempt in 0..=3usize {
            match self
                .client
                .get(&url)
                .query(&[("scopes", DEFAULT_SCOPES_COMMA)])
                .header("Metadata-Flavor", "Google")
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp.text().await.map_err(|e| e.to_string())?;
                    if status.is_success() {
                        let tok: TokenResponse = serde_json::from_str(&body)
                            .map_err(|e| format!("oauth2/google: invalid token JSON from metadata: {e}"))?;
                        if tok.access_token.is_empty() || tok.expires_in.unwrap_or(0) == 0 {
                            return Err("oauth2/google: incomplete token received from metadata".to_string());
                        }
                        let ttl = Duration::from_secs(tok.expires_in.unwrap_or(3600));
                        return Ok((tok.access_token, ttl));
                    }
                    last_err = format!("{status}: {body}");
                    if attempt < 3 && is_retryable_status(status) {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                        tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                        continue;
                    }
                    return Err(last_err);
                }
                Err(e) => {
                    last_err = e.to_string();
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                        continue;
                    }
                    return Err(last_err);
                }
            }
        }
        Err(last_err)
    }
}

#[async_trait]
impl GcpAuth for GceMetadataProvider {
    async fn get_token(&self) -> object_store::Result<String> {
        let mut cache = self.cache.lock().await;
        if let Some((tok, exp)) = cache.as_ref() {
            if Instant::now() + TOKEN_MIN_TTL < *exp {
                return Ok(tok.clone());
            }
        }
        let host = std::env::var("GCE_METADATA_HOST")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("GCE_METADATA_ROOT")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| DEFAULT_METADATA_HOST.to_string());
        let ip = std::env::var("GCE_METADATA_IP")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_METADATA_IP.to_string());

        let (tok, ttl) = match self.fetch_from_host(&host).await {
            Ok(v) => v,
            Err(_) => self.fetch_from_host(&ip).await.map_err(|e| {
                object_store::Error::Generic {
                    store: STORE,
                    source: format!("metadata token request failed: {e}").into(),
                }
            })?,
        };
        *cache = Some((tok.clone(), Instant::now() + ttl));
        Ok(tok)
    }

    async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }
}

/// Go `authorized_user` credentials: OAuth2 refresh-token flow against `token_uri`
/// (default [`GOOGLE_TOKEN_URL`]), client credentials sent in the form body
/// (`AuthStyleInParams`), tokens refreshed 10s before expiry (oauth2 `defaultExpiryDelta`).
pub struct AuthorizedUserProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    token_url: String,
    client: reqwest::Client,
    cache: tokio::sync::Mutex<Option<(Arc<GcpCredential>, Option<Instant>)>>,
}

impl fmt::Debug for AuthorizedUserProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizedUserProvider")
            .field("client_id", &self.client_id)
            .field("token_url", &self.token_url)
            .finish()
    }
}

#[derive(serde::Deserialize)]
struct AuthorizedUserJson {
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    client_secret: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    token_uri: String,
    #[serde(default)]
    gcs_base_url: Option<String>,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

impl AuthorizedUserProvider {
    pub fn from_json(json: &str) -> Result<Self, String> {
        let j: AuthorizedUserJson = serde_json::from_str(json).map_err(|e| e.to_string())?;
        Ok(Self {
            client_id: j.client_id,
            client_secret: j.client_secret,
            refresh_token: j.refresh_token,
            token_url: if j.token_uri.is_empty() {
                GOOGLE_TOKEN_URL.to_string()
            } else {
                j.token_uri
            },
            client: exthttp::bounded_auth_client(),
            cache: tokio::sync::Mutex::new(None),
        })
    }

    pub async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }
}

#[async_trait]
impl CredentialProvider for AuthorizedUserProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let mut cache = self.cache.lock().await;
        if let Some((cred, expiry)) = cache.as_ref() {
            if expiry.is_none_or(|e| Instant::now() + EXPIRY_DELTA < e) {
                return Ok(Arc::clone(cred));
            }
        }
        let generic = |e: String| object_store::Error::Generic {
            store: STORE,
            source: format!("oauth2: cannot fetch token: {e}").into(),
        };
        let mut last_err = String::new();
        for attempt in 0..=3usize {
            let started = Instant::now();
            match self
                .client
                .post(&self.token_url)
                .form(&[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", self.refresh_token.as_str()),
                    ("client_id", self.client_id.as_str()),
                    ("client_secret", self.client_secret.as_str()),
                ])
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp.text().await.map_err(|e| generic(e.to_string()))?;
                    if status.is_success() {
                        let tok: TokenResponse =
                            serde_json::from_str(&body).map_err(|e| generic(e.to_string()))?;
                        let cred = Arc::new(GcpCredential {
                            bearer: tok.access_token,
                        });
                        let expiry = tok
                            .expires_in
                            .filter(|s| *s > 0)
                            .map(|s| started + Duration::from_secs(s));
                        *cache = Some((Arc::clone(&cred), expiry));
                        return Ok(cred);
                    }
                    last_err = format!("{status}\nResponse: {body}");
                    if attempt < 3 && is_retryable_status(status) {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                        tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
                Err(e) => {
                    last_err = e.to_string();
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
            }
        }
        Err(generic(last_err))
    }
}

#[async_trait]
impl GcpAuth for AuthorizedUserProvider {
    async fn get_token(&self) -> object_store::Result<String> {
        Ok(self.get_credential().await?.bearer.clone())
    }
    async fn invalidate(&self) {
        AuthorizedUserProvider::invalidate(self).await;
    }
}

#[derive(serde::Deserialize)]
struct ImpersonatedServiceAccountJson {
    #[serde(default)]
    service_account_impersonation_url: String,
    #[serde(default)]
    source_credentials: Option<serde_json::Value>,
    #[serde(default)]
    delegates: Vec<String>,
    #[serde(default)]
    gcs_base_url: Option<String>,
}

#[derive(serde::Serialize)]
struct GenerateAccessTokenReq<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    delegates: &'a Vec<String>,
    lifetime: String,
    scope: &'a [String],
}

#[derive(serde::Deserialize)]
struct ImpersonateTokenResponse {
    #[serde(rename = "accessToken", default)]
    access_token: String,
    #[serde(rename = "expireTime", default)]
    expire_time: String,
}

/// Go `oauth2/google/internal/impersonate.ImpersonateTokenSource`.
struct ImpersonatedServiceAccountProvider {
    impersonation_url: String,
    delegates: Vec<String>,
    scopes: Vec<String>,
    lifetime_seconds: Option<u64>,
    source: Arc<dyn GcpAuth>,
    client: reqwest::Client,
    cache: tokio::sync::Mutex<Option<(String, Option<Instant>)>>,
}

impl fmt::Debug for ImpersonatedServiceAccountProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImpersonatedServiceAccountProvider")
            .field("impersonation_url", &self.impersonation_url)
            .finish()
    }
}

impl ImpersonatedServiceAccountProvider {
    fn from_json(json: &str) -> Result<(Self, Option<String>), String> {
        let j: ImpersonatedServiceAccountJson =
            serde_json::from_str(json).map_err(|e| e.to_string())?;
        if j.service_account_impersonation_url.is_empty() || j.source_credentials.is_none() {
            return Err(
                "missing 'source_credentials' field or 'service_account_impersonation_url' in credentials"
                    .to_string(),
            );
        }
        let source_val = j.source_credentials.as_ref().ok_or_else(|| {
            "missing 'source_credentials' field or 'service_account_impersonation_url' in credentials"
                .to_string()
        })?;
        let source_json = serde_json::to_string(source_val).map_err(|e| e.to_string())?;
        let (source, _) = auth_from_json(&source_json)?;
        Ok((
            Self {
                impersonation_url: j.service_account_impersonation_url,
                delegates: j.delegates,
                scopes: vec![
                    STORAGE_FULL_CONTROL_SCOPE.to_string(),
                    CLOUD_PLATFORM_SCOPE.to_string(),
                ],
                lifetime_seconds: None,
                source,
                client: exthttp::bounded_auth_client(),
                cache: tokio::sync::Mutex::new(None),
            },
            j.gcs_base_url.filter(|s| !s.is_empty()),
        ))
    }
}

#[async_trait]
impl GcpAuth for ImpersonatedServiceAccountProvider {
    async fn get_token(&self) -> object_store::Result<String> {
        let mut cache = self.cache.lock().await;
        if let Some((tok, exp)) = cache.as_ref() {
            if exp.is_none_or(|e| Instant::now() + EXPIRY_DELTA < e) {
                return Ok(tok.clone());
            }
        }
        let generic = |e: String| object_store::Error::Generic {
            store: STORE,
            source: format!("oauth2/google: {e}").into(),
        };
        let lifetime = format!("{}s", self.lifetime_seconds.filter(|&s| s > 0).unwrap_or(3600));
        let req_body = GenerateAccessTokenReq {
            delegates: &self.delegates,
            lifetime,
            scope: &self.scopes,
        };
        let mut source_refreshed = false;
        let mut last_err = String::new();
        for attempt in 0..=3usize {
            let src_token = self.source.get_token().await?;
            let started = Instant::now();
            match self
                .client
                .post(&self.impersonation_url)
                .bearer_auth(&src_token)
                .json(&req_body)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp.text().await.map_err(|e| generic(e.to_string()))?;
                    if status == StatusCode::UNAUTHORIZED && !source_refreshed {
                        self.source.invalidate().await;
                        source_refreshed = true;
                        continue;
                    }
                    if status.is_success() {
                        let parsed: ImpersonateTokenResponse = serde_json::from_str(&body)
                            .map_err(|e| generic(format!("unable to parse response: {e}")))?;
                        if parsed.access_token.is_empty() {
                            return Err(generic(
                                "unable to parse response: empty accessToken".to_string(),
                            ));
                        }
                        let exp_dt = chrono::DateTime::parse_from_rfc3339(&parsed.expire_time)
                            .map_err(|e| generic(format!("unable to parse expiry: {e}")))?;
                        let ttl = exp_dt
                            .with_timezone(&chrono::Utc)
                            .signed_duration_since(chrono::Utc::now())
                            .to_std()
                            .unwrap_or(Duration::ZERO);
                        let expiry = Some(started + ttl);
                        *cache = Some((parsed.access_token.clone(), expiry));
                        return Ok(parsed.access_token);
                    }
                    last_err = format!("status code {}: {body}", status.as_u16());
                    if attempt < 3 && is_retryable_status(status) {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                        tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
                Err(e) => {
                    last_err = format!("unable to generate access token: {e}");
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
            }
        }
        Err(generic(last_err))
    }

    async fn invalidate(&self) {
        *self.cache.lock().await = None;
        self.source.invalidate().await;
    }
}

#[derive(serde::Deserialize, Default, Clone, Debug)]
struct ExternalCredentialFormat {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    subject_token_field_name: String,
}

#[derive(serde::Deserialize, Default, Clone, Debug)]
struct ExternalExecutableConfig {
    #[serde(default)]
    command: String,
    #[serde(default)]
    timeout_millis: Option<i64>,
    #[serde(default)]
    output_file: String,
}

#[derive(serde::Deserialize, Default, Clone, Debug)]
struct ExternalCredentialSourceJson {
    #[serde(default)]
    file: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    executable: Option<ExternalExecutableConfig>,
    #[serde(default)]
    environment_id: String,
    #[serde(default)]
    region_url: String,
    #[serde(default)]
    regional_cred_verification_url: String,
    #[serde(default)]
    imdsv2_session_token_url: String,
    #[serde(default)]
    format: ExternalCredentialFormat,
}

#[derive(serde::Deserialize, Default, Clone)]
struct ServiceAccountImpersonationInfoJson {
    #[serde(default)]
    token_lifetime_seconds: Option<u64>,
}

#[derive(serde::Deserialize)]
struct ExternalAccountJson {
    #[serde(default)]
    audience: String,
    #[serde(default)]
    subject_token_type: String,
    #[serde(default)]
    token_url: String,
    #[serde(default)]
    service_account_impersonation_url: String,
    #[serde(default)]
    service_account_impersonation: ServiceAccountImpersonationInfoJson,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    client_secret: String,
    #[serde(default)]
    credential_source: Option<ExternalCredentialSourceJson>,
    #[serde(default)]
    workforce_pool_user_project: String,
    #[serde(default)]
    universe_domain: String,
    #[serde(default)]
    gcs_base_url: Option<String>,
}

#[derive(Debug, Clone)]
enum ParsedExternalSource {
    File {
        path: String,
        format: ExternalCredentialFormat,
    },
    Url {
        url: String,
        headers: HashMap<String, String>,
        format: ExternalCredentialFormat,
    },
    Aws {
        region_url: String,
        regional_cred_verification_url: String,
        cred_verification_url: String,
        imdsv2_session_token_url: String,
        target_resource: String,
    },
    Executable {
        command: String,
        timeout: Duration,
        output_file: String,
        audience: String,
        subject_token_type: String,
        impersonation_url: String,
    },
}

fn extract_subject_token(
    bytes: &[u8],
    format: &ExternalCredentialFormat,
    trim: bool,
) -> Result<String, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| format!("oauth2/google/externalaccount: invalid UTF-8 subject token: {e}"))?;
    let trimmed = if trim { text.trim() } else { text };
    match format.kind.as_str() {
        "json" => {
            let v: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
                format!("oauth2/google/externalaccount: failed to unmarshal subject token file: {e}")
            })?;
            let field = v.get(&format.subject_token_field_name).ok_or_else(|| {
                "oauth2/google/externalaccount: provided subject_token_field_name not found in credentials".to_string()
            })?;
            field.as_str().map(str::to_string).ok_or_else(|| {
                "oauth2/google/externalaccount: improperly formatted subject token".to_string()
            })
        }
        "" | "text" => {
            if trimmed.is_empty() {
                Err("oauth2/google/externalaccount: missing subject token".to_string())
            } else {
                Ok(trimmed.to_string())
            }
        }
        _ => Err(
            "oauth2/google/externalaccount: invalid credential_source file format type".to_string(),
        ),
    }
}

#[derive(serde::Deserialize)]
struct ExecutableResponseJson {
    #[serde(default)]
    version: i64,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    expiration_time: i64,
    #[serde(default)]
    id_token: String,
    #[serde(default)]
    saml_response: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

fn parse_executable_response(bytes: &[u8], source: &str) -> Result<String, (bool, String)> {
    let text = String::from_utf8_lossy(bytes);
    let parsed: ExecutableResponseJson = serde_json::from_slice(bytes).map_err(|_| {
        (
            false,
            format!("oauth2/google/externalaccount: unable to parse {source}\nResponse: {text}"),
        )
    })?;
    if parsed.version == 0 {
        return Err((
            false,
            format!("oauth2/google/externalaccount: {source} missing `\"version\"` field"),
        ));
    }
    let Some(success) = parsed.success else {
        return Err((
            false,
            format!("oauth2/google/externalaccount: {source} missing `\"success\"` field"),
        ));
    };
    if !success {
        if parsed.code.is_empty() || parsed.message.is_empty() {
            return Err((
                true,
                "oauth2/google/externalaccount: response must include `error` and `message` fields when unsuccessful".to_string(),
            ));
        }
        return Err((
            true,
            format!(
                "oauth2/google/externalaccount: response contains unsuccessful response: ({}) {}",
                parsed.code, parsed.message
            ),
        ));
    }
    if parsed.version > 1 || parsed.version < 0 {
        return Err((
            false,
            format!(
                "oauth2/google/externalaccount: {source} contains unsupported version: {}",
                parsed.version
            ),
        ));
    }
    if parsed.expiration_time == 0 && source == "output file" {
        return Err((
            false,
            format!("oauth2/google/externalaccount: {source} missing `\"expiration_time\"` field"),
        ));
    }
    if parsed.token_type.is_empty() {
        return Err((
            false,
            format!("oauth2/google/externalaccount: {source} missing `\"token_type\"` field"),
        ));
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if parsed.expiration_time != 0 && parsed.expiration_time < now_secs {
        return Err((
            true,
            "oauth2/google/externalaccount: the token returned by the executable is expired"
                .to_string(),
        ));
    }
    match parsed.token_type.as_str() {
        "urn:ietf:params:oauth:token-type:jwt" | "urn:ietf:params:oauth:token-type:id_token" => {
            if parsed.id_token.is_empty() {
                Err((
                    false,
                    format!("oauth2/google/externalaccount: {source} missing `\"id_token\"` field"),
                ))
            } else {
                Ok(parsed.id_token)
            }
        }
        "urn:ietf:params:oauth:token-type:saml2" => {
            if parsed.saml_response.is_empty() {
                Err((
                    false,
                    format!(
                        "oauth2/google/externalaccount: {source} missing `\"saml_response\"` field"
                    ),
                ))
            } else {
                Ok(parsed.saml_response)
            }
        }
        _ => Err((
            false,
            format!("oauth2/google/externalaccount: {source} contains unsupported token type"),
        )),
    }
}

#[derive(serde::Deserialize)]
struct AwsSecurityCredentialsJson {
    #[serde(rename = "AccessKeyID", default)]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey", default)]
    secret_access_key: String,
    #[serde(rename = "Token", default)]
    session_token: String,
}

#[derive(serde::Serialize)]
struct AwsSignedHeader {
    key: String,
    value: String,
}

#[derive(serde::Serialize)]
struct AwsSignedRequest {
    url: String,
    method: &'static str,
    headers: Vec<AwsSignedHeader>,
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    hex::encode(digest.as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&k, data).as_ref().to_vec()
}

fn query_escape_form(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Go `oauth2/google/externalaccount` STS token provider.
struct ExternalStsTokenProvider {
    audience: String,
    subject_token_type: String,
    token_url: String,
    scopes: String,
    client_id: String,
    client_secret: String,
    workforce_pool_user_project: String,
    source: ParsedExternalSource,
    client: reqwest::Client,
    cache: tokio::sync::Mutex<Option<(String, Option<Instant>)>>,
}

impl fmt::Debug for ExternalStsTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalStsTokenProvider")
            .field("audience", &self.audience)
            .field("token_url", &self.token_url)
            .finish()
    }
}

impl ExternalStsTokenProvider {
    async fn fetch_subject_token(&self) -> Result<String, String> {
        match &self.source {
            ParsedExternalSource::File { path, format } => {
                let bytes = tokio::fs::read(path).await.map_err(|_| {
                    format!(
                        "oauth2/google/externalaccount: failed to open credential file {path:?}"
                    )
                })?;
                let limited = &bytes[..bytes.len().min(1 << 20)];
                extract_subject_token(limited, format, true)
            }
            ParsedExternalSource::Url {
                url,
                headers,
                format,
            } => {
                let mut req = self.client.get(url);
                for (k, v) in headers {
                    req = req.header(k.as_str(), v.as_str());
                }
                let resp = req.send().await.map_err(|e| {
                    format!(
                        "oauth2/google/externalaccount: invalid response when retrieving subject token: {e}"
                    )
                })?;
                let status = resp.status();
                let body = resp.bytes().await.map_err(|e| {
                    format!(
                        "oauth2/google/externalaccount: invalid body in subject token URL query: {e}"
                    )
                })?;
                if !status.is_success() {
                    return Err(format!(
                        "oauth2/google/externalaccount: status code {}: {}",
                        status.as_u16(),
                        String::from_utf8_lossy(&body)
                    ));
                }
                let limited = &body[..body.len().min(1 << 20)];
                extract_subject_token(limited, format, false)
            }
            ParsedExternalSource::Aws {
                region_url,
                regional_cred_verification_url,
                cred_verification_url,
                imdsv2_session_token_url,
                target_resource,
            } => {
                self.fetch_aws_subject_token(
                    region_url,
                    regional_cred_verification_url,
                    cred_verification_url,
                    imdsv2_session_token_url,
                    target_resource,
                )
                .await
            }
            ParsedExternalSource::Executable {
                command,
                timeout,
                output_file,
                audience,
                subject_token_type,
                impersonation_url,
            } => {
                if !output_file.is_empty() {
                    if let Ok(data) = tokio::fs::read(output_file).await {
                        if !data.is_empty() {
                            match parse_executable_response(&data, "output file") {
                                Ok(tok) => return Ok(tok),
                                Err((true, _)) => {}
                                Err((false, e)) => return Err(e),
                            }
                        }
                    }
                }
                if std::env::var("GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES").as_deref() != Ok("1")
                {
                    return Err(
                        "oauth2/google/externalaccount: executables need to be explicitly allowed (set GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES to '1') to run"
                            .to_string(),
                    );
                }
                let mut parts = command.split_whitespace();
                let Some(prog) = parts.next() else {
                    return Err(
                        "oauth2/google/externalaccount: missing `command` field — executable command must be provided"
                            .to_string(),
                    );
                };
                let mut cmd = tokio::process::Command::new(prog);
                cmd.args(parts);
                cmd.env("GOOGLE_EXTERNAL_ACCOUNT_AUDIENCE", audience);
                cmd.env("GOOGLE_EXTERNAL_ACCOUNT_TOKEN_TYPE", subject_token_type);
                cmd.env("GOOGLE_EXTERNAL_ACCOUNT_INTERACTIVE", "0");
                if !impersonation_url.is_empty() {
                    if let Some(rest) =
                        impersonation_url.split("/v1/projects/-/serviceAccounts/").nth(1)
                    {
                        if let Some(email) = rest.strip_suffix(":generateAccessToken") {
                            cmd.env("GOOGLE_EXTERNAL_ACCOUNT_IMPERSONATED_EMAIL", email);
                        }
                    }
                }
                if !output_file.is_empty() {
                    cmd.env("GOOGLE_EXTERNAL_ACCOUNT_OUTPUT_FILE", output_file);
                }
                let out = tokio::time::timeout(*timeout, cmd.output())
                    .await
                    .map_err(|_| {
                        "oauth2/google/externalaccount: executable command timed out".to_string()
                    })?
                    .map_err(|e| {
                        format!("oauth2/google/externalaccount: executable command failed: {e}")
                    })?;
                if !out.status.success() {
                    let code = out.status.code().unwrap_or(-1);
                    return Err(format!(
                        "oauth2/google/externalaccount: executable command failed with exit code {code}"
                    ));
                }
                let payload = if !out.stdout.trim_ascii().is_empty() {
                    out.stdout.trim_ascii()
                } else {
                    out.stderr.trim_ascii()
                };
                parse_executable_response(payload, "response").map_err(|(_, e)| e)
            }
        }
    }

    async fn fetch_aws_subject_token(
        &self,
        region_url: &str,
        regional_cred_verification_url: &str,
        cred_verification_url: &str,
        imdsv2_session_token_url: &str,
        target_resource: &str,
    ) -> Result<String, String> {
        let env_region = std::env::var("AWS_REGION")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok().filter(|s| !s.is_empty()));
        let env_ak = std::env::var("AWS_ACCESS_KEY_ID").ok().filter(|s| !s.is_empty());
        let env_sk = std::env::var("AWS_SECRET_ACCESS_KEY").ok().filter(|s| !s.is_empty());
        let env_st = std::env::var("AWS_SESSION_TOKEN").ok().unwrap_or_default();

        let use_imds = env_region.is_none() || env_ak.is_none() || env_sk.is_none();
        let imds_token = if use_imds && !imdsv2_session_token_url.is_empty() {
            let resp = self
                .client
                .put(imdsv2_session_token_url)
                .header("X-aws-ec2-metadata-token-ttl-seconds", "300")
                .send()
                .await
                .map_err(|e| e.to_string())?;
            let status = resp.status();
            let body = resp.text().await.map_err(|e| e.to_string())?;
            if status != StatusCode::OK {
                return Err(format!(
                    "oauth2/google/externalaccount: unable to retrieve AWS session token - {body}"
                ));
            }
            body
        } else {
            String::new()
        };

        let region = if let Some(r) = env_region {
            r
        } else {
            if region_url.is_empty() {
                return Err(
                    "oauth2/google/externalaccount: unable to determine AWS region".to_string(),
                );
            }
            let mut req = self.client.get(region_url);
            if !imds_token.is_empty() {
                req = req.header("X-aws-ec2-metadata-token", &imds_token);
            }
            let resp = req.send().await.map_err(|e| e.to_string())?;
            let status = resp.status();
            let body = resp.text().await.map_err(|e| e.to_string())?;
            if status != StatusCode::OK {
                return Err(format!(
                    "oauth2/google/externalaccount: unable to retrieve AWS region - {body}"
                ));
            }
            if body.len() > 1 {
                body[..body.len() - 1].to_string()
            } else {
                String::new()
            }
        };

        let creds = if let (Some(ak), Some(sk)) = (env_ak, env_sk) {
            AwsSecurityCredentialsJson {
                access_key_id: ak,
                secret_access_key: sk,
                session_token: env_st,
            }
        } else {
            if cred_verification_url.is_empty() {
                return Err(
                    "oauth2/google/externalaccount: unable to determine the AWS metadata server security credentials endpoint"
                        .to_string(),
                );
            }
            let mut req = self.client.get(cred_verification_url);
            if !imds_token.is_empty() {
                req = req.header("X-aws-ec2-metadata-token", &imds_token);
            }
            let resp = req.send().await.map_err(|e| e.to_string())?;
            let status = resp.status();
            let role = resp.text().await.map_err(|e| e.to_string())?;
            if status != StatusCode::OK {
                return Err(format!(
                    "oauth2/google/externalaccount: unable to retrieve AWS role name - {role}"
                ));
            }
            let cred_url = format!("{cred_verification_url}/{role}");
            let mut req2 = self.client.get(&cred_url);
            if !imds_token.is_empty() {
                req2 = req2.header("X-aws-ec2-metadata-token", &imds_token);
            }
            let resp2 = req2.send().await.map_err(|e| e.to_string())?;
            let status2 = resp2.status();
            let body2 = resp2.text().await.map_err(|e| e.to_string())?;
            if status2 != StatusCode::OK {
                return Err(format!(
                    "oauth2/google/externalaccount: unable to retrieve AWS security credentials - {body2}"
                ));
            }
            let parsed: AwsSecurityCredentialsJson =
                serde_json::from_str(&body2).map_err(|e| e.to_string())?;
            if parsed.access_key_id.is_empty() {
                return Err(
                    "oauth2/google/externalaccount: missing AccessKeyId credential".to_string(),
                );
            }
            if parsed.secret_access_key.is_empty() {
                return Err(
                    "oauth2/google/externalaccount: missing SecretAccessKey credential".to_string(),
                );
            }
            parsed
        };

        let tpl = if regional_cred_verification_url.is_empty() {
            "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
        } else {
            regional_cred_verification_url
        };
        let target_url = tpl.replace("{region}", &region);
        let parsed_url = reqwest::Url::parse(&target_url).map_err(|e| e.to_string())?;
        let host = match (parsed_url.host_str(), parsed_url.port()) {
            (Some(h), Some(p)) => format!("{h}:{p}"),
            (Some(h), None) => h.to_string(),
            _ => String::new(),
        };
        let service_name = host.split('.').next().unwrap_or("").to_string();
        let canonical_uri = if parsed_url.path().is_empty() {
            "/"
        } else {
            parsed_url.path()
        };
        let mut qp: Vec<(String, String)> = parsed_url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        qp.sort();
        let canonical_query = qp
            .iter()
            .map(|(k, v)| format!("{}={}", query_escape_form(k), query_escape_form(v)))
            .collect::<Vec<_>>()
            .join("&");

        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let mut hdr_map: std::collections::BTreeMap<String, (String, String)> =
            std::collections::BTreeMap::new();
        hdr_map.insert("host".to_string(), ("host".to_string(), host));
        hdr_map.insert(
            "x-amz-date".to_string(),
            ("x-amz-date".to_string(), amz_date.clone()),
        );
        if !creds.session_token.is_empty() {
            hdr_map.insert(
                "x-amz-security-token".to_string(),
                ("x-amz-security-token".to_string(), creds.session_token),
            );
        }
        if !target_resource.is_empty() {
            hdr_map.insert(
                "x-goog-cloud-target-resource".to_string(),
                (
                    "x-goog-cloud-target-resource".to_string(),
                    target_resource.to_string(),
                ),
            );
        }

        let signed_headers = hdr_map.keys().cloned().collect::<Vec<_>>().join(";");
        let mut canonical_headers = String::new();
        for (k, (_, v)) in &hdr_map {
            canonical_headers.push_str(k);
            canonical_headers.push(':');
            canonical_headers.push_str(v);
            canonical_headers.push('\n');
        }
        let payload_hash = sha256_hex(b"");
        let canonical_request = format!(
            "POST\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let credential_scope = format!("{date_stamp}/{region}/{service_name}/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        let k_date = hmac_sha256(
            format!("AWS4{}", creds.secret_access_key).as_bytes(),
            date_stamp.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service_name.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            creds.access_key_id
        );

        let mut out_headers: Vec<AwsSignedHeader> = hdr_map
            .into_values()
            .map(|(key, value)| AwsSignedHeader { key, value })
            .collect();
        out_headers.push(AwsSignedHeader {
            key: "Authorization".to_string(),
            value: auth_header,
        });
        out_headers.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

        let signed_req = AwsSignedRequest {
            url: target_url,
            method: "POST",
            headers: out_headers,
        };
        let json = serde_json::to_string(&signed_req).map_err(|e| e.to_string())?;
        Ok(query_escape_form(&json))
    }
}

#[async_trait]
impl GcpAuth for ExternalStsTokenProvider {
    async fn get_token(&self) -> object_store::Result<String> {
        let mut cache = self.cache.lock().await;
        if let Some((tok, exp)) = cache.as_ref() {
            if exp.is_none_or(|e| Instant::now() + EXPIRY_DELTA < e) {
                return Ok(tok.clone());
            }
        }
        let generic = |e: String| object_store::Error::Generic {
            store: STORE,
            source: e.into(),
        };
        let subject_token = self.fetch_subject_token().await.map_err(generic)?;
        let mut params: Vec<(&str, String)> = vec![
            ("audience", self.audience.clone()),
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token".to_string(),
            ),
            ("subject_token_type", self.subject_token_type.clone()),
            ("subject_token", subject_token),
            ("scope", self.scopes.clone()),
        ];
        let options_json =
            if !self.workforce_pool_user_project.is_empty() && self.client_id.is_empty() {
                Some(
                    serde_json::json!({ "userProject": self.workforce_pool_user_project })
                        .to_string(),
                )
            } else {
                None
            };
        if let Some(opts) = options_json {
            params.push(("options", opts));
        }

        let mut last_err = String::new();
        for attempt in 0..=3usize {
            let started = Instant::now();
            let mut rb = self.client.post(&self.token_url).form(&params);
            if !self.client_id.is_empty() && !self.client_secret.is_empty() {
                let creds = STANDARD.encode(format!("{}:{}", self.client_id, self.client_secret));
                rb = rb.header(reqwest::header::AUTHORIZATION, format!("Basic {creds}"));
            }
            match rb.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp.text().await.map_err(|e| generic(e.to_string()))?;
                    if status.is_success() {
                        let tok: TokenResponse = serde_json::from_str(&body).map_err(|e| {
                            generic(format!(
                                "oauth2/google: failed to unmarshal response body from Secure Token Server: {e}"
                            ))
                        })?;
                        let secs = tok.expires_in.unwrap_or(0);
                        if secs == 0 || tok.access_token.is_empty() {
                            return Err(generic(
                                "oauth2/google/externalaccount: got invalid expiry from security token service"
                                    .to_string(),
                            ));
                        }
                        let expiry = Some(started + Duration::from_secs(secs));
                        *cache = Some((tok.access_token.clone(), expiry));
                        return Ok(tok.access_token);
                    }
                    last_err = format!("oauth2/google: status code {}: {body}", status.as_u16());
                    if attempt < 3 && is_retryable_status(status) {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                        tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
                Err(e) => {
                    last_err = format!(
                        "oauth2/google: invalid response from Secure Token Server: {e}"
                    );
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                        continue;
                    }
                    return Err(generic(last_err));
                }
            }
        }
        Err(generic(last_err))
    }

    async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }
}

fn external_account_auth_from_json(json: &str) -> Result<(Arc<dyn GcpAuth>, Option<String>), String> {
    let j: ExternalAccountJson = serde_json::from_str(json).map_err(|e| e.to_string())?;
    if j.audience.is_empty() {
        return Err("oauth2/google/externalaccount: Audience must be set".to_string());
    }
    if j.subject_token_type.is_empty() {
        return Err("oauth2/google/externalaccount: Subject token type must be set".to_string());
    }
    if !j.workforce_pool_user_project.is_empty() {
        let re = regex::Regex::new(r"//iam\.googleapis\.com/locations/[^/]+/workforcePools/")
            .map_err(|e| e.to_string())?;
        if !re.is_match(&j.audience) {
            return Err(
                "oauth2/google/externalaccount: Workforce pool user project should not be set for non-workforce pool credentials"
                    .to_string(),
            );
        }
    }
    let cs = j.credential_source.ok_or_else(|| {
        "oauth2/google/externalaccount: One of CredentialSource, SubjectTokenSupplier, or AwsSecurityCredentialsSupplier must be set"
            .to_string()
    })?;
    let parsed_source = if let Some(ver) = cs.environment_id.strip_prefix("aws") {
        if ver != "1" {
            return Err(format!(
                "oauth2/google/externalaccount: aws version '{ver}' is not supported in the current build"
            ));
        }
        ParsedExternalSource::Aws {
            region_url: cs.region_url,
            regional_cred_verification_url: cs.regional_cred_verification_url,
            cred_verification_url: cs.url,
            imdsv2_session_token_url: cs.imdsv2_session_token_url,
            target_resource: j.audience.clone(),
        }
    } else if !cs.file.is_empty() {
        ParsedExternalSource::File {
            path: cs.file,
            format: cs.format,
        }
    } else if !cs.url.is_empty() {
        ParsedExternalSource::Url {
            url: cs.url,
            headers: cs.headers,
            format: cs.format,
        }
    } else if let Some(exec) = cs.executable {
        if exec.command.is_empty() {
            return Err(
                "oauth2/google/externalaccount: missing `command` field — executable command must be provided"
                    .to_string(),
            );
        }
        let timeout = match exec.timeout_millis {
            None => Duration::from_secs(30),
            Some(ms) if !(5_000..=120_000).contains(&ms) => {
                return Err(
                    "oauth2/google/externalaccount: invalid `timeout_millis` field — executable timeout must be between 5 and 120 seconds"
                        .to_string(),
                );
            }
            Some(ms) => Duration::from_millis(ms as u64),
        };
        ParsedExternalSource::Executable {
            command: exec.command,
            timeout,
            output_file: exec.output_file,
            audience: j.audience.clone(),
            subject_token_type: j.subject_token_type.clone(),
            impersonation_url: j.service_account_impersonation_url.clone(),
        }
    } else {
        return Err("oauth2/google/externalaccount: unable to parse credential source".to_string());
    };

    let token_url = if !j.token_url.is_empty() {
        j.token_url
    } else if !j.universe_domain.is_empty() {
        format!("https://sts.{}/v1/token", j.universe_domain)
    } else {
        DEFAULT_STS_TOKEN_URL.to_string()
    };

    let has_impersonation = !j.service_account_impersonation_url.is_empty();
    let sts = Arc::new(ExternalStsTokenProvider {
        audience: j.audience,
        subject_token_type: j.subject_token_type,
        token_url,
        scopes: if has_impersonation {
            CLOUD_PLATFORM_SCOPE.to_string()
        } else {
            DEFAULT_SCOPES_SPACE.to_string()
        },
        client_id: j.client_id,
        client_secret: j.client_secret,
        workforce_pool_user_project: j.workforce_pool_user_project,
        source: parsed_source,
        client: exthttp::bounded_auth_client(),
        cache: tokio::sync::Mutex::new(None),
    });

    let override_url = j.gcs_base_url.filter(|s| !s.is_empty());
    if has_impersonation {
        let imp = ImpersonatedServiceAccountProvider {
            impersonation_url: j.service_account_impersonation_url,
            delegates: Vec::new(),
            scopes: vec![
                STORAGE_FULL_CONTROL_SCOPE.to_string(),
                CLOUD_PLATFORM_SCOPE.to_string(),
            ],
            lifetime_seconds: j.service_account_impersonation.token_lifetime_seconds,
            source: sts,
            client: exthttp::bounded_auth_client(),
            cache: tokio::sync::Mutex::new(None),
        };
        Ok((Arc::new(imp), override_url))
    } else {
        Ok((sts, override_url))
    }
}

fn auth_from_json(json: &str) -> Result<(Arc<dyn GcpAuth>, Option<String>), String> {
    match credential_kind(json)? {
        CredentialKind::ServiceAccount => auth_from_service_account_json(json),
        CredentialKind::AuthorizedUser => {
            let j: AuthorizedUserJson = serde_json::from_str(json).map_err(|e| e.to_string())?;
            let override_url = j.gcs_base_url.filter(|s| !s.is_empty());
            Ok((
                Arc::new(AuthorizedUserProvider::from_json(json)?),
                override_url,
            ))
        }
        CredentialKind::ExternalAccount => external_account_auth_from_json(json),
        CredentialKind::ImpersonatedServiceAccount => {
            let (imp, override_url) = ImpersonatedServiceAccountProvider::from_json(json)?;
            Ok((Arc::new(imp), override_url))
        }
    }
}

fn parse_storage_emulator_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    let candidate = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };
    let parsed = reqwest::Url::parse(&candidate)
        .map_err(|e| format!("invalid STORAGE_EMULATOR_HOST {trimmed:?}: {e}"))?;
    if parsed.host_str().is_none() {
        return Err(format!("invalid STORAGE_EMULATOR_HOST {trimmed:?}: missing host"));
    }
    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

/// Builds the GCS object store for `cfg` (Go `gcs.NewBucketClient`).
pub fn new_store(cfg: &GCSConfig) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let env = |k: &str| std::env::var(k).ok();
    new_store_with_env(cfg, &env)
}

fn new_store_with_env(
    cfg: &GCSConfig,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let wrap = |e: String| BucketError::Config(format!("NewBucketClient: create bucket: {e}"));
    if cfg.bucket_name.is_empty() {
        return Err(wrap(
            "missing Google Cloud Storage bucket name for stored blocks".to_string(),
        ));
    }
    let emulator_url = match env("STORAGE_EMULATOR_HOST").filter(|s| !s.trim().is_empty()) {
        Some(raw) => Some(parse_storage_emulator_url(&raw).map_err(wrap)?),
        None => None,
    };

    let read = |p: &str| {
        if std::path::Path::new(p).exists() {
            Some(std::fs::read_to_string(p))
        } else {
            None
        }
    };

    let total_timeout = if cfg.http.idle_conn_timeout.0 > 0 {
        cfg.http.idle_conn_timeout.as_std()
    } else {
        None
    };
    let client = exthttp::reqwest_client(&cfg.http, total_timeout, true)?;

    let mut base_url = emulator_url
        .clone()
        .unwrap_or_else(|| DEFAULT_GCS_BASE_URL.to_string());
    let auth: Arc<dyn GcpAuth> = if emulator_url.is_some() && cfg.service_account.is_empty() {
        Arc::new(NoOauthAuth)
    } else {
        let source = credential_source(cfg, env, &read).map_err(wrap)?;
        match source {
            CredentialSource::ServiceAccountKey(json)
            | CredentialSource::AuthorizedUserJson(json)
            | CredentialSource::ExternalAccountJson(json)
            | CredentialSource::ImpersonatedServiceAccountJson(json) => {
                let (auth, override_url) = auth_from_json(&json).map_err(wrap)?;
                if emulator_url.is_none() {
                    if let Some(u) = override_url {
                        base_url = u;
                    }
                }
                auth
            }
            CredentialSource::ApplicationCredentialsFile(path) => {
                let content = std::fs::read_to_string(&path).map_err(|e| wrap(e.to_string()))?;
                let (auth, override_url) = auth_from_json(&content).map_err(wrap)?;
                if emulator_url.is_none() {
                    if let Some(u) = override_url {
                        base_url = u;
                    }
                }
                auth
            }
            CredentialSource::Metadata => Arc::new(GceMetadataProvider::new()),
        }
    };

    let bucket_name_encoded = exthttp::percent_encode_non_alnum(&cfg.bucket_name);
    let inner = Arc::new(GcsClient {
        base_url: base_url.trim_end_matches('/').to_string(),
        bucket_name: cfg.bucket_name.clone(),
        bucket_name_encoded,
        client,
        auth,
    });

    let idempotent: Arc<dyn ObjectStore> = Arc::new(GcsStore {
        client: Arc::clone(&inner),
        retry: retry_config(cfg.max_retries, true),
        retry_always: false,
    });
    let non_idempotent: Arc<dyn ObjectStore> = Arc::new(GcsStore {
        client: Arc::clone(&inner),
        retry: retry_config(cfg.max_retries, false),
        retry_always: false,
    });
    let upload: Arc<dyn ObjectStore> = if cfg.enable_upload_retries {
        Arc::new(GcsStore {
            client: Arc::clone(&inner),
            retry: retry_config(cfg.max_retries, true),
            retry_always: true,
        })
    } else {
        Arc::clone(&non_idempotent)
    };

    Ok(Arc::new(GcsRetryPolicyStore {
        idempotent,
        non_idempotent,
        upload,
    }))
}

fn auth_from_service_account_json(
    json: &str,
) -> Result<(Arc<dyn GcpAuth>, Option<String>), String> {
    let j: ServiceAccountJson = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let override_url = j.gcs_base_url.clone().filter(|s| !s.is_empty());
    let auth: Arc<dyn GcpAuth> = if j.disable_oauth {
        Arc::new(NoOauthAuth)
    } else {
        Arc::new(ServiceAccountJwtProvider::from_sa_json(&j)?)
    };
    Ok((auth, override_url))
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn next_backoff_delay(cfg: &BackoffConfig, attempt: usize) -> Duration {
    let exp = cfg.base.powi(attempt.min(30) as i32);
    let base_secs = (cfg.init_backoff.as_secs_f64() * exp).min(cfg.max_backoff.as_secs_f64());
    let jitter = rand::thread_rng().gen_range(0.5..=1.0);
    Duration::from_secs_f64(base_secs * jitter)
}

#[derive(Debug)]
struct GcsClient {
    base_url: String,
    bucket_name: String,
    bucket_name_encoded: String,
    client: reqwest::Client,
    auth: Arc<dyn GcpAuth>,
}

impl GcsClient {
    fn object_url(&self, path: &Path) -> String {
        let encoded = exthttp::percent_encode_non_alnum(path.as_ref());
        format!(
            "{}/{}/{}",
            self.base_url, self.bucket_name_encoded, encoded
        )
    }

    fn bucket_url(&self) -> String {
        format!("{}/{}", self.base_url, self.bucket_name_encoded)
    }

    /// Executes an HTTP request with per-attempt token acquisition, 401 token cache
    /// invalidation + 1x retry, and 408/429/5xx retries honoring `Retry-After`.
    async fn send_with_retry(
        &self,
        retry: &RetryConfig,
        path_label: &str,
        idempotent: bool,
        mut build_req: impl FnMut(&reqwest::Client) -> object_store::Result<reqwest::RequestBuilder>,
    ) -> object_store::Result<reqwest::Response> {
        let mut retries = 0usize;
        let mut auth_refreshed = false;

        loop {
            let token = self.auth.get_token().await?;
            let mut rb = build_req(&self.client)?;
            if !token.is_empty() {
                rb = rb.bearer_auth(&token);
            }
            let req = rb.build().map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;

            match self.client.execute(req).await {
                Ok(resp) => {
                    let status = resp.status();
                    if status == StatusCode::UNAUTHORIZED && !auth_refreshed {
                        self.auth.invalidate().await;
                        auth_refreshed = true;
                        continue;
                    }
                    if status.is_success() {
                        return Ok(resp);
                    }

                    let headers = resp.headers().clone();
                    let body = resp.text().await.unwrap_or_default();
                    if idempotent && is_retryable_status(status) && retries < retry.max_retries {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| next_backoff_delay(&retry.backoff, retries))
                            .min(retry.backoff.max_backoff);
                        retries += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    if status == StatusCode::NOT_FOUND
                        && (path_label.is_empty()
                            || xml_text_tag(body.as_bytes(), "Code").as_deref()
                                == Some("NoSuchBucket")
                            || body.contains("specified bucket does not exist"))
                    {
                        let source: Box<dyn std::error::Error + Send + Sync> = if body.is_empty() {
                            format!("GCS bucket not found with status {status}").into()
                        } else {
                            format!("HTTP error with status {status}: {body}").into()
                        };
                        return Err(object_store::Error::Generic {
                            store: STORE,
                            source,
                        });
                    }
                    return Err(exthttp::map_http_error(STORE, path_label, status, body));
                }
                Err(e) => {
                    if (idempotent || e.is_connect()) && retries < retry.max_retries {
                        let delay = next_backoff_delay(&retry.backoff, retries);
                        retries += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(e),
                    });
                }
            }
        }
    }

    async fn send_bytes_checking_error_body(
        &self,
        retry: &RetryConfig,
        path_label: &str,
        mut build_req: impl FnMut(&reqwest::Client) -> object_store::Result<reqwest::RequestBuilder>,
    ) -> object_store::Result<(reqwest::header::HeaderMap, Bytes)> {
        let mut retries = 0usize;
        loop {
            let resp = self
                .send_with_retry(retry, path_label, true, &mut build_req)
                .await?;
            let headers = resp.headers().clone();
            let bytes = resp.bytes().await.map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;
            let text = String::from_utf8_lossy(&bytes);
            let is_err_body = text.contains("InternalError")
                || text.contains("SlowDown")
                || text.contains("<Error>")
                || bytes.iter().all(|b| b.is_ascii_whitespace());
            if !is_err_body {
                return Ok((headers, bytes));
            }
            if retries < retry.max_retries {
                let delay = exthttp::parse_retry_after(&headers)
                    .unwrap_or_else(|| next_backoff_delay(&retry.backoff, retries))
                    .min(retry.backoff.max_backoff);
                retries += 1;
                tokio::time::sleep(delay).await;
                continue;
            }
            return Err(exthttp::map_http_error(
                STORE,
                path_label,
                StatusCode::INTERNAL_SERVER_ERROR,
                text.into_owned(),
            ));
        }
    }
}

#[derive(Debug)]
struct GcsStore {
    client: Arc<GcsClient>,
    retry: RetryConfig,
    retry_always: bool,
}

impl fmt::Display for GcsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GoogleCloudStorage({})", self.client.bucket_name)
    }
}

fn apply_gcs_attributes(
    mut rb: reqwest::RequestBuilder,
    attrs: &object_store::Attributes,
) -> object_store::Result<reqwest::RequestBuilder> {
    let mut has_ct = false;
    for (k, v) in attrs {
        rb = match k {
            Attribute::CacheControl => rb.header(CACHE_CONTROL, v.as_ref()),
            Attribute::ContentDisposition => rb.header(CONTENT_DISPOSITION, v.as_ref()),
            Attribute::ContentEncoding => rb.header(CONTENT_ENCODING, v.as_ref()),
            Attribute::ContentLanguage => rb.header(CONTENT_LANGUAGE, v.as_ref()),
            Attribute::ContentType => {
                has_ct = true;
                rb.header(CONTENT_TYPE, v.as_ref())
            }
            Attribute::Metadata(suffix) => {
                let name = HeaderName::from_bytes(format!("{META_PREFIX}{suffix}").as_bytes())
                    .map_err(|e| object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(e),
                    })?;
                let val = HeaderValue::from_str(v.as_ref()).map_err(|e| {
                    object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(e),
                    }
                })?;
                rb.header(name, val)
            }
            _ => rb,
        };
    }
    if !has_ct {
        rb = rb.header(CONTENT_TYPE, "application/octet-stream");
    }
    Ok(rb)
}

fn xml_text_tag(xml: &[u8], target: &str) -> Option<String> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(xml);
    let mut buf = Vec::new();
    let mut in_tag = false;
    let mut out = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) if e.local_name().as_ref() == target.as_bytes() => {
                in_tag = true;
                out.clear();
            }
            Ok(Event::End(e)) if e.local_name().as_ref() == target.as_bytes() => {
                return Some(out);
            }
            Ok(Event::Text(t)) if in_tag => {
                if let Ok(s) = t.unescape() {
                    out.push_str(&s);
                }
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
}

fn parse_gcs_list_xml(xml: &[u8]) -> object_store::Result<(ListResult, Option<String>)> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(xml);
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut text = String::new();

    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut next_token: Option<String> = None;

    let mut cur_key = String::new();
    let mut cur_size = 0usize;
    let mut cur_lm = String::new();
    let mut cur_etag: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "Contents" {
                    cur_key.clear();
                    cur_size = 0;
                    cur_lm.clear();
                    cur_etag = None;
                }
                path.push(name);
                text.clear();
            }
            Ok(Event::Text(t)) => {
                if let Ok(s) = t.unescape() {
                    text.push_str(&s);
                }
            }
            Ok(Event::End(_)) => {
                let tags: Vec<&str> = path.iter().map(String::as_str).collect();
                match tags.as_slice() {
                    [_, "NextContinuationToken"] => {
                        if !text.is_empty() {
                            next_token = Some(text.clone());
                        }
                    }
                    [_, "CommonPrefixes", "Prefix"] => {
                        common_prefixes.push(Path::parse(&text)?);
                    }
                    [_, "Contents", "Key"] => cur_key = text.clone(),
                    [_, "Contents", "Size"] => {
                        cur_size = text.trim().parse().unwrap_or(0);
                    }
                    [_, "Contents", "LastModified"] => cur_lm = text.clone(),
                    [_, "Contents", "ETag"] => cur_etag = Some(text.clone()),
                    [_, "Contents"] => {
                        let last_modified = chrono::DateTime::parse_from_rfc3339(cur_lm.trim())
                            .map(|d| d.with_timezone(&chrono::Utc))
                            .map_err(|e| object_store::Error::Generic {
                                store: STORE,
                                source: format!("invalid GCS list LastModified '{}': {e}", cur_lm)
                                    .into(),
                            })?;
                        objects.push(ObjectMeta {
                            location: Path::parse(&cur_key)?,
                            last_modified,
                            size: cur_size,
                            e_tag: cur_etag.take(),
                            version: None,
                        });
                    }
                    _ => {}
                }
                path.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => {
                return Err(object_store::Error::Generic {
                    store: STORE,
                    source: Box::new(e),
                })
            }
        }
        buf.clear();
    }
    Ok((
        ListResult {
            common_prefixes,
            objects,
        },
        next_token,
    ))
}

#[derive(Debug)]
struct GcsUploadState {
    client: Arc<GcsClient>,
    retry: RetryConfig,
    path: Path,
    upload_id: MultipartId,
    parts: parking_lot::Mutex<Vec<(usize, String)>>,
    finished: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct GcsMultipartUpload {
    state: Arc<GcsUploadState>,
    part_idx: usize,
}

impl Drop for GcsMultipartUpload {
    fn drop(&mut self) {
        if !self.state.finished.load(std::sync::atomic::Ordering::SeqCst) {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let state = Arc::clone(&self.state);
                handle.spawn(async move {
                    let url = state.client.object_url(&state.path);
                    let _ = state
                        .client
                        .send_with_retry(&state.retry, state.path.as_ref(), true, |c| {
                            Ok(c.delete(&url)
                                .header(CONTENT_TYPE, "application/octet-stream")
                                .header(CONTENT_LENGTH, "0")
                                .query(&[("uploadId", state.upload_id.as_str())]))
                        })
                        .await;
                });
            }
        }
    }
}

#[async_trait]
impl MultipartUpload for GcsMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let url = state.client.object_url(&state.path);
            let len = payload.content_length();
            let part_num = (idx + 1).to_string();
            let resp = state
                .client
                .send_with_retry(&state.retry, state.path.as_ref(), true, |c| {
                    Ok(c.put(&url)
                        .query(&[
                            ("partNumber", part_num.as_str()),
                            ("uploadId", state.upload_id.as_str()),
                        ])
                        .header(CONTENT_LENGTH, len)
                        .body(exthttp::payload_to_body(&payload)))
                })
                .await?;
            let etag = resp
                .headers()
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| object_store::Error::Generic {
                    store: STORE,
                    source: "missing ETag header in GCS put_part response".into(),
                })?
                .to_string();
            state.parts.lock().push((idx, etag));
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut parts = std::mem::take(&mut *self.state.parts.lock());
        parts.sort_by_key(|(idx, _)| *idx);
        if parts.is_empty() {
            let url = self.state.client.object_url(&self.state.path);
            let resp = self
                .state
                .client
                .send_with_retry(
                    &self.state.retry,
                    self.state.path.as_ref(),
                    true,
                    |c| Ok(c.put(&url).header(CONTENT_LENGTH, 0)),
                )
                .await?;
            let e_tag = resp
                .headers()
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let version = resp
                .headers()
                .get(VERSION_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            self.abort().await?;
            return Ok(PutResult { e_tag, version });
        }

        let mut xml = String::from("<CompleteMultipartUpload>");
        for (idx, etag) in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{etag}</ETag></Part>",
                idx + 1
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let url = self.state.client.object_url(&self.state.path);
        let (headers, bytes) = self
            .state
            .client
            .send_bytes_checking_error_body(
                &self.state.retry,
                self.state.path.as_ref(),
                |c| {
                    Ok(c.post(&url)
                        .query(&[("uploadId", self.state.upload_id.as_str())])
                        .header(CONTENT_LENGTH, xml.len())
                        .body(xml.clone()))
                },
            )
            .await?;
        self.state
            .finished
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let version = headers
            .get(VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let e_tag = xml_text_tag(&bytes, "ETag");
        Ok(PutResult { e_tag, version })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.state
            .finished
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let url = self.state.client.object_url(&self.state.path);
        self.state
            .client
            .send_with_retry(
                &self.state.retry,
                self.state.path.as_ref(),
                true,
                |c| {
                    Ok(c.delete(&url)
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .header(CONTENT_LENGTH, "0")
                        .query(&[("uploadId", self.state.upload_id.as_str())]))
                },
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for GcsStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let url = self.client.object_url(location);
        let len = payload.content_length();
        let if_gen_not_match: Option<i64> = opts
            .tags
            .encoded()
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == IF_GEN_NOT_MATCH_TAG)
            .and_then(|(_, v)| v.parse::<i64>().ok());
        let idempotent = self.retry_always
            || (matches!(opts.mode, PutMode::Overwrite) && if_gen_not_match.is_none());
        let res = self
            .client
            .send_with_retry(&self.retry, location.as_ref(), idempotent, |c| {
                let mut rb = c.put(&url).header(CONTENT_LENGTH, len);
                rb = apply_gcs_attributes(rb, &opts.attributes)?;
                rb = match &opts.mode {
                    PutMode::Overwrite => {
                        if let Some(gen_val) = if_gen_not_match {
                            rb.header("x-goog-if-generation-not-match", gen_val.to_string())
                        } else {
                            rb
                        }
                    }
                    PutMode::Create => rb.header(VERSION_MATCH, "0"),
                    PutMode::Update(v) => {
                        let ver = v.version.as_ref().ok_or_else(|| object_store::Error::Generic {
                            store: STORE,
                            source: "Version required for conditional update".into(),
                        })?;
                        rb.header(VERSION_MATCH, ver)
                    }
                };
                Ok(rb.body(exthttp::payload_to_body(&payload)))
            })
            .await;

        let resp = match (&opts.mode, res) {
            (PutMode::Create, Err(object_store::Error::Precondition { path, source })) => {
                return Err(object_store::Error::AlreadyExists { path, source });
            }
            (_, r) => r?,
        };
        let e_tag = resp
            .headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let version = resp
            .headers()
            .get(VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        Ok(PutResult { e_tag, version })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let url = self.client.object_url(location);
        let idempotent = self.retry_always;
        let resp = self
            .client
            .send_with_retry(&self.retry, location.as_ref(), idempotent, |c| {
                let rb = c
                    .post(&url)
                    .header(CONTENT_LENGTH, "0")
                    .query(&[("uploads", "")]);
                apply_gcs_attributes(rb, &opts.attributes)
            })
            .await?;
        let bytes = resp.bytes().await.map_err(|e| object_store::Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;
        let upload_id =
            xml_text_tag(&bytes, "UploadId").ok_or_else(|| object_store::Error::Generic {
                store: STORE,
                source: "missing UploadId in GCS InitiateMultipartUploadResult".into(),
            })?;
        Ok(Box::new(GcsMultipartUpload {
            part_idx: 0,
            state: Arc::new(GcsUploadState {
                client: Arc::clone(&self.client),
                retry: self.retry.clone(),
                path: location.clone(),
                upload_id,
                parts: parking_lot::Mutex::new(Vec::new()),
                finished: std::sync::atomic::AtomicBool::new(false),
            }),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if let Some(r) = options.range.as_ref() {
            exthttp::validate_get_range(STORE, r)?;
        }
        let range_opt = options.range.clone();
        let url = self.client.object_url(location);
        let method = if options.head { Method::HEAD } else { Method::GET };
        let resp = self
            .client
            .send_with_retry(&self.retry, location.as_ref(), true, |c| {
                let mut rb = c.request(method.clone(), &url);
                if let Some(ref v) = options.version {
                    rb = rb.query(&[("generation", v.as_str())]);
                }
                Ok(exthttp::apply_get_options(rb, &options))
            })
            .await?;

        let mut meta =
            exthttp::extract_object_meta(STORE, location, resp.headers(), Some(VERSION_HEADER))?;
        let range = if let Some(expected) = range_opt {
            if resp.status() != StatusCode::PARTIAL_CONTENT {
                return Err(object_store::Error::Generic {
                    store: STORE,
                    source: "Received non-partial response when range requested".into(),
                });
            }
            let cr = resp
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(exthttp::parse_content_range)
                .ok_or_else(|| object_store::Error::Generic {
                    store: STORE,
                    source: "Invalid or missing Content-Range header".into(),
                })?;
            meta.size = cr.1;
            let expected_range = exthttp::resolve_get_range(STORE, &expected, meta.size)?;
            if cr.0 != expected_range {
                return Err(object_store::Error::Generic {
                    store: STORE,
                    source: format!("Requested {expected_range:?}, got {:?}", cr.0).into(),
                });
            }
            cr.0
        } else {
            0..meta.size
        };
        let attributes = exthttp::extract_attributes(resp.headers(), Some(META_PREFIX));
        let stream = resp
            .bytes_stream()
            .map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })
            .boxed();
        Ok(GetResult {
            range,
            meta,
            attributes,
            payload: GetResultPayload::Stream(stream),
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let url = self.client.object_url(location);
        self.client
            .send_with_retry(&self.retry, location.as_ref(), false, |c| {
                Ok(c.delete(&url))
            })
            .await?;
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.list_impl(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.list_impl(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix_str = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{DELIMITER}", p.as_ref()));
        let mut common_prefixes = std::collections::BTreeSet::new();
        let mut objects = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let (res, next) = self
                .list_page(prefix_str.as_deref(), true, token.as_deref(), None)
                .await?;
            common_prefixes.extend(res.common_prefixes);
            objects.extend(res.objects);
            match next {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => break,
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_impl(from, to, false).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_impl(from, to, true).await
    }
}

impl GcsStore {
    async fn list_page(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        page_token: Option<&str>,
        offset: Option<&str>,
    ) -> object_store::Result<(ListResult, Option<String>)> {
        let url = self.client.bucket_url();
        let mut query: Vec<(&str, &str)> = vec![("list-type", "2")];
        if delimiter {
            query.push(("delimiter", DELIMITER));
        }
        if let Some(p) = prefix {
            query.push(("prefix", p));
        }
        if let Some(t) = page_token {
            query.push(("continuation-token", t));
        }
        if let Some(o) = offset {
            query.push(("start-after", o));
        }
        let resp = self
            .client
            .send_with_retry(&self.retry, "", true, |c| {
                Ok(c.get(&url).query(&query))
            })
            .await?;
        let bytes = resp.bytes().await.map_err(|e| object_store::Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;
        parse_gcs_list_xml(&bytes)
    }

    fn list_impl(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{DELIMITER}", p.as_ref()));
        let offset = offset.map(|x| x.to_string());
        let client = Arc::clone(&self.client);
        let retry = self.retry.clone();

        stream::try_unfold(
            (Some(None::<String>), prefix, offset, client, retry),
            move |(maybe_token, prefix, offset, client, retry)| async move {
                let Some(token) = maybe_token else {
                    return Ok::<_, object_store::Error>(None);
                };
                let tmp = GcsStore {
                    client: Arc::clone(&client),
                    retry: retry.clone(),
                    retry_always: false,
                };
                let (res, next) = tmp
                    .list_page(
                        prefix.as_deref(),
                        false,
                        token.as_deref(),
                        offset.as_deref(),
                    )
                    .await?;
                let next_state = next.filter(|t| !t.is_empty()).map(Some);
                Ok(Some((
                    stream::iter(res.objects.into_iter().map(Ok)),
                    (next_state, prefix, offset, client, retry),
                )))
            },
        )
        .try_flatten()
        .boxed()
    }

    async fn copy_impl(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> object_store::Result<()> {
        let url = self.client.object_url(to);
        let source = format!(
            "{}/{}",
            self.client.bucket_name_encoded,
            exthttp::percent_encode_non_alnum(from.as_ref())
        );
        let res = self
            .client
            .send_with_retry(&self.retry, to.as_ref(), false, |c| {
                let mut rb = c
                    .put(&url)
                    .header("x-goog-copy-source", &source)
                    .header(CONTENT_LENGTH, 0);
                if if_not_exists {
                    rb = rb.header(VERSION_MATCH, 0);
                }
                Ok(rb)
            })
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(object_store::Error::Precondition { path, source }) if if_not_exists => {
                Err(object_store::Error::AlreadyExists { path, source })
            }
            Err(e) => Err(e),
        }
    }
}

/// Routes each operation to the client whose retry policy matches Go's for that operation.
#[derive(Debug)]
pub struct GcsRetryPolicyStore {
    idempotent: Arc<dyn ObjectStore>,
    non_idempotent: Arc<dyn ObjectStore>,
    upload: Arc<dyn ObjectStore>,
}

impl fmt::Display for GcsRetryPolicyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.idempotent)
    }
}

#[async_trait]
impl ObjectStore for GcsRetryPolicyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.upload.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.upload.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.idempotent.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        self.idempotent.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.idempotent.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.idempotent.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.non_idempotent.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.idempotent.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.idempotent.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.idempotent.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.non_idempotent.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.non_idempotent.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }
    fn files_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<std::io::Result<String>> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned().map(Ok)
    }

    #[test]
    fn retry_config_matches_go_max_attempts() {
        assert_eq!(retry_config(20, true).max_retries, 19);
        assert_eq!(retry_config(1, true).max_retries, 0);
        assert_eq!(retry_config(0, true).max_retries, usize::MAX);
        assert_eq!(retry_config(0, true).retry_timeout, Duration::MAX);
        assert_eq!(retry_config(20, false).max_retries, 0);
        let b = retry_config(20, true).backoff;
        assert_eq!(b.init_backoff, Duration::from_secs(1));
        assert_eq!(b.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn credential_kinds() {
        assert_eq!(
            credential_kind(r#"{"type":"service_account"}"#),
            Ok(CredentialKind::ServiceAccount)
        );
        assert_eq!(
            credential_kind(r#"{"type":"authorized_user"}"#),
            Ok(CredentialKind::AuthorizedUser)
        );
        assert_eq!(
            credential_kind(r#"{"type":"external_account"}"#),
            Ok(CredentialKind::ExternalAccount)
        );
        assert_eq!(
            credential_kind(r#"{"type":"impersonated_service_account"}"#),
            Ok(CredentialKind::ImpersonatedServiceAccount)
        );
        for t in ["external_account_authorized_user", "gdch_service_account"] {
            let err = credential_kind(&format!(r#"{{"type":"{t}"}}"#)).unwrap_err();
            assert!(
                err.contains(t) && err.contains("not supported by mimir-rs"),
                "{err}"
            );
        }
        assert!(credential_kind(r#"{"installed":{}}"#)
            .unwrap_err()
            .contains("not supported by mimir-rs"));
        assert_eq!(
            credential_kind("{}").unwrap_err(),
            "missing 'type' field in credentials"
        );
        assert_eq!(
            credential_kind(r#"{"type":"foo"}"#).unwrap_err(),
            "unknown credential type: \"foo\""
        );
        assert!(credential_kind("not json").is_err());
    }

    #[test]
    fn credential_source_precedence() {
        let sa = r#"{"type":"service_account"}"#;
        let au = r#"{"type":"authorized_user"}"#;
        let ext = r#"{"type":"external_account"}"#;
        let imp = r#"{"type":"impersonated_service_account"}"#;
        let gdch = r#"{"type":"gdch_service_account"}"#;
        let cfg_sa = GCSConfig {
            bucket_name: "b".into(),
            service_account: sa.into(),
            ..Default::default()
        };
        let env = env_of(&[
            ("GOOGLE_APPLICATION_CREDENTIALS", "/adc.json"),
            ("GOOGLE_SERVICE_ACCOUNT", "/other.json"),
        ]);
        let files = files_of(&[("/adc.json", gdch)]);
        assert_eq!(
            credential_source(&cfg_sa, &env, &files),
            Ok(CredentialSource::ServiceAccountKey(sa.into()))
        );
        let cfg_au = GCSConfig {
            service_account: au.into(),
            ..cfg_sa.clone()
        };
        assert_eq!(
            credential_source(&cfg_au, &env, &files),
            Ok(CredentialSource::AuthorizedUserJson(au.into()))
        );
        let cfg_ext = GCSConfig {
            service_account: ext.into(),
            ..cfg_sa.clone()
        };
        assert_eq!(
            credential_source(&cfg_ext, &env, &files),
            Ok(CredentialSource::ExternalAccountJson(ext.into()))
        );
        let cfg_imp = GCSConfig {
            service_account: imp.into(),
            ..cfg_sa.clone()
        };
        assert_eq!(
            credential_source(&cfg_imp, &env, &files),
            Ok(CredentialSource::ImpersonatedServiceAccountJson(imp.into()))
        );
        let cfg_gdch = GCSConfig {
            service_account: gdch.into(),
            ..cfg_sa.clone()
        };
        let err = credential_source(&cfg_gdch, &env, &files).unwrap_err();
        assert!(
            err.starts_with("failed to create credentials from JSON: GCS credential type \"gdch_service_account\""),
            "{err}"
        );

        let cfg = GCSConfig {
            bucket_name: "b".into(),
            ..Default::default()
        };
        let err = credential_source(&cfg, &env, &files).unwrap_err();
        assert!(err.contains("GOOGLE_APPLICATION_CREDENTIALS"), "{err}");
        assert!(err.contains("gdch_service_account"), "{err}");
        let files = files_of(&[("/adc.json", au)]);
        assert_eq!(
            credential_source(&cfg, &env, &files),
            Ok(CredentialSource::ApplicationCredentialsFile("/adc.json".into()))
        );
        let env = env_of(&[("GOOGLE_SERVICE_ACCOUNT", "/other.json"), ("HOME", "/home/u")]);
        let files = files_of(&[(
            "/home/u/.config/gcloud/application_default_credentials.json",
            au,
        )]);
        assert_eq!(
            credential_source(&cfg, &env, &files),
            Ok(CredentialSource::ApplicationCredentialsFile(
                "/home/u/.config/gcloud/application_default_credentials.json".into()
            ))
        );
        assert_eq!(
            credential_source(&cfg, &env, &files_of(&[])),
            Ok(CredentialSource::Metadata)
        );
    }

    #[test]
    fn missing_bucket_name_fails_like_go() {
        let err = new_store(&GCSConfig::default()).unwrap_err().to_string();
        assert!(
            err.contains("NewBucketClient: create bucket: missing Google Cloud Storage bucket name for stored blocks"),
            "{err}"
        );
    }

    fn failing_server() -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log2 = Arc::clone(&log);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let log3 = Arc::clone(&log2);
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 {
                            break;
                        }
                        let mut len = 0usize;
                        loop {
                            let mut h = String::new();
                            reader.read_line(&mut h).unwrap();
                            let h = h.trim().to_ascii_lowercase();
                            if h.is_empty() {
                                break;
                            }
                            if let Some(v) = h.strip_prefix("content-length:") {
                                len = v.trim().parse().unwrap_or(0);
                            }
                        }
                        let mut body = vec![0u8; len];
                        reader.read_exact(&mut body).unwrap();
                        let mut parts = line.split_whitespace();
                        let method = parts.next().unwrap_or("").to_string();
                        let path = parts
                            .next()
                            .unwrap_or("")
                            .split('?')
                            .next()
                            .unwrap_or("")
                            .to_string();
                        log3.lock().unwrap().push(format!("{method} {path}"));
                        let resp = "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n";
                        if stream.write_all(resp.as_bytes()).is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), log)
    }

    fn take(log: &std::sync::Mutex<Vec<String>>) -> Vec<String> {
        std::mem::take(&mut *log.lock().unwrap())
    }

    #[tokio::test]
    async fn per_operation_retry_policy() {
        let (url, log) = failing_server();
        let sa = format!(
            r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"{url}","disable_oauth":true}}"#
        );
        for enable_upload_retries in [true, false] {
            let cfg = GCSConfig {
                bucket_name: "bkt".into(),
                service_account: sa.clone(),
                max_retries: 2,
                enable_upload_retries,
                ..Default::default()
            };
            let store = new_store(&cfg).unwrap();
            let p = Path::from("t1/obj");
            assert!(store.head(&p).await.is_err());
            assert_eq!(take(&log), vec!["HEAD /bkt/t1%2Fobj"; 2]);
            assert!(store.delete(&p).await.is_err());
            assert_eq!(take(&log), vec!["DELETE /bkt/t1%2Fobj"]);
            assert!(store.put(&p, PutPayload::from_static(b"x")).await.is_err());
            let expected = if enable_upload_retries { 2 } else { 1 };
            assert_eq!(take(&log), vec!["PUT /bkt/t1%2Fobj"; expected]);
        }
    }

    #[test]
    fn mimir_rs_support_validation() {
        let ok = GCSConfig {
            bucket_name: "b".into(),
            service_account: r#"{"type":"authorized_user"}"#.into(),
            ..Default::default()
        };
        assert!(ok.validate_mimir_rs_support().is_ok());
        for supported in ["external_account", "impersonated_service_account"] {
            let cfg = GCSConfig {
                service_account: format!(r#"{{"type":"{supported}"}}"#),
                ..ok.clone()
            };
            assert!(cfg.validate_mimir_rs_support().is_ok(), "{supported} must be accepted");
        }
        let bad = GCSConfig {
            service_account: r#"{"type":"gdch_service_account"}"#.into(),
            ..ok.clone()
        };
        let err = bad.validate_mimir_rs_support().unwrap_err().to_string();
        assert!(err.contains("\"gdch_service_account\" is not supported by mimir-rs"), "{err}");
        let mut bad = ok.clone();
        bad.http.tls_server_name = "x".into();
        let err = bad.validate_mimir_rs_support().unwrap_err().to_string();
        assert!(
            err.contains("gcs.http.tls_server_name is not supported by mimir-rs for the gcs backend"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn authorized_user_refresh_token_flow() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let mut got = Vec::new();
            loop {
                let n = s.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).to_string();
                if let Some(idx) = text.find("\r\n\r\n") {
                    let len: usize = text
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if got.len() >= idx + 4 + len {
                        break;
                    }
                }
            }
            let body = r#"{"access_token":"tok123","expires_in":3600,"token_type":"Bearer"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&got).to_string()
        });
        let json = format!(
            r#"{{"type":"authorized_user","client_id":"cid","client_secret":"csec","refresh_token":"rt","token_uri":"http://{addr}/token"}}"#
        );
        let p = AuthorizedUserProvider::from_json(&json).unwrap();
        let c = p.get_credential().await.unwrap();
        assert_eq!(c.bearer, "tok123");
        let c2 = p.get_credential().await.unwrap();
        assert!(Arc::ptr_eq(&c, &c2));
        let req = server.await.unwrap();
        assert!(req.starts_with("POST /token HTTP/1.1"), "{req}");
        assert!(req.contains("grant_type=refresh_token"), "{req}");
        assert!(req.contains("refresh_token=rt"), "{req}");
        assert!(req.contains("client_id=cid"), "{req}");
        assert!(req.contains("client_secret=csec"), "{req}");
    }

    #[tokio::test]
    async fn gce_metadata_requests_scopes_not_audience() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = s.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let body = r#"{"access_token":"gce_tok","expires_in":3600,"token_type":"Bearer"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
            req
        });
        let provider = GceMetadataProvider::new();
        let (tok, ttl) = provider.fetch_from_host(&addr.to_string()).await.unwrap();
        assert_eq!(tok, "gce_tok");
        assert_eq!(ttl, Duration::from_secs(3600));
        let req = server.await.unwrap();
        let first_line = req.lines().next().unwrap_or("");
        assert!(
            first_line.starts_with("GET /computeMetadata/v1/instance/service-accounts/default/token?scopes="),
            "expected ?scopes= query param, got: {first_line}"
        );
        assert!(first_line.contains("devstorage.full_control"), "{first_line}");
        assert!(!first_line.contains("audience="), "{first_line}");
    }

    #[tokio::test]
    async fn w23_csc_10_multipart_and_create_upload_retry_transport_eof_when_enabled() {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_srv = Arc::clone(&attempts);
        let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let log_srv = Arc::clone(&log);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let attempts_c = Arc::clone(&attempts_srv);
                let log_c = Arc::clone(&log_srv);
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        return;
                    }
                    let mut len = 0usize;
                    loop {
                        let mut h = String::new();
                        if reader.read_line(&mut h).unwrap_or(0) == 0 {
                            break;
                        }
                        let lower = h.trim().to_ascii_lowercase();
                        if lower.is_empty() {
                            break;
                        }
                        if let Some(v) = lower.strip_prefix("content-length:") {
                            len = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; len];
                    let _ = reader.read_exact(&mut body);
                    log_c.lock().unwrap().push(line.trim().to_string());

                    let n = attempts_c.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        // Drop socket without response -> post-connect transport EOF error.
                        drop(reader);
                        drop(stream);
                        return;
                    }
                    if line.starts_with("POST ") && line.contains("uploads=") {
                        let xml = "<InitiateMultipartUploadResult><UploadId>up-123</UploadId></InitiateMultipartUploadResult>";
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{xml}",
                            xml.len()
                        );
                        let _ = stream.write_all(resp.as_bytes());
                    } else {
                        let resp = "HTTP/1.1 200 OK\r\netag: \"e1\"\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
                        let _ = stream.write_all(resp.as_bytes());
                    }
                });
            }
        });

        let sa = format!(
            r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{addr}","disable_oauth":true}}"#
        );

        // 1. enable_upload_retries = true: put_multipart_opts retries post-connect EOF and succeeds.
        let store_retry = new_store(&GCSConfig {
            bucket_name: "bkt".into(),
            service_account: sa.clone(),
            max_retries: 2,
            enable_upload_retries: true,
            ..Default::default()
        })
        .unwrap();
        let mp = store_retry
            .put_multipart(&Path::from("big/chunks/000001"))
            .await
            .expect("put_multipart must retry post-connect EOF when enable_upload_retries=true");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        drop(mp);

        // 2. enable_upload_retries = true: PutMode::Create also retries post-connect EOF.
        attempts.store(0, Ordering::SeqCst);
        store_retry
            .put_opts(
                &Path::from("create/obj"),
                PutPayload::from_static(b"hello"),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("PutMode::Create must retry post-connect EOF when enable_upload_retries=true");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        // 3. enable_upload_retries = false: put_multipart fails on attempt 1 with 0 retries.
        attempts.store(0, Ordering::SeqCst);
        let store_no_retry = new_store(&GCSConfig {
            bucket_name: "bkt".into(),
            service_account: sa,
            max_retries: 2,
            enable_upload_retries: false,
            ..Default::default()
        })
        .unwrap();
        let res = store_no_retry
            .put_multipart(&Path::from("big/chunks/000002"))
            .await;
        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn d2_02_gcs_no_such_bucket_is_not_obj_not_found_and_errors_on_iter() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    loop {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 4096];
                        let hdr_end = loop {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                return;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break pos;
                            }
                        };
                        let _ = hdr_end;
                        let body = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message></Error>";
                        let resp = format!(
                            "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let sa = format!(
            r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{addr}","disable_oauth":true}}"#
        );
        let store = new_store(&GCSConfig {
            bucket_name: "missing-bkt".into(),
            service_account: sa,
            max_retries: 0,
            ..Default::default()
        })
        .unwrap();

        let reg = prometheus::Registry::new();
        let mut bkt = crate::bucket::BucketClient::from_object_store(store, "missing-bkt", "GCS");
        bkt.metrics = Some(Arc::new(
            crate::bucket::metrics::BucketMetrics::register(&reg, "ingester", "missing-bkt").unwrap(),
        ));

        let dl_err = bkt.download("obj.bin").await.unwrap_err();
        assert!(!dl_err.is_not_found(), "NoSuchBucket must not be NotFound: {dl_err:?}");
        assert!(bkt.iter_dir("").await.is_err());
        assert!(bkt.list_all("").await.is_err());

        let iter_failures: f64 = reg
            .gather()
            .iter()
            .filter(|mf| mf.get_name() == "thanos_objstore_bucket_operation_failures_total")
            .flat_map(|mf| mf.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|lp| lp.get_name() == "operation" && lp.get_value() == "iter")
            })
            .map(|m| m.get_counter().get_value())
            .sum();
        assert_eq!(iter_failures, 2.0);

        srv.abort();
    }

    #[tokio::test]
    async fn d2_11_storage_emulator_host_skips_oauth_without_credentials() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen_auth = Arc::new(std::sync::Mutex::new(Vec::<bool>::new()));
        let seen_auth_srv = Arc::clone(&seen_auth);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let sa_log = Arc::clone(&seen_auth_srv);
                tokio::spawn(async move {
                    loop {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 4096];
                        let hdr_end = loop {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                return;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break pos;
                            }
                        };
                        let head = String::from_utf8_lossy(&buf[..hdr_end]);
                        let has_auth = head
                            .lines()
                            .any(|l| l.to_ascii_lowercase().starts_with("authorization:"));
                        sa_log.lock().unwrap().push(has_auth);
                        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        // Empty service_account + STORAGE_EMULATOR_HOST without scheme ("127.0.0.1:port").
        let cfg = GCSConfig {
            bucket_name: "emu-bkt".into(),
            ..Default::default()
        };
        let host_str = addr.to_string();
        let store = new_store_with_env(&cfg, &|k| {
            if k == "STORAGE_EMULATOR_HOST" {
                Some(host_str.clone())
            } else {
                None
            }
        })
        .unwrap();

        let data = store.get(&Path::from("k1")).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"hello");
        assert_eq!(*seen_auth.lock().unwrap(), vec![false]);

        // Also accepts explicit http:// scheme and rejects invalid URLs.
        let http_str = format!("http://{addr}/");
        assert!(new_store_with_env(&cfg, &|k| {
            (k == "STORAGE_EMULATOR_HOST").then(|| http_str.clone())
        })
        .is_ok());
        assert!(new_store_with_env(&cfg, &|k| {
            (k == "STORAGE_EMULATOR_HOST").then(|| "http://[::1".to_string())
        })
        .is_err());

        // STORAGE_EMULATOR_HOST must NOT be clobbered when service_account JSON provides a base URL / universe override.
        let cfg_with_sa = GCSConfig {
            bucket_name: "emu-bkt".into(),
            service_account: r#"{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"https://storage.custom.universe.example","disable_oauth":true}"#.into(),
            ..Default::default()
        };
        let store_with_sa = new_store_with_env(&cfg_with_sa, &|k| {
            (k == "STORAGE_EMULATOR_HOST").then(|| host_str.clone())
        })
        .unwrap();
        let data2 = store_with_sa
            .get(&Path::from("k2"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data2.as_ref(), b"hello");

        srv.abort();
    }
}
