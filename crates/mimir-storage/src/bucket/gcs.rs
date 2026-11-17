//! Google Cloud Storage backend for the blocks-storage bucket client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/bucket_client.go (`NewBucketClient`,
//!   `retryAlwaysBucket`)
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/config.go
//! - github.com/thanos-io/objstore/providers/gcs/gcs.go (`NewBucketWithConfig`,
//!   `appendHttpOptions`, `newBucket`)
//! - cloud.google.com/go/storage invoke.go (`run`: retry policy, idempotency, max attempts,
//!   default backoff) and storage.go (`ObjectHandle.Delete` idempotency)
//! - golang.org/x/oauth2/google default.go / google.go (`CredentialsFromJSON`,
//!   `FindDefaultCredentials` credential file types)
//!
//! Retry semantics (Go): every operation goes through the storage client's `run` with the
//! default `RetryIdempotent` policy and, when `max_retries > 0`, `WithMaxAttempts(max_retries)`
//! (0 = unlimited attempts until the context ends, 1 = no retries). Idempotent calls (get,
//! attributes, list) are retried; deletes and uploads without preconditions are not idempotent
//! and are attempted once. With `enable_upload_retries` (default true) uploads use `RetryAlways`
//! with the same attempt limit. Backoff is gax's default (1s initial, 30s max, multiplier 2).
//! This is mapped onto three `object_store` clients sharing one credential provider, selected per
//! operation by [`GcsRetryPolicyStore`].

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use mimir_config::GCSConfig;
use object_store::gcp::{GcpCredential, GcpCredentialProvider, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{
    BackoffConfig, CredentialProvider, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, RetryConfig,
};

use super::exthttp;
use super::BucketError;

/// Go `golang.org/x/oauth2/google.Endpoint.TokenURL`.
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Credential JSON kinds accepted by Go `google.CredentialsFromJSON`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialKind {
    ServiceAccount,
    AuthorizedUser,
}

/// Classifies a credentials JSON like Go `CredentialsFromJSONWithParams`, failing explicitly for
/// the kinds mimir-rs cannot use (with the same "missing 'type'" / "unknown credential type"
/// messages as Go for invalid ones).
pub fn credential_kind(json: &str) -> Result<CredentialKind, String> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    // Go first tries ConfigFromJSON: a Google Developers Console client_credentials.json
    // ("web"/"installed" OAuth client) yields a 3-legged flow that needs an interactive handler.
    if v.get("web").is_some() || v.get("installed").is_some() {
        return Err(unsupported_kind("OAuth client (client_credentials.json \"web\"/\"installed\")"));
    }
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "service_account" => Ok(CredentialKind::ServiceAccount),
        "authorized_user" => Ok(CredentialKind::AuthorizedUser),
        t @ ("external_account"
        | "external_account_authorized_user"
        | "impersonated_service_account"
        | "gdch_service_account") => Err(unsupported_kind(&format!("{t:?}"))),
        "" => Err("missing 'type' field in credentials".to_string()),
        t => Err(format!("unknown credential type: {t:?}")),
    }
}

fn unsupported_kind(kind: &str) -> String {
    format!("GCS credential type {kind} is not supported by mimir-rs for the gcs backend")
}

/// `object_store` retry config for the Go storage client with `WithMaxAttempts(max_attempts)`
/// (applied only when > 0) and gax default backoff. `retry = false` models a non-idempotent call
/// under `RetryIdempotent` (single attempt).
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

/// Where the GCS client gets its credentials from.
#[derive(Clone, Debug, PartialEq, Eq)]
enum CredentialSource {
    ServiceAccountKey(String),
    AuthorizedUserJson(String),
    /// Application default credentials file (`GOOGLE_APPLICATION_CREDENTIALS` or the gcloud
    /// well-known file).
    ApplicationCredentialsFile(String),
    /// GCE/GKE metadata server.
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
        };
    }
    // Go google.FindDefaultCredentials (only GOOGLE_APPLICATION_CREDENTIALS is read from the
    // environment; object_store's GOOGLE_SERVICE_ACCOUNT* / SERVICE_ACCOUNT are not).
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

/// Builds the GCS object store for `cfg` (Go `gcs.NewBucketClient`).
pub fn new_store(cfg: &GCSConfig) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let wrap = |e: String| BucketError::Config(format!("NewBucketClient: create bucket: {e}"));
    if cfg.bucket_name.is_empty() {
        return Err(wrap(
            "missing Google Cloud Storage bucket name for stored blocks".to_string(),
        ));
    }
    let env = |k: &str| std::env::var(k).ok();
    let read = |p: &str| {
        if std::path::Path::new(p).exists() {
            Some(std::fs::read_to_string(p))
        } else {
            None
        }
    };
    let source = credential_source(cfg, &env, &read).map_err(wrap)?;

    // thanos appendHttpOptions: http.Client{Timeout: IdleConnTimeout}.
    let total_timeout = if cfg.http.idle_conn_timeout.0 > 0 {
        cfg.http.idle_conn_timeout.as_std()
    } else {
        None
    };
    // Go's http.Client is not restricted to https (the endpoint is https unless overridden).
    let options = exthttp::client_options(&cfg.http, total_timeout)?.with_allow_http(true);

    // Start from an empty builder (not `from_env`), see `credential_source`.
    let base = GoogleCloudStorageBuilder::new()
        .with_bucket_name(&cfg.bucket_name)
        .with_client_options(options);
    let base = match &source {
        CredentialSource::ServiceAccountKey(json) => base.with_service_account_key(json),
        CredentialSource::AuthorizedUserJson(json) => {
            let provider = AuthorizedUserProvider::from_json(json).map_err(wrap)?;
            base.with_credentials(Arc::new(provider) as GcpCredentialProvider)
        }
        CredentialSource::ApplicationCredentialsFile(path) => base.with_application_credentials(path),
        CredentialSource::Metadata => base,
    };

    let idempotent = base
        .clone()
        .with_retry(retry_config(cfg.max_retries, true))
        .build()?;
    // Share one credential provider (token cache) between the per-policy clients.
    let creds = Arc::clone(idempotent.credentials());
    let single_attempt: Arc<dyn ObjectStore> = Arc::new(
        base.clone()
            .with_credentials(Arc::clone(&creds))
            .with_retry(retry_config(cfg.max_retries, false))
            .build()?,
    );
    let upload: Arc<dyn ObjectStore> = if cfg.enable_upload_retries {
        // Mimir retryAlwaysBucket: RetryAlways + WithMaxAttempts(max_retries) when > 0.
        Arc::new(
            base.with_credentials(creds)
                .with_retry(retry_config(cfg.max_retries, true))
                .build()?,
        )
    } else {
        Arc::clone(&single_attempt)
    };
    Ok(Arc::new(GcsRetryPolicyStore {
        idempotent: Arc::new(idempotent),
        non_idempotent: single_attempt,
        upload,
    }))
}

/// Routes each operation to the client whose retry policy matches Go's for that operation.
#[derive(Debug)]
pub struct GcsRetryPolicyStore {
    /// get / attributes / list (`RetryIdempotent`, idempotent).
    idempotent: Arc<dyn ObjectStore>,
    /// delete / copy without preconditions (`RetryIdempotent`, non-idempotent: single attempt).
    non_idempotent: Arc<dyn ObjectStore>,
    /// uploads (`RetryAlways` when `enable_upload_retries`, otherwise single attempt).
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
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
}

#[derive(serde::Deserialize)]
struct TokenResponse {
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
            client: reqwest::Client::new(),
            cache: tokio::sync::Mutex::new(None),
        })
    }
}

const EXPIRY_DELTA: Duration = Duration::from_secs(10);

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
            store: "GCS",
            source: format!("oauth2: cannot fetch token: {e}").into(),
        };
        let started = Instant::now();
        let resp = self
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
            .map_err(|e| generic(e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| generic(e.to_string()))?;
        if !status.is_success() {
            return Err(generic(format!("{status}\nResponse: {body}")));
        }
        let tok: TokenResponse = serde_json::from_str(&body).map_err(|e| generic(e.to_string()))?;
        let cred = Arc::new(GcpCredential {
            bearer: tok.access_token,
        });
        let expiry = tok
            .expires_in
            .filter(|s| *s > 0)
            .map(|s| started + Duration::from_secs(s));
        *cache = Some((Arc::clone(&cred), expiry));
        Ok(cred)
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
        // default: 20 attempts -> 19 retries
        assert_eq!(retry_config(20, true).max_retries, 19);
        // 1 = no retries
        assert_eq!(retry_config(1, true).max_retries, 0);
        // 0 = unlimited (until context ends)
        assert_eq!(retry_config(0, true).max_retries, usize::MAX);
        assert_eq!(retry_config(0, true).retry_timeout, Duration::MAX);
        // non-idempotent operations are attempted once
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
        for t in [
            "external_account",
            "external_account_authorized_user",
            "impersonated_service_account",
            "gdch_service_account",
        ] {
            let err = credential_kind(&format!(r#"{{"type":"{t}"}}"#)).unwrap_err();
            assert!(err.contains(t) && err.contains("not supported by mimir-rs"), "{err}");
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
        let cfg_sa = GCSConfig {
            bucket_name: "b".into(),
            service_account: sa.into(),
            ..Default::default()
        };
        // Config key wins over the environment (no "path and key provided" conflict).
        let env = env_of(&[
            ("GOOGLE_APPLICATION_CREDENTIALS", "/adc.json"),
            ("GOOGLE_SERVICE_ACCOUNT", "/other.json"),
        ]);
        let files = files_of(&[("/adc.json", ext)]);
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
        let err = credential_source(&cfg_ext, &env, &files).unwrap_err();
        assert!(
            err.starts_with("failed to create credentials from JSON: GCS credential type \"external_account\""),
            "{err}"
        );

        let cfg = GCSConfig {
            bucket_name: "b".into(),
            ..Default::default()
        };
        // ADC file with unsupported type -> explicit error.
        let err = credential_source(&cfg, &env, &files).unwrap_err();
        assert!(err.contains("GOOGLE_APPLICATION_CREDENTIALS"), "{err}");
        assert!(err.contains("external_account"), "{err}");
        // ADC file with supported type.
        let files = files_of(&[("/adc.json", au)]);
        assert_eq!(
            credential_source(&cfg, &env, &files),
            Ok(CredentialSource::ApplicationCredentialsFile("/adc.json".into()))
        );
        // GOOGLE_SERVICE_ACCOUNT alone is ignored (Go does not read it): well-known file, then
        // metadata server.
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

    /// HTTP server answering every request with 503, recording `METHOD path`.
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
                        let path = parts.next().unwrap_or("").split('?').next().unwrap_or("").to_string();
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

    // Go: idempotent calls retried up to max_retries attempts, deletes once, uploads retried only
    // with enable_upload_retries (Mimir retryAlwaysBucket).
    #[tokio::test]
    async fn per_operation_retry_policy() {
        let (url, log) = failing_server();
        // `gcs_base_url` / `disable_oauth` are object_store test hooks (ignored by Go).
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
        let bad = GCSConfig {
            service_account: r#"{"type":"external_account"}"#.into(),
            ..ok.clone()
        };
        let err = bad.validate_mimir_rs_support().unwrap_err().to_string();
        assert!(err.contains("\"external_account\" is not supported by mimir-rs"), "{err}");
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
        // cached
        let c2 = p.get_credential().await.unwrap();
        assert!(Arc::ptr_eq(&c, &c2));
        let req = server.await.unwrap();
        assert!(req.starts_with("POST /token HTTP/1.1"), "{req}");
        assert!(req.contains("grant_type=refresh_token"), "{req}");
        assert!(req.contains("refresh_token=rt"), "{req}");
        assert!(req.contains("client_id=cid"), "{req}");
        assert!(req.contains("client_secret=csec"), "{req}");
    }
}
