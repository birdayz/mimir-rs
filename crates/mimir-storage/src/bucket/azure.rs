//! Azure Blob Storage backend for the blocks-storage bucket client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/azure/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/azure/config.go
//! - github.com/thanos-io/objstore/providers/azure/azure.go (`DefaultConfig`, `validate`,
//!   `NewBucketWithConfig` container auto-creation)
//! - github.com/thanos-io/objstore/providers/azure/helpers.go (`getContainerClient`,
//!   `getTokenCredential`)
//! - github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/internal/shared/shared.go
//!   (`ParseConnectionString`)
//! - github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container/client.go
//!   (`NewClientFromConnectionString`)
//! - github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime/policy_retry.go (`setDefaults`,
//!   408/429/5xx retries, Retry-After / x-ms-retry-after-ms)
//! - github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime/policy_bearer_token.go
//!   (per-attempt token acquisition + 401 invalidation)
//! - github.com/Azure/azure-sdk-for-go/sdk/azidentity/default_azure_credential.go,
//!   environment_credential.go, workload_identity.go, managed_identity_client.go and
//!   github.com/AzureAD/microsoft-authentication-library-for-go/apps/managedidentity
//!   (`GetSource`, IMDS `api-version=2018-02-01`, `IMDS_ENDPOINT`, 404/410/429/5xx retries).

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use mimir_config::{AzureConfig, BucketHTTPConfig};
use object_store::azure::{authority_hosts, AzureAccessKey, AzureAuthorizer, AzureCredential};
use object_store::path::{Path, DELIMITER};
use object_store::{
    Attribute, BackoffConfig, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload,
    PutResult, RetryConfig, UploadPart,
};
use rand::Rng;
use reqwest::header::{
    HeaderName, HeaderValue, ACCEPT, CONTENT_LENGTH, CONTENT_RANGE, IF_MATCH,
    IF_NONE_MATCH,
};
use reqwest::{Method, StatusCode, Url};

use super::exthttp;
use super::BucketError;

const STORE: &str = "MicrosoftAzure";
const VERSION_HEADER: &str = "x-ms-version-id";
const META_PREFIX: &str = "x-ms-meta-";
const AZURE_STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const AZURE_STORAGE_RESOURCE: &str = "https://storage.azure.com";
const IMDS_API_VERSION: &str = "2018-02-01";
const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";
const TOKEN_MIN_TTL: Duration = Duration::from_secs(300);

/// thanos `azure.DefaultConfig.Endpoint`.
pub const DEFAULT_ENDPOINT: &str = "blob.core.windows.net";

/// The subset of thanos `azure.Config` that Mimir's `newBucketClient` populates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThanosAzureConfig {
    pub storage_account_name: String,
    pub storage_account_key: String,
    pub storage_connection_string: String,
    pub storage_create_container: bool,
    pub container_name: String,
    pub endpoint: String,
    pub user_assigned_id: String,
    pub max_retries: i64,
    pub pipeline_max_tries: i32,
    pub reader_max_retry_requests: i64,
    pub http: BucketHTTPConfig,
}

/// Port of Mimir `azure.newBucketClient`'s config mapping on top of thanos `azure.DefaultConfig`.
pub fn thanos_config(cfg: &AzureConfig) -> ThanosAzureConfig {
    ThanosAzureConfig {
        storage_account_name: cfg.account_name.clone(),
        storage_account_key: cfg.account_key.clone(),
        storage_connection_string: cfg.connection_string.clone(),
        storage_create_container: true,
        container_name: cfg.container_name.clone(),
        endpoint: if cfg.endpoint_suffix.is_empty() {
            DEFAULT_ENDPOINT.to_string()
        } else {
            cfg.endpoint_suffix.clone()
        },
        user_assigned_id: cfg.user_assigned_id.clone(),
        max_retries: cfg.max_retries,
        pipeline_max_tries: 0,
        reader_max_retry_requests: 0,
        http: cfg.http.clone(),
    }
}

impl ThanosAzureConfig {
    pub fn validate(&self) -> Result<(), String> {
        let mut errs: Vec<&str> = Vec::new();
        if !self.user_assigned_id.is_empty() && !self.storage_account_key.is_empty() {
            errs.push("user_assigned_id cannot be set when using storage_account_key authentication");
        }
        if !self.user_assigned_id.is_empty() && !self.storage_connection_string.is_empty() {
            errs.push(
                "user_assigned_id cannot be set when using storage_connection_string authentication",
            );
        }
        if !self.storage_account_key.is_empty() && !self.storage_connection_string.is_empty() {
            errs.push("storage_account_key and storage_connection_string cannot both be set");
        }
        if self.storage_account_name.is_empty() {
            errs.push("storage_account_name is required but not configured");
        }
        if self.container_name.is_empty() {
            errs.push("no container specified");
        }
        if self.pipeline_max_tries < 0 {
            errs.push(
                "The value of max_tries must be greater than or equal to 0 in the config file",
            );
        }
        if self.reader_max_retry_requests < 0 {
            errs.push("The value of max_retry_requests must be greater than or equal to 0 in the config file");
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.join(", "))
        }
    }
}

/// Go azblob `errConnectionString`.
pub const ERR_CONNECTION_STRING: &str = "connection string is either blank or malformed. The expected connection string should contain key value pairs separated by semicolons. For example 'DefaultEndpointsProtocol=https;AccountName=<accountName>;AccountKey=<accountKey>;EndpointSuffix=core.windows.net'";

/// Result of Go azblob `shared.ParseConnectionString`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedConnectionString {
    pub service_url: String,
    pub account_name: String,
    pub account_key: String,
    pub sas: String,
}

/// Port of Go azblob `shared.ParseConnectionString`.
pub fn parse_connection_string(connection_string: &str) -> Result<ParsedConnectionString, String> {
    const DEFAULT_SCHEME: &str = "https";
    const DEFAULT_SUFFIX: &str = "core.windows.net";

    let trimmed = connection_string.trim_end_matches(';');
    let mut map: HashMap<&str, &str> = HashMap::new();
    for part in trimmed.split(';') {
        match part.split_once('=') {
            Some((k, v)) => {
                map.insert(k, v);
            }
            None => return Err(ERR_CONNECTION_STRING.to_string()),
        }
    }

    let protocol = map
        .get("DefaultEndpointsProtocol")
        .copied()
        .unwrap_or(DEFAULT_SCHEME);
    let suffix = map.get("EndpointSuffix").copied().unwrap_or(DEFAULT_SUFFIX);
    let blob_endpoint = map.get("BlobEndpoint").copied();
    let account_name = map.get("AccountName").copied();

    let mut service_url = match (blob_endpoint, account_name) {
        (Some(ep), _) => ep.to_string(),
        (None, Some(name)) => format!("{protocol}://{name}.blob.{suffix}"),
        (None, None) => {
            return Err("connection string needs either AccountName or BlobEndpoint".to_string())
        }
    };
    if !service_url.ends_with('/') {
        service_url.push('/');
    }

    let account_key = map.get("AccountKey").copied();
    let sas = map.get("SharedAccessSignature").copied();
    match (account_name, account_key, sas) {
        (Some(name), Some(key), _) => Ok(ParsedConnectionString {
            service_url,
            account_name: name.to_string(),
            account_key: key.to_string(),
            sas: String::new(),
        }),
        (_, _, Some(sas)) => Ok(ParsedConnectionString {
            service_url: format!("{service_url}?{sas}"),
            account_name: account_name.unwrap_or_default().to_string(),
            account_key: String::new(),
            sas: sas.to_string(),
        }),
        _ => Err("connection string needs either AccountKey or SharedAccessSignature".to_string()),
    }
}

/// Port of azcore `runtime.setDefaults` for the retry options thanos passes: 3 retries,
/// 800ms initial delay, 60s maximum delay, and no total retry duration (`TryTimeout` 0).
pub fn retry_config(max_tries: i32) -> RetryConfig {
    let max_retries = match max_tries {
        0 => 3,
        n if n < 0 => 0,
        n => n as usize,
    };
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(800),
            max_backoff: Duration::from_secs(60),
            base: 2.0,
        },
        max_retries,
        retry_timeout: Duration::MAX,
    }
}

/// Token credential source, as Go's `getTokenCredential` would pick it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenSource {
    ClientSecret {
        tenant_id: String,
        client_id: String,
        client_secret: String,
        authority_host: Option<String>,
    },
    WorkloadIdentity {
        tenant_id: String,
        client_id: String,
        token_file: String,
        authority_host: Option<String>,
    },
    ManagedIdentity {
        client_id: Option<String>,
    },
    AzureCli,
}

const CRED_ENV: u8 = 1;
const CRED_WORKLOAD: u8 = 2;
const CRED_MANAGED: u8 = 4;
const CRED_AZ: u8 = 8;
const CRED_AZD: u8 = 16;
const CRED_PWSH: u8 = 32;

fn unsupported(what: &str) -> String {
    format!("{what} is not supported by mimir-rs for the azure backend")
}

/// Port of MSAL `managedidentity.GetSource`: mimir-rs only supports the IMDS source.
fn check_managed_identity_source(
    env: &dyn Fn(&str) -> Option<String>,
    path_exists: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    let get = |k: &str| env(k).unwrap_or_default();
    let identity_endpoint = get("IDENTITY_ENDPOINT");
    if !identity_endpoint.is_empty() && !get("IDENTITY_HEADER").is_empty() {
        let source = if get("IDENTITY_SERVER_THUMBPRINT").is_empty() {
            "App Service"
        } else {
            "Service Fabric"
        };
        return Err(unsupported(&format!(
            "managed identity source {source} (IDENTITY_ENDPOINT/IDENTITY_HEADER)"
        )));
    }
    if !get("MSI_ENDPOINT").is_empty() {
        let source = if get("MSI_SECRET").is_empty() {
            "Cloud Shell"
        } else {
            "Azure ML"
        };
        return Err(unsupported(&format!(
            "managed identity source {source} (MSI_ENDPOINT)"
        )));
    }
    let arc = (!identity_endpoint.is_empty() && !get("IMDS_ENDPOINT").is_empty())
        || (cfg!(target_os = "linux") && path_exists("/opt/azcmagent/bin/himds"));
    if arc {
        return Err(unsupported("managed identity source Azure Arc"));
    }
    Ok(())
}

/// Port of thanos `getTokenCredential`.
pub fn select_token_source(
    user_assigned_id: &str,
    env: &dyn Fn(&str) -> Option<String>,
    path_exists: &dyn Fn(&str) -> bool,
) -> Result<TokenSource, String> {
    if !user_assigned_id.is_empty() {
        check_managed_identity_source(env, path_exists)?;
        return Ok(TokenSource::ManagedIdentity {
            client_id: Some(user_assigned_id.to_string()),
        });
    }

    let mut selected = CRED_ENV | CRED_WORKLOAD | CRED_MANAGED | CRED_AZ | CRED_AZD | CRED_PWSH;
    if let Some(atc) = env("AZURE_TOKEN_CREDENTIALS") {
        let eq = |name: &str| atc.eq_ignore_ascii_case(name);
        selected = if atc == "dev" {
            CRED_AZ | CRED_AZD | CRED_PWSH
        } else if atc == "prod" {
            CRED_ENV | CRED_WORKLOAD | CRED_MANAGED
        } else if eq("EnvironmentCredential") {
            CRED_ENV
        } else if eq("WorkloadIdentityCredential") {
            CRED_WORKLOAD
        } else if eq("ManagedIdentityCredential") {
            CRED_MANAGED
        } else if eq("AzureCLICredential") {
            CRED_AZ
        } else if eq("AzureDeveloperCLICredential") {
            CRED_AZD
        } else if eq("AzurePowerShellCredential") {
            CRED_PWSH
        } else {
            return Err(format!(
                "invalid AZURE_TOKEN_CREDENTIALS value {atc:?}. Valid values are \"dev\", \"prod\", or the name of any credential type in the default chain. See https://aka.ms/azsdk/go/identity/docs#DefaultAzureCredential for more information"
            ));
        };
    }

    let non_empty = |k: &str| env(k).filter(|v| !v.is_empty());
    let authority_host = non_empty("AZURE_AUTHORITY_HOST");
    let mut unavailable: Vec<String> = Vec::new();

    if selected & CRED_ENV != 0 {
        match (non_empty("AZURE_TENANT_ID"), non_empty("AZURE_CLIENT_ID")) {
            (None, _) => unavailable
                .push("EnvironmentCredential: missing environment variable AZURE_TENANT_ID".into()),
            (Some(_), None) => unavailable
                .push("EnvironmentCredential: missing environment variable AZURE_CLIENT_ID".into()),
            (Some(tenant_id), Some(client_id)) => {
                if let Some(client_secret) = non_empty("AZURE_CLIENT_SECRET") {
                    return Ok(TokenSource::ClientSecret {
                        tenant_id,
                        client_id,
                        client_secret,
                        authority_host,
                    });
                }
                if non_empty("AZURE_CLIENT_CERTIFICATE_PATH").is_some() {
                    return Err(unsupported(
                        "EnvironmentCredential with a client certificate (AZURE_CLIENT_CERTIFICATE_PATH)",
                    ));
                }
                if non_empty("AZURE_USERNAME").is_some() {
                    return Err(unsupported(
                        "EnvironmentCredential with username/password (AZURE_USERNAME)",
                    ));
                }
                unavailable.push("EnvironmentCredential: incomplete environment variable configuration. Only AZURE_TENANT_ID and AZURE_CLIENT_ID are set".into());
            }
        }
    }
    if selected & CRED_WORKLOAD != 0 {
        match (
            non_empty("AZURE_CLIENT_ID"),
            non_empty("AZURE_FEDERATED_TOKEN_FILE"),
            non_empty("AZURE_TENANT_ID"),
        ) {
            (Some(client_id), Some(token_file), Some(tenant_id)) => {
                return Ok(TokenSource::WorkloadIdentity {
                    tenant_id,
                    client_id,
                    token_file,
                    authority_host,
                })
            }
            (None, _, _) => unavailable.push("WorkloadIdentityCredential: no client ID specified. Check pod configuration or set ClientID in the options".into()),
            (_, None, _) => unavailable.push("WorkloadIdentityCredential: no token file specified. Check pod configuration or set TokenFilePath in the options".into()),
            (_, _, None) => unavailable.push("WorkloadIdentityCredential: no tenant ID specified. Check pod configuration or set TenantID in the options".into()),
        }
    }
    if selected & CRED_MANAGED != 0 {
        check_managed_identity_source(env, path_exists)?;
        return Ok(TokenSource::ManagedIdentity {
            client_id: non_empty("AZURE_CLIENT_ID"),
        });
    }
    if selected & CRED_AZ != 0 {
        return Ok(TokenSource::AzureCli);
    }
    if selected & CRED_AZD != 0 {
        return Err(unsupported("AzureDeveloperCLICredential"));
    }
    if selected & CRED_PWSH != 0 {
        return Err(unsupported("AzurePowerShellCredential"));
    }
    Err(format!(
        "DefaultAzureCredential: failed to acquire a token.\n\t{}",
        unavailable.join("\n\t")
    ))
}

/// How the container client authenticates (Go `getContainerClient` branches).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Auth {
    SharedKey { account: String, key: String },
    Sas { sas: String },
    Token(TokenSource),
}

/// Fully resolved connection settings for the container client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerClientConfig {
    pub service_url: String,
    pub account: String,
    pub container: String,
    pub auth: Auth,
}

impl ContainerClientConfig {
    pub fn container_url(&self) -> String {
        format!("{}/{}", self.service_url, self.container)
    }
}

/// Port of thanos `getContainerClient`'s precedence.
pub fn container_client_config(
    conf: &ThanosAzureConfig,
    env: &dyn Fn(&str) -> Option<String>,
    path_exists: &dyn Fn(&str) -> bool,
) -> Result<ContainerClientConfig, String> {
    if !conf.storage_connection_string.is_empty() {
        let parsed = parse_connection_string(&conf.storage_connection_string)?;
        let base = parsed
            .service_url
            .split_once('?')
            .map(|(u, _)| u)
            .unwrap_or(&parsed.service_url)
            .trim_end_matches('/')
            .to_string();
        let auth = if !parsed.account_key.is_empty() {
            validate_account_key(&parsed.account_key)?;
            Auth::SharedKey {
                account: parsed.account_name.clone(),
                key: parsed.account_key.clone(),
            }
        } else {
            Auth::Sas { sas: parsed.sas }
        };
        let account = if parsed.account_name.is_empty() {
            conf.storage_account_name.clone()
        } else {
            parsed.account_name
        };
        return Ok(ContainerClientConfig {
            service_url: base,
            account,
            container: conf.container_name.clone(),
            auth,
        });
    }

    let service_url = format!("https://{}.{}", conf.storage_account_name, conf.endpoint);
    let auth = if !conf.storage_account_key.is_empty() {
        validate_account_key(&conf.storage_account_key)?;
        Auth::SharedKey {
            account: conf.storage_account_name.clone(),
            key: conf.storage_account_key.clone(),
        }
    } else {
        Auth::Token(select_token_source(
            &conf.user_assigned_id,
            env,
            path_exists,
        )?)
    };
    Ok(ContainerClientConfig {
        service_url,
        account: conf.storage_account_name.clone(),
        container: conf.container_name.clone(),
        auth,
    })
}

fn validate_account_key(key: &str) -> Result<(), String> {
    BASE64_STANDARD
        .decode(key)
        .map(|_| ())
        .map_err(|e| format!("decode account key: {e}"))
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(hex) = std::str::from_utf8(&b[i + 1..i + 3]) {
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(serde::Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(serde::Deserialize)]
struct ImdsTokenResponse {
    access_token: String,
    #[serde(default)]
    expires_on: Option<serde_json::Value>,
    #[serde(default)]
    expires_in: Option<serde_json::Value>,
}

fn parse_imds_expiry(resp: &ImdsTokenResponse) -> Option<Instant> {
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Some(v) = &resp.expires_on {
        let secs_opt = v
            .as_u64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()));
        if let Some(exp_unix) = secs_opt {
            return Some(Instant::now() + Duration::from_secs(exp_unix.saturating_sub(now_unix)));
        }
    }
    if let Some(v) = &resp.expires_in {
        let secs_opt = v
            .as_u64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()));
        if let Some(ttl) = secs_opt {
            return Some(Instant::now() + Duration::from_secs(ttl));
        }
    }
    None
}

#[derive(Debug)]
enum AzureAuth {
    Static(Arc<AzureCredential>),
    Token {
        source: TokenSource,
        imds_endpoint: String,
        client: reqwest::Client,
        cache: tokio::sync::Mutex<Option<(Arc<AzureCredential>, Option<Instant>)>>,
    },
}

impl AzureAuth {
    fn from_auth(auth: &Auth, env: &dyn Fn(&str) -> Option<String>) -> Result<Self, String> {
        match auth {
            Auth::SharedKey { key, .. } => {
                let k = AzureAccessKey::try_new(key).map_err(|e| e.to_string())?;
                Ok(Self::Static(Arc::new(AzureCredential::AccessKey(k))))
            }
            Auth::Sas { sas } => {
                let pairs = sas
                    .trim_start_matches('?')
                    .split('&')
                    .filter(|p| !p.is_empty())
                    .map(|p| match p.split_once('=') {
                        Some((k, v)) => (percent_decode(k), percent_decode(v)),
                        None => (percent_decode(p), String::new()),
                    })
                    .collect();
                Ok(Self::Static(Arc::new(AzureCredential::SASToken(pairs))))
            }
            Auth::Token(source) => {
                let imds_endpoint = env("IMDS_ENDPOINT")
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| DEFAULT_IMDS_ENDPOINT.to_string())
                    .trim_end_matches('/')
                    .to_string();
                Ok(Self::Token {
                    source: source.clone(),
                    imds_endpoint,
                    client: exthttp::bounded_auth_client(),
                    cache: tokio::sync::Mutex::new(None),
                })
            }
        }
    }

    async fn invalidate(&self) {
        if let Self::Token { cache, .. } = self {
            *cache.lock().await = None;
        }
    }

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        match self {
            Self::Static(c) => Ok(Arc::clone(c)),
            Self::Token {
                source,
                imds_endpoint,
                client,
                cache,
            } => {
                let mut guard = cache.lock().await;
                if let Some((cred, exp)) = guard.as_ref() {
                    if exp.is_none_or(|e| Instant::now() + TOKEN_MIN_TTL < e) {
                        return Ok(Arc::clone(cred));
                    }
                }
                let (tok, exp) = fetch_azure_token(source, imds_endpoint, client)
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: STORE,
                        source: e.into(),
                    })?;
                let cred = Arc::new(AzureCredential::BearerToken(tok));
                *guard = Some((Arc::clone(&cred), exp));
                Ok(cred)
            }
        }
    }
}

/// MSAL Go `retryCodesForIMDS`: 404, 410, 429, 500..=511 (and 408).
fn is_imds_retryable(status: StatusCode) -> bool {
    matches!(status.as_u16(), 404 | 408 | 410 | 429 | 500..=511)
}

async fn fetch_azure_token(
    source: &TokenSource,
    imds_endpoint: &str,
    client: &reqwest::Client,
) -> Result<(String, Option<Instant>), String> {
    match source {
        TokenSource::ManagedIdentity { client_id } => {
            let url = format!("{imds_endpoint}/metadata/identity/oauth2/token");
            let mut query: Vec<(&str, &str)> = vec![
                ("api-version", IMDS_API_VERSION),
                ("resource", AZURE_STORAGE_RESOURCE),
            ];
            if let Some(cid) = client_id.as_deref() {
                query.push(("client_id", cid));
            }
            let mut last_err = String::new();
            for attempt in 0..=4usize {
                match client
                    .get(&url)
                    .header("Metadata", "true")
                    .query(&query)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let status = resp.status();
                        let headers = resp.headers().clone();
                        let body = resp.text().await.map_err(|e| e.to_string())?;
                        if status.is_success() {
                            let parsed: ImdsTokenResponse =
                                serde_json::from_str(&body).map_err(|e| e.to_string())?;
                            let exp = parse_imds_expiry(&parsed);
                            return Ok((parsed.access_token, exp));
                        }
                        last_err = format!("IMDS {status}: {body}");
                        if attempt < 4 && is_imds_retryable(status) {
                            let delay = exthttp::parse_retry_after(&headers)
                                .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                            tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                            continue;
                        }
                        return Err(last_err);
                    }
                    Err(e) => {
                        last_err = format!("IMDS request failed: {e}");
                        if attempt < 4 {
                            tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
                            continue;
                        }
                        return Err(last_err);
                    }
                }
            }
            Err(last_err)
        }
        TokenSource::WorkloadIdentity {
            tenant_id,
            client_id,
            token_file,
            authority_host,
        } => {
            let host = authority_host
                .as_deref()
                .unwrap_or(authority_hosts::AZURE_PUBLIC_CLOUD)
                .trim_end_matches('/');
            let url = format!("{host}/{tenant_id}/oauth2/v2.0/token");
            let assertion = tokio::fs::read_to_string(token_file)
                .await
                .map_err(|e| format!("failed to read AZURE_FEDERATED_TOKEN_FILE {token_file}: {e}"))?;
            let assertion = assertion.trim().to_string();
            let form = [
                ("client_id", client_id.as_str()),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", assertion.as_str()),
                ("scope", AZURE_STORAGE_SCOPE),
                ("grant_type", "client_credentials"),
            ];
            post_oauth_form(client, &url, &form).await
        }
        TokenSource::ClientSecret {
            tenant_id,
            client_id,
            client_secret,
            authority_host,
        } => {
            let host = authority_host
                .as_deref()
                .unwrap_or(authority_hosts::AZURE_PUBLIC_CLOUD)
                .trim_end_matches('/');
            let url = format!("{host}/{tenant_id}/oauth2/v2.0/token");
            let form = [
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("scope", AZURE_STORAGE_SCOPE),
                ("grant_type", "client_credentials"),
            ];
            post_oauth_form(client, &url, &form).await
        }
        TokenSource::AzureCli => {
            let program = if cfg!(target_os = "windows") { "cmd" } else { "az" };
            let mut args = Vec::new();
            if cfg!(target_os = "windows") {
                args.push("/C");
                args.push("az");
            }
            args.extend([
                "account",
                "get-access-token",
                "--output",
                "json",
                "--scope",
                AZURE_STORAGE_SCOPE,
            ]);
            let out = tokio::time::timeout(
                Duration::from_secs(30),
                tokio::process::Command::new(program).args(args).output(),
            )
            .await
            .map_err(|_| "az account get-access-token timed out".to_string())?
            .map_err(|e| format!("az account get-access-token failed: {e}"))?;
            if !out.status.success() {
                return Err(format!(
                    "az account get-access-token failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                ));
            }
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct AzCliOut {
                access_token: String,
            }
            let parsed: AzCliOut =
                serde_json::from_slice(&out.stdout).map_err(|e| e.to_string())?;
            Ok((parsed.access_token, Some(Instant::now() + Duration::from_secs(3600))))
        }
    }
}

async fn post_oauth_form(
    client: &reqwest::Client,
    url: &str,
    form: &[(&str, &str)],
) -> Result<(String, Option<Instant>), String> {
    let mut last_err = String::new();
    for attempt in 0..=3usize {
        let started = Instant::now();
        match client
            .post(url)
            .header(ACCEPT, "application/json")
            .form(form)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let headers = resp.headers().clone();
                let body = resp.text().await.map_err(|e| e.to_string())?;
                if status.is_success() {
                    let parsed: OAuthTokenResponse =
                        serde_json::from_str(&body).map_err(|e| e.to_string())?;
                    let exp = parsed
                        .expires_in
                        .filter(|s| *s > 0)
                        .map(|s| started + Duration::from_secs(s));
                    return Ok((parsed.access_token, exp));
                }
                last_err = format!("OAuth {status}: {body}");
                if attempt < 3 && is_retryable_status(status) {
                    let delay = exthttp::parse_retry_after(&headers)
                        .unwrap_or_else(|| Duration::from_millis(200 << attempt));
                    tokio::time::sleep(delay.min(Duration::from_secs(5))).await;
                    continue;
                }
                return Err(last_err);
            }
            Err(e) => {
                last_err = format!("OAuth request failed: {e}");
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

/// Builds the Azure object store for `cfg` (Go `azure.NewBucketClient` ->
/// thanos `azure.NewBucketWithConfig`), including the startup container check/creation.
pub fn new_store(cfg: &AzureConfig) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let conf = thanos_config(cfg);
    conf.validate().map_err(BucketError::Config)?;
    let env = |k: &str| std::env::var(k).ok();
    let exists = |p: &str| std::path::Path::new(p).exists();
    let cc = container_client_config(&conf, &env, &exists).map_err(BucketError::Config)?;
    let retry = retry_config(conf.pipeline_max_tries);
    let auth = Arc::new(AzureAuth::from_auth(&cc.auth, &env).map_err(BucketError::Config)?);
    let allow_http = cc.service_url.starts_with("http://");
    let client = exthttp::reqwest_client(&conf.http, None, allow_http)?;
    let container_url = Url::parse(&cc.container_url())
        .map_err(|e| BucketError::Config(format!("invalid Azure container URL: {e}")))?;

    let az_client = Arc::new(AzureHttpClient {
        container_url: container_url.clone(),
        account: cc.account.clone(),
        client,
        auth: Arc::clone(&auth),
        retry: retry.clone(),
    });

    if conf.storage_create_container {
        let http = conf.http.clone();
        let cc2 = cc.clone();
        let retry2 = retry.clone();
        let auth2 = Arc::clone(&auth);
        let res = std::thread::spawn(move || -> Result<(), BucketError> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async move {
                let probe_client = exthttp::reqwest_client(&http, None, allow_http)?;
                let probe = AzureHttpClient {
                    container_url,
                    account: cc2.account.clone(),
                    client: probe_client,
                    auth: auth2,
                    retry: retry2,
                };
                ensure_container(&probe, &cc2).await
            })
        })
        .join()
        .map_err(|_| BucketError::Config("azure container check panicked".to_string()))?;
        res?;
    }

    Ok(Arc::new(AzureStore {
        inner: az_client,
        container: cc.container,
    }))
}

/// azcore default retryable status codes.
fn is_retryable_status(s: StatusCode) -> bool {
    matches!(s.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// azcore `calcDelay`: `(2^try - 1) * RetryDelay * [0.8, 1.3)`, capped at `MaxRetryDelay`.
fn calc_delay(retry: &RetryConfig, attempt: u32) -> Duration {
    let factor = (1u64 << attempt.min(62)) - 1;
    let jitter = rand::thread_rng().gen_range(0.8..1.3);
    let d = retry.backoff.init_backoff.as_secs_f64() * factor as f64 * jitter;
    Duration::from_secs_f64(d.min(retry.backoff.max_backoff.as_secs_f64()))
}

#[derive(Debug)]
struct AzureHttpClient {
    container_url: Url,
    account: String,
    client: reqwest::Client,
    auth: Arc<AzureAuth>,
    retry: RetryConfig,
}

impl AzureHttpClient {
    fn path_url(&self, path: &Path) -> object_store::Result<Url> {
        let mut url = self.container_url.clone();
        if !path.as_ref().is_empty() {
            url.path_segments_mut()
                .map_err(|_| object_store::Error::Generic {
                    store: STORE,
                    source: "invalid Azure container URL".into(),
                })?
                .extend(path.parts());
        }
        Ok(url)
    }

    /// Executes an Azure Blob request: acquires fresh credentials AND re-signs via
    /// `AzureAuthorizer` (fresh `Date` / `SharedKey` / `Bearer`) on every retry attempt,
    /// invalidates token cache and retries once on `401`, and retries `408/429/5xx` honoring
    /// `x-ms-retry-after-ms` / `Retry-After`.
    async fn send_with_retry(
        &self,
        path_label: &str,
        idempotent: bool,
        mut build_req: impl FnMut(&reqwest::Client) -> object_store::Result<reqwest::RequestBuilder>,
    ) -> object_store::Result<reqwest::Response> {
        let mut attempt = 0u32;
        let mut auth_refreshed = false;

        loop {
            attempt += 1;
            let cred = self.auth.get_credential().await?;
            let rb = build_req(&self.client)?;
            let mut req = rb.build().map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;
            AzureAuthorizer::new(&cred, &self.account).authorize(&mut req);

            match self.client.execute(req).await {
                Ok(resp) => {
                    let status = resp.status();
                    if status == StatusCode::UNAUTHORIZED && !auth_refreshed {
                        self.auth.invalidate().await;
                        auth_refreshed = true;
                        attempt = attempt.saturating_sub(1);
                        continue;
                    }
                    if status.is_success() {
                        return Ok(resp);
                    }
                    let headers = resp.headers().clone();
                    let body = resp.text().await.unwrap_or_default();
                    if is_retryable_status(status) && (attempt as usize) <= self.retry.max_retries {
                        let delay = exthttp::parse_retry_after(&headers)
                            .unwrap_or_else(|| calc_delay(&self.retry, attempt))
                            .min(self.retry.backoff.max_backoff);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    if let Some(code) = extract_azure_error_code(&headers, &body) {
                        if code == "BlobNotFound" || code == "InvalidUri" {
                            let source: Box<dyn std::error::Error + Send + Sync> = if body.is_empty()
                            {
                                format!("Azure error code {code} with status {status}").into()
                            } else {
                                format!("HTTP error with status {status}: {body}").into()
                            };
                            return Err(object_store::Error::NotFound {
                                path: path_label.to_string(),
                                source,
                            });
                        }
                    }
                    return Err(exthttp::map_http_error(STORE, path_label, status, body));
                }
                Err(e) => {
                    if (idempotent || e.is_connect())
                        && (attempt as usize) <= self.retry.max_retries
                    {
                        tokio::time::sleep(calc_delay(&self.retry, attempt)).await;
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
}

fn extract_azure_error_code(headers: &reqwest::header::HeaderMap, body: &str) -> Option<String> {
    if let Some(code) = headers
        .get("x-ms-error-code")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Some(code.to_string());
    }
    if !body.is_empty() {
        use quick_xml::events::Event;
        let mut reader = quick_xml::Reader::from_str(body);
        let mut buf = Vec::new();
        let mut in_code = false;
        let mut code_text = String::new();
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) if e.local_name().as_ref() == b"Code" => {
                    in_code = true;
                    code_text.clear();
                }
                Ok(Event::Text(t)) if in_code => {
                    if let Ok(s) = t.unescape() {
                        code_text.push_str(&s);
                    }
                }
                Ok(Event::End(e)) if e.local_name().as_ref() == b"Code" => {
                    let trimmed = code_text.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.to_string());
                    }
                    in_code = false;
                }
                Ok(Event::Eof) | Err(_) => break,
                _ => {}
            }
            buf.clear();
        }
    }
    None
}

struct Resp {
    status: StatusCode,
    error_code: String,
    body: String,
}

async fn send_container_request(
    client: &AzureHttpClient,
    cc: &ContainerClientConfig,
    method: Method,
) -> Result<Resp, String> {
    let mut url = Url::parse(&cc.container_url()).map_err(|e| e.to_string())?;
    url.query_pairs_mut().append_pair("restype", "container");
    let display = format!("{} {}", method, url.as_str().split('?').next().unwrap_or(""));
    let mut attempt = 0u32;
    let mut auth_refreshed = false;
    loop {
        attempt += 1;
        let credential = client
            .auth
            .get_credential()
            .await
            .map_err(|e| format!("{display}: {e}"))?;
        let mut rb = client.client.request(method.clone(), url.clone());
        if method == Method::PUT {
            rb = rb.header(CONTENT_LENGTH, "0");
        }
        let mut req = rb.build().map_err(|e| e.to_string())?;
        AzureAuthorizer::new(&credential, &cc.account).authorize(&mut req);
        match client.client.execute(req).await {
            Ok(r) => {
                let status = r.status();
                if status == StatusCode::UNAUTHORIZED && !auth_refreshed {
                    client.auth.invalidate().await;
                    auth_refreshed = true;
                    attempt = attempt.saturating_sub(1);
                    continue;
                }
                if is_retryable_status(status) && (attempt as usize) <= client.retry.max_retries {
                    let delay = exthttp::parse_retry_after(r.headers())
                        .unwrap_or_else(|| calc_delay(&client.retry, attempt))
                        .min(client.retry.backoff.max_backoff);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                let error_code = r
                    .headers()
                    .get("x-ms-error-code")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let body = r.text().await.unwrap_or_default();
                return Ok(Resp {
                    status,
                    error_code,
                    body,
                });
            }
            Err(e) => {
                if (attempt as usize) <= client.retry.max_retries {
                    tokio::time::sleep(calc_delay(&client.retry, attempt)).await;
                    continue;
                }
                return Err(format!("{display}: {e}"));
            }
        }
    }
}

fn response_error(method: &str, cc: &ContainerClientConfig, r: &Resp) -> String {
    let mut msg = format!(
        "{method} {}\nRESPONSE {}: {}\nERROR CODE: {}",
        cc.container_url(),
        r.status.as_u16(),
        r.status,
        if r.error_code.is_empty() {
            "UNAVAILABLE"
        } else {
            &r.error_code
        }
    );
    if !r.body.is_empty() {
        msg.push('\n');
        msg.push_str(&r.body);
    }
    msg
}

async fn ensure_container(
    client: &AzureHttpClient,
    cc: &ContainerClientConfig,
) -> Result<(), BucketError> {
    let props = send_container_request(client, cc, Method::GET)
        .await
        .map_err(BucketError::Config)?;
    if props.status.is_success() {
        return Ok(());
    }
    if props.error_code != "ContainerNotFound" {
        return Err(BucketError::Config(response_error("GET", cc, &props)));
    }
    let created = send_container_request(client, cc, Method::PUT)
        .await
        .map_err(|e| {
            BucketError::Config(format!(
                "error creating Azure blob container: {}: {e}",
                cc.container
            ))
        })?;
    if !created.status.is_success() {
        return Err(BucketError::Config(format!(
            "error creating Azure blob container: {}: {}",
            cc.container,
            response_error("PUT", cc, &created)
        )));
    }
    tracing::info!(address = %cc.container, "Azure blob container successfully created");
    Ok(())
}

#[derive(Debug)]
struct AzureStore {
    inner: Arc<AzureHttpClient>,
    container: String,
}

impl fmt::Display for AzureStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MicrosoftAzure {{ account: {}, container: {} }}",
            self.inner.account, self.container
        )
    }
}

fn apply_azure_attributes(
    mut rb: reqwest::RequestBuilder,
    attrs: &object_store::Attributes,
) -> object_store::Result<reqwest::RequestBuilder> {
    for (k, v) in attrs {
        rb = match k {
            Attribute::CacheControl => rb.header("x-ms-blob-cache-control", v.as_ref()),
            Attribute::ContentDisposition => rb.header("x-ms-blob-content-disposition", v.as_ref()),
            Attribute::ContentEncoding => rb.header("x-ms-blob-content-encoding", v.as_ref()),
            Attribute::ContentLanguage => rb.header("x-ms-blob-content-language", v.as_ref()),
            Attribute::ContentType => rb.header("x-ms-blob-content-type", v.as_ref()),
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
    Ok(rb)
}

#[derive(Debug)]
struct AzureUploadState {
    inner: Arc<AzureHttpClient>,
    path: Path,
    blocks: parking_lot::Mutex<Vec<(usize, String)>>,
}

#[derive(Debug)]
struct AzureMultipartUpload {
    state: Arc<AzureUploadState>,
    part_idx: usize,
    opts: PutMultipartOpts,
}

#[async_trait]
impl MultipartUpload for AzureMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let block_id_raw = format!("{:016x}{:016x}", idx, rand::random::<u64>());
            let block_id_b64 = BASE64_STANDARD.encode(block_id_raw.as_bytes());
            let url = state.inner.path_url(&state.path)?;
            let len = payload.content_length();
            state
                .inner
                .send_with_retry(state.path.as_ref(), true, |c| {
                    Ok(c.put(url.clone())
                        .query(&[("comp", "block"), ("blockid", block_id_b64.as_str())])
                        .header(CONTENT_LENGTH, len)
                        .body(exthttp::payload_to_body(&payload)))
                })
                .await?;
            state.blocks.lock().push((idx, block_id_raw));
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut blocks = std::mem::take(&mut *self.state.blocks.lock());
        blocks.sort_by_key(|(idx, _)| *idx);
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for (_, id) in blocks {
            xml.push_str(&format!(
                "\t<Latest>{}</Latest>\n",
                BASE64_STANDARD.encode(id.as_bytes())
            ));
        }
        xml.push_str("</BlockList>");
        let url = self.state.inner.path_url(&self.state.path)?;
        let opts = std::mem::take(&mut self.opts);
        let resp = self
            .state
            .inner
            .send_with_retry(self.state.path.as_ref(), true, |c| {
                let rb = c
                    .put(url.clone())
                    .query(&[("comp", "blocklist")])
                    .header(CONTENT_LENGTH, xml.len())
                    .body(xml.clone());
                apply_azure_attributes(rb, &opts.attributes)
            })
            .await?;
        let e_tag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let version = resp
            .headers()
            .get(VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        Ok(PutResult { e_tag, version })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        Ok(())
    }
}

fn parse_azure_list_xml(
    xml: &[u8],
    prefix: Option<&str>,
) -> object_store::Result<(ListResult, Option<String>)> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(xml);
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut text = String::new();

    let prefix_str = prefix.unwrap_or_default();
    let mut common_prefixes = Vec::new();
    let mut objects = Vec::new();
    let mut next_marker: Option<String> = None;

    let mut cur_name = String::new();
    let mut cur_lm = String::new();
    let mut cur_len = 0usize;
    let mut cur_etag: Option<String> = None;
    let mut cur_res_type: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "Blob" {
                    cur_name.clear();
                    cur_lm.clear();
                    cur_len = 0;
                    cur_etag = None;
                    cur_res_type = None;
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
                    [_, "NextMarker"] => {
                        if !text.is_empty() {
                            next_marker = Some(text.clone());
                        }
                    }
                    [_, "Blobs", "BlobPrefix", "Name"] => {
                        common_prefixes.push(Path::parse(&text)?);
                    }
                    [_, "Blobs", "Blob", "Name"] => cur_name = text.clone(),
                    [_, "Blobs", "Blob", "Properties", "Last-Modified"] => cur_lm = text.clone(),
                    [_, "Blobs", "Blob", "Properties", "Content-Length"] => {
                        cur_len = text.trim().parse().unwrap_or(0);
                    }
                    [_, "Blobs", "Blob", "Properties", "Etag"] => cur_etag = Some(text.clone()),
                    [_, "Blobs", "Blob", "Properties", "ResourceType"] => {
                        cur_res_type = Some(text.clone());
                    }
                    [_, "Blobs", "Blob"] => {
                        let is_dir = cur_res_type.as_deref() == Some("directory");
                        if !is_dir && cur_name.len() > prefix_str.len() {
                            let last_modified = chrono::DateTime::parse_from_rfc2822(cur_lm.trim())
                                .map(|d| d.with_timezone(&chrono::Utc))
                                .map_err(|e| object_store::Error::Generic {
                                    store: STORE,
                                    source: format!(
                                        "invalid Azure Last-Modified '{}': {e}",
                                        cur_lm
                                    )
                                    .into(),
                                })?;
                            objects.push(ObjectMeta {
                                location: Path::parse(&cur_name)?,
                                last_modified,
                                size: cur_len,
                                e_tag: cur_etag.take(),
                                version: None,
                            });
                        }
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
        next_marker,
    ))
}

#[async_trait]
impl ObjectStore for AzureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let url = self.inner.path_url(location)?;
        let len = payload.content_length();
        let idempotent = matches!(opts.mode, PutMode::Overwrite);
        let resp = self
            .inner
            .send_with_retry(location.as_ref(), idempotent, |c| {
                let mut rb = c
                    .put(url.clone())
                    .header("x-ms-blob-type", "BlockBlob")
                    .header(CONTENT_LENGTH, len);
                rb = apply_azure_attributes(rb, &opts.attributes)?;
                rb = match &opts.mode {
                    PutMode::Overwrite => rb,
                    PutMode::Create => rb.header(IF_NONE_MATCH, "*"),
                    PutMode::Update(v) => {
                        let etag = v.e_tag.as_ref().ok_or_else(|| object_store::Error::Generic {
                            store: STORE,
                            source: "ETag required for conditional update".into(),
                        })?;
                        rb.header(IF_MATCH, etag)
                    }
                };
                Ok(rb.body(exthttp::payload_to_body(&payload)))
            })
            .await?;
        let e_tag = resp
            .headers()
            .get(reqwest::header::ETAG)
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
        Ok(Box::new(AzureMultipartUpload {
            part_idx: 0,
            opts,
            state: Arc::new(AzureUploadState {
                inner: Arc::clone(&self.inner),
                path: location.clone(),
                blocks: parking_lot::Mutex::new(Vec::new()),
            }),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if let Some(GetRange::Suffix(_)) = options.range.as_ref() {
            return Err(object_store::Error::NotSupported {
                source: "Azure does not support suffix range requests".into(),
            });
        }
        if let Some(r) = options.range.as_ref() {
            exthttp::validate_get_range(STORE, r)?;
        }
        let range_opt = options.range.clone();
        let url = self.inner.path_url(location)?;
        let method = if options.head { Method::HEAD } else { Method::GET };
        let resp = self
            .inner
            .send_with_retry(location.as_ref(), true, |c| {
                let mut rb = c
                    .request(method.clone(), url.clone())
                    .header(CONTENT_LENGTH, "0");
                if let Some(ref v) = options.version {
                    rb = rb.query(&[("versionid", v.as_str())]);
                }
                Ok(exthttp::apply_get_options(rb, &options))
            })
            .await?;

        if let Some(res_type) = resp.headers().get("x-ms-resource-type") {
            if res_type.as_bytes() != b"file" {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: format!(
                        "Not a file, got x-ms-resource-type: {}",
                        String::from_utf8_lossy(res_type.as_bytes())
                    )
                    .into(),
                });
            }
        }

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
        let url = self.inner.path_url(location)?;
        self.inner
            .send_with_retry(location.as_ref(), false, |c| {
                Ok(c.delete(url.clone())
                    .header("x-ms-delete-snapshots", "include"))
            })
            .await?;
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{DELIMITER}", p.as_ref()));
        let inner = Arc::clone(&self.inner);
        stream::try_unfold(
            (Some(None::<String>), prefix, inner),
            move |(maybe_marker, prefix, inner)| async move {
                let Some(marker) = maybe_marker else {
                    return Ok::<_, object_store::Error>(None);
                };
                let (res, next) =
                    list_page(&inner, prefix.as_deref(), false, marker.as_deref()).await?;
                let next_state = next.filter(|m| !m.is_empty()).map(Some);
                Ok(Some((
                    stream::iter(res.objects.into_iter().map(Ok)),
                    (next_state, prefix, inner),
                )))
            },
        )
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{DELIMITER}", p.as_ref()));
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let (res, next) =
                list_page(&self.inner, prefix.as_deref(), true, marker.as_deref()).await?;
            common_prefixes.extend(res.common_prefixes);
            objects.extend(res.objects);
            match next {
                Some(m) if !m.is_empty() => marker = Some(m),
                _ => break,
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_impl(from, to, true).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_impl(from, to, false).await
    }
}

async fn list_page(
    inner: &AzureHttpClient,
    prefix: Option<&str>,
    delimiter: bool,
    marker: Option<&str>,
) -> object_store::Result<(ListResult, Option<String>)> {
    let url = inner.container_url.clone();
    let mut query: Vec<(&str, &str)> = vec![("restype", "container"), ("comp", "list")];
    if let Some(p) = prefix {
        query.push(("prefix", p));
    }
    if delimiter {
        query.push(("delimiter", DELIMITER));
    }
    if let Some(m) = marker {
        query.push(("marker", m));
    }
    let resp = inner
        .send_with_retry("", true, |c| Ok(c.get(url.clone()).query(&query)))
        .await?;
    let bytes = resp.bytes().await.map_err(|e| object_store::Error::Generic {
        store: STORE,
        source: Box::new(e),
    })?;
    parse_azure_list_xml(&bytes, prefix)
}

impl AzureStore {
    async fn copy_impl(
        &self,
        from: &Path,
        to: &Path,
        overwrite: bool,
    ) -> object_store::Result<()> {
        let url = self.inner.path_url(to)?;
        let mut src = self.inner.path_url(from)?;
        let cred = self.inner.auth.get_credential().await?;
        if let AzureCredential::SASToken(pairs) = cred.as_ref() {
            src.query_pairs_mut().extend_pairs(pairs);
        }
        let src_str = src.to_string();
        self.inner
            .send_with_retry(to.as_ref(), overwrite, |c| {
                let mut rb = c
                    .put(url.clone())
                    .header("x-ms-copy-source", &src_str)
                    .header(CONTENT_LENGTH, "0");
                if !overwrite {
                    rb = rb.header(IF_NONE_MATCH, "*");
                }
                Ok(rb)
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;

    fn no_env(_: &str) -> Option<String> {
        None
    }
    fn no_path(_: &str) -> bool {
        false
    }
    fn env_of(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |k| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    }

    fn base_cfg() -> AzureConfig {
        AzureConfig {
            account_name: "test".into(),
            account_key: "dGVzdA==".into(),
            container_name: "test".into(),
            max_retries: 3,
            ..Default::default()
        }
    }

    #[test]
    fn new_bucket_client_config_mapping() {
        let cfg = base_cfg();
        let got = thanos_config(&cfg);
        let mut exp = ThanosAzureConfig {
            storage_account_name: "test".into(),
            storage_account_key: "dGVzdA==".into(),
            storage_connection_string: String::new(),
            storage_create_container: true,
            container_name: "test".into(),
            endpoint: "blob.core.windows.net".into(),
            user_assigned_id: String::new(),
            max_retries: 3,
            pipeline_max_tries: 0,
            reader_max_retry_requests: 0,
            http: BucketHTTPConfig::default(),
        };
        assert_eq!(got, exp);

        let cfg = AzureConfig {
            endpoint_suffix: "test-endpoint".into(),
            ..base_cfg()
        };
        exp.endpoint = "test-endpoint".into();
        assert_eq!(thanos_config(&cfg), exp);

        let cfg = AzureConfig {
            endpoint_suffix: "test-endpoint".into(),
            connection_string: "test-connection-string".into(),
            ..base_cfg()
        };
        exp.storage_connection_string = "test-connection-string".into();
        assert_eq!(thanos_config(&cfg), exp);
    }

    #[test]
    fn thanos_validate() {
        let ok = thanos_config(&base_cfg());
        assert_eq!(ok.validate(), Ok(()));

        let mut c = ok.clone();
        c.user_assigned_id = "user_id".into();
        assert_eq!(
            c.validate(),
            Err("user_assigned_id cannot be set when using storage_account_key authentication".into())
        );

        let mut c = ok.clone();
        c.storage_account_key = String::new();
        c.storage_connection_string = "cs".into();
        c.user_assigned_id = "user_id".into();
        assert_eq!(
            c.validate(),
            Err("user_assigned_id cannot be set when using storage_connection_string authentication".into())
        );

        let mut c = ok.clone();
        c.storage_connection_string = "cs".into();
        assert_eq!(
            c.validate(),
            Err("storage_account_key and storage_connection_string cannot both be set".into())
        );

        let mut c = ok.clone();
        c.storage_account_name = String::new();
        assert_eq!(
            c.validate(),
            Err("storage_account_name is required but not configured".into())
        );

        let mut c = ok.clone();
        c.container_name = String::new();
        assert_eq!(c.validate(), Err("no container specified".into()));

        let mut c = ok.clone();
        c.pipeline_max_tries = -1;
        c.reader_max_retry_requests = -1;
        assert_eq!(
            c.validate(),
            Err("The value of max_tries must be greater than or equal to 0 in the config file, The value of max_retry_requests must be greater than or equal to 0 in the config file".into())
        );
    }

    #[test]
    fn parse_connection_string_matches_azblob() {
        let p = parse_connection_string(
            "DefaultEndpointsProtocol=http;AccountName=dummyaccount;AccountKey=secretkeykey;EndpointSuffix=chinacloudapi.cn",
        )
        .unwrap();
        assert_eq!(p.service_url, "http://dummyaccount.blob.chinacloudapi.cn/");
        assert_eq!(p.account_name, "dummyaccount");
        assert_eq!(p.account_key, "secretkeykey");
        assert_eq!(p.sas, "");

        let p = parse_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=dummyaccount;SharedAccessSignature=fakesharedaccesssignature",
        )
        .unwrap();
        assert_eq!(
            p.service_url,
            "https://dummyaccount.blob.core.windows.net/?fakesharedaccesssignature"
        );
        assert_eq!(p.sas, "fakesharedaccesssignature");
        assert_eq!(p.account_key, "");

        let p = parse_connection_string(
            "AccountName=dummyaccount;SharedAccessSignature=fakesharedaccesssignature",
        )
        .unwrap();
        assert_eq!(
            p.service_url,
            "https://dummyaccount.blob.core.windows.net/?fakesharedaccesssignature"
        );

        let p = parse_connection_string(
            "AccountName=a;AccountKey=k;SharedAccessSignature=sas;BlobEndpoint=http://127.0.0.1:10000/a",
        )
        .unwrap();
        assert_eq!(p.service_url, "http://127.0.0.1:10000/a/");
        assert_eq!(p.account_key, "k");
        assert_eq!(p.sas, "");

        let p = parse_connection_string("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;").unwrap();
        assert_eq!(p.service_url, "http://127.0.0.1:10000/devstoreaccount1/");
        assert_eq!(p.account_name, "devstoreaccount1");

        assert_eq!(
            parse_connection_string("").unwrap_err(),
            ERR_CONNECTION_STRING
        );
        assert_eq!(
            parse_connection_string("AccountName").unwrap_err(),
            ERR_CONNECTION_STRING
        );
        assert_eq!(
            parse_connection_string("UseDevelopmentStorage=true").unwrap_err(),
            "connection string needs either AccountName or BlobEndpoint"
        );
        assert_eq!(
            parse_connection_string("AccountName=a").unwrap_err(),
            "connection string needs either AccountKey or SharedAccessSignature"
        );
        assert_eq!(
            parse_connection_string("BlobEndpoint=https://x;AccountKey=k").unwrap_err(),
            "connection string needs either AccountKey or SharedAccessSignature"
        );
    }

    #[test]
    fn container_client_precedence() {
        let mut conf = thanos_config(&AzureConfig {
            account_name: "acct".into(),
            container_name: "c".into(),
            endpoint_suffix: "blob.core.chinacloudapi.cn".into(),
            connection_string: "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;SharedAccessSignature=sv=2020&sig=abc".into(),
            ..Default::default()
        });
        let cc = container_client_config(&conf, &no_env, &no_path).unwrap();
        assert_eq!(cc.container_url(), "http://127.0.0.1:10000/devstoreaccount1/c");
        assert_eq!(cc.account, "acct");
        assert_eq!(
            cc.auth,
            Auth::Sas {
                sas: "sv=2020&sig=abc".into()
            }
        );

        conf.storage_connection_string = String::new();
        conf.storage_account_key = "a2V5".into();
        let cc = container_client_config(&conf, &no_env, &no_path).unwrap();
        assert_eq!(
            cc.container_url(),
            "https://acct.blob.core.chinacloudapi.cn/c"
        );
        assert_eq!(
            cc.auth,
            Auth::SharedKey {
                account: "acct".into(),
                key: "a2V5".into()
            }
        );

        conf.storage_account_key = "not base64!".into();
        let err = container_client_config(&conf, &no_env, &no_path).unwrap_err();
        assert!(err.starts_with("decode account key: "), "{err}");

        conf.storage_account_key = String::new();
        conf.user_assigned_id = "uid".into();
        let cc = container_client_config(&conf, &no_env, &no_path).unwrap();
        assert_eq!(
            cc.auth,
            Auth::Token(TokenSource::ManagedIdentity {
                client_id: Some("uid".into())
            })
        );
    }

    #[test]
    fn token_source_selection() {
        let env = env_of(&[
            ("AZURE_TENANT_ID", "t"),
            ("AZURE_CLIENT_ID", "c"),
            ("AZURE_CLIENT_SECRET", "s"),
        ]);
        assert_eq!(
            select_token_source("uid", &env, &no_path),
            Ok(TokenSource::ManagedIdentity {
                client_id: Some("uid".into())
            })
        );
        let env = env_of(&[
            ("AZURE_TENANT_ID", "t"),
            ("AZURE_CLIENT_ID", "c"),
            ("AZURE_CLIENT_SECRET", "s"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
        ]);
        assert_eq!(
            select_token_source("", &env, &no_path),
            Ok(TokenSource::ClientSecret {
                tenant_id: "t".into(),
                client_id: "c".into(),
                client_secret: "s".into(),
                authority_host: None
            })
        );
        let env = env_of(&[
            ("AZURE_TENANT_ID", "t"),
            ("AZURE_CLIENT_ID", "c"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
            ("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.com/"),
        ]);
        assert_eq!(
            select_token_source("", &env, &no_path),
            Ok(TokenSource::WorkloadIdentity {
                tenant_id: "t".into(),
                client_id: "c".into(),
                token_file: "/var/run/token".into(),
                authority_host: Some("https://login.microsoftonline.com/".into())
            })
        );
        // Empty AZURE_FEDERATED_TOKEN_FILE="" falls through to ManagedIdentity.
        let env = env_of(&[
            ("AZURE_TENANT_ID", "t"),
            ("AZURE_CLIENT_ID", "c"),
            ("AZURE_FEDERATED_TOKEN_FILE", ""),
        ]);
        assert_eq!(
            select_token_source("", &env, &no_path),
            Ok(TokenSource::ManagedIdentity {
                client_id: Some("c".into())
            })
        );
        assert_eq!(
            select_token_source("", &no_env, &no_path),
            Ok(TokenSource::ManagedIdentity { client_id: None })
        );
        let env = env_of(&[("AZURE_CLIENT_ID", "c")]);
        assert_eq!(
            select_token_source("", &env, &no_path),
            Ok(TokenSource::ManagedIdentity {
                client_id: Some("c".into())
            })
        );
        let env = env_of(&[
            ("AZURE_TENANT_ID", "t"),
            ("AZURE_CLIENT_ID", "c"),
            ("AZURE_CLIENT_CERTIFICATE_PATH", "/cert.pem"),
        ]);
        assert!(select_token_source("", &env, &no_path)
            .unwrap_err()
            .contains("is not supported by mimir-rs"));
        let env = env_of(&[("IDENTITY_ENDPOINT", "http://x"), ("IDENTITY_HEADER", "h")]);
        assert!(select_token_source("", &env, &no_path)
            .unwrap_err()
            .contains("App Service"));
        assert!(select_token_source("", &no_env, &|p: &str| p
            == "/opt/azcmagent/bin/himds")
            .unwrap_err()
            .contains("Azure Arc"));
        let env = env_of(&[("AZURE_TOKEN_CREDENTIALS", "dev")]);
        assert_eq!(
            select_token_source("", &env, &no_path),
            Ok(TokenSource::AzureCli)
        );
        let env = env_of(&[("AZURE_TOKEN_CREDENTIALS", "environmentcredential")]);
        let err = select_token_source("", &env, &no_path).unwrap_err();
        assert!(
            err.contains("EnvironmentCredential: missing environment variable AZURE_TENANT_ID"),
            "{err}"
        );
        let env = env_of(&[("AZURE_TOKEN_CREDENTIALS", "bogus")]);
        assert!(select_token_source("", &env, &no_path)
            .unwrap_err()
            .starts_with("invalid AZURE_TOKEN_CREDENTIALS value \"bogus\""));
    }

    fn fake_blob_server(
        get_status: u16,
        get_code: &'static str,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = Arc::clone(&log);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    let request_line = line.trim().to_string();
                    let mut len = 0usize;
                    let mut auth = String::new();
                    loop {
                        let mut h = String::new();
                        reader.read_line(&mut h).unwrap();
                        let h = h.trim().to_string();
                        if h.is_empty() {
                            break;
                        }
                        let lower = h.to_ascii_lowercase();
                        if let Some(v) = lower.strip_prefix("content-length:") {
                            len = v.trim().parse().unwrap_or(0);
                        }
                        if lower.starts_with("authorization:") {
                            auth = h.clone();
                        }
                    }
                    let mut body = vec![0u8; len];
                    reader.read_exact(&mut body).unwrap();
                    log2.lock()
                        .unwrap()
                        .push(format!("{request_line} | {}", auth.split(':').next().unwrap_or("")));
                    let resp = if request_line.starts_with("GET ") {
                        format!(
                            "HTTP/1.1 {get_status} X\r\nx-ms-error-code: {get_code}\r\ncontent-length: 0\r\n\r\n"
                        )
                    } else {
                        "HTTP/1.1 201 Created\r\ncontent-length: 0\r\n\r\n".to_string()
                    };
                    stream.write_all(resp.as_bytes()).unwrap();
                }
            }
        });
        (format!("http://{addr}"), log)
    }

    #[test]
    fn creates_missing_container_at_construction() {
        let (url, log) = fake_blob_server(404, "ContainerNotFound");
        let cfg = AzureConfig {
            account_name: "devstoreaccount1".into(),
            container_name: "blocks".into(),
            connection_string: format!(
                "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint={url}/devstoreaccount1;"
            ),
            ..Default::default()
        };
        new_store(&cfg).unwrap();
        let log = log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "GET /devstoreaccount1/blocks?restype=container HTTP/1.1 | authorization",
                "PUT /devstoreaccount1/blocks?restype=container HTTP/1.1 | authorization",
            ]
        );
    }

    #[test]
    fn existing_container_is_not_recreated_and_other_errors_fail() {
        let (url, log) = fake_blob_server(200, "");
        let mk = |url: &str| AzureConfig {
            account_name: "acct".into(),
            container_name: "blocks".into(),
            connection_string: format!(
                "BlobEndpoint={url}/acct;SharedAccessSignature=sv=2020-01-01&sig=abc%2B"
            ),
            ..Default::default()
        };
        new_store(&mk(&url)).unwrap();
        let entries = log.lock().unwrap().clone();
        assert_eq!(entries.len(), 1);
        assert!(
            entries[0].starts_with("GET /acct/blocks?restype=container&sv=2020-01-01&sig=abc%2B"),
            "{entries:?}"
        );

        let (url, log) = fake_blob_server(403, "AuthorizationFailure");
        let err = new_store(&mk(&url)).unwrap_err().to_string();
        assert!(err.contains("RESPONSE 403"), "{err}");
        assert!(err.contains("ERROR CODE: AuthorizationFailure"), "{err}");
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    #[test]
    fn mimir_rs_support_validation() {
        assert!(base_cfg().validate_mimir_rs_support().is_ok());
        for (mutate, setting) in [
            (
                (|c: &mut AzureConfig| c.http.tls_cert_path = "/c.pem".into())
                    as fn(&mut AzureConfig),
                "tls_cert_path",
            ),
            (|c| c.http.tls_key_path = "/k.pem".into(), "tls_key_path"),
            (|c| c.http.tls_server_name = "x".into(), "tls_server_name"),
            (|c| c.http.max_connections_per_host = 5, "max_connections_per_host"),
        ] {
            let mut c = base_cfg();
            mutate(&mut c);
            let err = c.validate_mimir_rs_support().unwrap_err().to_string();
            assert!(
                err.contains(&format!("azure.http.{setting}"))
                    && err.contains("is not supported by mimir-rs for the azure backend"),
                "{err}"
            );
        }
        let mut c = base_cfg();
        c.http.insecure_skip_verify = true;
        c.http.force_attempt_http2 = true;
        c.http.tls_ca_path = "/ca.pem".into();
        c.http.max_idle_connections = 3;
        assert!(c.validate_mimir_rs_support().is_ok());
    }

    #[test]
    fn construction_fails_on_thanos_validation() {
        let cfg = AzureConfig {
            account_name: "a".into(),
            account_key: "a2V5".into(),
            container_name: "c".into(),
            user_assigned_id: "id".into(),
            ..Default::default()
        };
        let err = new_store(&cfg).unwrap_err().to_string();
        assert!(
            err.contains(
                "user_assigned_id cannot be set when using storage_account_key authentication"
            ),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_identity_imds_2018_02_01_retries_429_and_401_invalidation() {
        use object_store::PutPayload;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // 1. Mock IMDS server: asserts api-version=2018-02-01, retries 404 -> 410 -> 429 -> 200.
        let imds_calls = Arc::new(AtomicUsize::new(0));
        let imds_calls2 = Arc::clone(&imds_calls);
        let imds_queries = Arc::new(Mutex::new(Vec::<String>::new()));
        let imds_queries2 = Arc::clone(&imds_queries);
        let imds_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let imds_addr = imds_listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = imds_listener.accept().await else { break };
                let calls = Arc::clone(&imds_calls2);
                let queries = Arc::clone(&imds_queries2);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        let req = String::from_utf8_lossy(&buf[..n]).to_string();
                        let line = req.lines().next().unwrap_or("").to_string();
                        queries.lock().unwrap().push(line);
                        let idx = calls.fetch_add(1, Ordering::SeqCst);
                        let resp = match idx {
                            0 => "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n".to_string(),
                            1 => "HTTP/1.1 410 Gone\r\ncontent-length: 0\r\n\r\n".to_string(),
                            2 => "HTTP/1.1 429 Too Many Requests\r\nx-ms-retry-after-ms: 10\r\ncontent-length: 0\r\n\r\n".to_string(),
                            3 => {
                                let exp = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() + 3600;
                                let body = format!(r#"{{"access_token":"imds-tok-1","expires_on":"{exp}"}}"#);
                                format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}", body.len())
                            }
                            _ => {
                                let exp = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() + 3600;
                                let body = format!(r#"{{"access_token":"imds-tok-2","expires_on":"{exp}"}}"#);
                                format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}", body.len())
                            }
                        };
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        // 2. Mock HTTPS Azure Blob server on *.localhost.
        let fixture = |name: &str| {
            format!("{}/tests/fixtures/kafka_tls/{name}", env!("CARGO_MANIFEST_DIR"))
        };
        use rustls_pki_types::pem::PemObject;
        let certs: Vec<rustls_pki_types::CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_file_iter(fixture("server.crt"))
                .unwrap()
                .map(Result::unwrap)
                .collect();
        let key = rustls_pki_types::PrivateKeyDer::from_pem_file(fixture("server.key")).unwrap();
        let tls_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_cfg));

        let blob_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blob_port = blob_listener.local_addr().unwrap().port();
        let blob_auths = Arc::new(Mutex::new(Vec::<String>::new()));
        let blob_auths2 = Arc::clone(&blob_auths);
        let put_calls = Arc::new(AtomicUsize::new(0));
        let put_calls2 = Arc::clone(&put_calls);
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = blob_listener.accept().await else { break };
                let acceptor = acceptor.clone();
                let auths = Arc::clone(&blob_auths2);
                let puts = Arc::clone(&put_calls2);
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else { return };
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        let n = match tls.read(&mut tmp).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&tmp[..n]);
                        let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
                        let head = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
                        let req_line = head.lines().next().unwrap_or("").to_string();
                        let mut content_len = 0usize;
                        let mut auth = String::new();
                        for l in head.lines().skip(1) {
                            let lower = l.to_ascii_lowercase();
                            if let Some(v) = lower.strip_prefix("content-length:") {
                                content_len = v.trim().parse().unwrap_or(0);
                            }
                            if let Some((_, v)) = l.split_once(':') {
                                if l.to_ascii_lowercase().starts_with("authorization:") {
                                    auth = v.trim().to_string();
                                }
                            }
                        }
                        while buf.len() < hdr_end + 4 + content_len {
                            let n = match tls.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => n,
                            };
                            buf.extend_from_slice(&tmp[..n]);
                        }
                        buf.drain(..hdr_end + 4 + content_len);
                        auths.lock().unwrap().push(auth.clone());

                        let resp = if req_line.contains("restype=container") {
                            "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_string()
                        } else if req_line.starts_with("PUT ") {
                            let p = puts.fetch_add(1, Ordering::SeqCst);
                            if p == 0 {
                                "HTTP/1.1 429 Too Many Requests\r\nx-ms-retry-after-ms: 20\r\ncontent-length: 0\r\n\r\n".to_string()
                            } else if p == 1 {
                                "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n".to_string()
                            } else {
                                "HTTP/1.1 201 Created\r\netag: \"e1\"\r\ncontent-length: 0\r\n\r\n".to_string()
                            }
                        } else {
                            "HTTP/1.1 200 OK\r\netag: \"e1\"\r\ncontent-length: 2\r\n\r\nok".to_string()
                        };
                        if tls.write_all(resp.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        // Temporarily set env vars for container creation + token fetching.
        let prev_imds = std::env::var_os("IMDS_ENDPOINT");
        let prev_fed = std::env::var_os("AZURE_FEDERATED_TOKEN_FILE");
        let prev_cid = std::env::var_os("AZURE_CLIENT_ID");
        let prev_tid = std::env::var_os("AZURE_TENANT_ID");
        std::env::set_var("IMDS_ENDPOINT", format!("http://{imds_addr}"));
        std::env::set_var("AZURE_FEDERATED_TOKEN_FILE", "");
        std::env::set_var("AZURE_CLIENT_ID", "cid-test");
        std::env::set_var("AZURE_TENANT_ID", "tid-test");

        let mut cfg = AzureConfig {
            account_name: "devstoreaccount1".into(),
            container_name: "blocks".into(),
            endpoint_suffix: format!("blob.localhost:{blob_port}"),
            ..Default::default()
        };
        cfg.http.insecure_skip_verify = true;

        let cfg2 = cfg.clone();
        let store_res = tokio::task::spawn_blocking(move || new_store(&cfg2)).await.unwrap();
        let restore = |k: &str, v: Option<std::ffi::OsString>| match v {
            Some(val) => std::env::set_var(k, val),
            None => std::env::remove_var(k),
        };
        let store = store_res.unwrap();
        // PUT triggers 429 (retry) -> 401 (invalidate cache, fetch imds-tok-2) -> 201!
        let put_res = store.put(&Path::from("obj.bin"), PutPayload::from_static(b"ok")).await;
        restore("IMDS_ENDPOINT", prev_imds);
        restore("AZURE_FEDERATED_TOKEN_FILE", prev_fed);
        restore("AZURE_CLIENT_ID", prev_cid);
        restore("AZURE_TENANT_ID", prev_tid);
        put_res.unwrap();

        let q = imds_queries.lock().unwrap().clone();
        assert!(q.len() >= 5, "expected IMDS retries + 401 refresh: {q:?}");
        assert!(q[0].contains("api-version=2018-02-01"), "{q:?}");
        assert!(q[0].contains("client_id=cid-test"), "{q:?}");
        let auths = blob_auths.lock().unwrap().clone();
        assert!(auths.iter().any(|a| a == "Bearer imds-tok-1"), "{auths:?}");
        assert!(auths.iter().any(|a| a == "Bearer imds-tok-2"), "{auths:?}");
    }
}
