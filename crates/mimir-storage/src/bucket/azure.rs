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
//! - github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime/policy_retry.go (`setDefaults`)
//! - github.com/Azure/azure-sdk-for-go/sdk/azidentity/default_azure_credential.go,
//!   environment_credential.go, workload_identity.go, managed_identity_client.go and
//!   github.com/AzureAD/microsoft-authentication-library-for-go/apps/managedidentity
//!   (`GetSource`) for the credential selection when no key / connection string is configured.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mimir_config::{AzureConfig, BucketHTTPConfig};
use object_store::azure::{AzureAuthorizer, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::{BackoffConfig, ObjectStore, RetryConfig};
use rand::Rng;
use reqwest::{Method, StatusCode, Url};

use super::exthttp;
use super::BucketError;

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
    /// Copied from Mimir's `max_retries`. NOTE: thanos only uses `MaxRetries` to backfill
    /// `PipelineConfig.MaxTries` in `parseConfig` (YAML path); `NewBucketWithConfig`, which Mimir
    /// calls, does not, so the effective Azure SDK retry count is the SDK default (see
    /// [`retry_config`]). Ported as-is for drop-in compatibility.
    pub max_retries: i64,
    /// thanos `PipelineConfig.MaxTries` (never set by Mimir).
    pub pipeline_max_tries: i32,
    /// thanos `ReaderConfig.MaxRetryRequests` (never set by Mimir).
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
        // azure.DefaultConfig has the default Endpoint, overwrite it only if a different one
        // was explicitly provided.
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
    /// Port of thanos `azure.Config.validate` (same messages, joined with ", "). The
    /// `az_tenant_id` / `client_id` / `client_secret` checks never fire for Mimir, which does not
    /// expose those settings.
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
    /// Service URL with trailing `/`; when authenticating with a SAS it carries `?<sas>`.
    pub service_url: String,
    pub account_name: String,
    pub account_key: String,
    /// The `SharedAccessSignature` value (only set when no `AccountName`+`AccountKey` pair).
    pub sas: String,
}

/// Port of Go azblob `shared.ParseConnectionString`. Keys are case-sensitive and values are not
/// trimmed, exactly like Go. Unknown keys (including `UseDevelopmentStorage`) are ignored, which
/// means `UseDevelopmentStorage=true` alone is rejected (it names neither an account nor an
/// endpoint), as in Go.
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

    let protocol = map.get("DefaultEndpointsProtocol").copied().unwrap_or(DEFAULT_SCHEME);
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
        // add a trailing slash to be consistent with the portal
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

/// Port of azcore `runtime.setDefaults` for the retry options thanos passes
/// (`PipelineConfig.MaxTries`, `RetryDelay`, `MaxRetryDelay` all zero for Mimir): 3 retries,
/// 800ms initial delay, 60s maximum delay, and no total retry duration (`TryTimeout` 0).
///
/// Divergences inherent to `object_store` 0.11 (tracked in PORT_STATUS.md): its backoff uses
/// decorrelated jitter instead of azcore's `(2^n - 1) * delay * [0.8, 1.3)`, and it retries
/// 5xx + transport errors but not 408/429.
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
    /// `azidentity.EnvironmentCredential` with a client secret.
    ClientSecret {
        tenant_id: String,
        client_id: String,
        client_secret: String,
        authority_host: Option<String>,
    },
    /// `azidentity.WorkloadIdentityCredential`.
    WorkloadIdentity {
        tenant_id: String,
        client_id: String,
        token_file: String,
        authority_host: Option<String>,
    },
    /// `azidentity.ManagedIdentityCredential` via IMDS, optionally user-assigned (client ID).
    ManagedIdentity { client_id: Option<String> },
    /// `azidentity.AzureCLICredential`.
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

/// Port of thanos `getTokenCredential` (Mimir never sets `ClientSecret`/`AzTenantID`/`ClientID`):
/// `user_assigned_id` set -> `ManagedIdentityCredential{ID: ClientID(user_assigned_id)}`;
/// otherwise `DefaultAzureCredential` (EnvironmentCredential, WorkloadIdentityCredential,
/// ManagedIdentityCredential, then developer CLIs; `AZURE_TOKEN_CREDENTIALS` restricts the
/// chain).
///
/// Go's chain is evaluated lazily at token time and falls through on "credential unavailable".
/// The environment and workload identity credentials are unavailable exactly when their
/// environment variables are missing, so selecting the first constructible one here is
/// equivalent. Not ported (explicit error or documented): the runtime fallback from an
/// unreachable IMDS to the Azure CLI / azd / PowerShell credentials, client-certificate and
/// username/password environment credentials, and non-IMDS managed identity sources.
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
            env("AZURE_CLIENT_ID"),
            env("AZURE_FEDERATED_TOKEN_FILE"),
            env("AZURE_TENANT_ID"),
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
    /// Service (account) URL without trailing slash and without query string.
    pub service_url: String,
    /// Account name used for shared-key signing.
    pub account: String,
    pub container: String,
    pub auth: Auth,
}

impl ContainerClientConfig {
    /// Container URL (Go `runtime.JoinPaths(serviceURL, container)` minus any SAS query).
    pub fn container_url(&self) -> String {
        format!("{}/{}", self.service_url, self.container)
    }
}

/// Port of thanos `getContainerClient`'s precedence: connection string, then shared key on
/// `https://<account>.<endpoint>/<container>`, then a token credential.
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
        // The account name only matters for shared-key signing; with a SAS it is unused, but
        // object_store requires one, so fall back to the (Go-validated, non-empty) config value.
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
        Auth::Token(select_token_source(&conf.user_assigned_id, env, path_exists)?)
    };
    Ok(ContainerClientConfig {
        service_url,
        account: conf.storage_account_name.clone(),
        container: conf.container_name.clone(),
        auth,
    })
}

/// Go `SharedKeyCredential.SetAccountKey` (`base64.StdEncoding.DecodeString`).
fn validate_account_key(key: &str) -> Result<(), String> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(key)
        .map(|_| ())
        .map_err(|e| format!("decode account key: {e}"))
}

fn builder_for(
    cc: &ContainerClientConfig,
    http: &BucketHTTPConfig,
    retry: RetryConfig,
) -> Result<MicrosoftAzureBuilder, BucketError> {
    // Start from an empty builder (not `from_env`): Go never reads AZURE_STORAGE_* variables, and
    // the only environment it consults (the DefaultAzureCredential chain) is resolved explicitly
    // in `select_token_source`.
    let mut b = MicrosoftAzureBuilder::new()
        .with_client_options(exthttp::client_options(http, None)?)
        .with_retry(retry)
        .with_account(&cc.account)
        .with_container_name(&cc.container)
        .with_endpoint(cc.service_url.clone());
    if cc.service_url.starts_with("http://") {
        b = b.with_allow_http(true);
    }
    b = match &cc.auth {
        Auth::SharedKey { key, .. } => b.with_access_key(key),
        Auth::Sas { sas } => b.with_config(object_store::azure::AzureConfigKey::SasKey, sas),
        Auth::Token(TokenSource::ClientSecret {
            tenant_id,
            client_id,
            client_secret,
            authority_host,
        }) => {
            let mut b = b
                .with_tenant_id(tenant_id)
                .with_client_id(client_id)
                .with_client_secret(client_secret);
            if let Some(h) = authority_host {
                b = b.with_authority_host(h);
            }
            b
        }
        Auth::Token(TokenSource::WorkloadIdentity {
            tenant_id,
            client_id,
            token_file,
            authority_host,
        }) => {
            let mut b = b
                .with_tenant_id(tenant_id)
                .with_client_id(client_id)
                .with_federated_token_file(token_file);
            if let Some(h) = authority_host {
                b = b.with_authority_host(h);
            }
            b
        }
        Auth::Token(TokenSource::ManagedIdentity { client_id }) => match client_id {
            Some(id) => b.with_client_id(id),
            None => b,
        },
        Auth::Token(TokenSource::AzureCli) => b.with_use_azure_cli(true),
    };
    Ok(b)
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
    let builder = builder_for(&cc, &conf.http, retry.clone())?;
    let store = builder.clone().build()?;

    if conf.storage_create_container {
        let http = conf.http.clone();
        let cc2 = cc.clone();
        // Go performs this at client construction and fails startup on error. `from_config` is
        // synchronous and may run inside a Tokio runtime, so run the probe on a dedicated thread
        // with its own runtime and its own clients (never sharing connection pools across
        // runtimes).
        let res = std::thread::spawn(move || -> Result<(), BucketError> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async move {
                let probe_store = builder.build()?;
                let client =
                    exthttp::reqwest_client(&http, None, cc2.service_url.starts_with("http://"))?;
                ensure_container(&probe_store, &client, &cc2, &retry).await
            })
        })
        .join()
        .map_err(|_| BucketError::Config("azure container check panicked".to_string()))?;
        res?;
    }
    Ok(Arc::new(store))
}

/// azcore default retryable status codes.
fn is_retryable_status(s: StatusCode) -> bool {
    matches!(s.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// azcore `calcDelay`: `(2^try - 1) * RetryDelay * [0.8, 1.3)`, capped at `MaxRetryDelay`.
fn calc_delay(retry: &RetryConfig, attempt: u32) -> Duration {
    let factor = (1u64 << attempt.min(62)) - 1;
    let jitter = rand::thread_rng().gen_range(0.8..1.3);
    let d = retry
        .backoff
        .init_backoff
        .as_secs_f64()
        * factor as f64
        * jitter;
    Duration::from_secs_f64(d.min(retry.backoff.max_backoff.as_secs_f64()))
}

struct Resp {
    status: StatusCode,
    error_code: String,
    body: String,
}

async fn send_container_request(
    store: &MicrosoftAzure,
    client: &reqwest::Client,
    cc: &ContainerClientConfig,
    method: Method,
    retry: &RetryConfig,
) -> Result<Resp, String> {
    let mut url = Url::parse(&cc.container_url()).map_err(|e| e.to_string())?;
    url.query_pairs_mut().append_pair("restype", "container");
    let display = format!("{} {}", method, url.as_str().split('?').next().unwrap_or(""));
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let credential = store
            .credentials()
            .get_credential()
            .await
            .map_err(|e| format!("{display}: {e}"))?;
        let mut rb = client.request(method.clone(), url.clone());
        if method == Method::PUT {
            rb = rb.header(reqwest::header::CONTENT_LENGTH, "0");
        }
        let mut req = rb.build().map_err(|e| e.to_string())?;
        AzureAuthorizer::new(&credential, &cc.account).authorize(&mut req);
        let result = client.execute(req).await;
        let retryable = match &result {
            Ok(r) => is_retryable_status(r.status()),
            Err(_) => true,
        };
        if retryable && (attempt as usize) <= retry.max_retries {
            tokio::time::sleep(calc_delay(retry, attempt)).await;
            continue;
        }
        let r = result.map_err(|e| format!("{display}: {e}"))?;
        let status = r.status();
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
}

fn response_error(method: &str, cc: &ContainerClientConfig, r: &Resp) -> String {
    // Condensed azcore `ResponseError.Error()`.
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

/// thanos `NewBucketWithConfig` with `StorageCreateContainer`: `GetProperties` on the container,
/// create it when the error code is `ContainerNotFound`, fail on any other error.
async fn ensure_container(
    store: &MicrosoftAzure,
    client: &reqwest::Client,
    cc: &ContainerClientConfig,
    retry: &RetryConfig,
) -> Result<(), BucketError> {
    let props = send_container_request(store, client, cc, Method::GET, retry)
        .await
        .map_err(BucketError::Config)?;
    if props.status.is_success() {
        return Ok(());
    }
    if props.error_code != "ContainerNotFound" {
        return Err(BucketError::Config(response_error("GET", cc, &props)));
    }
    let created = send_container_request(store, client, cc, Method::PUT, retry)
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

    // Ported from pkg/storage/bucket/azure/bucket_client_test.go TestNewBucketClient.
    #[test]
    fn new_bucket_client_config_mapping() {
        // empty endpoint should be replaced with default
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

        // non-empty endpoint should be kept
        let cfg = AzureConfig {
            endpoint_suffix: "test-endpoint".into(),
            ..base_cfg()
        };
        exp.endpoint = "test-endpoint".into();
        assert_eq!(thanos_config(&cfg), exp);

        // endpoint using connection string
        let cfg = AzureConfig {
            endpoint_suffix: "test-endpoint".into(),
            connection_string: "test-connection-string".into(),
            ..base_cfg()
        };
        exp.storage_connection_string = "test-connection-string".into();
        assert_eq!(thanos_config(&cfg), exp);
    }

    // Ported from thanos objstore providers/azure azure_test.go TestConfig_validate cases that
    // are reachable through Mimir's config.
    #[test]
    fn thanos_validate() {
        let ok = thanos_config(&base_cfg());
        assert_eq!(ok.validate(), Ok(()));

        let mut c = ok.clone();
        c.user_assigned_id = "user_id".into();
        assert_eq!(
            c.validate().unwrap_err(),
            "user_assigned_id cannot be set when using storage_account_key authentication"
        );

        let mut c = ok.clone();
        c.storage_account_key = String::new();
        c.storage_connection_string = "cs".into();
        c.user_assigned_id = "user_id".into();
        assert_eq!(
            c.validate().unwrap_err(),
            "user_assigned_id cannot be set when using storage_connection_string authentication"
        );

        let mut c = ok.clone();
        c.storage_connection_string = "cs".into();
        assert_eq!(
            c.validate().unwrap_err(),
            "storage_account_key and storage_connection_string cannot both be set"
        );

        let mut c = ok.clone();
        c.storage_account_name = String::new();
        c.container_name = String::new();
        assert_eq!(
            c.validate().unwrap_err(),
            "storage_account_name is required but not configured, no container specified"
        );

        // Managed identity with only the account name is valid.
        let mut c = ok.clone();
        c.storage_account_key = String::new();
        c.user_assigned_id = "user_id".into();
        assert_eq!(c.validate(), Ok(()));
    }

    #[test]
    fn retry_config_follows_azcore_defaults_ignoring_max_retries() {
        // Mimir's max_retries (20 by default) does not reach the SDK pipeline.
        let conf = thanos_config(&AzureConfig::default());
        assert_eq!(conf.max_retries, 20);
        let r = retry_config(conf.pipeline_max_tries);
        assert_eq!(r.max_retries, 3);
        assert_eq!(r.backoff.init_backoff, Duration::from_millis(800));
        assert_eq!(r.backoff.max_backoff, Duration::from_secs(60));
        assert_eq!(r.retry_timeout, Duration::MAX);
        assert_eq!(retry_config(-1).max_retries, 0);
        assert_eq!(retry_config(7).max_retries, 7);
    }

    // Ported from azblob internal/shared/shared_test.go connection string cases.
    #[test]
    fn connection_string_parsing() {
        let p = parse_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=dummyaccount;AccountKey=secretkeykey;EndpointSuffix=core.windows.net",
        )
        .unwrap();
        assert_eq!(p.service_url, "https://dummyaccount.blob.core.windows.net/");
        assert_eq!(p.account_name, "dummyaccount");
        assert_eq!(p.account_key, "secretkeykey");

        // default protocol/suffix
        let p = parse_connection_string("AccountName=dummyaccount;AccountKey=secretkeykey").unwrap();
        assert_eq!(p.service_url, "https://dummyaccount.blob.core.windows.net/");

        // custom protocol + suffix (sovereign clouds)
        let p = parse_connection_string(
            "DefaultEndpointsProtocol=http;AccountName=dummyaccount;AccountKey=secretkeykey;EndpointSuffix=core.chinacloudapi.cn;",
        )
        .unwrap();
        assert_eq!(p.service_url, "http://dummyaccount.blob.core.chinacloudapi.cn/");

        // SAS with BlobEndpoint
        let p = parse_connection_string(
            "BlobEndpoint=https://dummyaccount.blob.core.windows.net;SharedAccessSignature=fakesharedaccesssignature",
        )
        .unwrap();
        assert_eq!(
            p.service_url,
            "https://dummyaccount.blob.core.windows.net/?fakesharedaccesssignature"
        );
        assert_eq!(p.sas, "fakesharedaccesssignature");
        assert_eq!(p.account_key, "");

        // SAS with AccountName only
        let p = parse_connection_string(
            "AccountName=dummyaccount;SharedAccessSignature=fakesharedaccesssignature",
        )
        .unwrap();
        assert_eq!(
            p.service_url,
            "https://dummyaccount.blob.core.windows.net/?fakesharedaccesssignature"
        );

        // AccountName + AccountKey wins over SAS
        let p = parse_connection_string(
            "AccountName=a;AccountKey=k;SharedAccessSignature=sas;BlobEndpoint=http://127.0.0.1:10000/a",
        )
        .unwrap();
        assert_eq!(p.service_url, "http://127.0.0.1:10000/a/");
        assert_eq!(p.account_key, "k");
        assert_eq!(p.sas, "");

        // Azurite dev connection string
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
        // connection string wins over endpoint_suffix (flag doc: "If connection-string is set,
        // the value of endpoint-suffix will not be used").
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

        // endpoint_suffix -> https://<account>.<suffix>/<container> with shared key
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

        // invalid base64 key fails like Go's NewSharedKeyCredential
        conf.storage_account_key = "not base64!".into();
        let err = container_client_config(&conf, &no_env, &no_path).unwrap_err();
        assert!(err.starts_with("decode account key: "), "{err}");

        // no key -> token credential (user assigned managed identity)
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
        // user_assigned_id ignores the environment chain entirely.
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
        // EnvironmentCredential (secret) comes first, even when workload identity is configured.
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
        // AKS workload identity webhook env.
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
        // Nothing configured -> system-assigned managed identity (IMDS).
        assert_eq!(
            select_token_source("", &no_env, &no_path),
            Ok(TokenSource::ManagedIdentity { client_id: None })
        );
        // AZURE_CLIENT_ID alone selects that user-assigned identity in DefaultAzureCredential.
        let env = env_of(&[("AZURE_CLIENT_ID", "c")]);
        assert_eq!(
            select_token_source("", &env, &no_path),
            Ok(TokenSource::ManagedIdentity {
                client_id: Some("c".into())
            })
        );
        // Unsupported credential types fail explicitly.
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
        // AZURE_TOKEN_CREDENTIALS.
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

    /// Minimal Azure Blob container endpoint: records requests, answers GetProperties with the
    /// configured status, and Create with 201.
    fn fake_blob_server(get_status: u16, get_code: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
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
        // Settings that are honoured are accepted.
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
}
