//! S3 credential providers, ported from minio-go's `credentials` package and the thanos objstore
//! S3 provider's credential chain.
//!
//! Ported from:
//! - github.com/thanos-io/objstore/providers/s3/s3.go (`NewBucketWithConfig` credential chain:
//!   `AWSSDKAuth` | `Static` | `EnvAWS` -> `FileAWSCredentials` -> `IAM{Endpoint: STSEndpoint}`)
//! - github.com/thanos-io/objstore/providers/s3/s3_aws_sdk_auth.go (`AWSSDKAuth`)
//! - github.com/minio/minio-go/v7/pkg/credentials/{credentials,chain,static,env_aws,
//!   file_aws_credentials,iam_aws,sts_web_identity,error_response}.go
//! - github.com/aws/aws-sdk-go-v2/config (default credential chain resolution used by
//!   `AWSSDKAuth`, see [`AwsSdkAuth`] for the supported subset)

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

/// minio-go `SignatureType` (only the variants reachable from thanos).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum SignerType {
    #[default]
    V4,
    Anonymous,
}

/// minio-go `credentials.Value`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Value {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: Option<DateTime<Utc>>,
    pub signer_type: SignerType,
}

impl Value {
    fn anonymous() -> Self {
        Self {
            signer_type: SignerType::Anonymous,
            ..Default::default()
        }
    }
}

/// minio-go `credentials.Provider`.
#[async_trait]
pub(crate) trait Provider: Send + Sync {
    async fn retrieve(&mut self) -> Result<Value, String>;
    fn is_expired(&self) -> bool;
    fn expire(&mut self) {}
}

/// minio-go `credentials.Expiry` (`DefaultExpiryWindow` = -1: refresh after 80% of the
/// remaining lifetime).
#[derive(Clone, Debug, Default)]
#[doc(hidden)]
pub struct Expiry {
    expiration: Option<DateTime<Utc>>,
}

impl Expiry {
    #[doc(hidden)]
    pub fn set_expiration(&mut self, expiration: DateTime<Utc>) {
        let now = Utc::now();
        let expire_in = (expiration - now).max(chrono::Duration::zero());
        let cut_ns = (expire_in.num_nanoseconds().unwrap_or(i64::MAX) as f64 * (1.0 - 0.8)) as i64;
        self.expiration = Some(expiration - chrono::Duration::nanoseconds(cut_ns));
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn effective_expiration(&self) -> Option<DateTime<Utc>> {
        self.expiration
    }

    fn is_expired(&self) -> bool {
        match self.expiration {
            None => true,
            Some(e) => e < Utc::now(),
        }
    }
}

/// minio-go `credentials.Static`.
pub(crate) struct Static(pub Value);

#[async_trait]
impl Provider for Static {
    async fn retrieve(&mut self) -> Result<Value, String> {
        if self.0.access_key_id.is_empty() || self.0.secret_access_key.is_empty() {
            return Ok(Value::anonymous());
        }
        Ok(self.0.clone())
    }
    fn is_expired(&self) -> bool {
        false
    }
}

/// Environment access, injectable for tests.
pub(crate) type Env = Arc<dyn Fn(&str) -> String + Send + Sync>;

pub(crate) fn process_env() -> Env {
    Arc::new(|k: &str| std::env::var(k).unwrap_or_default())
}

/// minio-go `credentials.EnvAWS`.
pub(crate) struct EnvAws {
    env: Env,
    retrieved: bool,
}

#[async_trait]
impl Provider for EnvAws {
    async fn retrieve(&mut self) -> Result<Value, String> {
        self.retrieved = false;
        let env = &self.env;
        let mut id = env("AWS_ACCESS_KEY_ID");
        if id.is_empty() {
            id = env("AWS_ACCESS_KEY");
        }
        let mut secret = env("AWS_SECRET_ACCESS_KEY");
        if secret.is_empty() {
            secret = env("AWS_SECRET_KEY");
        }
        let signer_type = if id.is_empty() || secret.is_empty() {
            SignerType::Anonymous
        } else {
            SignerType::V4
        };
        self.retrieved = true;
        Ok(Value {
            access_key_id: id,
            secret_access_key: secret,
            session_token: env("AWS_SESSION_TOKEN"),
            expiration: None,
            signer_type,
        })
    }
    fn is_expired(&self) -> bool {
        !self.retrieved
    }
    fn expire(&mut self) {
        self.retrieved = false;
    }
}

/// A parsed INI file (gopkg.in/ini.v1 semantics for the subset used by AWS files).
pub(crate) struct Ini {
    sections: HashMap<String, HashMap<String, String>>,
}

impl Ini {
    pub fn parse(data: &str) -> Self {
        let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut current = "DEFAULT".to_string();
        sections.entry(current.clone()).or_default();
        for raw in data.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if line.starts_with('[') {
                if let Some(end) = line.find(']') {
                    current = line[1..end].trim().to_string();
                    sections.entry(current.clone()).or_default();
                }
                continue;
            }
            let Some(idx) = line.find(['=', ':']) else {
                continue;
            };
            let key = line[..idx].trim().to_string();
            let mut value = line[idx + 1..].trim().to_string();
            let quoted = value.len() >= 2
                && ((value.starts_with('"') && value.ends_with('"'))
                    || (value.starts_with('\'') && value.ends_with('\'')));
            if quoted {
                value = value[1..value.len() - 1].to_string();
            } else if let Some(i) = value.find([';', '#']) {
                // ini.v1: inline comments start at the first '#' / ';' (IgnoreInlineComment=false).
                value = value[..i].trim_end().to_string();
            }
            sections.entry(current.clone()).or_default().insert(key, value);
        }
        Self { sections }
    }

    pub fn load(path: &str) -> Result<Self, String> {
        let data = std::fs::read_to_string(path).map_err(|e| format!("open {path}: {e}"))?;
        Ok(Self::parse(&data))
    }

    pub fn section(&self, name: &str) -> Result<&HashMap<String, String>, String> {
        self.sections
            .get(name)
            .ok_or_else(|| format!("section {name:?} does not exist"))
    }
}

fn key(section: &HashMap<String, String>, k: &str) -> String {
    section.get(k).cloned().unwrap_or_default()
}

fn home_dir(env: &Env) -> Result<String, String> {
    let h = env("HOME");
    if h.is_empty() {
        return Err("$HOME is not defined".to_string());
    }
    Ok(h)
}

#[doc(hidden)]
pub fn split_credential_process_args(command: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_token = false;
    let mut quote: Option<char> = None;
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some('\'') => {
                if c == '\'' {
                    quote = None;
                } else {
                    cur.push(c);
                }
            }
            Some('"') => {
                if c == '"' {
                    quote = None;
                } else if c == '\\' {
                    if let Some(&next) = chars.peek() {
                        if matches!(next, '"' | '\\' | '$' | '`') {
                            cur.push(next);
                            chars.next();
                        } else {
                            cur.push(c);
                        }
                    } else {
                        return Err("unterminated escape in credential process command".to_string());
                    }
                } else {
                    cur.push(c);
                }
            }
            Some(_) => unreachable!(),
            None => {
                if c.is_whitespace() {
                    if in_token {
                        args.push(std::mem::take(&mut cur));
                        in_token = false;
                    }
                } else if c == '\'' || c == '"' {
                    quote = Some(c);
                    in_token = true;
                } else if c == '\\' {
                    let next = chars
                        .next()
                        .ok_or_else(|| "trailing backslash in credential process command".to_string())?;
                    cur.push(next);
                    in_token = true;
                } else {
                    cur.push(c);
                    in_token = true;
                }
            }
        }
    }
    if quote.is_some() {
        return Err("unterminated quote in credential process command".to_string());
    }
    if in_token {
        args.push(cur);
    }
    if args.is_empty() {
        return Err("invalid credential process args".to_string());
    }
    Ok(args)
}

/// `credential_process` output (`externalProcessCredentials`).
async fn run_credential_process(command: &str) -> Result<Value, String> {
    let args = split_credential_process_args(command)?;
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(&args[0])
            .args(&args[1..])
            .output(),
    )
    .await
    .map_err(|_| "credential_process timed out".to_string())?
    .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!("exit status {}", out.status.code().unwrap_or(-1)));
    }
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).map_err(|e| e.to_string())?;
    let expiration = json_field(&v, "Expiration")
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|d| d.with_timezone(&Utc));
    Ok(Value {
        access_key_id: json_field(&v, "AccessKeyId").unwrap_or_default(),
        secret_access_key: json_field(&v, "SecretAccessKey").unwrap_or_default(),
        session_token: json_field(&v, "SessionToken").unwrap_or_default(),
        expiration,
        signer_type: SignerType::V4,
    })
}

/// Go `encoding/json` field lookup (exact match first, then case-insensitive).
fn json_field(v: &serde_json::Value, name: &str) -> Option<String> {
    let obj = v.as_object()?;
    let found = obj
        .get(name)
        .or_else(|| obj.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v))?;
    found.as_str().map(str::to_string)
}

/// minio-go `credentials.FileAWSCredentials` (profile from `AWS_PROFILE`, file from
/// `AWS_SHARED_CREDENTIALS_FILE` or `~/.aws/credentials`).
pub(crate) struct FileAwsCredentials {
    env: Env,
    expiry: Expiry,
    retrieved: bool,
}

#[async_trait]
impl Provider for FileAwsCredentials {
    async fn retrieve(&mut self) -> Result<Value, String> {
        let env = &self.env;
        let mut filename = env("AWS_SHARED_CREDENTIALS_FILE");
        if filename.is_empty() {
            filename = format!("{}/.aws/credentials", home_dir(env)?);
        }
        let mut profile = env("AWS_PROFILE");
        if profile.is_empty() {
            profile = "default".to_string();
        }
        self.retrieved = false;
        let ini = Ini::load(&filename)?;
        let section = ini.section(&profile)?;
        let process = key(section, "credential_process").trim().to_string();
        if !process.is_empty() {
            let v = run_credential_process(&process).await?;
            self.retrieved = true;
            if let Some(e) = v.expiration {
                self.expiry.set_expiration(e);
            }
            return Ok(v);
        }
        self.retrieved = true;
        Ok(Value {
            access_key_id: key(section, "aws_access_key_id"),
            secret_access_key: key(section, "aws_secret_access_key"),
            session_token: key(section, "aws_session_token"),
            expiration: None,
            signer_type: SignerType::V4,
        })
    }
    fn is_expired(&self) -> bool {
        // minio-go: the embedded Expiry decides once credentials came from a process; plain
        // file credentials never expire after being retrieved.
        if !self.retrieved {
            return true;
        }
        self.expiry.expiration.is_some() && self.expiry.is_expired()
    }
    fn expire(&mut self) {
        self.retrieved = false;
        self.expiry.expiration = None;
    }
}

/// minio-go `ec2RoleCredRespBody`.
#[derive(Default, Debug)]
struct RoleCreds {
    expiration: Option<DateTime<Utc>>,
    access_key_id: String,
    secret_access_key: String,
    token: String,
    code: String,
    message: String,
}

fn parse_role_creds(body: &[u8]) -> Result<RoleCreds, String> {
    let v: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    Ok(RoleCreds {
        expiration: json_field(&v, "Expiration")
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|d| d.with_timezone(&Utc)),
        access_key_id: json_field(&v, "AccessKeyID").unwrap_or_default(),
        secret_access_key: json_field(&v, "SecretAccessKey").unwrap_or_default(),
        token: json_field(&v, "Token").unwrap_or_default(),
        code: json_field(&v, "Code").unwrap_or_default(),
        message: json_field(&v, "Message").unwrap_or_default(),
    })
}

fn status_text(status: reqwest::StatusCode) -> String {
    format!(
        "{} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
}

const CREDENTIAL_HTTP_RETRIES: usize = 3;

fn iam_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn is_cred_http_status_retryable(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn cred_retry_delay(attempt: usize, headers: Option<&reqwest::header::HeaderMap>) -> Duration {
    if let Some(h) = headers {
        if let Some(v) = h.get("retry-after").and_then(|v| v.to_str().ok()) {
            if let Ok(secs) = v.trim().parse::<u64>() {
                return Duration::from_secs(secs.min(5));
            }
        }
    }
    Duration::from_millis((100u64 << attempt.min(5)).min(2_000))
}

/// `getEcsTaskCredentials` (with retries on transient transport/408/429/5xx errors).
async fn get_ecs_task_credentials(
    client: &reqwest::Client,
    endpoint: &str,
    token: &str,
) -> Result<RoleCreds, String> {
    let mut last_err = String::new();
    for attempt in 0..=CREDENTIAL_HTTP_RETRIES {
        let mut req = client.get(endpoint);
        if !token.is_empty() {
            req = req.header("Authorization", token);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status == reqwest::StatusCode::OK {
                    let body = resp.bytes().await.map_err(|e| e.to_string())?;
                    return parse_role_creds(&body);
                }
                last_err = status_text(status);
                if attempt < CREDENTIAL_HTTP_RETRIES && is_cred_http_status_retryable(status) {
                    tokio::time::sleep(cred_retry_delay(attempt, Some(resp.headers()))).await;
                    continue;
                }
                return Err(last_err);
            }
            Err(e) => {
                last_err = e.to_string();
                if attempt < CREDENTIAL_HTTP_RETRIES {
                    tokio::time::sleep(cred_retry_delay(attempt, None)).await;
                    continue;
                }
                return Err(last_err);
            }
        }
    }
    Err(last_err)
}

const DEFAULT_IAM_ROLE_ENDPOINT: &str = "http://169.254.169.254";
const DEFAULT_ECS_ROLE_ENDPOINT: &str = "http://169.254.170.2";
const DEFAULT_STS_ROLE_ENDPOINT: &str = "https://sts.amazonaws.com";
const DEFAULT_IAM_SECURITY_CREDS_PATH: &str = "/latest/meta-data/iam/security-credentials/";

/// `getCredentials` (EC2 IMDS, IMDSv2 token with 1s timeout and v1 fallback on timeout).
async fn get_imds_credentials(client: &reqwest::Client, endpoint: &str) -> Result<RoleCreds, String> {
    let endpoint = if endpoint.is_empty() {
        DEFAULT_IAM_ROLE_ENDPOINT
    } else {
        endpoint
    };
    let token = match tokio::time::timeout(Duration::from_secs(1), async {
        let resp = client
            .put(format!("{endpoint}/latest/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let data = resp.text().await.map_err(|e| e.to_string())?;
        if status != reqwest::StatusCode::OK {
            return Err(status_text(status));
        }
        Ok(data)
    })
    .await
    {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(e),
        Err(_) => String::new(),
    };
    let base = match reqwest::Url::parse(endpoint) {
        Ok(mut u) => {
            u.set_path(DEFAULT_IAM_SECURITY_CREDS_PATH);
            u
        }
        Err(e) => return Err(e.to_string()),
    };
    let list = imds_get_text(client, base.clone(), &token).await?;
    let roles: Vec<&str> = list.lines().collect();
    let Some(role) = roles.first() else {
        return Err("No IAM roles attached to this EC2 service".to_string());
    };
    let mut u = base;
    u.set_path(&format!("{}{}", DEFAULT_IAM_SECURITY_CREDS_PATH.trim_end_matches('/'), format_args!("/{role}")));
    let body = imds_get_text(client, u, &token).await?;
    let creds = parse_role_creds(body.as_bytes())?;
    if creds.code != "Success" {
        return Err(creds.message);
    }
    Ok(creds)
}

async fn imds_get_text(
    client: &reqwest::Client,
    url: reqwest::Url,
    token: &str,
) -> Result<String, String> {
    imds_get_text_with_status(client, url, token)
        .await
        .map_err(|(_, msg)| msg)
}

async fn imds_get_text_with_status(
    client: &reqwest::Client,
    url: reqwest::Url,
    token: &str,
) -> Result<String, (Option<reqwest::StatusCode>, String)> {
    let mut last_err = (None, String::new());
    for attempt in 0..=CREDENTIAL_HTTP_RETRIES {
        let mut req = client.get(url.clone());
        if !token.is_empty() {
            req = req.header("X-aws-ec2-metadata-token", token);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status == reqwest::StatusCode::OK {
                    return resp.text().await.map_err(|e| (None, e.to_string()));
                }
                last_err = (Some(status), status_text(status));
                if attempt < CREDENTIAL_HTTP_RETRIES && is_cred_http_status_retryable(status) {
                    tokio::time::sleep(cred_retry_delay(attempt, Some(resp.headers()))).await;
                    continue;
                }
                return Err(last_err);
            }
            Err(e) => {
                last_err = (None, e.to_string());
                if attempt < CREDENTIAL_HTTP_RETRIES {
                    tokio::time::sleep(cred_retry_delay(attempt, None)).await;
                    continue;
                }
                return Err(last_err);
            }
        }
    }
    Err(last_err)
}

const DEFAULT_IAM_IPV6_ROLE_ENDPOINT: &str = "http://[fd00:ec2::254]";

fn is_aws_container_ip_allowed(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4 == Ipv4Addr::new(169, 254, 170, 2)
                || v4 == Ipv4Addr::new(169, 254, 170, 23)
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6 == Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x23)
                || v6.to_ipv4_mapped().is_some_and(|v4| {
                    v4.is_loopback()
                        || v4 == Ipv4Addr::new(169, 254, 170, 2)
                        || v4 == Ipv4Addr::new(169, 254, 170, 23)
                })
        }
    }
}

async fn validate_aws_sdk_container_full_uri(uri: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(uri).map_err(|e| format!("invalid URL, {e}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!(
            "unsupported local HTTP cred provider URL scheme {:?}",
            parsed.scheme()
        ));
    }
    let host = parsed
        .host_str()
        .unwrap_or("")
        .trim_matches(['[', ']']);
    if host.is_empty() {
        return Err("unable to parse host from local HTTP cred provider URL".to_string());
    }
    if parsed.scheme() == "http" {
        let allowed = if let Ok(ip) = host.parse::<IpAddr>() {
            is_aws_container_ip_allowed(ip)
        } else {
            let lookup_target = if host.contains(':') {
                format!("[{host}]:0")
            } else {
                format!("{host}:0")
            };
            let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(&lookup_target)
                .await
                .map_err(|e| format!("failed to resolve host {host:?}, {e}"))?
                .collect();
            !addrs.is_empty() && addrs.iter().all(|a| is_aws_container_ip_allowed(a.ip()))
        };
        if !allowed {
            return Err(format!(
                "invalid endpoint host, {host:?}, only loopback/ecs/eks hosts are allowed"
            ));
        }
    }
    Ok(())
}

fn parse_aws_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("invalid boolean value {s:?}")),
    }
}

async fn imds_fetch_token_aws_sdk(
    client: &reqwest::Client,
    endpoint: &str,
    v1_fallback_disabled: bool,
    token_disabled: &AtomicBool,
) -> Result<String, String> {
    if !v1_fallback_disabled && token_disabled.load(Ordering::Acquire) {
        return Ok(String::new());
    }
    let res = tokio::time::timeout(Duration::from_secs(1), async {
        let resp = client
            .put(format!("{endpoint}/latest/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await
            .map_err(|e| (None, e.to_string()))?;
        let status = resp.status();
        let data = resp.text().await.map_err(|e| (None, e.to_string()))?;
        if status != reqwest::StatusCode::OK {
            return Err((Some(status), status_text(status)));
        }
        Ok(data)
    })
    .await;
    match res {
        Ok(Ok(t)) => Ok(t),
        Ok(Err((Some(reqwest::StatusCode::BAD_REQUEST), msg))) => {
            Err(format!("failed to get API token, {msg}"))
        }
        Ok(Err((Some(status), msg))) => {
            if matches!(
                status,
                reqwest::StatusCode::FORBIDDEN
                    | reqwest::StatusCode::NOT_FOUND
                    | reqwest::StatusCode::METHOD_NOT_ALLOWED
            ) {
                if !v1_fallback_disabled {
                    token_disabled.store(true, Ordering::Release);
                    return Ok(String::new());
                }
                return Err(format!("failed to get API token, {msg}"));
            }
            if !v1_fallback_disabled {
                return Ok(String::new());
            }
            Err(format!("failed to get API token, {msg}"))
        }
        Ok(Err((None, msg))) => {
            token_disabled.store(true, Ordering::Release);
            if !v1_fallback_disabled {
                return Ok(String::new());
            }
            Err(format!("failed to get API token, {msg}"))
        }
        Err(_) => {
            token_disabled.store(true, Ordering::Release);
            if !v1_fallback_disabled {
                return Ok(String::new());
            }
            Err("failed to get API token, request timeout".to_string())
        }
    }
}

async fn get_imds_credentials_aws_sdk(
    client: &reqwest::Client,
    endpoint: &str,
    v1_fallback_disabled: bool,
    token_disabled: &AtomicBool,
) -> Result<RoleCreds, String> {
    let base = reqwest::Url::parse(endpoint)
        .map(|mut u| {
            u.set_path(DEFAULT_IAM_SECURITY_CREDS_PATH);
            u
        })
        .map_err(|e| e.to_string())?;

    for attempt in 0..2 {
        let token = imds_fetch_token_aws_sdk(client, endpoint, v1_fallback_disabled, token_disabled).await?;
        let list = match imds_get_text_with_status(client, base.clone(), &token).await {
            Ok(v) => v,
            Err((Some(reqwest::StatusCode::UNAUTHORIZED), msg)) => {
                if attempt == 0 && token.is_empty() && !v1_fallback_disabled {
                    token_disabled.store(false, Ordering::Release);
                    continue;
                }
                return Err(msg);
            }
            Err((_, msg)) => return Err(msg),
        };
        let roles: Vec<&str> = list.lines().collect();
        let Some(role) = roles.first() else {
            return Err("No IAM roles attached to this EC2 service".to_string());
        };
        let mut u = base.clone();
        u.set_path(&format!(
            "{}{}",
            DEFAULT_IAM_SECURITY_CREDS_PATH.trim_end_matches('/'),
            format_args!("/{role}")
        ));
        let body = match imds_get_text_with_status(client, u, &token).await {
            Ok(v) => v,
            Err((Some(reqwest::StatusCode::UNAUTHORIZED), msg)) => {
                if attempt == 0 && token.is_empty() && !v1_fallback_disabled {
                    token_disabled.store(false, Ordering::Release);
                    continue;
                }
                return Err(msg);
            }
            Err((_, msg)) => return Err(msg),
        };
        let creds = parse_role_creds(body.as_bytes())?;
        if creds.code != "Success" {
            return Err(creds.message);
        }
        return Ok(creds);
    }
    Err("failed to get EC2 IMDS credentials".to_string())
}

/// `isLoopback`.
async fn is_loopback(uri: &str) -> Result<bool, String> {
    let u = reqwest::Url::parse(uri).map_err(|e| e.to_string())?;
    let host = u.host_str().unwrap_or("").trim_matches(['[', ']']).to_string();
    if host.is_empty() {
        return Err(format!("can't parse host from uri: {uri}"));
    }
    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(if host.contains(':') { format!("[{host}]:0") } else { format!("{host}:0") })
        .await
        .map_err(|e| e.to_string())?
        .collect();
    Ok(addrs.iter().all(|a| a.ip().is_loopback()))
}

/// `getWebIdentityCredentials` + `STSWebIdentity.RetrieveWithCredContext`.
async fn sts_web_identity(
    client: &reqwest::Client,
    endpoint: &str,
    role_arn: &str,
    role_session_name: &str,
    token_file: &str,
) -> Result<Value, String> {
    if endpoint.is_empty() {
        return Err("STS endpoint unknown".to_string());
    }
    let token = std::fs::read_to_string(token_file).map_err(|e| format!("open {token_file}: {e}"))?;
    let mut form: Vec<(String, String)> = vec![("Action".into(), "AssumeRoleWithWebIdentity".into())];
    if !role_arn.is_empty() {
        form.push(("RoleArn".into(), role_arn.into()));
        let session = if role_session_name.is_empty() {
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
                .to_string()
        } else {
            role_session_name.to_string()
        };
        form.push(("RoleSessionName".into(), session));
    }
    form.push(("WebIdentityToken".into(), token));
    form.push(("Version".into(), "2011-06-15".into()));
    // Go url.Values.Encode (sorted keys, '+' for spaces).
    form.sort_by(|a, b| a.0.cmp(&b.0));
    let body = form
        .iter()
        .map(|(k, v)| format!("{}={}", super::signer::go_query_escape(k), super::signer::go_query_escape(v)))
        .collect::<Vec<_>>()
        .join("&");
    let mut last_err = String::new();
    let bytes = {
        let mut got = None;
        for attempt in 0..=CREDENTIAL_HTTP_RETRIES {
            match client
                .post(endpoint)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(body.clone())
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let b = resp.bytes().await.map_err(|e| e.to_string())?;
                    if status == reqwest::StatusCode::OK {
                        got = Some(b);
                        break;
                    }
                    let fields = xml_leaf_fields(&b);
                    let code = fields.get("Code").cloned().unwrap_or_default();
                    let message = fields.get("Message").cloned().unwrap_or_default();
                    last_err = if message.is_empty() {
                        format!("Error response code {code}.")
                    } else {
                        message
                    };
                    let sts_retryable = matches!(
                        code.as_str(),
                        "InvalidIdentityToken"
                            | "InvalidIdentityTokenException"
                            | "IDPCommunicationError"
                            | "ServiceUnavailable"
                            | "Throttling"
                            | "ThrottlingException"
                    ) || is_cred_http_status_retryable(status);
                    if attempt < CREDENTIAL_HTTP_RETRIES && sts_retryable {
                        tokio::time::sleep(cred_retry_delay(attempt, Some(&headers))).await;
                        continue;
                    }
                    return Err(last_err);
                }
                Err(e) => {
                    last_err = e.to_string();
                    if attempt < CREDENTIAL_HTTP_RETRIES {
                        tokio::time::sleep(cred_retry_delay(attempt, None)).await;
                        continue;
                    }
                    return Err(last_err);
                }
            }
        }
        got.ok_or(last_err)?
    };
    let fields = xml_leaf_fields(&bytes);
    let expiration = fields
        .get("Expiration")
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));
    Ok(Value {
        access_key_id: fields.get("AccessKeyId").cloned().unwrap_or_default(),
        secret_access_key: fields.get("SecretAccessKey").cloned().unwrap_or_default(),
        session_token: fields.get("SessionToken").cloned().unwrap_or_default(),
        expiration,
        signer_type: SignerType::V4,
    })
}

/// All leaf element texts of an XML document keyed by local name (first occurrence wins).
fn xml_leaf_fields(body: &[u8]) -> HashMap<String, String> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<String> = Vec::new();
    let mut out = HashMap::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                stack.push(String::from_utf8_lossy(e.local_name().as_ref()).into_owned())
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                if let (Some(name), Ok(text)) = (stack.last(), t.unescape()) {
                    let text = text.trim().to_string();
                    if !text.is_empty() {
                        out.entry(name.clone()).or_insert(text);
                    }
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    out
}

/// minio-go `credentials.IAM` (thanos: `Client: http.DefaultTransport`, `Endpoint: STSEndpoint`).
pub(crate) struct Iam {
    env: Env,
    client: reqwest::Client,
    endpoint: String,
    expiry: Expiry,
}

#[async_trait]
impl Provider for Iam {
    async fn retrieve(&mut self) -> Result<Value, String> {
        let env = &self.env;
        let token = env("AWS_CONTAINER_AUTHORIZATION_TOKEN");
        let token_file = env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");
        let relative_uri = env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        let full_uri = env("AWS_CONTAINER_CREDENTIALS_FULL_URI");
        let identity_file = env("AWS_WEB_IDENTITY_TOKEN_FILE");
        let role_arn = env("AWS_ROLE_ARN");
        let role_session_name = env("AWS_ROLE_SESSION_NAME");
        let region = env("AWS_REGION");
        let mut endpoint = self.endpoint.clone();

        let role_creds = if !identity_file.is_empty() {
            if endpoint.is_empty() {
                endpoint = if !region.is_empty() {
                    regional_sts_endpoint(&region)
                } else {
                    DEFAULT_STS_ROLE_ENDPOINT.to_string()
                };
            }
            let v = sts_web_identity(&self.client, &endpoint, &role_arn, &role_session_name, &identity_file)
                .await?;
            if let Some(e) = v.expiration {
                self.expiry.set_expiration(e);
            }
            return Ok(v);
        } else if !relative_uri.is_empty() {
            if endpoint.is_empty() {
                endpoint = format!("{DEFAULT_ECS_ROLE_ENDPOINT}{relative_uri}");
            }
            get_ecs_task_credentials(&self.client, &endpoint, &token).await?
        } else if !token_file.is_empty() && !full_uri.is_empty() {
            // getEKSPodIdentityCredentials
            let tok = std::fs::read_to_string(&token_file).map_err(|e| {
                format!("getEKSPodIdentityCredentials: failed to read token file:open {token_file}: {e}")
            })?;
            get_ecs_task_credentials(&self.client, &full_uri, tok.trim()).await?
        } else if !full_uri.is_empty() {
            if endpoint.is_empty() {
                endpoint = full_uri.clone();
                if !is_loopback(&endpoint).await? {
                    return Err(format!("uri host is not a loopback address: {endpoint}"));
                }
            }
            get_ecs_task_credentials(&self.client, &endpoint, &token).await?
        } else {
            get_imds_credentials(&self.client, &endpoint).await?
        };
        if let Some(e) = role_creds.expiration {
            self.expiry.set_expiration(e);
        } else {
            self.expiry.expiration = Some(DateTime::<Utc>::MIN_UTC);
        }
        Ok(Value {
            access_key_id: role_creds.access_key_id,
            secret_access_key: role_creds.secret_access_key,
            session_token: role_creds.token,
            expiration: role_creds.expiration,
            signer_type: SignerType::V4,
        })
    }
    fn is_expired(&self) -> bool {
        self.expiry.is_expired()
    }
    fn expire(&mut self) {
        self.expiry.expiration = None;
    }
}

/// thanos `AWSSDKAuth`: aws-sdk-go-v2 `config.LoadDefaultConfig(WithRegion(region))` +
/// `Credentials.Retrieve` on every refresh.
///
/// Supported subset of the AWS SDK default chain (in the SDK's precedence order):
/// - explicit `AWS_PROFILE` with credentials in the shared credentials/config files;
/// - environment static credentials (`AWS_ACCESS_KEY_ID`/`AWS_ACCESS_KEY`,
///   `AWS_SECRET_ACCESS_KEY`/`AWS_SECRET_KEY`, `AWS_SESSION_TOKEN`);
/// - web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` or the profile's
///   `web_identity_token_file`/`role_arn`) against the regional STS endpoint;
/// - shared profile static keys and `credential_process`;
/// - ECS/EKS container credentials (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`/`_FULL_URI`,
///   `AWS_CONTAINER_AUTHORIZATION_TOKEN[_FILE]`);
/// - EC2 IMDS (`AWS_EC2_METADATA_DISABLED`, `AWS_EC2_METADATA_SERVICE_ENDPOINT`).
///
/// Profiles using `source_profile`/`role_arn` role chaining, `credential_source` or SSO
/// (`sso_*`) are rejected with an explicit error (tracked in PORT_STATUS.md).
fn regional_sts_endpoint(region: &str) -> String {
    if region.starts_with("cn-") {
        format!("https://sts.{region}.amazonaws.com.cn")
    } else if region.starts_with("us-isob-") {
        format!("https://sts.{region}.sc2s.sgov.gov")
    } else if region.starts_with("us-iso-") {
        format!("https://sts.{region}.c2s.ic.gov")
    } else {
        format!("https://sts.{region}.amazonaws.com")
    }
}

/// AWS SDK Go v2 `ExpiryWindow = 5 * time.Minute` (or 80% of TTL when TTL < 15m).
fn aws_sdk_refresh_at(expiration: DateTime<Utc>, now: DateTime<Utc>) -> DateTime<Utc> {
    let ttl = expiration - now;
    if ttl <= chrono::Duration::zero() {
        return expiration;
    }
    let window = if ttl >= chrono::Duration::minutes(15) {
        chrono::Duration::minutes(5)
    } else {
        let cut_ns = (ttl.num_nanoseconds().unwrap_or(i64::MAX) as f64 * 0.2) as i64;
        chrono::Duration::nanoseconds(cut_ns)
    };
    expiration - window
}

pub(crate) struct AwsSdkAuth {
    env: Env,
    client: reqwest::Client,
    region: String,
    expires: Option<DateTime<Utc>>,
    refresh_at: Option<DateTime<Utc>>,
    retrieved: bool,
    imds_token_disabled: AtomicBool,
}

impl AwsSdkAuth {
    fn region(&self, profile: Option<&HashMap<String, String>>) -> String {
        if !self.region.is_empty() {
            return self.region.clone();
        }
        let r = (self.env)("AWS_REGION");
        if !r.is_empty() {
            return r;
        }
        let r = (self.env)("AWS_DEFAULT_REGION");
        if !r.is_empty() {
            return r;
        }
        profile.map(|p| key(p, "region")).unwrap_or_default()
    }

    fn sts_endpoint(region: &str) -> Result<String, String> {
        if region.is_empty() {
            return Err("failed to retrieve credentials: operation error STS: AssumeRoleWithWebIdentity, failed to resolve service endpoint, an AWS region is required, but was not found".to_string());
        }
        Ok(regional_sts_endpoint(region))
    }

    /// Loads the merged shared config/credentials profile (`None` if no file defines it).
    fn shared_profile(&self, name: &str) -> Result<Option<HashMap<String, String>>, String> {
        let env = &self.env;
        let home = home_dir(env).unwrap_or_default();
        let mut creds_file = env("AWS_SHARED_CREDENTIALS_FILE");
        if creds_file.is_empty() {
            creds_file = format!("{home}/.aws/credentials");
        }
        let mut config_file = env("AWS_CONFIG_FILE");
        if config_file.is_empty() {
            config_file = format!("{home}/.aws/config");
        }
        let mut merged: Option<HashMap<String, String>> = None;
        let config_section = if name == "default" {
            "default".to_string()
        } else {
            format!("profile {name}")
        };
        if let Ok(ini) = Ini::load(&config_file) {
            if let Ok(s) = ini.section(&config_section) {
                merged.get_or_insert_with(HashMap::new).extend(s.clone());
            }
        }
        if let Ok(ini) = Ini::load(&creds_file) {
            if let Ok(s) = ini.section(name) {
                merged.get_or_insert_with(HashMap::new).extend(s.clone());
            }
        }
        Ok(merged)
    }

    async fn creds_from_profile(
        &self,
        name: &str,
        profile: &HashMap<String, String>,
    ) -> Result<Option<Value>, String> {
        let unsupported = ["source_profile", "credential_source", "sso_session", "sso_start_url", "sso_account_id", "sso_role_name"];
        for k in unsupported {
            if profile.contains_key(k) {
                return Err(format!(
                    "AWS shared config profile {name:?} uses {k:?}, which is not supported by mimir-rs (native_aws_auth_enabled)"
                ));
            }
        }
        let token_file = key(profile, "web_identity_token_file");
        let role_arn = key(profile, "role_arn");
        if !token_file.is_empty() {
            if role_arn.is_empty() {
                return Err("role ARN is not set".to_string());
            }
            let endpoint = Self::sts_endpoint(&self.region(Some(profile)))?;
            let session = key(profile, "role_session_name");
            return sts_web_identity(&self.client, &endpoint, &role_arn, &session, &token_file)
                .await
                .map(Some);
        }
        if !role_arn.is_empty() {
            return Err(format!(
                "AWS shared config profile {name:?} uses \"role_arn\" without web identity, which is not supported by mimir-rs (native_aws_auth_enabled)"
            ));
        }
        let ak = key(profile, "aws_access_key_id");
        let sk = key(profile, "aws_secret_access_key");
        if !ak.is_empty() && !sk.is_empty() {
            return Ok(Some(Value {
                access_key_id: ak,
                secret_access_key: sk,
                session_token: key(profile, "aws_session_token"),
                expiration: None,
                signer_type: SignerType::V4,
            }));
        }
        let process = key(profile, "credential_process");
        if !process.trim().is_empty() {
            return run_credential_process(process.trim()).await.map(Some);
        }
        Ok(None)
    }

    fn imds_options(&self, profile: Option<&HashMap<String, String>>) -> Result<(String, bool), String> {
        let env = &self.env;
        let v1_disabled_env = env("AWS_EC2_METADATA_V1_DISABLED");
        let v1_fallback_disabled = if !v1_disabled_env.is_empty() {
            parse_aws_bool(v1_disabled_env.trim())?
        } else if let Some(p) = profile {
            let v = key(p, "ec2_metadata_v1_disabled");
            if !v.is_empty() {
                parse_aws_bool(v.trim())?
            } else {
                false
            }
        } else {
            false
        };

        let mut endpoint = env("AWS_EC2_METADATA_SERVICE_ENDPOINT");
        if endpoint.is_empty() {
            if let Some(p) = profile {
                endpoint = key(p, "ec2_metadata_service_endpoint");
            }
        }
        let resolved_endpoint = if !endpoint.is_empty() {
            endpoint.trim_end_matches('/').to_string()
        } else {
            let mode_env = env("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE");
            let mode = if !mode_env.is_empty() {
                mode_env
            } else {
                profile
                    .map(|p| key(p, "ec2_metadata_service_endpoint_mode"))
                    .unwrap_or_default()
            };
            let m = mode.trim();
            if m.is_empty() || m.eq_ignore_ascii_case("IPv4") {
                DEFAULT_IAM_ROLE_ENDPOINT.to_string()
            } else if m.eq_ignore_ascii_case("IPv6") {
                DEFAULT_IAM_IPV6_ROLE_ENDPOINT.to_string()
            } else {
                return Err(format!(
                    "unknown EC2 IMDS endpoint mode, must be either IPv6 or IPv4 (got {m:?})"
                ));
            }
        };
        Ok((resolved_endpoint, v1_fallback_disabled))
    }

    async fn container_or_imds(&self, profile: Option<&HashMap<String, String>>) -> Result<Value, String> {
        let env = &self.env;
        let relative = env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        let full = env("AWS_CONTAINER_CREDENTIALS_FULL_URI");
        let rc = if !relative.is_empty() || !full.is_empty() {
            let endpoint = if !relative.is_empty() {
                format!("{DEFAULT_ECS_ROLE_ENDPOINT}{relative}")
            } else {
                validate_aws_sdk_container_full_uri(&full).await?;
                full
            };
            let mut token = env("AWS_CONTAINER_AUTHORIZATION_TOKEN");
            let token_file = env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");
            if !token_file.is_empty() {
                token = std::fs::read_to_string(&token_file)
                    .map_err(|e| format!("failed to read authorization token from {token_file}: {e}"))?;
            }
            get_ecs_task_credentials(&self.client, &endpoint, token.trim()).await?
        } else {
            if env("AWS_EC2_METADATA_DISABLED").eq_ignore_ascii_case("true") {
                return Err("failed to refresh cached credentials, no EC2 IMDS role found, access disabled".to_string());
            }
            let (endpoint, v1_fallback_disabled) = self.imds_options(profile)?;
            get_imds_credentials_aws_sdk(
                &self.client,
                &endpoint,
                v1_fallback_disabled,
                &self.imds_token_disabled,
            )
            .await?
        };
        Ok(Value {
            access_key_id: rc.access_key_id,
            secret_access_key: rc.secret_access_key,
            session_token: rc.token,
            expiration: rc.expiration,
            signer_type: SignerType::V4,
        })
    }

    async fn resolve(&self) -> Result<Value, String> {
        let env = &self.env;
        let mut explicit_profile = env("AWS_PROFILE");
        if explicit_profile.is_empty() {
            explicit_profile = env("AWS_DEFAULT_PROFILE");
        }
        let profile_name = if explicit_profile.is_empty() {
            "default".to_string()
        } else {
            explicit_profile.clone()
        };
        let profile = self.shared_profile(&profile_name)?;
        if !explicit_profile.is_empty() && profile.is_none() {
            return Err(format!(
                "failed to get shared config profile, {explicit_profile}"
            ));
        }
        let mut ak = env("AWS_ACCESS_KEY_ID");
        if ak.is_empty() {
            ak = env("AWS_ACCESS_KEY");
        }
        let mut sk = env("AWS_SECRET_ACCESS_KEY");
        if sk.is_empty() {
            sk = env("AWS_SECRET_KEY");
        }
        if !ak.is_empty() && !sk.is_empty() {
            return Ok(Value {
                access_key_id: ak,
                secret_access_key: sk,
                session_token: env("AWS_SESSION_TOKEN"),
                expiration: None,
                signer_type: SignerType::V4,
            });
        }
        let token_file = env("AWS_WEB_IDENTITY_TOKEN_FILE");
        if !token_file.is_empty() {
            let role_arn = env("AWS_ROLE_ARN");
            if role_arn.is_empty() {
                return Err("role ARN is not set".to_string());
            }
            let endpoint = Self::sts_endpoint(&self.region(profile.as_ref()))?;
            return sts_web_identity(
                &self.client,
                &endpoint,
                &role_arn,
                &env("AWS_ROLE_SESSION_NAME"),
                &token_file,
            )
            .await;
        }
        if let Some(p) = &profile {
            if let Some(v) = self.creds_from_profile(&profile_name, p).await? {
                return Ok(v);
            }
        }
        self.container_or_imds(profile.as_ref()).await
    }
}

#[async_trait]
impl Provider for AwsSdkAuth {
    async fn retrieve(&mut self) -> Result<Value, String> {
        let v = self
            .resolve()
            .await
            .map_err(|e| format!("retrieve AWS SDK credentials: {e}"))?;
        let now = Utc::now();
        self.expires = v.expiration;
        self.refresh_at = v.expiration.map(|e| aws_sdk_refresh_at(e, now));
        self.retrieved = true;
        Ok(v)
    }
    fn is_expired(&self) -> bool {
        // aws.Credentials.Expired() with 5-minute ExpiryWindow.
        if !self.retrieved {
            return true;
        }
        matches!(self.refresh_at, Some(r) if r <= Utc::now())
    }
    fn expire(&mut self) {
        self.retrieved = false;
        self.expires = None;
        self.refresh_at = None;
    }
}

/// minio-go `credentials.Chain`: the first provider returning a non-empty key pair wins.
/// Once a provider has succeeded (or when `AwsSdkAuth` is the sole provider), transient
/// refresh errors are propagated rather than silently falling back to `Anonymous`.
pub(crate) struct Chain {
    providers: Vec<Box<dyn Provider>>,
    curr: Option<usize>,
}

#[async_trait]
impl Provider for Chain {
    async fn retrieve(&mut self) -> Result<Value, String> {
        if let Some(i) = self.curr {
            let creds = self.providers[i].retrieve().await?;
            if !creds.access_key_id.is_empty() && !creds.secret_access_key.is_empty() {
                return Ok(creds);
            }
        }
        let sole_provider = self.providers.len() == 1;
        for (i, p) in self.providers.iter_mut().enumerate() {
            let creds = match p.retrieve().await {
                Ok(v) => v,
                Err(e) => {
                    if sole_provider {
                        return Err(e);
                    }
                    tracing::debug!(err = %e, "S3 credential provider failed");
                    Value::default()
                }
            };
            if creds.access_key_id.is_empty() && creds.secret_access_key.is_empty() {
                continue;
            }
            self.curr = Some(i);
            return Ok(creds);
        }
        Ok(Value::anonymous())
    }
    fn is_expired(&self) -> bool {
        match self.curr {
            Some(i) => self.providers[i].is_expired(),
            None => true,
        }
    }
    fn expire(&mut self) {
        if let Some(i) = self.curr {
            self.providers[i].expire();
        }
    }
}

/// minio-go `credentials.Credentials` (cached value + `expire` / `forceRefresh`).
pub(crate) struct Credentials {
    inner: Mutex<(Chain, Option<Value>)>,
}

impl Credentials {
    pub async fn get(&self) -> Result<Value, String> {
        let mut guard = self.inner.lock().await;
        let (chain, cached) = &mut *guard;
        if cached.is_none() || chain.is_expired() {
            match chain.retrieve().await {
                Ok(v) => *cached = Some(v),
                Err(e) => {
                    if let Some(prev) = cached.as_ref() {
                        let still_valid = prev.signer_type != SignerType::Anonymous
                            && prev.expiration.is_some_and(|exp| exp > Utc::now());
                        if still_valid {
                            tracing::warn!(err = %e, "S3 credential refresh failed; retaining still-unexpired credentials");
                            return Ok(prev.clone());
                        }
                    }
                    return Err(e);
                }
            }
        }
        Ok(cached.clone().unwrap_or_default())
    }

    pub async fn expire(&self) {
        let mut guard = self.inner.lock().await;
        let (chain, cached) = &mut *guard;
        *cached = None;
        chain.expire();
    }
}

/// Settings of thanos' credential chain selection.
pub(crate) struct CredentialsConfig {
    pub aws_sdk_auth: bool,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub region: String,
    pub sts_endpoint: String,
}

/// thanos `NewBucketWithConfig` credential chain.
pub(crate) fn new_credentials(cfg: &CredentialsConfig, env: Env) -> Credentials {
    let iam_client = iam_http_client();
    let providers: Vec<Box<dyn Provider>> = if cfg.aws_sdk_auth {
        vec![Box::new(AwsSdkAuth {
            env,
            client: iam_client,
            region: cfg.region.clone(),
            expires: None,
            refresh_at: None,
            retrieved: false,
            imds_token_disabled: AtomicBool::new(false),
        })]
    } else if !cfg.access_key.is_empty() {
        vec![Box::new(Static(Value {
            access_key_id: cfg.access_key.clone(),
            secret_access_key: cfg.secret_key.clone(),
            session_token: cfg.session_token.clone(),
            expiration: None,
            signer_type: SignerType::V4,
        }))]
    } else {
        vec![
            Box::new(EnvAws {
                env: env.clone(),
                retrieved: false,
            }),
            Box::new(FileAwsCredentials {
                env: env.clone(),
                expiry: Expiry::default(),
                retrieved: false,
            }),
            Box::new(Iam {
                env,
                client: iam_client,
                endpoint: cfg.sts_endpoint.clone(),
                expiry: Expiry::default(),
            }),
        ]
    };
    Credentials {
        inner: Mutex::new((
            Chain {
                providers,
                curr: None,
            },
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&str, &str)]) -> Env {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(move |k: &str| map.get(k).cloned().unwrap_or_default())
    }

    fn cfg() -> CredentialsConfig {
        CredentialsConfig {
            aws_sdk_auth: false,
            access_key: String::new(),
            secret_key: String::new(),
            session_token: String::new(),
            region: String::new(),
            sts_endpoint: String::new(),
        }
    }

    // minio-go env_aws_test.go TestEnvAWSRetrieve.
    #[tokio::test]
    async fn env_aws_retrieve() {
        let mut p = EnvAws {
            env: env_of(&[
                ("AWS_ACCESS_KEY_ID", "access"),
                ("AWS_SECRET_ACCESS_KEY", "secret"),
                ("AWS_SESSION_TOKEN", "token"),
            ]),
            retrieved: false,
        };
        assert!(p.is_expired());
        let v = p.retrieve().await.unwrap();
        assert_eq!(
            (v.access_key_id.as_str(), v.secret_access_key.as_str(), v.session_token.as_str()),
            ("access", "secret", "token")
        );
        assert!(!p.is_expired());
        let mut p = EnvAws {
            env: env_of(&[("AWS_ACCESS_KEY", "access"), ("AWS_SECRET_KEY", "secret")]),
            retrieved: false,
        };
        let v = p.retrieve().await.unwrap();
        assert_eq!((v.access_key_id.as_str(), v.secret_access_key.as_str()), ("access", "secret"));
        assert_eq!(v.signer_type, SignerType::V4);
    }

    // minio-go file_aws_credentials_test.go TestFileAWS (profile selection, missing file).
    #[tokio::test]
    async fn file_aws_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        std::fs::write(
            &path,
            "[default]\naws_access_key_id = accessKey\naws_secret_access_key = secret\naws_session_token = token\n\n[no_token]\naws_access_key_id = accessKey\naws_secret_access_key = secret\n",
        )
        .unwrap();
        let file = path.to_string_lossy().into_owned();
        let mut p = FileAwsCredentials {
            env: env_of(&[("AWS_SHARED_CREDENTIALS_FILE", &file)]),
            expiry: Expiry::default(),
            retrieved: false,
        };
        let v = p.retrieve().await.unwrap();
        assert_eq!(
            (v.access_key_id.as_str(), v.secret_access_key.as_str(), v.session_token.as_str()),
            ("accessKey", "secret", "token")
        );
        assert!(!p.is_expired());
        let mut p = FileAwsCredentials {
            env: env_of(&[("AWS_SHARED_CREDENTIALS_FILE", &file), ("AWS_PROFILE", "no_token")]),
            expiry: Expiry::default(),
            retrieved: false,
        };
        let v = p.retrieve().await.unwrap();
        assert_eq!(v.session_token, "");
        let mut p = FileAwsCredentials {
            env: env_of(&[("AWS_SHARED_CREDENTIALS_FILE", &file), ("AWS_PROFILE", "nope")]),
            expiry: Expiry::default(),
            retrieved: false,
        };
        assert!(p.retrieve().await.is_err());
        assert!(p.is_expired());
    }

    // minio-go chain_test.go: first non-empty provider wins, all-empty -> anonymous.
    #[tokio::test]
    async fn chain_semantics() {
        let mut chain = Chain {
            providers: vec![
                Box::new(Static(Value::default())),
                Box::new(EnvAws {
                    env: env_of(&[("AWS_ACCESS_KEY_ID", "a"), ("AWS_SECRET_ACCESS_KEY", "b")]),
                    retrieved: false,
                }),
            ],
            curr: None,
        };
        assert!(chain.is_expired());
        let v = chain.retrieve().await.unwrap();
        assert_eq!(v.access_key_id, "a");
        assert_eq!(chain.curr, Some(1));
        assert!(!chain.is_expired());

        let mut chain = Chain {
            providers: vec![Box::new(EnvAws {
                env: env_of(&[]),
                retrieved: false,
            })],
            curr: None,
        };
        let v = chain.retrieve().await.unwrap();
        assert_eq!(v.signer_type, SignerType::Anonymous);
        assert!(chain.is_expired());
    }

    /// Fake credential endpoints (ECS / EKS Pod Identity / STS / IMDS) on 127.0.0.1.
    async fn fake_endpoint(
        handler: impl Fn(&str, &str, &HashMap<String, String>, &str) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let log2 = Arc::clone(&log);
        let handler = Arc::new(handler);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                let handler = Arc::clone(&handler);
                let log = Arc::clone(&log2);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
                        let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                        let mut lines = head.lines();
                        let first = lines.next().unwrap_or("").to_string();
                        let headers: HashMap<String, String> = lines
                            .filter_map(|l| l.split_once(':'))
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                            .collect();
                        let len: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
                        if buf.len() < end + 4 + len {
                            continue;
                        }
                        let body = String::from_utf8_lossy(&buf[end + 4..end + 4 + len]).into_owned();
                        buf.drain(..end + 4 + len);
                        let mut parts = first.split_whitespace();
                        let method = parts.next().unwrap_or("").to_string();
                        let path = parts.next().unwrap_or("").to_string();
                        log.lock().unwrap().push(format!(
                            "{method} {path} auth={} ",
                            headers.get("authorization").cloned().unwrap_or_default()
                        ));
                        let (status, resp_body) = handler(&method, &path, &headers, &body);
                        let resp = format!(
                            "HTTP/1.1 {status} X\r\nContent-Length: {}\r\n\r\n{resp_body}",
                            resp_body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                    }
                });
            }
        });
        (format!("http://{addr}"), log)
    }

    const ECS_BODY: &str = r#"{"AccessKeyId":"ecs-ak","SecretAccessKey":"ecs-sk","Token":"ecs-tok","Expiration":"2099-01-01T00:00:00Z"}"#;

    // B12: EKS Pod Identity (FULL_URI + AUTHORIZATION_TOKEN_FILE) must be used, not IMDS.
    #[tokio::test]
    async fn iam_eks_pod_identity() {
        let (base, log) = fake_endpoint(|_, _, _, _| (200, ECS_BODY.to_string())).await;
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("token");
        std::fs::write(&tok, "pod-identity-token").unwrap();
        let creds = new_credentials(
            &cfg(),
            env_of(&[
                ("HOME", dir.path().to_str().unwrap()),
                ("AWS_CONTAINER_CREDENTIALS_FULL_URI", &format!("{base}/v1/credentials")),
                ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tok.to_str().unwrap()),
            ]),
        );
        let v = creds.get().await.unwrap();
        assert_eq!(
            (v.access_key_id.as_str(), v.secret_access_key.as_str(), v.session_token.as_str()),
            ("ecs-ak", "ecs-sk", "ecs-tok")
        );
        let log = log.lock().unwrap().clone();
        assert_eq!(log, vec!["GET /v1/credentials auth=pod-identity-token ".to_string()]);
        // Cached until 80% of the lifetime elapsed.
        creds.get().await.unwrap();
    }

    // ECS relative URI is resolved against the sts_endpoint override when set (minio-go IAM).
    #[tokio::test]
    async fn iam_ecs_relative_uri_with_endpoint_override() {
        let (base, log) = fake_endpoint(|_, _, _, _| (200, ECS_BODY.to_string())).await;
        let mut c = cfg();
        c.sts_endpoint = format!("{base}/override");
        let creds = new_credentials(
            &c,
            env_of(&[("HOME", "/nonexistent"), ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds")]),
        );
        assert_eq!(creds.get().await.unwrap().access_key_id, "ecs-ak");
        assert_eq!(log.lock().unwrap()[0], "GET /override auth= ");
    }

    // Web identity (IRSA) with sts_endpoint.
    #[tokio::test]
    async fn iam_web_identity_with_sts_endpoint() {
        let (base, log) = fake_endpoint(|method, _, headers, body| {
            assert_eq!(method, "POST");
            assert_eq!(headers.get("content-type").unwrap(), "application/x-www-form-urlencoded");
            assert!(body.starts_with("Action=AssumeRoleWithWebIdentity&RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr&RoleSessionName=sess&Version=2011-06-15&WebIdentityToken=jwt"), "{body}");
            (200, r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>sts-ak</AccessKeyId><SecretAccessKey>sts-sk</SecretAccessKey><SessionToken>sts-tok</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#.to_string())
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("jwt");
        std::fs::write(&tok, "jwt").unwrap();
        let mut c = cfg();
        c.sts_endpoint = format!("{base}/sts");
        let creds = new_credentials(
            &c,
            env_of(&[
                ("HOME", dir.path().to_str().unwrap()),
                ("AWS_WEB_IDENTITY_TOKEN_FILE", tok.to_str().unwrap()),
                ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
                ("AWS_ROLE_SESSION_NAME", "sess"),
            ]),
        );
        let v = creds.get().await.unwrap();
        assert_eq!((v.access_key_id.as_str(), v.session_token.as_str()), ("sts-ak", "sts-tok"));
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    // IMDSv2 flow against sts_endpoint override (no other source configured).
    #[tokio::test]
    async fn iam_imds_v2() {
        let (base, log) = fake_endpoint(|method, path, headers, _| match (method, path) {
            ("PUT", "/latest/api/token") => {
                assert_eq!(headers.get("x-aws-ec2-metadata-token-ttl-seconds").unwrap(), "21600");
                (200, "imds-token".into())
            }
            ("GET", "/latest/meta-data/iam/security-credentials/") => (200, "my-role\n".into()),
            ("GET", "/latest/meta-data/iam/security-credentials/my-role") => {
                assert_eq!(headers.get("x-aws-ec2-metadata-token").unwrap(), "imds-token");
                (200, r#"{"Code":"Success","AccessKeyId":"imds-ak","SecretAccessKey":"imds-sk","Token":"t","Expiration":"2099-01-01T00:00:00Z"}"#.into())
            }
            _ => (404, String::new()),
        })
        .await;
        let mut c = cfg();
        c.sts_endpoint = base;
        let creds = new_credentials(&c, env_of(&[("HOME", "/nonexistent")]));
        assert_eq!(creds.get().await.unwrap().access_key_id, "imds-ak");
        assert_eq!(log.lock().unwrap().len(), 3);
    }

    // Static credentials take precedence over everything (thanos access_key branch).
    #[tokio::test]
    async fn static_credentials_ignore_env() {
        let mut c = cfg();
        c.access_key = "cfg-ak".into();
        c.secret_key = "cfg-sk".into();
        let creds = new_credentials(
            &c,
            env_of(&[("AWS_SESSION_TOKEN", "ambient"), ("AWS_ACCESS_KEY_ID", "env")]),
        );
        let v = creds.get().await.unwrap();
        assert_eq!((v.access_key_id.as_str(), v.session_token.as_str()), ("cfg-ak", ""));
    }

    // AWS SDK auth: env keys, explicit profile, unsupported profile features.
    #[tokio::test]
    async fn aws_sdk_auth_chain() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".aws")).unwrap();
        std::fs::write(
            dir.path().join(".aws/credentials"),
            "[default]\naws_access_key_id=def-ak\naws_secret_access_key=def-sk\n[p1]\naws_access_key_id=p1-ak\naws_secret_access_key=p1-sk\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join(".aws/config"),
            "[profile sso]\nsso_start_url = https://x\n",
        )
        .unwrap();
        let home = dir.path().to_str().unwrap().to_string();
        let mut c = cfg();
        c.aws_sdk_auth = true;
        let get = |env: Env| {
            let c = CredentialsConfig { aws_sdk_auth: true, region: "eu-west-1".into(), ..cfg() };
            async move {
                let mut p = AwsSdkAuth {
                    env,
                    client: iam_http_client(),
                    region: c.region.clone(),
                    expires: None,
                    refresh_at: None,
                    retrieved: false,
                    imds_token_disabled: AtomicBool::new(false),
                };
                p.retrieve().await
            }
        };
        let v = get(env_of(&[("HOME", &home)])).await.unwrap();
        assert_eq!(v.access_key_id, "def-ak");
        let v = get(env_of(&[("HOME", &home), ("AWS_ACCESS_KEY_ID", "e"), ("AWS_SECRET_ACCESS_KEY", "s")]))
            .await
            .unwrap();
        assert_eq!(v.access_key_id, "e");
        let v = get(env_of(&[("HOME", &home), ("AWS_PROFILE", "p1"), ("AWS_ACCESS_KEY_ID", "e")]))
            .await
            .unwrap();
        assert_eq!(v.access_key_id, "p1-ak");
        let e = get(env_of(&[("HOME", &home), ("AWS_PROFILE", "sso")])).await.unwrap_err();
        assert!(e.contains("not supported by mimir-rs"), "{e}");
        let _ = c;
    }

    #[tokio::test]
    async fn aws_sdk_auth_5min_expiry_window_and_iso_sts() {
        assert_eq!(
            AwsSdkAuth::sts_endpoint("us-iso-east-1").unwrap(),
            "https://sts.us-iso-east-1.c2s.ic.gov"
        );
        assert_eq!(
            AwsSdkAuth::sts_endpoint("us-isob-east-1").unwrap(),
            "https://sts.us-isob-east-1.sc2s.sgov.gov"
        );
        let now = Utc::now();
        // 1h token -> refresh_at is 5m before expiration.
        let exp_1h = now + chrono::Duration::hours(1);
        let refresh = aws_sdk_refresh_at(exp_1h, now);
        let diff = (exp_1h - refresh).num_seconds();
        assert!((299..=301).contains(&diff), "expected ~300s window, got {diff}");

        // Pod Identity returning a 4-minute TTL (< 5m) -> 80% window, NOT immediately expired,
        // and token file trailing newline is trimmed.
        let exp_4m = (Utc::now() + chrono::Duration::minutes(4)).to_rfc3339();
        let body = format!(
            r#"{{"AccessKeyId":"ak1","SecretAccessKey":"sk1","Token":"tok1","Expiration":"{exp_4m}"}}"#
        );
        let (base, log) = fake_endpoint(move |_, _, _, _| (200, body.clone())).await;
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("pod-tok");
        std::fs::write(&tok, "pod-token-with-nl\n").unwrap();
        let creds = new_credentials(
            &CredentialsConfig { aws_sdk_auth: true, ..cfg() },
            env_of(&[
                ("HOME", "/nonexistent"),
                ("AWS_CONTAINER_CREDENTIALS_FULL_URI", &format!("{base}/creds")),
                ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tok.to_str().unwrap()),
            ]),
        );
        let v = creds.get().await.unwrap();
        assert_eq!(v.access_key_id, "ak1");
        // Second call uses cache (not expired immediately despite TTL < 5m).
        let _ = creds.get().await.unwrap();
        assert_eq!(log.lock().unwrap().len(), 1);
        assert_eq!(log.lock().unwrap()[0], "GET /creds auth=pod-token-with-nl ");

        // Force expire() invalidates cache.
        creds.expire().await;
        let _ = creds.get().await.unwrap();
        assert_eq!(log.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn chain_does_not_fallback_to_anonymous_on_refresh_error() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let (base, log) = fake_endpoint(move |_, _, _, _| {
            let n = calls2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                // Already expired in 1s? Use past expiration so cached is expired on next check
                (200, r#"{"AccessKeyId":"ak1","SecretAccessKey":"sk1","Token":"t1","Expiration":"2000-01-01T00:00:00Z"}"#.to_string())
            } else {
                (400, "bad".to_string())
            }
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("pod-tok");
        std::fs::write(&tok, "pod-tok\n").unwrap();
        let creds = new_credentials(
            &cfg(),
            env_of(&[
                ("HOME", "/nonexistent"),
                ("AWS_CONTAINER_CREDENTIALS_FULL_URI", &format!("{base}/creds")),
                ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tok.to_str().unwrap()),
            ]),
        );
        let v = creds.get().await.unwrap();
        assert_eq!(v.access_key_id, "ak1");
        assert_eq!(log.lock().unwrap()[0], "GET /creds auth=pod-tok ");
        // Next get() sees expired cached token and fails refresh -> MUST error, NOT return Anonymous!
        let err = creds.get().await.unwrap_err();
        assert!(err.contains("400"), "{err}");
    }

    #[test]
    fn ini_parsing() {
        let ini = Ini::parse("; c\n[default]\na = 1 ; inline\nb=\"quoted ; x\"\n[profile x]\nc: 3\n");
        let d = ini.section("default").unwrap();
        assert_eq!(d.get("a").unwrap(), "1");
        assert_eq!(d.get("b").unwrap(), "quoted ; x");
        assert_eq!(ini.section("profile x").unwrap().get("c").unwrap(), "3");
        assert_eq!(ini.section("nope").unwrap_err(), "section \"nope\" does not exist");
    }
}
