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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use super::transport::HttpTransport;

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
}

/// minio-go `credentials.Expiry` (`DefaultExpiryWindow` = -1: refresh after 80% of the
/// remaining lifetime).
#[derive(Clone, Debug, Default)]
struct Expiry {
    expiration: Option<DateTime<Utc>>,
}

impl Expiry {
    fn set_expiration(&mut self, expiration: DateTime<Utc>) {
        let now = Utc::now();
        let expire_in = expiration - now;
        let cut_ns = (expire_in.num_nanoseconds().unwrap_or(i64::MAX) as f64 * (1.0 - 0.8)) as i64;
        self.expiration = Some(expiration - chrono::Duration::nanoseconds(cut_ns));
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

/// `credential_process` output (`externalProcessCredentials`).
async fn run_credential_process(command: &str) -> Result<Value, String> {
    let args: Vec<&str> = command.split_whitespace().collect();
    if args.len() <= 1 {
        return Err("invalid credential process args".to_string());
    }
    let out = tokio::process::Command::new(args[0])
        .args(&args[1..])
        .output()
        .await
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

/// `getEcsTaskCredentials`.
async fn get_ecs_task_credentials(
    client: &reqwest::Client,
    endpoint: &str,
    token: &str,
) -> Result<RoleCreds, String> {
    let mut req = client.get(endpoint);
    if !token.is_empty() {
        req = req.header("Authorization", token);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.status() != reqwest::StatusCode::OK {
        return Err(status_text(resp.status()));
    }
    let body = resp.bytes().await.map_err(|e| e.to_string())?;
    parse_role_creds(&body)
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
    let mut req = client.get(base.clone());
    if !token.is_empty() {
        req = req.header("X-aws-ec2-metadata-token", &token);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.status() != reqwest::StatusCode::OK {
        return Err(status_text(resp.status()));
    }
    let list = resp.text().await.map_err(|e| e.to_string())?;
    let roles: Vec<&str> = list.lines().collect();
    let Some(role) = roles.first() else {
        return Err("No IAM roles attached to this EC2 service".to_string());
    };
    let mut u = base;
    u.set_path(&format!("{}{}", DEFAULT_IAM_SECURITY_CREDS_PATH.trim_end_matches('/'), format_args!("/{role}")));
    let mut req = client.get(u);
    if !token.is_empty() {
        req = req.header("X-aws-ec2-metadata-token", &token);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.status() != reqwest::StatusCode::OK {
        return Err(status_text(resp.status()));
    }
    let body = resp.bytes().await.map_err(|e| e.to_string())?;
    let creds = parse_role_creds(&body)?;
    if creds.code != "Success" {
        return Err(creds.message);
    }
    Ok(creds)
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
    let resp = client
        .post(endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    let fields = xml_leaf_fields(&bytes);
    if status != reqwest::StatusCode::OK {
        let code = fields.get("Code").cloned().unwrap_or_default();
        let message = fields.get("Message").cloned().unwrap_or_default();
        if message.is_empty() {
            return Err(format!("Error response code {code}."));
        }
        return Err(message);
    }
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
                    if region.starts_with("cn-") {
                        format!("https://sts.{region}.amazonaws.com.cn")
                    } else {
                        format!("https://sts.{region}.amazonaws.com")
                    }
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
            get_ecs_task_credentials(&self.client, &full_uri, &tok).await?
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
pub(crate) struct AwsSdkAuth {
    env: Env,
    client: reqwest::Client,
    region: String,
    expires: Option<DateTime<Utc>>,
    retrieved: bool,
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
        if region.starts_with("cn-") {
            Ok(format!("https://sts.{region}.amazonaws.com.cn"))
        } else {
            Ok(format!("https://sts.{region}.amazonaws.com"))
        }
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

    async fn container_or_imds(&self) -> Result<Value, String> {
        let env = &self.env;
        let relative = env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        let full = env("AWS_CONTAINER_CREDENTIALS_FULL_URI");
        let rc = if !relative.is_empty() || !full.is_empty() {
            let endpoint = if !relative.is_empty() {
                format!("{DEFAULT_ECS_ROLE_ENDPOINT}{relative}")
            } else {
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
            let endpoint = env("AWS_EC2_METADATA_SERVICE_ENDPOINT");
            get_imds_credentials(&self.client, endpoint.trim_end_matches('/')).await?
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
        let explicit_profile = env("AWS_PROFILE");
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
        let env_has_keys = !ak.is_empty() && !sk.is_empty();
        if !explicit_profile.is_empty() && !env_has_keys {
            if let Some(p) = &profile {
                if let Some(v) = self.creds_from_profile(&profile_name, p).await? {
                    return Ok(v);
                }
            }
            return self.container_or_imds().await;
        }
        if env_has_keys {
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
            let endpoint = Self::sts_endpoint(&self.region(profile.as_ref()))?;
            return sts_web_identity(
                &self.client,
                &endpoint,
                &env("AWS_ROLE_ARN"),
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
        self.container_or_imds().await
    }
}

#[async_trait]
impl Provider for AwsSdkAuth {
    async fn retrieve(&mut self) -> Result<Value, String> {
        let v = self
            .resolve()
            .await
            .map_err(|e| format!("retrieve AWS SDK credentials: {e}"))?;
        self.expires = v.expiration;
        self.retrieved = true;
        Ok(v)
    }
    fn is_expired(&self) -> bool {
        // aws.Credentials.Expired(): only expiring credentials expire.
        if !self.retrieved {
            return true;
        }
        matches!(self.expires, Some(e) if e <= Utc::now())
    }
}

/// minio-go `credentials.Chain`: the first provider returning a non-empty key pair wins;
/// errors are ignored and an empty chain yields anonymous credentials.
pub(crate) struct Chain {
    providers: Vec<Box<dyn Provider>>,
    curr: Option<usize>,
}

#[async_trait]
impl Provider for Chain {
    async fn retrieve(&mut self) -> Result<Value, String> {
        for (i, p) in self.providers.iter_mut().enumerate() {
            let creds = match p.retrieve().await {
                Ok(v) => v,
                Err(e) => {
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
}

/// minio-go `credentials.Credentials` (cached value + `forceRefresh`).
pub(crate) struct Credentials {
    inner: Mutex<(Chain, Option<Value>)>,
}

impl Credentials {
    pub async fn get(&self) -> Result<Value, String> {
        let mut guard = self.inner.lock().await;
        let (chain, cached) = &mut *guard;
        if cached.is_none() || chain.is_expired() {
            let v = chain.retrieve().await?;
            *cached = Some(v);
        }
        Ok(cached.clone().unwrap_or_default())
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
    let iam_client = HttpTransport::go_default().client;
    let providers: Vec<Box<dyn Provider>> = if cfg.aws_sdk_auth {
        vec![Box::new(AwsSdkAuth {
            env,
            client: iam_client,
            region: cfg.region.clone(),
            expires: None,
            retrieved: false,
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
                    client: reqwest::Client::new(),
                    region: c.region.clone(),
                    expires: None,
                    retrieved: false,
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
