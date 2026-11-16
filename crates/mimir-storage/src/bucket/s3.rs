//! S3 backend for the blocks-storage bucket client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/s3/bucket_client.go
//! - github.com/thanos-io/objstore/providers/s3/s3.go (`NewBucketWithConfig`, `validate`)
//! - github.com/minio/minio-go/v7/api.go (`privateNew` region selection, `newRequest`
//!   location resolution, `makeTargetURL`, `isVirtualHostStyleRequest`)
//! - github.com/minio/minio-go/v7/bucket-cache.go (`getBucketLocation`,
//!   `getBucketLocationRequest`, `processBucketLocationResponse`)
//! - github.com/minio/minio-go/v7/api-error-response.go (`httpRespToErrorResponse`)
//!
//! Region handling follows minio-go exactly: an explicit `region` wins; otherwise the region is
//! derived from an AWS endpoint hostname (`s3.<region>.amazonaws.com`, dualstack, FIPS, China,
//! PrivateLink, ...); otherwise the bucket location is discovered once with a `GetBucketLocation`
//! request (signed for `us-east-1`, path style) and cached. For AWS endpoints the request host is
//! the regional (dual-stack by default) endpoint of the bucket location.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use mimir_config::{S3BucketLookupType, S3Config};
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use tokio::sync::OnceCell;

use super::s3utils::{self, EndpointUrl};
use super::BucketError;

const STORE: &str = "S3";

/// Resolved addressing for one S3 bucket (what minio-go computes per request).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct S3Addressing {
    /// Signing region (bucket location).
    pub region: String,
    /// Base URL handed to `object_store` (`scheme://[bucket.]host`).
    pub endpoint: String,
    pub virtual_hosted: bool,
}

/// Static (configuration-derived) part of the S3 client: everything except the bucket location.
#[derive(Clone, Debug)]
struct S3Target {
    cfg: S3Config,
    endpoint: EndpointUrl,
    /// minio-go `Client.region`: configured region, else derived from the endpoint hostname.
    region: String,
    /// minio-go `s3DualstackEnabled` (only meaningful for AWS endpoints).
    dualstack: bool,
}

impl S3Target {
    fn new(cfg: &S3Config) -> Result<Self, BucketError> {
        validate(cfg)?;
        let endpoint = s3utils::get_endpoint_url(&cfg.endpoint, !cfg.insecure)
            .map_err(|e| BucketError::Config(format!("initialize s3 client: {e}")))?;
        let region = if cfg.region.is_empty() {
            s3utils::get_region_from_url(&endpoint.hostname)
        } else {
            cfg.region.clone()
        };
        // minio.New: AWS endpoints are dual-stack by default; thanos calls
        // SetS3EnableDualstack(false) when DisableDualstack (= !dualstack_enabled).
        let dualstack = s3utils::is_amazon_endpoint(&endpoint.hostname) && cfg.dualstack_enabled;
        Ok(Self {
            cfg: cfg.clone(),
            endpoint,
            region,
            dualstack,
        })
    }

    /// `isVirtualHostStyleRequest(endpointURL, bucketName)`.
    fn is_virtual_host_style(&self) -> bool {
        if self.cfg.bucket_name.is_empty() {
            return false;
        }
        match self.cfg.bucket_lookup_type {
            S3BucketLookupType::VirtualHost => true,
            S3BucketLookupType::Path => false,
            S3BucketLookupType::Auto => {
                s3utils::is_virtual_host_supported(&self.endpoint, &self.cfg.bucket_name)
            }
        }
    }

    /// Request addressing for `bucket_location` (`makeTargetURL`).
    fn addressing(&self, bucket_location: &str) -> S3Addressing {
        let host = s3utils::target_host(&self.endpoint, bucket_location, self.dualstack);
        let virtual_hosted = self.is_virtual_host_style();
        let endpoint = if virtual_hosted {
            format!("{}://{}.{}", self.endpoint.scheme, self.cfg.bucket_name, host)
        } else {
            format!("{}://{}", self.endpoint.scheme, host)
        };
        S3Addressing {
            region: bucket_location.to_string(),
            endpoint,
            virtual_hosted,
        }
    }

    fn builder(&self, addressing: &S3Addressing) -> AmazonS3Builder {
        let s3 = &self.cfg;
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&s3.bucket_name)
            .with_region(&addressing.region)
            .with_endpoint(&addressing.endpoint)
            .with_virtual_hosted_style_request(addressing.virtual_hosted);
        if s3.insecure {
            builder = builder.with_allow_http(true);
        }
        if !s3.access_key_id.is_empty() {
            builder = builder.with_access_key_id(&s3.access_key_id);
        }
        if !s3.secret_access_key.is_empty() {
            builder = builder.with_secret_access_key(&s3.secret_access_key);
        }
        if !s3.session_token.is_empty() {
            builder = builder.with_token(&s3.session_token);
        }
        builder
    }

    fn build(&self, bucket_location: &str) -> Result<AmazonS3, object_store::Error> {
        self.builder(&self.addressing(bucket_location)).build()
    }

    /// minio-go `getBucketLocationRequest` URL.
    fn bucket_location_url(&self) -> String {
        let host = s3utils::strip_default_port(self.endpoint.scheme, &self.endpoint.host);
        let bucket = &self.cfg.bucket_name;
        if self.is_virtual_host_style() {
            format!("{}://{}.{}/?location", self.endpoint.scheme, bucket, host)
        } else {
            format!("{}://{}/{}/?location=", self.endpoint.scheme, host, bucket)
        }
    }

    /// minio-go `getBucketLocation` (uncached part): `GET ?location`, signed for `us-east-1`.
    async fn fetch_bucket_location(&self) -> Result<String, object_store::Error> {
        let generic = |source: Box<dyn std::error::Error + Send + Sync>| {
            object_store::Error::Generic {
                store: STORE,
                source,
            }
        };
        // The credential chain of the regular client, built for the signing region minio-go
        // uses for this request.
        let signer_store = self.build("us-east-1")?;
        let credential = signer_store.credentials().get_credential().await?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| generic(Box::new(e)))?;
        let mut request = client
            .get(self.bucket_location_url())
            .build()
            .map_err(|e| generic(Box::new(e)))?;
        // X-Amz-Content-Sha256: UNSIGNED-PAYLOAD over https, the empty-body SHA-256 otherwise.
        let secure = self.endpoint.scheme == "https";
        AwsAuthorizer::new(&credential, "s3", "us-east-1")
            .with_sign_payload(!secure)
            .authorize(&mut request, None);
        let response = client
            .execute(request)
            .await
            .map_err(|e| generic(Box::new(e)))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.map_err(|e| generic(Box::new(e)))?;
        process_bucket_location_response(status, &headers, &body, &self.cfg.bucket_name)
            .map_err(|e| generic(e))
    }
}

/// minio-go `ErrorResponse` (subset needed by the location lookup).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct S3ErrorResponse {
    pub code: String,
    pub message: String,
    pub bucket_name: String,
    pub region: String,
    pub server: String,
    pub status_code: u16,
}

impl fmt::Display for S3ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "Error response code {}.", self.code)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl std::error::Error for S3ErrorResponse {}

/// Extracts the text of the top-level elements of an S3 XML document (`<Root><K>v</K>...`).
/// Returns `None` if the document is not well-formed XML with a root element.
fn xml_root_fields(body: &[u8]) -> Option<(String, Vec<(String, String)>)> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut root: Option<String> = None;
    let mut root_text = String::new();
    let mut depth = 0usize;
    let mut fields = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                depth += 1;
                if depth == 1 {
                    root = Some(name);
                } else if depth == 2 {
                    fields.push((name, String::new()));
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if depth == 0 {
                    return Some((name, Vec::new()));
                }
                if depth == 1 {
                    fields.push((name, String::new()));
                }
            }
            Ok(Event::Text(t)) => {
                let text = t.unescape().ok()?.into_owned();
                if depth == 1 {
                    root_text.push_str(&text);
                } else if depth == 2 {
                    if let Some(last) = fields.last_mut() {
                        last.1.push_str(&text);
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = String::from_utf8_lossy(&t).into_owned();
                if depth == 1 {
                    root_text.push_str(&text);
                } else if depth == 2 {
                    if let Some(last) = fields.last_mut() {
                        last.1.push_str(&text);
                    }
                }
            }
            Ok(Event::End(_)) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let root = root?;
                    if fields.is_empty() {
                        return Some((root, vec![(String::new(), root_text)]));
                    }
                    return Some((root, fields));
                }
            }
            Ok(Event::Eof) => return None,
            Ok(_) => {}
            Err(_) => return None,
        }
        buf.clear();
    }
}

/// minio-go `httpRespToErrorResponse` for a non-2xx response.
pub(crate) fn http_resp_to_error_response(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    bucket_name: &str,
    object_name: &str,
) -> S3ErrorResponse {
    let header = |k: &str| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };
    let mut resp = S3ErrorResponse {
        status_code: status,
        server: header("Server"),
        ..Default::default()
    };
    let parsed = xml_root_fields(body).filter(|(root, _)| root == "Error");
    match parsed {
        Some((_, fields)) => {
            for (k, v) in fields {
                match k.as_str() {
                    "Code" => resp.code = v,
                    "Message" => resp.message = v,
                    "BucketName" => resp.bucket_name = v,
                    "Region" => resp.region = v,
                    _ => {}
                }
            }
        }
        None => {
            let reason = reqwest::StatusCode::from_u16(status)
                .ok()
                .and_then(|s| s.canonical_reason())
                .unwrap_or("");
            let status_text = format!("{status} {reason}");
            resp = match status {
                404 if object_name.is_empty() => S3ErrorResponse {
                    status_code: status,
                    code: "NoSuchBucket".into(),
                    message: "The specified bucket does not exist.".into(),
                    bucket_name: bucket_name.into(),
                    ..Default::default()
                },
                404 => S3ErrorResponse {
                    status_code: status,
                    code: "NoSuchKey".into(),
                    message: "The specified key does not exist.".into(),
                    bucket_name: bucket_name.into(),
                    ..Default::default()
                },
                403 => S3ErrorResponse {
                    status_code: status,
                    code: "AccessDenied".into(),
                    message: "Access Denied.".into(),
                    bucket_name: bucket_name.into(),
                    ..Default::default()
                },
                409 => S3ErrorResponse {
                    status_code: status,
                    code: "Conflict".into(),
                    message: "Bucket not empty.".into(),
                    bucket_name: bucket_name.into(),
                    ..Default::default()
                },
                412 => S3ErrorResponse {
                    status_code: status,
                    code: "PreconditionFailed".into(),
                    message: "Pre condition failed.".into(),
                    bucket_name: bucket_name.into(),
                    ..Default::default()
                },
                _ => {
                    let mut msg = status_text.clone();
                    if !body.is_empty() {
                        msg = String::from_utf8_lossy(body).into_owned();
                        if msg.len() > 1024 {
                            let mut cut = 1024;
                            while !msg.is_char_boundary(cut) {
                                cut -= 1;
                            }
                            msg = format!("{}...", &msg[..cut]);
                        }
                    }
                    S3ErrorResponse {
                        status_code: status,
                        code: status_text,
                        message: msg,
                        bucket_name: bucket_name.into(),
                        ..Default::default()
                    }
                }
            };
        }
    }
    let code = header("x-minio-error-code");
    if !code.is_empty() {
        resp.code = code;
    }
    let desc = header("x-minio-error-desc");
    if !desc.is_empty() {
        resp.message = desc.trim_matches('"').to_string();
    }
    if resp.region.is_empty() {
        resp.region = header("x-amz-bucket-region");
    }
    if resp.code == "InvalidRegion" && !resp.region.is_empty() {
        resp.message = format!(
            "Region does not match, expecting region \u{2018}{}\u{2019}.",
            resp.region
        );
    }
    resp
}

/// minio-go `processBucketLocationResponse`.
pub(crate) fn process_bucket_location_response(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    bucket_name: &str,
) -> Result<String, Box<S3ErrorResponse>> {
    if status != 200 {
        let err = http_resp_to_error_response(status, headers, body, bucket_name, "");
        match err.code.as_str() {
            "NotImplemented" => match err.server.as_str() {
                "AmazonSnowball" => return Ok("snowball".to_string()),
                "cloudflare" => return Ok("us-east-1".to_string()),
                _ => {}
            },
            "AuthorizationHeaderMalformed" | "InvalidRegion" | "AccessDenied" => {
                if err.region.is_empty() {
                    return Ok("us-east-1".to_string());
                }
                return Ok(err.region);
            }
            _ => {}
        }
        return Err(Box::new(err));
    }
    let location = match xml_root_fields(body) {
        Some((_, fields)) => fields
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<_>>()
            .concat(),
        None => {
            return Err(Box::new(S3ErrorResponse {
                status_code: status,
                code: "XMLDecodeError".to_string(),
                message: "unable to decode GetBucketLocation response".to_string(),
                bucket_name: bucket_name.to_string(),
                ..Default::default()
            }))
        }
    };
    Ok(match location.as_str() {
        "" => "us-east-1".to_string(),
        "EU" => "eu-west-1".to_string(),
        _ => location,
    })
}

/// thanos `s3.validate` (the checks that apply to the Mimir-provided config).
fn validate(cfg: &S3Config) -> Result<(), BucketError> {
    if cfg.endpoint.is_empty() {
        return Err(BucketError::Config("no s3 endpoint in config file".to_string()));
    }
    if cfg.native_aws_auth_enabled && !cfg.access_key_id.is_empty() {
        return Err(BucketError::Config(
            "aws_sdk_auth and access_key are mutually exclusive configurations".to_string(),
        ));
    }
    if cfg.access_key_id.is_empty() && !cfg.secret_access_key.is_empty() {
        return Err(BucketError::Config("no s3 access_key specified while secret_key is present in config file; either both should be present in config or envvars/IAM should be used.".to_string()));
    }
    if !cfg.access_key_id.is_empty() && cfg.secret_access_key.is_empty() {
        return Err(BucketError::Config("no s3 secret_key specified while access_key is present in config file; either both should be present in config or envvars/IAM should be used.".to_string()));
    }
    if cfg.sse.sse_type == "SSE-KMS" && cfg.sse.kms_key_id.is_empty() {
        return Err(BucketError::Config(
            "kms_key_id must be set if sse_config.type is set to 'SSE-KMS'".to_string(),
        ));
    }
    Ok(())
}

/// Builds the S3 object store for `cfg` (Go `s3.NewBucketClient`).
pub fn new_store(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let target = S3Target::new(cfg)?;
    if !target.region.is_empty() {
        // Region known up front (configured or derived from the endpoint hostname).
        let store = target.build(&target.region.clone())?;
        return Ok(Arc::new(store));
    }
    // Validate the builder inputs eagerly so configuration errors still fail construction.
    target.build("us-east-1")?;
    Ok(Arc::new(LocationResolvingS3 {
        target,
        store: OnceCell::new(),
    }))
}

/// S3 store whose bucket location is discovered on first use (minio-go `getBucketLocation` with
/// `bucketLocCache`): a failed lookup fails that request and is retried by the next one.
struct LocationResolvingS3 {
    target: S3Target,
    store: OnceCell<AmazonS3>,
}

impl fmt::Debug for LocationResolvingS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocationResolvingS3")
            .field("bucket", &self.target.cfg.bucket_name)
            .field("endpoint", &self.target.endpoint)
            .field("location", &self.store.get().map(|_| "resolved"))
            .finish()
    }
}

impl fmt::Display for LocationResolvingS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocationResolvingS3({})", self.target.cfg.bucket_name)
    }
}

impl LocationResolvingS3 {
    async fn store(&self) -> object_store::Result<&AmazonS3> {
        self.store
            .get_or_try_init(|| async {
                let location = self.target.fetch_bucket_location().await?;
                tracing::debug!(bucket = %self.target.cfg.bucket_name, location = %location, "resolved S3 bucket location");
                self.target.build(&location)
            })
            .await
    }
}

#[async_trait]
impl ObjectStore for LocationResolvingS3 {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store().await?.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store().await?.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.store().await?.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        self.store().await?.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.store().await?.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.store().await?.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            match self.store().await {
                Ok(store) => store.list(prefix.as_ref()),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store().await?.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store().await?.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store().await?.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    fn cfg(endpoint: &str, region: &str) -> S3Config {
        S3Config {
            endpoint: endpoint.to_string(),
            region: region.to_string(),
            bucket_name: "mimir-blocks".to_string(),
            access_key_id: "AKID".to_string(),
            secret_access_key: "SECRET".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn region_from_endpoint_and_regional_dualstack_host() {
        // jsonnet default: endpoint s3.dualstack.<region>.amazonaws.com, no region.
        let t = S3Target::new(&cfg("s3.dualstack.eu-west-1.amazonaws.com", "")).unwrap();
        assert_eq!(t.region, "eu-west-1");
        assert_eq!(
            t.addressing(&t.region),
            S3Addressing {
                region: "eu-west-1".into(),
                endpoint: "https://mimir-blocks.s3.dualstack.eu-west-1.amazonaws.com".into(),
                virtual_hosted: true,
            }
        );

        // Plain regional endpoint: minio rewrites to the dual-stack endpoint by default...
        let t = S3Target::new(&cfg("s3.eu-central-1.amazonaws.com", "")).unwrap();
        assert_eq!(t.region, "eu-central-1");
        assert_eq!(
            t.addressing(&t.region).endpoint,
            "https://mimir-blocks.s3.dualstack.eu-central-1.amazonaws.com"
        );
        // ...unless dualstack is disabled.
        let mut c = cfg("s3.eu-central-1.amazonaws.com", "");
        c.dualstack_enabled = false;
        let t = S3Target::new(&c).unwrap();
        assert_eq!(
            t.addressing(&t.region).endpoint,
            "https://mimir-blocks.s3.eu-central-1.amazonaws.com"
        );

        // China partition.
        let t = S3Target::new(&cfg("s3.cn-north-1.amazonaws.com.cn", "")).unwrap();
        assert_eq!(t.region, "cn-north-1");
        assert_eq!(
            t.addressing(&t.region).endpoint,
            "https://mimir-blocks.s3.dualstack.cn-north-1.amazonaws.com.cn"
        );
    }

    #[test]
    fn region_unknown_requires_location_lookup() {
        // The global endpoint carries no region: minio-go asks the bucket for its location.
        let t = S3Target::new(&cfg("s3.amazonaws.com", "")).unwrap();
        assert_eq!(t.region, "");
        assert_eq!(
            t.bucket_location_url(),
            "https://mimir-blocks.s3.amazonaws.com/?location"
        );
        // Once resolved, requests go to the regional (dual-stack) endpoint.
        assert_eq!(
            t.addressing("ap-southeast-2").endpoint,
            "https://mimir-blocks.s3.dualstack.ap-southeast-2.amazonaws.com"
        );

        // Non-AWS endpoint without region: path-style location request, host unchanged.
        let mut c = cfg("minio.local:9000", "");
        c.insecure = true;
        let t = S3Target::new(&c).unwrap();
        assert_eq!(t.region, "");
        assert_eq!(t.bucket_location_url(), "http://minio.local:9000/mimir-blocks/?location=");
        assert_eq!(
            t.addressing("eu-west-1"),
            S3Addressing {
                region: "eu-west-1".into(),
                endpoint: "http://minio.local:9000".into(),
                virtual_hosted: false,
            }
        );
    }

    #[test]
    fn explicit_region_wins_and_lookup_types() {
        let mut c = cfg("minio.local:443", "us-west-2");
        c.bucket_lookup_type = S3BucketLookupType::VirtualHost;
        let t = S3Target::new(&c).unwrap();
        assert_eq!(t.region, "us-west-2");
        assert_eq!(
            t.addressing(&t.region).endpoint,
            "https://mimir-blocks.minio.local"
        );
        assert_eq!(t.bucket_location_url(), "https://mimir-blocks.minio.local/?location");
        c.bucket_lookup_type = S3BucketLookupType::Path;
        let t = S3Target::new(&c).unwrap();
        assert_eq!(t.addressing(&t.region).endpoint, "https://minio.local");
        // AWS endpoint with path lookup still uses the regional host.
        let mut c = cfg("s3.amazonaws.com", "eu-west-1");
        c.bucket_lookup_type = S3BucketLookupType::Path;
        let t = S3Target::new(&c).unwrap();
        assert_eq!(
            t.addressing(&t.region),
            S3Addressing {
                region: "eu-west-1".into(),
                endpoint: "https://s3.dualstack.eu-west-1.amazonaws.com".into(),
                virtual_hosted: false,
            }
        );
        // Bucket names with dots are not virtual-hosted over https in auto mode.
        let mut c = cfg("s3.eu-west-1.amazonaws.com", "");
        c.bucket_name = "my.bucket".into();
        let t = S3Target::new(&c).unwrap();
        assert!(!t.addressing(&t.region).virtual_hosted);
    }

    // thanos objstore providers/s3/s3_test.go validate cases (subset reachable from Mimir).
    #[test]
    fn thanos_validate() {
        let err = |c: S3Config| match S3Target::new(&c) {
            Err(BucketError::Config(e)) => e,
            other => panic!("expected config error, got {other:?}"),
        };
        assert_eq!(err(cfg("", "")), "no s3 endpoint in config file");
        let mut c = cfg("s3.amazonaws.com", "");
        c.native_aws_auth_enabled = true;
        assert_eq!(err(c), "aws_sdk_auth and access_key are mutually exclusive configurations");
        let mut c = cfg("s3.amazonaws.com", "");
        c.access_key_id.clear();
        assert!(err(c).starts_with("no s3 access_key specified while secret_key is present"));
        let mut c = cfg("s3.amazonaws.com", "");
        c.secret_access_key.clear();
        assert!(err(c).starts_with("no s3 secret_key specified while access_key is present"));
        let mut c = cfg("s3.amazonaws.com", "");
        c.sse.sse_type = "SSE-KMS".into();
        assert_eq!(err(c), "kms_key_id must be set if sse_config.type is set to 'SSE-KMS'");
        assert_eq!(
            err(cfg("s3.amazonaws.com/path", "")),
            "initialize s3 client: Endpoint url cannot have fully qualified paths."
        );
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    // minio-go bucket-cache_test.go TestProcessBucketLocationResponse.
    #[test]
    fn process_bucket_location_responses() {
        let ok = |body: &str| {
            process_bucket_location_response(200, &HeaderMap::new(), body.as_bytes(), "b")
                .unwrap()
        };
        assert_eq!(
            ok(r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">eu-west-1</LocationConstraint>"#),
            "eu-west-1"
        );
        assert_eq!(
            ok(r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#),
            "us-east-1"
        );
        assert_eq!(
            ok(r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#),
            "us-east-1"
        );
        assert_eq!(ok("<LocationConstraint>EU</LocationConstraint>"), "eu-west-1");

        // Access denied: region from the error (or us-east-1).
        let denied = r#"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#;
        assert_eq!(
            process_bucket_location_response(403, &HeaderMap::new(), denied.as_bytes(), "b")
                .unwrap(),
            "us-east-1"
        );
        assert_eq!(
            process_bucket_location_response(
                403,
                &headers(&[("x-amz-bucket-region", "ap-south-1")]),
                denied.as_bytes(),
                "b"
            )
            .unwrap(),
            "ap-south-1"
        );
        let malformed = r#"<Error><Code>AuthorizationHeaderMalformed</Code><Message>the region 'us-east-1' is wrong; expecting 'eu-west-2'</Message><Region>eu-west-2</Region></Error>"#;
        assert_eq!(
            process_bucket_location_response(400, &HeaderMap::new(), malformed.as_bytes(), "b")
                .unwrap(),
            "eu-west-2"
        );
        // Header-only 403 (no XML) maps to AccessDenied.
        assert_eq!(
            process_bucket_location_response(403, &HeaderMap::new(), b"", "b").unwrap(),
            "us-east-1"
        );
        // NotImplemented from cloudflare / snowball.
        let ni = r#"<Error><Code>NotImplemented</Code></Error>"#;
        assert_eq!(
            process_bucket_location_response(
                501,
                &headers(&[("Server", "cloudflare")]),
                ni.as_bytes(),
                "b"
            )
            .unwrap(),
            "us-east-1"
        );
        assert_eq!(
            process_bucket_location_response(
                501,
                &headers(&[("Server", "AmazonSnowball")]),
                ni.as_bytes(),
                "b"
            )
            .unwrap(),
            "snowball"
        );
        // Other errors propagate.
        let e = process_bucket_location_response(404, &HeaderMap::new(), b"", "b").unwrap_err();
        assert_eq!(e.code, "NoSuchBucket");
        assert_eq!(e.to_string(), "The specified bucket does not exist.");
        let e = process_bucket_location_response(
            500,
            &HeaderMap::new(),
            br#"<Error><Code>InternalError</Code><Message>boom</Message></Error>"#,
            "b",
        )
        .unwrap_err();
        assert_eq!((e.code.as_str(), e.message.as_str()), ("InternalError", "boom"));
    }

    /// End-to-end against an in-process fake S3: an unset region triggers exactly one
    /// `GET /<bucket>/?location=` (signed for us-east-1) and all following requests are signed
    /// for the discovered region.
    #[tokio::test]
    async fn discovers_region_with_get_bucket_location() {
        use std::sync::Mutex;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Arc::default();
        let seen2 = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let seen = Arc::clone(&seen2);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    loop {
                        let mut chunk = [0u8; 8192];
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        while let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                            let len = head
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    k.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().ok())?
                                })
                                .unwrap_or(0);
                            if buf.len() < end + 4 + len {
                                break;
                            }
                            buf.drain(..end + 4 + len);
                            let request_line = head.lines().next().unwrap_or("").to_string();
                            let scope = head
                                .lines()
                                .find_map(|l| {
                                    let l = l.to_ascii_lowercase();
                                    let i = l.find("credential=")?;
                                    Some(l[i + 11..].split(',').next()?.to_string())
                                })
                                .unwrap_or_default();
                            seen.lock().unwrap().push(format!("{request_line} {scope}"));
                            let body = if request_line.contains("?location") {
                                r#"<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">eu-west-1</LocationConstraint>"#
                            } else {
                                ""
                            };
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"x\"\r\nLast-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            if sock.write_all(resp.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });

        let mut c = cfg(&addr.to_string(), "");
        c.insecure = true;
        let store = new_store(&c).unwrap();
        store
            .put(&Path::from("t1/a"), PutPayload::from_static(b"hello"))
            .await
            .unwrap();
        store
            .put(&Path::from("t1/b"), PutPayload::from_static(b"world"))
            .await
            .unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 3, "{seen:?}");
        assert!(
            seen[0].starts_with("GET /mimir-blocks/?location= HTTP/1.1 akid/")
                && seen[0].ends_with("/us-east-1/s3/aws4_request"),
            "{seen:?}"
        );
        for r in &seen[1..] {
            assert!(r.starts_with("PUT /mimir-blocks/t1/"), "{seen:?}");
            assert!(r.ends_with("/eu-west-1/s3/aws4_request"), "{seen:?}");
        }
    }
}
