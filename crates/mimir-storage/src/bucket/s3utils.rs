//! S3 endpoint helpers used to resolve the request host, the signing region and the bucket
//! lookup style exactly like minio-go (the S3 client behind thanos-io/objstore's S3 provider).
//!
//! Ported from:
//! - github.com/minio/minio-go/v7/pkg/s3utils/utils.go (`GetRegionFromURL`, `IsAmazonEndpoint`,
//!   `IsVirtualHostSupported`, `IsValidDomain`, `IsAmazon*Endpoint`, `IsGoogleEndpoint`, ...)
//! - github.com/minio/minio-go/v7/endpoints.go (`awsS3EndpointMap`, `getS3Endpoint`)
//! - github.com/minio/minio-go/v7/utils.go (`getEndpointURL`, `isValidEndpointURL`,
//!   `getDefaultLocation`)
//! - github.com/minio/minio-go/v7/api.go (`makeTargetURL` host selection)

use std::net::IpAddr;
use std::sync::OnceLock;

use regex::Regex;

/// A parsed minio-go endpoint URL (`scheme://endpoint`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointUrl {
    /// `http` or `https`.
    pub scheme: &'static str,
    /// `url.URL.Host` (host and optional port, IPv6 hosts in brackets).
    pub host: String,
    /// `url.URL.Hostname()` (host without port and brackets).
    pub hostname: String,
}

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("valid s3utils regex"))
}

// The patterns are copied verbatim from minio-go, including the unescaped `.` (which matches
// any character, as in Go).
fn amazon_s3_host_hyphen() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3-(.*?).amazonaws.com$")
}
fn amazon_s3_host_dual_stack() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3.dualstack.(.*?).amazonaws.com$")
}
fn amazon_s3_host_fips() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3-fips.(.*?).amazonaws.com$")
}
fn amazon_s3_host_fips_dual_stack() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3-fips.dualstack.(.*?).amazonaws.com$")
}
fn amazon_s3_host_express() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3express-[a-z0-9]{3,7}-az[1-6]\.([a-z0-9-]+)\.amazonaws\.com$")
}
fn amazon_s3_host_express_control() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3express-control\.([a-z0-9-]+)\.amazonaws\.com$")
}
fn amazon_s3_host_dot() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3.(.*?).amazonaws.com$")
}
fn amazon_s3_china_host() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3.(cn.*?).amazonaws.com.cn$")
}
fn amazon_s3_china_host_dual_stack() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^s3.dualstack.(cn.*?).amazonaws.com.cn$")
}
fn elb_amazon() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"elb(.*?).amazonaws.com$")
}
fn elb_amazon_cn() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"elb(.*?).amazonaws.com.cn$")
}
fn amazon_s3_host_private_link() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^(?:bucket|accesspoint).vpce-.*?.s3.(.*?).vpce.amazonaws.com$")
}
fn amazon_s3_host_outposts() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^(.+)\.s3-outposts\.([a-z0-9-]+)\.amazonaws\.com$")
}

fn submatch<'a>(r: &Regex, s: &'a str, idx: usize) -> Option<&'a str> {
    r.captures(s).and_then(|c| c.get(idx)).map(|m| m.as_str())
}

/// `s3utils.GetRegionFromURL`.
pub fn get_region_from_url(hostname: &str) -> String {
    if hostname.is_empty() {
        return String::new();
    }
    if hostname == "s3-external-1.amazonaws.com" {
        return String::new();
    }
    if elb_amazon().is_match(hostname) || elb_amazon_cn().is_match(hostname) {
        return String::new();
    }
    for r in [
        amazon_s3_host_fips_dual_stack(),
        amazon_s3_host_fips(),
        amazon_s3_host_dual_stack(),
        amazon_s3_host_hyphen(),
        amazon_s3_china_host(),
        amazon_s3_china_host_dual_stack(),
        amazon_s3_host_private_link(),
        amazon_s3_host_express(),
        amazon_s3_host_express_control(),
    ] {
        if let Some(region) = submatch(r, hostname, 1) {
            return region.to_string();
        }
    }
    if let Some(region) = submatch(amazon_s3_host_outposts(), hostname, 2) {
        return region.to_string();
    }
    if let Some(region) = submatch(amazon_s3_host_dot(), hostname, 1) {
        if region.starts_with("xpress-") {
            return String::new();
        }
        if region.starts_with("dualstack.")
            || region.starts_with("control.")
            || region.starts_with("website-")
        {
            return String::new();
        }
        return region.to_string();
    }
    String::new()
}

/// `s3utils.IsAmazonOutpostsEndpoint`.
pub fn is_amazon_outposts_endpoint(hostname: &str) -> bool {
    !hostname.is_empty() && amazon_s3_host_outposts().is_match(hostname)
}

/// `s3utils.IsAmazonEndpoint`.
pub fn is_amazon_endpoint(hostname: &str) -> bool {
    if is_amazon_outposts_endpoint(hostname) {
        return false;
    }
    if hostname == "s3-external-1.amazonaws.com" || hostname == "s3.amazonaws.com" {
        return true;
    }
    !get_region_from_url(hostname).is_empty()
}

/// `s3utils.IsAmazonFIPSEndpoint`.
pub fn is_amazon_fips_endpoint(hostname: &str) -> bool {
    hostname.starts_with("s3-fips") && hostname.ends_with(".amazonaws.com")
}

/// `s3utils.IsAmazonPrivateLinkEndpoint`.
pub fn is_amazon_private_link_endpoint(hostname: &str) -> bool {
    !hostname.is_empty() && amazon_s3_host_private_link().is_match(hostname)
}

/// `s3utils.IsAmazonExpressRegionalEndpoint`.
pub fn is_amazon_express_regional_endpoint(hostname: &str) -> bool {
    amazon_s3_host_express_control().is_match(hostname)
}

/// `s3utils.IsGoogleEndpoint`.
pub fn is_google_endpoint(hostname: &str) -> bool {
    hostname == "storage.googleapis.com"
}

/// `s3utils.IsAliyunOSSEndpoint`.
pub fn is_aliyun_oss_endpoint(hostname: &str) -> bool {
    hostname.ends_with("aliyuncs.com")
}

/// `s3utils.IsVirtualHostSupported`.
pub fn is_virtual_host_supported(endpoint: &EndpointUrl, bucket_name: &str) -> bool {
    if endpoint.scheme == "https" && bucket_name.contains('.') {
        return false;
    }
    is_amazon_endpoint(&endpoint.hostname)
        || is_google_endpoint(&endpoint.hostname)
        || is_aliyun_oss_endpoint(&endpoint.hostname)
}

/// `s3utils.IsValidDomain`.
pub fn is_valid_domain(host: &str) -> bool {
    let host = host.trim();
    if host.is_empty() || host.len() > 255 {
        return false;
    }
    if host.ends_with('-') || host.starts_with('-') {
        return false;
    }
    if host.ends_with('_') || host.starts_with('_') {
        return false;
    }
    if host.starts_with('.') {
        return false;
    }
    if host.chars().any(|c| "`~!@#$%^&*()+={}[]|\\\"';:><?/".contains(c)) {
        return false;
    }
    true
}

/// `getEndpointURL(endpoint, secure)` including `isValidEndpointURL`. Error strings are the
/// minio-go `ErrorResponse` messages (`errInvalidArgument(...)`).
pub fn get_endpoint_url(endpoint: &str, secure: bool) -> Result<EndpointUrl, String> {
    let scheme = if secure { "https" } else { "http" };
    // url.Parse("scheme://" + endpoint): the authority ends at the first '/', '?' or '#'.
    let (authority, rest) = match endpoint.find(['/', '?', '#']) {
        Some(i) => (&endpoint[..i], &endpoint[i..]),
        None => (endpoint, ""),
    };
    if authority.contains('@') {
        return Err(format!(
            "Endpoint: {authority} does not follow ip address or domain name standards."
        ));
    }
    let path = rest.split(['?', '#']).next().unwrap_or("");
    if path != "/" && !path.is_empty() {
        return Err("Endpoint url cannot have fully qualified paths.".to_string());
    }
    let hostname = hostname_of(authority);
    if hostname.parse::<IpAddr>().is_err() && !is_valid_domain(&hostname) {
        return Err(format!(
            "Endpoint: {authority} does not follow ip address or domain name standards."
        ));
    }
    if hostname.contains(".s3.amazonaws.com") && !is_amazon_endpoint(&hostname) {
        return Err("Amazon S3 endpoint should be 's3.amazonaws.com'.".to_string());
    }
    if hostname.contains(".googleapis.com") && !is_google_endpoint(&hostname) {
        return Err(
            "Google Cloud Storage endpoint should be 'storage.googleapis.com'.".to_string(),
        );
    }
    if hostname.contains("s3-outposts") && !is_amazon_outposts_endpoint(&hostname) {
        return Err(
            "S3 Outposts endpoint must match <prefix>.s3-outposts.<region>.amazonaws.com"
                .to_string(),
        );
    }
    Ok(EndpointUrl {
        scheme,
        host: authority.to_string(),
        hostname,
    })
}

/// Go `url.URL.Hostname()`.
fn hostname_of(host: &str) -> String {
    if let Some(rest) = host.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return rest[..end].to_string();
        }
    }
    match host.rfind(':') {
        Some(i) => host[..i].to_string(),
        None => host.to_string(),
    }
}

/// `getDefaultLocation(u, regionOverride)`.
pub fn get_default_location(hostname: &str, region_override: &str) -> String {
    if !region_override.is_empty() {
        return region_override.to_string();
    }
    let region = get_region_from_url(hostname);
    if region.is_empty() {
        "us-east-1".to_string()
    } else {
        region
    }
}

/// `awsS3EndpointMap`: (location, endpoint, dualstack endpoint).
const AWS_S3_ENDPOINT_MAP: &[(&str, &str, &str)] = &[
    ("us-east-1", "s3.us-east-1.amazonaws.com", "s3.dualstack.us-east-1.amazonaws.com"),
    ("us-east-2", "s3.us-east-2.amazonaws.com", "s3.dualstack.us-east-2.amazonaws.com"),
    ("us-iso-east-1", "s3.us-iso-east-1.c2s.ic.gov", ""),
    ("us-isob-east-1", "s3.us-isob-east-1.sc2s.sgov.gov", ""),
    ("us-iso-west-1", "s3.us-iso-west-1.c2s.ic.gov", ""),
    ("us-west-2", "s3.us-west-2.amazonaws.com", "s3.dualstack.us-west-2.amazonaws.com"),
    ("us-west-1", "s3.us-west-1.amazonaws.com", "s3.dualstack.us-west-1.amazonaws.com"),
    ("ca-central-1", "s3.ca-central-1.amazonaws.com", "s3.dualstack.ca-central-1.amazonaws.com"),
    ("ca-west-1", "s3.ca-west-1.amazonaws.com", "s3.dualstack.ca-west-1.amazonaws.com"),
    ("eu-west-1", "s3.eu-west-1.amazonaws.com", "s3.dualstack.eu-west-1.amazonaws.com"),
    ("eu-west-2", "s3.eu-west-2.amazonaws.com", "s3.dualstack.eu-west-2.amazonaws.com"),
    ("eu-west-3", "s3.eu-west-3.amazonaws.com", "s3.dualstack.eu-west-3.amazonaws.com"),
    ("eu-central-1", "s3.eu-central-1.amazonaws.com", "s3.dualstack.eu-central-1.amazonaws.com"),
    ("eu-central-2", "s3.eu-central-2.amazonaws.com", "s3.dualstack.eu-central-2.amazonaws.com"),
    ("eu-north-1", "s3.eu-north-1.amazonaws.com", "s3.dualstack.eu-north-1.amazonaws.com"),
    ("eu-south-1", "s3.eu-south-1.amazonaws.com", "s3.dualstack.eu-south-1.amazonaws.com"),
    ("eu-south-2", "s3.eu-south-2.amazonaws.com", "s3.dualstack.eu-south-2.amazonaws.com"),
    ("ap-east-1", "s3.ap-east-1.amazonaws.com", "s3.dualstack.ap-east-1.amazonaws.com"),
    ("ap-south-1", "s3.ap-south-1.amazonaws.com", "s3.dualstack.ap-south-1.amazonaws.com"),
    ("ap-south-2", "s3.ap-south-2.amazonaws.com", "s3.dualstack.ap-south-2.amazonaws.com"),
    ("ap-southeast-1", "s3.ap-southeast-1.amazonaws.com", "s3.dualstack.ap-southeast-1.amazonaws.com"),
    ("ap-southeast-2", "s3.ap-southeast-2.amazonaws.com", "s3.dualstack.ap-southeast-2.amazonaws.com"),
    ("ap-southeast-3", "s3.ap-southeast-3.amazonaws.com", "s3.dualstack.ap-southeast-3.amazonaws.com"),
    ("ap-southeast-4", "s3.ap-southeast-4.amazonaws.com", "s3.dualstack.ap-southeast-4.amazonaws.com"),
    ("ap-northeast-1", "s3.ap-northeast-1.amazonaws.com", "s3.dualstack.ap-northeast-1.amazonaws.com"),
    ("ap-northeast-2", "s3.ap-northeast-2.amazonaws.com", "s3.dualstack.ap-northeast-2.amazonaws.com"),
    ("ap-northeast-3", "s3.ap-northeast-3.amazonaws.com", "s3.dualstack.ap-northeast-3.amazonaws.com"),
    ("af-south-1", "s3.af-south-1.amazonaws.com", "s3.dualstack.af-south-1.amazonaws.com"),
    ("me-central-1", "s3.me-central-1.amazonaws.com", "s3.dualstack.me-central-1.amazonaws.com"),
    ("me-south-1", "s3.me-south-1.amazonaws.com", "s3.dualstack.me-south-1.amazonaws.com"),
    ("sa-east-1", "s3.sa-east-1.amazonaws.com", "s3.dualstack.sa-east-1.amazonaws.com"),
    ("us-gov-west-1", "s3.us-gov-west-1.amazonaws.com", "s3.dualstack.us-gov-west-1.amazonaws.com"),
    ("us-gov-east-1", "s3.us-gov-east-1.amazonaws.com", "s3.dualstack.us-gov-east-1.amazonaws.com"),
    ("cn-north-1", "s3.cn-north-1.amazonaws.com.cn", "s3.dualstack.cn-north-1.amazonaws.com.cn"),
    ("cn-northwest-1", "s3.cn-northwest-1.amazonaws.com.cn", "s3.dualstack.cn-northwest-1.amazonaws.com.cn"),
    ("il-central-1", "s3.il-central-1.amazonaws.com", "s3.dualstack.il-central-1.amazonaws.com"),
    ("ap-southeast-5", "s3.ap-southeast-5.amazonaws.com", "s3.dualstack.ap-southeast-5.amazonaws.com"),
    ("ap-southeast-7", "s3.ap-southeast-7.amazonaws.com", "s3.dualstack.ap-southeast-7.amazonaws.com"),
    ("mx-central-1", "s3.mx-central-1.amazonaws.com", "s3.dualstack.mx-central-1.amazonaws.com"),
    ("ap-east-2", "s3.ap-east-2.amazonaws.com", "s3.dualstack.ap-east-2.amazonaws.com"),
    ("ap-southeast-6", "s3.ap-southeast-6.amazonaws.com", "s3.dualstack.ap-southeast-6.amazonaws.com"),
];

/// `getS3Endpoint(bucketLocation, useDualstack)`.
pub fn get_s3_endpoint(bucket_location: &str, use_dualstack: bool) -> String {
    match AWS_S3_ENDPOINT_MAP
        .iter()
        .find(|(loc, _, _)| *loc == bucket_location)
    {
        None => {
            if use_dualstack {
                "s3.dualstack.us-east-1.amazonaws.com".to_string()
            } else {
                "s3.us-east-1.amazonaws.com".to_string()
            }
        }
        Some((_, endpoint, dualstack)) => {
            if use_dualstack && !dualstack.is_empty() {
                dualstack.to_string()
            } else {
                endpoint.to_string()
            }
        }
    }
}

/// Strips `:80` (http) / `:443` (https) from `host` like `makeTargetURL` and
/// `getBucketLocationRequest` do, re-bracketing IPv6 literals.
pub fn strip_default_port(scheme: &str, host: &str) -> String {
    if let Some((h, p)) = split_host_port(host) {
        if (scheme == "http" && p == "80") || (scheme == "https" && p == "443") {
            if let Ok(IpAddr::V6(_)) = h.parse::<IpAddr>() {
                return format!("[{h}]");
            }
            return h.to_string();
        }
    }
    host.to_string()
}

/// Go `net.SplitHostPort` (only the success cases needed here).
fn split_host_port(host: &str) -> Option<(&str, &str)> {
    if let Some(rest) = host.strip_prefix('[') {
        let end = rest.find(']')?;
        let after = &rest[end + 1..];
        let port = after.strip_prefix(':')?;
        return Some((&rest[..end], port));
    }
    let i = host.rfind(':')?;
    let h = &host[..i];
    if h.contains(':') {
        return None;
    }
    Some((h, &host[i + 1..]))
}

/// Host selection of `makeTargetURL` for a request to `bucket_location`.
pub fn target_host(endpoint: &EndpointUrl, bucket_location: &str, dualstack: bool) -> String {
    let mut host = endpoint.host.clone();
    if is_amazon_endpoint(&endpoint.hostname)
        && !is_amazon_fips_endpoint(&endpoint.hostname)
        && !is_amazon_private_link_endpoint(&endpoint.hostname)
        && !is_amazon_express_regional_endpoint(&endpoint.hostname)
    {
        host = get_s3_endpoint(bucket_location, dualstack);
    }
    strip_default_port(endpoint.scheme, &host)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `url.Parse(s).Hostname()` for the test inputs (scheme-less inputs parse as a path).
    fn parsed_hostname(s: &str) -> String {
        match s.split_once("://") {
            Some((_, rest)) => hostname_of(rest.split('/').next().unwrap_or("")),
            None => String::new(),
        }
    }

    // Ported verbatim from minio-go v7.0.98 pkg/s3utils/utils_test.go TestGetRegionFromURL
    // (`url.URL{Host: u}`).
    #[test]
    fn test_get_region_from_url() {
        let cases = [
            ("storage.googleapis.com", ""),
            ("s3.cn-north-1.amazonaws.com.cn", "cn-north-1"),
            ("s3.dualstack.cn-north-1.amazonaws.com.cn", "cn-north-1"),
            ("s3.cn-northwest-1.amazonaws.com.cn", "cn-northwest-1"),
            ("s3.dualstack.cn-northwest-1.amazonaws.com.cn", "cn-northwest-1"),
            ("s3-fips-us-gov-west-1.amazonaws.com", "us-gov-west-1"),
            ("s3-fips.us-gov-west-1.amazonaws.com", "us-gov-west-1"),
            ("s3-fips.us-gov-east-1.amazonaws.com", "us-gov-east-1"),
            ("s3-us-gov-west-1.amazonaws.com", "us-gov-west-1"),
            ("192.168.1.1", ""),
            ("s3-eu-west-1.amazonaws.com", "eu-west-1"),
            ("s3.eu-west-1.amazonaws.com", "eu-west-1"),
            ("s3.dualstack.eu-west-1.amazonaws.com", "eu-west-1"),
            ("s3.amazonaws.com", ""),
            ("s3-external-1.amazonaws.com", ""),
            ("s3.kubernetesfrontendlb-caf78da2b1f7516c.elb.us-west-2.amazonaws.com", ""),
            ("s3.kubernetesfrontendlb-caf78da2b1f7516c.elb.amazonaws.com", ""),
            ("s3.kubernetesfrontendlb-caf78da2b1f7516c.elb.amazonaws.com.cn", ""),
            ("bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com", "us-east-1"),
            ("accesspoint.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com", "us-east-1"),
            ("s3-fips.us-east-1.amazonaws.com", "us-east-1"),
            ("s3-fips.dualstack.us-west-1.amazonaws.com", "us-west-1"),
            ("s3express-usw2-az1.us-west-2.amazonaws.com", "us-west-2"),
            ("s3express-use1-az5.us-east-1.amazonaws.com", "us-east-1"),
            ("s3express-apne1-az4.ap-northeast-1.amazonaws.com", "ap-northeast-1"),
            ("s3express-euc1-az2.eu-central-1.amazonaws.com", "eu-central-1"),
            ("s3express-usgw1-az3.us-gov-west-1.amazonaws.com", "us-gov-west-1"),
            ("s3express-control.us-west-2.amazonaws.com", "us-west-2"),
            ("storage.googleapis.com:80", ""),
            ("s3.cn-north-1.amazonaws.com.cn:80", "cn-north-1"),
            ("s3.dualstack.cn-north-1.amazonaws.com.cn:80", "cn-north-1"),
            ("s3.cn-northwest-1.amazonaws.com.cn:80", "cn-northwest-1"),
            ("s3.dualstack.cn-northwest-1.amazonaws.com.cn:80", "cn-northwest-1"),
            ("s3-fips-us-gov-west-1.amazonaws.com:80", "us-gov-west-1"),
            ("s3-fips.us-gov-west-1.amazonaws.com:80", "us-gov-west-1"),
            ("s3-fips.us-gov-east-1.amazonaws.com:80", "us-gov-east-1"),
            ("s3-us-gov-west-1.amazonaws.com:80", "us-gov-west-1"),
            ("s3-eu-west-1.amazonaws.com:80", "eu-west-1"),
            ("s3.eu-west-1.amazonaws.com:80", "eu-west-1"),
            ("s3.dualstack.eu-west-1.amazonaws.com:80", "eu-west-1"),
            ("s3.kubernetesfrontendlb-caf78da2b1f7516c.elb.us-west-2.amazonaws.com:80", ""),
            ("s3.kubernetesfrontendlb-caf78da2b1f7516c.elb.amazonaws.com:80", ""),
            ("s3.kubernetesfrontendlb-caf78da2b1f7516c.elb.amazonaws.com.cn:80", ""),
            ("bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com:80", "us-east-1"),
            ("accesspoint.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com:80", "us-east-1"),
            ("s3-fips.us-east-1.amazonaws.com:80", "us-east-1"),
            ("s3-fips.dualstack.us-west-1.amazonaws.com:80", "us-west-1"),
            ("192.168.1.1:80", ""),
            ("s3express-usw2-az7.us-west-2.amazonaws.com", ""),
            ("invalid-endpoint.com", ""),
            ("s3.amazonaws.com:80", ""),
            ("s3-external-1.amazonaws.com:80", ""),
        ];
        for (host, want) in cases {
            assert_eq!(get_region_from_url(&hostname_of(host)), want, "{host}");
        }
    }

    // Ported verbatim from minio-go v7.0.98 pkg/s3utils/utils_test.go TestIsAmazonEndpoint
    // (`url.Parse(endpoint)`).
    #[test]
    fn test_is_amazon_endpoint() {
        let cases = [
            ("https://192.168.1.1", false),
            ("192.168.1.1", false),
            ("http://storage.googleapis.com", false),
            ("https://storage.googleapis.com", false),
            ("storage.googleapis.com", false),
            ("s3.amazonaws.com", false),
            ("https://amazons3.amazonaws.com", false),
            ("-192.168.1.1", false),
            ("260.192.1.1", false),
            ("https://s3-.amazonaws.com", false),
            ("https://s3..amazonaws.com", false),
            ("https://s3.dualstack.us-west-1.amazonaws.com.cn", false),
            ("https://s3..us-west-1.amazonaws.com.cn", false),
            ("https://s3.amazonaws.com", true),
            ("https://s3-external-1.amazonaws.com", true),
            ("https://s3.cn-north-1.amazonaws.com.cn", true),
            ("https://s3-us-west-1.amazonaws.com", true),
            ("https://s3.us-west-1.amazonaws.com", true),
            ("https://s3.dualstack.us-west-1.amazonaws.com", true),
            ("https://bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com", true),
            ("https://accesspoint.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com", true),
            ("https://s3express-usw2-az1.us-west-2.amazonaws.com", true),
        ];
        for (endpoint, want) in cases {
            assert_eq!(is_amazon_endpoint(&parsed_hostname(endpoint)), want, "{endpoint}");
        }
    }

    // Ported verbatim from minio-go v7.0.98 pkg/s3utils/utils_test.go TestIsVirtualHostSupported.
    #[test]
    fn test_is_virtual_host_supported() {
        let cases = [
            ("https://s3.amazonaws.com", "my-bucket", true),
            ("https://s3.cn-north-1.amazonaws.com.cn", "my-bucket", true),
            ("https://s3.amazonaws.com", "my-bucket.", false),
            ("https://amazons3.amazonaws.com", "my-bucket.", false),
            ("https://storage.googleapis.com/", "my-bucket", true),
            ("https://mystorage.googleapis.com/", "my-bucket", false),
        ];
        for (endpoint, bucket, want) in cases {
            let u = EndpointUrl {
                scheme: "https",
                host: parsed_hostname(endpoint),
                hostname: parsed_hostname(endpoint),
            };
            assert_eq!(is_virtual_host_supported(&u, bucket), want, "{endpoint} {bucket}");
        }
    }

    // Ported verbatim from minio-go v7.0.98 pkg/s3utils/utils_test.go TestIsValidDomain.
    #[test]
    fn test_is_valid_domain() {
        let cases = [
            ("s3.amazonaws.com", true),
            ("s3.cn-north-1.amazonaws.com.cn", true),
            ("s3.cn-northwest-1.amazonaws.com.cn", true),
            ("s3.amazonaws.com_", false),
            ("%$$$", false),
            ("s3.amz.test.com", true),
            ("s3.%%", false),
            ("localhost", true),
            ("localhost.", true),
            ("-localhost", false),
            ("", false),
            ("\n \t", false),
            ("   ", false),
        ];
        for (host, want) in cases {
            assert_eq!(is_valid_domain(host), want, "{host:?}");
        }
    }

    // Ported verbatim from minio-go v7.0.98 utils_test.go TestGetEndpointURL.
    #[test]
    fn test_get_endpoint_url() {
        let ok = [
            ("s3.amazonaws.com", true, "https://s3.amazonaws.com"),
            ("s3.cn-north-1.amazonaws.com.cn", true, "https://s3.cn-north-1.amazonaws.com.cn"),
            ("s3.cn-northwest-1.amazonaws.com.cn", true, "https://s3.cn-northwest-1.amazonaws.com.cn"),
            ("s3.amazonaws.com", false, "http://s3.amazonaws.com"),
            ("s3.cn-north-1.amazonaws.com.cn", false, "http://s3.cn-north-1.amazonaws.com.cn"),
            ("s3.cn-northwest-1.amazonaws.com.cn", false, "http://s3.cn-northwest-1.amazonaws.com.cn"),
            ("192.168.1.1:9000", false, "http://192.168.1.1:9000"),
            ("192.168.1.1:9000", true, "https://192.168.1.1:9000"),
            ("s3.amazonaws.com:443", true, "https://s3.amazonaws.com:443"),
            ("storage.googleapis.com:443", true, "https://storage.googleapis.com:443"),
            ("[::1]", false, "http://[::1]"),
            ("[::1]", true, "https://[::1]"),
            ("[::1]:80", false, "http://[::1]:80"),
            ("[::1]:443", true, "https://[::1]:443"),
            ("[::1]:9000", false, "http://[::1]:9000"),
            ("[::1]:9000", true, "https://[::1]:9000"),
        ];
        for (ep, secure, want) in ok {
            let u = get_endpoint_url(ep, secure).unwrap_or_else(|e| panic!("{ep}: {e}"));
            assert_eq!(format!("{}://{}", u.scheme, u.host), want, "{ep}");
        }
        let errs = [
            ("13333.123123.-", "Endpoint: 13333.123123.- does not follow ip address or domain name standards."),
            ("s3.aamzza.-", "Endpoint: s3.aamzza.- does not follow ip address or domain name standards."),
            ("", "Endpoint:  does not follow ip address or domain name standards."),
            // isValidEndpointURL branches.
            ("s3.amazonaws.com/minio", "Endpoint url cannot have fully qualified paths."),
            ("minio-bucket.s3.amazonaws.com", "Amazon S3 endpoint should be 's3.amazonaws.com'."),
            ("minio-bucket.storage.googleapis.com", "Google Cloud Storage endpoint should be 'storage.googleapis.com'."),
        ];
        for (ep, want) in errs {
            assert_eq!(get_endpoint_url(ep, true).unwrap_err(), want, "{ep}");
        }
    }

    #[test]
    fn test_target_host_and_default_port_stripping() {
        let aws = get_endpoint_url("s3.amazonaws.com", true).unwrap();
        assert_eq!(target_host(&aws, "eu-west-1", true), "s3.dualstack.eu-west-1.amazonaws.com");
        assert_eq!(target_host(&aws, "eu-west-1", false), "s3.eu-west-1.amazonaws.com");
        assert_eq!(target_host(&aws, "unknown-1", true), "s3.dualstack.us-east-1.amazonaws.com");
        assert_eq!(target_host(&aws, "us-iso-east-1", true), "s3.us-iso-east-1.c2s.ic.gov");
        let fips = get_endpoint_url("s3-fips.us-gov-west-1.amazonaws.com", true).unwrap();
        assert_eq!(target_host(&fips, "us-gov-west-1", true), "s3-fips.us-gov-west-1.amazonaws.com");
        let minio = get_endpoint_url("minio:443", true).unwrap();
        assert_eq!(target_host(&minio, "us-east-1", true), "minio");
        let minio = get_endpoint_url("minio:9000", true).unwrap();
        assert_eq!(target_host(&minio, "us-east-1", true), "minio:9000");
        let v6 = get_endpoint_url("[::1]:80", false).unwrap();
        assert_eq!(target_host(&v6, "us-east-1", true), "[::1]");
    }
}
