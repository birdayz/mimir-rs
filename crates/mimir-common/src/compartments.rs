//! Read/write compartments config helpers, index page, router, and tenant ID encoding.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/compartments/config.go
//! - github.com/grafana/mimir/pkg/compartments/index_page.go
//! - github.com/grafana/mimir/pkg/compartments/index_page.gohtml
//! - github.com/grafana/mimir/pkg/compartments/router.go
//! - github.com/grafana/mimir/pkg/compartments/tenant_id.go

use mimir_proto::sharding::shard_by_metric_name;

use crate::tenant::{Metadata, TenantError, METADATA_SEPARATOR};

pub const READ_COMPARTMENT_ID_PLACEHOLDER: &str = "<read-compartment-id>";
pub const WRITE_COMPARTMENT_ID_PLACEHOLDER: &str = "<write-compartment-id>";
pub const MAX_READ_COMPARTMENTS: usize = (u8::MAX as usize) + 1;

const READ_COMPARTMENT_TENANT_ID_INFIX: &str = "/rc/";

/// Replaces `<read-compartment-id>` in `s` with `read_compartment_id`.
pub fn replace_read_compartment(s: &str, read_compartment_id: i64) -> String {
    s.replace(
        READ_COMPARTMENT_ID_PLACEHOLDER,
        &read_compartment_id.to_string(),
    )
}

/// Replaces `<write-compartment-id>` in `s` with `write_compartment_id`.
pub fn replace_write_compartment(s: &str, write_compartment_id: i64) -> String {
    s.replace(
        WRITE_COMPARTMENT_ID_PLACEHOLDER,
        &write_compartment_id.to_string(),
    )
}

/// Appends `-rc-<compartment_id>` to `name`.
pub fn with_read_compartment_suffix(name: &str, compartment_id: usize) -> String {
    format!("{name}-rc-{compartment_id}")
}

fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Renders the compartments index page HTML (`compartments.NewIndexPageHandler`).
pub fn index_page_html(page_title: &str, link_path_format: &str, num_compartments: usize) -> String {
    let title = html_escape(page_title);
    let mut out = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>{title}</title>\n</head>\n<body>\n    <h1>{title}</h1>\n    <ul>\n    "
    );
    for id in 0..num_compartments {
        let path = link_path_format.replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id.to_string());
        out.push_str(&format!(
            "\n        <li><a href=\"{}\">Compartment {id}</a></li>\n    ",
            html_escape(&path)
        ));
    }
    out.push_str("\n    </ul>\n</body>\n</html>\n");
    out
}

/// Lightweight view of a label matcher for `Router::compartments_for_matchers`.
#[derive(Debug, Clone, Copy)]
pub enum CompartmentMatcher<'a> {
    Equal {
        name: &'a str,
        value: &'a str,
    },
    Regexp {
        name: &'a str,
        set_matches: Option<&'a [String]>,
    },
    Other {
        name: &'a str,
    },
}

/// Assigns a tenant metric to a read compartment based on `ShardByMetricName`.
#[derive(Debug, Clone)]
pub struct Router {
    num_compartments: usize,
    all_read_compartment_ids: Vec<usize>,
}

impl Router {
    pub fn new(num_read_compartments: usize) -> Self {
        Self {
            num_compartments: num_read_compartments,
            all_read_compartment_ids: (0..num_read_compartments).collect(),
        }
    }

    pub fn num_compartments(&self) -> usize {
        self.num_compartments
    }

    pub fn compartment_for_metric(&self, user_id: &str, metric_name: &str) -> usize {
        let hash = shard_by_metric_name(user_id, metric_name);
        (hash as usize) % self.num_compartments
    }

    pub fn compartments_for_matchers<'a>(
        &self,
        user_id: &str,
        matchers: impl IntoIterator<Item = CompartmentMatcher<'a>>,
    ) -> Vec<usize> {
        let mut exact_name: Option<&str> = None;
        let mut set_matches: Option<&[String]> = None;

        for m in matchers {
            match m {
                CompartmentMatcher::Equal { name: "__name__", value } => {
                    if exact_name.is_none() {
                        exact_name = Some(value);
                    }
                }
                CompartmentMatcher::Regexp {
                    name: "__name__",
                    set_matches: Some(sm),
                } if !sm.is_empty()
                    && set_matches.is_none_or(|prev| sm.len() < prev.len()) =>
                {
                    set_matches = Some(sm);
                }
                _ => {}
            }
        }

        if let Some(name) = exact_name {
            return vec![self.compartment_for_metric(user_id, name)];
        }
        let Some(sm) = set_matches else {
            return self.all_read_compartment_ids.clone();
        };

        let mut compartments = Vec::new();
        for name in sm {
            let cid = self.compartment_for_metric(user_id, name);
            if !compartments.contains(&cid) {
                compartments.push(cid);
            }
        }
        compartments
    }
}

/// Extends `Router` with the Kafka topic of each read compartment.
#[derive(Debug, Clone)]
pub struct TopicRouter {
    router: Router,
    topics: Vec<String>,
}

impl TopicRouter {
    pub fn new(num_read_compartments: usize, kafka_topic_template: &str) -> Self {
        let topics = (0..num_read_compartments)
            .map(|id| replace_read_compartment(kafka_topic_template, id as i64))
            .collect();
        Self {
            router: Router::new(num_read_compartments),
            topics,
        }
    }

    pub fn num_compartments(&self) -> usize {
        self.router.num_compartments()
    }

    pub fn compartment_for_metric(&self, user_id: &str, metric_name: &str) -> usize {
        self.router.compartment_for_metric(user_id, metric_name)
    }

    pub fn compartments_for_matchers<'a>(
        &self,
        user_id: &str,
        matchers: impl IntoIterator<Item = CompartmentMatcher<'a>>,
    ) -> Vec<usize> {
        self.router.compartments_for_matchers(user_id, matchers)
    }

    pub fn topic_for_metric(&self, user_id: &str, metric_name: &str) -> &str {
        &self.topics[self.compartment_for_metric(user_id, metric_name)]
    }

    pub fn topic_for_compartment(&self, compartment_id: usize) -> &str {
        &self.topics[compartment_id]
    }

    pub fn topics(&self) -> Vec<String> {
        self.topics.clone()
    }
}

/// Returns `tenant_id` with the `/rc/<compartment_id>` suffix.
pub fn tenant_id_with_read_compartment(tenant_id: &str, compartment_id: usize) -> String {
    format!("{tenant_id}{READ_COMPARTMENT_TENANT_ID_INFIX}{compartment_id}")
}

/// Returns `tenant_id_with_metadata` without tenant metadata and without a well-formed
/// `/rc/<compartment_id>` suffix. Does not allocate.
pub fn base_tenant_id(tenant_id_with_metadata: &str) -> &str {
    if let Some((rc_idx, _)) = find_read_compartment_split(tenant_id_with_metadata.as_bytes()) {
        return &tenant_id_with_metadata[..rc_idx];
    }
    crate::tenant::trim_metadata(tenant_id_with_metadata)
}

/// Parses `tenant_id_with_metadata` like `tenant::parse_with_metadata`, preserving any
/// well-formed `/rc/<compartment_id>` suffix in the returned tenant ID.
pub fn parse_tenant_id_with_metadata(
    tenant_id_with_metadata: &str,
) -> Result<(String, Metadata), TenantError> {
    parse_tenant_id_with_metadata_bytes(tenant_id_with_metadata.as_bytes())
}

/// Byte-slice variant of `parse_tenant_id_with_metadata`.
pub fn parse_tenant_id_with_metadata_bytes(
    tenant_id_with_metadata: &[u8],
) -> Result<(String, Metadata), TenantError> {
    let Some((rc_idx, meta_idx)) = find_read_compartment_split(tenant_id_with_metadata) else {
        return crate::tenant::parse_with_metadata_bytes(tenant_id_with_metadata);
    };
    let base_bytes = &tenant_id_with_metadata[..rc_idx];
    let meta_bytes = &tenant_id_with_metadata[meta_idx..];
    crate::tenant::valid_tenant_id_bytes(base_bytes)?;
    let metadata = Metadata::parse_bytes(meta_bytes)?;
    let tenant_id = String::from_utf8_lossy(&tenant_id_with_metadata[..meta_idx]).into_owned();
    Ok((tenant_id, metadata))
}

fn find_read_compartment_split(bytes: &[u8]) -> Option<(usize, usize)> {
    if !bytes.contains(&b'/') {
        return None;
    }
    let meta_idx = bytes
        .iter()
        .position(|&b| b == METADATA_SEPARATOR as u8)
        .unwrap_or(bytes.len());
    let tenant_with_comp = &bytes[..meta_idx];
    let infix = READ_COMPARTMENT_TENANT_ID_INFIX.as_bytes();
    let rc_idx = tenant_with_comp
        .windows(infix.len())
        .rposition(|w| w == infix)?;
    let suffix = &tenant_with_comp[rc_idx + infix.len()..];
    let suffix_str = std::str::from_utf8(suffix).ok()?;
    let _: i64 = suffix_str.parse().ok()?;
    Some((rc_idx, meta_idx))
}
