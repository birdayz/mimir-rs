//! Ported from github.com/grafana/mimir/pkg/ingester/client/compat.go

use std::fmt;

use crate::compat::{from_label_adapters_to_labels_with_copy, LabelAdapter, PromLabels};
use crate::ingesterpb;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromMatchType {
    MatchEqual = 0,
    MatchNotEqual = 1,
    MatchRegexp = 2,
    MatchNotRegexp = 3,
}

impl fmt::Display for PromMatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PromMatchType::MatchEqual => f.write_str("="),
            PromMatchType::MatchNotEqual => f.write_str("!="),
            PromMatchType::MatchRegexp => f.write_str("=~"),
            PromMatchType::MatchNotRegexp => f.write_str("!~"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromLabelMatcher {
    pub match_type: PromMatchType,
    pub name: String,
    pub value: String,
}

impl PromLabelMatcher {
    pub fn new(
        match_type: PromMatchType,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, String> {
        let name = name.into();
        let value = value.into();
        if matches!(
            match_type,
            PromMatchType::MatchRegexp | PromMatchType::MatchNotRegexp
        ) {
            regex::Regex::new(&value).map_err(|e| format!("invalid regexp {value:?}: {e}"))?;
        }
        Ok(Self {
            match_type,
            name,
            value,
        })
    }

    pub fn must_new(
        match_type: PromMatchType,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self::new(match_type, name, value).expect("valid matcher")
    }
}

impl fmt::Display for PromLabelMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.name,
            self.match_type,
            serde_json::to_string(&self.value).unwrap_or_else(|_| format!("{:?}", self.value))
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LabelHints {
    pub limit: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SelectHints {
    pub limit: usize,
}

pub fn to_label_matchers(
    matchers: &[PromLabelMatcher],
) -> Result<Vec<ingesterpb::LabelMatcher>, String> {
    let mut result = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        let m_type = match matcher.match_type {
            PromMatchType::MatchEqual => ingesterpb::MatchType::Equal,
            PromMatchType::MatchNotEqual => ingesterpb::MatchType::NotEqual,
            PromMatchType::MatchRegexp => ingesterpb::MatchType::RegexMatch,
            PromMatchType::MatchNotRegexp => ingesterpb::MatchType::RegexNoMatch,
        };
        result.push(ingesterpb::LabelMatcher {
            r#type: m_type as i32,
            name: matcher.name.clone(),
            value: matcher.value.clone(),
        });
    }
    Ok(result)
}

pub fn from_label_matchers(
    matchers: &[ingesterpb::LabelMatcher],
) -> Result<Vec<PromLabelMatcher>, String> {
    let mut result = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        let m_type = match ingesterpb::MatchType::try_from(matcher.r#type) {
            Ok(ingesterpb::MatchType::Equal) => PromMatchType::MatchEqual,
            Ok(ingesterpb::MatchType::NotEqual) => PromMatchType::MatchNotEqual,
            Ok(ingesterpb::MatchType::RegexMatch) => PromMatchType::MatchRegexp,
            Ok(ingesterpb::MatchType::RegexNoMatch) => PromMatchType::MatchNotRegexp,
            Err(_) => return Err("invalid matcher type".to_string()),
        };
        result.push(PromLabelMatcher::new(
            m_type,
            matcher.name.clone(),
            matcher.value.clone(),
        )?);
    }
    Ok(result)
}

pub fn to_query_request(
    from: i64,
    to: i64,
    matchers: &[PromLabelMatcher],
) -> Result<ingesterpb::QueryRequest, String> {
    let ms = to_label_matchers(matchers)?;
    Ok(ingesterpb::QueryRequest {
        start_timestamp_ms: from,
        end_timestamp_ms: to,
        matchers: ms,
        streaming_chunks_batch_size: 0,
    })
}

pub fn from_query_request(
    req: &ingesterpb::QueryRequest,
) -> Result<(i64, i64, Vec<PromLabelMatcher>), String> {
    let matchers = from_label_matchers(&req.matchers)?;
    Ok((req.start_timestamp_ms, req.end_timestamp_ms, matchers))
}

pub fn to_exemplar_query_request(
    from: i64,
    to: i64,
    matchers: &[&[PromLabelMatcher]],
) -> Result<ingesterpb::ExemplarQueryRequest, String> {
    let mut req_matchers = Vec::with_capacity(matchers.len());
    for m in matchers {
        let ms = to_label_matchers(m)?;
        req_matchers.push(ingesterpb::LabelMatchers { matchers: ms });
    }
    Ok(ingesterpb::ExemplarQueryRequest {
        start_timestamp_ms: from,
        end_timestamp_ms: to,
        matchers: req_matchers,
    })
}

pub fn from_exemplar_query_request(
    req: &ingesterpb::ExemplarQueryRequest,
) -> Result<(i64, i64, Vec<Vec<PromLabelMatcher>>), String> {
    let mut result = Vec::with_capacity(req.matchers.len());
    for m in &req.matchers {
        result.push(from_label_matchers(&m.matchers)?);
    }
    Ok((req.start_timestamp_ms, req.end_timestamp_ms, result))
}

pub fn to_metrics_for_label_matchers_request(
    from: i64,
    to: i64,
    hints: Option<&SelectHints>,
    matchers: &[PromLabelMatcher],
) -> Result<ingesterpb::MetricsForLabelMatchersRequest, String> {
    let ms = to_label_matchers(matchers)?;
    let limit = hints.map_or(0, |h| h.limit as i64);
    Ok(ingesterpb::MetricsForLabelMatchersRequest {
        start_timestamp_ms: from,
        end_timestamp_ms: to,
        matchers_set: vec![ingesterpb::LabelMatchers { matchers: ms }],
        limit,
    })
}

pub fn from_metrics_for_label_matchers_request(
    req: &ingesterpb::MetricsForLabelMatchersRequest,
) -> Result<(SelectHints, Vec<Vec<PromLabelMatcher>>), String> {
    let mut matchers_set = Vec::with_capacity(req.matchers_set.len());
    for m in &req.matchers_set {
        matchers_set.push(from_label_matchers(&m.matchers)?);
    }
    let limit = if req.limit > 0 { req.limit as usize } else { 0 };
    Ok((SelectHints { limit }, matchers_set))
}

pub fn from_metrics_for_label_matchers_response(
    resp: &ingesterpb::MetricsForLabelMatchersResponse,
) -> Vec<PromLabels> {
    resp.metric
        .iter()
        .map(|m| {
            let adapters: Vec<LabelAdapter> = m.labels.iter().map(Into::into).collect();
            from_label_adapters_to_labels_with_copy(&adapters)
        })
        .collect()
}

pub fn to_label_values_request(
    label_name: &str,
    from: i64,
    to: i64,
    hints: Option<&LabelHints>,
    matchers: &[PromLabelMatcher],
) -> Result<ingesterpb::LabelValuesRequest, String> {
    let ms = to_label_matchers(matchers)?;
    let limit = hints.map_or(0, |h| h.limit as i64);
    Ok(ingesterpb::LabelValuesRequest {
        label_name: label_name.to_string(),
        start_timestamp_ms: from,
        end_timestamp_ms: to,
        matchers: Some(ingesterpb::LabelMatchers { matchers: ms }),
        limit,
    })
}

pub type UnpackedLabelValuesRequest = (String, i64, i64, Option<LabelHints>, Vec<PromLabelMatcher>);

pub fn from_label_values_request(
    req: &ingesterpb::LabelValuesRequest,
) -> Result<UnpackedLabelValuesRequest, String> {
    let matchers = match &req.matchers {
        Some(m) => from_label_matchers(&m.matchers)?,
        None => Vec::new(),
    };
    let hints = if req.limit > 0 {
        Some(LabelHints {
            limit: req.limit as usize,
        })
    } else {
        None
    };
    Ok((
        req.label_name.clone(),
        req.start_timestamp_ms,
        req.end_timestamp_ms,
        hints,
        matchers,
    ))
}

pub fn to_label_names_request(
    from: i64,
    to: i64,
    hints: Option<&LabelHints>,
    matchers: &[PromLabelMatcher],
) -> Result<ingesterpb::LabelNamesRequest, String> {
    let ms = to_label_matchers(matchers)?;
    let limit = hints.map_or(0, |h| h.limit as i64);
    Ok(ingesterpb::LabelNamesRequest {
        start_timestamp_ms: from,
        end_timestamp_ms: to,
        matchers: Some(ingesterpb::LabelMatchers { matchers: ms }),
        limit,
    })
}

pub fn from_label_names_request(
    req: &ingesterpb::LabelNamesRequest,
) -> Result<(i64, i64, Option<LabelHints>, Vec<PromLabelMatcher>), String> {
    let matchers = match &req.matchers {
        Some(m) => from_label_matchers(&m.matchers)?,
        None => Vec::new(),
    };
    let hints = if req.limit > 0 {
        Some(LabelHints {
            limit: req.limit as usize,
        })
    } else {
        None
    };
    Ok((req.start_timestamp_ms, req.end_timestamp_ms, hints, matchers))
}

pub fn to_active_series_request(
    matchers: &[PromLabelMatcher],
) -> Result<ingesterpb::ActiveSeriesRequest, String> {
    let ms = to_label_matchers(matchers)?;
    Ok(ingesterpb::ActiveSeriesRequest {
        matchers: ms,
        r#type: 0,
    })
}
