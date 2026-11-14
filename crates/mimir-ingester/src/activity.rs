//! Ingester request activity formatting matching `github.com/grafana/mimir/pkg/ingester/ingester_activity.go`.

use std::fmt::Write as _;

use mimir_proto::ingesterpb;

pub enum ActivityRequest<'a> {
    None,
    Query(&'a ingesterpb::QueryRequest),
    MetricsMetadata(&'a ingesterpb::MetricsMetadataRequest),
    LabelValuesCardinality(&'a ingesterpb::LabelValuesCardinalityRequest),
}

pub fn default_metrics_metadata_request() -> ingesterpb::MetricsMetadataRequest {
    ingesterpb::MetricsMetadataRequest {
        limit: -1,
        limit_per_metric: -1,
        metric: String::new(),
        metric_names: Vec::new(),
    }
}

pub fn request_activity(
    user_id: &str,
    trace_id: &str,
    name: &str,
    req: ActivityRequest<'_>,
) -> String {
    match req {
        ActivityRequest::Query(q) => query_request_activity(name, user_id, trace_id, Some(q)),
        ActivityRequest::None => {
            format!("{name}: user={user_id:?} trace={trace_id:?} request=<nil>")
        }
        ActivityRequest::MetricsMetadata(m) => {
            let mut s = String::new();
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&MetricsMetadataRequest{{Limit:{},LimitPerMetric:{},Metric:{},MetricNames:[{}],}}",
                m.limit,
                m.limit_per_metric,
                m.metric,
                m.metric_names.join(" "),
            );
            s
        }
        ActivityRequest::LabelValuesCardinality(r) => {
            let mut s = String::new();
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&LabelValuesCardinalityRequest{{LabelNames:[{}],Matchers:[]*LabelMatcher{{",
                r.label_names.join(" "),
            );
            for m in &r.matchers {
                label_matcher_to_string(&mut s, Some(m));
                s.push(',');
            }
            let cm = match ingesterpb::CountMethod::try_from(r.count_method) {
                Ok(ingesterpb::CountMethod::InMemory) => "IN_MEMORY",
                Ok(ingesterpb::CountMethod::Active) => "ACTIVE",
                Err(_) => "UNKNOWN",
            };
            let _ = write!(s, "}},CountMethod:{cm},}}");
            s
        }
    }
}

pub fn query_request_activity(
    name: &str,
    user_id: &str,
    trace_id: &str,
    req: Option<&ingesterpb::QueryRequest>,
) -> String {
    let mut sb = String::with_capacity(256);
    let _ = write!(sb, "{name}: user={user_id:?} trace={trace_id:?} request=");
    query_request_to_string(&mut sb, req);
    sb
}

pub fn query_request_to_string(sb: &mut String, req: Option<&ingesterpb::QueryRequest>) {
    let Some(req) = req else {
        sb.push_str("nil");
        return;
    };
    let _ = write!(
        sb,
        "&QueryRequest{{StartTimestampMs:{},EndTimestampMs:{},Matchers:[]*LabelMatcher{{",
        req.start_timestamp_ms, req.end_timestamp_ms
    );
    for m in &req.matchers {
        label_matcher_to_string(sb, Some(m));
        sb.push(',');
    }
    let _ = write!(
        sb,
        "}},StreamingChunksBatchSize:{},}}",
        req.streaming_chunks_batch_size
    );
}

pub fn label_matcher_to_string(sb: &mut String, m: Option<&ingesterpb::LabelMatcher>) {
    let Some(m) = m else {
        sb.push_str("nil");
        return;
    };
    let ty = match ingesterpb::MatchType::try_from(m.r#type) {
        Ok(ingesterpb::MatchType::Equal) => "EQUAL",
        Ok(ingesterpb::MatchType::NotEqual) => "NOT_EQUAL",
        Ok(ingesterpb::MatchType::RegexMatch) => "REGEX_MATCH",
        Ok(ingesterpb::MatchType::RegexNoMatch) => "REGEX_NO_MATCH",
        Err(_) => "UNKNOWN",
    };
    let _ = write!(
        sb,
        "&LabelMatcher{{Type:{ty},Name:{},Value:{},}}",
        m.name, m.value
    );
}
