//! Ingester request activity formatting matching `github.com/grafana/mimir/pkg/ingester/ingester_activity.go`.

use std::fmt::Write as _;

use mimir_proto::ingesterpb;

pub enum ActivityRequest<'a> {
    None,
    Query(&'a ingesterpb::QueryRequest),
    ExemplarQuery(&'a ingesterpb::ExemplarQueryRequest),
    LabelValues(&'a ingesterpb::LabelValuesRequest),
    LabelNames(&'a ingesterpb::LabelNamesRequest),
    UserStats(&'a ingesterpb::UserStatsRequest),
    MetricsForLabelMatchers(&'a ingesterpb::MetricsForLabelMatchersRequest),
    MetricsMetadata(&'a ingesterpb::MetricsMetadataRequest),
    LabelNamesAndValues(&'a ingesterpb::LabelNamesAndValuesRequest),
    LabelValuesCardinality(&'a ingesterpb::LabelValuesCardinalityRequest),
    ActiveSeries(&'a ingesterpb::ActiveSeriesRequest),
    SearchLabelNames(&'a ingesterpb::SearchLabelNamesRequest),
    SearchLabelValues(&'a ingesterpb::SearchLabelValuesRequest),
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
        ActivityRequest::ExemplarQuery(r) => {
            let mut s = String::with_capacity(128);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&ExemplarQueryRequest{{StartTimestampMs:{},EndTimestampMs:{},Matchers:[]*LabelMatchers{{",
                r.start_timestamp_ms, r.end_timestamp_ms
            );
            for m in &r.matchers {
                label_matchers_to_string(&mut s, Some(m));
                s.push(',');
            }
            s.push_str("},}");
            s
        }
        ActivityRequest::LabelValues(r) => {
            let mut s = String::with_capacity(128);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&LabelValuesRequest{{LabelName:{},StartTimestampMs:{},EndTimestampMs:{},Matchers:",
                r.label_name, r.start_timestamp_ms, r.end_timestamp_ms
            );
            label_matchers_to_string(&mut s, r.matchers.as_ref());
            let _ = write!(s, ",Limit:{},}}", r.limit);
            s
        }
        ActivityRequest::LabelNames(r) => {
            let mut s = String::with_capacity(128);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&LabelNamesRequest{{StartTimestampMs:{},EndTimestampMs:{},Matchers:",
                r.start_timestamp_ms, r.end_timestamp_ms
            );
            label_matchers_to_string(&mut s, r.matchers.as_ref());
            let _ = write!(s, ",Limit:{},}}", r.limit);
            s
        }
        ActivityRequest::UserStats(r) => {
            let mut s = String::with_capacity(96);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&UserStatsRequest{{CountMethod:"
            );
            push_count_method(&mut s, r.count_method);
            s.push_str(",}");
            s
        }
        ActivityRequest::MetricsForLabelMatchers(r) => {
            let mut s = String::with_capacity(128);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&MetricsForLabelMatchersRequest{{StartTimestampMs:{},EndTimestampMs:{},MatchersSet:[]*LabelMatchers{{",
                r.start_timestamp_ms, r.end_timestamp_ms
            );
            for m in &r.matchers_set {
                label_matchers_to_string(&mut s, Some(m));
                s.push(',');
            }
            let _ = write!(s, "}},Limit:{},}}", r.limit);
            s
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
        ActivityRequest::LabelNamesAndValues(r) => {
            let mut s = String::with_capacity(128);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&LabelNamesAndValuesRequest{{Matchers:[]*LabelMatcher{{"
            );
            for m in &r.matchers {
                label_matcher_to_string(&mut s, Some(m));
                s.push(',');
            }
            s.push_str("},CountMethod:");
            push_count_method(&mut s, r.count_method);
            s.push_str(",}");
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
            s.push_str("},CountMethod:");
            push_count_method(&mut s, r.count_method);
            s.push_str(",}");
            s
        }
        ActivityRequest::ActiveSeries(r) => {
            let mut s = String::with_capacity(128);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&ActiveSeriesRequest{{Matchers:[]*LabelMatcher{{"
            );
            for m in &r.matchers {
                label_matcher_to_string(&mut s, Some(m));
                s.push(',');
            }
            s.push_str("},Type:");
            match ingesterpb::active_series_request::RequestType::try_from(r.r#type) {
                Ok(ingesterpb::active_series_request::RequestType::Series) => s.push_str("SERIES"),
                Ok(ingesterpb::active_series_request::RequestType::NativeHistogramSeries) => {
                    s.push_str("NATIVE_HISTOGRAM_SERIES");
                }
                Err(_) => {
                    let _ = write!(s, "{}", r.r#type);
                }
            }
            s.push_str(",}");
            s
        }
        ActivityRequest::SearchLabelNames(r) => {
            let mut s = String::with_capacity(160);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&SearchLabelNamesRequest{{StartTimestampMs:{},EndTimestampMs:{},Matchers:[]*LabelMatcher{{",
                r.start_timestamp_ms, r.end_timestamp_ms
            );
            for m in &r.matchers {
                label_matcher_to_string(&mut s, Some(m));
                s.push(',');
            }
            s.push_str("},Filter:");
            search_filter_to_string(&mut s, r.filter.as_ref());
            s.push_str(",Ordering:");
            push_search_ordering(&mut s, r.ordering);
            let _ = write!(s, ",Limit:{},}}", r.limit);
            s
        }
        ActivityRequest::SearchLabelValues(r) => {
            let mut s = String::with_capacity(160);
            let _ = write!(
                s,
                "{name}: user={user_id:?} trace={trace_id:?} request=&SearchLabelValuesRequest{{StartTimestampMs:{},EndTimestampMs:{},Name:{},Matchers:[]*LabelMatcher{{",
                r.start_timestamp_ms, r.end_timestamp_ms, r.name
            );
            for m in &r.matchers {
                label_matcher_to_string(&mut s, Some(m));
                s.push(',');
            }
            s.push_str("},Filter:");
            search_filter_to_string(&mut s, r.filter.as_ref());
            s.push_str(",Ordering:");
            push_search_ordering(&mut s, r.ordering);
            let _ = write!(s, ",Limit:{},}}", r.limit);
            s
        }
    }
}

fn push_count_method(sb: &mut String, count_method: i32) {
    match ingesterpb::CountMethod::try_from(count_method) {
        Ok(ingesterpb::CountMethod::InMemory) => sb.push_str("IN_MEMORY"),
        Ok(ingesterpb::CountMethod::Active) => sb.push_str("ACTIVE"),
        Err(_) => {
            let _ = write!(sb, "{count_method}");
        }
    }
}

fn push_search_ordering(sb: &mut String, ordering: i32) {
    match ingesterpb::SearchOrdering::try_from(ordering) {
        Ok(ingesterpb::SearchOrdering::OrderByValueAsc) => sb.push_str("ORDER_BY_VALUE_ASC"),
        Ok(ingesterpb::SearchOrdering::OrderByValueDesc) => sb.push_str("ORDER_BY_VALUE_DESC"),
        Ok(ingesterpb::SearchOrdering::OrderByScoreDesc) => sb.push_str("ORDER_BY_SCORE_DESC"),
        Err(_) => {
            let _ = write!(sb, "{ordering}");
        }
    }
}

fn search_filter_to_string(sb: &mut String, filter: Option<&ingesterpb::SearchFilter>) {
    let Some(f) = filter else {
        sb.push_str("nil");
        return;
    };
    let _ = write!(
        sb,
        "&SearchFilter{{Terms:[{}],CaseInsensitive:{},FuzzAlg:",
        f.terms.join(" "),
        f.case_insensitive
    );
    match ingesterpb::search_filter::FuzzAlg::try_from(f.fuzz_alg) {
        Ok(ingesterpb::search_filter::FuzzAlg::Subsequence) => {
            sb.push_str("FUZZ_ALG_SUBSEQUENCE");
        }
        Ok(ingesterpb::search_filter::FuzzAlg::JaroWinkler) => {
            sb.push_str("FUZZ_ALG_JARO_WINKLER");
        }
        Ok(ingesterpb::search_filter::FuzzAlg::SubstringLeft) => {
            sb.push_str("FUZZ_ALG_SUBSTRING_LEFT");
        }
        Ok(ingesterpb::search_filter::FuzzAlg::Substring) => sb.push_str("FUZZ_ALG_SUBSTRING"),
        Err(_) => {
            let _ = write!(sb, "{}", f.fuzz_alg);
        }
    }
    let _ = write!(sb, ",FuzzThreshold:{},}}", f.fuzz_threshold);
}

fn label_matchers_to_string(sb: &mut String, matchers: Option<&ingesterpb::LabelMatchers>) {
    let Some(lm) = matchers else {
        sb.push_str("nil");
        return;
    };
    sb.push_str("&LabelMatchers{Matchers:[]*LabelMatcher{");
    for m in &lm.matchers {
        label_matcher_to_string(sb, Some(m));
        sb.push(',');
    }
    sb.push_str("},}");
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
    sb.push_str("&LabelMatcher{Type:");
    match ingesterpb::MatchType::try_from(m.r#type) {
        Ok(ingesterpb::MatchType::Equal) => sb.push_str("EQUAL"),
        Ok(ingesterpb::MatchType::NotEqual) => sb.push_str("NOT_EQUAL"),
        Ok(ingesterpb::MatchType::RegexMatch) => sb.push_str("REGEX_MATCH"),
        Ok(ingesterpb::MatchType::RegexNoMatch) => sb.push_str("REGEX_NO_MATCH"),
        Err(_) => {
            let _ = write!(sb, "{}", m.r#type);
        }
    }
    let _ = write!(sb, ",Name:{},Value:{},}}", m.name, m.value);
}
