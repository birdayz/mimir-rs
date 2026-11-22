//! Upstream Go conformance tests for prometheus/model/labels and prometheus/model/histogram.
//! Ported from github.com/prometheus/prometheus/model/labels/*_test.go
//! and github.com/prometheus/prometheus/model/histogram/*_test.go.

#![allow(clippy::approx_constant, clippy::excessive_precision, clippy::type_complexity)]

use std::collections::BTreeMap;

use bytes::Bytes;
use mimir_tsdb::chunkenc::histogram_model::{
    classic_bucket_counts, convert_nhcb_to_classic, custom_bucket_bounds_match, get_bound_exponential,
    reduce_resolution_generic, Bucket, ClassicSeriesCache, CounterResetHint,
    FloatHistogram, Histogram, HistogramErrorKind, HistogramSpan, NhcbRef, ReduceResolutionError,
    CLASSIC_SUFFIX_BUCKET, CLASSIC_SUFFIX_COUNT, CLASSIC_SUFFIX_SUM, CUSTOM_BUCKETS_SCHEMA,
};
use mimir_tsdb::chunkenc::STALE_NAN_BITS;
use mimir_tsdb::labels::cost::PromMatcher;
use mimir_tsdb::labels::prom_regexp::{
    contains_in_order, find_equal_or_prefix_string_matchers, find_set_matches,
    has_prefix_case_insensitive, is_simple_concatenation_pattern, new_equal_multi_string_matcher,
    optimize_concat_regex, optimize_equal_or_prefix_string_matchers, parse_go_syntax,
    prefix_case_insensitive_match_len, prefix_case_insensitive_match_len_bytes,
    string_matcher_from_regexp,
    suffix_case_insensitive_match_len, suffix_case_insensitive_match_len_bytes, to_normalised_lower,
    FastRegexMatcher, FastRegexMatcherCache, StringMatcher,
    MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD,
};
use mimir_tsdb::labels::{
    append_openmetrics_float, decode_stringlabel_size, encode_stringlabel_size,
    format_openmetrics_float, size_of_labels, Labels, LabelsBuilder, ScratchBuilder,
    ValidationScheme, METRIC_NAME,
};
use mimir_tsdb::matcher::{FastRegexMatcher as TsdbFastRegexMatcher, LabelMatcher, MatchType};

// ============================================================================
// model/labels tests (61 / 61)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/model/labels/cost_test.go::TestSingleMatchCost
#[test]
fn test_single_match_cost() {
    let long_regex1 = (0..420).map(|i| i.to_string()).collect::<Vec<_>>().join("|");
    let long_regex2 = (0..7049)
        .map(|i| format!("querier-burst-744b9bc4b8-{i:05x}"))
        .collect::<Vec<_>>()
        .join("|");
    let long_regex3 = format!(
        "({})",
        (0..1142)
            .map(|i| format!("synthetic-monitoring-agent-{i:04}-0000:agent:http-metrics"))
            .collect::<Vec<_>>()
            .join("|")
    );
    let long_regex4 = format!(
        "({})",
        (0..1028)
            .map(|i| format!("bigquery-datasource-grafana-app-{i:04}-0000"))
            .collect::<Vec<_>>()
            .join("|")
    );

    let cases: Vec<(f64, &str, MatchType, &str)> = vec![
        // = matchers
        (1.0, "__name__", MatchType::Equal, "mimir_target_series_per_ingester"),
        (1.0, "__name__", MatchType::Equal, "cortex_partition_ring_partitions"),
        (1.0, "container", MatchType::Equal, "distributor"),
        (1.0, "state", MatchType::Equal, "Active"),
        (1.0, "name", MatchType::Equal, "ingester-partitions"),
        (1.0, "__name__", MatchType::Equal, "cortex_distributor_samples_in_total"),
        (1.0, "__name__", MatchType::Equal, "namespace_user:cortex_ingester_owned_series:sum_filtered_max_over_time_1d"),
        (1.0, "__name__", MatchType::Equal, "kube_statefulset_replicas"),
        (1.0, "__name__", MatchType::Equal, "adaptive_metrics_canary_agg"),
        (1.0, "__name__", MatchType::Equal, "loki_distributor_bytes_received_total"),
        (1.0, "__name__", MatchType::Equal, "cortex_lifecycler_read_only"),
        (1.0, "cluster", MatchType::Equal, "ops-region-0"),
        (1.0, "job", MatchType::Equal, "integrations/db-o11y"),
        (1.0, "__name__", MatchType::Equal, "up"),
        (1.0, "namespace", MatchType::Equal, "prod-service-01"),
        (1.0, "namespace", MatchType::Equal, "prod-service-02"),
        // =~ matchers
        (20.0, "statefulset", MatchType::RegexMatch, "(ingester|mimir-write).*"),
        (1.0, "cluster", MatchType::RegexMatch, ".+"),
        (1.0, "cluster", MatchType::RegexMatch, ".*"),
        (1.1, "cluster", MatchType::RegexMatch, "prod-region-1"),
        (1.1, "tenant", MatchType::RegexMatch, "(29)"),
        (4.0, "partition", MatchType::RegexMatch, &long_regex1),
        (1.1, "cluster", MatchType::RegexMatch, "prod-region-2"),
        (1.0, "__name__", MatchType::RegexMatch, "aws_.+_info"),
        (1.1, "cluster", MatchType::RegexMatch, "prod-region-3"),
        (1.1, "cluster", MatchType::RegexMatch, "ops-region-0"),
        (1.1, "namespace", MatchType::RegexMatch, "prod-service-03"),
        (4.4, "reason", MatchType::RegexMatch, "(rate_limited|per_stream_rate_limit|blocked_ingestion|missing_enforced_labels)"),
        (1.1, "cluster", MatchType::RegexMatch, "prod-region-4"),
        (27.0, "route", MatchType::RegexMatch, "(prometheus|api_prom)_api_v1_.+"),
        (1.1, "namespace", MatchType::RegexMatch, "prod-service-04"),
        (1.1, "namespace", MatchType::RegexMatch, "ops-service-01"),
        (4.4, "route", MatchType::RegexMatch, "api_(v1|prom)_push|otlp_v1_metrics|api_v1_push_influx_write"),
        (7.0, "route", MatchType::RegexMatch, ".*v1.*|.*prom.*"),
        (1.0, "job", MatchType::RegexMatch, "(prod-service-05)/((gateway|cortex-gw.*))"),
        (1.0, "job", MatchType::RegexMatch, "(ops-service-01)/((compactor.*|cortex|mimir))"),
        (1.1, "namespace", MatchType::RegexMatch, "prod-service-06"),
        (1.0, "route", MatchType::RegexMatch, "(/base.Ruler/Rules|/indexgatewaypb.IndexGateway/GetChunkRef|/indexgatewaypb.IndexGateway/GetSeries|/indexgatewaypb.IndexGateway/GetShards|/indexgatewaypb.IndexGateway/GetStats|/indexgatewaypb.IndexGateway/GetVolume|/indexgatewaypb.IndexGateway/LabelNamesForMetricName|/indexgatewaypb.IndexGateway/LabelValuesForMetricName|/indexgatewaypb.IndexGateway/QueryIndex|/logproto.BloomGateway/FilterChunkRefs|/logproto.Pattern/Query|/logproto.Querier/GetChunkIDs|/logproto.Querier/GetDetectedLabels|/logproto.Querier/GetStats|/logproto.Querier/GetVolume|/logproto.Querier/Label|/logproto.Querier/Query|/logproto.Querier/QuerySample|/logproto.Querier/Series|/logproto.StreamData/GetStreamRates)"),
        (279.0, "route", MatchType::RegexMatch, "(base.Ruler/Rules|indexgatewaypb.IndexGateway/GetChunkRef|indexgatewaypb.IndexGateway/GetSeries|indexgatewaypb.IndexGateway/GetShards|indexgatewaypb.IndexGateway/GetStats|indexgatewaypb.IndexGateway/GetVolume|indexgatewaypb.IndexGateway/LabelNamesForMetricName|indexgatewaypb.IndexGateway/LabelValuesForMetricName|indexgatewaypb.IndexGateway/QueryIndex|logproto.BloomGateway/FilterChunkRefs|logproto.Pattern/Query|logproto.Querier/GetChunkIDs|logproto.Querier/GetDetectedLabels|logproto.Querier/GetStats|logproto.Querier/GetVolume|logproto.Querier/Label|logproto.Querier/Query|logproto.Querier/QuerySample|logproto.Querier/Series|logproto.StreamData/GetStreamRates)"),
        (4.0, "namespace", MatchType::RegexMatch, ".*prod.*"),
        (2.0, "namespace", MatchType::RegexMatch, ".*-.*-.*"),
        // != matchers
        (1.0, "job", MatchType::NotEqual, "integrations/db-o11y"),
        (1.0, "version", MatchType::NotEqual, "12.1.0-91295"),
        (1.0, "namespace", MatchType::NotEqual, "AWS/ECS"),
        (1.0, "container", MatchType::NotEqual, "istio-proxy"),
        (1.0, "owner_kind", MatchType::NotEqual, "ReplicaSet"),
        (1.0, "target", MatchType::NotEqual, "remote"),
        (1.0, "status", MatchType::NotEqual, "ok"),
        (1.0, "topic", MatchType::NotEqual, ""),
        // !~ matchers
        (1.0, "statefulset", MatchType::RegexNotMatch, "ingester-zone-.-partition"),
        (31.0, "topic", MatchType::RegexNotMatch, "(.+)-KSTREAM-AGGREGATE-STATE-STORE-(.+)"),
        (2.2, "workload_type", MatchType::RegexNotMatch, "job|cronjob"),
        (2.2, "created_by_kind", MatchType::RegexNotMatch, "Job|TaskRun"),
        (1.0, "namespace", MatchType::RegexNotMatch, "kube-.*"),
        (2.2, "job", MatchType::RegexNotMatch, "(ecs-dockerstats-exporter)|(vmagent)"),
        (11.0, "job", MatchType::RegexNotMatch, ".*envoy-stats.*"),
        (11.0, "db_name", MatchType::RegexNotMatch, "template.*|^$"),
        (11.0, "exported_job", MatchType::RegexNotMatch, ".*envoy-stats.*"),
        (5.5, "job", MatchType::RegexNotMatch, "integrations/(windows|node_exporter|unix|docker|db-o11y)"),
        (4.4, "job", MatchType::RegexNotMatch, "integrations/(windows|node_exporter|unix|docker)"),
        (2.2, "k8s_src_owner_type", MatchType::RegexNotMatch, "Pod|Node"),
        (2.2, "k8s_dst_owner_type", MatchType::RegexNotMatch, "Pod|Node"),
        (8.0, "image_spec", MatchType::RegexNotMatch, "(.*):1364de3"),
        (8.0, "image_spec", MatchType::RegexNotMatch, "(.*):cfc5ca8"),
        (1.1, "namespace", MatchType::RegexNotMatch, "(ops-service-02)"),
        (1.0, "slug", MatchType::RegexNotMatch, "ephemeral.*"),
        (8.0, "image_spec", MatchType::RegexNotMatch, "(.*):d849bcd"),
        (8.0, "image_spec", MatchType::RegexNotMatch, "(.*):cb8eaaa"),
        // long matchers
        (4.0, "pod", MatchType::RegexMatch, &long_regex2),
        (1.0, "pod", MatchType::RegexMatch, &long_regex3),
        (1.0, "pod", MatchType::RegexMatch, &long_regex4),
    ];

    for (expected_cost, name, mt, val) in cases {
        let m = PromMatcher::new(mt, name, val).unwrap();
        let got = m.single_match_cost();
        assert!(
            (got - expected_cost).abs() < 1e-9,
            "cost mismatch for {m}: expected {expected_cost}, got {got}"
        );
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/cost_test.go::TestSelectivity
#[test]
fn test_selectivity() {
    let cases = [
        (0u64, 1.0, "name", MatchType::Equal, "up"),
        (0, 1.0, "name", MatchType::Equal, ""),
        (0, 1.0, "name", MatchType::NotEqual, "up"),
        (0, 1.0, "name", MatchType::NotEqual, ""),
        (0, 1.0, "name", MatchType::RegexMatch, "up|kube_pod_info"),
        (0, 1.0, "name", MatchType::RegexMatch, ".+"),
        (0, 1.0, "name", MatchType::RegexNotMatch, "up|kube_pod_info"),
        (0, 1.0, "name", MatchType::RegexNotMatch, ".+"),
        (1, 1.0, "name", MatchType::RegexMatch, "up|kube_pod_info"),
        (1, 0.0, "name", MatchType::RegexNotMatch, "up|kube_pod_info"),
        (2, 1.0, "name", MatchType::RegexMatch, "up|kube_pod_info"),
        (8, 0.25, "name", MatchType::RegexMatch, "up|kube_pod_info"),
        (8, 0.75, "name", MatchType::RegexNotMatch, "up|kube_pod_info"),
        (8, 0.375, "name", MatchType::RegexMatch, "up|kube_pod_info|down"),
        (8, 0.125, "name", MatchType::Equal, "up"),
        (8, 0.875, "name", MatchType::NotEqual, "up"),
        (8, 0.125, "name", MatchType::RegexMatch, "up"),
        (8, 0.875, "name", MatchType::RegexNotMatch, "up"),
        (8, 0.0, "name", MatchType::RegexMatch, ""),
        (8, 1.0, "name", MatchType::RegexNotMatch, ""),
        (8, 0.0, "name", MatchType::Equal, ""),
        (8, 1.0, "name", MatchType::NotEqual, ""),
        (8, 1.0, "name", MatchType::RegexMatch, ".+"),
        (8, 0.0, "name", MatchType::RegexNotMatch, ".+"),
        (8, 0.1, "name", MatchType::RegexMatch, ".*(foo|bar).*"),
        (8, 0.9, "name", MatchType::RegexNotMatch, ".*(foo|bar).*"),
        (8, 0.1, "name", MatchType::RegexMatch, "(foo|bar).*"),
        (8, 0.1, "name", MatchType::RegexMatch, ".*foo.*|.*bar.*"),
        (u64::MAX, 0.0, "name", MatchType::Equal, "up"),
        (u64::MAX, 0.0, "name", MatchType::RegexMatch, "up|kube_pod_info"),
    ];

    for (num_series, expected, name, mt, val) in cases {
        let m = PromMatcher::new(mt, name, val).unwrap();
        let actual = m.estimate_selectivity(num_series);
        assert!(
            (actual - expected).abs() <= 1e-7,
            "{num_series} series {{{m}}}: expected {expected}, got {actual}"
        );
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/float_test.go::TestOpenMetricsFloatFormatting
#[test]
fn test_open_metrics_float_formatting() {
    let cases = [
        (0.0, "0.0"),
        (-0.0, "0.0"),
        (1.0, "1.0"),
        (-1.0, "-1.0"),
        (42.0, "42.0"),
        (-100.0, "-100.0"),
        (0.005, "0.005"),
        (1.5, "1.5"),
        (1e6, "1e+06"),
        (1e-5, "1e-05"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
    ];
    for (f, want) in cases {
        assert_eq!(format_openmetrics_float(f), want);
        let mut buf = Vec::new();
        append_openmetrics_float(&mut buf, f);
        assert_eq!(String::from_utf8(buf).unwrap(), want);
        let mut prefixed = b"le.e=".to_vec();
        append_openmetrics_float(&mut prefixed, f);
        assert_eq!(String::from_utf8(prefixed).unwrap(), format!("le.e={want}"));
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_dedupelabels_test.go::TestVarint
#[test]
fn test_varint() {
    let cases: &[(usize, &[u8])] = &[
        (0, &[0]),
        (1, &[1]),
        (2, &[2]),
        (254, &[254]),
        (255, &[255, 255, 0, 0]),
        (256, &[255, 0, 1, 0]),
        (0x7FFF, &[255, 0xFF, 0x7F, 0]),
        (0x8000, &[255, 0x00, 0x80, 0]),
        (0x3FFFFF, &[255, 0xFF, 0xFF, 0x3F]),
        (0xFFFFFF, &[255, 0xFF, 0xFF, 0xFF]),
    ];
    for &(v, expected) in cases {
        let mut buf = Vec::new();
        encode_stringlabel_size(&mut buf, v);
        assert_eq!(buf.as_slice(), expected);
        let (got, next) = decode_stringlabel_size(&buf, 0);
        assert_eq!(got, v);
        assert_eq!(next, buf.len());
    }
    let panic_res = std::panic::catch_unwind(|| {
        let mut buf = Vec::new();
        encode_stringlabel_size(&mut buf, 1 << 24);
    });
    assert!(panic_res.is_err());
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_slicelabels_test.go::TestScratchBuilderAdd_Strings
#[test]
fn test_scratch_builder_add_strings() {
    let mut name_buf = b"__name__".to_vec();
    let mut val_buf = b"metric1".to_vec();
    let mut sb = ScratchBuilder::new(0);
    sb.set_unsafe_add(true);
    sb.add(
        std::str::from_utf8(&name_buf).unwrap(),
        std::str::from_utf8(&val_buf).unwrap(),
    );
    let ret = sb.labels();
    name_buf[1] = b'?';
    val_buf[2] = b'?';
    assert_eq!(ret.get("__name__"), "metric1");
}

fn test_case_labels() -> Vec<Labels> {
    let s254 = "x".repeat(254);
    let s255 = "x".repeat(255);
    vec![
        Labels::from_strings(&["t1", "t1", "t2", "t2"]),
        Labels::empty(),
        Labels::from_strings(&["service.name", "t1", "whatever\\whatever", "t2"]),
        Labels::from_strings(&["aaa", "111", "xx", &s254]),
        Labels::from_strings(&["aaa", "111", "xx", &s255]),
        Labels::from_strings(&[
            "__name__", "kube_pod_container_status_last_terminated_exitcode",
            "cluster", "prod-af-north-0",
            " container", "prometheus",
            "instance", "kube-state-metrics-0:kube-state-metrics:ksm",
            "job", "kube-state-metrics/kube-state-metrics",
            " namespace", "observability-prometheus",
            "pod", "observability-prometheus-0",
            "uid", "d3ec90b2-4975-4607-b45d-b9ad64bb417e",
        ]),
    ]
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_String
#[test]
fn test_labels_string() {
    let s254 = "x".repeat(254);
    let s255 = "x".repeat(255);
    let cases = [
        (
            Labels::from_strings(&["t1", "t1", "t2", "t2"]),
            r#"{t1="t1", t2="t2"}"#.to_string(),
            r#"{t1="t1",t2="t2"}"#.to_string(),
        ),
        (Labels::empty(), "{}".to_string(), "{}".to_string()),
        (
            Labels::from_strings(&["service.name", "t1", "whatever\\whatever", "t2"]),
            r#"{"service.name"="t1", "whatever\\whatever"="t2"}"#.to_string(),
            r#"{"service.name"="t1","whatever\\whatever"="t2"}"#.to_string(),
        ),
        (
            Labels::from_strings(&["aaa", "111", "xx", &s254]),
            format!(r#"{{aaa="111", xx="{s254}"}}"#),
            format!(r#"{{aaa="111",xx="{s254}"}}"#),
        ),
        (
            Labels::from_strings(&["aaa", "111", "xx", &s255]),
            format!(r#"{{aaa="111", xx="{s255}"}}"#),
            format!(r#"{{aaa="111",xx="{s255}"}}"#),
        ),
        (
            Labels::from_strings(&[
                "__name__", "kube_pod_container_status_last_terminated_exitcode",
                "cluster", "prod-af-north-0",
                " container", "prometheus",
                "instance", "kube-state-metrics-0:kube-state-metrics:ksm",
                "job", "kube-state-metrics/kube-state-metrics",
                " namespace", "observability-prometheus",
                "pod", "observability-prometheus-0",
                "uid", "d3ec90b2-4975-4607-b45d-b9ad64bb417e",
            ]),
            r#"{" container"="prometheus", " namespace"="observability-prometheus", __name__="kube_pod_container_status_last_terminated_exitcode", cluster="prod-af-north-0", instance="kube-state-metrics-0:kube-state-metrics:ksm", job="kube-state-metrics/kube-state-metrics", pod="observability-prometheus-0", uid="d3ec90b2-4975-4607-b45d-b9ad64bb417e"}"#.to_string(),
            r#"{" container"="prometheus"," namespace"="observability-prometheus",__name__="kube_pod_container_status_last_terminated_exitcode",cluster="prod-af-north-0",instance="kube-state-metrics-0:kube-state-metrics:ksm",job="kube-state-metrics/kube-state-metrics",pod="observability-prometheus-0",uid="d3ec90b2-4975-4607-b45d-b9ad64bb417e"}"#.to_string(),
        ),
        (
            Labels::from_strings(&["a", "1, 2", "b", "3"]),
            r#"{a="1, 2", b="3"}"#.to_string(),
            r#"{a="1, 2",b="3"}"#.to_string(),
        ),
    ];
    for (lbls, exp_str, exp_no_space) in cases {
        assert_eq!(lbls.to_string(), exp_str);
        assert_eq!(lbls.string_no_space(), exp_no_space);
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestSizeOfLabels
#[test]
fn test_size_of_labels() {
    let expected = [12u64, 0, 37, 266, 270, 309];
    let cases = test_case_labels();
    assert_eq!(expected.len(), cases.len());
    for (exp, lbls) in expected.iter().zip(cases.iter()) {
        let mut total = 0u64;
        lbls.range(|k, v| {
            total += size_of_labels(k, v, 1);
        });
        assert_eq!(total, *exp);
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestByteSize
#[test]
fn test_byte_size() {
    let expected = [12u64, 0, 37, 266, 270, 309];
    let cases = test_case_labels();
    assert_eq!(expected.len(), cases.len());
    for (exp, lbls) in expected.iter().zip(cases.iter()) {
        assert_eq!(lbls.byte_size(), *exp);
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_MatchLabels
#[test]
fn test_labels_match_labels() {
    let labels = Labels::from_strings(&[
        "__name__", "ALERTS",
        "alertname", "HTTPRequestRateLow",
        "alertstate", "pending",
        "instance", "0",
        "job", "app-server",
        "severity", "critical",
    ]);
    let cases = [
        (
            vec!["__name__", "alertname", "alertstate", "instance"],
            true,
            Labels::from_strings(&[
                "__name__", "ALERTS",
                "alertname", "HTTPRequestRateLow",
                "alertstate", "pending",
                "instance", "0",
            ]),
        ),
        (
            vec!["__name__", "alertname", "alertstate", "instance"],
            false,
            Labels::from_strings(&["job", "app-server", "severity", "critical"]),
        ),
        (
            vec!["alertname", "alertstate", "instance"],
            true,
            Labels::from_strings(&[
                "alertname", "HTTPRequestRateLow",
                "alertstate", "pending",
                "instance", "0",
            ]),
        ),
        (
            vec!["alertname", "alertstate", "instance"],
            false,
            Labels::from_strings(&["job", "app-server", "severity", "critical"]),
        ),
    ];
    for (names, on, expected) in cases {
        let got = labels.match_labels(on, &names);
        assert!(Labels::equal(&expected, &got));
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_HasDuplicateLabelNames
#[test]
fn test_labels_has_duplicate_label_names() {
    let no_dup = Labels::from_map([("__name__", "up"), ("hostname", "localhost")]);
    assert_eq!(no_dup.has_duplicate_label_names(), None);

    let dup = Labels::from_strings(&[
        "__name__", "up",
        "hostname", "localhost",
        "hostname", "127.0.0.1",
    ]);
    assert_eq!(dup.has_duplicate_label_names(), Some("hostname"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_WithoutEmpty
#[test]
fn test_labels_without_empty() {
    let cases = [
        (Labels::from_strings(&["foo", "", "bar", ""]), Labels::empty()),
        (Labels::from_strings(&["foo", "", "bar", "", "baz", ""]), Labels::empty()),
        (
            Labels::from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
            Labels::from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
        (
            Labels::from_strings(&["__name__", "test", "hostname", "localhost", "bar", "", "job", "check"]),
            Labels::from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
        (
            Labels::from_strings(&["__name__", "test", "foo", "", "hostname", "localhost", "bar", "", "job", "check"]),
            Labels::from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
        (
            Labels::from_strings(&["__name__", "test", "foo", "", "baz", "", "hostname", "localhost", "bar", "", "job", "check"]),
            Labels::from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
    ];
    for (input, expected) in cases {
        assert!(Labels::equal(&expected, &input.without_empty()));
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_IsValid
#[test]
fn test_labels_is_valid() {
    let cases = [
        (Labels::from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]), true),
        (Labels::from_strings(&["__name__", "test:ms", "hostname_123", "localhost", "_job", "check"]), true),
        (Labels::from_strings(&["__name__", "test-ms"]), false),
        (Labels::from_strings(&["__name__", "0zz"]), false),
        (Labels::from_strings(&["abc:xyz", "invalid"]), false),
        (Labels::from_strings(&["123abc", "invalid"]), false),
        (Labels::from_strings(&["中文abc", "invalid"]), false),
    ];
    for (input, expected) in cases {
        assert_eq!(input.is_valid(ValidationScheme::Legacy), expected);
    }
    // Non-UTF-8 label values are rejected at construction in Rust Labels.
    let bad1 = Bytes::from_static(&[7, b'i', b'n', b'v', b'a', b'l', b'i', b'd', 3, b'a', b'a', 0xe2]);
    let bad2 = Bytes::from_static(&[7, b'i', b'n', b'v', b'a', b'l', b'i', b'd', 4, 0xf7, 0xbf, 0xbf, 0xbf]);
    assert!(Labels::try_from_raw_stringlabels_bytes(bad1).is_err());
    assert!(Labels::try_from_raw_stringlabels_bytes(bad2).is_err());
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_ValidationModes
#[test]
fn test_labels_validation_modes() {
    assert!(Labels::from_strings(&["__name__", "test.metric", "hostname", "localhost", "job", "check"]).is_valid(ValidationScheme::Utf8));
    assert!(Labels::from_strings(&["__name__", "test", "host.name", "localhost", "team/name", "platform"]).is_valid(ValidationScheme::Utf8));
    let bad_utf8_name = Bytes::from_static(&[9, 0xc5, b' ', b'b', b'a', b'd', b' ', b'u', b't', b'f', b'8', 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't']);
    assert!(Labels::try_from_raw_stringlabels_bytes(bad_utf8_name).is_err());
    assert!(!Labels::from_strings(&["__name__", "test.utf8.metric", "hostname", "localhost", "job", "check"]).is_valid(ValidationScheme::Legacy));
    assert!(!Labels::from_strings(&["__name__", "test", "host.name", "localhost", "job", "check"]).is_valid(ValidationScheme::Legacy));
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Equal
#[test]
fn test_labels_equal() {
    let labels = Labels::from_strings(&["aaa", "111", "bbb", "222"]);
    assert!(!Labels::equal(&labels, &Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"])));
    assert!(!Labels::equal(&labels, &Labels::from_strings(&["aaa", "111", "bar", "222"])));
    assert!(!Labels::equal(&labels, &Labels::from_strings(&["aaa", "111", "bbb", "233"])));
    assert!(Labels::equal(&labels, &Labels::from_strings(&["aaa", "111", "bbb", "222"])));
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_FromStrings
#[test]
fn test_labels_from_strings() {
    let labels = Labels::from_strings(&["aaa", "111", "bbb", "222"]);
    let mut x = 0;
    labels.range(|k, v| {
        match x {
            0 => assert_eq!((k, v), ("aaa", "111")),
            1 => assert_eq!((k, v), ("bbb", "222")),
            _ => panic!("unexpected label {x}: {k}={v}"),
        }
        x += 1;
    });
    assert!(std::panic::catch_unwind(|| Labels::from_strings(&["aaa", "111", "bbb"])).is_err());
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Compare
#[test]
fn test_labels_compare() {
    let labels = Labels::from_strings(&["aaa", "111", "bbb", "222"]);
    let cases = [
        (Labels::from_strings(&["aaa", "110", "bbb", "222"]), 1),
        (Labels::from_strings(&["aaa", "111", "bbb", "233"]), -1),
        (Labels::from_strings(&["aaa", "111", "bar", "222"]), 1),
        (Labels::from_strings(&["aaa", "111", "bbc", "222"]), -1),
        (Labels::from_strings(&["aaa", "111", "bb", "222"]), 1),
        (Labels::from_strings(&["aaa", "111", "bbbb", "222"]), -1),
        (Labels::from_strings(&["aaa", "111"]), 1),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333", "ddd", "444"]), -1),
        (Labels::from_strings(&["aaa", "111", "bbb", "222"]), 0),
        (Labels::empty(), 1),
    ];
    for (compared, expected) in cases {
        assert_eq!(Labels::compare(&labels, &compared), expected);
        assert_eq!(Labels::compare(&compared, &labels), -expected);
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Has
#[test]
fn test_labels_has() {
    let set = Labels::from_strings(&["aaa", "111", "bbb", "222"]);
    assert!(!set.has("foo"));
    assert!(set.has("aaa"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Get
#[test]
fn test_labels_get() {
    assert_eq!(Labels::from_strings(&["aaa", "111", "bbb", "222"]).get("foo"), "");
    assert_eq!(Labels::from_strings(&["aaaa", "111", "bbb", "222"]).get("aaaa"), "111");
    assert_eq!(Labels::from_strings(&["aaaa", "111", "bbb", "222"]).get("bbb"), "222");
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_DropMetricName
#[test]
fn test_labels_drop_metric_name() {
    assert!(Labels::equal(
        &Labels::from_strings(&["aaa", "111", "bbb", "222"]),
        &Labels::from_strings(&["aaa", "111", "bbb", "222"]).drop_metric_name()
    ));
    assert!(Labels::equal(
        &Labels::from_strings(&["aaa", "111"]),
        &Labels::from_strings(&[METRIC_NAME, "myname", "aaa", "111"]).drop_metric_name()
    ));
    let original = Labels::from_strings(&["__aaa__", "111", METRIC_NAME, "myname", "bbb", "222"]);
    let check = original.copy();
    assert!(Labels::equal(
        &Labels::from_strings(&["__aaa__", "111", "bbb", "222"]),
        &check.drop_metric_name()
    ));
    assert!(Labels::equal(&original, &check));
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_DropReserved
#[test]
fn test_labels_drop_reserved() {
    let should_drop = |n: &str| n == METRIC_NAME || n == "__something__";
    assert!(Labels::equal(
        &Labels::from_strings(&["aaa", "111", "bbb", "222"]),
        &Labels::from_strings(&["aaa", "111", "bbb", "222"]).drop_reserved(should_drop)
    ));
    assert!(Labels::equal(
        &Labels::from_strings(&["aaa", "111"]),
        &Labels::from_strings(&[METRIC_NAME, "myname", "aaa", "111"]).drop_reserved(should_drop)
    ));
    assert!(Labels::equal(
        &Labels::from_strings(&["aaa", "111"]),
        &Labels::from_strings(&[METRIC_NAME, "myname", "__something__", "counter", "aaa", "111"]).drop_reserved(should_drop)
    ));
    let original = Labels::from_strings(&["__aaa__", "111", METRIC_NAME, "myname", "bbb", "222"]);
    let check = original.copy();
    assert!(Labels::equal(
        &Labels::from_strings(&["__aaa__", "111", "bbb", "222"]),
        &check.drop_reserved(should_drop)
    ));
    assert!(Labels::equal(&original, &check));
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Copy
#[test]
fn test_labels_copy() {
    assert_eq!(
        Labels::from_strings(&["aaa", "111", "bbb", "222"]),
        Labels::from_strings(&["aaa", "111", "bbb", "222"]).copy()
    );
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Map
#[test]
fn test_labels_map() {
    let mut expected = BTreeMap::new();
    expected.insert("aaa".to_string(), "111".to_string());
    expected.insert("bbb".to_string(), "222".to_string());
    assert_eq!(Labels::from_strings(&["aaa", "111", "bbb", "222"]).map(), expected);
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_BytesWithLabels
#[test]
fn test_labels_bytes_with_labels() {
    let mut exp = Vec::new();
    Labels::from_strings(&["aaa", "111", "bbb", "222"]).bytes(&mut exp);
    let mut got = Vec::new();
    Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]).bytes_with_labels(&mut got, &["aaa", "bbb"]);
    assert_eq!(got, exp);

    let mut exp_empty = Vec::new();
    Labels::empty().bytes(&mut exp_empty);
    Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]).bytes_with_labels(&mut got, &[]);
    assert_eq!(got, exp_empty);
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_BytesWithoutLabels
#[test]
fn test_labels_bytes_without_labels() {
    let mut exp = Vec::new();
    let mut got = Vec::new();

    Labels::from_strings(&["aaa", "111"]).bytes(&mut exp);
    Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]).bytes_without_labels(&mut got, &["bbb", "ccc"]);
    assert_eq!(got, exp);

    Labels::from_strings(&[METRIC_NAME, "333", "aaa", "111"]).bytes(&mut exp);
    Labels::from_strings(&[METRIC_NAME, "333", "aaa", "111", "bbb", "222"]).bytes_without_labels(&mut got, &["bbb"]);
    assert_eq!(got, exp);

    Labels::from_strings(&["aaa", "111"]).bytes(&mut exp);
    Labels::from_strings(&[METRIC_NAME, "333", "aaa", "111", "bbb", "222"]).bytes_without_labels(&mut got, &[METRIC_NAME, "bbb"]);
    assert_eq!(got, exp);
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestBuilder
#[test]
fn test_builder() {
    let cases = [
        (Labels::from_strings(&["aaa", "111"]), vec![], vec![], vec![], Labels::from_strings(&["aaa", "111"])),
        (Labels::empty(), vec![], vec![], vec![("aaa", "444"), ("bbb", "555"), ("ccc", "666")], Labels::from_strings(&["aaa", "444", "bbb", "555", "ccc", "666"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec![], vec![], vec![("aaa", "444"), ("bbb", "555"), ("ccc", "666")], Labels::from_strings(&["aaa", "444", "bbb", "555", "ccc", "666"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec!["bbb"], vec![], vec![], Labels::from_strings(&["aaa", "111", "ccc", "333"])),
        (Labels::empty(), vec!["bbb"], vec![], vec![("aaa", "111"), ("bbb", "222"), ("ccc", "333")], Labels::from_strings(&["aaa", "111", "ccc", "333"])),
        (Labels::from_strings(&["aaa", "111"]), vec![], vec![], vec![("bbb", "222")], Labels::from_strings(&["aaa", "111", "bbb", "222"])),
        (Labels::from_strings(&["aaa", "111"]), vec![], vec![], vec![("bbb", "222"), ("bbb", "333")], Labels::from_strings(&["aaa", "111", "bbb", "333"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec!["bbb"], vec![], vec![("ddd", "444")], Labels::from_strings(&["aaa", "111", "ccc", "333", "ddd", "444"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "", "ccc", "333"]), vec![], vec![], vec![], Labels::from_strings(&["aaa", "111", "ccc", "333"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec![], vec![], vec![("bbb", "")], Labels::from_strings(&["aaa", "111", "ccc", "333"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec![], vec!["bbb"], vec![], Labels::from_strings(&["bbb", "222"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec![], vec!["aaa", "ccc"], vec![], Labels::from_strings(&["aaa", "111", "ccc", "333"])),
        (Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), vec!["bbb"], vec!["aaa", "ddd"], vec![("ddd", "444")], Labels::from_strings(&["aaa", "111", "ddd", "444"])),
    ];

    let mut reuse = LabelsBuilder::new(Labels::empty());
    for (base, del, keep, set, want) in cases {
        for builder in [LabelsBuilder::new(base.clone()), { reuse.reset(base.clone()); reuse.clone() }] {
            let mut b = builder;
            for (k, v) in &set {
                b.set(k, v);
            }
            if !keep.is_empty() {
                b.keep(&keep);
            }
            b.del(&del);
            assert!(Labels::equal(&want, &b.labels()));

            let mut to_del = Vec::new();
            b.range(|k, _| {
                if k == "aaa" || k == "bbb" {
                    to_del.push(k.to_string());
                }
            });
            for k in &to_del {
                b.del(&[k.as_str()]);
            }
            let mut exp_bytes = Vec::new();
            want.bytes_without_labels(&mut exp_bytes, &["aaa", "bbb"]);
            let mut got_bytes = Vec::new();
            b.labels().bytes(&mut got_bytes);
            assert_eq!(got_bytes, exp_bytes);
        }
    }

    let mut b = LabelsBuilder::new(Labels::from_strings(&["aaa", "111"]));
    b.del(&["bbb"]);
    b.set("bbb", "222");
    assert_eq!(b.labels(), Labels::from_strings(&["aaa", "111", "bbb", "222"]));
    assert_eq!(b.get("bbb"), "222");
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestScratchBuilder
#[test]
fn test_scratch_builder() {
    let cases = [
        (vec![], Labels::empty()),
        (vec![("aaa", "111")], Labels::from_strings(&["aaa", "111"])),
        (vec![("aaa", "111"), ("bbb", "222"), ("ccc", "333")], Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"])),
        (vec![("bbb", "222"), ("aaa", "111"), ("ccc", "333")], Labels::from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"])),
        (vec![("ddd", "444")], Labels::from_strings(&["ddd", "444"])),
    ];
    for (add, want) in cases {
        let mut b = ScratchBuilder::new(add.len());
        for (k, v) in add {
            b.add(k, v);
        }
        b.sort();
        assert!(Labels::equal(&want, &b.labels()));
        b.assign(want.clone());
        assert!(Labels::equal(&want, &b.labels()));
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_Hash
#[test]
fn test_labels_hash() {
    let lbls = Labels::from_strings(&["foo", "bar", "baz", "qux"]);
    assert_eq!(lbls.hash(), lbls.hash());
    assert_ne!(lbls.hash(), Labels::from_strings(&["foo", "bar"]).hash());
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_HashForLabels
#[test]
fn test_labels_hash_for_labels() {
    let lbls1 = Labels::from_strings(&["__name__", "http_requests_total", "a", "1", "b", "2", "c", "3"]);
    let lbls2 = Labels::from_strings(&["__name__", "different_metric", "a", "1", "b", "999", "c", "3"]);
    let mut b1 = Vec::new();
    let mut b2 = Vec::new();
    let h1 = lbls1.hash_for_labels(&mut b1, &["a", "c"]);
    let h2 = lbls1.hash_for_labels(&mut b2, &["a", "c"]);
    assert_eq!(h1, h2);
    assert_eq!(b1, b2);

    let mut b3 = Vec::new();
    let h3 = lbls2.hash_for_labels(&mut b3, &["a", "c"]);
    assert_eq!(h1, h3);
    assert_eq!(b1, b3);

    let lbls3 = Labels::from_strings(&["__name__", "http_requests_total", "a", "999", "b", "2", "c", "3"]);
    let mut b4 = Vec::new();
    assert_ne!(h1, lbls3.hash_for_labels(&mut b4, &["a", "c"]));

    let mut b_int = Vec::new();
    assert_eq!(h1, lbls1.hash_for_labels(&mut b_int, &["0", "a", "bb", "c", "z"]));

    let mut b5 = Vec::new();
    let h5 = lbls1.hash_for_labels(&mut b5, &["nonexistent", "z"]);
    assert!(b5.is_empty());
    let mut b6 = Vec::new();
    assert_eq!(h5, lbls2.hash_for_labels(&mut b6, &["nonexistent", "z"]));
    let mut b_emp = Vec::new();
    assert_eq!(h5, Labels::empty().hash_for_labels(&mut b_emp, &[]));

    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(b"stalebytes");
    let orig_ptr = buf.as_ptr();
    let h_buf = lbls1.hash_for_labels(&mut buf, &["a", "c"]);
    assert_eq!(h_buf, h1);
    assert_eq!(buf.as_ptr(), orig_ptr);
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestLabels_HashWithoutLabels
#[test]
fn test_labels_hash_without_labels() {
    let lbls1 = Labels::from_strings(&["__name__", "http_requests_total", "a", "1", "b", "2", "c", "3"]);
    let lbls2 = Labels::from_strings(&["__name__", "other_metric_name", "a", "1", "b", "999", "c", "3"]);

    let mut b1 = Vec::new();
    let mut b2 = Vec::new();
    let h1 = lbls1.hash_without_labels(&mut b1, &["b"]);
    let h2 = lbls1.hash_without_labels(&mut b2, &["b"]);
    assert_eq!(h1, h2);
    assert_eq!(b1, b2);

    let mut b_for = Vec::new();
    assert_eq!(h1, lbls1.hash_for_labels(&mut b_for, &["a", "c"]));
    let mut b_mn = Vec::new();
    assert_eq!(h1, lbls1.hash_without_labels(&mut b_mn, &[METRIC_NAME, "b"]));
    let mut b_ne = Vec::new();
    assert_eq!(h1, lbls1.hash_without_labels(&mut b_ne, &["0", "b", "z"]));

    let mut b3 = Vec::new();
    assert_eq!(h1, lbls2.hash_without_labels(&mut b3, &["b"]));
    assert_eq!(b1, b3);

    let lbls3 = Labels::from_strings(&["__name__", "http_requests_total", "a", "999", "b", "2", "c", "3"]);
    let mut b4 = Vec::new();
    assert_ne!(h1, lbls3.hash_without_labels(&mut b4, &["b"]));

    let mut b5 = Vec::new();
    let h5 = lbls1.hash_without_labels(&mut b5, &["a", "b", "c"]);
    assert!(b5.is_empty());
    let mut b_emp = Vec::new();
    assert_eq!(h5, Labels::empty().hash_without_labels(&mut b_emp, &[]));

    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(b"stalebytes");
    let orig_ptr = buf.as_ptr();
    let h_buf = lbls1.hash_without_labels(&mut buf, &["b"]);
    assert_eq!(h_buf, h1);
    assert_eq!(buf.as_ptr(), orig_ptr);
}

// Upstream: github.com/prometheus/prometheus/model/labels/labels_test.go::TestMarshaling
#[test]
fn test_marshaling() {
    let lbls = Labels::from_strings(&["aaa", "111", "bbb", "2222", "ccc", "33333"]);
    let expected_json = r#"{"aaa":"111","bbb":"2222","ccc":"33333"}"#;
    let json_str = serde_json::to_string(&lbls).unwrap();
    assert_eq!(json_str, expected_json);
    let got: Labels = serde_json::from_str(&json_str).unwrap();
    assert_eq!(got, lbls);

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Foo {
        #[serde(default, skip_serializing_if = "Labels::is_empty")]
        a_labels: Labels,
    }
    let f = Foo { a_labels: lbls.clone() };
    let fj = serde_json::to_string(&f).unwrap();
    assert_eq!(fj, format!(r#"{{"a_labels":{expected_json}}}"#));
    let got_f: Foo = serde_json::from_str(&fj).unwrap();
    assert_eq!(got_f, f);

    let empty_f = Foo { a_labels: Labels::empty() };
    assert_eq!(serde_json::to_string(&empty_f).unwrap(), "{}");
}

// Upstream: github.com/prometheus/prometheus/model/labels/matcher_test.go::TestMatcher
#[test]
fn test_matcher() {
    let cases = [
        (MatchType::Equal, "bar", "bar", true),
        (MatchType::Equal, "bar", "foo-bar", false),
        (MatchType::NotEqual, "bar", "bar", false),
        (MatchType::NotEqual, "bar", "foo-bar", true),
        (MatchType::RegexMatch, "bar", "bar", true),
        (MatchType::RegexMatch, "bar", "foo-bar", false),
        (MatchType::RegexMatch, ".*bar", "foo-bar", true),
        (MatchType::RegexNotMatch, "bar", "bar", false),
        (MatchType::RegexNotMatch, "bar", "foo-bar", true),
        (MatchType::RegexNotMatch, ".*bar", "foo-bar", false),
        (MatchType::RegexMatch, "$*bar", "foo-bar", false),
        (MatchType::RegexMatch, "bar^+", "foo-bar", false),
        (MatchType::RegexMatch, "$+bar", "foo-bar", false),
    ];
    for (mt, pat, val, expected) in cases {
        let pm = PromMatcher::must_new(mt, "test_label_name", pat);
        assert_eq!(pm.matches(val), expected, "PromMatcher {pm} on {val:?}");
        let lm = LabelMatcher::new(mt, "test_label_name", pat).unwrap();
        assert_eq!(lm.matches(val), expected, "LabelMatcher {lm} on {val:?}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/matcher_test.go::TestInverse
#[test]
fn test_inverse() {
    let cases = [
        (MatchType::Equal, "name1", "value1", MatchType::NotEqual),
        (MatchType::NotEqual, "name2", "value2", MatchType::Equal),
        (MatchType::RegexMatch, "name3", "value3.*", MatchType::RegexNotMatch),
        (MatchType::RegexNotMatch, "name4", "value4.*", MatchType::RegexMatch),
    ];
    for (mt, name, val, exp) in cases {
        let pm = PromMatcher::must_new(mt, name, val);
        assert_eq!(pm.inverse().unwrap().match_type, exp);
        let lm = LabelMatcher::new(mt, name, val).unwrap();
        assert_eq!(lm.inverse().unwrap().match_type, exp);
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/matcher_test.go::TestPrefix
#[test]
fn test_prefix() {
    let cases = [
        (MatchType::Equal, "abc", "", false),
        (MatchType::NotEqual, "abc", "", false),
        (MatchType::RegexMatch, "abc.+", "abc", false),
        (MatchType::RegexMatch, "abcd|abc.+", "abc", false),
        (MatchType::RegexNotMatch, "abcd|abc.+", "abc", false),
        (MatchType::RegexMatch, "abc(def|ghj)|ab|a.", "a", false),
        (MatchType::RegexMatch, "foo.+bar|foo.*baz", "foo", false),
        (MatchType::RegexMatch, "abc|.*", "", false),
        (MatchType::RegexMatch, "abc|def", "", false),
        (MatchType::RegexMatch, ".+def", "", false),
        (MatchType::RegexNotMatch, "(?i)abc.+", "", true),
    ];
    for (mt, pat, exp_prefix, exp_ci) in cases {
        let pm = PromMatcher::must_new(mt, "test_label_name", pat);
        assert_eq!(pm.prefix(), exp_prefix, "prefix for {pat}");
        assert_eq!(pm.has_case_insensitive_prefix(), exp_ci, "ci prefix for {pat}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/matcher_test.go::TestIsRegexOptimized
#[test]
fn test_is_regex_optimized() {
    assert!(!PromMatcher::must_new(MatchType::Equal, "x", "abc").is_regex_optimized());
    assert!(!PromMatcher::must_new(MatchType::RegexMatch, "x", ".").is_regex_optimized());
    assert!(PromMatcher::must_new(MatchType::RegexMatch, "x", "abc.+").is_regex_optimized());

    assert!(!LabelMatcher::new(MatchType::Equal, "x", "abc").unwrap().is_regex_optimized());
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", ".").unwrap().is_regex_optimized());
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "abc.+").unwrap().is_regex_optimized());
}

// Upstream: github.com/prometheus/prometheus/model/labels/matcher_test.go::TestMatcher_Helpers
#[test]
fn test_matcher_helpers() {
    let cases: &[(&str, MatchType, &str, &str, &str, Option<&[&str]>)] = &[
        ("MatchEqual", MatchType::Equal, "service", "api", "", None),
        ("MatchNotEqual", MatchType::NotEqual, "service", "api", "", None),
        ("MatchRegexp set", MatchType::RegexMatch, "zone", "us-east-1|us-west-2", "us-east-1|us-west-2", Some(&["us-east-1", "us-west-2"])),
        ("MatchRegexp wildcard", MatchType::RegexMatch, "env", "prod.*", "prod.*", None),
        ("MatchNotRegexp set", MatchType::RegexNotMatch, "zone", "us-east-1|us-west-2", "us-east-1|us-west-2", Some(&["us-east-1", "us-west-2"])),
        ("MatchNotRegexp wildcard", MatchType::RegexNotMatch, "env", "prod.*", "prod.*", None),
    ];
    for &(name, mt, ln, lv, exp_re, exp_set) in cases {
        let pm = PromMatcher::must_new(mt, ln, lv);
        assert_eq!(pm.match_type, mt, "{name}");
        assert_eq!(pm.name, ln, "{name}");
        assert_eq!(pm.value, lv, "{name}");
        assert_eq!(pm.get_regex_string(), exp_re, "{name}");
        match (exp_set, pm.set_matches()) {
            (None, None) => {}
            (Some(exp), Some(mut got)) => {
                let mut exp_v: Vec<String> = exp.iter().map(|s| s.to_string()).collect();
                exp_v.sort();
                got.sort();
                assert_eq!(got, exp_v, "{name}");
            }
            (e, g) => panic!("{name}: expected {e:?}, got {g:?}"),
        }

        let lm = LabelMatcher::new(mt, ln, lv).unwrap();
        assert_eq!(lm.regex_string(), exp_re);
    }

    assert!(std::panic::catch_unwind(|| PromMatcher::must_new(MatchType::RegexMatch, "invalid", "[open-bracket")).is_err());
    assert!(std::panic::catch_unwind(|| PromMatcher::must_new(MatchType::RegexNotMatch, "invalid", "[open-bracket")).is_err());
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestFastRegexMatcher_MatchString
#[test]
fn test_fast_regex_matcher_match_string() {
    let f20 = "f".repeat(20);
    let test_values = vec![
        "foo", " foo bar", "bar", "buzz\nbar", "bar foo", "bfoo", "\n", "\nfoo", "foo\n",
        "hello foo world", "hello foo\n world", "", "FOO", "Foo", "fOo", "foO", "OO", "Oo",
        "\nfoo\n", &f20, "prometheus", "prometheus_api_v1", "prometheus_api_v1_foo",
        "10.0.1.20", "10.0.2.10", "10.0.3.30", "10.0.4.40",
        "report.scheduled.job_runscheduledreports", "Report.Scheduled.JobRunScheduledReports",
        "Report.Scheduled.Job_RunScheduledReports", "foofoo0", "foofoo", "😀foo0", "ſſs", "ſſS",
        "AAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBB", "cccccccccccccccccccccccC",
        "ſſſſſſſſſſſſſſſſſſſſſſſſS", "SSSSSSSSSSSSSSSSSSSSSSSSſ",
        "a-b-c-d-e", "aaaaaa-bbbbbb-cccccc-dddddd-eeeeee", "aaaaaa----eeeeee", "----", "-a-a-a-",
        "|foo|", "|foo-bar|", "x-ab-y", "x-abc-y",
        "zQPbMkNO", "zQPbMkNo", "jyyfj00j0061", "jyyfj00j006", "jyyfj00j00612",
        "NNSPdvMi", "NNSPdvMiXXX", "NNSPdvMixxx", "nnSPdvMi", "nnSPdvMiXXX",
        "\u{212a}", "\u{212a}a", "\u{212a}b", "\u{212a}-a", "\u{212a}-abc", "\u{212a}elvin",
        "abc\u{212a}", "a\u{212a}", "axſk", "axs\u{212a}", "ſome-pattern", "SOME-pattern", "ſk", "kſ",
    ];

    let regexes = [
        "", "()", "foo", "foo()", "^foo", "(foo|bar)", "foo.*", ".*foo", "^.*foo$", "^.+foo$",
        ".?", ".*", "().*", ".*()", "().*()", ".+", ".+()", "foo.+", ".+foo", "foo\n.+",
        "foo\n.*", ".*foo.*", ".+foo.+", ".*foo.*|", "(?i).*foo.*", ".*foo.*|bar.*",
        "foo.*|.*bar.*", ".*foo.*|.*bar.*", ".*foo.*bar.*|.*hello.*", ".*foo.*|.*bar.*|.*hello.*",
        ".+.*foo.*|.*bar.*", "(?s:.*)", "(?s:.+)", "(?s:^.*foo$)", "(?i:foo)", "(?i:(foo|bar))",
        "(?i:(foo1|foo2|bar))", "^(?i:foo|oo)|(bar)$",
        "(?i:(foo1|foo2|aaa|bbb|ccc|ddd|eee|fff|ggg|hhh|iii|lll|mmm|nnn|ooo|ppp|qqq|rrr|sss|ttt|uuu|vvv|www|xxx|yyy|zzz))",
        "((.*)(bar|b|buzz)(.+)|foo)$", "^$", "(prometheus|api_prom)_api_v1_.+",
        "10\\.0\\.(1|2)\\.+", "10\\.0\\.(1|2).+", "((fo(bar))|.+foo)",
        "(?i)report.scheduled.job_runscheduledreports", "report.scheduled.job_runscheduledreports",
        "fo.?", "foo.?", "f.?o", ".*foo.?", ".?foo.+", "foo.?|bar", "ſſs",
        ".*-.*-.*-.*-.*", ".+-.*-.*-.*-.+", "-.*-.*-.*-.*", ".*-.*-.*-.*-",
        "(.+)-(.+)-(.+)-(.+)-(.+)", "((.*))(?i:f)((.*))o((.*))o((.*))",
        "((.*))f((.*))(?i:o)((.*))o((.*))", "(.*0.*)",
        r".*\|(foo)\|.*", ".*-(ab)-.*", "(?i)k.*", "(?i)k-(a|b)", "(?i)(ka.+|kb.+)",
        ".*(?i:k)", "a.*(?i:sk)", "(?i:some)-pattern.*",
    ];

    for pat in regexes {
        let m = FastRegexMatcher::new_without_cache(pat).unwrap();
        let tm = TsdbFastRegexMatcher::new(pat).unwrap();
        let translated = mimir_tsdb::go_regexp_syntax::translate_to_rust(pat).unwrap();
        let ref_re = regex::Regex::new(&format!(r"\A(?s:{})\z", translated.pattern)).unwrap();
        for &val in &test_values {
            let expected = ref_re.is_match(val);
            assert_eq!(m.is_match(val), expected, "FastRegexMatcher pat={pat:?} val={val:?}");
            assert_eq!(tm.is_match(val), expected, "TsdbFastRegexMatcher pat={pat:?} val={val:?}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestNewFastRegexMatcher_CacheSizeLimit
#[test]
fn test_new_fast_regex_matcher_cache_size_limit() {
    let alternates: Vec<String> = (0..200).map(|i| format!("alt_{i:04}_xxxxxxxxxx.{i}")).collect();
    let expensive = alternates.join("|");
    let sample = FastRegexMatcher::new_without_cache(&expensive).unwrap();
    let item_size = sample.estimated_size_bytes();
    let max_items = 5usize;
    let mut cache = FastRegexMatcherCache::new(item_size * max_items + item_size / 2);

    for i in 0..max_items {
        let key = format!("{expensive}{i}");
        let m = FastRegexMatcher::new_without_cache(&key).unwrap();
        cache.set(key, m.clone(), item_size);
    }
    for i in 0..max_items {
        assert!(cache.contains(&format!("{expensive}{i}")), "item {i} must still be in cache");
    }

    let extra = format!("{expensive}{max_items}");
    let extra_m = FastRegexMatcher::new_without_cache(&extra).unwrap();
    cache.set(extra, extra_m, item_size);

    let num_evicted = (0..max_items)
        .filter(|i| !cache.contains(&format!("{expensive}{i}")))
        .count();
    assert_eq!(num_evicted, 1);
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestOptimizeConcatRegex
#[test]
fn test_optimize_concat_regex() {
    let cases: &[(&str, &str, bool, &str, &[&str])] = &[
        ("foo(hello|bar)", "foo", false, "", &[]),
        ("foo(hello|bar)world", "foo", false, "world", &[]),
        ("foo.*", "foo", false, "", &[]),
        ("foo.*hello.*bar", "foo", false, "bar", &["hello"]),
        (".*foo", "", false, "foo", &[]),
        ("^.*foo$", "", false, "foo", &[]),
        (".*foo.*", "", false, "", &["foo"]),
        (".*foo.*bar.*", "", false, "", &["foo", "bar"]),
        (".*(foo|bar).*", "", false, "", &[]),
        (".*[abc].*", "", false, "", &[]),
        (".*((?i)abc).*", "", false, "", &[]),
        (".*(?i:abc).*", "", false, "", &[]),
        ("(?i:abc).*", "ABC", true, "", &[]),
        (".*(?i:abc)", "", false, "", &[]),
        (".*(?i:abc)def.*", "", false, "", &["def"]),
        ("(?i).*(?-i:abc)def", "", false, "", &["abc"]),
        (".*(?msU:abc).*", "", false, "", &["abc"]),
        ("[aA]bc.*", "A", true, "", &["bc"]),
        ("^5..$", "5", false, "", &[]),
        ("^release.*", "release", false, "", &[]),
        ("^env-[0-9]+laio[1]?[^0-9].*", "env-", false, "", &["laio"]),
        (".*-.*-.*-.*-.*", "", false, "", &["-", "-", "-", "-"]),
    ];
    for &(pat, exp_pref, exp_ci, exp_suf, exp_contains) in cases {
        let parsed = parse_go_syntax(pat).unwrap();
        let (ci, pref, suf, contains) = optimize_concat_regex(&parsed);
        assert_eq!(pref, exp_pref, "pat={pat}");
        assert_eq!(ci, exp_ci, "pat={pat}");
        assert_eq!(suf, exp_suf, "pat={pat}");
        let exp_c: Vec<String> = exp_contains.iter().map(|s| s.to_string()).collect();
        assert_eq!(contains, exp_c, "pat={pat}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestFindSetMatches
#[test]
fn test_find_set_matches() {
    let cases: &[(&str, Option<&[&str]>, bool)] = &[
        ("foo", Some(&["foo"]), true),
        ("^foo", Some(&["foo"]), true),
        ("^foo$", Some(&["foo"]), true),
        ("foo|bar|zz", Some(&["foo", "bar", "zz"]), true),
        ("foo|bar|baz", Some(&["foo", "bar", "baz"]), true),
        ("foo|bar|baz|(zz)", Some(&["foo", "bar", "baz", "zz"]), true),
        ("bar|b|buzz", Some(&["bar", "b", "buzz"]), true),
        ("^((bar|b|buzz))$", Some(&["bar", "b", "buzz"]), true),
        ("^(bar|b|buzz)$", Some(&["bar", "b", "buzz"]), true),
        ("^(?:prod|production)$", Some(&["prod", "production"]), true),
        ("(bar|b|b^uz$z)", None, false),
        ("^$|Running", None, false),
        ("fo\\.o|bar\\?|\\^baz", Some(&["fo.o", "bar?", "^baz"]), true),
        ("[abc]d", Some(&["ad", "bd", "cd"]), true),
        ("ABC|ABD|AEF|BCX|BCY", Some(&["ABC", "ABD", "AEF", "BCX", "BCY"]), true),
        ("api_(v1|prom)_push", Some(&["api_v1_push", "api_prom_push"]), true),
        ("(api|rpc)_(v1|prom)_push", Some(&["api_v1_push", "api_prom_push", "rpc_v1_push", "rpc_prom_push"]), true),
        ("(api|rpc)_(v1|prom)_(push|query)", Some(&["api_v1_push", "api_v1_query", "api_prom_push", "api_prom_query", "rpc_v1_push", "rpc_v1_query", "rpc_prom_push", "rpc_prom_query"]), true),
        ("[-1-2][a-c]", Some(&["-a", "-b", "-c", "1a", "1b", "1c", "2a", "2b", "2c"]), true),
        ("[1^3]", Some(&["1", "3", "^"]), true),
        ("(.+)/(foo|bar)", None, false),
        ("fo.o|bar?|^baz", None, false),
        ("(?i)foo", Some(&["FOO"]), false),
        ("(?i)foo|bar|baz", Some(&["FOO", "BAR", "BAZ", "BAr", "BAz"]), false),
        ("(api|rpc)_(v1|prom)_((?i)push|query)", None, false),
        ("api_v1_(?i)push", None, false),
        ("api|(?i)rpc", None, false),
        ("rpc|(?i)(?-i)api", Some(&["rpc", "api"]), true),
        ("(?i)((?-i)api|(?-i)rpc)", Some(&["api", "rpc"]), true),
        ("(?i)rpc|(?-i)api", None, false),
        ("(api|rpc)_[^0-9]", None, false),
        ("[a-z][a-z]", None, false),
    ];

    for &(pat, exp_matches, exp_cs) in cases {
        let mut parsed = parse_go_syntax(pat).unwrap();
        let (matches, cs) = find_set_matches(&mut parsed);
        assert_eq!(cs, exp_cs, "pat={pat}");
        match (exp_matches, matches) {
            (None, None) => {}
            (Some(exp), Some(mut got)) => {
                let mut exp_v: Vec<String> = exp.iter().map(|s| s.to_string()).collect();
                exp_v.sort();
                got.sort();
                assert_eq!(got, exp_v, "pat={pat}");
            }
            (e, g) => panic!("pat={pat}: expected {e:?}, got {g:?}"),
        }
        if exp_cs {
            let r = FastRegexMatcher::new_without_cache(pat).unwrap();
            let mut got = r.set_matches().unwrap();
            let mut exp_v: Vec<String> = exp_matches.unwrap().iter().map(|s| s.to_string()).collect();
            exp_v.sort();
            got.sort();
            assert_eq!(got, exp_v, "pat={pat}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestFastRegexMatcher_SetMatches_ShouldReturnACopy
#[test]
fn test_fast_regex_matcher_set_matches_should_return_a_copy() {
    let m = FastRegexMatcher::new_without_cache("a|b").unwrap();
    assert_eq!(m.set_matches(), Some(vec!["a".to_string(), "b".to_string()]));
    let mut copy = m.set_matches().unwrap();
    copy[0] = "xxx".to_string();
    copy[1] = "yyy".to_string();
    assert_eq!(m.set_matches(), Some(vec!["a".to_string(), "b".to_string()]));

    let tm = TsdbFastRegexMatcher::new("a|b").unwrap();
    assert_eq!(tm.set_matches(), Some(&["a".to_string(), "b".to_string()][..]));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestNewFastRegexMatcher
#[test]
fn test_new_fast_regex_matcher() {
    let eq_s = |s: &str, cs: bool| StringMatcher::Equal { s: s.to_string(), case_sensitive: cs };
    let true_m = || StringMatcher::True;
    let empty_m = || StringMatcher::Empty;
    let non_empty_nl = || StringMatcher::AnyNonEmpty { match_nl: true };
    let quest_nl = || StringMatcher::ZeroOrOneChar { match_nl: true };

    let cases: Vec<(&str, Option<StringMatcher>)> = vec![
        (".*", Some(true_m())),
        ("().*", Some(true_m())),
        (".*()", Some(true_m())),
        ("().*()", Some(true_m())),
        (".*?", Some(true_m())),
        ("(?s:.*)", Some(true_m())),
        ("(.*)", Some(true_m())),
        ("^.*$", Some(true_m())),
        (".+", Some(non_empty_nl())),
        ("(?s:.+)", Some(non_empty_nl())),
        ("^.+$", Some(non_empty_nl())),
        ("(.+)", Some(non_empty_nl())),
        ("", Some(empty_m())),
        ("()", Some(empty_m())),
        ("^$", Some(empty_m())),
        ("^foo$", Some(eq_s("foo", true))),
        ("^foo()$", Some(eq_s("foo", true))),
        ("^()foo$", Some(eq_s("foo", true))),
        ("^(?i:foo)$", Some(eq_s("FOO", false))),
        ("^((?i:foo)|(bar))$", Some(StringMatcher::Or(vec![eq_s("FOO", false), eq_s("bar", true)]))),
        ("(?i:((foo|bar)))", Some(StringMatcher::EqualMultiSlice { values: vec!["FOO".into(), "BAR".into()], lengths_mask: 0b1000, case_sensitive: false })),
        ("(?i:((foo1|foo2|bar)))", Some(StringMatcher::EqualMultiSlice { values: vec!["BAR".into(), "FOO1".into(), "FOO2".into()], lengths_mask: 0b11000, case_sensitive: false })),
        ("^((?i:foo|oo)|(bar))$", Some(StringMatcher::Or(vec![eq_s("FOO", false), eq_s("OO", false), eq_s("bar", true)]))),
        ("(?i:(foo1|foo2|bar))", Some(StringMatcher::EqualMultiSlice { values: vec!["BAR".into(), "FOO1".into(), "FOO2".into()], lengths_mask: 0b11000, case_sensitive: false })),
        (".*foo.*", Some(StringMatcher::Contains { substrings: vec!["foo".into()], left: Some(Box::new(true_m())), right: Some(Box::new(true_m())) })),
        ("(.*)foo.*", Some(StringMatcher::Contains { substrings: vec!["foo".into()], left: Some(Box::new(true_m())), right: Some(Box::new(true_m())) })),
        ("(.*)foo(.*)", Some(StringMatcher::Contains { substrings: vec!["foo".into()], left: Some(Box::new(true_m())), right: Some(Box::new(true_m())) })),
        ("(.+)foo(.*)", Some(StringMatcher::Contains { substrings: vec!["foo".into()], left: Some(Box::new(non_empty_nl())), right: Some(Box::new(true_m())) })),
        ("^.+foo.+", Some(StringMatcher::Contains { substrings: vec!["foo".into()], left: Some(Box::new(non_empty_nl())), right: Some(Box::new(non_empty_nl())) })),
        ("^(.*)(foo)(.*)$", Some(StringMatcher::Contains { substrings: vec!["foo".into()], left: Some(Box::new(true_m())), right: Some(Box::new(true_m())) })),
        ("^(.*)(foo|foobar)(.*)$", Some(StringMatcher::Contains { substrings: vec!["foo".into(), "foobar".into()], left: Some(Box::new(true_m())), right: Some(Box::new(true_m())) })),
        ("^(.*)(foo|foobar)(.+)$", Some(StringMatcher::Contains { substrings: vec!["foo".into(), "foobar".into()], left: Some(Box::new(true_m())), right: Some(Box::new(non_empty_nl())) })),
        ("^(.*)(bar|b|buzz)(.+)$", Some(StringMatcher::Contains { substrings: vec!["bar".into(), "b".into(), "buzz".into()], left: Some(Box::new(true_m())), right: Some(Box::new(non_empty_nl())) })),
        ("10\\.0\\.(1|2)\\.+", None),
        ("10\\.0\\.(1|2).+", Some(StringMatcher::Contains { substrings: vec!["10.0.1".into(), "10.0.2".into()], left: None, right: Some(Box::new(non_empty_nl())) })),
        ("^.+foo", Some(StringMatcher::LiteralSuffix { left: Box::new(non_empty_nl()), suffix: "foo".into(), suffix_case_sensitive: true })),
        ("foo-.*$", Some(StringMatcher::LiteralPrefixSensitive { prefix: "foo-".into(), right: Box::new(true_m()) })),
        ("(prometheus|api_prom)_api_v1_.+", Some(StringMatcher::Contains { substrings: vec!["prometheus_api_v1_".into(), "api_prom_api_v1_".into()], left: None, right: Some(Box::new(non_empty_nl())) })),
        ("^((.*)(bar|b|buzz)(.+)|foo)$", Some(StringMatcher::Or(vec![StringMatcher::Contains { substrings: vec!["bar".into(), "b".into(), "buzz".into()], left: Some(Box::new(true_m())), right: Some(Box::new(non_empty_nl())) }, eq_s("foo", true)]))),
        ("((fo(bar))|.+foo)", Some(StringMatcher::Or(vec![StringMatcher::Or(vec![eq_s("fobar", true)]), StringMatcher::LiteralSuffix { suffix: "foo".into(), suffix_case_sensitive: true, left: Box::new(non_empty_nl()) }]))),
        ("(.+)/(gateway|cortex-gw|cortex-gw-internal)", Some(StringMatcher::Contains { substrings: vec!["/cortex-gw".into(), "/cortex-gw-internal".into(), "/gateway".into()], left: Some(Box::new(non_empty_nl())), right: None })),
        ("^(.*)((?i)foo|foobar)(.*)$", None),
        ("(api|rpc)_(v1|prom)_((?i)push|query)", None),
        ("[a-z][a-z]", None),
        ("[1^3]", Some(StringMatcher::EqualMultiSlice { values: vec!["1".into(), "3".into(), "^".into()], lengths_mask: 0b10, case_sensitive: true })),
        (".*foo.*bar.*", Some(true_m())),
        (r"\d*", None),
        (".", None),
        ("/|/bar.*", Some(StringMatcher::LiteralPrefixSensitive { prefix: "/".into(), right: Box::new(StringMatcher::Or(vec![empty_m(), StringMatcher::LiteralPrefixSensitive { prefix: "bar".into(), right: Box::new(true_m()) }])) })),
        ("(.+)/(foo.*|bar$)", None),
        ("(xyz-016a-ixb-dp.*|xyz-016a-ixb-op.*)", Some(StringMatcher::LiteralPrefixSensitive { prefix: "xyz-016a-ixb-".into(), right: Box::new(StringMatcher::Or(vec![StringMatcher::LiteralPrefixSensitive { prefix: "dp".into(), right: Box::new(true_m()) }, StringMatcher::LiteralPrefixSensitive { prefix: "op".into(), right: Box::new(true_m()) }])) })),
        ("(?i:(xyz-016a-ixb-dp.*|xyz-016a-ixb-op.*))", Some(StringMatcher::LiteralPrefixInsensitive { prefix: "XYZ-016A-IXB-".into(), right: Box::new(StringMatcher::Or(vec![StringMatcher::LiteralPrefixInsensitive { prefix: "DP".into(), right: Box::new(true_m()) }, StringMatcher::LiteralPrefixInsensitive { prefix: "OP".into(), right: Box::new(true_m()) }])) })),
        ("(?i)(xyz-016a-ixb-dp.*|xyz-016a-ixb-op.*)", Some(StringMatcher::LiteralPrefixInsensitive { prefix: "XYZ-016A-IXB-".into(), right: Box::new(StringMatcher::Or(vec![StringMatcher::LiteralPrefixInsensitive { prefix: "DP".into(), right: Box::new(true_m()) }, StringMatcher::LiteralPrefixInsensitive { prefix: "OP".into(), right: Box::new(true_m()) }])) })),
        ("foo.*.*", None),
        ("foo.+.+", None),
        (".*.*foo", None),
        (".+.+foo", None),
        ("aaa.?.?", None),
        ("aaa.?.*", None),
        ("ext.?|xfs", Some(StringMatcher::Or(vec![StringMatcher::LiteralPrefixSensitive { prefix: "ext".into(), right: Box::new(quest_nl()) }, eq_s("xfs", true)]))),
        ("(?s)(ext.?|xfs)", Some(StringMatcher::Or(vec![StringMatcher::LiteralPrefixSensitive { prefix: "ext".into(), right: Box::new(quest_nl()) }, eq_s("xfs", true)]))),
        ("foo.?", Some(StringMatcher::LiteralPrefixSensitive { prefix: "foo".into(), right: Box::new(quest_nl()) })),
        ("f.?o", None),
        (".*foo.*|.*bar.*|.*baz.*", Some(StringMatcher::Contains { left: Some(Box::new(true_m())), substrings: vec!["foo".into(), "bar".into(), "baz".into()], right: Some(Box::new(true_m())) })),
        ("(?i)report.scheduled.job_runscheduledreports", None),
        ("report.scheduled.job_runscheduledreports", None),
    ];

    for (pat, exp) in cases {
        let m = FastRegexMatcher::new_without_cache(pat).unwrap();
        assert_eq!(m.string_matcher, exp, "pat={pat}");
    }
}

fn count_matching_nodes(m: &StringMatcher, pred: &dyn Fn(&StringMatcher) -> bool) -> usize {
    let mut count = if pred(m) { 1 } else { 0 };
    match m {
        StringMatcher::Contains { left, right, .. } => {
            if let Some(l) = left { count += count_matching_nodes(l, pred); }
            if let Some(r) = right { count += count_matching_nodes(r, pred); }
        }
        StringMatcher::LiteralPrefixSensitive { right, .. }
        | StringMatcher::LiteralPrefixInsensitive { right, .. } => {
            count += count_matching_nodes(right, pred);
        }
        StringMatcher::LiteralSuffix { left, .. } => {
            count += count_matching_nodes(left, pred);
        }
        StringMatcher::Or(list) => {
            for item in list {
                count += count_matching_nodes(item, pred);
            }
        }
        StringMatcher::EqualMultiMap { prefixes, .. } => {
            for list in prefixes.values() {
                for item in list {
                    count += count_matching_nodes(item, pred);
                }
            }
        }
        _ => {}
    }
    count
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestStringMatcherFromRegexp_LiteralPrefix
#[test]
fn test_string_matcher_from_regexp_literal_prefix() {
    let cases: &[(&str, usize, &[&str], &[&str])] = &[
        (
            "(xyz-016a-ixb-dp.*|xyz-016a-ixb-op.*)",
            3,
            &["xyz-016a-ixb-dp", "xyz-016a-ixb-dpXXX", "xyz-016a-ixb-op", "xyz-016a-ixb-opXXX", "xyz-016a-ixb-dp\n"],
            &["XYZ-016a-ixb-dp", "xyz-016a-ixb-d", "XYZ-016a-ixb-op", "xyz-016a-ixb-o", "xyz", "dp"],
        ),
        (
            "(?i)(xyz-016a-ixb-dp.*|xyz-016a-ixb-op.*)",
            3,
            &["xyz-016a-ixb-dp", "XYZ-016a-ixb-dpXXX", "xyz-016a-ixb-op", "XYZ-016a-ixb-opXXX", "xyz-016a-ixb-dp\n"],
            &["xyz-016a-ixb-d", "xyz", "dp"],
        ),
        (
            "(xyz-(aaa-(111.*)|bbb-(222.*)))|(xyz-(aaa-(333.*)|bbb-(444.*)))",
            10,
            &["xyz-aaa-111", "xyz-aaa-111XXX", "xyz-aaa-333", "xyz-aaa-333XXX", "xyz-bbb-222", "xyz-bbb-222XXX", "xyz-bbb-444", "xyz-bbb-444XXX"],
            &["XYZ-aaa-111", "xyz-aaa-11", "xyz-aaa-222", "xyz-bbb-111"],
        ),
        (
            "(?i)(xyz-(aaa-(111.*)|bbb-(222.*)))|(xyz-(aaa-(333.*)|bbb-(444.*)))",
            10,
            &["xyz-aaa-111", "XYZ-aaa-111XXX", "xyz-aaa-333", "xyz-AAA-333XXX", "xyz-bbb-222", "xyz-BBB-222XXX", "XYZ-bbb-444", "xyz-bbb-444XXX"],
            &["xyz-aaa-11", "xyz-aaa-222", "xyz-bbb-111"],
        ),
        (
            "(xyz-((?i)(aaa.*|bbb.*)))",
            3,
            &["xyz-aaa", "xyz-AAA", "xyz-aaaXXX", "xyz-AAAXXX", "xyz-bbb", "xyz-BBBXXX"],
            &["XYZ-aaa", "xyz-aa", "yz-aaa", "aaa"],
        ),
    ];
    for &(pat, exp_count, matches, not_matches) in cases {
        let mut parsed = parse_go_syntax(pat).unwrap();
        let matcher = string_matcher_from_regexp(&mut parsed).unwrap();
        let num = count_matching_nodes(&matcher, &|m| {
            matches!(m, StringMatcher::LiteralPrefixSensitive { .. } | StringMatcher::LiteralPrefixInsensitive { .. })
        });
        assert_eq!(num, exp_count, "pat={pat}");
        for &v in matches {
            assert!(matcher.matches(v), "pat={pat} val={v:?}");
        }
        for &v in not_matches {
            assert!(!matcher.matches(v), "pat={pat} val={v:?}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestStringMatcherFromRegexp_LiteralSuffix
#[test]
fn test_string_matcher_from_regexp_literal_suffix() {
    let cases: &[(&str, usize, &[&str], &[&str])] = &[
        (
            "(.*xyz-016a-ixb-dp|.*xyz-016a-ixb-op)",
            2,
            &["xyz-016a-ixb-dp", "XXXxyz-016a-ixb-dp", "xyz-016a-ixb-op", "XXXxyz-016a-ixb-op", "\nxyz-016a-ixb-dp"],
            &["XYZ-016a-ixb-dp", "yz-016a-ixb-dp", "XYZ-016a-ixb-op", "xyz-016a-ixb-o", "xyz", "dp"],
        ),
        (
            "(?i)(.*xyz-016a-ixb-dp|.*xyz-016a-ixb-op)",
            2,
            &["xyz-016a-ixb-dp", "XYZ-016a-ixb-dp", "XXXxyz-016a-ixb-dp", "XyZ-016a-ixb-op", "XXXxyz-016a-ixb-op", "\nxyz-016a-ixb-dp"],
            &["yz-016a-ixb-dp", "xyz-016a-ixb-o", "xyz", "dp"],
        ),
        (
            "(.*aaa|.*bbb(.*ccc|.*ddd))",
            3,
            &["aaa", "XXXaaa", "bbbccc", "XXXbbbccc", "XXXbbbXXXccc", "bbbddd", "XXXbbbddd", "XXXbbbXXXddd", "bbbXXXccc", "aaabbbccc", "aaabbbddd"],
            &["AAA", "aa", "Xaa", "BBBCCC", "bb", "Xbb", "bbccc", "bbbcc", "bbbdd"],
        ),
        (
            "(.*aaa|.*bbb((?i)(.*ccc|.*ddd)))",
            3,
            &["aaa", "XXXaaa", "bbbccc", "bbbCCC", "bbbXXXCCC", "bbbddd", "bbbDDD", "bbbXXXddd", "bbbXXXDDD"],
            &["AAA", "XXXAAA", "BBBccc", "BBBCCC", "aaaBBB"],
        ),
    ];
    for &(pat, exp_count, matches, not_matches) in cases {
        let mut parsed = parse_go_syntax(pat).unwrap();
        let matcher = string_matcher_from_regexp(&mut parsed).unwrap();
        let num = count_matching_nodes(&matcher, &|m| matches!(m, StringMatcher::LiteralSuffix { .. }));
        assert_eq!(num, exp_count, "pat={pat}");
        for &v in matches {
            assert!(matcher.matches(v), "pat={pat} val={v:?}");
        }
        for &v in not_matches {
            assert!(!matcher.matches(v), "pat={pat} val={v:?}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestStringMatcherFromRegexp_Quest
#[test]
fn test_string_matcher_from_regexp_quest() {
    let cases: &[(&str, usize, &[&str], &[&str])] = &[
        ("test.?", 1, &["test\n", "test", "test!"], &["tes", "test!!"]),
        (".?test", 1, &["\ntest", "test", "!test"], &["tes", "test!"]),
        ("(aaa.?|bbb.?)", 2, &["aaa", "aaaX", "bbb", "bbbX", "aaa\n", "bbb\n"], &["aa", "aaaXX", "bb", "bbbXX"]),
        (".*aaa.?", 1, &["aaa", "Xaaa", "aaaX", "XXXaaa", "XXXaaaX", "XXXaaa\n"], &["aa", "aaaXX", "XXXaaaXXX"]),
        ("(?s)test.?", 1, &["test", "test!", "test\n"], &["tes", "test!!", "test\n\n"]),
        ("(aaa.?|((?s).?bbb.+))", 2, &["aaa", "aaaX", "bbbX", "XbbbX", "bbbXXX", "\nbbbX", "aaa\n"], &["aa", "Xbbb", "\nbbb"]),
    ];
    for &(pat, exp_count, matches, not_matches) in cases {
        let mut parsed = parse_go_syntax(pat).unwrap();
        let matcher = string_matcher_from_regexp(&mut parsed).unwrap();
        let num = count_matching_nodes(&matcher, &|m| matches!(m, StringMatcher::ZeroOrOneChar { .. }));
        assert_eq!(num, exp_count, "pat={pat}");
        for &v in matches {
            assert!(matcher.matches(v), "pat={pat} val={v:?}");
        }
        for &v in not_matches {
            assert!(!matcher.matches(v), "pat={pat} val={v:?}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestAnalyzeRealQueries
#[test]
fn test_analyze_real_queries() {
    let log_lines = [
        r#"ts=2025-01-01T00:00:00Z query_wall_time_seconds=0.123 sharded_queries=2 split_queries=4 query='{namespace=~"(cortex|mimir).*",pod!~"foo|bar"}'"#,
        r#"ts=2025-01-01T00:00:05Z query_wall_time_seconds=0.250 sharded_queries=1 split_queries=1 query='{route=~"[a-z][a-z]"}'"#,
    ];
    let re = regex::Regex::new(r#"[=!]~"([^"]*)""#).unwrap();
    let mut checked = 0usize;
    let mut optimized = 0usize;
    for line in log_lines {
        for cap in re.captures_iter(line) {
            let pat = &cap[1];
            let m = FastRegexMatcher::new_without_cache(pat).unwrap();
            checked += 1;
            if m.is_optimized() {
                optimized += 1;
            }
        }
    }
    assert_eq!(checked, 3);
    assert_eq!(optimized, 2);
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestOptimizeEqualOrPrefixStringMatchers
#[test]
fn test_optimize_equal_or_prefix_string_matchers() {
    let eq = |s: &str, cs: bool| StringMatcher::Equal { s: s.to_string(), case_sensitive: cs };
    let c_m = StringMatcher::Contains { left: None, substrings: vec!["a".into(), "b".into(), "c".into()], right: None };

    let m1 = StringMatcher::Or(vec![eq("FOO", true), c_m]);
    assert!(matches!(optimize_equal_or_prefix_string_matchers(m1, 0), StringMatcher::Or(_)));

    let m2 = StringMatcher::Or(vec![eq("FOO", true), eq("bar", true), eq("baz", true)]);
    assert_eq!(
        optimize_equal_or_prefix_string_matchers(m2, 0),
        StringMatcher::EqualMultiSlice {
            values: vec!["FOO".into(), "bar".into(), "baz".into()],
            lengths_mask: (1 << 3),
            case_sensitive: true,
        }
    );

    let m3 = StringMatcher::Or(vec![eq("FOO", true), eq("bar", false), eq("baz", true)]);
    assert!(matches!(optimize_equal_or_prefix_string_matchers(m3, 0), StringMatcher::Or(_)));

    let m4 = StringMatcher::Or(vec![
        eq("FOO", true),
        StringMatcher::Or(vec![eq("bar", true), eq("xxx", true)]),
        eq("baz", true),
    ]);
    assert_eq!(
        optimize_equal_or_prefix_string_matchers(m4, 0),
        StringMatcher::EqualMultiSlice {
            values: vec!["FOO".into(), "bar".into(), "xxx".into(), "baz".into()],
            lengths_mask: (1 << 3),
            case_sensitive: true,
        }
    );

    let m5 = StringMatcher::Or(vec![
        eq("FOO", true),
        StringMatcher::Or(vec![eq("bar", true), eq("xxx", false)]),
        eq("baz", true),
    ]);
    assert!(matches!(optimize_equal_or_prefix_string_matchers(m5, 0), StringMatcher::Or(_)));

    let m6 = StringMatcher::Or(vec![
        eq("FOO", true),
        StringMatcher::Or(vec![eq("bar", false), eq("xxx", false)]),
        eq("baz", true),
    ]);
    assert!(matches!(optimize_equal_or_prefix_string_matchers(m6, 0), StringMatcher::Or(_)));

    let m7 = StringMatcher::Or(vec![
        eq("FOO", false),
        StringMatcher::Or(vec![eq("bAr", false)]),
        eq("baZ", false),
    ]);
    assert_eq!(
        optimize_equal_or_prefix_string_matchers(m7, 0),
        StringMatcher::EqualMultiSlice {
            values: vec!["FOO".into(), "bAr".into(), "baZ".into()],
            lengths_mask: (1 << 3),
            case_sensitive: false,
        }
    );
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestNewEqualMultiStringMatcher
#[test]
fn test_new_equal_multi_string_matcher() {
    assert_eq!(MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD, 16);

    let mut m1 = new_equal_multi_string_matcher(true, 2, 0, 0);
    m1.add("a");
    m1.add("B");
    assert!(matches!(m1, StringMatcher::EqualMultiSlice { case_sensitive: true, .. }));

    let mut m2 = new_equal_multi_string_matcher(false, 2, 0, 0);
    m2.add("a");
    m2.add("B");
    assert!(matches!(m2, StringMatcher::EqualMultiSlice { case_sensitive: false, .. }));

    let p = StringMatcher::LiteralPrefixSensitive {
        prefix: "B".into(),
        right: Box::new(StringMatcher::AnyWithoutNewline),
    };
    let mut m3 = new_equal_multi_string_matcher(true, 1, 1, 1);
    m3.add("a");
    m3.add_prefix("B", true, p.clone());
    assert!(matches!(m3, StringMatcher::EqualMultiMap { case_sensitive: true, .. }));
    assert!(m3.matches("a"));
    assert!(m3.matches("BX"));

    let vals = ["a", "B", "c", "D", "e", "F", "g", "H", "i", "L", "m", "N", "o", "P", "q", "r"];
    let mut m4 = new_equal_multi_string_matcher(true, vals.len(), 0, 0);
    for v in vals { m4.add(v); }
    assert!(matches!(m4, StringMatcher::EqualMultiMap { case_sensitive: true, .. }));

    let mut m5 = new_equal_multi_string_matcher(false, vals.len(), 0, 0);
    for v in vals { m5.add(v); }
    assert!(matches!(m5, StringMatcher::EqualMultiMap { case_sensitive: false, .. }));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestEqualMultiStringMapMatcher_addPrefix
#[test]
fn test_equal_multi_string_map_matcher_add_prefix() {
    let p1 = std::panic::catch_unwind(|| {
        let mut m = new_equal_multi_string_matcher(true, 0, 1, 1);
        m.add_prefix("a", false, StringMatcher::LiteralPrefixInsensitive { prefix: "a".into(), right: Box::new(StringMatcher::Empty) });
    });
    assert!(p1.is_err());

    let p2 = std::panic::catch_unwind(|| {
        let mut m = new_equal_multi_string_matcher(false, 0, 1, 1);
        m.add_prefix("a", true, StringMatcher::LiteralPrefixSensitive { prefix: "a".into(), right: Box::new(StringMatcher::Empty) });
    });
    assert!(p2.is_err());
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestEqualMultiStringMatcher_Matches
#[test]
fn test_equal_multi_string_matcher_matches() {
    let many = ["a", "B", "c", "D", "e", "F", "g", "H", "i", "L", "m", "N", "o", "P", "q", "r"];

    let mut m = new_equal_multi_string_matcher(true, 2, 0, 0);
    m.add("a"); m.add("B");
    assert!(m.matches("a")); assert!(m.matches("B"));
    assert!(!m.matches("A")); assert!(!m.matches("b"));

    let mut m = new_equal_multi_string_matcher(false, 2, 0, 0);
    m.add("a"); m.add("B");
    for s in ["a", "A", "b", "B"] { assert!(m.matches(s)); }
    for s in ["c", "C"] { assert!(!m.matches(s)); }

    let mut m = new_equal_multi_string_matcher(true, 0, 2, 1);
    m.add_prefix("a", true, StringMatcher::LiteralPrefixSensitive { prefix: "a".into(), right: Box::new(StringMatcher::AnyWithoutNewline) });
    m.add_prefix("B", true, StringMatcher::LiteralPrefixSensitive { prefix: "B".into(), right: Box::new(StringMatcher::AnyWithoutNewline) });
    for s in ["a", "aX", "B", "BX"] { assert!(m.matches(s)); }
    for s in ["A", "b"] { assert!(!m.matches(s)); }

    let mut m = new_equal_multi_string_matcher(false, 0, 2, 1);
    m.add_prefix("a", false, StringMatcher::LiteralPrefixInsensitive { prefix: "a".into(), right: Box::new(StringMatcher::AnyWithoutNewline) });
    m.add_prefix("B", false, StringMatcher::LiteralPrefixInsensitive { prefix: "B".into(), right: Box::new(StringMatcher::AnyWithoutNewline) });
    for s in ["a", "aX", "A", "AX", "b", "bX", "B", "BX"] { assert!(m.matches(s)); }
    for s in ["c", "cX", "C", "CX"] { assert!(!m.matches(s)); }

    let mut m = new_equal_multi_string_matcher(true, many.len(), 0, 0);
    for v in many { m.add(v); }
    assert!(m.matches("a")); assert!(m.matches("B"));
    assert!(!m.matches("A")); assert!(!m.matches("b"));

    let mut m = new_equal_multi_string_matcher(false, many.len(), 0, 0);
    for v in many { m.add(v); }
    for s in ["a", "A", "b", "B"] { assert!(m.matches(s)); }
    for s in ["x", "X"] { assert!(!m.matches(s)); }

    let mut m = new_equal_multi_string_matcher(true, 1, 1, 1);
    m.add("a");
    m.add_prefix("B", true, StringMatcher::LiteralPrefixSensitive { prefix: "B".into(), right: Box::new(StringMatcher::AnyWithoutNewline) });
    for s in ["a", "B", "BX"] { assert!(m.matches(s)); }
    for s in ["aX", "A", "b", "bX"] { assert!(!m.matches(s)); }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestFindEqualOrPrefixStringMatchers
#[test]
fn test_find_equal_or_prefix_string_matchers() {
    let collect = |input: &StringMatcher| -> (Vec<(String, bool)>, bool) {
        let mut out = Vec::new();
        let out_ptr: *mut Vec<(String, bool)> = &mut out;
        // Split callbacks via RefCell so both closures can append cleanly.
        let cell = std::cell::RefCell::new(Vec::new());
        let mut eq_cb = |s: &str, cs: bool| -> bool {
            cell.borrow_mut().push((s.to_string(), cs));
            true
        };
        let mut pref_cb = |p: &str, cs: bool, _m: &StringMatcher| -> bool {
            cell.borrow_mut().push((p.to_string(), cs));
            true
        };
        let ok = find_equal_or_prefix_string_matchers(input, &mut eq_cb, Some(&mut pref_cb));
        let _ = out_ptr;
        (cell.into_inner(), ok)
    };

    let (m, ok) = collect(&StringMatcher::Empty);
    assert!(!ok);
    assert!(m.is_empty());

    let eq = |s: &str, cs: bool| StringMatcher::Equal { s: s.into(), case_sensitive: cs };
    let ps = |p: &str| StringMatcher::LiteralPrefixSensitive { prefix: p.into(), right: Box::new(StringMatcher::True) };
    let pi = |p: &str| StringMatcher::LiteralPrefixInsensitive { prefix: p.into(), right: Box::new(StringMatcher::True) };

    assert_eq!(
        collect(&StringMatcher::Or(vec![eq("test-1", true), eq("test-2", true)])),
        (vec![("test-1".into(), true), ("test-2".into(), true)], true)
    );
    assert_eq!(
        collect(&StringMatcher::Or(vec![eq("test-1", false), eq("test-2", false)])),
        (vec![("test-1".into(), false), ("test-2".into(), false)], true)
    );
    assert_eq!(
        collect(&StringMatcher::Or(vec![eq("test-1", false), eq("test-2", true)])),
        (vec![("test-1".into(), false), ("test-2".into(), true)], true)
    );
    assert_eq!(
        collect(&StringMatcher::Or(vec![ps("test-1"), ps("test-2")])),
        (vec![("test-1".into(), true), ("test-2".into(), true)], true)
    );
    assert_eq!(
        collect(&StringMatcher::Or(vec![pi("test-1"), pi("test-2")])),
        (vec![("test-1".into(), false), ("test-2".into(), false)], true)
    );
    assert_eq!(
        collect(&StringMatcher::Or(vec![pi("test-1"), ps("test-2")])),
        (vec![("test-1".into(), false), ("test-2".into(), true)], true)
    );
    assert_eq!(
        collect(&StringMatcher::Or(vec![eq("test-1", true), ps("test-2")])),
        (vec![("test-1".into(), true), ("test-2".into(), true)], true)
    );
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestZeroOrOneCharacterStringMatcher
#[test]
fn test_zero_or_one_character_string_matcher() {
    let m_nl = StringMatcher::ZeroOrOneChar { match_nl: true };
    assert!(m_nl.matches(""));
    assert!(m_nl.matches("x"));
    assert!(m_nl.matches("\n"));
    assert!(!m_nl.matches("xx"));
    assert!(!m_nl.matches("\n\n"));

    let m_no_nl = StringMatcher::ZeroOrOneChar { match_nl: false };
    assert!(m_no_nl.matches(""));
    assert!(m_no_nl.matches("x"));
    assert!(!m_no_nl.matches("\n"));
    assert!(!m_no_nl.matches("xx"));
    assert!(!m_no_nl.matches("\n\n"));

    let emoji1 = "😀";
    let emoji2 = "❤️";
    assert_eq!(emoji1.chars().count(), 1);
    assert_eq!(emoji2.chars().count(), 2);
    assert!(m_nl.matches(emoji1));
    assert!(!m_nl.matches(emoji2));
    assert!(!m_nl.matches(&format!("{emoji1}{emoji1}")));
    assert!(!m_nl.matches(&format!("x{emoji1}")));
    assert!(!m_nl.matches(&format!("{emoji1}x")));
    assert!(!m_nl.matches(&format!("{emoji1}{emoji2}")));

    assert!(m_nl.matches_bytes(b"\xff"));
    assert!(!m_nl.matches_bytes(b"x\xff"));
    assert!(!m_nl.matches_bytes(b"\xffx"));
    assert!(!m_nl.matches_bytes(b"\xff\xfe"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestLiteralPrefixSensitiveStringMatcher
#[test]
fn test_literal_prefix_sensitive_string_matcher() {
    let m = StringMatcher::LiteralPrefixSensitive { prefix: "mar".into(), right: Box::new(StringMatcher::Empty) };
    assert!(m.matches("mar"));
    assert!(!m.matches("marco"));
    assert!(!m.matches("ma"));
    assert!(!m.matches("mAr"));

    let m = StringMatcher::LiteralPrefixSensitive { prefix: "mar".into(), right: Box::new(StringMatcher::Equal { s: "co".into(), case_sensitive: false }) };
    assert!(m.matches("marco"));
    assert!(m.matches("marCO"));
    assert!(!m.matches("MARco"));
    assert!(!m.matches("mar"));
    assert!(!m.matches("marcopracucci"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestLiteralPrefixInsensitiveStringMatcher
#[test]
fn test_literal_prefix_insensitive_string_matcher() {
    let m = StringMatcher::LiteralPrefixInsensitive { prefix: "mar".into(), right: Box::new(StringMatcher::Empty) };
    assert!(m.matches("mar"));
    assert!(!m.matches("marco"));
    assert!(!m.matches("ma"));
    assert!(m.matches("mAr"));

    let m = StringMatcher::LiteralPrefixInsensitive { prefix: "kar".into(), right: Box::new(StringMatcher::Equal { s: "co".into(), case_sensitive: false }) };
    assert!(m.matches("karco"));
    assert!(m.matches("\u{212a}arco"));
    assert!(m.matches("\u{212a}arCO"));
    assert!(!m.matches("\u{212a}ar"));
    assert!(!m.matches("\u{212a}arcopracucci"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestLiteralSuffixStringMatcher
#[test]
fn test_literal_suffix_string_matcher() {
    let m = StringMatcher::LiteralSuffix { left: Box::new(StringMatcher::Empty), suffix: "co".into(), suffix_case_sensitive: true };
    assert!(m.matches("co"));
    assert!(!m.matches("marco"));
    assert!(!m.matches("coo"));
    assert!(!m.matches("Co"));

    let m = StringMatcher::LiteralSuffix { left: Box::new(StringMatcher::Empty), suffix: "co".into(), suffix_case_sensitive: false };
    assert!(m.matches("co"));
    assert!(!m.matches("marco"));
    assert!(!m.matches("coo"));
    assert!(m.matches("Co"));

    let m = StringMatcher::LiteralSuffix { left: Box::new(StringMatcher::Equal { s: "mar".into(), case_sensitive: false }), suffix: "co".into(), suffix_case_sensitive: true };
    assert!(m.matches("marco"));
    assert!(m.matches("MARco"));
    assert!(!m.matches("marCO"));
    assert!(!m.matches("mar"));
    assert!(!m.matches("marcopracucci"));

    let m = StringMatcher::LiteralSuffix { left: Box::new(StringMatcher::Equal { s: "mar".into(), case_sensitive: false }), suffix: "co".into(), suffix_case_sensitive: false };
    assert!(m.matches("marco"));
    assert!(m.matches("MARco"));
    assert!(m.matches("marCO"));
    assert!(!m.matches("mar"));
    assert!(!m.matches("marcopracucci"));

    let m = StringMatcher::LiteralSuffix { left: Box::new(StringMatcher::Equal { s: "mar".into(), case_sensitive: false }), suffix: "ck".into(), suffix_case_sensitive: false };
    assert!(m.matches("marck"));
    assert!(m.matches("marc\u{212a}"));
    assert!(m.matches("MARc\u{212a}"));
    assert!(!m.matches("marc"));
    assert!(!m.matches("c\u{212a}"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestHasPrefixCaseInsensitive
#[test]
fn test_has_prefix_case_insensitive() {
    assert!(has_prefix_case_insensitive("marco", "mar"));
    assert!(has_prefix_case_insensitive("mArco", "mar"));
    assert!(has_prefix_case_insensitive("marco", "MaR"));
    assert!(has_prefix_case_insensitive("marco", "marco"));
    assert!(has_prefix_case_insensitive("mArco", "marco"));
    assert!(!has_prefix_case_insensitive("marco", "a"));
    assert!(!has_prefix_case_insensitive("marco", "abcdefghi"));

    assert!(has_prefix_case_insensitive("\u{212a}elvin", "k"));
    assert!(has_prefix_case_insensitive("\u{212a}elvin", "kel"));
    assert!(has_prefix_case_insensitive("kelvin", "\u{212a}"));
    assert!(has_prefix_case_insensitive("ſtreet", "st"));
    assert!(!has_prefix_case_insensitive("\u{212a}elvin", "s"));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestPrefixCaseInsensitiveMatchLen
#[test]
fn test_prefix_case_insensitive_match_len() {
    let cases: &[(&str, &str, Option<usize>)] = &[
        ("marco", "mar", Some(3)),
        ("mArco", "MaR", Some(3)),
        ("marco", "marco", Some(5)),
        ("mar", "marco", None),
        ("marco", "x", None),
        ("", "", Some(0)),
        ("marco", "", Some(0)),
        ("\u{212a}elvin", "k", Some(3)),
        ("\u{212a}elvin", "kel", Some(5)),
        ("kelvin", "\u{212a}", Some(1)),
        ("ſtreet", "st", Some(3)),
        ("streets", "ſt", Some(2)),
        ("ſſs", "sss", Some(5)),
        ("ſtreet", "str", Some(4)),
        ("\u{212a}elvin", "s", None),
        ("ſtreet", "sx", None),
    ];
    for &(s, p, exp) in cases {
        assert_eq!(prefix_case_insensitive_match_len(s, p), exp, "s={s:?} prefix={p:?}");
    }
    assert_eq!(prefix_case_insensitive_match_len_bytes(b"\xff", b"\xff"), Some(1));
    assert_eq!(prefix_case_insensitive_match_len_bytes(b"\xffabc", b"\xffab"), Some(3));
    assert_eq!(prefix_case_insensitive_match_len_bytes("\u{212a}elvin".as_bytes(), b"\xff"), None);
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestSuffixCaseInsensitiveMatchLen
#[test]
fn test_suffix_case_insensitive_match_len() {
    let cases: &[(&str, &str, Option<usize>)] = &[
        ("marco", "rco", Some(3)),
        ("marco", "RcO", Some(3)),
        ("marco", "marco", Some(5)),
        ("rco", "marco", None),
        ("marco", "x", None),
        ("", "", Some(0)),
        ("marco", "", Some(0)),
        ("a\u{212a}", "k", Some(3)),
        ("drin\u{212a}", "ink", Some(5)),
        ("drink", "in\u{212a}", Some(3)),
        ("streetſ", "ts", Some(3)),
        ("ſſs", "sss", Some(5)),
        ("a\u{212a}", "s", None),
        ("streetſ", "tſt", None),
    ];
    for &(s, suf, exp) in cases {
        assert_eq!(suffix_case_insensitive_match_len(s, suf), exp, "s={s:?} suffix={suf:?}");
    }
    assert_eq!(suffix_case_insensitive_match_len_bytes(b"\xff", b"\xff"), Some(1));
    assert_eq!(suffix_case_insensitive_match_len_bytes(b"abc\xff", b"bc\xff"), Some(3));
    assert_eq!(suffix_case_insensitive_match_len_bytes("a\u{212a}".as_bytes(), b"\xff"), None);
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestContainsInOrder
#[test]
fn test_contains_in_order() {
    assert!(contains_in_order("abcdefghilmno", &["ab", "cd", "no"]));
    assert!(contains_in_order("abcdefghilmno", &["def", "hil"]));
    assert!(!contains_in_order("abcdefghilmno", &["ac"]));
    assert!(!contains_in_order("abcdefghilmno", &["ab", "cd", "de"]));
    assert!(!contains_in_order("abcdefghilmno", &["cd", "ab"]));
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestToNormalisedLower
#[test]
fn test_to_normalised_lower() {
    let cases = [
        ("foo", "foo"),
        ("FOO", "foo"),
        ("Foo", "foo"),
        ("foO", "foo"),
        ("fOo", "foo"),
        ("AAAAAAAAAAAAAAAAAAAAAAAA", "aaaaaaaaaaaaaaaaaaaaaaaa"),
        ("cccccccccccccccccccccccC", "cccccccccccccccccccccccc"),
        ("ſſſſſſſſſſſſſſſſſſſſſſſſS", "sssssssssssssssssssssssss"),
        ("ſſAſſa", "ssassa"),
    ];
    for (input, expected) in cases {
        assert_eq!(to_normalised_lower(input), expected, "input={input}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/regexp_test.go::TestIsSimpleConcatenationPattern
#[test]
fn test_is_simple_concatenation_pattern() {
    let cases = [
        (".*-.*-.*-.*-.*", true),
        (".+-.*-.*-.*-.+", false),
        ("-.*-.*-.*-.*", false),
        (".*-.*-.*-.*-", false),
        ("-", false),
        (".*", false),
        (r".*\|foo\|.*", false),
        (r".*\|(foo)\|.*", false),
    ];
    for (pat, expected) in cases {
        let parsed = parse_go_syntax(pat).unwrap();
        assert_eq!(is_simple_concatenation_pattern(&parsed), expected, "pat={pat}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/labels/sharding_test.go::TestStableHash
#[test]
fn test_stable_hash() {
    let cases = [
        (0xef46db3751d8e999u64, Labels::empty()),
        (0x347c8ee7a9e29708u64, Labels::from_strings(&["hello", "world"])),
        (0xcbab40540f26097du64, Labels::from_strings(&[METRIC_NAME, "metric", "label", "value"])),
    ];
    for (expected, lbls) in cases {
        assert_eq!(lbls.stable_hash(), expected);
    }
}

// ============================================================================
// model/histogram tests (42 / 42)
// ============================================================================

fn sp(offset: i32, length: u32) -> HistogramSpan {
    HistogramSpan { offset, length }
}

enum NhcbOwned {
    H(Histogram),
    F(FloatHistogram),
    None,
}

impl NhcbOwned {
    fn as_ref(&self) -> Option<NhcbRef<'_>> {
        match self {
            Self::H(h) => Some(NhcbRef::Histogram(h)),
            Self::F(f) => Some(NhcbRef::FloatHistogram(f)),
            Self::None => None,
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/convert_test.go::TestConvertNHCBToClassicHistogram
#[test]
fn test_convert_nhcb_to_classic_histogram() {
    let cases: Vec<(&str, NhcbOwned, Labels, bool, Vec<(Labels, f64)>)> = vec![
        (
            "valid histogram",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0, 2.0, 3.0],
                positive_buckets: vec![10, 20, 30],
                positive_spans: vec![sp(0, 3)],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), 40.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "3.0"]), 100.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 100.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 100.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 100.0),
            ],
        ),
        (
            "valid floatHistogram",
            NhcbOwned::F(FloatHistogram {
                custom_values: vec![1.0, 2.0, 3.0],
                positive_buckets: vec![20.0, 40.0, 60.0],
                positive_spans: vec![sp(0, 3)],
                count: 120.0,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 20.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), 60.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "3.0"]), 120.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 120.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 120.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 100.0),
            ],
        ),
        (
            "empty histogram",
            NhcbOwned::H(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 0.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 0.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 0.0),
            ],
        ),
        (
            "missing __name__ label",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0, 2.0, 3.0],
                positive_buckets: vec![10, 20, 30],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["job", "test_job"]),
            true,
            vec![],
        ),
        (
            "unsupported histogram type",
            NhcbOwned::None,
            Labels::from_strings(&["__name__", "test_metric"]),
            true,
            vec![],
        ),
        (
            "histogram with zero bucket counts",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0, 2.0, 3.0],
                positive_buckets: vec![0, 10, 0],
                positive_spans: vec![sp(0, 3)],
                count: 20,
                sum: 50.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 0.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "3.0"]), 20.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 20.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 20.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 50.0),
            ],
        ),
        (
            "extra bucket counts than custom values",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0, 2.0],
                positive_buckets: vec![10, 20, 30],
                positive_spans: vec![sp(0, 3)],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), 40.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 100.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 100.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 100.0),
            ],
        ),
        (
            "mismatched bucket lengths",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0, 2.0],
                positive_buckets: vec![10],
                positive_spans: vec![sp(0, 2)],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric_bucket"]),
            true,
            vec![],
        ),
        (
            "single series Histogram",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0],
                positive_buckets: vec![10],
                positive_spans: vec![sp(0, 1)],
                count: 10,
                sum: 20.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 20.0),
            ],
        ),
        (
            "multiset label histogram",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0],
                positive_buckets: vec![10],
                positive_spans: vec![sp(0, 1)],
                count: 10,
                sum: 20.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric", "job", "test_job", "instance", "localhost:9090"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "job", "test_job", "instance", "localhost:9090", "le", "1.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "job", "test_job", "instance", "localhost:9090", "le", "+Inf"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_count", "job", "test_job", "instance", "localhost:9090"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_sum", "job", "test_job", "instance", "localhost:9090"]), 20.0),
            ],
        ),
        (
            "exponential histogram",
            NhcbOwned::F(FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![sp(-2, 1), sp(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![sp(3, 2), sp(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric_bucket"]),
            true,
            vec![],
        ),
        (
            "sparse histogram",
            NhcbOwned::H(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                positive_spans: vec![sp(0, 2), sp(4, 1), sp(1, 2)],
                positive_buckets: vec![1, 2, 3, 4, 5],
                count: 35,
                sum: 123.0,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 1.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), 4.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "3.0"]), 4.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "4.0"]), 4.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "5.0"]), 4.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "6.0"]), 4.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "7.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "8.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "9.0"]), 20.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "10.0"]), 35.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 35.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 35.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 123.0),
            ],
        ),
        (
            "sparse float histogram",
            NhcbOwned::F(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                positive_spans: vec![sp(0, 2), sp(4, 1), sp(1, 2)],
                positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0],
                count: 15.0,
                sum: 123.0,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), 1.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), 3.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "3.0"]), 3.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "4.0"]), 3.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "5.0"]), 3.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "6.0"]), 3.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "7.0"]), 6.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "8.0"]), 6.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "9.0"]), 10.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "10.0"]), 15.0),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), 15.0),
                (Labels::from_strings(&["__name__", "test_metric_count"]), 15.0),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), 123.0),
            ],
        ),
        (
            "stale histogram",
            NhcbOwned::H(Histogram {
                custom_values: vec![1.0, 2.0],
                positive_buckets: vec![2, 1, 1],
                positive_spans: vec![sp(0, 3)],
                count: 9,
                sum: f64::from_bits(STALE_NAN_BITS),
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            Labels::from_strings(&["__name__", "test_metric"]),
            false,
            vec![
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "1.0"]), f64::from_bits(STALE_NAN_BITS)),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "2.0"]), f64::from_bits(STALE_NAN_BITS)),
                (Labels::from_strings(&["__name__", "test_metric_bucket", "le", "+Inf"]), f64::from_bits(STALE_NAN_BITS)),
                (Labels::from_strings(&["__name__", "test_metric_count"]), f64::from_bits(STALE_NAN_BITS)),
                (Labels::from_strings(&["__name__", "test_metric_sum"]), f64::from_bits(STALE_NAN_BITS)),
            ],
        ),
    ];

    let mut lb = LabelsBuilder::new(Labels::empty());
    for (name, nhcb_owned, lbls, expect_err, expected) in cases {
        let nhcb = nhcb_owned.as_ref();
        let mut emitted = Vec::new();
        let res = convert_nhcb_to_classic(nhcb, &lbls, &mut lb, "", None, |l, v| {
            emitted.push((l, v));
            Ok(())
        });
        assert_eq!(res.is_err(), expect_err, "{name}: {res:?}");
        if !expect_err {
            assert_eq!(emitted.len(), expected.len(), "{name}");
            for (i, ((el, ev), (gl, gv))) in expected.iter().zip(emitted.iter()).enumerate() {
                assert!(Labels::equal(el, gl), "{name}[{i}]: {el} != {gl}");
                if ev.to_bits() == STALE_NAN_BITS {
                    assert_eq!(gv.to_bits(), STALE_NAN_BITS, "{name}[{i}]");
                } else {
                    assert_eq!(gv, ev, "{name}[{i}]");
                }
            }
            let mut scratch = vec![42.0; 16];
            let (mut buckets, count, sum) = classic_bucket_counts(nhcb, &mut scratch).unwrap();
            buckets.push(count);
            buckets.push(sum);
            assert_eq!(buckets.len(), emitted.len(), "{name}");
            for (i, ((_, sv), &cv)) in emitted.iter().zip(buckets.iter()).enumerate() {
                if sv.to_bits() == STALE_NAN_BITS {
                    assert_eq!(cv.to_bits(), STALE_NAN_BITS, "{name}[{i}]");
                } else {
                    assert_eq!(cv, *sv, "{name}[{i}]");
                }
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/convert_test.go::TestConvertNHCBToClassicHistogram_CacheMatchesNoCache
#[test]
fn test_convert_nhcb_to_classic_histogram_cache_matches_no_cache() {
    let h = Histogram {
        custom_values: vec![1.0, 2.0, 3.0],
        positive_buckets: vec![10, 20, 30],
        positive_spans: vec![sp(0, 3)],
        count: 100,
        sum: 100.0,
        schema: CUSTOM_BUCKETS_SCHEMA,
        ..Default::default()
    };
    let lsets = [
        Labels::from_strings(&["__name__", "test_metric", "job", "job_a"]),
        Labels::from_strings(&["__name__", "test_metric", "job", "job_a"]),
        Labels::from_strings(&["__name__", "test_metric", "job", "job_b"]),
        Labels::from_strings(&["__name__", "other_metric", "job", "job_b"]),
    ];
    let suffixes = ["", CLASSIC_SUFFIX_BUCKET, CLASSIC_SUFFIX_COUNT, CLASSIC_SUFFIX_SUM];
    let mut lb = LabelsBuilder::new(Labels::empty());

    for suffix in suffixes {
        let mut cache = ClassicSeriesCache::default();
        for lset in &lsets {
            let mut without = Vec::new();
            let mut with = Vec::new();
            convert_nhcb_to_classic(Some(NhcbRef::Histogram(&h)), lset, &mut lb, suffix, None, |l, v| {
                without.push((l, v));
                Ok(())
            })
            .unwrap();
            convert_nhcb_to_classic(Some(NhcbRef::Histogram(&h)), lset, &mut lb, suffix, Some(&mut cache), |l, v| {
                with.push((l, v));
                Ok(())
            })
            .unwrap();
            assert_eq!(with, without);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/convert_test.go::TestConvertNHCBToClassicIntFloatAgreement
#[test]
fn test_convert_nhcb_to_classic_int_float_agreement() {
    let h = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        positive_spans: vec![sp(0, 2), sp(4, 1), sp(1, 2)],
        positive_buckets: vec![1, 2, 3, 4, 5],
        count: 35,
        sum: 123.0,
        ..Default::default()
    };
    h.validate().unwrap();
    let fh = h.to_float();
    let lset = Labels::from_strings(&["__name__", "test_metric"]);
    let mut lb = LabelsBuilder::new(Labels::empty());

    let mut from_int = Vec::new();
    convert_nhcb_to_classic(Some(NhcbRef::Histogram(&h)), &lset, &mut lb, "", None, |l, v| {
        from_int.push((l, v));
        Ok(())
    })
    .unwrap();

    let mut from_float = Vec::new();
    convert_nhcb_to_classic(Some(NhcbRef::FloatHistogram(&fh)), &lset, &mut lb, "", None, |l, v| {
        from_float.push((l, v));
        Ok(())
    })
    .unwrap();

    assert_eq!(from_int, from_float);

    let mut inf_bucket = 0.0;
    let mut count = 0.0;
    for (l, v) in from_int {
        match l.get(METRIC_NAME) {
            "test_metric_bucket" if l.get("le") == "+Inf" => inf_bucket = v,
            "test_metric_count" => count = v,
            _ => {}
        }
    }
    assert_eq!(count, inf_bucket);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/generic_test.go::TestGetBoundExponential
#[test]
fn test_get_bound_exponential() {
    let cases = [
        (-1, -1, 0.25),
        (0, -1, 1.0),
        (1, -1, 4.0),
        (512, -1, f64::MAX),
        (513, -1, f64::INFINITY),
        (-1, 0, 0.5),
        (0, 0, 1.0),
        (1, 0, 2.0),
        (1024, 0, f64::MAX),
        (1025, 0, f64::INFINITY),
        (-1, 2, 0.8408964152537144),
        (0, 2, 1.0),
        (1, 2, 1.189207115002721),
        (4096, 2, f64::MAX),
        (4097, 2, f64::INFINITY),
    ];
    for (idx, schema, want) in cases {
        assert_eq!(get_bound_exponential(idx, schema), want, "idx={idx} schema={schema}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/generic_test.go::TestBucketFractionBelow
#[test]
fn test_bucket_fraction_below() {
    let cases = [
        (0.0, 10.0, 5.0, true, 0.5),
        (2.0, 6.0, 3.0, true, 0.25),
        (-10.0, -5.0, -6.0, true, 0.8),
        (2.0, 8.0, 4.0, false, 0.5),
        (1.0, 16.0, 2.0, false, 0.25),
        (-8.0, -2.0, -4.0, false, 0.5),
        (-4.0, -2.0, -3.0, false, 0.41503749927884383),
    ];
    for (lower, upper, v, linear, want) in cases {
        let b = Bucket {
            lower,
            upper,
            lower_inclusive: false,
            upper_inclusive: true,
            count: 100.0,
            index: 0,
        };
        assert!((b.fraction_below(v, linear) - want).abs() < 1e-12);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/generic_test.go::TestReduceResolutionHistogram
#[test]
fn test_reduce_resolution_histogram() {
    let spans = vec![sp(0, 4), sp(0, 0), sp(3, 2)];
    let buckets = vec![1i64, 2, -2, 1, -1, 0];
    let (got_spans, got_buckets) = reduce_resolution_generic(&spans, &buckets, 0, -1, true).unwrap();
    assert_eq!(got_spans, vec![sp(0, 3), sp(1, 1)]);
    assert_eq!(got_buckets, vec![1, 3, -2, 0]);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/generic_test.go::TestReduceResolutionFloatHistogram
#[test]
fn test_reduce_resolution_float_histogram() {
    let spans = vec![sp(0, 4), sp(0, 0), sp(3, 2)];
    let buckets = vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0];
    let (got_spans, got_buckets) = reduce_resolution_generic(&spans, &buckets, 0, -1, false).unwrap();
    assert_eq!(got_spans, vec![sp(0, 3), sp(1, 1)]);
    assert_eq!(got_buckets, vec![1.0, 4.0, 2.0, 2.0]);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/generic_test.go::TestCustomBucketBoundsMatch
#[test]
fn test_custom_bucket_bounds_match() {
    let cases: &[(&[f64], &[f64], bool)] = &[
        (&[], &[], true),
        (&[], &[1.0], false),
        (&[1.0, 2.0], &[1.0, 2.0, 3.0], false),
        (&[1.5], &[1.5], true),
        (&[1.5], &[2.5], false),
        (&[1.0, 2.0, 3.0, 4.0, 5.0], &[1.0, 2.0, 3.0, 4.0, 5.0], true),
        (&[1.0, 2.1, 3.0], &[1.0, 2.0, 3.0], false),
    ];
    for &(c1, c2, want) in cases {
        assert_eq!(custom_bucket_bounds_match(c1, c2), want);
        assert_eq!(custom_bucket_bounds_match(c2, c1), want);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramString
#[test]
fn test_histogram_string() {
    let cases = [
        (
            Histogram { schema: 0, ..Default::default() },
            "{count:0, sum:0}",
        ),
        (
            Histogram {
                schema: 0,
                count: 9,
                sum: -3.1415,
                zero_count: 12,
                zero_threshold: 0.001,
                negative_spans: vec![sp(0, 5), sp(1, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            "{count:9, sum:-3.1415, [-64,-32):1, [-16,-8):1, [-8,-4):2, [-4,-2):1, [-2,-1):3, [-1,-0.5):1, [-0.001,0.001]:12}",
        ),
        (
            Histogram {
                schema: 0,
                count: 19,
                sum: 2.7,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![sp(0, 5), sp(1, 0), sp(0, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            "{count:19, sum:2.7, [-64,-32):1, [-16,-8):1, [-8,-4):2, [-4,-2):1, [-2,-1):3, [-1,-0.5):1, (0.5,1]:1, (1,2]:3, (2,4]:1, (4,8]:2, (8,16]:1, (16,32]:1, (32,64]:1}",
        ),
        (
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 19,
                sum: 2.7,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                custom_values: vec![1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0, 50.0],
                ..Default::default()
            },
            "{count:19, sum:2.7, [-Inf,1]:1, (1,2]:3, (2,5]:1, (5,10]:2, (10,15]:1, (15,20]:1, (20,25]:1}",
        ),
    ];
    for (h, exp) in cases {
        assert_eq!(h.to_string(), exp);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestCumulativeBucketIterator
#[test]
fn test_cumulative_bucket_iterator() {
    let b = |upper: f64, count: u64, index: i32| Bucket {
        lower: f64::NEG_INFINITY,
        upper,
        lower_inclusive: true,
        upper_inclusive: true,
        count,
        index,
    };
    let cases = [
        (
            Histogram { schema: 0, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], ..Default::default() },
            vec![b(1.0, 1, 0), b(2.0, 3, 1), b(4.0, 3, 2), b(8.0, 4, 3), b(16.0, 5, 4)],
        ),
        (
            Histogram { schema: 0, positive_spans: vec![sp(0, 5), sp(1, 1)], positive_buckets: vec![1, 2, -2, 1, -1, 0], ..Default::default() },
            vec![b(1.0, 1, 0), b(2.0, 4, 1), b(4.0, 5, 2), b(8.0, 7, 3), b(16.0, 8, 4), b(32.0, 8, 5), b(64.0, 9, 6)],
        ),
        (
            Histogram { schema: 0, positive_spans: vec![sp(0, 7)], positive_buckets: vec![1, 2, -2, 1, -1, 0, 0], ..Default::default() },
            vec![b(1.0, 1, 0), b(2.0, 4, 1), b(4.0, 5, 2), b(8.0, 7, 3), b(16.0, 8, 4), b(32.0, 9, 5), b(64.0, 10, 6)],
        ),
        (
            Histogram { schema: 3, positive_spans: vec![sp(-5, 2), sp(2, 3), sp(2, 2)], positive_buckets: vec![1, 2, -2, 1, -1, 0, 3], ..Default::default() },
            vec![
                b(0.6484197773255048, 1, -5), b(0.7071067811865475, 4, -4), b(0.7711054127039704, 4, -3),
                b(0.8408964152537144, 4, -2), b(0.9170040432046711, 5, -1), b(1.0, 7, 0),
                b(1.0905077326652577, 8, 1), b(1.189207115002721, 8, 2), b(1.2968395546510096, 8, 3),
                b(1.414213562373095, 9, 4), b(1.5422108254079407, 13, 5),
            ],
        ),
        (
            Histogram { schema: -2, positive_spans: vec![sp(-2, 4), sp(2, 2)], positive_buckets: vec![1, 2, -2, 1, -1, 0], ..Default::default() },
            vec![b(0.00390625, 1, -2), b(0.0625, 4, -1), b(1.0, 5, 0), b(16.0, 7, 1), b(256.0, 7, 2), b(4096.0, 7, 3), b(65536.0, 8, 4), b(1048576.0, 9, 5)],
        ),
        (
            Histogram { schema: -1, positive_spans: vec![sp(-2, 5)], positive_buckets: vec![1, 2, -2, 1, -1], ..Default::default() },
            vec![b(0.0625, 1, -2), b(0.25, 4, -1), b(1.0, 5, 0), b(4.0, 7, 1), b(16.0, 8, 2)],
        ),
        (
            Histogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![5.0, 10.0, 20.0, 50.0], ..Default::default() },
            vec![b(5.0, 1, 0), b(10.0, 3, 1), b(20.0, 3, 2), b(50.0, 4, 3), b(f64::INFINITY, 5, 4)],
        ),
        (
            Histogram { schema: 0, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0, 2], ..Default::default() },
            vec![b(1.0, 1, 0), b(2.0, 3, 1), b(4.0, 3, 2), b(8.0, 4, 3), b(16.0, 5, 4)],
        ),
        (
            Histogram { schema: 0, positive_spans: vec![sp(0, 2), sp(1, 3)], positive_buckets: vec![1, 1, -1, 0], ..Default::default() },
            vec![b(1.0, 1, 0), b(2.0, 3, 1), b(4.0, 3, 2), b(8.0, 4, 3), b(16.0, 5, 4)],
        ),
    ];
    for (h, exp) in cases {
        let got: Vec<_> = h.cumulative_bucket_iter().collect();
        assert_eq!(got, exp);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestRegularBucketIterator
#[test]
fn test_regular_bucket_iterator() {
    let pb = |lower, upper, count, index| Bucket { lower, upper, lower_inclusive: false, upper_inclusive: true, count, index };
    let nb = |lower, upper, count, index| Bucket { lower, upper, lower_inclusive: true, upper_inclusive: false, count, index };
    let cb = |lower: f64, upper: f64, count: u64, index: i32| Bucket { lower, upper, lower_inclusive: index == 0, upper_inclusive: true, count, index };

    let cases = [
        (Histogram { schema: 0, ..Default::default() }, vec![], vec![]),
        (
            Histogram { schema: 0, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], ..Default::default() },
            vec![pb(0.5, 1.0, 1, 0), pb(1.0, 2.0, 2, 1), pb(4.0, 8.0, 1, 3), pb(8.0, 16.0, 1, 4)],
            vec![],
        ),
        (
            Histogram { schema: 0, negative_spans: vec![sp(0, 5), sp(1, 1)], negative_buckets: vec![1, 2, -2, 1, -1, 0], ..Default::default() },
            vec![],
            vec![nb(-1.0, -0.5, 1, 0), nb(-2.0, -1.0, 3, 1), nb(-4.0, -2.0, 1, 2), nb(-8.0, -4.0, 2, 3), nb(-16.0, -8.0, 1, 4), nb(-64.0, -32.0, 1, 6)],
        ),
        (
            Histogram {
                schema: 0,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![sp(0, 5), sp(1, 0), sp(0, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            vec![pb(0.5, 1.0, 1, 0), pb(1.0, 2.0, 3, 1), pb(2.0, 4.0, 1, 2), pb(4.0, 8.0, 2, 3), pb(8.0, 16.0, 1, 4), pb(16.0, 32.0, 1, 5), pb(32.0, 64.0, 1, 6)],
            vec![nb(-1.0, -0.5, 1, 0), nb(-2.0, -1.0, 3, 1), nb(-4.0, -2.0, 1, 2), nb(-8.0, -4.0, 2, 3), nb(-16.0, -8.0, 1, 4), nb(-64.0, -32.0, 1, 6)],
        ),
        (
            Histogram { schema: 3, positive_spans: vec![sp(-5, 2), sp(2, 3), sp(2, 2)], positive_buckets: vec![1, 2, -2, 1, -1, 0, 3], ..Default::default() },
            vec![
                pb(0.5946035575013605, 0.6484197773255048, 1, -5),
                pb(0.6484197773255048, 0.7071067811865475, 3, -4),
                pb(0.8408964152537144, 0.9170040432046711, 1, -1),
                pb(0.9170040432046711, 1.0, 2, 0),
                pb(1.0, 1.0905077326652577, 1, 1),
                pb(1.2968395546510096, 1.414213562373095, 1, 4),
                pb(1.414213562373095, 1.5422108254079407, 4, 5),
            ],
            vec![],
        ),
        (
            Histogram { schema: -2, positive_spans: vec![sp(-2, 4), sp(2, 2)], positive_buckets: vec![1, 2, -2, 1, -1, 0], ..Default::default() },
            vec![pb(0.000244140625, 0.00390625, 1, -2), pb(0.00390625, 0.0625, 3, -1), pb(0.0625, 1.0, 1, 0), pb(1.0, 16.0, 2, 1), pb(4096.0, 65536.0, 1, 4), pb(65536.0, 1048576.0, 1, 5)],
            vec![],
        ),
        (
            Histogram { schema: -1, positive_spans: vec![sp(-2, 5)], positive_buckets: vec![1, 2, -2, 1, -1], ..Default::default() },
            vec![pb(0.015625, 0.0625, 1, -2), pb(0.0625, 0.25, 3, -1), pb(0.25, 1.0, 1, 0), pb(1.0, 4.0, 2, 1), pb(4.0, 16.0, 1, 2)],
            vec![],
        ),
        (
            Histogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![5.0, 10.0, 20.0, 50.0], ..Default::default() },
            vec![cb(f64::NEG_INFINITY, 5.0, 1, 0), cb(5.0, 10.0, 2, 1), cb(20.0, 50.0, 1, 3), cb(50.0, f64::INFINITY, 1, 4)],
            vec![],
        ),
        (
            Histogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![0.0, 10.0, 20.0, 50.0], ..Default::default() },
            vec![cb(f64::NEG_INFINITY, 0.0, 1, 0), cb(0.0, 10.0, 2, 1), cb(20.0, 50.0, 1, 3), cb(50.0, f64::INFINITY, 1, 4)],
            vec![],
        ),
        (
            Histogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 5)], positive_buckets: vec![1, 1, 0, -1, 0], custom_values: vec![-5.0, 0.0, 20.0, 50.0], ..Default::default() },
            vec![cb(f64::NEG_INFINITY, -5.0, 1, 0), cb(-5.0, 0.0, 2, 1), cb(0.0, 20.0, 2, 2), cb(20.0, 50.0, 1, 3), cb(50.0, f64::INFINITY, 1, 4)],
            vec![],
        ),
        (
            Histogram { schema: 0, negative_spans: vec![sp(0, 5), sp(1, 1)], negative_buckets: vec![1, 2, -2, 1, -1, 0, 3], ..Default::default() },
            vec![],
            vec![nb(-1.0, -0.5, 1, 0), nb(-2.0, -1.0, 3, 1), nb(-4.0, -2.0, 1, 2), nb(-8.0, -4.0, 2, 3), nb(-16.0, -8.0, 1, 4), nb(-64.0, -32.0, 1, 6)],
        ),
        (
            Histogram { schema: 0, negative_spans: vec![sp(0, 5), sp(1, 1)], negative_buckets: vec![1, 2, -2, 1, -1], ..Default::default() },
            vec![],
            vec![nb(-1.0, -0.5, 1, 0), nb(-2.0, -1.0, 3, 1), nb(-4.0, -2.0, 1, 2), nb(-8.0, -4.0, 2, 3), nb(-16.0, -8.0, 1, 4)],
        ),
    ];

    for (h, exp_pos, exp_neg) in cases {
        assert_eq!(h.positive_bucket_iter().collect::<Vec<_>>(), exp_pos);
        assert_eq!(h.negative_bucket_iter().collect::<Vec<_>>(), exp_neg);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramToFloat
#[test]
fn test_histogram_to_float() {
    let h = Histogram {
        schema: 3,
        count: 61,
        sum: 2.7,
        zero_threshold: 0.1,
        zero_count: 42,
        positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        negative_spans: vec![sp(0, 5), sp(1, 0), sp(0, 1)],
        negative_buckets: vec![1, 2, -2, 1, -1, 0],
        ..Default::default()
    };
    let mut prior_more = FloatHistogram {
        schema: 2, count: 3.0, sum: 5.0, zero_threshold: 4.0, zero_count: 1.0,
        positive_spans: vec![sp(1, 2); 3], positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        negative_spans: vec![sp(20, 6), sp(12, 7), sp(33, 10)], negative_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        ..Default::default()
    };
    let mut prior_fewer = FloatHistogram {
        schema: 2, count: 3.0, sum: 5.0, zero_threshold: 4.0, zero_count: 1.0,
        positive_spans: vec![sp(1, 2); 3], positive_buckets: vec![1.0, 2.0],
        negative_spans: vec![sp(20, 6), sp(12, 7), sp(33, 10)], negative_buckets: vec![1.0, 2.0],
        ..Default::default()
    };

    assert_eq!(h.to_string(), h.to_float_into(None).to_string());
    assert_eq!(h.to_string(), h.to_float_into(Some(&mut prior_more)).to_string());
    assert_eq!(h.to_string(), h.to_float_into(Some(&mut prior_fewer)).to_string());
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestCustomBucketsHistogramToFloat
#[test]
fn test_custom_buckets_histogram_to_float() {
    let h = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10,
        sum: 2.7,
        positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        custom_values: vec![5.0, 10.0, 20.0, 50.0, 100.0, 500.0],
        ..Default::default()
    };
    h.validate().unwrap();
    let h_str = h.to_string();

    let mut prior_more = FloatHistogram {
        schema: 2, count: 3.0, sum: 5.0, zero_threshold: 4.0, zero_count: 1.0,
        positive_spans: vec![sp(1, 2); 3], positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        negative_spans: vec![sp(20, 6), sp(12, 7), sp(33, 10)], negative_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        ..Default::default()
    };
    let mut prior_fewer = FloatHistogram {
        schema: 2, count: 3.0, sum: 5.0, zero_threshold: 4.0, zero_count: 1.0,
        positive_spans: vec![sp(1, 2); 3], positive_buckets: vec![1.0, 2.0],
        negative_spans: vec![sp(20, 6), sp(12, 7), sp(33, 10)], negative_buckets: vec![1.0, 2.0],
        ..Default::default()
    };

    for fh in [
        h.to_float(),
        h.to_float_into(Some(&mut prior_more)),
        h.to_float_into(Some(&mut prior_fewer)),
    ] {
        fh.validate().unwrap();
        assert_eq!(h.to_string(), h_str);
        assert_eq!(fh.to_string(), h_str);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramEquals
#[test]
fn test_histogram_equals() {
    let h1 = Histogram {
        schema: 3,
        count: 62,
        sum: 2.7,
        zero_threshold: 0.1,
        zero_count: 42,
        positive_spans: vec![sp(0, 4), sp(10, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        negative_spans: vec![sp(0, 4), sp(10, 3)],
        negative_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        ..Default::default()
    };
    h1.validate().unwrap();

    let equals = |a: &Histogram, b: &Histogram| {
        assert!(a.prom_equals(b));
        assert!(b.prom_equals(a));
        assert!(a.to_float().prom_equals(&b.to_float()));
        assert!(b.to_float().prom_equals(&a.to_float()));
    };
    let not_equals = |a: &Histogram, b: &Histogram| {
        assert!(!a.prom_equals(b));
        assert!(!b.prom_equals(a));
        assert!(!a.to_float().prom_equals(&b.to_float()));
        assert!(!b.to_float().prom_equals(&a.to_float()));
    };
    let not_equals_until_float = |a: &Histogram, b: &Histogram| {
        assert!(!a.prom_equals(b));
        assert!(!b.prom_equals(a));
        assert!(a.to_float().prom_equals(&b.to_float()));
        assert!(b.to_float().prom_equals(&a.to_float()));
    };

    let mut h2 = h1.copy();
    equals(&h1, &h2);
    h2.positive_spans.push(sp(5, 0));
    h2.negative_spans.push(sp(2, 0));
    equals(&h1, &h2);
    h2.positive_spans = vec![h2.positive_spans[0], sp(1, 0), sp(3, 0), sp(6, 3), h2.positive_spans[2]];
    h2.negative_spans = vec![h2.negative_spans[0], sp(2, 0), sp(3, 0), sp(5, 3), h2.negative_spans[2]];
    equals(&h1, &h2);

    not_equals(&h1, &Histogram::default());
    h2.schema = 1;
    not_equals(&h1, &h2);
    h2 = h1.copy(); h2.count += 1; not_equals(&h1, &h2);
    h2 = h1.copy(); h2.sum += 1.0; not_equals(&h1, &h2);
    h2 = h1.copy(); h2.zero_threshold += 1.0; not_equals(&h1, &h2);
    h2 = h1.copy(); h2.zero_count += 1; not_equals(&h1, &h2);
    h2 = h1.copy(); *h2.positive_buckets.last_mut().unwrap() += 1; not_equals(&h1, &h2);
    h2 = h1.copy(); *h2.negative_buckets.last_mut().unwrap() += 1; not_equals(&h1, &h2);
    h2 = h1.copy(); h2.positive_spans[1].offset += 1; not_equals(&h1, &h2);
    h2 = h1.copy(); h2.negative_spans[1].offset += 1; not_equals(&h1, &h2);
    h2 = h1.copy(); h2.positive_spans[0].offset -= 1; h2.positive_spans[0].length += 1; h2.positive_buckets.insert(0, 0); not_equals(&h1, &h2);
    h2 = h1.copy(); h2.negative_spans[0].offset -= 1; h2.negative_spans[0].length += 1; h2.negative_buckets.insert(0, 0); not_equals(&h1, &h2);
    h2 = h1.copy(); h2.positive_spans.push(sp(1, 1)); h2.positive_buckets.push(1); not_equals(&h1, &h2);
    h2 = h1.copy(); h2.negative_spans.push(sp(1, 1)); h2.negative_buckets.push(1); not_equals(&h1, &h2);

    let mut h_stale = h1.copy(); h_stale.sum = f64::from_bits(STALE_NAN_BITS);
    not_equals(&h1, &h_stale); equals(&h_stale, &h_stale);
    let mut h_nan = h1.copy(); h_nan.sum = f64::NAN;
    not_equals(&h1, &h_nan); equals(&h_nan, &h_nan);
    not_equals(&h_stale, &h_nan);

    let mut h_custom = h1.copy(); h_custom.custom_values = vec![1.0, 2.0, 3.0];
    equals(&h1, &h_custom);

    let cbh1 = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10,
        sum: 2.7,
        positive_spans: vec![sp(0, 4), sp(10, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        custom_values: vec![0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0, 50.0, 75.0, 100.0, 200.0, 250.0, 500.0, 1000.0],
        ..Default::default()
    };
    cbh1.validate().unwrap();
    let mut cbh2 = cbh1.copy();
    equals(&cbh1, &cbh2);
    cbh2.custom_values = vec![0.1, 0.2, 0.5];
    not_equals(&cbh1, &cbh2);
    cbh2 = cbh1.copy(); cbh2.negative_spans = vec![sp(0, 1)]; cbh2.negative_buckets = vec![1];
    not_equals_until_float(&cbh1, &cbh2);
    cbh2 = cbh1.copy(); cbh2.zero_threshold = 0.1; cbh2.zero_count = 10;
    not_equals_until_float(&cbh1, &cbh2);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramCopy
#[test]
fn test_histogram_copy() {
    let cases = [
        (Histogram::default(), Histogram::default()),
        (
            Histogram {
                positive_spans: vec![sp(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![sp(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
            Histogram {
                positive_spans: vec![sp(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![sp(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
        ),
        (
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![sp(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: vec![5.0, 10.0, 15.0],
                ..Default::default()
            },
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![sp(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: vec![5.0, 10.0, 15.0],
                ..Default::default()
            },
        ),
    ];
    for (mut orig, expected) in cases {
        let mut h_copy = orig.copy();
        orig.sum += 1.0;
        orig.positive_spans.push(sp(1, 2));
        assert_eq!(h_copy, expected);
        h_copy.positive_spans.push(sp(9, 9));
        assert_ne!(h_copy, orig);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramCopyTo
#[test]
fn test_histogram_copy_to() {
    let orig = Histogram {
        positive_spans: vec![sp(-2, 1)],
        positive_buckets: vec![1, 3, -3, 42],
        negative_spans: vec![sp(3, 2)],
        negative_buckets: vec![5, 3, 123400, 1000],
        ..Default::default()
    };
    let expected = orig.copy();
    let mut h_copy = Histogram::default();
    orig.copy_to(&mut h_copy);
    assert_eq!(h_copy, expected);

    let orig_cb = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(-2, 1)],
        positive_buckets: vec![1, 3, -3, 42],
        custom_values: vec![5.0, 10.0, 15.0],
        ..Default::default()
    };
    let expected_cb = orig_cb.copy();
    orig_cb.copy_to(&mut h_copy);
    assert_eq!(h_copy, expected_cb);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramCompact
#[test]
fn test_histogram_compact() {
    let cases = [
        ("empty", Histogram::default(), 0, Histogram::default()),
        (
            "nothing should happen",
            Histogram { positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1, 3, -3, 42], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![5, 3, 123400, 1000], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1, 3, -3, 42], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![5, 3, 123400, 1000], ..Default::default() },
        ),
        (
            "eliminate zero offsets",
            Histogram { positive_spans: vec![sp(-2, 1), sp(0, 3), sp(0, 1)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(0, 2), sp(2, 1), sp(0, 1)], negative_buckets: vec![5, 3, 123400, 1000, 3, 4], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 5)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 4), sp(2, 2)], negative_buckets: vec![5, 3, 123400, 1000, 3, 4], ..Default::default() },
        ),
        (
            "eliminate zero length",
            Histogram { positive_spans: vec![sp(-2, 2), sp(2, 0), sp(3, 3)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(0, 0), sp(2, 0), sp(1, 4)], negative_buckets: vec![5, 3, 123400, 1000, 3, 4], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 2), sp(5, 3)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 4)], negative_buckets: vec![5, 3, 123400, 1000, 3, 4], ..Default::default() },
        ),
        (
            "eliminate multiple zero length spans",
            Histogram { positive_spans: vec![sp(-2, 2), sp(2, 0), sp(2, 0), sp(2, 0), sp(3, 3)], positive_buckets: vec![1, 3, -3, 42, 3], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 2), sp(9, 3)], positive_buckets: vec![1, 3, -3, 42, 3], ..Default::default() },
        ),
        (
            "cut empty buckets at start or end",
            Histogram { positive_spans: vec![sp(-4, 4), sp(5, 3)], positive_buckets: vec![0, 0, 1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, 3, 4, -9], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 2), sp(5, 3)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 4)], negative_buckets: vec![5, 3, -4, -2, 3, 4], ..Default::default() },
        ),
        (
            "cut empty buckets at start and end",
            Histogram { positive_spans: vec![sp(-4, 4), sp(5, 6)], positive_buckets: vec![0, 0, 1, 3, -3, 42, 3, -46, 0, 0], negative_spans: vec![sp(-2, 4), sp(3, 5)], negative_buckets: vec![0, 0, 5, 3, -4, -2, 3, 4, -9], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 2), sp(5, 3)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 4)], negative_buckets: vec![5, 3, -4, -2, 3, 4], ..Default::default() },
        ),
        (
            "cut empty buckets at start or end of spans, even in the middle",
            Histogram { positive_spans: vec![sp(-4, 6), sp(3, 6)], positive_buckets: vec![0, 0, 1, 3, -4, 0, 1, 42, 3, -46, 0, 0], negative_spans: vec![sp(0, 2), sp(2, 6)], negative_buckets: vec![5, 3, -8, 4, -2, 3, 4, -9], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 2), sp(5, 3)], positive_buckets: vec![1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 4)], negative_buckets: vec![5, 3, -4, -2, 3, 4], ..Default::default() },
        ),
        (
            "cut empty buckets at start or end but merge spans due to maxEmptyBuckets",
            Histogram { positive_spans: vec![sp(-4, 4), sp(5, 3)], positive_buckets: vec![0, 0, 1, 3, -3, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, 3, 4, -9], ..Default::default() },
            10,
            Histogram { positive_spans: vec![sp(-2, 10)], positive_buckets: vec![1, 3, -4, 0, 0, 0, 0, 1, 42, 3], negative_spans: vec![sp(0, 9)], negative_buckets: vec![5, 3, -8, 0, 0, 4, -2, 3, 4], ..Default::default() },
        ),
        (
            "cut empty buckets from the middle of a span",
            Histogram { positive_spans: vec![sp(-4, 6), sp(3, 3)], positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, -2, 3, 4], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 1), sp(2, 1), sp(3, 3)], positive_buckets: vec![1, 2, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 2), sp(1, 2)], negative_buckets: vec![5, 3, -4, -2, 1, 4], ..Default::default() },
        ),
        (
            "cut out a span containing only empty buckets",
            Histogram { positive_spans: vec![sp(-4, 3), sp(2, 2), sp(3, 4)], positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-2, 1), sp(7, 4)], positive_buckets: vec![1, 2, -2, 42, 3], ..Default::default() },
        ),
        (
            "cut empty buckets from the middle of a span, avoiding some due to maxEmptyBuckets",
            Histogram { positive_spans: vec![sp(-4, 6), sp(3, 3)], positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, -2, 3, 4], ..Default::default() },
            1,
            Histogram { positive_spans: vec![sp(-2, 1), sp(2, 1), sp(3, 3)], positive_buckets: vec![1, 2, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, -2, 3, 4], ..Default::default() },
        ),
        (
            "avoiding all cutting of empty buckets",
            Histogram { positive_spans: vec![sp(-4, 6), sp(3, 3)], positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, -2, 3, 4], ..Default::default() },
            2,
            Histogram { positive_spans: vec![sp(-2, 4), sp(3, 3)], positive_buckets: vec![1, -1, 0, 3, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, -2, 3, 4], ..Default::default() },
        ),
        (
            "everything merged into one span",
            Histogram { positive_spans: vec![sp(-4, 6), sp(3, 3)], positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3], negative_spans: vec![sp(0, 2), sp(3, 5)], negative_buckets: vec![5, 3, -4, -2, -2, 3, 4], ..Default::default() },
            3,
            Histogram { positive_spans: vec![sp(-2, 10)], positive_buckets: vec![1, -1, 0, 3, -3, 0, 0, 1, 42, 3], negative_spans: vec![sp(0, 10)], negative_buckets: vec![5, 3, -8, 0, 0, 4, -2, -2, 3, 4], ..Default::default() },
        ),
        (
            "more than one span merged",
            Histogram { count: 30, positive_spans: vec![sp(0, 2), sp(2, 2), sp(2, 2)], positive_buckets: vec![1, 1, 1, 1, 1, 1], negative_spans: vec![sp(0, 1), sp(1, 1), sp(1, 1)], negative_buckets: vec![2, 1, 1], ..Default::default() },
            3,
            Histogram { count: 30, positive_spans: vec![sp(0, 10)], positive_buckets: vec![1, 1, -2, 0, 3, 1, -4, 0, 5, 1], negative_spans: vec![sp(0, 5)], negative_buckets: vec![2, -2, 3, -3, 4], ..Default::default() },
        ),
        (
            "only empty buckets",
            Histogram { positive_spans: vec![sp(-4, 6), sp(3, 3)], positive_buckets: vec![0; 9], negative_spans: vec![sp(0, 7)], negative_buckets: vec![0; 7], ..Default::default() },
            3,
            Histogram::default(),
        ),
        (
            "multiple spans of only empty buckets",
            Histogram { positive_spans: vec![sp(-10, 2), sp(2, 1), sp(3, 3)], positive_buckets: vec![0, 0, 0, 0, 2, 3], negative_spans: vec![sp(-10, 2), sp(2, 1), sp(3, 3)], negative_buckets: vec![2, 3, -5, 0, 0, 0], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(-1, 2)], positive_buckets: vec![2, 3], negative_spans: vec![sp(-10, 2)], negative_buckets: vec![2, 3], ..Default::default() },
        ),
        (
            "all zero-length spans",
            Histogram { positive_spans: vec![sp(0, 0), sp(2, 0)], positive_buckets: vec![1, 3], negative_spans: vec![sp(1, 0)], negative_buckets: vec![2], ..Default::default() },
            0,
            Histogram::default(),
        ),
        (
            "more buckets than spans account for",
            Histogram { positive_spans: vec![sp(0, 1), sp(2, 1)], positive_buckets: vec![1, 2, 3, 4], negative_spans: vec![sp(0, 1)], negative_buckets: vec![5, 6], ..Default::default() },
            0,
            Histogram { positive_spans: vec![sp(0, 1), sp(2, 1)], positive_buckets: vec![1, 2, 3, 4], negative_spans: vec![sp(0, 1)], negative_buckets: vec![5, 6], ..Default::default() },
        ),
        (
            "custom buckets compact",
            Histogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(-4, 6), sp(3, 6)], positive_buckets: vec![0, 0, 1, 3, -4, 0, 1, 42, 3, -46, 0, 0], custom_values: vec![5.0, 10.0, 15.0, 20.0], ..Default::default() },
            0,
            Histogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(-2, 2), sp(5, 3)], positive_buckets: vec![1, 3, -3, 42, 3], custom_values: vec![5.0, 10.0, 15.0, 20.0], ..Default::default() },
        ),
    ];
    for (name, mut h, max_empty, exp) in cases {
        let was_valid = h.validate().is_ok();
        h.compact(max_empty);
        assert_eq!(h, exp, "{name}");
        if was_valid {
            assert!(h.validate().is_ok(), "{name} became invalid");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramValidation
#[test]
fn test_histogram_validation() {
    let cases: &[(&str, Histogram, &str, bool)] = &[
        ("valid", Histogram { count: 12, zero_count: 2, zero_threshold: 0.001, sum: 19.4, schema: 1, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], negative_spans: vec![sp(0, 2), sp(1, 2)], negative_buckets: vec![1, 1, -1, 0], ..Default::default() }, "", false),
        ("valid NaN", Histogram { zero_count: 2, count: 4, sum: f64::NAN, positive_spans: vec![sp(0, 1)], positive_buckets: vec![1], ..Default::default() }, "", false),
        ("count mismatch", Histogram { zero_count: 2, count: 4, sum: 333.0, positive_spans: vec![sp(0, 1)], positive_buckets: vec![1], ..Default::default() }, "3 observations found in buckets, but the Count field is 4: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)", true),
        ("too few neg", Histogram { negative_spans: vec![sp(0, 1)], ..Default::default() }, "negative side: spans need 1 buckets, have 0 buckets: histogram spans specify different number of buckets than provided", false),
        ("too few pos", Histogram { positive_spans: vec![sp(0, 1)], ..Default::default() }, "positive side: spans need 1 buckets, have 0 buckets: histogram spans specify different number of buckets than provided", false),
        ("too many neg", Histogram { negative_spans: vec![sp(0, 1)], negative_buckets: vec![1, 2], ..Default::default() }, "negative side: spans need 1 buckets, have 2 buckets: histogram spans specify different number of buckets than provided", false),
        ("too many pos", Histogram { positive_spans: vec![sp(0, 1)], positive_buckets: vec![1, 2], ..Default::default() }, "positive side: spans need 1 buckets, have 2 buckets: histogram spans specify different number of buckets than provided", false),
        ("neg offset neg span", Histogram { negative_spans: vec![sp(-1, 1), sp(-1, 1)], negative_buckets: vec![1, 2], ..Default::default() }, "negative side: span number 2 with offset -1: histogram has a span whose offset is negative", false),
        ("neg offset pos span", Histogram { positive_spans: vec![sp(-1, 1), sp(-1, 1)], positive_buckets: vec![1, 2], ..Default::default() }, "positive side: span number 2 with offset -1: histogram has a span whose offset is negative", false),
        ("neg span overflow", Histogram { negative_spans: vec![sp(0, 1), sp(i32::MAX, 1)], negative_buckets: vec![1, 0], ..Default::default() }, "negative side: span number 2 ends at bucket index 2147483648: histogram has a span whose bucket index is out of range", false),
        ("pos span overflow", Histogram { positive_spans: vec![sp(0, 1), sp(i32::MAX, 1), sp(i32::MAX, 1)], positive_buckets: vec![1, 0, 0], ..Default::default() }, "positive side: span number 2 ends at bucket index 2147483648: histogram has a span whose bucket index is out of range", false),
        ("max int32 index", Histogram { count: 1, positive_spans: vec![sp(i32::MAX, 1)], positive_buckets: vec![1], ..Default::default() }, "", false),
        ("pos span sum overflow", Histogram { positive_spans: vec![sp(i32::MAX / 2, 1), sp(i32::MAX / 2, 1), sp(1, 1)], positive_buckets: vec![1, 0, 0], ..Default::default() }, "positive side: span number 3 ends at bucket index 2147483649: histogram has a span whose bucket index is out of range", false),
        ("min int32 neg", Histogram { negative_spans: vec![sp(i32::MIN, 1)], negative_buckets: vec![1], ..Default::default() }, "negative side: span number 1 starts at bucket index -2147483648: histogram has a span whose bucket index is out of range", false),
        ("min int32 pos", Histogram { positive_spans: vec![sp(i32::MIN, 1)], positive_buckets: vec![1], ..Default::default() }, "positive side: span number 1 starts at bucket index -2147483648: histogram has a span whose bucket index is out of range", false),
        ("min allowed index", Histogram { count: 1, positive_spans: vec![sp(i32::MIN + 1, 1)], positive_buckets: vec![1], ..Default::default() }, "", false),
        ("neg bucket count neg", Histogram { negative_spans: vec![sp(-1, 1)], negative_buckets: vec![-1], ..Default::default() }, "negative side: bucket number 1 has observation count of -1: histogram has a bucket whose observation count is negative", false),
        ("pos bucket count neg", Histogram { positive_spans: vec![sp(-1, 1)], positive_buckets: vec![-1], ..Default::default() }, "positive side: bucket number 1 has observation count of -1: histogram has a bucket whose observation count is negative", false),
        ("lower count", Histogram { count: 0, negative_spans: vec![sp(-1, 1)], positive_spans: vec![sp(-1, 1)], negative_buckets: vec![1], positive_buckets: vec![1], ..Default::default() }, "2 observations found in buckets, but the Count field is 0: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)", true),
        ("missing zero count", Histogram { count: 2, zero_count: 1, negative_spans: vec![sp(-1, 1)], positive_spans: vec![sp(-1, 1)], negative_buckets: vec![1], positive_buckets: vec![1], ..Default::default() }, "3 observations found in buckets, but the Count field is 2: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)", true),
        ("exp with cb schema", Histogram { count: 12, zero_count: 2, zero_threshold: 0.001, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], negative_spans: vec![sp(0, 2), sp(1, 2)], negative_buckets: vec![1, 1, -1, 0], ..Default::default() }, "custom buckets: only 0 custom bounds defined which is insufficient to cover total span length of 5: histogram custom bounds are too few", false),
        ("cb with exp schema", Histogram { count: 5, sum: 19.4, schema: 0, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() }, "histogram with exponential schema must not have custom bounds", true),
        ("cb with zero/neg", Histogram { count: 12, zero_count: 2, zero_threshold: 0.001, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], negative_spans: vec![sp(0, 2), sp(1, 2)], negative_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() }, "custom buckets: must have zero count of 0", true),
        ("cb neg offset 1", Histogram { count: 5, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(-1, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() }, "custom buckets: span number 1 with offset -1: histogram has a span whose offset is negative", false),
        ("cb neg offset 2", Histogram { count: 5, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(-1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() }, "custom buckets: span number 2 with offset -1: histogram has a span whose offset is negative", false),
        ("cb mismatch", Histogram { count: 5, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() }, "custom buckets: spans need 4 buckets, have 3 buckets: histogram spans specify different number of buckets than provided", false),
        ("cb too few bounds", Histogram { count: 5, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0], ..Default::default() }, "custom buckets: only 3 custom bounds defined which is insufficient to cover total span length of 5: histogram custom bounds are too few", false),
        ("valid cb", Histogram { count: 5, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() }, "", false),
        ("valid cb extra bounds", Histogram { count: 5, sum: 19.4, schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 2), sp(1, 2)], positive_buckets: vec![1, 1, -1, 0], custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], ..Default::default() }, "", false),
        ("cb non-increasing", Histogram { schema: CUSTOM_BUCKETS_SCHEMA, custom_values: vec![0.0, 0.0], ..Default::default() }, "custom buckets: previous bound is 0.000000 and current is 0.000000: histogram custom bounds must be in strictly increasing order", false),
        ("cb explicit +Inf", Histogram { schema: CUSTOM_BUCKETS_SCHEMA, custom_values: vec![1.0, f64::INFINITY], ..Default::default() }, "custom buckets: last +Inf bound must not be explicitly defined: histogram custom bounds must be finite", false),
        ("valid cb -Inf", Histogram { schema: CUSTOM_BUCKETS_SCHEMA, custom_values: vec![f64::NEG_INFINITY, 1.0], ..Default::default() }, "", false),
        ("cb NaN bound", Histogram { schema: CUSTOM_BUCKETS_SCHEMA, custom_values: vec![1.0, f64::NAN, 3.0], ..Default::default() }, "custom buckets: histogram custom bounds must not be NaN", false),
        ("schema too high", Histogram { schema: 10, ..Default::default() }, "histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets, got schema 10", false),
        ("schema too low", Histogram { schema: -10, ..Default::default() }, "histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets, got schema -10", false),
    ];

    for (name, h, err_msg, skip_float) in cases {
        let res = h.validate();
        if err_msg.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            assert_eq!(res.unwrap_err().to_string(), *err_msg, "{name}");
        }
        if *skip_float {
            continue;
        }
        let fh = h.to_float();
        let fres = fh.validate();
        if err_msg.is_empty() {
            assert!(fres.is_ok(), "{name} (float): {fres:?}");
        } else {
            assert_eq!(fres.unwrap_err().to_string(), *err_msg, "{name} (float)");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/histogram_test.go::TestHistogramReduceResolution
#[test]
fn test_histogram_reduce_resolution() {
    let mut valid = Histogram {
        schema: 0,
        positive_spans: vec![sp(-2, 4), sp(0, 0), sp(3, 2)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0],
        negative_spans: vec![sp(0, 4), sp(0, 0), sp(3, 2)],
        negative_buckets: vec![1, 2, -2, 1, -1, 0],
        ..Default::default()
    };
    valid.reduce_resolution(-1).unwrap();
    assert_eq!(
        valid,
        Histogram {
            schema: -1,
            positive_spans: vec![sp(-1, 3), sp(1, 1)],
            positive_buckets: vec![1, 3, -2, 0],
            negative_spans: vec![sp(0, 3), sp(1, 1)],
            negative_buckets: vec![1, 3, -2, 0],
            ..Default::default()
        }
    );

    let mut not_enough = Histogram { schema: 0, positive_spans: vec![sp(-2, 4), sp(0, 0), sp(3, 2)], positive_buckets: vec![1, 2, -2, 1, -1], ..Default::default() };
    let err = not_enough.reduce_resolution(-1).unwrap_err();
    assert!(matches!(err, ReduceResolutionError::Validation(_)));
    assert_eq!(
        err.to_string(),
        "have 5 buckets but spans need more: histogram spans specify different number of buckets than provided"
    );

    let mut too_many = Histogram { schema: 0, positive_spans: vec![sp(-2, 4), sp(0, 0), sp(3, 2)], positive_buckets: vec![1, 2, -2, 1, -1, 0, 3], ..Default::default() };
    assert_eq!(
        too_many.reduce_resolution(-1).unwrap_err().to_string(),
        "spans need 6 buckets, have 7 buckets: histogram spans specify different number of buckets than provided"
    );

    let mut neg_off = Histogram { schema: 0, positive_spans: vec![sp(-2, 4), sp(-1, 0), sp(3, 2)], positive_buckets: vec![1, 2, -2, 1, -1, 0], ..Default::default() };
    assert_eq!(
        neg_off.reduce_resolution(-1).unwrap_err().to_string(),
        "span number 2 with offset -1: histogram has a span whose offset is negative"
    );
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramMul
#[test]
fn test_float_histogram_mul() {
    let cases = [
        ("zero value", FloatHistogram::default(), 3.1415, FloatHistogram::default()),
        (
            "zero multiplier",
            FloatHistogram { zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
            0.0,
            FloatHistogram { zero_threshold: 0.01, zero_count: 0.0, count: 0.0, sum: 0.0, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![0.0; 4], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![0.0; 4], ..Default::default() },
        ),
        (
            "no-op",
            FloatHistogram { zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
            1.0,
            FloatHistogram { zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
        ),
        (
            "double",
            FloatHistogram { zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
            2.0,
            FloatHistogram { zero_threshold: 0.01, zero_count: 11.0, count: 3493.3 * 2.0, sum: 2349209.324 * 2.0, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![2.0, 6.6, 8.4, 0.2], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![6.2, 6.0, 1.234e5 * 2.0, 2000.0], ..Default::default() },
        ),
        (
            "negation",
            FloatHistogram { zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 23.0, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.0, 1.0, 5.0, 6.0], ..Default::default() },
            -1.0,
            FloatHistogram { counter_reset_hint: CounterResetHint::GaugeType, zero_threshold: 0.01, zero_count: -11.0, count: -30.0, sum: -23.0, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![-1.0, -0.0, -3.0, -4.0, -7.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![-3.0, -1.0, -5.0, -6.0], ..Default::default() },
        ),
        (
            "triple custom",
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 30.0, sum: 23.0, positive_spans: vec![sp(0, 2), sp(1, 3)], positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() },
            3.0,
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 90.0, sum: 69.0, positive_spans: vec![sp(0, 2), sp(1, 3)], positive_buckets: vec![3.0, 0.0, 9.0, 12.0, 21.0], custom_values: vec![1.0, 2.0, 3.0, 4.0], ..Default::default() },
        ),
    ];
    for (name, mut fh, scale, exp) in cases {
        fh.mul(scale);
        // Check arithmetic equality (note 0.0 * -1.0 = -0.0 in IEEE-754).
        assert_eq!(fh.schema, exp.schema, "{name}");
        assert_eq!(fh.counter_reset_hint, exp.counter_reset_hint, "{name}");
        assert_eq!(fh.count, exp.count, "{name}");
        assert_eq!(fh.sum, exp.sum, "{name}");
        assert_eq!(fh.zero_count, exp.zero_count, "{name}");
        assert_eq!(fh.positive_spans, exp.positive_spans, "{name}");
        assert_eq!(fh.positive_buckets, exp.positive_buckets, "{name}");
        assert_eq!(fh.negative_buckets, exp.negative_buckets, "{name}");
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramCopy
#[test]
fn test_float_histogram_copy() {
    let mut orig = FloatHistogram {
        positive_spans: vec![sp(-2, 1)],
        positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
        negative_spans: vec![sp(3, 2)],
        negative_buckets: vec![5.0, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };
    let expected = orig.clone();
    let mut c = orig.copy();
    orig.sum += 1.0;
    orig.positive_spans.push(sp(1, 2));
    assert_eq!(c, expected);
    c.positive_spans.push(sp(9, 9));
    assert_ne!(c, orig);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramCopyTo
#[test]
fn test_float_histogram_copy_to() {
    let orig = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(0, 1)],
        positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
        custom_values: vec![1.0, 2.0, 3.0],
        ..Default::default()
    };
    let mut dst = FloatHistogram {
        schema: 1,
        zero_threshold: 0.5,
        zero_count: 10.0,
        negative_spans: vec![sp(0, 1)],
        negative_buckets: vec![1.0],
        ..Default::default()
    };
    orig.copy_to(&mut dst);
    assert_eq!(dst, orig);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramDiv
#[test]
fn test_float_histogram_div() {
    let mut zero_div = FloatHistogram {
        zero_threshold: 0.01,
        zero_count: 5.5,
        count: 3493.3,
        sum: 2349209.324,
        positive_spans: vec![sp(-2, 1), sp(2, 3)],
        positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };
    zero_div.div(0.0);
    assert_eq!(
        zero_div,
        FloatHistogram {
            zero_threshold: 0.01,
            count: f64::INFINITY,
            sum: f64::INFINITY,
            zero_count: f64::INFINITY,
            ..Default::default()
        }
    );

    let mut half = FloatHistogram {
        zero_threshold: 0.01,
        zero_count: 11.0,
        count: 30.0,
        sum: 23.0,
        positive_spans: vec![sp(-2, 2), sp(1, 3)],
        positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
        ..Default::default()
    };
    half.div(2.0);
    assert_eq!(
        half,
        FloatHistogram {
            zero_threshold: 0.01,
            zero_count: 5.5,
            count: 15.0,
            sum: 11.5,
            positive_spans: vec![sp(-2, 2), sp(1, 3)],
            positive_buckets: vec![0.5, 0.0, 1.5, 2.0, 3.5],
            negative_spans: vec![sp(3, 2), sp(3, 2)],
            negative_buckets: vec![1.5, 0.5, 2.5, 3.0],
            ..Default::default()
        }
    );

    let mut neg = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 30.0,
        sum: 23.0,
        positive_spans: vec![sp(0, 2), sp(1, 3)],
        positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
        custom_values: vec![1.0, 2.0, 3.0, 4.0],
        ..Default::default()
    };
    neg.div(-2.0);
    assert_eq!(neg.counter_reset_hint, CounterResetHint::GaugeType);
    assert_eq!(neg.count, -15.0);
    assert_eq!(neg.sum, -11.5);
    assert_eq!(neg.positive_buckets[0], -0.5);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramDetectReset
#[test]
fn test_float_histogram_detect_reset() {
    let base = FloatHistogram {
        zero_threshold: 0.01,
        zero_count: 5.5,
        count: 3493.3,
        sum: 2349209.324,
        positive_spans: vec![sp(-2, 2), sp(1, 3)],
        positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };

    assert!(!FloatHistogram::default().detect_reset(&FloatHistogram::default()));
    assert!(!base.detect_reset(&FloatHistogram::default()));
    assert!(FloatHistogram::default().detect_reset(&base));

    let mut zero_up = base.clone();
    zero_up.zero_count += 0.1;
    assert!(!zero_up.detect_reset(&base));
    assert!(base.detect_reset(&zero_up));

    let mut count_up = base.clone();
    count_up.count += 0.1;
    assert!(!count_up.detect_reset(&base));
    assert!(base.detect_reset(&count_up));

    let mut pos_up = base.clone();
    pos_up.positive_buckets[2] += 0.1;
    assert!(!pos_up.detect_reset(&base));
    assert!(base.detect_reset(&pos_up));

    let mut neg_up = base.clone();
    neg_up.negative_buckets[1] += 0.1;
    assert!(!neg_up.detect_reset(&base));
    assert!(base.detect_reset(&neg_up));

    // Unpopulated bucket disappears -> no reset
    let mut no_empty = base.clone();
    no_empty.positive_spans = vec![sp(-2, 1), sp(2, 3)];
    no_empty.positive_buckets = vec![1.0, 3.3, 4.2, 0.1];
    assert!(!no_empty.detect_reset(&base));

    // Zero threshold increases to cover buckets
    let prev_zt = FloatHistogram {
        zero_threshold: 0.01,
        zero_count: 5.5,
        count: 3493.3,
        sum: 2349209.324,
        positive_spans: vec![sp(-2, 2), sp(1, 3)],
        positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };
    let curr_zt_cov = FloatHistogram {
        zero_threshold: 1.0,
        zero_count: 7.73,
        count: 3493.3,
        sum: 2349209.324,
        positive_spans: vec![sp(1, 3)],
        positive_buckets: vec![3.3, 4.2, 0.1],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };
    assert!(!curr_zt_cov.detect_reset(&prev_zt));

    let mut curr_zt_mid = prev_zt.clone();
    curr_zt_mid.zero_threshold = 0.3;
    assert!(curr_zt_mid.detect_reset(&prev_zt));

    // Schema increases -> reset, schema decreases -> no reset if merged buckets match
    let s1 = FloatHistogram {
        zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, schema: 1,
        positive_spans: vec![sp(-5, 4), sp(2, 6)],
        positive_buckets: vec![0.4, 0.6, 1.0, 0.23, 2.0, 1.3, 1.2, 3.0, 0.05, 0.05],
        negative_spans: vec![sp(5, 4), sp(6, 4)],
        negative_buckets: vec![2.0, 1.1, 2.0, 1.0, 0.234e5, 1e5, 500.0, 500.0],
        ..Default::default()
    };
    assert!(s1.detect_reset(&prev_zt));
    assert!(!prev_zt.detect_reset(&s1));

    // Mismatched custom bounds
    let cb_prev = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 200.0,
        sum: 1000.0,
        positive_spans: vec![sp(0, 4)],
        positive_buckets: vec![20.0, 35.0, 40.0, 50.0],
        custom_values: vec![0.0, 1.0, 2.0, 3.0],
        ..Default::default()
    };
    let cb_curr_ok = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 200.0,
        sum: 1000.0,
        positive_spans: vec![sp(0, 5)],
        positive_buckets: vec![25.0, 15.0, 40.0, 50.0, 70.0],
        custom_values: vec![0.0, 0.5, 1.0, 2.0, 3.0],
        ..Default::default()
    };
    assert!(!cb_curr_ok.detect_reset(&cb_prev));

    let cb_reset_mid_prev = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA, count: 100.0, sum: 500.0,
        positive_spans: vec![sp(1, 4)], positive_buckets: vec![10.0, 15.0, 20.0, 25.0],
        custom_values: vec![0.0, 1.0, 3.0, 5.0],
        ..Default::default()
    };
    let cb_reset_mid_curr = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA, count: 100.0, sum: 500.0,
        positive_spans: vec![sp(1, 5)], positive_buckets: vec![10.0, 16.0, 10.0, 5.0, 25.0],
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 5.0],
        ..Default::default()
    };
    assert!(cb_reset_mid_curr.detect_reset(&cb_reset_mid_prev));

    // Populated bucket following empty one disappears
    let neg_empty_prev = FloatHistogram {
        zero_threshold: 0.001, count: 10.0,
        positive_spans: vec![sp(0, 1)], positive_buckets: vec![5.0],
        negative_spans: vec![sp(1, 2)], negative_buckets: vec![0.0, 5.0],
        ..Default::default()
    };
    let neg_empty_curr = FloatHistogram {
        zero_threshold: 0.001, count: 20.0,
        positive_spans: vec![sp(0, 1)], positive_buckets: vec![20.0],
        ..Default::default()
    };
    assert!(neg_empty_curr.detect_reset(&neg_empty_prev));
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramCompact
#[test]
fn test_float_histogram_compact() {
    let cases = [
        ("empty", FloatHistogram::default(), 0, FloatHistogram::default()),
        (
            "eliminate zero offsets",
            FloatHistogram { positive_spans: vec![sp(-2, 1), sp(0, 3), sp(0, 1)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3], negative_spans: vec![sp(0, 2), sp(0, 2), sp(2, 1), sp(0, 1)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0], ..Default::default() },
            0,
            FloatHistogram { positive_spans: vec![sp(-2, 5)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3], negative_spans: vec![sp(0, 4), sp(2, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0], ..Default::default() },
        ),
        (
            "cut empty buckets in middle",
            FloatHistogram { positive_spans: vec![sp(5, 4)], positive_buckets: vec![1.0, 3.0, 0.0, 2.0], ..Default::default() },
            0,
            FloatHistogram { positive_spans: vec![sp(5, 2), sp(1, 1)], positive_buckets: vec![1.0, 3.0, 2.0], ..Default::default() },
        ),
        (
            "merge spans with maxEmptyBuckets",
            FloatHistogram { positive_spans: vec![sp(-2, 2), sp(2, 2), sp(2, 2)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3, 2.0], negative_spans: vec![sp(0, 1), sp(1, 1), sp(1, 1)], negative_buckets: vec![3.1, 1000.0, 4.0], ..Default::default() },
            3,
            FloatHistogram { positive_spans: vec![sp(-2, 10)], positive_buckets: vec![1.0, 3.3, 0.0, 0.0, 4.2, 0.1, 0.0, 0.0, 3.3, 2.0], negative_spans: vec![sp(0, 5)], negative_buckets: vec![3.1, 0.0, 1000.0, 0.0, 4.0], ..Default::default() },
        ),
        (
            "only empty buckets",
            FloatHistogram { positive_spans: vec![sp(-4, 6), sp(3, 3)], positive_buckets: vec![0.0; 9], negative_spans: vec![sp(0, 7)], negative_buckets: vec![0.0; 7], ..Default::default() },
            3,
            FloatHistogram::default(),
        ),
        (
            "custom buckets compact",
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(0, 4), sp(5, 6)], positive_buckets: vec![0.0, 0.0, 1.0, 3.3, 4.2, 0.1, 3.3, 0.0, 0.0, 0.0], custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], ..Default::default() },
            0,
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_spans: vec![sp(2, 2), sp(5, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3], custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], ..Default::default() },
        ),
    ];
    for (name, mut fh, max_empty, exp) in cases {
        let was_valid = fh.validate().is_ok();
        fh.compact(max_empty);
        assert_eq!(fh, exp, "{name}");
        if was_valid {
            assert!(fh.validate().is_ok(), "{name}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramKahanCompactSpanMerge
#[test]
fn test_float_histogram_kahan_compact_span_merge() {
    let mut h = FloatHistogram {
        positive_spans: vec![sp(0, 1), sp(1, 1), sp(1, 1)],
        positive_buckets: vec![1.0, 2.0, 3.0],
        ..Default::default()
    };
    let mut c = FloatHistogram {
        positive_spans: vec![sp(0, 1), sp(1, 1), sp(1, 1)],
        positive_buckets: vec![0.1, 0.2, 0.3],
        ..Default::default()
    };
    h.kahan_compact(3, &mut c);
    assert_eq!(h.positive_spans, vec![sp(0, 5)]);
    assert_eq!(h.positive_buckets, vec![1.0, 0.0, 2.0, 0.0, 3.0]);
    assert_eq!(c.positive_buckets, vec![0.1, 0.0, 0.2, 0.0, 0.3]);
}

fn check_fh_add(
    a: &FloatHistogram,
    b: &FloatHistogram,
    expected: Option<&FloatHistogram>,
    exp_err: Option<HistogramErrorKind>,
    exp_collision: bool,
    exp_reconciled: bool,
) {
    a.validate().unwrap();
    b.validate().unwrap();
    let mut a_copy = a.copy();
    let b_copy = b.copy();
    let res = a_copy.add(&b_copy);
    if let Some(kind) = exp_err {
        assert_eq!(res.unwrap_err().kind, kind);
        return;
    }
    let (collision, reconciled) = res.unwrap();
    assert_eq!(collision, exp_collision);
    assert_eq!(reconciled, exp_reconciled);
    if let Some(exp) = expected {
        a_copy.compact(0);
        let mut exp_c = exp.copy();
        exp_c.compact(0);
        assert_eq!(a_copy, exp_c);
    }
    assert_eq!(b, &b_copy);
}

fn check_fh_kahan_add(
    a: &FloatHistogram,
    c: Option<&FloatHistogram>,
    b: &FloatHistogram,
    expected_sum: Option<&FloatHistogram>,
    exp_err: Option<HistogramErrorKind>,
    exp_collision: bool,
    exp_reconciled: bool,
) {
    let mut a_copy = a.copy();
    let b_copy = b.copy();
    let res = a_copy.kahan_add(&b_copy, c.map(FloatHistogram::copy));
    if let Some(kind) = exp_err {
        assert_eq!(res.unwrap_err().kind, kind);
        return;
    }
    let (comp, collision, reconciled) = res.unwrap();
    assert_eq!(a_copy.schema, comp.schema);
    assert_eq!(a_copy.zero_threshold, comp.zero_threshold);
    assert_eq!(a_copy.positive_spans, comp.positive_spans);
    assert_eq!(a_copy.negative_spans, comp.negative_spans);
    assert_eq!(a_copy.custom_values.len(), comp.custom_values.len());
    assert_eq!(a_copy.positive_buckets.len(), comp.positive_buckets.len());
    assert_eq!(a_copy.negative_buckets.len(), comp.negative_buckets.len());
    a_copy.add(&comp).unwrap();
    assert_eq!(collision, exp_collision);
    assert_eq!(reconciled, exp_reconciled);
    if let Some(exp) = expected_sum {
        a_copy.compact(0);
        let mut exp_c = exp.copy();
        exp_c.compact(0);
        assert_eq!(a_copy, exp_c);
    }
    assert_eq!(b, &b_copy);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramAdd
#[test]
fn test_float_histogram_add() {
    let cases: Vec<(&str, FloatHistogram, FloatHistogram, Option<FloatHistogram>, Option<HistogramErrorKind>, bool, bool)> = vec![
        (
            "same layout",
            FloatHistogram { zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 2.345, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.0, 1.0, 5.0, 6.0], ..Default::default() },
            FloatHistogram { zero_threshold: 0.01, zero_count: 8.0, count: 21.0, sum: 1.234, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![1.0, 1.0, 4.0, 4.0], ..Default::default() },
            Some(FloatHistogram { zero_threshold: 0.01, zero_count: 19.0, count: 51.0, sum: 3.579, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![1.0, 0.0, 5.0, 7.0, 13.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![4.0, 2.0, 9.0, 10.0], ..Default::default() }),
            None, false, false,
        ),
        (
            "overlapping spans",
            FloatHistogram { zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 2.345, positive_spans: vec![sp(-2, 2), sp(2, 3)], positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.0, 1.0, 5.0, 6.0], ..Default::default() },
            FloatHistogram { zero_threshold: 0.01, zero_count: 8.0, count: 21.0, sum: 1.234, positive_spans: vec![sp(-1, 4), sp(0, 3)], positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0], negative_spans: vec![sp(4, 2), sp(1, 2)], negative_buckets: vec![1.0, 1.0, 4.0, 4.0], ..Default::default() },
            Some(FloatHistogram { zero_threshold: 0.01, zero_count: 19.0, count: 51.0, sum: 3.579, positive_spans: vec![sp(-2, 4), sp(0, 4)], positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0], negative_spans: vec![sp(3, 3), sp(1, 3)], negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0], ..Default::default() }),
            None, false, false,
        ),
        (
            "schema change + zero bucket",
            FloatHistogram { zero_threshold: 0.01, zero_count: 8.0, count: 21.0, sum: 1.234, schema: 0, positive_spans: vec![sp(-2, 5), sp(0, 3)], positive_buckets: vec![2.0, 5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0], negative_spans: vec![sp(4, 2), sp(1, 2)], negative_buckets: vec![1.0, 1.0, 4.0, 4.0], ..Default::default() },
            FloatHistogram { zero_threshold: 0.25, zero_count: 12.0, count: 30.0, sum: 2.345, schema: 1, positive_spans: vec![sp(-3, 2), sp(5, 5)], positive_buckets: vec![1.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0], negative_spans: vec![sp(6, 3), sp(6, 4)], negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0], ..Default::default() },
            Some(FloatHistogram { zero_threshold: 0.25, zero_count: 22.0, count: 51.0, sum: 3.579, positive_spans: vec![sp(-1, 7)], positive_buckets: vec![6.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0], negative_spans: vec![sp(3, 3), sp(1, 3)], negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0], ..Default::default() }),
            None, false, false,
        ),
        (
            "custom buckets partial intersection",
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 10.0, sum: 100.0, positive_spans: vec![sp(0, 3)], positive_buckets: vec![2.0, 3.0, 5.0], custom_values: vec![1.0, 2.5], ..Default::default() },
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 8.0, sum: 80.0, positive_spans: vec![sp(0, 4)], positive_buckets: vec![1.0, 2.0, 3.0, 2.0], custom_values: vec![1.0, 2.0, 3.0], ..Default::default() },
            Some(FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 18.0, sum: 180.0, positive_spans: vec![sp(0, 2)], positive_buckets: vec![3.0, 15.0], custom_values: vec![1.0], ..Default::default() }),
            None, false, true,
        ),
        (
            "custom buckets no common bounds",
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 3.0, sum: 50.0, positive_spans: vec![sp(0, 2)], positive_buckets: vec![1.0, 2.0], custom_values: vec![1.5], ..Default::default() },
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 30.0, sum: 40.0, positive_spans: vec![sp(0, 2)], positive_buckets: vec![10.0, 20.0], custom_values: vec![2.5], ..Default::default() },
            Some(FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 33.0, sum: 90.0, positive_spans: vec![sp(0, 1)], positive_buckets: vec![33.0], ..Default::default() }),
            None, false, true,
        ),
        (
            "incompatible schemas",
            FloatHistogram { schema: 0, ..Default::default() },
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, ..Default::default() },
            None, Some(HistogramErrorKind::IncompatibleSchema), false, false,
        ),
        (
            "collision",
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, counter_reset_hint: CounterResetHint::CounterReset, ..Default::default() },
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, counter_reset_hint: CounterResetHint::NotCounterReset, ..Default::default() },
            None, None, true, false,
        ),
    ];

    for (_name, in1, in2, exp, err, coll, rec) in &cases {
        check_fh_add(in1, in2, exp.as_ref(), *err, *coll, *rec);
        check_fh_add(in2, in1, exp.as_ref(), *err, *coll, *rec);
        check_fh_kahan_add(in1, None, in2, exp.as_ref(), *err, *coll, *rec);
        check_fh_kahan_add(in2, None, in1, exp.as_ref(), *err, *coll, *rec);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestKahanAddWithCompHistogram
#[test]
fn test_kahan_add_with_comp_histogram() {
    let in1 = FloatHistogram {
        schema: 2, zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 2.345,
        positive_spans: vec![sp(-2, 2), sp(1, 1), sp(1, 3)],
        positive_buckets: vec![1.0, 3.0, 1e100, 0.0, 7.0, -6.0],
        ..Default::default()
    };
    let comp = FloatHistogram {
        schema: 2, zero_threshold: 0.01, zero_count: 1.0,
        positive_spans: vec![sp(-2, 2), sp(1, 1), sp(1, 3)],
        positive_buckets: vec![7.0, 2.0, 0.03, 0.0, 0.05, 0.06],
        ..Default::default()
    };
    let in2 = FloatHistogram {
        schema: 1, zero_threshold: 1.0, zero_count: 17.0, count: 21.0, sum: 1.234,
        positive_spans: vec![sp(1, 2), sp(1, 2)],
        positive_buckets: vec![-1e100, 3.0, 2.0, 5.0],
        ..Default::default()
    };
    let expected = FloatHistogram {
        schema: 1, zero_threshold: 1.0, zero_count: 42.0, count: 51.0, sum: 3.579,
        positive_spans: vec![sp(1, 5)],
        positive_buckets: vec![0.03, 10.05, -5.94, 2.0, 5.0],
        ..Default::default()
    };
    check_fh_kahan_add(&in1, Some(&comp), &in2, Some(&expected), None, false, false);

    // Also test reducing 'other' histogram with 1e100 cancellation
    let in1_b = FloatHistogram {
        schema: 0, zero_threshold: 1.0, zero_count: 17.0, count: 21.0, sum: 1.234,
        positive_spans: vec![sp(1, 2), sp(1, 2)],
        positive_buckets: vec![2.0, 3.0, 2.0, 5.0],
        ..Default::default()
    };
    let comp_b = FloatHistogram {
        schema: 0, zero_threshold: 1.0, zero_count: 1.0,
        positive_spans: vec![sp(1, 2), sp(1, 2)],
        positive_buckets: vec![17.0, 2.0, 0.03, 0.0],
        ..Default::default()
    };
    let in2_b = FloatHistogram {
        schema: 2, zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 2.345,
        positive_spans: vec![sp(-2, 3), sp(1, 1), sp(1, 3)],
        positive_buckets: vec![1e100, 4.1, -1e100, 2.1, 0.0, 7.0, -6.0],
        ..Default::default()
    };
    let exp_b = FloatHistogram {
        schema: 0, zero_threshold: 1.0, zero_count: 33.1, count: 51.0, sum: 3.579,
        positive_spans: vec![sp(1, 2), sp(1, 2)],
        positive_buckets: vec![21.1, 6.0, 2.03, 5.0],
        ..Default::default()
    };
    check_fh_kahan_add(&in1_b, Some(&comp_b), &in2_b, Some(&exp_b), None, false, false);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramSub
#[test]
fn test_float_histogram_sub() {
    let cases = [
        (
            "same layout",
            FloatHistogram { zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 23.0, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.0, 1.0, 5.0, 6.0], ..Default::default() },
            FloatHistogram { zero_threshold: 0.01, zero_count: 8.0, count: 21.0, sum: 12.0, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![1.0, 1.0, 4.0, 4.0], ..Default::default() },
            Some(FloatHistogram { zero_threshold: 0.01, zero_count: 3.0, count: 9.0, sum: 11.0, positive_spans: vec![sp(-2, 2), sp(1, 3)], positive_buckets: vec![1.0, 0.0, 1.0, 1.0, 1.0], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![2.0, 0.0, 1.0, 2.0], ..Default::default() }),
            None, false, false,
        ),
        (
            "different custom buckets",
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 220.0, sum: 9.9, positive_spans: vec![sp(0, 2), sp(1, 1), sp(0, 2)], positive_buckets: vec![10.0, 20.0, 40.0, 50.0, 100.0], custom_values: vec![1.0, 2.0, 3.0, 4.0, 5.0], counter_reset_hint: CounterResetHint::GaugeType, ..Default::default() },
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 6.0, sum: 4.4, positive_spans: vec![sp(0, 1), sp(1, 1)], positive_buckets: vec![1.0, 5.0], custom_values: vec![2.0, 4.0], counter_reset_hint: CounterResetHint::GaugeType, ..Default::default() },
            Some(FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: 214.0, sum: 5.5, positive_spans: vec![sp(0, 3)], positive_buckets: vec![29.0, 40.0, 145.0], custom_values: vec![2.0, 4.0], counter_reset_hint: CounterResetHint::GaugeType, ..Default::default() }),
            None, false, true,
        ),
        (
            "incompatible schemas",
            FloatHistogram { schema: 0, ..Default::default() },
            FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, ..Default::default() },
            None, Some(HistogramErrorKind::IncompatibleSchema), false, false,
        ),
    ];
    for (name, a, b, exp, err, coll, rec) in cases {
        let mut a_c = a.copy();
        let res = a_c.sub(&b);
        if let Some(k) = err {
            assert_eq!(res.unwrap_err().kind, k, "{name}");
            continue;
        }
        let (c, r) = res.unwrap();
        assert_eq!((c, r), (coll, rec), "{name}");
        if let Some(mut e) = exp {
            a_c.compact(0);
            e.compact(0);
            assert_eq!(a_c, e, "{name}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramCopyToSchema
#[test]
fn test_float_histogram_copy_to_schema() {
    let orig = FloatHistogram {
        zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 2.345, schema: 1,
        positive_spans: vec![sp(-4, 3), sp(5, 5)],
        positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
        negative_spans: vec![sp(6, 3), sp(6, 4)],
        negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
        ..Default::default()
    };
    let expected = FloatHistogram {
        zero_threshold: 0.01, zero_count: 11.0, count: 30.0, sum: 2.345, schema: 0,
        positive_spans: vec![sp(-2, 2), sp(2, 3)],
        positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
        ..Default::default()
    };
    for hint in [
        CounterResetHint::UnknownCounterReset,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::GaugeType,
    ] {
        let mut h = orig.copy();
        h.counter_reset_hint = hint;
        let mut e = expected.copy();
        e.counter_reset_hint = hint;
        let res = h.copy_to_schema(0).unwrap();
        assert_eq!(res, e);
        assert_eq!(h.copy_to_schema(1).unwrap(), h);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramInvalidBucketLayoutInResolutionReduction
#[test]
fn test_float_histogram_invalid_bucket_layout_in_resolution_reduction() {
    let valid = FloatHistogram {
        schema: 0, count: 5.0, sum: 10.0,
        positive_spans: vec![sp(0, 2)], positive_buckets: vec![2.0, 3.0],
        ..Default::default()
    };
    let invalid_cases = [
        (FloatHistogram { schema: 1, count: 5.0, sum: 10.0, positive_spans: vec![sp(0, 1), sp(-2, 1)], positive_buckets: vec![2.0, 3.0], ..Default::default() }, HistogramErrorKind::SpanNegativeOffset),
        (FloatHistogram { schema: 1, count: 5.0, sum: 10.0, negative_spans: vec![sp(0, 1), sp(-2, 1)], negative_buckets: vec![2.0, 3.0], ..Default::default() }, HistogramErrorKind::SpanNegativeOffset),
        (FloatHistogram { schema: 1, count: 5.0, sum: 10.0, positive_spans: vec![sp(0, 3)], positive_buckets: vec![2.0, 3.0], ..Default::default() }, HistogramErrorKind::SpansBucketsMismatch),
    ];
    for (inv, kind) in invalid_cases {
        let mut i1 = inv.copy();
        assert_eq!(i1.add(&valid).unwrap_err().kind, kind);
        assert_eq!(i1, inv);
        let mut v1 = valid.copy();
        assert_eq!(v1.add(&inv).unwrap_err().kind, kind);
        assert_eq!(v1, valid);
        let mut i2 = inv.copy();
        assert_eq!(i2.sub(&valid).unwrap_err().kind, kind);
        assert_eq!(i2, inv);
        let mut v2 = valid.copy();
        assert_eq!(v2.sub(&inv).unwrap_err().kind, kind);
        assert_eq!(v2, valid);
        assert_eq!(inv.copy_to_schema(0).unwrap_err().kind, kind);
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestReverseFloatBucketIterator
#[test]
fn test_reverse_float_bucket_iterator() {
    let h = FloatHistogram {
        count: 405.0, zero_count: 102.0, zero_threshold: 0.001, sum: 1008.4, schema: 1,
        positive_spans: vec![sp(0, 4), sp(1, 0), sp(3, 3), sp(3, 0), sp(2, 0), sp(5, 3)],
        positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
        negative_spans: vec![sp(0, 3), sp(1, 0), sp(3, 0), sp(3, 4), sp(2, 0), sp(5, 3)],
        negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
        ..Default::default()
    };
    let mut exp_pos: Vec<_> = h.positive_bucket_iter().collect();
    exp_pos.reverse();
    let act_pos: Vec<_> = h.positive_reverse_bucket_iter().collect();
    assert!(!act_pos.is_empty());
    assert_eq!(act_pos, exp_pos);

    let mut exp_neg: Vec<_> = h.negative_bucket_iter().collect();
    exp_neg.reverse();
    let act_neg: Vec<_> = h.negative_reverse_bucket_iter().collect();
    assert!(!act_neg.is_empty());
    assert_eq!(act_neg, exp_neg);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestAllFloatBucketIterator
#[test]
fn test_all_float_bucket_iterator() {
    let h = FloatHistogram {
        count: 447.0, zero_count: 42.0, zero_threshold: 0.6, sum: 1008.4, schema: 0,
        positive_spans: vec![sp(0, 4), sp(1, 0), sp(3, 3)],
        positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0],
        negative_spans: vec![sp(0, 3), sp(1, 0), sp(3, 4)],
        negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0],
        ..Default::default()
    };
    let mut exp = Vec::new();
    for mut b in h.negative_reverse_bucket_iter() {
        if b.upper > -h.zero_threshold { b.upper = -h.zero_threshold; }
        exp.push(b);
    }
    exp.push(h.zero_bucket());
    for mut b in h.positive_bucket_iter() {
        if b.lower < h.zero_threshold { b.lower = h.zero_threshold; }
        exp.push(b);
    }
    let act: Vec<_> = h.all_bucket_iter().collect();
    assert_eq!(act, exp);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestAllReverseFloatBucketIterator
#[test]
fn test_all_reverse_float_bucket_iterator() {
    let h = FloatHistogram {
        count: 447.0, zero_count: 42.0, zero_threshold: 0.6, sum: 1008.4, schema: 0,
        positive_spans: vec![sp(0, 4), sp(1, 0), sp(3, 3)],
        positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0],
        negative_spans: vec![sp(0, 3), sp(1, 0), sp(3, 4)],
        negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0],
        ..Default::default()
    };
    let mut exp = Vec::new();
    for mut b in h.positive_reverse_bucket_iter() {
        if b.lower < h.zero_threshold { b.lower = h.zero_threshold; }
        exp.push(b);
    }
    exp.push(h.zero_bucket());
    for mut b in h.negative_bucket_iter() {
        if b.upper > -h.zero_threshold { b.upper = -h.zero_threshold; }
        exp.push(b);
    }
    let act: Vec<_> = h.all_reverse_bucket_iter().collect();
    assert_eq!(act, exp);

    // Invalid (fewer buckets than spans) must not panic.
    let mut h_missing = h.clone();
    h_missing.positive_buckets.pop();
    h_missing.negative_buckets.pop();
    assert!(!h_missing.all_reverse_bucket_iter().collect::<Vec<_>>().is_empty());
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatBucketIteratorTargetSchema
#[test]
fn test_float_bucket_iterator_target_schema() {
    let b = |lower: f64, upper: f64, count: f64, index: i32, neg: bool| Bucket {
        lower, upper,
        lower_inclusive: neg,
        upper_inclusive: !neg,
        count, index,
    };
    let h = FloatHistogram {
        count: 405.0, sum: 1008.4, schema: 1,
        positive_spans: vec![sp(0, 4), sp(1, 3), sp(2, 3)],
        positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
        negative_spans: vec![sp(0, 3), sp(7, 4), sp(1, 3)],
        negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
        ..Default::default()
    };
    assert_eq!(
        h.float_bucket_iter(true, 0.0, -1).collect::<Vec<_>>(),
        vec![
            b(0.25, 1.0, 100.0, 0, false),
            b(1.0, 4.0, 522.0, 1, false),
            b(4.0, 16.0, 68.0, 2, false),
            b(16.0, 64.0, 322.0, 3, false),
        ]
    );
    assert_eq!(
        h.float_bucket_iter(false, 0.0, -1).collect::<Vec<_>>(),
        vec![
            b(-1.0, -0.25, 10.0, 0, true),
            b(-4.0, -1.0, 1264.0, 1, true),
            b(-64.0, -16.0, 184.0, 3, true),
            b(-256.0, -64.0, 791.0, 4, true),
            b(-1024.0, -256.0, 33.0, 5, true),
        ]
    );
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatCustomBucketsIterators
#[test]
fn test_float_custom_buckets_iterators() {
    let cb = |lower: f64, upper: f64, count: f64, index: i32| Bucket {
        lower, upper,
        lower_inclusive: index == 0,
        upper_inclusive: true,
        count, index,
    };
    let h = FloatHistogram {
        count: 622.0, sum: 10008.4, schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(0, 2), sp(1, 1), sp(1, 1)],
        positive_buckets: vec![100.0, 344.0, 123.0, 55.0],
        custom_values: vec![10.0, 25.0, 50.0, 100.0, 500.0],
        ..Default::default()
    };
    let exp = vec![
        cb(f64::NEG_INFINITY, 10.0, 100.0, 0),
        cb(10.0, 25.0, 344.0, 1),
        cb(50.0, 100.0, 123.0, 3),
        cb(500.0, f64::INFINITY, 55.0, 5),
    ];
    let mut exp_rev = exp.clone();
    exp_rev.reverse();

    assert_eq!(h.all_bucket_iter().collect::<Vec<_>>(), exp);
    assert_eq!(h.all_reverse_bucket_iter().collect::<Vec<_>>(), exp_rev);
    assert_eq!(h.positive_bucket_iter().collect::<Vec<_>>(), exp);
    assert_eq!(h.positive_reverse_bucket_iter().collect::<Vec<_>>(), exp_rev);
    assert!(h.negative_bucket_iter().next().is_none());
    assert!(h.negative_reverse_bucket_iter().next().is_none());
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramEquals
#[test]
fn test_float_histogram_equals() {
    let h1 = FloatHistogram {
        schema: 3, count: 2.2, sum: 9.7, zero_threshold: 0.1, zero_count: 1.1,
        positive_buckets: vec![3.0], negative_buckets: vec![4.0],
        ..Default::default()
    };
    assert!(h1.prom_equals(&h1.copy()));

    let mut h_nan_c = h1.copy();
    h_nan_c.count = f64::NAN;
    assert!(!h1.prom_equals(&h_nan_c));
    assert!(h_nan_c.prom_equals(&h_nan_c));

    let mut h_nan_zc = h1.copy();
    h_nan_zc.zero_count = f64::NAN;
    assert!(!h1.prom_equals(&h_nan_zc));
    assert!(h_nan_zc.prom_equals(&h_nan_zc));

    let mut h_nan_pb = h1.copy();
    h_nan_pb.positive_buckets[0] = f64::NAN;
    assert!(!h1.prom_equals(&h_nan_pb));
    assert!(h_nan_pb.prom_equals(&h_nan_pb));

    let mut h_custom = h1.copy();
    h_custom.custom_values = vec![1.0, 2.0, 3.0];
    assert!(h1.prom_equals(&h_custom));

    let cbh1 = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA, count: 2.2, sum: 9.7,
        positive_spans: vec![sp(0, 1)], positive_buckets: vec![3.0],
        custom_values: vec![1.0, 2.0, 3.0],
        ..Default::default()
    };
    cbh1.validate().unwrap();
    let mut cbh2 = cbh1.copy();
    cbh2.custom_values = vec![1.0, 2.0, 3.0, 4.0];
    assert!(!cbh1.prom_equals(&cbh2));
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramSize
#[test]
fn test_float_histogram_size() {
    let empty = FloatHistogram {
        schema: 1, zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324,
        ..Default::default()
    };
    assert_eq!(empty.size(), 168);

    let full = FloatHistogram {
        schema: 1, zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324,
        positive_spans: vec![sp(-2, 1), sp(2, 3)],
        positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };
    assert_eq!(full.size(), 168 + 16 + 16 + 32 + 32);

    let custom = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA, count: 3493.3, sum: 2349209.324,
        positive_spans: vec![sp(0, 1), sp(2, 3)],
        positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
        custom_values: vec![1.0, 2.0, 3.0],
        ..Default::default()
    };
    assert_eq!(custom.size(), 168 + 16 + 32 + 24);
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramString
#[test]
fn test_float_histogram_string() {
    let exp_fh = FloatHistogram {
        schema: 1, zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324,
        positive_spans: vec![sp(-2, 1), sp(2, 3)],
        positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
        negative_spans: vec![sp(3, 2), sp(3, 2)],
        negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
        ..Default::default()
    };
    exp_fh.validate().unwrap();
    assert_eq!(
        exp_fh.to_string(),
        "{count:3493.3, sum:2.349209324e+06, [-22.62741699796952,-16):1000, [-16,-11.31370849898476):123400, [-4,-2.82842712474619):3, [-2.82842712474619,-2):3.1, [-0.01,0.01]:5.5, (0.35355339059327373,0.5]:1, (1,1.414213562373095]:3.3, (1.414213562373095,2]:4.2, (2,2.82842712474619]:0.1}"
    );

    let cb_fh = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA, count: 3493.3, sum: 2349209.324,
        positive_spans: vec![sp(0, 1), sp(2, 4)],
        positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 5.0],
        custom_values: vec![1.0, 2.0, 5.0, 10.0, 15.0, 20.0],
        ..Default::default()
    };
    cb_fh.validate().unwrap();
    assert_eq!(
        cb_fh.to_string(),
        "{count:3493.3, sum:2.349209324e+06, [-Inf,1]:1, (5,10]:3.3, (10,15]:4.2, (15,20]:0.1, (20,+Inf]:5}"
    );
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramValidateNegativeHistogram
#[test]
fn test_float_histogram_validate_negative_histogram() {
    let cases = [
        FloatHistogram { schema: 1, zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, -4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
        FloatHistogram { schema: 1, zero_threshold: 0.01, zero_count: 5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, -3.0, 1.234e5, 1000.0], ..Default::default() },
        FloatHistogram { schema: 1, zero_threshold: 0.01, zero_count: 5.5, count: -3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
        FloatHistogram { schema: 1, zero_threshold: 0.01, zero_count: -5.5, count: 3493.3, sum: 2349209.324, positive_spans: vec![sp(-2, 1), sp(2, 3)], positive_buckets: vec![1.0, 3.3, 4.2, 0.1], negative_spans: vec![sp(3, 2), sp(3, 2)], negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0], ..Default::default() },
    ];
    for fh in cases {
        assert!(fh.validate().is_err());
    }
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramReduceResolution
#[test]
fn test_float_histogram_reduce_resolution() {
    let mut valid = FloatHistogram {
        schema: 0,
        positive_spans: vec![sp(-2, 4), sp(0, 0), sp(3, 2)],
        positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0],
        negative_spans: vec![sp(0, 4), sp(0, 0), sp(3, 2)],
        negative_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0],
        ..Default::default()
    };
    valid.reduce_resolution(-1).unwrap();
    assert_eq!(
        valid,
        FloatHistogram {
            schema: -1,
            positive_spans: vec![sp(-1, 3), sp(1, 1)],
            positive_buckets: vec![1.0, 4.0, 2.0, 2.0],
            negative_spans: vec![sp(0, 3), sp(1, 1)],
            negative_buckets: vec![1.0, 4.0, 2.0, 2.0],
            ..Default::default()
        }
    );

    let mut not_enough = FloatHistogram { schema: 0, positive_spans: vec![sp(-2, 4), sp(0, 0), sp(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0], ..Default::default() };
    let err = not_enough.reduce_resolution(-1).unwrap_err();
    assert!(matches!(err, ReduceResolutionError::Validation(_)));
    assert_eq!(err.to_string(), "have 5 buckets but spans need more: histogram spans specify different number of buckets than provided");

    let mut too_many = FloatHistogram { schema: 0, positive_spans: vec![sp(-2, 4), sp(0, 0), sp(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 5.0], ..Default::default() };
    assert_eq!(too_many.reduce_resolution(-1).unwrap_err().to_string(), "spans need 6 buckets, have 7 buckets: histogram spans specify different number of buckets than provided");

    let mut neg_off = FloatHistogram { schema: 0, positive_spans: vec![sp(-2, 4), sp(-1, 0), sp(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0], ..Default::default() };
    assert_eq!(neg_off.reduce_resolution(-1).unwrap_err().to_string(), "span number 2 with offset -1: histogram has a span whose offset is negative");
}

// Upstream: github.com/prometheus/prometheus/model/histogram/float_histogram_test.go::TestFloatHistogramTrimBucketsInPlace
#[test]
fn test_float_histogram_trim_buckets_in_place() {
    let new_hist = || FloatHistogram {
        schema: 0, count: 30.0, sum: 100.0,
        positive_spans: vec![sp(0, 4)],
        positive_buckets: vec![5.0, 5.0, 10.0, 10.0],
        ..Default::default()
    };
    let mut h = new_hist();
    let ptr_before: *const FloatHistogram = &h;
    let got = h.trim_buckets(4.0, true);
    let ptr_after: *const FloatHistogram = got;
    assert_eq!(ptr_before, ptr_after);
    assert_ne!(h, new_hist());

    let orig = new_hist();
    let mut trimmed = orig.copy();
    trimmed.trim_buckets(4.0, true);
    assert_eq!(orig, new_hist());
    assert_ne!(orig, trimmed);
}


