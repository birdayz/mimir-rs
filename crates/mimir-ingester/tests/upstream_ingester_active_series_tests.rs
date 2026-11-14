//! Ported from github.com/grafana/mimir/pkg/ingester/active_series_test.go

use std::sync::Arc;

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, LimitsConfig, Overrides,
};
use mimir_ingester::active_series::match_all_series;
use mimir_ingester::query::ACTIVE_SERIES_MAX_SIZE_BYTES;
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_proto::prost::Message;
use tonic::metadata::MetadataMap;

const USER_ID: &str = "test";
const METRIC_NAME_LABEL: &str = "__name__";
const SHARD_LABEL: &str = "__query_shard__";

/// Port of `prepareHealthyIngester`: an ingester without a ring, with per-user/per-metric
/// series limits disabled, started and ready to serve push and read requests.
async fn prepare_healthy_ingester(
    tmp: &tempfile::TempDir,
    mutate_limits: Option<&dyn Fn(&mut LimitsConfig)>,
) -> Arc<Ingester> {
    let mut cfg = IngesterConfig::default();
    cfg.active_series.active_series_metrics_enabled = true;
    cfg.ring.instance_id = "localhost".to_string();

    let mut limits = LimitsConfig {
        max_global_series_per_metric: 0,
        max_global_series_per_user: 0,
        ..Default::default()
    };
    if let Some(f) = mutate_limits {
        f(&mut limits);
    }

    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.path().join("tsdb").to_string_lossy().into_owned();
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_secs(60);
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_hours(1);
    blocks_cfg.tsdb.ship_interval = GoDuration::ZERO;

    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(limits),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.start().await.unwrap();
    ing
}

/// Port of `client.ToActiveSeriesRequest`.
fn to_active_series_request(matchers: &[LabelMatcher]) -> ingesterpb::ActiveSeriesRequest {
    ingesterpb::ActiveSeriesRequest {
        matchers: matchers
            .iter()
            .map(|m| ingesterpb::LabelMatcher {
                r#type: match m.match_type {
                    MatchType::Equal => ingesterpb::MatchType::Equal,
                    MatchType::NotEqual => ingesterpb::MatchType::NotEqual,
                    MatchType::RegexMatch => ingesterpb::MatchType::RegexMatch,
                    MatchType::RegexNotMatch => ingesterpb::MatchType::RegexNoMatch,
                } as i32,
                name: m.name.clone(),
                value: m.value.clone(),
            })
            .collect(),
        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
    }
}

fn must_matcher(t: MatchType, name: &str, value: &str) -> LabelMatcher {
    LabelMatcher::new(t, name, value).unwrap()
}

/// `fmt.Sprint(matchers)` equivalent, only used for assertion messages.
fn fmt_matchers(matchers: &[LabelMatcher]) -> String {
    let parts: Vec<String> = matchers
        .iter()
        .map(|m| {
            let op = match m.match_type {
                MatchType::Equal => "=",
                MatchType::NotEqual => "!=",
                MatchType::RegexMatch => "=~",
                MatchType::RegexNotMatch => "!~",
            };
            format!("{}{}\"{}\"", m.name, op, m.value)
        })
        .collect();
    format!("[{}]", parts.join(" "))
}

/// Port of `GenerateTestHistogram(1)` (prometheus `tsdbutil.GenerateTestHistogram`) converted with
/// `mimirpb.FromHistogramToHistogramProto(1_000, ...)`.
fn test_histogram_proto(ts: i64, i: i64) -> cortexpb::Histogram {
    let spans = vec![
        cortexpb::BucketSpan { offset: 0, length: 2 },
        cortexpb::BucketSpan { offset: 1, length: 2 },
    ];
    let deltas = vec![i + 1, 1, -1, 0];
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(12 + (i * 9) as u64)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2 + i as u64)),
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: spans.clone(),
        positive_deltas: deltas.clone(),
        negative_spans: spans,
        negative_deltas: deltas,
        reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
        timestamp: ts,
        ..Default::default()
    }
}

fn series_with_labels_of_size(size: usize, index: usize, histogram: Option<bool>) -> cortexpb::TimeSeries {
    // 24 bytes of static strings and slice overhead, the remaining bytes are used to
    // pad the value of the "lbl" label.
    assert!(size > 24, "minimum message size is 24 bytes");
    let lbl = format!("{:0width$}", index, width = size - 24);
    let mut ts = cortexpb::TimeSeries {
        labels: vec![
            cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test"),
            cortexpb::LabelPair::from_strs("lbl", lbl),
        ],
        ..Default::default()
    };
    match histogram {
        Some(true) => ts.histograms = vec![test_histogram_proto(1_000, 1)],
        _ => {
            ts.samples = vec![cortexpb::Sample {
                timestamp_ms: 1_000,
                value: 1.0,
            }]
        }
    }
    ts
}

fn md() -> MetadataMap {
    let mut md = MetadataMap::new();
    md.insert(LOWER_ORG_ID_HEADER_NAME, USER_ID.parse().unwrap());
    md
}

// Upstream: pkg/ingester/active_series_test.go::TestIngester_ActiveSeries
#[test]
fn test_ingester_active_series() {
    block_on_for_tests(async {
        let total_message_count = 4usize;
        let total_series_size = total_message_count * ACTIVE_SERIES_MAX_SIZE_BYTES;

        let mut write_req = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let mut current_size = 0usize;
        let mut i = 0usize;
        while current_size < total_series_size {
            let s = series_with_labels_of_size(1024, i, None);
            current_size += s.encoded_len();
            write_req.timeseries.push(s);
            i += 1;
        }

        // Write the series.
        let tmp = tempfile::tempdir().unwrap();
        let ingester = prepare_healthy_ingester(&tmp, None).await;
        ingester.push_grpc(USER_ID, &write_req).await.unwrap();

        struct Case {
            matchers: Vec<LabelMatcher>,
            expected_message_count: usize,
            expected_series_count: usize,
        }
        let test_cases = vec![
            Case {
                // Match all series by name.
                matchers: vec![must_matcher(MatchType::Equal, METRIC_NAME_LABEL, "test")],
                expected_message_count: total_message_count,
                expected_series_count: write_req.timeseries.len(),
            },
            Case {
                // Match all series by blanket regex, but sharded.
                matchers: vec![
                    must_matcher(MatchType::Equal, SHARD_LABEL, "1_of_4"),
                    must_matcher(MatchType::RegexMatch, METRIC_NAME_LABEL, ".*"),
                ],
                expected_message_count: total_message_count / 4,
                expected_series_count: 1012, // Note this number should change if the sample data changes.
            },
        ];

        for tc in test_cases {
            let name = fmt_matchers(&tc.matchers);
            // Get active series
            let req = to_active_series_request(&tc.matchers);
            let responses = ingester
                .handle_active_series(&md(), USER_ID, req)
                .await
                .unwrap();

            // Check that all series were returned.
            let mut returned_series_count = 0;
            for res in &responses {
                returned_series_count += res.metric.len();
                // Check that all series have the expected number of labels.
                for m in &res.metric {
                    assert_eq!(2, m.labels.len(), "{name}");
                }
            }
            assert_eq!(tc.expected_series_count, returned_series_count, "{name}");
            assert_eq!(tc.expected_message_count, responses.len(), "{name}");
        }

        ingester.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/active_series_test.go::TestIngester_ActiveNativeHistogramSeries
#[test]
fn test_ingester_active_native_histogram_series() {
    block_on_for_tests(async {
        let expected_message_count = 4usize;
        let total_series_size = expected_message_count * ACTIVE_SERIES_MAX_SIZE_BYTES;

        let mut write_req = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let mut current_size = 0usize;
        let mut i = 0usize;
        while current_size < total_series_size {
            let is_histogram = !i.is_multiple_of(2); // Half of the series will be float and the other half will be native histograms.
            let s = series_with_labels_of_size(1024, i, Some(is_histogram));
            if is_histogram {
                current_size += s.encoded_len();
            }
            write_req.timeseries.push(s);
            i += 1;
        }

        // Write the series.
        let tmp = tempfile::tempdir().unwrap();
        let ingester = prepare_healthy_ingester(
            &tmp,
            Some(&|limits: &mut LimitsConfig| limits.native_histograms_ingestion_enabled = true),
        )
        .await;
        ingester.push_grpc(USER_ID, &write_req).await.unwrap();

        // Get active series
        let mut req =
            to_active_series_request(&[must_matcher(MatchType::Equal, METRIC_NAME_LABEL, "test")]);
        req.r#type = ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32;

        let responses = ingester
            .handle_active_series(&md(), USER_ID, req)
            .await
            .unwrap();

        // Check that all series were returned.
        let mut returned_series_count = 0;
        for res in &responses {
            returned_series_count += res.metric.len();
            // Check that all series have a corresponding bucket count.
            assert_eq!(
                res.metric.len(),
                res.bucket_count.len(),
                "All series should have a bucket count."
            );
            for bc in &res.bucket_count {
                assert_eq!(8u64, *bc);
            }
        }
        assert_eq!(write_req.timeseries.len() / 2, returned_series_count);

        // Check that we got the correct number of messages.
        assert_eq!(expected_message_count, responses.len());

        ingester.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/active_series_test.go::TestMatchAllSeries
#[test]
fn test_match_all_series() {
    struct Case {
        name: &'static str,
        matchers: Vec<LabelMatcher>,
        expected: bool,
    }
    let tests = vec![
        Case {
            name: "empty matchers",
            matchers: vec![],
            expected: false,
        },
        Case {
            name: "single matcher with regexp .*",
            matchers: vec![must_matcher(MatchType::RegexMatch, "foo", ".*")],
            expected: true,
        },
        Case {
            name: "single matcher on __name__ with regexp .+",
            matchers: vec![must_matcher(MatchType::RegexMatch, METRIC_NAME_LABEL, ".+")],
            expected: true,
        },
        Case {
            name: "single matcher on __name__ with not-equal to empty string",
            matchers: vec![must_matcher(MatchType::NotEqual, METRIC_NAME_LABEL, "")],
            expected: true,
        },
        Case {
            name: "single matcher with different regexp pattern",
            matchers: vec![must_matcher(MatchType::RegexMatch, "foo", "bar")],
            expected: false,
        },
        Case {
            name: "multiple matchers with regexp .*",
            matchers: vec![
                must_matcher(MatchType::RegexMatch, "foo", ".*"),
                must_matcher(MatchType::Equal, "bar", "baz"),
            ],
            expected: false,
        },
    ];

    for tc in tests {
        let result = match_all_series(&tc.matchers);
        assert_eq!(tc.expected, result, "{}", tc.name);
    }
}
