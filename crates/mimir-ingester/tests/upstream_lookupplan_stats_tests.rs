//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/query_stats_test.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/stats_test.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/testing/comparison_test.go

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use tonic::metadata::MetadataMap;

use mimir_common::block_on_for_tests;
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, LimitsConfig, Overrides,
};
use mimir_ingester::lookupplan::{
    query_stats_from_current_context, with_query_stats_context, BlockIndexReader, BlockStatistics,
    PlanningBlockMeta, QueryStats, Statistics, StatisticsGenerator,
    DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH, DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH,
};

// ---------------------------------------------------------------------------
// query_stats_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/lookupplan/query_stats_test.go::TestQueryStats_EstimatedFinalCardinality
#[test]
fn test_query_stats_estimated_final_cardinality() {
    // set and load estimated final cardinality
    let stats = QueryStats::new();
    QueryStats::set_estimated_final_cardinality(Some(&stats), 100);
    QueryStats::set_estimated_final_cardinality(Some(&stats), 50);
    assert_eq!(50, QueryStats::load_estimated_final_cardinality(Some(&stats)));

    // set and load estimated final cardinality nil receiver
    QueryStats::set_estimated_final_cardinality(None, 100);
    assert_eq!(0, QueryStats::load_estimated_final_cardinality(None));
}

// Upstream: pkg/ingester/lookupplan/query_stats_test.go::TestQueryStats_EstimatedSelectedPostings
#[test]
fn test_query_stats_estimated_selected_postings() {
    let stats = QueryStats::new();
    QueryStats::set_estimated_selected_postings(Some(&stats), 200);
    QueryStats::set_estimated_selected_postings(Some(&stats), 150);
    assert_eq!(150, QueryStats::load_estimated_selected_postings(Some(&stats)));

    QueryStats::set_estimated_selected_postings(None, 200);
    assert_eq!(0, QueryStats::load_estimated_selected_postings(None));
}

// Upstream: pkg/ingester/lookupplan/query_stats_test.go::TestQueryStats_ActualSelectedPostings
#[test]
fn test_query_stats_actual_selected_postings() {
    let stats = QueryStats::new();
    QueryStats::set_actual_selected_postings(Some(&stats), 180);
    QueryStats::set_actual_selected_postings(Some(&stats), 120);
    assert_eq!(120, QueryStats::load_actual_selected_postings(Some(&stats)));

    QueryStats::set_actual_selected_postings(None, 180);
    assert_eq!(0, QueryStats::load_actual_selected_postings(None));
}

// Upstream: pkg/ingester/lookupplan/query_stats_test.go::TestQueryStats_ActualFinalCardinality
#[test]
fn test_query_stats_actual_final_cardinality() {
    let stats = QueryStats::new();
    QueryStats::set_actual_final_cardinality(Some(&stats), 80);
    QueryStats::set_actual_final_cardinality(Some(&stats), 60);
    assert_eq!(60, QueryStats::load_actual_final_cardinality(Some(&stats)));

    QueryStats::set_actual_final_cardinality(None, 80);
    assert_eq!(0, QueryStats::load_actual_final_cardinality(None));
}

// Upstream: pkg/ingester/lookupplan/query_stats_test.go::TestQueryStats_Context
#[test]
fn test_query_stats_context() {
    // context with empty stats
    let stats = Arc::new(QueryStats::new());
    with_query_stats_context(Some(Arc::clone(&stats)), || {
        let retrieved = query_stats_from_current_context().expect("stats must be in context");
        assert!(Arc::ptr_eq(&stats, &retrieved));
    });

    // context without stats
    assert!(query_stats_from_current_context().is_none());
}

// Upstream: pkg/ingester/lookupplan/query_stats_test.go::TestQueryStats_AllFields
#[test]
fn test_query_stats_all_fields() {
    let stats = QueryStats::new();

    QueryStats::set_estimated_final_cardinality(Some(&stats), 1000);
    QueryStats::set_estimated_selected_postings(Some(&stats), 2000);
    QueryStats::set_actual_selected_postings(Some(&stats), 1800);
    QueryStats::set_actual_final_cardinality(Some(&stats), 900);

    assert_eq!(1000, QueryStats::load_estimated_final_cardinality(Some(&stats)));
    assert_eq!(2000, QueryStats::load_estimated_selected_postings(Some(&stats)));
    assert_eq!(1800, QueryStats::load_actual_selected_postings(Some(&stats)));
    assert_eq!(900, QueryStats::load_actual_final_cardinality(Some(&stats)));
}

// ---------------------------------------------------------------------------
// stats_test.go
// ---------------------------------------------------------------------------

const COUNT_MIN_EPSILON: f64 = 0.005;

/// Simplified in-memory index reader for testing (port of `mockIndexReader`,
/// backed by an in-memory postings map like `index.MemPostings`).
#[derive(Default)]
struct MockIndexReader {
    /// label name -> label value -> sorted series refs
    postings: HashMap<String, HashMap<String, Vec<u64>>>,
    all_postings: Vec<u64>,
}

impl MockIndexReader {
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, series_ref: u64, lset: &[(&str, &str)]) {
        self.all_postings.push(series_ref);
        for (name, value) in lset {
            self.postings
                .entry((*name).to_string())
                .or_default()
                .entry((*value).to_string())
                .or_default()
                .push(series_ref);
        }
    }

    fn meta(&self) -> PlanningBlockMeta {
        PlanningBlockMeta::default()
    }
}

impl BlockIndexReader for MockIndexReader {
    fn all_postings_count(&self) -> Result<u64, String> {
        let mut refs = self.all_postings.clone();
        refs.sort_unstable();
        refs.dedup();
        Ok(refs.len() as u64)
    }

    // Mirrors `mockIndexReader.LabelNames`: label names with an empty name are skipped.
    fn label_names(&self) -> Result<Vec<String>, String> {
        Ok(self
            .postings
            .keys()
            .filter(|n| !n.is_empty())
            .cloned()
            .collect())
    }

    fn label_values(&self, name: &str) -> Result<Vec<String>, String> {
        Ok(self
            .postings
            .get(name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    // `PostingsForAllLabelValues`: the union of postings for all values of the label name.
    fn postings_for_all_values_count(&self, name: &str) -> Result<u64, String> {
        let mut refs: Vec<u64> = self
            .postings
            .get(name)
            .map(|m| m.values().flatten().copied().collect())
            .unwrap_or_default();
        refs.sort_unstable();
        refs.dedup();
        Ok(refs.len() as u64)
    }

    fn postings_count(&self, name: &str, value: &str) -> Result<u64, String> {
        Ok(self
            .postings
            .get(name)
            .and_then(|m| m.get(value))
            .map_or(0, |v| v.len() as u64))
    }
}

fn gen_stats(
    p: &MockIndexReader,
    small: u64,
    large: u64,
) -> Result<BlockStatistics, String> {
    StatisticsGenerator::new().stats(&p.meta(), p, small, large)
}

// TestLabelsValuesSketches_LabelName tests getting cardinality and value counts for label names,
// but not specific values for a given label name, for small-count cases.
// Upstream: pkg/ingester/lookupplan/stats_test.go::TestLabelsValuesSketches_LabelName
#[test]
fn test_labels_values_sketches_label_name() {
    struct Expected {
        label_name: &'static str,
        cardinality: u64,
        values_count: u64,
    }
    let e = |label_name, cardinality, values_count| Expected {
        label_name,
        cardinality,
        values_count,
    };
    type Series = BTreeMap<u64, Vec<(&'static str, &'static str)>>;
    let tests: Vec<(&str, Series, Vec<Expected>)> = vec![
        (
            "empty postings should return no values",
            Series::new(),
            vec![e("", 0, 0)],
        ),
        (
            // labels.FromStrings(index.AllPostingsKey()) is the ("", "") label.
            "one series with empty label name should return no values",
            Series::from([(1, vec![("", "")])]),
            vec![e("", 0, 0)],
        ),
        (
            "empty label value contributes",
            Series::from([(1, vec![("test", "")])]),
            vec![e("test", 1, 1)],
        ),
        (
            "multiple label names on a single series",
            Series::from([(1, vec![("label1", "value1"), ("label2", "value2")])]),
            vec![e("label1", 1, 1), e("label2", 1, 1)],
        ),
        (
            "multiple series with the same label name and value",
            Series::from([(1, vec![("label1", "value1")]), (2, vec![("label1", "value1")])]),
            vec![e("label1", 2, 1)],
        ),
        (
            "multiple series with the same label name and different values",
            Series::from([(1, vec![("label1", "value1")]), (2, vec![("label1", "value2")])]),
            vec![e("label1", 2, 2)],
        ),
        (
            "multiple series with overlapping label names",
            Series::from([
                (1, vec![("label1", "value1"), ("label2", "value1")]),
                (2, vec![("label2", "value2")]),
            ]),
            vec![e("label1", 1, 1), e("label2", 2, 2)],
        ),
        (
            "multiple series with non-overlapping label names",
            Series::from([
                (1, vec![("label1", "value1"), ("label3", "value1")]),
                (2, vec![("label2", "value2")]),
            ]),
            vec![e("label1", 1, 1), e("label2", 1, 1), e("label3", 1, 1)],
        ),
    ];

    for (name, series, expected) in tests {
        let mut p = MockIndexReader::new();
        for (r, ls) in &series {
            p.add(*r, ls);
        }
        let sketches = gen_stats(
            &p,
            DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH,
            DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
        )
        .unwrap_or_else(|e| panic!("{name}: {e}"));

        for ev in expected {
            let values_count = sketches.label_values_count(ev.label_name);
            let values_card = sketches.label_values_cardinality(ev.label_name, &[]);
            assert_eq!(ev.values_count, values_count, "{name}: values count for {:?}", ev.label_name);
            assert_eq!(ev.cardinality, values_card, "{name}: cardinality for {:?}", ev.label_name);
        }
    }
}

// TestLabelsValuesSketches_LabelValue tests the cardinality calculation when looking at specific label values,
// for small-count cases.
// Upstream: pkg/ingester/lookupplan/stats_test.go::TestLabelsValuesSketches_LabelValue
#[test]
fn test_labels_values_sketches_label_value() {
    struct Expected {
        label_name: &'static str,
        label_values: Vec<&'static str>,
        cardinality: u64,
    }
    let e = |label_name, label_values, cardinality| Expected {
        label_name,
        label_values,
        cardinality,
    };
    type Series = BTreeMap<u64, Vec<(&'static str, &'static str)>>;
    let tests: Vec<(&str, Series, Vec<Expected>)> = vec![
        (
            "empty label value should match",
            Series::from([(1, vec![("test", "")])]),
            vec![e("test", vec![""], 1)],
        ),
        (
            "multiple series with matching label values",
            Series::from([(1, vec![("label1", "value1")]), (2, vec![("label1", "value1")])]),
            vec![e("label1", vec!["value1"], 2), e("label1", vec![""], 0)],
        ),
        (
            "multiple series with non-matching label values",
            Series::from([(1, vec![("label1", "value1")]), (2, vec![("label1", "value2")])]),
            vec![
                e("label1", vec!["value1"], 1),
                e("label1", vec!["value2"], 1),
                e("label1", vec!["value1", "value2"], 2),
                e("label2", vec!["value1", "value2"], 0),
            ],
        ),
        (
            "multiple series with non-matching label names",
            Series::from([
                (1, vec![("label1", "value1")]),
                (2, vec![("label2", "value2")]),
                (3, vec![("label2", "value1")]),
            ]),
            vec![
                e("label1", vec!["value1"], 1),
                e("label2", vec!["value1"], 1),
                e("label2", vec!["value2"], 1),
                e("label2", vec!["value1", "value2"], 2),
            ],
        ),
    ];

    for (name, series, expected) in tests {
        let mut p = MockIndexReader::new();
        for (r, ls) in &series {
            p.add(*r, ls);
        }
        let sketches = gen_stats(
            &p,
            DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH,
            DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
        )
        .unwrap_or_else(|e| panic!("{name}: {e}"));

        for ev in expected {
            let card = sketches.label_values_cardinality(ev.label_name, &ev.label_values);
            assert_eq!(
                ev.cardinality, card,
                "{name}: {:?} {:?}",
                ev.label_name, ev.label_values
            );
        }
    }
}

// TestLabelName_ComparisonAcrossLabelNames tests comparison across multiple count-min sketches.
// The count-min sketch epsilon is set by number of label name values such that a series with a relatively
// low number of label name values would produce a sketch with a higher relative-accuracy factor -- that is, a smaller width.
// It requires that the estimated cardinality for all high-cardinality values for a label name are greater than
// the estimated cardinality for all low-cardinality values of a different label name.
// Limitation: This does not test against label names with a different number of series across label names.
// Upstream: pkg/ingester/lookupplan/stats_test.go::TestLabelName_ComparisonAcrossLabelNames
#[test]
fn test_label_name_comparison_across_label_names() {
    let low_card_label = "low_card";
    let high_card_label = "high_card";
    struct Case {
        name: &'static str,
        num_low_card_series: usize,
        num_high_card_series: usize,
        larger_sketch_threshold: u64,
    }
    let tests = [
        Case {
            name: "labels would have same epsilon with default",
            num_low_card_series: 1_000,
            num_high_card_series: 500_000,
            larger_sketch_threshold: DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
        },
        Case {
            name: "labels would have different epsilon with default",
            num_low_card_series: 1_000,
            num_high_card_series: 1_000_000,
            larger_sketch_threshold: DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
        },
        Case {
            name: "labels would have high epsilon with custom setting",
            num_low_card_series: 1_000,
            num_high_card_series: 100_000,
            larger_sketch_threshold: 200_000,
        },
        Case {
            name: "labels would have low epsilon with custom setting",
            num_low_card_series: 1_000,
            num_high_card_series: 100_000,
            larger_sketch_threshold: 100,
        },
        Case {
            name: "labels would have different epsilon with custom setting",
            num_low_card_series: 1_000,
            num_high_card_series: 100_000,
            larger_sketch_threshold: 10_000,
        },
    ];

    for tt in tests {
        let mut p = MockIndexReader::new();
        assert!(tt.num_low_card_series < tt.num_high_card_series);

        // Populate postings
        for i in 0..tt.num_high_card_series {
            let s = i.to_string();
            if i < tt.num_low_card_series {
                p.add(i as u64, &[(low_card_label, &s), (high_card_label, &s)]);
            } else {
                p.add(i as u64, &[(high_card_label, &s)]);
            }
        }

        let s = gen_stats(&p, DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH, tt.larger_sketch_threshold)
            .unwrap();

        let low_card = s.label_values_cardinality(low_card_label, &[]);
        assert_eq!(tt.num_low_card_series as u64, s.label_values_count(low_card_label), "{}", tt.name);
        assert_eq!(
            tt.num_low_card_series as u64, low_card,
            "{}: low card: {}, expected: {}",
            tt.name, low_card, tt.num_low_card_series
        );

        let high_card = s.label_values_cardinality(high_card_label, &[]);
        assert_eq!(tt.num_high_card_series as u64, s.label_values_count(high_card_label), "{}", tt.name);
        assert_eq!(tt.num_high_card_series as u64, high_card, "{}", tt.name);

        assert!(high_card > low_card, "{}", tt.name);
    }
}

// TestLabelName_ManySeries tests the accuracy of label value sketches at high volume.
// It evenly distributes 6M series across 1k labels,
// and expects the result to be within 30k (i.e., 0.5% or countMinEpsilon of 6M) of 6000 (6M / 1k).
// Upstream: pkg/ingester/lookupplan/stats_test.go::TestLabelName_ManySeries
#[test]
fn test_label_name_many_series() {
    let label_name = "test_label";
    let mut p = MockIndexReader::new();
    let num_series: usize = 6_000_000;
    let num_label_values: usize = 1_000;
    let values: Vec<String> = (0..num_label_values).map(|i| i.to_string()).collect();
    for i in 0..num_series {
        p.add(i as u64, &[(label_name, &values[i % num_label_values])]);
    }

    let s = gen_stats(
        &p,
        DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH,
        DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
    )
    .unwrap();

    assert_eq!(num_label_values as u64, s.label_values_count(label_name));
    assert_eq!(num_series as u64, s.label_values_cardinality(label_name, &[]));

    let delta = num_series as f64 * COUNT_MIN_EPSILON;
    for (i, v) in values.iter().enumerate() {
        // The cardinality for every label should be within epsilon of the total number of series to the expected cardinality.
        // Technically, it should be within epsilon of the total increments seen by the count-min sketch,
        // but that's more opaque to understand. The total increments seen will always be equal or greater than the number of series.
        let expected = (num_series / num_label_values) as f64;
        let actual = s.label_values_cardinality(label_name, &[v.as_str()]) as f64;
        assert!(
            (expected - actual).abs() <= delta,
            "Cardinality for label {i} is not within {delta} of expected ({expected} vs {actual})"
        );
    }
}

// TestLabelName_NonUniformValueDistribution tests that for a given label, if one value is much lower-cardinality
// than all others, the resulting count-min sketch reflects that difference in order of magnitude against all
// higher-cardinality label values.
// Upstream: pkg/ingester/lookupplan/stats_test.go::TestLabelName_NonUniformValueDistribution
#[test]
fn test_label_name_non_uniform_value_distribution() {
    let label_name = "test_label";
    let num_series: usize = 6_000_000;
    let low_card: usize = 10;
    let low_card_value = "low";
    let num_high_occurrence_values: usize = 1_000;

    let mut p = MockIndexReader::new();
    assert!(low_card + num_high_occurrence_values < num_series);

    let values: Vec<String> = (0..num_high_occurrence_values).map(|i| i.to_string()).collect();
    for i in 0..(num_series - low_card) {
        p.add(i as u64, &[(label_name, &values[i % num_high_occurrence_values])]);
    }

    // cardinality of "low" value will be 10
    for i in (num_series - low_card)..num_series {
        p.add(i as u64, &[(label_name, low_card_value)]);
    }

    let s = gen_stats(
        &p,
        DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH,
        DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
    )
    .unwrap();

    let low_val_card = s.label_values_cardinality(label_name, &[low_card_value]);

    // The cardinality of every other value should be ≥6000. We care about these values being correct in magnitude,
    // i.e., floor(log(highOccurrenceCardinality) / log(lowOccurrenceCardinality)) should be consistent every time.
    // We add a little margin since the margin of error (30k) is enough to push us one power up, but not two.
    for v in &values {
        let card = s.label_values_cardinality(label_name, &[v.as_str()]);
        let mag = (card as f64).ln() / (low_val_card as f64).ln();
        let floor = mag.floor() as i64;
        assert!(floor >= 3, "mag {mag} for value {v} (card {card}, low {low_val_card})");
        assert!(floor < 5, "mag {mag} for value {v} (card {card}, low {low_val_card})");
    }
}

// ---------------------------------------------------------------------------
// testing/comparison_test.go
// ---------------------------------------------------------------------------

/// Port of `detailedQueryResult` / `seriesWithChunks` / `chunkInfo`.
#[derive(Default)]
struct DetailedQueryResult {
    series_count: usize,
    chunks_count: usize,
    series: Vec<SeriesWithChunks>,
}

struct SeriesWithChunks {
    labels: Vec<(String, String)>,
    chunk_count: usize,
    chunks: Vec<ChunkInfo>,
}

struct ChunkInfo {
    min_time: i64,
    max_time: i64,
    encoding: i32,
    data: Vec<u8>,
}

/// Port of `mockDetailedQueryStreamServer.Send` applied to every streamed response.
fn collect_detailed(
    responses: &[ingesterpb::QueryStreamResponse],
) -> Result<DetailedQueryResult, String> {
    let mut result = DetailedQueryResult::default();
    let mut seen_end_of_series_stream = false;
    for resp in responses {
        // First, process series metadata
        for s in &resp.streaming_series {
            result.series_count += 1;
            result.chunks_count += s.chunk_count as usize;
            result.series.push(SeriesWithChunks {
                labels: s
                    .labels
                    .iter()
                    .map(|l| {
                        (
                            l.name_str().unwrap().to_string(),
                            l.value_str().unwrap().to_string(),
                        )
                    })
                    .collect(),
                chunk_count: s.chunk_count as usize,
                chunks: Vec::with_capacity(s.chunk_count as usize),
            });
        }

        // Then, process chunk data
        for sc in &resp.streaming_series_chunks {
            // SeriesIndex refers to the series in the order they were sent
            if sc.series_index as usize >= result.series.len() {
                return Err(format!(
                    "invalid series index {} (have {} series)",
                    sc.series_index,
                    result.series.len()
                ));
            }
            for chunk in &sc.chunks {
                result.series[sc.series_index as usize].chunks.push(ChunkInfo {
                    min_time: chunk.start_timestamp_ms,
                    max_time: chunk.end_timestamp_ms,
                    encoding: chunk.encoding,
                    data: chunk.data.to_vec(),
                });
            }
        }

        if resp.is_end_of_series_stream {
            seen_end_of_series_stream = true;
        }
    }
    assert!(seen_end_of_series_stream);
    Ok(result)
}

/// Port of `queryIngesterDetailed`: executes a query and returns detailed results for comparison.
async fn query_ingester_detailed(
    ing: &Ingester,
    matchers: Vec<ingesterpb::LabelMatcher>,
    user_id: &str,
) -> Result<DetailedQueryResult, String> {
    let req = ingesterpb::QueryRequest {
        // Cover all blocks by using extreme timestamps.
        start_timestamp_ms: i64::MIN,
        end_timestamp_ms: i64::MAX,
        matchers,
        streaming_chunks_batch_size: 0,
    };
    let responses = ing
        .handle_query_stream(&MetadataMap::new(), user_id, req)
        .await
        .map_err(|e| format!("failed to execute query: {e}"))?;
    collect_detailed(&responses)
}

/// Starts an ingester like `benchmarks.StartBenchmarkIngester` with the given
/// `IndexLookupPlanning.Enabled` setting.
async fn start_ingester(dir: &std::path::Path, lookup_planning_enabled: bool) -> Arc<Ingester> {
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "localhost".to_string();
    let limits = LimitsConfig {
        max_global_series_per_user: 0,
        max_global_series_per_metric: 0,
        ..Default::default()
    };
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
    blocks_cfg.tsdb.ship_interval = GoDuration::ZERO;
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = lookup_planning_enabled;
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

fn matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

// TestCompareIngesterConfigs runs queries against two ingesters with different configurations
// and asserts that they return the same results.
//
// The Go test is skipped unless `-data-dir` / `-query-file` flags point at a production-like TSDB
// data directory. Here the same comparison logic runs against a deterministic synthetic data set
// pushed into both ingesters (planning enabled vs. disabled), with the expected series count of
// every selector computed independently.
// Upstream: pkg/ingester/lookupplan/testing/comparison_test.go::TestCompareIngesterConfigs
#[test]
fn test_compare_ingester_configs() {
    use ingesterpb::MatchType as M;
    block_on_for_tests(async {
        let tenant = "tenant-1";
        let tmp = tempfile::tempdir().unwrap();

        // 2 metric names x 3 jobs x 10 instances = 60 series, 5 samples each.
        let mut req = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        for name in ["m_a", "m_b"] {
            for job in 0..3 {
                for inst in 0..10 {
                    req.timeseries.push(cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_strs("__name__", name),
                            cortexpb::LabelPair::from_strs("instance", format!("i{inst}")),
                            cortexpb::LabelPair::from_strs("job", format!("j{job}")),
                        ],
                        samples: (0..5)
                            .map(|k| cortexpb::Sample {
                                timestamp_ms: 1_000 * (k + 1),
                                value: (job * 100 + inst * 10 + k) as f64,
                            })
                            .collect(),
                        ..Default::default()
                    });
                }
            }
        }

        let ing1 = start_ingester(&tmp.path().join("data-copy"), true).await;
        let ing2 = start_ingester(&tmp.path().join("data"), false).await;
        ing1.push_grpc(tenant, &req).await.unwrap();
        ing2.push_grpc(tenant, &req).await.unwrap();

        let selectors: Vec<(&str, Vec<ingesterpb::LabelMatcher>, usize)> = vec![
            ("name equal", vec![matcher(M::Equal, "__name__", "m_a")], 30),
            (
                "regex and not-equal",
                vec![matcher(M::RegexMatch, "job", "j0|j1"), matcher(M::NotEqual, "instance", "i3")],
                36,
            ),
            (
                "regex name and negative regex",
                vec![matcher(M::RegexMatch, "__name__", "m_.*"), matcher(M::RegexNoMatch, "job", "j.*")],
                0,
            ),
            (
                "equal and empty matcher on missing label",
                vec![matcher(M::Equal, "job", "j2"), matcher(M::Equal, "missing", "")],
                20,
            ),
            (
                "regex and not-equal on job",
                vec![matcher(M::RegexMatch, "instance", "i[0-4]"), matcher(M::NotEqual, "job", "j0")],
                20,
            ),
            ("all series", vec![matcher(M::RegexMatch, "__name__", ".+")], 60),
        ];

        for (name, matchers, expected_series) in selectors {
            // Query first ingester
            let result1 = query_ingester_detailed(&ing1, matchers.clone(), tenant)
                .await
                .unwrap_or_else(|e| panic!("Query failed on ingester 1: {name}: {e}"));
            // Query second ingester
            let result2 = query_ingester_detailed(&ing2, matchers, tenant)
                .await
                .unwrap_or_else(|e| panic!("Query failed on ingester 2: {name}: {e}"));

            assert_eq!(expected_series, result1.series_count, "Unexpected series count for {name}");
            assert_eq!(result1.series_count, result2.series_count, "Series count mismatch for query: {name}");
            assert_eq!(result1.chunks_count, result2.chunks_count, "Chunks count mismatch for query: {name}");
            assert_eq!(result1.series.len(), result2.series.len(), "Number of series mismatch for query: {name}");

            // Compare each series - order must be the same between both ingesters
            for (i, (s1, s2)) in result1.series.iter().zip(result2.series.iter()).enumerate() {
                assert_eq!(s1.labels, s2.labels, "Labels mismatch at series {i} for query: {name}");
                assert_eq!(s1.chunk_count, s2.chunk_count, "Chunk count mismatch for series {:?} in query: {name}", s1.labels);
                assert_eq!(s1.chunks.len(), s2.chunks.len(), "Number of chunks mismatch for series {:?} in query: {name}", s1.labels);
                assert_eq!(s1.chunk_count, s1.chunks.len(), "Streamed chunks must match announced chunk count for {:?}", s1.labels);

                // Compare chunks
                for (j, (c1, c2)) in s1.chunks.iter().zip(s2.chunks.iter()).enumerate() {
                    assert_eq!(c1.min_time, c2.min_time, "Chunk {j} MinTime mismatch for series {:?} in query: {name}", s1.labels);
                    assert_eq!(c1.max_time, c2.max_time, "Chunk {j} MaxTime mismatch for series {:?} in query: {name}", s1.labels);
                    assert_eq!(c1.encoding, c2.encoding, "Chunk {j} Encoding mismatch for series {:?} in query: {name}", s1.labels);
                    assert_eq!(
                        c1.data, c2.data,
                        "Chunk {j} Data mismatch for series {:?} in query: {name} (len1={}, len2={})",
                        s1.labels,
                        c1.data.len(),
                        c2.data.len()
                    );
                }
            }
        }

        ing1.stop().await.unwrap();
        ing2.stop().await.unwrap();
    });
}
