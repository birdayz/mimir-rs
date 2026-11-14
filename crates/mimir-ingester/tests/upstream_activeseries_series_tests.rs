//! Ported from github.com/grafana/mimir/pkg/ingester/activeseries/active_series_test.go

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_ingester::active_series::{
    ActiveSeries, CostAttributionActiveTracker, CostAttributionLabel, CustomTrackersConfig,
    Matchers, SeriesIndexReader,
};
use mimir_tsdb::labels::Labels;
use prometheus::Registry;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

fn must_new_custom_trackers_config_from_map(source: &[(&str, &str)]) -> CustomTrackersConfig {
    let m: BTreeMap<String, String> = source
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    CustomTrackersConfig::new(m).expect("NewCustomTrackersConfig")
}

fn new_matchers(source: &[(&str, &str)]) -> Matchers {
    Matchers::new(must_new_custom_trackers_config_from_map(source))
}

fn from_strings(pairs: &[(&str, &str)]) -> Labels {
    Labels::from_pairs(pairs.iter().copied())
}

fn unix(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn now() -> SystemTime {
    SystemTime::now()
}

/// Verifies that the actual counts match expected counts by matcher name.
fn assert_matcher_counts(expected: &[(&str, usize)], actual: &[usize], names: &[String]) {
    let mut actual_map: HashMap<&str, usize> = HashMap::new();
    for (i, count) in actual.iter().enumerate() {
        if i < names.len() {
            actual_map.insert(names[i].as_str(), *count);
        }
    }
    for (name, expected_count) in expected {
        let actual_count = actual_map.get(name).copied().unwrap_or(0);
        assert_eq!(
            *expected_count, actual_count,
            "Matcher {name} count mismatch"
        );
    }
    for (name, actual_count) in &actual_map {
        if !expected.iter().any(|(n, _)| n == name) && *actual_count > 0 {
            panic!("Unexpected matcher {name} has count {actual_count}, expected 0");
        }
    }
}

struct MockIndex {
    existing_labels: HashMap<u64, Labels>,
}

impl MockIndex {
    fn new(existing: &[(u64, &Labels)]) -> Self {
        Self {
            existing_labels: existing.iter().map(|(r, l)| (*r, (*l).clone())).collect(),
        }
    }
}

impl SeriesIndexReader for MockIndex {
    fn series_labels(&self, ref_id: u64) -> Option<Labels> {
        self.existing_labels.get(&ref_id).cloned()
    }
}

fn idx_opt(idx: Option<&MockIndex>) -> Option<&dyn SeriesIndexReader> {
    idx.map(|i| i as &dyn SeriesIndexReader)
}

fn purge(c: &ActiveSeries, t: SystemTime, idx: Option<&MockIndex>) {
    c.purge_with_index(t, idx_opt(idx));
}

fn update(
    c: &ActiveSeries,
    ls: &Labels,
    r: u64,
    t: SystemTime,
    buckets: i64,
    otlp: bool,
    idx: Option<&MockIndex>,
) {
    c.update_series_labels(ls, r, t, buckets, otlp, idx_opt(idx));
}

fn assert_deleted_empty(c: &ActiveSeries) {
    // Make sure deleted is empty, so we're not leaking.
    assert_eq!(
        c.deleted_counts(),
        (0, 0),
        "deleted.refs and deleted.keys must be empty"
    );
}

/// Asserts both `ActiveWithMatchers()` (total/otlp/hist/buckets) and `Active()`.
fn assert_totals(c: &ActiveSeries, total: usize, otlp: usize, hist: usize, buckets: usize) {
    let (a, _, o, h, _, b, _) = c.active_with_matchers();
    assert_eq!(total, a);
    assert_eq!(otlp, o);
    assert_eq!(hist, h);
    assert_eq!(buckets, b);
    let (a, o, h, b) = c.active();
    assert_eq!(total, a);
    assert_eq!(otlp, o);
    assert_eq!(hist, h);
    assert_eq!(buckets, b);
}

fn labels_with_hash_collision() -> (Labels, Labels) {
    // These two series have the same XXHash; thanks to https://github.com/pstibrany/labels_hash_collisions
    let mut ls1 = from_strings(&[("__name__", "metric"), ("lbl1", "value"), ("lbl2", "l6CQ5y")]);
    let mut ls2 = from_strings(&[("__name__", "metric"), ("lbl1", "value"), ("lbl2", "v7uDlF")]);

    if ls1.hash() != ls2.hash() {
        // These ones are the same when using -tags stringlabels
        ls1 = from_strings(&[("__name__", "metric"), ("lbl", "HFnEaGl")]);
        ls2 = from_strings(&[("__name__", "metric"), ("lbl", "RqcXatm")]);
    }

    assert_eq!(
        ls1.hash(),
        ls2.hash(),
        "This code needs to be updated: find new labels with colliding hash values."
    );
    (ls1, ls2)
}

// ---------------------------------------------------------------------------
// GatherAndCompare equivalent (client_golang testutil.GatherAndCompare).
// ---------------------------------------------------------------------------

type Family = (String, String, BTreeMap<String, f64>); // (help, type, labels->value)

fn gather_families(reg: &Registry, names: &[&str]) -> BTreeMap<String, Family> {
    let mut out = BTreeMap::new();
    for mf in reg.gather() {
        if !names.contains(&mf.get_name()) {
            continue;
        }
        let mut samples = BTreeMap::new();
        for m in mf.get_metric() {
            let mut lbls: Vec<(&str, &str)> = m
                .get_label()
                .iter()
                .map(|l| (l.get_name(), l.get_value()))
                .collect();
            lbls.sort();
            let key = lbls
                .iter()
                .map(|(k, v)| format!("{k}=\"{v}\""))
                .collect::<Vec<_>>()
                .join(",");
            let v = match mf.get_field_type() {
                prometheus::proto::MetricType::GAUGE => m.get_gauge().get_value(),
                prometheus::proto::MetricType::COUNTER => m.get_counter().get_value(),
                other => panic!("unsupported metric type {other:?}"),
            };
            samples.insert(key, v);
        }
        if samples.is_empty() {
            continue;
        }
        let ty = format!("{:?}", mf.get_field_type()).to_lowercase();
        out.insert(
            mf.get_name().to_string(),
            (mf.get_help().to_string(), ty, samples),
        );
    }
    out
}

fn parse_expected(expected: &str, names: &[&str]) -> BTreeMap<String, Family> {
    let mut out: BTreeMap<String, Family> = BTreeMap::new();
    for line in expected.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest.split_once(' ').expect("HELP line");
            out.entry(name.to_string()).or_default().0 = help.to_string();
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, ty) = rest.split_once(' ').expect("TYPE line");
            out.entry(name.to_string()).or_default().1 = ty.to_string();
        } else {
            let (series, value) = line.rsplit_once(' ').expect("sample line");
            let (name, labels) = match series.split_once('{') {
                Some((n, l)) => (n, l.trim_end_matches('}')),
                None => (series, ""),
            };
            out.entry(name.to_string())
                .or_default()
                .2
                .insert(labels.to_string(), value.parse().expect("value"));
        }
    }
    out.retain(|k, _| names.contains(&k.as_str()));
    out
}

fn gather_and_compare(reg: &Registry, expected: &str, names: &[&str]) {
    let got = gather_families(reg, names);
    let want = parse_expected(expected, names);
    assert_eq!(want, got, "metrics mismatch");
}

const ALL_CAT_METRICS: [&str; 3] = [
    "cortex_ingester_attributed_active_series",
    "cortex_ingester_attributed_active_native_histogram_series",
    "cortex_ingester_attributed_active_native_histogram_buckets",
];
const CAT_SERIES: [&str; 1] = ["cortex_ingester_attributed_active_series"];

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_UpdateSeries_NoMatchers
#[test]
fn active_series_update_series_no_matchers() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let (ref3, ls3) = (3u64, from_strings(&[("a", "3")]));
    let (ref4, ls4) = (4u64, from_strings(&[("a", "4")]));
    let ref5 = 5u64; // will be used for ls1 again.
    let (ref6, ls6) = (6u64, from_strings(&[("a", "6")]));

    let c = ActiveSeries::with_matchers(Matchers::default(), DEFAULT_TIMEOUT, None);
    purge(&c, now(), None);
    let (all_active, active_matching, all_otlp, all_hist, active_matching_hist, all_buckets, active_matching_buckets) =
        c.active_with_matchers();
    assert_eq!(0, all_active);
    assert_eq!(0, all_otlp);
    assert!(active_matching.is_empty());
    assert_eq!(0, all_hist);
    assert!(active_matching_hist.is_empty());
    assert_eq!(0, all_buckets);
    assert!(active_matching_buckets.is_empty());

    update(&c, &ls1, ref1, now(), -1, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 1, 0, 0, 0);

    update(&c, &ls1, ref1, now(), -1, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 1, 0, 0, 0);

    update(&c, &ls2, ref2, now(), -1, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 2, 0, 0, 0);

    update(&c, &ls3, ref3, now(), 5, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 3, 0, 1, 5);

    update(&c, &ls4, ref4, now(), 3, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 4, 0, 2, 8);

    // more buckets for a histogram
    update(&c, &ls3, ref3, now(), 7, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 4, 0, 2, 10);

    // changing a metric from histogram to float
    update(&c, &ls4, ref4, now(), -1, false, None);
    purge(&c, now(), None);
    assert_totals(&c, 4, 0, 1, 7);

    // ref1 was deleted from head, but still active.
    c.post_deletion(&[(ref1, ls1.clone())]);
    assert_totals(&c, 4, 0, 1, 7);

    // Doesn't change after purging.
    purge(&c, now(), None);
    assert_totals(&c, 4, 0, 1, 7);

    // ref5 is created with the same labelset as ls1, it shouldn't be accounted as different series.
    update(&c, &ls1, ref5, now(), -1, false, None);
    assert_totals(&c, 4, 0, 1, 7);

    // Add a series with OTLP source
    update(&c, &ls6, ref6, now(), -1, true, None);
    assert_totals(&c, 5, 1, 1, 7);

    // Doesn't change after purging.
    purge(&c, now(), None);
    assert_totals(&c, 5, 1, 1, 7);

    assert_deleted_empty(&c);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ContainsRef
#[test]
fn active_series_contains_ref() {
    let (collision1, collision2) = labels_with_hash_collision();
    let series = [
        from_strings(&[("a", "1")]),
        from_strings(&[("a", "2")]),
        collision1,
        collision2,
    ];
    let refs = [1u64, 2, 3, 4];

    // Run the same test for increasing TTL values
    for ttl in 1..=series.len() {
        let mocked_time = unix(ttl as u64);
        let c = ActiveSeries::with_matchers(Matchers::default(), DEFAULT_TIMEOUT, None);

        // Update each series with a different timestamp according to each index
        for i in 0..series.len() {
            update(&c, &series[i], refs[i], unix(i as u64), -1, false, None);
        }

        c.purge_at(unix(ttl as u64), None);

        // The expected number of series is the total number of series minus the ttl
        // because the first ttl series should be purged
        let exp = series.len() - ttl;
        purge(&c, mocked_time, None);
        let (all_active, _, all_otlp, ..) = c.active_with_matchers();
        assert_eq!(exp, all_active, "ttl: {ttl}");
        assert_eq!(0, all_otlp, "ttl: {ttl}");

        for (i, r) in refs.iter().enumerate() {
            assert_eq!(i >= ttl, c.contains_ref(*r), "ttl: {ttl}, i: {i}");
        }
    }
}

fn new_user5_cat_tracker(reg: &Registry) -> Arc<CostAttributionActiveTracker> {
    // user5 attributes cost by the "a" label (MaxCostAttributionCardinality: 10).
    CostAttributionActiveTracker::new(
        "user5",
        "cost-attribution",
        vec![CostAttributionLabel {
            input: "a".to_string(),
            output: "a".to_string(),
        }],
        10,
        Duration::ZERO,
        Some(reg.clone()),
    )
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_UpdateSeries_WithCostAttribution
#[test]
fn active_series_update_series_with_cost_attribution() {
    let reg = Registry::new();
    let cat = new_user5_cat_tracker(&reg);
    let c = ActiveSeries::with_matchers(Matchers::default(), DEFAULT_TIMEOUT, Some(cat));
    test_cost_attribution_update_series(&c, &reg);
}

fn test_cost_attribution_update_series(c: &ActiveSeries, reg: &Registry) {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let (ref3, ls3) = (3u64, from_strings(&[("a", "3")]));
    let (ref4, ls4) = (4u64, from_strings(&[("a", "4")]));
    let (ref5, ls5) = (5u64, from_strings(&[("a", "5")]));
    let ref6 = 6u64; // same as ls2
    let (ref7, ls7) = (7u64, from_strings(&[("a", "2"), ("b", "1")]));
    let idx = MockIndex::new(&[
        (ref1, &ls1),
        (ref2, &ls2),
        (ref3, &ls3),
        (ref4, &ls4),
        (ref5, &ls5),
        (ref7, &ls7),
    ]);
    let idx = Some(&idx);
    purge(c, now(), idx);
    gather_and_compare(reg, "", &CAT_SERIES);

    update(c, &ls1, ref1, now(), 3, false, idx);
    purge(c, now(), idx);
    let mut expected_metrics = r#"
	# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
    cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
    cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    update(c, &ls2, ref2, now(), -1, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
    cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
    cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    update(c, &ls3, ref3, now(), -1, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
    cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
    cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    // ref7 has the same cost attribution labels as ref2, but it's a different series.
    update(c, &ls7, ref7, now(), -1, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
    cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
    cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 2
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    update(c, &ls4, ref4, now(), 3, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 2
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &CAT_SERIES);

    update(c, &ls5, ref5, now(), 5, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
	cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
	cortex_ingester_attributed_active_native_histogram_buckets{a="4",tenant="user5",tracker="cost-attribution"} 3
	cortex_ingester_attributed_active_native_histogram_buckets{a="5",tenant="user5",tracker="cost-attribution"} 5
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
	cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="5",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 2
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="5",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    // changing a metric from float to histogram
    update(c, &ls3, ref3, now(), 6, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
	cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
	cortex_ingester_attributed_active_native_histogram_buckets{a="3",tenant="user5",tracker="cost-attribution"} 6
	cortex_ingester_attributed_active_native_histogram_buckets{a="4",tenant="user5",tracker="cost-attribution"} 3
	cortex_ingester_attributed_active_native_histogram_buckets{a="5",tenant="user5",tracker="cost-attribution"} 5
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
	cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="5",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 2
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="5",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    // fewer (zero) buckets for a histogram
    update(c, &ls4, ref4, now(), 0, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
	cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
	cortex_ingester_attributed_active_native_histogram_buckets{a="3",tenant="user5",tracker="cost-attribution"} 6
	cortex_ingester_attributed_active_native_histogram_buckets{a="5",tenant="user5",tracker="cost-attribution"} 5
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
	cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="5",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 2
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="5",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    // changing from histogram to float
    update(c, &ls3, ref3, now(), -1, false, idx);
    purge(c, now(), idx);
    expected_metrics = r#"
    # HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
	cortex_ingester_attributed_active_native_histogram_buckets{a="1",tenant="user5",tracker="cost-attribution"} 3
	cortex_ingester_attributed_active_native_histogram_buckets{a="5",tenant="user5",tracker="cost-attribution"} 5
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
	cortex_ingester_attributed_active_native_histogram_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_native_histogram_series{a="5",tenant="user5",tracker="cost-attribution"} 1
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{a="1",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="2",tenant="user5",tracker="cost-attribution"} 2
	cortex_ingester_attributed_active_series{a="3",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="4",tenant="user5",tracker="cost-attribution"} 1
	cortex_ingester_attributed_active_series{a="5",tenant="user5",tracker="cost-attribution"} 1
	"#
    .to_string();
    gather_and_compare(reg, &expected_metrics, &ALL_CAT_METRICS);

    // ref2 is deleted from the head, but still active.
    c.post_deletion(&[(ref2, ls2.clone())]);
    // Numbers don't change.
    gather_and_compare(reg, &expected_metrics, &CAT_SERIES);

    // Don't change after purging.
    purge(c, now(), idx);
    gather_and_compare(reg, &expected_metrics, &CAT_SERIES);

    // ls2 is pushed again, this time with ref6
    update(c, &ls2, ref6, now(), -1, false, idx);
    // Numbers don't change.
    gather_and_compare(reg, &expected_metrics, &CAT_SERIES);

    // Don't change after purging.
    purge(c, now(), idx);
    gather_and_compare(reg, &expected_metrics, &CAT_SERIES);

    // Make sure deleted is empty, so we're not leaking.
    assert_deleted_empty(c);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_UpdateSeries_WithMatchers
#[test]
fn active_series_update_series_with_matchers() {
    let asm = new_matchers(&[("foo", r#"{a=~"2|3|4"}"#)]);
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);
    test_update_series(&c);
}

/// Checks `ActiveWithMatchers()` for a single matcher "foo" and `Active()`.
#[allow(clippy::too_many_arguments)]
fn assert_foo(
    c: &ActiveSeries,
    total: usize,
    foo_count: usize,
    hist: usize,
    foo_hist: usize,
    buckets: usize,
    foo_buckets: usize,
) {
    let names = c.current_matcher_names();
    let (a, matching, o, h, matching_hist, b, matching_buckets) = c.active_with_matchers();
    assert_eq!(total, a);
    assert_eq!(0, o);
    assert_matcher_counts(&[("foo", foo_count)], &matching, &names);
    assert_eq!(hist, h);
    assert_matcher_counts(&[("foo", foo_hist)], &matching_hist, &names);
    assert_eq!(buckets, b);
    assert_matcher_counts(&[("foo", foo_buckets)], &matching_buckets, &names);
    let (a, o, h, b) = c.active();
    assert_eq!(total, a);
    assert_eq!(0, o);
    assert_eq!(hist, h);
    assert_eq!(buckets, b);
}

fn test_update_series(c: &ActiveSeries) {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let (ref3, ls3) = (3u64, from_strings(&[("a", "3")]));
    let (ref4, ls4) = (4u64, from_strings(&[("a", "4")]));
    let (ref5, ls5) = (5u64, from_strings(&[("a", "5")]));
    let ref6 = 6u64; // same as ls2

    purge(c, now(), None);
    assert_foo(c, 0, 0, 0, 0, 0, 0);

    update(c, &ls1, ref1, now(), -1, false, None);
    purge(c, now(), None);
    assert_foo(c, 1, 0, 0, 0, 0, 0);

    update(c, &ls2, ref2, now(), -1, false, None);
    purge(c, now(), None);
    assert_foo(c, 2, 1, 0, 0, 0, 0);

    update(c, &ls3, ref3, now(), -1, false, None);
    purge(c, now(), None);
    assert_foo(c, 3, 2, 0, 0, 0, 0);

    update(c, &ls3, ref3, now(), -1, false, None);
    purge(c, now(), None);
    assert_foo(c, 3, 2, 0, 0, 0, 0);

    update(c, &ls4, ref4, now(), 3, false, None);
    purge(c, now(), None);
    assert_foo(c, 4, 3, 1, 1, 3, 3);

    update(c, &ls5, ref5, now(), 5, false, None);
    purge(c, now(), None);
    assert_foo(c, 5, 3, 2, 1, 8, 3);

    // changing a metric from float to histogram
    update(c, &ls3, ref3, now(), 6, false, None);
    purge(c, now(), None);
    assert_foo(c, 5, 3, 3, 2, 14, 9);

    // fewer (zero) buckets for a histogram
    update(c, &ls4, ref4, now(), 0, false, None);
    purge(c, now(), None);
    assert_foo(c, 5, 3, 3, 2, 11, 6);

    // ref2 is deleted from the head, but still active.
    c.post_deletion(&[(ref2, ls2.clone())]);
    // Numbers don't change.
    assert_foo(c, 5, 3, 3, 2, 11, 6);

    // Don't change after purging.
    purge(c, now(), None);
    assert_foo(c, 5, 3, 3, 2, 11, 6);

    // ls2 is pushed again, this time with ref6
    update(c, &ls2, ref6, now(), -1, false, None);
    // Numbers don't change.
    assert_foo(c, 5, 3, 3, 2, 11, 6);

    // Don't change after purging.
    purge(c, now(), None);
    assert_foo(c, 5, 3, 3, 2, 11, 6);

    // Make sure deleted is empty, so we're not leaking.
    assert_deleted_empty(c);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_UpdateSeries_Clear
#[test]
fn active_series_update_series_clear() {
    let asm = new_matchers(&[("foo", r#"{a=~"2|3|4"}"#)]);
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);
    test_update_series(&c);

    c.clear();
    let names = c.current_matcher_names();
    let (all_active, active_matching, all_otlp, all_hist, matching_hist, all_buckets, matching_buckets) =
        c.active_with_matchers();
    assert_eq!(0, all_active);
    assert_eq!(0, all_otlp);
    assert_matcher_counts(&[], &active_matching, &names);
    assert_eq!(0, all_hist);
    assert_matcher_counts(&[("foo", 0)], &matching_hist, &names);
    assert_eq!(0, all_buckets);
    assert_matcher_counts(&[("foo", 0)], &matching_buckets, &names);
    let (a, o, h, b) = c.active();
    assert_eq!(0, a);
    assert_eq!(0, o);
    assert_eq!(0, h);
    assert_eq!(0, b);

    test_update_series(&c);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ShouldCorrectlyHandleHashCollisions
#[test]
fn active_series_should_correctly_handle_hash_collisions() {
    let (ls1, ls2) = labels_with_hash_collision();
    let (ref1, ref2) = (1u64, 2u64);
    let c = ActiveSeries::with_matchers(Matchers::default(), DEFAULT_TIMEOUT, None);
    update(&c, &ls1, ref1, now(), -1, false, None);
    update(&c, &ls2, ref2, now(), -1, false, None);

    purge(&c, now(), None);
    let (all_active, ..) = c.active_with_matchers();
    assert_eq!(2, all_active);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_Purge_NoMatchers
#[test]
fn active_series_purge_no_matchers() {
    let (collision1, collision2) = labels_with_hash_collision();
    let deleted_labels = from_strings(&[("deleted", "true")]);
    let series = [
        deleted_labels.clone(),
        from_strings(&[("a", "1")]),
        from_strings(&[("a", "2")]),
        collision1,
        collision2,
    ];

    const DELETED_REF: u64 = 1;
    let refs = [1u64, 2, 3, 4, 5];

    // Run the same test for increasing TTL values
    for ttl in 1..=series.len() {
        let mocked_time = unix(ttl as u64);
        let c = ActiveSeries::with_matchers(Matchers::default(), DEFAULT_TIMEOUT, None);

        for i in 0..series.len() {
            update(&c, &series[i], refs[i], unix(i as u64), -1, false, None);
        }
        c.post_deletion(&[(DELETED_REF, deleted_labels.clone())]);

        c.purge_at(unix(ttl as u64), None);
        // call purge twice, just to hit "quick" path. It doesn't really do anything.
        c.purge_at(unix(ttl as u64), None);

        let exp = series.len() - ttl;
        // Purge is not intended to purge
        purge(&c, mocked_time, None);
        let (all_active, _, all_otlp, ..) = c.active_with_matchers();
        assert_eq!(exp, all_active, "ttl: {ttl}");
        assert_eq!(0, all_otlp, "ttl: {ttl}");

        // Deleted series is the first one so it should be always deleted and we should see empty deleted refs & keys.
        assert_deleted_empty(&c);
    }
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_Purge_WithMatchers
#[test]
fn active_series_purge_with_matchers() {
    let (collision1, collision2) = labels_with_hash_collision();
    let series = [
        from_strings(&[("a", "1")]),
        from_strings(&[("a", "2")]),
        collision1,
        collision2,
    ];
    let refs = [1u64, 2, 3, 4];

    let asm = new_matchers(&[("foo", r#"{_=~"y.*"}"#)]);

    // Run the same test for increasing TTL values
    for ttl in 1..=series.len() {
        let mocked_time = unix(ttl as u64);

        let c = ActiveSeries::with_matchers(asm.clone(), Duration::from_secs(5 * 60), None);

        let exp = series.len() - ttl;
        let mut exp_matching_series = 0;

        for (i, s) in series.iter().enumerate() {
            update(&c, &series[i], refs[i], unix(i as u64), -1, false, None);

            // if this series is matching, and they're within the ttl
            let tmp = asm.matches(s);
            if !tmp.is_empty() && i >= ttl {
                exp_matching_series += 1;
            }
        }

        c.purge_at(unix(ttl as u64), None);
        // call purge twice, just to hit "quick" path. It doesn't really do anything.
        c.purge_at(unix(ttl as u64), None);

        purge(&c, mocked_time, None);
        let (all_active, active_matching, ..) = c.active_with_matchers();
        assert_eq!(exp, all_active, "ttl={ttl}");
        // For this dynamic test, we use the count directly since there's only one matcher "foo"
        let names = c.current_matcher_names();
        if !names.is_empty() && !active_matching.is_empty() {
            assert_matcher_counts(
                &[(names[0].as_str(), exp_matching_series)],
                &active_matching,
                &names,
            );
        } else {
            assert_matcher_counts(&[], &active_matching, &names);
        }
    }
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_PurgeOpt
#[test]
fn active_series_purge_opt() {
    let (ls1, ls2) = labels_with_hash_collision();
    let (ref1, ref2) = (1u64, 2u64);

    let current_time = now();
    let c = ActiveSeries::with_matchers(Matchers::default(), Duration::from_secs(59), None);

    update(&c, &ls1, ref1, current_time - Duration::from_secs(120), -1, false, None);
    update(&c, &ls2, ref2, current_time, -1, false, None);

    purge(&c, current_time, None);
    let (all_active, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);

    update(&c, &ls1, ref1, current_time - Duration::from_secs(60), -1, false, None);
    update(&c, &ls2, ref2, current_time, -1, false, None);

    purge(&c, current_time, None);
    let (all_active, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);

    // This will *not* update the series, since there is already newer timestamp.
    update(&c, &ls2, ref2, current_time - Duration::from_secs(60), -1, false, None);

    purge(&c, current_time, None);
    let (all_active, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);
}

fn cat_for_tests(
    user: &str,
    labels: &[&str],
    max_cardinality: usize,
) -> Arc<CostAttributionActiveTracker> {
    CostAttributionActiveTracker::new(
        user,
        "cost-attribution",
        labels
            .iter()
            .map(|l| CostAttributionLabel {
                input: (*l).to_string(),
                output: String::new(),
            })
            .collect(),
        max_cardinality,
        Duration::from_secs(5 * 60),
        None,
    )
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadCostAttributionTrackers
#[test]
fn active_series_reload_cost_attribution_trackers() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let idx = MockIndex::new(&[(ref1, &ls1), (ref2, &ls2)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("foo", "{a=~.*}")]);
    let current_time = now();
    let c = ActiveSeries::with_matchers(asm.clone(), DEFAULT_TIMEOUT, None);
    purge(&c, current_time, None);

    update(&c, &ls1, ref1, current_time, -1, false, None);
    update(&c, &ls2, ref2, current_time, -1, false, None);

    // Change the cost attribution tracker, series should be preserved, purge result valid.
    let cat = cat_for_tests("a", &["a", "b"], 4);
    c.reload_series_config(asm.clone(), Some(cat), false, true, idx_opt(idx));
    purge(&c, current_time, idx);
    assert!(c.cost_attribution_tracker().is_some());
    let (all_active, ..) = c.active_with_matchers();
    assert_eq!(2, all_active);

    let cat = cat_for_tests("a", &["a"], 4);
    c.reload_series_config(asm, Some(Arc::clone(&cat)), false, true, idx_opt(idx));
    purge(&c, current_time, idx);
    assert!(cat.equals(c.cost_attribution_tracker().as_deref()));
    let (all_active, ..) = c.active_with_matchers();
    assert_eq!(2, all_active);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadSeriesMatchersAndTrackers
#[test]
fn active_series_reload_series_matchers_and_trackers() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let (ref3, ls3) = (3u64, from_strings(&[("a", "3")]));
    let (ref4, ls4) = (4u64, from_strings(&[("a", "4")]));
    let idx = MockIndex::new(&[(ref1, &ls1), (ref2, &ls2), (ref3, &ls3), (ref4, &ls4)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("foo", "{a=~.*}")]);

    let current_time = now();
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);

    purge(&c, current_time, None);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(0, all_active);
    assert_matcher_counts(&[], &active_matching, &c.current_matcher_names());

    update(&c, &ls1, ref1, current_time, -1, false, None);
    purge(&c, current_time, None);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);
    assert_matcher_counts(&[("foo", 1)], &active_matching, &c.current_matcher_names());

    // Reload same matchers (no cat change): no-op since neither changed.
    update(&c, &ls1, ref1, current_time, -1, false, None);
    update(&c, &ls2, ref2, current_time, -1, false, None);
    purge(&c, current_time, idx);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(2, all_active);
    assert_matcher_counts(&[("foo", 2)], &active_matching, &c.current_matcher_names());

    // Reload with fewer matchers: series preserved, matcher counts re-evaluated on purge.
    let asm_with_less_matchers = new_matchers(&[]);
    c.reload_series_config(asm_with_less_matchers, None, true, false, idx_opt(idx));
    purge(&c, current_time, idx);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(2, all_active);
    assert!(active_matching.is_empty());

    // Reload with more matchers: series preserved, matcher counts re-evaluated.
    update(&c, &ls3, ref3, current_time, -1, false, None);
    update(&c, &ls4, ref4, current_time, -1, false, None);
    let asm_with_more_matchers = new_matchers(&[("a", r#"{a="3"}"#), ("b", r#"{a="4"}"#)]);
    c.reload_series_config(asm_with_more_matchers, None, true, false, idx_opt(idx));
    purge(&c, current_time, idx);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(4, all_active);
    assert_matcher_counts(
        &[("a", 1), ("b", 1)],
        &active_matching,
        &c.current_matcher_names(),
    );
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadSeriesMatchers_LessMatchers
#[test]
fn active_series_reload_series_matchers_less_matchers() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let idx = MockIndex::new(&[(ref1, &ls1)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("foo", "{a=~.+}"), ("bar", "{a=~.+}")]);

    let cat = cat_for_tests("a", &["a", "b"], 4);
    let current_time = now();
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, Some(Arc::clone(&cat)));
    purge(&c, current_time, None);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(0, all_active);
    assert_matcher_counts(
        &[("foo", 0), ("bar", 0)],
        &active_matching,
        &c.current_matcher_names(),
    );
    assert!(cat.equals(c.cost_attribution_tracker().as_deref()));

    update(&c, &ls1, ref1, current_time, -1, false, None);
    purge(&c, current_time, None);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);
    assert_matcher_counts(
        &[("foo", 1), ("bar", 1)],
        &active_matching,
        &c.current_matcher_names(),
    );
    assert!(cat.equals(c.cost_attribution_tracker().as_deref()));

    // Reload with fewer matchers and remove cat: series preserved, matchers re-evaluated.
    let asm = new_matchers(&[("foo", "{a=~.+}")]);
    c.reload_series_config(asm, None, true, true, idx_opt(idx));
    purge(&c, current_time, idx);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);
    assert_matcher_counts(&[("foo", 1)], &active_matching, &c.current_matcher_names());
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadSeriesMatchers_SameSizeNewLabels
#[test]
fn active_series_reload_series_matchers_same_size_new_labels() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let idx = MockIndex::new(&[(ref1, &ls1)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("foo", "{a=~.+}"), ("bar", "{a=~.+}")]);

    let current_time = now();
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);
    purge(&c, current_time, None);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(0, all_active);
    assert_matcher_counts(
        &[("foo", 0), ("bar", 0)],
        &active_matching,
        &c.current_matcher_names(),
    );

    update(&c, &ls1, ref1, current_time, -1, false, None);
    purge(&c, current_time, None);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);
    assert_matcher_counts(
        &[("foo", 1), ("bar", 1)],
        &active_matching,
        &c.current_matcher_names(),
    );

    // Reload with different matchers that don't match existing series: series preserved, no matcher matches.
    let asm = new_matchers(&[("foo", "{b=~.+}"), ("bar", "{b=~.+}")]);
    c.reload_series_config(asm, None, true, false, idx_opt(idx));
    purge(&c, current_time, idx);
    let (all_active, active_matching, ..) = c.active_with_matchers();
    assert_eq!(1, all_active);
    assert_matcher_counts(
        &[("foo", 0), ("bar", 0)],
        &active_matching,
        &c.current_matcher_names(),
    );
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadMatchersOnly_PreservesTotalCount
#[test]
fn active_series_reload_matchers_only_preserves_total_count() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1"), ("team", "x")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2"), ("team", "y")]));
    let (ref3, ls3) = (3u64, from_strings(&[("a", "3"), ("team", "x")]));
    let idx = MockIndex::new(&[(ref1, &ls1), (ref2, &ls2), (ref3, &ls3)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("team_x", r#"{team="x"}"#)]);
    let current_time = now();
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);

    update(&c, &ls1, ref1, current_time, -1, false, None);
    update(&c, &ls2, ref2, current_time, -1, false, None);
    update(&c, &ls3, ref3, current_time, 5, false, None);
    purge(&c, current_time, idx);

    let (total, matching, _, histograms, matching_hist, buckets, matching_buckets) =
        c.active_with_matchers();
    assert_eq!(3, total);
    assert_eq!(1, histograms);
    assert_eq!(5, buckets);
    let names = c.current_matcher_names();
    assert_matcher_counts(&[("team_x", 2)], &matching, &names);
    assert_matcher_counts(&[("team_x", 1)], &matching_hist, &names);
    assert_matcher_counts(&[("team_x", 5)], &matching_buckets, &names);

    // Reload with different matchers.
    let asm2 = new_matchers(&[("team_y", r#"{team="y"}"#)]);
    c.reload_series_config(asm2, None, true, false, idx_opt(idx));
    purge(&c, current_time, idx);

    let (total, matching, _, histograms, matching_hist, buckets, matching_buckets) =
        c.active_with_matchers();
    assert_eq!(3, total);
    assert_eq!(1, histograms);
    assert_eq!(5, buckets);
    let names = c.current_matcher_names();
    assert_matcher_counts(&[("team_y", 1)], &matching, &names);
    assert_matcher_counts(&[("team_y", 0)], &matching_hist, &names);
    assert_matcher_counts(&[("team_y", 0)], &matching_buckets, &names);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadCatOnly_PreservesTotalCount
#[test]
fn active_series_reload_cat_only_preserves_total_count() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let idx = MockIndex::new(&[(ref1, &ls1), (ref2, &ls2)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("foo", "{a=~.+}")]);
    let current_time = now();

    let cat1 = cat_for_tests("u1", &["a"], 4);
    let c = ActiveSeries::with_matchers(asm.clone(), DEFAULT_TIMEOUT, Some(cat1));

    update(&c, &ls1, ref1, current_time, -1, false, idx);
    update(&c, &ls2, ref2, current_time, -1, false, idx);
    purge(&c, current_time, idx);

    let (total, matching, ..) = c.active_with_matchers();
    assert_eq!(2, total);
    assert_matcher_counts(&[("foo", 2)], &matching, &c.current_matcher_names());

    // Reload with new cat only.
    let cat2 = cat_for_tests("u1", &["a"], 4);
    c.reload_series_config(asm, Some(cat2), false, true, idx_opt(idx));
    purge(&c, current_time, idx);

    let (total, matching, ..) = c.active_with_matchers();
    assert_eq!(2, total);
    assert_matcher_counts(&[("foo", 2)], &matching, &c.current_matcher_names());
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadBothMatchersAndCat
#[test]
fn active_series_reload_both_matchers_and_cat() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1"), ("team", "x")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2"), ("team", "y")]));
    let idx = MockIndex::new(&[(ref1, &ls1), (ref2, &ls2)]);
    let idx = Some(&idx);

    let asm = new_matchers(&[("team_x", r#"{team="x"}"#)]);
    let cat1 = cat_for_tests("u1", &["a"], 4);
    let current_time = now();
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, Some(cat1));

    update(&c, &ls1, ref1, current_time, -1, false, idx);
    update(&c, &ls2, ref2, current_time, -1, false, idx);
    purge(&c, current_time, idx);

    let (total, matching, ..) = c.active_with_matchers();
    assert_eq!(2, total);
    assert_matcher_counts(&[("team_x", 1)], &matching, &c.current_matcher_names());

    // Reload both matchers and cat.
    let asm2 = new_matchers(&[("team_y", r#"{team="y"}"#)]);
    let cat2 = cat_for_tests("u1", &["team"], 4);
    c.reload_series_config(asm2, Some(cat2), true, true, idx_opt(idx));
    purge(&c, current_time, idx);

    let (total, matching, ..) = c.active_with_matchers();
    assert_eq!(2, total);
    assert_matcher_counts(&[("team_y", 1)], &matching, &c.current_matcher_names());
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadMatchersOnly_PurgeReturnsValid
#[test]
fn active_series_reload_matchers_only_purge_returns_valid() {
    let asm = new_matchers(&[("foo", "{a=~.+}")]);
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);

    let asm2 = new_matchers(&[("bar", "{b=~.+}")]);
    c.reload_series_config(asm2, None, true, false, None);

    purge(&c, now(), None);
}

// Upstream: pkg/ingester/activeseries/active_series_test.go::TestActiveSeries_ReloadMatchersOnly_BoundsCheckOnRemove
#[test]
fn active_series_reload_matchers_only_bounds_check_on_remove() {
    let (ref1, ls1) = (1u64, from_strings(&[("a", "1")]));
    let (ref2, ls2) = (2u64, from_strings(&[("a", "2")]));
    let idx = MockIndex::new(&[(ref1, &ls1), (ref2, &ls2)]);
    let idx = Some(&idx);

    // Start with 2 matchers.
    let asm = new_matchers(&[("m1", "{a=~.+}"), ("m2", "{a=~.+}")]);
    let current_time = now();
    let c = ActiveSeries::with_matchers(asm, DEFAULT_TIMEOUT, None);

    update(&c, &ls1, ref1, current_time, -1, false, None);
    update(&c, &ls2, ref2, current_time, -1, false, None);
    purge(&c, current_time, idx);

    let (total, ..) = c.active_with_matchers();
    assert_eq!(2, total);

    // Reload with 0 matchers: activeMatching slices are now empty, but entries still have old match indices.
    let asm2 = new_matchers(&[]);
    c.reload_series_config(asm2, None, true, false, idx_opt(idx));

    // Remove a series before purge re-evaluates matchers. This should not panic.
    c.delete_with_index(ref1, idx_opt(idx));

    purge(&c, current_time, idx);
    let (total, ..) = c.active_with_matchers();
    assert_eq!(1, total);
}
