//! Ported from github.com/grafana/mimir/pkg/costattribution/{manager_test.go,sample_tracker_test.go,
//! active_tracker_test.go,costattributionmodel/labels_test.go}

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_config::{
    CostAttributionLabel, CostAttributionTrackerConfig, LimitsConfig, ModelDuration, Overrides,
};
use mimir_ingester::costattribution::{
    cost_attribution_config, validate_label, validate_labels, Manager, TrackerKind,
    DEFAULT_TRACKER_NAME,
};
use mimir_tsdb::labels::Labels;
use prometheus::Registry;

const TEST_ATTRIBUTION_COOLDOWN: Duration = Duration::from_secs(20 * 60);

fn unix(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn ca_labels(pairs: &[(&str, &str)]) -> Vec<CostAttributionLabel> {
    pairs
        .iter()
        .map(|(i, o)| CostAttributionLabel {
            input: (*i).to_string(),
            output: (*o).to_string(),
        })
        .collect()
}

fn trackers(
    entries: &[(&str, Vec<CostAttributionLabel>, bool)],
) -> BTreeMap<String, CostAttributionTrackerConfig> {
    entries
        .iter()
        .map(|(n, l, internal)| {
            (
                (*n).to_string(),
                CostAttributionTrackerConfig {
                    labels: l.clone(),
                    internal: *internal,
                },
            )
        })
        .collect()
}

fn default_tracker(pairs: &[(&str, &str)]) -> BTreeMap<String, CostAttributionTrackerConfig> {
    trackers(&[(DEFAULT_TRACKER_NAME, ca_labels(pairs), false)])
}

fn limits(max_card: i64, base: BTreeMap<String, CostAttributionTrackerConfig>) -> LimitsConfig {
    LimitsConfig {
        max_cost_attribution_cardinality: max_card,
        cost_attribution_trackers: base,
        ..LimitsConfig::default()
    }
}

fn no_trackers_default() -> LimitsConfig {
    LimitsConfig {
        cost_attribution_trackers: BTreeMap::new(),
        additional_cost_attribution_trackers: BTreeMap::new(),
        ..LimitsConfig::default()
    }
}

fn overrides(tenants: Vec<(&str, LimitsConfig)>) -> Overrides {
    let map: HashMap<String, LimitsConfig> = tenants
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    Overrides::with_static_map(no_trackers_default(), map)
}

fn set_tenants(o: &Overrides, tenants: Vec<(&str, LimitsConfig)>) {
    o.clear_static_overrides();
    for (k, v) in tenants {
        o.set_static_override(k, Some(v));
    }
}

/// Go: newManagerWithLimits (cleanup 5s, inactive timeout 10s).
fn new_manager(
    tenants: Vec<(&str, LimitsConfig)>,
) -> (Arc<Manager>, Overrides, Registry, Registry) {
    let o = overrides(tenants);
    let reg = Registry::new();
    let ca_reg = Registry::new();
    let m = Manager::new(Duration::from_secs(10), o.clone(), Some(&reg), &ca_reg).unwrap();
    (m, o, reg, ca_reg)
}

fn lbls(pairs: &[(&str, &str)]) -> Labels {
    Labels::from_pairs(pairs.iter().copied())
}

/// Renders the gathered families named in `names` as a set of `name{sorted labels} value` lines plus
/// `# HELP` lines, mirroring Go's `testutil.GatherAndCompare` semantics.
fn gather(reg: &Registry, names: &[&str]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for mf in reg.gather() {
        if !names.contains(&mf.get_name()) || mf.get_metric().is_empty() {
            continue;
        }
        out.insert(format!("# HELP {} {}", mf.get_name(), mf.get_help()));
        for m in mf.get_metric() {
            let mut pairs: Vec<String> = m
                .get_label()
                .iter()
                .map(|l| format!("{}=\"{}\"", l.get_name(), l.get_value()))
                .collect();
            pairs.sort();
            let v = if m.has_counter() {
                m.get_counter().get_value()
            } else {
                m.get_gauge().get_value()
            };
            out.insert(format!("{}{{{}}} {}", mf.get_name(), pairs.join(","), v));
        }
    }
    out
}

fn expected(text: &str, names: &[&str]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if line.starts_with("# TYPE") {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = rest.split_whitespace().next().unwrap();
            if names.contains(&name) {
                out.insert(line.to_string());
            }
            continue;
        }
        let name = line.split('{').next().unwrap();
        if !names.contains(&name) {
            continue;
        }
        let (series, value) = line.rsplit_once(' ').unwrap();
        let open = series.find('{').unwrap();
        let inner = &series[open + 1..series.len() - 1];
        let mut pairs: Vec<&str> = inner.split(',').filter(|s| !s.is_empty()).collect();
        pairs.sort();
        let v: f64 = value.parse().unwrap();
        out.insert(format!("{}{{{}}} {}", name, pairs.join(","), v));
    }
    out
}

fn assert_metrics(reg: &Registry, text: &str, names: &[&str]) {
    assert_eq!(expected(text, names), gather(reg, names));
}

const CA_ALL: [&str; 6] = [
    "cortex_discarded_attributed_samples_total",
    "cortex_distributor_received_attributed_samples_total",
    "cortex_ingester_attributed_active_series",
    "cortex_ingester_attributed_active_native_histogram_series",
    "cortex_ingester_attributed_active_native_histogram_buckets",
    "cortex_attributed_series_overflow_labels",
];
const OPERATIONAL: [&str; 5] = [
    "cortex_cost_attribution_sample_tracker_cardinality",
    "cortex_cost_attribution_sample_tracker_overflown",
    "cortex_cost_attribution_active_series_tracker_cardinality",
    "cortex_cost_attribution_active_series_tracker_overflown",
    "cortex_cost_attribution_tracker_creation_errors_total",
];

const HELP_DISCARDED: &str = "# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.";
const HELP_RECEIVED: &str = "# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.";
const HELP_BUCKETS: &str = "# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.";
const HELP_NH: &str = "# HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.";
const HELP_AS: &str = "# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.";
const HELP_OVERFLOW: &str = "# HELP cortex_attributed_series_overflow_labels The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.";
const HELP_AS_CARD: &str = "# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.";
const HELP_S_CARD: &str = "# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.";
const HELP_S_OVERFLOWN: &str = "# HELP cortex_cost_attribution_sample_tracker_overflown This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.";
const HELP_ERRORS: &str = "# HELP cortex_cost_attribution_tracker_creation_errors_total The total number of errors creating cost attribution trackers for each user.";

fn user1() -> LimitsConfig {
    limits(5, default_tracker(&[("team", "my_team")]))
}
fn user3() -> LimitsConfig {
    limits(
        2,
        default_tracker(&[("department", "my_department"), ("service", "my_service")]),
    )
}

// Upstream: manager_test.go::TestManager_New
#[test]
fn manager_new() {
    let (m, ..) = new_manager(vec![]);
    assert_eq!(m.inactive_timeout(), Duration::from_secs(10));
}

// Upstream: manager_test.go::TestManager_CreateDeleteTracker/Tracker existence and attributes
#[test]
fn create_delete_tracker_existence_and_attributes() {
    let (m, ..) = new_manager(vec![
        ("user1", user1()),
        ("user2", limits(2, BTreeMap::new())),
        ("user3", user3()),
    ]);
    assert!(m.sample_tracker("user1").is_some());
    let st = m
        .debug_tracker(TrackerKind::Samples, "user1", DEFAULT_TRACKER_NAME)
        .unwrap();
    assert_eq!(st.labels, ca_labels(&[("team", "my_team")]));
    assert_eq!(st.max_cardinality, 5);
    assert!(m.sample_tracker("user2").is_none());
    assert!(m.active_series_tracker("user3").is_some());
    let at = m
        .debug_tracker(TrackerKind::ActiveSeries, "user3", DEFAULT_TRACKER_NAME)
        .unwrap();
    assert_eq!(
        at.labels,
        ca_labels(&[("department", "my_department"), ("service", "my_service")])
    );
    assert_eq!(at.max_cardinality, 2);
}

// Upstream: manager_test.go::TestManager_CreateDeleteTracker/Metrics tracking
#[test]
fn create_delete_tracker_metrics_tracking() {
    let (m, _, reg, ca_reg) = new_manager(vec![("user1", user1()), ("user3", user3())]);
    let st1 = m.sample_tracker("user1").unwrap();
    st1.increment_discarded_samples_pairs(&[("team", "bar")], 1.0, "invalid-metrics-name", unix(6));
    st1.increment_discarded_samples_pairs(
        &[("team", "foo")],
        1.0,
        "invalid-metrics-name",
        unix(12),
    );
    m.sample_tracker("user3")
        .unwrap()
        .increment_received_samples(
            &[(&[("department", "foo"), ("service", "dodo")][..], 1)],
            unix(20),
        );
    m.active_series_tracker("user3");
    let at1 = m.active_series_tracker("user1").unwrap();
    at1.increment(&lbls(&[("team", "bar")]), unix(10), 50);
    at1.increment(&lbls(&[("team", "bar")]), unix(10), -1);
    at1.increment(&lbls(&[("team", "bar")]), unix(10), 2);

    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        cortex_discarded_attributed_samples_total{{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_BUCKETS}
        cortex_ingester_attributed_active_native_histogram_buckets{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 52
        {HELP_NH}
        cortex_ingester_attributed_active_native_histogram_series{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 2
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 3
        {HELP_OVERFLOW}
        cortex_attributed_series_overflow_labels{{my_team="__overflow__",tenant="user1",tracker="cost-attribution"}} 1
        cortex_attributed_series_overflow_labels{{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL);

    at1.decrement(&lbls(&[("team", "bar")]), 50);
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        cortex_discarded_attributed_samples_total{{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_BUCKETS}
        cortex_ingester_attributed_active_native_histogram_buckets{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 2
        {HELP_NH}
        cortex_ingester_attributed_active_native_histogram_series{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 2
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL[..5]);

    at1.decrement(&lbls(&[("team", "bar")]), 2);
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        cortex_discarded_attributed_samples_total{{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL[..5]);

    let exp = format!(
        r#"
        {HELP_AS_CARD}
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user1"}} 1
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 0
        {HELP_S_CARD}
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="cost-attribution",user="user1"}} 2
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 1
        "#
    );
    assert_metrics(&reg, &exp, &OPERATIONAL);
}

// Upstream: manager_test.go::TestManager_CreateDeleteTracker/Purge inactive attributions, only received/discarded samples are purged
#[test]
fn create_delete_tracker_purge_only_samples() {
    let (m, _, _, ca_reg) = new_manager(vec![("user1", user1())]);
    let st = m.sample_tracker("user1").unwrap();
    st.increment_discarded_samples_pairs(&[("team", "bar")], 1.0, "invalid-metrics-name", unix(6));
    st.increment_discarded_samples_pairs(&[("team", "foo")], 1.0, "invalid-metrics-name", unix(12));
    m.active_series_tracker("user1")
        .unwrap()
        .increment(&lbls(&[("team", "bar")]), unix(10), -1);
    m.purge_inactive_attributions_until(unix(10) + m.inactive_timeout());
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_team="bar",tenant="user1",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(
        &ca_reg,
        &exp,
        &[
            "cortex_discarded_attributed_samples_total",
            "cortex_ingester_attributed_active_series",
        ],
    );
}

// Upstream: manager_test.go::TestManager_CreateDeleteTracker/Disabling user cost attribution
#[test]
fn create_delete_tracker_disabling_user() {
    let (m, o, _, ca_reg) = new_manager(vec![("user1", user1()), ("user3", user3())]);
    m.sample_tracker("user1")
        .unwrap()
        .increment_discarded_samples_pairs(
            &[("team", "foo")],
            1.0,
            "invalid-metrics-name",
            unix(12),
        );
    m.sample_tracker("user3")
        .unwrap()
        .increment_received_samples(
            &[(&[("department", "foo"), ("service", "dodo")][..], 1)],
            unix(20),
        );
    assert_eq!(m.debug_num_composites(TrackerKind::Samples), 2);
    set_tenants(
        &o,
        vec![("user1", limits(5, BTreeMap::new())), ("user3", user3())],
    );
    m.purge_inactive_attributions_until(unix(11) + m.inactive_timeout());
    assert_eq!(m.debug_num_composites(TrackerKind::Samples), 1);
    let exp = format!(
        r#"
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL[..3]);
}

// Upstream: manager_test.go::TestManager_CreateDeleteTracker/Updating user cardinality and labels
#[test]
fn create_delete_tracker_updating_labels() {
    let (m, o, _, ca_reg) = new_manager(vec![("user3", user3())]);
    m.sample_tracker("user3")
        .unwrap()
        .increment_received_samples(
            &[(&[("department", "foo"), ("service", "dodo")][..], 1)],
            unix(20),
        );
    set_tenants(
        &o,
        vec![(
            "user3",
            limits(
                2,
                default_tracker(&[("feature", "my_feature"), ("team", "my_team")]),
            ),
        )],
    );
    m.purge_inactive_attributions_until(unix(12) + m.inactive_timeout());
    m.sample_tracker("user3");
    assert_eq!(m.debug_num_composites(TrackerKind::Samples), 1);
    let want = ca_labels(&[("feature", "my_feature"), ("team", "my_team")]);
    assert_eq!(
        m.debug_tracker(TrackerKind::Samples, "user3", DEFAULT_TRACKER_NAME)
            .unwrap()
            .labels,
        want
    );
    m.active_series_tracker("user3");
    assert_eq!(
        m.debug_tracker(TrackerKind::ActiveSeries, "user3", DEFAULT_TRACKER_NAME)
            .unwrap()
            .labels,
        want
    );
    m.sample_tracker("user3")
        .unwrap()
        .increment_discarded_samples_pairs(
            &[("team", "foo")],
            1.0,
            "invalid-metrics-name",
            unix(13),
        );
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_feature="__missing__",my_team="foo",reason="invalid-metrics-name",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(
        &ca_reg,
        &exp,
        &["cortex_discarded_attributed_samples_total"],
    );
}

// Upstream: manager_test.go::TestManager_CreateDeleteTracker/Overflow metrics on cardinality limit
#[test]
fn create_delete_tracker_overflow_metrics() {
    let (m, _, reg, ca_reg) = new_manager(vec![(
        "user3",
        limits(
            2,
            default_tracker(&[("feature", "my_feature"), ("team", "my_team")]),
        ),
    )]);
    m.active_series_tracker("user3");
    let st = m.sample_tracker("user3").unwrap();
    st.increment_discarded_samples_pairs(&[("team", "foo")], 1.0, "invalid-metrics-name", unix(13));
    st.increment_received_samples(&[(&[("team", "bar"), ("feature", "bar")][..], 1)], unix(15));
    st.increment_received_samples(&[(&[("team", "baz"), ("feature", "baz")][..], 1)], unix(16));
    st.increment_received_samples(&[(&[("team", "foo"), ("feature", "foo")][..], 1)], unix(17));
    let exp = format!(
        r#"
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_feature="__overflow__",my_team="__overflow__",tenant="user3",tracker="cost-attribution"}} 2
        "#
    );
    assert_metrics(
        &ca_reg,
        &exp,
        &["cortex_distributor_received_attributed_samples_total"],
    );
    let exp = format!(
        r#"
        {HELP_AS_CARD}
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 0
        {HELP_S_CARD}
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 2
        {HELP_S_OVERFLOWN}
        cortex_cost_attribution_sample_tracker_overflown{{tracker="cost-attribution",user="user3"}} 1
        "#
    );
    assert_metrics(&reg, &exp, &OPERATIONAL);
}

// Upstream: manager_test.go::TestManager_PurgeInactiveAttributionsUntil
#[test]
fn purge_inactive_attributions_until() {
    let user7 = || LimitsConfig {
        cost_attribution_cooldown: ModelDuration(TEST_ATTRIBUTION_COOLDOWN.as_nanos() as i64),
        ..limits(2, default_tracker(&[("team", "my_team")]))
    };
    let (m, o, _, ca_reg) = new_manager(vec![
        ("user1", user1()),
        ("user3", user3()),
        ("user7", user7()),
    ]);
    let st1 = m.sample_tracker("user1").unwrap();
    st1.increment_received_samples(&[(&[("team", "foo")][..], 1)], unix(1));
    st1.increment_discarded_samples_pairs(&[("team", "foo")], 1.0, "invalid-metrics-name", unix(1));
    m.sample_tracker("user3")
        .unwrap()
        .increment_discarded_samples_pairs(
            &[("department", "foo"), ("service", "bar")],
            1.0,
            "out-of-window",
            unix(10),
        );

    // Purge before inactive timeout.
    m.purge_inactive_attributions_until(unix(0) + m.inactive_timeout());
    assert_eq!(m.debug_num_composites(TrackerKind::Samples), 2);
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"}} 1
        cortex_discarded_attributed_samples_total{{my_department="foo",my_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(
        &ca_reg,
        &exp,
        &["cortex_discarded_attributed_samples_total"],
    );

    // Purge after inactive timeout.
    set_tenants(
        &o,
        vec![
            ("user1", limits(5, BTreeMap::new())),
            ("user3", user3()),
            ("user7", user7()),
        ],
    );
    m.purge_inactive_attributions_until(unix(5) + m.inactive_timeout());
    assert_eq!(m.debug_num_composites(TrackerKind::Samples), 1);
    assert!(m.sample_tracker("user1").is_none());
    assert!(m.active_series_tracker("user1").is_none());
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_department="foo",my_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(
        &ca_reg,
        &exp,
        &["cortex_discarded_attributed_samples_total"],
    );

    // Purge all trackers.
    m.purge_inactive_attributions_until(unix(20) + m.inactive_timeout());
    assert_eq!(m.debug_num_composites(TrackerKind::Samples), 0);
    assert_metrics(
        &ca_reg,
        "",
        &[
            "cortex_discarded_attributed_samples_total",
            "cortex_distributor_received_attributed_samples_total",
        ],
    );

    // Overflown active series recover only after they are under the limit.
    let state = |m: &Manager| {
        m.active_series_tracker("user7");
        let d = m
            .debug_tracker(TrackerKind::ActiveSeries, "user7", DEFAULT_TRACKER_NAME)
            .unwrap();
        (d.observed, d.overflown, d.overflow_active_series)
    };
    let inc = |team: &str, now: SystemTime| {
        m.active_series_tracker("user7")
            .unwrap()
            .increment(&lbls(&[("team", team)]), now, 0)
    };
    let dec = |team: &str| {
        m.active_series_tracker("user7")
            .unwrap()
            .decrement(&lbls(&[("team", team)]), 0)
    };

    let mut now = unix(10);
    inc("1", now);
    inc("2", now);
    assert_eq!(state(&m), (2, false, 0));
    inc("3", now);
    assert_eq!(state(&m), (3, true, 0));
    inc("4", now);
    inc("5", now);
    inc("6", now);
    assert_eq!(state(&m), (4, true, 2));

    now += TEST_ATTRIBUTION_COOLDOWN;
    m.purge_inactive_attributions_until(now);
    assert_eq!(state(&m), (4, true, 2));

    now += TEST_ATTRIBUTION_COOLDOWN / 10;
    dec("1");
    dec("6");
    assert_eq!(state(&m), (3, true, 1));

    now += TEST_ATTRIBUTION_COOLDOWN;
    m.purge_inactive_attributions_until(now);
    assert_eq!(state(&m), (3, true, 1));

    now += TEST_ATTRIBUTION_COOLDOWN / 10;
    inc("7", now);
    assert_eq!(state(&m), (4, true, 1));

    now += TEST_ATTRIBUTION_COOLDOWN / 10;
    dec("3");
    dec("4");
    assert_eq!(state(&m), (2, true, 1));

    now += TEST_ATTRIBUTION_COOLDOWN;
    m.purge_inactive_attributions_until(now);
    assert_eq!(state(&m), (0, false, 0));
}

// Upstream: manager_test.go::TestManager_OutputLabels
#[test]
fn output_labels() {
    let (m, _, _, ca_reg) = new_manager(vec![(
        "user6",
        limits(5, default_tracker(&[("team", "eng_team")])),
    )]);
    let st = m.sample_tracker("user6").unwrap();
    st.increment_discarded_samples_pairs(&[("team", "bar")], 1.0, "invalid-metrics-name", unix(6));
    st.increment_discarded_samples_pairs(&[("team", "foo")], 1.0, "invalid-metrics-name", unix(12));
    st.increment_received_samples(
        &[(&[("team", "foo"), ("feature", "dodo")][..], 1)],
        unix(20),
    );
    let at = m.active_series_tracker("user6").unwrap();
    at.increment(&lbls(&[("team", "bar")]), unix(10), 50);
    at.increment(&lbls(&[("team", "bar")]), unix(10), -1);
    at.increment(&lbls(&[("team", "bar")]), unix(10), 2);
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{eng_team="bar",reason="invalid-metrics-name",tenant="user6",tracker="cost-attribution"}} 1
        cortex_discarded_attributed_samples_total{{eng_team="foo",reason="invalid-metrics-name",tenant="user6",tracker="cost-attribution"}} 1
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{eng_team="foo",tenant="user6",tracker="cost-attribution"}} 1
        {HELP_BUCKETS}
        cortex_ingester_attributed_active_native_histogram_buckets{{eng_team="bar",tenant="user6",tracker="cost-attribution"}} 52
        {HELP_NH}
        cortex_ingester_attributed_active_native_histogram_series{{eng_team="bar",tenant="user6",tracker="cost-attribution"}} 2
        {HELP_AS}
        cortex_ingester_attributed_active_series{{eng_team="bar",tenant="user6",tracker="cost-attribution"}} 3
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL[..5]);
}

fn user8() -> LimitsConfig {
    LimitsConfig {
        additional_cost_attribution_trackers: trackers(&[(
            "by-platform",
            ca_labels(&[("platform", "my_platform")]),
            false,
        )]),
        ..limits(5, default_tracker(&[("team", "my_team")]))
    }
}

fn user9() -> LimitsConfig {
    LimitsConfig {
        additional_cost_attribution_trackers: trackers(&[
            ("by-team", ca_labels(&[("team", "my_team")]), true),
            ("by-service", ca_labels(&[("service", "my_service")]), true),
        ]),
        ..limits(3, BTreeMap::new())
    }
}

// Upstream: manager_test.go::TestManager_MultipleTrackers
#[test]
fn multiple_trackers() {
    // Default plus additional tracker.
    let (m, ..) = new_manager(vec![("user8", user8())]);
    assert!(m.sample_tracker("user8").is_some());
    assert_eq!(m.debug_num_trackers(TrackerKind::Samples, "user8"), Some(2));
    assert!(m.active_series_tracker("user8").is_some());
    assert_eq!(
        m.debug_num_trackers(TrackerKind::ActiveSeries, "user8"),
        Some(2)
    );
    assert_eq!(
        m.debug_tracker(TrackerKind::Samples, "user8", DEFAULT_TRACKER_NAME)
            .unwrap()
            .labels,
        ca_labels(&[("team", "my_team")])
    );
    assert_eq!(
        m.debug_tracker(TrackerKind::Samples, "user8", "by-platform")
            .unwrap()
            .labels,
        ca_labels(&[("platform", "my_platform")])
    );

    // Only additional trackers.
    let (m, ..) = new_manager(vec![("user9", user9())]);
    assert!(m.sample_tracker("user9").is_some());
    assert_eq!(m.debug_num_trackers(TrackerKind::Samples, "user9"), Some(2));
    assert!(m
        .debug_tracker(TrackerKind::Samples, "user9", DEFAULT_TRACKER_NAME)
        .is_none());
    assert!(m
        .debug_tracker(TrackerKind::Samples, "user9", "by-team")
        .is_some());
    assert!(m
        .debug_tracker(TrackerKind::Samples, "user9", "by-service")
        .is_some());

    // Metrics fan out to all trackers.
    let (m, _, _, ca_reg) = new_manager(vec![("user8", user8())]);
    let st = m.sample_tracker("user8").unwrap();
    st.increment_received_samples(
        &[(&[("team", "backend"), ("platform", "k8s")][..], 5)],
        unix(10),
    );
    st.increment_discarded_samples_pairs(
        &[("team", "backend"), ("platform", "k8s")],
        2.0,
        "sample-out-of-order",
        unix(11),
    );
    let exp = format!(
        r#"
        {HELP_DISCARDED}
        cortex_discarded_attributed_samples_total{{my_platform="k8s",reason="sample-out-of-order",tenant="user8",tracker="by-platform"}} 2
        cortex_discarded_attributed_samples_total{{my_team="backend",reason="sample-out-of-order",tenant="user8",tracker="cost-attribution"}} 2
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_platform="k8s",tenant="user8",tracker="by-platform"}} 5
        cortex_distributor_received_attributed_samples_total{{my_team="backend",tenant="user8",tracker="cost-attribution"}} 5
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL[..2]);

    // Active series fan out to all trackers.
    let (m, _, _, ca_reg) = new_manager(vec![("user8", user8())]);
    m.active_series_tracker("user8").unwrap().increment(
        &lbls(&[("platform", "bare-metal"), ("team", "frontend")]),
        unix(20),
        3,
    );
    let exp = format!(
        r#"
        {HELP_BUCKETS}
        cortex_ingester_attributed_active_native_histogram_buckets{{my_platform="bare-metal",tenant="user8",tracker="by-platform"}} 3
        cortex_ingester_attributed_active_native_histogram_buckets{{my_team="frontend",tenant="user8",tracker="cost-attribution"}} 3
        {HELP_NH}
        cortex_ingester_attributed_active_native_histogram_series{{my_platform="bare-metal",tenant="user8",tracker="by-platform"}} 1
        cortex_ingester_attributed_active_native_histogram_series{{my_team="frontend",tenant="user8",tracker="cost-attribution"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_platform="bare-metal",tenant="user8",tracker="by-platform"}} 1
        cortex_ingester_attributed_active_series{{my_team="frontend",tenant="user8",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL[2..5]);

    // Operational metrics include tracker name.
    let (m, _, reg, _) = new_manager(vec![("user8", user8()), ("user9", user9())]);
    m.sample_tracker("user8")
        .unwrap()
        .increment_received_samples(
            &[(&[("team", "backend"), ("platform", "k8s")][..], 5)],
            unix(10),
        );
    m.active_series_tracker("user8").unwrap().increment(
        &lbls(&[("platform", "bare-metal"), ("team", "frontend")]),
        unix(20),
        3,
    );
    m.sample_tracker("user9");
    let exp = format!(
        r#"
        {HELP_S_CARD}
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="by-platform",user="user8"}} 1
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="by-service",user="user9"}} 0
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="by-team",user="user9"}} 0
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="cost-attribution",user="user8"}} 1
        {HELP_AS_CARD}
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="by-platform",user="user8"}} 1
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user8"}} 1
        "#
    );
    assert_metrics(
        &reg,
        &exp,
        &[
            "cortex_cost_attribution_sample_tracker_cardinality",
            "cortex_cost_attribution_active_series_tracker_cardinality",
        ],
    );

    // Operational metrics include the internal trackers.
    let (m, _, reg, _) = new_manager(vec![("user9", user9())]);
    m.sample_tracker("user9")
        .unwrap()
        .increment_received_samples(
            &[(
                &[
                    ("__name__", "testing"),
                    ("service", "gateway"),
                    ("team", "ops"),
                ][..],
                1,
            )],
            unix(100),
        );
    m.active_series_tracker("user9").unwrap().increment(
        &lbls(&[
            ("__name__", "testing"),
            ("service", "gateway"),
            ("team", "ops"),
        ]),
        SystemTime::now(),
        0,
    );
    let exp = format!(
        r#"
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_service="gateway",tenant="user9",tracker="by-service"}} 1
        cortex_distributor_received_attributed_samples_total{{my_team="ops",tenant="user9",tracker="by-team"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_service="gateway",tenant="user9",tracker="by-service"}} 1
        cortex_ingester_attributed_active_series{{my_team="ops",tenant="user9",tracker="by-team"}} 1
        "#
    );
    assert_metrics(
        &reg,
        &exp,
        &[
            "cortex_distributor_received_attributed_samples_total",
            "cortex_ingester_attributed_active_series",
        ],
    );

    // Purge removes all trackers for a user when inactive.
    let (m, ..) = new_manager(vec![("user9", user9())]);
    m.sample_tracker("user9")
        .unwrap()
        .increment_received_samples(
            &[(&[("team", "ops"), ("service", "gateway")][..], 1)],
            unix(100),
        );
    assert_eq!(m.debug_num_trackers(TrackerKind::Samples, "user9"), Some(2));
    m.purge_inactive_attributions_until(unix(105));
    assert_eq!(m.debug_num_trackers(TrackerKind::Samples, "user9"), Some(2));
    m.purge_inactive_attributions_until(unix(120));
    assert_eq!(m.debug_num_trackers(TrackerKind::Samples, "user9"), None);
}

// Upstream: manager_test.go::TestManager_InvalidTrackers/Update cost attribution labels with a bad label name
#[test]
fn invalid_trackers_bad_label_name() {
    let (m, o, reg, ca_reg) = new_manager(vec![("user1", user1()), ("user3", user3())]);
    m.sample_tracker("user1")
        .unwrap()
        .increment_discarded_samples_pairs(
            &[("team", "bar")],
            1.0,
            "invalid-metrics-name",
            unix(6),
        );
    m.active_series_tracker("user1")
        .unwrap()
        .increment(&lbls(&[("team", "bar")]), unix(10), 50);
    m.sample_tracker("user3")
        .unwrap()
        .increment_received_samples(
            &[(&[("department", "foo"), ("service", "dodo")][..], 1)],
            unix(20),
        );
    m.active_series_tracker("user3").unwrap().increment(
        &lbls(&[("department", "foo"), ("service", "dodo")]),
        unix(10),
        2,
    );
    set_tenants(
        &o,
        vec![
            ("user1", limits(10, default_tracker(&[("__team__", "")]))),
            ("user3", user3()),
        ],
    );
    m.purge_inactive_attributions_until(unix(11) + m.inactive_timeout());

    let exp = format!(
        r#"
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_BUCKETS}
        cortex_ingester_attributed_active_native_histogram_buckets{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 2
        {HELP_NH}
        cortex_ingester_attributed_active_native_histogram_series{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_OVERFLOW}
        cortex_attributed_series_overflow_labels{{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL);
    let exp = format!(
        r#"
        {HELP_AS_CARD}
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 1
        {HELP_S_CARD}
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 1
        {HELP_ERRORS}
        cortex_cost_attribution_tracker_creation_errors_total{{tracker_name="cost-attribution",tracker_type="active-series",user="user1"}} 1
        cortex_cost_attribution_tracker_creation_errors_total{{tracker_name="cost-attribution",tracker_type="samples",user="user1"}} 1
        "#
    );
    assert_metrics(&reg, &exp, &OPERATIONAL);
}

// Upstream: manager_test.go::TestManager_InvalidTrackers/Update cost attribution labels with a good label name
#[test]
fn invalid_trackers_good_label_name() {
    let (m, o, reg, ca_reg) = new_manager(vec![("user1", user1()), ("user3", user3())]);
    m.active_series_tracker("user1")
        .unwrap()
        .increment(&lbls(&[("team", "bar")]), unix(10), 50);
    m.sample_tracker("user3")
        .unwrap()
        .increment_received_samples(
            &[(&[("department", "foo"), ("service", "dodo")][..], 1)],
            unix(20),
        );
    m.active_series_tracker("user3").unwrap().increment(
        &lbls(&[("department", "foo"), ("service", "dodo")]),
        unix(10),
        2,
    );
    set_tenants(
        &o,
        vec![
            ("user1", limits(10, default_tracker(&[("team", "")]))),
            ("user3", user3()),
        ],
    );
    m.purge_inactive_attributions_until(unix(11) + m.inactive_timeout());
    m.active_series_tracker("user1")
        .unwrap()
        .increment(&lbls(&[("team", "bar")]), unix(10), 50);
    let exp = format!(
        r#"
        {HELP_OVERFLOW}
        cortex_attributed_series_overflow_labels{{team="__overflow__",tenant="user1",tracker="cost-attribution"}} 1
        cortex_attributed_series_overflow_labels{{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_RECEIVED}
        cortex_distributor_received_attributed_samples_total{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_BUCKETS}
        cortex_ingester_attributed_active_native_histogram_buckets{{team="bar",tenant="user1",tracker="cost-attribution"}} 50
        cortex_ingester_attributed_active_native_histogram_buckets{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 2
        {HELP_NH}
        cortex_ingester_attributed_active_native_histogram_series{{team="bar",tenant="user1",tracker="cost-attribution"}} 1
        cortex_ingester_attributed_active_native_histogram_series{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        {HELP_AS}
        cortex_ingester_attributed_active_series{{team="bar",tenant="user1",tracker="cost-attribution"}} 1
        cortex_ingester_attributed_active_series{{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"}} 1
        "#
    );
    assert_metrics(&ca_reg, &exp, &CA_ALL);
    let exp = format!(
        r#"
        {HELP_AS_CARD}
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user1"}} 1
        cortex_cost_attribution_active_series_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 1
        {HELP_S_CARD}
        cortex_cost_attribution_sample_tracker_cardinality{{tracker="cost-attribution",user="user3"}} 1
        "#
    );
    assert_metrics(&reg, &exp, &OPERATIONAL);
}

// Upstream: manager_test.go::TestManager_ActiveSeriesTrackerReplacedAfterOverflowRecovery
#[test]
fn active_series_tracker_replaced_after_overflow_recovery() {
    let purge_at = unix(3600);
    let base = unix(0);
    let mk = |additional: bool| {
        let mut l = LimitsConfig {
            cost_attribution_cooldown: ModelDuration(60_000_000_000),
            ..limits(2, trackers(&[("t1", ca_labels(&[("a", "a")]), false)]))
        };
        if additional {
            l.additional_cost_attribution_trackers =
                trackers(&[("t2", ca_labels(&[("b", "b")]), false)]);
        }
        let o = overrides(vec![("u", l)]);
        Manager::new(
            Duration::from_secs(1),
            o,
            Some(&Registry::new()),
            &Registry::new(),
        )
        .unwrap()
    };

    // Single tracker.
    let m = mk(false);
    let before = m.active_series_tracker("u").unwrap();
    before.increment(&lbls(&[("a", "v1")]), base, -1);
    before.increment(&lbls(&[("a", "v2")]), base, -1);
    before.increment(&lbls(&[("a", "v3")]), base, -1);
    before.decrement(&lbls(&[("a", "v3")]), -1);
    m.purge_inactive_attributions_until(purge_at);
    let after = m.active_series_tracker("u");
    assert!(!before.equals(after.as_ref()));

    // One of several trackers recovers.
    let m = mk(true);
    let before = m.active_series_tracker("u").unwrap();
    before.increment(&lbls(&[("a", "v1"), ("b", "w1")]), base, -1);
    before.increment(&lbls(&[("a", "v2"), ("b", "w1")]), base, -1);
    before.increment(&lbls(&[("a", "v3"), ("b", "w1")]), base, -1);
    before.decrement(&lbls(&[("a", "v3"), ("b", "w1")]), -1);
    m.purge_inactive_attributions_until(purge_at);
    let after = m.active_series_tracker("u");
    assert!(!before.equals(after.as_ref()));
}

// Upstream: manager_test.go::TestManager_ActiveSeriesTrackerRebuiltFreshOnConfigReload
#[test]
fn active_series_tracker_rebuilt_fresh_on_config_reload() {
    let config_a = limits(100, trackers(&[("t1", ca_labels(&[("a", "a")]), false)]));
    let config_b = LimitsConfig {
        additional_cost_attribution_trackers: trackers(&[("t2", ca_labels(&[("b", "b")]), false)]),
        ..config_a.clone()
    };
    let o = overrides(vec![("u", config_a)]);
    let m = Manager::new(
        Duration::from_secs(1),
        o.clone(),
        Some(&Registry::new()),
        &Registry::new(),
    )
    .unwrap();

    let ast_before = m.active_series_tracker("u").unwrap();
    ast_before.increment(&lbls(&[("a", "v1")]), unix(0), -1);
    m.sample_tracker("u")
        .unwrap()
        .increment_received_samples(&[(&[("a", "v1")][..], 1)], unix(0));
    let active_before = m
        .debug_tracker(TrackerKind::ActiveSeries, "u", "t1")
        .unwrap();
    let sample_before = m.debug_tracker(TrackerKind::Samples, "u", "t1").unwrap();
    assert_eq!(active_before.observed, 1);

    set_tenants(&o, vec![("u", config_b)]);
    let ast_after = m.active_series_tracker("u");
    assert!(!ast_before.equals(ast_after.as_ref()));
    let active_after = m
        .debug_tracker(TrackerKind::ActiveSeries, "u", "t1")
        .unwrap();
    assert_ne!(active_before.id, active_after.id);
    assert_eq!(active_after.observed, 0);

    assert!(m.sample_tracker("u").is_some());
    let sample_after = m.debug_tracker(TrackerKind::Samples, "u", "t1").unwrap();
    assert_eq!(sample_before.id, sample_after.id);
    assert_eq!(sample_after.observed, 1);
}

// Upstream: sample_tracker_test.go::TestSampleTracker_inactiveObservations
// (Go purges the individual tracker with deadline=t; here via the manager with now=t+inactiveTimeout).
#[test]
fn sample_tracker_inactive_observations() {
    let (m, ..) = new_manager(vec![("user1", user1())]);
    let st = m.sample_tracker("user1").unwrap();
    st.increment_discarded_samples_pairs(&[("team", "foo")], 1.0, "invalid-metrics-name", unix(1));
    st.increment_discarded_samples_pairs(&[("team", "bar")], 2.0, "out-of-window-sample", unix(12));
    st.increment_discarded_samples_pairs(&[("team", "baz")], 3.0, "invalid-metrics-name", unix(20));
    let observed = |m: &Manager| {
        m.debug_tracker(TrackerKind::Samples, "user1", DEFAULT_TRACKER_NAME)
            .map_or(0, |d| d.observed)
    };
    assert_eq!(observed(&m), 3);
    m.purge_inactive_attributions_until(unix(0) + m.inactive_timeout());
    assert_eq!(observed(&m), 3);
    m.purge_inactive_attributions_until(unix(10) + m.inactive_timeout());
    assert_eq!(observed(&m), 2);
    m.purge_inactive_attributions_until(unix(15) + m.inactive_timeout());
    assert_eq!(observed(&m), 1);
    m.purge_inactive_attributions_until(unix(25) + m.inactive_timeout());
    assert_eq!(observed(&m), 0);
}

// Upstream: costattributionmodel/labels_test.go::TestLabel_Validate + TestLabels_Validate
#[test]
fn labels_validate() {
    let l = |i: &str, o: &str| CostAttributionLabel {
        input: i.to_string(),
        output: o.to_string(),
    };
    assert!(validate_label(&l("team", "")).is_ok());
    assert!(validate_label(&l("team", "my_team")).is_ok());
    assert!(validate_label(&l("", "my_team")).is_err());
    assert!(validate_label(&l("team", "__reserved")).is_err());
    assert!(validate_label(&l("__name__", "")).is_ok());
    assert!(validate_labels(&[l("team", "x"), l("service", "x")]).is_err());
    assert!(validate_labels(&[l("team", ""), l("team", "")]).is_err());
    assert!(validate_labels(&[l("team", ""), l("service", "")]).is_ok());
}

// Go: limits.go migrateCostAttributionLabelsStructured + Canonicalize + CostAttributionConfigHash.
#[test]
fn config_migration_and_canonicalization() {
    let mut l = LimitsConfig {
        cost_attribution_labels_structured: ca_labels(&[("z", ""), ("a", "")]),
        ..no_trackers_default()
    };
    let (h1, cfg) = cost_attribution_config(&l).unwrap();
    let t = cfg.trackers.get(DEFAULT_TRACKER_NAME).unwrap();
    assert_eq!(t.labels, ca_labels(&[("a", ""), ("z", "")]));
    assert!(cost_attribution_config(&no_trackers_default()).is_none());
    l.max_cost_attribution_cardinality += 1;
    let (h2, _) = cost_attribution_config(&l).unwrap();
    assert_ne!(h1, h2);
}
