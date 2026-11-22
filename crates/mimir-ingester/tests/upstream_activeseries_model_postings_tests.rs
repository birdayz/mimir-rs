//! Ported from github.com/grafana/mimir/pkg/ingester/activeseries/{active_labels_test.go,
//! active_native_histogram_postings_test.go, active_postings_test.go,
//! model/custom_trackers_config_test.go, model/matchers_test.go}

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_ingester::active_series::{
    am_label_matcher_to_prom, expand_postings, is_label_value_active, merge_custom_trackers_config,
    ActivePostings, ActiveSeries, AmMatchType, AmMatcher, BucketCountPostings, CustomTrackersConfig,
    ListPostings, MemPostingsReader, Matchers, NativeHistogramPostings, PostingsIter,
    MAX_NUMBER_OF_TRACKERS,
};
use mimir_tsdb::labels::{Labels, SymbolLabels};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings::MemPostings;
use mimir_tsdb::symbols::SymbolTable;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unix(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn series_a(n: usize) -> Vec<Labels> {
    (1..=n)
        .map(|i| Labels::from_pairs([("a", i.to_string().as_str())]))
        .collect()
}

/// `time.Duration(ttl)` in Go is `ttl` nanoseconds.
fn new_active_series(ttl: u64) -> ActiveSeries {
    ActiveSeries::with_matchers(Matchers::default(), Duration::from_nanos(ttl), None)
}

fn must_new_from_map(m: &[(&str, &str)]) -> CustomTrackersConfig {
    let map: BTreeMap<String, String> = m
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    CustomTrackersConfig::new(map).unwrap()
}

fn must_new_from_string(s: &str) -> CustomTrackersConfig {
    let mut m = CustomTrackersConfig::default();
    m.set(s).unwrap();
    m
}

/// `yaml.Unmarshal([]byte(s), &CustomTrackersConfig{})`; empty documents leave the zero value.
fn yaml_unmarshal(s: &str) -> Result<CustomTrackersConfig, String> {
    if s.trim().is_empty() {
        return Ok(CustomTrackersConfig::default());
    }
    serde_yaml::from_str::<CustomTrackersConfig>(s).map_err(|e| e.to_string())
}

fn must_new_from_yaml(s: &str) -> CustomTrackersConfig {
    yaml_unmarshal(s).unwrap()
}

const FLAG: &str = "ingester.active-series-custom-trackers";

/// Emulates `flag.FlagSet.Parse` for a `flag.Value` registered under `FLAG`.
fn parse_flags(flags: &[&str], config: &mut CustomTrackersConfig) -> Result<(), String> {
    for f in flags {
        let rest = f.strip_prefix('-').unwrap();
        let (name, value) = rest.split_once('=').unwrap();
        assert_eq!(name, FLAG);
        config
            .set(value)
            .map_err(|e| format!("invalid value {value:?} for flag -{FLAG}: {e}"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// active_labels_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/activeseries/active_labels_test.go::TestIsLabelValueActive
#[test]
fn test_is_label_value_active() {
    let ttl = 3u64;
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: [u64; 5] = [1, 2, 3, 4, 5];
    let active_series = new_active_series(ttl);

    let mut symbols = SymbolTable::new();
    let mem_postings = MemPostings::new();
    for (i, l) in series.iter().enumerate() {
        let sl = SymbolLabels::from_labels(&mut symbols, l);
        mem_postings.add(all_storage_refs[i], &sl);
    }
    let reader = MemPostingsReader {
        postings: &mem_postings,
        symbols: &symbols,
    };

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            -1,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);

    for (value, expected) in [("1", false), ("2", false), ("3", false), ("4", true), ("5", true)] {
        let result = is_label_value_active(&reader, &active_series, "a", value).unwrap();
        assert_eq!(result, expected, "value {value}");
    }
}

// ---------------------------------------------------------------------------
// active_native_histogram_postings_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/activeseries/active_native_histogram_postings_test.go::TestNativeHistogramPostings_Expand
#[test]
fn test_native_histogram_postings_expand() {
    let ttl = 3u64;
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: Vec<u64> = vec![1, 2, 3, 4, 5];
    let storage_postings = ListPostings::new(all_storage_refs.clone());
    let active_series = new_active_series(ttl);

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        let mut buckets = -1; // No native histogram buckets.
        if i + 1 == 3 || i + 1 == 4 {
            buckets = 10; // Native histogram with 10 buckets.
        }
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            buckets,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(2, all_active);

    let mut active_series_postings = NativeHistogramPostings::new(&active_series, storage_postings);

    let active_refs = expand_postings(&mut active_series_postings).unwrap();

    assert_eq!(all_storage_refs[3..4].to_vec(), active_refs);
}

// Upstream: pkg/ingester/activeseries/active_native_histogram_postings_test.go::TestNativeHistogramPostings_ExpandWithBucketCount
#[test]
fn test_native_histogram_postings_expand_with_bucket_count() {
    let ttl = 0u64;
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: Vec<u64> = vec![1, 2, 3, 4, 5];
    let storage_postings = ListPostings::new(all_storage_refs.clone());
    let active_series = new_active_series(ttl);

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        let mut buckets = -1; // No native histogram buckets.
        if i == 2 || i == 3 {
            buckets = i as i64 * 10; // Native histogram with i*10 buckets.
        }
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            buckets,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(5, all_active);

    let mut active_series_postings = NativeHistogramPostings::new(&active_series, storage_postings);

    let mut series_ref: Vec<u64> = Vec::new();
    let mut bucket_counts: Vec<usize> = Vec::new();
    while active_series_postings.next() {
        let (r, count) = active_series_postings.at_bucket_count();
        series_ref.push(r);
        bucket_counts.push(count);
    }
    assert!(active_series_postings.err().is_none());

    assert_eq!(all_storage_refs[2..4].to_vec(), series_ref);
    assert_eq!(vec![20usize, 30], bucket_counts);
}

// Upstream: pkg/ingester/activeseries/active_native_histogram_postings_test.go::TestNativeHistogramPostings_SeekSkipsNonNative
#[test]
fn test_native_histogram_postings_seek_skips_non_native() {
    let ttl = 3u64;
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: Vec<u64> = vec![1, 2, 3, 4, 5];
    let storage_postings = ListPostings::new(all_storage_refs.clone());
    let active_series = new_active_series(ttl);

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        let mut buckets = i as i64 * 10;
        if i + 1 == 4 {
            buckets = -1; // Make ref==4 not a native histogram to check that Seek skips it.
        }
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            buckets,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(2, all_active);

    let mut active_series_postings = NativeHistogramPostings::new(&active_series, storage_postings);

    // Seek to a series that is not active.
    assert!(active_series_postings.seek(3));
    // The next active series is 4, but it's not a native histogram.
    assert_eq!(5, active_series_postings.at());
    // Check the bucket count as well.
    let (r, count) = active_series_postings.at_bucket_count();
    assert_eq!(5, r);
    assert_eq!(40, count);
}

// Upstream: pkg/ingester/activeseries/active_native_histogram_postings_test.go::TestNativeHistogramPostings_Seek
#[test]
fn test_native_histogram_postings_seek() {
    let ttl = 3u64;
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: Vec<u64> = vec![1, 2, 3, 4, 5];
    let storage_postings = ListPostings::new(all_storage_refs.clone());
    let active_series = new_active_series(ttl);

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        let buckets = i as i64 * 10;
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            buckets,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(2, all_active);

    let mut active_series_postings = NativeHistogramPostings::new(&active_series, storage_postings);

    // Seek to a series that is active.
    assert!(active_series_postings.seek(4));
    // The next active series is 4.
    assert_eq!(4, active_series_postings.at());
    // Check the bucket count as well.
    let (r, count) = active_series_postings.at_bucket_count();
    assert_eq!(4, r);
    assert_eq!(30, count);
}

// Upstream: pkg/ingester/activeseries/active_native_histogram_postings_test.go::TestNativeHistogramPostings_SeekToEnd
#[test]
fn test_native_histogram_postings_seek_to_end() {
    let ttl = 5u64;
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: Vec<u64> = vec![1, 2, 3, 4, 5];
    let storage_postings = ListPostings::new(all_storage_refs.clone());
    let active_series = new_active_series(ttl);

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            10,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(0, all_active);

    let mut active_series_postings = NativeHistogramPostings::new(&active_series, storage_postings);

    // Seek to a series that is not active.
    // There are no active series after 3, so Seek should return false.
    assert!(!active_series_postings.seek(3));
}

// ---------------------------------------------------------------------------
// active_postings_test.go
// ---------------------------------------------------------------------------

fn postings_fixture(ttl: u64) -> (ActiveSeries, Vec<u64>) {
    let mocked_time = unix(ttl);
    let series = series_a(5);
    let all_storage_refs: Vec<u64> = vec![1, 2, 3, 4, 5];
    let active_series = new_active_series(ttl);

    // Update each series at a different time according to its index.
    for i in 0..all_storage_refs.len() {
        active_series.update_series_labels(
            &series[i],
            all_storage_refs[i],
            unix(i as u64),
            -1,
            false,
            None,
        );
    }

    active_series.purge(mocked_time);
    (active_series, all_storage_refs)
}

// Upstream: pkg/ingester/activeseries/active_postings_test.go::TestPostings_Expand
#[test]
fn test_postings_expand() {
    let ttl = 3usize;
    let (active_series, all_storage_refs) = postings_fixture(ttl as u64);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(2, all_active);

    let storage_postings = ListPostings::new(all_storage_refs.clone());
    let mut active_series_postings = ActivePostings::new(&active_series, storage_postings);

    let active_refs = expand_postings(&mut active_series_postings).unwrap();

    assert_eq!(all_storage_refs[ttl..].to_vec(), active_refs);
}

// Upstream: pkg/ingester/activeseries/active_postings_test.go::TestPostings_Seek
#[test]
fn test_postings_seek() {
    let (active_series, all_storage_refs) = postings_fixture(3);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(2, all_active);

    let storage_postings = ListPostings::new(all_storage_refs);
    let mut active_series_postings = ActivePostings::new(&active_series, storage_postings);

    // Seek to a series that is not active.
    assert!(active_series_postings.seek(3));
    // The next active series is 4.
    assert_eq!(4, active_series_postings.at());
}

// Upstream: pkg/ingester/activeseries/active_postings_test.go::TestPostings_SeekToEnd
#[test]
fn test_postings_seek_to_end() {
    let (active_series, all_storage_refs) = postings_fixture(5);
    let (all_active, _, _, _, _, _, _) = active_series.active_with_matchers();
    assert_eq!(0, all_active);

    let storage_postings = ListPostings::new(all_storage_refs);
    let mut active_series_postings = ActivePostings::new(&active_series, storage_postings);

    // Seek to a series that is not active.
    // There are no active series after 3, so Seek should return false.
    assert!(!active_series_postings.seek(3));
}

// ---------------------------------------------------------------------------
// model/custom_trackers_config_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestCustomTrackersConfigs
#[test]
fn test_custom_trackers_configs() {
    struct Tc {
        name: &'static str,
        flags: Vec<String>,
        expected: CustomTrackersConfig,
        error: Option<String>,
    }
    let f = |v: &str| format!("-{FLAG}={v}");
    let cases = vec![
        Tc {
            name: "empty flag value produces empty config",
            flags: vec![f("")],
            expected: CustomTrackersConfig::default(),
            error: None,
        },
        Tc {
            name: "empty matcher fails",
            flags: vec![f("foo:")],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value "foo:" for flag -{FLAG}: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: "foo:""#)),
        },
        Tc {
            name: "empty whitespace-only matcher fails",
            flags: vec![f("foo: ")],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value "foo: " for flag -{FLAG}: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: "foo: ""#)),
        },
        Tc {
            name: "second empty whitespace-only matcher fails",
            flags: vec![f("foo: ;bar:{}")],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value "foo: ;bar:{{}}" for flag -{FLAG}: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: "foo: ""#)),
        },
        Tc {
            name: "empty name fails",
            flags: vec![f(":{}")],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value ":{{}}" for flag -{FLAG}: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: ":{{}}""#)),
        },
        Tc {
            name: "empty whitespace-only name fails",
            flags: vec![f(" :{}")],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value " :{{}}" for flag -{FLAG}: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: " :{{}}""#)),
        },
        Tc {
            name: "one matcher",
            flags: vec![f(r#"foo:{foo="bar"}"#)],
            expected: must_new_from_map(&[("foo", r#"{foo="bar"}"#)]),
            error: None,
        },
        Tc {
            name: "whitespaces are trimmed from name and matcher",
            flags: vec![f(" foo :      {foo=\"bar\"}\n ")],
            expected: must_new_from_map(&[("foo", r#"{foo="bar"}"#)]),
            error: None,
        },
        Tc {
            name: "two matchers in one flag value",
            flags: vec![f(r#"foo:{foo="bar"};baz:{baz="bar"}"#)],
            expected: must_new_from_map(&[("foo", r#"{foo="bar"}"#), ("baz", r#"{baz="bar"}"#)]),
            error: None,
        },
        Tc {
            name: "two matchers in two flag values",
            flags: vec![f(r#"foo:{foo="bar"}"#), f(r#"baz:{baz="bar"}"#)],
            expected: must_new_from_map(&[("foo", r#"{foo="bar"}"#), ("baz", r#"{baz="bar"}"#)]),
            error: None,
        },
        Tc {
            name: "two matchers with same name in same flag",
            flags: vec![f(r#"foo:{foo="bar"};foo:{boo="bam"}"#)],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value "foo:{{foo=\"bar\"}};foo:{{boo=\"bam\"}}" for flag -{FLAG}: matcher "foo" for active series custom trackers is provided twice"#)),
        },
        Tc {
            name: "two matchers with same name in separate flags",
            flags: vec![f(r#"foo:{foo="bar"}"#), f(r#"foo:{boo="bam"}"#)],
            expected: CustomTrackersConfig::default(),
            error: Some(format!(r#"invalid value "foo:{{boo=\"bam\"}}" for flag -{FLAG}: matcher "foo" for active series custom trackers is provided more than once"#)),
        },
    ];

    for tc in cases {
        let flags: Vec<&str> = tc.flags.iter().map(String::as_str).collect();
        let mut config = CustomTrackersConfig::default();
        let res = parse_flags(&flags, &mut config);

        if let Some(err) = &tc.error {
            assert_eq!(res.unwrap_err(), *err, "case {:?}", tc.name);
            continue;
        }
        res.unwrap_or_else(|e| panic!("case {:?}: {e}", tc.name));

        assert_eq!(tc.expected, config, "case {:?}", tc.name);

        // Check that CustomTrackersConfig.String() value is a valid flag value.
        let mut config_again = CustomTrackersConfig::default();
        let again = format!("-{FLAG}={}", config);
        parse_flags(&[again.as_str()], &mut config_again)
            .unwrap_or_else(|e| panic!("case {:?}: {e}", tc.name));

        assert_eq!(tc.expected, config_again, "case {:?}", tc.name);
    }
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestMaximumNumberOfTrackers
#[test]
fn test_maximum_number_of_trackers() {
    // Flag based setup
    {
        let number_of_trackers = MAX_NUMBER_OF_TRACKERS + 1;
        let flag_to_set = (0..number_of_trackers)
            .map(|i| format!("name{i}:{{__name__={i}}}"))
            .collect::<Vec<_>>()
            .join(";");

        let mut c = CustomTrackersConfig::default();
        assert!(
            c.set(&flag_to_set).is_err(),
            "custom tracker config should not accept more than {MAX_NUMBER_OF_TRACKERS} trackers"
        );
    }

    // Map based setup
    {
        let number_of_trackers = MAX_NUMBER_OF_TRACKERS + 1;
        let mut config_map = BTreeMap::new();
        for i in 0..number_of_trackers {
            config_map.insert(format!("name{i}"), format!("{{__name__={i}}}"));
        }
        assert!(
            CustomTrackersConfig::new(config_map).is_err(),
            "custom tracker config should not accept more than {MAX_NUMBER_OF_TRACKERS} trackers"
        );
    }
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestCustomTrackerConfig_Equality
#[test]
fn test_custom_tracker_config_equality() {
    let config_sets: Vec<Vec<CustomTrackersConfig>> = vec![
        vec![
            must_new_from_string(r#"foo:{foo='bar'};baz:{baz='bar'}"#),
            must_new_from_map(&[("baz", "{baz='bar'}"), ("foo", "{foo='bar'}")]),
            must_new_from_yaml(
                r#"
                baz: "{baz='bar'}"
                foo: "{foo='bar'}""#,
            ),
        ],
        vec![
            must_new_from_string(r#"test:{test='true'}"#),
            must_new_from_map(&[("test", "{test='true'}")]),
            must_new_from_yaml(r#"test: "{test='true'}""#),
        ],
        vec![must_new_from_yaml(
            r#"
        baz: "{baz='bar'}"
        foo: "{foo='bar'}"
        extra: "{extra='extra'}""#,
        )],
    ];

    // EqualityBetweenSet
    for config_set in &config_sets {
        for i in 0..config_set.len() {
            for j in (i + 1)..config_set.len() {
                assert_eq!(
                    config_set[i].to_string(),
                    config_set[j].to_string(),
                    "matcher configs should be equal"
                );
            }
        }
    }

    // NotEqualsAcrossSets
    let active_series_matchers: Vec<&CustomTrackersConfig> =
        config_sets.iter().map(|s| &s[0]).collect();
    for i in 0..active_series_matchers.len() {
        for j in (i + 1)..active_series_matchers.len() {
            assert_ne!(
                active_series_matchers[i].to_string(),
                active_series_matchers[j].to_string(),
                "matcher configs should NOT be equal"
            );
        }
    }
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestTrackersConfigs_Deserialization
#[test]
fn test_trackers_configs_deserialization() {
    let correct_input = r#"
        baz: "{baz='bar'}"
        foo: "{foo='bar'}"
    "#;
    let malformed_input = r#"
        baz: "123"
        foo: "{foo='bar'}"
    "#;

    // ShouldDeserializeCorrectInput
    {
        let config = yaml_unmarshal(correct_input).expect("failed do deserialize Matchers");
        let expected_config =
            must_new_from_map(&[("baz", "{baz='bar'}"), ("foo", "{foo='bar'}")]);
        assert_eq!(expected_config.to_string(), config.to_string());
    }

    // ShouldErrorOnMalformedInput
    {
        assert!(
            yaml_unmarshal(malformed_input).is_err(),
            "should not deserialize malformed input"
        );
    }
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestTrackersConfigs_SerializeDeserialize
#[test]
fn test_trackers_configs_serialize_deserialize() {
    let source_yaml = r#"
    baz: "{baz='bar'}"
    foo: "{foo='bar'}"
    "#;

    let obj = must_new_from_yaml(source_yaml);

    // ShouldSerializeDeserializeResultsTheSame
    let out = serde_yaml::to_string(&obj).expect("failed do serialize Custom trackers config");
    let re_serialized = yaml_unmarshal(&out).expect("Failed to deserialize serialized object");
    assert_eq!(obj, re_serialized);
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestCustomTrackersConfig_Equal
#[test]
fn test_custom_trackers_config_equal() {
    let base = || must_new_from_string(r#"foo:{foo="bar"};baz:{baz="bar"}"#);
    let cfg = |source: &[(&str, &str)], config_keys: &[&str], string: &str| CustomTrackersConfig {
        source: source
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        config: config_keys
            .iter()
            .map(|k| (k.to_string(), Vec::<LabelMatcher>::new()))
            .collect(),
        canonical_string: string.to_string(),
    };

    let cases: Vec<(&str, CustomTrackersConfig, CustomTrackersConfig, bool)> = vec![
        ("Equal configurations", base(), base(), true),
        (
            "Different descriptions",
            {
                let mut c = base();
                c.canonical_string = "cfg1".to_string();
                c
            },
            {
                let mut c = base();
                c.canonical_string = "cfg2".to_string();
                c
            },
            false,
        ),
        (
            "Different source maps",
            {
                let mut c = base();
                c.source = BTreeMap::from([("a".to_string(), "source1".to_string())]);
                c
            },
            {
                let mut c = base();
                c.source = BTreeMap::from([("b".to_string(), "source2".to_string())]);
                c
            },
            false,
        ),
        (
            "Different config maps",
            {
                let mut c = base();
                c.config = BTreeMap::from([("a".to_string(), Vec::new())]);
                c
            },
            {
                let mut c = base();
                c.config = BTreeMap::from([("b".to_string(), Vec::new())]);
                c
            },
            false,
        ),
        (
            "Different keys in source maps",
            cfg(&[("a", "source1")], &["b"], "cfg1"),
            cfg(&[("c", "source1")], &["b"], "cfg1"),
            false,
        ),
        (
            "Different keys in config maps",
            cfg(&[("a", "source1")], &["b"], "cfg1"),
            cfg(&[("a", "source1")], &["c"], "cfg1"),
            false,
        ),
    ];

    for (name, cfg1, cfg2, expected) in cases {
        assert_eq!(expected, cfg1.equal(&cfg2), "case {name:?}");
        assert_eq!(expected, cfg1 == cfg2, "case {name:?}");
    }
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestMergeCustomTrackersConfig
#[test]
fn test_merge_custom_trackers_config() {
    let two = r#"
                baz: "{baz='bar'}"
                foo: "{foo='bar'}""#;
    let tests: Vec<(&str, &str, &str, &str)> = vec![
        ("both configs are empty", "", "", ""),
        (
            "the first config is empty",
            "",
            r#"
                baz: "{baz='bar'}"
                foo: "{foo='bar'}""#,
            r#"
                baz: "{baz='bar'}"
                foo: "{foo='bar'}""#,
        ),
        ("the second config is empty", two, "", two),
        (
            "both configs are non-empty and they both have the same key-value pairs in the same order",
            two,
            two,
            two,
        ),
        (
            "both configs are non-empty and they both have the same key-value pairs but in different order",
            two,
            r#"
                foo: "{foo='bar'}"
                baz: "{baz='bar'}""#,
            two,
        ),
        (
            "both configs are non-empty and they some but not all overlapping keys",
            r#"
                baz: "{baz='first'}"
                foo: "{foo='first'}""#,
            r#"
                foo: "{foo='second'}"
                bar: "{bar='second'}""#,
            r#"
                bar: "{bar='second'}"
                baz: "{baz='first'}"
                foo: "{foo='second'}""#,
        ),
        (
            "both configs are non-empty and they no overlapping keys",
            r#"
                baz: "{baz='first'}"
                foo: "{foo='first'}""#,
            r#"
                bar: "{bar='second'}""#,
            r#"
                bar: "{bar='second'}"
                baz: "{baz='first'}"
                foo: "{foo='first'}""#,
        ),
    ];

    for (name, first_s, second_s, expected_s) in tests {
        let first = must_new_from_yaml(first_s);
        let second = must_new_from_yaml(second_s);
        let expected = must_new_from_yaml(expected_s);

        let merged = merge_custom_trackers_config(&first, &second);
        assert_eq!(expected.source, merged.source, "case {name:?}");
        assert_eq!(expected.config, merged.config, "case {name:?}");
        assert_eq!(expected.canonical_string, merged.canonical_string, "case {name:?}");

        // The original configs should NOT have been changed.
        let expected_first = must_new_from_yaml(first_s);
        assert_eq!(expected_first.source, first.source, "case {name:?}");
        assert_eq!(expected_first.config, first.config, "case {name:?}");
        assert_eq!(expected_first.canonical_string, first.canonical_string, "case {name:?}");

        let expected_second = must_new_from_yaml(second_s);
        assert_eq!(expected_second.source, second.source, "case {name:?}");
        assert_eq!(expected_second.config, second.config, "case {name:?}");
        assert_eq!(expected_second.canonical_string, second.canonical_string, "case {name:?}");
    }
}

// Upstream: pkg/ingester/activeseries/model/custom_trackers_config_test.go::TestCustomTrackersConfig_Validate
#[test]
fn test_custom_trackers_config_validate() {
    struct Tc {
        name: &'static str,
        config: Vec<(&'static str, &'static str)>,
        length: isize,
        expect_error: bool,
        error_contains: &'static str,
    }
    let tests = vec![
        Tc {
            name: "within limit",
            config: vec![("tracker1", r#"{foo="bar"}"#), ("tracker2", r#"{baz="qux"}"#)],
            length: 5,
            expect_error: false,
            error_contains: "",
        },
        Tc {
            name: "at limit",
            config: vec![
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
            ],
            length: 3,
            expect_error: false,
            error_contains: "",
        },
        Tc {
            name: "exceeds limit",
            config: vec![
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
            ],
            length: 2,
            expect_error: true,
            error_contains: "the number of custom trackers [3] exceeds the configured limit [2]",
        },
        Tc {
            name: "unlimited (maxCardinality = 0)",
            config: vec![
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
            ],
            length: 0,
            expect_error: false,
            error_contains: "",
        },
        Tc {
            name: "invalid negative limit",
            config: vec![("tracker1", r#"{foo="bar"}"#)],
            length: -1,
            expect_error: true,
            error_contains: "invalid max custom trackers limit: -1",
        },
        Tc {
            name: "empty config with limit",
            config: vec![],
            length: 5,
            expect_error: false,
            error_contains: "",
        },
    ];

    for t in tests {
        let config = must_new_from_map(&t.config);
        let res = config.validate(t.length);
        if t.expect_error {
            let err = res.expect_err(t.name);
            assert!(
                err.contains(t.error_contains),
                "case {:?}: error {err:?} does not contain {:?}",
                t.name,
                t.error_contains
            );
        } else {
            res.unwrap_or_else(|e| panic!("case {:?}: {e}", t.name));
        }
    }
}

// ---------------------------------------------------------------------------
// model/matchers_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/activeseries/model/matchers_test.go::TestMatcher_MatchesSeries
#[test]
fn test_matcher_matches_series() {
    let asm = Matchers::new(must_new_from_map(&[
        ("bar_starts_with_1", r#"{bar=~"1.*"}"#),
        ("does_not_have_foo_label", r#"{foo=""}"#),
        ("has_foo_and_bar_starts_with_1", r#"{foo!="", bar=~"1.*"}"#),
        ("has_foo_label", r#"{foo!=""}"#),
        ("has_foo_and_is_not_true_or_false", r#"{foo!="", foo!~"(true|false)"}"#),
        ("foo_is_true", r#"{foo="true"}"#),
        ("foo_is_false", r#"{foo="false"}"#),
        ("foo_is_true_bar_starts_with_1", r#"{foo="true", bar=~"1.*"}"#),
        ("foo_is_true_bar_is_100", r#"{foo="true", bar="100"}"#),
        ("baz_is_boolean", r#"{baz=~"true|false"}"#),
        ("baz_is_boolean_zzz_is_ok", r#"{baz=~"true|false", zzz="ok"}"#),
    ]));

    #[allow(clippy::type_complexity)]
    let cases: Vec<(Vec<(&str, &str)>, Vec<&str>)> = vec![
        (
            vec![("foo", "true"), ("baz", "unrelated")],
            vec!["foo_is_true", "has_foo_label"],
        ),
        (
            vec![("foo", "true"), ("bar", "100"), ("baz", "unrelated")],
            vec![
                "bar_starts_with_1",
                "foo_is_true",
                "foo_is_true_bar_is_100",
                "foo_is_true_bar_starts_with_1",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
            ],
        ),
        (
            vec![("foo", "true"), ("bar", "200"), ("baz", "unrelated")],
            vec!["foo_is_true", "has_foo_label"],
        ),
        (
            vec![("bar", "200"), ("baz", "unrelated")],
            vec!["does_not_have_foo_label"],
        ),
        (
            vec![("bar", "100"), ("baz", "unrelated")],
            vec!["bar_starts_with_1", "does_not_have_foo_label"],
        ),
        (vec![("baz", "unrelated")], vec!["does_not_have_foo_label"]),
        // Test cases for foo=false
        (
            vec![("foo", "false")],
            vec!["foo_is_false", "has_foo_label"],
        ),
        (
            vec![("foo", "false"), ("bar", "100")],
            vec![
                "bar_starts_with_1",
                "foo_is_false",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
            ],
        ),
        (
            vec![("foo", "false"), ("bar", "200")],
            vec!["foo_is_false", "has_foo_label"],
        ),
        (
            vec![("foo", "false"), ("bar", "150")],
            vec![
                "bar_starts_with_1",
                "foo_is_false",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
            ],
        ),
        // Test cases for baz boolean values
        (
            vec![("baz", "true")],
            vec!["baz_is_boolean", "does_not_have_foo_label"],
        ),
        (
            vec![("baz", "false")],
            vec!["baz_is_boolean", "does_not_have_foo_label"],
        ),
        (
            vec![("foo", "true"), ("baz", "true")],
            vec!["baz_is_boolean", "foo_is_true", "has_foo_label"],
        ),
        (
            vec![("foo", "false"), ("baz", "false")],
            vec!["baz_is_boolean", "foo_is_false", "has_foo_label"],
        ),
        // Test cases for zzz label combinations
        (
            vec![("baz", "true"), ("zzz", "ok")],
            vec![
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "does_not_have_foo_label",
            ],
        ),
        (
            vec![("baz", "false"), ("zzz", "ok")],
            vec![
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "does_not_have_foo_label",
            ],
        ),
        (
            vec![("foo", "true"), ("baz", "true"), ("zzz", "ok")],
            vec![
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "foo_is_true",
                "has_foo_label",
            ],
        ),
        (
            vec![("foo", "false"), ("baz", "false"), ("zzz", "ok")],
            vec![
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "foo_is_false",
                "has_foo_label",
            ],
        ),
        (
            vec![("baz", "true"), ("zzz", "not_ok")],
            vec!["baz_is_boolean", "does_not_have_foo_label"],
        ),
        // Complex combinations
        (
            vec![("foo", "true"), ("bar", "123"), ("baz", "true"), ("zzz", "ok")],
            vec![
                "bar_starts_with_1",
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "foo_is_true",
                "foo_is_true_bar_starts_with_1",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
            ],
        ),
        (
            vec![("foo", "false"), ("bar", "155"), ("baz", "false"), ("zzz", "ok")],
            vec![
                "bar_starts_with_1",
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "foo_is_false",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
            ],
        ),
        (
            vec![("foo", "other_value"), ("bar", "100")],
            vec![
                "bar_starts_with_1",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
                "has_foo_and_is_not_true_or_false",
            ],
        ),
        (
            vec![("foo", "other_value")],
            vec!["has_foo_label", "has_foo_and_is_not_true_or_false"],
        ),
        // Edge cases
        (
            vec![("foo", "true"), ("bar", "100"), ("baz", "invalid")],
            vec![
                "bar_starts_with_1",
                "foo_is_true",
                "foo_is_true_bar_is_100",
                "foo_is_true_bar_starts_with_1",
                "has_foo_and_bar_starts_with_1",
                "has_foo_label",
            ],
        ),
        (
            vec![("foo", "false"), ("bar", "not_a_number"), ("baz", "true")],
            vec!["baz_is_boolean", "foo_is_false", "has_foo_label"],
        ),
        (
            vec![("bar", "1"), ("baz", "true"), ("zzz", "ok")],
            vec![
                "bar_starts_with_1",
                "baz_is_boolean",
                "baz_is_boolean_zzz_is_ok",
                "does_not_have_foo_label",
            ],
        ),
    ];

    for (pairs, expected) in cases {
        let series = Labels::from_pairs(pairs.iter().copied());
        let got = asm.matches(&series);
        let mut got_values: Vec<String> = (0..got.len())
            .map(|i| asm.matcher_names()[got.get(i) as usize].clone())
            .collect();
        got_values.sort();
        let mut expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(expected, got_values, "series {pairs:?}");

        // The allocation-free pair-based matcher must agree.
        let got_pairs = asm.matches_pairs(&pairs);
        let mut got_pair_values: Vec<String> = (0..got_pairs.len())
            .map(|i| asm.matcher_names()[got_pairs.get(i) as usize].clone())
            .collect();
        got_pair_values.sort();
        assert_eq!(expected, got_pair_values, "series (pairs) {pairs:?}");
    }
}

// Upstream: pkg/ingester/activeseries/model/matchers_test.go::TestCustomTrackersConfigs_MalformedMatcher
#[test]
fn test_custom_trackers_configs_malformed_matcher() {
    for matcher in [r#"{foo}"#, r#"{foo=~"}"#] {
        let config = BTreeMap::from([("malformed".to_string(), matcher.to_string())]);
        assert!(
            CustomTrackersConfig::new(config).is_err(),
            "matcher {matcher:?} should be rejected"
        );
    }
}

// Upstream: pkg/ingester/activeseries/model/matchers_test.go::TestAmlabelMatchersToProm_HappyCase
#[test]
fn test_amlabel_matchers_to_prom_happy_case() {
    let am_matcher = AmMatcher::new(AmMatchType::RegexMatch, "foo", "bar.*").unwrap();

    let expected = LabelMatcher::new(MatchType::RegexMatch, "foo", "bar.*").unwrap();
    assert_eq!(
        expected.to_string(),
        am_label_matcher_to_prom(&am_matcher).unwrap().to_string()
    );
}

// Upstream: pkg/ingester/activeseries/model/matchers_test.go::TestAmlabelMatchersToProm_MatchTypeValues
#[test]
fn test_amlabel_matchers_to_prom_match_type_values() {
    let last_type = AmMatchType::RegexNotMatch;
    // just checking that our assumption on that MatchType enums are the same is correct
    for v in (AmMatchType::Equal as u8)..=(last_type as u8) {
        let mt = AmMatchType::from_u8(v).expect("valid AmMatchType value");
        assert_eq!(mt.to_string(), mt.to_prom().to_string());
    }
    // and that nobody just added more match types in amlabels,
    assert!(
        AmMatchType::from_u8(last_type as u8 + 1).is_none(),
        "amlabels.MatchNotRegexp is expected to be the last enum value, update the test and check mapping"
    );
}
