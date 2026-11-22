//! Faithful 1:1 port of upstream Go tests in pkg/util/validation/*_test.go
//! (excluding exporter/ and TestMain, which only initializes global YAML defaults).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/validation/limits_test.go (57 Test* funcs; TestMain omitted)
//! - github.com/grafana/mimir/pkg/util/validation/blocked_query_test.go (4 Test* funcs)
//! - github.com/grafana/mimir/pkg/util/validation/limited_query_test.go (3 Test* funcs)
//! - github.com/grafana/mimir/pkg/util/validation/notifications_limit_flag_test.go (4 Test* funcs)
//! - github.com/grafana/mimir/pkg/util/validation/otel_translation_strategy_test.go (3 Test* funcs)
//! - github.com/grafana/mimir/pkg/util/validation/label_value_length_over_limit_strategy_test.go (1 Test* func)
//! - github.com/grafana/mimir/pkg/util/validation/user_limits_handler_test.go (1 Test* func)

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::thread;

use chrono::{TimeZone, Utc};
use mimir_config::{
    all_true_booleans_per_tenant, clear_registered_extensions, handle_user_limits_request,
    is_limit_error, largest_positive_non_zero_duration_per_tenant, merge_limits,
    min_duration_per_tenant, must_register_extension, new_limit_error,
    parse_float_chunk_encoding, parse_histogram_st_encoding_enabled,
    smallest_positive_int_per_tenant, smallest_positive_non_zero_duration_per_tenant,
    smallest_positive_non_zero_int_per_tenant, validate_blocked_queries, yaml_v3_emit,
    BlockedQueriesConfig, BlockedQuery, BlockedRequest, BlockedRequestQueryParam,
    CostAttributionLabel, CostAttributionTrackerConfig, CostAttributionTrackerConfigs, FlagSet,
    FloatChunkEncoding, GoDuration, LabelValueLengthOverLimitStrategy, LimitedQuery,
    LimitsConfig, LimitsExtensionDefault, LimitsMap, MetricRelabelConfig, ModelDuration,
    NameValidationScheme, NotificationRateLimitMap, OAuth2ClientConfig, OTelTranslationStrategy,
    Overrides, RulerAlertmanagerClientConfig, StringSliceCSV, UserLimitsResponse,
    FLOAT_CHUNK_ENCODING_VALUES, HISTOGRAM_CHUNK_ENCODING_VALUES, LABEL_VALUE_HASH_LEN,
};
use serde::{Deserialize, Serialize};

type LimitsCheckCase = (&'static str, &'static str, Box<dyn Fn(&LimitsConfig)>);

fn day_secs() -> i64 {
    24 * 3600
}

// =============================================================================================
// pkg/util/validation/user_limits_handler_test.go
// =============================================================================================

// Upstream: pkg/util/validation/user_limits_handler_test.go::TestUserLimitsHandler
#[test]
fn test_user_limits_handler() {
    let mut defaults = LimitsConfig::zeroed();
    defaults.ingestion_rate = 100.0;
    defaults.ingestion_burst_size = 10;
    defaults.compactor_blocks_retention_period = ModelDuration::from_secs(day_secs());

    let mut test_limits = defaults.clone();
    test_limits.ingestion_rate = 200.0;
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert("test-with-override".to_string(), test_limits);

    let ov = Overrides::with_static_map(defaults, tenant_limits);

    struct Case {
        name: &'static str,
        org_id: Option<&'static [u8]>,
        expected_status: u16,
        expected_limits: UserLimitsResponse,
    }

    let cases = [
        Case {
            name: "Authenticated user with override",
            org_id: Some(b"test-with-override"),
            expected_status: 200,
            expected_limits: UserLimitsResponse {
                ingestion_rate: 200.0,
                ingestion_burst_size: 10,
                compactor_blocks_retention_period_seconds: 86400,
                ..Default::default()
            },
        },
        Case {
            name: "Authenticated user without override",
            org_id: Some(b"test-no-override"),
            expected_status: 200,
            expected_limits: UserLimitsResponse {
                ingestion_rate: 100.0,
                ingestion_burst_size: 10,
                compactor_blocks_retention_period_seconds: 86400,
                ..Default::default()
            },
        },
        Case {
            name: "Unauthenticated user",
            org_id: None,
            expected_status: 401,
            expected_limits: UserLimitsResponse::default(),
        },
    ];

    for tc in cases {
        match handle_user_limits_request(tc.org_id, &ov) {
            Ok(resp) => {
                assert_eq!(tc.expected_status, 200, "{}", tc.name);
                let json_bytes = serde_json::to_vec(&resp).unwrap();
                let decoded: UserLimitsResponse = serde_json::from_slice(&json_bytes).unwrap();
                assert_eq!(tc.expected_limits, decoded, "{}", tc.name);
            }
            Err((status, _)) => {
                assert_eq!(tc.expected_status, status, "{}", tc.name);
            }
        }
    }
}

// =============================================================================================
// pkg/util/validation/otel_translation_strategy_test.go
// =============================================================================================

// Upstream: pkg/util/validation/otel_translation_strategy_test.go::TestOTelTranslationStrategyValue_UnmarshalYAML
#[test]
fn test_otel_translation_strategy_value_unmarshal_yaml() {
    let cases = [
        ("invalid", "invalid", "", true),
        ("empty", r#""""#, "", false),
        (
            "underscore escaping without suffixes",
            "UnderscoreEscapingWithoutSuffixes",
            "UnderscoreEscapingWithoutSuffixes",
            false,
        ),
        (
            "underscore escaping with suffixes",
            "UnderscoreEscapingWithSuffixes",
            "UnderscoreEscapingWithSuffixes",
            false,
        ),
        (
            "No UTF-8 escaping with suffixes",
            "NoUTF8EscapingWithSuffixes",
            "NoUTF8EscapingWithSuffixes",
            false,
        ),
        ("No translation", "NoTranslation", "NoTranslation", false),
    ];

    for (name, input, want, want_err) in cases {
        let res: Result<OTelTranslationStrategy, _> = serde_yaml::from_str(input);
        if want_err {
            assert!(res.is_err(), "{name}");
            continue;
        }
        let got = res.expect(name);
        assert_eq!(got.0, want, "{name}");

        let output = serde_yaml::to_string(&got).unwrap();
        let trimmed = output.trim();
        if want.is_empty() {
            assert!(trimmed == r#""""# || trimmed == "''", "{name}: {trimmed}");
        } else {
            assert_eq!(trimmed, input, "{name}");
        }
    }
}

// Upstream: pkg/util/validation/otel_translation_strategy_test.go::TestOTelTranslationStrategyValue_UnmarshalJSON
#[test]
fn test_otel_translation_strategy_value_unmarshal_json() {
    let cases = [
        ("invalid", r#""invalid""#, "", true),
        ("empty", r#""""#, "", false),
        (
            "underscore escaping without suffixes",
            r#""UnderscoreEscapingWithoutSuffixes""#,
            "UnderscoreEscapingWithoutSuffixes",
            false,
        ),
        (
            "underscore escaping with suffixes",
            r#""UnderscoreEscapingWithSuffixes""#,
            "UnderscoreEscapingWithSuffixes",
            false,
        ),
        (
            "No UTF-8 escaping with suffixes",
            r#""NoUTF8EscapingWithSuffixes""#,
            "NoUTF8EscapingWithSuffixes",
            false,
        ),
        ("No translation", r#""NoTranslation""#, "NoTranslation", false),
    ];

    for (name, input, want, want_err) in cases {
        let res: Result<OTelTranslationStrategy, _> = serde_json::from_str(input);
        if want_err {
            assert!(res.is_err(), "{name}");
            continue;
        }
        let got = res.expect(name);
        assert_eq!(got.0, want, "{name}");
        let output = serde_json::to_string(&got).unwrap();
        assert_eq!(output, input, "{name}");
    }
}

// Upstream: pkg/util/validation/otel_translation_strategy_test.go::TestOTelTranslationStrategyValue_Set
#[test]
fn test_otel_translation_strategy_value_set() {
    let cases = [
        ("invalid", "invalid", "", true),
        ("empty", "", "", false),
        (
            "Underscore escaping without suffixes",
            "UnderscoreEscapingWithoutSuffixes",
            "UnderscoreEscapingWithoutSuffixes",
            false,
        ),
        (
            "Underscore escaping with suffixes",
            "UnderscoreEscapingWithSuffixes",
            "UnderscoreEscapingWithSuffixes",
            false,
        ),
        (
            "No UTF-8 escaping with suffixes",
            "NoUTF8EscapingWithSuffixes",
            "NoUTF8EscapingWithSuffixes",
            false,
        ),
        ("No translation", "NoTranslation", "NoTranslation", false),
    ];

    for (name, input, want, want_err) in cases {
        let mut got = OTelTranslationStrategy::default();
        let err = got.set(input);
        if want_err {
            assert!(err.is_err(), "{name}");
            continue;
        }
        err.expect(name);
        assert_eq!(got.0, want, "{name}");
    }
}

// =============================================================================================
// pkg/util/validation/notifications_limit_flag_test.go
// =============================================================================================

// Upstream: pkg/util/validation/notifications_limit_flag_test.go::TestNotificationLimitsMap
#[test]
fn test_notification_limits_map() {
    #[derive(Default)]
    struct Target {
        map: LimitsMap<f64>,
    }

    let validate = |k: &str, _v: &f64| {
        if ![
            "webhook",
            "email",
            "pagerduty",
            "opsgenie",
            "wechat",
            "slack",
            "victorops",
            "pushover",
            "sns",
            "webex",
            "telegram",
            "discord",
            "msteams",
            "msteamsv2",
            "jira",
        ]
        .contains(&k)
        {
            return Err(mimir_config::ConfigError::Flag(format!(
                "unknown integration name: {k}"
            )));
        }
        Ok(())
    };

    // basic test
    {
        let mut fs = FlagSet::new();
        fs.limits_map_var_validated(
            "map-flag",
            &LimitsMap::new(),
            "Map flag, you can pass JSON into this",
            validate,
            |t: &Target| &t.map,
            |t: &mut Target| &mut t.map,
        );
        let mut target = Target::default();
        fs.parse(&mut target, &["-map-flag".into(), "{\"email\": 100 }".into()])
            .unwrap();
        let mut expected = LimitsMap::new();
        expected.insert("email".to_string(), 100.0);
        assert_eq!(target.map, expected);
    }

    // unknown integration
    {
        let mut fs = FlagSet::new();
        fs.limits_map_var_validated(
            "map-flag",
            &LimitsMap::new(),
            "Map flag, you can pass JSON into this",
            validate,
            |t: &Target| &t.map,
            |t: &mut Target| &mut t.map,
        );
        let mut target = Target::default();
        let err = fs
            .parse(&mut target, &["-map-flag".into(), "{\"unknown\": 200 }".into()])
            .unwrap_err();
        let msg = err.to_string();
        assert_eq!(
            msg.strip_prefix("invalid flag: ").unwrap_or(&msg),
            "invalid value \"{\\\"unknown\\\": 200 }\" for flag -map-flag: unknown integration name: unknown"
        );
    }

    // parsing error
    {
        let mut fs = FlagSet::new();
        fs.limits_map_var_validated(
            "map-flag",
            &LimitsMap::new(),
            "Map flag, you can pass JSON into this",
            validate,
            |t: &Target| &t.map,
            |t: &mut Target| &mut t.map,
        );
        let mut target = Target::default();
        let err = fs
            .parse(&mut target, &["-map-flag".into(), "{\"hello\": ...".into()])
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid value \"{\\\"hello\\\": ...\" for flag -map-flag:"),
            "unexpected error: {msg}"
        );
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct NotifTestStruct {
    flag: NotificationRateLimitMap,
}

// Upstream: pkg/util/validation/notifications_limit_flag_test.go::TestNotificationsLimitMapYaml
#[test]
fn test_notifications_limit_map_yaml() {
    let mut s = NotifTestStruct::default();
    s.flag.set("{\"email\": 500 }").unwrap();

    let yaml_val = serde_yaml::to_value(&s).unwrap();
    let rendered = yaml_v3_emit::marshal_struct(&yaml_val).unwrap();
    assert_eq!(rendered, "flag:\n    email: 500\n");

    let actual: NotifTestStruct = serde_yaml::from_str(&rendered).unwrap();
    assert_eq!(s, actual);
}

// Upstream: pkg/util/validation/notifications_limit_flag_test.go::TestUnknownIntegrationWhenLoadingYaml
#[test]
fn test_unknown_integration_when_loading_yaml() {
    let yaml_input = "flag:\n  unknown_integration: 500\n";
    let err = serde_yaml::from_str::<NotifTestStruct>(yaml_input).unwrap_err();
    assert!(
        err.to_string().contains("unknown integration name: unknown_integration"),
        "unexpected error: {err}"
    );
}

// Upstream: pkg/util/validation/notifications_limit_flag_test.go::TestWrongYamlStructureWhenLoadingYaml
#[test]
fn test_wrong_yaml_structure_when_loading_yaml() {
    let yaml_input = "flag:\n  email:\n    rate_limit: 7777\n    burst_size: 7777\n";
    let err = serde_yaml::from_str::<NotifTestStruct>(yaml_input).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("expected f64") || msg.contains("invalid type: map"),
        "unexpected error: {msg}"
    );
}

// =============================================================================================
// pkg/util/validation/label_value_length_over_limit_strategy_test.go
// =============================================================================================

// Upstream: pkg/util/validation/label_value_length_over_limit_strategy_test.go::TestLabelValueLengthOverLimitStrategy_Unmarshal
#[test]
fn test_label_value_length_over_limit_strategy_unmarshal() {
    let cases = [
        ("invalid", None, true),
        ("", Some(LabelValueLengthOverLimitStrategy::Error), false),
        ("error", Some(LabelValueLengthOverLimitStrategy::Error), false),
        (
            "truncate",
            Some(LabelValueLengthOverLimitStrategy::Truncate),
            false,
        ),
        ("drop", Some(LabelValueLengthOverLimitStrategy::Drop), false),
    ];

    for (input, want, want_err) in cases {
        let expected_str = if input.is_empty() { "error" } else { input };

        let json_bytes = serde_json::to_string(input).unwrap();
        let json_res = serde_json::from_str::<LabelValueLengthOverLimitStrategy>(&json_bytes);
        if want_err {
            assert!(json_res.is_err(), "json {input}");
        } else {
            let got = json_res.unwrap();
            assert_eq!(got, want.unwrap(), "json {input}");
            assert_eq!(got.to_string(), expected_str, "json {input}");
        }

        let yaml_str = serde_yaml::to_string(input).unwrap();
        let yaml_res = serde_yaml::from_str::<LabelValueLengthOverLimitStrategy>(&yaml_str);
        if want_err {
            assert!(yaml_res.is_err(), "yaml {input}");
        } else {
            let got = yaml_res.unwrap();
            assert_eq!(got, want.unwrap(), "yaml {input}");
            assert_eq!(got.to_string(), expected_str, "yaml {input}");
        }

        let flag_res = LabelValueLengthOverLimitStrategy::parse(input);
        if want_err {
            assert!(flag_res.is_err(), "flag {input}");
        } else {
            let got = flag_res.unwrap();
            assert_eq!(got, want.unwrap(), "flag {input}");
            assert_eq!(got.to_string(), expected_str, "flag {input}");
        }
    }
}

// =============================================================================================
// pkg/util/validation/limited_query_test.go
// =============================================================================================

// Upstream: pkg/util/validation/limited_query_test.go::TestLimitedQuery_IsExpired
#[test]
fn test_limited_query_is_expired() {
    let now = Utc.with_ymd_and_hms(2026, 6, 15, 0, 0, 0).unwrap();
    let cases = [
        ("zero value never expires", None, false),
        (
            "zero go time never expires",
            Some("0001-01-01T00:00:00Z"),
            false,
        ),
        ("expiry in the future", Some("2026-06-15T01:00:00Z"), false),
        ("expiry in the past", Some("2026-06-14T23:00:00Z"), true),
        (
            "expiry exactly now is not yet expired",
            Some("2026-06-15T00:00:00Z"),
            false,
        ),
    ];
    for (name, expires_at, expected) in cases {
        let q = LimitedQuery {
            expires_at: expires_at.map(str::to_string),
            ..Default::default()
        };
        assert_eq!(q.is_expired(now), expected, "{name}");
    }
}

// Upstream: pkg/util/validation/limited_query_test.go::TestLimitedQuery_MetadataFieldsRoundTripThroughYAML
#[test]
fn test_limited_query_metadata_fields_round_trip_through_yaml() {
    let input = LimitedQuery {
        query: "rate(metric_counter[5m])".to_string(),
        allowed_frequency: GoDuration::from_secs(60),
        reason: "the query is expensive and should not run more than once a minute".to_string(),
        id: "limit-metric-counter-rate".to_string(),
        note: "added per incident INC-1234".to_string(),
        created_by: "alice".to_string(),
        created_at: Some("2026-01-01T00:00:00Z".to_string()),
        expires_at: Some("2026-12-31T00:00:00Z".to_string()),
    };

    let data = serde_yaml::to_string(&input).unwrap();
    let output: LimitedQuery = serde_yaml::from_str(&data).unwrap();
    assert_eq!(input, output);
}

// Upstream: pkg/util/validation/limited_query_test.go::TestLimitedQuery_UnmarshalYAML_MissingMetadataFields
#[test]
fn test_limited_query_unmarshal_yaml_missing_metadata_fields() {
    let input = "\nquery: rate(metric_counter[5m])\nallowed_frequency: 1m\n";
    let output: LimitedQuery = serde_yaml::from_str(input).unwrap();
    assert_eq!(
        output,
        LimitedQuery {
            query: "rate(metric_counter[5m])".to_string(),
            allowed_frequency: GoDuration::from_secs(60),
            ..Default::default()
        }
    );
}

// =============================================================================================
// pkg/util/validation/blocked_query_test.go
// =============================================================================================

// Upstream: pkg/util/validation/blocked_query_test.go::TestBlockedQueriesConfig_Validate
#[test]
fn test_blocked_queries_config_validate() {
    let cases: Vec<(&str, BlockedQueriesConfig, &str)> = vec![
        ("no rules", vec![], ""),
        (
            "literal pattern",
            vec![BlockedQuery {
                pattern: "rate(metric_counter[5m])".to_string(),
                regex: false,
                ..Default::default()
            }],
            "",
        ),
        (
            "empty pattern",
            vec![BlockedQuery {
                time_range_longer_than: ModelDuration::from_hours(24),
                ..Default::default()
            }],
            "blocked_queries[0]: pattern is required",
        ),
        (
            "empty pattern second rule",
            vec![
                BlockedQuery {
                    pattern: "rate(metric_counter[5m])".to_string(),
                    regex: false,
                    ..Default::default()
                },
                BlockedQuery {
                    pattern: String::new(),
                    time_range_longer_than: ModelDuration::from_hours(24),
                    ..Default::default()
                },
            ],
            "blocked_queries[1]: pattern is required",
        ),
        (
            "valid regex",
            vec![BlockedQuery {
                pattern: ".*expensive.*".to_string(),
                regex: true,
                ..Default::default()
            }],
            "",
        ),
        (
            "invalid regex",
            vec![BlockedQuery {
                pattern: "[a-9}".to_string(),
                regex: true,
                ..Default::default()
            }],
            "blocked_queries[0]: invalid regex pattern \"[a-9}\"",
        ),
        (
            "metadata fields set",
            vec![BlockedQuery {
                pattern: "rate(metric_counter[5m])".to_string(),
                id: "block-metric-counter-rate".to_string(),
                note: "added per incident INC-1234".to_string(),
                created_by: "alice".to_string(),
                created_at: Some("2026-01-01T00:00:00Z".to_string()),
                expires_at: Some("2026-12-31T00:00:00Z".to_string()),
                ..Default::default()
            }],
            "",
        ),
    ];

    for (name, input, expected_err) in cases {
        let res = validate_blocked_queries(&input);
        if expected_err.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            let err = res.unwrap_err().to_string();
            assert!(err.contains(expected_err), "{name}: got {err:?}");
        }
    }
}

// Upstream: pkg/util/validation/blocked_query_test.go::TestBlockedQuery_IsExpired
#[test]
fn test_blocked_query_is_expired() {
    let now = Utc.with_ymd_and_hms(2026, 6, 15, 0, 0, 0).unwrap();
    let cases = [
        ("zero value never expires", None, false),
        (
            "zero go time never expires",
            Some("0001-01-01T00:00:00Z"),
            false,
        ),
        ("expiry in the future", Some("2026-06-15T01:00:00Z"), false),
        ("expiry in the past", Some("2026-06-14T23:00:00Z"), true),
        (
            "expiry exactly now is not yet expired",
            Some("2026-06-15T00:00:00Z"),
            false,
        ),
    ];
    for (name, expires_at, expected) in cases {
        let q = BlockedQuery {
            expires_at: expires_at.map(str::to_string),
            ..Default::default()
        };
        assert_eq!(q.is_expired(now), expected, "{name}");
    }
}

// Upstream: pkg/util/validation/blocked_query_test.go::TestBlockedQuery_MetadataFieldsRoundTripThroughYAML
#[test]
fn test_blocked_query_metadata_fields_round_trip_through_yaml() {
    let input = BlockedQuery {
        pattern: "rate(metric_counter[5m])".to_string(),
        reason: "because the query is misconfigured".to_string(),
        id: "block-metric-counter-rate".to_string(),
        note: "added per incident INC-1234".to_string(),
        created_by: "alice".to_string(),
        created_at: Some("2026-01-01T00:00:00Z".to_string()),
        expires_at: Some("2026-12-31T00:00:00Z".to_string()),
        ..Default::default()
    };

    let data = serde_yaml::to_string(&input).unwrap();
    let output: BlockedQuery = serde_yaml::from_str(&data).unwrap();
    assert_eq!(input, output);
}

// Upstream: pkg/util/validation/blocked_query_test.go::TestBlockedQuery_UnmarshalYAML_MissingMetadataFields
#[test]
fn test_blocked_query_unmarshal_yaml_missing_metadata_fields() {
    let input = "\npattern: rate(metric_counter[5m])\nreason: because the query is misconfigured\n";
    let output: BlockedQuery = serde_yaml::from_str(input).unwrap();
    assert_eq!(
        output,
        BlockedQuery {
            pattern: "rate(metric_counter[5m])".to_string(),
            reason: "because the query is misconfigured".to_string(),
            ..Default::default()
        }
    );
}

// =============================================================================================
// pkg/util/validation/limits_test.go (57 Test* funcs)
// =============================================================================================

// Upstream: pkg/util/validation/limits_test.go::TestOverridesManager_GetOverrides
#[test]
fn test_overrides_manager_get_overrides() {
    let mut defaults = LimitsConfig::zeroed();
    defaults.max_label_names_per_series = 100;
    let ov = Overrides::new(defaults.clone());

    assert_eq!(100, ov.max_label_names_per_series("user1"));
    assert_eq!(0, ov.max_label_value_length("user1"));

    let mut l = defaults;
    l.max_label_value_length = 150;
    ov.set_static_override("user1", Some(l));

    assert_eq!(100, ov.max_label_names_per_series("user1"));
    assert_eq!(150, ov.max_label_value_length("user1"));

    assert_eq!(100, ov.max_label_names_per_series("user2"));
    assert_eq!(0, ov.max_label_value_length("user2"));
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsLoadingFromYamlAndMap
#[test]
fn test_limits_loading_from_yaml_and_map() {
    let cases: Vec<LimitsCheckCase> = vec![
        (
            "defaults",
            "{}",
            Box::new(|l| {
                assert_eq!(1024, l.max_label_name_length);
                assert_eq!(NameValidationScheme::Unset, l.name_validation_scheme);
                assert!(l.otel_label_name_underscore_sanitization);
                assert!(l.otel_label_name_preserve_multiple_underscores);
            }),
        ),
        (
            "ingestion_rate",
            "ingestion_rate: 0.5",
            Box::new(|l| {
                assert_eq!(0.5, l.ingestion_rate);
            }),
        ),
        (
            "name_validation_scheme: legacy",
            "name_validation_scheme: \"legacy\"",
            Box::new(|l| {
                assert_eq!(NameValidationScheme::Legacy, l.name_validation_scheme);
            }),
        ),
        (
            "name_validation_scheme: utf8",
            "name_validation_scheme: \"utf8\"",
            Box::new(|l| {
                assert_eq!(NameValidationScheme::Utf8, l.name_validation_scheme);
            }),
        ),
        (
            "otel_label_name_underscore_sanitization: true",
            "otel_label_name_underscore_sanitization: true",
            Box::new(|l| {
                assert!(l.otel_label_name_underscore_sanitization);
            }),
        ),
        (
            "otel_label_name_preserve_multiple_underscores: true",
            "otel_label_name_preserve_multiple_underscores: true",
            Box::new(|l| {
                assert!(l.otel_label_name_preserve_multiple_underscores);
            }),
        ),
    ];

    for (name, input, check) in cases {
        let l = LimitsConfig::unmarshal_yaml(input).expect(name);
        check(&l);

        let m: serde_yaml::Value = serde_yaml::from_str(input).unwrap();
        let lm = LimitsConfig::unmarshal_map_with_defaults(m, &LimitsConfig::default()).expect(name);
        check(&lm);
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsLoadingFromJson
#[test]
fn test_limits_loading_from_json() {
    let cases: Vec<LimitsCheckCase> = vec![
        (
            "defaults",
            "{}",
            Box::new(|l| {
                assert_eq!(1024, l.max_label_name_length);
                assert_eq!(NameValidationScheme::Unset, l.name_validation_scheme);
                assert!(l.otel_label_name_underscore_sanitization);
                assert!(l.otel_label_name_preserve_multiple_underscores);
            }),
        ),
        (
            "ingestion_rate",
            r#"{"ingestion_rate": 0.5}"#,
            Box::new(|l| {
                assert_eq!(0.5, l.ingestion_rate);
            }),
        ),
        (
            "name_validation_scheme: utf8",
            r#"{"name_validation_scheme": "utf8"}"#,
            Box::new(|l| {
                assert_eq!(NameValidationScheme::Utf8, l.name_validation_scheme);
            }),
        ),
        (
            "otel_label_name_underscore_sanitization: true",
            r#"{"otel_label_name_underscore_sanitization": true}"#,
            Box::new(|l| {
                assert!(l.otel_label_name_underscore_sanitization);
            }),
        ),
        (
            "otel_label_name_preserve_multiple_underscores: true",
            r#"{"otel_label_name_preserve_multiple_underscores": true}"#,
            Box::new(|l| {
                assert!(l.otel_label_name_preserve_multiple_underscores);
            }),
        ),
    ];

    for (name, input, check) in cases {
        let l = LimitsConfig::unmarshal_json(input).expect(name);
        check(&l);
    }

    let inp = r#"{"unknown_fields": 100}"#;
    assert!(LimitsConfig::unmarshal_json(inp).is_err());
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsTagsYamlMatchJson
#[test]
fn test_limits_tags_yaml_match_json() {
    let mut cfg = LimitsConfig::default();
    cfg.blocked_queries.push(BlockedQuery {
        pattern: "up".into(),
        ..Default::default()
    });
    cfg.limited_queries.push(LimitedQuery {
        query: "up".into(),
        ..Default::default()
    });
    cfg.blocked_requests.push(BlockedRequest {
        path: "/foo".into(),
        ..Default::default()
    });
    cfg.metric_relabel_configs.push(MetricRelabelConfig::default());
    cfg.cost_attribution_labels_structured
        .push(CostAttributionLabel {
            input: "a".into(),
            output: "b".into(),
        });
    cfg.cost_attribution_trackers.insert(
        "t".into(),
        CostAttributionTrackerConfig {
            labels: vec![CostAttributionLabel {
                input: "a".into(),
                output: "b".into(),
            }],
            internal: false,
        },
    );
    cfg.additional_cost_attribution_trackers = cfg.cost_attribution_trackers.clone();

    let json_val = serde_json::to_value(&cfg).unwrap();
    let yaml_val = serde_yaml::to_value(&cfg).unwrap();
    let mut json_keys: Vec<String> = json_val.as_object().unwrap().keys().cloned().collect();
    let mut yaml_keys: Vec<String> = yaml_val
        .as_mapping()
        .unwrap()
        .keys()
        .map(|k| k.as_str().unwrap().to_string())
        .collect();
    json_keys.sort();
    yaml_keys.sort();
    assert_eq!(yaml_keys, json_keys);
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsStringDurationYamlMatch
#[test]
fn test_limits_string_duration_yaml_match() {
    let input_yaml = "\nmax_query_lookback: 1s\nmax_partial_query_length: 1s\n";
    let defaults = LimitsConfig::default();
    let limits_yaml = LimitsConfig::unmarshal_yaml_with_defaults(input_yaml, &defaults).unwrap();

    let input_json = r#"{"max_query_lookback": "1s", "max_partial_query_length": "1s"}"#;
    let limits_json = LimitsConfig::unmarshal_json_with_defaults(input_json, &defaults).unwrap();
    assert_eq!(limits_yaml, limits_json);

    let mut m = serde_yaml::Mapping::new();
    m.insert(
        serde_yaml::Value::String("max_query_lookback".to_string()),
        serde_yaml::Value::String("1s".to_string()),
    );
    m.insert(
        serde_yaml::Value::String("max_partial_query_length".to_string()),
        serde_yaml::Value::String("1s".to_string()),
    );
    let limits_map =
        LimitsConfig::unmarshal_map_with_defaults(serde_yaml::Value::Mapping(m), &defaults)
            .unwrap();
    assert_eq!(limits_yaml, limits_map);
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsAlwaysUsesPromDuration
#[test]
fn test_limits_always_uses_prom_duration() {
    let yaml = "\ncompactor_blocks_retention_period: 1d\nresults_cache_ttl: 1w\n";
    let limits = LimitsConfig::unmarshal_yaml(yaml).unwrap();
    assert_eq!(
        limits.compactor_blocks_retention_period,
        ModelDuration::from_days(1)
    );
    assert_eq!(limits.results_cache_ttl, ModelDuration::from_days(7));
}

// Upstream: pkg/util/validation/limits_test.go::TestMetricRelabelConfigLimitsLoadingFrom
#[test]
fn test_metric_relabel_config_limits_loading_from() {
    let yaml_input = "\nmetric_relabel_configs:\n- action: drop\n  source_labels: [le]\n  regex: .+\n";
    let l_yaml = LimitsConfig::unmarshal_yaml(yaml_input).unwrap();

    let map_val: serde_yaml::Value = serde_yaml::from_str(yaml_input).unwrap();
    let l_map = LimitsConfig::unmarshal_map_with_defaults(map_val, &LimitsConfig::default()).unwrap();

    let expected = vec![MetricRelabelConfig {
        source_labels: vec!["le".to_string()],
        separator: Some(";".to_string()),
        regex: Some(".+".to_string()),
        modulus: 0,
        target_label: None,
        replacement: Some("$1".to_string()),
        action: Some("drop".to_string()),
    }];
    assert_eq!(l_yaml.metric_relabel_configs, expected);
    assert_eq!(l_map.metric_relabel_configs, expected);
}

// Upstream: pkg/util/validation/limits_test.go::TestSmallestPositiveIntPerTenant
#[test]
fn test_smallest_positive_int_per_tenant() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_query_parallelism: 5,
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-b".to_string(),
        LimitsConfig {
            max_query_parallelism: 10,
            ..LimitsConfig::zeroed()
        },
    );
    let defaults = LimitsConfig {
        max_query_parallelism: 0,
        ..LimitsConfig::zeroed()
    };
    let ov = Overrides::with_static_map(defaults, tenant_limits);

    let cases: &[(&[&str], i64)] = &[
        (&[], 0),
        (&["tenant-a"], 5),
        (&["tenant-b"], 10),
        (&["tenant-c"], 0),
        (&["tenant-a", "tenant-b"], 5),
        (&["tenant-c", "tenant-d", "tenant-e"], 0),
        (&["tenant-a", "tenant-b", "tenant-c"], 0),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            smallest_positive_int_per_tenant(tenants, |id| ov.max_query_parallelism(id)),
            *exp
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestSmallestPositiveNonZeroIntPerTenant
#[test]
fn test_smallest_positive_non_zero_int_per_tenant() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_queriers_per_tenant: 5,
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-b".to_string(),
        LimitsConfig {
            max_queriers_per_tenant: 10,
            ..LimitsConfig::zeroed()
        },
    );
    let defaults = LimitsConfig {
        max_queriers_per_tenant: 0,
        ..LimitsConfig::zeroed()
    };
    let ov = Overrides::with_static_map(defaults, tenant_limits);

    let cases: &[(&[&str], i64)] = &[
        (&[], 0),
        (&["tenant-a"], 5),
        (&["tenant-b"], 10),
        (&["tenant-c"], 0),
        (&["tenant-a", "tenant-b"], 5),
        (&["tenant-c", "tenant-d", "tenant-e"], 0),
        (&["tenant-a", "tenant-b", "tenant-c"], 5),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            smallest_positive_non_zero_int_per_tenant(tenants, |id| ov.max_queriers_per_user(id)),
            *exp
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestSmallestPositiveNonZeroDurationPerTenant
#[test]
fn test_smallest_positive_non_zero_duration_per_tenant() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_partial_query_length: ModelDuration::from_hours(1),
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-b".to_string(),
        LimitsConfig {
            max_partial_query_length: ModelDuration::from_hours(4),
            ..LimitsConfig::zeroed()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::zeroed(), tenant_limits);

    let cases: &[(&[&str], ModelDuration)] = &[
        (&[], ModelDuration::ZERO),
        (&["tenant-a"], ModelDuration::from_hours(1)),
        (&["tenant-b"], ModelDuration::from_hours(4)),
        (&["tenant-c"], ModelDuration::ZERO),
        (&["tenant-a", "tenant-b"], ModelDuration::from_hours(1)),
        (&["tenant-c", "tenant-d", "tenant-e"], ModelDuration::ZERO),
        (
            &["tenant-a", "tenant-b", "tenant-c"],
            ModelDuration::from_hours(1),
        ),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            smallest_positive_non_zero_duration_per_tenant(tenants, |id| ov
                .max_partial_query_length(id)),
            *exp
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestLargestPositiveNonZeroDurationPerTenant
#[test]
fn test_largest_positive_non_zero_duration_per_tenant() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_partial_query_length: ModelDuration::from_hours(1),
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-b".to_string(),
        LimitsConfig {
            max_partial_query_length: ModelDuration::from_hours(4),
            ..LimitsConfig::zeroed()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::zeroed(), tenant_limits);

    let cases: &[(&[&str], ModelDuration)] = &[
        (&[], ModelDuration::ZERO),
        (&["tenant-a"], ModelDuration::from_hours(1)),
        (&["tenant-b"], ModelDuration::from_hours(4)),
        (&["tenant-c"], ModelDuration::ZERO),
        (&["tenant-a", "tenant-b"], ModelDuration::from_hours(4)),
        (&["tenant-c", "tenant-d", "tenant-e"], ModelDuration::ZERO),
        (
            &["tenant-a", "tenant-b", "tenant-c"],
            ModelDuration::from_hours(4),
        ),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            largest_positive_non_zero_duration_per_tenant(tenants, |id| ov
                .max_partial_query_length(id)),
            *exp
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestMinDurationPerTenant
#[test]
fn test_min_duration_per_tenant() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            results_cache_ttl_for_cardinality_query: ModelDuration::ZERO,
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-b".to_string(),
        LimitsConfig {
            results_cache_ttl_for_cardinality_query: ModelDuration::from_mins(1),
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-c".to_string(),
        LimitsConfig {
            results_cache_ttl_for_cardinality_query: ModelDuration::from_hours(1),
            ..LimitsConfig::zeroed()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::zeroed(), tenant_limits);

    let cases: &[(&[&str], ModelDuration)] = &[
        (&[], ModelDuration::ZERO),
        (&["tenant-a"], ModelDuration::ZERO),
        (&["tenant-b"], ModelDuration::from_mins(1)),
        (&["tenant-c"], ModelDuration::from_hours(1)),
        (&["tenant-a", "tenant-b"], ModelDuration::ZERO),
        (&["tenant-c", "tenant-b"], ModelDuration::from_mins(1)),
        (&["tenant-c", "tenant-d", "tenant-e"], ModelDuration::ZERO),
        (&["tenant-c", "tenant-b", "tenant-a"], ModelDuration::ZERO),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            min_duration_per_tenant(tenants, |id| ov.results_cache_ttl_for_cardinality_query(id)),
            *exp
        );
    }
    assert!(all_true_booleans_per_tenant(&["tenant-a"], |_| true));
}

// Upstream: pkg/util/validation/limits_test.go::TestMaxTotalQueryLengthWithoutDefault
#[test]
fn test_max_total_query_length_without_default() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_total_query_length: ModelDuration::from_hours(4),
            ..LimitsConfig::zeroed()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::zeroed(), tenant_limits);

    let cases: &[(&[&str], ModelDuration)] = &[
        (&[], ModelDuration::ZERO),
        (&["tenant-a"], ModelDuration::from_hours(4)),
        (&["tenant-b"], ModelDuration::ZERO),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            smallest_positive_non_zero_duration_per_tenant(tenants, |id| ov
                .max_total_query_length(id)),
            *exp
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestMaxTotalQueryLengthWithDefault
#[test]
fn test_max_total_query_length_with_default() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_total_query_length: ModelDuration::from_hours(4),
            ..LimitsConfig::zeroed()
        },
    );
    let defaults = LimitsConfig {
        max_total_query_length: ModelDuration::from_hours(3),
        ..LimitsConfig::zeroed()
    };
    let ov = Overrides::with_static_map(defaults, tenant_limits);

    let cases: &[(&[&str], ModelDuration)] = &[
        (&[], ModelDuration::ZERO),
        (&["tenant-a"], ModelDuration::from_hours(4)),
        (&["tenant-b"], ModelDuration::from_hours(3)),
    ];
    for (tenants, exp) in cases {
        assert_eq!(
            smallest_positive_non_zero_duration_per_tenant(tenants, |id| ov
                .max_total_query_length(id)),
            *exp
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestMaxPartialQueryLengthWithDefault
#[test]
fn test_max_partial_query_length_with_default() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_partial_query_length: ModelDuration::from_hours(1),
            ..LimitsConfig::zeroed()
        },
    );
    let defaults = LimitsConfig {
        max_partial_query_length: ModelDuration::from_hours(6),
        ..LimitsConfig::zeroed()
    };
    let ov = Overrides::with_static_map(defaults, tenant_limits);

    assert_eq!(ov.max_partial_query_length(""), ModelDuration::from_hours(6));
    assert_eq!(
        ov.max_partial_query_length("tenant-a"),
        ModelDuration::from_hours(1)
    );
    assert_eq!(
        ov.max_partial_query_length("tenant-b"),
        ModelDuration::from_hours(6)
    );
}

// Upstream: pkg/util/validation/limits_test.go::TestMaxPartialQueryLengthWithoutDefault
#[test]
fn test_max_partial_query_length_without_default() {
    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            max_partial_query_length: ModelDuration::from_hours(3),
            ..LimitsConfig::zeroed()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::zeroed(), tenant_limits);

    assert_eq!(ov.max_partial_query_length(""), ModelDuration::ZERO);
    assert_eq!(
        ov.max_partial_query_length("tenant-a"),
        ModelDuration::from_hours(3)
    );
    assert_eq!(ov.max_partial_query_length("tenant-b"), ModelDuration::ZERO);
}

// Upstream: pkg/util/validation/limits_test.go::TestDistributorIngestionArtificialDelay
#[test]
fn test_distributor_ingestion_artificial_delay() {
    let cases = [
        ("should not apply delay by default", "tenant-a", ModelDuration::ZERO),
        (
            "should apply delay if a plain delay has been configured for the tenant",
            "tenant-a",
            ModelDuration::from_secs(1),
        ),
    ];
    for (name, tenant_id, expected_delay) in cases {
        let tenant_limits = LimitsConfig {
            ingestion_artificial_delay: expected_delay,
            ..LimitsConfig::default()
        };
        let mut map = HashMap::new();
        map.insert(tenant_id.to_string(), tenant_limits);
        let ov = Overrides::with_static_map(LimitsConfig::zeroed(), map);
        assert_eq!(
            ov.distributor_ingestion_artificial_delay(tenant_id),
            expected_delay,
            "{name}"
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestAlertmanagerNotificationLimits
#[test]
fn test_alertmanager_notification_limits() {
    let cases = [
        (
            "no email specific limit",
            "\nalertmanager_notification_rate_limit: 100\n",
            100.0,
            100i64,
        ),
        (
            "zero limit",
            "\nalertmanager_notification_rate_limit: 100\n\nalertmanager_notification_rate_limit_per_integration:\n  email: 0\n",
            f64::INFINITY,
            i64::MAX,
        ),
        (
            "negative limit",
            "\nalertmanager_notification_rate_limit_per_integration:\n  email: -10\n",
            0.0,
            0,
        ),
        (
            "positive limit, negative burst",
            "\nalertmanager_notification_rate_limit_per_integration:\n  email: 222\n",
            222.0,
            222,
        ),
        (
            "infinte limit",
            "\nalertmanager_notification_rate_limit_per_integration:\n  email: .inf\n",
            f64::INFINITY,
            i64::MAX,
        ),
    ];

    for (name, input_yaml, expected_rate, expected_burst) in cases {
        let limits = LimitsConfig::unmarshal_yaml(input_yaml).expect(name);
        let ov = Overrides::new(limits);
        assert_eq!(ov.notification_rate_limit("user", "email"), expected_rate, "{name}");
        assert_eq!(ov.notification_burst_size("user", "email"), expected_burst, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestAlertmanagerNotificationLimitsOverrides
#[test]
fn test_alertmanager_notification_limits_overrides() {
    let base_yaml = "\nalertmanager_notification_rate_limit: 5\n\nalertmanager_notification_rate_limit_per_integration:\n email: 100\n";
    let override_generic = "\ntestuser:\n  alertmanager_notification_rate_limit: 333\n";
    let override_email = "\ntestuser:\n  alertmanager_notification_rate_limit_per_integration:\n    email: 7777\n";
    let override_both = "\ntestuser:\n  alertmanager_notification_rate_limit: 333\n\n  alertmanager_notification_rate_limit_per_integration:\n    email: 7777\n";
    let different_user = "\ndifferentuser:\n  alertmanager_notification_rate_limit_per_integration:\n    email: 500\n";

    let cases = [
        ("no overrides, pushover", "pushover", "", 5.0, 5i64),
        ("no overrides, email", "email", "", 100.0, 100),
        ("generic override, pushover", "pushover", override_generic, 333.0, 333),
        ("generic override, email", "email", override_generic, 100.0, 100),
        ("email limit override, pushover", "pushover", override_email, 5.0, 5),
        ("email limit override, email", "email", override_email, 7777.0, 7777),
        ("generic and email limit override, pushover", "pushover", override_both, 333.0, 333),
        ("generic and email limit override, email", "email", override_both, 7777.0, 7777),
        (
            "partial email limit override",
            "email",
            "\ntestuser:\n  alertmanager_notification_rate_limit_per_integration:\n    email: 500\n",
            500.0,
            500,
        ),
        ("different user override, pushover", "pushover", different_user, 5.0, 5),
        ("different user override, email", "email", different_user, 100.0, 100),
    ];

    for (name, integration, overrides_yaml, exp_rate, exp_burst) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(base_yaml).unwrap();
        let parsed = LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
            .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(ov.notification_rate_limit("testuser", integration), exp_rate, "{name}");
        assert_eq!(ov.notification_burst_size("testuser", integration), exp_burst, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMaxRulesPerRuleGroupLimits
#[test]
fn test_ruler_max_rules_per_rule_group_limits() {
    let cases = [
        (
            "no namespace specific limit",
            "\nruler_max_rules_per_rule_group: 100\n",
            100,
            "mynamespace",
        ),
        (
            "zero limit for the right namespace",
            "\nruler_max_rules_per_rule_group: 100\n\nruler_max_rules_per_rule_group_by_namespace:\n  mynamespace: 0\n",
            0,
            "mynamespace",
        ),
        (
            "other namespaces are not affected",
            "\nruler_max_rules_per_rule_group: 100\n\nruler_max_rules_per_rule_group_by_namespace:\n  mynamespace: 10\n",
            100,
            "othernamespace",
        ),
    ];
    for (name, input_yaml, expected, ns) in cases {
        let limits = LimitsConfig::unmarshal_yaml(input_yaml).unwrap();
        let ov = Overrides::new(limits);
        assert_eq!(ov.ruler_max_rules_per_rule_group("user", ns), expected, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMaxRulesPerRuleGroupLimitsOverrides
#[test]
fn test_ruler_max_rules_per_rule_group_limits_overrides() {
    let base_yaml = "\nruler_max_rules_per_rule_group: 5\n\nruler_max_rules_per_rule_group_by_namespace:\n  mynamespace: 10\n";
    let override_generic = "\ntestuser:\n  ruler_max_rules_per_rule_group: 333\n";
    let override_ns = "\ntestuser:\n  ruler_max_rules_per_rule_group_by_namespace:\n    mynamespace: 7777\n";
    let override_both = "\ntestuser:\n  ruler_max_rules_per_rule_group: 333\n\n  ruler_max_rules_per_rule_group_by_namespace:\n    mynamespace: 7777\n";
    let different_user = "\ndifferentuser:\n  ruler_max_rules_per_rule_group_by_namespace:\n    mynamespace: 500\n";

    let cases = [
        ("no overrides, mynamespace", "", "mynamespace", 10),
        ("no overrides, othernamespace", "", "othernamespace", 5),
        ("generic override, mynamespace", override_generic, "mynamespace", 10),
        ("generic override, othernamespace", override_generic, "othernamespace", 333),
        ("namespace limit override, mynamespace", override_ns, "mynamespace", 7777),
        ("namespace limit override, othernamespace", override_ns, "othernamespace", 5),
        ("generic and namespace limit override, mynamespace", override_both, "mynamespace", 7777),
        ("generic and namespace limit override, othernamespace", override_both, "othernamespace", 333),
        ("different user override, mynamespace", different_user, "mynamespace", 10),
        ("different user override, othernamespace", different_user, "othernamespace", 5),
    ];

    for (name, overrides_yaml, ns, exp) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(base_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(ov.ruler_max_rules_per_rule_group("testuser", ns), exp, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMaxRuleGroupsPerTenantLimits
#[test]
fn test_ruler_max_rule_groups_per_tenant_limits() {
    let cases = [
        (
            "no namespace specific limit",
            "\nruler_max_rule_groups_per_tenant: 200\n",
            200,
            "mynamespace",
        ),
        (
            "zero limit for the right namespace",
            "\nruler_max_rule_groups_per_tenant: 200\n\nruler_max_rule_groups_per_tenant_by_namespace:\n  mynamespace: 1\n",
            1,
            "mynamespace",
        ),
        (
            "other namespaces are not affected",
            "\nruler_max_rule_groups_per_tenant: 200\n\nruler_max_rule_groups_per_tenant_by_namespace:\n  mynamespace: 20\n",
            200,
            "othernamespace",
        ),
    ];
    for (name, input_yaml, expected, ns) in cases {
        let limits = LimitsConfig::unmarshal_yaml(input_yaml).unwrap();
        let ov = Overrides::new(limits);
        assert_eq!(ov.ruler_max_rule_groups_per_tenant("user", ns), expected, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMaxRuleGroupsPerTenantLimitsOverrides
#[test]
fn test_ruler_max_rule_groups_per_tenant_limits_overrides() {
    let base_yaml = "\nruler_max_rule_groups_per_tenant: 20\n\nruler_max_rule_groups_per_tenant_by_namespace:\n  mynamespace: 20\n";
    let override_generic = "\ntestuser:\n  ruler_max_rule_groups_per_tenant: 444\n";
    let override_ns = "\ntestuser:\n  ruler_max_rule_groups_per_tenant_by_namespace:\n    mynamespace: 8888\n";
    let override_both = "\ntestuser:\n  ruler_max_rule_groups_per_tenant: 444\n\n  ruler_max_rule_groups_per_tenant_by_namespace:\n    mynamespace: 8888\n";
    let different_user = "\ndifferentuser:\n  ruler_max_rule_groups_per_tenant_by_namespace:\n    mynamespace: 600\n";

    let cases = [
        ("no overrides, mynamespace", "", "mynamespace", 20),
        ("no overrides, othernamespace", "", "othernamespace", 20),
        ("generic override, mynamespace", override_generic, "mynamespace", 20),
        ("generic override, othernamespace", override_generic, "othernamespace", 444),
        ("namespace limit override, mynamespace", override_ns, "mynamespace", 8888),
        ("namespace limit override, othernamespace", override_ns, "othernamespace", 20),
        ("generic and namespace limit override, mynamespace", override_both, "mynamespace", 8888),
        ("generic and namespace limit override, othernamespace", override_both, "othernamespace", 444),
        ("different user override, mynamespace", different_user, "mynamespace", 20),
        ("different user override, othernamespace", different_user, "othernamespace", 20),
    ];

    for (name, overrides_yaml, ns, exp) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(base_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(ov.ruler_max_rule_groups_per_tenant("testuser", ns), exp, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerProtectedNamespacesOverrides
#[test]
fn test_ruler_protected_namespaces_overrides() {
    let cases = [
        (
            "no user specific protected namespaces",
            "\nruler_protected_namespaces: \"ns1,ns2\"\n",
            "",
            vec!["ns1", "ns2"],
        ),
        (
            "default limit for not specific user",
            "\nruler_protected_namespaces: \"ns1,ns2\"\n",
            "\nrandomuser:\n  ruler_protected_namespaces: \"ns3\"\n",
            vec!["ns1", "ns2"],
        ),
        (
            "overridden limit for specific user",
            "\nruler_protected_namespaces: \"ns1,ns2\"\n",
            "\nuser1:\n  ruler_protected_namespaces: \"ns3\"\n",
            vec!["ns3"],
        ),
    ];

    for (name, input_yaml, overrides_yaml, exp) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(input_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        let expected: Vec<String> = exp.into_iter().map(str::to_string).collect();
        assert_eq!(ov.ruler_protected_namespaces("user1"), expected, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMaxConcurrentRuleEvaluationsPerTenantOverrides
#[test]
fn test_ruler_max_concurrent_rule_evaluations_per_tenant_overrides() {
    let cases = [
        (
            "no user specific concurrency",
            "\nruler_max_independent_rule_evaluation_concurrency_per_tenant: 5\n",
            "",
            5i64,
        ),
        (
            "default limit for not specific user",
            "\nruler_max_independent_rule_evaluation_concurrency_per_tenant: 5\n",
            "\nrandomuser:\n  ruler_max_independent_rule_evaluation_concurrency_per_tenant: 10\n",
            5,
        ),
        (
            "overridden limit for specific user",
            "\nruler_max_independent_rule_evaluation_concurrency_per_tenant: 5\n",
            "\nuser1:\n  ruler_max_independent_rule_evaluation_concurrency_per_tenant: 15\n",
            15,
        ),
    ];

    for (name, input_yaml, overrides_yaml, exp) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(input_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(
            ov.ruler_max_independent_rule_evaluation_concurrency_per_tenant("user1"),
            exp,
            "{name}"
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMinRuleEvaluationIntervalPerTenantOverrides
#[test]
fn test_ruler_min_rule_evaluation_interval_per_tenant_overrides() {
    let cases = [
        (
            "no user specific minimum",
            "\nruler_min_rule_evaluation_interval: 15s\n",
            "",
            ModelDuration::from_secs(15),
        ),
        (
            "default limit if user not specified",
            "\nruler_min_rule_evaluation_interval: 5s\n",
            "\nrandomuser:\n  ruler_min_rule_evaluation_interval: 5m\n",
            ModelDuration::from_secs(5),
        ),
        (
            "overridden limit for specific user",
            "\nruler_min_rule_evaluation_interval: 5s\n",
            "\nuser1:\n  ruler_min_rule_evaluation_interval: 10m\n",
            ModelDuration::from_mins(10),
        ),
    ];

    for (name, input_yaml, overrides_yaml, exp) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(input_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(ov.ruler_min_rule_evaluation_interval("user1"), exp, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerMaxRuleEvaluationResults
#[test]
fn test_ruler_max_rule_evaluation_results() {
    let cases = [
        (
            "default limit",
            "\nruler_max_rule_evaluation_results: 100\n",
            "",
            100i64,
        ),
        (
            "zero disables limit",
            "\nruler_max_rule_evaluation_results: 0\n",
            "",
            0,
        ),
        (
            "user specific limit overrides default",
            "\nruler_max_rule_evaluation_results: 100\n",
            "\nuser1:\n  ruler_max_rule_evaluation_results: 500\n",
            500,
        ),
        (
            "zero user limit overrides default",
            "\nruler_max_rule_evaluation_results: 100\n",
            "\nuser1:\n  ruler_max_rule_evaluation_results: 0\n",
            0,
        ),
    ];

    for (name, input_yaml, overrides_yaml, exp) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(input_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(ov.ruler_max_rule_evaluation_results("user1"), exp, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestRulerAlertmanagerClientConfig
#[test]
fn test_ruler_alertmanager_client_config() {
    let mut custom_oauth = OAuth2ClientConfig {
        client_id: "myclient".to_string(),
        client_secret: "mysecret".to_string(),
        token_url: "http://token-url".to_string(),
        scopes: StringSliceCSV(vec!["abc".to_string(), "def".to_string()]),
        endpoint_params: LimitsMap::new(),
    };
    custom_oauth
        .endpoint_params
        .insert("key1".to_string(), "value1".to_string());

    let cases = [
        (
            "no override provided",
            "",
            "",
            RulerAlertmanagerClientConfig::default(),
        ),
        (
            "no user specific client config",
            "\nruler_alertmanager_client_config:\n  alertmanager_url: http://custom-url:8080\n  proxy_url: http://some-proxy:1234\n  oauth2:\n    client_id: myclient\n    client_secret: mysecret\n    token_url: http://token-url\n    scopes: abc,def\n    endpoint_params:\n      key1: value1\n",
            "",
            RulerAlertmanagerClientConfig {
                alertmanager_url: "http://custom-url:8080".to_string(),
                proxy_url: "http://some-proxy:1234".to_string(),
                tls_enabled: true,
                oauth2: custom_oauth.clone(),
                ..Default::default()
            },
        ),
        (
            "overridden config for specific user",
            "\nruler_alertmanager_client_config:\n  alertmanager_url: http://some-base-url:8080\n",
            "\nuser1:\n  ruler_alertmanager_client_config:\n    alertmanager_url: http://custom-url-for-this-tenant:8080\n    proxy_url: http://some-proxy:1234\n    oauth2:\n      client_id: myclient\n      client_secret: mysecret\n      token_url: http://token-url\n      scopes: abc,def\n      endpoint_params:\n        key1: value1\n",
            RulerAlertmanagerClientConfig {
                alertmanager_url: "http://custom-url-for-this-tenant:8080".to_string(),
                proxy_url: "http://some-proxy:1234".to_string(),
                tls_enabled: true,
                oauth2: custom_oauth,
                ..Default::default()
            },
        ),
    ];

    for (name, base_yaml, overrides_yaml, expected_cfg) in cases {
        let base_limits = LimitsConfig::unmarshal_yaml(base_yaml).unwrap();
        let parsed =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(overrides_yaml, &base_limits)
                .unwrap();
        let map: HashMap<String, LimitsConfig> = parsed
            .into_iter()
            .filter_map(|(k, v)| v.map(|l| (k, l)))
            .collect();
        let ov = Overrides::with_static_map(base_limits, map);
        assert_eq!(ov.ruler_alertmanager_client_config("user1"), expected_cfg, "{name}");
    }
}

fn format_trackers_map(m: &LimitsMap<String>) -> String {
    m.iter()
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>()
        .join(";")
}

// Upstream: pkg/util/validation/limits_test.go::TestActiveSeriesCustomTrackersConfig
#[test]
fn test_active_series_custom_trackers_config() {
    let cases = [
        (
            "no base and no additional config",
            "\nmax_global_series_per_user: 10\n",
            "",
            "",
            "",
        ),
        (
            "only base config is set",
            "\nactive_series_custom_trackers:\n  base_1: '{foo=\"base_1\"}'",
            "base_1:{foo=\"base_1\"}",
            "",
            "base_1:{foo=\"base_1\"}",
        ),
        (
            "only additional config is set",
            "\nactive_series_additional_custom_trackers:\n  additional_1: '{foo=\"additional_1\"}'",
            "",
            "additional_1:{foo=\"additional_1\"}",
            "additional_1:{foo=\"additional_1\"}",
        ),
        (
            "both base and additional configs are set",
            "\nactive_series_custom_trackers:\n  base_1: '{foo=\"base_1\"}'\n  common_1: '{foo=\"base\"}'\n\nactive_series_additional_custom_trackers:\n  additional_1: '{foo=\"additional_1\"}'\n  common_1: '{foo=\"additional\"}'",
            "base_1:{foo=\"base_1\"};common_1:{foo=\"base\"}",
            "additional_1:{foo=\"additional_1\"};common_1:{foo=\"additional\"}",
            "additional_1:{foo=\"additional_1\"};base_1:{foo=\"base_1\"};common_1:{foo=\"additional\"}",
        ),
    ];

    for (name, cfg, exp_base, exp_add, exp_merged) in cases {
        for with_default_values in [true, false] {
            // Go TestMain sets defaultLimits = getDefaultLimits(), so UnmarshalYAML always
            // seeds from getDefaultLimits() regardless of whether flagext.DefaultValues was
            // called on the destination struct beforehand.
            let _pre_defaults = if with_default_values {
                LimitsConfig::default()
            } else {
                LimitsConfig::zeroed()
            };
            let limits = LimitsConfig::unmarshal_yaml_with_defaults(cfg, &LimitsConfig::default()).unwrap();
            let ov = Overrides::new(limits);
            assert_eq!(
                format_trackers_map(&ov.for_tenant("test").active_series_custom_trackers),
                exp_base,
                "{name} default={with_default_values}"
            );
            assert_eq!(
                format_trackers_map(
                    &ov.for_tenant("user")
                        .active_series_additional_custom_trackers
                ),
                exp_add,
                "{name} default={with_default_values}"
            );
            assert_eq!(
                format_trackers_map(&ov.active_series_custom_trackers_config("user")),
                exp_merged,
                "{name} default={with_default_values}"
            );
        }
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestActiveSeriesCustomTrackersConfig_Concurrency
#[test]
fn test_active_series_custom_trackers_config_concurrency() {
    let cfg = "\nactive_series_custom_trackers:\n  base_1: '{foo=\"base_1\"}'\n  common_1: '{foo=\"base\"}'\n\nactive_series_additional_custom_trackers:\n  additional_1: '{foo=\"additional_1\"}'\n  common_1: '{foo=\"additional\"}'";
    for _ in 0..25 {
        let limits = LimitsConfig::unmarshal_yaml(cfg).unwrap();
        let ov = Arc::new(Overrides::new(limits));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let ov = Arc::clone(&ov);
            handles.push(thread::spawn(move || {
                let merged = ov.active_series_custom_trackers_config("user");
                assert_eq!(
                    merged.get("common_1").map(String::as_str),
                    Some("{foo=\"additional\"}")
                );
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestUnmarshalYAML_ShouldValidateConfig
#[test]
fn test_unmarshal_yaml_should_validate_config() {
    let cases = [
        (
            "should fail on invalid metric_relabel_configs",
            "\nmetric_relabel_configs:\n  -\n",
            "invalid metric_relabel_configs",
        ),
        (
            "should fail on negative max_estimated_fetched_chunks_per_query_multiplier",
            "max_estimated_fetched_chunks_per_query_multiplier: -0.1",
            "invalid value for -querier.max-estimated-fetched-chunks-per-query-multiplier: must be 0 or greater than or equal to 1",
        ),
        (
            "should pass on max_estimated_fetched_chunks_per_query_multiplier = 0",
            "max_estimated_fetched_chunks_per_query_multiplier: 0",
            "",
        ),
        (
            "should fail on max_estimated_fetched_chunks_per_query_multiplier greater than 0 but less than 1",
            "max_estimated_fetched_chunks_per_query_multiplier: 0.9",
            "invalid value for -querier.max-estimated-fetched-chunks-per-query-multiplier: must be 0 or greater than or equal to 1",
        ),
        (
            "should pass on max_estimated_fetched_chunks_per_query_multiplier = 1",
            "max_estimated_fetched_chunks_per_query_multiplier: 1",
            "",
        ),
        (
            "should pass on max_estimated_fetched_chunks_per_query_multiplier greater than 1",
            "max_estimated_fetched_chunks_per_query_multiplier: 1.1",
            "",
        ),
        (
            "should fail on invalid ingest_storage_read_consistency",
            "ingest_storage_read_consistency: xyz",
            "invalid ingest storage read consistency (supported values: eventual, strong)",
        ),
    ];

    for (name, cfg, expected_err) in cases {
        let res = LimitsConfig::unmarshal_yaml(cfg);
        if expected_err.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            let err = res.unwrap_err().to_string();
            assert!(err.contains(expected_err), "{name}: got {err:?}");
        }
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestUnmarshalYAML_ShouldRoundShardCounts
#[test]
fn test_unmarshal_yaml_should_round_shard_counts() {
    let cfg = "\nquery_sharding_total_shards: 10\ncompactor_split_and_merge_shards: 10\ncompactor_ooo_split_and_merge_shards: 5\n";
    let limits = LimitsConfig::unmarshal_yaml(cfg).unwrap();
    assert_eq!(16, limits.query_sharding_total_shards);
    assert_eq!(16, limits.compactor_split_and_merge_shards);
    assert_eq!(8, limits.compactor_ooo_split_and_merge_shards);
}

// Upstream: pkg/util/validation/limits_test.go::TestUnmarshalJSON_ShouldValidateConfig
#[test]
fn test_unmarshal_json_should_validate_config() {
    let err = LimitsConfig::unmarshal_json(r#"{"metric_relabel_configs": [null]}"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid metric_relabel_configs"), "got {err:?}");
}

// Upstream: pkg/util/validation/limits_test.go::TestLimits_Validate
#[test]
fn test_limits_validate() {
    struct Case {
        name: &'static str,
        cfg: LimitsConfig,
        expected_err: Option<&'static str>,
        verify: Option<fn(&LimitsConfig)>,
    }

    let cases = vec![
        Case {
            name: "should fail if max update timeout jitter is negative",
            cfg: LimitsConfig {
                ha_tracker_update_timeout_jitter_max: ModelDuration::from_nanos(-1),
                ..Default::default()
            },
            expected_err: Some("HA tracker max update timeout jitter shouldn't be negative"),
            verify: None,
        },
        Case {
            name: "should fail if failover timeout is < update timeout + jitter + 1 sec",
            cfg: LimitsConfig {
                ha_tracker_failover_timeout: ModelDuration::from_secs(5),
                ha_tracker_update_timeout: ModelDuration::from_secs(4),
                ha_tracker_update_timeout_jitter_max: ModelDuration::from_secs(2),
                ..Default::default()
            },
            expected_err: Some(
                "HA Tracker failover timeout (5s) must be at least 1s greater than update timeout - max jitter (7s)",
            ),
            verify: None,
        },
        Case {
            name: "should pass if failover timeout is >= update timeout + jitter + 1 sec",
            cfg: LimitsConfig {
                ha_tracker_failover_timeout: ModelDuration::from_secs(7),
                ha_tracker_update_timeout: ModelDuration::from_secs(4),
                ha_tracker_update_timeout_jitter_max: ModelDuration::from_secs(2),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "should fail if float_chunk_encoding is not a known encoding",
            cfg: LimitsConfig {
                float_chunk_encoding: "XOR2".to_string(),
                ..Default::default()
            },
            expected_err: Some("invalid float chunk encoding (supported values: xor, xor2)"),
            verify: None,
        },
        Case {
            name: "should pass if float_chunk_encoding is xor2",
            cfg: LimitsConfig {
                float_chunk_encoding: "xor2".to_string(),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "should fail if float_chunk_encoding is empty",
            cfg: LimitsConfig {
                float_chunk_encoding: String::new(),
                ..Default::default()
            },
            expected_err: Some("invalid float chunk encoding (supported values: xor, xor2)"),
            verify: None,
        },
        Case {
            name: "should fail if histogram_chunk_encoding is not a known encoding",
            cfg: LimitsConfig {
                histogram_chunk_encoding: "HISTOGRAM_ST".to_string(),
                ..Default::default()
            },
            expected_err: Some(
                "invalid histogram chunk encoding (supported values: histogram, histogram_st)",
            ),
            verify: None,
        },
        Case {
            name: "should pass if histogram_chunk_encoding is histogram",
            cfg: LimitsConfig {
                histogram_chunk_encoding: "histogram".to_string(),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "should pass if histogram_chunk_encoding is histogram_st",
            cfg: LimitsConfig {
                histogram_chunk_encoding: "histogram_st".to_string(),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "should pass if histogram_chunk_encoding is empty",
            cfg: LimitsConfig {
                histogram_chunk_encoding: String::new(),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "otel UnderscoreEscapingWithoutSuffixes + legacy + false passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithoutSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "otel UnderscoreEscapingWithSuffixes + legacy + true passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "otel NoUTF8EscapingWithSuffixes + utf8 + true passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(
                    "NoUTF8EscapingWithSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "otel NoTranslation + utf8 + false passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "otel empty + legacy + false passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy(String::new()),
                ..Default::default()
            },
            expected_err: None,
            verify: Some(|cfg| assert_eq!(cfg.otel_translation_strategy.0, "")),
        },
        Case {
            name: "otel empty + legacy + true passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(String::new()),
                ..Default::default()
            },
            expected_err: None,
            verify: Some(|cfg| assert_eq!(cfg.otel_translation_strategy.0, "")),
        },
        Case {
            name: "otel empty + utf8 + true passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(String::new()),
                ..Default::default()
            },
            expected_err: None,
            verify: Some(|cfg| assert_eq!(cfg.otel_translation_strategy.0, "")),
        },
        Case {
            name: "otel empty + utf8 + false passes",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy(String::new()),
                ..Default::default()
            },
            expected_err: None,
            verify: Some(|cfg| assert_eq!(cfg.otel_translation_strategy.0, "")),
        },
        Case {
            name: "otel UnderscoreEscapingWithoutSuffixes + utf8 + false fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithoutSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy UnderscoreEscapingWithoutSuffixes is not allowed unless validation scheme is legacy",
            ),
            verify: None,
        },
        Case {
            name: "otel UnderscoreEscapingWithoutSuffixes + legacy + true fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithoutSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy UnderscoreEscapingWithoutSuffixes is not allowed unless metric suffixes are disabled",
            ),
            verify: None,
        },
        Case {
            name: "otel UnderscoreEscapingWithSuffixes + utf8 + true fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy UnderscoreEscapingWithSuffixes is not allowed unless validation scheme is legacy",
            ),
            verify: None,
        },
        Case {
            name: "otel UnderscoreEscapingWithSuffixes + legacy + false fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy UnderscoreEscapingWithSuffixes is not allowed unless metric suffixes are enabled",
            ),
            verify: None,
        },
        Case {
            name: "otel NoUTF8EscapingWithSuffixes + legacy + true fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy(
                    "NoUTF8EscapingWithSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy NoUTF8EscapingWithSuffixes is not allowed unless validation scheme is utf8",
            ),
            verify: None,
        },
        Case {
            name: "otel NoUTF8EscapingWithSuffixes + utf8 + false fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy(
                    "NoUTF8EscapingWithSuffixes".to_string(),
                ),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy NoUTF8EscapingWithSuffixes is not allowed unless metric suffixes are enabled",
            ),
            verify: None,
        },
        Case {
            name: "otel NoTranslation + legacy + false fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Legacy,
                otel_metric_suffixes_enabled: Some(false),
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy NoTranslation is not allowed unless validation scheme is utf8",
            ),
            verify: None,
        },
        Case {
            name: "otel NoTranslation + utf8 + true fails",
            cfg: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_metric_suffixes_enabled: Some(true),
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..Default::default()
            },
            expected_err: Some(
                "OTLP translation strategy NoTranslation is not allowed unless metric suffixes are disabled",
            ),
            verify: None,
        },
        Case {
            name: "truncate + max_label_value_length < hash len fails",
            cfg: LimitsConfig {
                label_value_length_over_limit_strategy:
                    LabelValueLengthOverLimitStrategy::Truncate,
                max_label_value_length: (LABEL_VALUE_HASH_LEN - 1) as i64,
                ..Default::default()
            },
            expected_err: Some(
                "cannot set -validation.label-value-length-over-limit-strategy to \"truncate\": label value hash suffix would exceed max label value length of 70",
            ),
            verify: None,
        },
        Case {
            name: "drop + max_label_value_length < hash len fails",
            cfg: LimitsConfig {
                label_value_length_over_limit_strategy: LabelValueLengthOverLimitStrategy::Drop,
                max_label_value_length: (LABEL_VALUE_HASH_LEN - 1) as i64,
                ..Default::default()
            },
            expected_err: Some(
                "cannot set -validation.label-value-length-over-limit-strategy to \"drop\": label value hash suffix would exceed max label value length of 70",
            ),
            verify: None,
        },
        Case {
            name: "truncate + max_label_value_length == hash len passes",
            cfg: LimitsConfig {
                label_value_length_over_limit_strategy:
                    LabelValueLengthOverLimitStrategy::Truncate,
                max_label_value_length: LABEL_VALUE_HASH_LEN as i64,
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "drop + max_label_value_length > hash len passes",
            cfg: LimitsConfig {
                label_value_length_over_limit_strategy: LabelValueLengthOverLimitStrategy::Drop,
                max_label_value_length: (LABEL_VALUE_HASH_LEN + 1) as i64,
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "cost_attribution_trackers valid passes",
            cfg: LimitsConfig {
                cost_attribution_trackers: [(
                    "by-team".to_string(),
                    CostAttributionTrackerConfig {
                        labels: vec![
                            CostAttributionLabel {
                                input: "team".to_string(),
                                output: "my_team".to_string(),
                            },
                            CostAttributionLabel {
                                input: "service".to_string(),
                                output: "my_service".to_string(),
                            },
                        ],
                        internal: false,
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "first cost attribution input label has reserved prefix passes",
            cfg: LimitsConfig {
                cost_attribution_trackers: [(
                    "by-team".to_string(),
                    CostAttributionTrackerConfig {
                        labels: vec![
                            CostAttributionLabel {
                                input: "__team__".to_string(),
                                output: "my_team".to_string(),
                            },
                            CostAttributionLabel {
                                input: "service".to_string(),
                                output: "my_service".to_string(),
                            },
                        ],
                        internal: false,
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            expected_err: None,
            verify: None,
        },
        Case {
            name: "cost attribution output label invalid fails",
            cfg: LimitsConfig {
                cost_attribution_trackers: [(
                    "by-team".to_string(),
                    CostAttributionTrackerConfig {
                        labels: vec![
                            CostAttributionLabel {
                                input: "team".to_string(),
                                output: "my_team".to_string(),
                            },
                            CostAttributionLabel {
                                input: "service".to_string(),
                                output: "__my_service__".to_string(),
                            },
                        ],
                        internal: false,
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            expected_err: Some(
                "cost attribution tracker \"by-team\": invalid cost attribution output label: \"service:__my_service__\"",
            ),
            verify: None,
        },
        Case {
            name: "both deprecated and new cost attribution fields fails",
            cfg: LimitsConfig {
                cost_attribution_labels_structured: vec![CostAttributionLabel {
                    input: "team".to_string(),
                    output: String::new(),
                }],
                cost_attribution_trackers: [(
                    "by-team".to_string(),
                    CostAttributionTrackerConfig {
                        labels: vec![CostAttributionLabel {
                            input: "team".to_string(),
                            output: String::new(),
                        }],
                        internal: false,
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            expected_err: Some(
                "cost_attribution_labels_structured and cost_attribution_trackers are mutually exclusive; use cost_attribution_trackers only",
            ),
            verify: None,
        },
        Case {
            name: "rounds shard counts up to next power of two",
            cfg: LimitsConfig {
                query_sharding_total_shards: 10,
                compactor_split_and_merge_shards: 10,
                compactor_ooo_split_and_merge_shards: 5,
                ..Default::default()
            },
            expected_err: None,
            verify: Some(|cfg| {
                assert_eq!(16, cfg.query_sharding_total_shards);
                assert_eq!(16, cfg.compactor_split_and_merge_shards);
                assert_eq!(8, cfg.compactor_ooo_split_and_merge_shards);
            }),
        },
        Case {
            name: "leaves power-of-two and disabled shard counts untouched",
            cfg: LimitsConfig {
                query_sharding_total_shards: 16,
                compactor_split_and_merge_shards: 0,
                compactor_ooo_split_and_merge_shards: 1,
                ..Default::default()
            },
            expected_err: None,
            verify: Some(|cfg| {
                assert_eq!(16, cfg.query_sharding_total_shards);
                assert_eq!(0, cfg.compactor_split_and_merge_shards);
                assert_eq!(1, cfg.compactor_ooo_split_and_merge_shards);
            }),
        },
    ];

    for mut tc in cases {
        let res = tc.cfg.validate();
        if let Some(exp) = tc.expected_err {
            let msg = res.unwrap_err().to_string();
            let stripped = msg.strip_prefix("error validating config: ").unwrap_or(&msg);
            assert_eq!(stripped, exp, "{}", tc.name);
        } else {
            res.unwrap_or_else(|e| panic!("{}: {e}", tc.name));
            if let Some(verify) = tc.verify {
                verify(&tc.cfg);
            }
        }
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestCostAttributionTrackerLabelsAreSortedAfterUnmarshal
#[test]
fn test_cost_attribution_tracker_labels_are_sorted_after_unmarshal() {
    let yaml_input = "\ncost_attribution_trackers:\n  by-team:\n    labels:\n      - input: z_label\n      - input: a_label\nadditional_cost_attribution_trackers:\n  by-svc:\n    labels:\n      - input: service\n      - input: env\n";
    let cfg = LimitsConfig::unmarshal_yaml(yaml_input).unwrap();
    assert_eq!(
        cfg.cost_attribution_trackers["by-team"].labels,
        vec![
            CostAttributionLabel {
                input: "a_label".to_string(),
                output: String::new(),
            },
            CostAttributionLabel {
                input: "z_label".to_string(),
                output: String::new(),
            },
        ]
    );
    assert_eq!(
        cfg.additional_cost_attribution_trackers["by-svc"].labels,
        vec![
            CostAttributionLabel {
                input: "env".to_string(),
                output: String::new(),
            },
            CostAttributionLabel {
                input: "service".to_string(),
                output: String::new(),
            },
        ]
    );
}

// Upstream: pkg/util/validation/limits_test.go::TestCostAttributionLabelsStructuredMigrationOnUnmarshal
#[test]
fn test_cost_attribution_labels_structured_migration_on_unmarshal() {
    {
        let cfg = LimitsConfig::unmarshal_yaml(
            "\ncost_attribution_labels_structured:\n  - input: team\n    output: my_team\n",
        )
        .unwrap();
        assert!(cfg.cost_attribution_labels_structured.is_empty());
        let mut expected = CostAttributionTrackerConfigs::new();
        expected.insert(
            "cost-attribution".to_string(),
            CostAttributionTrackerConfig {
                labels: vec![CostAttributionLabel {
                    input: "team".to_string(),
                    output: "my_team".to_string(),
                }],
                internal: false,
            },
        );
        assert_eq!(cfg.cost_attribution_trackers, expected);
    }

    {
        let err = LimitsConfig::unmarshal_yaml(
            "\ncost_attribution_labels_structured:\n  - input: team\ncost_attribution_trackers:\n  by-team:\n    labels:\n      - input: team\n",
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(
                "cost_attribution_labels_structured and cost_attribution_trackers are mutually exclusive; use cost_attribution_trackers only"
            ),
            "got {err:?}"
        );
    }

    {
        let cfg = LimitsConfig::unmarshal_yaml(
            "\ncost_attribution_labels_structured:\n  - input: team\n    output: my_team\nadditional_cost_attribution_trackers:\n  by-svc:\n    labels:\n      - input: service\n",
        )
        .unwrap();
        assert!(cfg.cost_attribution_labels_structured.is_empty());
        assert_eq!(
            cfg.cost_attribution_trackers["cost-attribution"].labels,
            vec![CostAttributionLabel {
                input: "team".to_string(),
                output: "my_team".to_string(),
            }]
        );
        assert_eq!(
            cfg.additional_cost_attribution_trackers["by-svc"].labels,
            vec![CostAttributionLabel {
                input: "service".to_string(),
                output: String::new(),
            }]
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestCostAttributionTrackerLabelsAreSortedAfterFlagParsing
#[test]
fn test_cost_attribution_tracker_labels_are_sorted_after_flag_parsing() {
    let mut cfg = LimitsConfig::default();
    let mut fs = FlagSet::new();
    cfg.register_flags_into(&mut fs, |c: &LimitsConfig| c, |c: &mut LimitsConfig| c);
    fs.parse(
        &mut cfg,
        &[
            "-validation.cost-attribution-trackers".into(),
            r#"{"by-team":{"labels":[{"input":"z_label"},{"input":"a_label"}]}}"#.into(),
        ],
    )
    .unwrap();
    assert_eq!(
        cfg.cost_attribution_trackers["by-team"].labels,
        vec![
            CostAttributionLabel {
                input: "a_label".to_string(),
                output: String::new(),
            },
            CostAttributionLabel {
                input: "z_label".to_string(),
                output: String::new(),
            },
        ]
    );
}

// Upstream: pkg/util/validation/limits_test.go::TestLimits_NormalizesBlockedLabelNamesForLabelValueBytes
#[test]
fn test_limits_normalizes_blocked_label_names_for_label_value_bytes() {
    let mut defaults = LimitsConfig {
        blocked_label_names_for_label_value_bytes: StringSliceCSV(vec![
            "logs".to_string(),
            "__name__".to_string(),
        ]),
        ..LimitsConfig::default()
    };

    let overrides = LimitsConfig::unmarshal_overrides_yaml_with_defaults(
        "\nexplicit:\n  blocked_label_names_for_label_value_bytes: logs,logs,__name__,,message\ninherited: {}\n",
        &defaults,
    )
    .unwrap();

    assert_eq!(
        overrides["explicit"]
            .as_ref()
            .unwrap()
            .blocked_label_names_for_label_value_bytes,
        StringSliceCSV(vec!["logs".to_string(), "message".to_string()])
    );
    assert_eq!(
        overrides["inherited"]
            .as_ref()
            .unwrap()
            .blocked_label_names_for_label_value_bytes,
        StringSliceCSV(vec!["logs".to_string()])
    );
    assert_eq!(
        defaults.blocked_label_names_for_label_value_bytes,
        StringSliceCSV(vec!["logs".to_string(), "__name__".to_string()])
    );

    defaults.validate().unwrap();
    assert_eq!(
        defaults.blocked_label_names_for_label_value_bytes,
        StringSliceCSV(vec!["logs".to_string()])
    );
}

// Upstream: pkg/util/validation/limits_test.go::TestLimits_ValidateMaxActiveSeriesAdditionalCustomTrackers
#[test]
fn test_limits_validate_max_active_series_additional_custom_trackers() {
    let mk_map = |pairs: &[(&str, &str)]| -> LimitsMap<String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    };

    let cases = [
        (
            "additional config within limit",
            5i64,
            mk_map(&[]),
            mk_map(&[("tracker1", r#"{foo="bar"}"#), ("tracker2", r#"{baz="qux"}"#)]),
            "",
        ),
        (
            "additional config exceeds limit",
            2,
            mk_map(&[]),
            mk_map(&[
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
            ]),
            "active_series_additional_custom_trackers validation failed: the number of custom trackers [3] exceeds the configured limit [2]",
        ),
        (
            "base config not affected by limit",
            2,
            mk_map(&[
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
                ("tracker4", r#"{ping="pong"}"#),
                ("tracker5", r#"{alpha="beta"}"#),
            ]),
            mk_map(&[]),
            "",
        ),
        (
            "limit only applies to additional config not merged",
            2,
            mk_map(&[
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
            ]),
            mk_map(&[
                ("tracker4", r#"{ping="pong"}"#),
                ("tracker5", r#"{alpha="beta"}"#),
            ]),
            "",
        ),
        (
            "limit 0 means unlimited for additional config",
            0,
            mk_map(&[]),
            mk_map(&[
                ("tracker1", r#"{foo="bar"}"#),
                ("tracker2", r#"{baz="qux"}"#),
                ("tracker3", r#"{hello="world"}"#),
                ("tracker4", r#"{ping="pong"}"#),
                ("tracker5", r#"{alpha="beta"}"#),
                ("tracker6", r#"{gamma="delta"}"#),
                ("tracker7", r#"{epsilon="zeta"}"#),
                ("tracker8", r#"{eta="theta"}"#),
                ("tracker9", r#"{iota="kappa"}"#),
                ("tracker10", r#"{lambda="mu"}"#),
            ]),
            "",
        ),
    ];

    for (name, max_add, base, add, expected_err) in cases {
        let mut cfg = LimitsConfig {
            max_active_series_additional_custom_trackers: max_add,
            active_series_custom_trackers: base,
            active_series_additional_custom_trackers: add,
            ..Default::default()
        };
        let res = cfg.validate();
        if expected_err.is_empty() {
            assert!(res.is_ok(), "{name}: {res:?}");
        } else {
            let msg = res.unwrap_err().to_string();
            let stripped = msg.strip_prefix("error validating config: ").unwrap_or(&msg);
            assert_eq!(stripped, expected_err, "{name}");
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StructExtension {
    foo: i64,
}

impl LimitsExtensionDefault for StructExtension {
    fn extension_default() -> Self {
        Self { foo: 42 }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
struct StringExtension(String);

impl LimitsExtensionDefault for StringExtension {
    fn extension_default() -> Self {
        Self("default string extension value".to_string())
    }
}

struct ExtCleanup;
impl Drop for ExtCleanup {
    fn drop(&mut self) {
        clear_registered_extensions();
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestExtensions
#[test]
fn test_extensions() {
    let _cleanup = ExtCleanup;
    clear_registered_extensions();

    let get_ext_struct = must_register_extension::<StructExtension>("test_extension_struct");
    let get_ext_string = must_register_extension::<StringExtension>("test_extension_string");

    let cfg = r#"{"user": {"test_extension_struct": {"foo": 1}, "test_extension_string": "bar"}}"#;

    {
        let overrides =
            LimitsConfig::unmarshal_overrides_yaml_with_defaults(cfg, &LimitsConfig::default())
                .unwrap();
        let u = overrides["user"].as_ref();
        assert_eq!(get_ext_struct(u), StructExtension { foo: 1 });
        assert_eq!(get_ext_string(u), StringExtension("bar".to_string()));
    }

    {
        let raw: BTreeMap<String, serde_json::Value> = serde_json::from_str(cfg).unwrap();
        let u = LimitsConfig::unmarshal_json(&raw["user"].to_string()).unwrap();
        assert_eq!(get_ext_struct(Some(&u)), StructExtension { foo: 1 });
        assert_eq!(get_ext_string(Some(&u)), StringExtension("bar".to_string()));
    }

    {
        let res = std::panic::catch_unwind(|| {
            let _ = must_register_extension::<StructExtension>("foo");
            let _ = must_register_extension::<StructExtension>("foo");
        });
        assert!(res.is_err());
    }

    {
        let res = std::panic::catch_unwind(|| {
            let _ = must_register_extension::<StringExtension>("max_global_series_per_user");
        });
        assert!(res.is_err());
    }

    {
        let res = std::panic::catch_unwind(|| {
            let _ = must_register_extension::<StringExtension>("");
        });
        assert!(res.is_err());
    }

    {
        let limits = LimitsConfig::unmarshal_json("{}").unwrap();
        assert_eq!(get_ext_struct(Some(&limits)), StructExtension { foo: 42 });
        assert_eq!(
            get_ext_string(Some(&limits)),
            StringExtension("default string extension value".to_string())
        );
    }

    {
        let mut limits = LimitsConfig::zeroed();
        limits.register_extensions_defaults();
        assert_eq!(get_ext_struct(Some(&limits)), StructExtension { foo: 42 });
        assert_eq!(
            get_ext_string(Some(&limits)),
            StringExtension("default string extension value".to_string())
        );
    }

    {
        let custom_defaults = LimitsConfig {
            request_rate: 100.0,
            ..LimitsConfig::default()
        };
        let limits = LimitsConfig::unmarshal_limits_value_map_yaml("foo:", &custom_defaults).unwrap();
        let foo_limits = limits.get("foo").unwrap();
        assert_eq!(0.0, foo_limits.request_rate);
        assert_eq!(get_ext_struct(Some(foo_limits)), StructExtension::default());
        assert_eq!(get_ext_string(Some(foo_limits)), StringExtension::default());
    }

    {
        let def = LimitsConfig::unmarshal_json_with_defaults(
            r#"{"test_extension_string": "default"}"#,
            &LimitsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            get_ext_string(Some(&def)),
            StringExtension("default".to_string())
        );

        let overrides1 = LimitsConfig::unmarshal_overrides_yaml_with_defaults(
            r#"{"one": {"test_extension_string": "one"}, "two": {"test_extension_string": "two"}}"#,
            &def,
        )
        .unwrap();
        assert_eq!(
            get_ext_string(overrides1["one"].as_ref()),
            StringExtension("one".to_string())
        );
        assert_eq!(
            get_ext_string(overrides1["two"].as_ref()),
            StringExtension("two".to_string())
        );

        let overrides2 = LimitsConfig::unmarshal_overrides_yaml_with_defaults(
            r#"{"three": {"test_extension_string": "three"}}"#,
            &def,
        )
        .unwrap();
        assert_eq!(
            get_ext_string(overrides2["three"].as_ref()),
            StringExtension("three".to_string())
        );
        assert_eq!(
            get_ext_string(overrides1["one"].as_ref()),
            StringExtension("one".to_string())
        );
        assert_eq!(
            get_ext_string(overrides1["two"].as_ref()),
            StringExtension("two".to_string())
        );
        assert_eq!(
            get_ext_string(Some(&def)),
            StringExtension("default".to_string())
        );
    }

    assert_eq!(get_ext_struct(None), StructExtension::extension_default());
}

// Upstream: pkg/util/validation/limits_test.go::TestExtensionMarshalling
#[test]
fn test_extension_marshalling() {
    let _cleanup = ExtCleanup;
    clear_registered_extensions();

    let _ = must_register_extension::<StructExtension>("test_extension_struct");
    let _ = must_register_extension::<StringExtension>("test_extension_string");

    {
        let l = LimitsConfig::zeroed();
        let yaml_val = l.to_yaml_value_with_extensions().unwrap();
        let mut root = serde_yaml::Mapping::new();
        root.insert(serde_yaml::Value::String("test".to_string()), yaml_val);
        let yaml_str = yaml_v3_emit::marshal_struct(&serde_yaml::Value::Mapping(root)).unwrap();
        assert!(
            yaml_str.contains(
                "test:\n    test_extension_struct:\n        foo: 0\n    test_extension_string: \"\"\n    max_active_series_per_user: 0\n    active_series_limit_response_code: 0\n    request_rate: 0"
            ),
            "yaml:\n{yaml_str}"
        );

        let mut json_root = serde_json::Map::new();
        json_root.insert("test".to_string(), l.to_json_value_with_extensions().unwrap());
        let json_str = serde_json::to_string(&serde_json::Value::Object(json_root)).unwrap();
        assert!(json_str.contains(r#""test_extension_struct":{"foo":0}"#), "json: {json_str}");
        assert!(json_str.contains(r#""test_extension_string":"""#), "json: {json_str}");
        assert!(json_str.contains(r#""active_series_limit_response_code":0"#), "json: {json_str}");
    }

    {
        let mut l = LimitsConfig::zeroed();
        l.extensions.insert(
            "test_extension_struct".to_string(),
            serde_json::to_value(StructExtension { foo: 421237 }).unwrap(),
        );
        let yaml_val = l.to_yaml_value_with_extensions().unwrap();
        let mut root = serde_yaml::Mapping::new();
        root.insert(serde_yaml::Value::String("test".to_string()), yaml_val);
        let yaml_str = yaml_v3_emit::marshal_struct(&serde_yaml::Value::Mapping(root)).unwrap();
        assert!(
            yaml_str.contains(
                "test:\n    test_extension_struct:\n        foo: 421237\n    test_extension_string: \"\"\n    max_active_series_per_user: 0\n    active_series_limit_response_code: 0\n    request_rate: 0\n    request_burst_size: 0"
            ),
            "yaml:\n{yaml_str}"
        );

        let mut json_root = serde_json::Map::new();
        json_root.insert("test".to_string(), l.to_json_value_with_extensions().unwrap());
        let json_str = serde_json::to_string(&serde_json::Value::Object(json_root)).unwrap();
        assert!(json_str.contains(r#""test_extension_struct":{"foo":421237}"#), "json: {json_str}");
        assert!(json_str.contains(r#""test_extension_string":"""#), "json: {json_str}");
        assert!(json_str.contains(r#""active_series_limit_response_code":0"#), "json: {json_str}");
    }

    {
        let overrides = LimitsConfig::unmarshal_overrides_yaml_with_defaults(
            r#"{"user": {}}"#,
            &LimitsConfig::default(),
        )
        .unwrap();
        let u = overrides["user"].as_ref().unwrap();
        let yaml_val = u.to_yaml_value_with_extensions().unwrap();
        let mut root = serde_yaml::Mapping::new();
        root.insert(serde_yaml::Value::String("user".to_string()), yaml_val);
        let yaml_str = yaml_v3_emit::marshal_struct(&serde_yaml::Value::Mapping(root)).unwrap();
        assert!(
            yaml_str.contains(
                "user:\n    test_extension_struct:\n        foo: 42\n    test_extension_string: default string extension value\n    max_active_series_per_user: 0\n    active_series_limit_response_code: 429\n    request_rate: 0\n    request_burst_size: 0"
            ),
            "yaml:\n{yaml_str}"
        );

        let mut json_root = serde_json::Map::new();
        json_root.insert("user".to_string(), u.to_json_value_with_extensions().unwrap());
        let json_str = serde_json::to_string(&serde_json::Value::Object(json_root)).unwrap();
        assert!(json_str.contains(r#""test_extension_struct":{"foo":42}"#), "json: {json_str}");
        assert!(json_str.contains(r#""test_extension_string":"default string extension value""#), "json: {json_str}");
        assert!(json_str.contains(r#""active_series_limit_response_code":429"#), "json: {json_str}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestIsLimitError
#[test]
fn test_is_limit_error() {
    #[derive(Debug)]
    struct RandomError(&'static str);
    impl fmt::Display for RandomError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for RandomError {}

    #[derive(Debug)]
    struct WrappedError(Box<dyn std::error::Error>);
    impl fmt::Display for WrappedError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "wrapped: {}", self.0)
        }
    }
    impl std::error::Error for WrappedError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    let msg = "this is an error";
    assert!(!is_limit_error(&RandomError(msg)));
    assert!(is_limit_error(&new_limit_error(msg)));
    let wrapped = WrappedError(Box::new(new_limit_error(msg)));
    assert!(is_limit_error(&wrapped));
}

// Upstream: pkg/util/validation/limits_test.go::TestBlockedRequestsUnmarshal
#[test]
fn test_blocked_requests_unmarshal() {
    let input_yaml = "\nuser1:\n  blocked_requests:\n    - path: /api/v1/query\n      method: POST\n      query_params:\n        foo:\n          value: bar\n    - query_params:\n        first:\n          value: bar.*\n          is_regexp: true\n        other:\n          value: bar\n          is_regexp: false\n";
    let defaults = LimitsConfig::default();
    let parsed = LimitsConfig::unmarshal_overrides_yaml_with_defaults(input_yaml, &defaults).unwrap();
    let map: HashMap<String, LimitsConfig> = parsed
        .into_iter()
        .filter_map(|(k, v)| v.map(|l| (k, l)))
        .collect();
    let ov = Overrides::with_static_map(defaults, map);

    let blocked = ov.blocked_requests("user1");
    assert_eq!(blocked.len(), 2);
    assert_eq!(
        blocked[0],
        BlockedRequest {
            path: "/api/v1/query".to_string(),
            method: "POST".to_string(),
            query_params: [(
                "foo".to_string(),
                BlockedRequestQueryParam {
                    value: "bar".to_string(),
                    is_regexp: false,
                }
            )]
            .into_iter()
            .collect(),
        }
    );
    assert_eq!(
        blocked[1],
        BlockedRequest {
            path: String::new(),
            method: String::new(),
            query_params: [
                (
                    "first".to_string(),
                    BlockedRequestQueryParam {
                        value: "bar.*".to_string(),
                        is_regexp: true,
                    }
                ),
                (
                    "other".to_string(),
                    BlockedRequestQueryParam {
                        value: "bar".to_string(),
                        is_regexp: false,
                    }
                ),
            ]
            .into_iter()
            .collect(),
        }
    );
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsCanonicalizeQueries
#[test]
fn test_limits_canonicalize_queries() {
    let cases = [
        (
            "mixed queries",
            vec![
                BlockedQuery {
                    pattern: r#"up{pod="test", job="test"}"#.to_string(),
                    regex: false,
                    ..Default::default()
                },
                BlockedQuery {
                    pattern: r#"up{pod="test", job="test""#.to_string(),
                    regex: false,
                    ..Default::default()
                },
                BlockedQuery {
                    pattern: r#"up{pod="test", job=~".*"}"#.to_string(),
                    regex: true,
                    ..Default::default()
                },
            ],
            vec![
                BlockedQuery {
                    pattern: r#"up{job="test",pod="test"}"#.to_string(),
                    regex: false,
                    ..Default::default()
                },
                BlockedQuery {
                    pattern: r#"up{pod="test", job="test""#.to_string(),
                    regex: false,
                    ..Default::default()
                },
                BlockedQuery {
                    pattern: r#"up{pod="test", job=~".*"}"#.to_string(),
                    regex: true,
                    ..Default::default()
                },
            ],
        ),
        ("empty blocked queries list", vec![], vec![]),
    ];

    for (name, input, expected) in cases {
        let mut limits = LimitsConfig {
            blocked_queries: input,
            ..Default::default()
        };
        limits.canonicalize_queries();
        assert_eq!(limits.blocked_queries, expected, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestOverrides_OTelTranslationStrategy
#[test]
fn test_overrides_otel_translation_strategy() {
    let mk = |strategy: &str, scheme: NameValidationScheme, suffixes: Option<bool>| LimitsConfig {
        otel_translation_strategy: OTelTranslationStrategy(strategy.to_string()),
        name_validation_scheme: scheme,
        otel_metric_suffixes_enabled: suffixes,
        ..LimitsConfig::default()
    };

    let cases = [
        (
            "explicit strategy takes precedence",
            Some(mk(
                "UnderscoreEscapingWithSuffixes",
                NameValidationScheme::Utf8,
                Some(false),
            )),
            "tenant1",
            "UnderscoreEscapingWithSuffixes",
        ),
        (
            "auto-deduced: legacy validation + suffixes enabled",
            Some(mk("", NameValidationScheme::Legacy, Some(true))),
            "tenant1",
            "UnderscoreEscapingWithSuffixes",
        ),
        (
            "auto-deduced: legacy validation + suffixes disabled",
            Some(mk("", NameValidationScheme::Legacy, Some(false))),
            "tenant1",
            "UnderscoreEscapingWithoutSuffixes",
        ),
        (
            "auto-deduced: utf8 validation + suffixes enabled",
            Some(mk("", NameValidationScheme::Utf8, Some(true))),
            "tenant1",
            "NoUTF8EscapingWithSuffixes",
        ),
        (
            "auto-deduced: utf8 validation + suffixes disabled",
            Some(mk("", NameValidationScheme::Utf8, Some(false))),
            "tenant1",
            "NoTranslation",
        ),
        (
            "uses defaults for unknown tenant",
            None,
            "unknown",
            "UnderscoreEscapingWithoutSuffixes",
        ),
    ];

    for (name, tenant_cfg, tenant_id, expected) in cases {
        let mut map = HashMap::new();
        if let Some(cfg) = tenant_cfg {
            map.insert("tenant1".to_string(), cfg);
        }
        let ov = Overrides::with_static_map(LimitsConfig::default(), map);
        assert_eq!(ov.otel_translation_strategy(tenant_id).0, expected, "{name}");
    }

    assert!(NameValidationScheme::parse("invalid_999").is_err());
}

// Upstream: pkg/util/validation/limits_test.go::TestEffectiveIngestionPartitionsTenantWriteShardSize
#[test]
fn test_effective_ingestion_partitions_tenant_write_shard_size() {
    let cases = [
        ("both zero (default)", 0, 0, 0),
        ("write shard size not set, falls back to read shard size", 4, 0, 4),
        ("write shard size smaller than read shard size", 4, 2, 2),
        ("write shard size equal to read shard size", 4, 4, 4),
        ("write shard size larger than read shard size, clamped", 4, 6, 4),
        ("write shard size set, read shard size zero (no read limit)", 0, 2, 2),
    ];

    for (name, read, write, expected) in cases {
        let limits = LimitsConfig {
            ingestion_partitions_tenant_shard_size: read,
            ingestion_partitions_tenant_write_shard_size: write,
            ..LimitsConfig::default()
        };
        let ov = Overrides::new(limits);
        assert_eq!(
            ov.effective_ingestion_partitions_tenant_write_shard_size("test"),
            expected,
            "{name}"
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestOverridesWithMetadata
#[test]
fn test_overrides_with_metadata() {
    let defaults = LimitsConfig {
        ingestion_rate: 1000.0,
        max_active_series_per_user: 10000,
        ingestion_burst_size: 100000,
        ingestion_burst_factor: 1.5,
        ..LimitsConfig::default()
    };

    let mut tenant_limits = HashMap::new();
    tenant_limits.insert(
        "tenant-a".to_string(),
        LimitsConfig {
            ingestion_rate: 100.0,
            max_active_series_per_user: 1000,
            ingestion_burst_size: 10000,
            ingestion_burst_factor: 2.0,
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-a:source=test-run".to_string(),
        LimitsConfig {
            ingestion_rate: 5000.0,
            max_active_series_per_user: 50000,
            ingestion_burst_size: 50000,
            ingestion_burst_factor: 5.0,
            otel_metric_suffixes_enabled: Some(true),
            ..LimitsConfig::zeroed()
        },
    );
    tenant_limits.insert(
        "tenant-a:run-id=specific:source=test-run".to_string(),
        LimitsConfig {
            ingestion_rate: 9999.0,
            max_active_series_per_user: 99999,
            ingestion_burst_size: 99999,
            ingestion_burst_factor: 9.0,
            ..LimitsConfig::zeroed()
        },
    );

    let ov = Overrides::with_static_map(defaults, tenant_limits);

    assert_eq!(100.0, ov.ingestion_rate("tenant-a"));
    assert_eq!(5000.0, ov.ingestion_rate("tenant-a:source=test-run"));
    assert_eq!(
        5000.0,
        ov.ingestion_rate("tenant-a:run-id=unknown:source=test-run")
    );
    assert_eq!(
        9999.0,
        ov.ingestion_rate("tenant-a:run-id=specific:source=test-run")
    );
    assert_eq!(1000.0, ov.ingestion_rate("unknown-tenant"));

    assert_eq!(1000, ov.max_active_or_global_series_per_user("tenant-a"));
    assert_eq!(
        50000,
        ov.max_active_or_global_series_per_user("tenant-a:source=test-run")
    );
    assert_eq!(
        50000,
        ov.max_active_or_global_series_per_user("tenant-a:run-id=unknown:source=test-run")
    );
    assert_eq!(
        99999,
        ov.max_active_or_global_series_per_user("tenant-a:run-id=specific:source=test-run")
    );
    assert_eq!(10000, ov.max_active_or_global_series_per_user("unknown-tenant"));

    assert_eq!(10000, ov.ingestion_burst_size("tenant-a"));
    assert_eq!(50000, ov.ingestion_burst_size("tenant-a:source=test-run"));
    assert_eq!(
        50000,
        ov.ingestion_burst_size("tenant-a:run-id=unknown:source=test-run")
    );
    assert_eq!(
        99999,
        ov.ingestion_burst_size("tenant-a:run-id=specific:source=test-run")
    );
    assert_eq!(100000, ov.ingestion_burst_size("unknown-tenant"));

    assert_eq!(2.0, ov.ingestion_burst_factor("tenant-a"));
    assert_eq!(5.0, ov.ingestion_burst_factor("tenant-a:source=test-run"));
    assert_eq!(
        5.0,
        ov.ingestion_burst_factor("tenant-a:run-id=unknown:source=test-run")
    );
    assert_eq!(
        9.0,
        ov.ingestion_burst_factor("tenant-a:run-id=specific:source=test-run")
    );
    assert_eq!(1.5, ov.ingestion_burst_factor("unknown-tenant"));

    assert!(!ov.otel_metric_suffixes_enabled("tenant-a"));
    assert!(ov.otel_metric_suffixes_enabled("tenant-a:source=test-run"));
    assert!(ov.otel_metric_suffixes_enabled("tenant-a:run-id=unknown:source=test-run"));
    assert!(ov.otel_metric_suffixes_enabled("tenant-a:run-id=specific:source=test-run"));
    assert!(!ov.otel_metric_suffixes_enabled("unknown-tenant"));
}

// Upstream: pkg/util/validation/limits_test.go::TestGetOverridesForUserWithMetadata
#[test]
fn test_get_overrides_for_user_with_metadata() {
    struct Case {
        name: &'static str,
        tenant_limits: Option<Vec<(&'static str, LimitsConfig)>>,
        user_id: &'static str,
        exp_rate: f64,
        exp_series: i64,
        exp_burst_size: i64,
        exp_burst_factor: f64,
        exp_otel_suffix: Option<bool>,
    }

    let z = LimitsConfig::zeroed;
    let cases = vec![
        Case {
            name: "nil tenantLimits returns defaults",
            tenant_limits: None,
            user_id: "tenant-a:source=test-run",
            exp_rate: 500.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "parse error falls back to raw userID lookup",
            tenant_limits: Some(vec![("", LimitsConfig { ingestion_rate: 999.0, ..z() })]),
            user_id: "",
            exp_rate: 999.0,
            exp_series: 0,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "plain tenant ID without metadata",
            tenant_limits: Some(vec![("tenant-a", LimitsConfig { ingestion_rate: 100.0, ..z() })]),
            user_id: "tenant-a",
            exp_rate: 100.0,
            exp_series: 0,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "unknown tenant falls back to defaults",
            tenant_limits: Some(vec![]),
            user_id: "tenant-a",
            exp_rate: 500.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "single metadata key overrides matched fields, inherits unmatched",
            tenant_limits: Some(vec![
                (
                    "tenant-a",
                    LimitsConfig {
                        ingestion_rate: 100.0,
                        max_active_series_per_user: 1000,
                        ingestion_burst_size: 10000,
                        ..z()
                    },
                ),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 200.0,
                        otel_metric_suffixes_enabled: Some(true),
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:source=test-run",
            exp_rate: 200.0,
            exp_series: 1000,
            exp_burst_size: 10000,
            exp_burst_factor: 0.0,
            exp_otel_suffix: Some(true),
        },
        Case {
            name: "global metadata override (empty tenant prefix) applies to any tenant",
            tenant_limits: Some(vec![(
                ":source=test-run",
                LimitsConfig {
                    ingestion_rate: 9000.0,
                    ..z()
                },
            )]),
            user_id: "any-tenant:source=test-run",
            exp_rate: 9000.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "tenant-specific metadata takes precedence over global metadata",
            tenant_limits: Some(vec![
                (
                    ":source=test-run",
                    LimitsConfig {
                        ingestion_rate: 9000.0,
                        ..z()
                    },
                ),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 200.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:source=test-run",
            exp_rate: 200.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "multiple metadata keys merged individually in sorted key order",
            tenant_limits: Some(vec![
                (
                    "tenant-a",
                    LimitsConfig {
                        ingestion_rate: 100.0,
                        max_active_series_per_user: 1000,
                        ..z()
                    },
                ),
                (
                    "tenant-a:env=prod",
                    LimitsConfig {
                        max_active_series_per_user: 2000,
                        ..z()
                    },
                ),
                (
                    ":source=test-run",
                    LimitsConfig {
                        ingestion_rate: 9000.0,
                        ..z()
                    },
                ),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 300.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:env=prod:source=test-run",
            exp_rate: 300.0,
            exp_series: 2000,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "full metadata match overrides individual key matches",
            tenant_limits: Some(vec![
                ("tenant-a", LimitsConfig { ingestion_rate: 100.0, ..z() }),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 300.0,
                        ..z()
                    },
                ),
                (
                    "tenant-a:env=prod:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 777.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:env=prod:source=test-run",
            exp_rate: 777.0,
            exp_series: 0,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "unmatched metadata keys are skipped",
            tenant_limits: Some(vec![
                ("tenant-a", LimitsConfig { ingestion_rate: 100.0, ..z() }),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 300.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:run-id=unknown:source=test-run",
            exp_rate: 300.0,
            exp_series: 0,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "metadata on unknown tenant returns defaults",
            tenant_limits: Some(vec![(
                "tenant-a",
                LimitsConfig {
                    ingestion_rate: 100.0,
                    ..z()
                },
            )]),
            user_id: "unknown:source=test-run",
            exp_rate: 500.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "burst size inherited from base when overlay omits it",
            tenant_limits: Some(vec![
                (
                    "tenant-a",
                    LimitsConfig {
                        ingestion_rate: 100.0,
                        ingestion_burst_size: 10000,
                        ..z()
                    },
                ),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 200.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:source=test-run",
            exp_rate: 200.0,
            exp_series: 0,
            exp_burst_size: 10000,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "burst factor overridden by metadata",
            tenant_limits: Some(vec![
                (
                    "tenant-a",
                    LimitsConfig {
                        ingestion_burst_factor: 2.0,
                        ..z()
                    },
                ),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        ingestion_burst_factor: 5.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:source=test-run",
            exp_rate: 0.0,
            exp_series: 0,
            exp_burst_size: 0,
            exp_burst_factor: 5.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "read compartment key without metadata uses its own overrides",
            tenant_limits: Some(vec![
                (
                    "tenant-a",
                    LimitsConfig {
                        ingestion_rate: 100.0,
                        max_active_series_per_user: 1000,
                        ..z()
                    },
                ),
                (
                    "tenant-a/rc/1",
                    LimitsConfig {
                        ingestion_rate: 200.0,
                        max_active_series_per_user: 300,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a/rc/1",
            exp_rate: 200.0,
            exp_series: 300,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "read compartment key with metadata merges the read compartment metadata overrides",
            tenant_limits: Some(vec![
                (
                    "tenant-a/rc/1",
                    LimitsConfig {
                        ingestion_rate: 200.0,
                        max_active_series_per_user: 300,
                        ..z()
                    },
                ),
                (
                    "tenant-a/rc/1:source=test-run",
                    LimitsConfig {
                        max_active_series_per_user: 40,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a/rc/1:source=test-run",
            exp_rate: 200.0,
            exp_series: 40,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "read compartment key with metadata merges global metadata overrides",
            tenant_limits: Some(vec![(
                ":source=test-run",
                LimitsConfig {
                    max_active_series_per_user: 150,
                    ..z()
                },
            )]),
            user_id: "tenant-a/rc/1:source=test-run",
            exp_rate: 500.0,
            exp_series: 150,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "read compartment key with metadata ignores metadata overrides of the whole tenant",
            tenant_limits: Some(vec![(
                "tenant-a:source=test-run",
                LimitsConfig {
                    max_active_series_per_user: 40,
                    ..z()
                },
            )]),
            user_id: "tenant-a/rc/1:source=test-run",
            exp_rate: 500.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "malformed read compartment key falls back to defaults",
            tenant_limits: Some(vec![(
                "tenant-a/rc/x:source=test-run",
                LimitsConfig {
                    max_active_series_per_user: 40,
                    ..z()
                },
            )]),
            user_id: "tenant-a/rc/x:source=test-run",
            exp_rate: 500.0,
            exp_series: 5000,
            exp_burst_size: 50000,
            exp_burst_factor: 2.0,
            exp_otel_suffix: None,
        },
        Case {
            name: "OTelMetricSuffixesEnabled inherited from parent overlay when child omits it",
            tenant_limits: Some(vec![
                ("tenant-a", LimitsConfig { ingestion_rate: 100.0, ..z() }),
                (
                    "tenant-a:source=test-run",
                    LimitsConfig {
                        otel_metric_suffixes_enabled: Some(true),
                        ..z()
                    },
                ),
                (
                    "tenant-a:run-id=specific:source=test-run",
                    LimitsConfig {
                        ingestion_rate: 200.0,
                        ..z()
                    },
                ),
            ]),
            user_id: "tenant-a:run-id=specific:source=test-run",
            exp_rate: 200.0,
            exp_series: 0,
            exp_burst_size: 0,
            exp_burst_factor: 0.0,
            exp_otel_suffix: Some(true),
        },
    ];

    for tc in cases {
        let defaults = LimitsConfig {
            ingestion_rate: 500.0,
            max_active_series_per_user: 5000,
            ingestion_burst_size: 50000,
            ingestion_burst_factor: 2.0,
            ..LimitsConfig::default()
        };

        let ov = match tc.tenant_limits {
            None => Overrides::new(defaults),
            Some(entries) => Overrides::with_static_map(
                defaults,
                entries
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
            ),
        };
        let got = ov.get_overrides_for_limits_key(tc.user_id);
        assert_eq!(got.ingestion_rate, tc.exp_rate, "{}", tc.name);
        assert_eq!(got.max_active_series_per_user, tc.exp_series, "{}", tc.name);
        assert_eq!(got.ingestion_burst_size, tc.exp_burst_size, "{}", tc.name);
        assert_eq!(got.ingestion_burst_factor, tc.exp_burst_factor, "{}", tc.name);
        if tc.exp_otel_suffix.is_some() {
            assert_eq!(
                got.otel_metric_suffixes_enabled, tc.exp_otel_suffix,
                "{}",
                tc.name
            );
        }
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestMergeLimits
#[test]
fn test_merge_limits() {
    let z = LimitsConfig::zeroed;
    struct Case {
        name: &'static str,
        dst: Option<LimitsConfig>,
        overlay: Option<LimitsConfig>,
        want_nil: bool,
        want: LimitsConfig,
    }

    let cases = vec![
        Case {
            name: "nil dst copies overlay",
            dst: None,
            overlay: Some(LimitsConfig {
                ingestion_rate: 42.0,
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 42.0,
                ..z()
            },
        },
        Case {
            name: "nil overlay returns dst",
            dst: Some(LimitsConfig {
                ingestion_rate: 42.0,
                ..z()
            }),
            overlay: None,
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 42.0,
                ..z()
            },
        },
        Case {
            name: "both nil returns nil",
            dst: None,
            overlay: None,
            want_nil: true,
            want: z(),
        },
        Case {
            name: "overlay overrides non-zero fields only",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                max_active_series_per_user: 100,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                ingestion_rate: 20.0,
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 20.0,
                max_active_series_per_user: 100,
                ..z()
            },
        },
        Case {
            name: "merges IngestionBurstSize",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                ingestion_burst_size: 100,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                ingestion_burst_size: 200,
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 10.0,
                ingestion_burst_size: 200,
                ..z()
            },
        },
        Case {
            name: "merges IngestionBurstFactor",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                ingestion_burst_factor: 1.5,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                ingestion_burst_factor: 2.0,
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 10.0,
                ingestion_burst_factor: 2.0,
                ..z()
            },
        },
        Case {
            name: "merges OTelMetricSuffixesEnabled true overlay",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                otel_metric_suffixes_enabled: Some(true),
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 10.0,
                otel_metric_suffixes_enabled: Some(true),
                ..z()
            },
        },
        Case {
            name: "OTelMetricSuffixesEnabled false overlay overrides true dst",
            dst: Some(LimitsConfig {
                otel_metric_suffixes_enabled: Some(true),
                ..z()
            }),
            overlay: Some(LimitsConfig {
                otel_metric_suffixes_enabled: Some(false),
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                otel_metric_suffixes_enabled: Some(false),
                ..z()
            },
        },
        Case {
            name: "OTelMetricSuffixesEnabled nil overlay does not override dst",
            dst: Some(LimitsConfig {
                otel_metric_suffixes_enabled: Some(true),
                ..z()
            }),
            overlay: Some(z()),
            want_nil: false,
            want: LimitsConfig {
                otel_metric_suffixes_enabled: Some(true),
                ..z()
            },
        },
        Case {
            name: "merges NameValidationScheme when overlay is set",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                name_validation_scheme: NameValidationScheme::Legacy,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 10.0,
                name_validation_scheme: NameValidationScheme::Utf8,
                ..z()
            },
        },
        Case {
            name: "NameValidationScheme unset overlay does not override dst",
            dst: Some(LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                name_validation_scheme: NameValidationScheme::Unset,
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                name_validation_scheme: NameValidationScheme::Utf8,
                ..z()
            },
        },
        Case {
            name: "merges OTelTranslationStrategy when overlay is set",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithoutSuffixes".to_string(),
                ),
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 10.0,
                otel_translation_strategy: OTelTranslationStrategy(
                    "UnderscoreEscapingWithoutSuffixes".to_string(),
                ),
                ..z()
            },
        },
        Case {
            name: "OTelTranslationStrategy empty overlay does not override dst",
            dst: Some(LimitsConfig {
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..z()
            }),
            overlay: Some(z()),
            want_nil: false,
            want: LimitsConfig {
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..z()
            },
        },
        Case {
            name: "all mergeable fields",
            dst: Some(LimitsConfig {
                ingestion_rate: 10.0,
                max_active_series_per_user: 100,
                ingestion_burst_size: 1000,
                ingestion_burst_factor: 1.5,
                ..z()
            }),
            overlay: Some(LimitsConfig {
                ingestion_rate: 20.0,
                max_active_series_per_user: 200,
                ingestion_burst_size: 2000,
                ingestion_burst_factor: 3.0,
                otel_metric_suffixes_enabled: Some(true),
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..z()
            }),
            want_nil: false,
            want: LimitsConfig {
                ingestion_rate: 20.0,
                max_active_series_per_user: 200,
                ingestion_burst_size: 2000,
                ingestion_burst_factor: 3.0,
                otel_metric_suffixes_enabled: Some(true),
                name_validation_scheme: NameValidationScheme::Utf8,
                otel_translation_strategy: OTelTranslationStrategy("NoTranslation".to_string()),
                ..z()
            },
        },
    ];

    for tc in cases {
        let got = merge_limits(tc.dst, tc.overlay.as_ref());
        if tc.want_nil {
            assert!(got.is_none(), "{}", tc.name);
            continue;
        }
        let got = got.expect(tc.name);
        assert_eq!(got.ingestion_rate, tc.want.ingestion_rate, "{}", tc.name);
        assert_eq!(
            got.max_active_series_per_user,
            tc.want.max_active_series_per_user,
            "{}",
            tc.name
        );
        assert_eq!(got.ingestion_burst_size, tc.want.ingestion_burst_size, "{}", tc.name);
        assert_eq!(
            got.ingestion_burst_factor, tc.want.ingestion_burst_factor,
            "{}",
            tc.name
        );
        assert_eq!(
            got.otel_metric_suffixes_enabled,
            tc.want.otel_metric_suffixes_enabled,
            "{}",
            tc.name
        );
        assert_eq!(
            got.name_validation_scheme, tc.want.name_validation_scheme,
            "{}",
            tc.name
        );
        assert_eq!(
            got.otel_translation_strategy,
            tc.want.otel_translation_strategy,
            "{}",
            tc.name
        );
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestOverrides_FloatChunkEncoding
#[test]
fn test_overrides_float_chunk_encoding() {
    let mut map = HashMap::new();
    map.insert(
        "user1".to_string(),
        LimitsConfig {
            float_chunk_encoding: "xor2".to_string(),
            ..LimitsConfig::default()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::default(), map);
    assert_eq!(ov.float_chunk_encoding("user1"), FloatChunkEncoding::Xor2);
    assert_eq!(ov.float_chunk_encoding("user2"), FloatChunkEncoding::Xor);
}

// Upstream: pkg/util/validation/limits_test.go::TestLimitsLoading_FloatChunkEncoding
#[test]
fn test_limits_loading_float_chunk_encoding() {
    let defaults = LimitsConfig {
        float_chunk_encoding: "xor2".to_string(),
        ..LimitsConfig::default()
    };

    let cases = [
        ("omitted inherits global", "{}", Some("xor2")),
        ("explicit xor", r#"{"float_chunk_encoding":"xor"}"#, Some("xor")),
        ("explicit xor2", r#"{"float_chunk_encoding":"xor2"}"#, Some("xor2")),
        ("empty is rejected", r#"{"float_chunk_encoding":""}"#, None),
    ];

    for (name, input, expected) in cases {
        for (fmt_name, res) in [
            ("YAML", LimitsConfig::unmarshal_yaml_with_defaults(input, &defaults)),
            ("JSON", LimitsConfig::unmarshal_json_with_defaults(input, &defaults)),
            (
                "map",
                LimitsConfig::unmarshal_map_with_defaults(
                    serde_yaml::from_str(input).unwrap(),
                    &defaults,
                ),
            ),
        ] {
            match expected {
                None => {
                    let err = res.unwrap_err().to_string();
                    assert!(
                        err.contains("invalid float chunk encoding"),
                        "{fmt_name} {name}: {err}"
                    );
                }
                Some(exp) => {
                    let limits = res.unwrap_or_else(|e| panic!("{fmt_name} {name}: {e}"));
                    assert_eq!(limits.float_chunk_encoding, exp, "{fmt_name} {name}");
                }
            }
        }
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestFloatChunkEncodingValues
#[test]
fn test_float_chunk_encoding_values() {
    assert_eq!(FLOAT_CHUNK_ENCODING_VALUES, &["xor", "xor2"]);
    let mut seen = HashMap::new();
    for &value in FLOAT_CHUNK_ENCODING_VALUES {
        let mut limits = LimitsConfig {
            float_chunk_encoding: value.to_string(),
            ..LimitsConfig::default()
        };
        assert!(limits.validate().is_ok(), "value {value}");

        let enc = parse_float_chunk_encoding(value);
        assert!(!seen.contains_key(&enc), "duplicate encoding for {value}");
        seen.insert(enc, value);
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestParseFloatChunkEncoding
#[test]
fn test_parse_float_chunk_encoding() {
    let cases = [
        ("empty selects the default", "", FloatChunkEncoding::Xor),
        ("xor", "xor", FloatChunkEncoding::Xor),
        ("xor2", "xor2", FloatChunkEncoding::Xor2),
        ("uppercase is not accepted", "XOR2", FloatChunkEncoding::Xor),
        ("histogram is not a float chunk encoding", "histogram", FloatChunkEncoding::Xor),
    ];
    for (name, value, expected) in cases {
        assert_eq!(parse_float_chunk_encoding(value), expected, "{name}");
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestOverrides_FloatChunkEncodingValue
#[test]
fn test_overrides_float_chunk_encoding_value() {
    let mut map = HashMap::new();
    map.insert(
        "user1".to_string(),
        LimitsConfig {
            float_chunk_encoding: "xor2".to_string(),
            ..LimitsConfig::default()
        },
    );
    map.insert(
        "user2".to_string(),
        LimitsConfig {
            float_chunk_encoding: String::new(),
            ..LimitsConfig::default()
        },
    );
    map.insert(
        "user3".to_string(),
        LimitsConfig {
            float_chunk_encoding: "nope".to_string(),
            ..LimitsConfig::default()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::default(), map);
    assert_eq!(ov.float_chunk_encoding_value("user1"), "xor2");
    assert_eq!(ov.float_chunk_encoding_value("user2"), "xor");
    assert_eq!(ov.float_chunk_encoding_value("user3"), "xor");
}

// Upstream: pkg/util/validation/limits_test.go::TestOverrides_HistogramSTEncodingEnabled
#[test]
fn test_overrides_histogram_st_encoding_enabled() {
    let mut map = HashMap::new();
    map.insert(
        "user1".to_string(),
        LimitsConfig {
            histogram_chunk_encoding: "histogram_st".to_string(),
            ..LimitsConfig::default()
        },
    );
    let ov = Overrides::with_static_map(LimitsConfig::default(), map);
    assert!(ov.histogram_st_encoding_enabled("user1"));
    assert!(!ov.histogram_st_encoding_enabled("user2"));
}

// Upstream: pkg/util/validation/limits_test.go::TestHistogramChunkEncodingValues
#[test]
fn test_histogram_chunk_encoding_values() {
    assert_eq!(
        HISTOGRAM_CHUNK_ENCODING_VALUES,
        &["histogram", "histogram_st"]
    );
    let mut seen = HashMap::new();
    for &value in HISTOGRAM_CHUNK_ENCODING_VALUES {
        let mut limits = LimitsConfig {
            histogram_chunk_encoding: value.to_string(),
            ..LimitsConfig::default()
        };
        assert!(limits.validate().is_ok());

        let enabled = parse_histogram_st_encoding_enabled(value);
        assert!(!seen.contains_key(&enabled));
        seen.insert(enabled, value);
    }
}

// Upstream: pkg/util/validation/limits_test.go::TestParseHistogramSTEncodingEnabled
#[test]
fn test_parse_histogram_st_encoding_enabled() {
    let cases = [
        ("empty selects the default", "", false),
        ("histogram", "histogram", false),
        ("histogram_st", "histogram_st", true),
        ("uppercase is not accepted", "HISTOGRAM_ST", false),
        ("xor2 is not a histogram chunk encoding", "xor2", false),
    ];
    for (name, value, expected) in cases {
        assert_eq!(parse_histogram_st_encoding_enabled(value), expected, "{name}");
    }
}
