//! Ported from github.com/grafana/dskit/flagext/*_test.go (47 tests).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use mimir_config::yaml_v3_emit::marshal_struct;
use mimir_config::{
    flag_to_env_var, parse_flags_and_arguments_with_env, parse_flags_without_arguments_with_env,
    track_registered_flags, CIDRSliceCSV, DayValue, FlagSet, FlagTime, HumanBytes,
    LimitsMapValidator, Secret, StringSliceCSV, StringSliceCSVMulti, URLValue, ValidatedLimitsMap,
};
use serde::{Deserialize, Serialize};

/// Guard tests that mutate real process environment variables so they cannot race in parallel.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

struct EnvGuard {
    key: String,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self {
            key: key.to_string(),
            prev,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(&self.key, v),
            None => std::env::remove_var(&self.key),
        }
    }
}

fn marshal_go_yaml<T: Serialize>(v: &T) -> String {
    let val = serde_yaml::to_value(v).unwrap();
    marshal_struct(&val).unwrap()
}

// =============================================================================================
// bytes_test.go (4 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/bytes_test.go::Test_Bytes_String
#[test]
fn test_bytes_string() {
    for (input, expected) in [
        (1024 * 1024 * 1024u64, "1GiB"),
        ((1024 + 512) * 1024 * 1024, "1GiB512MiB"),
        (1024 * 1024, "1MiB"),
        (1024, "1KiB"),
    ] {
        let b = HumanBytes(input);
        assert_eq!(b.to_string(), expected);
    }
}

// Upstream: github.com/grafana/dskit/flagext/bytes_test.go::Test_Bytes_Set
#[test]
fn test_bytes_set() {
    for (input, expected) in [
        ("1.5GiB", (1024 + 512) * 1024 * 1024u64),
        ("1.5GB", (1024 + 512) * 1024 * 1024),
        ("1536MiB", (1024 + 512) * 1024 * 1024),
        ("1536MB", (1024 + 512) * 1024 * 1024),
        ("1GiB", 1024 * 1024 * 1024),
        ("1GB", 1024 * 1024 * 1024),
        ("1MiB", 1024 * 1024),
        ("1MB", 1024 * 1024),
        ("1KiB", 1024),
        ("1KB", 1024),
    ] {
        let b = HumanBytes(HumanBytes::parse(input).unwrap());
        assert_eq!(b, HumanBytes(expected));
    }
}

// Upstream: github.com/grafana/dskit/flagext/bytes_test.go::Test_Bytes_MarshalYAML
#[test]
fn test_bytes_marshal_yaml() {
    for (input, expected) in [
        ((1024 + 512) * 1024 * 1024u64, "1GiB512MiB\n"),
        (1024 * 1024 * 1024, "1GiB\n"),
        (1024 * 1024, "1MiB\n"),
        (1024, "1KiB\n"),
    ] {
        let b = HumanBytes(input);
        let y = marshal_go_yaml(&b);
        assert_eq!(y, expected);
    }
}

// Upstream: github.com/grafana/dskit/flagext/bytes_test.go::Test_Bytes_UnmarshalYAML
#[test]
fn test_bytes_unmarshal_yaml() {
    for (input, expected) in [
        ("1.5GiB\n", (1024 + 512) * 1024 * 1024u64),
        ("1.5GB\n", (1024 + 512) * 1024 * 1024),
        ("1536MiB\n", (1024 + 512) * 1024 * 1024),
        ("1536MB\n", (1024 + 512) * 1024 * 1024),
        ("1GiB\n", 1024 * 1024 * 1024),
        ("1GB\n", 1024 * 1024 * 1024),
        ("1MiB\n", 1024 * 1024),
        ("1MB\n", 1024 * 1024),
        ("1KiB\n", 1024),
        ("1KB\n", 1024),
    ] {
        let b: HumanBytes = serde_yaml::from_str(input).unwrap();
        assert_eq!(b, HumanBytes(expected));
    }
}

// =============================================================================================
// cidr_test.go (1 test)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/cidr_test.go::Test_CIDRSliceCSV_YamlMarshalling
#[test]
fn test_cidr_slice_csv_yaml_marshalling() {
    #[derive(Default, Serialize, Deserialize)]
    struct TestStruct {
        cidrs: CIDRSliceCSV,
    }

    let cases: &[(&str, &str, &[&str])] = &[
        ("should marshal empty config", "cidrs: \"\"\n", &[]),
        (
            "should marshal single value",
            "cidrs: 127.0.0.1/32\n",
            &["127.0.0.1/32"],
        ),
        (
            "should marshal multiple comma-separated values",
            "cidrs: 127.0.0.1/32,10.0.10.0/28,fdf8:f53b:82e4::/100,192.168.0.0/20\n",
            &[
                "127.0.0.1/32",
                "10.0.10.0/28",
                "fdf8:f53b:82e4::/100",
                "192.168.0.0/20",
            ],
        ),
    ];

    for &(name, input, expected) in cases {
        let actual: TestStruct = serde_yaml::from_str(input).expect(name);
        assert_eq!(actual.cidrs.len(), expected.len(), "{name}");
        for (idx, cidr) in actual.cidrs.iter().enumerate() {
            assert_eq!(cidr, expected[idx], "{name}");
        }
        let out = marshal_go_yaml(&actual);
        assert_eq!(out, input, "{name}");
    }
}

// =============================================================================================
// day_test.go (1 test)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/day_test.go::TestDayValueYAML
#[test]
fn test_day_value_yaml() {
    // embedding DayValue
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            day: DayValue,
        }

        let mut test_struct = TestStruct::default();
        test_struct.day.set("1985-06-02").unwrap();
        let expected = "day: \"1985-06-02\"\n";

        let actual = marshal_go_yaml(&test_struct);
        assert_eq!(actual, expected);

        let actual_struct: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(test_struct, actual_struct);
    }

    // pointer (Option) of DayValue
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            day: Option<DayValue>,
        }

        let mut dv = DayValue::default();
        dv.set("1985-06-02").unwrap();
        let test_struct = TestStruct { day: Some(dv) };
        let expected = "day: \"1985-06-02\"\n";

        let actual = marshal_go_yaml(&test_struct);
        assert_eq!(actual, expected);

        let actual_struct: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(test_struct, actual_struct);
    }
}

// =============================================================================================
// env_test.go (14 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestFlagToEnvVar
#[test]
fn test_flag_to_env_var() {
    let tests = [
        ("MYAPP", "server.http-listen-port", "MYAPP_SERVER_HTTP_LISTEN_PORT"),
        ("myApp.Name", "server.http-listen-port", "MYAPP_NAME_SERVER_HTTP_LISTEN_PORT"),
        ("", "server.http-listen-port", "SERVER_HTTP_LISTEN_PORT"),
        ("APP", "verbose", "APP_VERBOSE"),
        ("MIMIR", "store.engine-config.max-retries", "MIMIR_STORE_ENGINE_CONFIG_MAX_RETRIES"),
        ("APP", "TLS", "APP_TLS"),
        ("APP", "my_flag", "APP_MY_FLAG"),
        ("", "", ""),
    ];
    for (prefix, flag_name, expected) in tests {
        assert_eq!(flag_to_env_var(prefix, flag_name), expected);
    }
}

fn map_lookup(m: HashMap<&'static str, &'static str>) -> impl FnMut(&str) -> Option<String> {
    move |key| m.get(key).map(|v| (*v).to_string())
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_Basic
#[test]
fn test_set_flags_from_env_basic() {
    #[derive(Default)]
    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("server.http-listen-port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([("MYAPP_SERVER_HTTP_LISTEN_PORT", "9090")]);
    fs.set_flags_from_env_with_lookup(&mut t, "MYAPP", map_lookup(env))
        .unwrap();
    assert_eq!(t.port, 9090);
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_CLIPrecedence
#[test]
fn test_set_flags_from_env_cli_precedence() {
    #[derive(Default)]
    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("server.http-listen-port", 80, "", |x| &x.port, |x| &mut x.port);
    let args = vec!["-server.http-listen-port".into(), "3000".into()];
    fs.parse(&mut t, &args).unwrap();

    let env = HashMap::from([("MYAPP_SERVER_HTTP_LISTEN_PORT", "9090")]);
    fs.set_flags_from_env_with_lookup(&mut t, "MYAPP", map_lookup(env))
        .unwrap();
    assert_eq!(t.port, 3000);
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_DefaultPrecedence
#[test]
fn test_set_flags_from_env_default_precedence() {
    struct Target {
        host: String,
    }
    let mut t = Target {
        host: "localhost".into(),
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("server.host", "localhost", "", |x| &x.host, |x| &mut x.host);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([("APP_SERVER_HOST", "0.0.0.0")]);
    fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
        .unwrap();
    assert_eq!(t.host, "0.0.0.0");
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_NoEnvVar
#[test]
fn test_set_flags_from_env_no_env_var() {
    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("server.http-listen-port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.parse(&mut t, &[]).unwrap();

    fs.set_flags_from_env_with_lookup(&mut t, "MYAPP", map_lookup(HashMap::new()))
        .unwrap();
    assert_eq!(t.port, 80);
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_InvalidValue
#[test]
fn test_set_flags_from_env_invalid_value() {
    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("server.http-listen-port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([("MYAPP_SERVER_HTTP_LISTEN_PORT", "not-a-number")]);
    let err = fs
        .set_flags_from_env_with_lookup(&mut t, "MYAPP", map_lookup(env))
        .unwrap_err()
        .to_string();
    assert!(err.contains("server.http-listen-port"), "{err}");
    assert!(err.contains("MYAPP_SERVER_HTTP_LISTEN_PORT"), "{err}");
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_BoolFlag
#[test]
fn test_set_flags_from_env_bool_flag() {
    for (env_val, expected) in [("true", true), ("false", false), ("1", true), ("0", false)] {
        #[derive(Default)]
        struct Target {
            verbose: bool,
        }
        let mut t = Target::default();
        let mut fs = FlagSet::<Target>::new();
        fs.bool_var("verbose", false, "", |x| &x.verbose, |x| &mut x.verbose);
        fs.parse(&mut t, &[]).unwrap();

        let env = HashMap::from([("APP_VERBOSE", env_val)]);
        fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
            .unwrap();
        assert_eq!(t.verbose, expected);
    }
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_MultipleFlags
#[test]
fn test_set_flags_from_env_multiple_flags() {
    struct Target {
        host: String,
        port: i64,
        verbose: bool,
    }
    let mut t = Target {
        host: "localhost".into(),
        port: 80,
        verbose: false,
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("server.host", "localhost", "", |x| &x.host, |x| &mut x.host);
    fs.i64_var("server.port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.bool_var("verbose", false, "", |x| &x.verbose, |x| &mut x.verbose);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([
        ("APP_SERVER_HOST", "0.0.0.0"),
        ("APP_SERVER_PORT", "9090"),
        ("APP_VERBOSE", "true"),
    ]);
    fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
        .unwrap();
    assert_eq!(t.host, "0.0.0.0");
    assert_eq!(t.port, 9090);
    assert!(t.verbose);
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_EmptyEnvValue
#[test]
fn test_set_flags_from_env_empty_env_value() {
    struct Target {
        host: String,
    }
    let mut t = Target {
        host: "localhost".into(),
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("server.host", "localhost", "", |x| &x.host, |x| &mut x.host);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([("APP_SERVER_HOST", "")]);
    fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
        .unwrap();
    assert_eq!(t.host, "");
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_CustomFlagTypes
#[test]
fn test_set_flags_from_env_custom_flag_types() {
    // StringSliceCSV
    {
        #[derive(Default)]
        struct Target {
            csv: StringSliceCSV,
        }
        let mut t = Target::default();
        let mut fs = FlagSet::<Target>::new();
        fs.string_slice_csv_var(
            "server.allowed-origins",
            &StringSliceCSV::default(),
            "",
            |x| &x.csv,
            |x| &mut x.csv,
        );
        fs.parse(&mut t, &[]).unwrap();

        let env = HashMap::from([(
            "APP_SERVER_ALLOWED_ORIGINS",
            "http://localhost,http://example.com",
        )]);
        fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
            .unwrap();
        assert_eq!(
            t.csv.0,
            vec!["http://localhost".to_string(), "http://example.com".to_string()]
        );
    }

    // Secret
    {
        #[derive(Default)]
        struct Target {
            secret: Secret,
        }
        let mut t = Target::default();
        let mut fs = FlagSet::<Target>::new();
        fs.secret_var("auth.token", &Secret::default(), "", |x| &x.secret, |x| &mut x.secret);
        fs.parse(&mut t, &[]).unwrap();

        let env = HashMap::from([("APP_AUTH_TOKEN", "super-secret")]);
        fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
            .unwrap();
        assert_eq!(t.secret, Secret::with_value("super-secret"));
    }
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_EmptyPrefix
#[test]
fn test_set_flags_from_env_empty_prefix() {
    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("server.port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([("SERVER_PORT", "9090")]);
    fs.set_flags_from_env_with_lookup(&mut t, "", map_lookup(env))
        .unwrap();
    assert_eq!(t.port, 9090);
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_MixedCLIAndEnv
#[test]
fn test_set_flags_from_env_mixed_cli_and_env() {
    struct Target {
        host: String,
        port: i64,
    }
    let mut t = Target {
        host: "localhost".into(),
        port: 80,
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("server.host", "localhost", "", |x| &x.host, |x| &mut x.host);
    fs.i64_var("server.port", 80, "", |x| &x.port, |x| &mut x.port);

    let args = vec!["-server.host".into(), "myhost".into()];
    fs.parse(&mut t, &args).unwrap();

    let env = HashMap::from([("APP_SERVER_HOST", "envhost"), ("APP_SERVER_PORT", "9090")]);
    fs.set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
        .unwrap();
    assert_eq!(t.host, "myhost");
    assert_eq!(t.port, 9090);
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_MultipleErrors
#[test]
fn test_set_flags_from_env_multiple_errors() {
    struct Target {
        port: i64,
        timeout: f64,
    }
    let mut t = Target {
        port: 80,
        timeout: 1.0,
    };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.f64_var("timeout", 1.0, "", |x| &x.timeout, |x| &mut x.timeout);
    fs.parse(&mut t, &[]).unwrap();

    let env = HashMap::from([("APP_PORT", "not-int"), ("APP_TIMEOUT", "not-float")]);
    let err = fs
        .set_flags_from_env_with_lookup(&mut t, "APP", map_lookup(env))
        .unwrap_err()
        .to_string();
    assert!(err.contains("port"), "{err}");
    assert!(err.contains("timeout"), "{err}");
}

// Upstream: github.com/grafana/dskit/flagext/env_test.go::TestSetFlagsFromEnv_WithRealEnv
#[test]
fn test_set_flags_from_env_with_real_env() {
    let _lock = ENV_MUTEX.lock().unwrap();
    let _env = EnvGuard::set("APP_SERVER_PORT", "9090");

    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("server.port", 80, "", |x| &x.port, |x| &mut x.port);
    fs.parse(&mut t, &[]).unwrap();

    fs.set_flags_from_env(&mut t, "APP").unwrap();
    assert_eq!(t.port, 9090);
}

// =============================================================================================
// map_test.go (8 tests)
// =============================================================================================

fn f64_validator() -> LimitsMapValidator<f64> {
    Arc::new(|_, v| {
        if *v < 0.0 {
            Err("value cannot be negative".into())
        } else {
            Ok(())
        }
    })
}

fn int_validator() -> LimitsMapValidator<i64> {
    Arc::new(|_, v| {
        if *v < 0 {
            Err("value cannot be negative".into())
        } else {
            Ok(())
        }
    })
}

fn str_validator() -> LimitsMapValidator<String> {
    Arc::new(|_, v| {
        if v.is_empty() {
            Err("value cannot be empty".into())
        } else {
            Ok(())
        }
    })
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestNewLimitsMap
#[test]
fn test_new_limits_map() {
    let mut lm_f = ValidatedLimitsMap::new(Some(f64_validator()));
    lm_f.read_mut().unwrap().insert("key1".into(), 10.6);
    assert_eq!(lm_f.read().unwrap().len(), 1);

    let mut lm_i = ValidatedLimitsMap::new(Some(int_validator()));
    lm_i.read_mut().unwrap().insert("key1".into(), 10);
    assert_eq!(lm_i.read().unwrap().len(), 1);

    let mut lm_s = ValidatedLimitsMap::new(Some(str_validator()));
    lm_s.read_mut().unwrap().insert("key1".into(), "test".into());
    assert_eq!(lm_s.read().unwrap().len(), 1);
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_IsNil
#[test]
fn test_limits_map_is_nil() {
    let init = ValidatedLimitsMap::new_with_data(
        Some(BTreeMap::from([("key1".into(), 10.0)])),
        None,
    );
    assert!(init.is_initialized());

    let uninit = ValidatedLimitsMap::<f64>::uninitialized();
    assert!(!uninit.is_initialized());
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_SetAndString
#[test]
fn test_limits_map_set_and_string() {
    // numeric
    {
        let mut lm = ValidatedLimitsMap::new(Some(f64_validator()));
        lm.set(r#"{"key1":10,"key2":20}"#).unwrap();
        assert_eq!(
            lm.read().unwrap(),
            &BTreeMap::from([("key1".into(), 10.0), ("key2".into(), 20.0)])
        );
        assert_eq!(lm.to_string(), r#"{"key1":10,"key2":20}"#);

        let err = ValidatedLimitsMap::new(Some(f64_validator()))
            .set(r#"{"key1": 10, "key2": 20"#)
            .unwrap_err();
        assert!(!err.is_empty());

        let err = ValidatedLimitsMap::new(Some(f64_validator()))
            .set(r#"{"key1": -10, "key2": 20}"#)
            .unwrap_err();
        assert_eq!(err, "value cannot be negative");

        let err = ValidatedLimitsMap::new(Some(f64_validator()))
            .set(r#"{"key1": "abc", "key2": "def"}"#)
            .unwrap_err();
        assert!(err.contains("invalid type"), "{err}");
    }

    // string
    {
        let mut lm = ValidatedLimitsMap::new(Some(str_validator()));
        lm.set(r#"{"key1":"abc","key2":"def"}"#).unwrap();
        assert_eq!(
            lm.read().unwrap(),
            &BTreeMap::from([("key1".into(), "abc".into()), ("key2".into(), "def".into())])
        );
        assert_eq!(lm.to_string(), r#"{"key1":"abc","key2":"def"}"#);

        let err = ValidatedLimitsMap::new(Some(str_validator()))
            .set(r#"{"key1": "abc", "key2": "def"#)
            .unwrap_err();
        assert!(!err.is_empty());

        let err = ValidatedLimitsMap::new(Some(str_validator()))
            .set(r#"{"key1": "", "key2": "def"}"#)
            .unwrap_err();
        assert_eq!(err, "value cannot be empty");

        let err = ValidatedLimitsMap::new(Some(str_validator()))
            .set(r#"{"key1": 10, "key2": 20}"#)
            .unwrap_err();
        assert!(err.contains("invalid type"), "{err}");
    }
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_UnmarshalYAML
#[test]
fn test_limits_map_unmarshal_yaml() {
    // numeric
    {
        let mut lm = ValidatedLimitsMap::new(Some(f64_validator()));
        lm.unmarshal_yaml("\nkey1: 10\nkey2: 20\n").unwrap();
        assert_eq!(
            lm.read().unwrap(),
            &BTreeMap::from([("key1".into(), 10.0), ("key2".into(), 20.0)])
        );

        let mut lm = ValidatedLimitsMap::new(Some(f64_validator()));
        assert_eq!(
            lm.unmarshal_yaml("\nkey1: -10\nkey2: 20\n").unwrap_err(),
            "value cannot be negative"
        );

        let mut lm = ValidatedLimitsMap::new(Some(f64_validator()));
        assert!(lm
            .unmarshal_yaml("\nkey1: 10\nkey2: 20\n\tkey3: 30\n")
            .is_err());
    }

    // string
    {
        let mut lm = ValidatedLimitsMap::new(Some(str_validator()));
        lm.unmarshal_yaml("\nkey1: abc\nkey2: def\n").unwrap();
        assert_eq!(
            lm.read().unwrap(),
            &BTreeMap::from([("key1".into(), "abc".into()), ("key2".into(), "def".into())])
        );

        let mut lm = ValidatedLimitsMap::new(Some(str_validator()));
        assert_eq!(
            lm.unmarshal_yaml("\nkey1: abc\nkey2: \"\"\n").unwrap_err(),
            "value cannot be empty"
        );

        let mut lm = ValidatedLimitsMap::new(Some(str_validator()));
        assert!(lm
            .unmarshal_yaml("\nkey1: abc\nkey2: def\n\tkey3: ghi\n")
            .is_err());
    }
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_MarshalYAML
#[test]
fn test_limits_map_marshal_yaml() {
    let mut lm_f = ValidatedLimitsMap::new(Some(f64_validator()));
    lm_f.read_mut().unwrap().insert("key1".into(), 10.0);
    lm_f.read_mut().unwrap().insert("key2".into(), 20.0);
    assert_eq!(marshal_go_yaml(&lm_f), "key1: 10\nkey2: 20\n");

    let mut lm_s = ValidatedLimitsMap::new(Some(str_validator()));
    lm_s.read_mut().unwrap().insert("key1".into(), "abc".into());
    lm_s.read_mut().unwrap().insert("key2".into(), "def".into());
    assert_eq!(marshal_go_yaml(&lm_s), "key1: abc\nkey2: def\n");
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_Equal
#[test]
fn test_limits_map_equal() {
    let m1 = ValidatedLimitsMap::new_with_data(
        Some(BTreeMap::from([("key1".into(), 1.1), ("key2".into(), 2.2)])),
        None,
    );
    let m2 = ValidatedLimitsMap::new_with_data(
        Some(BTreeMap::from([("key1".into(), 1.1), ("key2".into(), 2.2)])),
        None,
    );
    assert_eq!(m1, m2);

    let e1 = ValidatedLimitsMap::<f64>::new(None);
    let e2 = ValidatedLimitsMap::<f64>::new(None);
    assert_eq!(e1, e2);

    let d1 = ValidatedLimitsMap::new_with_data(Some(BTreeMap::from([("key1".into(), 1.1)])), None);
    let d2 = ValidatedLimitsMap::new_with_data(Some(BTreeMap::from([("key1".into(), 1.2)])), None);
    assert_ne!(d1, m2);
    assert_ne!(d1, d2);

    let s1 = ValidatedLimitsMap::new_with_data(
        Some(BTreeMap::from([
            ("key1".into(), "abc".to_string()),
            ("key2".into(), "def".to_string()),
        ])),
        None,
    );
    let s2 = ValidatedLimitsMap::new_with_data(
        Some(BTreeMap::from([
            ("key1".into(), "abc".to_string()),
            ("key2".into(), "def".to_string()),
        ])),
        None,
    );
    assert_eq!(s1, s2);
    let s3 = ValidatedLimitsMap::new_with_data(
        Some(BTreeMap::from([("key1".into(), "abc".to_string())])),
        None,
    );
    assert_ne!(s1, s3);
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_Clone
#[test]
fn test_limits_map_clone() {
    let mut original = ValidatedLimitsMap::<f64>::new(None);
    original.read_mut().unwrap().insert("limit1".into(), 1.0);
    original.read_mut().unwrap().insert("limit2".into(), 2.0);
    let mut cloned = original.clone();
    assert_eq!(original, cloned);
    original.read_mut().unwrap().insert("limit1".into(), 10.0);
    assert_ne!(cloned.read().unwrap().get("limit1"), Some(&10.0));
    cloned.read_mut().unwrap().insert("limit3".into(), 3.0);
    assert!(!original.read().unwrap().contains_key("limit3"));

    let mut original_s = ValidatedLimitsMap::<String>::new(None);
    original_s
        .read_mut()
        .unwrap()
        .insert("limit1".into(), "abc".into());
    original_s
        .read_mut()
        .unwrap()
        .insert("limit2".into(), "def".into());
    let mut cloned_s = original_s.clone();
    assert_eq!(original_s, cloned_s);
    original_s
        .read_mut()
        .unwrap()
        .insert("limit1".into(), "zxcv".into());
    assert_ne!(
        cloned_s.read().unwrap().get("limit1").map(String::as_str),
        Some("zxcv")
    );
    cloned_s
        .read_mut()
        .unwrap()
        .insert("limit3".into(), "test".into());
    assert!(!original_s.read().unwrap().contains_key("limit3"));
}

// Upstream: github.com/grafana/dskit/flagext/map_test.go::TestLimitsMap_updateMap
#[test]
fn test_limits_map_update_map() {
    let initial = BTreeMap::from([("a".into(), 1.0), ("b".into(), 2.0)]);
    let bad_update = BTreeMap::from([("a".into(), 3.0), ("b".into(), -3.0), ("c".into(), 5.0)]);
    let mut lm = ValidatedLimitsMap::new_with_data(Some(initial.clone()), Some(f64_validator()));
    assert!(lm.update_map(bad_update).is_err());
    assert_eq!(lm.read().unwrap(), &initial);

    let good_update = BTreeMap::from([("b".into(), 5.0), ("c".into(), 6.0)]);
    lm.update_map(good_update.clone()).unwrap();
    assert_eq!(lm.read().unwrap(), &good_update);
}

// =============================================================================================
// parse_test.go (6 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/parse_test.go::TestParseFlagsAndArgumentsWithEnv
#[test]
fn test_parse_flags_and_arguments_with_env() {
    let _lock = ENV_MUTEX.lock().unwrap();
    let _env = EnvGuard::set("APP_SERVER_PORT", "9090");

    struct Target {
        host: String,
        port: i64,
    }
    let mut t = Target {
        host: "localhost".into(),
        port: 80,
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("server.host", "localhost", "", |x| &x.host, |x| &mut x.host);
    fs.i64_var("server.port", 80, "", |x| &x.port, |x| &mut x.port);

    let args = vec!["-server.host".into(), "clihost".into()];
    let rest = parse_flags_and_arguments_with_env(&mut fs, &mut t, &args, "APP").unwrap();
    assert!(rest.is_empty());
    assert_eq!(t.host, "clihost");
    assert_eq!(t.port, 9090);
}

// Upstream: github.com/grafana/dskit/flagext/parse_test.go::TestParseFlagsWithoutArgumentsWithEnv
#[test]
fn test_parse_flags_without_arguments_with_env() {
    let _lock = ENV_MUTEX.lock().unwrap();
    let _env = EnvGuard::set("APP_SERVER_PORT", "9090");

    struct Target {
        host: String,
        port: i64,
    }
    let mut t = Target {
        host: "localhost".into(),
        port: 80,
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("server.host", "localhost", "", |x| &x.host, |x| &mut x.host);
    fs.i64_var("server.port", 80, "", |x| &x.port, |x| &mut x.port);

    let args = vec!["-server.host".into(), "clihost".into()];
    parse_flags_without_arguments_with_env(&mut fs, &mut t, &args, "APP").unwrap();
    assert_eq!(t.host, "clihost");
    assert_eq!(t.port, 9090);
}

// Upstream: github.com/grafana/dskit/flagext/parse_test.go::TestParseFlagsWithoutArgumentsWithEnv_RejectsArguments
#[test]
fn test_parse_flags_without_arguments_with_env_rejects_arguments() {
    let _lock = ENV_MUTEX.lock().unwrap();
    struct Target {
        flag: String,
    }
    let mut t = Target {
        flag: "default".into(),
    };
    let mut fs = FlagSet::<Target>::new();
    fs.string_var("flag", "default", "", |x| &x.flag, |x| &mut x.flag);

    let args = vec!["unexpected-argument".into()];
    let err = parse_flags_without_arguments_with_env(&mut fs, &mut t, &args, "APP")
        .unwrap_err()
        .to_string();
    assert!(err.contains("unexpected-argument"), "{err}");
}

// Upstream: github.com/grafana/dskit/flagext/parse_test.go::TestParseFlagsAndArgumentsWithEnv_CLIPrecedence
#[test]
fn test_parse_flags_and_arguments_with_env_cli_precedence() {
    let _lock = ENV_MUTEX.lock().unwrap();
    let _env = EnvGuard::set("APP_PORT", "9090");

    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("port", 80, "", |x| &x.port, |x| &mut x.port);

    let args = vec!["-port".into(), "3000".into()];
    parse_flags_and_arguments_with_env(&mut fs, &mut t, &args, "APP").unwrap();
    assert_eq!(t.port, 3000);
}

// Upstream: github.com/grafana/dskit/flagext/parse_test.go::TestParseFlagsAndArgumentsWithEnv_EnvError
#[test]
fn test_parse_flags_and_arguments_with_env_env_error() {
    let _lock = ENV_MUTEX.lock().unwrap();
    let _env = EnvGuard::set("APP_PORT", "not-a-number");

    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("port", 80, "", |x| &x.port, |x| &mut x.port);

    let err = parse_flags_and_arguments_with_env(&mut fs, &mut t, &[], "APP")
        .unwrap_err()
        .to_string();
    assert!(err.contains("port"), "{err}");
}

// Upstream: github.com/grafana/dskit/flagext/parse_test.go::TestParseFlagsAndArgumentsWithEnv_ReturnsPositionalArgs
#[test]
fn test_parse_flags_and_arguments_with_env_returns_positional_args() {
    let _lock = ENV_MUTEX.lock().unwrap();
    struct Target {
        port: i64,
    }
    let mut t = Target { port: 80 };
    let mut fs = FlagSet::<Target>::new();
    fs.i64_var("port", 80, "", |x| &x.port, |x| &mut x.port);

    let args = vec![
        "-port".into(),
        "3000".into(),
        "arg1".into(),
        "arg2".into(),
    ];
    let rest = parse_flags_and_arguments_with_env(&mut fs, &mut t, &args, "APP").unwrap();
    assert_eq!(rest, &["arg1".to_string(), "arg2".to_string()]);
    assert_eq!(t.port, 3000);
}

// =============================================================================================
// register_test.go (1 test)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/register_test.go::TestTrackRegisteredFlags
#[test]
fn test_track_registered_flags() {
    #[derive(Default)]
    struct Target {
        previous: String,
        registered: String,
        non_prefixed: String,
    }
    let prefix = "testing.";
    let flag_name = "flag";
    let mut t = Target::default();
    let mut fs = FlagSet::<Target>::new();
    fs.string_var(
        "previous.flag",
        "previous",
        "",
        |x| &x.previous,
        |x| &mut x.previous,
    );

    let rf = track_registered_flags(prefix, &mut fs, |pfx, fs| {
        let full = format!("{pfx}{flag_name}");
        fs.string_var(
            &full,
            "registered",
            "",
            |x| &x.registered,
            |x| &mut x.registered,
        );
        fs.string_var(
            flag_name,
            "non-prefixed",
            "",
            |x| &x.non_prefixed,
            |x| &mut x.non_prefixed,
        );
    });

    assert_eq!(rf.prefix, prefix);
    assert_eq!(rf.flags.len(), 1);
    let tracked = rf.flags.get(flag_name).unwrap();
    assert_eq!(tracked, &format!("{prefix}{flag_name}"));
    fs.set_value(&mut t, tracked, "changed").unwrap();
    assert_eq!(t.registered, "changed");
}

// =============================================================================================
// secret_test.go (2 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/secret_test.go::TestSecretdYAML
#[test]
fn test_secretd_yaml() {
    // Embedded Secret
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            secret: Secret,
        }
        let mut test_struct = TestStruct::default();
        test_struct.secret.set("pa55w0rd");
        let expected = "secret: '********'\n";
        assert_eq!(marshal_go_yaml(&test_struct), expected);

        let actual: TestStruct = serde_yaml::from_str("secret: pa55w0rd\n").unwrap();
        assert_eq!(test_struct, actual);
    }

    // Pointer (Option) Secret
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            secret: Option<Secret>,
        }
        let mut s = Secret::default();
        s.set("pa55w0rd");
        let test_struct = TestStruct { secret: Some(s) };
        let expected = "secret: '********'\n";
        assert_eq!(marshal_go_yaml(&test_struct), expected);

        let actual: TestStruct = serde_yaml::from_str("secret: pa55w0rd\n").unwrap();
        assert_eq!(test_struct, actual);
    }

    // Empty Secret
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            secret: Secret,
        }
        let test_struct = TestStruct::default();
        let expected = "secret: \"\"\n";
        assert_eq!(marshal_go_yaml(&test_struct), expected);

        let actual: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(test_struct, actual);
    }
}

// Upstream: github.com/grafana/dskit/flagext/secret_test.go::TestSecret_Equals
#[test]
fn test_secret_equals() {
    assert_eq!(
        Secret::with_value("somesecret"),
        Secret::with_value("somesecret")
    );
    assert_eq!(Secret::with_value(""), Secret::with_value(""));
    assert_ne!(
        Secret::with_value("somesecret"),
        Secret::with_value("anothersecret")
    );
    assert_ne!(
        Secret::with_value("secretone"),
        Secret::with_value("secrettwo")
    );
    assert_ne!(Secret::with_value("somesecret"), Secret::with_value(""));
}

// =============================================================================================
// stringslicecsvmulti_test.go (5 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/stringslicecsvmulti_test.go::TestStringSliceCSVMulti
#[test]
fn test_string_slice_csv_multi() {
    #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct {
        csv: StringSliceCSVMulti,
    }
    let mut ts = TestStruct::default();
    let s = "a,b,c,d";
    ts.csv.append_csv(s);
    assert_eq!(ts.csv.0, vec!["a", "b", "c", "d"]);
    assert_eq!(ts.csv.to_string(), s);

    let expected = "csv: a,b,c,d\n";
    assert_eq!(marshal_go_yaml(&ts), expected);

    let ts2: TestStruct = serde_yaml::from_str(expected).unwrap();
    assert_eq!(ts, ts2);
}

// Upstream: github.com/grafana/dskit/flagext/stringslicecsvmulti_test.go::TestStringSliceCSVMulti_MultipleSet
#[test]
fn test_string_slice_csv_multi_multiple_set() {
    let mut v = StringSliceCSVMulti::default();
    v.append_csv("a,b");
    v.append_csv("c");
    v.append_csv("d,e,f");
    assert_eq!(v.0, vec!["a", "b", "c", "d", "e", "f"]);
    assert_eq!(v.to_string(), "a,b,c,d,e,f");
}

// Upstream: github.com/grafana/dskit/flagext/stringslicecsvmulti_test.go::TestStringSliceCSVMulti_EmptyString
#[test]
fn test_string_slice_csv_multi_empty_string() {
    #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct {
        csv: StringSliceCSVMulti,
    }
    let ts_empty = TestStruct::default();
    assert!(ts_empty.csv.is_empty());
    let expected = "csv: \"\"\n";
    let actual = marshal_go_yaml(&ts_empty);
    assert_eq!(actual, expected);

    let ts2: TestStruct = serde_yaml::from_str(&actual).unwrap();
    assert_eq!(ts_empty, ts2);
}

// Upstream: github.com/grafana/dskit/flagext/stringslicecsvmulti_test.go::TestStringSliceCSVMulti_EmptyStringPreservesExisting
#[test]
fn test_string_slice_csv_multi_empty_string_preserves_existing() {
    let mut v = StringSliceCSVMulti::default();
    v.append_csv("a,b");
    v.append_csv("");
    v.append_csv("c");
    assert_eq!(v.0, vec!["a", "b", "c"]);
}

// Upstream: github.com/grafana/dskit/flagext/stringslicecsvmulti_test.go::TestStringSliceCSVMulti_YAMLListFormat
#[test]
fn test_string_slice_csv_multi_yaml_list_format() {
    #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct {
        csv: StringSliceCSVMulti,
    }
    let yaml_list = "csv:\n  - node1:7946\n  - node2:7946\n";
    let ts: TestStruct = serde_yaml::from_str(yaml_list).unwrap();
    assert_eq!(ts.csv.0, vec!["node1:7946", "node2:7946"]);
}

// =============================================================================================
// stringslicecsv_test.go (2 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/stringslicecsv_test.go::Test_StringSliceCSV
#[test]
fn test_string_slice_csv() {
    #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct {
        csv: StringSliceCSV,
    }
    let mut ts = TestStruct::default();
    let s = "a,b,c,d";
    ts.csv.set(s);
    assert_eq!(ts.csv.0, vec!["a", "b", "c", "d"]);
    assert_eq!(ts.csv.to_string(), s);

    let expected = "csv: a,b,c,d\n";
    assert_eq!(marshal_go_yaml(&ts), expected);

    let ts2: TestStruct = serde_yaml::from_str(expected).unwrap();
    assert_eq!(ts, ts2);
}

// Upstream: github.com/grafana/dskit/flagext/stringslicecsv_test.go::Test_EmptyStringSliceCSV
#[test]
fn test_empty_string_slice_csv() {
    #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct {
        csv: StringSliceCSV,
    }
    let ts_empty = TestStruct::default();
    assert!(ts_empty.csv.is_empty());
    let expected = "csv: \"\"\n";
    let actual = marshal_go_yaml(&ts_empty);
    assert_eq!(actual, expected);

    let ts2: TestStruct = serde_yaml::from_str(&actual).unwrap();
    assert_eq!(ts_empty, ts2);
}

// =============================================================================================
// time_test.go (2 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/time_test.go::TestTimeYAML
#[test]
fn test_time_yaml() {
    // Embedded FlagTime
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            time: FlagTime,
        }
        let mut ts = TestStruct::default();
        ts.time.set("2020-10-20").unwrap();
        let expected = "time: \"2020-10-20T00:00:00Z\"\n";
        assert_eq!(marshal_go_yaml(&ts), expected);

        let actual: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(ts, actual);
    }

    // Pointer (Option) FlagTime
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            time: Option<FlagTime>,
        }
        let mut t = FlagTime::default();
        t.set("2020-10-20").unwrap();
        let ts = TestStruct { time: Some(t) };
        let expected = "time: \"2020-10-20T00:00:00Z\"\n";
        assert_eq!(marshal_go_yaml(&ts), expected);

        let actual: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(ts, actual);
    }
}

// Upstream: github.com/grafana/dskit/flagext/time_test.go::TestTimeFormats
#[test]
fn test_time_formats() {
    let mut ts = FlagTime::default();
    ts.set("0").unwrap();
    assert!(ts.is_zero());
    assert_eq!(ts.to_string(), "0");

    ts.set("2020-10-05").unwrap();
    assert_eq!(ts.to_string(), "2020-10-05T00:00:00Z");

    ts.set("2020-10-05T13:27").unwrap();
    assert_eq!(ts.to_string(), "2020-10-05T13:27:00Z");

    ts.set("2020-10-05T13:27:00Z").unwrap();
    assert_eq!(ts.to_string(), "2020-10-05T13:27:00Z");

    ts.set("2020-10-05T13:27:00+02:00").unwrap();
    assert_eq!(ts.to_string(), "2020-10-05T13:27:00+02:00");
}

// =============================================================================================
// url_test.go (1 test)
// =============================================================================================

// Upstream: github.com/grafana/dskit/flagext/url_test.go::TestURLValueYAML
#[test]
fn test_url_value_yaml() {
    // Embedded URLValue
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            url: URLValue,
        }
        let mut ts = TestStruct::default();
        ts.url.set("http://google.com").unwrap();
        let expected = "url: http://google.com\n";
        assert_eq!(marshal_go_yaml(&ts), expected);

        let actual: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(ts, actual);
    }

    // Pointer (Option) URLValue
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            url: Option<URLValue>,
        }
        let mut u = URLValue::default();
        u.set("http://google.com").unwrap();
        let ts = TestStruct { url: Some(u) };
        let expected = "url: http://google.com\n";
        assert_eq!(marshal_go_yaml(&ts), expected);

        let actual: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(ts, actual);
    }

    // Empty URLValue
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            url: URLValue,
        }
        let ts = TestStruct::default();
        let expected = "url: \"\"\n";
        assert_eq!(marshal_go_yaml(&ts), expected);

        let actual: TestStruct = serde_yaml::from_str(expected).unwrap();
        assert_eq!(ts, actual);
    }

    // Password masking
    {
        #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        struct TestStruct {
            url: URLValue,
        }
        let mut ts = TestStruct::default();
        ts.url.set("http://username:password@google.com").unwrap();
        let expected = "url: http://username:xxxxx@google.com\n";
        assert_eq!(marshal_go_yaml(&ts), expected);
    }
}
