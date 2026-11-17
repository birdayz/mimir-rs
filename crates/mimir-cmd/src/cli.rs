//! CLI argument parsing, config loading, YAML dumping, and diffing.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/util/yaml.go (`DiffConfig`)

use mimir_config::schema::{FieldKind, SchemaBlock, SchemaField, EXTRA_TOP_BLOCKS};
use mimir_config::{
    format_go_duration, format_modules, format_version, parse_cli_and_load_config, CliAction,
    ConfigError, GoDuration, MimirConfig,
};
use serde_yaml::{Mapping, Value};

/// Result of running the CLI frontend (used by `main` and `TestFlagParsing` in test mode).
#[derive(Debug, Clone)]
pub struct CliOutcome {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub config: Option<MimirConfig>,
    pub config_hash_sha256: Option<String>,
    pub action: Option<CliAction>,
    pub ballast_bytes: i64,
    pub rate_limited_logs_enabled: bool,
    pub rate_limited_logs_per_second: f64,
    pub rate_limited_logs_burst_size: i64,
}

/// Runs the CLI parser and config loader in test mode (mirroring `main()` with `testMode = true`).
pub fn run_cli_test_mode(prog_name: &str, args: &[String]) -> CliOutcome {
    execute_cli_inner(prog_name, args, true)
}

/// Parses CLI flags and loads configuration for normal binary startup.
pub fn parse_cli_for_run(prog_name: &str, args: &[String]) -> CliOutcome {
    execute_cli_inner(prog_name, args, false)
}

fn execute_cli_inner(prog_name: &str, args: &[String], test_mode: bool) -> CliOutcome {
    mimir_config::limits::set_active_series_custom_trackers_validator(
        mimir_ingester::active_series::validate_custom_trackers,
    );
    match parse_cli_and_load_config(prog_name, args) {
        Err(ConfigError::Flag(msg)) => CliOutcome {
            stdout: String::new(),
            stderr: format!("{msg}\nRun with -help to get a list of available parameters\n"),
            exit_code: 2,
            config: None,
            config_hash_sha256: None,
            action: None,
            ballast_bytes: 0,
            rate_limited_logs_enabled: false,
            rate_limited_logs_per_second: 10000.0,
            rate_limited_logs_burst_size: 1000,
        },
        Err(err) => CliOutcome {
            stdout: String::new(),
            stderr: format!("{err}\n"),
            exit_code: err.exit_code(),
            config: None,
            config_hash_sha256: None,
            action: None,
            ballast_bytes: 0,
            rate_limited_logs_enabled: false,
            rate_limited_logs_per_second: 10000.0,
            rate_limited_logs_burst_size: 1000,
        },
        Ok(loaded) => {
            let mut stdout = String::new();
            let exit_code = match &loaded.action {
                CliAction::PrintHelp { .. } => {
                    stdout = loaded.usage_text.clone().unwrap_or_default();
                    2
                }
                CliAction::PrintVersion => {
                    stdout = format!("{}\n", format_version());
                    0
                }
                CliAction::PrintModules => {
                    stdout = format_modules();
                    0
                }
                CliAction::PrintConfig => {
                    stdout = dump_yaml(&loaded.config);
                    0
                }
                CliAction::Run => {
                    if test_mode {
                        stdout = dump_yaml(&loaded.config);
                    }
                    0
                }
            };

            CliOutcome {
                stdout,
                stderr: String::new(),
                exit_code,
                config: Some(loaded.config),
                config_hash_sha256: loaded.config_hash_sha256,
                action: Some(loaded.action),
                ballast_bytes: loaded.main_flags.ballast_bytes,
                rate_limited_logs_enabled: loaded.main_flags.rate_limited_logs_enabled,
                rate_limited_logs_per_second: loaded.main_flags.rate_limited_logs_per_second,
                rate_limited_logs_burst_size: loaded.main_flags.rate_limited_logs_burst_size,
            }
        }
    }
}

/// Serializes `MimirConfig` (including schema defaults & CLI overrides for non-ingester blocks)
/// into a `serde_yaml::Value`.
pub fn effective_config_yaml(cfg: &MimirConfig) -> Value {
    let mut val = serde_yaml::to_value(cfg).unwrap_or(Value::Mapping(Mapping::new()));
    let Some(root_map) = val.as_mapping_mut() else {
        return val;
    };

    for block in EXTRA_TOP_BLOCKS {
        let existing = cfg.extra_sections.get(block.name);
        let merged = build_block_yaml(block, existing, cfg);
        root_map.insert(Value::String(block.name.to_string()), Value::Mapping(merged));
    }

    // Sections without a schema (hidden `usage_tracker`, `block_builder*`) are rendered from the
    // raw YAML; mask their `flagext.Secret` values like Go's MarshalYAML does.
    for name in cfg.extra_sections.keys() {
        if EXTRA_TOP_BLOCKS.iter().any(|b| b.name == name) {
            continue;
        }
        if let Some(v) = root_map.get_mut(Value::String(name.clone())) {
            mimir_config::secret::mask_secrets_by_key_name(v);
        }
    }

    val
}

/// Dumps the effective configuration as YAML (matching `DumpYaml` in `cmd/mimir/main.go`).
pub fn dump_yaml(cfg: &MimirConfig) -> String {
    let val = effective_config_yaml(cfg);
    match serde_yaml::to_string(&val) {
        Ok(s) => format!("{s}\n"),
        Err(e) => format!("# error encoding config: {e}\n"),
    }
}

fn build_block_yaml(
    block: &SchemaBlock,
    yaml_val: Option<&Value>,
    cfg: &MimirConfig,
) -> Mapping {
    let mut out = Mapping::new();
    let yaml_map = yaml_val.and_then(Value::as_mapping);

    for field in block.fields {
        let yaml_key = Value::String(field.name.to_string());
        let val = if !field.flag.is_empty() {
            if let Some(cli_val) = cfg.extra_cli_flag_value(field.flag) {
                parse_cli_value_for_yaml(field.kind, cli_val)
            } else if let Some(explicit) = yaml_map.and_then(|m| m.get(&yaml_key)) {
                if explicit.is_null() {
                    field_default_yaml_value(field)
                } else {
                    explicit.clone()
                }
            } else {
                field_default_yaml_value(field)
            }
        } else if let Some(explicit) = yaml_map.and_then(|m| m.get(&yaml_key)) {
            explicit.clone()
        } else {
            field_default_yaml_value(field)
        };
        let val = match field.kind {
            FieldKind::Secret => mask_secret_yaml_value(&val),
            FieldKind::SecretOmitEmpty => {
                if secret_yaml_value_is_empty(&val) {
                    continue;
                }
                mask_secret_yaml_value(&val)
            }
            _ => val,
        };
        out.insert(yaml_key, val);
    }

    for sub in block.blocks {
        let sub_key = Value::String(sub.name.to_string());
        let sub_yaml = yaml_map.and_then(|m| m.get(&sub_key));
        let sub_map = build_block_yaml(sub, sub_yaml, cfg);
        out.insert(sub_key, Value::Mapping(sub_map));
    }

    out
}

/// A secret is empty when its YAML value unmarshals into an empty string (`""`, `null`, `~`).
fn secret_yaml_value_is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

/// Renders a secret the way `flagext.Secret.MarshalYAML` does (`dskit/flagext/secret.go`).
fn mask_secret_yaml_value(v: &Value) -> Value {
    let masked = if secret_yaml_value_is_empty(v) {
        ""
    } else {
        mimir_config::secret::FLAGEXT_SECRET_MASK
    };
    Value::String(masked.to_string())
}

/// Default YAML value of a schema field. `instance_interface_names` defaults are computed at
/// flag registration time in Go (`netutil.PrivateNetworkInterfacesWithFallback([eth0 en0])`,
/// e.g. pkg/util/ring_config.go:53), so the effective config shows the detected interfaces.
fn field_default_yaml_value(field: &SchemaField) -> Value {
    if field.name == "instance_interface_names" && field.kind == FieldKind::StringList {
        return Value::Sequence(
            mimir_common::netutil::default_instance_interface_names()
                .into_iter()
                .map(Value::String)
                .collect(),
        );
    }
    default_yaml_value(field.kind, field.default_str)
}

fn default_yaml_value(kind: FieldKind, default_str: &str) -> Value {
    match kind {
        FieldKind::Bool => Value::Bool(default_str == "true"),
        FieldKind::Int => default_str
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|_| Value::Number(0.into())),
        FieldKind::Float => default_str
            .parse::<f64>()
            .ok()
            .and_then(|n| serde_yaml::to_value(n).ok())
            .unwrap_or_else(|| Value::Number(0.into())),
        FieldKind::Duration => {
            if let Ok(ns) = default_str.parse::<i64>() {
                Value::String(format_go_duration(ns))
            } else if default_str.is_empty() {
                Value::String("0s".to_string())
            } else {
                Value::String(default_str.to_string())
            }
        }
        FieldKind::String => Value::String(default_str.to_string()),
        FieldKind::StringList => {
            if default_str.is_empty() || default_str == "[]" {
                Value::Sequence(Vec::new())
            } else {
                Value::String(default_str.to_string())
            }
        }
        FieldKind::Other | FieldKind::Secret | FieldKind::SecretOmitEmpty => {
            Value::String(default_str.to_string())
        }
    }
}

fn parse_cli_value_for_yaml(kind: FieldKind, raw: &str) -> Value {
    match kind {
        FieldKind::Bool => Value::Bool(raw == "true" || raw == "1" || raw == "t" || raw == "T"),
        FieldKind::Int => raw
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        FieldKind::Float => raw
            .parse::<f64>()
            .ok()
            .and_then(|n| serde_yaml::to_value(n).ok())
            .unwrap_or_else(|| Value::String(raw.to_string())),
        FieldKind::Duration => {
            if let Ok(d) = raw.parse::<GoDuration>() {
                Value::String(d.to_string())
            } else {
                Value::String(raw.to_string())
            }
        }
        FieldKind::String => Value::String(raw.to_string()),
        FieldKind::StringList => {
            if raw.is_empty() {
                Value::Sequence(Vec::new())
            } else {
                Value::Sequence(raw.split(',').map(|s| Value::String(s.to_string())).collect())
            }
        }
        FieldKind::Other | FieldKind::Secret | FieldKind::SecretOmitEmpty => {
            Value::String(raw.to_string())
        }
    }
}

/// Recursively computes the diff between `default_val` and `actual_val`, returning only
/// keys whose values differ from defaults (matching `pkg/util/yaml.go:DiffConfig`).
pub fn diff_yaml_values(default_val: &Value, actual_val: &Value) -> Option<Value> {
    if default_val == actual_val {
        return None;
    }

    match (default_val, actual_val) {
        (Value::Mapping(def_map), Value::Mapping(act_map)) => {
            let mut diff_map = Mapping::new();
            for (k, act_v) in act_map {
                match def_map.get(k) {
                    Some(def_v) => {
                        if let Some(child_diff) = diff_yaml_values(def_v, act_v) {
                            diff_map.insert(k.clone(), child_diff);
                        }
                    }
                    None => {
                        diff_map.insert(k.clone(), act_v.clone());
                    }
                }
            }
            if diff_map.is_empty() {
                None
            } else {
                Some(Value::Mapping(diff_map))
            }
        }
        _ => Some(actual_val.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(yaml: &str) -> MimirConfig {
        let mut cfg = MimirConfig::default();
        cfg.load_config_from_bytes(yaml.as_bytes(), false).unwrap();
        cfg
    }

    // Go: dskit/flagext/secret.go MarshalYAML, prometheus/common/config.Secret MarshalYAML.
    #[test]
    fn dump_yaml_masks_secrets_like_go() {
        let cfg = load(
            r#"
blocks_storage:
  s3:
    secret_access_key: S3SECRETXYZ
    session_token: S3TOKENXYZ
  azure:
    account_key: AZKEYXYZ
    connection_string: AZCONNXYZ
  gcs:
    service_account: GCSSAXYZ
  swift:
    password: SWIFTPWXYZ
    application_credential_secret: SWIFTACSXYZ
ingest_storage:
  kafka:
    sasl_password: KAFKAPWXYZ
    sasl_oauthbearer_token: OAUTHXYZ
ingester:
  ring:
    kvstore:
      consul:
        acl_token: ACLXYZ
      etcd:
        password: ETCDXYZ
limits:
  ruler_alertmanager_client_config:
    basic_auth_password: RULERPWXYZ
    oauth2:
      client_secret: OAUTH2XYZ
server:
  http_tls_config:
    key: TLSKEYXYZ
ruler_storage:
  s3:
    secret_access_key: RSXYZ
distributor:
  ha_tracker:
    kvstore:
      etcd:
        password: HAXYZ
vault:
  auth:
    token:
      token: VAULTXYZ
usage_tracker:
  snapshots_storage:
    s3:
      secret_access_key: UTXYZ
"#,
        );
        let out = dump_yaml(&cfg);
        assert!(!out.contains("XYZ"), "secret leaked:\n{out}");
        assert!(out.contains("secret_access_key: '********'"));
        assert!(out.contains("sasl_password: '********'"));
        assert!(out.contains("basic_auth_password: '********'"));
        assert!(out.contains("key: <secret>"));
        // Empty secrets render as "" (flagext.Secret) / null (prometheus config.Secret).
        assert!(out.contains("sasl_msk_iam_secret_key: ''"));
        assert!(out.contains("key: null"));
        // `service_account_token` is `omitempty` in Go.
        assert!(!out.contains("service_account_token:"));

        // The in-memory config still holds the real values.
        assert_eq!(cfg.ingest_storage.kafka.sasl_password, "KAFKAPWXYZ");

        // `?mode=diff` compares masked values.
        let diff = diff_yaml_values(
            &effective_config_yaml(&MimirConfig::default()),
            &effective_config_yaml(&cfg),
        )
        .unwrap();
        let diff = serde_yaml::to_string(&diff).unwrap();
        assert!(!diff.contains("XYZ"), "secret leaked in diff:\n{diff}");
        assert!(diff.contains("sasl_password: '********'"));
    }
}
