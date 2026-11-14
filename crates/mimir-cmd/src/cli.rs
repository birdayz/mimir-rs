//! CLI argument parsing, config loading, YAML dumping, and diffing.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/util/yaml.go (`DiffConfig`)

use mimir_config::schema::{FieldKind, SchemaBlock, EXTRA_TOP_BLOCKS};
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
                    default_yaml_value(field.kind, field.default_str)
                } else {
                    explicit.clone()
                }
            } else {
                default_yaml_value(field.kind, field.default_str)
            }
        } else if let Some(explicit) = yaml_map.and_then(|m| m.get(&yaml_key)) {
            explicit.clone()
        } else {
            default_yaml_value(field.kind, field.default_str)
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
        FieldKind::Other => Value::String(default_str.to_string()),
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
        FieldKind::Other => Value::String(raw.to_string()),
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
