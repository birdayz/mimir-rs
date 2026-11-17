//! Per-source decoding and left-to-right merging of runtime config sources.
//!
//! Ported from github.com/grafana/dskit/runtimeconfig/manager.go
//! (`unmarshalMaybeGzipped`, `unmarshalJSONOrYAML`, `looksLikeJSONObject`, `isGzip`,
//! `mergeConfigMaps`).
//!
//! Go decodes every source into a `map[string]any` with `go.yaml.in/yaml/v3`. That decoder only
//! reads the *first* YAML document, turns top-level keys into strings, and represents a nested
//! mapping as `map[string]any` only when all of its keys are `!!str`; any other nested mapping
//! (e.g. unquoted numeric tenant IDs) becomes `map[any]any`, which `mergeConfigMaps` does not
//! treat as a map. These rules are reproduced on top of `serde_yaml::Value`: a mapping whose keys
//! are all strings is a "string map", anything else is an opaque value.

use std::io::Read;

use serde::Deserialize;
use serde_yaml::{Mapping, Value};

/// True if `v` is what yaml.v3 decodes into `map[string]interface{}`.
pub(crate) fn is_string_map(v: &Value) -> bool {
    match v {
        Value::Mapping(m) => m.keys().all(|k| matches!(k, Value::String(_))),
        _ => false,
    }
}

/// Go `%T` of the value yaml.v3 / encoding/json would produce.
pub(crate) fn go_type_name(v: Option<&Value>) -> &'static str {
    match v {
        None | Some(Value::Null) => "<nil>",
        Some(Value::Bool(_)) => "bool",
        Some(Value::Number(n)) => {
            if n.is_i64() {
                "int"
            } else if n.is_u64() {
                "uint64"
            } else {
                "float64"
            }
        }
        Some(Value::String(_)) | Some(Value::Tagged(_)) => "string",
        Some(Value::Sequence(_)) => "[]interface {}",
        Some(m @ Value::Mapping(_)) => {
            if is_string_map(m) {
                "map[string]interface {}"
            } else {
                "map[interface {}]interface {}"
            }
        }
    }
}

/// Converts a YAML scalar key to the string yaml.v3 stores when decoding into `map[string]any`.
pub(crate) fn scalar_key_to_string(k: &Value) -> Option<String> {
    match k {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

fn looks_like_json_object(data: &[u8]) -> bool {
    data.iter()
        .find(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        .is_some_and(|b| *b == b'{')
}

fn is_gzip(data: &[u8]) -> bool {
    data.len() > 2 && data[0] == 0x1f && data[1] == 0x8b
}

/// JSON values: encoding/json yields `float64` for every number. Integral values are kept as
/// integers so that the subsequent YAML-style decoding into integer fields succeeds exactly as
/// Go's `yaml.Marshal(float64(n))` -> `int` round trip does.
fn json_to_yaml(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                Value::Number(u.into())
            } else {
                let f = n.as_f64().unwrap_or(0.0);
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Value::Number((f as i64).into())
                } else {
                    Value::Number(f.into())
                }
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(a) => Value::Sequence(a.into_iter().map(json_to_yaml).collect()),
        serde_json::Value::Object(o) => Value::Mapping(
            o.into_iter()
                .map(|(k, v)| (Value::String(k), json_to_yaml(v)))
                .collect(),
        ),
    }
}

/// `yaml.Unmarshal(data, &map[string]any{})`: first document only, top-level keys stringified.
fn unmarshal_yaml_map(data: &[u8]) -> Result<Mapping, String> {
    let Some(doc) = serde_yaml::Deserializer::from_slice(data).next() else {
        return Ok(Mapping::new());
    };
    let mut val = Value::deserialize(doc).map_err(|e| format!("yaml: {e}"))?;
    val.apply_merge().map_err(|e| format!("yaml: {e}"))?;
    match val {
        Value::Null => Ok(Mapping::new()),
        Value::Mapping(m) => {
            let mut out = Mapping::with_capacity(m.len());
            for (k, v) in m {
                let key = scalar_key_to_string(&k).ok_or_else(|| {
                    "yaml: unmarshal errors:\n  invalid map key: non-scalar key".to_string()
                })?;
                out.insert(Value::String(key), v);
            }
            Ok(out)
        }
        other => Err(format!(
            "yaml: unmarshal errors:\n  cannot unmarshal {} into map[string]interface {{}}",
            match other {
                Value::Sequence(_) => "!!seq",
                Value::Bool(_) => "!!bool",
                Value::Number(_) => "!!int",
                _ => "!!str",
            }
        )),
    }
}

fn unmarshal_json_or_yaml(data: &[u8]) -> Result<Mapping, String> {
    if looks_like_json_object(data) {
        if let Ok(serde_json::Value::Object(o)) = serde_json::from_slice::<serde_json::Value>(data)
        {
            return match json_to_yaml(serde_json::Value::Object(o)) {
                Value::Mapping(m) => Ok(m),
                _ => Ok(Mapping::new()),
            };
        }
    }
    unmarshal_yaml_map(data)
}

/// `unmarshalMaybeGzipped`.
pub(crate) fn unmarshal_maybe_gzipped(name: &str, data: &[u8]) -> Result<Mapping, String> {
    if name.ends_with(".gz") {
        let mut dec = flate2::read::GzDecoder::new(data);
        let mut out = Vec::new();
        dec.read_to_end(&mut out)
            .map_err(|e| format!("read gzipped file: {e}"))?;
        return unmarshal_yaml_map(&out).map_err(|e| format!("uncompress/unmarshal gzipped file: {e}"));
    }
    unmarshal_json_or_yaml(data).map_err(|e| {
        if is_gzip(data) {
            format!("file looks gzipped but doesn't have a .gz extension: {e}")
        } else {
            e
        }
    })
}

/// `mergeConfigMaps(a, b, path)`: deep merge of string maps, erroring on map/non-map conflicts.
pub(crate) fn merge_config_maps(a: &Mapping, b: &Mapping, path: &str) -> Result<Mapping, String> {
    let mut out = a.clone();
    for (k, v) in b {
        let key = k.as_str().unwrap_or_default();
        let a_val = a.get(k);
        let a_has_key = a_val.is_some();
        let mut a_is_map = a_val.is_some_and(is_string_map);
        let mut b_is_map = is_string_map(v);
        let mut v = v.clone();

        if a_has_key && a_val.is_some_and(Value::is_null) && b_is_map {
            a_is_map = true;
            out.insert(k.clone(), Value::Mapping(Mapping::new()));
        }
        if v.is_null() && a_is_map {
            b_is_map = true;
            v = Value::Mapping(Mapping::new());
        }

        if a_has_key && a_is_map != b_is_map {
            return Err(format!(
                "conflicting types for {:?}: {} != {}",
                format!("{path}.{key}"),
                go_type_name(a_val),
                go_type_name(b.get(k)),
            ));
        }

        if let Value::Mapping(vm) = &v {
            if b_is_map {
                if let Some(existing @ Value::Mapping(existing_map)) = out.get(k) {
                    if is_string_map(existing) {
                        let merged =
                            merge_config_maps(existing_map, vm, &format!("{path}.{key}"))?;
                        out.insert(k.clone(), Value::Mapping(merged));
                        continue;
                    }
                }
            }
        }
        out.insert(k.clone(), v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(y: &str) -> Mapping {
        unmarshal_maybe_gzipped("x.yaml", y.as_bytes()).unwrap()
    }

    #[test]
    fn first_document_only() {
        let got = m("a: 1\n---\nb: 2\n");
        assert_eq!(got, m("a: 1\n"));
    }

    #[test]
    fn json_object_detected() {
        let got = unmarshal_maybe_gzipped("x.json", br#" {"overrides": {"t": {"x": 1}}}"#).unwrap();
        assert_eq!(got, m("overrides:\n  t:\n    x: 1\n"));
    }

    #[test]
    fn gz_extension_and_hint() {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(b"a: 1\n").unwrap();
        let gz = enc.finish().unwrap();
        assert_eq!(unmarshal_maybe_gzipped("x.yaml.gz", &gz).unwrap(), m("a: 1\n"));
        let err = unmarshal_maybe_gzipped("x.yaml", &gz).unwrap_err();
        assert!(err.starts_with("file looks gzipped but doesn't have a .gz extension"), "{err}");
    }

    #[test]
    fn merge_deep_and_conflicts() {
        let a = m("overrides:\n  t1:\n    x: 1\n");
        let b = m("overrides:\n  t2:\n    x: 2\n");
        let got = merge_config_maps(&a, &b, "").unwrap();
        assert_eq!(got, m("overrides:\n  t1:\n    x: 1\n  t2:\n    x: 2\n"));

        let err = merge_config_maps(&a, &m("overrides: foo\n"), "").unwrap_err();
        assert_eq!(
            err,
            r#"conflicting types for ".overrides": map[string]interface {} != string"#
        );

        // nil on either side of a map merges as an empty map.
        let got = merge_config_maps(&a, &m("overrides:\n"), "").unwrap();
        assert_eq!(got, a);
        let got = merge_config_maps(&m("overrides:\n"), &b, "").unwrap();
        assert_eq!(got, b);

        // Non-string-keyed maps are opaque: replaced instead of merged.
        let n1 = m("overrides:\n  1:\n    x: 1\n");
        let n2 = m("overrides:\n  2:\n    x: 2\n");
        assert_eq!(merge_config_maps(&n1, &n2, "").unwrap(), n2);
        let err = merge_config_maps(&a, &n2, "").unwrap_err();
        assert_eq!(
            err,
            r#"conflicting types for ".overrides": map[string]interface {} != map[interface {}]interface {}"#
        );
    }
}
