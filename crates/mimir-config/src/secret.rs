//! YAML marshalling of secret configuration values.
//!
//! Ported from:
//! - github.com/grafana/dskit/flagext/secret.go (`flagext.Secret.MarshalYAML`)
//! - github.com/prometheus/common/config/config.go (`config.Secret.MarshalYAML`)
//!
//! Go keeps secrets in dedicated types whose `MarshalYAML` never reveals the value, so every
//! YAML rendering of the config (`/config` in all modes, `/runtime_config`, `-print.config`)
//! is masked. The Rust config structs keep secrets as plain `String`s (all consumers read them
//! as strings), and the masking happens at the serde `Serialize` level via
//! `#[serde(serialize_with = "...")]` on each secret field.
//!
//! Internal round trips through `serde_yaml::Value` (e.g. merging tenant overrides on top of
//! the default limits) must keep the real value; they run inside [`with_secrets_revealed`].

use std::cell::Cell;

use serde::Serializer;

/// Masked value emitted by `flagext.Secret.MarshalYAML` for non-empty secrets.
pub const FLAGEXT_SECRET_MASK: &str = "********";

/// Masked value emitted by Prometheus `config.Secret.MarshalYAML` (`secretToken`).
pub const PROMETHEUS_SECRET_TOKEN: &str = "<secret>";

thread_local! {
    static REVEAL_SECRETS: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with secret masking disabled on the current thread. Used only for internal
/// serialize/deserialize round trips that must preserve the real secret values.
pub fn with_secrets_revealed<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            REVEAL_SECRETS.with(|r| r.set(self.0));
        }
    }
    let prev = REVEAL_SECRETS.with(|r| r.replace(true));
    let _restore = Restore(prev);
    f()
}

fn secrets_revealed() -> bool {
    REVEAL_SECRETS.with(Cell::get)
}

/// Value rendered for a `flagext.Secret` holding `value`: `""` when empty, `"********"` otherwise.
pub fn mask_flagext_secret(value: &str) -> &'static str {
    if value.is_empty() {
        ""
    } else {
        FLAGEXT_SECRET_MASK
    }
}

/// Serializes a `String` field the way `flagext.Secret.MarshalYAML` does.
pub fn serialize_flagext_secret<S: Serializer>(value: &str, s: S) -> Result<S::Ok, S::Error> {
    if secrets_revealed() {
        return s.serialize_str(value);
    }
    s.serialize_str(mask_flagext_secret(value))
}

/// Serializes a `String` field the way Prometheus `config.Secret.MarshalYAML` does
/// (`MarshalSecretValue` is false in Mimir): `null` when empty, `"<secret>"` otherwise.
pub fn serialize_prometheus_secret<S: Serializer>(value: &str, s: S) -> Result<S::Ok, S::Error> {
    if secrets_revealed() {
        return s.serialize_str(value);
    }
    if value.is_empty() {
        s.serialize_none()
    } else {
        s.serialize_str(PROMETHEUS_SECRET_TOKEN)
    }
}

/// YAML keys whose Go type is `flagext.Secret` anywhere in `mimir.Config` (storage backends,
/// Kafka SASL, consul/etcd KV clients, Alertmanager client basic auth / OAuth2, vault auth).
pub const FLAGEXT_SECRET_YAML_KEYS: &[&str] = &[
    "acl_token",
    "password",
    "secret_access_key",
    "session_token",
    "account_key",
    "connection_string",
    "service_account",
    "application_credential_secret",
    "basic_auth_password",
    "client_secret",
    "sasl_password",
    "sasl_oauthbearer_token",
    "sasl_msk_iam_access_key",
    "sasl_msk_iam_secret_key",
    "sasl_msk_iam_session_token",
    "secret_id",
    "service_account_token",
    "token",
];

/// Masks, by key name, every `flagext.Secret` value inside a raw YAML subtree. Used for
/// sections kept as raw YAML (hidden `usage_tracker`, `block_builder*` blocks) so their
/// secrets are rendered like Go does instead of verbatim.
pub fn mask_secrets_by_key_name(v: &mut serde_yaml::Value) {
    match v {
        serde_yaml::Value::Mapping(m) => {
            for (k, child) in m.iter_mut() {
                let is_secret = k
                    .as_str()
                    .is_some_and(|k| FLAGEXT_SECRET_YAML_KEYS.contains(&k));
                if is_secret && !child.is_mapping() && !child.is_sequence() {
                    let empty = match child {
                        serde_yaml::Value::Null => true,
                        serde_yaml::Value::String(s) => s.is_empty(),
                        _ => false,
                    };
                    *child = serde_yaml::Value::String(
                        if empty { "" } else { FLAGEXT_SECRET_MASK }.to_string(),
                    );
                } else {
                    mask_secrets_by_key_name(child);
                }
            }
        }
        serde_yaml::Value::Sequence(s) => s.iter_mut().for_each(mask_secrets_by_key_name),
        serde_yaml::Value::Tagged(t) => mask_secrets_by_key_name(&mut t.value),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[test]
    fn masks_raw_yaml_by_key_name() {
        let mut v: serde_yaml::Value = serde_yaml::from_str(
            "snapshots_storage:\n  s3:\n    secret_access_key: X\n    endpoint: e\n  azure:\n    account_key: ''\nevents_storage_writer:\n  sasl_password: P\n",
        )
        .unwrap();
        mask_secrets_by_key_name(&mut v);
        assert_eq!(
            serde_yaml::to_string(&v).unwrap(),
            "snapshots_storage:\n  s3:\n    secret_access_key: '********'\n    endpoint: e\n  azure:\n    account_key: ''\nevents_storage_writer:\n  sasl_password: '********'\n"
        );
    }

    #[derive(Serialize)]
    struct S {
        #[serde(serialize_with = "serialize_flagext_secret")]
        a: String,
        #[serde(serialize_with = "serialize_flagext_secret")]
        b: String,
        #[serde(serialize_with = "serialize_prometheus_secret")]
        c: String,
        #[serde(serialize_with = "serialize_prometheus_secret")]
        d: String,
    }

    fn sample() -> S {
        S {
            a: "pw".into(),
            b: String::new(),
            c: "key".into(),
            d: String::new(),
        }
    }

    #[test]
    fn masks_like_go() {
        let out = serde_yaml::to_string(&sample()).unwrap();
        assert_eq!(out, "a: '********'\nb: ''\nc: <secret>\nd: null\n");
    }

    #[test]
    fn reveal_is_scoped() {
        let out = with_secrets_revealed(|| serde_yaml::to_string(&sample()).unwrap());
        assert_eq!(out, "a: pw\nb: ''\nc: key\nd: ''\n");
        let out = serde_yaml::to_string(&sample()).unwrap();
        assert!(out.contains("'********'"));
    }
}
