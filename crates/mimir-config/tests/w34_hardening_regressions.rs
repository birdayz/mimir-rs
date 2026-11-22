use mimir_config::mimir::MimirConfig;
use mimir_config::yaml_v3::check_config_document;

#[test]
fn yaml_v3_rejects_billion_laughs_excessive_aliasing() {
    let yaml = "a: &a [1, 1, 1, 1, 1, 1, 1, 1, 1]
b: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a]
c: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b]
d: &d [*c, *c, *c, *c, *c, *c, *c, *c, *c]
e: &e [*d, *d, *d, *d, *d, *d, *d, *d, *d]
f: [*e, *e, *e, *e, *e, *e, *e, *e, *e]
";
    let err = check_config_document(yaml.as_bytes())
        .err()
        .expect("Billion Laughs YAML must be rejected");
    assert!(
        err.contains("document contains excessive aliasing"),
        "expected excessive aliasing error, got: {err}"
    );
}

#[test]
fn yaml_v3_rejects_deeply_nested_yaml_without_stack_overflow() {
    let depth = 300;
    let mut yaml = String::from("x: ");
    yaml.push_str(&"[".repeat(depth));
    yaml.push('1');
    yaml.push_str(&"]".repeat(depth));
    yaml.push('\n');

    let err = check_config_document(yaml.as_bytes())
        .err()
        .expect("deeply nested YAML must be rejected");
    assert!(
        err.contains("exceeded maximum nesting depth"),
        "expected nesting depth error, got: {err}"
    );
}

#[test]
fn common_storage_inheritance_on_non_mapping_target_returns_error_not_panic() {
    let yaml = "common:
  storage:
    backend: filesystem
blocks_storage: 123
";
    let mut cfg = MimirConfig::default();
    let err = cfg.load_config_from_bytes(yaml.as_bytes(), false).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("cannot apply common config")
            || msg.contains("blocks_storage")
            || msg.contains("cannot unmarshal"),
        "expected clean ConfigError, got: {msg}"
    );

    let yaml2 = "common:
  client_cluster_validation:
    label_values:
      cluster: test
ingester_client: 123
";
    let mut cfg2 = MimirConfig::default();
    let err2 = cfg2.load_config_from_bytes(yaml2.as_bytes(), false).unwrap_err();
    let msg2 = err2.to_string();
    assert!(
        msg2.contains("cannot apply common config")
            || msg2.contains("ingester_client")
            || msg2.contains("cannot unmarshal"),
        "expected clean ConfigError, got: {msg2}"
    );
}

#[test]
fn yaml_v3_rejects_merge_sequence_billion_laughs_and_deep_alias_chain() {
    let mut merge_bomb = String::from("a0: &a0 { k: 1 }\n");
    for i in 1..=20 {
        merge_bomb.push_str(&format!("a{i}: &a{i} {{ <<: [*a{}, *a{}] }}\n", i - 1, i - 1));
    }
    let doc = mimir_config::yaml_v3::parse_first_document(merge_bomb.as_bytes())
        .expect("parse merge bomb")
        .expect("doc present");
    assert!(
        mimir_config::yaml_v3::doc_to_serde_value(&doc).is_err(),
        "merge-sequence Billion Laughs must be rejected by doc_to_serde_value"
    );

    let mut chain = String::from("a0: &a0 x\n");
    for i in 1..=350 {
        chain.push_str(&format!("a{i}: &a{i} [*a{}]\n", i - 1));
    }
    let doc_chain = mimir_config::yaml_v3::parse_first_document(chain.as_bytes())
        .expect("parse deep alias chain")
        .expect("doc present");
    assert!(
        mimir_config::yaml_v3::doc_to_serde_value(&doc_chain).is_err(),
        "350-deep alias chain must be rejected by MAX_ALIAS_DEPTH"
    );
}
