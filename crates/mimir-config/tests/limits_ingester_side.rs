//! Ported from github.com/grafana/mimir/pkg/util/validation/limits_test.go (ingester-side limits).

use mimir_config::{CostAttributionLabel, CostAttributionTrackerConfig, LimitsConfig};

// Upstream: limits_test.go::TestEffectiveIngestionPartitionsTenantWriteShardSize
#[test]
fn effective_ingestion_partitions_tenant_write_shard_size() {
    let cases = [
        ("both zero (default)", 0, 0, 0),
        (
            "write shard size not set, falls back to read shard size",
            4,
            0,
            4,
        ),
        ("write shard size smaller than read shard size", 4, 2, 2),
        ("write shard size equal to read shard size", 4, 4, 4),
        (
            "write shard size larger than read shard size, clamped",
            4,
            6,
            4,
        ),
        (
            "write shard size set, read shard size zero (no read limit)",
            0,
            2,
            2,
        ),
    ];
    for (name, read, write, expected) in cases {
        let l = LimitsConfig {
            ingestion_partitions_tenant_shard_size: read,
            ingestion_partitions_tenant_write_shard_size: write,
            ..LimitsConfig::default()
        };
        assert_eq!(
            l.effective_ingestion_partitions_tenant_write_shard_size(),
            expected,
            "{name}"
        );
    }
}

fn tracker(labels: &[(&str, &str)]) -> CostAttributionTrackerConfig {
    CostAttributionTrackerConfig {
        labels: labels
            .iter()
            .map(|(i, o)| CostAttributionLabel {
                input: (*i).to_string(),
                output: (*o).to_string(),
            })
            .collect(),
        internal: false,
    }
}

// Go: Limits.Validate -> CostAttributionBaseTrackers/AdditionalCostAttributionTrackers.Validate
#[test]
fn cost_attribution_trackers_are_validated() {
    let validate = |base: Vec<(&str, CostAttributionTrackerConfig)>| {
        let mut l = LimitsConfig {
            cost_attribution_trackers: base.into_iter().map(|(n, t)| (n.to_string(), t)).collect(),
            ..LimitsConfig::default()
        };
        l.validate()
    };
    assert!(validate(vec![("by-team", tracker(&[("team", "")]))]).is_ok());
    assert!(validate(vec![("", tracker(&[("team", "")]))]).is_err());
    assert!(validate(vec![("by-team", tracker(&[]))]).is_err());
    assert!(validate(vec![("by-team", tracker(&[("", "x")]))]).is_err());
    assert!(validate(vec![("by-team", tracker(&[("team", "__x")]))]).is_err());
    assert!(validate(vec![("by-team", tracker(&[("a", "x"), ("b", "x")]))]).is_err());

    let mut l = LimitsConfig {
        additional_cost_attribution_trackers: [("bad".to_string(), tracker(&[]))].into_iter().collect(),
        ..LimitsConfig::default()
    };
    assert!(l.validate().is_err());
}
