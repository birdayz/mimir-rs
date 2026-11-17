//! Equivalents of `prometheus.WrapRegistererWithPrefix` / `prometheus.WrapRegistererWith`
//! from client_golang, used by Mimir to namespace dskit metrics (e.g. ring and lifecycler
//! metrics are registered through `prometheus.WrapRegistererWithPrefix("cortex_", reg)` and KV
//! client metrics through `kv.RegistererWithKVName`, which adds a `kv_name` label).
//!
//! Ported from:
//! - github.com/prometheus/client_golang/prometheus/wrap.go (semantics only)
//! - github.com/grafana/dskit/kv/metrics.go (`RegistererWithKVName`)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use prometheus::core::{Collector, Desc, Describer};
use prometheus::proto::MetricFamily;
use prometheus::{Opts, Registry};

static NEXT_BRIDGE_ID: AtomicU64 = AtomicU64::new(0);

/// Collector exposing every metric family of a child registry through its parent.
struct RegistryBridge {
    child: Registry,
    // A unique placeholder descriptor so that each bridge is a distinct collector for the
    // parent registry; it never produces samples.
    descs: Vec<Desc>,
}

impl Collector for RegistryBridge {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.child.gather()
    }
}

/// Returns a registry whose metrics are exposed through `parent` with `prefix` prepended to
/// every metric name (`prometheus.WrapRegistererWithPrefix`) and `labels` added as constant
/// labels (`prometheus.WrapRegistererWith`). Either part may be empty.
///
/// A trailing `_` in `prefix` is the separator used by Mimir (`"cortex_"`) and is handled
/// transparently.
pub fn wrap_registry(parent: &Registry, prefix: &str, labels: &[(&str, &str)]) -> Registry {
    let namespace = prefix.trim_end_matches('_');
    let labels: HashMap<String, String> = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let child = Registry::new_custom(
        if namespace.is_empty() {
            None
        } else {
            Some(namespace.to_string())
        },
        if labels.is_empty() { None } else { Some(labels) },
    )
    .expect("valid wrapped registry");

    let id = NEXT_BRIDGE_ID.fetch_add(1, Ordering::Relaxed);
    let desc = Opts::new(
        format!("mimir_rs_wrapped_registry_bridge_{id}"),
        "Placeholder descriptor of a wrapped registry (never exported).",
    )
    .describe()
    .expect("valid placeholder descriptor");
    let _ = parent.register(Box::new(RegistryBridge {
        child: child.clone(),
        descs: vec![desc],
    }));
    child
}

/// `prometheus.WrapRegistererWithPrefix(prefix, reg)`.
pub fn wrap_registry_with_prefix(parent: &Registry, prefix: &str) -> Registry {
    wrap_registry(parent, prefix, &[])
}

/// `kv.RegistererWithKVName(reg, name)`: adds the `kv_name` label.
pub fn registry_with_kv_name(parent: &Registry, name: &str) -> Registry {
    wrap_registry(parent, "", &[("kv_name", name)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{IntCounter, IntGaugeVec};

    #[test]
    fn prefix_and_labels_are_applied_through_parent() {
        let parent = Registry::new();
        let cortex = wrap_registry_with_prefix(&parent, "cortex_");
        let g = IntGaugeVec::new(
            Opts::new("ring_members", "Number of members in the ring")
                .const_label("name", "ingester"),
            &["state"],
        )
        .unwrap();
        cortex.register(Box::new(g.clone())).unwrap();
        g.with_label_values(&["ACTIVE"]).set(3);

        let kv = registry_with_kv_name(&cortex, "ingester-ring");
        let c = IntCounter::new("kv_request_total", "help").unwrap();
        kv.register(Box::new(c.clone())).unwrap();
        c.inc();

        let names: Vec<String> = parent
            .gather()
            .iter()
            .map(|mf| mf.get_name().to_string())
            .collect();
        assert!(names.contains(&"cortex_ring_members".to_string()), "{names:?}");
        assert!(names.contains(&"cortex_kv_request_total".to_string()), "{names:?}");
        assert!(!names.iter().any(|n| n.contains("bridge")));

        let kv_mf = parent
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "cortex_kv_request_total")
            .unwrap();
        let lbls = kv_mf.get_metric()[0].get_label();
        assert!(lbls
            .iter()
            .any(|l| l.get_name() == "kv_name" && l.get_value() == "ingester-ring"));
    }

    #[test]
    fn two_wrapped_registries_with_same_prefix_coexist() {
        let parent = Registry::new();
        let a = wrap_registry_with_prefix(&parent, "cortex_");
        let b = wrap_registry_with_prefix(&parent, "cortex_");
        let ca = IntCounter::with_opts(Opts::new("x_total", "h").const_label("name", "a")).unwrap();
        let cb = IntCounter::with_opts(Opts::new("x_total", "h").const_label("name", "b")).unwrap();
        a.register(Box::new(ca.clone())).unwrap();
        b.register(Box::new(cb.clone())).unwrap();
        ca.inc();
        cb.inc();
        let mf = parent
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "cortex_x_total")
            .unwrap();
        assert_eq!(mf.get_metric().len(), 2);
    }
}
