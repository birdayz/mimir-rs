//! Go `prometheus.WrapRegistererWith(labels, reg)` for components that register their metrics
//! into a plain [`Registry`].
//!
//! Ported from github.com/prometheus/client_golang/prometheus/wrap.go (as used by
//! github.com/grafana/mimir/pkg/storage/ingest/multi_cluster_reader.go for the per write
//! compartment `write_compartment="<id>"` registerer).
//!
//! The component registers into a private child registry; [`register_labeled_child`] then
//! exposes everything gathered from the child through the parent with the constant labels added.

use std::collections::HashMap;

use prometheus::core::{Collector, Desc};
use prometheus::proto::{LabelPair, MetricFamily};
use prometheus::Registry;

struct LabeledRegistryCollector {
    child: Registry,
    labels: Vec<(String, String)>,
    descs: Vec<Desc>,
}

impl Collector for LabeledRegistryCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut mfs = self.child.gather();
        for mf in &mut mfs {
            for m in mf.mut_metric().iter_mut() {
                let mut pairs: Vec<LabelPair> = m.take_label().into_vec();
                for (k, v) in &self.labels {
                    let mut lp = LabelPair::default();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    pairs.push(lp);
                }
                pairs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                m.set_label(pairs.into());
            }
        }
        mfs
    }
}

/// Registers every metric of `child` into `parent` with the constant `labels` added (Go
/// `prometheus.WrapRegistererWith(labels, parent)`).
///
/// Call it once the component has registered its metrics into `child`: the descriptors used
/// for `parent`'s consistency checks are taken from the metrics present at that time, while
/// metrics registered into `child` later are still exported on every gather.
pub fn register_labeled_child(
    parent: &Registry,
    child: Registry,
    labels: &[(&str, &str)],
) -> prometheus::Result<()> {
    let const_labels: HashMap<String, String> = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let mut descs = Vec::new();
    for mf in child.gather() {
        let mut variable_labels: Vec<String> = mf
            .get_metric()
            .first()
            .map(|m| m.get_label().iter().map(|l| l.get_name().to_string()).collect())
            .unwrap_or_default();
        variable_labels.sort();
        if let Ok(d) = Desc::new(
            mf.get_name().to_string(),
            mf.get_help().to_string(),
            variable_labels,
            const_labels.clone(),
        ) {
            descs.push(d);
        }
    }
    parent.register(Box::new(LabeledRegistryCollector {
        child,
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
        descs,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Counter, Encoder, TextEncoder};

    fn child_with_counter(v: f64) -> Registry {
        let reg = Registry::new();
        let c = Counter::new("cortex_test_total", "help").unwrap();
        c.inc_by(v);
        reg.register(Box::new(c)).unwrap();
        reg
    }

    #[test]
    fn children_are_exported_with_distinct_labels() {
        let parent = Registry::new();
        register_labeled_child(&parent, child_with_counter(1.0), &[("write_compartment", "0")])
            .unwrap();
        register_labeled_child(&parent, child_with_counter(2.0), &[("write_compartment", "1")])
            .unwrap();
        // Registering the same labels twice is a duplicate, like Go.
        assert!(register_labeled_child(
            &parent,
            child_with_counter(3.0),
            &[("write_compartment", "1")]
        )
        .is_err());

        let mut buf = Vec::new();
        TextEncoder::new().encode(&parent.gather(), &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("cortex_test_total{write_compartment=\"0\"} 1"), "{text}");
        assert!(text.contains("cortex_test_total{write_compartment=\"1\"} 2"), "{text}");
    }
}
