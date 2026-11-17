//! Memberlist KV metrics.
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/metrics.go (`createAndRegisterMetrics`,
//! `Describe`, `Collect`). Names, help strings, types and labels are identical to dskit; Mimir
//! sets `MetricsNamespace` to "" so every name starts with `memberlist_client_`.

use std::sync::Weak;

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge as PGauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts, Registry};

use mimir_common::services::State;

use super::client::MemberlistKv;

const SUBSYSTEM: &str = "memberlist_client";

fn fq(ns: &str, name: &str) -> String {
    if ns.is_empty() {
        format!("{SUBSYSTEM}_{name}")
    } else {
        format!("{ns}_{SUBSYSTEM}_{name}")
    }
}

pub(crate) struct KvMetrics {
    pub received_broadcasts: Counter,
    pub received_broadcasts_bytes: Counter,
    pub received_broadcasts_invalid: Counter,
    pub received_broadcasts_dropped: Counter,
    pub state_pushes: Counter,
    pub state_pushes_bytes: Counter,
    pub state_pulls: Counter,
    pub state_pulls_bytes: Counter,
    pub local_state_cache_hits: CounterVec,
    pub local_state_cache_misses: CounterVec,
    pub messages_in_broadcast_queue_bytes: Gauge,
    pub messages_to_broadcast_dropped: Counter,
    pub cas_attempts: Counter,
    pub cas_successes: Counter,
    pub cas_failures: Counter,
    pub store_tombstones: GaugeVec,
    pub store_removed_tombstones: CounterVec,
    pub watch_prefix_dropped_notifications: CounterVec,
    pub pending_key_notifications: Gauge,
}

impl KvMetrics {
    pub fn new(ns: &str) -> Self {
        let c = |name: &str, help: &str| Counter::with_opts(Opts::new(fq(ns, name), help)).expect("valid");
        let cv = |name: &str, help: &str, l: &str| {
            CounterVec::new(Opts::new(fq(ns, name), help), &[l]).expect("valid")
        };
        Self {
            received_broadcasts: c("received_broadcasts_total", "Number of received broadcast user messages"),
            received_broadcasts_bytes: c("received_broadcasts_bytes_total", "Total size of received broadcast user messages"),
            received_broadcasts_invalid: c(
                "received_broadcasts_invalid_total",
                "Number of received broadcast user messages that were invalid. Hopefully 0.",
            ),
            received_broadcasts_dropped: c(
                "received_broadcasts_dropped_total",
                "Number of received broadcast user messages that were dropped. Hopefully 0.",
            ),
            state_pushes: c("state_pushes_total", "How many times did this node push its full state to another node"),
            state_pushes_bytes: c("state_pushes_bytes_total", "Total size of pushed state"),
            state_pulls: c("state_pulls_total", "How many times did this node pull full state from another node"),
            state_pulls_bytes: c("state_pulls_bytes_total", "Total size of pulled state"),
            local_state_cache_hits: cv(
                "local_state_cache_hits_total",
                "Number of store entries served from the serialized local state cache when sending full state",
                "key",
            ),
            local_state_cache_misses: cv(
                "local_state_cache_misses_total",
                "Number of store entries that had to be serialized when sending full state, because they changed since the last time they were sent",
                "key",
            ),
            messages_in_broadcast_queue_bytes: Gauge::with_opts(Opts::new(
                fq(ns, "messages_in_broadcast_queue_bytes"),
                "Total size of messages waiting in the broadcast queue",
            ))
            .expect("valid"),
            messages_to_broadcast_dropped: c(
                "messages_to_broadcast_dropped_total",
                "Number of broadcast messages intended to be sent but were dropped due to encoding errors or for being too big",
            ),
            cas_attempts: c("cas_attempt_total", "Attempted CAS operations"),
            cas_successes: c("cas_success_total", "Successful CAS operations"),
            cas_failures: c("cas_failure_total", "Failed CAS operations"),
            store_tombstones: GaugeVec::new(
                Opts::new(
                    fq(ns, "kv_store_value_tombstones"),
                    "Number of tombstones currently present in KV store values",
                ),
                &["key"],
            )
            .expect("valid"),
            store_removed_tombstones: cv(
                "kv_store_value_tombstones_removed_total",
                "Total number of tombstones which have been removed from KV store values",
                "key",
            ),
            watch_prefix_dropped_notifications: cv(
                "watch_prefix_dropped_notifications",
                "Number of dropped notifications in WatchPrefix function",
                "prefix",
            ),
            pending_key_notifications: Gauge::with_opts(Opts::new(
                fq(ns, "pending_key_notifications"),
                "Number of pending notifications in WatchPrefix function",
            ))
            .expect("valid"),
        }
    }

    /// Registers every metric plus the GaugeFunc/const collector (Go `promauto.With(reg)` +
    /// `registerer.MustRegister(m)`).
    pub fn register(&self, reg: &Registry, ns: &str, kv: Weak<MemberlistKv>) {
        let cs: Vec<Box<dyn Collector>> = vec![
            Box::new(self.received_broadcasts.clone()),
            Box::new(self.received_broadcasts_bytes.clone()),
            Box::new(self.received_broadcasts_invalid.clone()),
            Box::new(self.received_broadcasts_dropped.clone()),
            Box::new(self.state_pushes.clone()),
            Box::new(self.state_pushes_bytes.clone()),
            Box::new(self.state_pulls.clone()),
            Box::new(self.state_pulls_bytes.clone()),
            Box::new(self.local_state_cache_hits.clone()),
            Box::new(self.local_state_cache_misses.clone()),
            Box::new(self.messages_in_broadcast_queue_bytes.clone()),
            Box::new(self.messages_to_broadcast_dropped.clone()),
            Box::new(self.cas_attempts.clone()),
            Box::new(self.cas_successes.clone()),
            Box::new(self.cas_failures.clone()),
            Box::new(self.store_tombstones.clone()),
            Box::new(self.store_removed_tombstones.clone()),
            Box::new(self.watch_prefix_dropped_notifications.clone()),
            Box::new(self.pending_key_notifications.clone()),
            Box::new(FuncCollector::new(ns, kv)),
        ];
        for c in cs {
            if let Err(err) = reg.register(c) {
                tracing::error!(%err, "failed to register memberlist KV metric");
            }
        }
    }
}

/// GaugeFuncs + const metric from dskit: `messages_in_broadcast_queue{queue}`, `kv_store_count`,
/// `cluster_members_count`, `cluster_node_health_score`.
struct FuncCollector {
    kv: Weak<MemberlistKv>,
    queue: Desc,
    store_count: Desc,
    members: Desc,
    health: Desc,
    queue_gossip: Desc,
}

impl FuncCollector {
    fn new(ns: &str, kv: Weak<MemberlistKv>) -> Self {
        let d = |name: &str, help: &str, labels: Vec<(&str, &str)>| {
            Desc::new(
                fq(ns, name),
                help.to_string(),
                vec![],
                labels.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            )
            .expect("valid desc")
        };
        Self {
            kv,
            queue: d("messages_in_broadcast_queue", "Number of user messages in the broadcast queue", vec![("queue", "local")]),
            queue_gossip: d("messages_in_broadcast_queue", "Number of user messages in the broadcast queue", vec![("queue", "gossip")]),
            store_count: d("kv_store_count", "Number of values in KV Store", vec![]),
            members: d("cluster_members_count", "Number of members in memberlist cluster", vec![]),
            health: d("cluster_node_health_score", "Health score of this cluster. Lower value is better. 0 = healthy", vec![]),
        }
    }
}

fn gauge_family(desc: &Desc, values: Vec<(Vec<(String, String)>, f64)>) -> MetricFamily {
    let mut mf = MetricFamily::default();
    mf.set_name(desc.fq_name.clone());
    mf.set_help(desc.help.clone());
    mf.set_field_type(MetricType::GAUGE);
    for (labels, v) in values {
        let mut m = Metric::default();
        let pairs: Vec<LabelPair> = labels
            .into_iter()
            .map(|(k, val)| {
                let mut lp = LabelPair::default();
                lp.set_name(k);
                lp.set_value(val);
                lp
            })
            .collect();
        m.set_label(pairs.into());
        let mut g = PGauge::default();
        g.set_value(v);
        m.set_gauge(g);
        mf.mut_metric().push(m);
    }
    mf
}

impl Collector for FuncCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.queue, &self.queue_gossip, &self.store_count, &self.members, &self.health]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let Some(kv) = self.kv.upgrade() else {
            return Vec::new();
        };
        let running = matches!(kv.state(), State::Running | State::Stopping);
        let (local_q, gossip_q, members, health) = if running {
            (
                kv.local_broadcasts.num_queued() as f64,
                kv.gossip_broadcasts.num_queued() as f64,
                kv.num_members() as f64,
                kv.health_score() as f64,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0)
        };
        vec![
            gauge_family(
                &self.queue,
                vec![
                    (vec![("queue".to_string(), "gossip".to_string())], gossip_q),
                    (vec![("queue".to_string(), "local".to_string())], local_q),
                ],
            ),
            gauge_family(&self.store_count, vec![(vec![], kv.store_len() as f64)]),
            gauge_family(&self.members, vec![(vec![], members)]),
            gauge_family(&self.health, vec![(vec![], health)]),
        ]
    }
}
