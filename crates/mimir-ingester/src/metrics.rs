//! Prometheus metrics registration matching `github.com/grafana/mimir/pkg/ingester/metrics.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, Opts, Registry,
};

use mimir_common::errors::discard_reasons;
use mimir_common::native_histogram::{
    HistogramOpts as NativeHistogramOpts, HistogramVec as NativeHistogramVec, SingleHistogram,
};

pub const REASON_MAX_INGESTION_RATE: &str = "ingester_max_ingestion_rate";
pub const REASON_MAX_TENANTS: &str = "ingester_max_tenants";
pub const REASON_MAX_IN_MEMORY_SERIES: &str = "ingester_max_series";
pub const REASON_MAX_INFLIGHT_PUSH_REQUESTS: &str = "ingester_max_inflight_push_requests";
pub const REASON_MAX_INFLIGHT_PUSH_REQUESTS_BYTES: &str =
    "ingester_max_inflight_push_requests_bytes";
pub const REASON_MAX_INFLIGHT_READ_REQUESTS: &str = "ingester_max_inflight_read_requests";

pub const PER_USER_METADATA_LIMIT: &str = "per_user_metadata_limit";
pub const PER_METRIC_METADATA_LIMIT: &str = "per_metric_metadata_limit";

/// Dynamic `GaugeFunc`-like collector backed by a closure.
struct FnGaugeCollector {
    desc: Desc,
    f: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl FnGaugeCollector {
    fn new(name: &str, help: &str, const_labels: Vec<(&str, &str)>, f: impl Fn() -> f64 + Send + Sync + 'static) -> Self {
        let labels = const_labels
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>();
        let desc = Desc::new(name.to_string(), help.to_string(), vec![], labels)
            .expect("valid gauge desc");
        Self {
            desc,
            f: Box::new(f),
        }
    }
}

impl Collector for FnGaugeCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let val = (self.f)();
        let mut g = prometheus::proto::Gauge::default();
        g.set_value(val);
        let mut m = prometheus::proto::Metric::default();
        m.set_gauge(g);
        let pairs = self.desc.const_label_pairs.to_vec();
        m.set_label(pairs.into());
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(prometheus::proto::MetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

fn register_fn_gauge(
    reg: Option<&Registry>,
    name: &str,
    help: &str,
    const_labels: Vec<(&str, &str)>,
    f: impl Fn() -> f64 + Send + Sync + 'static,
) {
    if let Some(r) = reg {
        let _ = r.register(Box::new(FnGaugeCollector::new(name, help, const_labels, f)));
    }
}

fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(count);
    let mut cur = start;
    for _ in 0..count {
        out.push(cur);
        cur *= factor;
    }
    out
}

#[derive(Clone)]
pub struct DiscardedMetrics {
    pub sample_timestamp_too_old: CounterVec,
    pub sample_out_of_order: CounterVec,
    pub sample_too_old: CounterVec,
    pub sample_too_far_in_future: CounterVec,
    pub new_value_for_timestamp: CounterVec,
    pub per_user_series_limit: CounterVec,
    pub per_metric_series_limit: CounterVec,
    pub invalid_native_histogram: CounterVec,
    pub labels_not_sorted: CounterVec,
}

impl DiscardedMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let mk = |reason: &str| {
            let mut cl = HashMap::new();
            cl.insert("reason".to_string(), reason.to_string());
            let cv = CounterVec::new(
                Opts::new(
                    "cortex_discarded_samples_total",
                    "The total number of samples that were discarded.",
                )
                .const_labels(cl),
                &["user", "group"],
            )
            .expect("valid discarded counter");
            if let Some(r) = reg {
                let _ = r.register(Box::new(cv.clone()));
            }
            cv
        };
        Self {
            sample_timestamp_too_old: mk(discard_reasons::SAMPLE_TIMESTAMP_TOO_OLD),
            sample_out_of_order: mk(discard_reasons::SAMPLE_OUT_OF_ORDER),
            sample_too_old: mk(discard_reasons::SAMPLE_TOO_OLD),
            sample_too_far_in_future: mk(discard_reasons::SAMPLE_TOO_FAR_IN_FUTURE),
            new_value_for_timestamp: mk(discard_reasons::NEW_VALUE_FOR_TIMESTAMP),
            per_user_series_limit: mk(discard_reasons::PER_USER_SERIES_LIMIT),
            per_metric_series_limit: mk(discard_reasons::PER_METRIC_SERIES_LIMIT),
            invalid_native_histogram: mk(discard_reasons::INVALID_NATIVE_HISTOGRAM),
            labels_not_sorted: mk(discard_reasons::LABELS_NOT_SORTED),
        }
    }

    fn all(&self) -> [&CounterVec; 9] {
        [
            &self.sample_timestamp_too_old,
            &self.sample_out_of_order,
            &self.sample_too_old,
            &self.sample_too_far_in_future,
            &self.new_value_for_timestamp,
            &self.per_user_series_limit,
            &self.per_metric_series_limit,
            &self.invalid_native_histogram,
            &self.labels_not_sorted,
        ]
    }

    /// Deletes all series of `user_id`, whatever their group (Go: `DeletePartialMatch({user})`).
    pub fn delete_user(&self, user_id: &str) {
        for cv in self.all() {
            let mut groups = Vec::new();
            for mf in cv.collect() {
                for m in mf.get_metric() {
                    let lp = m.get_label();
                    let user = lp.iter().find(|l| l.get_name() == "user").map(|l| l.get_value());
                    if user == Some(user_id) {
                        let group = lp
                            .iter()
                            .find(|l| l.get_name() == "group")
                            .map_or("", |l| l.get_value());
                        groups.push(group.to_string());
                    }
                }
            }
            for g in groups {
                let _ = cv.remove_label_values(&[user_id, &g]);
            }
        }
    }

    /// Deletes the series of `(user_id, group)` (Go: `discardedMetrics.DeleteLabelValues`).
    pub fn delete_label_values(&self, user_id: &str, group: &str) {
        for cv in self.all() {
            let _ = cv.remove_label_values(&[user_id, group]);
        }
    }
}

pub struct IngesterMetrics {
    pub ingested_samples: CounterVec,
    pub ingested_exemplars: Counter,
    pub ingested_metadata: Counter,
    pub ingested_samples_fail: CounterVec,
    pub ingested_exemplars_fail: Counter,
    pub ingested_metadata_fail: Counter,
    pub series_with_floats_and_histograms: CounterVec,

    pub queries: Counter,
    pub queried_samples: Histogram,
    pub queried_exemplars: Histogram,
    /// Classic `ExponentialBuckets(10, 8, 6)` + native (factor 1.1) buckets.
    pub queried_series: NativeHistogramVec,
    pub queried_blocks: CounterVec,
    /// Native-only histogram (factor 1.1, no classic buckets).
    pub discarded_series_ratio: SingleHistogram,

    pub mem_metadata: Gauge,
    pub mem_users: Gauge,
    pub mem_metadata_created_total: CounterVec,
    pub mem_metadata_removed_total: CounterVec,

    pub active_series_per_user: GaugeVec,
    pub active_series_per_user_otlp: GaugeVec,
    pub active_series_custom_trackers_per_user: GaugeVec,
    pub active_series_per_user_native_histograms: GaugeVec,
    pub active_series_custom_trackers_per_user_native_histograms: GaugeVec,
    pub active_native_histogram_buckets_per_user: GaugeVec,
    pub active_native_histogram_buckets_custom_trackers_per_user: GaugeVec,
    pub active_series_loading: Gauge,
    pub attributed_active_series_failures_per_user: CounterVec,

    pub owned_series_per_user: GaugeVec,
    pub label_value_bytes_over_threshold: GaugeVec,
    pub max_local_series_per_user: GaugeVec,

    pub compactions_triggered: Counter,
    pub compactions_failed: Counter,
    pub forced_compaction_in_progress: Gauge,
    /// Go `inflightRequestsSummary` (sampled every 250ms by the metrics updater). Quantiles are
    /// exact over a sliding 60s window (Go: CKMS estimates over 6 rotating age buckets).
    pub inflight_requests_summary: mimir_storage::ingest::SlidingWindowSummary,
    pub per_tenant_early_compactions_triggered: Counter,
    pub early_compaction_non_owned_series_triggered: CounterVec,
    pub appender_add_duration: Histogram,
    pub appender_commit_duration: Histogram,
    pub idle_tsdb_checks: CounterVec,
    pub open_existing_tsdb: Counter,

    forced_compactions_count: Mutex<i64>,

    pub discarded: DiscardedMetrics,
    pub rejected: CounterVec,

    pub discarded_metadata_per_user_limit: CounterVec,
    pub discarded_metadata_per_metric_limit: CounterVec,

    pub shutdown_marker: Gauge,
    pub utilization_limited_requests: CounterVec,
    pub index_lookup_comparison_outcomes: CounterVec,
    label_value_bytes_labels: Mutex<HashMap<String, std::collections::HashSet<String>>>,
}

fn reg_counter(reg: Option<&Registry>, name: &str, help: &str) -> Counter {
    let c = Counter::with_opts(Opts::new(name, help)).expect("valid counter");
    if let Some(r) = reg {
        let _ = r.register(Box::new(c.clone()));
    }
    c
}

fn reg_counter_vec(reg: Option<&Registry>, name: &str, help: &str, labels: &[&str]) -> CounterVec {
    let cv = CounterVec::new(Opts::new(name, help), labels).expect("valid counter vec");
    if let Some(r) = reg {
        let _ = r.register(Box::new(cv.clone()));
    }
    cv
}

fn reg_gauge(reg: Option<&Registry>, name: &str, help: &str) -> Gauge {
    let g = Gauge::with_opts(Opts::new(name, help)).expect("valid gauge");
    if let Some(r) = reg {
        let _ = r.register(Box::new(g.clone()));
    }
    g
}

fn reg_gauge_vec(reg: Option<&Registry>, name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let gv = GaugeVec::new(Opts::new(name, help), labels).expect("valid gauge vec");
    if let Some(r) = reg {
        let _ = r.register(Box::new(gv.clone()));
    }
    gv
}

fn reg_histogram(reg: Option<&Registry>, name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    let opts = HistogramOpts::new(name, help).buckets(buckets);
    let h = Histogram::with_opts(opts).expect("valid histogram");
    if let Some(r) = reg {
        let _ = r.register(Box::new(h.clone()));
    }
    h
}

impl IngesterMetrics {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reg: Option<&Registry>,
        active_series_enabled: bool,
        inflight_requests: Arc<AtomicI64>,
        inflight_requests_bytes: Arc<AtomicI64>,
        ingestion_rate_fn: impl Fn() -> f64 + Send + Sync + 'static,
        instance_limits_fn: impl Fn() -> Option<mimir_config::InstanceLimits> + Send + Sync + Clone + 'static,
        head_min_ts_fn: impl Fn() -> f64 + Send + Sync + 'static,
        head_max_ts_fn: impl Fn() -> f64 + Send + Sync + 'static,
        oldest_unshipped_fn: impl Fn() -> f64 + Send + Sync + 'static,
    ) -> Self {
        let idle_tsdb_checks = reg_counter_vec(
            reg,
            "cortex_ingester_idle_tsdb_checks_total",
            "The total number of various results for idle TSDB checks.",
            &["result"],
        );
        for res in [
            "shipping_disabled",
            "not_idle",
            "not_compacted",
            "not_shipped",
            "check_failed",
            "close_failed",
            "not_active",
            "data_removal_failed",
            "tenant_marked_for_deletion",
            "idle_closed",
        ] {
            let _ = idle_tsdb_checks.with_label_values(&[res]);
        }

        let active_reg = if active_series_enabled { reg } else { None };

        let queried_series = NativeHistogramVec::new(
            NativeHistogramOpts::new(
                "cortex_ingester_queried_series",
                "The total number of series returned from queries.",
            )
            .buckets(exponential_buckets(10.0, 8.0, 6))
            .native(1.1, 0, std::time::Duration::ZERO),
            &["stage"],
        )
        .expect("valid histogram vec");
        if let Some(r) = reg {
            let _ = r.register(Box::new(queried_series.clone()));
        }

        let max_local_series_per_user = {
            let mut cl = HashMap::new();
            cl.insert("limit".to_string(), "max_global_series_per_user".to_string());
            let gv = GaugeVec::new(
                Opts::new(
                    "cortex_ingester_local_limits",
                    "Local per-user limits used by this ingester.",
                )
                .const_labels(cl),
                &["user"],
            )
            .expect("valid gauge vec");
            if let Some(r) = reg {
                let _ = r.register(Box::new(gv.clone()));
            }
            gv
        };

        let discarded_metadata_per_user_limit = {
            let mut cl = HashMap::new();
            cl.insert("reason".to_string(), PER_USER_METADATA_LIMIT.to_string());
            let cv = CounterVec::new(
                Opts::new(
                    "cortex_discarded_metadata_total",
                    "The total number of metadata that were discarded.",
                )
                .const_labels(cl),
                &["user"],
            )
            .expect("valid counter vec");
            if let Some(r) = reg {
                let _ = r.register(Box::new(cv.clone()));
            }
            cv
        };

        let discarded_metadata_per_metric_limit = {
            let mut cl = HashMap::new();
            cl.insert("reason".to_string(), PER_METRIC_METADATA_LIMIT.to_string());
            let cv = CounterVec::new(
                Opts::new(
                    "cortex_discarded_metadata_total",
                    "The total number of metadata that were discarded.",
                )
                .const_labels(cl),
                &["user"],
            )
            .expect("valid counter vec");
            if let Some(r) = reg {
                let _ = r.register(Box::new(cv.clone()));
            }
            cv
        };

        let rejected = reg_counter_vec(
            reg,
            "cortex_ingester_instance_rejected_requests_total",
            "Requests rejected for hitting per-instance limits",
            &["reason"],
        );
        for reason in [
            REASON_MAX_INGESTION_RATE,
            REASON_MAX_TENANTS,
            REASON_MAX_IN_MEMORY_SERIES,
            REASON_MAX_INFLIGHT_PUSH_REQUESTS,
            REASON_MAX_INFLIGHT_PUSH_REQUESTS_BYTES,
            REASON_MAX_INFLIGHT_READ_REQUESTS,
        ] {
            let _ = rejected.with_label_values(&[reason]);
        }

        // Dynamic GaugeFuncs matching Go promauto.NewGaugeFunc registrations.
        const INSTANCE_LIMITS: &str = "cortex_ingester_instance_limits";
        const INSTANCE_LIMITS_HELP: &str = "Instance limits used by this ingester.";
        let f_tenants = instance_limits_fn.clone();
        register_fn_gauge(
            reg,
            INSTANCE_LIMITS,
            INSTANCE_LIMITS_HELP,
            vec![("limit", "max_tenants")],
            move || f_tenants().map_or(0.0, |l| l.max_tenants as f64),
        );
        let f_series = instance_limits_fn.clone();
        register_fn_gauge(
            reg,
            INSTANCE_LIMITS,
            INSTANCE_LIMITS_HELP,
            vec![("limit", "max_series")],
            move || f_series().map_or(0.0, |l| l.max_series as f64),
        );
        let f_rate = instance_limits_fn.clone();
        register_fn_gauge(
            reg,
            INSTANCE_LIMITS,
            INSTANCE_LIMITS_HELP,
            vec![("limit", "max_ingestion_rate")],
            move || f_rate().map_or(0.0, |l| l.max_ingestion_rate),
        );
        let f_inflight = instance_limits_fn.clone();
        register_fn_gauge(
            reg,
            INSTANCE_LIMITS,
            INSTANCE_LIMITS_HELP,
            vec![("limit", "max_inflight_push_requests")],
            move || f_inflight().map_or(0.0, |l| l.max_inflight_push_requests as f64),
        );
        let f_inflight_bytes = instance_limits_fn;
        register_fn_gauge(
            reg,
            INSTANCE_LIMITS,
            INSTANCE_LIMITS_HELP,
            vec![("limit", "max_inflight_push_requests_bytes")],
            move || f_inflight_bytes().map_or(0.0, |l| l.max_inflight_push_requests_bytes as f64),
        );

        register_fn_gauge(
            reg,
            "cortex_ingester_ingestion_rate_samples_per_second",
            "Current ingestion rate in samples/sec that ingester is using to limit access.",
            vec![],
            ingestion_rate_fn,
        );
        let ir_req = Arc::clone(&inflight_requests);
        register_fn_gauge(
            reg,
            "cortex_ingester_inflight_push_requests",
            "Current number of inflight push requests in ingester.",
            vec![],
            move || ir_req.load(Ordering::Relaxed) as f64,
        );
        let ir_bytes = Arc::clone(&inflight_requests_bytes);
        register_fn_gauge(
            reg,
            "cortex_ingester_inflight_push_requests_bytes",
            "Total sum of inflight push request sizes in ingester in bytes.",
            vec![],
            move || ir_bytes.load(Ordering::Relaxed) as f64,
        );
        register_fn_gauge(
            reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            "Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.",
            vec![],
            oldest_unshipped_fn,
        );
        register_fn_gauge(
            reg,
            "cortex_ingester_tsdb_head_min_timestamp_seconds",
            "Minimum timestamp of the head block across all tenants.",
            vec![],
            head_min_ts_fn,
        );
        register_fn_gauge(
            reg,
            "cortex_ingester_tsdb_head_max_timestamp_seconds",
            "Maximum timestamp of the head block across all tenants.",
            vec![],
            head_max_ts_fn,
        );

        let active_series_loading = reg_gauge(
            active_reg,
            "cortex_ingester_active_series_loading",
            "1 if active series counts are still warming up and may be underreported, 0 once they are accurate.",
        );
        if active_series_enabled {
            active_series_loading.set(1.0);
        }

        let appender_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];

        let inflight_requests_summary = mimir_storage::ingest::SlidingWindowSummary::new(
            "cortex_ingester_inflight_push_requests_summary",
            "Number of inflight requests sampled at a regular interval. Quantile buckets keep track of inflight requests over the last 60s.",
            vec![0.5, 0.9, 0.95, 0.99, 1.0],
            std::time::Duration::from_secs(60),
        );
        if let Some(r) = reg {
            let _ = r.register(Box::new(inflight_requests_summary.clone()));
        }

        Self {
            inflight_requests_summary,
            ingested_samples: reg_counter_vec(
                reg,
                "cortex_ingester_ingested_samples_total",
                "The total number of samples ingested per user.",
                &["user"],
            ),
            ingested_exemplars: reg_counter(
                reg,
                "cortex_ingester_ingested_exemplars_total",
                "The total number of exemplars ingested.",
            ),
            ingested_metadata: reg_counter(
                reg,
                "cortex_ingester_ingested_metadata_total",
                "The total number of metadata ingested.",
            ),
            ingested_samples_fail: reg_counter_vec(
                reg,
                "cortex_ingester_ingested_samples_failures_total",
                "The total number of samples that errored on ingestion per user.",
                &["user"],
            ),
            ingested_exemplars_fail: reg_counter(
                reg,
                "cortex_ingester_ingested_exemplars_failures_total",
                "The total number of exemplars that errored on ingestion.",
            ),
            ingested_metadata_fail: reg_counter(
                reg,
                "cortex_ingester_ingested_metadata_failures_total",
                "The total number of metadata that errored on ingestion.",
            ),
            series_with_floats_and_histograms: reg_counter_vec(
                reg,
                "cortex_ingester_series_with_floats_and_histograms_total",
                "The total number of series in write requests that have both float samples and histograms per user.",
                &["user"],
            ),
            queries: reg_counter(
                reg,
                "cortex_ingester_queries_total",
                "The total number of queries the ingester has handled.",
            ),
            queried_samples: reg_histogram(
                reg,
                "cortex_ingester_queried_samples",
                "The total number of samples returned from queries.",
                exponential_buckets(10.0, 8.0, 8),
            ),
            queried_exemplars: reg_histogram(
                reg,
                "cortex_ingester_queried_exemplars",
                "The total number of exemplars returned from queries.",
                exponential_buckets(10.0, 5.0, 5),
            ),
            queried_series,
            queried_blocks: reg_counter_vec(
                reg,
                "cortex_ingester_queried_blocks_total",
                "Number of times blocks were queried by generation. Generation 0 is the head block; higher generations count persisted blocks back from the head (1 = most recent).",
                &["generation"],
            ),
            discarded_series_ratio: {
                let h = SingleHistogram::new(
                    NativeHistogramOpts::new(
                        "cortex_ingester_discarded_series_ratio",
                        "Ratio of discarded series during query processing. These are series fetched from the index, but then discarded because they don't match the vector selector. This is the ratio of cortex_ingester_queried_series{stage=\"index\"} over {stage=\"send\"}.",
                    )
                    .native(1.1, 0, std::time::Duration::ZERO),
                )
                .expect("valid histogram");
                if let Some(r) = reg {
                    let _ = r.register(Box::new(h.clone()));
                }
                h
            },
            mem_metadata: reg_gauge(
                reg,
                "cortex_ingester_memory_metadata",
                "The current number of metadata in memory.",
            ),
            mem_users: reg_gauge(
                reg,
                "cortex_ingester_memory_users",
                "The current number of users in memory.",
            ),
            mem_metadata_created_total: reg_counter_vec(
                reg,
                "cortex_ingester_memory_metadata_created_total",
                "The total number of metadata that were created per user",
                &["user"],
            ),
            mem_metadata_removed_total: reg_counter_vec(
                reg,
                "cortex_ingester_memory_metadata_removed_total",
                "The total number of metadata that were removed per user.",
                &["user"],
            ),
            active_series_per_user: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_series",
                "Number of currently active series per user.",
                &["user"],
            ),
            active_series_per_user_otlp: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_otlp_series",
                "Number of currently active series per user ingested via OTLP.",
                &["user"],
            ),
            active_series_custom_trackers_per_user: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_series_custom_tracker",
                "Number of currently active series matching a pre-configured label matchers per user.",
                &["user", "name"],
            ),
            active_series_per_user_native_histograms: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_native_histogram_series",
                "Number of currently active native histogram series per user.",
                &["user"],
            ),
            active_series_custom_trackers_per_user_native_histograms: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_native_histogram_series_custom_tracker",
                "Number of currently active native histogram series matching a pre-configured label matchers per user.",
                &["user", "name"],
            ),
            active_native_histogram_buckets_per_user: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_native_histogram_buckets",
                "Number of currently active native histogram buckets per user.",
                &["user"],
            ),
            active_native_histogram_buckets_custom_trackers_per_user: reg_gauge_vec(
                active_reg,
                "cortex_ingester_active_native_histogram_buckets_custom_tracker",
                "Number of currently active native histogram buckets matching a pre-configured label matchers per user.",
                &["user", "name"],
            ),
            active_series_loading,
            attributed_active_series_failures_per_user: reg_counter_vec(
                reg,
                "cortex_ingester_attributed_active_series_failure",
                "The total number of failed active series decrement per user",
                &["user"],
            ),
            owned_series_per_user: reg_gauge_vec(
                reg,
                "cortex_ingester_owned_series",
                "Number of currently owned series per user.",
                &["user"],
            ),
            label_value_bytes_over_threshold: reg_gauge_vec(
                reg,
                "cortex_ingester_label_value_bytes_over_threshold",
                "Set to 1 for each (user, label name) pair whose distinct label values currently reach the local label value bytes metric threshold on this ingester.",
                &["user", "label"],
            ),
            max_local_series_per_user,
            compactions_triggered: reg_counter(
                reg,
                "cortex_ingester_tsdb_compactions_triggered_total",
                "Total number of triggered compactions.",
            ),
            compactions_failed: reg_counter(
                reg,
                "cortex_ingester_tsdb_compactions_failed_total",
                "Total number of compactions that failed.",
            ),
            forced_compaction_in_progress: reg_gauge(
                reg,
                "cortex_ingester_tsdb_forced_compactions_in_progress",
                "Reports 1 if there's a forced or idle TSDB head compaction in progress, 0 otherwise.",
            ),
            per_tenant_early_compactions_triggered: reg_counter(
                reg,
                "cortex_ingester_tsdb_per_tenant_early_compactions_triggered_total",
                "Total number of triggered per-tenant early compactions.",
            ),
            early_compaction_non_owned_series_triggered: reg_counter_vec(
                reg,
                "cortex_ingester_tsdb_early_compaction_non_owned_series_triggered_total",
                "Total number of triggered early head compactions of non-owned series, per tenant.",
                &["user"],
            ),
            appender_add_duration: reg_histogram(
                reg,
                "cortex_ingester_tsdb_appender_add_duration_seconds",
                "The total time it takes for a push request to add samples to the TSDB appender.",
                appender_buckets.clone(),
            ),
            appender_commit_duration: reg_histogram(
                reg,
                "cortex_ingester_tsdb_appender_commit_duration_seconds",
                "The total time it takes for a push request to commit samples appended to TSDB.",
                appender_buckets,
            ),
            idle_tsdb_checks,
            open_existing_tsdb: reg_counter(
                reg,
                "cortex_ingester_tsdb_open_duration_seconds_total",
                "The total time it takes to open all existing TSDBs at ingester startup. This time also includes the TSDBs WAL replay duration.",
            ),
            forced_compactions_count: Mutex::new(0),
            discarded: DiscardedMetrics::new(reg),
            rejected,
            discarded_metadata_per_user_limit,
            discarded_metadata_per_metric_limit,
            shutdown_marker: reg_gauge(
                reg,
                "cortex_ingester_prepare_shutdown_requested",
                "If the ingester has been requested to prepare for shutdown via endpoint or marker file.",
            ),
            utilization_limited_requests: reg_counter_vec(
                reg,
                "cortex_ingester_utilization_limited_read_requests_total",
                "Total number of times read requests have been rejected due to utilization based limiting.",
                &["reason"],
            ),
            index_lookup_comparison_outcomes: reg_counter_vec(
                reg,
                "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
                "Total number of index lookup planning comparison outcomes when using mirrored chunk querier.",
                &["outcome", "user"],
            ),
            label_value_bytes_labels: Mutex::new(HashMap::new()),
        }
    }

    pub fn increase_forced_compactions(&self) {
        let mut count = self.forced_compactions_count.lock();
        *count += 1;
        if *count == 1 {
            self.forced_compaction_in_progress.set(1.0);
        }
    }

    pub fn decrease_forced_compactions(&self) {
        let mut count = self.forced_compactions_count.lock();
        *count = (*count - 1).max(0);
        if *count == 0 {
            self.forced_compaction_in_progress.set(0.0);
        }
    }

    pub fn reset_forced_compactions(&self) {
        let mut count = self.forced_compactions_count.lock();
        *count = 0;
        self.forced_compaction_in_progress.set(0.0);
    }

    pub fn forced_compactions_count(&self) -> i64 {
        *self.forced_compactions_count.lock()
    }

    pub fn delete_per_user_metrics(&self, user_id: &str, custom_tracker_names: &[String]) {
        let _ = self.ingested_samples.remove_label_values(&[user_id]);
        let _ = self.ingested_samples_fail.remove_label_values(&[user_id]);
        let _ = self
            .series_with_floats_and_histograms
            .remove_label_values(&[user_id]);
        let _ = self
            .mem_metadata_created_total
            .remove_label_values(&[user_id]);
        let _ = self
            .mem_metadata_removed_total
            .remove_label_values(&[user_id]);
        self.discarded.delete_user(user_id);
        let _ = self
            .discarded_metadata_per_user_limit
            .remove_label_values(&[user_id]);
        let _ = self
            .discarded_metadata_per_metric_limit
            .remove_label_values(&[user_id]);
        let _ = self.max_local_series_per_user.remove_label_values(&[user_id]);
        let _ = self.owned_series_per_user.remove_label_values(&[user_id]);
        let _ = self
            .early_compaction_non_owned_series_triggered
            .remove_label_values(&[user_id]);
        let _ = self
            .attributed_active_series_failures_per_user
            .remove_label_values(&[user_id]);
        self.delete_per_user_custom_tracker_metrics(user_id, custom_tracker_names);
    }

    pub fn record_label_value_bytes_over_threshold(&self, user_id: &str, label_name: &str) {
        self.label_value_bytes_over_threshold
            .with_label_values(&[user_id, label_name])
            .set(1.0);
        self.label_value_bytes_labels
            .lock()
            .entry(user_id.to_string())
            .or_default()
            .insert(label_name.to_string());
    }

    pub fn delete_user_label_value_bytes_metrics(&self, user_id: &str, label_names: &[String]) {
        for lbl in label_names {
            let _ = self
                .label_value_bytes_over_threshold
                .remove_label_values(&[user_id, lbl]);
        }
        if let Some(tracked) = self.label_value_bytes_labels.lock().remove(user_id) {
            for lbl in tracked {
                let _ = self
                    .label_value_bytes_over_threshold
                    .remove_label_values(&[user_id, &lbl]);
            }
        }
    }

    pub fn delete_per_user_custom_tracker_metrics(
        &self,
        user_id: &str,
        custom_tracker_names: &[String],
    ) {
        let _ = self.active_series_per_user.remove_label_values(&[user_id]);
        let _ = self
            .active_series_per_user_otlp
            .remove_label_values(&[user_id]);
        let _ = self
            .active_series_per_user_native_histograms
            .remove_label_values(&[user_id]);
        let _ = self
            .active_native_histogram_buckets_per_user
            .remove_label_values(&[user_id]);
        for name in custom_tracker_names {
            let _ = self
                .active_series_custom_trackers_per_user
                .remove_label_values(&[user_id, name]);
            let _ = self
                .active_series_custom_trackers_per_user_native_histograms
                .remove_label_values(&[user_id, name]);
            let _ = self
                .active_native_histogram_buckets_custom_trackers_per_user
                .remove_label_values(&[user_id, name]);
        }
    }
}
