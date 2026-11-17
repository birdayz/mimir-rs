//! Cost attribution: per-tenant attribution of active series and discarded samples to a
//! configurable set of labels, exported on a dedicated registry (`-cost-attribution.registry-path`).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/costattribution/manager.go
//! - github.com/grafana/mimir/pkg/costattribution/composite.go
//! - github.com/grafana/mimir/pkg/costattribution/active_tracker.go
//! - github.com/grafana/mimir/pkg/costattribution/sample_tracker.go
//! - github.com/grafana/mimir/pkg/costattribution/metrics.go
//! - github.com/grafana/mimir/pkg/costattribution/costattributionmodel/{labels.go,tracker_config.go}
//! - github.com/grafana/mimir/pkg/util/validation/limits.go (CostAttributionConfig / CostAttributionConfigHash)

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_config::limits::{CostAttributionLabel, CostAttributionTrackerConfig, LimitsConfig};
use mimir_config::Overrides;
use mimir_tsdb::labels::Labels;
use parking_lot::RwLock;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{CounterVec, Opts, Registry};
use tracing::warn;

const TRACKER_LABEL: &str = "tracker";
const TENANT_LABEL: &str = "tenant";
const REASON_LABEL: &str = "reason";
const MISSING_VALUE: &str = "__missing__";
const OVERFLOW_VALUE: &str = "__overflow__";
const ACTIVE_SERIES_TRACKER_TYPE: &str = "active-series";
const SAMPLES_TRACKER_TYPE: &str = "samples";

/// Name of the default tracker configured via the deprecated `cost_attribution_labels_structured`.
pub const DEFAULT_TRACKER_NAME: &str = "cost-attribution";

/// Separator between label values in observation keys (Go: `sep = rune(0x80)`).
const SEP: char = '\u{80}';

/// Max number of tracked combinations, as a factor of max cardinality (Go: `trackedSeriesFactor`).
const TRACKED_SERIES_FACTOR: usize = 2;

// ---------------------------------------------------------------------------
// Model (`costattributionmodel`)
// ---------------------------------------------------------------------------

fn output_label(l: &CostAttributionLabel) -> &str {
    if l.output.is_empty() {
        &l.input
    } else {
        &l.output
    }
}

fn label_string(l: &CostAttributionLabel) -> String {
    format!("{}:{}", l.input, l.output)
}

fn is_valid_utf8_label_name(name: &str) -> bool {
    // model.UTF8Validation.IsValidLabelName: non-empty valid UTF-8 (Rust strings are always UTF-8).
    !name.is_empty()
}

/// Ported from `costattributionmodel.Label.Validate`.
pub fn validate_label(l: &CostAttributionLabel) -> Result<(), String> {
    if l.input.is_empty() {
        return Err(format!(
            "cost attribution input label must not be empty: {:?}",
            label_string(l)
        ));
    }
    if !is_valid_utf8_label_name(&l.input) {
        return Err(format!(
            "invalid cost attribution input label: {:?}",
            label_string(l)
        ));
    }
    if !l.output.is_empty() && (!is_valid_utf8_label_name(&l.output) || l.output.starts_with("__"))
    {
        return Err(format!(
            "invalid cost attribution output label: {:?}",
            label_string(l)
        ));
    }
    Ok(())
}

/// Ported from `costattributionmodel.Labels.Validate`.
pub fn validate_labels(labels: &[CostAttributionLabel]) -> Result<(), String> {
    let mut outputs = std::collections::HashSet::new();
    for l in labels {
        validate_label(l)?;
        if !outputs.insert(output_label(l).to_string()) {
            return Err(format!("duplicate output label: {:?}", label_string(l)));
        }
    }
    Ok(())
}

/// Ported from `costattributionmodel.TrackerConfigs.Validate`.
pub fn validate_tracker_configs(
    tc: &BTreeMap<String, CostAttributionTrackerConfig>,
) -> Result<(), String> {
    for (name, cfg) in tc {
        if name.is_empty() {
            return Err("cost attribution tracker name must not be empty".to_string());
        }
        // model.LabelValue(name).IsValid(): valid UTF-8, always true for Rust strings.
        validate_labels(&cfg.labels)
            .map_err(|e| format!("cost attribution tracker {name:?}: {e}"))?;
        if cfg.labels.is_empty() {
            return Err(format!(
                "cost attribution tracker {name:?} must have at least one label"
            ));
        }
    }
    Ok(())
}

/// Resolved per-tenant cost attribution config (Go: `validation.CostAttributionConfig`).
#[derive(Clone, Debug, PartialEq)]
pub struct CostAttributionConfig {
    pub trackers: BTreeMap<String, CostAttributionTrackerConfig>,
    pub max_cardinality: usize,
    pub cooldown: Duration,
}

/// Returns the canonical base trackers: the deprecated `cost_attribution_labels_structured` is
/// migrated into a tracker named `cost-attribution`, and labels are sorted by input name
/// (Go: `Limits.migrateCostAttributionLabelsStructured` + `TrackerConfigs.Canonicalize`).
fn canonical_trackers(
    limits: &LimitsConfig,
) -> (
    BTreeMap<String, CostAttributionTrackerConfig>,
    BTreeMap<String, CostAttributionTrackerConfig>,
) {
    let mut base = limits.cost_attribution_trackers.clone();
    if base.is_empty() && !limits.cost_attribution_labels_structured.is_empty() {
        base.insert(
            DEFAULT_TRACKER_NAME.to_string(),
            CostAttributionTrackerConfig {
                labels: limits.cost_attribution_labels_structured.clone(),
                internal: false,
            },
        );
    }
    let mut additional = limits.additional_cost_attribution_trackers.clone();
    for cfg in base.values_mut().chain(additional.values_mut()) {
        cfg.labels.sort_by(|a, b| a.input.cmp(&b.input));
    }
    (base, additional)
}

/// Returns `(config_hash, config)` if the tenant has any cost attribution tracker configured
/// (Go: `Overrides.CostAttributionConfigHash` + `Overrides.CostAttributionConfig`).
pub fn cost_attribution_config(limits: &LimitsConfig) -> Option<(u64, CostAttributionConfig)> {
    let (base, additional) = canonical_trackers(limits);
    if base.is_empty() && additional.is_empty() {
        return None;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(&base)
        .unwrap_or_default()
        .hash(&mut h);
    limits.max_cost_attribution_cardinality.hash(&mut h);
    limits.cost_attribution_cooldown.0.hash(&mut h);
    serde_json::to_string(&additional)
        .unwrap_or_default()
        .hash(&mut h);
    let hash = h.finish();

    // MergeTrackerConfigs: if a key exists in both, additional wins.
    let mut trackers = base;
    for (k, v) in additional {
        trackers.insert(k, v);
    }
    Some((
        hash,
        CostAttributionConfig {
            trackers,
            max_cardinality: limits.max_cost_attribution_cardinality.max(0) as usize,
            cooldown: Duration::from_nanos(limits.cost_attribution_cooldown.0.max(0) as u64),
        },
    ))
}

// ---------------------------------------------------------------------------
// Metric helpers (`metrics.go`)
// ---------------------------------------------------------------------------

/// Go `prometheus.NewDesc` label name check (UTF-8 validation scheme, reserved `__` prefix).
fn validate_metric_label_name(name: &str) -> Result<(), String> {
    if !is_valid_utf8_label_name(name) || name.starts_with("__") {
        return Err(format!("{name:?} is not a valid label name"));
    }
    Ok(())
}

/// A metric descriptor with dynamic label values (Go: `descriptor`).
#[derive(Clone, Debug)]
struct Descriptor {
    name: String,
    help: String,
    labels: Vec<String>,
    const_labels: Vec<(String, String)>,
}

impl Descriptor {
    fn new(
        name: &str,
        help: &str,
        labels: Vec<String>,
        const_labels: Vec<(String, String)>,
    ) -> Result<Self, String> {
        for l in labels.iter().chain(const_labels.iter().map(|(k, _)| k)) {
            validate_metric_label_name(l)?;
        }
        let mut seen = std::collections::HashSet::new();
        for l in labels.iter().chain(const_labels.iter().map(|(k, _)| k)) {
            if !seen.insert(l.as_str()) {
                return Err(format!(
                    "duplicate label names in constant and variable labels for metric {name:?}"
                ));
            }
        }
        Ok(Self {
            name: name.to_string(),
            help: help.to_string(),
            labels,
            const_labels,
        })
    }

    fn metric(&self, label_values: &[&str]) -> Metric {
        let mut pairs: Vec<LabelPair> = Vec::with_capacity(self.labels.len() + 1);
        for (n, v) in self.labels.iter().zip(label_values.iter()) {
            let mut lp = LabelPair::default();
            lp.set_name(n.clone());
            lp.set_value((*v).to_string());
            pairs.push(lp);
        }
        for (n, v) in &self.const_labels {
            let mut lp = LabelPair::default();
            lp.set_name(n.clone());
            lp.set_value(v.clone());
            pairs.push(lp);
        }
        // Go's MakeLabelPairs sorts label pairs by name.
        pairs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
        let mut m = Metric::default();
        m.set_label(pairs.into());
        m
    }

    fn gauge(&self, v: f64, label_values: &[&str]) -> (String, String, MetricType, Metric) {
        let mut m = self.metric(label_values);
        let mut g = prometheus::proto::Gauge::default();
        g.set_value(v);
        m.set_gauge(g);
        (self.name.clone(), self.help.clone(), MetricType::GAUGE, m)
    }

    fn counter(&self, v: f64, label_values: &[&str]) -> (String, String, MetricType, Metric) {
        let mut m = self.metric(label_values);
        let mut c = prometheus::proto::Counter::default();
        c.set_value(v);
        m.set_counter(c);
        (self.name.clone(), self.help.clone(), MetricType::COUNTER, m)
    }
}

type Sample = (String, String, MetricType, Metric);

/// Groups individual metrics into metric families (preserving first-seen order of names).
fn to_families(samples: Vec<Sample>) -> Vec<MetricFamily> {
    let mut order: Vec<String> = Vec::new();
    let mut by_name: HashMap<String, MetricFamily> = HashMap::new();
    for (name, help, typ, m) in samples {
        let mf = by_name.entry(name.clone()).or_insert_with(|| {
            order.push(name.clone());
            let mut mf = MetricFamily::default();
            mf.set_name(name.clone());
            mf.set_help(help);
            mf.set_field_type(typ);
            mf
        });
        mf.mut_metric().push(m);
    }
    order
        .into_iter()
        .filter_map(|n| by_name.remove(&n))
        .collect()
}

/// `f64` stored in an `AtomicU64` (Go: `atomic.Float64`).
#[derive(Default)]
struct AtomicF64(AtomicU64);

impl AtomicF64 {
    fn new(v: f64) -> Self {
        Self(AtomicU64::new(v.to_bits()))
    }
    fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
    fn add(&self, delta: f64) {
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let new = (f64::from_bits(cur) + delta).to_bits();
            match self
                .0
                .compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(x) => cur = x,
            }
        }
    }
}

fn unix_secs(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => -(e.duration().as_secs() as i64),
    }
}

// ---------------------------------------------------------------------------
// Active series tracker (`active_tracker.go`)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Counters {
    active_series: AtomicI64,
    native_histograms: AtomicI64,
    native_histogram_buckets: AtomicI64,
}

impl Counters {
    fn inc(&self, nh_buckets: i64) {
        self.active_series.fetch_add(1, Ordering::Relaxed);
        if nh_buckets >= 0 {
            self.native_histograms.fetch_add(1, Ordering::Relaxed);
            self.native_histogram_buckets
                .fetch_add(nh_buckets, Ordering::Relaxed);
        }
    }

    /// Decrements and returns the new active series value.
    fn dec(&self, nh_buckets: i64) -> i64 {
        let nv = self.active_series.fetch_sub(1, Ordering::Relaxed) - 1;
        if nh_buckets >= 0 {
            self.native_histograms.fetch_sub(1, Ordering::Relaxed);
            self.native_histogram_buckets
                .fetch_sub(nh_buckets, Ordering::Relaxed);
        }
        nv
    }
}

struct ActiveState {
    observed: HashMap<String, Arc<Counters>>,
    overflow_since: Option<SystemTime>,
}

/// Ported from `pkg/costattribution.activeSeriesTracker`.
struct IndividualActiveTracker {
    user_id: String,
    name: String,
    internal: bool,
    labels: Vec<CostAttributionLabel>,
    overflow_labels: Vec<String>,
    max_cardinality: usize,
    cooldown: Duration,

    active_series: Descriptor,
    overflow_labels_desc: Descriptor,
    native_histogram_series: Descriptor,
    native_histogram_buckets: Descriptor,

    state: RwLock<ActiveState>,
    overflow_counter: Counters,
}

fn fill_key(labels: &[CostAttributionLabel], get: &dyn Fn(&str) -> Option<String>) -> String {
    let mut key = String::new();
    for (idx, cal) in labels.iter().enumerate() {
        if idx > 0 {
            key.push(SEP);
        }
        match get(&cal.input) {
            Some(v) if !v.is_empty() => key.push_str(&v),
            _ => key.push_str(MISSING_VALUE),
        }
    }
    key
}

impl IndividualActiveTracker {
    fn new(
        user_id: &str,
        name: &str,
        labels: Vec<CostAttributionLabel>,
        internal: bool,
        max_cardinality: usize,
        cooldown: Duration,
    ) -> Result<Self, String> {
        let mut overflow_labels = vec![OVERFLOW_VALUE.to_string(); labels.len()];
        overflow_labels.push(user_id.to_string());

        let mut variable: Vec<String> =
            labels.iter().map(|l| output_label(l).to_string()).collect();
        variable.push(TENANT_LABEL.to_string());
        let cl = vec![(TRACKER_LABEL.to_string(), name.to_string())];
        let mk = |n: &str, h: &str| Descriptor::new(n, h, variable.clone(), cl.clone());
        let res = (|| -> Result<_, String> {
            Ok((
                mk(
                    "cortex_ingester_attributed_active_series",
                    "The total number of active series per user and attribution.",
                )?,
                mk(
                    "cortex_attributed_series_overflow_labels",
                    "The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.",
                )?,
                mk(
                    "cortex_ingester_attributed_active_native_histogram_series",
                    "The total number of active native histogram series per user and attribution.",
                )?,
                mk(
                    "cortex_ingester_attributed_active_native_histogram_buckets",
                    "The total number of active native histogram buckets per user and attribution.",
                )?,
            ))
        })()
        .map_err(|e| {
            format!("failed to create an active series tracker for tenant {user_id}: {e}")
        })?;

        Ok(Self {
            user_id: user_id.to_string(),
            name: name.to_string(),
            internal,
            labels,
            overflow_labels,
            max_cardinality,
            cooldown,
            active_series: res.0,
            overflow_labels_desc: res.1,
            native_histogram_series: res.2,
            native_histogram_buckets: res.3,
            state: RwLock::new(ActiveState {
                observed: HashMap::new(),
                overflow_since: None,
            }),
            overflow_counter: Counters::default(),
        })
    }

    fn increment(&self, lbls: &Labels, now: SystemTime, nh_buckets: i64) {
        let key = fill_key(&self.labels, &|n| {
            let v = lbls.get(n);
            (!v.is_empty()).then(|| v.to_string())
        });
        {
            let st = self.state.read();
            if let Some(c) = st.observed.get(&key) {
                c.inc(nh_buckets);
                return;
            }
            if st.observed.len() >= TRACKED_SERIES_FACTOR * self.max_cardinality {
                self.overflow_counter.inc(nh_buckets);
                return;
            }
        }
        let mut st = self.state.write();
        if let Some(c) = st.observed.get(&key) {
            c.inc(nh_buckets);
            return;
        }
        if st.observed.len() >= TRACKED_SERIES_FACTOR * self.max_cardinality {
            self.overflow_counter.inc(nh_buckets);
            return;
        }
        let c = Arc::new(Counters::default());
        c.inc(nh_buckets);
        st.observed.insert(key, c);
        if st.observed.len() > self.max_cardinality && st.overflow_since.is_none() {
            st.overflow_since = Some(now);
        }
    }

    fn decrement(&self, lbls: &Labels, nh_buckets: i64) {
        let key = fill_key(&self.labels, &|n| {
            let v = lbls.get(n);
            (!v.is_empty()).then(|| v.to_string())
        });
        {
            let st = self.state.read();
            if let Some(c) = st.observed.get(&key) {
                if c.dec(nh_buckets) > 0 {
                    return;
                }
            } else {
                if st.overflow_since.is_some() {
                    self.overflow_counter.dec(nh_buckets);
                    return;
                }
                panic!(
                    "decrementing non-existent active series: labels={lbls}, cost attribution keys: {:?}, the current observation map length: {}, the current cost attribution key: {key}",
                    self.labels,
                    st.observed.len()
                );
            }
        }
        let mut st = self.state.write();
        if st
            .observed
            .get(&key)
            .is_some_and(|c| c.active_series.load(Ordering::Relaxed) == 0)
        {
            st.observed.remove(&key);
        }
    }

    fn collect(&self, out: &mut Vec<Sample>) {
        let ov: Vec<&str> = self.overflow_labels.iter().map(String::as_str).collect();
        out.push(self.overflow_labels_desc.gauge(1.0, &ov));

        let st = self.state.read();
        if st.overflow_since.is_some() {
            let (mut a, mut h, mut b) = (0i64, 0i64, 0i64);
            for c in st.observed.values() {
                a += c.active_series.load(Ordering::Relaxed);
                h += c.native_histograms.load(Ordering::Relaxed);
                b += c.native_histogram_buckets.load(Ordering::Relaxed);
            }
            drop(st);
            out.push(self.active_series.gauge(
                (a + self.overflow_counter.active_series.load(Ordering::Relaxed)) as f64,
                &ov,
            ));
            let hc = (h + self
                .overflow_counter
                .native_histograms
                .load(Ordering::Relaxed)) as f64;
            if hc > 0.0 {
                out.push(self.native_histogram_series.gauge(hc, &ov));
            }
            let bc = (b + self
                .overflow_counter
                .native_histogram_buckets
                .load(Ordering::Relaxed)) as f64;
            if bc > 0.0 {
                out.push(self.native_histogram_buckets.gauge(bc, &ov));
            }
            return;
        }
        for (key, c) in &st.observed {
            let mut keys: Vec<&str> = key.split(SEP).collect();
            keys.push(&self.user_id);
            out.push(
                self.active_series
                    .gauge(c.active_series.load(Ordering::Relaxed) as f64, &keys),
            );
            let h = c.native_histograms.load(Ordering::Relaxed);
            if h > 0 {
                out.push(self.native_histogram_series.gauge(h as f64, &keys));
            }
            let b = c.native_histogram_buckets.load(Ordering::Relaxed);
            if b > 0 {
                out.push(self.native_histogram_buckets.gauge(b as f64, &keys));
            }
        }
    }

    fn purge(&self, now: SystemTime, deadline: SystemTime) -> (usize, bool) {
        let (try_recover, cardinality) = {
            let st = self.state.read();
            (
                st.overflow_since
                    .is_some_and(|s| s + self.cooldown < deadline),
                st.observed.len(),
            )
        };
        let mut should_recreate = false;
        if try_recover {
            let mut st = self.state.write();
            if st.observed.len() <= self.max_cardinality {
                should_recreate = true;
            } else {
                st.overflow_since = Some(now);
            }
        }
        (cardinality, should_recreate)
    }

    fn cardinality(&self) -> (usize, bool) {
        let st = self.state.read();
        (st.observed.len(), st.overflow_since.is_some())
    }
}

/// Composite active series tracker delegating to one or more individual trackers
/// (Go: `costattribution.ActiveSeriesTracker`).
pub struct ActiveSeriesTracker {
    trackers: Vec<IndividualActiveTracker>,
    config_hash: u64,
}

impl std::fmt::Debug for ActiveSeriesTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveSeriesTracker")
            .field(
                "trackers",
                &self.trackers.iter().map(|t| &t.name).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ActiveSeriesTracker {
    /// Always builds fresh trackers (active series trackers are never reused across composites).
    fn rebuild(
        user_id: &str,
        cfg: &CostAttributionConfig,
        config_hash: u64,
        creation_errors: &CounterVec,
    ) -> Self {
        let mut trackers = Vec::with_capacity(cfg.trackers.len());
        for (name, tcfg) in &cfg.trackers {
            match IndividualActiveTracker::new(
                user_id,
                name,
                tcfg.labels.clone(),
                tcfg.internal,
                cfg.max_cardinality,
                cfg.cooldown,
            ) {
                Ok(t) => trackers.push(t),
                Err(err) => {
                    creation_errors
                        .with_label_values(&[user_id, ACTIVE_SERIES_TRACKER_TYPE, name])
                        .inc();
                    warn!(user = user_id, tracker_name = %name, error = %err, "error creating cost attribution tracker, skipping it");
                }
            }
        }
        trackers.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            trackers,
            config_hash,
        }
    }

    /// Creates a tracker with a single non-internal tracker (Go: `NewActiveSeriesTrackerForTests`).
    pub fn new_for_tests(
        user_id: &str,
        tracker_name: &str,
        labels: Vec<CostAttributionLabel>,
        max_cardinality: usize,
        cooldown: Duration,
    ) -> Result<Arc<Self>, String> {
        let t = IndividualActiveTracker::new(
            user_id,
            tracker_name,
            labels,
            false,
            max_cardinality,
            cooldown,
        )?;
        Ok(Arc::new(Self {
            trackers: vec![t],
            config_hash: 0,
        }))
    }

    /// Registers a collector exposing this tracker's (non-internal) cost attribution metrics.
    pub fn register_for_tests(self: &Arc<Self>, reg: &Registry) {
        let _ = reg.register(Box::new(ActiveCompositeCollector(Arc::clone(self))));
    }

    pub fn increment(&self, lbls: &Labels, now: SystemTime, native_histogram_bucket_num: i64) {
        for t in &self.trackers {
            t.increment(lbls, now, native_histogram_bucket_num);
        }
    }

    pub fn decrement(&self, lbls: &Labels, native_histogram_bucket_num: i64) {
        for t in &self.trackers {
            t.decrement(lbls, native_histogram_bucket_num);
        }
    }

    /// Returns true if both composites are the same instance (Go: `Equals`).
    pub fn equals(self: &Arc<Self>, other: Option<&Arc<Self>>) -> bool {
        other.is_some_and(|o| Arc::ptr_eq(self, o))
    }

    fn purge(&self, now: SystemTime, deadline: SystemTime) -> (usize, bool) {
        let mut card = 0;
        let mut recreate = false;
        for t in &self.trackers {
            let (c, r) = t.purge(now, deadline);
            card += c;
            recreate |= r;
        }
        (card, recreate)
    }

    fn collect(&self, internal: bool, out: &mut Vec<Sample>) {
        for t in self.trackers.iter().filter(|t| t.internal == internal) {
            t.collect(out);
        }
    }
}

struct ActiveCompositeCollector(Arc<ActiveSeriesTracker>);

impl Collector for ActiveCompositeCollector {
    fn desc(&self) -> Vec<&Desc> {
        Vec::new()
    }
    fn collect(&self) -> Vec<MetricFamily> {
        let mut out = Vec::new();
        self.0.collect(false, &mut out);
        to_families(out)
    }
}

// ---------------------------------------------------------------------------
// Sample tracker (`sample_tracker.go`)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Observation {
    last_update: AtomicI64,
    received_sample: AtomicF64,
    discarded_sample: RwLock<HashMap<String, Arc<AtomicF64>>>,
    total_discarded: AtomicF64,
}

impl Observation {
    fn add_discarded(&self, reason: &str, v: f64) {
        {
            let g = self.discarded_sample.read();
            if let Some(r) = g.get(reason) {
                r.add(v);
                return;
            }
        }
        let mut g = self.discarded_sample.write();
        match g.get(reason) {
            Some(r) => r.add(v),
            None => {
                g.insert(reason.to_string(), Arc::new(AtomicF64::new(v)));
            }
        }
    }
}

struct SampleState {
    observed: HashMap<String, Arc<Observation>>,
    overflow_since: Option<SystemTime>,
    overflow_counter: Arc<Observation>,
}

/// Ported from `pkg/costattribution.sampleTracker`.
struct IndividualSampleTracker {
    user_id: String,
    name: String,
    internal: bool,
    labels: Vec<CostAttributionLabel>,
    overflow_labels: Vec<String>,
    max_cardinality: usize,
    cooldown: Duration,

    received: Descriptor,
    discarded: Descriptor,

    state: RwLock<SampleState>,
}

impl IndividualSampleTracker {
    fn new(
        user_id: &str,
        name: &str,
        labels: Vec<CostAttributionLabel>,
        internal: bool,
        max_cardinality: usize,
        cooldown: Duration,
    ) -> Result<Self, String> {
        let mut overflow_labels = vec![OVERFLOW_VALUE.to_string(); labels.len()];
        overflow_labels.push(user_id.to_string());
        overflow_labels.push(OVERFLOW_VALUE.to_string());

        let mut variable: Vec<String> =
            labels.iter().map(|l| output_label(l).to_string()).collect();
        variable.push(TENANT_LABEL.to_string());
        variable.push(REASON_LABEL.to_string());
        let cl = vec![(TRACKER_LABEL.to_string(), name.to_string())];
        let received = Descriptor::new(
            "cortex_distributor_received_attributed_samples_total",
            "The total number of samples that were received per attribution.",
            variable[..variable.len() - 1].to_vec(),
            cl.clone(),
        );
        let discarded = Descriptor::new(
            "cortex_discarded_attributed_samples_total",
            "The total number of samples that were discarded per attribution.",
            variable,
            cl,
        );
        let (received, discarded) = match (received, discarded) {
            (Ok(r), Ok(d)) => (r, d),
            (Err(e), _) | (_, Err(e)) => {
                return Err(format!(
                    "failed to create a sample tracker for tenant {user_id}: {e}"
                ))
            }
        };
        Ok(Self {
            user_id: user_id.to_string(),
            name: name.to_string(),
            internal,
            labels,
            overflow_labels,
            max_cardinality,
            cooldown,
            received,
            discarded,
            state: RwLock::new(SampleState {
                observed: HashMap::new(),
                overflow_since: None,
                overflow_counter: Arc::new(Observation::default()),
            }),
        })
    }

    fn same_config(
        &self,
        tcfg: &CostAttributionTrackerConfig,
        cfg: &CostAttributionConfig,
    ) -> bool {
        self.max_cardinality == cfg.max_cardinality
            && self.internal == tcfg.internal
            && self.cooldown == cfg.cooldown
            && self.labels == tcfg.labels
    }

    fn update_observations(
        &self,
        key: &str,
        ts: SystemTime,
        received_inc: f64,
        discarded_inc: f64,
        reason: Option<&str>,
    ) {
        {
            let st = self.state.read();
            if st.overflow_since.is_some() {
                st.overflow_counter.received_sample.add(received_inc);
                if discarded_inc > 0.0 && reason.is_some() {
                    st.overflow_counter.total_discarded.add(discarded_inc);
                }
                return;
            }
            if let Some(o) = st.observed.get(key) {
                o.last_update.store(unix_secs(ts), Ordering::Relaxed);
                o.received_sample.add(received_inc);
                if let (true, Some(r)) = (discarded_inc > 0.0, reason) {
                    o.add_discarded(r, discarded_inc);
                }
                return;
            }
        }

        let mut st = self.state.write();
        if st.overflow_since.is_none() {
            if let Some(o) = st.observed.get(key) {
                o.last_update.store(unix_secs(ts), Ordering::Relaxed);
                o.received_sample.add(received_inc);
                if let (true, Some(r)) = (discarded_inc > 0.0, reason) {
                    o.add_discarded(r, discarded_inc);
                }
                return;
            }
            if st.observed.len() >= self.max_cardinality {
                st.overflow_since = Some(ts);
            }
        }
        if st.overflow_since.is_some() {
            st.overflow_counter.received_sample.add(received_inc);
            if discarded_inc > 0.0 && reason.is_some() {
                st.overflow_counter.total_discarded.add(discarded_inc);
            }
            return;
        }
        let o = Arc::new(Observation {
            last_update: AtomicI64::new(unix_secs(ts)),
            received_sample: AtomicF64::new(received_inc),
            ..Observation::default()
        });
        if let (true, Some(r)) = (discarded_inc > 0.0, reason) {
            o.add_discarded(r, discarded_inc);
        }
        st.observed.insert(key.to_string(), o);
    }

    fn collect(&self, out: &mut Vec<Sample>) {
        let st = self.state.read();
        if st.overflow_since.is_some() {
            let ov: Vec<&str> = self.overflow_labels.iter().map(String::as_str).collect();
            out.push(self.received.counter(
                st.overflow_counter.received_sample.load(),
                &ov[..ov.len() - 1],
            ));
            out.push(
                self.discarded
                    .counter(st.overflow_counter.total_discarded.load(), &ov),
            );
            return;
        }
        for (key, o) in &st.observed {
            let mut keys: Vec<&str> = key.split(SEP).collect();
            keys.push(&self.user_id);
            let r = o.received_sample.load();
            if r > 0.0 {
                out.push(self.received.counter(r, &keys));
            }
            let d = o.discarded_sample.read();
            for (reason, v) in d.iter() {
                let mut kr = keys.clone();
                kr.push(reason);
                out.push(self.discarded.counter(v.load(), &kr));
            }
        }
    }

    fn purge(&self, now: SystemTime, deadline: SystemTime) -> usize {
        let deadline_secs = unix_secs(deadline);
        let (expired, try_recover, mut cardinality) = {
            let st = self.state.read();
            let expired: Vec<String> = st
                .observed
                .iter()
                .filter(|(_, o)| o.last_update.load(Ordering::Relaxed) <= deadline_secs)
                .map(|(k, _)| k.clone())
                .collect();
            let try_recover = st
                .overflow_since
                .is_some_and(|s| s + self.cooldown < deadline);
            let card = st.observed.len() - expired.len();
            (expired, try_recover, card)
        };
        if !expired.is_empty() || try_recover {
            let mut st = self.state.write();
            for k in &expired {
                st.observed.remove(k);
            }
            if try_recover {
                if st.observed.len() < self.max_cardinality {
                    st.observed = HashMap::new();
                    st.overflow_since = None;
                    st.overflow_counter = Arc::new(Observation::default());
                    cardinality = 0;
                } else {
                    st.overflow_since = Some(now);
                }
            }
        }
        cardinality
    }

    fn cardinality(&self) -> (usize, bool) {
        let st = self.state.read();
        (st.observed.len(), st.overflow_since.is_some())
    }
}

/// Composite sample tracker (Go: `costattribution.SampleTracker`).
pub struct SampleTracker {
    trackers: Vec<Arc<IndividualSampleTracker>>,
    config_hash: u64,
}

impl SampleTracker {
    /// Reuses individual trackers whose config didn't change, preserving their counts.
    fn rebuild(
        prev: Option<&SampleTracker>,
        user_id: &str,
        cfg: &CostAttributionConfig,
        config_hash: u64,
        creation_errors: &CounterVec,
    ) -> Self {
        let existing: HashMap<&str, &Arc<IndividualSampleTracker>> = prev
            .map(|p| p.trackers.iter().map(|t| (t.name.as_str(), t)).collect())
            .unwrap_or_default();
        let mut trackers = Vec::with_capacity(cfg.trackers.len());
        for (name, tcfg) in &cfg.trackers {
            if let Some(t) = existing.get(name.as_str()) {
                if t.same_config(tcfg, cfg) {
                    trackers.push(Arc::clone(t));
                    continue;
                }
            }
            match IndividualSampleTracker::new(
                user_id,
                name,
                tcfg.labels.clone(),
                tcfg.internal,
                cfg.max_cardinality,
                cfg.cooldown,
            ) {
                Ok(t) => trackers.push(Arc::new(t)),
                Err(err) => {
                    creation_errors
                        .with_label_values(&[user_id, SAMPLES_TRACKER_TYPE, name])
                        .inc();
                    warn!(user = user_id, tracker_name = %name, error = %err, "error creating cost attribution tracker, skipping it");
                }
            }
        }
        trackers.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            trackers,
            config_hash,
        }
    }

    /// Attributes `value` discarded samples with `reason` to the series identified by `get`
    /// (a label-name -> value lookup).
    pub fn increment_discarded_samples(
        &self,
        get: &dyn Fn(&str) -> Option<String>,
        value: f64,
        reason: &str,
        now: SystemTime,
    ) {
        for t in &self.trackers {
            let key = fill_key(&t.labels, get);
            t.update_observations(&key, now, 0.0, value, Some(reason));
        }
    }

    /// Convenience wrapper over [`Self::increment_discarded_samples`] for a label pair slice.
    pub fn increment_discarded_samples_pairs(
        &self,
        labels: &[(&str, &str)],
        value: f64,
        reason: &str,
        now: SystemTime,
    ) {
        let get = |n: &str| {
            labels
                .iter()
                .find(|(k, _)| *k == n)
                .map(|(_, v)| v.to_string())
        };
        self.increment_discarded_samples(&get, value, reason, now);
    }

    /// Attributes received samples of a write request: `series` is `(labels, samples+histograms)`
    /// per time series (Go: `IncrementReceivedSamples`, used by the distributor).
    pub fn increment_received_samples(&self, series: &[(&[(&str, &str)], usize)], now: SystemTime) {
        for t in &self.trackers {
            let mut dict: HashMap<String, usize> = HashMap::new();
            for (labels, count) in series {
                let get = |n: &str| {
                    labels
                        .iter()
                        .find(|(k, _)| *k == n)
                        .map(|(_, v)| v.to_string())
                };
                *dict.entry(fill_key(&t.labels, &get)).or_default() += count;
            }
            for (k, v) in dict {
                t.update_observations(&k, now, v as f64, 0.0, None);
            }
        }
    }

    fn purge(&self, now: SystemTime, deadline: SystemTime) -> (usize, bool) {
        let card = self.trackers.iter().map(|t| t.purge(now, deadline)).sum();
        (card, false)
    }

    fn collect(&self, internal: bool, out: &mut Vec<Sample>) {
        for t in self.trackers.iter().filter(|t| t.internal == internal) {
            t.collect(out);
        }
    }
}

// ---------------------------------------------------------------------------
// Manager (`manager.go`)
// ---------------------------------------------------------------------------

trait Composite: Sized {
    fn hash(&self) -> u64;
    fn tracker_infos(&self) -> Vec<(String, usize, bool)>;
    fn build(
        prev: Option<&Self>,
        user: &str,
        cfg: &CostAttributionConfig,
        hash: u64,
        errs: &CounterVec,
    ) -> Self;
    fn purge(&self, now: SystemTime, deadline: SystemTime) -> (usize, bool);
    fn collect(&self, internal: bool, out: &mut Vec<Sample>);
}

impl Composite for ActiveSeriesTracker {
    fn hash(&self) -> u64 {
        self.config_hash
    }
    fn tracker_infos(&self) -> Vec<(String, usize, bool)> {
        self.trackers
            .iter()
            .map(|t| {
                let (c, o) = t.cardinality();
                (t.name.clone(), c, o)
            })
            .collect()
    }
    fn build(
        _prev: Option<&Self>,
        user: &str,
        cfg: &CostAttributionConfig,
        hash: u64,
        errs: &CounterVec,
    ) -> Self {
        Self::rebuild(user, cfg, hash, errs)
    }
    fn purge(&self, now: SystemTime, deadline: SystemTime) -> (usize, bool) {
        ActiveSeriesTracker::purge(self, now, deadline)
    }
    fn collect(&self, internal: bool, out: &mut Vec<Sample>) {
        ActiveSeriesTracker::collect(self, internal, out)
    }
}

impl Composite for SampleTracker {
    fn hash(&self) -> u64 {
        self.config_hash
    }
    fn tracker_infos(&self) -> Vec<(String, usize, bool)> {
        self.trackers
            .iter()
            .map(|t| {
                let (c, o) = t.cardinality();
                (t.name.clone(), c, o)
            })
            .collect()
    }
    fn build(
        prev: Option<&Self>,
        user: &str,
        cfg: &CostAttributionConfig,
        hash: u64,
        errs: &CounterVec,
    ) -> Self {
        Self::rebuild(prev, user, cfg, hash, errs)
    }
    fn purge(&self, now: SystemTime, deadline: SystemTime) -> (usize, bool) {
        SampleTracker::purge(self, now, deadline)
    }
    fn collect(&self, internal: bool, out: &mut Vec<Sample>) {
        SampleTracker::collect(self, internal, out)
    }
}

struct ManagerTrackers<C: Composite> {
    composite: RwLock<HashMap<String, Arc<C>>>,
    creation_errors: CounterVec,
    cardinality_desc: Descriptor,
    overflow_desc: Descriptor,
}

/// Cached resolved config per tenant, keyed by the identity of the tenant's `LimitsConfig`.
type ConfigCache =
    RwLock<HashMap<String, (Arc<LimitsConfig>, Option<(u64, Arc<CostAttributionConfig>)>)>>;

impl<C: Composite> ManagerTrackers<C> {
    fn get(
        &self,
        user: &str,
        cfg: Option<(u64, Arc<CostAttributionConfig>)>,
        cleanup_if_no_config: bool,
    ) -> Option<Arc<C>> {
        let Some((hash, cfg)) = cfg else {
            if cleanup_if_no_config {
                self.composite.write().remove(user);
            }
            return None;
        };
        if let Some(t) = self.composite.read().get(user) {
            if t.hash() == hash {
                return Some(Arc::clone(t));
            }
        }
        let mut g = self.composite.write();
        if let Some(t) = g.get(user) {
            if t.hash() == hash {
                return Some(Arc::clone(t));
            }
        }
        let built = Arc::new(C::build(
            g.get(user).map(|a| a.as_ref()),
            user,
            &cfg,
            hash,
            &self.creation_errors,
        ));
        g.insert(user.to_string(), Arc::clone(&built));
        Some(built)
    }

    fn snapshot(&self) -> Vec<(String, Arc<C>)> {
        self.composite
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect()
    }

    fn collect_operational(&self, out: &mut Vec<Sample>) {
        for (user, composite) in self.snapshot() {
            for (name, card, overflown) in composite.tracker_infos() {
                out.push(self.cardinality_desc.gauge(card as f64, &[&user, &name]));
                if overflown {
                    out.push(self.overflow_desc.gauge(1.0, &[&user, &name]));
                }
            }
            composite.collect(true, out);
        }
    }

    fn collect_cost_attribution(&self, out: &mut Vec<Sample>) {
        for (_, composite) in self.snapshot() {
            composite.collect(false, out);
        }
    }
}

/// Cost attribution manager (Go: `costattribution.Manager`). Tracker lookups rebuild the
/// per-tenant composite when the tenant's cost attribution config changes.
pub struct Manager {
    overrides: Overrides,
    inactive_timeout: Duration,
    config_cache: ConfigCache,
    sample_trackers: ManagerTrackers<SampleTracker>,
    active_series_trackers: ManagerTrackers<ActiveSeriesTracker>,
}

impl Manager {
    /// Creates a manager registering operational metrics in `reg` and cost attribution metrics
    /// in `cost_attribution_reg` (Go: `NewManager`).
    pub fn new(
        inactive_timeout: Duration,
        overrides: Overrides,
        reg: Option<&Registry>,
        cost_attribution_reg: &Registry,
    ) -> Result<Arc<Self>, String> {
        let creation_errors = CounterVec::new(
            Opts::new(
                "cortex_cost_attribution_tracker_creation_errors_total",
                "The total number of errors creating cost attribution trackers for each user.",
            ),
            &["user", "tracker_type", "tracker_name"],
        )
        .map_err(|e| e.to_string())?;
        if let Some(r) = reg {
            r.register(Box::new(creation_errors.clone()))
                .map_err(|e| format!("can't register operational metrics: {e}"))?;
        }
        let ul = || vec!["user".to_string(), TRACKER_LABEL.to_string()];
        let m = Arc::new(Self {
            overrides,
            inactive_timeout,
            config_cache: RwLock::new(HashMap::new()),
            sample_trackers: ManagerTrackers {
                composite: RwLock::new(HashMap::new()),
                creation_errors: creation_errors.clone(),
                cardinality_desc: Descriptor::new(
                    "cortex_cost_attribution_sample_tracker_cardinality",
                    "The cardinality of a cost attribution sample tracker for each user.",
                    ul(),
                    vec![],
                )?,
                overflow_desc: Descriptor::new(
                    "cortex_cost_attribution_sample_tracker_overflown",
                    "This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.",
                    ul(),
                    vec![],
                )?,
            },
            active_series_trackers: ManagerTrackers {
                composite: RwLock::new(HashMap::new()),
                creation_errors,
                cardinality_desc: Descriptor::new(
                    "cortex_cost_attribution_active_series_tracker_cardinality",
                    "The cardinality of a cost attribution active series tracker for each user.",
                    ul(),
                    vec![],
                )?,
                overflow_desc: Descriptor::new(
                    "cortex_cost_attribution_active_series_tracker_overflown",
                    "This metric is exported with value 1 when an active series tracker for a user is overflown. It's not exported otherwise.",
                    ul(),
                    vec![],
                )?,
            },
        });
        if let Some(r) = reg {
            r.register(Box::new(OperationalCollector::new(Arc::clone(&m))))
                .map_err(|e| format!("can't register operational metrics: {e}"))?;
        }
        cost_attribution_reg
            .register(Box::new(CostAttributionCollector::new(Arc::clone(&m))))
            .map_err(|e| format!("can't register cost attribution metrics: {e}"))?;
        Ok(m)
    }

    fn tenant_config(&self, user: &str) -> Option<(u64, Arc<CostAttributionConfig>)> {
        let limits = self.overrides.for_tenant(user);
        if let Some((cached_limits, cfg)) = self.config_cache.read().get(user) {
            if Arc::ptr_eq(cached_limits, &limits) {
                return cfg.clone();
            }
        }
        let cfg = cost_attribution_config(&limits).map(|(h, c)| (h, Arc::new(c)));
        self.config_cache
            .write()
            .insert(user.to_string(), (limits, cfg.clone()));
        cfg
    }

    /// Returns the tenant's active series tracker, or `None` if not configured.
    pub fn active_series_tracker(&self, user: &str) -> Option<Arc<ActiveSeriesTracker>> {
        let cfg = self.tenant_config(user);
        self.active_series_trackers.get(user, cfg, false)
    }

    /// Returns the tenant's sample tracker, or `None` if not configured.
    pub fn sample_tracker(&self, user: &str) -> Option<Arc<SampleTracker>> {
        let cfg = self.tenant_config(user);
        self.sample_trackers.get(user, cfg, false)
    }

    /// One cleanup iteration (Go: `Manager.iteration`).
    pub fn purge_inactive_attributions_until(&self, now: SystemTime) {
        let deadline = now.checked_sub(self.inactive_timeout).unwrap_or(UNIX_EPOCH);
        self.purge_trackers(&self.active_series_trackers, now, deadline);
        self.purge_trackers(&self.sample_trackers, now, deadline);
    }

    fn purge_trackers<C: Composite>(
        &self,
        mt: &ManagerTrackers<C>,
        now: SystemTime,
        deadline: SystemTime,
    ) {
        for (user, _) in mt.snapshot() {
            let cfg = self.tenant_config(&user);
            let Some(composite) = mt.get(&user, cfg, true) else {
                continue;
            };
            let (card, recreate) = composite.purge(now, deadline);
            if card == 0 || recreate {
                mt.composite.write().remove(&user);
            }
        }
    }

    /// Cleanup inactivity timeout (`-cost-attribution.eviction-interval`).
    pub fn inactive_timeout(&self) -> Duration {
        self.inactive_timeout
    }

    /// Introspection used by tests: number of tenants with a composite tracker.
    #[doc(hidden)]
    pub fn debug_num_composites(&self, kind: TrackerKind) -> usize {
        match kind {
            TrackerKind::Samples => self.sample_trackers.composite.read().len(),
            TrackerKind::ActiveSeries => self.active_series_trackers.composite.read().len(),
        }
    }

    /// Introspection used by tests: state of the individual tracker `name` of `user`, if any.
    #[doc(hidden)]
    pub fn debug_tracker(&self, kind: TrackerKind, user: &str, name: &str) -> Option<TrackerDebug> {
        match kind {
            TrackerKind::Samples => {
                let g = self.sample_trackers.composite.read();
                let t = g.get(user)?.trackers.iter().find(|t| t.name == name)?;
                let st = t.state.read();
                Some(TrackerDebug {
                    id: Arc::as_ptr(t) as usize,
                    labels: t.labels.clone(),
                    max_cardinality: t.max_cardinality,
                    observed: st.observed.len(),
                    overflown: st.overflow_since.is_some(),
                    overflow_active_series: 0,
                })
            }
            TrackerKind::ActiveSeries => {
                let g = self.active_series_trackers.composite.read();
                let t = g.get(user)?.trackers.iter().find(|t| t.name == name)?;
                let st = t.state.read();
                Some(TrackerDebug {
                    id: t as *const IndividualActiveTracker as usize,
                    labels: t.labels.clone(),
                    max_cardinality: t.max_cardinality,
                    observed: st.observed.len(),
                    overflown: st.overflow_since.is_some(),
                    overflow_active_series: t
                        .overflow_counter
                        .active_series
                        .load(Ordering::Relaxed),
                })
            }
        }
    }

    /// Introspection used by tests: number of individual trackers of `user`'s composite.
    #[doc(hidden)]
    pub fn debug_num_trackers(&self, kind: TrackerKind, user: &str) -> Option<usize> {
        match kind {
            TrackerKind::Samples => self
                .sample_trackers
                .composite
                .read()
                .get(user)
                .map(|c| c.trackers.len()),
            TrackerKind::ActiveSeries => self
                .active_series_trackers
                .composite
                .read()
                .get(user)
                .map(|c| c.trackers.len()),
        }
    }

    fn collect_operational(&self) -> Vec<MetricFamily> {
        let mut out = Vec::new();
        self.active_series_trackers.collect_operational(&mut out);
        self.sample_trackers.collect_operational(&mut out);
        to_families(out)
    }

    fn collect_cost_attribution(&self) -> Vec<MetricFamily> {
        let mut out = Vec::new();
        self.active_series_trackers
            .collect_cost_attribution(&mut out);
        self.sample_trackers.collect_cost_attribution(&mut out);
        to_families(out)
    }
}

/// Tracker kind selector for test introspection.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum TrackerKind {
    Samples,
    ActiveSeries,
}

/// Snapshot of an individual tracker's state (test introspection).
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TrackerDebug {
    /// Identity of the individual tracker instance.
    pub id: usize,
    pub labels: Vec<CostAttributionLabel>,
    pub max_cardinality: usize,
    pub observed: usize,
    pub overflown: bool,
    pub overflow_active_series: i64,
}

/// Unchecked collectors: a single descriptor is exposed only so that the registry accepts them.
struct OperationalCollector {
    desc: Desc,
    m: Arc<Manager>,
}

impl OperationalCollector {
    fn new(m: Arc<Manager>) -> Self {
        let desc = Desc::new(
            "cortex_cost_attribution_active_series_tracker_cardinality".to_string(),
            "The cardinality of a cost attribution active series tracker for each user."
                .to_string(),
            vec!["user".to_string(), TRACKER_LABEL.to_string()],
            HashMap::new(),
        )
        .expect("valid desc");
        Self { desc, m }
    }
}

impl Collector for OperationalCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }
    fn collect(&self) -> Vec<MetricFamily> {
        self.m.collect_operational()
    }
}

struct CostAttributionCollector {
    desc: Desc,
    m: Arc<Manager>,
}

impl CostAttributionCollector {
    fn new(m: Arc<Manager>) -> Self {
        let desc = Desc::new(
            "cortex_ingester_attributed_active_series".to_string(),
            "The total number of active series per user and attribution.".to_string(),
            vec![],
            HashMap::new(),
        )
        .expect("valid desc");
        Self { desc, m }
    }
}

impl Collector for CostAttributionCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }
    fn collect(&self) -> Vec<MetricFamily> {
        self.m.collect_cost_attribution()
    }
}
