//! hashicorp/go-metrics global sink as configured by dskit for memberlist's internal metrics.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/metrics.go (`metrics.NewGlobal` with
//!   `EnableTypePrefix=true`, `TimerGranularity=time.Second`, no hostname/service labels)
//! - github.com/hashicorp/go-metrics/prometheus/prometheus.go (`PrometheusSink`: metric name =
//!   `<type>_<key parts joined by "_">`, help = name, summaries with MaxAge 10s and objectives
//!   {0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, `Expiration: 0` so series never expire).
//!
//! Series are created lazily on first emission, exactly like the Go sink.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType, Quantile, Summary,
};

const SUMMARY_MAX_AGE: Duration = Duration::from_secs(10);
const QUANTILES: [f64; 3] = [0.5, 0.9, 0.99];
const OBSERVE_PRUNE_THRESHOLD: usize = 256;
const OBSERVE_MAX_SAMPLES: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Summary,
}

#[derive(Default)]
struct Series {
    value: f64,
    samples: VecDeque<(Instant, f64)>,
    sum: f64,
    count: u64,
}

struct Family {
    kind: Kind,
    series: BTreeMap<Vec<(String, String)>, Series>,
}

/// Go-metrics compatible sink (one per process in Go, since `metrics.NewGlobal` is global).
#[derive(Default)]
pub struct GoMetrics {
    families: Mutex<BTreeMap<String, Family>>,
}

fn flatten(type_prefix: &str, parts: &[&str]) -> String {
    let mut key = String::from(type_prefix);
    for p in parts {
        key.push('_');
        key.push_str(p);
    }
    // go-metrics prometheus `forbiddenCharsReplacer`: " ", ".", "=", "-", "/", ":" -> "_".
    key.chars()
        .map(|c| match c {
            ' ' | '.' | '=' | '-' | '/' | ':' => '_',
            c => c,
        })
        .collect()
}

impl GoMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_series(&self, name: String, kind: Kind, labels: &[(&str, &str)], f: impl FnOnce(&mut Series)) {
        let mut fams = self.families.lock();
        let fam = fams.entry(name).or_insert_with(|| Family {
            kind,
            series: BTreeMap::new(),
        });
        let mut key: Vec<(String, String)> =
            labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        key.sort();
        f(fam.series.entry(key).or_default());
    }

    /// go-metrics `IncrCounterWithLabels`.
    pub fn incr_counter(&self, key: &[&str], val: f64, labels: &[(&str, &str)]) {
        self.with_series(flatten("counter", key), Kind::Counter, labels, |s| s.value += val);
    }

    /// go-metrics `SetGaugeWithLabels`.
    pub fn set_gauge(&self, key: &[&str], val: f64, labels: &[(&str, &str)]) {
        // go-metrics passes float32 values to the sink.
        let v = val as f32 as f64;
        self.with_series(flatten("gauge", key), Kind::Gauge, labels, |s| s.value = v);
    }

    /// go-metrics `AddSampleWithLabels`.
    pub fn add_sample(&self, key: &[&str], val: f64, labels: &[(&str, &str)]) {
        self.observe(flatten("sample", key), val as f32 as f64, labels);
    }

    /// go-metrics `MeasureSinceWithLabels` with `TimerGranularity = time.Second`.
    pub fn measure_since(&self, key: &[&str], start: Instant, labels: &[(&str, &str)]) {
        let secs = start.elapsed().as_nanos() as f64 / 1e9;
        self.observe(flatten("timer", key), secs as f32 as f64, labels);
    }

    fn observe(&self, name: String, v: f64, labels: &[(&str, &str)]) {
        let now = Instant::now();
        self.with_series(name, Kind::Summary, labels, |s| {
            if s.samples.len() >= OBSERVE_PRUNE_THRESHOLD {
                while s
                    .samples
                    .front()
                    .is_some_and(|(t, _)| now.duration_since(*t) > SUMMARY_MAX_AGE)
                {
                    s.samples.pop_front();
                }
                while s.samples.len() >= OBSERVE_MAX_SAMPLES {
                    s.samples.pop_front();
                }
            }
            s.samples.push_back((now, v));
            s.sum += v;
            s.count += 1;
        });
    }

    /// Current value of a counter or gauge series (test helper).
    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let fams = self.families.lock();
        let fam = fams.get(name)?;
        let mut key: Vec<(String, String)> =
            labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        key.sort();
        fam.series.get(&key).map(|s| s.value)
    }

    /// Registers the sink as a collector (errors are logged like dskit does).
    pub fn register(self: &Arc<Self>, reg: &prometheus::Registry) {
        if let Err(err) = reg.register(Box::new(GoMetricsCollector(Arc::clone(self)))) {
            tracing::error!(%err, "failed to register prometheus metrics for memberlist");
        }
    }
}

struct GoMetricsCollector(Arc<GoMetrics>);

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[idx]
}

impl Collector for GoMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        Vec::new()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let now = Instant::now();
        let mut fams = self.0.families.lock();
        let mut out = Vec::with_capacity(fams.len());
        for (name, fam) in fams.iter_mut() {
            let mut mf = MetricFamily::default();
            mf.set_name(name.clone());
            mf.set_help(name.clone());
            mf.set_field_type(match fam.kind {
                Kind::Counter => MetricType::COUNTER,
                Kind::Gauge => MetricType::GAUGE,
                Kind::Summary => MetricType::SUMMARY,
            });
            for (labels, s) in fam.series.iter_mut() {
                let mut m = Metric::default();
                let pairs: Vec<LabelPair> = labels
                    .iter()
                    .map(|(k, v)| {
                        let mut lp = LabelPair::default();
                        lp.set_name(k.clone());
                        lp.set_value(v.clone());
                        lp
                    })
                    .collect();
                m.set_label(pairs.into());
                match fam.kind {
                    Kind::Counter => {
                        let mut c = Counter::default();
                        c.set_value(s.value);
                        m.set_counter(c);
                    }
                    Kind::Gauge => {
                        let mut g = Gauge::default();
                        g.set_value(s.value);
                        m.set_gauge(g);
                    }
                    Kind::Summary => {
                        while s
                            .samples
                            .front()
                            .is_some_and(|(t, _)| now.duration_since(*t) > SUMMARY_MAX_AGE)
                        {
                            s.samples.pop_front();
                        }
                        let mut sorted: Vec<f64> = s.samples.iter().map(|(_, v)| *v).collect();
                        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let mut sum = Summary::default();
                        sum.set_sample_count(s.count);
                        sum.set_sample_sum(s.sum);
                        let qs: Vec<Quantile> = QUANTILES
                            .iter()
                            .map(|q| {
                                let mut qq = Quantile::default();
                                qq.set_quantile(*q);
                                qq.set_value(quantile(&sorted, *q));
                                qq
                            })
                            .collect();
                        sum.set_quantile(qs.into());
                        m.set_summary(sum);
                    }
                }
                mf.mut_metric().push(m);
            }
            out.push(mf);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_names_match_go_metrics_prometheus_sink() {
        let gm = Arc::new(GoMetrics::new());
        let reg = prometheus::Registry::new();
        gm.register(&reg);
        gm.incr_counter(&["memberlist", "msg", "alive"], 1.0, &[]);
        gm.incr_counter(&["memberlist", "compress", "attempts_total"], 1.0, &[("algo", "lzw")]);
        gm.set_gauge(&["memberlist", "node", "instances"], 2.0, &[("node_state", "alive")]);
        gm.measure_since(&["memberlist", "probeNode"], Instant::now(), &[]);
        gm.add_sample(&["memberlist", "queue", "broadcasts"], 0.0, &[]);
        let names: Vec<String> = reg.gather().iter().map(|f| f.get_name().to_string()).collect();
        assert_eq!(
            names,
            vec![
                "counter_memberlist_compress_attempts_total",
                "counter_memberlist_msg_alive",
                "gauge_memberlist_node_instances",
                "sample_memberlist_queue_broadcasts",
                "timer_memberlist_probeNode",
            ]
        );
        let text = prometheus::TextEncoder::new()
            .encode_to_string(&reg.gather())
            .unwrap();
        assert!(text.contains("# HELP counter_memberlist_msg_alive counter_memberlist_msg_alive"));
        assert!(text.contains("timer_memberlist_probeNode{quantile=\"0.99\"}"));
        assert!(text.contains("timer_memberlist_probeNode_count 1"));
    }

    #[test]
    fn c10_observe_prunes_expired_and_bounds_unscraped_samples() {
        let gm = GoMetrics::new();
        // Seed expired samples directly, then observe OBSERVE_PRUNE_THRESHOLD times without scraping.
        let expired_ts = Instant::now() - SUMMARY_MAX_AGE - Duration::from_secs(5);
        gm.with_series(
            "timer_memberlist_gossip".to_string(),
            Kind::Summary,
            &[],
            |s| {
                for _ in 0..OBSERVE_PRUNE_THRESHOLD {
                    s.samples.push_back((expired_ts, 0.01));
                    s.sum += 0.01;
                    s.count += 1;
                }
            },
        );
        gm.observe("timer_memberlist_gossip".to_string(), 0.02, &[]);

        {
            let fams = gm.families.lock();
            let s = fams["timer_memberlist_gossip"]
                .series
                .values()
                .next()
                .unwrap();
            assert_eq!(s.samples.len(), 1, "expired samples must be pruned on observe");
            assert_eq!(s.count, (OBSERVE_PRUNE_THRESHOLD + 1) as u64);
        }

        // Also verify safety cap under heavy burst without scraping.
        for i in 0..(OBSERVE_MAX_SAMPLES + 500) {
            gm.observe("timer_memberlist_burst".to_string(), i as f64, &[]);
        }
        let fams = gm.families.lock();
        let s = fams["timer_memberlist_burst"]
            .series
            .values()
            .next()
            .unwrap();
        assert!(s.samples.len() <= OBSERVE_MAX_SAMPLES);
        assert_eq!(s.count, (OBSERVE_MAX_SAMPLES + 500) as u64);
    }
}
