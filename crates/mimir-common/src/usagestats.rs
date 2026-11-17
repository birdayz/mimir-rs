//! Anonymous usage statistics registry (Go publishes them as `expvar`s under the
//! `github.com/grafana/mimir/` prefix; the usage-stats reporter sends them periodically).
//!
//! Ported from github.com/grafana/mimir/pkg/usagestats/stats.go

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

pub const TARGET_KEY: &str = "target";
pub const EDITION_KEY: &str = "edition";
pub const MODE_KEY: &str = "mode";

pub const EDITION_OSS: &str = "oss";
pub const EDITION_ENTERPRISE: &str = "enterprise";

pub const MODE_CLASSIC: &str = "classic";
pub const MODE_INGEST_STORAGE: &str = "ingest_storage";

/// `expvar.Int`.
#[derive(Default)]
pub struct IntStat(AtomicI64);

impl IntStat {
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn add(&self, v: i64) {
        self.0.fetch_add(v, Ordering::Relaxed);
    }
    pub fn value(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// `expvar.Float`.
#[derive(Default)]
pub struct FloatStat(AtomicU64);

impl FloatStat {
    pub fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed);
    }
    pub fn value(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

/// `expvar.String`.
#[derive(Default)]
pub struct StringStat(Mutex<String>);

impl StringStat {
    pub fn set(&self, v: &str) {
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = v.to_string();
    }
    pub fn value(&self) -> String {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
}

/// `usagestats.Counter`: a total and the rate since the last reset.
pub struct CounterStat {
    total: AtomicI64,
    rate: AtomicU64,
    reset_time: Mutex<Instant>,
}

impl Default for CounterStat {
    fn default() -> Self {
        Self {
            total: AtomicI64::new(0),
            rate: AtomicU64::new(0f64.to_bits()),
            reset_time: Mutex::new(Instant::now()),
        }
    }
}

impl CounterStat {
    pub fn inc(&self, i: i64) {
        self.total.fetch_add(i, Ordering::Relaxed);
    }
    pub fn total(&self) -> i64 {
        self.total.load(Ordering::Relaxed)
    }
    fn update_rate(&self) {
        let elapsed = self
            .reset_time
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .elapsed()
            .as_secs_f64();
        let rate = if elapsed > 0.0 {
            self.total() as f64 / elapsed
        } else {
            0.0
        };
        self.rate.store(rate.to_bits(), Ordering::Relaxed);
    }
    fn reset(&self) {
        self.total.store(0, Ordering::Relaxed);
        self.rate.store(0f64.to_bits(), Ordering::Relaxed);
        *self.reset_time.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
    }
    /// `Counter.Value()`: `{"total": ..., "rate": ...}`.
    pub fn value(&self) -> serde_json::Value {
        serde_json::json!({
            "total": self.total(),
            "rate": f64::from_bits(self.rate.load(Ordering::Relaxed)),
        })
    }
}

#[derive(Clone)]
enum Stat {
    Int(Arc<IntStat>),
    Float(Arc<FloatStat>),
    String(Arc<StringStat>),
    Counter(Arc<CounterStat>),
}

fn registry() -> &'static Mutex<BTreeMap<String, Stat>> {
    static REG: OnceLock<Mutex<BTreeMap<String, Stat>>> = OnceLock::new();
    REG.get_or_init(|| {
        let mut m = BTreeMap::new();
        // `init()`: SetEdition(EditionOSS); SetMode(ModeClassic).
        let edition = Arc::new(StringStat::default());
        edition.set(EDITION_OSS);
        m.insert(EDITION_KEY.to_string(), Stat::String(edition));
        let mode = Arc::new(StringStat::default());
        mode.set(MODE_CLASSIC);
        m.insert(MODE_KEY.to_string(), Stat::String(mode));
        Mutex::new(m)
    })
}

macro_rules! getter {
    ($fn:ident, $variant:ident, $ty:ty, $kind:literal) => {
        pub fn $fn(name: &str) -> Arc<$ty> {
            let mut reg = registry().lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match reg.get(name) {
                Some(Stat::$variant(s)) => Arc::clone(s),
                Some(_) => panic!("{name} is set to a non-{} value", $kind),
                None => {
                    let s = Arc::new(<$ty>::default());
                    reg.insert(name.to_string(), Stat::$variant(Arc::clone(&s)));
                    s
                }
            }
        }
    };
}

getter!(get_int, Int, IntStat, "int");
getter!(get_float, Float, FloatStat, "float");
getter!(get_string, String, StringStat, "string");
getter!(get_counter, Counter, CounterStat, "Counter");

/// `GetAndResetInt`.
pub fn get_and_reset_int(name: &str) -> Arc<IntStat> {
    let s = get_int(name);
    s.set(0);
    s
}

/// `GetAndResetFloat`.
pub fn get_and_reset_float(name: &str) -> Arc<FloatStat> {
    let s = get_float(name);
    s.set(0.0);
    s
}

/// `GetAndResetCounter`.
pub fn get_and_reset_counter(name: &str) -> Arc<CounterStat> {
    let s = get_counter(name);
    s.reset();
    s
}

pub fn set_target(target: &str) {
    get_string(TARGET_KEY).set(target);
}

pub fn set_edition(edition: &str) {
    get_string(EDITION_KEY).set(edition);
}

pub fn set_mode(mode: &str) {
    get_string(MODE_KEY).set(mode);
}

/// Value of a string stat, if registered.
pub fn string_value(name: &str) -> Option<String> {
    match registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(name)
    {
        Some(Stat::String(s)) => Some(s.value()),
        _ => None,
    }
}

/// The stats part of `buildMetrics`: every stat except target/edition/mode; counters report
/// their rate since the last report and are reset.
pub fn collect_metrics() -> BTreeMap<String, serde_json::Value> {
    let reg = registry().lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut out = BTreeMap::new();
    for (k, v) in reg.iter() {
        if k == TARGET_KEY || k == EDITION_KEY || k == MODE_KEY {
            continue;
        }
        let value = match v {
            Stat::Int(s) => serde_json::Value::from(s.value()),
            Stat::Float(s) => serde_json::Value::from(s.value()),
            Stat::String(s) => serde_json::Value::from(s.value()),
            Stat::Counter(c) => {
                c.update_rate();
                let v = c.value();
                c.reset();
                v
            }
        };
        out.insert(k.clone(), value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream: pkg/usagestats/stats_test.go::TestStringer (Counter JSON shape) + defaults.
    #[test]
    fn stats_registry() {
        assert_eq!(string_value(EDITION_KEY).as_deref(), Some(EDITION_OSS));
        get_int("test_int").set(42);
        get_string("test_string").set("foo");
        let c = get_and_reset_counter("test_counter");
        c.inc(10);
        let m = collect_metrics();
        assert_eq!(m["test_int"], serde_json::json!(42));
        assert_eq!(m["test_string"], serde_json::json!("foo"));
        assert_eq!(m["test_counter"]["total"], serde_json::json!(10));
        assert!(!m.contains_key(EDITION_KEY));
        assert_eq!(c.total(), 0, "counters are reset after being reported");
    }
}
