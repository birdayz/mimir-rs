//! Anonymous usage statistics registry, seed management, report builder, and reporter.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/usagestats/stats.go
//! - github.com/grafana/mimir/pkg/usagestats/report.go
//! - github.com/grafana/mimir/pkg/usagestats/seed.go
//! - github.com/grafana/mimir/pkg/usagestats/reporter.go

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::{Method, Request};
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::backoff::{Backoff, BackoffConfig};
use crate::logging::format_rfc3339_nano_utc;
use crate::services::{basic_service, ManagedBasicService, ServiceContext, ServiceError};

pub const TARGET_KEY: &str = "target";
pub const EDITION_KEY: &str = "edition";
pub const MODE_KEY: &str = "mode";

pub const EDITION_OSS: &str = "oss";
pub const EDITION_ENTERPRISE: &str = "enterprise";

pub const MODE_CLASSIC: &str = "classic";
pub const MODE_INGEST_STORAGE: &str = "ingest_storage";

pub const DEFAULT_REPORT_SEND_INTERVAL: Duration = Duration::from_secs(4 * 3600);
pub const DEFAULT_REPORT_CHECK_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_CPU_USAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_STATS_SERVER_URL: &str = "https://stats.grafana.org/mimir-usage-report";

pub const CLUSTER_SEED_FILE_NAME: &str = "mimir_cluster_seed.json";
pub const CLUSTER_SEED_FILE_MIN_STABILITY: Duration = Duration::from_secs(5 * 60);

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
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = v.to_string();
    }
    pub fn value(&self) -> String {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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
    pub fn update_rate(&self) {
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
    pub fn reset(&self) {
        self.total.store(0, Ordering::Relaxed);
        self.rate.store(0f64.to_bits(), Ordering::Relaxed);
        *self
            .reset_time
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
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
            let mut reg = registry()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    let reg = registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// `ClusterSeed` (`pkg/usagestats/seed.go`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterSeed {
    #[serde(rename = "UID")]
    pub uid: String,
    #[serde(rename = "created_at", with = "go_time")]
    pub created_at: SystemTime,
}

/// `prom.PrometheusVersion` (`pkg/usagestats/report.go`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrometheusVersion {
    pub version: String,
    pub revision: String,
    pub branch: String,
    #[serde(rename = "buildUser")]
    pub build_user: String,
    #[serde(rename = "buildDate")]
    pub build_date: String,
    #[serde(rename = "goVersion")]
    pub go_version: String,
}

/// `Report` (`pkg/usagestats/report.go`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Report {
    #[serde(rename = "clusterID")]
    pub cluster_id: String,
    #[serde(rename = "createdAt", with = "go_time")]
    pub created_at: SystemTime,
    #[serde(with = "go_time")]
    pub interval: SystemTime,
    #[serde(rename = "intervalPeriod")]
    pub interval_period: f64,
    pub target: String,
    pub version: PrometheusVersion,
    pub os: String,
    pub arch: String,
    pub edition: String,
    pub mode: String,
    pub metrics: serde_json::Map<String, serde_json::Value>,
}

mod go_time {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::format_rfc3339_nano_utc(*t))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        let s = String::deserialize(d)?;
        super::parse_rfc3339(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid time {s:?}")))
    }
}

/// Howard Hinnant's `days_from_civil` (proleptic Gregorian) for RFC3339 parsing without `chrono`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parses RFC3339 / RFC3339Nano timestamps (e.g. `2006-01-02T15:04:05.999999999Z` or `+01:00`).
pub fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || (bytes[10] != b'T' && bytes[10] != b't') || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    let month: i64 = s[5..7].parse().ok()?;
    let day: i64 = s[8..10].parse().ok()?;
    let hour: i64 = s[11..13].parse().ok()?;
    let minute: i64 = s[14..16].parse().ok()?;
    let second: i64 = s[17..19].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }

    let mut pos = 19usize;
    let mut nanos: u32 = 0;
    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let frac_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        if pos == frac_start {
            return None;
        }
        let frac = &s[frac_start..pos];
        let mut mul = 100_000_000u32;
        for &b in frac.as_bytes().iter().take(9) {
            nanos += u32::from(b - b'0') * mul;
            mul /= 10;
        }
    }

    let tz_offset_secs: i64 = match bytes.get(pos) {
        Some(b'Z') | Some(b'z') => {
            if pos + 1 != bytes.len() {
                return None;
            }
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            if pos + 6 != bytes.len() || bytes[pos + 3] != b':' {
                return None;
            }
            let off_h: i64 = s[pos + 1..pos + 3].parse().ok()?;
            let off_m: i64 = s[pos + 4..pos + 6].parse().ok()?;
            let secs = off_h * 3600 + off_m * 60;
            if sign == b'+' { secs } else { -secs }
        }
        _ => return None,
    };

    let days = days_from_civil(year, month, day);
    let unix_secs = days * 86_400 + hour * 3600 + minute * 60 + second - tz_offset_secs;
    if unix_secs >= 0 {
        Some(UNIX_EPOCH + Duration::new(unix_secs as u64, nanos))
    } else {
        let abs = (-unix_secs) as u64;
        if nanos == 0 {
            Some(UNIX_EPOCH - Duration::from_secs(abs))
        } else {
            Some(UNIX_EPOCH - Duration::new(abs - 1, 1_000_000_000 - nanos))
        }
    }
}

fn new_uuid_v4() -> String {
    let mut b: [u8; 16] = rand::thread_rng().gen();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

pub fn new_cluster_seed() -> ClusterSeed {
    ClusterSeed {
        uid: new_uuid_v4(),
        created_at: SystemTime::now(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedError {
    NotFound,
    Corrupted(String),
    Other(String),
    Canceled,
}

impl std::fmt::Display for SeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeedError::NotFound => f.write_str("the cluster seed file does not exist"),
            SeedError::Corrupted(e) | SeedError::Other(e) => f.write_str(e),
            SeedError::Canceled => f.write_str("context canceled"),
        }
    }
}

impl std::error::Error for SeedError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedBucketError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for SeedBucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeedBucketError::NotFound => f.write_str("object does not exist"),
            SeedBucketError::Other(s) => f.write_str(s),
        }
    }
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object storage abstraction for `pkg/usagestats/{seed,reporter}.go`.
pub trait SeedBucket: Send + Sync {
    fn get_object<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, SeedBucketError>>;
    fn put_object<'a>(&'a self, name: &'a str, data: Vec<u8>) -> BoxFuture<'a, Result<(), String>>;
}

pub async fn read_seed_file(bucket: &dyn SeedBucket) -> Result<ClusterSeed, SeedError> {
    let data = bucket
        .get_object(CLUSTER_SEED_FILE_NAME)
        .await
        .map_err(|e| match e {
            SeedBucketError::NotFound => SeedError::NotFound,
            SeedBucketError::Other(msg) => SeedError::Other(msg),
        })?;
    serde_json::from_slice(&data)
        .map_err(|e| SeedError::Corrupted(format!("{e}: the cluster seed file is corrupted")))
}

pub async fn write_seed_file(bucket: &dyn SeedBucket, seed: &ClusterSeed) -> Result<(), String> {
    let data = serde_json::to_vec(seed).map_err(|e| e.to_string())?;
    bucket.put_object(CLUSTER_SEED_FILE_NAME, data).await
}

fn duration_with_jitter(d: Duration, variance: f64) -> Duration {
    let v = d.as_secs_f64() * variance;
    if v <= 0.0 {
        return d;
    }
    let delta = rand::thread_rng().gen_range(-v..v);
    Duration::from_secs_f64((d.as_secs_f64() + delta).max(0.0))
}

fn duration_with_positive_jitter(d: Duration, variance: f64) -> Duration {
    let v = d.as_secs_f64() * variance;
    if v <= 0.0 {
        return d;
    }
    Duration::from_secs_f64(d.as_secs_f64() + rand::thread_rng().gen_range(0.0..v))
}

async fn sleep_or_cancel(ctx: &ServiceContext, d: Duration) -> Result<(), SeedError> {
    tokio::select! {
        _ = ctx.canceled() => Err(SeedError::Canceled),
        _ = tokio::time::sleep(d) => Ok(()),
    }
}

pub async fn wait_seed_file_stability(
    ctx: &ServiceContext,
    bucket: &dyn SeedBucket,
    min_stability: Duration,
) -> Result<ClusterSeed, SeedError> {
    let mut backoff = Backoff::new(BackoffConfig {
        min_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(60),
        max_retries: 0,
    });
    while backoff.ongoing() && !ctx.is_canceled() {
        let seed = match read_seed_file(bucket).await {
            Ok(seed) => seed,
            Err(e @ (SeedError::NotFound | SeedError::Corrupted(_))) => return Err(e),
            Err(_) => {
                tokio::select! {
                    _ = ctx.canceled() => return Err(SeedError::Canceled),
                    _ = backoff.wait() => {}
                }
                continue;
            }
        };
        let created_ago = SystemTime::now()
            .duration_since(seed.created_at)
            .unwrap_or_default();
        if created_ago >= min_stability {
            return Ok(seed);
        }
        sleep_or_cancel(
            ctx,
            duration_with_positive_jitter(min_stability - created_ago, 0.2),
        )
        .await?;
    }
    Err(SeedError::Canceled)
}

pub async fn init_seed_file(
    ctx: &ServiceContext,
    bucket: &dyn SeedBucket,
    min_stability: Duration,
) -> Result<ClusterSeed, SeedError> {
    let mut backoff = Backoff::new(BackoffConfig {
        min_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(60),
        max_retries: 0,
    });
    while backoff.ongoing() && !ctx.is_canceled() {
        if let Ok(seed) = wait_seed_file_stability(ctx, bucket, min_stability).await {
            return Ok(seed);
        }
        sleep_or_cancel(ctx, duration_with_jitter(min_stability / 2, 1.0)).await?;

        if read_seed_file(bucket).await.is_ok() {
            tokio::select! {
                _ = ctx.canceled() => return Err(SeedError::Canceled),
                _ = backoff.wait() => {}
            }
            continue;
        }

        let seed = new_cluster_seed();
        let _ = write_seed_file(bucket, &seed).await;
        tokio::select! {
            _ = ctx.canceled() => return Err(SeedError::Canceled),
            _ = backoff.wait() => {}
        }
    }
    Err(SeedError::Canceled)
}

/// `getNextReportAt`: `createdAt + ceil((now - createdAt) / interval) * interval`.
pub fn get_next_report_at(
    interval: Duration,
    created_at: SystemTime,
    now: SystemTime,
) -> SystemTime {
    let since_ns: i128 = match now.duration_since(created_at) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    };
    let interval_ns = interval.as_nanos() as i128;
    let n = ((since_ns as f64) / (interval_ns as f64)).ceil() as i128;
    let offset = n * interval_ns;
    if offset >= 0 {
        created_at + Duration::from_nanos(offset as u64)
    } else {
        created_at - Duration::from_nanos((-offset) as u64)
    }
}

pub fn go_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

pub fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        other => other,
    }
}

fn build_memstats() -> serde_json::Value {
    let (vsz, rss) = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            let mut it = s.split_whitespace().map(|x| x.parse::<u64>().ok());
            Some((it.next()?? * 4096, it.next()?? * 4096))
        })
        .unwrap_or((0, 0));
    serde_json::json!({
        "alloc": rss,
        "total_alloc": rss,
        "sys": vsz,
        "heap_alloc": rss,
        "heap_inuse": rss,
        "stack_inuse": 0,
        "pause_total_ns": 0,
        "num_gc": 0,
        "gc_cpu_fraction": 0.0,
    })
}

pub fn build_report(seed: &ClusterSeed, report_at: SystemTime, interval: Duration) -> Report {
    build_report_with_version(seed, report_at, interval, PrometheusVersion::default())
}

pub fn build_report_with_version(
    seed: &ClusterSeed,
    report_at: SystemTime,
    interval: Duration,
    version: PrometheusVersion,
) -> Report {
    let cpu_usage = get_float("cpu_usage");
    let mut metrics = serde_json::Map::new();
    metrics.insert("memstats".to_string(), build_memstats());
    let tasks = tokio::runtime::Handle::try_current()
        .map(|h| h.metrics().num_alive_tasks())
        .unwrap_or(0);
    metrics.insert("num_goroutine".to_string(), serde_json::Value::from(tasks));
    for (k, v) in collect_metrics() {
        metrics.insert(k, v);
    }
    cpu_usage.set(0.0);
    Report {
        cluster_id: seed.uid.clone(),
        created_at: seed.created_at,
        interval: report_at,
        interval_period: interval.as_secs_f64(),
        target: string_value(TARGET_KEY).unwrap_or_default(),
        version,
        os: go_os().to_string(),
        arch: go_arch().to_string(),
        edition: string_value(EDITION_KEY).unwrap_or_default(),
        mode: string_value(MODE_KEY).unwrap_or_default(),
        metrics,
    }
}

#[derive(Clone, Debug)]
pub struct ReporterConfig {
    pub server_url: String,
    pub report_check_interval: Duration,
    pub report_send_interval: Duration,
    pub seed_file_min_stability: Duration,
    pub cpu_usage_sample_interval: Duration,
}

impl Default for ReporterConfig {
    fn default() -> Self {
        Self {
            server_url: DEFAULT_STATS_SERVER_URL.to_string(),
            report_check_interval: DEFAULT_REPORT_CHECK_INTERVAL,
            report_send_interval: DEFAULT_REPORT_SEND_INTERVAL,
            seed_file_min_stability: CLUSTER_SEED_FILE_MIN_STABILITY,
            cpu_usage_sample_interval: DEFAULT_CPU_USAGE_SAMPLE_INTERVAL,
        }
    }
}

struct TokioIo(TcpStream);

impl hyper::rt::Read for TokioIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let n = unsafe {
            let mut tbuf = ReadBuf::uninit(buf.as_mut());
            match Pin::new(&mut self.0).poll_read(cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        };
        unsafe { buf.advance(n) };
        Poll::Ready(Ok(()))
    }
}

impl hyper::rt::Write for TokioIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

fn parse_http_url(url: &str) -> Result<(String, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("unsupported scheme in URL {url:?}"))?;
    match rest.find('/') {
        Some(idx) => Ok((rest[..idx].to_string(), rest[idx..].to_string())),
        None => Ok((rest.to_string(), "/".to_string())),
    }
}

async fn read_body_bytes(mut body: Incoming) -> Result<Vec<u8>, String> {
    use http_body_util_fallback::collect_body;
    collect_body(&mut body).await
}

mod http_body_util_fallback {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Buf;
    use hyper::body::{Body, Incoming};

    pub async fn collect_body(body: &mut Incoming) -> Result<Vec<u8>, String> {
        struct NextFrame<'a>(&'a mut Incoming);
        impl std::future::Future for NextFrame<'_> {
            type Output = Option<Result<hyper::body::Frame<bytes::Bytes>, hyper::Error>>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                Pin::new(&mut *self.0).poll_frame(cx)
            }
        }
        let mut out = Vec::new();
        while let Some(frame) = NextFrame(body).await {
            let frame = frame.map_err(|e| format!("read the response from the stats server: {e}"))?;
            if let Some(chunk) = frame.data_ref() {
                out.extend_from_slice(chunk.chunk());
            }
        }
        Ok(out)
    }
}

/// `usagestats.Reporter` (`pkg/usagestats/reporter.go`).
pub struct Reporter {
    pub cfg: Mutex<ReporterConfig>,
    bucket: Arc<dyn SeedBucket>,
    pub requests_total: Counter,
    pub requests_failed_total: Counter,
    pub requests_latency: Histogram,
}

impl Reporter {
    pub fn new(
        cfg: ReporterConfig,
        bucket: Arc<dyn SeedBucket>,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        let requests_total = Counter::with_opts(Opts::new(
            "cortex_usage_stats_report_sends_total",
            "The total number of attempted send requests.",
        ))
        .expect("valid metric");
        let requests_failed_total = Counter::with_opts(Opts::new(
            "cortex_usage_stats_report_sends_failed_total",
            "The total number of failed send requests.",
        ))
        .expect("valid metric");
        let requests_latency = Histogram::with_opts(
            HistogramOpts::new(
                "cortex_usage_stats_report_sends_latency_seconds",
                "The latency of report send requests in seconds (include both succeeded and failed requests).",
            )
            .buckets(prometheus::DEFAULT_BUCKETS.to_vec()),
        )
        .expect("valid metric");
        if let Some(r) = reg {
            let _ = r.register(Box::new(requests_total.clone()));
            let _ = r.register(Box::new(requests_failed_total.clone()));
            let _ = r.register(Box::new(requests_latency.clone()));
        }
        Arc::new(Self {
            cfg: Mutex::new(cfg),
            bucket,
            requests_total,
            requests_failed_total,
            requests_latency,
        })
    }

    pub async fn send_report(&self, report: &Report) -> Result<(), String> {
        let start = Instant::now();
        self.requests_total.inc();
        let res = self.do_send(report).await;
        self.requests_latency.observe(start.elapsed().as_secs_f64());
        if res.is_err() {
            self.requests_failed_total.inc();
        }
        res
    }

    async fn do_send(&self, report: &Report) -> Result<(), String> {
        let url = self
            .cfg
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .server_url
            .clone();
        let (host, path) = parse_http_url(&url)?;
        let data = serde_json::to_vec(report).map_err(|e| format!("marshal the report: {e}"))?;

        let stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&host))
            .await
            .map_err(|_| "send the report to the stats server: timeout".to_string())?
            .map_err(|e| {
                let msg = e.to_string().to_lowercase();
                format!("send the report to the stats server: {msg}")
            })?;

        let (mut sender, conn) = http1::handshake(TokioIo(stream))
            .await
            .map_err(|e| format!("send the report to the stats server: {e}"))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let req = Request::builder()
            .method(Method::POST)
            .uri(&path)
            .header("Host", &host)
            .header("Content-Type", "application/json")
            .header("Content-Length", data.len().to_string())
            .body(FullBytesBody::new(Bytes::from(data)))
            .map_err(|e| format!("build request: {e}"))?;

        let resp = tokio::time::timeout(Duration::from_secs(5), sender.send_request(req))
            .await
            .map_err(|_| "send the report to the stats server: timeout".to_string())?
            .map_err(|e| format!("send the report to the stats server: {e}"))?;

        let status = resp.status();
        let body = read_body_bytes(resp.into_body()).await?;
        if !status.is_success() {
            let shown = &body[..body.len().min(128)];
            return Err(format!(
                "received status code: {} and body: {:?}",
                status.as_u16(),
                String::from_utf8_lossy(shown)
            ));
        }
        Ok(())
    }

    async fn running(self: Arc<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let cfg = self
            .cfg
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let seed = match init_seed_file(&ctx, &*self.bucket, cfg.seed_file_min_stability).await {
            Ok(seed) => seed,
            Err(SeedError::Canceled) => return Ok(()),
            Err(e) => return Err(ServiceError::failure(e.to_string())),
        };

        let interval = cfg.report_send_interval;
        let mut next_report_at = get_next_report_at(interval, seed.created_at, SystemTime::now());
        let mut next_report: Option<Report> = None;

        let mut check = tokio::time::interval(cfg.report_check_interval);
        check.tick().await;

        loop {
            tokio::select! {
                _ = ctx.canceled() => return Ok(()),
                _ = check.tick() => {
                    let now = SystemTime::now();
                    if now < next_report_at {
                        continue;
                    }
                    if now.duration_since(next_report_at).unwrap_or_default() >= interval {
                        next_report_at = get_next_report_at(interval, seed.created_at, now);
                        next_report = None;
                        continue;
                    }
                    let report = next_report.get_or_insert_with(|| build_report(&seed, next_report_at, interval));
                    if self.send_report(report).await.is_err() {
                        continue;
                    }
                    next_report_at = get_next_report_at(interval, seed.created_at, SystemTime::now());
                    next_report = None;
                }
            }
        }
    }

    pub fn into_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        basic_service(
            None,
            Some(Box::new(move |ctx: ServiceContext| {
                let r = Arc::clone(&self);
                Box::pin(r.running(ctx))
            })),
            None,
        )
        .with_name("usage-stats")
    }
}

struct FullBytesBody(Option<Bytes>);

impl FullBytesBody {
    fn new(b: Bytes) -> Self {
        Self(if b.is_empty() { None } else { Some(b) })
    }
}

impl hyper::body::Body for FullBytesBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.0.take().map(|b| Ok(hyper::body::Frame::data(b))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
