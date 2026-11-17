//! Anonymous usage statistics reporter (`usage-stats` module).
//!
//! Every Mimir cluster shares a seed file (`__mimir_cluster/mimir_cluster_seed.json`) in the
//! blocks storage bucket; a report built from the process-wide usage stats is sent to
//! `https://stats.grafana.org/mimir-usage-report` every 4 hours (aligned on the seed creation
//! time, so all instances of a cluster report the same interval).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/usagestats/reporter.go
//! - github.com/grafana/mimir/pkg/usagestats/report.go
//! - github.com/grafana/mimir/pkg/usagestats/seed.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go (`initUsageStats`)

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext, ServiceError};
use mimir_common::usagestats;
use mimir_storage::bucket::{BucketClient, MIMIR_INTERNALS_PREFIX};
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::metrics::{branch_str, go_version_str, revision_str, version_str};

pub const DEFAULT_REPORT_SEND_INTERVAL: Duration = Duration::from_secs(4 * 3600);
const DEFAULT_REPORT_CHECK_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_CPU_USAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_STATS_SERVER_URL: &str = "https://stats.grafana.org/mimir-usage-report";

pub const CLUSTER_SEED_FILE_NAME: &str = "mimir_cluster_seed.json";
const CLUSTER_SEED_FILE_MIN_STABILITY: Duration = Duration::from_secs(5 * 60);

/// `ClusterSeed`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterSeed {
    #[serde(rename = "UID")]
    pub uid: String,
    #[serde(rename = "created_at", with = "go_time")]
    pub created_at: SystemTime,
}

/// `Report`.
#[derive(Clone, Debug, Serialize)]
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

/// `prom.PrometheusVersion`.
#[derive(Clone, Debug, Serialize)]
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

/// Go `time.Time` JSON (RFC 3339 with nanoseconds).
mod go_time {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&mimir_common::logging::format_rfc3339_nano_utc(*t))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        let s = String::deserialize(d)?;
        super::parse_rfc3339(&s).ok_or_else(|| serde::de::Error::custom(format!("invalid time {s:?}")))
    }
}

/// Parses RFC 3339 timestamps as written by Go (`2006-01-02T15:04:05.999999999Z07:00`).
pub fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || (b[10] != b'T' && b[10] != b't') || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (year, month, day) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hour, min, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let mut idx = 19;
    let mut nanos: i64 = 0;
    if b.get(idx) == Some(&b'.') {
        idx += 1;
        let start = idx;
        while idx < b.len() && b[idx].is_ascii_digit() {
            idx += 1;
        }
        let frac = &s[start..idx];
        let digits = frac.len().min(9);
        nanos = frac[..digits].parse::<i64>().ok()? * 10i64.pow((9 - digits) as u32);
    }
    let offset_secs = match s.get(idx..)? {
        "Z" | "z" => 0,
        tz if tz.len() == 6 && (tz.starts_with('+') || tz.starts_with('-')) => {
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            sign * (tz[1..3].parse::<i64>().ok()? * 3600 + tz[4..6].parse::<i64>().ok()? * 60)
        }
        _ => return None,
    };
    // days_from_civil (Howard Hinnant).
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + hour * 3600 + min * 60 + sec - offset_secs;
    let d = Duration::new(secs.unsigned_abs(), nanos as u32);
    if secs >= 0 {
        UNIX_EPOCH.checked_add(d)
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs(secs.unsigned_abs()))?
            .checked_add(Duration::from_nanos(nanos as u64))
    }
}

/// `uuid.NewString()` (random, version 4).
fn new_uuid_v4() -> String {
    let mut b: [u8; 16] = rand::thread_rng().gen();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// `newClusterSeed`.
pub fn new_cluster_seed() -> ClusterSeed {
    ClusterSeed {
        uid: new_uuid_v4(),
        created_at: SystemTime::now(),
    }
}

#[derive(Debug)]
enum SeedError {
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

/// `readSeedFile`.
async fn read_seed_file(bucket: &BucketClient) -> Result<ClusterSeed, SeedError> {
    let data = bucket.download(CLUSTER_SEED_FILE_NAME).await.map_err(|e| {
        if e.is_not_found() {
            SeedError::NotFound
        } else {
            SeedError::Other(e.to_string())
        }
    })?;
    serde_json::from_slice(&data).map_err(|e| {
        SeedError::Corrupted(format!("{e}: the cluster seed file is corrupted"))
    })
}

/// `writeSeedFile`.
async fn write_seed_file(bucket: &BucketClient, seed: &ClusterSeed) -> Result<(), String> {
    let data = serde_json::to_vec(seed).map_err(|e| e.to_string())?;
    bucket
        .upload(CLUSTER_SEED_FILE_NAME, data)
        .await
        .map_err(|e| e.to_string())
}

/// `util.DurationWithJitter(d, variance)`: d ± d*variance.
fn duration_with_jitter(d: Duration, variance: f64) -> Duration {
    let v = d.as_secs_f64() * variance;
    if v <= 0.0 {
        return d;
    }
    let delta = rand::thread_rng().gen_range(-v..v);
    Duration::from_secs_f64((d.as_secs_f64() + delta).max(0.0))
}

/// `util.DurationWithPositiveJitter(d, variance)`: d + [0, d*variance).
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

/// `waitSeedFileStability`.
async fn wait_seed_file_stability(
    ctx: &ServiceContext,
    bucket: &BucketClient,
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
            Err(e) => {
                warn!(err = %e, "failed to read cluster seed file from object storage");
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
        info!(cluster_id = %seed.uid, created_at = %format_rfc3339_nano_utc(seed.created_at),
            "found a cluster seed file created recently, waiting until stable");
        sleep_or_cancel(ctx, duration_with_positive_jitter(min_stability - created_ago, 0.2)).await?;
    }
    Err(SeedError::Canceled)
}

/// `initSeedFile`: waits until a stable seed file exists, creating one if there is none.
async fn init_seed_file(
    ctx: &ServiceContext,
    bucket: &BucketClient,
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
        // Wait between 0s and minStability.
        sleep_or_cancel(ctx, duration_with_jitter(min_stability / 2, 1.0)).await?;

        if let Ok(seed) = read_seed_file(bucket).await {
            debug!(cluster_id = %seed.uid, "skipping creation of cluster seed file because found one");
            tokio::select! {
                _ = ctx.canceled() => return Err(SeedError::Canceled),
                _ = backoff.wait() => {}
            }
            continue;
        }

        let seed = new_cluster_seed();
        info!(cluster_id = %seed.uid, "creating cluster seed file");
        if let Err(err) = write_seed_file(bucket, &seed).await {
            warn!(%err, "failed to create cluster seed file");
        }
        tokio::select! {
            _ = ctx.canceled() => return Err(SeedError::Canceled),
            _ = backoff.wait() => {}
        }
    }
    Err(SeedError::Canceled)
}

/// `getNextReportAt`: `createdAt + ceil((now - createdAt) / interval) * interval`, i.e. the same
/// instant for all instances of the cluster.
pub fn get_next_report_at(interval: Duration, created_at: SystemTime, now: SystemTime) -> SystemTime {
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

/// `runtime.GOOS` / `runtime.GOARCH` naming.
fn go_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        other => other,
    }
}

/// Process memory statistics standing in for Go's `runtime.MemStats` (mimir-rs has no garbage
/// collector: GC fields are reported as 0, memory fields from the process RSS/VSZ).
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

/// `buildReport`.
pub fn build_report(seed: &ClusterSeed, report_at: SystemTime, interval: Duration) -> Report {
    let mut metrics = serde_json::Map::new();
    metrics.insert("memstats".to_string(), build_memstats());
    let tasks = tokio::runtime::Handle::try_current()
        .map(|h| h.metrics().num_alive_tasks())
        .unwrap_or(0);
    metrics.insert("num_goroutine".to_string(), serde_json::Value::from(tasks));
    for (k, v) in usagestats::collect_metrics() {
        metrics.insert(k, v);
    }
    // `defer cpuUsage.Set(0)`.
    usagestats::get_float("cpu_usage").set(0.0);
    Report {
        cluster_id: seed.uid.clone(),
        created_at: seed.created_at,
        interval: report_at,
        interval_period: interval.as_secs_f64(),
        target: usagestats::string_value(usagestats::TARGET_KEY).unwrap_or_default(),
        version: PrometheusVersion {
            version: version_str().to_string(),
            revision: revision_str().to_string(),
            branch: branch_str().to_string(),
            build_user: String::new(),
            build_date: String::new(),
            go_version: go_version_str(),
        },
        os: go_os().to_string(),
        arch: go_arch().to_string(),
        edition: usagestats::string_value(usagestats::EDITION_KEY).unwrap_or_default(),
        mode: usagestats::string_value(usagestats::MODE_KEY).unwrap_or_default(),
        metrics,
    }
}

/// CPU usage of this process in percent, averaged since the process started
/// (gopsutil `Process.CPUPercent`).
fn process_cpu_percent() -> Option<f64> {
    const CLK_TCK: f64 = 100.0;
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the parenthesised command name.
    let rest = &stat[stat.rfind(')')? + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // utime = field 14, stime = 15, starttime = 22 (1-based in proc(5)); rest starts at field 3.
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    let start_ticks: f64 = fields.get(19)?.parse().ok()?;
    let uptime: f64 = std::fs::read_to_string("/proc/uptime")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    let alive = uptime - start_ticks / CLK_TCK;
    if alive <= 0.0 {
        return None;
    }
    Some(100.0 * ((utime + stime) / CLK_TCK) / alive)
}

/// Reporter configuration (Go keeps these as unexported fields so tests can shorten them).
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

/// `usagestats.Reporter`.
pub struct Reporter {
    cfg: ReporterConfig,
    bucket: BucketClient,
    client: reqwest::Client,
    requests_total: Counter,
    requests_failed_total: Counter,
    requests_latency: Histogram,
}

impl Reporter {
    /// `NewReporter`: `bucket` is the blocks storage bucket (the seed lives under
    /// `__mimir_cluster/`).
    pub fn new(cfg: ReporterConfig, bucket: &BucketClient, reg: Option<&Registry>) -> Arc<Self> {
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
            cfg,
            bucket: bucket.with_prefix(MIMIR_INTERNALS_PREFIX),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            requests_total,
            requests_failed_total,
            requests_latency,
        })
    }

    /// `sendReport`.
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
        let data = serde_json::to_vec(report).map_err(|e| format!("marshal the report: {e}"))?;
        let resp = self
            .client
            .post(&self.cfg.server_url)
            .header("Content-Type", "application/json")
            .body(data)
            .send()
            .await
            .map_err(|e| format!("send the report to the stats server: {e}"))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("read the response from the stats server: {e}"))?;
        if !status.is_success() {
            let shown = &body[..body.len().min(128)];
            return Err(format!(
                "received status code: {status} and body: {:?}",
                String::from_utf8_lossy(shown)
            ));
        }
        Ok(())
    }

    /// `running`.
    async fn running(self: Arc<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let seed = match init_seed_file(&ctx, &self.bucket, self.cfg.seed_file_min_stability).await {
            Ok(seed) => seed,
            Err(SeedError::Canceled) => return Ok(()),
            Err(e) => return Err(ServiceError::failure(e.to_string())),
        };
        info!(cluster_id = %seed.uid, "usage stats reporter initialized");

        let interval = self.cfg.report_send_interval;
        let mut next_report_at = get_next_report_at(interval, seed.created_at, SystemTime::now());
        let mut next_report: Option<Report> = None;

        let mut check = tokio::time::interval(self.cfg.report_check_interval);
        check.tick().await;
        let mut cpu = tokio::time::interval(self.cfg.cpu_usage_sample_interval);
        cpu.tick().await;
        let cpu_usage = usagestats::get_and_reset_float("cpu_usage");

        loop {
            tokio::select! {
                _ = ctx.canceled() => return Ok(()),
                _ = cpu.tick() => {
                    if let Some(p) = process_cpu_percent() {
                        if cpu_usage.value() < p {
                            cpu_usage.set(p);
                        }
                    }
                }
                _ = check.tick() => {
                    let now = SystemTime::now();
                    if now < next_report_at {
                        continue;
                    }
                    if now.duration_since(next_report_at).unwrap_or_default() >= interval {
                        next_report_at = get_next_report_at(interval, seed.created_at, now);
                        next_report = None;
                        info!(next_report_at = %format_rfc3339_nano_utc(next_report_at),
                            "failed to send anonymous usage stats report for too long, skipping to next report");
                        continue;
                    }
                    let report = next_report.get_or_insert_with(|| build_report(&seed, next_report_at, interval));
                    debug!("sending anonymous usage stats report");
                    if let Err(err) = self.send_report(report).await {
                        info!(%err, "failed to send anonymous usage stats report");
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

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream: pkg/usagestats/reporter_test.go::TestGetNextReportAt
    #[test]
    fn test_get_next_report_at() {
        let at = |ns: u64| UNIX_EPOCH + Duration::from_nanos(ns);
        const H: u64 = 3_600_000_000_000;
        const M: u64 = 60_000_000_000;
        const MS: u64 = 1_000_000;
        let hour = Duration::from_secs(3600);
        // createdAt aligned with interval and now
        assert_eq!(get_next_report_at(hour, at(H), at(2 * H)), at(2 * H));
        // createdAt aligned with interval
        assert_eq!(get_next_report_at(hour, at(H), at(2 * H + 1)), at(3 * H));
        // createdAt not aligned
        assert_eq!(
            get_next_report_at(hour, at(H + 18 * M + 20 * MS), at(2 * H + 1)),
            at(2 * H + 18 * M + 20 * MS)
        );
    }

    #[test]
    fn test_parse_go_time() {
        let a = parse_rfc3339("2026-11-17T00:24:13.728715478+01:00").unwrap();
        let b = parse_rfc3339("2026-11-16T23:24:13.728715478Z").unwrap();
        assert_eq!(a, b);
        assert_eq!(format_rfc3339_nano_utc(b), "2026-11-16T23:24:13.728715478Z");
    }

    // Upstream: pkg/usagestats/seed_test.go (seed creation, reading an existing seed).
    #[tokio::test]
    async fn test_init_seed_file_creates_and_reuses_seed() {
        let bucket = BucketClient::new_in_memory("seed");
        let ctx = ServiceContext::background();
        let seed = tokio::time::timeout(
            Duration::from_secs(10),
            init_seed_file(&ctx, &bucket, Duration::ZERO),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(seed.uid.len(), 36);
        let again = init_seed_file(&ctx, &bucket, Duration::ZERO).await.unwrap();
        assert_eq!(seed.uid, again.uid);

        // Go-written seed file.
        let go_bucket = BucketClient::new_in_memory("go");
        go_bucket
            .upload(
                CLUSTER_SEED_FILE_NAME,
                br#"{"UID":"3a7a4c2c-5c2b-4a6a-9d31-1d9f1b2b9f10","created_at":"2024-01-01T10:00:00.123456789+02:00"}"#.to_vec(),
            )
            .await
            .unwrap();
        let s = read_seed_file(&go_bucket).await.unwrap();
        assert_eq!(s.uid, "3a7a4c2c-5c2b-4a6a-9d31-1d9f1b2b9f10");
        assert_eq!(format_rfc3339_nano_utc(s.created_at), "2024-01-01T08:00:00.123456789Z");
    }

    #[tokio::test]
    async fn test_send_report_shape() {
        use std::sync::Mutex;
        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::clone(&received);
        let app = axum::Router::new().route(
            "/report",
            axum::routing::post(move |body: axum::body::Bytes| {
                let rec = Arc::clone(&rec);
                async move {
                    rec.lock().unwrap().push(serde_json::from_slice(&body).unwrap());
                    "ok"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let reg = Registry::new();
        let r = Reporter::new(
            ReporterConfig {
                server_url: format!("http://{addr}/report"),
                ..ReporterConfig::default()
            },
            &BucketClient::new_in_memory("b"),
            Some(&reg),
        );
        usagestats::set_target("ingester");
        usagestats::get_string("installation_mode").set("helm");
        let seed = new_cluster_seed();
        let report = build_report(&seed, SystemTime::now(), DEFAULT_REPORT_SEND_INTERVAL);
        r.send_report(&report).await.unwrap();
        let got = received.lock().unwrap().pop().unwrap();
        assert_eq!(got["clusterID"], serde_json::json!(seed.uid));
        assert_eq!(got["target"], "ingester");
        assert_eq!(got["edition"], "oss");
        assert_eq!(got["intervalPeriod"], serde_json::json!(14400.0));
        assert_eq!(got["metrics"]["installation_mode"], "helm");
        assert!(got["metrics"]["memstats"].is_object());
        assert!(got["version"]["goVersion"].is_string());
        assert_eq!(r.requests_total.get(), 1.0);
        assert_eq!(r.requests_failed_total.get(), 0.0);
    }
}
