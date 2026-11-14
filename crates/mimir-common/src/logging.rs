//! Structured logfmt & JSON logging, unsafe-char sanitization, and rate-limited loggers.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/log/log.go
//! - github.com/grafana/mimir/pkg/util/log/rate_limit.go
//! - github.com/grafana/mimir/pkg/util/log/wrappers.go
//! - github.com/grafana/dskit/log/gokit.go
//! - github.com/grafana/dskit/log/level.go
//! - github.com/grafana/dskit/log/ratelimit.go
//! - github.com/grafana/dskit/log/sanitize.go

use std::borrow::Cow;
use std::fmt::Write as _;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use prometheus::{Counter, CounterVec, Opts, Registry};
use serde_json::{Map, Value};

use crate::limiter::TokenBucket;
use crate::tenant::join_tenant_ids;

/// Output log format (`dskit/log`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Logfmt,
    Json,
}

impl LogFormat {
    pub const LOGFMT_STR: &'static str = "logfmt";
    pub const JSON_STR: &'static str = "json";

    pub fn parse(s: &str) -> Self {
        if s == Self::JSON_STR {
            Self::Json
        } else {
            Self::Logfmt
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Logfmt => Self::LOGFMT_STR,
            Self::Json => Self::JSON_STR,
        }
    }
}

/// Minimum log level (`dskit/log/level.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum LogLevel {
    Debug = 0,
    #[default]
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl LogLevel {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(format!("unrecognized log level {other:?}")),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Formats a [`SystemTime`] as UTC RFC3339 (`YYYY-MM-DDTHH:MM:SSZ`), matching Go `time.RFC3339`.
pub fn format_rfc3339_utc(t: SystemTime) -> String {
    let (secs, _nanos) = system_time_to_unix(t);
    format_unix_rfc3339(secs, None)
}

/// Formats a [`SystemTime`] as UTC RFC3339Nano, matching Go `time.RFC3339Nano`
/// (trailing fractional zeros omitted, no decimal point when nanoseconds == 0).
pub fn format_rfc3339_nano_utc(t: SystemTime) -> String {
    let (secs, nanos) = system_time_to_unix(t);
    format_unix_rfc3339(secs, Some(nanos))
}

fn system_time_to_unix(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(e) => {
            let d = e.duration();
            let secs = d.as_secs() as i64;
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                (-secs, 0)
            } else {
                (-secs - 1, 1_000_000_000 - nanos)
            }
        }
    }
}

fn format_unix_rfc3339(unix_secs: i64, subsec_nanos: Option<u32>) -> String {
    let days = unix_secs.div_euclid(86_400);
    let secs_of_day = unix_secs.rem_euclid(86_400);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;

    // Howard Hinnant's civil_from_days algorithm (exact proleptic Gregorian).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let mut out = String::with_capacity(32);
    let _ = write!(
        out,
        "{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}"
    );
    if let Some(ns) = subsec_nanos {
        if ns != 0 {
            let mut frac = format!("{ns:09}");
            while frac.ends_with('0') {
                frac.pop();
            }
            out.push('.');
            out.push_str(&frac);
        }
    }
    out.push('Z');
    out
}

/// Formats key-value pairs as a `logfmt` line ending with `\n`.
pub fn format_logfmt_line(kvs: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(kvs.len() * 24 + 2);
    for (idx, &(k, v)) in kvs.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        write_logfmt_key(&mut out, k);
        out.push('=');
        write_logfmt_val(&mut out, v);
    }
    out.push('\n');
    out
}

fn write_logfmt_key(out: &mut String, k: &str) {
    if k.is_empty()
        || k.bytes()
            .any(|b| b <= b' ' || b == b'=' || b == b'"' || b == 0x7f)
    {
        write_quoted(out, k);
    } else {
        out.push_str(k);
    }
}

fn write_logfmt_val(out: &mut String, v: &str) {
    if v.is_empty()
        || v.bytes()
            .any(|b| b <= b' ' || b == b'=' || b == b'"' || b == 0x7f)
    {
        write_quoted(out, v);
    } else {
        out.push_str(v);
    }
}

fn write_quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || (c as u32) == 0x7f => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Formats key-value pairs as a JSON object line ending with `\n`.
pub fn format_json_line(kvs: &[(&str, &str)]) -> String {
    let mut map = Map::with_capacity(kvs.len());
    for &(k, v) in kvs {
        map.insert(k.to_string(), Value::String(v.to_string()));
    }
    let mut s = serde_json::to_string(&Value::Object(map)).unwrap_or_else(|_| "{}".to_string());
    s.push('\n');
    s
}

/// Removes Unicode control (`Cc`, except `\t`), formatting (`Cf`), line separator (`Zl`),
/// and paragraph separator (`Zp`) characters (`dskit/log.DropUnsafeChars`).
pub fn drop_unsafe_chars(s: &str) -> Cow<'_, str> {
    if !needs_sanitization(s) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if !is_unsafe_char(c) {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

/// Replaces unsafe control/formatting characters with printable `\xNN` / `\uNNNN` / `\UNNNNNNNN`
/// escape sequences (`dskit/log.EscapeUnsafeChars`).
pub fn escape_unsafe_chars(s: &str) -> Cow<'_, str> {
    if !needs_sanitization(s) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if is_unsafe_char(c) {
            let code = c as u32;
            if code < 0x100 {
                let _ = write!(out, "\\x{code:02x}");
            } else if code < 0x10000 {
                let _ = write!(out, "\\u{code:04x}");
            } else {
                let _ = write!(out, "\\U{code:08x}");
            }
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

#[inline]
fn is_unsafe_char(c: char) -> bool {
    if c == '\t' {
        return false;
    }
    // Cc (C0/C1 control), Cf (format), Zl (U+2028), Zp (U+2029)
    c.is_control()
        || matches!(
            c as u32,
            0x200B..=0x200F
                | 0x202A..=0x202E
                | 0x2028
                | 0x2029
                | 0x2060..=0x206F
                | 0xFEFF
                | 0xFFF9..=0xFFFB
        )
}

#[inline]
fn needs_sanitization(s: &str) -> bool {
    for &b in s.as_bytes() {
        if b == b'\t' || (0x20..0x7f).contains(&b) {
            continue;
        }
        if b < 0x80 {
            return true;
        }
        return s.chars().any(is_unsafe_char);
    }
    false
}

/// Logger trait for structured key-value logging.
pub trait Logger: Send + Sync {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]);
}

/// Structured logger writing `logfmt` or `json` lines filtered by [`LogLevel`].
pub struct StructuredLogger {
    format: LogFormat,
    min_level: LogLevel,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    default_kvs: Vec<(String, String)>,
    clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl StructuredLogger {
    pub fn new(
        format: LogFormat,
        min_level: LogLevel,
        writer: Box<dyn Write + Send>,
    ) -> Self {
        Self {
            format,
            min_level,
            writer: Arc::new(Mutex::new(writer)),
            default_kvs: Vec::new(),
            clock: Arc::new(SystemTime::now),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_fields(&self, kvs: &[(&str, &str)]) -> Self {
        let mut default_kvs = self.default_kvs.clone();
        for &(k, v) in kvs {
            default_kvs.push((k.to_string(), v.to_string()));
        }
        Self {
            format: self.format,
            min_level: self.min_level,
            writer: self.writer.clone(),
            default_kvs,
            clock: self.clock.clone(),
        }
    }

    pub fn with_user_id(&self, user_id: &str) -> Self {
        self.with_fields(&[("user", user_id)])
    }

    pub fn with_user_ids(&self, user_ids: &[String]) -> Self {
        let joined = join_tenant_ids(user_ids);
        self.with_fields(&[("user", &joined)])
    }

    pub fn with_trace_id(&self, trace_id: &str) -> Self {
        self.with_fields(&[("traceID", trace_id)])
    }

    pub fn with_source_ips(&self, source_ips: &str) -> Self {
        self.with_fields(&[("sourceIPs", source_ips)])
    }
}

impl Logger for StructuredLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        if level < self.min_level {
            return;
        }
        let ts = format_rfc3339_nano_utc((self.clock)());
        let mut all: Vec<(&str, &str)> = Vec::with_capacity(2 + self.default_kvs.len() + kvs.len());
        all.push(("ts", &ts));
        all.push(("level", level.as_str()));
        for (k, v) in &self.default_kvs {
            all.push((k.as_str(), v.as_str()));
        }
        all.extend_from_slice(kvs);

        let line = match self.format {
            LogFormat::Logfmt => format_logfmt_line(&all),
            LogFormat::Json => format_json_line(&all),
        };
        let mut w = self.writer.lock();
        let _ = w.write_all(line.as_bytes());
    }
}

/// Token-bucket rate-limited logger (`dskit/log/ratelimit.go`), tracking discarded lines
/// via `logger_rate_limit_discarded_log_lines_total{level=...}`.
pub struct TokenBucketRateLimitedLogger {
    next: Arc<dyn Logger>,
    limiter: TokenBucket,
    discarded_debug: Counter,
    discarded_info: Counter,
    discarded_warn: Counter,
    discarded_error: Counter,
}

impl TokenBucketRateLimitedLogger {
    pub fn new(
        next: Arc<dyn Logger>,
        logs_per_second: f64,
        logs_burst_size: usize,
        registry: Option<&Registry>,
    ) -> Self {
        let counter_vec = CounterVec::new(
            Opts::new(
                "logger_rate_limit_discarded_log_lines_total",
                "Total number of discarded log lines per level.",
            ),
            &["level"],
        )
        .expect("valid metric opts");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(counter_vec.clone()));
        }

        Self {
            next,
            limiter: TokenBucket::new(logs_per_second, logs_burst_size),
            discarded_debug: counter_vec.with_label_values(&["debug"]),
            discarded_info: counter_vec.with_label_values(&["info"]),
            discarded_warn: counter_vec.with_label_values(&["warn"]),
            discarded_error: counter_vec.with_label_values(&["error"]),
        }
    }

    pub fn log_at(&self, now: Instant, level: LogLevel, kvs: &[(&str, &str)]) {
        if self.limiter.allow_n(now, 1) {
            self.next.log(level, kvs);
        } else {
            match level {
                LogLevel::Debug => self.discarded_debug.inc(),
                LogLevel::Info => self.discarded_info.inc(),
                LogLevel::Warn => self.discarded_warn.inc(),
                LogLevel::Error => self.discarded_error.inc(),
            }
        }
    }
}

impl Logger for TokenBucketRateLimitedLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        self.log_at(Instant::now(), level, kvs);
    }
}

/// Interval-based rate-limited logger (`pkg/util/log/rate_limit.go`) permitting at most
/// one log invocation per `interval`.
pub struct IntervalRateLimitedLogger {
    interval_nanos: u64,
    next_log_nanos: AtomicU64,
    origin: Instant,
    delegate: Arc<dyn Logger>,
}

impl IntervalRateLimitedLogger {
    pub fn new(interval: Duration, delegate: Arc<dyn Logger>) -> Self {
        Self {
            interval_nanos: interval.as_nanos().min(u64::MAX as u128) as u64,
            next_log_nanos: AtomicU64::new(0),
            origin: Instant::now(),
            delegate,
        }
    }

    pub fn log_at(&self, now: Instant, level: LogLevel, kvs: &[(&str, &str)]) {
        let now_nanos = now.saturating_duration_since(self.origin).as_nanos() as u64;
        let next = self.next_log_nanos.load(Ordering::Acquire);
        if now_nanos <= next {
            return;
        }
        if self
            .next_log_nanos
            .compare_exchange(
                next,
                now_nanos.saturating_add(self.interval_nanos),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.delegate.log(level, kvs);
        }
    }
}

impl Logger for IntervalRateLimitedLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        self.log_at(Instant::now(), level, kvs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SharedBuf(Arc<Mutex<Vec<u8>>>);
    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_logfmt_and_json_formatting() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let logger = StructuredLogger::new(
            LogFormat::Logfmt,
            LogLevel::Info,
            Box::new(SharedBuf(buf.clone())),
        )
        .with_clock(Arc::new(|| UNIX_EPOCH))
        .with_user_id("tenant-1");

        logger.log(LogLevel::Debug, &[("msg", "ignored")]);
        logger.log(LogLevel::Info, &[("msg", "hello world"), ("count", "42")]);

        let out = String::from_utf8(buf.lock().clone()).unwrap();
        assert_eq!(
            out,
            "ts=1970-01-01T00:00:00Z level=info user=tenant-1 msg=\"hello world\" count=42\n"
        );

        let jbuf = Arc::new(Mutex::new(Vec::new()));
        let jlogger = StructuredLogger::new(
            LogFormat::Json,
            LogLevel::Info,
            Box::new(SharedBuf(jbuf.clone())),
        )
        .with_clock(Arc::new(|| UNIX_EPOCH));
        jlogger.log(LogLevel::Warn, &[("msg", "oops")]);
        let jout = String::from_utf8(jbuf.lock().clone()).unwrap();
        let parsed: Value = serde_json::from_str(jout.trim()).unwrap();
        assert_eq!(parsed["level"], "warn");
        assert_eq!(parsed["msg"], "oops");
        assert_eq!(parsed["ts"], "1970-01-01T00:00:00Z");
    }

    #[test]
    fn test_sanitize_and_rate_limited_logger() {
        assert_eq!(drop_unsafe_chars("abc\ndef\tghi"), "abcdef\tghi");
        assert_eq!(escape_unsafe_chars("abc\ndef"), "abc\\x0adef");

        let buf = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(
            StructuredLogger::new(
                LogFormat::Logfmt,
                LogLevel::Info,
                Box::new(SharedBuf(buf.clone())),
            )
            .with_clock(Arc::new(|| UNIX_EPOCH)),
        );
        let reg = Registry::new();
        let rl = TokenBucketRateLimitedLogger::new(inner, 1.0, 2, Some(&reg));
        let t0 = Instant::now();
        rl.log_at(t0, LogLevel::Info, &[("msg", "1")]);
        rl.log_at(t0, LogLevel::Info, &[("msg", "2")]);
        rl.log_at(t0, LogLevel::Info, &[("msg", "3")]);
        let out = String::from_utf8(buf.lock().clone()).unwrap();
        assert_eq!(out.lines().count(), 2);
        assert_eq!(rl.discarded_info.get(), 1.0);
    }
}
