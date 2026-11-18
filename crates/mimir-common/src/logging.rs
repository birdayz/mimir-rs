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
//! - github.com/go-kit/log/logfmt_logger.go, json_logger.go, value.go (DefaultTimestampUTC, Caller)
//! - github.com/go-logfmt/logfmt/encode.go, jsonstring.go
//!
//! [`LogSink`] + [`init_global_logger`] port `util_log.InitLogger`; [`TracingLogBridge`] routes
//! the Rust port's `tracing` events (Go's `level.X(logger).Log(...)` calls) into the same sink.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use prometheus::{Counter, CounterVec, Opts, Registry};

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

/// A go-kit keyval value.
///
/// Strings follow go-logfmt string rules; integers/bools/floats render like Go `fmt.Sprint` in
/// logfmt and as JSON numbers/bools in the JSON format (go-kit `NewJSONLogger` marshals the
/// native Go value).
#[derive(Debug, Clone, PartialEq)]
pub enum LogValue<'a> {
    Str(Cow<'a, str>),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    F64(f64),
    Bool(bool),
}

impl<'a> From<&'a str> for LogValue<'a> {
    fn from(s: &'a str) -> Self {
        LogValue::Str(Cow::Borrowed(s))
    }
}

impl From<String> for LogValue<'static> {
    fn from(s: String) -> Self {
        LogValue::Str(Cow::Owned(s))
    }
}

impl From<i64> for LogValue<'static> {
    fn from(v: i64) -> Self {
        LogValue::I64(v)
    }
}

impl From<u64> for LogValue<'static> {
    fn from(v: u64) -> Self {
        LogValue::U64(v)
    }
}

impl From<f64> for LogValue<'static> {
    fn from(v: f64) -> Self {
        LogValue::F64(v)
    }
}

impl From<bool> for LogValue<'static> {
    fn from(v: bool) -> Self {
        LogValue::Bool(v)
    }
}

/// Formats key-value pairs as a `logfmt` line ending with `\n` (go-kit `NewLogfmtLogger`).
///
/// Returns an empty string when a key is invalid (empty after go-logfmt's key rune filter):
/// go-kit drops such records entirely.
pub fn format_logfmt_line(kvs: &[(&str, &str)]) -> String {
    let pairs: Vec<(&str, LogValue<'_>)> = kvs.iter().map(|&(k, v)| (k, v.into())).collect();
    let mut out = String::with_capacity(kvs.len() * 24 + 2);
    encode_logfmt_record(&mut out, &pairs);
    out
}

/// Appends a logfmt record (go-logfmt `Encoder.EncodeKeyvals` + `EndRecord`) to `out`.
///
/// Returns `false` and leaves `out` unchanged when a key is invalid, mirroring go-kit's
/// `logfmtLogger.Log` which writes nothing when encoding fails.
pub fn encode_logfmt_record(out: &mut String, kvs: &[(&str, LogValue<'_>)]) -> bool {
    let refs: Vec<(&str, &LogValue<'_>)> = kvs.iter().map(|(k, v)| (*k, v)).collect();
    encode_logfmt_refs(out, &refs)
}

fn encode_logfmt_refs(out: &mut String, kvs: &[(&str, &LogValue<'_>)]) -> bool {
    let start = out.len();
    for (idx, &(k, v)) in kvs.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        // go-logfmt writeStringKey: invalid runes are dropped; an empty result is ErrInvalidKey.
        let key_start = out.len();
        out.extend(k.chars().filter(|&c| !needs_quoted_logfmt_rune(c)));
        if out.len() == key_start {
            out.truncate(start);
            return false;
        }
        out.push('=');
        match v {
            LogValue::Str(s) => write_logfmt_string_value(out, s),
            LogValue::I64(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::U64(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::I128(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::U128(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::F64(f) => out.push_str(&go_format_float_v(*f)),
            LogValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        }
    }
    out.push('\n');
    true
}

/// go-logfmt `needsQuotedValueRune` / `keyRuneFilter` (`utf8.RuneError` == U+FFFD).
#[inline]
fn needs_quoted_logfmt_rune(c: char) -> bool {
    c <= ' ' || c == '=' || c == '"' || c == '\u{7f}' || c == '\u{fffd}'
}

/// go-logfmt `writeStringValue(w, value, true)`.
fn write_logfmt_string_value(out: &mut String, s: &str) {
    if s == "null" {
        out.push_str("\"null\"");
    } else if s.chars().any(needs_quoted_logfmt_rune) {
        write_quoted(out, s);
    } else {
        out.push_str(s);
    }
}

/// go-logfmt `writeQuotedString`.
fn write_quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{fffd}' => out.push_str("\\ufffd"),
            c if (c as u32) < 0x20 || (c as u32) == 0x7f => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Formats key-value pairs as a JSON object line ending with `\n` (go-kit `NewJSONLogger`).
pub fn format_json_line(kvs: &[(&str, &str)]) -> String {
    let pairs: Vec<(&str, LogValue<'_>)> = kvs.iter().map(|&(k, v)| (k, v.into())).collect();
    let mut out = String::with_capacity(kvs.len() * 24 + 4);
    encode_json_record(&mut out, &pairs);
    out
}

/// Appends a JSON record to `out`, matching go-kit's `jsonLogger.Log`: keyvals are merged into a
/// Go map (later duplicate keys win), encoded by `encoding/json` with sorted keys and
/// `SetEscapeHTML(false)`, followed by `\n`.
///
/// Returns `false` and leaves `out` unchanged when Go's encoder would fail (NaN/±Inf floats).
pub fn encode_json_record(out: &mut String, kvs: &[(&str, LogValue<'_>)]) -> bool {
    let refs: Vec<(&str, &LogValue<'_>)> = kvs.iter().map(|(k, v)| (*k, v)).collect();
    encode_json_refs(out, &refs)
}

fn encode_json_refs(out: &mut String, kvs: &[(&str, &LogValue<'_>)]) -> bool {
    let mut map: BTreeMap<&str, &LogValue<'_>> = BTreeMap::new();
    for &(k, v) in kvs {
        map.insert(k, v);
    }
    let start = out.len();
    out.push('{');
    for (idx, (k, v)) in map.into_iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        write_json_string(out, k);
        out.push(':');
        match v {
            LogValue::Str(s) => write_json_string(out, s),
            LogValue::I64(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::U64(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::I128(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::U128(n) => {
                let _ = write!(out, "{n}");
            }
            LogValue::F64(f) => match go_format_float_json(*f) {
                Some(s) => out.push_str(&s),
                None => {
                    out.truncate(start);
                    return false;
                }
            },
            LogValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        }
    }
    out.push_str("}\n");
    true
}

/// Go `encoding/json` string encoding with `SetEscapeHTML(false)`.
fn write_json_string(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                let b = c as u32 as usize;
                out.push_str("\\u00");
                out.push(HEX[b >> 4] as char);
                out.push(HEX[b & 0xf] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Shortest round-trip decimal digits and base-10 exponent of the first digit (`f` finite, > 0).
fn shortest_digits(f: f64) -> (String, i32) {
    let s = format!("{f:e}");
    let (mant, exp) = s.split_once('e').unwrap_or((s.as_str(), "0"));
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    (digits, exp.parse().unwrap_or(0))
}

fn push_go_exp_format(out: &mut String, digits: &str, exp: i32) {
    out.push_str(&digits[..1]);
    if digits.len() > 1 {
        out.push('.');
        out.push_str(&digits[1..]);
    }
    out.push('e');
    out.push(if exp < 0 { '-' } else { '+' });
    let a = exp.unsigned_abs();
    if a < 10 {
        out.push('0');
    }
    let _ = write!(out, "{a}");
}

fn push_go_fixed_format(out: &mut String, digits: &str, exp: i32) {
    if exp >= 0 {
        let int_len = exp as usize + 1;
        if digits.len() <= int_len {
            out.push_str(digits);
            out.extend(std::iter::repeat_n('0', int_len - digits.len()));
        } else {
            out.push_str(&digits[..int_len]);
            out.push('.');
            out.push_str(&digits[int_len..]);
        }
    } else {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-exp - 1) as usize));
        out.push_str(digits);
    }
}

/// Formats a float like Go `fmt.Sprint(float64)` (`%v` == `strconv.FormatFloat(f, 'g', -1, 64)`).
pub fn go_format_float_v(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    let mut out = String::new();
    if f.is_sign_negative() {
        out.push('-');
    }
    if f == 0.0 {
        out.push('0');
        return out;
    }
    let (digits, exp) = shortest_digits(f.abs());
    // strconv %g with shortest precision uses eprec = 6.
    if !(-4..6).contains(&exp) {
        push_go_exp_format(&mut out, &digits, exp);
    } else {
        push_go_fixed_format(&mut out, &digits, exp);
    }
    out
}

/// Formats a float like Go `encoding/json` (`floatEncoder`); `None` for NaN/±Inf, which Go
/// refuses to encode.
pub fn go_format_float_json(f: f64) -> Option<String> {
    if !f.is_finite() {
        return None;
    }
    let mut out = String::new();
    if f.is_sign_negative() {
        out.push('-');
    }
    if f == 0.0 {
        out.push('0');
        return Some(out);
    }
    let abs = f.abs();
    let (digits, exp) = shortest_digits(abs);
    if !(1e-6..1e21).contains(&abs) {
        push_go_exp_format(&mut out, &digits, exp);
        // clean up e-09 to e-9
        let n = out.len();
        if n >= 4 && out.as_bytes()[n - 4] == b'e' && &out[n - 3..n - 1] == "-0" {
            out.remove(n - 2);
        }
    } else {
        push_go_fixed_format(&mut out, &digits, exp);
    }
    Some(out)
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

impl LogValue<'_> {
    /// Renders this value as its go-logfmt / `fmt.Sprint` string representation.
    pub fn to_logfmt_string(&self) -> String {
        match self {
            LogValue::Str(s) => s.to_string(),
            LogValue::I64(n) => n.to_string(),
            LogValue::U64(n) => n.to_string(),
            LogValue::I128(n) => n.to_string(),
            LogValue::U128(n) => n.to_string(),
            LogValue::F64(f) => go_format_float_v(*f),
            LogValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        }
    }
}

/// Logger trait for structured key-value logging.
pub trait Logger: Send + Sync {
    /// `#[track_caller]` so implementations that render go-kit's `caller` key (e.g. [`LogSink`])
    /// report the call site, like dskit's `spanlogger.Caller(n)`.
    #[track_caller]
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]);

    /// Logs typed key-value pairs (preserving native JSON booleans & numbers in JSON mode).
    #[track_caller]
    fn log_values(&self, level: LogLevel, kvs: &[(&str, LogValue<'_>)]) {
        let rendered: Vec<String> = kvs.iter().map(|(_, v)| v.to_logfmt_string()).collect();
        let pairs: Vec<(&str, &str)> = kvs
            .iter()
            .zip(rendered.iter())
            .map(|((k, _), s)| (*k, s.as_str()))
            .collect();
        self.log(level, &pairs);
    }
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
        let pairs: Vec<(&str, LogValue<'_>)> = kvs.iter().map(|&(k, v)| (k, v.into())).collect();
        self.log_values(level, &pairs);
    }

    fn log_values(&self, level: LogLevel, kvs: &[(&str, LogValue<'_>)]) {
        if level < self.min_level {
            return;
        }
        let ts = LogValue::Str(Cow::Owned(format_rfc3339_nano_utc((self.clock)())));
        let level_v = LogValue::Str(Cow::Borrowed(level.as_str()));
        let mut all: Vec<(&str, LogValue<'_>)> =
            Vec::with_capacity(2 + self.default_kvs.len() + kvs.len());
        all.push(("ts", ts));
        all.push(("level", level_v));
        for (k, v) in &self.default_kvs {
            all.push((k.as_str(), LogValue::Str(Cow::Borrowed(v.as_str()))));
        }
        all.extend(kvs.iter().map(|(k, v)| (*k, v.clone())));

        let mut line = String::with_capacity(128 + kvs.len() * 24);
        let ok = match self.format {
            LogFormat::Logfmt => encode_logfmt_record(&mut line, &all),
            LogFormat::Json => encode_json_record(&mut line, &all),
        };
        if ok {
            let mut w = self.writer.lock();
            let _ = w.write_all(line.as_bytes());
        }
    }
}

/// Process-wide log output, the equivalent of the logger chain built by Mimir's
/// `util_log.InitLogger` (`pkg/util/log/log.go`):
///
/// `levelFilter -> [dskit RateLimitedLogger] -> log.With(ts=DefaultTimestampUTC, caller=Caller(n))
/// -> NewLogfmtLogger | NewJSONLogger -> SyncWriter(os.Stderr)`.
///
/// Records are rendered as `ts`, `caller`, `level`, then the record's keyvals, each record as
/// exactly one `write_all` under one mutex, so lines from [`Logger::log`] callers and from
/// `tracing` events (via [`TracingLogBridge`]) never interleave and share one rate limiter.
pub struct LogSink {
    format: LogFormat,
    min_level: LogLevel,
    writer: Mutex<Box<dyn Write + Send>>,
    rate_limit: Option<SinkRateLimit>,
    clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

struct SinkRateLimit {
    limiter: TokenBucket,
    counter_vec: CounterVec,
    // Indexed by `LogLevel as usize`.
    discarded: [Counter; 4],
}

impl LogSink {
    pub fn new(format: LogFormat, min_level: LogLevel, writer: Box<dyn Write + Send>) -> Self {
        Self {
            format,
            min_level,
            writer: Mutex::new(writer),
            rate_limit: None,
            clock: Arc::new(SystemTime::now),
        }
    }

    /// Enables the dskit `RateLimitedLogger` (`-log.rate-limit-enabled`) with
    /// `-log.rate-limit-logs-per-second` / `-log.rate-limit-logs-burst-size`. The limiter sits
    /// after the level filter, so filtered-out lines never consume tokens.
    pub fn with_rate_limit(mut self, logs_per_second: f64, logs_burst_size: usize) -> Self {
        let counter_vec = CounterVec::new(
            Opts::new(
                "logger_rate_limit_discarded_log_lines_total",
                "Total number of discarded log lines per level.",
            ),
            &["level"],
        )
        .expect("valid metric opts");
        let discarded = [
            counter_vec.with_label_values(&["debug"]),
            counter_vec.with_label_values(&["info"]),
            counter_vec.with_label_values(&["warn"]),
            counter_vec.with_label_values(&["error"]),
        ];
        self.rate_limit = Some(SinkRateLimit {
            limiter: TokenBucket::new(logs_per_second, logs_burst_size),
            counter_vec,
            discarded,
        });
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// Registers `logger_rate_limit_discarded_log_lines_total` (only when rate limiting is
    /// enabled, as Go registers it in `NewRateLimitedLogger`).
    pub fn register_metrics(&self, registry: &Registry) {
        if let Some(rl) = &self.rate_limit {
            let _ = registry.register(Box::new(rl.counter_vec.clone()));
        }
    }

    pub fn format(&self) -> LogFormat {
        self.format
    }

    pub fn min_level(&self) -> LogLevel {
        self.min_level
    }

    /// Number of lines dropped by the rate limiter at `level`.
    pub fn discarded_lines(&self, level: LogLevel) -> f64 {
        self.rate_limit
            .as_ref()
            .map_or(0.0, |rl| rl.discarded[level as usize].get())
    }

    #[inline]
    pub fn enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level
    }

    /// Logs one record. `caller` is `(file, line)`; only the file's base name is printed.
    pub fn log_record(
        &self,
        level: LogLevel,
        caller: Option<(&str, u32)>,
        kvs: &[(&str, LogValue<'_>)],
    ) {
        self.log_record_at(Instant::now(), level, caller, kvs);
    }

    pub fn log_record_at(
        &self,
        now: Instant,
        level: LogLevel,
        caller: Option<(&str, u32)>,
        kvs: &[(&str, LogValue<'_>)],
    ) {
        if !self.enabled(level) {
            return;
        }
        if let Some(rl) = &self.rate_limit {
            if !rl.limiter.allow_n(now, 1) {
                rl.discarded[level as usize].inc();
                return;
            }
        }

        let ts = LogValue::Str(Cow::Owned(format_rfc3339_nano_utc((self.clock)())));
        let caller = caller.map(|(file, line)| {
            let base = file.rsplit(['/', '\\']).next().unwrap_or(file);
            LogValue::Str(Cow::Owned(format!("{base}:{line}")))
        });
        let level_v = LogValue::Str(Cow::Borrowed(level.as_str()));
        let mut all: Vec<(&str, &LogValue<'_>)> = Vec::with_capacity(3 + kvs.len());
        all.push(("ts", &ts));
        if let Some(c) = &caller {
            all.push(("caller", c));
        }
        all.push(("level", &level_v));
        all.extend(kvs.iter().map(|(k, v)| (*k, v)));

        let mut line = String::with_capacity(128 + kvs.len() * 24);
        let ok = match self.format {
            LogFormat::Logfmt => encode_logfmt_refs(&mut line, &all),
            LogFormat::Json => encode_json_refs(&mut line, &all),
        };
        if ok {
            let mut w = self.writer.lock();
            let _ = w.write_all(line.as_bytes());
        }
    }
}

impl Logger for LogSink {
    #[track_caller]
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        if !self.enabled(level) {
            return;
        }
        let loc = std::panic::Location::caller();
        let pairs: Vec<(&str, LogValue<'_>)> = kvs.iter().map(|&(k, v)| (k, v.into())).collect();
        self.log_record(level, Some((loc.file(), loc.line())), &pairs);
    }

    #[track_caller]
    fn log_values(&self, level: LogLevel, kvs: &[(&str, LogValue<'_>)]) {
        if !self.enabled(level) {
            return;
        }
        let loc = std::panic::Location::caller();
        self.log_record(level, Some((loc.file(), loc.line())), kvs);
    }
}

static GLOBAL_LOG_SINK: OnceLock<Arc<LogSink>> = OnceLock::new();

/// Installs `sink` as the process-wide logger (Go `util_log.InitLogger` setting
/// `util_log.Logger`) and as the global `tracing` subscriber via [`TracingLogBridge`].
///
/// The first call wins; later calls return the already-installed sink.
pub fn init_global_logger(sink: LogSink) -> Arc<LogSink> {
    let mut installed = false;
    let global = GLOBAL_LOG_SINK.get_or_init(|| {
        installed = true;
        Arc::new(sink)
    });
    if installed {
        let _ = tracing::subscriber::set_global_default(TracingLogBridge::new(Arc::clone(global)));
    }
    Arc::clone(global)
}

/// The sink installed by [`init_global_logger`], if any.
pub fn global_logger() -> Option<Arc<LogSink>> {
    GLOBAL_LOG_SINK.get().cloned()
}

/// Logs `level=error msg="error <location>" err=<err>` through the global logger and exits the
/// process with code 1 (`pkg/util/log/log.go` `CheckFatal`).
pub fn check_fatal(location: &str, err: &str) -> ! {
    let logger = global_logger().unwrap_or_else(|| {
        Arc::new(LogSink::new(
            LogFormat::Logfmt,
            LogLevel::Info,
            Box::new(std::io::stderr()),
        ))
    });
    let msg = format!("error {location}");
    if location.is_empty() {
        logger.log(LogLevel::Error, &[("err", err)]);
    } else {
        logger.log(LogLevel::Error, &[("msg", &msg), ("err", err)]);
    }
    std::process::exit(1);
}

/// Context keyvals that Go attaches with `log.With(logger, "component", ...)` to the logger of
/// a component, keyed by the `tracing` target (module path) prefix of its Rust port. The first
/// matching prefix wins.
const COMPONENT_CONTEXT: &[(&str, &[(&str, &str)])] = &[
    // dskit memberlist: NewTCPTransport(cfg, m.logger, ...) wraps the memberlist logger again.
    (
        "mimir_ring::kv::memberlist::tcp_transport",
        &[("component", "memberlist"), ("component", "memberlist TCPTransport")],
    ),
    // pkg/mimir/modules.go initMemberlistKV: log.With(util_log.Logger, "component", "memberlist").
    ("mimir_ring::kv::memberlist", &[("component", "memberlist")]),
];

fn component_context(target: &str) -> &'static [(&'static str, &'static str)] {
    COMPONENT_CONTEXT
        .iter()
        .find(|(prefix, _)| target.starts_with(prefix))
        .map_or(&[], |(_, kvs)| kvs)
}

/// `tracing` subscriber rendering every event through a [`LogSink`] in go-kit format.
///
/// - Level filtering is static: `register_callsite` returns `Interest::never()` for disabled
///   callsites and `max_level_hint` lowers tracing's global max level, so disabled
///   `debug!`/`trace!` calls cost one relaxed atomic load.
/// - `trace!` events are never logged (go-kit has no trace level).
/// - Third-party crates (targets not starting with `mimir`) are only logged at `warn`/`error`:
///   Go Mimir does not log its dependencies' debug/info chatter.
/// - Spans are disabled; only events are rendered.
/// - Field mapping: the `message` field becomes `msg` and is placed right after `level`; other
///   fields follow in declaration order. An event without a `message` but with an explicit `msg`
///   field keeps its declaration order, so call sites can reproduce Go key orders like
///   `level=info component=x msg=...`. Integers/bools/floats keep their type (JSON numbers).
pub struct TracingLogBridge {
    sink: Arc<LogSink>,
}

impl TracingLogBridge {
    pub fn new(sink: Arc<LogSink>) -> Self {
        Self { sink }
    }

    #[inline]
    fn level_of(level: &tracing::Level) -> Option<LogLevel> {
        match *level {
            tracing::Level::ERROR => Some(LogLevel::Error),
            tracing::Level::WARN => Some(LogLevel::Warn),
            tracing::Level::INFO => Some(LogLevel::Info),
            tracing::Level::DEBUG => Some(LogLevel::Debug),
            tracing::Level::TRACE => None,
        }
    }

    fn callsite_enabled(&self, meta: &tracing::Metadata<'_>) -> bool {
        if !meta.is_event() {
            return false;
        }
        let Some(level) = Self::level_of(meta.level()) else {
            return false;
        };
        self.sink.enabled(level) && (level >= LogLevel::Warn || meta.target().starts_with("mimir"))
    }
}

impl tracing::Subscriber for TracingLogBridge {
    fn register_callsite(
        &self,
        meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if self.callsite_enabled(meta) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn enabled(&self, meta: &tracing::Metadata<'_>) -> bool {
        self.callsite_enabled(meta)
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(match self.sink.min_level() {
            LogLevel::Debug => tracing::level_filters::LevelFilter::DEBUG,
            LogLevel::Info => tracing::level_filters::LevelFilter::INFO,
            LogLevel::Warn => tracing::level_filters::LevelFilter::WARN,
            LogLevel::Error => tracing::level_filters::LevelFilter::ERROR,
        })
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let meta = event.metadata();
        let Some(level) = Self::level_of(meta.level()) else {
            return;
        };
        if !self.sink.enabled(level) {
            return;
        }
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);

        let context = component_context(meta.target());
        let mut kvs: Vec<(&str, LogValue<'_>)> =
            Vec::with_capacity(context.len() + 1 + visitor.fields.len());
        kvs.extend(context.iter().map(|&(k, v)| (k, LogValue::from(v))));
        if let Some(msg) = visitor.message {
            kvs.push(("msg", LogValue::Str(Cow::Owned(msg))));
        }
        kvs.extend(visitor.fields);
        let caller = meta.file().map(|f| (f, meta.line().unwrap_or(0)));
        self.sink.log_record(level, caller, &kvs);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    fields: Vec<(&'static str, LogValue<'static>)>,
}

impl EventVisitor {
    fn push(&mut self, field: &tracing::field::Field, value: LogValue<'static>) {
        if field.name() == "message" {
            self.message = Some(match value {
                LogValue::Str(s) => s.into_owned(),
                other => {
                    let mut s = String::new();
                    encode_logfmt_refs(&mut s, &[("m", &other)]);
                    s.trim_end().trim_start_matches("m=").to_string()
                }
            });
        } else {
            self.fields.push((field.name(), value));
        }
    }
}

impl tracing::field::Visit for EventVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.push(field, LogValue::Str(Cow::Owned(format!("{value:?}"))));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.push(field, LogValue::Str(Cow::Owned(value.to_string())));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.push(field, LogValue::I64(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.push(field, LogValue::U64(value));
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.push(field, LogValue::I128(value));
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.push(field, LogValue::U128(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.push(field, LogValue::F64(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.push(field, LogValue::Bool(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        // go-kit renders `error` values via `err.Error()`.
        self.push(field, LogValue::Str(Cow::Owned(value.to_string())));
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
    use serde_json::Value;

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

    fn epoch_sink(format: LogFormat, level: LogLevel) -> (Arc<LogSink>, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = LogSink::new(format, level, Box::new(SharedBuf(buf.clone())))
            .with_clock(Arc::new(|| UNIX_EPOCH));
        (Arc::new(sink), buf)
    }

    fn out(buf: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(buf.lock().clone()).unwrap()
    }

    fn golden_kvs() -> Vec<(&'static str, LogValue<'static>)> {
        vec![
            ("ts", "1970-01-01T00:00:00Z".into()),
            ("caller", "x.go:1".into()),
            ("level", "info".into()),
            ("msg", "hello world".into()),
            ("empty", "".into()),
            ("nul", "null".into()),
            ("q", "a\"b\\c\nd\te\u{1}\u{7f}\u{fffd}".into()),
            ("eq", "a=b".into()),
            ("uni", "h\u{e9}llo\u{2028}".into()),
            ("n", 42i64.into()),
            ("neg", (-7i64).into()),
            ("f", 0.5f64.into()),
            ("big", 1e6f64.into()),
            ("tiny", 1e-7f64.into()),
            ("b", true.into()),
            ("err", "dial tcp: connection refused".into()),
        ]
    }

    // Expected strings produced by the vendored go-kit/log + go-logfmt (NewLogfmtLogger /
    // NewJSONLogger) for the same keyvals.
    #[test]
    fn test_logfmt_encoding_matches_go_kit() {
        let mut s = String::new();
        assert!(encode_logfmt_record(&mut s, &golden_kvs()));
        assert_eq!(
            s,
            "ts=1970-01-01T00:00:00Z caller=x.go:1 level=info msg=\"hello world\" empty= nul=\"null\" \
             q=\"a\\\"b\\\\c\\nd\\te\\u0001\\u007f\\ufffd\" eq=\"a=b\" uni=h\u{e9}llo\u{2028} n=42 neg=-7 \
             f=0.5 big=1e+06 tiny=1e-07 b=true err=\"dial tcp: connection refused\"\n"
        );

        // Invalid key runes are dropped; an empty key drops the whole record.
        assert_eq!(format_logfmt_line(&[("k ey\"=", "v"), ("level", "warn")]), "key=v level=warn\n");
        assert_eq!(format_logfmt_line(&[("", "v")]), "");
        // Duplicate keys are kept in logfmt.
        assert_eq!(format_logfmt_line(&[("a", "1"), ("a", "2")]), "a=1 a=2\n");
    }

    #[test]
    fn test_json_encoding_matches_go_kit() {
        let mut s = String::new();
        assert!(encode_json_record(&mut s, &golden_kvs()));
        assert_eq!(
            s,
            "{\"b\":true,\"big\":1000000,\"caller\":\"x.go:1\",\"empty\":\"\",\"eq\":\"a=b\",\
             \"err\":\"dial tcp: connection refused\",\"f\":0.5,\"level\":\"info\",\"msg\":\"hello world\",\
             \"n\":42,\"neg\":-7,\"nul\":\"null\",\"q\":\"a\\\"b\\\\c\\nd\\te\\u0001\u{7f}\u{fffd}\",\
             \"tiny\":1e-7,\"ts\":\"1970-01-01T00:00:00Z\",\"uni\":\"h\u{e9}llo\\u2028\"}\n"
        );
        assert_eq!(
            format_json_line(&[("k ey\"=", "v"), ("level", "warn")]),
            "{\"k ey\\\"=\":\"v\",\"level\":\"warn\"}\n"
        );
        assert_eq!(format_json_line(&[("", "v")]), "{\"\":\"v\"}\n");
        assert_eq!(format_json_line(&[("a", "1"), ("a", "2")]), "{\"a\":\"2\"}\n");
        let mut nan = String::new();
        assert!(!encode_json_record(&mut nan, &[("f", LogValue::F64(f64::NAN))]));
        assert!(nan.is_empty());
    }

    // Values checked against Go `fmt.Sprint(f)` and `json.Marshal(f)`.
    #[test]
    fn test_go_float_formatting() {
        for (f, v, j) in [
            (0.5, "0.5", "0.5"),
            (1e6, "1e+06", "1000000"),
            (123456.0, "123456", "123456"),
            (1e20, "1e+20", "100000000000000000000"),
            (1e21, "1e+21", "1e+21"),
            (0.0001, "0.0001", "0.0001"),
            (0.00001, "1e-05", "0.00001"),
            (1.0, "1", "1"),
            (3.14159e-7, "3.14159e-07", "3.14159e-7"),
            (100.0, "100", "100"),
            (-2.5, "-2.5", "-2.5"),
            (0.0, "0", "0"),
        ] {
            assert_eq!(go_format_float_v(f), v, "fmt.Sprint({f})");
            assert_eq!(go_format_float_json(f).as_deref(), Some(j), "json.Marshal({f})");
        }
        assert_eq!(go_format_float_v(f64::INFINITY), "+Inf");
        assert_eq!(go_format_float_v(f64::NEG_INFINITY), "-Inf");
        assert_eq!(go_format_float_v(f64::NAN), "NaN");
    }

    #[test]
    fn test_log_sink_logger_key_order_and_caller() {
        let (sink, buf) = epoch_sink(LogFormat::Logfmt, LogLevel::Info);
        let logger: Arc<dyn Logger> = sink.clone();
        let line = line!() + 1;
        logger.log(LogLevel::Info, &[("msg", "Starting application"), ("version", "(version=x)")]);
        logger.log(LogLevel::Debug, &[("msg", "filtered")]);
        assert_eq!(
            out(&buf),
            format!(
                "ts=1970-01-01T00:00:00Z caller=logging.rs:{line} level=info msg=\"Starting application\" version=\"(version=x)\"\n"
            )
        );

        let (jsink, jbuf) = epoch_sink(LogFormat::Json, LogLevel::Info);
        let line = line!() + 1;
        jsink.log(LogLevel::Warn, &[("msg", "oops"), ("count", "3")]);
        assert_eq!(
            out(&jbuf),
            format!(
                "{{\"caller\":\"logging.rs:{line}\",\"count\":\"3\",\"level\":\"warn\",\"msg\":\"oops\",\"ts\":\"1970-01-01T00:00:00Z\"}}\n"
            )
        );
    }

    #[derive(Debug)]
    struct TestErr;
    impl std::fmt::Display for TestErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("connection refused")
        }
    }
    impl std::error::Error for TestErr {}

    #[test]
    fn test_tracing_bridge_logfmt_rendering() {
        let (sink, buf) = epoch_sink(LogFormat::Logfmt, LogLevel::Info);
        let bridge = TracingLogBridge::new(sink);
        let err = TestErr;
        let (l1, l2, l3) = tracing::subscriber::with_default(bridge, || {
            let l1 = line!() + 1;
            tracing::info!(old_state = "ACTIVE", new_state = "LEAVING", ring = "ingester", "changing instance state from");
            let l2 = line!() + 1;
            tracing::warn!(err = &err as &(dyn std::error::Error + 'static), tokens = 128u64, ok = true, ratio = 0.25, d = ?std::time::Duration::from_millis(1500), "failed to \"join\"");
            let l3 = line!() + 1;
            tracing::info!(component = "x", msg = "explicit order", n = -1i64);
            (l1, l2, l3)
        });
        assert_eq!(
            out(&buf),
            format!(
                "ts=1970-01-01T00:00:00Z caller=logging.rs:{l1} level=info msg=\"changing instance state from\" old_state=ACTIVE new_state=LEAVING ring=ingester\n\
                 ts=1970-01-01T00:00:00Z caller=logging.rs:{l2} level=warn msg=\"failed to \\\"join\\\"\" err=\"connection refused\" tokens=128 ok=true ratio=0.25 d=1.5s\n\
                 ts=1970-01-01T00:00:00Z caller=logging.rs:{l3} level=info component=x msg=\"explicit order\" n=-1\n"
            )
        );
    }

    #[test]
    fn test_tracing_bridge_json_rendering() {
        let (sink, buf) = epoch_sink(LogFormat::Json, LogLevel::Info);
        let bridge = TracingLogBridge::new(sink);
        let line = tracing::subscriber::with_default(bridge, || {
            let line = line!() + 1;
            tracing::error!(count = 2u64, ok = false, user = "tenant-1", "module failed");
            line
        });
        let o = out(&buf);
        assert_eq!(
            o,
            format!(
                "{{\"caller\":\"logging.rs:{line}\",\"count\":2,\"level\":\"error\",\"msg\":\"module failed\",\"ok\":false,\"ts\":\"1970-01-01T00:00:00Z\",\"user\":\"tenant-1\"}}\n"
            )
        );
        let parsed: Value = serde_json::from_str(o.trim()).unwrap();
        assert_eq!(parsed["count"], 2);
    }

    #[test]
    fn test_tracing_bridge_component_context() {
        assert_eq!(component_context("mimir_ring::kv::memberlist::client"), &[("component", "memberlist")]);
        assert_eq!(
            component_context("mimir_ring::kv::memberlist::tcp_transport"),
            &[("component", "memberlist"), ("component", "memberlist TCPTransport")]
        );
        assert!(component_context("mimir_ring::lifecycler").is_empty());

        let (sink, buf) = epoch_sink(LogFormat::Logfmt, LogLevel::Info);
        tracing::subscriber::with_default(TracingLogBridge::new(sink), || {
            tracing::info!(target: "mimir_ring::kv::memberlist::client", "leaving memberlist cluster");
        });
        let o = out(&buf);
        assert!(
            o.contains(" level=info component=memberlist msg=\"leaving memberlist cluster\"\n"),
            "{o}"
        );
    }

    #[test]
    fn test_tracing_bridge_level_filtering() {
        let evaluated = std::sync::atomic::AtomicUsize::new(0);
        let side_effect = || {
            evaluated.fetch_add(1, Ordering::SeqCst);
            1u64
        };

        let (sink, buf) = epoch_sink(LogFormat::Logfmt, LogLevel::Warn);
        tracing::subscriber::with_default(TracingLogBridge::new(sink), || {
            tracing::debug!(v = side_effect(), "debug");
            tracing::info!(v = side_effect(), "info");
            tracing::warn!("warn");
            tracing::error!("error");
            // Third-party targets: only warn/error.
            tracing::warn!(target: "h2::proto", "third-party warn");
        });
        let o = out(&buf);
        assert_eq!(o.lines().count(), 3, "{o}");
        assert!(o.contains("level=warn msg=warn"));
        assert!(o.contains("level=error msg=error"));
        assert!(o.contains("msg=\"third-party warn\""));
        // Disabled callsites never evaluate their fields.
        assert_eq!(evaluated.load(Ordering::SeqCst), 0);

        let (sink, buf) = epoch_sink(LogFormat::Logfmt, LogLevel::Debug);
        tracing::subscriber::with_default(TracingLogBridge::new(sink), || {
            tracing::trace!("never: go-kit has no trace level");
            tracing::debug!("debug enabled");
            tracing::debug!(target: "hyper::proto", "third-party debug");
            tracing::info!(target: "tonic::transport", "third-party info");
            tracing::info_span!("spans are not logged").in_scope(|| {});
        });
        let o = out(&buf);
        assert_eq!(o.lines().count(), 1, "{o}");
        assert!(o.contains("level=debug msg=\"debug enabled\""));
    }

    // Port of pkg/util/log/log_test.go ExampleInitLogger: level filter applies before the rate
    // limiter, and the limiter is shared by all loggers writing through the global logger
    // (here: `Logger::log` callers and `tracing` events share one sink).
    #[test]
    fn test_init_logger_example_rate_limited_and_filtered() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(
            LogSink::new(LogFormat::Logfmt, LogLevel::Info, Box::new(SharedBuf(buf.clone())))
                .with_clock(Arc::new(|| UNIX_EPOCH))
                .with_rate_limit(1.0, 4),
        );
        let reg = Registry::new();
        sink.register_metrics(&reg);
        let logger: Arc<dyn Logger> = sink.clone();
        let (l1, l2) = tracing::subscriber::with_default(TracingLogBridge::new(sink.clone()), || {
            let (mut l1, mut l2) = (0, 0);
            for i in 0..1000 {
                let i_s = (i + 1).to_string();
                l1 = line!() + 1;
                logger.log(LogLevel::Info, &[("msg", "log.Logger"), ("test", &i_s)]);
                logger.log(LogLevel::Debug, &[("msg", "log.Logger"), ("test", &i_s)]);
                l2 = line!() + 1;
                tracing::info!(test = i + 1, "cfg.Log");
                tracing::debug!(test = i + 1, "log.Logger");
            }
            (l1, l2)
        });
        assert_eq!(
            out(&buf),
            format!(
                "ts=1970-01-01T00:00:00Z caller=logging.rs:{l1} level=info msg=log.Logger test=1\n\
                 ts=1970-01-01T00:00:00Z caller=logging.rs:{l2} level=info msg=cfg.Log test=1\n\
                 ts=1970-01-01T00:00:00Z caller=logging.rs:{l1} level=info msg=log.Logger test=2\n\
                 ts=1970-01-01T00:00:00Z caller=logging.rs:{l2} level=info msg=cfg.Log test=2\n"
            )
        );
        // 2000 info lines, 4 logged; debug lines are filtered before reaching the limiter.
        assert_eq!(sink.discarded_lines(LogLevel::Info), 1996.0);
        assert_eq!(sink.discarded_lines(LogLevel::Debug), 0.0);
        let families = reg.gather();
        assert!(families
            .iter()
            .any(|f| f.get_name() == "logger_rate_limit_discarded_log_lines_total"));
    }

    #[test]
    fn test_log_sink_lines_never_interleave() {
        let (sink, buf) = epoch_sink(LogFormat::Logfmt, LogLevel::Info);
        let bridge = Arc::new(TracingLogBridge::new(sink.clone()));
        let dispatch = tracing::Dispatch::from(bridge);
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let sink = sink.clone();
                let dispatch = dispatch.clone();
                std::thread::spawn(move || {
                    let long = "x".repeat(4096);
                    tracing::dispatcher::with_default(&dispatch, || {
                        for _ in 0..200 {
                            sink.log(LogLevel::Info, &[("msg", "a"), ("t", &t.to_string()), ("p", &long)]);
                            tracing::info!(t, p = %long, "b");
                        }
                    });
                })
            })
            .collect();
        for th in threads {
            th.join().unwrap();
        }
        let o = out(&buf);
        assert_eq!(o.lines().count(), 8 * 200 * 2);
        for l in o.lines() {
            assert!(l.starts_with("ts=1970-01-01T00:00:00Z caller=logging.rs:"), "{l}");
            assert!(l.ends_with(&"x".repeat(4096)), "interleaved line");
        }
    }

    #[test]
    fn log_values_preserves_native_json_types() {
        use std::sync::Arc;
        #[derive(Clone, Default)]
        struct BufWriter(Arc<parking_lot::Mutex<Vec<u8>>>);
        impl std::io::Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = BufWriter::default();
        let sink = LogSink::new(LogFormat::Json, LogLevel::Info, Box::new(buf.clone()));
        sink.log_values(
            LogLevel::Warn,
            &[
                ("soft_validation", LogValue::Bool(true)),
                ("count", LogValue::U64(3)),
                ("msg", LogValue::from("hello")),
            ],
        );
        let out = String::from_utf8(buf.0.lock().clone()).unwrap();
        let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
        assert_eq!(v["soft_validation"], serde_json::json!(true));
        assert_eq!(v["count"], serde_json::json!(3));
        assert_eq!(v["msg"], serde_json::json!("hello"));
    }
}
