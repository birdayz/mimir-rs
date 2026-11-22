//! Buffered logger, lazy sprintf, and log value sanitization utilities.
//!
//! Ported from:
//! - github.com/grafana/dskit/log/buffered.go
//! - github.com/grafana/dskit/log/gokit.go
//! - github.com/grafana/dskit/log/sanitize.go

use std::borrow::Cow;
use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::Mutex;
use serde::ser::{Serialize, Serializer};

// ---------------------------------------------------------------------------------------------
// dskit/log/buffered.go
// ---------------------------------------------------------------------------------------------

pub type FlushCallback = Arc<dyn Fn(u32) + Send + Sync>;

/// Thread-safe buffered `io::Write` sink matching `dskit/log.BufferedLogger`.
pub struct BufferedLogger {
    inner: Arc<BufferedInner>,
    stop_flag: Arc<AtomicBool>,
    flush_thread: Option<JoinHandle<()>>,
}

struct BufferedInner {
    buf: Mutex<Vec<u8>>,
    entries: AtomicU32,
    cap: u32,
    writer: Mutex<Box<dyn Write + Send>>,
    on_flush: Option<FlushCallback>,
}

impl BufferedLogger {
    pub fn new(writer: Box<dyn Write + Send>, cap: u32) -> Self {
        Self::with_options(writer, cap, None, None, None)
    }

    pub fn with_options(
        writer: Box<dyn Write + Send>,
        cap: u32,
        preallocated_capacity: Option<usize>,
        flush_period: Option<Duration>,
        on_flush: Option<FlushCallback>,
    ) -> Self {
        let buf = match preallocated_capacity {
            Some(sz) => Vec::with_capacity(sz),
            None => Vec::new(),
        };
        let inner = Arc::new(BufferedInner {
            buf: Mutex::new(buf),
            entries: AtomicU32::new(0),
            cap,
            writer: Mutex::new(writer),
            on_flush,
        });
        let stop_flag = Arc::new(AtomicBool::new(false));
        let flush_thread = flush_period.map(|period| {
            let inner_bg = Arc::clone(&inner);
            let stop_bg = Arc::clone(&stop_flag);
            thread::spawn(move || {
                while !stop_bg.load(Ordering::Relaxed) {
                    thread::sleep(period);
                    if stop_bg.load(Ordering::Relaxed) {
                        break;
                    }
                    let _ = flush_inner(&inner_bg);
                }
            })
        });
        Self {
            inner,
            stop_flag,
            flush_thread,
        }
    }

    pub fn size(&self) -> u32 {
        self.inner.entries.load(Ordering::Acquire)
    }

    pub fn write_entry(&self, p: &[u8]) -> io::Result<usize> {
        if self.size() >= self.inner.cap {
            if let Err(e) = self.flush() {
                self.inner.buf.lock().clear();
                return Err(e);
            }
        }
        self.inner.entries.fetch_add(1, Ordering::AcqRel);
        let mut buf = self.inner.buf.lock();
        buf.extend_from_slice(p);
        Ok(p.len())
    }

    pub fn flush(&self) -> io::Result<()> {
        flush_inner(&self.inner)
    }

    pub fn shared_writer(self: &Arc<Self>) -> SharedBufferedWriter {
        SharedBufferedWriter(Arc::clone(self))
    }
}

fn flush_inner(inner: &BufferedInner) -> io::Result<()> {
    let sz = inner.entries.swap(0, Ordering::AcqRel);
    if sz == 0 {
        return Ok(());
    }
    let data = {
        let mut buf = inner.buf.lock();
        let taken = buf.clone();
        buf.clear();
        taken
    };
    let res = {
        let mut w = inner.writer.lock();
        w.write_all(&data)
    };
    if res.is_ok() {
        if let Some(ref cb) = inner.on_flush {
            cb(sz);
        }
    }
    res
}

impl Drop for BufferedLogger {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(h) = self.flush_thread.take() {
            let _ = h.join();
        }
    }
}

impl Write for BufferedLogger {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_entry(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        BufferedLogger::flush(self)
    }
}

#[derive(Clone)]
pub struct SharedBufferedWriter(Arc<BufferedLogger>);

impl Write for SharedBufferedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write_entry(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

// ---------------------------------------------------------------------------------------------
// dskit/log/gokit.go
// ---------------------------------------------------------------------------------------------

/// Deferred string formatter matching `dskit/log.LazySprintf`: only evaluates the closure when
/// formatted via `Display`.
pub struct LazySprintf<F: Fn() -> String> {
    f: F,
}

impl<F: Fn() -> String> LazySprintf<F> {
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F: Fn() -> String> fmt::Display for LazySprintf<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&(self.f)())
    }
}

// ---------------------------------------------------------------------------------------------
// dskit/log/sanitize.go
// ---------------------------------------------------------------------------------------------

/// Input representation for `DroppedUnsafeChars` and `EscapedUnsafeChars`.
pub enum UnsafeInput<'a> {
    Str(&'a str),
    Bytes(&'a [u8]),
    Owned(String),
    Null,
}

/// Wrapper returned by `drop_unsafe_chars_wrapper` (`dskit/log.DropUnsafeChars`).
pub struct DroppedUnsafeChars<'a>(pub UnsafeInput<'a>);

/// Wrapper returned by `escape_unsafe_chars_wrapper` (`dskit/log.EscapeUnsafeChars`).
pub struct EscapedUnsafeChars<'a>(pub UnsafeInput<'a>);

pub fn drop_unsafe_chars_wrapper<'a>(input: UnsafeInput<'a>) -> DroppedUnsafeChars<'a> {
    DroppedUnsafeChars(input)
}

pub fn escape_unsafe_chars_wrapper<'a>(input: UnsafeInput<'a>) -> EscapedUnsafeChars<'a> {
    EscapedUnsafeChars(input)
}

#[inline]
fn is_unsafe_unicode(c: char) -> bool {
    if c == '\t' {
        return false;
    }
    if c.is_control() {
        return true;
    }
    let code = c as u32;
    matches!(
        code,
        0x00AD
            | 0x0600..=0x0605
            | 0x061C
            | 0x06DD
            | 0x070F
            | 0x0890..=0x0891
            | 0x08E2
            | 0x180E
            | 0x200B..=0x200F
            | 0x2028..=0x202E
            | 0x2060..=0x2064
            | 0x2066..=0x206F
            | 0xFEFF
            | 0xFFF9..=0xFFFB
            | 0x110BD
            | 0x110CD
            | 0x13430..=0x1343F
            | 0x1BCA0..=0x1BCA3
            | 0x1D173..=0x1D17A
            | 0xE0001
            | 0xE0020..=0xE007F
    )
}

#[inline]
fn needs_sanitization_bytes(bytes: &[u8]) -> bool {
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\t' || (0x20..0x7f).contains(&b) {
            continue;
        }
        if b < 0x80 {
            return true;
        }
        return match std::str::from_utf8(&bytes[i..]) {
            Ok(s) => s.chars().any(is_unsafe_unicode),
            Err(_) => true,
        };
    }
    false
}

fn sanitize_bytes_drop(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                for c in valid.chars() {
                    if !is_unsafe_unicode(c) {
                        out.push(c);
                    }
                }
                break;
            }
            Err(err) => {
                let valid_len = err.valid_up_to();
                if valid_len > 0 {
                    let valid = std::str::from_utf8(&rest[..valid_len]).expect("valid prefix");
                    for c in valid.chars() {
                        if !is_unsafe_unicode(c) {
                            out.push(c);
                        }
                    }
                }
                let invalid_len = err.error_len().unwrap_or(1);
                rest = &rest[valid_len + invalid_len..];
            }
        }
    }
    out
}

fn write_escaped_code(out: &mut String, code: u32) {
    use fmt::Write as _;
    if code < 0x100 {
        let _ = write!(out, "\\x{code:02x}");
    } else if code < 0x10000 {
        let _ = write!(out, "\\u{code:04x}");
    } else {
        let _ = write!(out, "\\U{code:08x}");
    }
}

fn sanitize_bytes_escape(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                for c in valid.chars() {
                    if is_unsafe_unicode(c) {
                        write_escaped_code(&mut out, c as u32);
                    } else {
                        out.push(c);
                    }
                }
                break;
            }
            Err(err) => {
                let valid_len = err.valid_up_to();
                if valid_len > 0 {
                    let valid = std::str::from_utf8(&rest[..valid_len]).expect("valid prefix");
                    for c in valid.chars() {
                        if is_unsafe_unicode(c) {
                            write_escaped_code(&mut out, c as u32);
                        } else {
                            out.push(c);
                        }
                    }
                }
                let bad_len = err.error_len().unwrap_or(1);
                for &b in &rest[valid_len..valid_len + bad_len] {
                    write_escaped_code(&mut out, u32::from(b));
                }
                rest = &rest[valid_len + bad_len..];
            }
        }
    }
    out
}

impl<'a> DroppedUnsafeChars<'a> {
    pub fn as_cow_str(&self) -> Cow<'_, str> {
        match &self.0 {
            UnsafeInput::Null => Cow::Borrowed("null"),
            UnsafeInput::Str(s) => {
                if !needs_sanitization_bytes(s.as_bytes()) {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(sanitize_bytes_drop(s.as_bytes()))
                }
            }
            UnsafeInput::Owned(s) => {
                if !needs_sanitization_bytes(s.as_bytes()) {
                    Cow::Borrowed(s.as_str())
                } else {
                    Cow::Owned(sanitize_bytes_drop(s.as_bytes()))
                }
            }
            UnsafeInput::Bytes(b) => {
                if !needs_sanitization_bytes(b) {
                    Cow::Borrowed(std::str::from_utf8(b).unwrap_or(""))
                } else {
                    Cow::Owned(sanitize_bytes_drop(b))
                }
            }
        }
    }
}

impl<'a> EscapedUnsafeChars<'a> {
    pub fn as_cow_str(&self) -> Cow<'_, str> {
        match &self.0 {
            UnsafeInput::Null => Cow::Borrowed("null"),
            UnsafeInput::Str(s) => {
                if !needs_sanitization_bytes(s.as_bytes()) {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(sanitize_bytes_escape(s.as_bytes()))
                }
            }
            UnsafeInput::Owned(s) => {
                if !needs_sanitization_bytes(s.as_bytes()) {
                    Cow::Borrowed(s.as_str())
                } else {
                    Cow::Owned(sanitize_bytes_escape(s.as_bytes()))
                }
            }
            UnsafeInput::Bytes(b) => {
                if !needs_sanitization_bytes(b) {
                    Cow::Borrowed(std::str::from_utf8(b).unwrap_or(""))
                } else {
                    Cow::Owned(sanitize_bytes_escape(b))
                }
            }
        }
    }
}

impl fmt::Display for DroppedUnsafeChars<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_cow_str())
    }
}

impl fmt::Display for EscapedUnsafeChars<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_cow_str())
    }
}

impl Serialize for DroppedUnsafeChars<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if matches!(self.0, UnsafeInput::Null) {
            return serializer.serialize_none();
        }
        serializer.serialize_str(&self.as_cow_str())
    }
}

impl Serialize for EscapedUnsafeChars<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if matches!(self.0, UnsafeInput::Null) {
            return serializer.serialize_none();
        }
        serializer.serialize_str(&self.as_cow_str())
    }
}

/// Formats `key=value\n` in go-kit logfmt encoding for sanitized values (emitting unquoted
/// `null` for `UnsafeInput::Null`, matching go-kit's `nil` / `TextMarshaler` behavior).
pub fn format_logfmt_sanitized(key: &str, val: &DroppedUnsafeChars<'_>) -> String {
    if matches!(val.0, UnsafeInput::Null) {
        return format!("{key}=null\n");
    }
    let s = val.as_cow_str();
    let mut out = String::new();
    mimir_common::logging::encode_logfmt_record(
        &mut out,
        &[(key, mimir_common::logging::LogValue::Str(s))],
    );
    out
}

/// Escaped-mode sibling of `format_logfmt_sanitized`.
pub fn format_logfmt_sanitized_escaped(key: &str, val: &EscapedUnsafeChars<'_>) -> String {
    if matches!(val.0, UnsafeInput::Null) {
        return format!("{key}=null\n");
    }
    let s = val.as_cow_str();
    let mut out = String::new();
    mimir_common::logging::encode_logfmt_record(
        &mut out,
        &[(key, mimir_common::logging::LogValue::Str(s))],
    );
    out
}
