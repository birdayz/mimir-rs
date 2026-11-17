//! `/debug/pprof/*` endpoints.
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go (`registerInstrumentation`:
//!   `/debug/pprof/cmdline` is `http.NotFoundHandler()`, every other `/debug/pprof` path goes to
//!   `http.DefaultServeMux`)
//! - Go net/http/pprof (`Index`, `Profile`, `Symbol`, `handler.ServeHTTP`, `serveError`,
//!   `durationExceedsWriteTimeout`) and net/http `ServeMux` (subtree redirect of `/debug/pprof`).
//!
//! The CPU profile is a gzip-compressed `perftools.profiles.Profile` protobuf like Go's
//! `pprof.StartCPUProfile` output (`samples/count` + `cpu/nanoseconds`, 100 Hz), so
//! `go tool pprof`, Pyroscope and Grafana profiling scrapes can read it. Go runtime profiles
//! (`heap`, `goroutine`, `allocs`, `block`, `mutex`, `threadcreate`, `trace`) have no Rust
//! equivalent and are answered like any profile Go does not know: `404 Unknown profile`.

use std::collections::HashMap;
use std::io::Write as _;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::http::AppHttpState;
use crate::server::http_server::ServedOverHttp;

/// Go `runtime.SetCPUProfileRate` default used by `pprof.StartCPUProfile`.
const CPU_PROFILE_HZ: i32 = 100;

/// Profiles served by mimir-rs, listed on the index page (name, description from
/// net/http/pprof `profileDescriptions`).
const PROFILES: &[(&str, &str)] = &[
    ("cmdline", "The command line invocation of the current program"),
    ("profile", "CPU profile. You can specify the duration in the seconds GET parameter. After you get the profile file, use the go tool pprof command to investigate the profile."),
    ("symbol", "Maps given program counters to function names. Counters can be specified in a GET raw query or POST body, multiple counters are separated by &#39;+&#39;."),
];

/// net/http/pprof `serveError`.
fn serve_error(status: StatusCode, txt: &str) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8")),
            (header::HeaderName::from_static("x-go-pprof"), HeaderValue::from_static("1")),
            (header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")),
        ],
        format!("{txt}\n"),
    )
        .into_response()
}

/// `ServeMux` redirect of the subtree root without its trailing slash.
pub async fn index_redirect_handler(req: Request) -> Response {
    let mut location = format!("{}/", req.uri().path());
    if let Some(q) = req.uri().query() {
        location.push('?');
        location.push_str(q);
    }
    let body = format!("<a href=\"{}\">Temporary Redirect</a>.\n\n", html_escape(&location));
    let mut resp = (
        StatusCode::TEMPORARY_REDIRECT,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&location) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    resp
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&#34;")
        .replace('\'', "&#39;")
}

/// net/http/pprof `Index` (template `indexTmplExecute`).
pub async fn index_handler() -> Response {
    let mut b = String::from(
        "<html>\n<head>\n<title>/debug/pprof/</title>\n<style>\n.profile-name{\n\tdisplay:inline-block;\n\twidth:6rem;\n}\n</style>\n</head>\n<body>\n/debug/pprof/\n<br>\n<p>Set debug=1 as a query parameter to export in legacy text format</p>\n<br>\nTypes of profiles available:\n<table>\n<thead><td>Count</td><td>Profile</td></thead>\n",
    );
    for (name, _) in PROFILES {
        b.push_str(&format!("<tr><td>0</td><td><a href='{name}?debug=1'>{name}</a></td></tr>\n"));
    }
    b.push_str("</table>\n<a href=\"goroutine?debug=2\">full goroutine stack dump</a>\n<br>\n<p>\nProfile Descriptions:\n<ul>\n");
    for (name, desc) in PROFILES {
        b.push_str(&format!("<li><div class=profile-name>{name}: </div> {desc}</li>\n"));
    }
    b.push_str("</ul>\n</p>\n</body>\n</html>");
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        b,
    )
        .into_response()
}

/// `Index` for `/debug/pprof/<name>`: `handler(name)` answers unknown profiles with 404.
pub async fn unknown_profile_handler() -> Response {
    serve_error(StatusCode::NOT_FOUND, "Unknown profile")
}

/// net/http/pprof `Symbol`: Go program counters cannot be resolved in a Rust binary, so only the
/// symbol-availability header is returned (Go prints a line per resolvable counter).
pub async fn symbol_handler() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        "num_symbols: 1\n",
    )
        .into_response()
}

/// Form value lookup (`r.FormValue`: first value of the query parameter).
fn form_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?
        .split('&')
        .filter_map(|kv| kv.split_once('=').or(Some((kv, ""))))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// net/http/pprof `Profile`.
pub async fn profile_handler(axum::extract::State(st): State<AppHttpState>, req: Request) -> Response {
    let sec = form_value(req.uri().query(), "seconds")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(30);
    // durationExceedsWriteTimeout: only requests served by the HTTP server have a WriteTimeout.
    if req.extensions().get::<ServedOverHttp>().is_some() {
        let wt = st.config.server.http_server_write_timeout.as_std().unwrap_or_default();
        if !wt.is_zero() && sec as f64 >= wt.as_secs_f64() {
            return serve_error(StatusCode::BAD_REQUEST, "profile duration exceeds server's WriteTimeout");
        }
    }
    cpu_profile(Duration::from_secs(sec.unsigned_abs())).await
}

#[cfg(unix)]
async fn cpu_profile(duration: Duration) -> Response {
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(CPU_PROFILE_HZ)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return serve_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Could not enable CPU profiling: {e}"),
            )
        }
    };
    tokio::time::sleep(duration).await;
    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return serve_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Could not enable CPU profiling: {e}"),
            )
        }
    };
    drop(guard);
    let profile = encode_profile(&report);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let body = match gz.write_all(&profile).and_then(|()| gz.finish()) {
        Ok(b) => b,
        Err(e) => return serve_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CONTENT_DISPOSITION, "attachment; filename=\"profile\""),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

#[cfg(not(unix))]
async fn cpu_profile(_duration: Duration) -> Response {
    serve_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Could not enable CPU profiling: not supported on this platform",
    )
}

/// `perftools.profiles.Profile` (github.com/google/pprof/proto/profile.proto), the subset
/// written by Go's CPU profiler.
mod proto {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Profile {
        #[prost(message, repeated, tag = "1")]
        pub sample_type: Vec<ValueType>,
        #[prost(message, repeated, tag = "2")]
        pub sample: Vec<Sample>,
        #[prost(message, repeated, tag = "4")]
        pub location: Vec<Location>,
        #[prost(message, repeated, tag = "5")]
        pub function: Vec<Function>,
        #[prost(string, repeated, tag = "6")]
        pub string_table: Vec<String>,
        #[prost(int64, tag = "9")]
        pub time_nanos: i64,
        #[prost(int64, tag = "10")]
        pub duration_nanos: i64,
        #[prost(message, optional, tag = "11")]
        pub period_type: Option<ValueType>,
        #[prost(int64, tag = "12")]
        pub period: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ValueType {
        #[prost(int64, tag = "1")]
        pub r#type: i64,
        #[prost(int64, tag = "2")]
        pub unit: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(uint64, repeated, tag = "1")]
        pub location_id: Vec<u64>,
        #[prost(int64, repeated, tag = "2")]
        pub value: Vec<i64>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Location {
        #[prost(uint64, tag = "1")]
        pub id: u64,
        #[prost(uint64, tag = "3")]
        pub address: u64,
        #[prost(message, repeated, tag = "4")]
        pub line: Vec<Line>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Line {
        #[prost(uint64, tag = "1")]
        pub function_id: u64,
        #[prost(int64, tag = "2")]
        pub line: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Function {
        #[prost(uint64, tag = "1")]
        pub id: u64,
        #[prost(int64, tag = "2")]
        pub name: i64,
        #[prost(int64, tag = "3")]
        pub system_name: i64,
        #[prost(int64, tag = "4")]
        pub filename: i64,
    }
}

#[derive(Default)]
struct StringTable {
    strings: Vec<String>,
    index: HashMap<String, i64>,
}

impl StringTable {
    fn new() -> Self {
        let mut t = Self::default();
        t.intern("");
        t
    }

    fn intern(&mut self, s: &str) -> i64 {
        if let Some(i) = self.index.get(s) {
            return *i;
        }
        let i = self.strings.len() as i64;
        self.strings.push(s.to_string());
        self.index.insert(s.to_string(), i);
        i
    }
}

#[cfg(unix)]
fn encode_profile(report: &pprof::Report) -> Vec<u8> {
    use prost::Message as _;

    let mut st = StringTable::new();
    let samples_t = proto::ValueType {
        r#type: st.intern("samples"),
        unit: st.intern("count"),
    };
    let cpu_t = proto::ValueType {
        r#type: st.intern("cpu"),
        unit: st.intern("nanoseconds"),
    };
    let period = 1_000_000_000 / i64::from(report.timing.frequency.max(1));

    // One function + location per distinct symbol (frames are already symbolized).
    let mut locations: Vec<proto::Location> = Vec::new();
    let mut functions: Vec<proto::Function> = Vec::new();
    let mut by_name: HashMap<String, u64> = HashMap::new();
    let mut samples = Vec::with_capacity(report.data.len());
    for (frames, count) in &report.data {
        let mut location_id = Vec::new();
        for frame in &frames.frames {
            for sym in frame {
                let name = sym.name();
                if let Some(id) = by_name.get(&name) {
                    location_id.push(*id);
                    continue;
                }
                let id = functions.len() as u64 + 1;
                functions.push(proto::Function {
                    id,
                    name: st.intern(&name),
                    system_name: st.intern(&sym.sys_name()),
                    filename: st.intern(&sym.filename()),
                });
                locations.push(proto::Location {
                    id,
                    address: 0,
                    line: vec![proto::Line {
                        function_id: id,
                        line: i64::from(sym.lineno()),
                    }],
                });
                by_name.insert(name, id);
                location_id.push(id);
            }
        }
        let count = *count as i64;
        samples.push(proto::Sample {
            location_id,
            value: vec![count, count.saturating_mul(period)],
        });
    }
    let time_nanos = report
        .timing
        .start_time
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64);
    proto::Profile {
        sample_type: vec![samples_t, cpu_t.clone()],
        sample: samples,
        location: locations,
        function: functions,
        string_table: st.strings,
        time_nanos,
        duration_nanos: report.timing.duration.as_nanos() as i64,
        period_type: Some(cpu_t),
        period,
    }
    .encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_values() {
        assert_eq!(form_value(Some("seconds=5&x=1"), "seconds"), Some("5"));
        assert_eq!(form_value(Some("x=1"), "seconds"), None);
        assert_eq!(form_value(None, "seconds"), None);
    }

    #[test]
    fn string_table_starts_empty() {
        let mut st = StringTable::new();
        assert_eq!(st.intern("a"), 1);
        assert_eq!(st.intern(""), 0);
        assert_eq!(st.intern("a"), 1);
        assert_eq!(st.strings, vec!["".to_string(), "a".to_string()]);
    }
}
