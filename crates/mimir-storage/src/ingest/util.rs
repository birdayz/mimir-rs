//! Kafka ingest_storage utilities: partition ID parsing, promises, SASL credential resolution,
//! sampled-only OpenTelemetry tracing hooks, topic creation, and SASL-capable Kafka transport.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/util.go

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::error::{ParseResponseErrorCode, ResponseError};
use mimir_config::KafkaConfig;
use parking_lot::Mutex as SyncMutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::compression::KafkaCompression;
use super::kafka_client::{FetchPartitionResult, KafkaClientError, ListedOffset, ProduceRecord};

/// Parses the partition ID suffix from an ingester instance ID (e.g. `"ingester-zone-a-0"` -> `0`).
/// Matches `IngesterPartitionID` in `pkg/storage/ingest/util.go` using `-([0-9]+)$`.
pub fn ingester_partition_id(ingester_id: &str) -> Result<i32, String> {
    const REGEX_STR: &str = "-([0-9]+)$";
    let Some(pos) = ingester_id.rfind('-') else {
        return Err(format!(
            "ingester ID {ingester_id} doesn't match regular expression {REGEX_STR:?}"
        ));
    };
    let suffix = &ingester_id[pos + 1..];
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "ingester ID {ingester_id} doesn't match regular expression {REGEX_STR:?}"
        ));
    }
    suffix
        .parse::<i32>()
        .map_err(|_| format!("no ingester sequence number in ingester ID {ingester_id}"))
}

// ---------------------------------------------------------------------------
// ResultPromise (ported from resultPromise[T] in util.go)
// ---------------------------------------------------------------------------

/// A single-shot promise allowing multiple tasks to await a result notified once by a producer.
pub struct ResultPromise<T: Clone + Send + Sync + 'static> {
    tx: watch::Sender<Option<Result<T, String>>>,
    _rx: watch::Receiver<Option<Result<T, String>>>,
}

impl<T: Clone + Send + Sync + 'static> Default for ResultPromise<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + Sync + 'static> ResultPromise<T> {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(None);
        Self { tx, _rx }
    }

    /// Notifies all waiting tasks. Must be called at most once.
    pub fn notify(&self, res: Result<T, String>) {
        self.tx.send_replace(Some(res));
    }

    /// Waits until a result is notified or `timeout` (if `Some`) expires.
    pub async fn wait(&self, timeout: Option<Duration>) -> Result<T, String> {
        let mut rx = self.tx.subscribe();
        let fut = async {
            loop {
                if let Some(ref res) = *rx.borrow_and_update() {
                    return res.clone();
                }
                if rx.changed().await.is_err() {
                    return Err("promise dropped before notify".to_string());
                }
            }
        };
        match timeout {
            Some(dur) => tokio::time::timeout(dur, fut)
                .await
                .map_err(|_| "context deadline exceeded".to_string())?,
            None => fut.await,
        }
    }
}

// ---------------------------------------------------------------------------
// SASL Credentials Resolution (ported from util.go)
// ---------------------------------------------------------------------------

/// franz-go `oauth.Auth`. JSON field names are the Go struct field names (`Zid`, `Token`,
/// `Extensions`), matched case-insensitively on decode like Go `encoding/json`.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct KafkaOauthbearerAuth {
    #[serde(rename = "Zid")]
    pub zid: String,
    #[serde(rename = "Token")]
    pub token: String,
    #[serde(rename = "Extensions")]
    pub extensions: BTreeMap<String, String>,
}

impl std::fmt::Debug for KafkaOauthbearerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaOauthbearerAuth")
            .field("zid", &self.zid)
            .field("token", &"<redacted>")
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// franz-go `aws.Auth`. JSON field names are the Go struct field names (`AccessKey`,
/// `SecretKey`, `SessionToken`, `UserAgent`), matched case-insensitively on decode like Go
/// `encoding/json`.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct KafkaMskIamAuth {
    #[serde(rename = "AccessKey")]
    pub access_key: String,
    #[serde(rename = "SecretKey")]
    pub secret_key: String,
    #[serde(rename = "SessionToken")]
    pub session_token: String,
    #[serde(rename = "UserAgent")]
    pub user_agent: String,
}

impl std::fmt::Debug for KafkaMskIamAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaMskIamAuth")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("session_token", &"<redacted>")
            .field("user_agent", &self.user_agent)
            .finish()
    }
}

/// Decodes a JSON object into Go struct fields the way Go `encoding/json` does: keys match field
/// names exactly or case-insensitively, `null` leaves the field untouched, unknown keys are
/// ignored, a later duplicate key wins.
fn decode_go_json_fields<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
    fields: &[&str],
    mut set: impl FnMut(usize, serde_json::Value) -> Result<(), String>,
) -> Result<(), D::Error> {
    use serde::de::Error as _;
    let obj = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
    for (key, value) in obj {
        let idx = fields
            .iter()
            .position(|f| *f == key)
            .or_else(|| fields.iter().position(|f| f.eq_ignore_ascii_case(&key)));
        if let Some(idx) = idx {
            if value.is_null() {
                continue;
            }
            set(idx, value).map_err(|e| D::Error::custom(format!("field {}: {e}", fields[idx])))?;
        }
    }
    Ok(())
}

impl<'de> Deserialize<'de> for KafkaOauthbearerAuth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut out = KafkaOauthbearerAuth {
            zid: String::new(),
            token: String::new(),
            extensions: BTreeMap::new(),
        };
        decode_go_json_fields(deserializer, &["Zid", "Token", "Extensions"], |idx, v| {
            match idx {
                0 => out.zid = serde_json::from_value(v).map_err(|e| e.to_string())?,
                1 => out.token = serde_json::from_value(v).map_err(|e| e.to_string())?,
                _ => out.extensions = serde_json::from_value(v).map_err(|e| e.to_string())?,
            }
            Ok(())
        })?;
        Ok(out)
    }
}

impl<'de> Deserialize<'de> for KafkaMskIamAuth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut out = KafkaMskIamAuth {
            access_key: String::new(),
            secret_key: String::new(),
            session_token: String::new(),
            user_agent: String::new(),
        };
        decode_go_json_fields(
            deserializer,
            &["AccessKey", "SecretKey", "SessionToken", "UserAgent"],
            |idx, v| {
                let s: String = serde_json::from_value(v).map_err(|e| e.to_string())?;
                match idx {
                    0 => out.access_key = s,
                    1 => out.secret_key = s,
                    2 => out.session_token = s,
                    _ => out.user_agent = s,
                }
                Ok(())
            },
        )?;
        Ok(out)
    }
}

pub type CredentialCallback<A> =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<A, String>> + Send>> + Send + Sync>;

pub enum ResolvedSaslMechanism<A> {
    Static(A),
    Callback(CredentialCallback<A>),
}

impl<A: Clone> Clone for ResolvedSaslMechanism<A> {
    fn clone(&self) -> Self {
        match self {
            Self::Static(a) => Self::Static(a.clone()),
            Self::Callback(cb) => Self::Callback(Arc::clone(cb)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSaslSecretConfig<S> {
    pub secret: Option<S>,
    pub file_path: String,
    pub http_socket_path: String,
    pub http_socket_timeout: Duration,
}

pub trait StaticSaslSecret: Clone + Send + Sync + 'static {
    type Auth: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static;
    fn into_auth(self) -> Option<Self::Auth>;
}

impl StaticSaslSecret for KafkaOauthbearerAuth {
    type Auth = KafkaOauthbearerAuth;
    fn into_auth(self) -> Option<Self::Auth> {
        if self.token.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

impl StaticSaslSecret for KafkaMskIamAuth {
    type Auth = KafkaMskIamAuth;
    fn into_auth(self) -> Option<Self::Auth> {
        if self.access_key.is_empty() || self.secret_key.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

/// Resolves a SASL mechanism from static secret, JSON file, or HTTP-over-Unix-socket,
/// matching `saslMechanism` in `pkg/storage/ingest/util.go`.
pub fn sasl_mechanism<S, R>(
    cfg: KafkaSaslSecretConfig<S>,
    from_callback: impl FnOnce(CredentialCallback<S::Auth>) -> R,
    from_static: impl FnOnce(S::Auth) -> R,
) -> Result<R, String>
where
    S: StaticSaslSecret,
{
    if let Some(secret) = cfg.secret {
        if let Some(auth) = secret.into_auth() {
            return Ok(from_static(auth));
        }
    }
    if !cfg.file_path.is_empty() {
        let path = cfg.file_path;
        let cb: CredentialCallback<S::Auth> = Arc::new(move || {
            let path = path.clone();
            Box::pin(async move {
                let bytes = tokio::fs::read(&path)
                    .await
                    .map_err(|e| format!("reading credentials file {path}: {e}"))?;
                serde_json::from_slice::<S::Auth>(&bytes)
                    .map_err(|e| format!("parsing credentials file {path}: {e}"))
            })
        });
        return Ok(from_callback(cb));
    }
    if !cfg.http_socket_path.is_empty() {
        let sock = cfg.http_socket_path;
        let timeout = if cfg.http_socket_timeout.is_zero() {
            Duration::from_secs(10)
        } else {
            cfg.http_socket_timeout
        };
        let cb: CredentialCallback<S::Auth> = Arc::new(move || {
            let sock = sock.clone();
            Box::pin(async move { request_json_from_socket::<S::Auth>(&sock, timeout).await })
        });
        return Ok(from_callback(cb));
    }
    Err("invalid kafkaSecretConfig; Validate must have been called first".to_string())
}

/// Fetches a JSON credential payload via HTTP GET `http://credentials/` over a Unix domain socket.
/// Matches `requestJSONFromSocket` in `pkg/storage/ingest/util.go`.
pub async fn request_json_from_socket<T: for<'de> Deserialize<'de>>(
    socket_path: &str,
    timeout: Duration,
) -> Result<T, String> {
    let fut = async {
        let mut stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| format!("requesting credentials from HTTP socket {socket_path}: {e}"))?;
        let req = "GET / HTTP/1.1\r\nHost: credentials\r\nConnection: close\r\n\r\n";
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| format!("requesting credentials from HTTP socket {socket_path}: {e}"))?;
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(|e| format!("requesting credentials from HTTP socket {socket_path}: {e}"))?;

        let raw = String::from_utf8_lossy(&buf);
        let Some((head, body)) = raw.split_once("\r\n\r\n") else {
            return Err(format!(
                "parsing credentials from HTTP socket {socket_path}: malformed HTTP response"
            ));
        };
        let status_line = head.lines().next().unwrap_or("");
        if status_line.split_whitespace().nth(1) != Some("200") {
            return Err(format!(
                "requesting credentials from HTTP socket {socket_path}: unexpected status {status_line}"
            ));
        }
        // Handle chunked transfer encoding if present
        let decoded_body = if head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            decode_http_chunked(body)
        } else {
            body.to_string()
        };
        serde_json::from_str::<T>(&decoded_body).map_err(|e| {
            format!("parsing credentials from HTTP socket {socket_path}: {e}")
        })
    };

    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| format!("requesting credentials from HTTP socket {socket_path}: timeout"))?
}

fn decode_http_chunked(input: &str) -> String {
    let mut out = String::new();
    let mut rest = input;
    while let Some((size_line, after_size)) = rest.split_once("\r\n") {
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let Ok(sz) = usize::from_str_radix(size_hex, 16) else {
            break;
        };
        if sz == 0 {
            break;
        }
        if after_size.len() < sz {
            out.push_str(after_size);
            break;
        }
        out.push_str(&after_size[..sz]);
        rest = after_size[sz..].strip_prefix("\r\n").unwrap_or(&after_size[sz..]);
    }
    out
}

// ---------------------------------------------------------------------------
// Sampled-Only Tracer & Propagator (ported from util.go)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceSpanContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
    pub remote: bool,
}

impl TraceSpanContext {
    pub fn is_valid(&self) -> bool {
        self.trace_id != [0u8; 16] && self.span_id != [0u8; 8]
    }

    pub fn is_sampled(&self) -> bool {
        self.sampled
    }

    pub fn is_remote(&self) -> bool {
        self.remote
    }

    pub fn trace_id_string(&self) -> String {
        hex_encode(&self.trace_id)
    }

    pub fn span_id_string(&self) -> String {
        hex_encode(&self.span_id)
    }

    pub fn to_traceparent(&self) -> String {
        let flags = if self.sampled { "01" } else { "00" };
        format!(
            "00-{}-{}-{}",
            self.trace_id_string(),
            self.span_id_string(),
            flags
        )
    }

    pub fn from_traceparent(header: &str) -> Option<Self> {
        let tc = mimir_common::tracing::parse_traceparent(header)?;
        Some(Self {
            trace_id: tc.trace_id,
            span_id: tc.span_id,
            sampled: tc.sampled,
            remote: tc.remote,
        })
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[derive(Debug, Clone)]
pub struct RecordedSpan {
    pub id: u64,
    pub name: String,
    pub parent_trace_id: Option<[u8; 16]>,
    pub ended: bool,
}

#[derive(Default)]
pub struct SpanRecorder {
    next_id: AtomicU64,
    spans: SyncMutex<Vec<RecordedSpan>>,
}

impl SpanRecorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn start_span(&self, name: impl Into<String>, parent_trace_id: Option<[u8; 16]>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.spans.lock().push(RecordedSpan {
            id,
            name: name.into(),
            parent_trace_id,
            ended: false,
        });
        id
    }

    pub fn end_span(&self, id: u64) {
        let mut guard = self.spans.lock();
        if let Some(span) = guard.iter_mut().find(|s| s.id == id) {
            span.ended = true;
        }
    }

    pub fn started(&self) -> Vec<RecordedSpan> {
        self.spans.lock().clone()
    }

    pub fn ended(&self) -> Vec<RecordedSpan> {
        self.spans
            .lock()
            .iter()
            .filter(|s| s.ended)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TracedKafkaRecord {
    pub topic: String,
    pub headers: Vec<(String, Bytes)>,
    pub span_ctx: Option<TraceSpanContext>,
    pub tracestate: Option<String>,
    pub active_span_id: Option<u64>,
}

/// Propagation wrapper that only injects W3C TraceContext (`traceparent` + `tracestate`)
/// when the span context is sampled, matching Go `recordsPropagator()`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SampledOnlyPropagator;

const KAFKA_TRACECONTEXT_PROPAGATORS: mimir_common::tracing::PropagatorSet =
    mimir_common::tracing::PropagatorSet {
        tracecontext: true,
        baggage: false,
        jaeger: false,
    };

impl SampledOnlyPropagator {
    pub fn inject(&self, ctx: Option<&TraceSpanContext>, headers: &mut Vec<(String, Bytes)>) {
        if let Some(sc) = ctx {
            if sc.is_sampled() {
                headers.push((
                    "traceparent".to_string(),
                    Bytes::from(sc.to_traceparent()),
                ));
            }
        }
    }

    pub fn inject_trace_context(
        &self,
        ctx: Option<&mimir_common::tracing::TraceContext>,
        headers: &mut Vec<(String, Bytes)>,
    ) {
        if let Some(tc) = ctx {
            if tc.is_sampled() {
                headers.push(("traceparent".to_string(), Bytes::from(tc.to_traceparent())));
                if let Some(ref ts) = tc.tracestate {
                    if !ts.is_empty() {
                        headers.push(("tracestate".to_string(), Bytes::from(ts.clone())));
                    }
                }
            }
        }
    }

    pub fn extract(&self, headers: &[(String, Bytes)]) -> Option<TraceSpanContext> {
        let tc = self.extract_trace_context(headers)?;
        Some(TraceSpanContext {
            trace_id: tc.trace_id,
            span_id: tc.span_id,
            sampled: tc.sampled,
            remote: tc.remote,
        })
    }

    pub fn extract_trace_context(
        &self,
        headers: &[(String, Bytes)],
    ) -> Option<mimir_common::tracing::TraceContext> {
        let tc = mimir_common::tracing::extract_from_headers(
            headers
                .iter()
                .filter_map(|(k, v)| std::str::from_utf8(v).ok().map(|vs| (k.as_str(), vs))),
            &KAFKA_TRACECONTEXT_PROPAGATORS,
        );
        tc.is_valid().then_some(tc)
    }
}

/// Hook wrapper skipping span creation and header injection for unsampled traces on produce & fetch.
pub struct SampledOnlyTracer {
    recorder: Arc<SpanRecorder>,
    propagator: SampledOnlyPropagator,
}

impl SampledOnlyTracer {
    pub fn new(recorder: Arc<SpanRecorder>) -> Self {
        Self {
            recorder,
            propagator: SampledOnlyPropagator,
        }
    }

    pub fn on_produce_record_buffered(&self, rec: &mut TracedKafkaRecord) {
        let Some(sc) = rec.span_ctx else {
            return;
        };
        if !sc.is_sampled() {
            return;
        }
        let span_id = self
            .recorder
            .start_span(format!("{} publish", rec.topic), Some(sc.trace_id));
        rec.active_span_id = Some(span_id);
        self.propagator.inject(Some(&sc), &mut rec.headers);
        if let Some(ref ts) = rec.tracestate {
            if !ts.is_empty() {
                rec.headers.push(("tracestate".to_string(), Bytes::from(ts.clone())));
            }
        }
    }

    pub fn on_produce_record_unbuffered(&self, rec: &mut TracedKafkaRecord) {
        if !rec.span_ctx.is_some_and(|sc| sc.is_sampled()) {
            return;
        }
        if let Some(id) = rec.active_span_id.take() {
            self.recorder.end_span(id);
        }
    }

    pub fn on_fetch_record_buffered(&self, rec: &mut TracedKafkaRecord) {
        if rec.span_ctx.is_none() {
            rec.span_ctx = Some(TraceSpanContext::default());
        }
        if let Some(tc) = self.propagator.extract_trace_context(&rec.headers) {
            rec.tracestate = tc.tracestate.clone();
            let extracted = TraceSpanContext {
                trace_id: tc.trace_id,
                span_id: tc.span_id,
                sampled: tc.sampled,
                remote: tc.remote,
            };
            rec.span_ctx = Some(extracted);
            if !extracted.is_sampled() {
                return;
            }
            let span_id = self
                .recorder
                .start_span(format!("{} receive", rec.topic), Some(extracted.trace_id));
            rec.active_span_id = Some(span_id);
        }
    }

    pub fn on_fetch_record_unbuffered(&self, rec: &mut TracedKafkaRecord, _polled: bool) {
        if !rec.span_ctx.is_some_and(|sc| sc.is_sampled()) {
            return;
        }
        if let Some(id) = rec.active_span_id.take() {
            self.recorder.end_span(id);
        }
    }
}

// ---------------------------------------------------------------------------
// Extended Kafka Client Metrics (ported from metrics.go / writer_client.go)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct KafkaClientExtendedMetrics {
    pub kafka_write_wait_seconds: prometheus::Histogram,
    pub kafka_write_time_seconds: prometheus::Histogram,
    pub kafka_read_wait_seconds: prometheus::Histogram,
    pub kafka_read_time_seconds: prometheus::Histogram,
    pub kafka_request_duration_e2e_seconds: prometheus::Histogram,
    pub kafka_request_throttled_seconds: prometheus::Histogram,
    pub connects_total: prometheus::Counter,
    pub read_bytes_total: prometheus::Counter,
    pub write_bytes_total: prometheus::Counter,
    pub produce_records_total: prometheus::Counter,
    pub produce_batches_total: prometheus::Counter,
    pub produce_bytes_total: prometheus::Counter,
    pub produce_compressed_bytes_total: prometheus::Counter,
    pub buffered_produce_records_total: prometheus::Gauge,
    pub buffered_produce_bytes: prometheus::Gauge,
    pub buffered_fetch_records_total: prometheus::Gauge,
    pub buffered_fetch_bytes: prometheus::Gauge,
}

impl KafkaClientExtendedMetrics {
    pub fn new(prefix: &str, registry: Option<&prometheus::Registry>, component: Option<&str>) -> Self {
        let mut const_labels = HashMap::new();
        if let Some(c) = component {
            const_labels.insert("component".to_string(), c.to_string());
        }
        let reg_hist = |name: &str, help: &str| {
            let mut opts = prometheus::HistogramOpts::new(format!("{prefix}{name}"), help);
            opts.common_opts.const_labels = const_labels.clone();
            let h = prometheus::Histogram::with_opts(opts).expect("valid metric");
            if let Some(r) = registry {
                let _ = r.register(Box::new(h.clone()));
            }
            h
        };
        let reg_cnt = |name: &str, help: &str| {
            let mut opts = prometheus::Opts::new(format!("{prefix}{name}"), help);
            opts.const_labels = const_labels.clone();
            let c = prometheus::Counter::with_opts(opts).expect("valid metric");
            if let Some(r) = registry {
                let _ = r.register(Box::new(c.clone()));
            }
            c
        };
        let reg_gauge = |name: &str, help: &str| {
            let mut opts = prometheus::Opts::new(format!("{prefix}{name}"), help);
            opts.const_labels = const_labels.clone();
            let g = prometheus::Gauge::with_opts(opts).expect("valid metric");
            if let Some(r) = registry {
                let _ = r.register(Box::new(g.clone()));
            }
            g
        };

        Self {
            kafka_write_wait_seconds: reg_hist(
                "kafka_write_wait_seconds",
                "Time spent waiting to write a Kafka request.",
            ),
            kafka_write_time_seconds: reg_hist(
                "kafka_write_time_seconds",
                "Time spent writing a Kafka request.",
            ),
            kafka_read_wait_seconds: reg_hist(
                "kafka_read_wait_seconds",
                "Time spent waiting to read a Kafka response.",
            ),
            kafka_read_time_seconds: reg_hist(
                "kafka_read_time_seconds",
                "Time spent reading a Kafka response.",
            ),
            kafka_request_duration_e2e_seconds: reg_hist(
                "kafka_request_duration_e2e_seconds",
                "End-to-end duration of a Kafka request.",
            ),
            kafka_request_throttled_seconds: reg_hist(
                "kafka_request_throttled_seconds",
                "Time a Kafka request was throttled by the broker.",
            ),
            connects_total: reg_cnt("connects_total", "Total number of Kafka broker connects."),
            read_bytes_total: reg_cnt("read_bytes_total", "Total bytes read from Kafka."),
            write_bytes_total: reg_cnt("write_bytes_total", "Total bytes written to Kafka."),
            produce_records_total: reg_cnt(
                "produce_records_total",
                "Total number of produced Kafka records.",
            ),
            produce_batches_total: reg_cnt(
                "produce_batches_total",
                "Total number of produced Kafka record batches.",
            ),
            produce_bytes_total: reg_cnt(
                "produce_bytes_total",
                "Total uncompressed bytes of produced records.",
            ),
            produce_compressed_bytes_total: reg_cnt(
                "produce_compressed_bytes_total",
                "Total compressed bytes of produced batches.",
            ),
            buffered_produce_records_total: reg_gauge(
                "buffered_produce_records_total",
                "Total number of records buffered within the client ready to be produced.",
            ),
            buffered_produce_bytes: reg_gauge(
                "buffered_produce_bytes",
                "Total bytes buffered within the client ready to be produced.",
            ),
            buffered_fetch_records_total: reg_gauge(
                "buffered_fetch_records_total",
                "Total number of records buffered within the client ready to be consumed.",
            ),
            buffered_fetch_bytes: reg_gauge(
                "buffered_fetch_bytes",
                "Total bytes buffered within the client ready to be consumed.",
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// CreateTopics (ported from CreateTopics in util.go)
// ---------------------------------------------------------------------------

/// Creates the given topics in the Kafka cluster.
/// A topic that already exists (`TopicAlreadyExists`) is treated as success.
/// Missing topics from the broker response or any other error cause an error to be returned.
///
/// Like upstream, the request goes through a client built from the common Kafka client options
/// (`commonKafkaClientOptions`: default 10s request timeout overhead, Mimir's retry timeouts) and
/// is issued as a kadm admin request: to the controller, retried by the kgo retry policy (see
/// [`super::kgo_retry`]). While the broker is unreachable but the failure is transient (dial
/// timeouts, unresponsive / paused broker) this keeps retrying for up to the 30s retry timeout;
/// a single bad seed (connection refused, DNS NXDOMAIN) fails fast.
pub async fn create_topics(cfg: &KafkaConfig, topics: &[&str]) -> Result<(), String> {
    let client = super::kafka_client::KafkaClient::from_config(cfg)
        .map_err(|e| format!("failed to create kafka client: {e}"))?
        .with_request_timeout(super::kgo_retry::DEFAULT_REQUEST_TIMEOUT_OVERHEAD);

    // As of kafka 2.4 we can pass -1 and the broker will use its default configuration.
    const DEFAULT_REPLICATION: i16 = -1;
    let resp = client
        .create_topics_request(
            topics,
            cfg.auto_create_topic_default_partitions as i32,
            DEFAULT_REPLICATION,
        )
        .await
        .map_err(|e| format!("failed to create topics [{}]: {e}", topics.join(" ")))?;

    let mut by_name: HashMap<String, &kafka_protocol::messages::create_topics_response::CreatableTopicResult> =
        HashMap::new();
    for t in &resp.topics {
        by_name.insert(t.name.0.as_str().to_owned(), t);
    }

    for topic in topics {
        let Some(t) = by_name.get(*topic) else {
            // The broker should return a response for every requested topic; a missing one means we
            // can't confirm it was created, so fail rather than report a success we didn't observe.
            return Err(format!(
                "failed to create topic {topic}: not part of the create topics response"
            ));
        };
        if let Some(err) = t.error_code.err() {
            if err == ResponseError::TopicAlreadyExists {
                tracing::info!(task = "autocreate_topic", topic = %topic, "skipped Kafka topic creation because it already exists");
                continue;
            }
            return Err(format!("failed to create topic {topic}: {err:?}"));
        }
        tracing::info!(
            task = "autocreate_topic",
            topic = %topic,
            num_partitions = t.num_partitions,
            replication_factor = t.replication_factor,
            "successfully created Kafka topic"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// TLS- and SASL-capable Kafka Client (unified onto super::kafka_client::KafkaClient)
// ---------------------------------------------------------------------------

/// TLS- and SASL-capable async Kafka client delegating connection pooling, SASL lifecycle,
/// singleflight reconnect, task cleanup, and kgo retries to [`super::kafka_client::KafkaClient`],
/// with extended client metrics.
pub struct IngestKafkaClient {
    inner: super::kafka_client::KafkaClient,
}

impl IngestKafkaClient {
    pub fn from_config(
        cfg: &KafkaConfig,
        metrics: Option<KafkaClientExtendedMetrics>,
    ) -> Result<Self, KafkaClientError> {
        let write_timeout = Duration::from_nanos(cfg.write_timeout.as_nanos().max(0) as u64);
        let overhead =
            Duration::from_nanos(cfg.write_timeout_overhead.as_nanos().max(0) as u64);
        let request_timeout = (write_timeout + overhead).max(Duration::from_secs(5));
        let inner = super::kafka_client::KafkaClient::from_config(cfg)?
            .with_request_timeout(request_timeout)
            .with_extended_metrics(metrics);
        Ok(Self { inner })
    }

    pub fn extended_metrics(&self) -> Option<&KafkaClientExtendedMetrics> {
        self.inner.extended_metrics()
    }

    /// Verifies broker reachability and SASL authentication by connecting and fetching metadata.
    pub async fn ping(&self) -> Result<(), KafkaClientError> {
        self.inner.ping().await
    }

    pub async fn refresh_metadata(&self, topics: &[&str]) -> Result<(), KafkaClientError> {
        self.inner.refresh_metadata(topics).await
    }

    pub async fn list_topics(&self) -> Result<Vec<String>, KafkaClientError> {
        self.inner.list_topics().await
    }

    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        compression: KafkaCompression,
        records: Vec<ProduceRecord>,
    ) -> Result<i64, KafkaClientError> {
        self.inner
            .produce(topic, partition, compression, records)
            .await
    }

    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        max_wait: Duration,
        min_bytes: i32,
        max_bytes: i32,
    ) -> Result<FetchPartitionResult, KafkaClientError> {
        self.inner
            .fetch(topic, partition, fetch_offset, max_wait, min_bytes, max_bytes)
            .await
    }

    pub async fn list_offset_with_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<ListedOffset, KafkaClientError> {
        self.inner
            .list_offset_with_timestamp(topic, partition, timestamp)
            .await
    }

    pub async fn fetch_partition_start_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, KafkaClientError> {
        self.inner.fetch_partition_start_offset(topic, partition).await
    }

    pub async fn fetch_partition_last_produced_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, KafkaClientError> {
        self.inner
            .fetch_partition_last_produced_offset(topic, partition)
            .await
    }

    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), KafkaClientError> {
        self.inner
            .commit_offset(group_id, topic, partition, offset)
            .await
    }

    pub async fn fetch_committed_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, KafkaClientError> {
        self.inner
            .fetch_committed_offset(group_id, topic, partition)
            .await
    }

    pub async fn close(&self) {
        self.inner.close().await;
    }
}

// ---------------------------------------------------------------------------
// Minimal Pure-Rust SHA-256 / SHA-512 / HMAC / PBKDF2 / Base64 for SCRAM
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub enum ScramAlg {
    Sha256,
    Sha512,
}

impl ScramAlg {
    pub fn hash(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => sha256(data).to_vec(),
            Self::Sha512 => sha512(data).to_vec(),
        }
    }

    pub fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let block_size = match self {
            Self::Sha256 => 64,
            Self::Sha512 => 128,
        };
        let mut k = if key.len() > block_size {
            self.hash(key)
        } else {
            key.to_vec()
        };
        k.resize(block_size, 0);
        let mut ipad = vec![0x36u8; block_size];
        let mut opad = vec![0x5cu8; block_size];
        for i in 0..block_size {
            ipad[i] ^= k[i];
            opad[i] ^= k[i];
        }
        ipad.extend_from_slice(data);
        let inner = self.hash(&ipad);
        opad.extend_from_slice(&inner);
        self.hash(&opad)
    }

    pub fn pbkdf2(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut msg = Vec::with_capacity(salt.len() + 4);
        msg.extend_from_slice(salt);
        msg.extend_from_slice(&1u32.to_be_bytes());
        let mut u = self.hmac(password, &msg);
        let mut out = u.clone();
        for _ in 1..iterations {
            u = self.hmac(password, &u);
            for (o, b) in out.iter_mut().zip(u.iter()) {
                *o ^= *b;
            }
        }
        out
    }
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for block in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, ch) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes([ch[0], ch[1], ch[2], ch[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (hv, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *hv = hv.wrapping_add(v);
        }
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

pub fn sha512(data: &[u8]) -> [u8; 64] {
    const K: [u64; 80] = [
        0x428a2f98d728ae22, 0x7137449123ef65cd, 0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
        0x3956c25bf348b538, 0x59f111f1b605d019, 0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
        0xd807aa98a3030242, 0x12835b0145706fbe, 0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
        0x72be5d74f27b896f, 0x80deb1fe3b1696b1, 0x9bdc06a725c71235, 0xc19bf174cf692694,
        0xe49b69c19ef14ad2, 0xefbe4786384f25e3, 0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
        0x2de92c6f592b0275, 0x4a7484aa6ea6e483, 0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
        0x983e5152ee66dfab, 0xa831c66d2db43210, 0xb00327c898fb213f, 0xbf597fc7beef0ee4,
        0xc6e00bf33da88fc2, 0xd5a79147930aa725, 0x06ca6351e003826f, 0x142929670a0e6e70,
        0x27b70a8546d22ffc, 0x2e1b21385c26c926, 0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
        0x650a73548baf63de, 0x766a0abb3c77b2a8, 0x81c2c92e47edaee6, 0x92722c851482353b,
        0xa2bfe8a14cf10364, 0xa81a664bbc423001, 0xc24b8b70d0f89791, 0xc76c51a30654be30,
        0xd192e819d6ef5218, 0xd69906245565a910, 0xf40e35855771202a, 0x106aa07032bbd1b8,
        0x19a4c116b8d2d0c8, 0x1e376c085141ab53, 0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
        0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb, 0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
        0x748f82ee5defb2fc, 0x78a5636f43172f60, 0x84c87814a1f0ab72, 0x8cc702081a6439ec,
        0x90befffa23631e28, 0xa4506cebde82bde9, 0xbef9a3f7b2c67915, 0xc67178f2e372532b,
        0xca273eceea26619c, 0xd186b8c721c0c207, 0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
        0x06f067aa72176fba, 0x0a637dc5a2c898a6, 0x113f9804bef90dae, 0x1b710b35131c471b,
        0x28db77f523047d84, 0x32caab7b40c72493, 0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
        0x4cc5d4becb3e42b6, 0x597f299cfc657e2a, 0x5fcb6fab3ad6faec, 0x6c44198c4a475817,
    ];
    let mut h: [u64; 8] = [
        0x6a09e667f3bcc908, 0xbb67ae8584caa73b, 0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
        0x510e527fade682d1, 0x9b05688c2b3e6c1f, 0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
    ];
    let bit_len = (data.len() as u128).wrapping_mul(8);
    let mut padded = Vec::with_capacity(data.len() + 144);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 128 != 112 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for block in padded.as_chunks::<128>().0 {
        let mut w = [0u64; 80];
        for (i, ch) in block.as_chunks::<8>().0.iter().enumerate() {
            w[i] = u64::from_be_bytes([
                ch[0], ch[1], ch[2], ch[3], ch[4], ch[5], ch[6], ch[7],
            ]);
        }
        for i in 16..80 {
            let s0 = w[i - 15].rotate_right(1) ^ w[i - 15].rotate_right(8) ^ (w[i - 15] >> 7);
            let s1 = w[i - 2].rotate_right(19) ^ w[i - 2].rotate_right(61) ^ (w[i - 2] >> 6);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..80 {
            let s1 = e.rotate_right(14) ^ e.rotate_right(18) ^ e.rotate_right(41);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(28) ^ a.rotate_right(34) ^ a.rotate_right(39);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (hv, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *hv = hv.wrapping_add(v);
        }
    }
    let mut out = [0u8; 64];
    for (i, v) in h.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&v.to_be_bytes());
    }
    out
}

pub fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub fn base64_decode(input: &str) -> Result<Vec<u8>, &'static str> {
    let bytes = input.trim().as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err("invalid base64 length");
    }
    let val = |b: u8| -> Result<u32, &'static str> {
        match b {
            b'A'..=b'Z' => Ok((b - b'A') as u32),
            b'a'..=b'z' => Ok((b - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((b - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err("invalid base64 byte"),
        }
    };
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3);
    for chunk in bytes.as_chunks::<4>().0 {
        let a = val(chunk[0])?;
        let b = val(chunk[1])?;
        let c = if chunk[2] == b'=' { 0 } else { val(chunk[2])? };
        let d = if chunk[3] == b'=' { 0 } else { val(chunk[3])? };
        let n = (a << 18) | (b << 12) | (c << 6) | d;
        out.push((n >> 16) as u8);
        if chunk[2] != b'=' {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if chunk[3] != b'=' {
            out.push((n & 0xff) as u8);
        }
    }
    Ok(out)
}
