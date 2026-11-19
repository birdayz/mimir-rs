//! Distributed tracing context extraction, propagation, sampling, and OTel/Jaeger env setup.
//!
//! Ported from:
//! - github.com/grafana/dskit/tracing/tracing.go
//! - github.com/grafana/dskit/tracing/otel.go
//! - github.com/grafana/dskit/tracing/otel_jaeger.go
//! - go.opentelemetry.io/contrib/propagators/jaeger/jaeger_propagator.go

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::RngCore;

use crate::logging::{LogLevel, Logger};

tokio::task_local! {
    static CURRENT_TRACE: TraceContext;
}

/// Extracted or active OpenTelemetry / Jaeger span context.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
    pub trace_flags: u8,
    pub remote: bool,
    pub jaeger_debug_id: Option<String>,
    pub tracestate: Option<String>,
}

impl TraceContext {
    pub fn is_valid(&self) -> bool {
        self.trace_id != [0u8; 16] && self.span_id != [0u8; 8]
    }

    pub fn is_sampled(&self) -> bool {
        self.is_valid() && self.sampled
    }

    pub fn trace_id_hex(&self) -> String {
        hex_encode(&self.trace_id)
    }

    pub fn span_id_hex(&self) -> String {
        hex_encode(&self.span_id)
    }

    pub fn to_traceparent(&self) -> String {
        let mut flags = self.trace_flags & 0x03;
        if self.sampled {
            flags |= 0x01;
        } else {
            flags &= !0x01;
        }
        format!("00-{}-{}-{flags:02x}", self.trace_id_hex(), self.span_id_hex())
    }

    pub fn to_uber_trace_id(&self) -> String {
        let flags = match (self.sampled, self.jaeger_debug_id.is_some()) {
            (true, true) => "3",
            (true, false) => "1",
            (false, _) => "0",
        };
        format!("{}:{}:0:{flags}", self.trace_id_hex(), self.span_id_hex())
    }

    /// dskit/tracing.ExtractTraceID.
    pub fn extract_trace_id(&self) -> Option<String> {
        self.is_valid().then(|| self.trace_id_hex())
    }

    /// dskit/tracing.ExtractSampledTraceID.
    pub fn extract_sampled_trace_id(&self) -> (String, bool) {
        if self.is_valid() {
            (self.trace_id_hex(), self.sampled)
        } else {
            (String::new(), false)
        }
    }
}

/// Runs fut with ctx bound as the task-local current TraceContext.
pub async fn with_trace_context<F: Future>(ctx: TraceContext, fut: F) -> F::Output {
    CURRENT_TRACE.scope(ctx, fut).await
}

/// Returns the task-local TraceContext if valid.
pub fn current_trace_context() -> Option<TraceContext> {
    CURRENT_TRACE
        .try_with(|c| c.clone())
        .ok()
        .filter(TraceContext::is_valid)
}

/// Returns the current valid trace ID if any (tracing.ExtractTraceID).
pub fn current_trace_id() -> Option<String> {
    current_trace_context().and_then(|c| c.extract_trace_id())
}

/// Returns (trace_id, sampled) for the current task-local context (tracing.ExtractSampledTraceID).
pub fn current_sampled_trace_id() -> (String, bool) {
    current_trace_context()
        .map(|c| c.extract_sampled_trace_id())
        .unwrap_or_default()
}

#[derive(Clone, Debug)]
pub struct SpanAttribute {
    pub key: String,
    pub value: SpanAttrValue,
}

#[derive(Clone, Debug)]
pub enum SpanAttrValue {
    String(String),
    StringArray(Vec<String>),
    Bool(bool),
    Int(i64),
}

#[derive(Clone, Debug)]
pub struct ExportedSpan {
    pub service_name: String,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: u8, // 2 = SERVER, 5 = CONSUMER
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: Vec<SpanAttribute>,
    pub status_error: bool,
}

pub type SpanExporterFn = Arc<dyn Fn(ExportedSpan) + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SamplerKind {
    AlwaysOn,
    AlwaysOff,
    TraceIdRatio(u64),
    #[default]
    ParentBasedAlwaysOn,
    ParentBasedAlwaysOff,
    ParentBasedTraceIdRatio(u64),
}

impl SamplerKind {
    fn ratio_from_f64(r: f64) -> u64 {
        if !(r.is_finite()) || r <= 0.0 {
            0
        } else if r >= 1.0 {
            u64::MAX
        } else {
            (r * (u64::MAX as f64)) as u64
        }
    }

    fn should_sample(self, parent: &TraceContext, trace_id: &[u8; 16]) -> bool {
        if parent.jaeger_debug_id.is_some() {
            return true;
        }
        let ratio_sample = |threshold: u64| -> bool {
            if threshold == 0 {
                return false;
            }
            if threshold == u64::MAX {
                return true;
            }
            let val = u64::from_be_bytes(trace_id[8..16].try_into().unwrap_or([0; 8]));
            val <= threshold
        };
        match self {
            SamplerKind::AlwaysOn => true,
            SamplerKind::AlwaysOff => false,
            SamplerKind::TraceIdRatio(t) => ratio_sample(t),
            SamplerKind::ParentBasedAlwaysOn => {
                if parent.is_valid() {
                    parent.sampled
                } else {
                    true
                }
            }
            SamplerKind::ParentBasedAlwaysOff => {
                if parent.is_valid() {
                    parent.sampled
                } else {
                    false
                }
            }
            SamplerKind::ParentBasedTraceIdRatio(t) => {
                if parent.is_valid() {
                    parent.sampled
                } else {
                    ratio_sample(t)
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PropagatorSet {
    pub tracecontext: bool,
    pub baggage: bool,
    pub jaeger: bool,
}

impl Default for PropagatorSet {
    fn default() -> Self {
        Self {
            tracecontext: true,
            baggage: true,
            jaeger: true,
        }
    }
}

impl PropagatorSet {
    pub fn from_env() -> Self {
        let Ok(raw) = std::env::var("OTEL_PROPAGATORS") else {
            return Self::default();
        };
        if raw.is_empty() {
            return Self::default();
        }
        let mut set = Self {
            tracecontext: false,
            baggage: false,
            jaeger: false,
        };
        for p in raw.split(',') {
            match p.trim() {
                "tracecontext" => set.tracecontext = true,
                "baggage" => set.baggage = true,
                "jaeger" => set.jaeger = true,
                "none" => {
                    return Self {
                        tracecontext: false,
                        baggage: false,
                        jaeger: false,
                    };
                }
                _ => {}
            }
        }
        set
    }
}

#[derive(Clone)]
pub struct TracingRuntime {
    pub enabled: bool,
    pub service_name: String,
    pub propagators: PropagatorSet,
    pub sampler: SamplerKind,
    pub resource_attributes: Vec<SpanAttribute>,
    pub exporter: Option<SpanExporterFn>,
}

impl Default for TracingRuntime {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "mimir".to_string(),
            propagators: PropagatorSet::default(),
            sampler: SamplerKind::ParentBasedAlwaysOn,
            resource_attributes: Vec::new(),
            exporter: None,
        }
    }
}

static GLOBAL_TRACING: OnceLock<TracingRuntime> = OnceLock::new();

pub fn global_tracing() -> Option<&'static TracingRuntime> {
    GLOBAL_TRACING.get()
}

pub fn is_tracing_enabled() -> bool {
    GLOBAL_TRACING.get().is_some_and(|t| t.enabled)
}

/// Initializes process-wide tracing from OTEL_* / JAEGER_* environment variables, matching
/// dskit/tracing.NewOTelOrJaegerFromEnv.
pub fn init_otel_or_jaeger_from_env(
    service_name: &str,
    logger: &dyn Logger,
    exporter: Option<SpanExporterFn>,
) -> Result<&'static TracingRuntime, String> {
    if let Some(rt) = GLOBAL_TRACING.get() {
        return Ok(rt);
    }

    let rt = build_tracing_runtime_from_env(service_name, logger, exporter)?;
    let _ = GLOBAL_TRACING.set(rt);
    GLOBAL_TRACING
        .get()
        .ok_or_else(|| "tracing initialization failed".to_string())
}

pub fn build_tracing_runtime_from_env(
    service_name: &str,
    logger: &dyn Logger,
    exporter: Option<SpanExporterFn>,
) -> Result<TracingRuntime, String> {
    let find_env = |keys: &[&'static str]| -> Option<(&'static str, String)> {
        for &k in keys {
            if let Ok(v) = std::env::var(k) {
                if !v.is_empty() {
                    return Some((k, v));
                }
            }
        }
        None
    };

    if let Some((env_key, _)) = find_env(&[
        "JAEGER_AGENT_HOST",
        "JAEGER_ENDPOINT",
        "JAEGER_SAMPLER_MANAGER_HOST_PORT",
    ]) {
        logger.log(
            LogLevel::Info,
            &[
                ("msg", "Configuring tracing with Jaeger exporter"),
                ("detected_env_var", env_key),
            ],
        );
        let sampler = parse_jaeger_sampler_from_env()?;
        let tags = parse_jaeger_tags(&std::env::var("JAEGER_TAGS").unwrap_or_default())?;
        let propagators = PropagatorSet::from_env();
        logger.log(
            LogLevel::Debug,
            &[(
                "msg",
                "OpenTelemetry tracer provider initialized from Jaeger env vars",
            )],
        );
        return Ok(TracingRuntime {
            enabled: true,
            service_name: service_name.to_string(),
            propagators,
            sampler,
            resource_attributes: tags,
            exporter,
        });
    }

    if let Some((env_key, _)) = find_env(&[
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]) {
        logger.log(
            LogLevel::Info,
            &[
                ("msg", "Configuring OpenTelemetry tracing"),
                ("detected_env_var", env_key),
            ],
        );
        logger.log(LogLevel::Info, &[("msg", "initialising OpenTelemetry tracer")]);
        let sampler = parse_otel_sampler_from_env()?;
        let propagators = PropagatorSet::from_env();
        logger.log(
            LogLevel::Debug,
            &[(
                "msg",
                "OpenTelemetry tracer provider initialized from OTel environment variables",
            )],
        );
        return Ok(TracingRuntime {
            enabled: true,
            service_name: service_name.to_string(),
            propagators,
            sampler,
            resource_attributes: Vec::new(),
            exporter,
        });
    }

    Ok(TracingRuntime {
        enabled: false,
        service_name: service_name.to_string(),
        propagators: PropagatorSet::default(),
        sampler: SamplerKind::ParentBasedAlwaysOn,
        resource_attributes: Vec::new(),
        exporter: None,
    })
}

fn parse_otel_sampler_from_env() -> Result<SamplerKind, String> {
    let name = std::env::var("OTEL_TRACES_SAMPLER")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let arg = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .unwrap_or_default()
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|v| (0.0..=1.0).contains(v))
        .unwrap_or(1.0);
    Ok(match name.as_str() {
        "always_on" => SamplerKind::AlwaysOn,
        "always_off" => SamplerKind::AlwaysOff,
        "traceidratio" => SamplerKind::TraceIdRatio(SamplerKind::ratio_from_f64(arg)),
        "parentbased_always_off" => SamplerKind::ParentBasedAlwaysOff,
        "parentbased_traceidratio" => {
            SamplerKind::ParentBasedTraceIdRatio(SamplerKind::ratio_from_f64(arg))
        }
        "" | "parentbased_always_on" => SamplerKind::ParentBasedAlwaysOn,
        _ => SamplerKind::ParentBasedAlwaysOn,
    })
}

fn parse_jaeger_sampler_from_env() -> Result<SamplerKind, String> {
    if let Ok(ep) = std::env::var("JAEGER_ENDPOINT") {
        if !ep.is_empty() && !ep.contains("://") {
            return Err(format!("cannot parse env var JAEGER_ENDPOINT={ep}: invalid URI"));
        }
    }
    let stype = std::env::var("JAEGER_SAMPLER_TYPE").unwrap_or_default();
    let param = match std::env::var("JAEGER_SAMPLER_PARAM") {
        Ok(s) if !s.is_empty() => s
            .parse::<f64>()
            .map_err(|e| format!("cannot parse env var JAEGER_SAMPLER_PARAM={s}: {e}"))?,
        _ => 1.0,
    };
    let not_parent_based = match std::env::var("JAEGER_SAMPLER_NOT_PARENT_BASED") {
        Ok(s) if !s.is_empty() => matches!(s.as_str(), "1" | "t" | "T" | "true" | "TRUE" | "True"),
        _ => false,
    };
    let base = match stype.as_str() {
        "" => SamplerKind::AlwaysOn,
        "const" => {
            if param == 0.0 {
                SamplerKind::AlwaysOff
            } else {
                SamplerKind::AlwaysOn
            }
        }
        "probabilistic" | "remote" => {
            SamplerKind::TraceIdRatio(SamplerKind::ratio_from_f64(param))
        }
        other => return Err(format!("unknown sampler type {other:?}")),
    };
    if not_parent_based {
        Ok(base)
    } else {
        Ok(match base {
            SamplerKind::AlwaysOn => SamplerKind::ParentBasedAlwaysOn,
            SamplerKind::AlwaysOff => SamplerKind::ParentBasedAlwaysOff,
            SamplerKind::TraceIdRatio(t) => SamplerKind::ParentBasedTraceIdRatio(t),
            other => other,
        })
    }
}

fn parse_jaeger_tags(raw: &str) -> Result<Vec<SpanAttribute>, String> {
    let mut out = Vec::new();
    for p in raw.split(',') {
        if let Some((k, v)) = p.split_once('=') {
            let k = k.trim();
            let mut v = v.trim().to_string();
            if v.starts_with("${") && v.ends_with('}') {
                let inner = &v[2..v.len() - 1];
                let (env_k, def) = inner.split_once(':').unwrap_or((inner, ""));
                let resolved = std::env::var(env_k).unwrap_or_default();
                v = if resolved.is_empty() && !def.is_empty() {
                    def.to_string()
                } else {
                    resolved
                };
            }
            if v.is_empty() {
                return Err(format!("invalid tag {p:?}, expected key=value"));
            }
            out.push(SpanAttribute {
                key: k.to_string(),
                value: SpanAttrValue::String(v),
            });
        } else if !p.is_empty() {
            return Err(format!("invalid tag {p:?}, expected key=value"));
        }
    }
    Ok(out)
}

/// Parses W3C traceparent (00-<32hex>-<16hex>-<2hex>).
pub fn parse_traceparent(header: &str) -> Option<TraceContext> {
    let header = header.trim();
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() < 4 {
        return None;
    }
    let version = parts[0];
    if version.len() != 2 || !version.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
        return None;
    }
    if version == "ff" {
        return None;
    }
    if version == "00" && parts.len() != 4 {
        return None;
    }
    let trace_id = decode_lower_hex_16(parts[1])?;
    let span_id = decode_lower_hex_8(parts[2])?;
    if parts[3].len() != 2 || !parts[3].bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
        return None;
    }
    let flags = u8::from_str_radix(parts[3], 16).ok()?;
    if version == "00" && flags > 3 {
        return None;
    }
    let trace_flags = flags & 0x03;
    let sc = TraceContext {
        trace_id,
        span_id,
        sampled: (trace_flags & 0x01) != 0,
        trace_flags,
        remote: true,
        jaeger_debug_id: None,
        tracestate: None,
    };
    sc.is_valid().then_some(sc)
}

/// Parses Jaeger uber-trace-id ({trace-id}:{span-id}:{parent-span-id}:{flags}),
/// left-padding 64-bit trace IDs to 32 hex chars per Go Jaeger propagator.
pub fn parse_uber_trace_id(header: &str) -> Option<TraceContext> {
    let parts: Vec<&str> = header.trim().split(':').collect();
    if parts.len() != 4 {
        return None;
    }
    let tid_raw = parts[0];
    if tid_raw.is_empty() || tid_raw.len() > 32 || !tid_raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let tid_padded = if tid_raw.len() < 32 {
        format!("{}{}", "0".repeat(32 - tid_raw.len()), tid_raw)
    } else {
        tid_raw.to_string()
    };
    let trace_id = decode_hex_16(&tid_padded)?;

    let sid_raw = parts[1];
    if sid_raw.is_empty() || sid_raw.len() > 16 || !sid_raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let sid_padded = if sid_raw.len() < 16 {
        format!("{}{}", "0".repeat(16 - sid_raw.len()), sid_raw)
    } else {
        sid_raw.to_string()
    };
    let span_id = decode_hex_8(&sid_padded)?;

    if parts[3].is_empty() {
        return None;
    }
    let flags = i64::from_str_radix(parts[3], 16).ok()?;
    let sampled = (flags & 0x01) == 0x01;
    let debug = sampled && (flags & 0x02) == 0x02;

    let sc = TraceContext {
        trace_id,
        span_id,
        sampled,
        trace_flags: if sampled { 0x01 } else { 0x00 },
        remote: true,
        jaeger_debug_id: debug.then(|| "1".to_string()),
        tracestate: None,
    };
    sc.is_valid().then_some(sc)
}

/// Extracts TraceContext from case-insensitive header pairs using Go's composite propagator
/// order (tracecontext, baggage, jaeger + Jaeger-Debug-Id).
pub fn extract_from_headers<'a>(
    headers: impl Iterator<Item = (&'a str, &'a str)>,
    propagators: &PropagatorSet,
) -> TraceContext {
    let mut traceparent: Option<&str> = None;
    let mut tracestate: Option<&str> = None;
    let mut uber_trace_id: Option<&str> = None;
    let mut jaeger_debug_id: Option<&str> = None;

    for (k, v) in headers {
        if k.eq_ignore_ascii_case("traceparent") && traceparent.is_none() {
            traceparent = Some(v);
        } else if k.eq_ignore_ascii_case("tracestate") && tracestate.is_none() {
            tracestate = Some(v);
        } else if k.eq_ignore_ascii_case("uber-trace-id") && uber_trace_id.is_none() {
            uber_trace_id = Some(v);
        } else if k.eq_ignore_ascii_case("jaeger-debug-id") && jaeger_debug_id.is_none() && !v.is_empty() {
            jaeger_debug_id = Some(v);
        }
    }

    let mut ctx = TraceContext::default();

    if propagators.tracecontext {
        if let Some(tp) = traceparent {
            if let Some(mut parsed) = parse_traceparent(tp) {
                parsed.tracestate = tracestate.map(str::to_string);
                ctx = parsed;
            }
        }
    }

    if propagators.jaeger {
        if let Some(utid) = uber_trace_id {
            if let Some(parsed) = parse_uber_trace_id(utid) {
                if ctx.is_valid() && ctx.trace_id == parsed.trace_id && ctx.span_id == parsed.span_id {
                    if parsed.jaeger_debug_id.is_some() && ctx.jaeger_debug_id.is_none() {
                        ctx.jaeger_debug_id = parsed.jaeger_debug_id;
                    }
                } else {
                    ctx = parsed;
                }
            }
        }
        if let Some(dbg) = jaeger_debug_id {
            ctx.jaeger_debug_id = Some(dbg.to_string());
        }
    }

    ctx
}

/// Extracts TraceContext from an http::HeaderMap.
pub fn extract_from_http_headers(headers: &hyper::http::HeaderMap) -> TraceContext {
    let props = global_tracing()
        .map(|t| &t.propagators)
        .cloned()
        .unwrap_or_default();
    let iter = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|vs| (k.as_str(), vs)));
    extract_from_headers(iter, &props)
}

/// Starts a server/consumer span context from an extracted parent context.
pub fn start_server_span_context(
    extracted: &TraceContext,
    create_new_trace: bool,
) -> (TraceContext, Option<[u8; 8]>) {
    let rt = global_tracing();
    let tracing_active = rt.is_some_and(|t| t.enabled);

    if !tracing_active {
        if create_new_trace {
            return (TraceContext::default(), None);
        }
        let mut out = extracted.clone();
        if out.is_valid() && out.jaeger_debug_id.is_some() {
            out.sampled = true;
        }
        return (out, None);
    }

    let sampler = rt.map(|t| t.sampler).unwrap_or_default();
    let effective_parent = if create_new_trace {
        TraceContext {
            jaeger_debug_id: extracted.jaeger_debug_id.clone(),
            ..TraceContext::default()
        }
    } else {
        extracted.clone()
    };

    let (trace_id, parent_span_id) = if effective_parent.is_valid() {
        (effective_parent.trace_id, Some(effective_parent.span_id))
    } else {
        (random_trace_id(), None)
    };
    let span_id = random_span_id();
    let sampled = sampler.should_sample(&effective_parent, &trace_id);
    if sampled && rt.is_some_and(|t| t.exporter.is_some()) {
        if let Some(ref dbg) = effective_parent.jaeger_debug_id {
            record_jaeger_debug_id(span_id, dbg.clone());
        }
    }

    (
        TraceContext {
            trace_id,
            span_id,
            sampled,
            trace_flags: if sampled { 0x01 } else { 0x00 },
            remote: false,
            jaeger_debug_id: effective_parent.jaeger_debug_id,
            tracestate: effective_parent.tracestate,
        },
        parent_span_id,
    )
}

pub fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub fn random_trace_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    let mut rng = rand::thread_rng();
    loop {
        rng.fill_bytes(&mut id);
        if id != [0u8; 16] {
            return id;
        }
    }
}

pub fn random_span_id() -> [u8; 8] {
    let mut id = [0u8; 8];
    let mut rng = rand::thread_rng();
    loop {
        rng.fill_bytes(&mut id);
        if id != [0u8; 8] {
            return id;
        }
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

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn decode_lower_hex_16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 || s.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    decode_hex_16(s)
}

fn decode_lower_hex_8(s: &str) -> Option<[u8; 8]> {
    if s.len() != 16 || s.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    decode_hex_8(s)
}

fn decode_hex_16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = (hex_nibble(b[i * 2])? << 4) | hex_nibble(b[i * 2 + 1])?;
    }
    Some(out)
}

fn decode_hex_8(s: &str) -> Option<[u8; 8]> {
    if s.len() != 16 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = [0u8; 8];
    for i in 0..8 {
        out[i] = (hex_nibble(b[i * 2])? << 4) | hex_nibble(b[i * 2 + 1])?;
    }
    Some(out)
}

/// Computes the service name for tracing (cmd/mimir/main.go).
pub fn resolve_service_name(targets: &[String]) -> String {
    if let Ok(name) = std::env::var("OTEL_SERVICE_NAME") {
        if !name.is_empty() {
            return name;
        }
    }
    if let Ok(name) = std::env::var("JAEGER_SERVICE_NAME") {
        if !name.is_empty() {
            return name;
        }
    }
    if targets.len() == 1 {
        format!("mimir-{}", targets[0])
    } else {
        "mimir".to_string()
    }
}

pub fn detected_exporter_kind() -> Option<String> {
    if let Ok(exp) = std::env::var("OTEL_TRACES_EXPORTER") {
        if !exp.is_empty() {
            return Some(exp);
        }
    }
    if std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .ok()
        .is_some_and(|v| !v.is_empty())
        || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .is_some_and(|v| !v.is_empty())
    {
        return Some("otlp".to_string());
    }
    None
}

type JaegerDebugIdQueue = Mutex<VecDeque<([u8; 8], String)>>;
static JAEGER_DEBUG_IDS: OnceLock<JaegerDebugIdQueue> = OnceLock::new();

fn record_jaeger_debug_id(span_id: [u8; 8], debug_id: String) {
    let q = JAEGER_DEBUG_IDS.get_or_init(|| Mutex::new(VecDeque::new()));
    if let Ok(mut g) = q.lock() {
        if g.len() >= 2048 {
            g.pop_front();
        }
        g.push_back((span_id, debug_id));
    }
}

fn take_jaeger_debug_id(span_id: [u8; 8]) -> Option<String> {
    let q = JAEGER_DEBUG_IDS.get()?;
    let mut g = q.lock().ok()?;
    let pos = g.iter().rposition(|(sid, _)| *sid == span_id)?;
    g.remove(pos).map(|(_, v)| v)
}

pub fn otlp_traces_endpoint_from_env() -> Option<String> {
    if let Ok(ep) = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
        if !ep.is_empty() {
            return Some(ep);
        }
    }
    if let Ok(ep) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        if !ep.is_empty() {
            let trimmed = ep.trim_end_matches('/');
            return Some(format!("{trimmed}/v1/traces"));
        }
    }
    if std::env::var("OTEL_TRACES_EXPORTER")
        .ok()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("otlp"))
    {
        return Some("http://localhost:4318/v1/traces".to_string());
    }
    None
}

pub fn export_span(mut span: ExportedSpan) {
    if let Some(rt) = global_tracing() {
        if let Some(ref exp) = rt.exporter {
            if !span.attributes.iter().any(|a| a.key == "jaeger-debug-id") {
                let dbg = take_jaeger_debug_id(span.span_id)
                    .or_else(|| current_trace_context().and_then(|c| c.jaeger_debug_id));
                if let Some(dbg) = dbg {
                    span.attributes.push(SpanAttribute {
                        key: "jaeger-debug-id".to_string(),
                        value: SpanAttrValue::String(dbg),
                    });
                }
            }
            exp(span);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_traceparent_and_uber_trace_id() {
        let tp = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        assert_eq!(tp.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tp.span_id_hex(), "00f067aa0ba902b7");
        assert!(tp.is_sampled());

        let tp_unsampled = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00").unwrap();
        assert!(!tp_unsampled.is_sampled());
        assert_eq!(
            tp_unsampled.extract_sampled_trace_id(),
            ("4bf92f3577b34da6a3ce929d0e0e4736".to_string(), false)
        );

        let j = parse_uber_trace_id("48475050943e8e05:70e8b02d28e4337b:0:1").unwrap();
        assert_eq!(j.trace_id_hex(), "000000000000000048475050943e8e05");
        assert_eq!(j.span_id_hex(), "70e8b02d28e4337b");
        assert!(j.is_sampled());

        let j_dbg = parse_uber_trace_id("48475050943e8e05:70e8b02d28e4337b:0:3").unwrap();
        assert!(j_dbg.is_sampled());
        assert!(j_dbg.jaeger_debug_id.is_some());

        assert!(parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01").is_none());
        assert!(parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff").is_none());
        assert!(parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-04").is_none());
        assert!(parse_uber_trace_id("0:70e8b02d28e4337b:0:1").is_none());

        let tp_random_sampled = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03").unwrap();
        assert!(tp_random_sampled.is_sampled());
        assert_eq!(tp_random_sampled.trace_flags, 0x03);
        assert_eq!(tp_random_sampled.to_traceparent(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03");

        let tp_random_unsampled = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02").unwrap();
        assert!(!tp_random_unsampled.is_sampled());
        assert_eq!(tp_random_unsampled.trace_flags, 0x02);
        assert_eq!(tp_random_unsampled.to_traceparent(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02");
    }

    #[test]
    fn extract_preserves_tracestate_when_both_headers_match() {
        let headers = [
            ("traceparent", "00-000000000000000048475050943e8e05-70e8b02d28e4337b-01"),
            ("tracestate", "congo=t61rcWkgMzE"),
            ("uber-trace-id", "48475050943e8e05:70e8b02d28e4337b:0:3"),
        ];
        let ctx = extract_from_headers(headers.into_iter(), &PropagatorSet::default());
        assert_eq!(ctx.tracestate.as_deref(), Some("congo=t61rcWkgMzE"));
        assert!(ctx.jaeger_debug_id.is_some());
    }

    #[test]
    fn jaeger_debug_id_forces_sampling_on_unsampled_parent() {
        let headers = [
            ("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"),
            ("Jaeger-Debug-Id", "debug-123"),
        ];
        let extracted = extract_from_headers(headers.into_iter(), &PropagatorSet::default());
        assert!(!extracted.is_sampled());
        assert_eq!(extracted.jaeger_debug_id.as_deref(), Some("debug-123"));
        let (started, _) = start_server_span_context(&extracted, false);
        assert!(started.is_sampled());
        assert_eq!(started.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
