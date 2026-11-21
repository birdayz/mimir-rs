//! Distributed tracing context extraction, propagation, sampling, and OTel/Jaeger env setup.
//!
//! Ported from:
//! - github.com/grafana/dskit/tracing/tracing.go
//! - github.com/grafana/dskit/tracing/otel.go
//! - github.com/grafana/dskit/tracing/otel_jaeger.go
//! - go.opentelemetry.io/contrib/propagators/jaeger/jaeger_propagator.go

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

pub const INTERNAL_SPAN_LINKS_KEY: &str = "__mimir_span_links";
pub const INTERNAL_SPAN_EVENTS_KEY: &str = "__mimir_span_events";

#[derive(Clone, Debug, PartialEq)]
pub struct SpanLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_state: Option<String>,
    pub attributes: Vec<SpanAttribute>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpanEvent {
    pub name: String,
    pub time_unix_nano: u64,
    pub attributes: Vec<SpanAttribute>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SpanAttrValue {
    String(String),
    StringArray(Vec<String>),
    Bool(bool),
    Int(i64),
    Float(f64),
    Links(Vec<SpanLink>),
    Events(Vec<SpanEvent>),
}

impl PartialEq for SpanAttribute {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
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

impl ExportedSpan {
    pub fn push_link(&mut self, link: SpanLink) {
        if let Some(attr) = self
            .attributes
            .iter_mut()
            .find(|a| matches!(a.value, SpanAttrValue::Links(_)))
        {
            if let SpanAttrValue::Links(ref mut links) = attr.value {
                links.push(link);
            }
        } else {
            self.attributes.push(SpanAttribute {
                key: INTERNAL_SPAN_LINKS_KEY.to_string(),
                value: SpanAttrValue::Links(vec![link]),
            });
        }
    }

    pub fn links(&self) -> &[SpanLink] {
        for attr in &self.attributes {
            if let SpanAttrValue::Links(ref links) = attr.value {
                return links.as_slice();
            }
        }
        &[]
    }

    pub fn push_event(&mut self, event: SpanEvent) {
        if let Some(attr) = self
            .attributes
            .iter_mut()
            .find(|a| matches!(a.value, SpanAttrValue::Events(_)))
        {
            if let SpanAttrValue::Events(ref mut events) = attr.value {
                events.push(event);
            }
        } else {
            self.attributes.push(SpanAttribute {
                key: INTERNAL_SPAN_EVENTS_KEY.to_string(),
                value: SpanAttrValue::Events(vec![event]),
            });
        }
    }

    pub fn events(&self) -> &[SpanEvent] {
        for attr in &self.attributes {
            if let SpanAttrValue::Events(ref events) = attr.value {
                return events.as_slice();
            }
        }
        &[]
    }

    pub fn regular_attributes(&self) -> impl Iterator<Item = &SpanAttribute> {
        self.attributes.iter().filter(|a| {
            !matches!(
                a.value,
                SpanAttrValue::Links(_) | SpanAttrValue::Events(_)
            )
        })
    }
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
    pub const fn none() -> Self {
        Self {
            tracecontext: false,
            baggage: false,
            jaeger: false,
        }
    }

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
            propagators: PropagatorSet::none(),
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
        let (sampler, mut jaeger_attrs) = parse_jaeger_config_from_env()?;
        let tags = parse_jaeger_tags(&std::env::var("JAEGER_TAGS").unwrap_or_default())?;
        let mut resource_attributes = otel_resource_attributes_from_env();
        resource_attributes.extend(tags);
        resource_attributes.append(&mut jaeger_attrs);
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
            resource_attributes,
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
        validate_otel_exporter_env()?;
        let sampler = parse_otel_sampler_from_env()?;
        let resource_attributes = otel_resource_attributes_from_env();
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
            resource_attributes,
            exporter,
        });
    }

    Ok(TracingRuntime {
        enabled: false,
        service_name: service_name.to_string(),
        propagators: PropagatorSet::none(),
        sampler: SamplerKind::ParentBasedAlwaysOn,
        resource_attributes: Vec::new(),
        exporter: None,
    })
}

fn validate_otel_exporter_env() -> Result<(), String> {
    let raw_exp = std::env::var("OTEL_TRACES_EXPORTER").unwrap_or_default();
    let exp = raw_exp.trim();
    match exp {
        "" | "otlp" => {
            let _ = otlp_protocol_from_env()?;
            Ok(())
        }
        "none" | "console" | "jaeger" => Ok(()),
        other => Err(format!("unknown exporter or metrics producer {other:?}")),
    }
}

fn parse_otel_sampler_from_env() -> Result<SamplerKind, String> {
    let name = std::env::var("OTEL_TRACES_SAMPLER")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if name == "jaeger_remote" || name == "parentbased_jaeger_remote" {
        let parent_based = name == "parentbased_jaeger_remote";
        let args = std::env::var("OTEL_TRACES_SAMPLER_ARG").unwrap_or_default();
        if args.is_empty() {
            return Err(format!(
                "OTEL_TRACES_SAMPLER_ARG is not set for Jaeger remote sampler {name}"
            ));
        }
        let mut endpoint_set = false;
        let mut initial_rate = 0.001_f64;
        for part in args.split(',') {
            let part = part.trim();
            if let Some(ep) = part.strip_prefix("endpoint=") {
                if ep.is_empty() {
                    return Err(format!(
                        "endpoint is not set in OTEL_TRACES_SAMPLER_ARG for Jaeger remote sampler {name}"
                    ));
                }
                endpoint_set = true;
            } else if let Some(ms_str) = part.strip_prefix("pollingIntervalMs=") {
                ms_str.parse::<i64>().map_err(|e| {
                    format!("invalid pollingIntervalMs value in OTEL_TRACES_SAMPLER_ARG: {e}")
                })?;
            } else if let Some(rate_str) = part.strip_prefix("initialSamplingRate=") {
                let r = rate_str.parse::<f64>().map_err(|e| {
                    format!("invalid initialSamplingRate value in OTEL_TRACES_SAMPLER_ARG: {e}")
                })?;
                if !(0.0..=1.0).contains(&r) {
                    return Err(format!(
                        "initialSamplingRate value set in OTEL_TRACES_SAMPLER_ARG must be between 0 and 1, got {r}"
                    ));
                }
                initial_rate = r;
            }
        }
        if !endpoint_set {
            return Err(format!(
                "endpoint is not set in OTEL_TRACES_SAMPLER_ARG for Jaeger remote sampler {name}"
            ));
        }
        let ratio = SamplerKind::ratio_from_f64(initial_rate);
        return Ok(if parent_based {
            SamplerKind::ParentBasedTraceIdRatio(ratio)
        } else {
            SamplerKind::TraceIdRatio(ratio)
        });
    }
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

fn parse_jaeger_config_from_env() -> Result<(SamplerKind, Vec<SpanAttribute>), String> {
    if let Ok(ep) = std::env::var("JAEGER_ENDPOINT") {
        if !ep.is_empty() && !ep.contains("://") {
            return Err(format!("cannot parse env var JAEGER_ENDPOINT={ep}: invalid URI"));
        }
    }
    if let Ok(port_s) = std::env::var("JAEGER_AGENT_PORT") {
        if !port_s.is_empty() && port_s.trim().parse::<u16>().is_err() {
            return Err(format!("invalid JAEGER_AGENT_PORT={port_s:?}"));
        }
    }
    let mut stype = std::env::var("JAEGER_SAMPLER_TYPE").unwrap_or_default();
    let param_set = std::env::var("JAEGER_SAMPLER_PARAM").ok().filter(|s| !s.is_empty());
    let param = match param_set {
        Some(ref s) => s
            .parse::<f64>()
            .map_err(|e| format!("cannot parse env var JAEGER_SAMPLER_PARAM={s}: {e}"))?,
        None => 0.0,
    };
    let sampling_server_url = if let Some(e) = non_empty_env("JAEGER_SAMPLING_ENDPOINT") {
        e
    } else if let Some(e) = non_empty_env("JAEGER_SAMPLER_MANAGER_HOST_PORT") {
        e
    } else if let Some(host) = non_empty_env("JAEGER_AGENT_HOST") {
        format!("http://{host}:5778/sampling")
    } else {
        String::new()
    };
    if !sampling_server_url.is_empty() && stype.is_empty() {
        stype = "remote".to_string();
    }
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
    let sampler = if not_parent_based {
        base
    } else {
        match base {
            SamplerKind::AlwaysOn => SamplerKind::ParentBasedAlwaysOn,
            SamplerKind::AlwaysOff => SamplerKind::ParentBasedAlwaysOff,
            SamplerKind::TraceIdRatio(t) => SamplerKind::ParentBasedTraceIdRatio(t),
            other => other,
        }
    };
    // Exact resource attributes emitted by dskit/tracing/otel_jaeger.go:268-276.
    let mut attrs = vec![
        SpanAttribute {
            key: "samplerType".to_string(),
            value: SpanAttrValue::String(stype),
        },
        SpanAttribute {
            key: "samplerParam".to_string(),
            value: SpanAttrValue::Float(param),
        },
        SpanAttribute {
            key: "samplingServerURL".to_string(),
            value: SpanAttrValue::String(sampling_server_url),
        },
    ];
    if not_parent_based {
        attrs.push(SpanAttribute {
            key: "samplerNotParentBased".to_string(),
            value: SpanAttrValue::Bool(true),
        });
    }
    Ok((sampler, attrs))
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
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

/// Extracts W3C TraceContext from raw headers regardless of global tracing state
/// (used by Kafka ingest_storage `recordsPropagator()`).
pub fn extract_w3c_trace_context<'a>(
    headers: impl Iterator<Item = (&'a str, &'a str)>,
) -> TraceContext {
    let w3c_only = PropagatorSet {
        tracecontext: true,
        baggage: false,
        jaeger: false,
    };
    extract_from_headers(headers, &w3c_only)
}

/// Extracts TraceContext from an http::HeaderMap using the global propagator.
/// When global tracing is unset or disabled, returns an empty no-op TraceContext
/// matching Go dskit/tracing (no-op global TextMapPropagator).
pub fn extract_from_http_headers(headers: &hyper::http::HeaderMap) -> TraceContext {
    let Some(rt) = global_tracing().filter(|t| t.enabled) else {
        return TraceContext::default();
    };
    let iter = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|vs| (k.as_str(), vs)));
    extract_from_headers(iter, &rt.propagators)
}

/// Starts a server/consumer span context from an extracted parent context using an explicit
/// `TracingRuntime`. When `!rt.enabled`, returns an empty no-op context.
pub fn start_server_span_context_with_runtime(
    rt: &TracingRuntime,
    extracted: &TraceContext,
    create_new_trace: bool,
) -> (TraceContext, Option<[u8; 8]>) {
    if !rt.enabled {
        return (TraceContext::default(), None);
    }

    let sampler = rt.sampler;
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
    if sampled && rt.exporter.is_some() {
        if let Some(ref dbg) = effective_parent.jaeger_debug_id {
            record_jaeger_debug_id(span_id, dbg.clone());
        }
    }

    (
        TraceContext {
            trace_id,
            span_id,
            sampled,
            // OTel SDK `tracer.newSpan`: `psc.TraceFlags() | FlagsSampled` / `&^ FlagsSampled`
            // (the parent's W3C random bit 0x02 is inherited by the child span context).
            trace_flags: (effective_parent.trace_flags & 0x02) | u8::from(sampled),
            remote: false,
            jaeger_debug_id: effective_parent.jaeger_debug_id,
            tracestate: effective_parent.tracestate,
        },
        parent_span_id,
    )
}

/// Starts a server/consumer span context from an extracted parent context.
/// When global tracing is unset or disabled, returns `(TraceContext::default(), None)`.
pub fn start_server_span_context(
    extracted: &TraceContext,
    create_new_trace: bool,
) -> (TraceContext, Option<[u8; 8]>) {
    let Some(rt) = global_tracing().filter(|t| t.enabled) else {
        return (TraceContext::default(), None);
    };
    start_server_span_context_with_runtime(rt, extracted, create_new_trace)
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
    for k in [
        "JAEGER_AGENT_HOST",
        "JAEGER_ENDPOINT",
        "JAEGER_SAMPLER_MANAGER_HOST_PORT",
    ] {
        if std::env::var(k).ok().is_some_and(|v| !v.is_empty()) {
            return Some("jaeger".to_string());
        }
    }
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

/// Resolves the Jaeger HTTP collector endpoint from env vars (`JAEGER_ENDPOINT`,
/// `OTEL_EXPORTER_JAEGER_ENDPOINT`, or default `http://localhost:14268/api/traces` when
/// `OTEL_TRACES_EXPORTER=jaeger`).
pub fn jaeger_endpoint_from_env() -> Option<String> {
    for k in ["JAEGER_ENDPOINT", "OTEL_EXPORTER_JAEGER_ENDPOINT"] {
        if let Ok(ep) = std::env::var(k) {
            let trimmed = ep.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    if std::env::var("OTEL_TRACES_EXPORTER")
        .ok()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("jaeger"))
    {
        return Some("http://localhost:14268/api/traces".to_string());
    }
    None
}

/// Returns `(user, password)` for Jaeger collector Basic Auth when both
/// `OTEL_EXPORTER_JAEGER_USER` and `OTEL_EXPORTER_JAEGER_PASSWORD` are non-empty.
pub fn jaeger_basic_auth_from_env() -> Option<(String, String)> {
    let user = std::env::var("OTEL_EXPORTER_JAEGER_USER").ok()?;
    let pass = std::env::var("OTEL_EXPORTER_JAEGER_PASSWORD").ok()?;
    if user.is_empty() || pass.is_empty() {
        return None;
    }
    Some((user, pass))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OtlpProtocol {
    HttpProtobuf,
    HttpJson,
    Grpc,
}

/// Resolves the OTLP transport protocol from `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` or
/// `OTEL_EXPORTER_OTLP_PROTOCOL`, defaulting to `http/protobuf` per OTel `autoexport`.
pub fn otlp_protocol_from_env() -> Result<OtlpProtocol, String> {
    let raw = non_empty_trimmed_env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        .or_else(|| non_empty_trimmed_env("OTEL_EXPORTER_OTLP_PROTOCOL"));
    match raw.as_deref() {
        None => Ok(OtlpProtocol::HttpProtobuf),
        Some("http/protobuf") => Ok(OtlpProtocol::HttpProtobuf),
        Some("http/json") => Ok(OtlpProtocol::HttpJson),
        Some("grpc") => Ok(OtlpProtocol::Grpc),
        Some(other) => Err(format!(
            "invalid OTLP protocol {other:?} - should be one of ['grpc', 'http/protobuf', 'http/json']"
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JaegerTarget {
    HttpCollector(String),
    UdpAgent { host: String, port: String },
}

/// Resolves the Jaeger target (HTTP Thrift collector or UDP Compact Thrift agent) from env vars,
/// matching `dskit/tracing/otel_jaeger.go`.
pub fn jaeger_target_from_env() -> Result<Option<JaegerTarget>, String> {
    if let Some(ep) = jaeger_endpoint_from_env() {
        if !ep.starts_with("http://") && !ep.starts_with("https://") {
            return Err(format!("cannot parse env var JAEGER_ENDPOINT={ep}: invalid URI"));
        }
        return Ok(Some(JaegerTarget::HttpCollector(ep)));
    }
    let has_agent_env = non_empty_trimmed_env("JAEGER_AGENT_HOST").is_some()
        || non_empty_trimmed_env("JAEGER_SAMPLER_MANAGER_HOST_PORT").is_some()
        || non_empty_trimmed_env("JAEGER_AGENT_PORT").is_some();
    if !has_agent_env {
        return Ok(None);
    }
    let host = non_empty_trimmed_env("JAEGER_AGENT_HOST").unwrap_or_else(|| "localhost".to_string());
    let port = non_empty_trimmed_env("JAEGER_AGENT_PORT").unwrap_or_else(|| "6831".to_string());
    if port.parse::<u16>().is_err() {
        return Err(format!("invalid JAEGER_AGENT_PORT={port:?}"));
    }
    Ok(Some(JaegerTarget::UdpAgent { host, port }))
}

/// Parses `OTEL_RESOURCE_ATTRIBUTES` (`k1=v1,k2=v2` with `%XX` URL-unescaping per OTel spec).
pub fn parse_otel_resource_attributes(raw: &str) -> Vec<SpanAttribute> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut order: Vec<String> = Vec::new();
    let mut vals: BTreeMap<String, String> = BTreeMap::new();
    for pair in trimmed.split(',') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        let key = k.trim();
        if key.is_empty() {
            continue;
        }
        let val_trimmed = v.trim();
        let val = path_unescape_percent(val_trimmed).unwrap_or_else(|| val_trimmed.to_string());
        if !vals.contains_key(key) {
            order.push(key.to_string());
        }
        vals.insert(key.to_string(), val);
    }
    order
        .into_iter()
        .filter_map(|k| {
            vals.remove(&k).map(|v| SpanAttribute {
                key: k,
                value: SpanAttrValue::String(v),
            })
        })
        .collect()
}

pub fn otel_resource_attributes_from_env() -> Vec<SpanAttribute> {
    non_empty_trimmed_env("OTEL_RESOURCE_ATTRIBUTES")
        .map(|raw| parse_otel_resource_attributes(&raw))
        .unwrap_or_default()
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OtlpTlsEnvConfig {
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

fn non_empty_trimmed_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Resolves OTLP TLS CA & mTLS certificate paths from `OTEL_EXPORTER_OTLP_[TRACES_]*` env vars,
/// matching `otlptracehttp/internal/otlpconfig/envconfig.go`.
pub fn otlp_tls_paths_from_env() -> OtlpTlsEnvConfig {
    let ca_cert_path = non_empty_trimmed_env("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE")
        .or_else(|| non_empty_trimmed_env("OTEL_EXPORTER_OTLP_CERTIFICATE"));
    let (client_cert_path, client_key_path) = match (
        non_empty_trimmed_env("OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE"),
        non_empty_trimmed_env("OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY"),
    ) {
        (Some(c), Some(k)) => (Some(c), Some(k)),
        _ => match (
            non_empty_trimmed_env("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"),
            non_empty_trimmed_env("OTEL_EXPORTER_OTLP_CLIENT_KEY"),
        ) {
            (Some(c), Some(k)) => (Some(c), Some(k)),
            _ => (None, None),
        },
    };
    OtlpTlsEnvConfig {
        ca_cert_path,
        client_cert_path,
        client_key_path,
    }
}

/// Resolves OTLP HTTP exporter timeout (milliseconds) from `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`
/// or `OTEL_EXPORTER_OTLP_TIMEOUT`.
pub fn otlp_timeout_from_env() -> Option<Duration> {
    for key in [
        "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
        "OTEL_EXPORTER_OTLP_TIMEOUT",
    ] {
        if let Some(raw) = non_empty_trimmed_env(key) {
            if let Ok(ms) = raw.parse::<u64>() {
                if ms > 0 {
                    return Some(Duration::from_millis(ms));
                }
            }
        }
    }
    None
}

fn is_http_token_char(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
        )
}

fn path_unescape_percent(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_nibble(bytes[i + 1])?;
            let lo = hex_nibble(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Parses comma-separated `key=value` pairs per OTel OTLP header spec (`url.PathUnescape` on
/// values, trimmed names and values).
pub fn parse_otlp_headers_str(raw: &str) -> Vec<(String, String)> {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    for pair in raw.split(',') {
        let Some((n, v)) = pair.split_once('=') else {
            continue;
        };
        let name = n.trim();
        if name.is_empty() || !name.bytes().all(is_http_token_char) {
            continue;
        }
        let Some(decoded) = path_unescape_percent(v) else {
            continue;
        };
        map.insert(name.to_string(), decoded.trim().to_string());
    }
    map.into_iter().collect()
}

/// Reads `OTEL_EXPORTER_OTLP_HEADERS` and `OTEL_EXPORTER_OTLP_TRACES_HEADERS` (with
/// `TRACES_HEADERS` taking precedence).
pub fn otlp_headers_from_env() -> Vec<(String, String)> {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    for key in [
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    ] {
        if let Some(raw) = non_empty_trimmed_env(key) {
            for (k, v) in parse_otlp_headers_str(&raw) {
                map.insert(k, v);
            }
        }
    }
    map.into_iter().collect()
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
    otlp_traces_endpoint_for_protocol(OtlpProtocol::HttpProtobuf)
}

fn otlp_insecure_from_env() -> Option<bool> {
    for key in [
        "OTEL_EXPORTER_OTLP_TRACES_INSECURE",
        "OTEL_EXPORTER_OTLP_INSECURE",
    ] {
        if let Some(raw) = non_empty_trimmed_env(key) {
            if raw.eq_ignore_ascii_case("true") || raw == "1" {
                return Some(true);
            }
            if raw.eq_ignore_ascii_case("false") || raw == "0" {
                return Some(false);
            }
        }
    }
    None
}

pub fn otlp_traces_endpoint_for_protocol(proto: OtlpProtocol) -> Option<String> {
    match proto {
        OtlpProtocol::HttpProtobuf | OtlpProtocol::HttpJson => {
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
        OtlpProtocol::Grpc => {
            let raw = non_empty_trimmed_env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                .or_else(|| non_empty_trimmed_env("OTEL_EXPORTER_OTLP_ENDPOINT"))
                .or_else(|| {
                    std::env::var("OTEL_TRACES_EXPORTER")
                        .ok()
                        .is_some_and(|v| v.trim().eq_ignore_ascii_case("otlp"))
                        .then(|| "http://localhost:4317".to_string())
                })?;
            if raw.contains("://") {
                Some(raw)
            } else {
                let tls = otlp_tls_paths_from_env();
                let use_https = match otlp_insecure_from_env() {
                    Some(true) => false,
                    Some(false) => true,
                    None => tls.ca_cert_path.is_some() || tls.client_cert_path.is_some(),
                };
                let scheme = if use_https { "https://" } else { "http://" };
                Some(format!("{scheme}{raw}"))
            }
        }
    }
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

// ---------------------------------------------------------------------------
// OTLP Protobuf & gRPC wire encoders (zero extra crate deps)
// ---------------------------------------------------------------------------

fn pb_write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn pb_write_tag(buf: &mut Vec<u8>, field_num: u32, wire_type: u8) {
    pb_write_varint(buf, ((field_num as u64) << 3) | (wire_type as u64));
}

fn pb_write_bytes(buf: &mut Vec<u8>, field_num: u32, data: &[u8]) {
    pb_write_tag(buf, field_num, 2);
    pb_write_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

fn pb_write_string(buf: &mut Vec<u8>, field_num: u32, s: &str) {
    pb_write_bytes(buf, field_num, s.as_bytes());
}

fn pb_write_fixed64(buf: &mut Vec<u8>, field_num: u32, v: u64) {
    pb_write_tag(buf, field_num, 1);
    buf.extend_from_slice(&v.to_le_bytes());
}

fn pb_encode_any_value(val: &SpanAttrValue) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match val {
        SpanAttrValue::String(s) => pb_write_string(&mut out, 1, s),
        SpanAttrValue::Bool(b) => {
            pb_write_tag(&mut out, 2, 0);
            pb_write_varint(&mut out, u64::from(*b));
        }
        SpanAttrValue::Int(i) => {
            pb_write_tag(&mut out, 3, 0);
            pb_write_varint(&mut out, *i as u64);
        }
        SpanAttrValue::Float(f) => {
            pb_write_fixed64(&mut out, 4, f.to_bits());
        }
        SpanAttrValue::StringArray(arr) => {
            let mut arr_buf = Vec::new();
            for s in arr {
                let mut elem = Vec::new();
                pb_write_string(&mut elem, 1, s);
                pb_write_bytes(&mut arr_buf, 1, &elem);
            }
            pb_write_bytes(&mut out, 5, &arr_buf);
        }
        SpanAttrValue::Links(_) | SpanAttrValue::Events(_) => return None,
    }
    Some(out)
}

fn pb_encode_key_value(attr: &SpanAttribute) -> Option<Vec<u8>> {
    let val_bytes = pb_encode_any_value(&attr.value)?;
    let mut out = Vec::new();
    pb_write_string(&mut out, 1, &attr.key);
    pb_write_bytes(&mut out, 2, &val_bytes);
    Some(out)
}

/// Encodes an `ExportedSpan` as an OTLP `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest`
/// protobuf message.
pub fn encode_otlp_trace_request_protobuf(
    span: &ExportedSpan,
    extra_resource_attrs: &[SpanAttribute],
) -> Vec<u8> {
    // 1. Resource
    let mut resource_buf = Vec::new();
    let svc_attr = SpanAttribute {
        key: "service.name".to_string(),
        value: SpanAttrValue::String(span.service_name.clone()),
    };
    if let Some(kv) = pb_encode_key_value(&svc_attr) {
        pb_write_bytes(&mut resource_buf, 1, &kv);
    }
    for attr in extra_resource_attrs {
        if attr.key != "service.name" {
            if let Some(kv) = pb_encode_key_value(attr) {
                pb_write_bytes(&mut resource_buf, 1, &kv);
            }
        }
    }

    // 2. Span
    let mut span_buf = Vec::new();
    pb_write_bytes(&mut span_buf, 1, &span.trace_id);
    pb_write_bytes(&mut span_buf, 2, &span.span_id);
    if let Some(pid) = span.parent_span_id {
        pb_write_bytes(&mut span_buf, 4, &pid);
    }
    pb_write_string(&mut span_buf, 5, &span.name);
    pb_write_tag(&mut span_buf, 6, 0);
    pb_write_varint(&mut span_buf, u64::from(span.kind));
    pb_write_fixed64(&mut span_buf, 7, span.start_time_unix_nano);
    pb_write_fixed64(&mut span_buf, 8, span.end_time_unix_nano);
    for attr in span.regular_attributes() {
        if let Some(kv) = pb_encode_key_value(attr) {
            pb_write_bytes(&mut span_buf, 9, &kv);
        }
    }
    for ev in span.events() {
        let mut ev_buf = Vec::new();
        pb_write_fixed64(&mut ev_buf, 1, ev.time_unix_nano);
        pb_write_string(&mut ev_buf, 2, &ev.name);
        for attr in &ev.attributes {
            if let Some(kv) = pb_encode_key_value(attr) {
                pb_write_bytes(&mut ev_buf, 3, &kv);
            }
        }
        pb_write_bytes(&mut span_buf, 11, &ev_buf);
    }
    for link in span.links() {
        let mut link_buf = Vec::new();
        pb_write_bytes(&mut link_buf, 1, &link.trace_id);
        pb_write_bytes(&mut link_buf, 2, &link.span_id);
        if let Some(ref ts) = link.trace_state {
            if !ts.is_empty() {
                pb_write_string(&mut link_buf, 3, ts);
            }
        }
        for attr in &link.attributes {
            if let Some(kv) = pb_encode_key_value(attr) {
                pb_write_bytes(&mut link_buf, 4, &kv);
            }
        }
        pb_write_bytes(&mut span_buf, 13, &link_buf);
    }
    let mut status_buf = Vec::new();
    pb_write_tag(&mut status_buf, 3, 0);
    pb_write_varint(&mut status_buf, if span.status_error { 2 } else { 0 });
    pb_write_bytes(&mut span_buf, 15, &status_buf);

    // 3. InstrumentationScope & ScopeSpans
    let mut scope_buf = Vec::new();
    pb_write_string(&mut scope_buf, 1, "mimir-rs");
    let mut scope_spans_buf = Vec::new();
    pb_write_bytes(&mut scope_spans_buf, 1, &scope_buf);
    pb_write_bytes(&mut scope_spans_buf, 2, &span_buf);

    // 4. ResourceSpans
    let mut resource_spans_buf = Vec::new();
    pb_write_bytes(&mut resource_spans_buf, 1, &resource_buf);
    pb_write_bytes(&mut resource_spans_buf, 2, &scope_spans_buf);

    // 5. ExportTraceServiceRequest
    let mut req_buf = Vec::new();
    pb_write_bytes(&mut req_buf, 1, &resource_spans_buf);
    req_buf
}

/// Wraps a protobuf payload in a 5-byte gRPC length-prefixed frame.
pub fn encode_otlp_grpc_frame(protobuf_payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + protobuf_payload.len());
    out.push(0);
    out.extend_from_slice(&(protobuf_payload.len() as u32).to_be_bytes());
    out.extend_from_slice(protobuf_payload);
    out
}

// ---------------------------------------------------------------------------
// Jaeger Thrift encoders (Binary for HTTP collector, Compact for UDP agent)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
#[repr(u8)]
enum ThriftType {
    Stop = 0,
    Bool = 2,
    Double = 4,
    I32 = 8,
    I64 = 10,
    String = 11,
    Struct = 12,
    List = 15,
}

enum JaegerTagVal<'a> {
    String(String),
    Double(f64),
    Bool(bool),
    Long(i64),
    StrRef(&'a str),
}

struct JaegerTag<'a> {
    key: &'a str,
    val: JaegerTagVal<'a>,
}

fn attr_to_jaeger_tag<'a>(attr: &'a SpanAttribute) -> Option<JaegerTag<'a>> {
    let val = match &attr.value {
        SpanAttrValue::String(s) => JaegerTagVal::StrRef(s.as_str()),
        SpanAttrValue::Float(f) => JaegerTagVal::Double(*f),
        SpanAttrValue::Bool(b) => JaegerTagVal::Bool(*b),
        SpanAttrValue::Int(i) => JaegerTagVal::Long(*i),
        SpanAttrValue::StringArray(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
            JaegerTagVal::String(json)
        }
        SpanAttrValue::Links(_) | SpanAttrValue::Events(_) => return None,
    };
    Some(JaegerTag {
        key: attr.key.as_str(),
        val,
    })
}

trait ThriftProtoWriter {
    fn write_struct_begin(&mut self);
    fn write_struct_end(&mut self);
    fn write_field_stop(&mut self);
    fn write_field_begin(&mut self, ttype: ThriftType, id: i16);
    fn write_bool_field(&mut self, id: i16, v: bool);
    fn write_i32(&mut self, v: i32);
    fn write_i64(&mut self, v: i64);
    fn write_double(&mut self, v: f64);
    fn write_string(&mut self, s: &str);
    fn write_list_begin(&mut self, elem_type: ThriftType, len: usize);
}

struct ThriftBinaryWriter {
    buf: Vec<u8>,
}

impl ThriftBinaryWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
        }
    }
}

impl ThriftProtoWriter for ThriftBinaryWriter {
    fn write_struct_begin(&mut self) {}
    fn write_struct_end(&mut self) {}
    fn write_field_stop(&mut self) {
        self.buf.push(ThriftType::Stop as u8);
    }
    fn write_field_begin(&mut self, ttype: ThriftType, id: i16) {
        self.buf.push(ttype as u8);
        self.buf.extend_from_slice(&id.to_be_bytes());
    }
    fn write_bool_field(&mut self, id: i16, v: bool) {
        self.write_field_begin(ThriftType::Bool, id);
        self.buf.push(u8::from(v));
    }
    fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    fn write_double(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_bits().to_be_bytes());
    }
    fn write_string(&mut self, s: &str) {
        self.write_i32(s.len() as i32);
        self.buf.extend_from_slice(s.as_bytes());
    }
    fn write_list_begin(&mut self, elem_type: ThriftType, len: usize) {
        self.buf.push(elem_type as u8);
        self.write_i32(len as i32);
    }
}

struct ThriftCompactWriter {
    buf: Vec<u8>,
    last_field_stack: Vec<i16>,
    last_field_id: i16,
}

impl ThriftCompactWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
            last_field_stack: Vec::new(),
            last_field_id: 0,
        }
    }

    fn compact_type(t: ThriftType) -> u8 {
        match t {
            ThriftType::Stop => 0x00,
            ThriftType::Bool => 0x01,
            ThriftType::I32 => 0x05,
            ThriftType::I64 => 0x06,
            ThriftType::Double => 0x07,
            ThriftType::String => 0x08,
            ThriftType::List => 0x09,
            ThriftType::Struct => 0x0c,
        }
    }

    fn write_varint_u32(&mut self, mut v: u32) {
        loop {
            if (v & !0x7f) == 0 {
                self.buf.push(v as u8);
                break;
            }
            self.buf.push(((v & 0x7f) as u8) | 0x80);
            v >>= 7;
        }
    }

    fn write_varint_u64(&mut self, mut v: u64) {
        loop {
            if (v & !0x7f) == 0 {
                self.buf.push(v as u8);
                break;
            }
            self.buf.push(((v & 0x7f) as u8) | 0x80);
            v >>= 7;
        }
    }

    fn write_zigzag_i16(&mut self, n: i16) {
        let zz = (((n as i32) << 1) ^ ((n as i32) >> 31)) as u32;
        self.write_varint_u32(zz);
    }

    fn write_field_header_internal(&mut self, compact_t: u8, id: i16) {
        let delta = id - self.last_field_id;
        if delta > 0 && delta <= 15 {
            self.buf.push((((delta as u8) << 4) & 0xf0) | (compact_t & 0x0f));
        } else {
            self.buf.push(compact_t & 0x0f);
            self.write_zigzag_i16(id);
        }
        self.last_field_id = id;
    }

    fn write_message_begin(&mut self, method: &str, msg_type: u8, seq_id: i32) {
        self.buf.push(0x82); // COMPACT_PROTOCOL_ID
        self.buf.push((0x01 & 0x1f) | ((msg_type << 5) & 0xe0));
        self.write_varint_u32(seq_id as u32);
        self.write_string(method);
    }
}

impl ThriftProtoWriter for ThriftCompactWriter {
    fn write_struct_begin(&mut self) {
        self.last_field_stack.push(self.last_field_id);
        self.last_field_id = 0;
    }
    fn write_struct_end(&mut self) {
        self.last_field_id = self.last_field_stack.pop().unwrap_or(0);
    }
    fn write_field_stop(&mut self) {
        self.buf.push(0x00);
    }
    fn write_field_begin(&mut self, ttype: ThriftType, id: i16) {
        self.write_field_header_internal(Self::compact_type(ttype), id);
    }
    fn write_bool_field(&mut self, id: i16, v: bool) {
        self.write_field_header_internal(if v { 0x01 } else { 0x02 }, id);
    }
    fn write_i32(&mut self, v: i32) {
        let zz = ((v << 1) ^ (v >> 31)) as u32;
        self.write_varint_u32(zz);
    }
    fn write_i64(&mut self, v: i64) {
        let zz = ((v << 1) ^ (v >> 63)) as u64;
        self.write_varint_u64(zz);
    }
    fn write_double(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
    }
    fn write_string(&mut self, s: &str) {
        self.write_varint_u32(s.len() as u32);
        self.buf.extend_from_slice(s.as_bytes());
    }
    fn write_list_begin(&mut self, elem_type: ThriftType, len: usize) {
        let ct = Self::compact_type(elem_type);
        if len <= 14 {
            self.buf.push((((len as u8) << 4) & 0xf0) | (ct & 0x0f));
        } else {
            self.buf.push(0xf0 | (ct & 0x0f));
            self.write_varint_u32(len as u32);
        }
    }
}

fn write_jaeger_tag<W: ThriftProtoWriter>(w: &mut W, tag: &JaegerTag<'_>) {
    w.write_struct_begin();
    w.write_field_begin(ThriftType::String, 1);
    w.write_string(tag.key);
    let vtype = match &tag.val {
        JaegerTagVal::String(_) | JaegerTagVal::StrRef(_) => 0,
        JaegerTagVal::Double(_) => 1,
        JaegerTagVal::Bool(_) => 2,
        JaegerTagVal::Long(_) => 3,
    };
    w.write_field_begin(ThriftType::I32, 2);
    w.write_i32(vtype);
    match &tag.val {
        JaegerTagVal::String(s) => {
            w.write_field_begin(ThriftType::String, 3);
            w.write_string(s);
        }
        JaegerTagVal::StrRef(s) => {
            w.write_field_begin(ThriftType::String, 3);
            w.write_string(s);
        }
        JaegerTagVal::Double(d) => {
            w.write_field_begin(ThriftType::Double, 4);
            w.write_double(*d);
        }
        JaegerTagVal::Bool(b) => {
            w.write_bool_field(5, *b);
        }
        JaegerTagVal::Long(l) => {
            w.write_field_begin(ThriftType::I64, 6);
            w.write_i64(*l);
        }
    }
    w.write_field_stop();
    w.write_struct_end();
}

fn write_jaeger_batch<W: ThriftProtoWriter>(
    w: &mut W,
    span: &ExportedSpan,
    extra_resource_attrs: &[SpanAttribute],
) {
    w.write_struct_begin();

    // field 1: Process
    w.write_field_begin(ThriftType::Struct, 1);
    w.write_struct_begin();
    w.write_field_begin(ThriftType::String, 1);
    w.write_string(&span.service_name);
    let proc_tags: Vec<JaegerTag<'_>> = extra_resource_attrs
        .iter()
        .filter(|a| a.key != "service.name")
        .filter_map(attr_to_jaeger_tag)
        .collect();
    if !proc_tags.is_empty() {
        w.write_field_begin(ThriftType::List, 2);
        w.write_list_begin(ThriftType::Struct, proc_tags.len());
        for t in &proc_tags {
            write_jaeger_tag(w, t);
        }
    }
    w.write_field_stop();
    w.write_struct_end();

    // field 2: Spans
    w.write_field_begin(ThriftType::List, 2);
    w.write_list_begin(ThriftType::Struct, 1);
    {
        w.write_struct_begin();
        let tid_hi = i64::from_be_bytes(span.trace_id[0..8].try_into().unwrap_or([0; 8]));
        let tid_lo = i64::from_be_bytes(span.trace_id[8..16].try_into().unwrap_or([0; 8]));
        let sid = i64::from_be_bytes(span.span_id);
        let psid = span.parent_span_id.map(i64::from_be_bytes).unwrap_or(0);

        w.write_field_begin(ThriftType::I64, 1);
        w.write_i64(tid_lo);
        w.write_field_begin(ThriftType::I64, 2);
        w.write_i64(tid_hi);
        w.write_field_begin(ThriftType::I64, 3);
        w.write_i64(sid);
        w.write_field_begin(ThriftType::I64, 4);
        w.write_i64(psid);
        w.write_field_begin(ThriftType::String, 5);
        w.write_string(&span.name);

        let links = span.links();
        if !links.is_empty() {
            w.write_field_begin(ThriftType::List, 6);
            w.write_list_begin(ThriftType::Struct, links.len());
            for link in links {
                let l_hi = i64::from_be_bytes(link.trace_id[0..8].try_into().unwrap_or([0; 8]));
                let l_lo = i64::from_be_bytes(link.trace_id[8..16].try_into().unwrap_or([0; 8]));
                let l_sid = i64::from_be_bytes(link.span_id);
                w.write_struct_begin();
                w.write_field_begin(ThriftType::I32, 1);
                w.write_i32(1); // SpanRefType_FOLLOWS_FROM
                w.write_field_begin(ThriftType::I64, 2);
                w.write_i64(l_lo);
                w.write_field_begin(ThriftType::I64, 3);
                w.write_i64(l_hi);
                w.write_field_begin(ThriftType::I64, 4);
                w.write_i64(l_sid);
                w.write_field_stop();
                w.write_struct_end();
            }
        }

        let flags = if span.attributes.iter().any(|a| a.key == "jaeger-debug-id") {
            3
        } else {
            1
        };
        w.write_field_begin(ThriftType::I32, 7);
        w.write_i32(flags);

        let start_us = (span.start_time_unix_nano / 1000) as i64;
        let dur_us = (span.end_time_unix_nano.saturating_sub(span.start_time_unix_nano) / 1000) as i64;
        w.write_field_begin(ThriftType::I64, 8);
        w.write_i64(start_us);
        w.write_field_begin(ThriftType::I64, 9);
        w.write_i64(dur_us);

        let mut tags: Vec<JaegerTag<'_>> = span
            .regular_attributes()
            .filter_map(attr_to_jaeger_tag)
            .collect();
        tags.push(JaegerTag {
            key: "otel.library.name",
            val: JaegerTagVal::StrRef("mimir-rs"),
        });
        match span.kind {
            2 => tags.push(JaegerTag {
                key: "span.kind",
                val: JaegerTagVal::StrRef("server"),
            }),
            5 => tags.push(JaegerTag {
                key: "span.kind",
                val: JaegerTagVal::StrRef("consumer"),
            }),
            _ => {}
        }
        if span.status_error {
            tags.push(JaegerTag {
                key: "error",
                val: JaegerTagVal::Bool(true),
            });
            tags.push(JaegerTag {
                key: "otel.status_code",
                val: JaegerTagVal::StrRef("ERROR"),
            });
        }
        if !tags.is_empty() {
            w.write_field_begin(ThriftType::List, 10);
            w.write_list_begin(ThriftType::Struct, tags.len());
            for t in &tags {
                write_jaeger_tag(w, t);
            }
        }

        let events = span.events();
        if !events.is_empty() {
            w.write_field_begin(ThriftType::List, 11);
            w.write_list_begin(ThriftType::Struct, events.len());
            for ev in events {
                w.write_struct_begin();
                w.write_field_begin(ThriftType::I64, 1);
                w.write_i64((ev.time_unix_nano / 1000) as i64);
                let mut fields = Vec::with_capacity(1 + ev.attributes.len());
                if !ev.name.is_empty() {
                    fields.push(JaegerTag {
                        key: "event",
                        val: JaegerTagVal::StrRef(ev.name.as_str()),
                    });
                }
                for a in &ev.attributes {
                    if let Some(t) = attr_to_jaeger_tag(a) {
                        fields.push(t);
                    }
                }
                w.write_field_begin(ThriftType::List, 2);
                w.write_list_begin(ThriftType::Struct, fields.len());
                for f in &fields {
                    write_jaeger_tag(w, f);
                }
                w.write_field_stop();
                w.write_struct_end();
            }
        }

        w.write_field_stop();
        w.write_struct_end();
    }

    w.write_field_stop();
    w.write_struct_end();
}

/// Encodes a `jaeger.thrift` `Batch` using Thrift Binary Protocol (`Content-Type: application/x-thrift`)
/// matching `go.opentelemetry.io/otel/exporters/jaeger/uploader.go`.
pub fn encode_jaeger_thrift_batch_binary(
    span: &ExportedSpan,
    extra_resource_attrs: &[SpanAttribute],
) -> Vec<u8> {
    let mut w = ThriftBinaryWriter::new();
    write_jaeger_batch(&mut w, span, extra_resource_attrs);
    w.buf
}

/// Encodes a Jaeger UDP `agent.thrift` `emitBatch` message using Thrift Compact Protocol
/// matching `go.opentelemetry.io/otel/exporters/jaeger/agent.go`.
pub fn encode_jaeger_agent_emit_batch_compact(
    span: &ExportedSpan,
    extra_resource_attrs: &[SpanAttribute],
) -> Vec<u8> {
    let mut w = ThriftCompactWriter::new();
    w.write_message_begin("emitBatch", 1 /* CALL */, 0);
    w.write_struct_begin(); // AgentEmitBatchArgs
    w.write_field_begin(ThriftType::Struct, 1);
    write_jaeger_batch(&mut w, span, extra_resource_attrs);
    w.write_field_stop();
    w.write_struct_end();
    w.buf
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

        // When tracing is disabled, start_server_span_context_with_runtime is a no-op.
        let disabled_rt = TracingRuntime::default();
        let (disabled_started, disabled_parent) =
            start_server_span_context_with_runtime(&disabled_rt, &extracted, false);
        assert!(!disabled_started.is_valid());
        assert!(disabled_parent.is_none());

        // When tracing is enabled, Jaeger-Debug-Id forces sampling on an unsampled parent.
        let enabled_rt = TracingRuntime {
            enabled: true,
            propagators: PropagatorSet::default(),
            ..TracingRuntime::default()
        };
        let (started, _) = start_server_span_context_with_runtime(&enabled_rt, &extracted, false);
        assert!(started.is_sampled());
        assert_eq!(started.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
