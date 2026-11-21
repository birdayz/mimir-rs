//! Wave 26 Track 3 unit/integration tests for mimir-common::tracing (S2-01, S2-02, S2-05).

use std::sync::Mutex;

use hyper::http::HeaderMap;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::tracing::{
    build_tracing_runtime_from_env, encode_jaeger_agent_emit_batch_compact,
    encode_jaeger_thrift_batch_binary, encode_otlp_grpc_frame, encode_otlp_trace_request_protobuf,
    extract_from_http_headers, extract_w3c_trace_context, jaeger_target_from_env,
    otlp_protocol_from_env, otlp_traces_endpoint_for_protocol, parse_otel_resource_attributes,
    start_server_span_context, start_server_span_context_with_runtime, ExportedSpan, JaegerTarget,
    OtlpProtocol, PropagatorSet, SpanAttrValue, SpanAttribute, SpanEvent, SpanLink, TraceContext,
    TracingRuntime,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct NoopLogger;
impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

fn clear_tracing_env() {
    for k in [
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_INSECURE",
        "OTEL_EXPORTER_OTLP_TRACES_INSECURE",
        "OTEL_EXPORTER_OTLP_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
        "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_TRACES_SAMPLER",
        "OTEL_TRACES_SAMPLER_ARG",
        "OTEL_PROPAGATORS",
        "JAEGER_AGENT_HOST",
        "JAEGER_AGENT_PORT",
        "JAEGER_ENDPOINT",
        "OTEL_EXPORTER_JAEGER_ENDPOINT",
        "JAEGER_SAMPLER_MANAGER_HOST_PORT",
        "JAEGER_SAMPLING_ENDPOINT",
        "JAEGER_SAMPLER_TYPE",
        "JAEGER_SAMPLER_PARAM",
        "JAEGER_SAMPLER_NOT_PARENT_BASED",
        "JAEGER_TAGS",
    ] {
        std::env::remove_var(k);
    }
}

#[test]
fn s2_02_disabled_mode_is_noop_and_w3c_helper_remains_independent() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    headers.insert("tracestate", "congo=t61rcWkgMzE".parse().unwrap());
    headers.insert(
        "uber-trace-id",
        "48475050943e8e05:70e8b02d28e4337b:0:3".parse().unwrap(),
    );
    headers.insert("Jaeger-Debug-Id", "force-123".parse().unwrap());

    // 1. When global tracing is unset, extract_from_http_headers is a no-op.
    let extracted = extract_from_http_headers(&headers);
    assert!(!extracted.is_valid());
    assert_eq!(extracted.extract_sampled_trace_id(), (String::new(), false));
    assert!(extracted.jaeger_debug_id.is_none());

    // 2. start_server_span_context is also a no-op when global tracing is unset/disabled.
    let fake_parent = TraceContext {
        trace_id: [0x4b; 16],
        span_id: [0x01; 8],
        sampled: false,
        trace_flags: 0,
        remote: true,
        jaeger_debug_id: Some("force-123".to_string()),
        tracestate: None,
    };
    let (started, parent_sid) = start_server_span_context(&fake_parent, false);
    assert!(!started.is_valid());
    assert!(parent_sid.is_none());

    let disabled_rt = TracingRuntime {
        enabled: false,
        propagators: PropagatorSet::default(),
        ..TracingRuntime::default()
    };
    let (started_rt, parent_rt) =
        start_server_span_context_with_runtime(&disabled_rt, &fake_parent, false);
    assert!(!started_rt.is_valid());
    assert!(parent_rt.is_none());

    // 3. Explicit W3C Kafka propagator helper still works independently of global tracing.
    let w3c = extract_w3c_trace_context(
        headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|vs| (k.as_str(), vs))),
    );
    assert!(w3c.is_valid());
    assert!(w3c.is_sampled());
    assert_eq!(w3c.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(w3c.tracestate.as_deref(), Some("congo=t61rcWkgMzE"));
}

#[test]
fn s2_01_otlp_protocol_validation_and_endpoints() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_tracing_env();

    assert_eq!(otlp_protocol_from_env().unwrap(), OtlpProtocol::HttpProtobuf);

    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    assert_eq!(otlp_protocol_from_env().unwrap(), OtlpProtocol::Grpc);

    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json");
    assert_eq!(otlp_protocol_from_env().unwrap(), OtlpProtocol::HttpJson);

    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "bogus");
    assert!(otlp_protocol_from_env().is_err());

    // Validation failures in build_tracing_runtime_from_env.
    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    assert!(build_tracing_runtime_from_env("mimir", &NoopLogger, None).is_err());

    std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL");
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "collector.local:4317");
    assert_eq!(
        otlp_traces_endpoint_for_protocol(OtlpProtocol::Grpc).as_deref(),
        Some("http://collector.local:4317")
    );
    std::env::set_var("OTEL_EXPORTER_OTLP_INSECURE", "false");
    assert_eq!(
        otlp_traces_endpoint_for_protocol(OtlpProtocol::Grpc).as_deref(),
        Some("https://collector.local:4317")
    );

    // Unknown OTEL_TRACES_EXPORTER must fail setup.
    clear_tracing_env();
    std::env::set_var("OTEL_TRACES_EXPORTER", "bogus-exporter");
    assert!(build_tracing_runtime_from_env("mimir", &NoopLogger, None).is_err());

    // Jaeger agent vs collector target resolution.
    clear_tracing_env();
    std::env::set_var("JAEGER_AGENT_HOST", "jaeger-agent");
    assert_eq!(
        jaeger_target_from_env().unwrap(),
        Some(JaegerTarget::UdpAgent {
            host: "jaeger-agent".to_string(),
            port: "6831".to_string(),
        })
    );
    std::env::set_var("JAEGER_AGENT_PORT", "16831");
    assert_eq!(
        jaeger_target_from_env().unwrap(),
        Some(JaegerTarget::UdpAgent {
            host: "jaeger-agent".to_string(),
            port: "16831".to_string(),
        })
    );
    std::env::set_var("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces");
    assert_eq!(
        jaeger_target_from_env().unwrap(),
        Some(JaegerTarget::HttpCollector(
            "http://jaeger:14268/api/traces".to_string()
        ))
    );

    clear_tracing_env();
}

#[test]
fn s2_05_resource_attributes_jaeger_sampler_attrs_and_remote_sampler_validation() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_tracing_env();

    let parsed = parse_otel_resource_attributes(
        "service.version=1.2.3, deployment.environment = staging%20eu , bad_pair, tags=a%2Cb, service.version=2.0.0",
    );
    assert_eq!(
        parsed,
        vec![
            SpanAttribute {
                key: "service.version".to_string(),
                value: SpanAttrValue::String("2.0.0".to_string()),
            },
            SpanAttribute {
                key: "deployment.environment".to_string(),
                value: SpanAttrValue::String("staging eu".to_string()),
            },
            SpanAttribute {
                key: "tags".to_string(),
                value: SpanAttrValue::String("a,b".to_string()),
            },
        ]
    );

    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "env=prod,team=mimir%20core");
    std::env::set_var("OTEL_TRACES_SAMPLER", "parentbased_jaeger_remote");
    // Missing OTEL_TRACES_SAMPLER_ARG must fail.
    assert!(build_tracing_runtime_from_env("mimir", &NoopLogger, None).is_err());

    std::env::set_var(
        "OTEL_TRACES_SAMPLER_ARG",
        "pollingIntervalMs=5000,initialSamplingRate=0.5",
    );
    // Missing endpoint= must fail.
    assert!(build_tracing_runtime_from_env("mimir", &NoopLogger, None).is_err());

    std::env::set_var(
        "OTEL_TRACES_SAMPLER_ARG",
        "endpoint=http://127.0.0.1:5778/sampling,pollingIntervalMs=5000,initialSamplingRate=0.5",
    );
    let rt = build_tracing_runtime_from_env("mimir", &NoopLogger, None).unwrap();
    assert!(rt.enabled);
    assert!(rt.resource_attributes.iter().any(|a| {
        a.key == "team" && a.value == SpanAttrValue::String("mimir core".to_string())
    }));

    // Jaeger sampler resource attributes.
    clear_tracing_env();
    std::env::set_var("JAEGER_AGENT_HOST", "jaeger-agent");
    std::env::set_var("JAEGER_SAMPLER_TYPE", "probabilistic");
    std::env::set_var("JAEGER_SAMPLER_PARAM", "0.25");
    std::env::set_var("JAEGER_SAMPLER_NOT_PARENT_BASED", "true");
    std::env::set_var("JAEGER_TAGS", "cluster=eu-west-1");
    let jrt = build_tracing_runtime_from_env("mimir-ingester", &NoopLogger, None).unwrap();
    assert!(jrt.resource_attributes.contains(&SpanAttribute {
        key: "cluster".to_string(),
        value: SpanAttrValue::String("eu-west-1".to_string()),
    }));
    assert!(jrt.resource_attributes.contains(&SpanAttribute {
        key: "samplerType".to_string(),
        value: SpanAttrValue::String("probabilistic".to_string()),
    }));
    assert!(jrt.resource_attributes.contains(&SpanAttribute {
        key: "samplerParam".to_string(),
        value: SpanAttrValue::Float(0.25),
    }));
    assert!(jrt.resource_attributes.contains(&SpanAttribute {
        key: "samplingServerURL".to_string(),
        value: SpanAttrValue::String("http://jaeger-agent:5778/sampling".to_string()),
    }));
    assert!(jrt.resource_attributes.contains(&SpanAttribute {
        key: "samplerNotParentBased".to_string(),
        value: SpanAttrValue::Bool(true),
    }));

    clear_tracing_env();
}

#[test]
fn s2_01_s2_05_wire_encoders_handle_links_events_and_attrs() {
    let mut span = ExportedSpan {
        service_name: "mimir-ingester".to_string(),
        trace_id: [0x11; 16],
        span_id: [0x22; 8],
        parent_span_id: None,
        name: "HTTP GET - ready".to_string(),
        kind: 2,
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_000_005_000_000,
        attributes: vec![
            SpanAttribute {
                key: "http.request.method".to_string(),
                value: SpanAttrValue::String("GET".to_string()),
            },
            SpanAttribute {
                key: "http.response.status_code".to_string(),
                value: SpanAttrValue::Int(200),
            },
        ],
        status_error: false,
    };
    span.push_link(SpanLink {
        trace_id: [0xaa; 16],
        span_id: [0xbb; 8],
        trace_state: Some("rojo=1".to_string()),
        attributes: Vec::new(),
    });
    span.push_event(SpanEvent {
        name: "Client sent too many headers, some of them were not included in the span attributes: ".to_string(),
        time_unix_nano: 1_700_000_000_001_000_000,
        attributes: vec![SpanAttribute {
            key: "headers_total".to_string(),
            value: SpanAttrValue::Int(105),
        }],
    });

    let res_attrs = vec![SpanAttribute {
        key: "env".to_string(),
        value: SpanAttrValue::String("prod".to_string()),
    }];

    let pb = encode_otlp_trace_request_protobuf(&span, &res_attrs);
    assert!(!pb.is_empty());
    assert!(pb.windows("mimir-ingester".len()).any(|w| w == b"mimir-ingester"));
    assert!(pb.windows("HTTP GET - ready".len()).any(|w| w == b"HTTP GET - ready"));
    assert!(pb.windows("rojo=1".len()).any(|w| w == b"rojo=1"));

    let grpc_frame = encode_otlp_grpc_frame(&pb);
    assert_eq!(grpc_frame[0], 0);
    assert_eq!(
        u32::from_be_bytes(grpc_frame[1..5].try_into().unwrap()) as usize,
        pb.len()
    );

    let thrift_bin = encode_jaeger_thrift_batch_binary(&span, &res_attrs);
    assert!(thrift_bin.windows("mimir-ingester".len()).any(|w| w == b"mimir-ingester"));
    assert!(thrift_bin.windows("HTTP GET - ready".len()).any(|w| w == b"HTTP GET - ready"));

    let thrift_compact = encode_jaeger_agent_emit_batch_compact(&span, &res_attrs);
    assert_eq!(thrift_compact[0], 0x82);
    assert_eq!(thrift_compact[1], 0x21); // version=1, type=CALL(1)
    assert!(thrift_compact.windows("emitBatch".len()).any(|w| w == b"emitBatch"));
    assert!(thrift_compact.windows("HTTP GET - ready".len()).any(|w| w == b"HTTP GET - ready"));
}
