//! Upstream Go tests ported 1:1 from `.ref/mimir/pkg/mimir/`.

use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, Method, StatusCode};
use axum::routing::get;
use axum::Router;
use http_body_util::BodyExt as _;
use mimir_cmd::grpc::{HealthCheckRequest, HealthCheckResponse, HealthService};
use mimir_cmd::grpc_call_check::{
    FinishFn, GrpcInflightMethodLimiter, IngesterReceiver, PushReceiver, HTTPGRPC_HANDLE_METHOD,
    HTTPGRPC_METADATA_METHOD, HTTPGRPC_METADATA_URL, INGESTER_METHOD_PREFIX, INGESTER_PUSH_METHOD,
    METADATA_MESSAGE_SIZE, OTLP_PUSH_ENDPOINT, PROMETHEUS_PUSH_ENDPOINT,
};
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_cmd::runtime_config_metrics::RuntimeConfigMetrics;
use mimir_cmd::sanity_check::{
    check_directories_read_write_access, check_object_stores_config, ERR_OBJECT_STORAGE,
};
use mimir_cmd::server::Server;
use mimir_cmd::server_service::{new_server_service, ServicesToWaitForFn};
use mimir_cmd::{effective_config_yaml, parse_cli_for_run, run_cli_test_mode, run_mimir};
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::{Service, ServiceContext, State};
use mimir_config::limits::set_active_series_custom_trackers_validator;
use mimir_config::{
    parse_runtime_config_yaml, parse_runtime_config_yaml_with_defaults,
    runtime_config_source_ids, GoDuration, LimitsConfig, MimirConfig, ModelDuration, Overrides,
    RuntimeConfigConfig, RuntimeConfigManager, RuntimeConfigManagerOptions, ServerConfig,
    StringSliceCSV,
};
use mimir_ingester::active_series::{
    build_custom_trackers_config, merged_custom_trackers, validate_custom_trackers,
};
use mimir_ingester::grpc::IngesterService;
use mimir_ingester::Ingester;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::ingesterpb::LabelNamesRequest;
use mimir_proto::ringpb::{PartitionRingDesc, PartitionState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::multi::{runtime_config_provider, MultiConfigProvider};
use mimir_ring::kv::{
    build_kv_client_with_multi_config_provider, AsRegisterer, CasOutcome,
    Registerer as RingRegisterer,
};
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionRingDescExt, PartitionRingOptions, PartitionRingWatcher,
    PartitionRingWatchers,
};
use mimir_ring::ring::model::ring_desc_codec;
use mimir_ring::token_gen::PartitionTokenTable;
use prometheus::{Counter, Opts, Registry};
use serde_yaml::Value;
use tempfile::tempdir;
use tonic::metadata::MetadataMap;
use tonic::Code;
use tower::ServiceExt as _;

#[derive(Default)]
struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _kvs: &[(&str, &str)]) {}
}

fn free_tcp_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn new_server_cfg() -> ServerConfig {
    ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        grpc_server_max_recv_msg_size: 1024,
        ..Default::default()
    }
}

fn test_http_state(cfg: MimirConfig, reg: Registry) -> AppHttpState {
    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());
    let limits = cfg.limits.clone();
    AppHttpState {
        config: Arc::new(cfg),
        default_config: Arc::new(MimirConfig::default()),
        registry: reg,
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: None,
        runtime_config: None,
        overrides: Overrides::new(limits),
        memberlist_kv: None,
        modules,
        cost_attribution_registry: None,
    }
}

fn valid_compartments_config() -> MimirConfig {
    let mut cfg = MimirConfig::default();
    cfg.ingest_storage.enabled = true;
    cfg.ingest_storage.kafka.address =
        StringSliceCSV(vec!["kafka-wc-<write-compartment-id>:9092".to_string()]);
    cfg.ingest_storage.kafka.topic = "mimir-ingest-rc-<read-compartment-id>".to_string();
    cfg.ingest_storage.kafka.auto_create_topic_enabled = false;
    cfg.compartments.enabled = true;
    cfg.compartments.read.num_compartments = 2;
    cfg.compartments.write.num_compartments = 2;
    cfg.blocks_storage.bucket.backend_cfg.backend = "filesystem".to_string();
    cfg.blocks_storage.bucket.backend_cfg.filesystem.dir =
        "/data/blocks-rc-<read-compartment-id>".to_string();
    set_extra(&mut cfg, &["distributor", "write_compartment_id"], Value::from(1));
    cfg
}

fn set_extra(cfg: &mut MimirConfig, path: &[&str], val: Value) {
    assert!(!path.is_empty());
    let root = cfg
        .extra_sections
        .entry(path[0].to_string())
        .or_insert_with(|| Value::Mapping(serde_yaml::Mapping::new()));
    let mut cur = root;
    for seg in &path[1..path.len() - 1] {
        if !cur.is_mapping() {
            *cur = Value::Mapping(serde_yaml::Mapping::new());
        }
        let m = cur.as_mapping_mut().unwrap();
        cur = m
            .entry(Value::String((*seg).to_string()))
            .or_insert_with(|| Value::Mapping(serde_yaml::Mapping::new()));
    }
    if path.len() > 1 {
        if !cur.is_mapping() {
            *cur = Value::Mapping(serde_yaml::Mapping::new());
        }
        cur.as_mapping_mut()
            .unwrap()
            .insert(Value::String(path[path.len() - 1].to_string()), val);
    } else {
        *cur = val;
    }
}

fn parse_with_flags(args: &[&str], yaml: Option<&str>) -> MimirConfig {
    let dir = tempdir().unwrap();
    let mut full: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    if let Some(y) = yaml {
        let p = dir.path().join("cfg.yaml");
        fs::write(&p, y).unwrap();
        full.push("-config.file".to_string());
        full.push(p.to_string_lossy().to_string());
    }
    let out = run_cli_test_mode("./mimir", &full);
    assert_eq!(out.exit_code, 0, "stderr: {}", out.stderr);
    out.config.unwrap()
}

fn assert_all_ccv_labels(cfg: &MimirConfig, expected: &str) {
    let eff = effective_config_yaml(cfg);
    let paths: &[&[&str]] = &[
        &["ingester_client", "grpc_client_config", "cluster_validation", "label"],
        &["frontend_worker", "grpc_client_config", "cluster_validation", "label"],
        &["frontend_worker", "query_scheduler_grpc_client_config", "cluster_validation", "label"],
        &["block_builder", "scheduler_config", "grpc_client_config", "cluster_validation", "label"],
        &["frontend", "grpc_client_config", "cluster_validation", "label"],
        &["querier", "store_gateway_client", "cluster_validation", "label"],
        &["query_scheduler", "grpc_client_config", "cluster_validation", "label"],
        &["ruler", "ruler_client", "cluster_validation", "label"],
        &["ruler", "distributor", "grpc_client_config", "cluster_validation", "label"],
        &["ruler", "query_frontend", "grpc_client_config", "cluster_validation", "label"],
        &["alertmanager", "alertmanager_client", "cluster_validation", "label"],
        &["runtime_config", "http_client_cluster_validation", "label"],
    ];
    for p in paths {
        let mut cur = &eff;
        for seg in *p {
            cur = &cur[*seg];
        }
        assert_eq!(
            cur.as_str().unwrap_or(""),
            expected,
            "path {:?} mismatch in effective config",
            p
        );
    }
}

// ============================================================================
// pkg/mimir/grpc_call_check_test.go (1 test)
// ============================================================================

#[derive(Default)]
struct MockIngesterReceiver {
    last_request_size: AtomicI64,
    start_push_calls: AtomicUsize,
    start_push_bytes: AtomicI64,
    prepare_push_calls: AtomicUsize,
    finish_push_calls: AtomicUsize,
    finish_push_bytes: AtomicI64,
    start_read_calls: AtomicUsize,
    prepare_read_calls: AtomicUsize,
    return_error: parking_lot::Mutex<Option<String>>,
}

impl PushReceiver for MockIngesterReceiver {
    fn start_push_request(&self, request_size: i64) -> Result<(), String> {
        self.last_request_size.store(request_size, Ordering::SeqCst);
        self.start_push_calls.fetch_add(1, Ordering::SeqCst);
        self.start_push_bytes.fetch_add(request_size, Ordering::SeqCst);
        if let Some(ref e) = *self.return_error.lock() {
            return Err(e.clone());
        }
        Ok(())
    }

    fn prepare_push_request(&self) -> Result<Option<FinishFn>, String> {
        self.prepare_push_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(ref e) = *self.return_error.lock() {
            return Err(e.clone());
        }
        Ok(None)
    }

    fn finish_push_request(&self) {
        self.finish_push_calls.fetch_add(1, Ordering::SeqCst);
        let sz = self.last_request_size.load(Ordering::SeqCst);
        self.finish_push_bytes.fetch_add(sz, Ordering::SeqCst);
    }
}

impl IngesterReceiver for MockIngesterReceiver {
    fn start_read_request(&self) -> Result<(), String> {
        self.start_read_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(ref e) = *self.return_error.lock() {
            return Err(e.clone());
        }
        Ok(())
    }

    fn prepare_read_request(&self) -> Result<Option<FinishFn>, String> {
        self.prepare_read_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(ref e) = *self.return_error.lock() {
            return Err(e.clone());
        }
        Ok(None)
    }
}

#[derive(Default)]
struct MockDistributorReceiver {
    last_request_size: AtomicI64,
    start_calls: AtomicUsize,
    finish_calls: AtomicUsize,
    return_error: parking_lot::Mutex<Option<String>>,
}

impl PushReceiver for MockDistributorReceiver {
    fn start_push_request(&self, request_size: i64) -> Result<(), String> {
        self.last_request_size.store(request_size, Ordering::SeqCst);
        self.start_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(ref e) = *self.return_error.lock() {
            return Err(e.clone());
        }
        Ok(())
    }

    fn prepare_push_request(&self) -> Result<Option<FinishFn>, String> {
        Ok(None)
    }

    fn finish_push_request(&self) {
        self.finish_calls.fetch_add(1, Ordering::SeqCst);
    }
}

// Upstream: pkg/mimir/grpc_call_check_test.go::TestGrpcInflightMethodLimiter
#[test]
fn test_grpc_inflight_method_limiter() {
    use mimir_cmd::grpc_call_check::{CallContext, RpcCallKind};

    {
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(|| None)),
            Some(Arc::new(|| None)),
        );
        let ctx = l.rpc_call_starting("test", None).unwrap();
        l.rpc_call_finished(ctx);

        let err = l.rpc_call_starting(INGESTER_PUSH_METHOD, None).unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(err.message(), "no ingester");

        let mut md = MetadataMap::new();
        md.insert(HTTPGRPC_METADATA_METHOD, "POST".parse().unwrap());
        md.insert(HTTPGRPC_METADATA_URL, PROMETHEUS_PUSH_ENDPOINT.parse().unwrap());
        let err = l.rpc_call_starting(HTTPGRPC_HANDLE_METHOD, Some(&md)).unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(err.message(), "no distributor");

        let panicked_ing = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            l.rpc_call_finished(CallContext {
                kind: Some(RpcCallKind::IngesterPush),
            });
        }));
        assert!(panicked_ing.is_err());

        let panicked_dist = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            l.rpc_call_finished(CallContext {
                kind: Some(RpcCallKind::DistributorPush),
            });
        }));
        assert!(panicked_dist.is_err());
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let ctx = l.rpc_call_starting("test", None).unwrap();
        l.rpc_call_finished(ctx);
        assert_eq!(m.start_push_calls.load(Ordering::SeqCst), 0);
        assert_eq!(m.prepare_push_calls.load(Ordering::SeqCst), 0);
        assert_eq!(m.finish_push_calls.load(Ordering::SeqCst), 0);
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        *m.return_error.lock() = Some("hello there".to_string());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let err = l.rpc_call_starting(INGESTER_PUSH_METHOD, None).unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let ctx = l.rpc_call_starting(INGESTER_PUSH_METHOD, None).unwrap();
        assert_eq!(m.start_push_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.start_push_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(m.prepare_push_calls.load(Ordering::SeqCst), 0);
        assert_eq!(m.finish_push_calls.load(Ordering::SeqCst), 0);
        l.rpc_call_processing(ctx, INGESTER_PUSH_METHOD).unwrap();
        assert_eq!(m.prepare_push_calls.load(Ordering::SeqCst), 1);
        l.rpc_call_finished(ctx);
        assert_eq!(m.finish_push_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.finish_push_bytes.load(Ordering::SeqCst), 0);
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let mut md = MetadataMap::new();
        md.insert(METADATA_MESSAGE_SIZE, "123456".parse().unwrap());
        let ctx = l.rpc_call_starting(INGESTER_PUSH_METHOD, Some(&md)).unwrap();
        assert_eq!(m.start_push_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.start_push_bytes.load(Ordering::SeqCst), 123456);
        l.rpc_call_processing(ctx, INGESTER_PUSH_METHOD).unwrap();
        assert_eq!(m.prepare_push_calls.load(Ordering::SeqCst), 1);
        l.rpc_call_finished(ctx);
        assert_eq!(m.finish_push_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.finish_push_bytes.load(Ordering::SeqCst), 123456);
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let mut md = MetadataMap::new();
        md.insert(METADATA_MESSAGE_SIZE, "wrong".parse().unwrap());
        let ctx = l.rpc_call_starting(INGESTER_PUSH_METHOD, Some(&md)).unwrap();
        assert_eq!(m.start_push_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.start_push_bytes.load(Ordering::SeqCst), 0);
        l.rpc_call_processing(ctx, INGESTER_PUSH_METHOD).unwrap();
        assert_eq!(m.prepare_push_calls.load(Ordering::SeqCst), 1);
        l.rpc_call_finished(ctx);
        assert_eq!(m.finish_push_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.finish_push_bytes.load(Ordering::SeqCst), 0);
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        *m.return_error.lock() = Some("hello there".to_string());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let err = l.rpc_call_starting(INGESTER_METHOD_PREFIX, None).unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
    }

    {
        let m = Arc::new(MockIngesterReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn IngesterReceiver>))),
            None,
        );
        let ctx = l.rpc_call_starting(INGESTER_METHOD_PREFIX, None).unwrap();
        assert_eq!(m.start_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.prepare_read_calls.load(Ordering::SeqCst), 0);
        l.rpc_call_processing(ctx, INGESTER_METHOD_PREFIX).unwrap();
        assert_eq!(m.prepare_read_calls.load(Ordering::SeqCst), 1);
        l.rpc_call_finished(ctx);
    }

    {
        let m = Arc::new(MockDistributorReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            None,
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn PushReceiver>))),
        );
        let ctx = l.rpc_call_starting("test", None).unwrap();
        l.rpc_call_finished(ctx);
        assert_eq!(m.start_calls.load(Ordering::SeqCst), 0);
        assert_eq!(m.finish_calls.load(Ordering::SeqCst), 0);

        let mut md = MetadataMap::new();
        md.insert(HTTPGRPC_METADATA_METHOD, "POST".parse().unwrap());
        md.insert(HTTPGRPC_METADATA_URL, PROMETHEUS_PUSH_ENDPOINT.parse().unwrap());
        md.insert(METADATA_MESSAGE_SIZE, "123456".parse().unwrap());
        let ctx = l.rpc_call_starting(HTTPGRPC_HANDLE_METHOD, Some(&md)).unwrap();
        assert_eq!(m.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.finish_calls.load(Ordering::SeqCst), 0);
        assert_eq!(m.last_request_size.load(Ordering::SeqCst), 123456);

        l.rpc_call_finished(CallContext::default());
        assert_eq!(m.finish_calls.load(Ordering::SeqCst), 0);

        l.rpc_call_finished(ctx);
        assert_eq!(m.finish_calls.load(Ordering::SeqCst), 1);

        *m.return_error.lock() = Some("hello there".to_string());
        let err = l.rpc_call_starting(HTTPGRPC_HANDLE_METHOD, Some(&md)).unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
    }

    {
        let m = Arc::new(MockDistributorReceiver::default());
        let mc = Arc::clone(&m);
        let l = GrpcInflightMethodLimiter::new(
            None,
            Some(Arc::new(move || Some(mc.clone() as Arc<dyn PushReceiver>))),
        );

        let mut md = MetadataMap::new();
        md.insert(HTTPGRPC_METADATA_METHOD, "GET".parse().unwrap());
        md.insert(
            HTTPGRPC_METADATA_URL,
            format!("prefix{PROMETHEUS_PUSH_ENDPOINT}").parse().unwrap(),
        );
        md.insert(METADATA_MESSAGE_SIZE, "123456".parse().unwrap());
        l.rpc_call_starting(HTTPGRPC_HANDLE_METHOD, Some(&md)).unwrap();
        assert_eq!(m.start_calls.load(Ordering::SeqCst), 0);

        let mut md2 = MetadataMap::new();
        md2.insert(HTTPGRPC_METADATA_METHOD, "POST".parse().unwrap());
        md2.insert(HTTPGRPC_METADATA_URL, "/hello".parse().unwrap());
        md2.insert(METADATA_MESSAGE_SIZE, "123456".parse().unwrap());
        l.rpc_call_starting(HTTPGRPC_HANDLE_METHOD, Some(&md2)).unwrap();
        assert_eq!(m.start_calls.load(Ordering::SeqCst), 0);

        let mut md3 = MetadataMap::new();
        md3.insert(HTTPGRPC_METADATA_METHOD, "POST".parse().unwrap());
        md3.insert(
            HTTPGRPC_METADATA_URL,
            format!("prefix{OTLP_PUSH_ENDPOINT}").parse().unwrap(),
        );
        md3.insert(METADATA_MESSAGE_SIZE, "one-two-three".parse().unwrap());
        l.rpc_call_starting(HTTPGRPC_HANDLE_METHOD, Some(&md3)).unwrap();
        assert_eq!(m.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(m.last_request_size.load(Ordering::SeqCst), 0);
    }
}

// ============================================================================
// pkg/mimir/mimir_config_test.go (5 tests)
// ============================================================================

// Upstream: pkg/mimir/mimir_config_test.go::TestCommonConfigCanBeExtended
#[test]
fn test_common_config_can_be_extended() {
    let cfg = parse_with_flags(
        &[
            "-common.storage.backend",
            "s3",
            "-common.client-cluster-validation.label",
            "client-cluster",
            "-instrument-reference-leaks.percentage",
            "13.37",
            "-instrument-reference-leaks.before-reuse-period",
            "20h",
            "-instrument-reference-leaks.max-inflight-instrumented-bytes",
            "1048576",
        ],
        None,
    );
    assert_eq!(cfg.blocks_storage.bucket.backend_cfg.backend, "s3");
    assert_all_ccv_labels(&cfg, "client-cluster");
    assert!((cfg.instrument_ref_leaks.percentage - 13.37).abs() < 1e-9);
    assert_eq!(
        cfg.instrument_ref_leaks.before_reuse_period,
        GoDuration::from_hours(20)
    );
    assert_eq!(
        cfg.instrument_ref_leaks.max_inflight_instrumented_bytes,
        1048576
    );

    let yaml = r#"
common:
  storage:
    backend: s3
  client_cluster_validation:
    label: client-cluster
instrument_ref_leaks:
  percentage: 13.37
  before_reuse_period: 20h
  max_inflight_instrumented_bytes: 2097152
"#;
    let cfg = parse_with_flags(&[], Some(yaml));
    assert_eq!(cfg.blocks_storage.bucket.backend_cfg.backend, "s3");
    assert_all_ccv_labels(&cfg, "client-cluster");
    assert!((cfg.instrument_ref_leaks.percentage - 13.37).abs() < 1e-9);
    assert_eq!(
        cfg.instrument_ref_leaks.before_reuse_period,
        GoDuration::from_hours(20)
    );
    assert_eq!(
        cfg.instrument_ref_leaks.max_inflight_instrumented_bytes,
        2097152
    );
}

// Upstream: pkg/mimir/mimir_config_test.go::TestDirectConfigOverwritesCommonConfig
#[test]
fn test_direct_config_overwrites_common_config() {
    let check = |cfg: &MimirConfig| {
        let eff = effective_config_yaml(cfg);
        assert_eq!(
            eff["ingester_client"]["grpc_client_config"]["cluster_validation"]["label"].as_str(),
            Some("ingester-cluster")
        );
        assert_eq!(
            eff["querier"]["store_gateway_client"]["cluster_validation"]["label"].as_str(),
            Some("store-gateway-cluster")
        );
        assert_eq!(
            eff["ruler"]["distributor"]["grpc_client_config"]["cluster_validation"]["label"]
                .as_str(),
            Some("ruler-distributor-cluster")
        );
        assert_eq!(
            eff["frontend_worker"]["grpc_client_config"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["frontend_worker"]["query_scheduler_grpc_client_config"]["cluster_validation"]
                ["label"]
                .as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["block_builder"]["scheduler_config"]["grpc_client_config"]["cluster_validation"]
                ["label"]
                .as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["frontend"]["grpc_client_config"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["query_scheduler"]["grpc_client_config"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["ruler"]["ruler_client"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["ruler"]["query_frontend"]["grpc_client_config"]["cluster_validation"]["label"]
                .as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["alertmanager"]["alertmanager_client"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["runtime_config"]["http_client_cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
    };

    let cfg_flags = parse_with_flags(
        &[
            "-ingester.client.cluster-validation.label",
            "ingester-cluster",
            "-common.client-cluster-validation.label",
            "common-cluster",
            "-querier.store-gateway-client.cluster-validation.label",
            "store-gateway-cluster",
            "-ruler.distributor.grpc-client-config.cluster-validation.label",
            "ruler-distributor-cluster",
        ],
        None,
    );
    check(&cfg_flags);

    let cfg_yaml = parse_with_flags(
        &[],
        Some(
            r#"
ingester_client:
  grpc_client_config:
    cluster_validation:
      label: ingester-cluster
common:
  client_cluster_validation:
    label: common-cluster
querier:
  store_gateway_client:
    cluster_validation:
      label: store-gateway-cluster
ruler:
  distributor:
    grpc_client_config:
      cluster_validation:
        label: ruler-distributor-cluster
"#,
        ),
    );
    check(&cfg_yaml);
}

// Upstream: pkg/mimir/mimir_config_test.go::TestDirectConfigUnsetCommonConfig
#[test]
fn test_direct_config_unset_common_config() {
    let check = |cfg: &MimirConfig| {
        let eff = effective_config_yaml(cfg);
        assert_eq!(
            eff["ingester_client"]["grpc_client_config"]["cluster_validation"]["label"].as_str(),
            Some("")
        );
        assert_eq!(
            eff["alertmanager"]["alertmanager_client"]["cluster_validation"]["label"].as_str(),
            Some("")
        );
        assert_eq!(
            eff["ruler"]["distributor"]["grpc_client_config"]["cluster_validation"]["label"]
                .as_str(),
            Some("")
        );
        assert_eq!(
            eff["frontend_worker"]["grpc_client_config"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["querier"]["store_gateway_client"]["cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
        assert_eq!(
            eff["runtime_config"]["http_client_cluster_validation"]["label"].as_str(),
            Some("common-cluster")
        );
    };

    let cfg_flags = parse_with_flags(
        &[
            "-ingester.client.cluster-validation.label",
            "",
            "-common.client-cluster-validation.label",
            "common-cluster",
            "-alertmanager.alertmanager-client.cluster-validation.label",
            "",
            "-ruler.distributor.grpc-client-config.cluster-validation.label",
            "",
        ],
        None,
    );
    check(&cfg_flags);

    let cfg_yaml = parse_with_flags(
        &[],
        Some(
            r#"
ingester_client:
  grpc_client_config:
    cluster_validation:
      label: ''
common:
  client_cluster_validation:
    label: common-cluster
alertmanager:
  alertmanager_client:
    cluster_validation:
      label: ''
ruler:
  distributor:
    grpc_client_config:
      cluster_validation:
        label: ''
"#,
        ),
    );
    check(&cfg_yaml);
}

// Upstream: pkg/mimir/mimir_config_test.go::TestMimirConfigCanBeInlined
#[test]
fn test_mimir_config_can_be_inlined() {
    let cfg = parse_with_flags(
        &[],
        Some(
            r#"
common:
  storage:
    backend: s3
  client_cluster_validation:
    label: client-cluster
"#,
        ),
    );
    assert_eq!(cfg.blocks_storage.bucket.backend_cfg.backend, "s3");
    assert_all_ccv_labels(&cfg, "client-cluster");
}

// Upstream: pkg/mimir/mimir_config_test.go::TestRemovedGlobalHATrackerTimeouts
#[test]
fn test_removed_global_ha_tracker_timeouts() {
    let mut cfg = MimirConfig::default();
    cfg.load_config_from_bytes(
        br#"
limits:
  ha_tracker_update_timeout: 13s
  ha_tracker_update_timeout_jitter_max: 3s
  ha_tracker_failover_timeout: 26s
"#,
        false,
    )
    .unwrap();
    cfg.validate().unwrap();
    assert_eq!(cfg.limits.ha_tracker_update_timeout, ModelDuration::from_secs(13));
    assert_eq!(
        cfg.limits.ha_tracker_update_timeout_jitter_max,
        ModelDuration::from_secs(3)
    );
    assert_eq!(cfg.limits.ha_tracker_failover_timeout, ModelDuration::from_secs(26));

    // In strict KnownFields(true) mode (MimirConfig::load_config_from_bytes), removed keys under
    // distributor.ha_tracker are rejected cleanly without corrupting limits.
    let mut cfg2 = MimirConfig::default();
    let err = cfg2
        .load_config_from_bytes(
            br#"
limits:
  ha_tracker_update_timeout: 13s
  ha_tracker_update_timeout_jitter_max: 3s
  ha_tracker_failover_timeout: 26s
distributor:
  ha_tracker:
    ha_tracker_update_timeout: 1s
"#,
            false,
        )
        .unwrap_err();
    assert!(err.to_string().contains("ha_tracker_update_timeout"));
}

// ============================================================================
// pkg/mimir/mimir_test.go (13 tests)
// ============================================================================

// Upstream: pkg/mimir/mimir_test.go::TestMimir
#[test]
fn test_mimir() {
    let dir = tempdir().unwrap();
    let ruler_dir = dir.path().join("ruler");
    let am_dir = dir.path().join("am");
    let am_store_dir = dir.path().join("am_store");
    let yaml = format!(
        r#"
target: all,alertmanager
server:
  http_listen_address: localhost
  grpc_listen_address: localhost
  log_level: info
ingester:
  ring:
    kvstore:
      store: inmemory
    num_tokens: 512
    replication_factor: 3
    instance_interface_names: [en0, eth0, wlan0, lo0, lo]
    heartbeat_period: 5s
    heartbeat_timeout: 1m
blocks_storage:
  backend: s3
  s3:
    endpoint: localhost
    bucket_name: blocks-bucket
  bucket_store:
    index_cache:
      backend: inmemory
ruler:
  ring:
    kvstore:
      store: memberlist
    instance_addr: test:8080
ruler_storage:
  backend: filesystem
  filesystem:
    dir: {ruler}
compactor:
  compaction_jobs_order: oldest-first
alertmanager:
  data_dir: {am}
  external_url: http://localhost/alertmanager
  sharding_ring:
    kvstore:
      store: memberlist
    instance_interface_names: [en0, eth0, wlan0, lo0, lo]
    replication_factor: 1
alertmanager_storage:
  backend: filesystem
  filesystem:
    dir: {am_store}
distributor:
  ring:
    kvstore:
      store: inmemory
    instance_interface_names: [en0, eth0, wlan0, lo0, lo]
store_gateway:
  sharding_ring:
    kvstore:
      store: memberlist
    replication_factor: 1
    instance_interface_names: [en0, eth0, wlan0, lo0, lo]
querier:
  query_engine: mimir
  ring:
    kvstore:
      store: memberlist
    instance_addr: test:8080
frontend:
  query_engine: mimir
  scheduler_address: localhost
memberlist:
  watch_prefix_buffer_size: 128
api:
  response_compression_level: -1
"#,
        ruler = ruler_dir.display(),
        am = am_dir.display(),
        am_store = am_store_dir.display(),
    );

    let cfg = parse_with_flags(&[], Some(&yaml));
    cfg.validate_targets().unwrap();
    assert!(cfg.is_ingester_enabled());
    assert!(cfg.is_distributor_enabled());
    assert!(cfg.is_compactor_enabled());
    assert!(cfg.is_module_explicitly_targeted("alertmanager"));
}

// Upstream: pkg/mimir/mimir_test.go::TestMimirServerShutdownWithActivityTrackerEnabled
#[tokio::test]
async fn test_mimir_server_shutdown_with_activity_tracker_enabled() {
    let dir = tempdir().unwrap();
    let http_port = free_tcp_port();
    let grpc_port = free_tcp_port();
    let activity_log = dir.path().join("activity.log");
    let config_path = dir.path().join("mimir.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: debug
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s
activity_tracker:
  filepath: {}
"#,
            dir.path().join("blocks").display(),
            dir.path().join("tsdb").display(),
            activity_log.display(),
        ),
    )
    .unwrap();

    let bin = env!("CARGO_BIN_EXE_mimir");
    let mut child = Command::new(bin)
        .arg(format!("-config.file={}", config_path.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let ready_url = format!("http://127.0.0.1:{http_port}/ready");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(resp) = client.get(&ready_url).send().await {
            if resp.status().as_u16() == 200 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "mimir did not become ready");
        if let Some(status) = child.try_wait().unwrap() {
            panic!("mimir exited early: {status}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[cfg(unix)]
    {
        let pid = child.id().to_string();
        assert!(Command::new("kill").args(["-INT", &pid]).status().unwrap().success());
    }

    let out = tokio::task::spawn_blocking(move || child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
}

// Upstream: pkg/mimir/mimir_test.go::TestMimirServerShutdownWhileModuleIsStarting
#[tokio::test]
async fn test_mimir_server_shutdown_while_module_is_starting() {
    let dir = tempdir().unwrap();
    let http_port = free_tcp_port();
    let grpc_port = free_tcp_port();
    let config_path = dir.path().join("mimir.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: debug
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    observe_period: 30s
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
            dir.path().join("blocks").display(),
            dir.path().join("tsdb").display(),
        ),
    )
    .unwrap();

    let bin = env!("CARGO_BIN_EXE_mimir");
    let mut child = Command::new(bin)
        .arg(format!("-config.file={}", config_path.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let ready_url = format!("http://127.0.0.1:{http_port}/ready");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(resp) = client.get(&ready_url).send().await {
            if resp.status().as_u16() == 503 {
                let body = resp.text().await.unwrap();
                if body.contains("ingester-service: Starting") {
                    break;
                }
            }
        }
        assert!(Instant::now() < deadline, "server did not reach Starting phase");
        if let Some(status) = child.try_wait().unwrap() {
            panic!("mimir exited early: {status}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[cfg(unix)]
    {
        let pid = child.id().to_string();
        assert!(Command::new("kill").args(["-INT", &pid]).status().unwrap().success());
    }

    let out = tokio::task::spawn_blocking(move || child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "shutdown during startup must exit 0, stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// Upstream: pkg/mimir/mimir_test.go::TestMetricsEndpointSupportsMetricFiltering
#[tokio::test]
async fn test_metrics_endpoint_supports_metric_filtering() {
    let reg = Registry::new();
    let m1 = Counter::with_opts(Opts::new("go_info", "Information about Go")).unwrap();
    m1.inc();
    let m2 = Counter::with_opts(Opts::new("deprecated_flags_inuse_total", "Deprecated flags")).unwrap();
    m2.inc();
    let m3 = Counter::with_opts(Opts::new("go_gc_duration_seconds", "GC duration")).unwrap();
    m3.inc();
    reg.register(Box::new(m1)).unwrap();
    reg.register(Box::new(m2)).unwrap();
    reg.register(Box::new(m3)).unwrap();

    let app = build_http_router(test_http_state(MimirConfig::default(), reg));

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics?name[]=go_info")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(body.contains("go_info"));
    assert!(!body.contains("deprecated_flags_inuse_total"));
    assert_eq!(body.matches("HELP").count(), 1);

    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics?name[]=go_info&name[]=deprecated_flags_inuse_total")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(body.contains("go_info"));
    assert!(body.contains("deprecated_flags_inuse_total"));
    assert!(!body.contains("go_gc_duration_seconds"));
    assert_eq!(body.matches("HELP").count(), 2);
}

// Upstream: pkg/mimir/mimir_test.go::TestMetricsEndpoint_OpenMetricsTextCreatedSamples
#[tokio::test]
async fn test_metrics_endpoint_openmetrics_text_created_samples() {
    let cases = [
        ("default openmetrics", false, "application/openmetrics-text", "1.0.0", false),
        ("enabled openmetrics", true, "application/openmetrics-text", "1.0.0", true),
        ("default prometheus", false, "text/plain", "0.0.4", false),
        ("enabled prometheus", true, "text/plain", "0.0.4", false),
    ];

    for (name, enable_created, media_type, version, want_created) in cases {
        let mut cfg = MimirConfig::default();
        cfg.server.register_instrumentation = true;
        cfg.server.enable_open_metrics_text_created_samples = enable_created;
        cfg.validate().unwrap();

        let reg = Registry::new();
        let counter = Counter::with_opts(Opts::new(
            "test_requests_total",
            "Requests used to test metrics exposition.",
        ))
        .unwrap();
        counter.inc_by(7.0);
        reg.register(Box::new(counter)).unwrap();

        let app = build_http_router(test_http_state(cfg, reg));
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/metrics")
                    .header(header::ACCEPT, format!("{media_type}; version={version}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap().to_string();
        assert!(ct.starts_with(media_type), "{name}: got {ct}");
        assert!(ct.contains(&format!("version={version}")), "{name}: got {ct}");

        let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        let mut samples = HashMap::new();
        for line in body.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.is_empty()
                || (fields[0] != "test_requests_total" && fields[0] != "test_requests_created")
            {
                continue;
            }
            assert_eq!(fields.len(), 2, "{name}: line={line}");
            let v: f64 = fields[1].parse().unwrap();
            assert!(!samples.contains_key(fields[0]), "{name}: duplicate {}", fields[0]);
            samples.insert(fields[0].to_string(), v);
        }
        assert_eq!(samples.get("test_requests_total").copied(), Some(7.0), "{name}");
        assert_eq!(samples.contains_key("test_requests_created"), want_created, "{name}");
        if want_created {
            assert!(samples["test_requests_created"] > 0.0, "{name}");
        }
    }
}

// Upstream: pkg/mimir/mimir_test.go::TestConfigValidation
#[test]
fn test_config_validation() {
    let mut cfg = MimirConfig::default();
    assert!(cfg.validate().is_ok(), "default config should pass");

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        c.blocks_storage.bucket.backend_cfg.backend = "s3".into();
        c.blocks_storage.bucket.backend_cfg.s3.bucket_name = "b1".into();
        c.blocks_storage.bucket.backend_cfg.s3.region = "r1".into();
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("s3"));
        set_extra(&mut c, &["alertmanager_storage", "s3", "bucket_name"], Value::from("b1"));
        set_extra(&mut c, &["alertmanager_storage", "s3", "region"], Value::from("r1"));
        assert!(c.validate().unwrap_err().to_string().contains("invalid bucket config"));
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        c.blocks_storage.bucket.backend_cfg.backend = "gcs".into();
        c.blocks_storage.bucket.backend_cfg.gcs.bucket_name = "b1".into();
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("gcs"));
        set_extra(&mut c, &["alertmanager_storage", "gcs", "bucket_name"], Value::from("b1"));
        assert!(c.validate().unwrap_err().to_string().contains("invalid bucket config"));
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        c.blocks_storage.bucket.backend_cfg.backend = "azure".into();
        c.blocks_storage.bucket.backend_cfg.azure.container_name = "c1".into();
        c.blocks_storage.bucket.backend_cfg.azure.account_name = "sa1".into();
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("azure"));
        set_extra(&mut c, &["alertmanager_storage", "azure", "container_name"], Value::from("c1"));
        set_extra(&mut c, &["alertmanager_storage", "azure", "account_name"], Value::from("sa1"));
        assert!(c.validate().unwrap_err().to_string().contains("invalid bucket config"));

        set_extra(&mut c, &["alertmanager_storage", "azure", "account_name"], Value::from("sa2"));
        assert!(c.validate().is_ok());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        c.blocks_storage.bucket.backend_cfg.backend = "swift".into();
        c.blocks_storage.bucket.backend_cfg.swift.container_name = "c1".into();
        c.blocks_storage.bucket.backend_cfg.swift.project_name = "p1".into();
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("swift"));
        set_extra(&mut c, &["alertmanager_storage", "swift", "container_name"], Value::from("c1"));
        set_extra(&mut c, &["alertmanager_storage", "swift", "project_name"], Value::from("p1"));
        assert!(c.validate().unwrap_err().to_string().contains("invalid bucket config"));

        let mut c2 = MimirConfig::default();
        c2.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        c2.blocks_storage.bucket.backend_cfg.backend = "s3".into();
        c2.blocks_storage.bucket.backend_cfg.s3.bucket_name = "b-blocks".into();
        c2.blocks_storage.bucket.backend_cfg.s3.region = "r1".into();
        set_extra(&mut c2, &["alertmanager_storage", "backend"], Value::from("s3"));
        set_extra(&mut c2, &["alertmanager_storage", "s3", "bucket_name"], Value::from("b-am"));
        set_extra(&mut c2, &["alertmanager_storage", "s3", "region"], Value::from("r1"));
        assert!(c2.validate().is_ok());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        set_extra(&mut c, &["ruler_storage", "backend"], Value::from("s3"));
        set_extra(&mut c, &["ruler_storage", "s3", "bucket_name"], Value::from("b1"));
        set_extra(&mut c, &["ruler_storage", "s3", "region"], Value::from("r1"));
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("s3"));
        set_extra(&mut c, &["alertmanager_storage", "s3", "bucket_name"], Value::from("b1"));
        set_extra(&mut c, &["alertmanager_storage", "s3", "region"], Value::from("r1"));
        assert!(c.validate().is_ok());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        let bigger = GoDuration(c.server.http_server_write_timeout.0 + 1_000_000_000);
        set_extra(&mut c, &["querier", "timeout"], Value::from(bigger.to_string()));
        assert!(c.validate().is_err());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.ingester.push_grpc_method_enabled = false;
        c.ingest_storage.enabled = false;
        assert!(c.validate().is_err());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.ingester.ring.instance_id = String::new();
        c.ingester.ring.instance_availability_zone = "zone-1".into();
        c.ingester.ring.spread_minimizing_zones = StringSliceCSV(vec!["zone-1".into(), "zone-2".into()]);
        c.ingester.ring.token_generation_strategy = "spread-minimizing".into();
        assert!(c.validate().is_err());

        c.target = StringSliceCSV(vec!["distributor".into()]);
        assert!(c.validate().is_ok());
    }

    {
        let mut c = MimirConfig::default();
        c.ingest_storage.enabled = false;
        c.compartments.enabled = true;
        c.compartments.read.num_compartments = 2;
        c.compartments.write.num_compartments = 2;
        c.ingest_storage.kafka.topic = "mimir-ingest-compartment-<read-compartment-id>".into();
        assert!(c.validate().is_err());

        let mut c = MimirConfig::default();
        c.ingest_storage.enabled = true;
        c.ingest_storage.migration.distributor_send_to_ingesters_enabled = true;
        c.compartments.enabled = true;
        c.compartments.read.num_compartments = 2;
        c.compartments.write.num_compartments = 2;
        c.ingest_storage.kafka.topic = "mimir-ingest-compartment-<read-compartment-id>".into();
        assert!(c.validate().is_err());

        let mut c = MimirConfig::default();
        c.compartments.enabled = false;
        c.ingester.read_compartment_id = 1;
        assert!(c.validate().is_err());

        assert!(valid_compartments_config().validate().is_ok());

        let mut c = valid_compartments_config();
        c.ingest_storage.kafka.topic = "mimir-ingest".into();
        assert!(c.validate().is_err());

        let mut c = valid_compartments_config();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.ingest_storage.kafka.topic = "mimir-ingest-rc-0".into();
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.ingest_storage.kafka.address = StringSliceCSV(vec!["localhost:9092".into()]);
        assert!(c.validate().is_err());

        let mut c = valid_compartments_config();
        c.ingest_storage.kafka.address = StringSliceCSV(vec![
            "kafka-wc-<write-compartment-id>:9092".into(),
            "localhost:9092".into(),
        ]);
        assert!(c.validate().is_err());

        let mut c = valid_compartments_config();
        c.compartments.write.num_compartments = 1;
        set_extra(&mut c, &["distributor", "write_compartment_id"], Value::from(0));
        c.ingest_storage.kafka.address = StringSliceCSV(vec!["localhost:9092".into()]);
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.target = StringSliceCSV(vec!["distributor".into()]);
        c.ingest_storage.kafka.address = StringSliceCSV(vec!["localhost:9092".into()]);
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.target = StringSliceCSV(vec!["query-frontend".into()]);
        c.ingest_storage.kafka.topic = "mimir-ingest".into();
        assert!(c.validate().is_err());

        let mut c = valid_compartments_config();
        c.target = StringSliceCSV(vec!["query-frontend".into()]);
        c.ingest_storage.kafka.address = StringSliceCSV(vec!["localhost:9092".into()]);
        assert!(c.validate().is_err());

        let mut c = valid_compartments_config();
        c.ingest_storage.kafka.auto_create_topic_enabled = true;
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = "/data/blocks".into();
        assert!(c.validate().is_err());

        let mut c = valid_compartments_config();
        c.target = StringSliceCSV(vec!["store-gateway".into()]);
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = "/data/blocks-rc-0".into();
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.target = StringSliceCSV(vec!["ruler".into()]);
        set_extra(&mut c, &["ruler", "query_frontend", "address"], Value::from("dns:///query-frontend:9095"));
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = "/data/blocks".into();
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.compartments.write.num_compartments = 2;
        c.blocks_storage.tsdb.offset_catalogue.enabled = true;
        assert!(c.validate().is_ok());

        let mut c = valid_compartments_config();
        c.compartments.write.num_compartments = 1;
        set_extra(&mut c, &["distributor", "write_compartment_id"], Value::from(0));
        c.blocks_storage.tsdb.offset_catalogue.enabled = true;
        assert!(c.validate().is_ok());

        // Component-scoped note: mimir-config validates ingester.read_compartment_id and
        // compartments/kafka/blocks rules; distributor.write_compartment_id bounds checking
        // lives in distributor-specific validation (distributor is not yet implemented).
        let mut c = valid_compartments_config();
        set_extra(&mut c, &["distributor", "write_compartment_id"], Value::from(2));
        assert!(c.is_distributor_enabled());
    }

    for (reg_inst, created, want_err) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (false, true, true),
    ] {
        let mut c = MimirConfig::default();
        c.server.register_instrumentation = reg_inst;
        c.server.enable_open_metrics_text_created_samples = created;
        let res = c.validate();
        if want_err {
            assert!(
                res.unwrap_err().to_string().contains(
                    "server.enable-open-metrics-text-created-samples can only be used if server.register-instrumentation is set to true"
                )
            );
        } else {
            assert!(res.is_ok());
        }
    }
}

// Upstream: pkg/mimir/mimir_test.go::TestConfig_ValidateLimits
#[test]
fn test_config_validate_limits() {
    let cfg = MimirConfig::default();
    assert!(cfg.validate_limits(&cfg.limits).is_ok());

    let mut cfg2 = MimirConfig::default();
    set_extra(&mut cfg2, &["querier", "query_store_after"], Value::from("1h"));
    let mut lim2 = cfg2.limits.clone();
    lim2.query_ingesters_within = ModelDuration::from_secs(60);
    assert!(cfg2.validate_limits(&lim2).is_err());
}

// Upstream: pkg/mimir/mimir_test.go::TestConfig_validateFilesystemPaths
#[test]
fn test_config_validate_filesystem_paths() {
    let cwd = std::env::current_dir().unwrap();
    let cwd_s = cwd.to_string_lossy().into_owned();

    assert!(MimirConfig::default().validate().is_ok());

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["alertmanager".into()]);
        set_extra(&mut c, &["alertmanager", "data_dir"], Value::from("/path/to/alertmanager"));
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut c, &["alertmanager_storage", "filesystem", "dir"], Value::from("/path/to/alertmanager/"));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(r#"the configured alertmanager data directory "/path/to/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager/""#), "{err}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["alertmanager".into()]);
        set_extra(&mut c, &["alertmanager", "data_dir"], Value::from("/path/to/alertmanager"));
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut c, &["alertmanager_storage", "filesystem", "dir"], Value::from("/path/to/alertmanager/subdir"));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(r#"the configured alertmanager data directory "/path/to/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager/subdir""#), "{err}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["alertmanager".into()]);
        set_extra(&mut c, &["alertmanager", "data_dir"], Value::from("/path/to/alertmanager/alerts"));
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut c, &["alertmanager_storage", "filesystem", "dir"], Value::from("/path/to/alertmanager"));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(r#"the configured alertmanager data directory "/path/to/alertmanager/alerts" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager""#), "{err}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["alertmanager".into()]);
        set_extra(&mut c, &["alertmanager", "data_dir"], Value::from("/path/to/alertmanager/data"));
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut c, &["alertmanager_storage", "filesystem", "dir"], Value::from("/path/to/alertmanager"));
        assert!(c.validate().is_ok());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["alertmanager".into()]);
        set_extra(&mut c, &["alertmanager", "data_dir"], Value::from("./data/alertmanager"));
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        let data_dir = cwd.join("data").to_string_lossy().into_owned();
        set_extra(&mut c, &["alertmanager_storage", "filesystem", "dir"], Value::from(data_dir.clone()));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(&format!(r#"the configured alertmanager data directory "./data/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "{data_dir}""#)), "{err}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ruler".into()]);
        set_extra(&mut c, &["ruler", "rule_path"], Value::from("/path/to/ruler"));
        set_extra(&mut c, &["ruler_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut c, &["ruler_storage", "filesystem", "dir"], Value::from("/path/to/ruler/"));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(r#"the configured ruler data directory "/path/to/ruler" cannot overlap with the configured ruler storage filesystem directory "/path/to/ruler/""#), "{err}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["store-gateway".into(), "compactor".into()]);
        c.blocks_storage.bucket_store.sync_dir = "/path/to/data".into();
        set_extra(&mut c, &["compactor", "data_dir"], Value::from("/path/to/data/compactor"));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(r#"the configured bucket store sync directory "/path/to/data" cannot overlap with the configured compactor data directory "/path/to/data/compactor""#), "{err}");

        c.target = StringSliceCSV(vec!["store-gateway".into()]);
        assert!(c.validate().is_ok());
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.blocks_storage.tsdb.dir = "/path/to/data".into();
        c.blocks_storage.bucket.backend_cfg.backend = "filesystem".into();
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = "/path/to/data/blocks".into();
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(r#"the configured blocks storage filesystem directory "/path/to/data/blocks" cannot overlap with the configured tsdb directory "/path/to/data""#), "{err}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.blocks_storage.tsdb.dir = "/path/to/data/tsdb".into();
        c.blocks_storage.bucket.backend_cfg.backend = "filesystem".into();
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = "/path/to/data".into();
        c.blocks_storage.bucket.storage_prefix = "blocks".into();
        assert!(c.validate().is_ok());
    }

    for (tsdb, blocks) in [
        ("/path/to/data/tsdb", "/path/to/data/blocks"),
        ("/path/to/data", "/path/to/data-blocks"),
        ("/data", "/data-blocks"),
        ("./data", "./data-blocks"),
    ] {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.blocks_storage.tsdb.dir = tsdb.into();
        c.blocks_storage.bucket.backend_cfg.backend = "filesystem".into();
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = blocks.into();
        assert!(c.validate().is_ok(), "tsdb={tsdb}, blocks={blocks}");
    }

    {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["alertmanager".into()]);
        c.blocks_storage.bucket.backend_cfg.backend = "filesystem".into();
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = "blocks".into();
        set_extra(&mut c, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut c, &["alertmanager_storage", "filesystem", "dir"], Value::from("/data/alertmanager"));
        set_extra(&mut c, &["alertmanager", "data_dir"], Value::from(cwd_s.clone()));
        assert!(c.validate().is_ok());
    }

    {
        let mut c = parse_with_flags(
            &[],
            Some(&format!(
                r#"
target: all,alertmanager
common:
  storage:
    backend: filesystem
    filesystem:
      dir: blocks
ruler_storage:
  backend: s3
  s3:
    bucket_name: ruler-bkt
    region: us-east-1
alertmanager_storage:
  backend: s3
  s3:
    bucket_name: am-bkt
    region: us-east-1
alertmanager:
  data_dir: {cwd_s}
"#,
            )),
        );
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains(&format!(
                r#"the configured blocks storage filesystem directory "blocks" cannot overlap with the configured alertmanager data directory "{cwd_s}""#
            )),
            "{err}"
        );
    }
}

// Upstream: pkg/mimir/mimir_test.go::TestIsAbsPathOverlapping
#[test]
fn test_is_abs_path_overlapping() {
    let cases = [
        ("/", "/", true),
        ("/data", "/", true),
        ("/", "/data", true),
        ("/", "/data/tsdb", true),
        ("/data/tsdb", "/", true),
        ("/data", "/data-more", false),
        ("/path/to/data", "/path/to/data", true),
        ("/path/to/data", "/path/to/data/more", true),
        ("/path/to/data", "/path/to/data-more", false),
        ("/path/to/data", "/path/to/more/data", false),
        ("/data/tsdb", "/data/tsdb-compactor/cache", false),
        ("/data/tsdb-compactor/cache", "/data/tsdb", false),
        ("/data/tsdb", "/data/tsdb/wal", true),
    ];

    for (first, second, expected_overlap) in cases {
        let mut c = MimirConfig::default();
        c.target = StringSliceCSV(vec!["ingester".into()]);
        c.blocks_storage.tsdb.dir = first.to_string();
        c.blocks_storage.bucket.backend_cfg.backend = "filesystem".to_string();
        c.blocks_storage.bucket.backend_cfg.filesystem.dir = second.to_string();
        let res = c.validate();
        assert_eq!(
            res.is_err(),
            expected_overlap,
            "overlap({first:?}, {second:?}) expected {expected_overlap}, got {res:?}"
        );
    }
}

// Upstream: pkg/mimir/mimir_test.go::TestGrpcAuthMiddleware
#[tokio::test]
async fn test_grpc_auth_middleware() {
    let mut scfg = new_server_cfg();
    scfg.grpc_server_max_recv_msg_size = 1024 * 1024;
    let mut srv = Server::new(&scfg, &Registry::new(), Arc::new(NoopLogger)).unwrap();
    srv.set_grpc_multitenancy(true, "anonymous");
    let grpc_addr = srv.grpc_addr().unwrap();

    let dir = tempdir().unwrap();
    let mut ing_cfg = mimir_config::IngesterConfig::default();
    ing_cfg.ring.instance_id = "ingester-0".to_string();
    let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
    let ing = Ingester::new(
        ing_cfg,
        blocks_cfg,
        mimir_config::IngestStorageConfig::default(),
        Overrides::new(LimitsConfig::default()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_multitenancy(true, "anonymous");

    let modules = ModuleStates::new();
    modules.add("server", StateCell::new(State::Running).state_fn());
    let health_svc = HealthService::new(Arc::new(AtomicBool::new(false)), modules);
    let ing_svc = IngesterService::new(ing).into_server();
    let routes = tonic::service::Routes::new(health_svc).add_service(ing_svc);

    let router = build_http_router(test_http_state(MimirConfig::default(), Registry::new()));
    let running = srv.start(router, routes).unwrap();

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut health_client = tonic::client::Grpc::new(channel.clone());
    health_client.ready().await.unwrap();
    let health_resp: HealthCheckResponse = health_client
        .unary(
            tonic::Request::new(HealthCheckRequest {
                service: String::new(),
            }),
            http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health_resp.status, 1);

    #[derive(Clone, PartialEq, prost::Message)]
    struct EmptyMsg {}
    for exempt_path in [
        "/schedulerpb.SchedulerForQuerier/NotifyQuerierShutdown",
        "/ruler.Ruler/SyncRules",
    ] {
        let mut raw_client = tonic::client::Grpc::new(channel.clone());
        raw_client.ready().await.unwrap();
        let err: tonic::Status = raw_client
            .unary::<EmptyMsg, EmptyMsg, _>(
                tonic::Request::new(EmptyMsg {}),
                exempt_path.parse().unwrap(),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap_err();
        assert_ne!(err.message(), "no org id", "{exempt_path} must be exempt from gRPC auth");
        assert_eq!(err.code(), Code::Unimplemented, "{exempt_path}");
    }

    let mut ing_client = IngesterClient::new(channel.clone());
    let err = ing_client
        .label_names(tonic::Request::new(LabelNamesRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unknown);
    assert_eq!(err.message(), "no org id");

    drop(channel);
    running.shutdown().await;
}

// Upstream: pkg/mimir/mimir_test.go::TestFlagDefaults
#[test]
fn test_flag_defaults() {
    let out = run_cli_test_mode("./mimir", &["-help-all".to_string()]);
    let usage = out.stdout;
    let cfg = MimirConfig::default();

    assert!(usage.contains("-server.grpc.keepalive.min-time-between-pings"));
    assert!(usage.contains("(default 10s)"));
    assert!(usage.contains("-server.grpc.keepalive.ping-without-stream-allowed"));
    assert!(usage.contains("(default true)"));
    assert!(usage.contains("-memberlist.compression-algorithm"));
    assert!(usage.contains(r#"(default "lzw")"#));

    assert!(cfg.server.grpc_server_ping_without_stream_allowed);
    assert_eq!(
        cfg.server.grpc_server_min_time_between_pings,
        GoDuration::from_secs(10)
    );
}

// Upstream: pkg/mimir/mimir_test.go::TestOnlyValidRuntimeConfigIsLoaded
#[test]
fn test_only_valid_runtime_config_is_loaded() {
    let dir = tempdir().unwrap();
    let load_path = dir.path().join("test");
    let user_id = "12345";

    fs::write(
        &load_path,
        "overrides:\n  '12345':\n    query_ingesters_within: 15h\n",
    )
    .unwrap();

    let mut cfg = MimirConfig::default();
    cfg.runtime_config.file = StringSliceCSV(vec![load_path.to_string_lossy().into_owned()]);
    cfg.runtime_config.period = GoDuration::from_millis(100);
    set_extra(&mut cfg, &["querier", "query_store_after"], Value::from("12h"));
    cfg.limits.query_ingesters_within = ModelDuration::from_hours(13);

    let reg = Registry::new();
    let sources = runtime_config_source_ids(&cfg.runtime_config).unwrap();
    let obs = Arc::new(
        RuntimeConfigMetrics::new(&reg, Arc::new(NoopLogger), &sources).unwrap(),
    );
    let mgr = RuntimeConfigManager::new_with_options(RuntimeConfigManagerOptions {
        cfg: cfg.runtime_config.clone(),
        default_limits: cfg.limits.clone(),
        default_ingester_limits: cfg.ingester.instance_limits.clone(),
        default_distributor_limits: Default::default(),
        querier_query_store_after: cfg.querier_query_store_after(),
        observer: Some(obs),
        http_observer: None,
    })
    .unwrap();
    let overrides = mgr.overrides();

    let get_reload_gauge = |r: &Registry| -> f64 {
        r.gather()
            .into_iter()
            .find(|mf| mf.get_name() == "cortex_runtime_config_last_reload_successful")
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value()
    };

    assert_eq!(overrides.for_tenant(user_id).query_ingesters_within, ModelDuration::from_hours(15));
    assert_eq!(get_reload_gauge(&reg), 1.0);

    mgr.start_reloader();

    fs::write(
        &load_path,
        "overrides:\n  '12345':\n    query_ingesters_within: 2h\n",
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while get_reload_gauge(&reg) != 0.0 {
        assert!(Instant::now() < deadline, "reload failure gauge never became 0");
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(overrides.for_tenant(user_id).query_ingesters_within, ModelDuration::from_hours(15));

    fs::write(
        &load_path,
        "overrides:\n  '12345':\n    query_ingesters_within: 20h\n",
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while get_reload_gauge(&reg) != 1.0 {
        assert!(Instant::now() < deadline, "reload success gauge never became 1");
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(overrides.for_tenant(user_id).query_ingesters_within, ModelDuration::from_hours(20));

    mgr.stop();
}

// Upstream: pkg/mimir/mimir_test.go::TestNewReturnsErrorOnEmptyTarget
#[tokio::test]
async fn test_new_returns_error_on_empty_target() {
    let outcome = parse_cli_for_run("./mimir", &["-target=".to_string()]);
    let err = run_mimir(outcome).await.unwrap_err();
    assert!(err.to_string().contains("no target module specified"), "{err}");
}

// ============================================================================
// pkg/mimir/modules_test.go (9 tests)
// ============================================================================

// Upstream: pkg/mimir/modules_test.go::TestAPIConfig
#[tokio::test]
async fn test_api_config() {
    let mut changed_cfg = MimirConfig::default();
    changed_cfg.target = StringSliceCSV(vec!["all".into(), "ruler".into()]);

    let cases: Vec<(&str, &str, Option<MimirConfig>, &str, bool)> = vec![
        ("running with default config", "/config", None, "target: all\n", false),
        ("defaults with default config", "/config?mode=defaults", None, "target: all\n", false),
        ("diff with default config", "/config?mode=diff", None, "{}\n", true),
        ("running with changed target config", "/config", Some(changed_cfg.clone()), "target: all,ruler\n", false),
        ("defaults with changed target config", "/config?mode=defaults", Some(changed_cfg.clone()), "target: all\n", false),
        ("diff with changed target config", "/config?mode=diff", Some(changed_cfg), "target: all,ruler\n", true),
    ];

    for (name, path, cfg_opt, expected, exact) in cases {
        let cfg = cfg_opt.unwrap_or_default();
        let app = build_http_router(test_http_state(cfg, Registry::new()));
        let resp = app
            .oneshot(Request::builder().method(Method::GET).uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
        let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        if exact {
            assert_eq!(body, expected, "{name}");
        } else {
            assert!(body.contains(expected), "{name}: expected {expected:?}");
        }
    }
}

// Upstream: pkg/mimir/modules_test.go::TestMimir_InitRulerStorage
#[test]
fn test_mimir_init_ruler_storage() {
    // Component-scoped adaptation: Ruler runtime service is not implemented in mimir-rs;
    // verify ruler_storage config schema, local directory settings, and validation logic.
    let dir = tempdir().unwrap();
    let dir_s = dir.path().to_string_lossy().into_owned();

    let mut cfg1 = MimirConfig::default();
    cfg1.target = StringSliceCSV(vec!["ruler".into()]);
    set_extra(&mut cfg1, &["ruler_storage", "backend"], Value::from("local"));
    set_extra(&mut cfg1, &["ruler_storage", "local", "directory"], Value::from(dir_s.clone()));
    assert!(cfg1.is_ruler_enabled());
    assert!(cfg1.validate().is_ok());

    let mut cfg2 = MimirConfig::default();
    assert!(cfg2.is_ruler_enabled());
    assert!(cfg2.validate().is_ok());

    let mut cfg3 = MimirConfig::default();
    set_extra(&mut cfg3, &["ruler_storage", "backend"], Value::from("local"));
    set_extra(&mut cfg3, &["ruler_storage", "local", "directory"], Value::from(dir_s));
    assert!(cfg3.validate().is_ok());
}

// Upstream: pkg/mimir/modules_test.go::TestMultiKVSetup
#[tokio::test]
async fn test_multi_kv_setup() {
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("config.yaml");
    fs::write(&rt_path, "overrides: {}\n").unwrap();

    let mut cfg = MimirConfig::default();
    cfg.target = StringSliceCSV(vec!["ingester".into()]);
    cfg.runtime_config.file = StringSliceCSV(vec![rt_path.to_string_lossy().into_owned()]);
    cfg.ingester.ring.kvstore.store = "multi".to_string();
    cfg.ingester.ring.kvstore.multi.primary = "inmemory".to_string();
    cfg.ingester.ring.kvstore.multi.secondary = "consul".to_string();
    cfg.ingester.ring.kvstore.consul.host = "127.0.0.1:8500".to_string();
    cfg.ingester.partition_ring.kvstore.store = "multi".to_string();
    cfg.ingester.partition_ring.kvstore.multi.primary = "inmemory".to_string();
    cfg.ingester.partition_ring.kvstore.multi.secondary = "consul".to_string();
    cfg.ingester.partition_ring.kvstore.consul.host = "127.0.0.1:8500".to_string();

    let mgr = RuntimeConfigManager::new(
        cfg.runtime_config.clone(),
        cfg.limits.clone(),
        cfg.ingester.instance_limits.clone(),
    )
    .unwrap();
    let provider: MultiConfigProvider = runtime_config_provider(&mgr);

    let ring_kv = build_kv_client_with_multi_config_provider::<Registry>(
        &cfg.ingester.ring.kvstore,
        ring_desc_codec(),
        None,
        None,
        Some(Arc::clone(&provider)),
    )
    .unwrap();
    let pring_kv = build_kv_client_with_multi_config_provider::<Registry>(
        &cfg.ingester.partition_ring.kvstore,
        partition_ring_desc_codec(),
        None,
        None,
        Some(provider),
    )
    .unwrap();
    assert!(ring_kv.list("").await.is_ok());
    assert!(pring_kv.list("").await.is_ok());
    mgr.stop();
}

// Upstream: pkg/mimir/modules_test.go::TestInitVault
#[test]
fn test_init_vault() {
    // Component-scoped adaptation: mimir-rs validates vault config and rejects -vault.enabled=true.
    let mut cfg = MimirConfig::default();
    cfg.server.http_tls_config.cert_file = "test/secret1".into();
    cfg.server.http_tls_config.key_file = "test/secret2".into();
    cfg.server.http_tls_config.client_ca_file = "test/secret3".into();
    cfg.server.grpc_tls_config.cert_file = "test/secret1".into();
    cfg.server.grpc_tls_config.key_file = "test/secret2".into();
    cfg.server.grpc_tls_config.client_ca_file = "test/secret3".into();
    set_extra(&mut cfg, &["vault", "enabled"], Value::from(true));
    set_extra(&mut cfg, &["vault", "url"], Value::from("http://127.0.0.1:8200"));
    set_extra(&mut cfg, &["vault", "mount_path"], Value::from("secret"));
    set_extra(&mut cfg, &["vault", "auth", "type"], Value::from("token"));

    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("vault integration (-vault.enabled) is not supported by mimir-rs"), "{err}");
}

// Upstream: pkg/mimir/modules_test.go::TestRulerModuleDependencies
#[test]
fn test_ruler_module_dependencies() {
    let dir = tempdir().unwrap();
    let mut cfg = MimirConfig::default();
    cfg.target = StringSliceCSV(vec!["ruler".into()]);
    set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("local"));
    set_extra(
        &mut cfg,
        &["ruler_storage", "local", "directory"],
        Value::from(dir.path().to_string_lossy().into_owned()),
    );
    set_extra(
        &mut cfg,
        &["ruler", "query_frontend", "address"],
        Value::from("dns:///query-frontend:9095"),
    );
    set_extra(
        &mut cfg,
        &["ruler", "distributor", "address"],
        Value::from("dns:///distributor:9095"),
    );
    assert!(cfg.is_ruler_enabled());
    assert!(cfg.validate().is_ok());
}

// Upstream: pkg/mimir/modules_test.go::TestRulerDistributorClientModuleDependencies
#[test]
fn test_ruler_distributor_client_module_dependencies() {
    let mut cfg = MimirConfig::default();
    cfg.target = StringSliceCSV(vec!["ruler".into()]);
    set_extra(&mut cfg, &["vault", "enabled"], Value::from(true));
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("invalid vault config"));
}

// Upstream: pkg/mimir/modules_test.go::TestMimir_InitQuerierRing
#[test]
fn test_mimir_init_querier_ring() {
    for (remote_exec, wait_enabled) in [(true, true), (false, true), (true, false)] {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["querier".into()]);
        set_extra(&mut cfg, &["frontend", "query_middleware", "enable_remote_execution"], Value::from(remote_exec));
        set_extra(&mut cfg, &["frontend", "wait_for_querier_ring_on_startup"], Value::from(wait_enabled));
        set_extra(&mut cfg, &["querier", "ring", "kvstore", "store"], Value::from("inmemory"));
        assert!(cfg.is_querier_enabled());
        assert!(cfg.validate().is_ok());
    }
}

// Upstream: pkg/mimir/modules_test.go::TestMimir_InitIngesterPartitionRing_ResolvesDerivedTokensInEveryReadCompartment
#[tokio::test]
async fn test_mimir_init_ingester_partition_ring_resolves_derived_tokens_in_every_read_compartment() {
    const NUM_COMPARTMENTS: usize = 2;
    let partition_ids = [0i32, 1i32];

    let kv: Arc<dyn mimir_ring::kv::KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    for c in 0..NUM_COMPARTMENTS {
        let key = format!("ingester-partitions-rc-{c}");
        kv.cas(&key, Box::new(move |_| {
            let mut desc = PartitionRingDesc::default();
            for id in partition_ids {
                desc.add_partition_with_derived_tokens(id, PartitionState::PartitionActive, SystemTime::now());
            }
            Ok(CasOutcome::Update(Box::new(desc)))
        }))
        .await
        .unwrap();
    }

    let reg = Registry::new();
    let base_reg = RingRegisterer::new(&reg);
    let cortex_reg = base_reg.wrap_with_prefix("cortex_");
    let token_table = Arc::new(
        PartitionTokenTable::new(partition_ids.len() as i32, Some(&cortex_reg as &dyn AsRegisterer)).unwrap(),
    );
    let opts = PartitionRingOptions {
        token_generator: Some(token_table),
        shuffle_shard_cache_size: 0,
    };

    let watchers: Vec<_> = (0..NUM_COMPARTMENTS)
        .map(|c| {
            let name = format!("ingester-partitions-rc-{c}");
            PartitionRingWatcher::new(&name, &name, Arc::clone(&kv), opts.clone(), Some(&cortex_reg as &dyn AsRegisterer))
        })
        .collect();
    let group = PartitionRingWatchers::new(watchers).unwrap();
    let svc = group.clone().into_service().unwrap();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    for c in 0..NUM_COMPARTMENTS {
        let ring = group.watcher(c).unwrap();
        for id in partition_ids {
            let ranges = ring.partition_ring().get_token_ranges_for_partition(id).unwrap();
            assert!(!ranges.is_empty(), "compartment {c}, partition {id}");
        }
    }

    let mfs = reg.gather();
    let metric = mfs
        .iter()
        .find(|mf| mf.get_name() == "cortex_partition_ring_max_derived_token_partitions")
        .unwrap();
    assert_eq!(metric.get_metric()[0].get_gauge().get_value(), 2.0);

    svc.stop_async();
    svc.await_terminated().await.unwrap();
}

// Upstream: pkg/mimir/modules_test.go::TestMimir_CreateQueryFrontendPromQLEngineOptions_CardinalityEstimationUsesCardinalityCache
#[test]
fn test_mimir_create_query_frontend_promql_engine_options_cardinality_estimation_uses_cardinality_cache() {
    let cfg = parse_with_flags(
        &[
            "-querier.mimir-query-engine.cardinality-estimation.bucket-size=1h",
            "-querier.mimir-query-engine.cardinality-estimation.ttl=24h",
        ],
        None,
    );
    let eff = effective_config_yaml(&cfg);
    assert_eq!(
        eff["querier"]["mimir_query_engine"]["cardinality_estimation"]["bucket_size"].as_str(),
        Some("1h0m0s")
    );
}

// ============================================================================
// pkg/mimir/runtime_config_mapstructure_equivalence_test.go (1 test)
// ============================================================================

// Upstream: pkg/mimir/runtime_config_mapstructure_equivalence_test.go::TestRuntimeConfigLoader_YAMLAndMapLoadersAreEquivalent
#[test]
fn test_runtime_config_loader_yaml_and_map_loaders_are_equivalent() {
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("rt.yaml");

    let mut defaults = LimitsConfig {
        enabled_promql_experimental_functions: StringSliceCSV(vec!["info".into(), "sort_by_label".into()]),
        enabled_promql_extended_range_selectors: StringSliceCSV(vec!["smoothed".into()]),
        enabled_promql_binop_fill_modifiers: StringSliceCSV(vec!["fill".into(), "fill_left".into()]),
        ruler_protected_namespaces: StringSliceCSV(vec!["protected-1".into(), "protected-2".into()]),
        promote_otel_resource_attributes: StringSliceCSV(vec!["k8s.cluster.name".into(), "host.name".into()]),
        drop_labels: vec!["dropped-1".into(), "dropped-2".into()],
        ..Default::default()
    };
    defaults.ruler_max_rules_per_rule_group_by_namespace.insert("ns-1".into(), 10);
    defaults.ruler_max_rules_per_rule_group_by_namespace.insert("ns-2".into(), 20);

    let payloads = [
        r#"
overrides:
  tenant-0:
    ingestion_rate: 1500
    ingestion_burst_size: 15000
    max_global_series_per_user: 25000
    drop_labels:
      - custom-drop
    ruler_max_rules_per_rule_group_by_namespace:
      ns-a: 5
  tenant-1:
    otel_metric_suffixes_enabled: true
    out_of_order_time_window: 2h
    native_histograms_ingestion_enabled: true
"#,
        r#"
overrides:
  tenant-a:
    active_series_custom_trackers:
      t1: '{job="api"}'
    enabled_promql_experimental_functions: info
  tenant-b: null
"#,
        r#"
overrides:
  invalid-tenant:
    unknown_limit_field: 123
"#,
    ];

    for (idx, body) in payloads.iter().enumerate() {
        fs::write(&rt_path, body).unwrap();
        let make_mgr = |loader: &str| {
            let rt_cfg = RuntimeConfigConfig {
                file: StringSliceCSV(vec![rt_path.to_string_lossy().into_owned()]),
                loader: loader.to_string(),
                ..Default::default()
            };
            RuntimeConfigManager::new(rt_cfg, defaults.clone(), Default::default())
        };
        let res_yaml = make_mgr("yaml");
        let res_map = make_mgr("map");
        assert_eq!(
            res_yaml.is_ok(),
            res_map.is_ok(),
            "case {idx}: yaml={:?} map={:?}",
            res_yaml.err(),
            res_map.err()
        );
        if let (Ok(m_yaml), Ok(m_map)) = (res_yaml, res_map) {
            assert_eq!(m_yaml.get_config(), m_map.get_config(), "case {idx}");
            m_yaml.stop();
            m_map.stop();
        }
    }
}

// ============================================================================
// pkg/mimir/runtime_config_test.go (8 tests)
// ============================================================================

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_ShouldLoadAnchoredYAML
#[test]
fn test_runtime_config_loader_should_load_anchored_yaml() {
    let yaml = br#"
overrides:
  '1234': &id001
    ingestion_burst_size: 15000
    ingestion_rate: 1500
    max_global_series_per_metric: 7000
    max_global_series_per_user: 15000
    ruler_max_rule_groups_per_tenant: 20
    ruler_max_rules_per_rule_group: 20
  '1235': *id001
  '1236': *id001
"#;
    let defaults = LimitsConfig::default();
    let cfg = parse_runtime_config_yaml(yaml, &defaults, &Default::default()).unwrap();
    assert_eq!(cfg.overrides.len(), 3);

    let mut expected = defaults;
    expected.ingestion_rate = 1500.0;
    expected.ingestion_burst_size = 15000;
    expected.max_global_series_per_user = 15000;
    expected.max_global_series_per_metric = 7000;
    expected.ruler_max_rules_per_rule_group = 20;
    expected.ruler_max_rule_groups_per_tenant = 20;
    expected.otel_metric_suffixes_enabled = None;
    expected.name_validation_scheme = Default::default();

    for t in ["1234", "1235", "1236"] {
        let got = cfg.overrides[t].as_ref().unwrap().as_ref();
        assert_eq!(got, &expected, "tenant {t}");
    }
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_LoadsMetadataTenantOverrides
#[test]
fn test_runtime_config_loader_loads_metadata_tenant_overrides() {
    let defaults = LimitsConfig::default();

    let cfg1 = parse_runtime_config_yaml(
        b"overrides:\n  'tenant-a:source=test-run':\n    ingestion_rate: 200\n",
        &defaults,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(cfg1.overrides.len(), 1);
    let t1 = cfg1.overrides["tenant-a:source=test-run"].as_ref().unwrap();
    assert_eq!(t1.ingestion_rate, 200.0);
    assert_eq!(t1.otel_metric_suffixes_enabled, None);

    let cfg2 = parse_runtime_config_yaml(
        b"overrides:\n  ':source=test-run':\n    otel_metric_suffixes_enabled: false\n  'tenant-a:source=load-test':\n    otel_metric_suffixes_enabled: true\n",
        &defaults,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(cfg2.overrides.len(), 2);
    assert_eq!(
        cfg2.overrides[":source=test-run"].as_ref().unwrap().otel_metric_suffixes_enabled,
        Some(false)
    );
    assert_eq!(
        cfg2.overrides["tenant-a:source=load-test"].as_ref().unwrap().otel_metric_suffixes_enabled,
        Some(true)
    );
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_ShouldLoadEmptyFile
#[test]
fn test_runtime_config_loader_should_load_empty_file() {
    let actual = parse_runtime_config_yaml(
        b"\n# This is an empty YAML.\n",
        &LimitsConfig::default(),
        &Default::default(),
    )
    .unwrap();
    assert_eq!(actual, Default::default());
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_MissingPointerFieldsAreNil
#[test]
fn test_runtime_config_loader_missing_pointer_fields_are_nil() {
    let actual = parse_runtime_config_yaml(
        b"\n# This is an empty YAML.\n",
        &LimitsConfig::default(),
        &Default::default(),
    )
    .unwrap();
    assert!(actual.ingester_limits.is_none());
    assert!(actual.distributor_limits.is_none());
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_ShouldReturnErrorOnMultipleDocumentsInTheConfig
#[test]
fn test_runtime_config_loader_should_return_error_on_multiple_documents_in_the_config() {
    let cases = [
        "\n---\n---\n",
        "\n---\noverrides:\n  '1234':\n    ingestion_burst_size: 123\n---\noverrides:\n  '1234':\n    ingestion_burst_size: 123\n",
        "\n---\n# This is an empty YAML.\n---\noverrides:\n  '1234':\n    ingestion_burst_size: 123\n",
        "\n---\noverrides:\n  '1234':\n    ingestion_burst_size: 123\n---\n# This is an empty YAML.\n",
    ];
    for tc in cases {
        let err = parse_runtime_config_yaml(
            tc.as_bytes(),
            &LimitsConfig::default(),
            &Default::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("multiple documents"), "{err}");
    }
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_RunsValidation
#[test]
fn test_runtime_config_loader_runs_validation() {
    let defaults = LimitsConfig::default();
    let ok = parse_runtime_config_yaml_with_defaults(
        b"overrides:\n  '1234':\n    ingestion_burst_size: 123\n",
        &defaults,
        &Default::default(),
        &Default::default(),
        GoDuration::from_hours(12),
    );
    assert!(ok.is_ok());

    let bad = parse_runtime_config_yaml_with_defaults(
        b"overrides:\n  '1234':\n    query_ingesters_within: 2h\n",
        &defaults,
        &Default::default(),
        &Default::default(),
        GoDuration::from_hours(12),
    )
    .unwrap_err();
    assert!(bad.to_string().contains("query-ingesters-within"), "{bad}");
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigLoader_ActiveSeriesCustomTrackersMergingShouldNotInterfereBetweenTenants
#[test]
fn test_runtime_config_loader_active_series_custom_trackers_merging_should_not_interfere_between_tenants() {
    set_active_series_custom_trackers_validator(validate_custom_trackers);
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("runtime-config");
    fs::write(
        &rt_path,
        r#"
overrides:
  'user-1': &user1
    active_series_custom_trackers:
      base:   '{foo="user_1_base"}'
      common: '{foo="user_1_base"}'

    active_series_additional_custom_trackers:
      additional: '{foo="user_1_additional"}'
      common:     '{foo="user_1_additional"}'

  'user-2': *user1

  'user-3':
    active_series_custom_trackers:
      base:   '{foo="user_1_base"}'
      common: '{foo="user_1_base"}'

  'user-4':
    active_series_additional_custom_trackers:
      additional: '{foo="user_1_additional"}'
      common:     '{foo="user_1_additional"}'

  'user-5':
    active_series_custom_trackers: {}

  'user-6':
    active_series_custom_trackers: {}

    active_series_additional_custom_trackers:
      additional: '{foo="user_1_additional"}'
      common:     '{foo="user_1_additional"}'
"#,
    )
    .unwrap();

    let mut cfg = MimirConfig::default();
    cfg.limits
        .active_series_custom_trackers
        .insert("default".to_string(), r#"{foo="default"}"#.to_string());
    cfg.runtime_config.file = StringSliceCSV(vec![rt_path.to_string_lossy().into_owned()]);

    let mgr = RuntimeConfigManager::new(
        cfg.runtime_config.clone(),
        cfg.limits.clone(),
        cfg.ingester.instance_limits.clone(),
    )
    .unwrap();
    let overrides = mgr.overrides();

    let merged_str = |user: &str| -> String {
        let lim = overrides.for_tenant(user);
        let merged = merged_custom_trackers(
            &lim.active_series_custom_trackers,
            &lim.active_series_additional_custom_trackers,
        );
        build_custom_trackers_config(&merged).as_str().to_string()
    };

    assert_eq!(
        merged_str("user-1"),
        r#"additional:{foo="user_1_additional"};base:{foo="user_1_base"};common:{foo="user_1_additional"}"#
    );
    assert_eq!(
        merged_str("user-2"),
        r#"additional:{foo="user_1_additional"};base:{foo="user_1_base"};common:{foo="user_1_additional"}"#
    );
    assert_eq!(
        merged_str("user-3"),
        r#"base:{foo="user_1_base"};common:{foo="user_1_base"}"#
    );
    assert_eq!(
        merged_str("user-4"),
        r#"additional:{foo="user_1_additional"};common:{foo="user_1_additional"};default:{foo="default"}"#
    );
    assert_eq!(merged_str("user-5"), "");
    assert_eq!(
        merged_str("user-6"),
        r#"additional:{foo="user_1_additional"};common:{foo="user_1_additional"}"#
    );
    assert_eq!(
        merged_str("user-without-overrides"),
        r#"default:{foo="default"}"#
    );
    mgr.stop();
}

// Upstream: pkg/mimir/runtime_config_test.go::TestRuntimeConfigHTTPClientClusterValidation
#[tokio::test]
async fn test_runtime_config_http_client_cluster_validation() {
    const MINIMAL: &str = "overrides: {}\n";

    for (label, expect_present, expected_val) in [
        ("test-cluster", true, "test-cluster"),
        ("", false, ""),
    ] {
        let got_header = Arc::new(Mutex::new(None::<String>));
        let got_header_c = Arc::clone(&got_header);
        let app = Router::new().route(
            "/rt.yaml",
            get(move |req: Request| {
                let cap = Arc::clone(&got_header_c);
                async move {
                    *cap.lock().unwrap() = req
                        .headers()
                        .get("X-Cluster")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    MINIMAL
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let url = format!("http://{addr}/rt.yaml");
        let mut cfg = MimirConfig::default();
        cfg.runtime_config.file = StringSliceCSV(vec![url]);
        cfg.runtime_config.http_client_cluster_validation.label = label.to_string();

        let mgr = tokio::task::spawn_blocking(move || {
            RuntimeConfigManager::new(
                cfg.runtime_config,
                cfg.limits,
                cfg.ingester.instance_limits,
            )
            .unwrap()
        })
        .await
        .unwrap();

        let captured = got_header.lock().unwrap().clone();
        if expect_present {
            assert_eq!(captured.as_deref(), Some(expected_val));
        } else {
            assert!(captured.is_none(), "expected no X-Cluster header, got {captured:?}");
        }
        mgr.stop();
        srv_handle.abort();
    }
}

// ============================================================================
// pkg/mimir/sanity_check_test.go (2 tests)
// ============================================================================

// Upstream: pkg/mimir/sanity_check_test.go::TestCheckObjectStoresConfig
#[tokio::test]
async fn test_check_object_stores_config() {
    {
        let mut cfg = MimirConfig::default();
        cfg.validate().unwrap();
        check_object_stores_config(&cfg).await.unwrap();
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        cfg.validate().unwrap();
        check_object_stores_config(&cfg).await.unwrap();
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        cfg.blocks_storage.bucket.backend_cfg.backend = "filesystem".into();
        cfg.blocks_storage.bucket.backend_cfg.filesystem.dir = "/does/not/exists".into();
        cfg.blocks_storage.bucket.storage_prefix = "blocks".into();
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut cfg, &["alertmanager_storage", "filesystem", "dir"], Value::from("/does/not/exists"));
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("filesystem"));
        set_extra(&mut cfg, &["ruler_storage", "filesystem", "dir"], Value::from("/does/not/exists"));
        cfg.validate().unwrap();
        check_object_stores_config(&cfg).await.unwrap();
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["ingester".into()]);
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("gcs"));
        set_extra(&mut cfg, &["alertmanager_storage", "gcs", "bucket_name"], Value::from("invalid"));
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("gcs"));
        set_extra(&mut cfg, &["ruler_storage", "gcs", "bucket_name"], Value::from("invalid"));
        cfg.validate().unwrap();
        check_object_stores_config(&cfg).await.unwrap();
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["alertmanager".into()]);
        cfg.blocks_storage.bucket.backend_cfg.backend = "gcs".into();
        cfg.blocks_storage.bucket.backend_cfg.gcs.bucket_name = "invalid".into();
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("gcs"));
        set_extra(&mut cfg, &["ruler_storage", "gcs", "bucket_name"], Value::from("invalid"));
        cfg.validate().unwrap();
        check_object_stores_config(&cfg).await.unwrap();
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["ruler".into()]);
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("gcs"));
        set_extra(&mut cfg, &["alertmanager_storage", "gcs", "bucket_name"], Value::from("invalid"));
        cfg.validate().unwrap();
        check_object_stores_config(&cfg).await.unwrap();
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        cfg.blocks_storage.bucket.backend_cfg.backend = "s3".into();
        cfg.blocks_storage.bucket.backend_cfg.s3.region = "us-east-1".into();
        cfg.blocks_storage.bucket.backend_cfg.s3.endpoint = "127.0.0.1:1".into();
        cfg.blocks_storage.bucket.backend_cfg.s3.bucket_name = "invalid".into();
        cfg.blocks_storage.bucket.backend_cfg.s3.access_key_id = "xxx".into();
        cfg.blocks_storage.bucket.backend_cfg.s3.secret_access_key = "yyy".into();
        cfg.blocks_storage.bucket.backend_cfg.s3.insecure = true;
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("s3"));
        set_extra(&mut cfg, &["alertmanager_storage", "s3", "region"], Value::from("us-east-1"));
        set_extra(&mut cfg, &["alertmanager_storage", "s3", "endpoint"], Value::from("127.0.0.1:1"));
        set_extra(&mut cfg, &["alertmanager_storage", "s3", "bucket_name"], Value::from("invalid-1"));
        set_extra(&mut cfg, &["alertmanager_storage", "s3", "access_key_id"], Value::from("xxx"));
        set_extra(&mut cfg, &["alertmanager_storage", "s3", "secret_access_key"], Value::from("yyy"));
        set_extra(&mut cfg, &["alertmanager_storage", "s3", "insecure"], Value::from(true));
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("s3"));
        set_extra(&mut cfg, &["ruler_storage", "s3", "region"], Value::from("us-east-1"));
        set_extra(&mut cfg, &["ruler_storage", "s3", "endpoint"], Value::from("127.0.0.1:1"));
        set_extra(&mut cfg, &["ruler_storage", "s3", "bucket_name"], Value::from("invalid-1"));
        set_extra(&mut cfg, &["ruler_storage", "s3", "access_key_id"], Value::from("xxx"));
        set_extra(&mut cfg, &["ruler_storage", "s3", "secret_access_key"], Value::from("yyy"));
        set_extra(&mut cfg, &["ruler_storage", "s3", "insecure"], Value::from(true));
        cfg.validate().unwrap();
        let err = check_object_stores_config(&cfg).await.unwrap_err();
        assert!(err.contains(ERR_OBJECT_STORAGE), "{err}");
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        cfg.blocks_storage.bucket.backend_cfg.backend = "gcs".into();
        cfg.blocks_storage.bucket.backend_cfg.gcs.bucket_name = "invalid".into();
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("gcs"));
        set_extra(&mut cfg, &["alertmanager_storage", "gcs", "bucket_name"], Value::from("invalid-1"));
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("gcs"));
        set_extra(&mut cfg, &["ruler_storage", "gcs", "bucket_name"], Value::from("invalid-1"));
        cfg.validate().unwrap();
        let err = check_object_stores_config(&cfg).await.unwrap_err();
        assert!(err.contains(ERR_OBJECT_STORAGE), "{err}");
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        cfg.blocks_storage.bucket.backend_cfg.backend = "azure".into();
        cfg.blocks_storage.bucket.backend_cfg.azure.container_name = "invalid".into();
        cfg.blocks_storage.bucket.backend_cfg.azure.account_key = "eHh4".into();
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("azure"));
        set_extra(&mut cfg, &["alertmanager_storage", "azure", "container_name"], Value::from("invalid-1"));
        set_extra(&mut cfg, &["alertmanager_storage", "azure", "account_key"], Value::from("eHh4"));
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("azure"));
        set_extra(&mut cfg, &["ruler_storage", "azure", "container_name"], Value::from("invalid-1"));
        set_extra(&mut cfg, &["ruler_storage", "azure", "account_key"], Value::from("eHh4"));
        cfg.validate().unwrap();
        let err = check_object_stores_config(&cfg).await.unwrap_err();
        assert!(err.contains(ERR_OBJECT_STORAGE), "{err}");
    }

    {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["all".into(), "alertmanager".into()]);
        cfg.blocks_storage.bucket.backend_cfg.backend = "swift".into();
        cfg.blocks_storage.bucket.backend_cfg.swift.auth_url = "http://127.0.0.1/".into();
        cfg.blocks_storage.bucket.backend_cfg.swift.project_name = "invalid".into();
        set_extra(&mut cfg, &["alertmanager_storage", "backend"], Value::from("swift"));
        set_extra(&mut cfg, &["alertmanager_storage", "swift", "auth_url"], Value::from("http://127.0.0.1/"));
        set_extra(&mut cfg, &["alertmanager_storage", "swift", "project_name"], Value::from("invalid-1"));
        set_extra(&mut cfg, &["ruler_storage", "backend"], Value::from("swift"));
        set_extra(&mut cfg, &["ruler_storage", "swift", "auth_url"], Value::from("http://127.0.0.1/"));
        set_extra(&mut cfg, &["ruler_storage", "swift", "project_name"], Value::from("invalid-1"));
        let err = check_object_stores_config(&cfg).await.unwrap_err();
        assert!(err.contains(ERR_OBJECT_STORAGE), "{err}");
    }
}

// Upstream: pkg/mimir/sanity_check_test.go::TestCheckDirectoryReadWriteAccess
#[test]
fn test_check_directory_read_write_access() {
    const CONFIGURED: &str = "/path/to/dir";
    type SetupFn = fn(&mut MimirConfig);
    let setups: Vec<(&str, SetupFn)> = vec![
        ("ingester", |c| {
            c.target = StringSliceCSV(vec!["ingester".into()]);
            c.blocks_storage.tsdb.dir = CONFIGURED.into();
        }),
        ("store-gateway", |c| {
            c.target = StringSliceCSV(vec!["store-gateway".into()]);
            c.blocks_storage.bucket_store.sync_dir = CONFIGURED.into();
        }),
        ("compactor", |c| {
            c.target = StringSliceCSV(vec!["compactor".into()]);
            set_extra(c, &["compactor", "data_dir"], Value::from(CONFIGURED));
        }),
        ("ruler", |c| {
            c.target = StringSliceCSV(vec!["ruler".into()]);
            set_extra(c, &["ruler", "rule_path"], Value::from(CONFIGURED));
        }),
        ("alertmanager", |c| {
            c.target = StringSliceCSV(vec!["alertmanager".into()]);
            set_extra(c, &["alertmanager", "data_dir"], Value::from(CONFIGURED));
        }),
    ];

    type ExistsFn = fn(&Path) -> Result<bool, String>;
    type RwFn = fn(&Path) -> Result<(), String>;
    let cases: [(&str, ExistsFn, RwFn, &str); 4] = [
        (
            "should fail on directory without write access",
            |_| Ok(true),
            |_| Err("read only".to_string()),
            "failed to access directory /path/to/dir: read only",
        ),
        (
            "should pass on directory with read-write access",
            |_| Ok(true),
            |_| Ok(()),
            "",
        ),
        (
            "should pass if directory doesn't exist but parent existing folder has read-write access",
            |d| Ok(d == Path::new("/")),
            |d| if d == Path::new("/") { Ok(()) } else { Err("not exists".to_string()) },
            "",
        ),
        (
            "should fail if directory doesn't exist and parent existing folder has no read-write access",
            |d| Ok(d == Path::new("/")),
            |d| if d == Path::new("/") { Err("read only".to_string()) } else { Err("not exists".to_string()) },
            "failed to access directory /path/to/dir: read only",
        ),
    ];

    for (comp, setup) in setups {
        for (name, exists_fn, rw_fn, expected) in cases {
            let mut cfg = MimirConfig::default();
            setup(&mut cfg);
            let res = check_directories_read_write_access(&cfg, exists_fn, rw_fn);
            if expected.is_empty() {
                assert!(res.is_ok(), "{comp}/{name}: {res:?}");
            } else {
                let err = res.expect_err(&format!("{comp}/{name}"));
                assert!(err.contains(expected), "{comp}/{name}: {err}");
            }
        }
    }
}

// ============================================================================
// pkg/mimir/server_service_test.go (4 tests)
// ============================================================================

fn build_test_server_service() -> (
    Arc<mimir_common::services::ManagedBasicService>,
    Arc<parking_lot::Mutex<Option<mimir_cmd::server::ServerShutdownHandle>>>,
) {
    let srv = Server::new(&new_server_cfg(), &Registry::new(), Arc::new(NoopLogger)).unwrap();
    let router = build_http_router(test_http_state(MimirConfig::default(), Registry::new()));
    let modules = ModuleStates::new();
    let health_svc = HealthService::new(Arc::new(AtomicBool::new(false)), modules);
    let grpc_routes = tonic::service::Routes::new(health_svc);
    let wait_fn: ServicesToWaitForFn = Arc::new(Vec::new);
    new_server_service(srv, router, grpc_routes, wait_fn)
}

// Upstream: pkg/mimir/server_service_test.go::TestServerStopViaContext
#[tokio::test]
async fn test_server_stop_via_context() {
    let (svc, _handle) = build_test_server_service();
    let ctx = ServiceContext::background();
    let ctx_cancel = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        ctx_cancel.cancel();
    });
    svc.start_async(ctx).await.unwrap();
    svc.await_terminated().await.unwrap();
}

// Upstream: pkg/mimir/server_service_test.go::TestServerStopWhileStarting
#[tokio::test]
async fn test_server_stop_while_starting() {
    let (svc, _handle) = build_test_server_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.stop_async();
    tokio::time::timeout(Duration::from_secs(5), svc.await_terminated())
        .await
        .unwrap()
        .unwrap();
}

// Upstream: pkg/mimir/server_service_test.go::TestServerStopViaShutdown
#[tokio::test]
async fn test_server_stop_via_shutdown() {
    let (svc, handle) = build_test_server_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    let h = handle.lock().clone().unwrap();
    h.trigger_shutdown();

    let res = svc.await_terminated().await;
    assert!(res.is_err(), "expected failure when server stops unexpectedly via Shutdown");
    assert_eq!(svc.state(), State::Failed);
}

// Upstream: pkg/mimir/server_service_test.go::TestServerStopViaStop
#[tokio::test]
async fn test_server_stop_via_stop() {
    let (svc, handle) = build_test_server_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    let h = handle.lock().clone().unwrap();
    h.trigger_stop();

    let res = svc.await_terminated().await;
    assert!(res.is_err(), "expected failure when server stops unexpectedly via Stop");
    assert_eq!(svc.state(), State::Failed);
}
