//! HTTP admin & lifecycle router matching `pkg/api/api.go`, `pkg/mimir/mimir.go`,
//! `pkg/mimir/status.go`, `pkg/mimir/runtime_config.go`, and `pkg/util/version/info_handler.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::{Query, State as AxumState};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_common::services::{Service, State};
use mimir_common::tenant::extract_tenant_id_from_http_headers;
use mimir_config::{LimitsConfig, MimirConfig, Overrides, RuntimeConfigManager};
use mimir_ingester::{ingester_router, Ingester};
use mimir_ring::kv::memberlist::MemberlistKv;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};

use crate::cli::{diff_yaml_values, effective_config_yaml};
use crate::metrics::{branch_str, go_version_str, revision_str, version_str};

#[derive(Clone)]
pub struct AppHttpState {
    pub config: Arc<MimirConfig>,
    pub default_config: Arc<MimirConfig>,
    pub registry: Registry,
    pub shutdown_requested: Arc<AtomicBool>,
    pub ingester: Option<Arc<Ingester>>,
    pub runtime_config: Option<Arc<RuntimeConfigManager>>,
    pub overrides: Overrides,
    pub memberlist_kv: Option<Arc<MemberlistKv>>,
    pub active_service_names: Vec<String>,
    /// Module services other than `ingester-service` whose state `/ready` reports (Go
    /// `Mimir.ServiceMap` entries backed by a Rust service), keyed by upstream module name.
    pub module_services: Vec<(String, Arc<dyn Service>)>,
}

/// Builds the full Axum HTTP router (with `server.http_path_prefix` honored).
pub fn build_http_router(state: AppHttpState) -> Router {
    let register_instrumentation = state.config.server.register_instrumentation;
    let has_memberlist = state.memberlist_kv.is_some();
    let ingester_opt = state.ingester.clone();
    let path_prefix = state.config.server.http_path_prefix.trim_end_matches('/').to_string();

    // `/ready` and `/metrics` are registered without a method restriction (Go
    // `Server.HTTP.Path("/ready")`, dskit `router.Handle("/metrics", ...)`).
    let mut router = Router::new()
        .route("/ready", any(ready_handler))
        .route("/services", get(services_handler))
        .route("/config", get(config_handler))
        .route("/runtime_config", get(runtime_config_handler))
        .route("/api/v1/user_limits", get(user_limits_handler))
        .route("/api/v1/status/buildinfo", get(buildinfo_handler))
        .route("/api/v1/status/config", get(status_config_handler))
        .route("/api/v1/status/flags", get(status_flags_handler))
        // dskit registerInstrumentation: `/debug/pprof/cmdline` is NotFound, everything else under
        // `/debug/pprof` goes to Go's net/http/pprof handlers (any method).
        .route("/debug/pprof/cmdline", any(not_found_handler))
        .route("/debug/pprof/profile", any(crate::pprof::profile_handler))
        .route("/debug/pprof/symbol", any(crate::pprof::symbol_handler))
        .route("/debug/pprof", any(crate::pprof::index_redirect_handler))
        .route("/debug/pprof/", any(crate::pprof::index_handler))
        .route("/debug/pprof/*name", any(crate::pprof::unknown_profile_handler));

    let has_prefix = !path_prefix.is_empty() && path_prefix != "/";
    if !has_prefix {
        router = router.route("/", get(index_handler));
    }

    if register_instrumentation {
        let cache = Arc::new(MetricsCache::new(
            state
                .config
                .server
                .metrics_cache_duration
                .as_std()
                .unwrap_or_default(),
        ));
        router = router.route(
            "/metrics",
            any(move |st: AxumState<AppHttpState>, headers: HeaderMap| {
                metrics_handler(st, headers, Arc::clone(&cache))
            }),
        );
    }
    if has_memberlist {
        router = router.route("/memberlist", get(memberlist_handler));
    }

    let state_for_root = state.clone();
    let mut app = router.with_state(state);

    if let Some(ing) = ingester_opt {
        app = app.merge(ingester_router(ing));
    }

    // pkg/api/api.go newRoute: `gziphandler.GzipHandler` on every gzip-enabled route.
    let gzip = crate::server::gzip::GzipConfig {
        level: state_for_root.config.api_response_compression_level().unwrap_or(-1),
    };
    app = app
        .route_layer(axum::middleware::from_fn_with_state(gzip, crate::server::gzip::gzip_handler))
        // gorilla/mux answers a known path with an unsupported method with an empty 405 and
        // reports it as an unmatched route.
        .method_not_allowed_fallback(crate::server::http_middleware::method_not_allowed);

    if has_prefix {
        let normalized = if path_prefix.starts_with('/') {
            path_prefix
        } else {
            format!("/{path_prefix}")
        };
        Router::new()
            .route(&normalized, get(index_handler).with_state(state_for_root.clone()))
            .route(
                &format!("{normalized}/"),
                get(index_handler).with_state(state_for_root),
            )
            .nest(&normalized, app)
            .fallback(not_found_handler)
    } else {
        // gorilla/mux NotFoundHandler default: http.NotFound.
        app.fallback(not_found_handler)
    }
}

/// `http.NotFoundHandler()` (`http.Error(w, "404 page not found", 404)`).
pub async fn not_found_handler() -> Response {
    (
        StatusCode::NOT_FOUND,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        "404 page not found\n",
    )
        .into_response()
}

async fn ready_handler(AxumState(st): AxumState<AppHttpState>) -> Response {
    if st.shutdown_requested.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Application is stopping\n").into_response();
    }

    // Go: `if !sm.IsHealthy()` lists every module service that is not Running, one
    // "<module>: <state>" per line (pkg/mimir/mimir.go readyHandler). Every module service is
    // started by the service manager at once, so a not yet started one is reported as Starting.
    let module_state = |s: State| if s == State::New { State::Starting } else { s };
    let mut not_running: Vec<String> = Vec::new();
    for (name, svc) in &st.module_services {
        let s = module_state(svc.state());
        if s != State::Running {
            not_running.push(format!("{name}: {s}"));
        }
    }
    if let Some(ref ing) = st.ingester {
        let s = module_state(ing.state());
        if s != State::Running {
            not_running.push(format!("ingester-service: {s}"));
        }
    }
    if !not_running.is_empty() {
        // http.Error appends a trailing newline.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Some services are not Running:\n{}\n", not_running.join("\n")),
        )
            .into_response();
    }

    if let Some(ref ing) = st.ingester {
        if let Err(err) = ing.check_ready().await {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Ingester not ready: {err}\n"),
            )
                .into_response();
        }
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain")],
        "ready",
    )
        .into_response()
}

/// dskit `registerInstrumentation`: `promhttp.HandlerFor(gatherer, HandlerOpts{EnableOpenMetrics:
/// true})` (wrapped by `metrics.CachedHandlerFor` when `-server.metrics-cache-duration` > 0):
/// content negotiation (OpenMetrics 1.0.0/0.0.1, delimited protobuf, text 0.0.4), `Vary` headers
/// and gzip response compression.
async fn metrics_handler(
    AxumState(st): AxumState<AppHttpState>,
    headers: HeaderMap,
    cache: Arc<MetricsCache>,
) -> Response {
    let header_str = |name: header::HeaderName| headers.get(name).and_then(|v| v.to_str().ok());
    let format = crate::expfmt::negotiate(header_str(header::ACCEPT));
    let families = cache.gather(&st.registry);
    let body = match crate::expfmt::encode(&families, &format) {
        Ok(b) => b,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                format!("An error has occurred while serving metrics:\n\n{err}\n"),
            )
                .into_response();
        }
    };
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::VARY, "Accept")
        .header(header::VARY, "Accept-Encoding")
        .header(header::CONTENT_TYPE, format.content_type);
    let body = match crate::expfmt::negotiate_content_encoding(header_str(header::ACCEPT_ENCODING)) {
        Some("gzip") => {
            builder = builder.header(header::CONTENT_ENCODING, "gzip");
            gzip_bytes(&body)
        }
        // Go fails to build a writer for an unacceptable encoding and falls back to identity.
        _ => body,
    };
    builder
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub(crate) fn gzip_bytes(data: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    let mut enc = flate2::write::GzEncoder::new(
        Vec::with_capacity(data.len() / 4 + 64),
        flate2::Compression::default(),
    );
    let _ = enc.write_all(data);
    enc.finish().unwrap_or_default()
}

/// dskit `metrics.CachedHandlerFor`: reuses the gathered metric families for
/// `-server.metrics-cache-duration` (0 disables caching).
pub struct MetricsCache {
    duration: std::time::Duration,
    cached: parking_lot::Mutex<Option<(std::time::Instant, Arc<Vec<prometheus::proto::MetricFamily>>)>>,
}

impl MetricsCache {
    pub fn new(duration: std::time::Duration) -> Self {
        Self {
            duration,
            cached: parking_lot::Mutex::new(None),
        }
    }

    fn gather(&self, registry: &Registry) -> Arc<Vec<prometheus::proto::MetricFamily>> {
        if self.duration.is_zero() {
            return Arc::new(registry.gather());
        }
        let mut cached = self.cached.lock();
        if let Some((at, ref fams)) = *cached {
            if at.elapsed() < self.duration {
                return Arc::clone(fams);
            }
        }
        let fams = Arc::new(registry.gather());
        *cached = Some((std::time::Instant::now(), Arc::clone(&fams)));
        fams
    }
}

#[derive(Serialize)]
struct RenderService {
    name: String,
    status: String,
}

#[derive(Serialize)]
struct StatusPageContents {
    now: String,
    services: Vec<RenderService>,
}

async fn services_handler(AxumState(st): AxumState<AppHttpState>, headers: HeaderMap) -> Response {
    let mut svcs = Vec::new();
    if let Some(ref ing) = st.ingester {
        svcs.push(RenderService {
            name: "ingester".to_string(),
            status: ing.state().to_string(),
        });
        svcs.push(RenderService {
            name: "ingester-service".to_string(),
            status: ing.state().to_string(),
        });
        if ing.instance_ring.is_some() {
            svcs.push(RenderService {
                name: "ingester-ring".to_string(),
                status: if ing.state() == State::Running {
                    "Running".to_string()
                } else {
                    ing.state().to_string()
                },
            });
        }
        if ing.partition_ring_watcher.is_some() {
            svcs.push(RenderService {
                name: "ingester-partitions-ring".to_string(),
                status: if ing.state() == State::Running {
                    "Running".to_string()
                } else {
                    ing.state().to_string()
                },
            });
        }
    }
    if st.memberlist_kv.is_some() {
        svcs.push(RenderService {
            name: "memberlist-kv".to_string(),
            status: if st.shutdown_requested.load(Ordering::Acquire) {
                "Stopping".to_string()
            } else {
                "Running".to_string()
            },
        });
    }
    if st.runtime_config.is_some() {
        svcs.push(RenderService {
            name: "runtime-config".to_string(),
            status: "Running".to_string(),
        });
    }
    for name in &st.active_service_names {
        if !svcs.iter().any(|s| s.name == *name) {
            svcs.push(RenderService {
                name: name.clone(),
                status: "Running".to_string(),
            });
        }
    }
    svcs.sort_by(|a, b| a.name.cmp(&b.name));

    let now = format_rfc3339_nano_utc(SystemTime::now());
    let accepts_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("application/json"));
    if accepts_json {
        return Json(StatusPageContents { now, services: svcs }).into_response();
    }

    let mut html = format!(
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Services' status</title></head>\
         <body><h1>Services' status</h1><p>Current time: {now}</p>\
         <table border=\"1\"><thead><tr><th>Service</th><th>Status</th></tr></thead><tbody>"
    );
    for s in svcs {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td></tr>",
            html_escape(&s.name),
            html_escape(&s.status)
        ));
    }
    html.push_str("</tbody></table></body></html>");
    Html(html).into_response()
}

#[derive(Deserialize)]
struct ConfigModeQuery {
    mode: Option<String>,
}

async fn config_handler(
    AxumState(st): AxumState<AppHttpState>,
    Query(q): Query<ConfigModeQuery>,
) -> Response {
    let val = match q.mode.as_deref() {
        Some("diff") => {
            let def = effective_config_yaml(&st.default_config);
            let act = effective_config_yaml(&st.config);
            diff_yaml_values(&def, &act).unwrap_or_else(|| Value::Mapping(Mapping::new()))
        }
        Some("defaults") => effective_config_yaml(&st.default_config),
        _ => effective_config_yaml(&st.config),
    };

    match serde_yaml::to_string(&val) {
        Ok(yaml) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            yaml,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn runtime_config_handler(
    AxumState(st): AxumState<AppHttpState>,
    Query(q): Query<ConfigModeQuery>,
) -> Response {
    let Some(ref mgr) = st.runtime_config else {
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain")],
            "runtime config file doesn't exist",
        )
            .into_response();
    };

    let cfg = mgr.get_config();
    let actual_val = runtime_config_to_yaml(&cfg);

    let output_val = match q.mode.as_deref() {
        Some("diff") => {
            let mut default_overrides = Mapping::new();
            let default_limit_val =
                serde_yaml::to_value(&st.config.limits).unwrap_or(Value::Mapping(Mapping::new()));
            for tenant in cfg.overrides.keys() {
                default_overrides.insert(Value::String(tenant.clone()), default_limit_val.clone());
            }
            let mut default_root = Mapping::new();
            default_root.insert(
                Value::String("overrides".to_string()),
                Value::Mapping(default_overrides),
            );
            default_root.insert(
                Value::String("multi_kv_config".to_string()),
                serde_yaml::to_value(mimir_config::MultiRuntimeConfig::default())
                    .unwrap_or(Value::Mapping(Mapping::new())),
            );
            diff_yaml_values(&Value::Mapping(default_root), &actual_val)
                .unwrap_or_else(|| Value::Mapping(Mapping::new()))
        }
        _ => actual_val,
    };

    match serde_yaml::to_string(&output_val) {
        Ok(yaml) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            yaml,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

fn runtime_config_to_yaml(cfg: &mimir_config::RuntimeConfigValues) -> Value {
    let mut root = Mapping::new();
    let mut overrides_map = Mapping::new();
    for (k, v) in &cfg.overrides {
        if let Ok(val) = serde_yaml::to_value(v.as_ref()) {
            overrides_map.insert(Value::String(k.clone()), val);
        }
    }
    root.insert(
        Value::String("overrides".to_string()),
        Value::Mapping(overrides_map),
    );
    if let Ok(multi_val) = serde_yaml::to_value(&cfg.multi_kv_config) {
        root.insert(Value::String("multi_kv_config".to_string()), multi_val);
    }
    if let Some(ref ing) = cfg.ingester_limits {
        if let Ok(v) = serde_yaml::to_value(ing) {
            root.insert(Value::String("ingester_limits".to_string()), v);
        }
    }
    if let Some(ref dist) = cfg.distributor_limits {
        if let Ok(v) = serde_yaml::to_value(dist) {
            root.insert(Value::String("distributor_limits".to_string()), v);
        }
    }
    Value::Mapping(root)
}

#[derive(Serialize)]
struct UserLimitsResponse {
    compactor_blocks_retention_period_seconds: i64,
    ingestion_rate: f64,
    ingestion_burst_size: i64,
    ingestion_burst_factor: f64,
    max_global_series_per_user: i64,
    max_global_series_per_metric: i64,
    max_global_exemplars_per_user: i64,
    max_fetched_chunks_per_query: i64,
    max_fetched_series_per_query: i64,
    max_fetched_chunk_bytes_per_query: i64,
    ruler_max_rules_per_rule_group: i64,
    ruler_max_rule_groups_per_tenant: i64,
    alertmanager_notification_rate_limit: f64,
    alertmanager_max_dispatcher_aggregation_groups: i64,
    alertmanager_max_templates_count: i64,
    alertmanager_max_alerts_count: i64,
}

impl From<&LimitsConfig> for UserLimitsResponse {
    fn from(l: &LimitsConfig) -> Self {
        Self {
            compactor_blocks_retention_period_seconds: l.compactor_blocks_retention_period.0
                / 1_000_000_000,
            ingestion_rate: l.ingestion_rate,
            ingestion_burst_size: l.ingestion_burst_size,
            ingestion_burst_factor: l.ingestion_burst_factor,
            max_global_series_per_user: l.max_global_series_per_user,
            max_global_series_per_metric: l.max_global_series_per_metric,
            max_global_exemplars_per_user: l.max_global_exemplars_per_user,
            max_fetched_chunks_per_query: l.max_fetched_chunks_per_query,
            max_fetched_series_per_query: l.max_fetched_series_per_query,
            max_fetched_chunk_bytes_per_query: l.max_fetched_chunk_bytes_per_query,
            ruler_max_rules_per_rule_group: l.ruler_max_rules_per_rule_group,
            ruler_max_rule_groups_per_tenant: l.ruler_max_rule_groups_per_tenant,
            alertmanager_notification_rate_limit: l.alertmanager_notification_rate_limit,
            alertmanager_max_dispatcher_aggregation_groups: l
                .alertmanager_max_dispatcher_aggregation_groups,
            alertmanager_max_templates_count: l.alertmanager_max_templates_count,
            alertmanager_max_alerts_count: l.alertmanager_max_alerts_count,
        }
    }
}

async fn user_limits_handler(
    AxumState(st): AxumState<AppHttpState>,
    headers: HeaderMap,
) -> Response {
    let user_id = if st.config.multitenancy_enabled {
        match extract_tenant_id_from_http_headers(&headers) {
            Ok(id) => id,
            Err(err) => return (StatusCode::UNAUTHORIZED, err.to_string()).into_response(),
        }
    } else {
        st.config.no_auth_tenant.clone()
    };

    let limits = st.overrides.for_tenant(&user_id);
    Json(UserLimitsResponse::from(limits.as_ref())).into_response()
}

#[derive(Serialize)]
struct BuildInfoResponse {
    status: &'static str,
    data: BuildInfoData,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildInfoData {
    application: &'static str,
    version: &'static str,
    revision: &'static str,
    branch: &'static str,
    go_version: String,
    features: BuildInfoFeatures,
}

/// Go `version.BuildInfoFeatures` (field order is the JSON order).
#[derive(Serialize)]
struct BuildInfoFeatures {
    ruler_config_api: String,
    alertmanager_config_api: String,
    query_sharding: String,
    federated_rules: String,
    ingest_storage: String,
}

/// A boolean option of a block mimir-rs keeps as an extra section: the CLI flag wins over the
/// YAML value, which wins over the flag default (Go flag registration + YAML + flag parsing).
fn extra_section_bool(cfg: &MimirConfig, path: &[&str], flag: &str, default: bool) -> bool {
    if let Some(v) = cfg.extra_cli_flag_value(flag) {
        // strconv.ParseBool (the value was validated when the flag was parsed).
        return matches!(v, "1" | "t" | "T" | "TRUE" | "true" | "True");
    }
    let Some((section, rest)) = path.split_first() else {
        return default;
    };
    let mut v = cfg.extra_sections.get(*section);
    for key in rest {
        v = v.and_then(|v| v.get(*key));
    }
    v.and_then(Value::as_bool).unwrap_or(default)
}

/// pkg/mimir/modules.go `initAPI` (`version.BuildInfoHandler("Grafana Mimir", ...)`).
async fn buildinfo_handler(AxumState(st): AxumState<AppHttpState>) -> Response {
    let cfg = &st.config;
    let features = BuildInfoFeatures {
        ruler_config_api: extra_section_bool(cfg, &["ruler", "enable_api"], "ruler.enable-api", true).to_string(),
        alertmanager_config_api: extra_section_bool(cfg, &["alertmanager", "enable_api"], "alertmanager.enable-api", true)
            .to_string(),
        query_sharding: extra_section_bool(
            cfg,
            &["frontend", "parallelize_shardable_queries"],
            "query-frontend.parallelize-shardable-queries",
            true,
        )
        .to_string(),
        federated_rules: extra_section_bool(
            cfg,
            &["ruler", "tenant_federation", "enabled"],
            "ruler.tenant-federation.enabled",
            false,
        )
        .to_string(),
        ingest_storage: cfg.ingest_storage.enabled.to_string(),
    };
    Json(BuildInfoResponse {
        status: "success",
        data: BuildInfoData {
            application: "Grafana Mimir",
            version: version_str(),
            revision: revision_str(),
            branch: branch_str(),
            go_version: go_version_str(),
            features,
        },
    })
    .into_response()
}

#[derive(Serialize)]
struct StatusDataResponse {
    status: &'static str,
    data: std::collections::BTreeMap<String, String>,
}

async fn status_config_handler() -> Response {
    Json(StatusDataResponse {
        status: "success",
        data: std::collections::BTreeMap::new(),
    })
    .into_response()
}

async fn status_flags_handler() -> Response {
    Json(StatusDataResponse {
        status: "success",
        data: std::collections::BTreeMap::new(),
    })
    .into_response()
}

#[derive(Serialize)]
struct MemberlistMemberJson {
    name: String,
    addr: String,
    state: String,
    incarnation: u32,
}

#[derive(Serialize)]
struct MemberlistStatusJson {
    node_name: String,
    advertise_addr: String,
    keys_count: usize,
    members: Vec<MemberlistMemberJson>,
}

async fn memberlist_handler(
    AxumState(st): AxumState<AppHttpState>,
    headers: HeaderMap,
) -> Response {
    let Some(ref ml) = st.memberlist_kv else {
        return (StatusCode::NOT_FOUND, "memberlist KV is not enabled").into_response();
    };

    let members_raw = ml.members();
    let keys_count = ml.list_keys("").len();
    let mut members: Vec<MemberlistMemberJson> = members_raw
        .into_iter()
        .map(|m| MemberlistMemberJson {
            name: m.name.clone(),
            addr: m.address(),
            state: format!("{:?}", m.state),
            incarnation: m.incarnation,
        })
        .collect();
    members.sort_by(|a, b| a.name.cmp(&b.name));

    let accepts_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("application/json"));

    if accepts_json {
        return Json(MemberlistStatusJson {
            node_name: ml.node_name().to_string(),
            advertise_addr: ml.advertise_addr(),
            keys_count,
            members,
        })
        .into_response();
    }

    let mut html = format!(
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Memberlist Status</title></head>\
         <body><h1>Memberlist Status</h1>\
         <p>Node: {} ({})</p><p>KV keys: {}</p>\
         <table border=\"1\"><thead><tr><th>Name</th><th>Address</th><th>State</th><th>Incarnation</th></tr></thead><tbody>",
        html_escape(ml.node_name()),
        html_escape(&ml.advertise_addr()),
        keys_count
    );
    for m in members {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&m.name),
            html_escape(&m.addr),
            html_escape(&m.state),
            m.incarnation
        ));
    }
    html.push_str("</tbody></table></body></html>");
    Html(html).into_response()
}

async fn index_handler(AxumState(st): AxumState<AppHttpState>) -> Response {
    let base = st.config.server.http_path_prefix.trim_end_matches('/');
    let link = |path: &str| -> String {
        if base.is_empty() {
            path.to_string()
        } else {
            format!("{base}{path}")
        }
    };

    let mut html = String::from(
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Grafana Mimir</title></head>\
         <body><h1>Grafana Mimir Admin</h1>\
         <h2>Overview</h2><ul>",
    );
    html.push_str(&format!(
        "<li><a href=\"{}\">Services' status</a></li>",
        link("/services")
    ));
    html.push_str(&format!(
        "<li><a href=\"{}\">Metrics</a></li>",
        link("/metrics")
    ));
    html.push_str(&format!(
        "<li><a href=\"{}\">Ready</a></li></ul>",
        link("/ready")
    ));
    html.push_str("<h2>Current config</h2><ul>");
    html.push_str(&format!(
        "<li><a href=\"{}\">Including the default values</a></li>",
        link("/config")
    ));
    html.push_str(&format!(
        "<li><a href=\"{}\">Only values that differ from the defaults</a></li></ul>",
        link("/config?mode=diff")
    ));

    if st.runtime_config.is_some() {
        html.push_str("<h2>Current runtime config</h2><ul>");
        html.push_str(&format!(
            "<li><a href=\"{}\">Entire runtime config (including overrides)</a></li>",
            link("/runtime_config")
        ));
        html.push_str(&format!(
            "<li><a href=\"{}\">Only values that differ from the defaults</a></li></ul>",
            link("/runtime_config?mode=diff")
        ));
    }

    if let Some(ref ing) = st.ingester {
        html.push_str("<h2>Ingester</h2><ul>");
        html.push_str(&format!(
            "<li><a href=\"{}\">Ring status</a></li>",
            link("/ingester/ring")
        ));
        if ing.partition_ring_watcher.is_some() {
            html.push_str(&format!(
                "<li><a href=\"{}\">Partition ring status</a></li>",
                link("/ingester/partition-ring")
            ));
        }
        html.push_str(&format!(
            "<li><a href=\"{}\">Ingester Tenants</a></li></ul>",
            link("/ingester/tenants")
        ));
    }

    if st.memberlist_kv.is_some() {
        html.push_str("<h2>Memberlist</h2><ul>");
        html.push_str(&format!(
            "<li><a href=\"{}\">Status</a></li></ul>",
            link("/memberlist")
        ));
    }

    if st.ingester.is_some() {
        html.push_str("<h2>Dangerous</h2><ul>");
        html.push_str(&format!(
            "<li><a href=\"{}\">Trigger a flush of data from ingester to storage</a></li>",
            link("/ingester/flush")
        ));
        html.push_str(&format!(
            "<li><a href=\"{}\">Trigger ingester shutdown</a></li></ul>",
            link("/ingester/shutdown")
        ));
    }

    html.push_str("</body></html>");
    Html(html).into_response()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
