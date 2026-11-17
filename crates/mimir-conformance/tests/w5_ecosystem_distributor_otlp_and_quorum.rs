//! Gate 2 Ecosystem Test Suite 2: Upstream Go Distributor (OTLP + Classic RF=3 Quorum).
//!
//! - Test A (`w5_01_go_distributor_otlp_metrics_differential`):
//!   Boots Go `distributor` + Go `querier` backed by Go `ingester` vs Rust `ingester` over
//!   memberlist. Sends rich OTLP `POST /otlp/v1/metrics` payloads (gauges, monotonic cumulative
//!   sums with `startTimeUnixNano`, OTLP exponential histograms, OTLP explicit histograms converted
//!   to NHCB `schema=-53`, exemplars, resource & scope attributes, UTF-8 names/values) and diffs
//!   PromQL `query`, `query_range`, `series`, `labels`, `query_exemplars`, and `metadata`
//!   between the Go-backed and Rust-backed pipelines.
//!
//! - Test B (`w5_02_go_distributor_classic_grpc_push_rf3_quorum_and_errors`):
//!   Boots Go `distributor` (`ingest_storage.enabled: false`, `ingester.push_grpc_method_enabled:
//!   true`, `replication_factor: 3`, `zone_awareness_enabled: true`, `s2`/`snappy` gRPC
//!   compression) against a 3-node all-Go reference ring AND a 3-node mixed Go + Rust ring across
//!   `zone-a`, `zone-b`, `zone-c`. Verifies identically on both rings:
//!   1. Normal 3/3 write + quorum read;
//!   2. Stop 1 ingester (`SIGKILL`, tokens retained) -> 2/3 quorum write & read succeed with 200;
//!   3. Soft `400` out-of-bounds/too-old sample -> Go distributor returns HTTP `400` (not `500`);
//!   4. Per-tenant `max_global_series_per_user` limit on ingesters -> HTTP `400` with matching error,
//!      plus `ingestion_rate` -> HTTP `429` with matching error.

#![allow(clippy::result_large_err)]

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl};
use mimir_proto::cortexpb;
use mimir_proto::prost::Message as _;
use serde_json::{json, Value};

const TENANT_OTLP: &str = "tenant-otlp";
const TENANT_QUORUM: &str = "tenant-quorum";
const TENANT_SERIES_LIM: &str = "tenant-series-limit";
const TENANT_RATE_LIM: &str = "tenant-rate-limit";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair::from_strs(name, value)
}

async fn prom_get_json(
    client: &reqwest::Client,
    port: u16,
    tenant: &str,
    path_and_query: &str,
) -> Result<Value> {
    let resp = client
        .get(format!("http://127.0.0.1:{port}{path_and_query}"))
        .header(LOWER_ORG_ID_HEADER_NAME, tenant)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    ensure!(
        status.is_success(),
        "GET {path_and_query} returned {status}: {body}"
    );
    let v: Value = serde_json::from_str(&body)
        .with_context(|| format!("invalid JSON from {path_and_query}: {body}"))?;
    ensure!(
        v["status"].as_str() == Some("success"),
        "PromQL status not success for {path_and_query}: {v}"
    );
    Ok(v)
}

async fn remote_write_push(
    client: &reqwest::Client,
    dist_http: u16,
    tenant: &str,
    req: &cortexpb::WriteRequest,
) -> Result<(u16, String)> {
    let encoded = req.encode_to_vec();
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .context("snappy encode WriteRequest")?;
    let resp = client
        .post(format!("http://127.0.0.1:{dist_http}/api/v1/push"))
        .header(LOWER_ORG_ID_HEADER_NAME, tenant)
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Prometheus-Remote-Write-Version", "0.1.0")
        .body(compressed)
        .send()
        .await?;
    let code = resp.status().as_u16();
    let body = resp.text().await?;
    Ok((code, body))
}

fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Test A: Go Distributor Rich OTLP /otlp/v1/metrics Differential (Go vs Rust Ingester)
// ---------------------------------------------------------------------------------------------

fn otlp_common_limits() -> &'static str {
    r#"limits:
  otel_created_timestamp_zero_ingestion_enabled: true
  otel_convert_histograms_to_nhcb: true
  otel_promote_scope_metadata: true
  promote_otel_resource_attributes: "cloud.region,k8s.cluster.name"
  otel_keep_identifying_resource_attributes: true
  native_histograms_ingestion_enabled: true
  name_validation_scheme: utf8
  max_global_exemplars_per_user: 1000
  max_global_series_per_user: 100000
  max_global_metadata_per_user: 10000
  query_ingesters_within: 13h
  out_of_order_time_window: 1h
  past_grace_period: 1h"#
}

fn otlp_ingester_yaml(http: u16, grpc: u16, ml: u16, dir: &Path) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: ingester-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}

ingest_storage:
  enabled: false

ingester:
  push_grpc_method_enabled: true
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    num_tokens: 128
    replication_factor: 1
    heartbeat_period: 500ms
    heartbeat_timeout: 5s
    min_ready_duration: 0s
    final_sleep: 0s
    kvstore:
      store: memberlist

blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s

{}
"#,
        dir.join("bucket").display(),
        dir.join("tsdb").display(),
        otlp_common_limits()
    )
}

fn otlp_distributor_yaml(http: u16, grpc: u16, ml: u16, ml_seed: u16) -> String {
    format!(
        r#"target: distributor
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: distributor-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  join_members: ["127.0.0.1:{ml_seed}"]

ingest_storage:
  enabled: false

ingester_client:
  grpc_client_config:
    grpc_compression: s2

ingester:
  ring:
    replication_factor: 1
    kvstore:
      store: memberlist

distributor:
  ring:
    instance_id: distributor-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    kvstore:
      store: memberlist

{}
"#,
        otlp_common_limits()
    )
}

fn otlp_querier_yaml(http: u16, grpc: u16, ml: u16, ml_seed: u16, dir: &Path) -> String {
    format!(
        r#"target: querier
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: querier-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  join_members: ["127.0.0.1:{ml_seed}"]

ingester:
  ring:
    replication_factor: 1
    kvstore:
      store: memberlist

querier:
  query_store_after: 12h
  ring:
    instance_id: querier-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    kvstore:
      store: memberlist

blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}

{}
"#,
        dir.join("bucket").display(),
        otlp_common_limits()
    )
}

fn build_rich_otlp_payload(t0_ms: i64, t1_ms: i64) -> Value {
    let start_ns = (t0_ms - 15_000) * 1_000_000;
    let t0_ns = t0_ms * 1_000_000;
    let t1_ns = t1_ms * 1_000_000;

    json!({
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": "checkout-api" } },
                        { "key": "service.namespace", "value": { "stringValue": "prod" } },
                        { "key": "service.instance.id", "value": { "stringValue": "pod-0" } },
                        { "key": "cloud.region", "value": { "stringValue": "eu-central-1" } },
                        { "key": "k8s.cluster.name", "value": { "stringValue": "g2-cluster" } },
                        { "key": "host.utf8.tag", "value": { "stringValue": "café-αβγ" } }
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {
                            "name": "io.otel.http",
                            "version": "1.2.3",
                            "attributes": [
                                { "key": "scope.env", "value": { "stringValue": "canary" } }
                            ]
                        },
                        "metrics": [
                            {
                                "name": "otlp.gauge.μs",
                                "description": "UTF-8 active gauge",
                                "unit": "us",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "attributes": [
                                                { "key": "http.route", "value": { "stringValue": "/pay/€" } }
                                            ],
                                            "timeUnixNano": t0_ns.to_string(),
                                            "asDouble": 42.5
                                        },
                                        {
                                            "attributes": [
                                                { "key": "http.route", "value": { "stringValue": "/pay/€" } }
                                            ],
                                            "timeUnixNano": t1_ns.to_string(),
                                            "asDouble": 57.25
                                        }
                                    ]
                                }
                            },
                            {
                                "name": "otlp_requests",
                                "description": "Monotonic cumulative sum with startTimeUnixNano",
                                "unit": "1",
                                "sum": {
                                    "aggregationTemporality": 2,
                                    "isMonotonic": true,
                                    "dataPoints": [
                                        {
                                            "attributes": [
                                                { "key": "method", "value": { "stringValue": "POST" } }
                                            ],
                                            "startTimeUnixNano": start_ns.to_string(),
                                            "timeUnixNano": t0_ns.to_string(),
                                            "asDouble": 12.0,
                                            "exemplars": [
                                                {
                                                    "filteredAttributes": [
                                                        { "key": "user_tier", "value": { "stringValue": "vip" } }
                                                    ],
                                                    "timeUnixNano": t0_ns.to_string(),
                                                    "asDouble": 12.0,
                                                    "traceId": "0102030405060708090a0b0c0d0e0f10",
                                                    "spanId": "0102030405060708"
                                                }
                                            ]
                                        },
                                        {
                                            "attributes": [
                                                { "key": "method", "value": { "stringValue": "POST" } }
                                            ],
                                            "startTimeUnixNano": start_ns.to_string(),
                                            "timeUnixNano": t1_ns.to_string(),
                                            "asDouble": 36.0
                                        }
                                    ]
                                }
                            },
                            {
                                "name": "otlp_exp_latency",
                                "description": "OTLP exponential histogram",
                                "unit": "s",
                                "exponentialHistogram": {
                                    "aggregationTemporality": 2,
                                    "dataPoints": [
                                        {
                                            "attributes": [
                                                { "key": "op", "value": { "stringValue": "charge" } }
                                            ],
                                            "startTimeUnixNano": start_ns.to_string(),
                                            "timeUnixNano": t0_ns.to_string(),
                                            "count": "10",
                                            "sum": 4.2,
                                            "scale": 1,
                                            "zeroCount": "2",
                                            "zeroThreshold": 0.001,
                                            "positive": {
                                                "offset": 0,
                                                "bucketCounts": ["3", "4", "1"]
                                            },
                                            "exemplars": [
                                                {
                                                    "timeUnixNano": t0_ns.to_string(),
                                                    "asDouble": 0.85,
                                                    "traceId": "1112131415161718191a1b1c1d1e1f20",
                                                    "spanId": "1112131415161718"
                                                }
                                            ]
                                        },
                                        {
                                            "attributes": [
                                                { "key": "op", "value": { "stringValue": "charge" } }
                                            ],
                                            "startTimeUnixNano": start_ns.to_string(),
                                            "timeUnixNano": t1_ns.to_string(),
                                            "count": "20",
                                            "sum": 9.4,
                                            "scale": 1,
                                            "zeroCount": "4",
                                            "zeroThreshold": 0.001,
                                            "positive": {
                                                "offset": 0,
                                                "bucketCounts": ["6", "8", "2"]
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "name": "otlp_explicit_nhcb",
                                "description": "OTLP explicit histogram converted to NHCB",
                                "unit": "s",
                                "histogram": {
                                    "aggregationTemporality": 2,
                                    "dataPoints": [
                                        {
                                            "attributes": [
                                                { "key": "op", "value": { "stringValue": "auth" } }
                                            ],
                                            "startTimeUnixNano": start_ns.to_string(),
                                            "timeUnixNano": t0_ns.to_string(),
                                            "count": "10",
                                            "sum": 6.5,
                                            "bucketCounts": ["2", "3", "4", "1", "0"],
                                            "explicitBounds": [0.1, 0.5, 1.0, 5.0]
                                        },
                                        {
                                            "attributes": [
                                                { "key": "op", "value": { "stringValue": "auth" } }
                                            ],
                                            "startTimeUnixNano": start_ns.to_string(),
                                            "timeUnixNano": t1_ns.to_string(),
                                            "count": "20",
                                            "sum": 13.0,
                                            "bucketCounts": ["4", "6", "8", "2", "0"],
                                            "explicitBounds": [0.1, 0.5, 1.0, 5.0]
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    })
}

fn normalize_series_list(v: &Value) -> Vec<Value> {
    let mut items = v["data"].as_array().cloned().unwrap_or_default();
    items.sort_by_key(|a| serde_json::to_string(a).unwrap_or_default());
    items
}

#[test]
fn w5_01_go_distributor_otlp_metrics_differential() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        struct Side {
            which: Impl,
            dist_http: u16,
            q_http: u16,
            _procs: Vec<mimir_conformance::twin::Proc>,
        }

        let mut sides = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let side_dir = tmp.path().join(which.name());
            fs::create_dir_all(side_dir.join("bucket")).unwrap();
            fs::create_dir_all(side_dir.join("tsdb")).unwrap();

            let ing_http = free_test_port();
            let ing_grpc = free_test_port();
            let ing_ml = free_test_port();

            let dist_http = free_test_port();
            let dist_grpc = free_test_port();
            let dist_ml = free_test_port();

            let q_http = free_test_port();
            let q_grpc = free_test_port();
            let q_ml = free_test_port();

            let mut ing_proc = start_mimir(
                which,
                &format!("otlp-ing-{}", which.name()),
                &side_dir.join("ing"),
                &otlp_ingester_yaml(ing_http, ing_grpc, ing_ml, &side_dir),
                &[],
            );
            wait_ready(&client, &mut ing_proc, ing_http, Duration::from_secs(45)).await;

            let mut dist_proc = start_mimir(
                Impl::Go,
                &format!("otlp-dist-{}", which.name()),
                &side_dir.join("dist"),
                &otlp_distributor_yaml(dist_http, dist_grpc, dist_ml, ing_ml),
                &[],
            );
            wait_ready(&client, &mut dist_proc, dist_http, Duration::from_secs(45)).await;

            let mut q_proc = start_mimir(
                Impl::Go,
                &format!("otlp-querier-{}", which.name()),
                &side_dir.join("querier"),
                &otlp_querier_yaml(q_http, q_grpc, q_ml, ing_ml, &side_dir),
                &[],
            );
            wait_ready(&client, &mut q_proc, q_http, Duration::from_secs(45)).await;

            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let d_ok = http_get(&client, dist_http, "/ingester/ring", true)
                    .await
                    .is_ok_and(|(c, _, b)| c == 200 && b.contains("ACTIVE"));
                let q_ok = http_get(&client, q_http, "/ingester/ring", true)
                    .await
                    .is_ok_and(|(c, _, b)| c == 200 && b.contains("ACTIVE"));
                if d_ok && q_ok {
                    break;
                }
                assert!(Instant::now() < deadline, "{} ring convergence timeout", which.name());
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            sides.push(Side {
                which,
                dist_http,
                q_http,
                _procs: vec![ing_proc, dist_proc, q_proc],
            });
        }

        let now = now_ms();
        let t0 = now - 60_000;
        let t1 = now - 30_000;
        let payload = serde_json::to_vec(&build_rich_otlp_payload(t0, t1)).unwrap();

        for s in &sides {
            let resp = client
                .post(format!("http://127.0.0.1:{}/otlp/v1/metrics", s.dist_http))
                .header(LOWER_ORG_ID_HEADER_NAME, TENANT_OTLP)
                .header("Content-Type", "application/json")
                .body(payload.clone())
                .send()
                .await
                .expect("POST /otlp/v1/metrics");
            let code = resp.status();
            let body = resp.text().await.unwrap();
            assert_eq!(
                code.as_u16(),
                200,
                "{} OTLP push failed ({code}): {body}",
                s.which.name()
            );
        }

        let start_s = (t0 - 30_000) / 1000;
        let end_s = (t1 + 10_000) / 1000;

        let queries = [
            r#"{"otlp.gauge.μs"}"#,
            "otlp_requests",
            "rate(otlp_requests[1m])",
            "histogram_quantile(0.9, rate(otlp_exp_latency[1m]))",
            "otlp_exp_latency",
            "histogram_quantile(0.9, rate(otlp_explicit_nhcb[1m]))",
            "otlp_explicit_nhcb",
            "target_info",
        ];

        for expr in queries {
            let q_path = format!(
                "/prometheus/api/v1/query?query={}&time={}",
                urlencoding(expr),
                t1 / 1000 + 1
            );
            let go_v = prom_get_json(&client, sides[0].q_http, TENANT_OTLP, &q_path)
                .await
                .unwrap();
            let rs_v = prom_get_json(&client, sides[1].q_http, TENANT_OTLP, &q_path)
                .await
                .unwrap();
            assert!(
                !go_v["data"]["result"].as_array().unwrap().is_empty(),
                "Go baseline empty for query `{expr}`: {go_v}"
            );
            assert_eq!(
                go_v["data"], rs_v["data"],
                "PromQL query `{expr}` mismatch between Go and Rust ingester pipelines"
            );
        }

        for expr in [r#"{"otlp.gauge.μs"}"#, "otlp_requests", "otlp_exp_latency"] {
            let qr_path = format!(
                "/prometheus/api/v1/query_range?query={}&start={start_s}&end={end_s}&step=15",
                urlencoding(expr)
            );
            let go_v = prom_get_json(&client, sides[0].q_http, TENANT_OTLP, &qr_path)
                .await
                .unwrap();
            let rs_v = prom_get_json(&client, sides[1].q_http, TENANT_OTLP, &qr_path)
                .await
                .unwrap();
            assert_eq!(
                go_v["data"], rs_v["data"],
                "PromQL query_range `{expr}` mismatch between Go and Rust"
            );
        }

        let go_labels = prom_get_json(
            &client,
            sides[0].q_http,
            TENANT_OTLP,
            &format!("/prometheus/api/v1/labels?start={start_s}&end={end_s}"),
        )
        .await
        .unwrap();
        let rs_labels = prom_get_json(
            &client,
            sides[1].q_http,
            TENANT_OTLP,
            &format!("/prometheus/api/v1/labels?start={start_s}&end={end_s}"),
        )
        .await
        .unwrap();
        assert_eq!(go_labels["data"], rs_labels["data"], "OTLP labels mismatch");
        let label_strs: BTreeSet<String> = go_labels["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        for expected_lbl in ["cloud.region", "k8s.cluster.name", "otel_scope_name", "otel_scope_version"] {
            assert!(
                label_strs.contains(expected_lbl),
                "expected promoted resource/scope label {expected_lbl} in {label_strs:?}"
            );
        }

        let go_series = prom_get_json(
            &client,
            sides[0].q_http,
            TENANT_OTLP,
            &format!(
                "/prometheus/api/v1/series?match%5B%5D=%7Bjob%21%3D%22%22%7D&start={start_s}&end={end_s}"
            ),
        )
        .await
        .unwrap();
        let rs_series = prom_get_json(
            &client,
            sides[1].q_http,
            TENANT_OTLP,
            &format!(
                "/prometheus/api/v1/series?match%5B%5D=%7Bjob%21%3D%22%22%7D&start={start_s}&end={end_s}"
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            normalize_series_list(&go_series),
            normalize_series_list(&rs_series),
            "OTLP series mismatch"
        );

        for ex_q in ["otlp_requests", "otlp_exp_latency"] {
            let ex_path = format!(
                "/prometheus/api/v1/query_exemplars?query={ex_q}&start={start_s}&end={end_s}"
            );
            let go_ex = prom_get_json(&client, sides[0].q_http, TENANT_OTLP, &ex_path)
                .await
                .unwrap();
            let rs_ex = prom_get_json(&client, sides[1].q_http, TENANT_OTLP, &ex_path)
                .await
                .unwrap();
            assert!(
                !go_ex["data"].as_array().unwrap().is_empty(),
                "expected OTLP exemplars for {ex_q}: {go_ex}"
            );
            assert_eq!(go_ex["data"], rs_ex["data"], "OTLP exemplars mismatch for {ex_q}");
        }

        let go_meta = prom_get_json(&client, sides[0].q_http, TENANT_OTLP, "/prometheus/api/v1/metadata")
            .await
            .unwrap();
        let rs_meta = prom_get_json(&client, sides[1].q_http, TENANT_OTLP, "/prometheus/api/v1/metadata")
            .await
            .unwrap();
        assert_eq!(go_meta["data"], rs_meta["data"], "OTLP metadata mismatch");

        anyhow::Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------------------------
// Test B: Go Distributor Classic gRPC Push RF=3 Quorum & Error Propagation
// ---------------------------------------------------------------------------------------------

fn quorum_runtime_config() -> String {
    format!(
        r#"overrides:
  {TENANT_QUORUM}:
    out_of_order_time_window: 1h
    past_grace_period: 0s
    enforce_out_of_order_window_on_distributor: false
    max_global_series_per_user: 100000
    ingestion_rate: 10000
    ingestion_burst_size: 10000
  {TENANT_SERIES_LIM}:
    max_global_series_per_user: 3
    ingestion_rate: 10000
    ingestion_burst_size: 10000
  {TENANT_RATE_LIM}:
    ingestion_rate: 2
    ingestion_burst_size: 2
    max_global_series_per_user: 100000
"#
    )
}

#[allow(clippy::too_many_arguments)]
fn quorum_ingester_yaml(
    id: &str,
    zone: &str,
    http: u16,
    grpc: u16,
    ml: u16,
    ml_seed: Option<u16>,
    dir: &Path,
    rt_path: &Path,
) -> String {
    let join = match ml_seed {
        Some(seed) => format!("  join_members: [\"127.0.0.1:{seed}\"]"),
        None => String::new(),
    };
    format!(
        r#"target: ingester
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: {id}
  bind_addr: ["127.0.0.1"]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
{join}

ingest_storage:
  enabled: false

ingester:
  push_grpc_method_enabled: true
  ring:
    instance_id: {id}
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    instance_availability_zone: {zone}
    num_tokens: 128
    replication_factor: 3
    zone_awareness_enabled: true
    heartbeat_period: 500ms
    heartbeat_timeout: 5s
    min_ready_duration: 0s
    final_sleep: 0s
    unregister_on_shutdown: false
    kvstore:
      store: memberlist

runtime_config:
  file: {}
  period: 1s

blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s

limits:
  out_of_order_time_window: 1h
  past_grace_period: 0s
  enforce_out_of_order_window_on_distributor: false
  query_ingesters_within: 13h
"#,
        rt_path.display(),
        dir.join("bucket").display(),
        dir.join("tsdb").display(),
    )
}

fn quorum_distributor_yaml(
    http: u16,
    grpc: u16,
    ml: u16,
    ml_seed: u16,
    compression: &str,
    rt_path: &Path,
) -> String {
    format!(
        r#"target: distributor
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: dist-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  join_members: ["127.0.0.1:{ml_seed}"]

ingest_storage:
  enabled: false

ingester_client:
  grpc_client_config:
    grpc_compression: {compression}

ingester:
  ring:
    replication_factor: 3
    zone_awareness_enabled: true
    kvstore:
      store: memberlist

distributor:
  ring:
    instance_id: dist-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    kvstore:
      store: memberlist

runtime_config:
  file: {}
  period: 1s

limits:
  out_of_order_time_window: 1h
  past_grace_period: 0s
  enforce_out_of_order_window_on_distributor: false
"#,
        rt_path.display()
    )
}

fn quorum_querier_yaml(
    http: u16,
    grpc: u16,
    ml: u16,
    ml_seed: u16,
    compression: &str,
    dir: &Path,
    rt_path: &Path,
) -> String {
    format!(
        r#"target: querier
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: querier-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  join_members: ["127.0.0.1:{ml_seed}"]

ingester_client:
  grpc_client_config:
    grpc_compression: {compression}

ingester:
  ring:
    replication_factor: 3
    zone_awareness_enabled: true
    kvstore:
      store: memberlist

querier:
  query_store_after: 12h
  ring:
    instance_id: querier-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    kvstore:
      store: memberlist

runtime_config:
  file: {}
  period: 1s

blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}

limits:
  query_ingesters_within: 13h
"#,
        rt_path.display(),
        dir.join("bucket").display()
    )
}

#[derive(Debug)]
struct QuorumRunOutcome {
    write_3_of_3: (u16, String),
    read_3_of_3: f64,
    write_2_of_3: (u16, String),
    read_2_of_3: f64,
    too_old_status: u16,
    too_old_body: String,
    series_limit_status: u16,
    series_limit_body: String,
    rate_limit_status: u16,
    rate_limit_body: String,
}

async fn run_quorum_ring_scenario(
    client: &reqwest::Client,
    label: &str,
    ing_impls: [Impl; 3],
    compression: &str,
    base_ts: i64,
) -> Result<QuorumRunOutcome> {
    let tmp = tempfile::tempdir()?;
    let rt_path = tmp.path().join("runtime.yaml");
    fs::write(&rt_path, quorum_runtime_config())?;

    let zones = ["zone-a", "zone-b", "zone-c"];
    let mut ing_procs = Vec::new();
    let mut seed_ml = None;
    for (idx, (&which, &zone)) in ing_impls.iter().zip(zones.iter()).enumerate() {
        let id = format!("ing-{zone}-{idx}");
        let dir = tmp.path().join(&id);
        fs::create_dir_all(dir.join("bucket"))?;
        fs::create_dir_all(dir.join("tsdb"))?;
        let http = free_test_port();
        let grpc = free_test_port();
        let ml = free_test_port();
        let yaml = quorum_ingester_yaml(&id, zone, http, grpc, ml, seed_ml, &dir, &rt_path);
        if seed_ml.is_none() {
            seed_ml = Some(ml);
        }
        let mut p = start_mimir(which, &format!("{label}-{id}"), &dir.join("run"), &yaml, &[]);
        wait_ready(client, &mut p, http, Duration::from_secs(45)).await;
        ing_procs.push((id, http, p));
    }
    let seed = seed_ml.unwrap();

    let dist_http = free_test_port();
    let dist_grpc = free_test_port();
    let dist_ml = free_test_port();
    let dist_dir = tmp.path().join("dist");
    let mut dist_proc = start_mimir(
        Impl::Go,
        &format!("{label}-dist"),
        &dist_dir,
        &quorum_distributor_yaml(dist_http, dist_grpc, dist_ml, seed, compression, &rt_path),
        &[],
    );
    wait_ready(client, &mut dist_proc, dist_http, Duration::from_secs(45)).await;

    let q_http = free_test_port();
    let q_grpc = free_test_port();
    let q_ml = free_test_port();
    let q_dir = tmp.path().join("querier");
    fs::create_dir_all(q_dir.join("bucket"))?;
    let mut q_proc = start_mimir(
        Impl::Go,
        &format!("{label}-querier"),
        &q_dir,
        &quorum_querier_yaml(q_http, q_grpc, q_ml, seed, compression, &q_dir, &rt_path),
        &[],
    );
    wait_ready(client, &mut q_proc, q_http, Duration::from_secs(45)).await;

    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let d_ring = mimir_conformance::twin::ingester_ring(client, dist_http)
            .await
            .unwrap_or_default();
        let q_ring = mimir_conformance::twin::ingester_ring(client, q_http)
            .await
            .unwrap_or_default();
        let all_active = |r: &std::collections::BTreeMap<String, (String, usize)>| {
            r.len() == 3 && r.values().all(|(st, _)| st == "ACTIVE")
        };
        if all_active(&d_ring) && all_active(&q_ring) {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "{label}: ring did not reach 3 ACTIVE ingesters: dist={d_ring:?}, q={q_ring:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 1. Normal 3/3 write + quorum read:
    let write_3_of_3 = remote_write_push(
        client,
        dist_http,
        TENANT_QUORUM,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "quorum_probe"), lp("step", "three")],
                samples: vec![cortexpb::Sample {
                    value: 101.0,
                    timestamp_ms: base_ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await?;
    ensure!(
        write_3_of_3.0 == 200,
        "{label}: 3/3 write failed: {write_3_of_3:?}"
    );
    let q3 = prom_get_json(
        client,
        q_http,
        TENANT_QUORUM,
        &format!(
            "/prometheus/api/v1/query?query=quorum_probe%7Bstep%3D%22three%22%7D&time={}",
            base_ts / 1000 + 1
        ),
    )
    .await?;
    let read_3_of_3: f64 = q3["data"]["result"][0]["value"][1]
        .as_str()
        .context("missing read_3_of_3")?
        .parse()?;

    // 2. Soft 400 out-of-bounds / too-old sample:
    let _ = remote_write_push(
        client,
        dist_http,
        TENANT_QUORUM,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "quorum_too_old_test"), lp("k", "1")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: base_ts + 20_000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await?;
    let (too_old_status, too_old_body) = remote_write_push(
        client,
        dist_http,
        TENANT_QUORUM,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "quorum_too_old_test"), lp("k", "1")],
                    samples: vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: base_ts - 2 * 3_600_000,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "quorum_too_old_ok"), lp("k", "1")],
                    samples: vec![cortexpb::Sample {
                        value: 3.0,
                        timestamp_ms: base_ts + 25_000,
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
    )
    .await?;

    // 3. Per-tenant max_global_series_per_user=3 on TENANT_SERIES_LIM -> 4th series fails with 400:
    for i in 0..3 {
        let res = remote_write_push(
            client,
            dist_http,
            TENANT_SERIES_LIM,
            &cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        lp("__name__", "series_lim_metric"),
                        lp("id", &format!("s-{i}")),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: i as f64,
                        timestamp_ms: base_ts + 1000,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await?;
        ensure!(
            res.0 == 200,
            "{label}: series {i} under limit 3 failed: {res:?}"
        );
    }
    let (series_limit_status, series_limit_body) = remote_write_push(
        client,
        dist_http,
        TENANT_SERIES_LIM,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "series_lim_metric"), lp("id", "s-overflow")],
                samples: vec![cortexpb::Sample {
                    value: 99.0,
                    timestamp_ms: base_ts + 2000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await?;

    // 4. Per-tenant ingestion_rate=2, burst=2 -> push 5 samples -> HTTP 429:
    let (rate_limit_status, rate_limit_body) = remote_write_push(
        client,
        dist_http,
        TENANT_RATE_LIM,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: (0..5)
                .map(|i| cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "rate_lim_metric"), lp("i", &format!("{i}"))],
                    samples: vec![cortexpb::Sample {
                        value: i as f64,
                        timestamp_ms: base_ts + 3000,
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
    )
    .await?;

    // 5. Stop 1 ingester (index 1: zone-b, Rust in mixed cluster) via SIGKILL -> 2/3 quorum write & read:
    {
        let (_, _, mut victim) = ing_procs.remove(1);
        victim.signal("KILL");
        let _ = victim.wait_exit(Duration::from_secs(10));
    }
    let write_2_of_3 = remote_write_push(
        client,
        dist_http,
        TENANT_QUORUM,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "quorum_probe"), lp("step", "two")],
                samples: vec![cortexpb::Sample {
                    value: 202.0,
                    timestamp_ms: base_ts + 30_000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await?;
    ensure!(
        write_2_of_3.0 == 200,
        "{label}: 2/3 quorum write failed after 1 node loss: {write_2_of_3:?}"
    );
    let q2 = prom_get_json(
        client,
        q_http,
        TENANT_QUORUM,
        &format!(
            "/prometheus/api/v1/query?query=quorum_probe%7Bstep%3D%22two%22%7D&time={}",
            base_ts / 1000 + 31
        ),
    )
    .await?;
    let read_2_of_3: f64 = q2["data"]["result"][0]["value"][1]
        .as_str()
        .context("missing read_2_of_3")?
        .parse()?;

    Ok(QuorumRunOutcome {
        write_3_of_3,
        read_3_of_3,
        write_2_of_3,
        read_2_of_3,
        too_old_status,
        too_old_body,
        series_limit_status,
        series_limit_body,
        rate_limit_status,
        rate_limit_body,
    })
}

#[test]
fn w5_02_go_distributor_classic_grpc_push_rf3_quorum_and_errors() {
    block_on_for_tests(async {
        let client = http_client();
        let base_ts = now_ms() - 60_000;

        let go_ref = run_quorum_ring_scenario(
            &client,
            "all-go",
            [Impl::Go, Impl::Go, Impl::Go],
            "s2",
            base_ts,
        )
        .await?;

        let mixed_s2 = run_quorum_ring_scenario(
            &client,
            "mixed-s2",
            [Impl::Rust, Impl::Rust, Impl::Go],
            "s2",
            base_ts,
        )
        .await?;

        let mixed_snappy = run_quorum_ring_scenario(
            &client,
            "mixed-snappy",
            [Impl::Rust, Impl::Rust, Impl::Go],
            "snappy",
            base_ts,
        )
        .await?;

        for (name, res) in [("mixed-s2", &mixed_s2), ("mixed-snappy", &mixed_snappy)] {
            assert_eq!(res.write_3_of_3.0, 200, "{name} 3/3 write");
            assert_eq!(res.read_3_of_3, go_ref.read_3_of_3, "{name} 3/3 read");
            assert_eq!(res.write_2_of_3.0, 200, "{name} 2/3 quorum write");
            assert_eq!(res.read_2_of_3, go_ref.read_2_of_3, "{name} 2/3 quorum read");

            assert_eq!(
                res.too_old_status, 400,
                "{name} soft too-old must return 400 (not 500): {}",
                res.too_old_body
            );
            assert_eq!(
                res.too_old_status, go_ref.too_old_status,
                "{name} too-old status vs Go"
            );
            assert!(
                res.too_old_body.contains("err-mimir-sample-timestamp-too-old"),
                "{name} too-old error code missing: {}",
                res.too_old_body
            );

            assert_eq!(
                res.series_limit_status, 400,
                "{name} max_global_series_per_user must return 400: {}",
                res.series_limit_body
            );
            assert_eq!(
                res.series_limit_status, go_ref.series_limit_status,
                "{name} series_limit status vs Go"
            );
            assert!(
                res.series_limit_body.contains("err-mimir-max-series-per-user")
                    || res.series_limit_body.contains("per-user series limit"),
                "{name} series_limit body unexpected (go={}): {}",
                go_ref.series_limit_body,
                res.series_limit_body
            );

            assert_eq!(
                res.rate_limit_status, 429,
                "{name} ingestion_rate must return 429: {}",
                res.rate_limit_body
            );
            assert_eq!(
                res.rate_limit_status, go_ref.rate_limit_status,
                "{name} rate_limit status vs Go"
            );
            assert_eq!(
                res.rate_limit_body, go_ref.rate_limit_body,
                "{name} rate_limit body vs Go"
            );
        }

        anyhow::Ok(())
    })
    .unwrap();
}
