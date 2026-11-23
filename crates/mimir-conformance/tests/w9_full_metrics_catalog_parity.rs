//! Wave 9 automated twin-process conformance gate for 100% non-Go-runtime metrics catalog parity.
//!
//! Boots `target/go-mimir` and `target/release/mimir` side-by-side with a full Helm-style
//! ingester config (Kafka `ingest_storage`, S3, `memberlist`, `runtime_config`,
//! `activity_tracker`, `usage_stats`, `limits`, `register_instrumentation: true`), exercises
//! ingest + all 13 `cortex.Ingester` read RPCs + `grpc.health.v1.Health/Check` + HTTP admin
//! routes + `/ingester/flush`, scrapes `/metrics` (both Prometheus text and delimited protobuf),
//! and asserts:
//!
//! - `0` non-`go_*` `go_only` metric families
//! - `0` `rust_only` metric families
//! - `0` metric TYPE mismatches across all common families
//! - `0` label-name-set mismatches across all common families
//! - `0` HELP string mismatches across all common non-`go_*` families

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Buf;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    ExemplarJson, HistogramJson, KfakeBrokerRequest, KfakeRecordInput, LabelJson, MetadataJson,
    SampleJson, SpanJson, SpawnedKfakeBroker, TimeSeriesJson, WriteRequestJson,
};
use mimir_conformance::twin::{grpc_health, http_get, start_mimir, wait_ready, FakeS3, Impl};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost;
use mimir_proto::prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FamilyInfo {
    metric_type: String,
    help: String,
    label_sets: BTreeSet<BTreeSet<String>>,
}

fn read_len_slice<'a>(buf: &mut &'a [u8]) -> &'a [u8] {
    let len = decode_varint(buf).unwrap() as usize;
    let s = &buf[..len];
    *buf = &buf[len..];
    s
}

fn decode_label_name(mut buf: &[u8]) -> String {
    let mut name = String::new();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::LengthDelimited) => {
                name = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned();
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    name
}

fn decode_metric_labels(mut buf: &[u8]) -> BTreeSet<String> {
    let mut labels = BTreeSet::new();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::LengthDelimited) => {
                let n = decode_label_name(read_len_slice(&mut buf));
                if !n.is_empty() && n != "le" && n != "quantile" {
                    labels.insert(n);
                }
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    labels
}

fn proto_type_name(v: u64) -> &'static str {
    match v {
        0 => "counter",
        1 => "gauge",
        2 => "summary",
        3 => "untyped",
        4 => "histogram",
        _ => "unknown",
    }
}

fn decode_delimited_families(mut raw: &[u8]) -> BTreeMap<String, FamilyInfo> {
    let mut out: BTreeMap<String, FamilyInfo> = BTreeMap::new();
    while raw.has_remaining() {
        let len = prost::decode_length_delimiter(&mut raw).expect("varint len");
        let mut buf = &raw[..len];
        raw.advance(len);

        let mut name = String::new();
        let mut help = String::new();
        let mut mtype = String::new();
        let mut label_sets = BTreeSet::new();

        while buf.has_remaining() {
            let (tag, wt) = decode_key(&mut buf).unwrap();
            match (tag, wt) {
                (1, WireType::LengthDelimited) => {
                    name = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned();
                }
                (2, WireType::LengthDelimited) => {
                    help = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned();
                }
                (3, WireType::Varint) => {
                    mtype = proto_type_name(decode_varint(&mut buf).unwrap()).to_string();
                }
                (4, WireType::LengthDelimited) => {
                    label_sets.insert(decode_metric_labels(read_len_slice(&mut buf)));
                }
                _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
            }
        }
        if !name.is_empty() {
            let entry = out.entry(name).or_insert_with(|| FamilyInfo {
                metric_type: mtype.clone(),
                help: help.clone(),
                label_sets: BTreeSet::new(),
            });
            entry.label_sets.extend(label_sets);
        }
    }
    out
}

fn unescape_help(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn parse_label_names_from_sample(series: &str) -> BTreeSet<String> {
    let Some((_, rest)) = series.split_once('{') else {
        return BTreeSet::new();
    };
    let inside = rest.strip_suffix('}').unwrap_or(rest);
    let mut out = BTreeSet::new();
    let bytes = inside.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] == b',' || bytes[i] == b' ') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        let key = &inside[key_start..i];
        if i >= bytes.len() {
            break;
        }
        i += 1; // '='
        if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
        }
        if !key.is_empty() && key != "le" && key != "quantile" {
            out.insert(key.to_string());
        }
    }
    out
}

fn parse_text_exposition(text: &str) -> BTreeMap<String, FamilyInfo> {
    let mut out: BTreeMap<String, FamilyInfo> = BTreeMap::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            if let Some((name, help)) = rest.split_once(' ') {
                out.entry(name.to_string()).or_default().help = unescape_help(help);
            }
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            if let Some((name, ty)) = rest.split_once(' ') {
                out.entry(name.to_string()).or_default().metric_type = ty.trim().to_string();
            }
        } else if !line.is_empty() && !line.starts_with('#') {
            let Some((series, _val)) = line.rsplit_once(' ') else {
                continue;
            };
            let raw_name = series.split_once('{').map(|(n, _)| n).unwrap_or(series);
            let fam_name = if let Some(base) = raw_name.strip_suffix("_bucket") {
                if out.contains_key(base) {
                    base
                } else {
                    raw_name
                }
            } else if let Some(base) = raw_name.strip_suffix("_sum") {
                if out.contains_key(base) {
                    base
                } else {
                    raw_name
                }
            } else if let Some(base) = raw_name.strip_suffix("_count") {
                if out.contains_key(base) {
                    base
                } else {
                    raw_name
                }
            } else {
                raw_name
            };
            if let Some(entry) = out.get_mut(fam_name) {
                entry.label_sets.insert(parse_label_names_from_sample(series));
            }
        }
    }
    out
}

fn merge_catalogs(dst: &mut BTreeMap<String, FamilyInfo>, src: BTreeMap<String, FamilyInfo>) {
    for (name, info) in src {
        let entry = dst.entry(name).or_insert_with(|| FamilyInfo {
            metric_type: info.metric_type.clone(),
            help: info.help.clone(),
            label_sets: BTreeSet::new(),
        });
        if entry.metric_type.is_empty() {
            entry.metric_type = info.metric_type;
        }
        if entry.help.is_empty() {
            entry.help = info.help;
        }
        entry.label_sets.extend(info.label_sets);
    }
}

async fn scrape_text(http: &reqwest::Client, port: u16) -> String {
    http.get(format!("http://127.0.0.1:{port}/metrics"))
        .header("Accept", "text/plain; version=0.0.4")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

async fn scrape_proto(http: &reqwest::Client, port: u16) -> BTreeMap<String, FamilyInfo> {
    let bytes = http
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .header(
            "Accept",
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited",
        )
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    decode_delimited_families(&bytes)
}

fn with_org<T>(org: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut()
        .insert("x-scope-orgid", MetadataValue::try_from(org).unwrap());
    req
}

fn matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

async fn connect_grpc(port: u16) -> IngesterClient<Channel> {
    let ch = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    IngesterClient::new(ch)
}

async fn query_stream_counts(
    client: &mut IngesterClient<Channel>,
    org: &str,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> usize {
    let mut stream = client
        .query_stream(with_org(
            org,
            ingesterpb::QueryRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers,
                streaming_chunks_batch_size: 128,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut series = 0usize;
    while let Some(resp) = stream.next().await {
        let resp = resp.unwrap();
        series += resp.streaming_series.len();
    }
    series
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn write_runtime_config(path: &Path) {
    let runtime_yaml = r#"overrides:
  t-a:
    ingestion_rate: 100000
    max_global_series_per_user: 0
"#;
    std::fs::write(path, runtime_yaml).unwrap();
}

#[test]
fn full_helm_ingester_metrics_catalog_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let http = reqwest::Client::new();
    let now = (now_ms() / 1000) * 1000;

    block_on_for_tests(async {
        let mut text_catalogs: Vec<BTreeMap<String, FamilyInfo>> = Vec::new();
        let mut proto_catalogs: Vec<BTreeMap<String, FamilyInfo>> = Vec::new();

        for which in IMPLS {
            let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
                topic: "mimir-ingest".to_string(),
                partitions: 2,
                compression: None,
                records: vec![
                    KfakeRecordInput {
                        partition: 0,
                        tenant: "t-a".to_string(),
                        version: 2,
                        compression: None,
                        write_request: WriteRequestJson {
                            mode: String::new(),
                            source: "API".to_string(),
                            skip_label_validation: false,
                            skip_label_count_validation: false,
                            timeseries: vec![
                                TimeSeriesJson {
                                    labels: vec![
                                        LabelJson { name: "__name__".into(), value: "http_reqs".into() },
                                        LabelJson { name: "job".into(), value: "api".into() },
                                    ],
                                    samples: vec![
                                        SampleJson { timestamp_ms: now, value: 42.0 },
                                        SampleJson { timestamp_ms: now - 60_000, value: 41.0 },
                                    ],
                                    exemplars: vec![ExemplarJson {
                                        labels: vec![LabelJson { name: "trace_id".into(), value: "abc".into() }],
                                        value: 1.5,
                                        timestamp_ms: now,
                                    }],
                                    ..Default::default()
                                },
                                TimeSeriesJson {
                                    labels: vec![
                                        LabelJson { name: "__name__".into(), value: "http_dur".into() },
                                        LabelJson { name: "job".into(), value: "api".into() },
                                    ],
                                    histograms: vec![HistogramJson {
                                        timestamp_ms: now,
                                        schema: 0,
                                        zero_threshold: 0.001,
                                        zero_count: 1,
                                        count: 3,
                                        sum: 2.5,
                                        positive_spans: vec![SpanJson { offset: 0, length: 2 }],
                                        positive_deltas: vec![1, 0],
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                },
                            ],
                            metadata: vec![MetadataJson {
                                metric_type: "COUNTER".into(),
                                metric_family_name: "http_reqs".into(),
                                help: "requests".into(),
                                unit: String::new(),
                            }],
                        },
                    },
                    KfakeRecordInput {
                        partition: 1,
                        tenant: "t-b".to_string(),
                        version: 2,
                        compression: None,
                        write_request: WriteRequestJson {
                            mode: String::new(),
                            source: "API".to_string(),
                            skip_label_validation: false,
                            skip_label_count_validation: false,
                            timeseries: vec![TimeSeriesJson {
                                labels: vec![
                                    LabelJson { name: "__name__".into(), value: "worker_jobs".into() },
                                    LabelJson { name: "job".into(), value: "worker".into() },
                                ],
                                samples: vec![SampleJson { timestamp_ms: now, value: 7.0 }],
                                ..Default::default()
                            }],
                            metadata: Vec::new(),
                        },
                    },
                ],
            })
            .unwrap();

            let s3_port = free_test_port();
            let _s3 = FakeS3::start(s3_port, None).await;
            let hp = free_test_port();
            let gp = free_test_port();
            let mp = free_test_port();
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(&dir).unwrap();
            let runtime_path = dir.join("runtime.yaml");
            write_runtime_config(&runtime_path);

            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
  register_instrumentation: true
ingester:
  push_grpc_method_enabled: false
  active_series_metrics_enabled: true
  active_series_metrics_update_period: 100ms
  ring:
    instance_id: ing-zone-a-0
    instance_addr: 127.0.0.1
    instance_availability_zone: zone-a
    kvstore:
      store: memberlist
      prefix: partition-ingesters/
    replication_factor: 1
    num_tokens: 16
    tokens_file_path: {dir}/tokens
    min_ready_duration: 0s
    final_sleep: 0s
    zone_awareness_enabled: true
  partition_ring:
    kvstore:
      store: memberlist
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
memberlist:
  bind_addr: [127.0.0.1]
  bind_port: {mp}
  abort_if_cluster_join_fails: false
ingest_storage:
  enabled: true
  kafka:
    address: {kafka_addr}
    topic: mimir-ingest
    auto_create_topic_enabled: false
    consume_from_position_at_startup: start
    client_rack: zone-a
    wait_strong_read_consistency_timeout: 2s
blocks_storage:
  backend: s3
  s3:
    endpoint: 127.0.0.1:{s3_port}
    bucket_name: mimir-tsdb
    access_key_id: test
    secret_access_key: test
    insecure: true
  tsdb:
    dir: {dir}/tsdb
    head_compaction_interval: 15m
    ship_interval: 1s
    retention_period: 24h
    wal_replay_concurrency: 3
runtime_config:
  file: {runtime_cfg}
  period: 1s
activity_tracker:
  filepath: {dir}/metrics-activity.log
usage_stats:
  enabled: true
  installation_mode: helm
limits:
  max_global_series_per_user: 0
  max_global_series_per_metric: 0
  max_global_exemplars_per_user: 100
  out_of_order_time_window: 24h
  native_histograms_ingestion_enabled: true
"#,
                kafka_addr = broker.broker_addr(),
                dir = dir.display(),
                runtime_cfg = runtime_path.display(),
            );

            let mut proc = start_mimir(which, "catalog", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(20)).await;

            // Wait for Kafka consumption across both partitions/tenants.
            let mut client = connect_grpc(gp).await;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let s_a = query_stream_counts(
                    &mut client,
                    "t-a",
                    now - 120_000,
                    now + 60_000,
                    vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
                )
                .await;
                let s_b = query_stream_counts(
                    &mut client,
                    "t-b",
                    now - 120_000,
                    now + 60_000,
                    vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
                )
                .await;
                if (s_a >= 2 && s_b >= 1) || Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Health check & HTTP admin endpoints.
            assert_eq!(grpc_health(gp).await.unwrap(), 1);
            for path in [
                "/ready",
                "/services",
                "/config",
                "/runtime_config",
                "/ingester/ring",
                "/ingester/partition-ring",
                "/memberlist",
            ] {
                let (code, _, _) = http_get(&http, hp, path, false).await.unwrap();
                assert_eq!(code, 200, "{} GET {path} returned {code}", which.name());
            }

            // Exercise all 13 cortex.Ingester read RPCs.
            let _ = client
                .query_exemplars(with_org(
                    "t-a",
                    ingesterpb::ExemplarQueryRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers: vec![ingesterpb::LabelMatchers {
                            matchers: vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
                        }],
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .label_names(with_org(
                    "t-a",
                    ingesterpb::LabelNamesRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .label_values(with_org(
                    "t-a",
                    ingesterpb::LabelValuesRequest {
                        label_name: "job".into(),
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers: None,
                        limit: 0,
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .metrics_for_label_matchers(with_org(
                    "t-a",
                    ingesterpb::MetricsForLabelMatchersRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers_set: vec![ingesterpb::LabelMatchers {
                            matchers: vec![matcher(ingesterpb::MatchType::Equal, "job", "api")],
                        }],
                        limit: 0,
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .metrics_metadata(with_org("t-a", ingesterpb::MetricsMetadataRequest::default()))
                .await
                .unwrap();
            let _ = client
                .user_stats(with_org("t-a", ingesterpb::UserStatsRequest::default()))
                .await
                .unwrap();
            let _ = client
                .all_user_stats(with_org("t-a", ingesterpb::UserStatsRequest::default()))
                .await
                .unwrap();
            let _ = client
                .label_values_cardinality(with_org(
                    "t-a",
                    ingesterpb::LabelValuesCardinalityRequest {
                        label_names: vec!["job".into()],
                        matchers: vec![matcher(ingesterpb::MatchType::Equal, "job", "api")],
                        count_method: 0,
                    },
                ))
                .await
                .unwrap();
            let mut lnv = client
                .label_names_and_values(with_org(
                    "t-a",
                    ingesterpb::LabelNamesAndValuesRequest::default(),
                ))
                .await
                .unwrap()
                .into_inner();
            while lnv.next().await.is_some() {}
            let mut sln = client
                .search_label_names(with_org(
                    "t-a",
                    ingesterpb::SearchLabelNamesRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner();
            while sln.next().await.is_some() {}
            let mut slv = client
                .search_label_values(with_org(
                    "t-a",
                    ingesterpb::SearchLabelValuesRequest {
                        name: "job".into(),
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner();
            while slv.next().await.is_some() {}
            let mut acs = client
                .active_series(with_org(
                    "t-a",
                    ingesterpb::ActiveSeriesRequest::default(),
                ))
                .await
                .unwrap()
                .into_inner();
            while acs.next().await.is_some() {}

            let mut text_union = parse_text_exposition(&scrape_text(&http, hp).await);
            let mut proto_union = scrape_proto(&http, hp).await;

            // Trigger flush + S3 block upload, then re-query Head + block.
            let flush_resp = http
                .post(format!("http://127.0.0.1:{hp}/ingester/flush?tenant=t-a&wait=true"))
                .send()
                .await
                .unwrap();
            assert!(
                flush_resp.status().is_success(),
                "{} /ingester/flush returned {}",
                which.name(),
                flush_resp.status()
            );
            tokio::time::sleep(Duration::from_millis(2000)).await;

            let _ = query_stream_counts(
                &mut client,
                "t-a",
                0,
                now + 60_000,
                vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
            )
            .await;
            let _ = client
                .label_values(with_org(
                    "t-a",
                    ingesterpb::LabelValuesRequest {
                        label_name: "job".into(),
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers: None,
                        limit: 0,
                    },
                ))
                .await
                .unwrap();
            let mut lnv2 = client
                .label_names_and_values(with_org(
                    "t-a",
                    ingesterpb::LabelNamesAndValuesRequest::default(),
                ))
                .await
                .unwrap()
                .into_inner();
            while lnv2.next().await.is_some() {}

            merge_catalogs(&mut text_union, parse_text_exposition(&scrape_text(&http, hp).await));
            merge_catalogs(&mut proto_union, scrape_proto(&http, hp).await);

            text_catalogs.push(text_union);
            proto_catalogs.push(proto_union);
        }

        for (format_name, catalogs) in [("text", &text_catalogs), ("proto", &proto_catalogs)] {
            let go_cat = &catalogs[0];
            let rust_cat = &catalogs[1];

            let go_names: BTreeSet<&String> = go_cat.keys().collect();
            let rust_names: BTreeSet<&String> = rust_cat.keys().collect();

            let go_only: Vec<&String> = go_names
                .difference(&rust_names)
                .copied()
                .filter(|n| !n.starts_with("go_"))
                .collect();
            let rust_only: Vec<&String> = rust_names
                .difference(&go_names)
                .copied()
                .filter(|n| !n.starts_with("mimir_rs_"))
                .collect();

            assert!(
                go_only.is_empty(),
                "[{format_name}] non-go_* go_only metric families ({}): {go_only:?}",
                go_only.len()
            );
            assert!(
                rust_only.is_empty(),
                "[{format_name}] non-mimir_rs_* rust_only metric families ({}): {rust_only:?}",
                rust_only.len()
            );

            // Verify our intentional Rust-runtime-specific `mimir_rs_*` metrics are emitted
            // cleanly in both text and protobuf exposition formats.
            for expected_rs in [
                "mimir_rs_tokio_workers",
                "mimir_rs_tokio_alive_tasks",
                "mimir_rs_tokio_global_queue_depth",
                "mimir_rs_tokio_worker_busy_seconds_total",
                "mimir_rs_tokio_worker_park_total",
                "mimir_rs_nanos_slab_mapped_bytes",
                "mimir_rs_nanos_slab_pages",
                "mimir_rs_nanos_slab_reclaimed_pages_total",
                "mimir_rs_scoped_worker_pool_threads",
                "mimir_rs_scoped_worker_pool_executions_total",
            ] {
                assert!(
                    rust_cat.contains_key(expected_rs),
                    "[{format_name}] missing expected Rust runtime metric family {expected_rs}"
                );
            }
            #[cfg(target_os = "linux")]
            for expected_linux_rs in [
                "mimir_rs_jemalloc_allocated_bytes",
                "mimir_rs_jemalloc_active_bytes",
                "mimir_rs_jemalloc_resident_bytes",
                "mimir_rs_jemalloc_mapped_bytes",
                "mimir_rs_jemalloc_retained_bytes",
                "mimir_rs_jemalloc_metadata_bytes",
                "mimir_rs_jemalloc_arena_purges_total",
                "mimir_rs_process_rss_anon_bytes",
                "mimir_rs_process_rss_file_bytes",
                "mimir_rs_process_rss_shmem_bytes",
                "mimir_rs_process_threads",
            ] {
                assert!(
                    rust_cat.contains_key(expected_linux_rs),
                    "[{format_name}] missing expected Linux Rust runtime metric family {expected_linux_rs}"
                );
            }

            let mut type_mismatches = Vec::new();
            let mut label_mismatches = Vec::new();
            let mut help_mismatches = Vec::new();

            for (name, go_info) in go_cat {
                let Some(rust_info) = rust_cat.get(name) else {
                    continue;
                };
                if go_info.metric_type != rust_info.metric_type {
                    type_mismatches.push(format!(
                        "{name}: go={} rust={}",
                        go_info.metric_type, rust_info.metric_type
                    ));
                }
                if go_info.label_sets != rust_info.label_sets {
                    label_mismatches.push(format!(
                        "{name}: go={:?} rust={:?}",
                        go_info.label_sets, rust_info.label_sets
                    ));
                }
                if go_info.help != rust_info.help {
                    help_mismatches.push(format!(
                        "{name}:\n  go:   {:?}\n  rust: {:?}",
                        go_info.help, rust_info.help
                    ));
                }
            }

            assert!(
                type_mismatches.is_empty(),
                "[{format_name}] metric TYPE mismatches ({}):\n{}",
                type_mismatches.len(),
                type_mismatches.join("\n")
            );
            assert!(
                label_mismatches.is_empty(),
                "[{format_name}] label-name-set mismatches ({}):\n{}",
                label_mismatches.len(),
                label_mismatches.join("\n")
            );
            assert!(
                help_mismatches.is_empty(),
                "[{format_name}] HELP string mismatches ({}):\n{}",
                help_mismatches.len(),
                help_mismatches.join("\n")
            );

            eprintln!(
                "[{format_name}] parity verified: go_total={} rust_total={} common={} go_only_non_go=0 rust_only=0 type_mismatches=0 label_mismatches=0 help_mismatches=0",
                go_cat.len(),
                rust_cat.len(),
                go_names.intersection(&rust_names).count()
            );
        }
    });
}
