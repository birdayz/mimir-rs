//! Wave 2 gRPC push / query differential suite: boots the unmodified Go `mimir`
//! (`target/go-mimir`) and the Rust `mimir` side by side with identical configuration and
//! compares externally observable behavior for the Wave 2 findings:
//!
//! - W2-01: canceled / timed-out RPCs (strong read consistency waits on a `kfake` broker) do not
//!   leak `cortex_inflight_requests` and are recorded in `cortex_request_duration_seconds`;
//!   `grpc-timeout` is enforced server side.
//! - W2-06: `-ingester.error-sample-rate` sampling and `DoNotLogError` instance-limit errors on
//!   the live Push path (server log lines).
//! - W2-07: hard TSDB / native histogram validation errors roll back the whole request with
//!   `Internal` and no `ErrorDetails`; the 8 mapped histogram errors stay soft.
//! - W2-08: `QueryExemplars` with `matchers: []` vs `[{}]`.
//! - W2-09: `/httpgrpc.HTTP/Handle` empty method, Snappy compression, multitenant auth; Snappy
//!   on `cortex.Ingester` RPCs.
//! - W2-10: read RPC status codes for invalid matchers / shard IDs / multiple org IDs and
//!   `AllUserStats` auth.
//! - W2-15: `cortex_ingester_queried_series{stage=...}` observations of a `QueryStream`.
//! - W2-16: the read circuit breaker counts request deadline timeouts.
//! - W2-17: soft error message formatting (histogram `series=`, unsorted labels, exemplars).
//! - W2-18: a duplicate out-of-order exemplar on a full exemplar buffer evicts nothing.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{KfakeBrokerRequest, SpawnedKfakeBroker};
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::HttpRequest;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Status};

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

/// Time for (possibly buffered) log lines to reach the captured process output.
const LOG_SETTLE: Duration = Duration::from_millis(1500);

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: Bytes::copy_from_slice(name.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
    }
}

fn float_series(labels: &[(&str, &str)], ts: i64, v: f64) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: labels.iter().map(|(n, v)| lp(n, v)).collect(),
        samples: vec![cortexpb::Sample {
            value: v,
            timestamp_ms: ts,
        }],
        ..Default::default()
    }
}

fn hist_series(labels: &[(&str, &str)], h: cortexpb::Histogram) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: labels.iter().map(|(n, v)| lp(n, v)).collect(),
        histograms: vec![h],
        ..Default::default()
    }
}

fn span(offset: i32, length: u32) -> cortexpb::BucketSpan {
    cortexpb::BucketSpan { offset, length }
}

fn int_hist(ts: i64, schema: i32, count: u64, deltas: Vec<i64>) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
        sum: 10.0,
        schema,
        positive_spans: vec![span(0, deltas.len() as u32)],
        positive_deltas: deltas,
        timestamp: ts,
        ..Default::default()
    }
}

fn float_hist(ts: i64, schema: i32, count: f64, counts: Vec<f64>) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.0)),
        sum: 10.0,
        schema,
        positive_spans: vec![span(0, counts.len() as u32)],
        positive_counts: counts,
        timestamp: ts,
        ..Default::default()
    }
}

fn with_org<T>(org: Option<&str>, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if let Some(org) = org {
        req.metadata_mut()
            .insert("x-scope-orgid", MetadataValue::try_from(org).expect("ascii org"));
    }
    req
}

fn matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn eq(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    matcher(ingesterpb::MatchType::Equal, name, value)
}

/// `code|message|details=<bool>` of an RPC outcome.
fn outcome<T>(res: &Result<T, Status>) -> String {
    match res {
        Ok(_) => "OK".to_string(),
        Err(s) => format!(
            "{:?}|{}|details={}",
            s.code(),
            s.message(),
            !s.details().is_empty()
        ),
    }
}

fn code_of<T>(res: &Result<T, Status>) -> Code {
    match res {
        Ok(_) => Code::Ok,
        Err(s) => s.code(),
    }
}

/// Value of a single exposition line `name{...all labels...}` (exact label subset match).
fn metric_value(exposition: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    exposition
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| l.rsplit_once(' '))
        .find(|(series, _)| {
            let (n, rest) = series.split_once('{').unwrap_or((series, ""));
            n == name
                && labels
                    .iter()
                    .all(|(k, v)| rest.contains(&format!("{k}=\"{v}\"")))
        })
        .and_then(|(_, v)| v.parse().ok())
}

/// All `status_code` values recorded for `route` in `cortex_request_duration_seconds_count`.
fn duration_counts_by_status(exposition: &str, route: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for l in exposition.lines().filter(|l| !l.starts_with('#')) {
        let Some((series, v)) = l.rsplit_once(' ') else {
            continue;
        };
        if !series.starts_with("cortex_request_duration_seconds_count{")
            || !series.contains(&format!("route=\"{route}\""))
        {
            continue;
        }
        if let Some(rest) = series.split("status_code=\"").nth(1) {
            let status = rest.split('"').next().unwrap_or_default().to_string();
            *out.entry(status).or_insert(0.0) += v.parse::<f64>().unwrap_or(0.0);
        }
    }
    out
}

struct Side {
    which: Impl,
    proc: Proc,
    http_port: u16,
    channel: Channel,
}

impl Side {
    fn ingester(&self) -> IngesterClient<Channel> {
        IngesterClient::new(self.channel.clone())
    }

    async fn metrics(&self, http: &reqwest::Client) -> String {
        let (code, _, body) = http_get(http, self.http_port, "/metrics", false)
            .await
            .expect("scrape /metrics");
        assert_eq!(code, 200);
        body
    }

    fn push_log_lines(&self) -> usize {
        self.proc
            .log()
            .lines()
            .filter(|l| l.contains("/cortex.Ingester/Push") && l.contains("level=warn"))
            .count()
    }
}

async fn boot(which: Impl, name: &str, dir: &Path, yaml: &str, http_port: u16, grpc_port: u16) -> Side {
    let mut proc = start_mimir(which, name, dir, yaml, &[]);
    let http = reqwest::Client::new();
    wait_ready(&http, &mut proc, http_port, Duration::from_secs(60)).await;
    let channel = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("valid uri")
        .connect()
        .await
        .expect("connect gRPC");
    Side {
        which,
        proc,
        http_port,
        channel,
    }
}

async fn push(side: &Side, org: &str, ts: Vec<cortexpb::TimeSeries>) -> Result<(), Status> {
    side.ingester()
        .push(with_org(
            Some(org),
            cortexpb::WriteRequest {
                timeseries: ts,
                ..Default::default()
            },
        ))
        .await
        .map(|_| ())
}

/// Number of series (with chunks) returned by `QueryStream` for `__name__="<metric>"`.
async fn query_series(side: &Side, org: &str, metric: &str) -> Result<usize, Status> {
    let mut stream = side
        .ingester()
        .query_stream(with_org(
            Some(org),
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: now_ms() + 3_600_000,
                matchers: vec![eq("__name__", metric)],
                streaming_chunks_batch_size: 128,
            },
        ))
        .await?
        .into_inner();
    let mut n = 0;
    while let Some(msg) = stream.next().await {
        n += msg?.streaming_series.len();
    }
    Ok(n)
}

async fn drain_stream<T>(
    res: Result<tonic::Response<tonic::Streaming<T>>, Status>,
) -> Result<usize, Status> {
    let mut s = res?.into_inner();
    let mut n = 0;
    while let Some(m) = s.next().await {
        m?;
        n += 1;
    }
    Ok(n)
}

/// Sends `msg` to `path` gRPC-framed with `grpc-encoding: snappy` and returns the status.
async fn snappy_unary(channel: &Channel, path: &str, org: &str, msg: Vec<u8>) -> (Code, Bytes) {
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let framed = mimir_proto::snappy::encode_grpc_frame(&msg, true).expect("snappy frame");
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("grpc-encoding", "snappy")
        .header("grpc-accept-encoding", "identity")
        .header("x-scope-orgid", org)
        .body(tonic::body::BoxBody::new(
            http_body_util::Full::new(framed).map_err(|e| match e {}),
        ))
        .expect("request");
    let resp = channel.clone().oneshot(req).await.expect("transport");
    let header_status = Status::from_header_map(resp.headers()).map(|s| s.code());
    let collected = resp.into_body().collect().await.expect("body");
    let trailer_status = collected
        .trailers()
        .and_then(Status::from_header_map)
        .map(|s| s.code());
    let code = header_status.or(trailer_status).unwrap_or(Code::Ok);
    let body = collected.to_bytes();
    // grpc-go answers with the request's compressor; tonic uncompressed: compare payloads.
    let payload = if body.is_empty() {
        Bytes::new()
    } else {
        mimir_proto::snappy::decode_grpc_frame(&body).expect("response frame")
    };
    (code, payload)
}

fn classic_yaml(http_port: u16, grpc_port: u16, dir: &Path) -> String {
    let runtime = dir.join("runtime.yaml");
    std::fs::create_dir_all(dir).expect("mkdir");
    std::fs::write(
        &runtime,
        "overrides:\n  t-ex:\n    out_of_order_time_window: 1h\n    max_global_exemplars_per_user: 3\n",
    )
    .expect("runtime config");
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
ingester:
  error_sample_rate: 10
  instance_limits:
    max_tenants: 4
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
limits:
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 100
runtime_config:
  file: {runtime}
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 10m
    head_compaction_interval: 10m
activity_tracker:
  filepath: ""
"#,
        runtime = runtime.display(),
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

/// Observations of one implementation, compared key by key between Go and Rust.
type Report = BTreeMap<String, String>;

/// `now` is shared by both implementations so error messages (timestamps) are comparable.
async fn classic_report(side: &Side, http: &reqwest::Client, now: i64) -> Report {
    let mut r = Report::new();

    // --- W2-06: sampled soft errors (1 log line per 10) ---------------------------------------
    push(side, "t-a", vec![float_series(&[("__name__", "ooo_metric")], now, 1.0)])
        .await
        .expect("initial push");
    // Go's logger may buffer lines: let earlier phases' lines arrive first.
    tokio::time::sleep(LOG_SETTLE).await;
    let before = side.push_log_lines();
    let mut sampled = Vec::new();
    for i in 0..10 {
        let res = push(
            side,
            "t-a",
            vec![float_series(&[("__name__", "ooo_metric")], now - 60_000 - i, 2.0)],
        )
        .await;
        sampled.push(code_of(&res));
    }
    r.insert("w2-06 sampled codes".into(), format!("{sampled:?}"));
    tokio::time::sleep(LOG_SETTLE).await;
    r.insert(
        "w2-06 sampled push warn lines".into(),
        (side.push_log_lines() - before).to_string(),
    );

    // --- W2-07: hard errors roll back the whole request ----------------------------------------
    let hard_cases: Vec<(&str, cortexpb::TimeSeries)> = vec![
        ("nameless", float_series(&[("job", "only_job")], now, 1.0)),
        ("empty-labelset", float_series(&[("a", "")], now, 1.0)),
        ("schema-9", hist_series(&[("__name__", "nh_bad")], int_hist(now, 9, 3, vec![1, 1]))),
        (
            "negative-float-count",
            hist_series(&[("__name__", "nh_bad")], float_hist(now, 0, -1.0, vec![1.0])),
        ),
        (
            "nhcb-nan-bound",
            hist_series(
                &[("__name__", "nh_bad")],
                cortexpb::Histogram {
                    custom_values: vec![f64::NAN],
                    ..float_hist(now, -53, 1.0, vec![1.0])
                },
            ),
        ),
        (
            "nhcb-zero-count",
            hist_series(
                &[("__name__", "nh_bad")],
                cortexpb::Histogram {
                    custom_values: vec![1.0],
                    zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(1.0)),
                    ..float_hist(now, -53, 2.0, vec![1.0])
                },
            ),
        ),
    ];
    for (name, bad) in hard_cases {
        let res = push(
            side,
            "t-b",
            vec![float_series(&[("__name__", "valid_s")], now, 1.0), bad],
        )
        .await;
        r.insert(format!("w2-07 {name}"), outcome(&res));
        let committed = query_series(side, "t-b", "valid_s").await.map_err(|e| e.to_string());
        r.insert(format!("w2-07 {name} committed valid_s"), format!("{committed:?}"));
    }

    // --- W2-07 / W2-17: soft errors and their messages -----------------------------------------
    let soft_cases: Vec<(&str, Vec<cortexpb::TimeSeries>)> = vec![
        (
            "nh-count-mismatch",
            vec![hist_series(
                &[("__name__", "nh_mismatch"), ("job", "api")],
                int_hist(now, 0, 22, vec![1, 19]),
            )],
        ),
        (
            "unsorted-with-empty-value",
            vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "m_new"), lp("b", ""), lp("a", "1")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: now,
                }],
                ..Default::default()
            }],
        ),
        (
            "duplicate-exemplar-label",
            vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "ex_dup")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: now,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![lp("trace_id", "1"), lp("trace_id", "2")],
                    value: 1.0,
                    timestamp_ms: now,
                }],
                ..Default::default()
            }],
        ),
    ];
    for (name, ts) in soft_cases {
        let res = push(side, "t-a", ts).await;
        r.insert(format!("w2-17 {name}"), outcome(&res));
    }

    // --- W2-18: duplicate OOO exemplar on a full buffer (t-ex: 3 exemplars, 1h OOO window) -----
    let ex = |metric: &str, trace: &str, ts: i64| cortexpb::TimeSeries {
        labels: vec![lp("__name__", metric)],
        samples: vec![cortexpb::Sample {
            value: 1.0,
            timestamp_ms: ts,
        }],
        exemplars: vec![cortexpb::Exemplar {
            labels: vec![lp("trace_id", trace)],
            value: 1.0,
            timestamp_ms: ts,
        }],
        ..Default::default()
    };
    let base = now - 600_000;
    for (metric, trace, ts) in [
        ("ex_other", "o", base),
        ("ex_s", "a", base + 100),
        ("ex_s", "b", base + 300),
        // Out of order, same timestamp as "a": a no-op that must not evict "ex_other".
        ("ex_s", "c", base + 100),
    ] {
        let res = push(side, "t-ex", vec![ex(metric, trace, ts)]).await;
        r.insert(format!("w2-18 push {metric} {trace}"), format!("{:?}", code_of(&res)));
    }

    // --- W2-08: QueryExemplars [] vs [{}] -------------------------------------------------------
    for (name, org, matchers) in [
        ("[]", "t-ex", Vec::new()),
        ("[{}]", "t-ex", vec![ingesterpb::LabelMatchers { matchers: Vec::new() }]),
    ] {
        let res = side
            .ingester()
            .query_exemplars(with_org(
                Some(org),
                ingesterpb::ExemplarQueryRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: now + 3_600_000,
                    matchers,
                },
            ))
            .await;
        let summary = match res {
            Ok(resp) => {
                let mut per_series: Vec<String> = resp
                    .into_inner()
                    .timeseries
                    .iter()
                    .map(|ts| {
                        let name = ts
                            .labels
                            .iter()
                            .map(|l| String::from_utf8_lossy(&l.value).into_owned())
                            .collect::<Vec<_>>()
                            .join(",");
                        let exs: Vec<i64> = ts.exemplars.iter().map(|e| e.timestamp_ms - base).collect();
                        format!("{name}:{exs:?}")
                    })
                    .collect();
                per_series.sort();
                format!("{per_series:?}")
            }
            Err(e) => outcome::<()>(&Err(e)),
        };
        r.insert(format!("w2-08/18 exemplars {name}"), summary);
    }

    // --- W2-15: queried_series stages of one QueryStream ----------------------------------------
    let stages = ["merged_blocks", "single_block_index", "single_block_filter"];
    let before = side.metrics(http).await;
    let n = query_series(side, "t-a", "ooo_metric").await.expect("query stream");
    r.insert("w2-15 query series".into(), n.to_string());
    let after = side.metrics(http).await;
    for stage in stages {
        for suffix in ["_count", "_sum"] {
            let name = format!("cortex_ingester_queried_series{suffix}");
            let d = metric_value(&after, &name, &[("stage", stage)]).unwrap_or(0.0)
                - metric_value(&before, &name, &[("stage", stage)]).unwrap_or(0.0);
            r.insert(format!("w2-15 queried_series{suffix} {stage}"), d.to_string());
        }
    }

    // --- W2-10: read RPC status codes -----------------------------------------------------------
    let bad_regex = || vec![matcher(ingesterpb::MatchType::RegexMatch, "job", "(")];
    let bad_shard = || vec![eq("__name__", "ooo_metric"), eq("__query_shard__", "bad")];
    let mut c = side.ingester();
    let qs = |m: Vec<ingesterpb::LabelMatcher>| {
        with_org(
            Some("t-a"),
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: now + 1000,
                matchers: m,
                streaming_chunks_batch_size: 128,
            },
        )
    };
    let res = drain_stream(c.query_stream(qs(bad_regex())).await).await;
    r.insert("w2-10 QueryStream bad regex".into(), format!("{:?}", code_of(&res)));
    let res = drain_stream(c.query_stream(qs(bad_shard())).await).await;
    r.insert("w2-10 QueryStream bad shard".into(), outcome(&res));
    let res = c
        .label_values(with_org(
            Some("t-a"),
            ingesterpb::LabelValuesRequest {
                label_name: "job".into(),
                start_timestamp_ms: 0,
                end_timestamp_ms: now + 1000,
                matchers: Some(ingesterpb::LabelMatchers { matchers: bad_regex() }),
                limit: 0,
            },
        ))
        .await;
    r.insert("w2-10 LabelValues bad regex".into(), format!("{:?}", code_of(&res)));
    let res = c
        .query_exemplars(with_org(
            Some("t-a"),
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: now + 1000,
                matchers: vec![ingesterpb::LabelMatchers { matchers: bad_regex() }],
            },
        ))
        .await;
    r.insert("w2-10 QueryExemplars bad regex".into(), format!("{:?}", code_of(&res)));
    let res = drain_stream(
        c.label_names_and_values(with_org(
            Some("t-a"),
            ingesterpb::LabelNamesAndValuesRequest {
                matchers: bad_regex(),
                ..Default::default()
            },
        ))
        .await,
    )
    .await;
    r.insert("w2-10 LabelNamesAndValues bad regex".into(), format!("{:?}", code_of(&res)));
    let res = drain_stream(
        c.search_label_names(with_org(
            Some("t-a"),
            ingesterpb::SearchLabelNamesRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: now + 1000,
                matchers: bad_regex(),
                ..Default::default()
            },
        ))
        .await,
    )
    .await;
    r.insert("w2-10 SearchLabelNames bad regex".into(), format!("{:?}", code_of(&res)));
    let res = c
        .label_names(with_org(Some("a|b"), ingesterpb::LabelNamesRequest::default()))
        .await;
    r.insert("w2-10 LabelNames org a|b".into(), outcome(&res));
    let res = c.all_user_stats(with_org(None, ingesterpb::UserStatsRequest::default())).await;
    r.insert("w2-10 AllUserStats no org".into(), outcome(&res));
    let res = c.all_user_stats(with_org(Some("t-a"), ingesterpb::UserStatsRequest::default())).await;
    r.insert("w2-10 AllUserStats with org".into(), format!("{:?}", code_of(&res)));

    // --- W2-09: httpgrpc and Snappy -------------------------------------------------------------
    let mut hg = HttpClient::new(side.channel.clone());
    let ready = |method: &str| HttpRequest {
        method: method.to_string(),
        url: "/ready".to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let res = hg.handle(with_org(None, ready("GET"))).await;
    r.insert("w2-09 httpgrpc no org".into(), outcome(&res));
    let res = hg.handle(with_org(Some("t-a"), ready(""))).await;
    r.insert(
        "w2-09 httpgrpc empty method".into(),
        match res {
            Ok(resp) => {
                let resp = resp.into_inner();
                format!("{}|{}", resp.code, String::from_utf8_lossy(&resp.body).trim())
            }
            Err(e) => outcome::<()>(&Err(e)),
        },
    );
    let (code, body) = snappy_unary(
        &side.channel,
        "/httpgrpc.HTTP/Handle",
        "t-a",
        ready("GET").encode_to_vec(),
    )
    .await;
    let resp = mimir_proto::httpgrpcpb::HttpResponse::decode(body).map(|r| r.code);
    r.insert("w2-09 httpgrpc snappy".into(), format!("{code:?}|{resp:?}"));
    let (code, body) = snappy_unary(
        &side.channel,
        "/cortex.Ingester/LabelNames",
        "t-a",
        ingesterpb::LabelNamesRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: now + 1000,
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;
    let names = ingesterpb::LabelNamesResponse::decode(body).map(|r| r.label_names);
    r.insert("w2-09 LabelNames snappy".into(), format!("{code:?}|{names:?}"));

    // --- W2-06: DoNotLogError instance-limit rejections are never logged -----------------------
    // Go's logger may buffer lines: let earlier phases' lines arrive first.
    tokio::time::sleep(LOG_SETTLE).await;
    let before = side.push_log_lines();
    let mut codes = Vec::new();
    for i in 0..5 {
        let res = push(
            side,
            &format!("t-over-{i}"),
            vec![float_series(&[("__name__", "m")], now, 1.0)],
        )
        .await;
        codes.push(outcome(&res));
    }
    r.insert("w2-06 max_tenants outcomes".into(), format!("{codes:?}"));
    tokio::time::sleep(LOG_SETTLE).await;
    r.insert(
        "w2-06 max_tenants push warn lines".into(),
        (side.push_log_lines() - before).to_string(),
    );

    r
}

fn assert_reports_equal(go: &Report, rust: &Report) {
    let mut diffs = Vec::new();
    for (k, gv) in go {
        let rv = rust.get(k).map(String::as_str).unwrap_or("<missing>");
        if gv != rv {
            diffs.push(format!("{k}:\n    go:   {gv}\n    rust: {rv}"));
        }
    }
    assert!(diffs.is_empty(), "Go vs Rust divergences:\n{}", diffs.join("\n"));
}

#[test]
fn twin_process_wave2_grpc_push_query_parity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    block_on_for_tests(async {
        let http = reqwest::Client::new();
        let mut reports = Vec::new();
        let now = now_ms();
        for which in IMPLS {
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(which.name());
            let side = boot(which, "w2-classic", &dir, &classic_yaml(hp, gp, &dir), hp, gp).await;
            let report = classic_report(&side, &http, now).await;
            reports.push((side.which, report, side.proc.log()));
        }
        let (_, go, go_log) = &reports[0];
        let (_, rust, rust_log) = &reports[1];
        eprintln!("go report: {go:#?}");
        if go != rust {
            eprintln!("--- go log tail ---\n{}", tail(go_log, 40));
            eprintln!("--- rust log tail ---\n{}", tail(rust_log, 40));
        }
        assert_reports_equal(go, rust);

        // Absolute expectations (guard against both sides regressing the same way).
        let expect = |k: &str, prefix: &str| {
            let v = &rust[k];
            assert!(v.starts_with(prefix), "{k}: {v}");
        };
        expect("w2-06 sampled push warn lines", "1");
        expect("w2-06 max_tenants push warn lines", "0");
        expect("w2-07 nameless", "Internal|user=t-b: no metric name label|details=false");
        expect("w2-07 empty-labelset", "Internal|user=t-b: empty labelset: invalid sample|details=false");
        expect("w2-07 schema-9", "Internal|");
        expect("w2-07 nhcb-nan-bound", "Internal|");
        expect("w2-07 nameless committed valid_s", "Ok(0)");
        expect("w2-07 schema-9 committed valid_s", "Ok(0)");
        expect("w2-17 nh-count-mismatch", "InvalidArgument|");
        assert!(rust["w2-17 nh-count-mismatch"].contains("series=[{__name__ nh_mismatch} {job api}]"));
        assert!(rust["w2-17 unsorted-with-empty-value"].contains("m_new{b=\"\", a=\"1\"}"));
        assert!(rust["w2-17 duplicate-exemplar-label"].contains("label name \"trace_id\" is not unique: invalid exemplar"));
        expect("w2-08/18 exemplars []", "[]");
        assert!(rust["w2-08/18 exemplars [{}]"].contains("ex_other"), "{}", rust["w2-08/18 exemplars [{}]"]);
        expect("w2-10 QueryStream bad regex", "Internal");
        expect("w2-10 SearchLabelNames bad regex", "InvalidArgument");
        expect("w2-10 LabelNames org a|b", "Internal|multiple org IDs present");
        expect("w2-10 AllUserStats no org", "Unknown|no org id");
        expect("w2-09 httpgrpc no org", "Unknown|no org id");
        expect("w2-09 httpgrpc empty method", "200|ready");
        expect("w2-09 httpgrpc snappy", "Ok|Ok(200)");
        expect("w2-09 LabelNames snappy", "Ok|");
        assert_ne!(rust["w2-15 queried_series_count single_block_index"], "0");
        assert_ne!(rust["w2-15 queried_series_count single_block_filter"], "0");
    });
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

fn kafka_yaml(http_port: u16, grpc_port: u16, dir: &Path, broker: &str, topic: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
ingester:
  ring:
    instance_id: ingester-zone-a-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
  push_grpc_method_enabled: false
  read_circuit_breaker:
    enabled: true
    initial_delay: 0s
    request_timeout: 1m
    failure_execution_threshold: 1000
ingest_storage:
  enabled: true
  kafka:
    address: {broker}
    topic: {topic}
    auto_create_topic_enabled: false
    wait_strong_read_consistency_timeout: 1s
    last_produced_offset_poll_interval: 25ms
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 10m
activity_tracker:
  filepath: ""
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

/// LabelNames with strong read consistency on an offset that is never produced.
fn strong_label_names() -> tonic::Request<ingesterpb::LabelNamesRequest> {
    let mut req = with_org(Some("t1"), ingesterpb::LabelNamesRequest::default());
    req.metadata_mut()
        .insert("__consistency_level__", MetadataValue::from_static("strong"));
    req.metadata_mut().insert(
        "__consistency_offsets__",
        MetadataValue::from_static("v1=0:999999"),
    );
    req
}

/// The same strong-consistency LabelNames over a raw HTTP/2 connection carrying a
/// `grpc-timeout` header but *without* a client-side deadline (tonic's client would enforce the
/// header itself and reset the stream first), so only the server's deadline handling is observed.
async fn strong_label_names_server_deadline(grpc_port: u16, grpc_timeout: &str) -> (Code, String) {
    use http_body_util::BodyExt;

    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", grpc_port))
        .await
        .expect("connect");
    let (mut sender, conn) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(tcp),
    )
    .await
    .expect("h2 handshake");
    tokio::spawn(conn);
    let msg = ingesterpb::LabelNamesRequest::default().encode_to_vec();
    let framed = mimir_proto::snappy::encode_grpc_frame(&msg, false).expect("frame");
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{grpc_port}/cortex.Ingester/LabelNames"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("grpc-timeout", grpc_timeout)
        .header("x-scope-orgid", "t1")
        .header("__consistency_level__", "strong")
        .header("__consistency_offsets__", "v1=0:999999")
        .body(http_body_util::Full::new(framed))
        .expect("request");
    let resp = tokio::time::timeout(Duration::from_secs(20), async {
        let resp = sender.send_request(req).await?;
        let headers = resp.headers().clone();
        let collected = resp.into_body().collect().await?;
        Ok::<_, hyper::Error>((headers, collected.trailers().cloned()))
    })
    .await
    .expect("server must enforce grpc-timeout");
    match resp {
        Ok((headers, trailers)) => {
            let st = Status::from_header_map(&headers)
                .or_else(|| trailers.as_ref().and_then(Status::from_header_map));
            match st {
                Some(st) => (st.code(), st.message().to_string()),
                None => (Code::Ok, String::new()),
            }
        }
        // grpc-go may reset the stream (RST_STREAM CANCEL) when its deadline timer fires.
        Err(e) => (Code::Cancelled, e.to_string()),
    }
}

#[test]
fn twin_process_wave2_canceled_and_timed_out_rpcs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    block_on_for_tests(async {
        let http = reqwest::Client::new();
        let route = "/cortex.Ingester/LabelNames";
        let mut results = Vec::new();
        for which in IMPLS {
            let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
                topic: "ingest".to_string(),
                partitions: 1,
                compression: None,
                records: Vec::new(),
            })
            .expect("kfake broker");
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(&dir).expect("mkdir");
            let yaml = kafka_yaml(hp, gp, &dir, broker.broker_addr(), broker.topic());
            let side = boot(which, "w2-kafka", &dir, &yaml, hp, gp).await;

            // W2-01: the client gives up (RST_STREAM) after 100ms, 3 times.
            for _ in 0..3 {
                let mut c = side.ingester();
                let res =
                    tokio::time::timeout(Duration::from_millis(100), c.label_names(strong_label_names()))
                        .await;
                assert!(res.is_err(), "{} LabelNames must still be waiting", which.name());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            let m = side.metrics(&http).await;
            let canceled = (
                metric_value(&m, "cortex_inflight_requests", &[("route", route)]),
                duration_counts_by_status(&m, route),
            );

            // W2-01 / W2-16: server-side grpc-timeout with a patient client.
            let started = Instant::now();
            let (code, msg) = strong_label_names_server_deadline(gp, "300m").await;
            let elapsed = started.elapsed();
            tokio::time::sleep(Duration::from_millis(500)).await;
            let m = side.metrics(&http).await;
            let timed_out = (
                metric_value(&m, "cortex_inflight_requests", &[("route", route)]),
                duration_counts_by_status(&m, route),
                metric_value(
                    &m,
                    "cortex_ingester_circuit_breaker_request_timeouts_total",
                    &[("request_type", "read")],
                ),
            );
            eprintln!(
                "{}: canceled={canceled:?}; server deadline: code={code:?} msg={msg:?} after {elapsed:?}, after={timed_out:?}",
                which.name()
            );
            assert!(elapsed >= Duration::from_millis(300), "{}: answered early", which.name());

            // W2-16: the configured wait timeout expires first: DeadlineExceeded, counted by the
            // read circuit breaker.
            let res = tokio::time::timeout(
                Duration::from_secs(10),
                side.ingester().label_names(strong_label_names()),
            )
            .await
            .expect("configured wait timeout enforced");
            let configured = outcome(&res);
            tokio::time::sleep(Duration::from_millis(300)).await;
            let m = side.metrics(&http).await;
            let after_configured = (
                metric_value(&m, "cortex_inflight_requests", &[("route", route)]),
                duration_counts_by_status(&m, route),
                metric_value(
                    &m,
                    "cortex_ingester_circuit_breaker_request_timeouts_total",
                    &[("request_type", "read")],
                ),
            );
            eprintln!("{}: configured timeout: {configured} -> {after_configured:?}", which.name());
            results.push((which, canceled, code, timed_out, configured, after_configured));
            drop(side);
            drop(broker);
        }

        let (_, go_canceled, go_code, go_timed_out, go_configured, go_after) = &results[0];
        let (_, rust_canceled, rust_code, rust_timed_out, rust_configured, rust_after) = &results[1];
        // Client cancellation: nothing leaks, every RPC is recorded as Canceled.
        assert_eq!(rust_canceled, go_canceled);
        assert_eq!(rust_canceled.0, Some(0.0));
        assert_eq!(rust_canceled.1.get("Canceled"), Some(&3.0), "{rust_canceled:?}");
        // Server-side grpc-timeout. Both grpc-go and Rust race the stream reset
        // (RST_STREAM CANCEL, handler context canceled -> recorded as Canceled) against the
        // context deadline (handler returns its DeadlineExceeded error first -> recorded as
        // Unknown and counted by the read circuit breaker).
        let delta = |after: &BTreeMap<String, f64>, before: &BTreeMap<String, f64>| {
            let mut d = BTreeMap::new();
            for (k, v) in after {
                let x = v - before.get(k).copied().unwrap_or(0.0);
                if x != 0.0 {
                    d.insert(k.clone(), x);
                }
            }
            d
        };
        let is_valid_deadline = |code: &Code,
                                 before_counts: &BTreeMap<String, f64>,
                                 after: &(Option<f64>, BTreeMap<String, f64>, Option<f64>)| {
            let reset = *code == Code::Cancelled
                && delta(&after.1, before_counts)
                    == BTreeMap::from([("Canceled".to_string(), 1.0)])
                && after.2 == Some(0.0);
            // When grpc-go's 300ms server-side grpc-timeout fires concurrently with the handler
            // observing context.DeadlineExceeded, the handler records Unknown + 1 read CB timeout
            // while http2_server.go concurrently sends RST_STREAM(CANCEL), so hyper sees an HTTP/2
            // stream-reset Err which strong_label_names_server_deadline maps to Code::Cancelled.
            let deadline = (*code == Code::DeadlineExceeded || *code == Code::Cancelled)
                && delta(&after.1, before_counts)
                    == BTreeMap::from([("Unknown".to_string(), 1.0)])
                && after.2 == Some(1.0);
            reset || deadline
        };
        assert!(
            is_valid_deadline(go_code, &go_canceled.1, go_timed_out),
            "unexpected Go outcome: {go_code:?} {go_timed_out:?}"
        );
        assert!(
            is_valid_deadline(rust_code, &rust_canceled.1, rust_timed_out),
            "unexpected Rust outcome: {rust_code:?} {rust_timed_out:?}"
        );
        assert_eq!(rust_timed_out.0, Some(0.0));
        assert_eq!(go_timed_out.0, Some(0.0));

        // Configured wait timeout: DeadlineExceeded (labelled Unknown like dskit does for the
        // wrapped context error) and one read circuit breaker timeout, in both.
        assert_eq!(rust_configured, go_configured);
        assert!(rust_configured.starts_with("DeadlineExceeded|"), "{rust_configured}");
        for (name, after, before) in [("go", go_after, go_timed_out), ("rust", rust_after, rust_timed_out)] {
            assert_eq!(after.0, Some(0.0), "{name}");
            assert_eq!(
                delta(&after.1, &before.1),
                BTreeMap::from([("Unknown".to_string(), 1.0)]),
                "{name}: {after:?}"
            );
            assert_eq!(
                after.2.unwrap_or(0.0) - before.2.unwrap_or(0.0),
                1.0,
                "{name}: read circuit breaker timeouts {after:?}"
            );
        }
    });
}
