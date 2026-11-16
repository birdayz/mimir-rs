//! Wave 4 Differential Conformance Tests:
//! 1. Twin-process classic Push + all 14 RPCs + error/ErrorDetails parity + HTTP endpoints + cross-engine block verification
//! 2. Live mixed Go + Rust cluster over memberlist ring gossip (`replication_factor: 2`)
//! 3. Twin-process Kafka `ingest_storage` v2 + strong read consistency parity

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_stream::StreamExt;

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, invoke_oracle,
    rust_mimir_release_bin, ExemplarJson, HistogramJson, KfakeBrokerRequest, KfakeRecordInput,
    LabelJson, MetadataJson, SampleJson, SpawnedKfakeBroker, TimeSeriesJson, VerifyTsdbDirRequest,
    VerifyTsdbDirResponse, WriteRequestJson,
};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::matcher::{LabelMatcher as TsdbLabelMatcher, MatchType as TsdbMatchType};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

/// RAII guard ensuring spawned child processes are always terminated and reaped on drop.
struct ProcessGuard {
    name: String,
    child: Option<Child>,
    stderr_buf: Arc<Mutex<Vec<u8>>>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl ProcessGuard {
    fn spawn(name: impl Into<String>, bin: &Path, args: &[String], workdir: &Path) -> Self {
        let name = name.into();
        let mut child = Command::new(bin)
            .args(args)
            .current_dir(workdir)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {name} ({}): {e}", bin.display()));
        let stderr_buf = Arc::new(Mutex::new(Vec::new()));
        let stderr_thread = child.stderr.take().map(|mut err| {
            let buf = Arc::clone(&stderr_buf);
            std::thread::spawn(move || {
                let mut tmp = [0u8; 8192];
                while let Ok(n) = err.read(&mut tmp) {
                    if n == 0 {
                        break;
                    }
                    if let Ok(mut g) = buf.lock() {
                        if g.len() < 1024 * 1024 {
                            let rem = (1024 * 1024) - g.len();
                            g.extend_from_slice(&tmp[..n.min(rem)]);
                        }
                    }
                }
            })
        });
        Self {
            name,
            child: Some(child),
            stderr_buf,
            stderr_thread,
        }
    }

    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.child.as_mut().and_then(|c| c.try_wait().ok().flatten())
    }

    fn send_sigterm(&self) {
        if let Some(ref c) = self.child {
            #[cfg(unix)]
            {
                let _ = Command::new("kill")
                    .args(["-TERM", &c.id().to_string()])
                    .status();
            }
        }
    }

    fn wait_for_exit(mut self, timeout: Duration) -> std::process::ExitStatus {
        let mut child = self.child.take().expect("child already taken");
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                if let Some(t) = self.stderr_thread.take() {
                    let _ = t.join();
                }
                return status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let status = child.wait().expect("wait after kill");
                if let Some(t) = self.stderr_thread.take() {
                    let _ = t.join();
                }
                panic!("{} did not exit within {:?} (killed: {:?})", self.name, timeout, status);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn dump_stderr_with_sigquit(&mut self) -> String {
        if let Some(ref mut c) = self.child {
            if c.try_wait().ok().flatten().is_none() {
                #[cfg(unix)]
                {
                    let _ = Command::new("kill")
                        .args(["-QUIT", &c.id().to_string()])
                        .status();
                    std::thread::sleep(Duration::from_millis(200));
                }
                if c.try_wait().ok().flatten().is_none() {
                    let _ = c.kill();
                }
                let _ = c.wait();
            }
        }
        if let Some(t) = self.stderr_thread.take() {
            let _ = t.join();
        }
        self.stderr_buf
            .lock()
            .map(|g| String::from_utf8_lossy(&g).into_owned())
            .unwrap_or_default()
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Some(t) = self.stderr_thread.take() {
            let _ = t.join();
        }
    }
}

fn free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn with_tenant<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from(tenant).unwrap(),
    );
    req
}

async fn wait_for_ready(http: &reqwest::Client, http_port: u16, proc: &mut ProcessGuard) {
    let url = format!("http://127.0.0.1:{http_port}/ready");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = proc.try_wait() {
            let stderr = proc.dump_stderr_with_sigquit();
            panic!("{} exited prematurely with {status} before /ready:\n{stderr}", proc.name);
        }
        if let Ok(resp) = http.get(&url).send().await {
            if resp.status().as_u16() == 200 {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{} did not become /ready within 20s",
            proc.name
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn wire_enc_to_tsdb(enc: i32) -> Encoding {
    match enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire chunk encoding {other}"),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum DecodedVal {
    Float(i64, f64),
    IntHist(i64, i32, u64, f64, Vec<f64>),
    FloatHist(i64, i32, f64, f64, Vec<f64>),
}

#[derive(Debug, Clone, PartialEq)]
struct DecodedSeries {
    labels: Vec<(String, String)>,
    values: Vec<DecodedVal>,
}

async fn run_query_stream(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
    extra_md: &[(&'static str, String)],
) -> Vec<DecodedSeries> {
    let mut req = with_tenant(
        tenant,
        ingesterpb::QueryRequest {
            start_timestamp_ms: start_ms,
            end_timestamp_ms: end_ms,
            matchers,
            streaming_chunks_batch_size: 4,
        },
    );
    for (k, v) in extra_md {
        req.metadata_mut()
            .insert(*k, MetadataValue::try_from(v.as_str()).unwrap());
    }

    let mut stream = client.query_stream(req).await.unwrap().into_inner();
    let mut series_labels: Vec<Vec<(String, String)>> = Vec::new();
    let mut saw_end_of_series = false;
    let mut chunks_by_series: BTreeMap<u64, Vec<DecodedVal>> = BTreeMap::new();

    while let Some(item) = stream.next().await {
        let msg = item.unwrap();
        if !msg.streaming_series.is_empty() || msg.is_end_of_series_stream {
            assert!(
                !saw_end_of_series,
                "received series batch after is_end_of_series_stream"
            );
            for s in msg.streaming_series {
                let mut pairs: Vec<(String, String)> = s
                    .labels
                    .into_iter()
                    .map(|l| {
                        (
                            String::from_utf8_lossy(&l.name).into_owned(),
                            String::from_utf8_lossy(&l.value).into_owned(),
                        )
                    })
                    .collect();
                pairs.sort();
                series_labels.push(pairs);
            }
            if msg.is_end_of_series_stream {
                saw_end_of_series = true;
            }
        } else {
            assert!(saw_end_of_series, "received chunks before end of series stream");
            for sc in msg.streaming_series_chunks {
                let entry = chunks_by_series.entry(sc.series_index).or_default();
                for ch in sc.chunks {
                    let enc = wire_enc_to_tsdb(ch.encoding);
                    let chunk = EncodedChunk::from_bytes(enc, ch.data.to_vec()).unwrap();
                    for s in decode_chunk_samples(&chunk).unwrap() {
                        match s.value {
                            SampleValue::Float(v) => entry.push(DecodedVal::Float(s.t, v)),
                            SampleValue::Histogram(h) => entry.push(DecodedVal::IntHist(
                                s.t,
                                h.schema,
                                h.count,
                                h.sum,
                                h.custom_values,
                            )),
                            SampleValue::FloatHistogram(fh) => entry.push(DecodedVal::FloatHist(
                                s.t,
                                fh.schema,
                                fh.count,
                                fh.sum,
                                fh.custom_values,
                            )),
                        }
                    }
                }
            }
        }
    }
    assert!(saw_end_of_series);

    let mut out: Vec<DecodedSeries> = series_labels
        .into_iter()
        .enumerate()
        .map(|(idx, labels)| {
            let values = chunks_by_series.remove(&(idx as u64)).unwrap_or_default();
            DecodedSeries { labels, values }
        })
        .collect();
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    out
}

fn canonical_labels(labels: Vec<cortexpb::LabelPair>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = labels
        .into_iter()
        .map(|l| {
            (
                String::from_utf8_lossy(&l.name).into_owned(),
                String::from_utf8_lossy(&l.value).into_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

type LabelPairs = Vec<(String, String)>;
type ExemplarTuple = (i64, f64, LabelPairs);

#[derive(Debug, PartialEq)]
struct SnapshotAll14 {
    query_all: Vec<DecodedSeries>,
    exemplars: Vec<(LabelPairs, Vec<ExemplarTuple>)>,
    label_names: Vec<String>,
    label_values_job: Vec<String>,
    label_names_and_values: BTreeMap<String, Vec<String>>,
    label_values_card_job: BTreeMap<String, u64>,
    metrics_for_job_api: Vec<Vec<(String, String)>>,
    metadata_http: Vec<(i32, String, String, String)>,
    user_stats_in_mem: (u64, f64, f64, f64),
    user_stats_active: u64,
    all_user_stats: Vec<(String, u64)>,
    active_series: Vec<Vec<(String, String)>>,
    active_nh_series: Vec<(Vec<(String, String)>, u64)>,
    search_label_names: Vec<String>,
    search_label_values: Vec<String>,
}

async fn collect_all_read_rpcs(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    base_ts: i64,
) -> SnapshotAll14 {
    let all_matchers = vec![ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::RegexMatch as i32,
        name: "__name__".to_string(),
        value: ".+".to_string(),
    }];
    let query_all = run_query_stream(
        client,
        tenant,
        base_ts - 120_000,
        base_ts + 120_000,
        all_matchers,
        &[],
    )
    .await;

    let ex_resp = client
        .query_exemplars(with_tenant(
            tenant,
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: base_ts - 120_000,
                end_timestamp_ms: base_ts + 120_000,
                matchers: vec![ingesterpb::LabelMatchers {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::Equal as i32,
                        name: "__name__".to_string(),
                        value: "http_requests_total".to_string(),
                    }],
                }],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut exemplars: Vec<_> = ex_resp
        .timeseries
        .into_iter()
        .map(|ts| {
            let lbls = canonical_labels(ts.labels);
            let mut exs: Vec<_> = ts
                .exemplars
                .into_iter()
                .map(|e| (e.timestamp_ms, e.value, canonical_labels(e.labels)))
                .collect();
            exs.sort_by_key(|a| a.0);
            (lbls, exs)
        })
        .collect();
    exemplars.sort_by(|a, b| a.0.cmp(&b.0));

    let label_names = client
        .label_names(with_tenant(
            tenant,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: base_ts - 120_000,
                end_timestamp_ms: base_ts + 120_000,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .label_names;

    let label_values_job = client
        .label_values(with_tenant(
            tenant,
            ingesterpb::LabelValuesRequest {
                label_name: "job".to_string(),
                start_timestamp_ms: base_ts - 120_000,
                end_timestamp_ms: base_ts + 120_000,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .label_values;

    let mut lnv_stream = client
        .label_names_and_values(with_tenant(
            tenant,
            ingesterpb::LabelNamesAndValuesRequest {
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut label_names_and_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    while let Some(item) = lnv_stream.next().await {
        for lv in item.unwrap().items {
            label_names_and_values
                .entry(lv.label_name)
                .or_default()
                .extend(lv.values);
        }
    }
    for vals in label_names_and_values.values_mut() {
        vals.sort();
    }

    let mut lvc_stream = client
        .label_values_cardinality(with_tenant(
            tenant,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["job".to_string()],
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut label_values_card_job = BTreeMap::new();
    while let Some(item) = lvc_stream.next().await {
        for it in item.unwrap().items {
            if it.label_name == "job" {
                for (k, v) in it.label_value_series {
                    label_values_card_job.insert(k, v);
                }
            }
        }
    }

    let mflm = client
        .metrics_for_label_matchers(with_tenant(
            tenant,
            ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: base_ts - 120_000,
                end_timestamp_ms: base_ts + 120_000,
                matchers_set: vec![ingesterpb::LabelMatchers {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::Equal as i32,
                        name: "job".to_string(),
                        value: "api".to_string(),
                    }],
                }],
                limit: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut metrics_for_job_api: Vec<_> = mflm
        .metric
        .into_iter()
        .map(|m| canonical_labels(m.labels))
        .collect();
    metrics_for_job_api.sort();

    let mm = client
        .metrics_metadata(with_tenant(
            tenant,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                metric: String::new(),
                metric_names: vec!["http_requests_total".to_string()],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut metadata_http: Vec<_> = mm
        .metadata
        .into_iter()
        .map(|m| (m.r#type, m.metric_family_name, m.help, m.unit))
        .collect();
    metadata_http.sort();

    let us_mem = client
        .user_stats(with_tenant(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let us_act = client
        .user_stats(with_tenant(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();

    let all_us = client
        .all_user_stats(with_tenant(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut all_user_stats: Vec<_> = all_us
        .stats
        .into_iter()
        .map(|s| (s.user_id, s.data.map_or(0, |d| d.num_series)))
        .collect();
    all_user_stats.sort();

    let active_matchers = vec![ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::RegexMatch as i32,
        name: "__name__".to_string(),
        value: ".+".to_string(),
    }];

    let mut as_stream = client
        .active_series(with_tenant(
            tenant,
            ingesterpb::ActiveSeriesRequest {
                matchers: active_matchers.clone(),
                r#type: ingesterpb::active_series_request::RequestType::Series as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut active_series = Vec::new();
    while let Some(item) = as_stream.next().await {
        for m in item.unwrap().metric {
            active_series.push(canonical_labels(m.labels));
        }
    }
    active_series.sort();

    let mut nh_stream = client
        .active_series(with_tenant(
            tenant,
            ingesterpb::ActiveSeriesRequest {
                matchers: active_matchers,
                r#type: ingesterpb::active_series_request::RequestType::NativeHistogramSeries
                    as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut active_nh_series = Vec::new();
    while let Some(item) = nh_stream.next().await {
        let msg = item.unwrap();
        assert_eq!(msg.metric.len(), msg.bucket_count.len());
        for (m, bc) in msg.metric.into_iter().zip(msg.bucket_count) {
            active_nh_series.push((canonical_labels(m.labels), bc));
        }
    }
    active_nh_series.sort();

    let mut sln_stream = client
        .search_label_names(with_tenant(
            tenant,
            ingesterpb::SearchLabelNamesRequest {
                start_timestamp_ms: base_ts - 120_000,
                end_timestamp_ms: base_ts + 120_000,
                matchers: Vec::new(),
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec!["meth".to_string()],
                    case_insensitive: true,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                    fuzz_threshold: 0,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                limit: 10,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut search_label_names = Vec::new();
    while let Some(b) = sln_stream.next().await {
        for r in b.unwrap().results {
            search_label_names.push(r.value);
        }
    }

    let mut slv_stream = client
        .search_label_values(with_tenant(
            tenant,
            ingesterpb::SearchLabelValuesRequest {
                start_timestamp_ms: base_ts - 120_000,
                end_timestamp_ms: base_ts + 120_000,
                name: "__name__".to_string(),
                matchers: Vec::new(),
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec!["hrt".to_string()],
                    case_insensitive: false,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Subsequence as i32,
                    fuzz_threshold: 0,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByScoreDesc as i32,
                limit: 10,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut search_label_values = Vec::new();
    while let Some(b) = slv_stream.next().await {
        for r in b.unwrap().results {
            search_label_values.push(r.value);
        }
    }

    SnapshotAll14 {
        query_all,
        exemplars,
        label_names,
        label_values_job,
        label_names_and_values,
        label_values_card_job,
        metrics_for_job_api,
        metadata_http,
        user_stats_in_mem: (
            us_mem.num_series,
            us_mem.ingestion_rate,
            us_mem.api_ingestion_rate,
            us_mem.rule_ingestion_rate,
        ),
        user_stats_active: us_act.num_series,
        all_user_stats,
        active_series,
        active_nh_series,
        search_label_names,
        search_label_values,
    }
}

fn write_classic_ingester_yaml(
    path: &Path,
    http_port: u16,
    grpc_port: u16,
    tsdb_dir: &Path,
    bucket_dir: &Path,
) {
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  register_instrumentation: true
multitenancy_enabled: true
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    unregister_on_shutdown: true
  active_series_metrics_enabled: true
limits:
  out_of_order_time_window: 10m
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 10000
  max_global_series_per_user: 20
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 10m
    head_compaction_interval: 10m
activity_tracker:
  filepath: ""
"#,
        bucket_dir.display(),
        tsdb_dir.display()
    );
    fs::write(path, yaml).unwrap();
}

#[test]
fn twin_process_classic_push_all_14_rpcs_and_errors_parity() {
    ensure_go_oracle_built().expect("go-mimir and go-oracle must build");
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");

    let tmp = tempfile::tempdir().unwrap();
    let go_dir = tmp.path().join("go");
    let rust_dir = tmp.path().join("rust");
    fs::create_dir_all(go_dir.join("tsdb")).unwrap();
    fs::create_dir_all(go_dir.join("bucket")).unwrap();
    fs::create_dir_all(rust_dir.join("tsdb")).unwrap();
    fs::create_dir_all(rust_dir.join("bucket")).unwrap();

    let go_http_port = free_tcp_port();
    let go_grpc_port = free_tcp_port();
    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();

    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    write_classic_ingester_yaml(
        &go_yaml,
        go_http_port,
        go_grpc_port,
        &go_dir.join("tsdb"),
        &go_dir.join("bucket"),
    );
    write_classic_ingester_yaml(
        &rust_yaml,
        rust_http_port,
        rust_grpc_port,
        &rust_dir.join("tsdb"),
        &rust_dir.join("bucket"),
    );

    let mut go_proc = ProcessGuard::spawn(
        "go-mimir",
        &go_mimir_bin(),
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", go_yaml.display()),
        ],
        &go_dir,
    );
    let mut rust_proc = ProcessGuard::spawn(
        "rust-mimir",
        &rust_mimir_release_bin(),
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", rust_yaml.display()),
        ],
        &rust_dir,
    );

    block_on_for_tests(async {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        wait_for_ready(&http, go_http_port, &mut go_proc).await;
        wait_for_ready(&http, rust_http_port, &mut rust_proc).await;

        let go_ch = Channel::from_shared(format!("http://127.0.0.1:{go_grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let rust_ch = Channel::from_shared(format!("http://127.0.0.1:{rust_grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();

        let mut go_client = IngesterClient::new(go_ch.clone());
        let mut rust_client = IngesterClient::new(rust_ch.clone());

        let base_ts = now_ms() - 120_000;
        let tenant = "tenant-w4";

        // Multi-batch Push workload:
        // Batch 1: float samples (in-order + OOO), created_timestamp, integer histogram,
        //          float histogram, custom-buckets NHCB histogram, exemplars, metadata
        let batch1 = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            metadata: vec![
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    metric_family_name: "http_requests_total".to_string(),
                    help: "Total HTTP requests".to_string(),
                    unit: "requests".to_string(),
                },
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                    metric_family_name: "request_duration_seconds".to_string(),
                    help: "Request latency".to_string(),
                    unit: "seconds".to_string(),
                },
            ],
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                        cortexpb::LabelPair::from_static("job", "api"),
                        cortexpb::LabelPair::from_static("method", "GET"),
                    ],
                    created_timestamp: base_ts + 500,
                    samples: vec![
                        cortexpb::Sample {
                            value: 10.0,
                            timestamp_ms: base_ts + 1000,
                        },
                        cortexpb::Sample {
                            value: 25.0,
                            timestamp_ms: base_ts + 3000,
                        },
                        // Out-of-order sample within 10m window
                        cortexpb::Sample {
                            value: 15.0,
                            timestamp_ms: base_ts + 2000,
                        },
                    ],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "abc-1")],
                        value: 1.0,
                        timestamp_ms: base_ts + 3000,
                    }],
                    histograms: Vec::new(),
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                        cortexpb::LabelPair::from_static("job", "worker"),
                        cortexpb::LabelPair::from_static("method", "POST"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 5.0,
                        timestamp_ms: base_ts + 1000,
                    }],
                    ..Default::default()
                },
                // Integer native histogram
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "request_duration_seconds"),
                        cortexpb::LabelPair::from_static("job", "api"),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(5)),
                        sum: 1.5,
                        schema: 0,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![2, 0],
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts + 1000,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                // Float native histogram
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "float_latency_seconds"),
                        cortexpb::LabelPair::from_static("job", "api"),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountFloat(7.5)),
                        sum: 3.25,
                        schema: 1,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(2.5)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_counts: vec![3.0, 2.0],
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts + 1000,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                // Custom buckets native histogram (NHCB, schema = -53)
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "nhcb_latency_seconds"),
                        cortexpb::LabelPair::from_static("job", "api"),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(4)),
                        sum: 2.0,
                        schema: -53,
                        zero_threshold: 0.0,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![3, -2],
                        custom_values: vec![0.1, 0.5, 1.0],
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts + 1000,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        // Batch 2: additional sample + rule-source tenant
        let batch2 = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                    cortexpb::LabelPair::from_static("job", "api"),
                    cortexpb::LabelPair::from_static("method", "GET"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 30.0,
                    timestamp_ms: base_ts + 4000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch_rule = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Rule as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("job", "rule_eval"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: base_ts + 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        for req in [&batch1, &batch2] {
            go_client
                .push(with_tenant(tenant, req.clone()))
                .await
                .unwrap();
            rust_client
                .push(with_tenant(tenant, req.clone()))
                .await
                .unwrap();
        }
        go_client
            .push(with_tenant("tenant-rule", batch_rule.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant("tenant-rule", batch_rule.clone()))
            .await
            .unwrap();

        // Compare ALL read RPCs between Go Mimir and Rust mimir-rs!
        let go_snap = collect_all_read_rpcs(&mut go_client, tenant, base_ts).await;
        let rust_snap = collect_all_read_rpcs(&mut rust_client, tenant, base_ts).await;

        assert_eq!(rust_snap.query_all, go_snap.query_all, "QueryStream mismatch");
        assert_eq!(rust_snap.exemplars, go_snap.exemplars, "QueryExemplars mismatch");
        assert_eq!(rust_snap.label_names, go_snap.label_names, "LabelNames mismatch");
        assert_eq!(rust_snap.label_values_job, go_snap.label_values_job, "LabelValues mismatch");
        assert_eq!(
            rust_snap.label_names_and_values, go_snap.label_names_and_values,
            "LabelNamesAndValues mismatch"
        );
        assert_eq!(
            rust_snap.label_values_card_job, go_snap.label_values_card_job,
            "LabelValuesCardinality mismatch"
        );
        assert_eq!(
            rust_snap.metrics_for_job_api, go_snap.metrics_for_job_api,
            "MetricsForLabelMatchers mismatch"
        );
        assert_eq!(rust_snap.metadata_http, go_snap.metadata_http, "MetricsMetadata mismatch");
        assert_eq!(rust_snap.user_stats_in_mem.0, go_snap.user_stats_in_mem.0, "UserStats InMemory num_series mismatch");
        assert_eq!(rust_snap.user_stats_active, go_snap.user_stats_active, "UserStats Active mismatch");
        assert_eq!(rust_snap.all_user_stats, go_snap.all_user_stats, "AllUserStats mismatch");
        assert_eq!(rust_snap.active_series, go_snap.active_series, "ActiveSeries SERIES mismatch");
        assert_eq!(
            rust_snap.active_nh_series, go_snap.active_nh_series,
            "ActiveSeries NATIVE_HISTOGRAM_SERIES mismatch"
        );
        assert_eq!(
            rust_snap.search_label_names, go_snap.search_label_names,
            "SearchLabelNames mismatch"
        );
        assert_eq!(
            rust_snap.search_label_values, go_snap.search_label_values,
            "SearchLabelValues mismatch"
        );

        // Error & ErrorDetails parity tests against BOTH!
        let err_tenant = "tenant-err";
        // Seed one series so duplicate timestamp and too-old OOO can be tested
        let seed_ts = base_ts + 5000;
        let seed_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "err_metric"),
                    cortexpb::LabelPair::from_static("k", "v"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 10.0,
                    timestamp_ms: seed_ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client
            .push(with_tenant(err_tenant, seed_req.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant(err_tenant, seed_req))
            .await
            .unwrap();

        let err_cases = vec![
            // (a) Unsorted labels
            (
                "unsorted_labels",
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("z", "1"),
                            cortexpb::LabelPair::from_static("__name__", "bad"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: seed_ts + 1000,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
            // (b) Duplicate timestamp with different value
            (
                "duplicate_timestamp",
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "err_metric"),
                            cortexpb::LabelPair::from_static("k", "v"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 99.0,
                            timestamp_ms: seed_ts,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
            // (c) Sample too old / outside OOO window (10m window + 10m default past_grace_period -> 30m old is outside both!)
            (
                "sample_too_old",
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "err_metric"),
                            cortexpb::LabelPair::from_static("k", "v"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: seed_ts - 30 * 60 * 1000,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
        ];

        for (label, req) in err_cases {
            let go_err = go_client
                .push(with_tenant(err_tenant, req.clone()))
                .await
                .unwrap_err();
            let rust_err = rust_client
                .push(with_tenant(err_tenant, req))
                .await
                .unwrap_err();
            assert_eq!(
                rust_err.code(),
                go_err.code(),
                "{label}: gRPC status code mismatch (rust={:?}, go={:?})",
                rust_err,
                go_err
            );
            let go_det = grpcutilpb::extract_cortex_error_details(&go_err)
                .unwrap_or_else(|| panic!("{label}: Go missing ErrorDetails: {go_err:?}"));
            let rust_det = grpcutilpb::extract_cortex_error_details(&rust_err)
                .unwrap_or_else(|| panic!("{label}: Rust missing ErrorDetails: {rust_err:?}"));
            assert_eq!(rust_det.cause, go_det.cause, "{label}: ErrorDetails.cause mismatch");
            assert_eq!(rust_det.soft, go_det.soft, "{label}: ErrorDetails.soft mismatch");
            assert_eq!(
                rust_det.rejected_samples, go_det.rejected_samples,
                "{label}: ErrorDetails.rejected_samples mismatch"
            );
        }

        // (d) Per-user series limit (max_global_series_per_user: 20)
        let mut limit_series = Vec::new();
        for idx in 0..20 {
            limit_series.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "lim_series"),
                    cortexpb::LabelPair::from_strs("idx", idx.to_string()),
                ],
                samples: vec![cortexpb::Sample {
                    value: idx as f64,
                    timestamp_ms: seed_ts,
                }],
                ..Default::default()
            });
        }
        let lim_fill = cortexpb::WriteRequest {
            timeseries: limit_series,
            ..Default::default()
        };
        go_client
            .push(with_tenant("tenant-limit", lim_fill.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant("tenant-limit", lim_fill))
            .await
            .unwrap();

        let lim_over = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "lim_series"),
                    cortexpb::LabelPair::from_static("idx", "overflow"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 999.0,
                    timestamp_ms: seed_ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let go_lim_err = go_client
            .push(with_tenant("tenant-limit", lim_over.clone()))
            .await
            .unwrap_err();
        let rust_lim_err = rust_client
            .push(with_tenant("tenant-limit", lim_over))
            .await
            .unwrap_err();
        assert_eq!(rust_lim_err.code(), go_lim_err.code(), "series limit code mismatch");
        let go_lim_det = grpcutilpb::extract_cortex_error_details(&go_lim_err).unwrap();
        let rust_lim_det = grpcutilpb::extract_cortex_error_details(&rust_lim_err).unwrap();
        assert_eq!(rust_lim_det.cause, go_lim_det.cause, "series limit cause mismatch");
        assert_eq!(rust_lim_det.soft, go_lim_det.soft, "series limit soft mismatch");
        assert_eq!(
            rust_lim_det.rejected_samples, go_lim_det.rejected_samples,
            "series limit rejected_samples mismatch"
        );

        // HTTP endpoint checks on BOTH
        for (name, http_port, grpc_ch) in [
            ("go", go_http_port, go_ch),
            ("rust", rust_http_port, rust_ch),
        ] {
            let ready = http
                .get(format!("http://127.0.0.1:{http_port}/ready"))
                .send()
                .await
                .unwrap();
            assert_eq!(ready.status().as_u16(), 200, "{name} /ready");

            let metrics_txt = http
                .get(format!("http://127.0.0.1:{http_port}/metrics"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            for metric_name in [
                "cortex_config_hash",
                "mimir_build_info",
                "cortex_build_info",
                "cortex_ingester_ingested_samples_total",
                "cortex_ingester_memory_series",
            ] {
                assert!(
                    metrics_txt.contains(metric_name),
                    "{name} /metrics missing {metric_name}"
                );
            }

            let buildinfo: serde_json::Value = http
                .get(format!("http://127.0.0.1:{http_port}/api/v1/status/buildinfo"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(buildinfo["status"], "success", "{name} buildinfo status");
            assert_eq!(
                buildinfo["data"]["application"], "Grafana Mimir",
                "{name} buildinfo application"
            );

            let ring_json: serde_json::Value = http
                .get(format!("http://127.0.0.1:{http_port}/ingester/ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(ring_json["shards"][0]["id"], "ingester-0", "{name} /ingester/ring id");
            assert_eq!(ring_json["shards"][0]["state"], "ACTIVE", "{name} /ingester/ring state");

            let tenants_html = http
                .get(format!("http://127.0.0.1:{http_port}/ingester/tenants"))
                .send()
                .await
                .unwrap();
            assert_eq!(tenants_html.status().as_u16(), 200, "{name} /ingester/tenants");
            let tenants_body = tenants_html.text().await.unwrap();
            assert!(
                tenants_body.contains(tenant),
                "{name} /ingester/tenants HTML should list {tenant}"
            );

            let mut httpgrpc_client = HttpClient::new(grpc_ch);
            let hresp = httpgrpc_client
                .handle(with_tenant(
                    tenant,
                    HttpRequest {
                        method: "GET".to_string(),
                        url: "/api/v1/status/buildinfo".to_string(),
                        headers: vec![HttpGrpcHeader {
                            key: "Accept".to_string(),
                            values: vec!["application/json".to_string()],
                        }],
                        body: Default::default(),
                    },
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(hresp.code, 200, "{name} httpgrpc status");
            let hjson: serde_json::Value = serde_json::from_slice(&hresp.body).unwrap();
            assert_eq!(hjson["status"], "success", "{name} httpgrpc buildinfo");
        }

        // Flush + upload ULID blocks on BOTH, then cross-verify!
        for (name, http_port) in [("go", go_http_port), ("rust", rust_http_port)] {
            let flush = tokio::time::timeout(
                Duration::from_secs(60),
                http.post(format!("http://127.0.0.1:{http_port}/ingester/flush?wait=true"))
                    .send(),
            )
            .await
            .unwrap_or_else(|_| {
                let go_err = go_proc.dump_stderr_with_sigquit();
                let rust_err = rust_proc.dump_stderr_with_sigquit();
                panic!("{name} /ingester/flush?wait=true timed out!\nGO stderr:\n{go_err}\nRUST stderr:\n{rust_err}");
            })
            .unwrap();
            assert_eq!(flush.status().as_u16(), 204, "{name} /ingester/flush?wait=true");
        }

        // Verify Go target/go-oracle opens Rust's uploaded block cleanly
        let rust_tenant_bucket = rust_dir.join("bucket").join(tenant);
        assert!(rust_tenant_bucket.exists(), "rust uploaded bucket dir must exist");
        fs::create_dir_all(rust_tenant_bucket.join("wal")).unwrap();
        let go_opened_rust: VerifyTsdbDirResponse = invoke_oracle(
            "verify-tsdb-dir",
            &VerifyTsdbDirRequest {
                dir: rust_tenant_bucket.to_string_lossy().into_owned(),
                read_only: true,
            },
        )
        .expect("Go prometheus/tsdb must open Rust uploaded block");
        assert!(
            !go_opened_rust.block_ulids.is_empty(),
            "Go must see >= 1 uploaded block from Rust"
        );
        assert_eq!(go_opened_rust.series.len(), 5);

        // Verify Rust Tsdb::open opens Go's uploaded block cleanly
        let go_tenant_bucket = go_dir.join("bucket").join(tenant);
        assert!(go_tenant_bucket.exists(), "go uploaded bucket dir must exist");
        let rust_opened_go = Tsdb::open(&go_tenant_bucket, TsdbOptions::default())
            .expect("Rust Tsdb::open must open Go uploaded block");
        assert!(
            !rust_opened_go.block_ulids().is_empty(),
            "Rust must see >= 1 uploaded block from Go"
        );
        let q = rust_opened_go.querier(i64::MIN, i64::MAX);
        let all_m = TsdbLabelMatcher::new(TsdbMatchType::RegexMatch, "__name__", ".+").unwrap();
        let rust_series = q.select(true, &[all_m]).unwrap();
        assert_eq!(rust_series.len(), 5);
    });
}

#[test]
fn mixed_cluster_go_and_rust_ingesters_memberlist_ring() {
    ensure_go_oracle_built().expect("go-mimir must build");
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");

    let tmp = tempfile::tempdir().unwrap();
    let go_dir = tmp.path().join("go");
    let rust_dir = tmp.path().join("rust");
    fs::create_dir_all(go_dir.join("tsdb")).unwrap();
    fs::create_dir_all(go_dir.join("bucket")).unwrap();
    fs::create_dir_all(rust_dir.join("tsdb")).unwrap();
    fs::create_dir_all(rust_dir.join("bucket")).unwrap();

    let go_http_port = free_tcp_port();
    let go_grpc_port = free_tcp_port();
    let go_ml_port = free_tcp_port();

    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();
    let rust_ml_port = free_tcp_port();

    let mk_yaml = |http_p: u16,
                   grpc_p: u16,
                   ml_p: u16,
                   join_p: u16,
                   id: &str,
                   dir: &Path| {
        format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_p}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_p}
multitenancy_enabled: true
memberlist:
  node_name: {id}
  randomize_node_name: false
  bind_addr:
    - 127.0.0.1
  bind_port: {ml_p}
  advertise_addr: 127.0.0.1
  advertise_port: {ml_p}
  join_members:
    - 127.0.0.1:{join_p}
  abort_if_cluster_join_fails: false
ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: {id}
    instance_addr: 127.0.0.1
    num_tokens: 16
    min_ready_duration: 0s
    replication_factor: 2
    unregister_on_shutdown: true
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
            dir.join("bucket").display(),
            dir.join("tsdb").display()
        )
    };

    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    fs::write(
        &go_yaml,
        mk_yaml(
            go_http_port,
            go_grpc_port,
            go_ml_port,
            rust_ml_port,
            "go-ingester-0",
            &go_dir,
        ),
    )
    .unwrap();
    fs::write(
        &rust_yaml,
        mk_yaml(
            rust_http_port,
            rust_grpc_port,
            rust_ml_port,
            go_ml_port,
            "rust-ingester-1",
            &rust_dir,
        ),
    )
    .unwrap();

    let mut go_proc = ProcessGuard::spawn(
        "go-mimir",
        &go_mimir_bin(),
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", go_yaml.display()),
        ],
        &go_dir,
    );
    let mut rust_proc = ProcessGuard::spawn(
        "rust-mimir",
        &rust_mimir_release_bin(),
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", rust_yaml.display()),
        ],
        &rust_dir,
    );

    block_on_for_tests(async {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        wait_for_ready(&http, go_http_port, &mut go_proc).await;
        wait_for_ready(&http, rust_http_port, &mut rust_proc).await;

        // Poll GET /ingester/ring on BOTH until BOTH see 2 ACTIVE ingesters!
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let go_resp = http
                .get(format!("http://127.0.0.1:{go_http_port}/ingester/ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap();
            let go_status = go_resp.status();
            let go_text = go_resp.text().await.unwrap();
            let go_ring: serde_json::Value = serde_json::from_str(&go_text).unwrap_or_else(|e| {
                panic!("go /ingester/ring status={go_status} body={go_text:?} err={e}")
            });
            let rust_resp = http
                .get(format!("http://127.0.0.1:{rust_http_port}/ingester/ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap();
            let rust_status = rust_resp.status();
            let rust_text = rust_resp.text().await.unwrap();
            let rust_ring: serde_json::Value = serde_json::from_str(&rust_text).unwrap_or_else(|e| {
                panic!("rust /ingester/ring status={rust_status} body={rust_text:?} err={e}")
            });

            let active_ids = |v: &serde_json::Value| -> Vec<String> {
                let mut ids: Vec<String> = v["shards"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|s| s["state"] == "ACTIVE")
                    .filter_map(|s| s["id"].as_str().map(str::to_string))
                    .collect();
                ids.sort();
                ids
            };
            let go_ids = active_ids(&go_ring);
            let rust_ids = active_ids(&rust_ring);
            let expected = vec!["go-ingester-0".to_string(), "rust-ingester-1".to_string()];
            if go_ids == expected && rust_ids == expected {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "rings did not converge to 2 ACTIVE ingesters: go={go_ids:?}, rust={rust_ids:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Push & query on both
        let go_ch = Channel::from_shared(format!("http://127.0.0.1:{go_grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let rust_ch = Channel::from_shared(format!("http://127.0.0.1:{rust_grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut go_client = IngesterClient::new(go_ch);
        let mut rust_client = IngesterClient::new(rust_ch);

        let ts = now_ms() - 5_000;
        let tenant = "mixed-tenant";
        let req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "mixed_cluster_metric"),
                    cortexpb::LabelPair::from_static("cluster", "go-rust"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 42.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client
            .push(with_tenant(tenant, req.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant(tenant, req))
            .await
            .unwrap();

        let matcher = vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "mixed_cluster_metric".to_string(),
        }];
        let go_q = run_query_stream(&mut go_client, tenant, ts - 1000, ts + 1000, matcher.clone(), &[]).await;
        let rust_q = run_query_stream(&mut rust_client, tenant, ts - 1000, ts + 1000, matcher, &[]).await;
        assert_eq!(rust_q, go_q);

        // Gracefully shut down Rust via POST /ingester/shutdown:
        // matches Go Mimir 1:1 by stopping the ingester + leaving the ring + returning 204
        // while keeping the HTTP server alive (GET /ready == 503) until SIGTERM.
        let sd = http
            .post(format!("http://127.0.0.1:{rust_http_port}/ingester/shutdown"))
            .send()
            .await
            .unwrap();
        assert_eq!(sd.status().as_u16(), 204);

        // Go ring should observe rust-ingester-1 unregistered cleanly
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let go_ring: serde_json::Value = http
                .get(format!("http://127.0.0.1:{go_http_port}/ingester/ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let has_rust = go_ring["shards"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|s| s["id"] == "rust-ingester-1");
            if !has_rust {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "go-mimir ring still has rust-ingester-1 after graceful shutdown: {go_ring}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Rust HTTP server remains alive with GET /ready == 503 until SIGTERM
        let ready_after_shutdown = http
            .get(format!("http://127.0.0.1:{rust_http_port}/ready"))
            .send()
            .await
            .unwrap();
        assert_eq!(ready_after_shutdown.status().as_u16(), 503);

        rust_proc.send_sigterm();
        let status = rust_proc.wait_for_exit(Duration::from_secs(15));
        assert_eq!(status.code(), Some(0), "rust-mimir shutdown exit status");
    });
}

#[test]
fn twin_process_kafka_ingest_storage_parity() {
    ensure_go_oracle_built().expect("go-mimir must build");
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");

    let base_ts = now_ms() - 30_000;
    let tenant = "tenant-kfake-w4";

    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-w4".to_string(),
        partitions: 1,
        compression: Some("snappy".to_string()),
        records: vec![
            KfakeRecordInput {
                partition: 0,
                tenant: tenant.to_string(),
                version: 2,
                compression: None,
                write_request: WriteRequestJson {
                    mode: "rw2".to_string(),
                    source: "API".to_string(),
                    skip_label_validation: false,
                    skip_label_count_validation: false,
                    timeseries: vec![
                        TimeSeriesJson {
                            labels: vec![
                                LabelJson {
                                    name: "__name__".to_string(),
                                    value: "kafka_samples_total".to_string(),
                                },
                                LabelJson {
                                    name: "service".to_string(),
                                    value: "auth".to_string(),
                                },
                            ],
                            samples: vec![
                                SampleJson {
                                    timestamp_ms: base_ts + 1000,
                                    value: 100.0,
                                },
                                SampleJson {
                                    timestamp_ms: base_ts + 2000,
                                    value: 150.0,
                                },
                            ],
                            exemplars: vec![ExemplarJson {
                                labels: vec![LabelJson {
                                    name: "trace_id".to_string(),
                                    value: "kafka-tr-1".to_string(),
                                }],
                                value: 0.42,
                                timestamp_ms: base_ts + 2000,
                            }],
                            ..Default::default()
                        },
                        TimeSeriesJson {
                            labels: vec![
                                LabelJson {
                                    name: "__name__".to_string(),
                                    value: "kafka_hist_seconds".to_string(),
                                },
                                LabelJson {
                                    name: "service".to_string(),
                                    value: "auth".to_string(),
                                },
                            ],
                            histograms: vec![HistogramJson {
                                timestamp_ms: base_ts + 2000,
                                schema: 0,
                                zero_threshold: 0.001,
                                zero_count: 1,
                                count: 4,
                                sum: 1.25,
                                reset_hint: 0,
                                is_float: false,
                                positive_spans: vec![mimir_conformance::oracle::SpanJson {
                                    offset: 0,
                                    length: 2,
                                }],
                                positive_deltas: vec![2, -1],
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                    ],
                    metadata: vec![MetadataJson {
                        metric_type: "COUNTER".to_string(),
                        metric_family_name: "kafka_samples_total".to_string(),
                        help: "Kafka ingested samples".to_string(),
                        unit: String::new(),
                    }],
                },
            },
            KfakeRecordInput {
                partition: 0,
                tenant: tenant.to_string(),
                version: 2,
                compression: None,
                write_request: WriteRequestJson {
                    mode: "rw2".to_string(),
                    source: "API".to_string(),
                    skip_label_validation: false,
                    skip_label_count_validation: false,
                    timeseries: vec![TimeSeriesJson {
                        labels: vec![
                            LabelJson {
                                name: "__name__".to_string(),
                                value: "kafka_samples_total".to_string(),
                            },
                            LabelJson {
                                name: "service".to_string(),
                                value: "auth".to_string(),
                            },
                        ],
                        samples: vec![SampleJson {
                            timestamp_ms: base_ts + 3000,
                            value: 200.0,
                        }],
                        ..Default::default()
                    }],
                    metadata: Vec::new(),
                },
            },
        ],
    })
    .expect("spawn kfake broker");

    let last_offset = broker.offsets()[0].end_offset - 1;
    assert_eq!(last_offset, 1);

    let tmp = tempfile::tempdir().unwrap();
    let go_dir = tmp.path().join("go");
    let rust_dir = tmp.path().join("rust");
    fs::create_dir_all(go_dir.join("tsdb")).unwrap();
    fs::create_dir_all(go_dir.join("bucket")).unwrap();
    fs::create_dir_all(rust_dir.join("tsdb")).unwrap();
    fs::create_dir_all(rust_dir.join("bucket")).unwrap();

    let go_http_port = free_tcp_port();
    let go_grpc_port = free_tcp_port();
    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();

    let mk_kafka_yaml = |http_p: u16, grpc_p: u16, dir: &Path| {
        format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_p}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_p}
multitenancy_enabled: true
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
  push_grpc_method_enabled: false
ingest_storage:
  enabled: true
  kafka:
    address: {}
    topic: {}
    consume_from_position_at_startup: start
    consumer_group_offset_commit_interval: 50ms
    last_produced_offset_poll_interval: 25ms
limits:
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 1000
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
            broker.broker_addr(),
            broker.topic(),
            dir.join("bucket").display(),
            dir.join("tsdb").display()
        )
    };

    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    fs::write(&go_yaml, mk_kafka_yaml(go_http_port, go_grpc_port, &go_dir)).unwrap();
    fs::write(
        &rust_yaml,
        mk_kafka_yaml(rust_http_port, rust_grpc_port, &rust_dir),
    )
    .unwrap();

    let mut go_proc = ProcessGuard::spawn(
        "go-mimir-kafka",
        &go_mimir_bin(),
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", go_yaml.display()),
        ],
        &go_dir,
    );
    let mut rust_proc = ProcessGuard::spawn(
        "rust-mimir-kafka",
        &rust_mimir_release_bin(),
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", rust_yaml.display()),
        ],
        &rust_dir,
    );

    block_on_for_tests(async {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        wait_for_ready(&http, go_http_port, &mut go_proc).await;
        wait_for_ready(&http, rust_http_port, &mut rust_proc).await;

        let go_ch = Channel::from_shared(format!("http://127.0.0.1:{go_grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let rust_ch = Channel::from_shared(format!("http://127.0.0.1:{rust_grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut go_client = IngesterClient::new(go_ch);
        let mut rust_client = IngesterClient::new(rust_ch);

        let consistency_md = [
            ("__consistency_level__", "strong".to_string()),
            ("__consistency_offsets__", format!("v1=0:{last_offset}")),
        ];
        let all_matchers = vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::RegexMatch as i32,
            name: "__name__".to_string(),
            value: ".+".to_string(),
        }];

        let go_q = run_query_stream(
            &mut go_client,
            tenant,
            base_ts - 10_000,
            base_ts + 20_000,
            all_matchers.clone(),
            &consistency_md,
        )
        .await;
        let rust_q = run_query_stream(
            &mut rust_client,
            tenant,
            base_ts - 10_000,
            base_ts + 20_000,
            all_matchers,
            &consistency_md,
        )
        .await;
        assert_eq!(rust_q, go_q, "Kafka QueryStream mismatch");
        assert_eq!(rust_q.len(), 2);

        let go_ln = go_client
            .label_names(with_tenant(
                tenant,
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: base_ts - 10_000,
                    end_timestamp_ms: base_ts + 20_000,
                    matchers: None,
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .label_names;
        let rust_ln = rust_client
            .label_names(with_tenant(
                tenant,
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: base_ts - 10_000,
                    end_timestamp_ms: base_ts + 20_000,
                    matchers: None,
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .label_names;
        assert_eq!(rust_ln, go_ln, "Kafka LabelNames mismatch");

        let go_lv = go_client
            .label_values(with_tenant(
                tenant,
                ingesterpb::LabelValuesRequest {
                    label_name: "__name__".to_string(),
                    start_timestamp_ms: base_ts - 10_000,
                    end_timestamp_ms: base_ts + 20_000,
                    matchers: None,
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .label_values;
        let rust_lv = rust_client
            .label_values(with_tenant(
                tenant,
                ingesterpb::LabelValuesRequest {
                    label_name: "__name__".to_string(),
                    start_timestamp_ms: base_ts - 10_000,
                    end_timestamp_ms: base_ts + 20_000,
                    matchers: None,
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .label_values;
        assert_eq!(rust_lv, go_lv, "Kafka LabelValues mismatch");

        let mk_ex_req = || {
            with_tenant(
                tenant,
                ingesterpb::ExemplarQueryRequest {
                    start_timestamp_ms: base_ts - 10_000,
                    end_timestamp_ms: base_ts + 20_000,
                    matchers: vec![ingesterpb::LabelMatchers {
                        matchers: vec![ingesterpb::LabelMatcher {
                            r#type: ingesterpb::MatchType::Equal as i32,
                            name: "__name__".to_string(),
                            value: "kafka_samples_total".to_string(),
                        }],
                    }],
                },
            )
        };
        let go_ex = go_client
            .query_exemplars(mk_ex_req())
            .await
            .unwrap()
            .into_inner();
        let rust_ex = rust_client
            .query_exemplars(mk_ex_req())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rust_ex.timeseries.len(), go_ex.timeseries.len());
        assert_eq!(
            rust_ex.timeseries[0].exemplars,
            go_ex.timeseries[0].exemplars,
            "Kafka QueryExemplars mismatch"
        );
    });
}
