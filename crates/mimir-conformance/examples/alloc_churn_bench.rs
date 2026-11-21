//! Multi-wave ephemeral tenant lifecycle & allocator heap-retention benchmark.
//!
//! Run via:
//!   just alloc-churn-bench --waves 4 --tenants 25 --series-per-tenant 6000

use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{ensure_rust_mimir_release_built, rust_mimir_release_bin};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

#[derive(Clone, Copy, Debug, Default)]
struct MemSnap {
    cpu_secs: f64,
    vm_rss_mib: f64,
    vm_hwm_mib: f64,
    vm_size_mib: f64,
    rss_anon_mib: f64,
    pss_mib: f64,
    private_dirty_mib: f64,
}

fn read_mem_snap(pid: u32) -> MemSnap {
    let mut s = MemSnap::default();
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        if let Some(rparen) = stat.rfind(')') {
            let fields: Vec<&str> = stat[rparen + 2..].split_whitespace().collect();
            if fields.len() > 12 {
                let utime: u64 = fields[11].parse().unwrap_or(0);
                let stime: u64 = fields[12].parse().unwrap_or(0);
                s.cpu_secs = (utime + stime) as f64 / 100.0;
            }
        }
    }
    if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) {
        for line in status.lines() {
            let mut parts = line.split_whitespace();
            let Some(k) = parts.next() else { continue };
            let Some(v) = parts.next().and_then(|x| x.parse::<f64>().ok()) else { continue };
            let mib = v / 1024.0;
            match k {
                "VmRSS:" => s.vm_rss_mib = mib,
                "VmHWM:" => s.vm_hwm_mib = mib,
                "VmSize:" => s.vm_size_mib = mib,
                "RssAnon:" => s.rss_anon_mib = mib,
                _ => {}
            }
        }
    }
    if let Ok(smaps) = fs::read_to_string(format!("/proc/{pid}/smaps_rollup")) {
        for line in smaps.lines() {
            let mut parts = line.split_whitespace();
            let Some(k) = parts.next() else { continue };
            let Some(v) = parts.next().and_then(|x| x.parse::<f64>().ok()) else { continue };
            let mib = v / 1024.0;
            match k {
                "Pss:" => s.pss_mib = mib,
                "Private_Dirty:" => s.private_dirty_mib = mib,
                _ => {}
            }
        }
    }
    s
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct ChildGuard(Option<Child>);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

async fn scrape_metric(http: &reqwest::Client, http_port: u16, metric_name: &str) -> f64 {
    let Ok(resp) = http.get(format!("http://127.0.0.1:{http_port}/metrics")).send().await else {
        return -1.0;
    };
    let Ok(text) = resp.text().await else { return -1.0 };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(metric_name) {
            if rest.starts_with(' ') || rest.starts_with('{') {
                if let Some(val_str) = line.split_whitespace().nth(1) {
                    if let Ok(v) = val_str.parse::<f64>() {
                        return v;
                    }
                }
            }
        }
    }
    0.0
}

fn build_wave_batches(
    wave: usize,
    tenants: usize,
    float_per_tenant: usize,
    hist_per_tenant: usize,
    batch_size: usize,
    base_ts: i64,
) -> Vec<(String, cortexpb::WriteRequest)> {
    let mut out = Vec::new();
    let metrics = [
        "http_server_requests_total",
        "grpc_server_handled_total",
        "container_cpu_usage_seconds_total",
        "node_network_receive_bytes_total",
        "kube_pod_container_resource_limits",
    ];
    for t in 0..tenants {
        let tenant = format!("ephem-w{wave}-t{t:02}");
        let mut s = 0usize;
        while s < float_per_tenant {
            let end = (s + batch_size).min(float_per_tenant);
            let mut timeseries = Vec::with_capacity(end - s);
            for k in s..end {
                let pad_len = (k * 13 + wave * 7 + t * 11) % 96;
                let pad = "x".repeat(pad_len);
                let pod = format!("pod-w{wave}-t{t}-{k:05}-{pad}");
                let instance = format!("10.{wave}.{t}.{}:8080", k & 0xff);
                let ns = format!("ns-{}-{}", t % 8, k % 16);
                let labels = vec![
                    cortexpb::LabelPair::from_static("__name__", metrics[k % metrics.len()]),
                    cortexpb::LabelPair::from_static("cluster", "prod-eu-central-1"),
                    cortexpb::LabelPair::from_strs("instance", &instance),
                    cortexpb::LabelPair::from_strs("namespace", &ns),
                    cortexpb::LabelPair::from_strs("pod", &pod),
                ];
                let exemplars = if k.is_multiple_of(25) {
                    vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_strs(
                            "trace_id",
                            format!("trace-{wave}-{t}-{k:06}"),
                        )],
                        value: 0.042,
                        timestamp_ms: base_ts,
                    }]
                } else {
                    Vec::new()
                };
                timeseries.push(cortexpb::TimeSeries {
                    labels,
                    samples: vec![cortexpb::Sample {
                        value: (k as f64) * 1.25,
                        timestamp_ms: base_ts,
                    }],
                    exemplars,
                    histograms: Vec::new(),
                    created_timestamp: 0,
                });
            }
            out.push((
                tenant.clone(),
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    timeseries,
                    ..Default::default()
                },
            ));
            s = end;
        }

        let mut h = 0usize;
        while h < hist_per_tenant {
            let end = (h + batch_size).min(hist_per_tenant);
            let mut timeseries = Vec::with_capacity(end - h);
            for k in h..end {
                let span_len = 3 + ((k % 6) as u32);
                let deltas: Vec<i64> = (0..span_len).map(|i| if i == 0 { 4 } else { 1 }).collect();
                let total_cnt: u64 = 2 + (0..span_len as u64).map(|i| 4 + i).sum::<u64>();
                let pod = format!("hpod-w{wave}-t{t}-{k:04}");
                let labels = vec![
                    cortexpb::LabelPair::from_static("__name__", "http_request_duration_seconds"),
                    cortexpb::LabelPair::from_static("cluster", "prod-eu-central-1"),
                    cortexpb::LabelPair::from_strs("pod", &pod),
                ];
                timeseries.push(cortexpb::TimeSeries {
                    labels,
                    samples: Vec::new(),
                    exemplars: Vec::new(),
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(total_cnt)),
                        sum: 9.75,
                        schema: 2,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: span_len,
                        }],
                        positive_deltas: deltas,
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts,
                        ..Default::default()
                    }],
                    created_timestamp: 0,
                });
            }
            out.push((
                tenant.clone(),
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    timeseries,
                    ..Default::default()
                },
            ));
            h = end;
        }
    }
    out
}

#[tokio::main]
async fn main() {
    let mut bin_path: Option<PathBuf> = None;
    let mut label = "mimir-rs".to_string();
    let mut waves = 4usize;
    let mut tenants = 25usize;
    let mut series_per_tenant = 6_000usize;
    let rounds = 5usize;
    let concurrency = 16usize;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bin" => {
                i += 1;
                bin_path = Some(PathBuf::from(&args[i]));
            }
            "--label" => {
                i += 1;
                label = args[i].clone();
            }
            "--waves" => {
                i += 1;
                waves = args[i].parse().unwrap();
            }
            "--tenants" => {
                i += 1;
                tenants = args[i].parse().unwrap();
            }
            "--series-per-tenant" => {
                i += 1;
                series_per_tenant = args[i].parse().unwrap();
            }
            other => panic!("unknown arg: {other}"),
        }
        i += 1;
    }
    let bin = match bin_path {
        Some(p) => p,
        None => {
            ensure_rust_mimir_release_built().expect("target/release/mimir must build");
            rust_mimir_release_bin()
        }
    };
    let hist_per_tenant = series_per_tenant / 6;
    let float_per_tenant = series_per_tenant - hist_per_tenant;

    let tmp = tempfile::tempdir().unwrap();
    let tsdb_dir = tmp.path().join("tsdb");
    let bucket_dir = tmp.path().join("bucket");
    fs::create_dir_all(&tsdb_dir).unwrap();
    fs::create_dir_all(&bucket_dir).unwrap();
    let http_port = free_port();
    let grpc_port = free_port();
    let yaml_path = tmp.path().join("mimir.yaml");
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  grpc_server_max_recv_msg_size: 104857600
  grpc_server_max_send_msg_size: 104857600
  grpc_server_max_concurrent_streams: 1000
  log_level: warn
  register_instrumentation: true
multitenancy_enabled: true
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-churn-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    unregister_on_shutdown: true
  active_series_metrics_enabled: true
limits:
  out_of_order_time_window: 10m
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 100000
  max_global_series_per_user: 0
  ingestion_rate: 10000000
  ingestion_burst_size: 10000000
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    retention_period: 1s
    ship_interval: 500ms
    head_compaction_interval: 500ms
    head_compaction_idle_timeout: 500ms
activity_tracker:
  filepath: ""
"#,
        bucket_dir.display(),
        tsdb_dir.display()
    );
    fs::write(&yaml_path, yaml).unwrap();

    let child = Command::new(&bin)
        .args([
            "-target=ingester",
            &format!("-config.file={}", yaml_path.display()),
        ])
        .current_dir(tmp.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mimir");
    let pid = child.id();
    let _guard = ChildGuard(Some(child));

    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(r) = http.get(format!("http://127.0.0.1:{http_port}/ready")).send().await {
            if r.status().is_success() {
                break;
            }
        }
        assert!(Instant::now() < deadline, "ingester failed to become ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let boot = read_mem_snap(pid);
    println!("=== Allocator Churn Benchmark: {label} ({}) ===", bin.display());
    println!(
        "Boot idle (0 series): RssAnon={:.2} MiB  PrivateDirty={:.2} MiB  Pss={:.2} MiB  VmRSS={:.2} MiB  VmSize={:.2} MiB",
        boot.rss_anon_mib, boot.private_dirty_mib, boot.pss_mib, boot.vm_rss_mib, boot.vm_size_mib
    );

    let mut channels = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        channels.push(ch);
    }

    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

    for w in 1..=waves {
        let wave_base_ts = now_ms - ((waves - w + 2) as i64) * 300_000;
        let templates = Arc::new(build_wave_batches(
            w,
            tenants,
            float_per_tenant,
            hist_per_tenant,
            500,
            wave_base_ts,
        ));

        let wave_start = Instant::now();
        for r in 0..rounds {
            let ts = wave_base_ts + (r as i64) * 15_000;
            let cursor = Arc::new(AtomicUsize::new(0));
            let mut joins = Vec::with_capacity(concurrency);
            for ch in &channels {
                let mut client = IngesterClient::new(ch.clone())
                    .max_decoding_message_size(128 * 1024 * 1024)
                    .max_encoding_message_size(128 * 1024 * 1024);
                let templates = Arc::clone(&templates);
                let cursor = Arc::clone(&cursor);
                joins.push(tokio::spawn(async move {
                    loop {
                        let idx = cursor.fetch_add(1, Ordering::Relaxed);
                        if idx >= templates.len() {
                            break;
                        }
                        let (ref tenant, ref tmpl) = templates[idx];
                        let mut req_msg = tmpl.clone();
                        for series in &mut req_msg.timeseries {
                            for s in &mut series.samples {
                                s.timestamp_ms = ts;
                                s.value += r as f64;
                            }
                            for ex in &mut series.exemplars {
                                ex.timestamp_ms = ts;
                            }
                            for h in &mut series.histograms {
                                h.timestamp = ts;
                            }
                        }
                        let mut req = tonic::Request::new(req_msg);
                        req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant.as_str()).unwrap(),
                        );
                        client.push(req).await.expect("push ok");
                    }
                }));
            }
            for j in joins {
                j.await.unwrap();
            }
        }

        let q_cursor = Arc::new(AtomicUsize::new(0));
        let mut q_joins = Vec::with_capacity(concurrency);
        for ch in &channels {
            let mut client = IngesterClient::new(ch.clone())
                .max_decoding_message_size(128 * 1024 * 1024)
                .max_encoding_message_size(128 * 1024 * 1024);
            let q_cursor = Arc::clone(&q_cursor);
            q_joins.push(tokio::spawn(async move {
                loop {
                    let qi = q_cursor.fetch_add(1, Ordering::Relaxed);
                    if qi >= 160 {
                        break;
                    }
                    let tenant = format!("ephem-w{w}-t{:02}", qi % tenants);
                    if qi.is_multiple_of(3) {
                        let mut req = tonic::Request::new(ingesterpb::LabelNamesAndValuesRequest {
                            matchers: Vec::new(),
                            count_method: ingesterpb::CountMethod::Active as i32,
                        });
                        req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant.as_str()).unwrap(),
                        );
                        let mut stream = client
                            .label_names_and_values(req)
                            .await
                            .expect("lnv ok")
                            .into_inner();
                        while let Some(msg) = stream.next().await {
                            let _ = msg.expect("lnv item ok");
                        }
                    } else {
                        let mut req = tonic::Request::new(ingesterpb::QueryRequest {
                            start_timestamp_ms: wave_base_ts - 60_000,
                            end_timestamp_ms: wave_base_ts + 120_000,
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: 2,
                                name: "__name__".to_string(),
                                value: "http_.*|container_.*".to_string(),
                            }],
                            ..Default::default()
                        });
                        req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant.as_str()).unwrap(),
                        );
                        let mut stream = client.query_stream(req).await.expect("qs ok").into_inner();
                        while let Some(msg) = stream.next().await {
                            let _ = msg.expect("qs chunk ok");
                        }
                    }
                }
            }));
        }
        for j in q_joins {
            j.await.unwrap();
        }
        let active_wall = wave_start.elapsed().as_secs_f64();
        let peak = read_mem_snap(pid);
        let open_users = scrape_metric(&http, http_port, "cortex_ingester_memory_users").await;
        let open_series = scrape_metric(&http, http_port, "cortex_ingester_memory_series").await;

        let r = http
            .post(format!("http://127.0.0.1:{http_port}/ingester/flush?wait=true"))
            .send()
            .await
            .expect("flush");
        assert!(r.status().is_success(), "flush status {}", r.status());
        let flush_deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let s = scrape_metric(&http, http_port, "cortex_ingester_memory_series").await;
            if s == 0.0 {
                break;
            }
            assert!(
                Instant::now() < flush_deadline,
                "series did not compact to 0 (remaining={s})"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let post_flush = read_mem_snap(pid);
        let post_series = scrape_metric(&http, http_port, "cortex_ingester_memory_series").await;

        println!(
            "Wave {w} | active ({active_wall:.2}s, users={open_users:.0}, series={open_series:.0}): RssAnon={:.2} MiB  VmRSS={:.2} MiB  VmHWM={:.2} MiB  CPU={:.2}s\n       | post-flush 0-series (series={post_series:.0}): RssAnon={:.2} MiB (+{:.2} MiB vs boot)  PrivateDirty={:.2} MiB  Pss={:.2} MiB  VmRSS={:.2} MiB",
            peak.rss_anon_mib,
            peak.vm_rss_mib,
            peak.vm_hwm_mib,
            peak.cpu_secs,
            post_flush.rss_anon_mib,
            post_flush.rss_anon_mib - boot.rss_anon_mib,
            post_flush.private_dirty_mib,
            post_flush.pss_mib,
            post_flush.vm_rss_mib,
        );
    }
}
