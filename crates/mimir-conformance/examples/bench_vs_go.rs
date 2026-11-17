//! Head-to-head Go Mimir vs Rust mimir-rs CPU, RSS, throughput, and latency benchmark.
//!
//! Run via: `just bench-vs-go`

use std::fs;
use std::future::Future;
use std::net::TcpListener;
use std::path::Path;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http_body_util::BodyExt;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin,
    rust_mimir_release_bin,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::{s2, snappy};
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Status;
use tower::Service;

const MAX_GRPC_MSG_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum GrpcCompression {
    #[default]
    Identity,
    Snappy,
    S2,
    Gzip,
}

impl GrpcCompression {
    fn from_str_flag(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "" | "identity" => Self::Identity,
            "snappy" => Self::Snappy,
            "s2" => Self::S2,
            "gzip" => Self::Gzip,
            other => panic!(
                "unsupported --compression value: {other:?} (expected identity|snappy|s2|gzip)"
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Snappy => "snappy",
            Self::S2 => "s2",
            Self::Gzip => "gzip",
        }
    }
}

fn encode_s2_framed(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() / 2 + 32);
    out.extend_from_slice(b"\xff\x06\x00\x00S2sTwO");
    let mut enc = snap::raw::Encoder::new();
    for block in payload.chunks(65_536) {
        let compressed = enc.compress_vec(block).expect("snappy raw compress");
        let crc = s2::masked_crc32c(block);
        if compressed.len() < block.len() {
            let chunk_len = 4 + compressed.len();
            out.push(0x00);
            out.push((chunk_len & 0xff) as u8);
            out.push(((chunk_len >> 8) & 0xff) as u8);
            out.push(((chunk_len >> 16) & 0xff) as u8);
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&compressed);
        } else {
            let chunk_len = 4 + block.len();
            out.push(0x01);
            out.push((chunk_len & 0xff) as u8);
            out.push(((chunk_len >> 8) & 0xff) as u8);
            out.push(((chunk_len >> 16) & 0xff) as u8);
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(block);
        }
    }
    out
}

fn compress_grpc_frames(raw_body: &[u8], mode: GrpcCompression) -> Bytes {
    let mut out = BytesMut::with_capacity(raw_body.len() + 32);
    let mut pos = 0usize;
    while pos + 5 <= raw_body.len() {
        let flag = raw_body[pos];
        let len = u32::from_be_bytes([
            raw_body[pos + 1],
            raw_body[pos + 2],
            raw_body[pos + 3],
            raw_body[pos + 4],
        ]) as usize;
        pos += 5;
        let end = pos + len;
        if end > raw_body.len() {
            break;
        }
        let payload = &raw_body[pos..end];
        pos = end;
        if flag == 0 {
            let comp = match mode {
                GrpcCompression::Snappy => snappy::compress_framed(payload).expect("snappy"),
                GrpcCompression::S2 => encode_s2_framed(payload),
                GrpcCompression::Identity | GrpcCompression::Gzip => payload.to_vec(),
            };
            out.put_u8(1);
            out.put_u32(comp.len() as u32);
            out.put_slice(&comp);
        } else {
            out.put_u8(flag);
            out.put_u32(len as u32);
            out.put_slice(payload);
        }
    }
    out.freeze()
}

struct SingleChunkBody(Option<Bytes>);

impl hyper::body::Body for SingleChunkBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.0.take() {
            Some(b) if !b.is_empty() => Poll::Ready(Some(Ok(hyper::body::Frame::data(b)))),
            _ => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.0.as_ref().is_none_or(Bytes::is_empty)
    }
}

struct DecompressGrpcBody {
    inner: tonic::body::BoxBody,
    mode: GrpcCompression,
    in_buf: BytesMut,
    decomp_scratch: Vec<u8>,
    pending_trailers: Option<http::HeaderMap>,
    done: bool,
}

impl DecompressGrpcBody {
    fn new(inner: tonic::body::BoxBody, mode: GrpcCompression) -> Self {
        Self {
            inner,
            mode,
            in_buf: BytesMut::new(),
            decomp_scratch: Vec::new(),
            pending_trailers: None,
            done: false,
        }
    }

    #[allow(clippy::result_large_err)]
    fn drain_complete_frames(&mut self) -> Result<Option<Bytes>, Status> {
        let mut out = BytesMut::new();
        while self.in_buf.len() >= 5 {
            let flag = self.in_buf[0];
            let len = u32::from_be_bytes([
                self.in_buf[1],
                self.in_buf[2],
                self.in_buf[3],
                self.in_buf[4],
            ]) as usize;
            let frame_end = 5 + len;
            if self.in_buf.len() < frame_end {
                break;
            }
            if flag == 0 {
                out.extend_from_slice(&self.in_buf[..frame_end]);
            } else if flag == 1 {
                let comp = &self.in_buf[5..frame_end];
                self.decomp_scratch.clear();
                match self.mode {
                    GrpcCompression::Snappy => {
                        snappy::decompress_framed_bounded_into(
                            comp,
                            &mut self.decomp_scratch,
                            MAX_GRPC_MSG_BYTES,
                        )
                        .map_err(|e| Status::internal(format!("snappy decompress: {e}")))?;
                    }
                    GrpcCompression::S2 => {
                        s2::decompress_framed_into(
                            comp,
                            &mut self.decomp_scratch,
                            MAX_GRPC_MSG_BYTES,
                        )
                        .map_err(|e| Status::internal(format!("s2 decompress: {e}")))?;
                    }
                    GrpcCompression::Identity | GrpcCompression::Gzip => {
                        self.decomp_scratch.extend_from_slice(comp);
                    }
                }
                out.reserve(5 + self.decomp_scratch.len());
                out.put_u8(0);
                out.put_u32(self.decomp_scratch.len() as u32);
                out.put_slice(&self.decomp_scratch);
            } else {
                return Err(Status::internal(format!(
                    "unexpected gRPC compression flag {flag}"
                )));
            }
            self.in_buf.advance(frame_end);
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out.freeze()))
        }
    }
}

impl hyper::body::Body for DecompressGrpcBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if let Some(trailers) = self.pending_trailers.take() {
            self.done = true;
            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
        }
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            match self.drain_complete_frames() {
                Ok(Some(chunk)) => return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk)))),
                Ok(None) => {}
                Err(e) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => {
                        if !data.is_empty() {
                            self.in_buf.extend_from_slice(&data);
                        }
                    }
                    Err(other) => {
                        if let Ok(trailers) = other.into_trailers() {
                            self.done = true;
                            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
                        }
                    }
                },
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

#[derive(Clone)]
struct CompressedChannel {
    inner: Channel,
    mode: GrpcCompression,
}

impl CompressedChannel {
    fn new(inner: Channel, mode: GrpcCompression) -> Self {
        Self { inner, mode }
    }
}

impl Service<http::Request<tonic::body::BoxBody>> for CompressedChannel {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = tonic::transport::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let mode = self.mode;
        if !matches!(mode, GrpcCompression::Snappy | GrpcCompression::S2) {
            let fut = self.inner.call(req);
            return Box::pin(fut);
        }

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let (mut parts, body) = req.into_parts();
            let collected = body
                .collect()
                .await
                .expect("collect unary gRPC request body")
                .to_bytes();
            let comp_body = compress_grpc_frames(&collected, mode);
            parts.headers.insert(
                "grpc-encoding",
                http::HeaderValue::from_static(mode.as_str()),
            );
            parts.headers.insert(
                "grpc-accept-encoding",
                http::HeaderValue::from_static(match mode {
                    GrpcCompression::Snappy => "snappy,identity",
                    GrpcCompression::S2 => "s2,identity",
                    _ => "identity",
                }),
            );
            parts.headers.remove(http::header::CONTENT_LENGTH);

            let new_req = http::Request::from_parts(
                parts,
                tonic::body::BoxBody::new(SingleChunkBody(Some(comp_body))),
            );
            let resp = inner.call(new_req).await?;
            let resp_enc = resp
                .headers()
                .get("grpc-encoding")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| match s.to_ascii_lowercase().as_str() {
                    "snappy" => Some(GrpcCompression::Snappy),
                    "s2" => Some(GrpcCompression::S2),
                    _ => None,
                });
            if let Some(enc) = resp_enc {
                let (mut rparts, rbody) = resp.into_parts();
                rparts.headers.remove("grpc-encoding");
                rparts.headers.remove(http::header::CONTENT_LENGTH);
                Ok(http::Response::from_parts(
                    rparts,
                    tonic::body::BoxBody::new(DecompressGrpcBody::new(rbody, enc)),
                ))
            } else {
                Ok(resp)
            }
        })
    }
}

fn build_ingester_client(
    ch: Channel,
    mode: GrpcCompression,
) -> IngesterClient<CompressedChannel> {
    let mut client = IngesterClient::new(CompressedChannel::new(ch, mode))
        .max_decoding_message_size(MAX_GRPC_MSG_BYTES)
        .max_encoding_message_size(MAX_GRPC_MSG_BYTES);
    if mode == GrpcCompression::Gzip {
        client = client
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
    }
    client
}

struct ProcessGuard {
    name: String,
    child: Option<Child>,
}

impl ProcessGuard {
    fn spawn(name: impl Into<String>, bin: &Path, args: &[String], workdir: &Path) -> Self {
        let name = name.into();
        let child = Command::new(bin)
            .args(args)
            .current_dir(workdir)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {name} ({}): {e}", bin.display()));
        Self {
            name,
            child: Some(child),
        }
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().expect("child alive").id()
    }

    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.child.as_mut().and_then(|c| c.try_wait().ok().flatten())
    }

    fn dump_stderr(&mut self) -> String {
        use std::io::Read;
        let Some(ref mut c) = self.child else {
            return String::new();
        };
        if c.try_wait().ok().flatten().is_none() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let mut s = String::new();
        if let Some(mut err) = c.stderr.take() {
            let _ = err.read_to_string(&mut s);
        }
        s
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
    }
}

#[derive(Clone, Copy, Debug)]
struct ProcStats {
    cpu_secs: f64,
    vm_rss_mib: f64,
    vm_hwm_mib: f64,
}

fn read_proc_stats(pid: u32) -> ProcStats {
    let stat_raw = fs::read_to_string(format!("/proc/{pid}/stat")).expect("read /proc/<pid>/stat");
    let rparen = stat_raw.rfind(')').expect("stat closing paren");
    let after = &stat_raw[rparen + 2..];
    let fields: Vec<&str> = after.split_whitespace().collect();
    // After ") ": field 3 is index 0, so field 14 (utime) is index 11, field 15 (stime) is index 12.
    let utime: u64 = fields[11].parse().expect("parse utime");
    let stime: u64 = fields[12].parse().expect("parse stime");
    let cpu_secs = (utime + stime) as f64 / 100.0;

    let status_raw =
        fs::read_to_string(format!("/proc/{pid}/status")).expect("read /proc/<pid>/status");
    let mut rss_kb = 0u64;
    let mut hwm_kb = 0u64;
    for line in status_raw.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss_kb = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            hwm_kb = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
        }
    }

    ProcStats {
        cpu_secs,
        vm_rss_mib: rss_kb as f64 / 1024.0,
        vm_hwm_mib: hwm_kb as f64 / 1024.0,
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
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = proc.try_wait() {
            let err = proc.dump_stderr();
            panic!("{} exited with {status} before /ready:\n{err}", proc.name);
        }
        if let Ok(resp) = http.get(&url).send().await {
            if resp.status().as_u16() == 200 {
                return;
            }
        }
        assert!(Instant::now() < deadline, "{} /ready timeout", proc.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Clone, Debug)]
struct BenchConfig {
    total_series: usize,
    hist_series: usize,
    tenants: usize,
    rounds: usize,
    batch_size: usize,
    concurrency: usize,
    queries: usize,
    compression: GrpcCompression,
    run_go: bool,
    run_rust: bool,
    perf_out: Option<String>,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            total_series: 100_000,
            hist_series: 2_000,
            tenants: 5,
            rounds: 20,
            batch_size: 1_000,
            concurrency: 8,
            queries: 200,
            compression: GrpcCompression::Identity,
            run_go: true,
            run_rust: true,
            perf_out: None,
        }
    }
}

fn parse_args() -> BenchConfig {
    let mut cfg = BenchConfig::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let next_str = |idx: &mut usize| -> String {
            *idx += 1;
            args.get(*idx).expect("missing flag value").clone()
        };
        let next_val = |idx: &mut usize| -> usize {
            *idx += 1;
            args.get(*idx)
                .expect("missing flag value")
                .parse()
                .expect("invalid integer flag value")
        };
        match arg.as_str() {
            "--series" => cfg.total_series = next_val(&mut i),
            "--hist-series" => cfg.hist_series = next_val(&mut i),
            "--tenants" => cfg.tenants = next_val(&mut i),
            "--rounds" => cfg.rounds = next_val(&mut i),
            "--batch-size" => cfg.batch_size = next_val(&mut i),
            "--concurrency" => cfg.concurrency = next_val(&mut i),
            "--queries" => cfg.queries = next_val(&mut i),
            "--compression" => {
                let mode = next_str(&mut i);
                cfg.compression = GrpcCompression::from_str_flag(&mode);
            }
            "--rust-only" => {
                cfg.run_go = false;
                cfg.run_rust = true;
            }
            "--go-only" => {
                cfg.run_go = true;
                cfg.run_rust = false;
            }
            "--perf-out" => cfg.perf_out = Some(next_str(&mut i)),
            other => panic!("unknown bench_vs_go flag: {other}"),
        }
        i += 1;
    }
    cfg
}

fn build_batch_templates(cfg: &BenchConfig, base_ts: i64) -> Vec<(String, cortexpb::WriteRequest)> {
    let series_per_tenant = cfg.total_series / cfg.tenants;
    let hist_per_tenant = cfg.hist_series / cfg.tenants;
    let metrics = [
        "http_server_requests_total",
        "http_client_requests_total",
        "grpc_server_handled_total",
        "container_cpu_usage_seconds_total",
        "process_network_bytes_total",
    ];
    let namespaces = [
        "prod-api",
        "prod-checkout",
        "prod-auth",
        "prod-billing",
        "prod-search",
    ];
    let containers = ["app", "envoy", "otel-sidecar", "worker"];
    let statuses = ["200", "201", "400", "404", "500"];

    let mut templates = Vec::new();
    for t_idx in 0..cfg.tenants {
        let tenant = format!("tenant-{t_idx}");
        let mut s_idx = 0usize;
        while s_idx < series_per_tenant {
            let end = (s_idx + cfg.batch_size).min(series_per_tenant);
            let mut timeseries = Vec::with_capacity(end - s_idx);
            for k in s_idx..end {
                let metric = metrics[k % metrics.len()];
                let ns = namespaces[(k / 5) % namespaces.len()];
                let pod_num = k / 25;
                let pod = format!("svc-{t_idx}-{pod_num}");
                let container = containers[k % containers.len()];
                let instance = format!("10.42.{}.{}:8080", (k >> 8) & 0xff, k & 0xff);
                let job = format!("{ns}/service");
                let status_code = statuses[k % statuses.len()];

                // Sorted lexicographically by label name:
                // __name__, cluster, container, instance, job, namespace, pod, status_code
                let labels = vec![
                    cortexpb::LabelPair::from_static("__name__", metric),
                    cortexpb::LabelPair::from_static("cluster", "prod-us-east-1"),
                    cortexpb::LabelPair::from_static("container", container),
                    cortexpb::LabelPair::from_strs("instance", &instance),
                    cortexpb::LabelPair::from_strs("job", &job),
                    cortexpb::LabelPair::from_static("namespace", ns),
                    cortexpb::LabelPair::from_strs("pod", &pod),
                    cortexpb::LabelPair::from_static("status_code", status_code),
                ];

                let exemplars = if k % 100 == 0 {
                    vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_strs(
                            "trace_id",
                            format!("tr-{t_idx}-{k:06}"),
                        )],
                        value: 0.025,
                        timestamp_ms: base_ts,
                    }]
                } else {
                    Vec::new()
                };

                timeseries.push(cortexpb::TimeSeries {
                    labels,
                    samples: vec![cortexpb::Sample {
                        value: (k as f64) * 1.5,
                        timestamp_ms: base_ts,
                    }],
                    exemplars,
                    histograms: Vec::new(),
                    created_timestamp: 0,
                });
            }
            templates.push((
                tenant.clone(),
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    timeseries,
                    ..Default::default()
                },
            ));
            s_idx = end;
        }

        // Native histogram batches
        let mut h_idx = 0usize;
        while h_idx < hist_per_tenant {
            let end = (h_idx + cfg.batch_size).min(hist_per_tenant);
            let mut timeseries = Vec::with_capacity(end - h_idx);
            for k in h_idx..end {
                let ns = namespaces[k % namespaces.len()];
                let pod = format!("hist-svc-{t_idx}-{}", k / 4);
                let container = containers[k % containers.len()];
                let instance = format!("10.43.{}.{}:9090", (k >> 8) & 0xff, k & 0xff);
                let job = format!("{ns}/hist-service");
                let status_code = statuses[k % statuses.len()];

                let labels = vec![
                    cortexpb::LabelPair::from_static("__name__", "http_request_duration_seconds"),
                    cortexpb::LabelPair::from_static("cluster", "prod-us-east-1"),
                    cortexpb::LabelPair::from_static("container", container),
                    cortexpb::LabelPair::from_strs("instance", &instance),
                    cortexpb::LabelPair::from_strs("job", &job),
                    cortexpb::LabelPair::from_static("namespace", ns),
                    cortexpb::LabelPair::from_strs("pod", &pod),
                    cortexpb::LabelPair::from_static("status_code", status_code),
                ];

                timeseries.push(cortexpb::TimeSeries {
                    labels,
                    samples: Vec::new(),
                    exemplars: Vec::new(),
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(12)),
                        sum: 4.5,
                        schema: 0,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 3,
                        }],
                        positive_deltas: vec![3, 1, -1],
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts,
                        ..Default::default()
                    }],
                    created_timestamp: 0,
                });
            }
            templates.push((
                tenant.clone(),
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    timeseries,
                    ..Default::default()
                },
            ));
            h_idx = end;
        }
    }
    templates
}

fn percentile_ms(sorted: &[Duration], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((sorted.len() - 1) as f64) * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)].as_secs_f64() * 1000.0
}

#[derive(Debug)]
struct BenchResult {
    name: &'static str,
    ingest_wall_s: f64,
    total_samples_ingested: usize,
    throughput_samples_per_s: f64,
    push_p50_ms: f64,
    push_p99_ms: f64,
    after_ingest: ProcStats,
    query_wall_s: f64,
    query_stream_p50_ms: f64,
    query_stream_p99_ms: f64,
    lnv_p50_ms: f64,
    lnv_p99_ms: f64,
    lvc_p50_ms: f64,
    lvc_p99_ms: f64,
    after_queries: ProcStats,
}

fn write_bench_yaml(path: &Path, http_port: u16, grpc_port: u16, tsdb_dir: &Path, bucket_dir: &Path) {
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
    instance_id: ingester-bench-0
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

async fn run_single_engine(
    name: &'static str,
    bin: &Path,
    cfg: &BenchConfig,
    templates: &[(String, cortexpb::WriteRequest)],
    base_ts: i64,
    perf_out: Option<&str>,
) -> BenchResult {
    let tmp = tempfile::tempdir().unwrap();
    let tsdb_dir = tmp.path().join("tsdb");
    let bucket_dir = tmp.path().join("bucket");
    fs::create_dir_all(&tsdb_dir).unwrap();
    fs::create_dir_all(&bucket_dir).unwrap();

    let http_port = free_tcp_port();
    let grpc_port = free_tcp_port();
    let yaml_path = tmp.path().join("mimir.yaml");
    write_bench_yaml(&yaml_path, http_port, grpc_port, &tsdb_dir, &bucket_dir);

    let mut proc = ProcessGuard::spawn(
        name,
        bin,
        &[
            "-target=ingester".to_string(),
            format!("-config.file={}", yaml_path.display()),
        ],
        tmp.path(),
    );

    let http = reqwest::Client::new();
    wait_for_ready(&http, http_port, &mut proc).await;
    let pid = proc.pid();
    let _perf_guard = perf_out.map(|out| {
        ProcessGuard::spawn(
            "perf",
            Path::new("perf"),
            &[
                "record".to_string(),
                "-F".to_string(),
                "999".to_string(),
                "-g".to_string(),
                "--call-graph".to_string(),
                "dwarf,16384".to_string(),
                "-p".to_string(),
                pid.to_string(),
                "-o".to_string(),
                out.to_string(),
            ],
            tmp.path(),
        )
    });

    let mut channels = Vec::with_capacity(cfg.concurrency);
    for _ in 0..cfg.concurrency {
        let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .unwrap()
            .tcp_nodelay(true)
            .connect()
            .await
            .unwrap();
        channels.push(ch);
    }

    let templates_arc = Arc::new(templates.to_vec());
    let mut push_latencies = Vec::with_capacity(cfg.rounds * templates.len());

    let ingest_start = Instant::now();
    for round in 0..cfg.rounds {
        let round_ts = base_ts + (round as i64) * 15_000;
        let round_f = (round as f64) * 10.0;
        let next_idx = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(cfg.concurrency);

        for ch in channels.iter().take(cfg.concurrency) {
            let ch = ch.clone();
            let tmpl = Arc::clone(&templates_arc);
            let idx_ctr = Arc::clone(&next_idx);
            let compression = cfg.compression;
            handles.push(tokio::spawn(async move {
                let mut client = build_ingester_client(ch, compression);
                let mut lats = Vec::new();
                loop {
                    let i = idx_ctr.fetch_add(1, Ordering::Relaxed);
                    if i >= tmpl.len() {
                        break;
                    }
                    let (tenant, base_req) = &tmpl[i];
                    let mut req = base_req.clone();
                    for ts in &mut req.timeseries {
                        for s in &mut ts.samples {
                            s.timestamp_ms = round_ts;
                            s.value += round_f;
                        }
                        for h in &mut ts.histograms {
                            h.timestamp = round_ts;
                            if let Some(cortexpb::histogram::ZeroCount::ZeroCountInt(ref mut zc)) =
                                h.zero_count
                            {
                                *zc += round as u64;
                            }
                            if let Some(cortexpb::histogram::Count::CountInt(ref mut c)) = h.count {
                                *c += round as u64;
                            }
                            h.sum += round_f;
                        }
                        for e in &mut ts.exemplars {
                            e.timestamp_ms = round_ts;
                        }
                    }
                    let t0 = Instant::now();
                    client.push(with_tenant(tenant, req)).await.unwrap();
                    lats.push(t0.elapsed());
                }
                lats
            }));
        }

        for h in handles {
            push_latencies.extend(h.await.unwrap());
        }
    }
    let ingest_wall = ingest_start.elapsed();
    let after_ingest = read_proc_stats(pid);

    push_latencies.sort();
    let total_samples_ingested = cfg.rounds * (cfg.total_series + cfg.hist_series);

    // Phase 2: concurrent QueryStream + LabelValues + ActiveSeries + LabelNamesAndValues + LabelValuesCardinality
    let start_ms = base_ts - 1000;
    let end_ms = base_ts + ((cfg.rounds as i64) * 15_000) + 1000;
    let next_q = Arc::new(AtomicUsize::new(0));
    let mut q_handles = Vec::with_capacity(cfg.concurrency);

    let query_start = Instant::now();
    for ch in channels.iter().take(cfg.concurrency) {
        let ch = ch.clone();
        let q_ctr = Arc::clone(&next_q);
        let total_q = cfg.queries;
        let tenants = cfg.tenants;
        let compression = cfg.compression;
        q_handles.push(tokio::spawn(async move {
            let mut client = build_ingester_client(ch, compression);
            let mut qs_lats = Vec::new();
            let mut lnv_lats = Vec::new();
            let mut lvc_lats = Vec::new();
            loop {
                let qi = q_ctr.fetch_add(1, Ordering::Relaxed);
                if qi >= total_q {
                    break;
                }
                let tenant = format!("tenant-{}", qi % tenants);
                let ns = match qi % 5 {
                    0 => "prod-api",
                    1 => "prod-checkout",
                    2 => "prod-auth",
                    3 => "prod-billing",
                    _ => "prod-search",
                };
                match qi % 5 {
                    0 => {
                        let t0 = Instant::now();
                        let mut stream = client
                            .query_stream(with_tenant(
                                &tenant,
                                ingesterpb::QueryRequest {
                                    start_timestamp_ms: start_ms,
                                    end_timestamp_ms: end_ms,
                                    matchers: vec![
                                        ingesterpb::LabelMatcher {
                                            r#type: ingesterpb::MatchType::Equal as i32,
                                            name: "__name__".to_string(),
                                            value: "http_server_requests_total".to_string(),
                                        },
                                        ingesterpb::LabelMatcher {
                                            r#type: ingesterpb::MatchType::Equal as i32,
                                            name: "namespace".to_string(),
                                            value: ns.to_string(),
                                        },
                                    ],
                                    streaming_chunks_batch_size: 100,
                                },
                            ))
                            .await
                            .unwrap()
                            .into_inner();
                        while let Some(item) = stream.next().await {
                            let _ = item.unwrap();
                        }
                        qs_lats.push(t0.elapsed());
                    }
                    1 => {
                        let _ = client
                            .label_values(with_tenant(
                                &tenant,
                                ingesterpb::LabelValuesRequest {
                                    label_name: "pod".to_string(),
                                    start_timestamp_ms: start_ms,
                                    end_timestamp_ms: end_ms,
                                    matchers: None,
                                    limit: 1000,
                                },
                            ))
                            .await
                            .unwrap();
                    }
                    2 => {
                        let mut stream = client
                            .active_series(with_tenant(
                                &tenant,
                                ingesterpb::ActiveSeriesRequest {
                                    matchers: vec![ingesterpb::LabelMatcher {
                                        r#type: ingesterpb::MatchType::Equal as i32,
                                        name: "namespace".to_string(),
                                        value: ns.to_string(),
                                    }],
                                    r#type: ingesterpb::active_series_request::RequestType::Series
                                        as i32,
                                },
                            ))
                            .await
                            .unwrap()
                            .into_inner();
                        while let Some(item) = stream.next().await {
                            let _ = item.unwrap();
                        }
                    }
                    3 => {
                        let t0 = Instant::now();
                        let mut stream = client
                            .label_names_and_values(with_tenant(
                                &tenant,
                                ingesterpb::LabelNamesAndValuesRequest {
                                    matchers: vec![ingesterpb::LabelMatcher {
                                        r#type: ingesterpb::MatchType::Equal as i32,
                                        name: "namespace".to_string(),
                                        value: ns.to_string(),
                                    }],
                                    count_method: ingesterpb::CountMethod::Active as i32,
                                },
                            ))
                            .await
                            .unwrap()
                            .into_inner();
                        while let Some(item) = stream.next().await {
                            let _ = item.unwrap();
                        }
                        lnv_lats.push(t0.elapsed());
                    }
                    _ => {
                        let t0 = Instant::now();
                        let mut stream = client
                            .label_values_cardinality(with_tenant(
                                &tenant,
                                ingesterpb::LabelValuesCardinalityRequest {
                                    label_names: vec![
                                        "namespace".to_string(),
                                        "cluster".to_string(),
                                        "status_code".to_string(),
                                    ],
                                    matchers: Vec::new(),
                                    count_method: ingesterpb::CountMethod::Active as i32,
                                },
                            ))
                            .await
                            .unwrap()
                            .into_inner();
                        while let Some(item) = stream.next().await {
                            let _ = item.unwrap();
                        }
                        lvc_lats.push(t0.elapsed());
                    }
                }
            }
            (qs_lats, lnv_lats, lvc_lats)
        }));
    }

    let mut qs_latencies = Vec::new();
    let mut lnv_latencies = Vec::new();
    let mut lvc_latencies = Vec::new();
    for h in q_handles {
        let (qs, lnv, lvc) = h.await.unwrap();
        qs_latencies.extend(qs);
        lnv_latencies.extend(lnv);
        lvc_latencies.extend(lvc);
    }
    let query_wall = query_start.elapsed();
    let after_queries = read_proc_stats(pid);
    qs_latencies.sort();
    lnv_latencies.sort();
    lvc_latencies.sort();

    BenchResult {
        name,
        ingest_wall_s: ingest_wall.as_secs_f64(),
        total_samples_ingested,
        throughput_samples_per_s: total_samples_ingested as f64 / ingest_wall.as_secs_f64(),
        push_p50_ms: percentile_ms(&push_latencies, 0.50),
        push_p99_ms: percentile_ms(&push_latencies, 0.99),
        after_ingest,
        query_wall_s: query_wall.as_secs_f64(),
        query_stream_p50_ms: percentile_ms(&qs_latencies, 0.50),
        query_stream_p99_ms: percentile_ms(&qs_latencies, 0.99),
        lnv_p50_ms: percentile_ms(&lnv_latencies, 0.50),
        lnv_p99_ms: percentile_ms(&lnv_latencies, 0.99),
        lvc_p50_ms: percentile_ms(&lvc_latencies, 0.50),
        lvc_p99_ms: percentile_ms(&lvc_latencies, 0.99),
        after_queries,
    }
}

#[tokio::main]
async fn main() {
    let cfg = parse_args();
    if cfg.run_go {
        ensure_go_oracle_built().expect("go-mimir must build");
    }
    if cfg.run_rust {
        ensure_rust_mimir_release_built().expect("target/release/mimir must build");
    }

    let base_ts = now_ms() - ((cfg.rounds as i64) * 20_000);
    println!(
        "Preparing benchmark workload: {} float series + {} histogram series across {} tenants, {} rounds ({} batches/round, batch_size={}, concurrency={}, compression={}), {} read RPCs...",
        cfg.total_series,
        cfg.hist_series,
        cfg.tenants,
        cfg.rounds,
        (cfg.total_series + cfg.hist_series).div_ceil(cfg.batch_size),
        cfg.batch_size,
        cfg.concurrency,
        cfg.compression.as_str(),
        cfg.queries
    );
    let templates = build_batch_templates(&cfg, base_ts);

    let go_res = if cfg.run_go {
        Some(run_single_engine("Go Mimir", &go_mimir_bin(), &cfg, &templates, base_ts, None).await)
    } else {
        None
    };
    let rust_res = if cfg.run_rust {
        Some(
            run_single_engine(
                "Rust mimir-rs",
                &rust_mimir_release_bin(),
                &cfg,
                &templates,
                base_ts,
                cfg.perf_out.as_deref(),
            )
            .await,
        )
    } else {
        None
    };

    let print_res = |r: &BenchResult| {
        let m_samples = r.total_samples_ingested as f64 / 1_000_000.0;
        println!(
            "--- {} ({}) ---\n\
             Ingest wall:             {:>8.3} s  ({:.0} samples/sec)\n\
             Push latency:            p50={:.2} ms   p99={:.2} ms\n\
             After ingest:            CPU={:.3} s ({:.3} CPU-s/1M)  VmRSS={:.2} MiB  VmHWM={:.2} MiB\n\
             Query wall ({}x):        {:>8.3} s\n\
             QueryStream latency:     p50={:.2} ms   p99={:.2} ms\n\
             LabelNamesAndValues:     p50={:.2} ms   p99={:.2} ms\n\
             LabelValuesCardinality:  p50={:.2} ms   p99={:.2} ms\n\
             After queries:           CPU={:.3} s  VmRSS={:.2} MiB  VmHWM={:.2} MiB",
            r.name,
            cfg.compression.as_str(),
            r.ingest_wall_s,
            r.throughput_samples_per_s,
            r.push_p50_ms,
            r.push_p99_ms,
            r.after_ingest.cpu_secs,
            r.after_ingest.cpu_secs / m_samples,
            r.after_ingest.vm_rss_mib,
            r.after_ingest.vm_hwm_mib,
            cfg.queries,
            r.query_wall_s,
            r.query_stream_p50_ms,
            r.query_stream_p99_ms,
            r.lnv_p50_ms,
            r.lnv_p99_ms,
            r.lvc_p50_ms,
            r.lvc_p99_ms,
            r.after_queries.cpu_secs,
            r.after_queries.vm_rss_mib,
            r.after_queries.vm_hwm_mib,
        );
    };

    if let Some(ref g) = go_res {
        println!();
        print_res(g);
    }
    if let Some(ref r) = rust_res {
        println!();
        print_res(r);
    }
    if let (Some(go_res), Some(rust_res)) = (go_res, rust_res) {
        println!();
        println!("=== Comparison (Go / Rust, compression={}) ===", cfg.compression.as_str());
        println!(
            "Ingest throughput speedup:        {:.2}x",
            rust_res.throughput_samples_per_s / go_res.throughput_samples_per_s
        );
        println!(
            "Ingest CPU reduction:             {:.2}x ({:.1}% less CPU)",
            go_res.after_ingest.cpu_secs / rust_res.after_ingest.cpu_secs,
            (1.0 - rust_res.after_ingest.cpu_secs / go_res.after_ingest.cpu_secs) * 100.0
        );
        println!(
            "Total CPU reduction:              {:.2}x ({:.1}% less CPU)",
            go_res.after_queries.cpu_secs / rust_res.after_queries.cpu_secs,
            (1.0 - rust_res.after_queries.cpu_secs / go_res.after_queries.cpu_secs) * 100.0
        );
        println!(
            "RSS memory reduction:             {:.2}x ({:.1}% less RSS)",
            go_res.after_queries.vm_rss_mib / rust_res.after_queries.vm_rss_mib,
            (1.0 - rust_res.after_queries.vm_rss_mib / go_res.after_queries.vm_rss_mib) * 100.0
        );
        println!(
            "Peak HWM reduction:               {:.2}x ({:.1}% less peak RAM)",
            go_res.after_queries.vm_hwm_mib / rust_res.after_queries.vm_hwm_mib,
            (1.0 - rust_res.after_queries.vm_hwm_mib / go_res.after_queries.vm_hwm_mib) * 100.0
        );
        println!(
            "Push p99 improvement:             {:.2}x",
            go_res.push_p99_ms / rust_res.push_p99_ms
        );
        println!(
            "QueryStream p99 improvement:      {:.2}x",
            go_res.query_stream_p99_ms / rust_res.query_stream_p99_ms
        );
        println!(
            "LabelNamesAndValues p99 improv.:  {:.2}x",
            go_res.lnv_p99_ms / rust_res.lnv_p99_ms
        );
        println!(
            "LabelValuesCardinality p99 imp.:  {:.2}x",
            go_res.lvc_p99_ms / rust_res.lvc_p99_ms
        );
    }
}
