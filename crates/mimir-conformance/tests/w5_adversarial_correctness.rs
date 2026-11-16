//! Wave 5 Adversarial Correctness & Differential Verification Suite:
//!
//! a) Real Go `grpc-go` + `dskit/httpgrpc` client against live Rust `mimir` across 2xx / 4xx / 5xx
//!    (zero `grpc-status-details-bin mismatch`, canonical header keys preserved).
//! b) Malformed raw protobuf frames (including exact underflow PoC `[0x0a, 0x04, 0x12, 0x01, 0x89, 0x00]`
//!    and 50 truncated/corrupted payloads) against live Go & Rust `mimir` -> neither panics nor leaks
//!    `cortex_ingester_inflight_push_requests`, and subsequent valid Push + QueryStream succeed 100%.
//! c) Twin-process `multitenancy_enabled: false` + `no_auth_tenant: "single-tenant"` parity without
//!    `x-scope-orgid` metadata.
//! d) Twin-process adversarial edge cases parity:
//!    (i) duplicate timestamps & `past_grace_period` (`SampleTooFarInPast` returning 200 OK when mixed
//!        with valid samples),
//!    (ii) non-UTF-8 bytes in `MetricMetadata.help`,
//!    (iii) `SIGKILL` recovery with a deliberately torn multi-page WAL record at the tail of the
//!          Rust WAL segment.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use bytes::{Buf, BufMut, Bytes};
use tokio_stream::StreamExt;

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, invoke_oracle,
    rust_mimir_release_bin, HttpGrpcCallRequest, HttpGrpcCallResponse,
};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::wlog::{PAGE_SIZE, RECORD_HEADER_SIZE, REC_FIRST, REC_LAST};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Status;

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

    fn sigkill_and_wait(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(t) = self.stderr_thread.take() {
            let _ = t.join();
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
                panic!(
                    "{} did not exit within {:?} (killed: {:?})",
                    self.name, timeout, status
                );
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
            panic!(
                "{} exited prematurely with {status} before /ready:\n{stderr}",
                proc.name
            );
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

async fn run_query_stream_raw(
    client: &mut IngesterClient<Channel>,
    req: tonic::Request<ingesterpb::QueryRequest>,
) -> Vec<DecodedSeries> {
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
            assert!(
                saw_end_of_series,
                "received chunks before end of series stream"
            );
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
        .map(|(idx, labels)| DecodedSeries {
            labels,
            values: chunks_by_series.remove(&(idx as u64)).unwrap_or_default(),
        })
        .collect();
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    out
}

async fn run_query_stream(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> Vec<DecodedSeries> {
    let req = with_tenant(
        tenant,
        ingesterpb::QueryRequest {
            start_timestamp_ms: start_ms,
            end_timestamp_ms: end_ms,
            matchers,
            streaming_chunks_batch_size: 4,
        },
    );
    run_query_stream_raw(client, req).await
}

#[derive(Clone, Default)]
struct RawBytesEncoder;

impl Encoder for RawBytesEncoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RawBytesDecoder;

impl Decoder for RawBytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let rem = src.remaining();
        Ok(Some(src.copy_to_bytes(rem)))
    }
}

#[derive(Clone, Default)]
struct RawBytesCodec;

impl Codec for RawBytesCodec {
    type Encode = Vec<u8>;
    type Decode = Bytes;
    type Encoder = RawBytesEncoder;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

#[allow(clippy::result_large_err)]
async fn send_raw_grpc_unary(
    ch: Channel,
    path: &'static str,
    tenant: Option<&str>,
    payload: Vec<u8>,
) -> Result<Bytes, Status> {
    let mut grpc = tonic::client::Grpc::new(ch);
    grpc.ready()
        .await
        .map_err(|e| Status::unavailable(format!("client not ready: {e}")))?;
    let mut req = tonic::Request::new(payload);
    if let Some(t) = tenant {
        req.metadata_mut().insert(
            LOWER_ORG_ID_HEADER_NAME,
            MetadataValue::try_from(t).unwrap(),
        );
    }
    let resp = grpc
        .unary(
            req,
            axum::http::uri::PathAndQuery::from_static(path),
            RawBytesCodec,
        )
        .await?;
    Ok(resp.into_inner())
}

#[allow(clippy::too_many_arguments)]
fn write_ingester_yaml(
    path: &Path,
    http_port: u16,
    grpc_port: u16,
    tsdb_dir: &Path,
    bucket_dir: &Path,
    multitenancy_enabled: bool,
    no_auth_tenant: &str,
    extra_limits_yaml: &str,
) {
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  register_instrumentation: true
multitenancy_enabled: {multitenancy_enabled}
no_auth_tenant: {no_auth_tenant}
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    unregister_on_shutdown: true
limits:
  native_histograms_ingestion_enabled: true
{extra_limits_yaml}
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

fn parse_prometheus_gauge(metrics_text: &str, metric_name: &str) -> Option<f64> {
    for line in metrics_text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(metric_name) {
            if rest.starts_with(' ') || rest.starts_with('\t') {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return Some(v);
                }
            }
        }
    }
    None
}

#[test]
fn real_go_httpgrpc_client_against_rust_mimir_2xx_4xx_5xx() {
    ensure_go_oracle_built().expect("go-oracle must build");
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");

    let tmp = tempfile::tempdir().unwrap();
    let rust_dir = tmp.path().join("rust");
    fs::create_dir_all(rust_dir.join("tsdb")).unwrap();
    fs::create_dir_all(rust_dir.join("bucket")).unwrap();

    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();
    let rust_yaml = rust_dir.join("mimir.yaml");
    write_ingester_yaml(
        &rust_yaml,
        rust_http_port,
        rust_grpc_port,
        &rust_dir.join("tsdb"),
        &rust_dir.join("bucket"),
        true,
        "anonymous",
        "",
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

    let grpc_addr = format!("127.0.0.1:{rust_grpc_port}");
    let b64 = base64::engine::general_purpose::STANDARD;

    block_on_for_tests(async {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        wait_for_ready(&http, rust_http_port, &mut rust_proc).await;

        // 1) 200 OK: GET /ready
        let ready_resp: HttpGrpcCallResponse = invoke_oracle(
            "httpgrpc-call",
            &HttpGrpcCallRequest {
                grpc_addr: grpc_addr.clone(),
                method: "GET".to_string(),
                url: "/ready".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(ready_resp.code, 200, "ready_resp: {ready_resp:?}");
        assert!(ready_resp.raw_grpc_error.is_none());
        let body = b64.decode(&ready_resp.body_b64).unwrap();
        assert_eq!(String::from_utf8_lossy(&body), "ready");
        assert_eq!(
            ready_resp.headers.get("Content-Type"),
            Some(&vec!["text/plain".to_string()]),
            "expected canonical Content-Type header: {:?}",
            ready_resp.headers
        );

        // 2) 404 Not Found: GET /nonexistent
        let not_found_resp: HttpGrpcCallResponse = invoke_oracle(
            "httpgrpc-call",
            &HttpGrpcCallRequest {
                grpc_addr: grpc_addr.clone(),
                method: "GET".to_string(),
                url: "/nonexistent".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(not_found_resp.code, 404, "not_found: {not_found_resp:?}");
        let raw_err = not_found_resp.raw_grpc_error.as_deref().unwrap_or("");
        assert!(
            !raw_err.contains("mismatch"),
            "unexpected grpc-status-details-bin mismatch: {raw_err}"
        );

        // 3) 400/401 Missing org header: GET /ingester/tsdb_metrics without X-Scope-OrgID
        let unauth_resp: HttpGrpcCallResponse = invoke_oracle(
            "httpgrpc-call",
            &HttpGrpcCallRequest {
                grpc_addr: grpc_addr.clone(),
                method: "GET".to_string(),
                url: "/ingester/tsdb_metrics".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            unauth_resp.code == 400 || unauth_resp.code == 401,
            "expected 400/401 for missing org header, got {:?}",
            unauth_resp
        );
        let raw_err = unauth_resp.raw_grpc_error.as_deref().unwrap_or("");
        assert!(
            !raw_err.contains("mismatch"),
            "unexpected grpc-status-details-bin mismatch: {raw_err}"
        );

        // 4) 503 Service Unavailable after POST /ingester/shutdown (while HTTP/gRPC stays up)
        let sd = http
            .post(format!("http://127.0.0.1:{rust_http_port}/ingester/shutdown"))
            .send()
            .await
            .unwrap();
        assert_eq!(sd.status().as_u16(), 204);

        let shutdown_ready_resp: HttpGrpcCallResponse = invoke_oracle(
            "httpgrpc-call",
            &HttpGrpcCallRequest {
                grpc_addr: grpc_addr.clone(),
                method: "GET".to_string(),
                url: "/ready".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            shutdown_ready_resp.code, 503,
            "expected 503 after shutdown: {shutdown_ready_resp:?}"
        );
        let raw_err = shutdown_ready_resp.raw_grpc_error.as_deref().unwrap_or("");
        assert!(
            !raw_err.contains("mismatch"),
            "unexpected grpc-status-details-bin mismatch: {raw_err}"
        );
        assert!(
            shutdown_ready_resp.headers.contains_key("Content-Type"),
            "expected canonical Content-Type header in 503 response: {:?}",
            shutdown_ready_resp.headers
        );

        rust_proc.send_sigterm();
        let exit = rust_proc.wait_for_exit(Duration::from_secs(15));
        assert_eq!(exit.code(), Some(0));
    });
}

fn generate_malformed_protobuf_frames() -> Vec<Vec<u8>> {
    let mut cases = Vec::with_capacity(55);
    // Exact underflow PoC
    cases.push(vec![0x0a, 0x04, 0x12, 0x01, 0x89, 0x00]);
    // Truncated varint / length-delimited / invalid wire types
    cases.push(vec![0x08]); // field 1 varint truncated
    cases.push(vec![0x08, 0x80, 0x80, 0x80, 0x80, 0x80]); // unterminated varint
    cases.push(vec![0x0a, 0xff, 0xff, 0xff, 0x0f, 0x01]); // length > remaining
    cases.push(vec![0x0e, 0x01, 0x02]); // wire type 6
    cases.push(vec![0x0f, 0x01, 0x02]); // wire type 7
    cases.push(vec![0x00]); // field number 0
    cases.push(vec![0x0a, 0x05, 0x0a, 0x10, 0x0a, 0x01, 0x61]); // nested TS underflow
    cases.push(vec![0x0a, 0x03, 0x12, 0x01, 0x09]); // truncated fixed64 sample
    cases.push(vec![0x0a, 0x03, 0x22, 0x01, 0x08]); // truncated histogram
    cases.push(vec![0x0a, 0x03, 0x1a, 0x01, 0x0a]); // truncated exemplar
    cases.push(vec![0x1a, 0x04, 0x12, 0x10, 0x61, 0x62]); // truncated metadata
    cases.push(vec![0x2a, 0x04, 0x0a, 0x10, 0x01, 0x02]); // truncated timeseries_rw2
    cases.push(vec![0x2a, 0x04, 0x08, 0xff, 0xff, 0x01]); // invalid rw2 symbol ref
    cases.push(vec![0x22, 0x02, 0x80, 0x80]); // truncated symbols_rw2
    cases.push(vec![0x0d, 0x01, 0x02]); // truncated fixed32
    cases.push(vec![0x09, 0x01, 0x02, 0x03]); // truncated fixed64

    // Deterministic pseudo-random/corrupted payloads up to 56 total frames (>50), each genuinely malformed
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    while cases.len() < 56 {
        let len = 4 + ((cases.len() * 7) % 48);
        let mut buf = Vec::with_capacity(len);
        // Use valid WriteRequest field tag with overflowing length or truncated nested field
        match cases.len() % 4 {
            0 => {
                // timeseries (tag 1, wire 2) with length exceeding remaining bytes
                buf.push(0x0a);
                buf.push((len + 5) as u8);
            }
            1 => {
                // timeseries_rw2 (tag 5, wire 2) with nested overflowing field
                buf.push(0x2a);
                buf.push((len - 2) as u8);
                buf.push(0x0a);
                buf.push(0x7f);
            }
            2 => {
                // metadata (tag 3, wire 2) with length exceeding remaining bytes
                buf.push(0x1a);
                buf.push((len + 3) as u8);
            }
            _ => {
                // symbols_rw2 (tag 4, wire 2) with unterminated varint length
                buf.push(0x22);
                buf.push(0x80);
            }
        }
        while buf.len() < len {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            buf.push((state >> 32) as u8);
        }
        cases.push(buf);
    }
    cases
}

#[test]
fn malformed_raw_protobuf_frames_neither_panic_nor_leak_inflight() {
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
    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();

    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    write_ingester_yaml(
        &go_yaml,
        go_http_port,
        go_grpc_port,
        &go_dir.join("tsdb"),
        &go_dir.join("bucket"),
        true,
        "anonymous",
        "",
    );
    write_ingester_yaml(
        &rust_yaml,
        rust_http_port,
        rust_grpc_port,
        &rust_dir.join("tsdb"),
        &rust_dir.join("bucket"),
        true,
        "anonymous",
        "",
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

        let malformed_frames = generate_malformed_protobuf_frames();
        assert!(malformed_frames.len() >= 51);

        let tenant = "tenant-malformed";
        for (i, payload) in malformed_frames.into_iter().enumerate() {
            let go_res = send_raw_grpc_unary(
                go_ch.clone(),
                "/cortex.Ingester/Push",
                Some(tenant),
                payload.clone(),
            )
            .await;
            let rust_res = send_raw_grpc_unary(
                rust_ch.clone(),
                "/cortex.Ingester/Push",
                Some(tenant),
                payload,
            )
            .await;
            assert!(
                go_res.is_err(),
                "frame {i}: expected Go to reject malformed frame"
            );
            assert!(
                rust_res.is_err(),
                "frame {i}: expected Rust to reject malformed frame"
            );
        }

        // Neither process should have exited/panicked
        assert!(go_proc.try_wait().is_none(), "go-mimir exited unexpectedly");
        assert!(
            rust_proc.try_wait().is_none(),
            "rust-mimir exited unexpectedly"
        );

        // Scrape /metrics on both and assert cortex_ingester_inflight_push_requests == 0
        let go_metrics = http
            .get(format!("http://127.0.0.1:{go_http_port}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let rust_metrics = http
            .get(format!("http://127.0.0.1:{rust_http_port}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let go_inflight = parse_prometheus_gauge(&go_metrics, "cortex_ingester_inflight_push_requests")
            .expect("go cortex_ingester_inflight_push_requests present");
        let rust_inflight =
            parse_prometheus_gauge(&rust_metrics, "cortex_ingester_inflight_push_requests")
                .expect("rust cortex_ingester_inflight_push_requests present");
        assert_eq!(go_inflight, 0.0, "go inflight leaked: {go_inflight}");
        assert_eq!(rust_inflight, 0.0, "rust inflight leaked: {rust_inflight}");

        // Subsequent valid Push + QueryStream succeed 100% on both
        let mut go_client = IngesterClient::new(go_ch);
        let mut rust_client = IngesterClient::new(rust_ch);
        let ts = now_ms() - 5_000;
        let valid_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "after_malformed_ok"),
                    cortexpb::LabelPair::from_static("env", "test"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 99.5,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client
            .push(with_tenant(tenant, valid_req.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant(tenant, valid_req))
            .await
            .unwrap();

        let matcher = vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "after_malformed_ok".to_string(),
        }];
        let go_q = run_query_stream(&mut go_client, tenant, ts - 1000, ts + 1000, matcher.clone()).await;
        let rust_q = run_query_stream(&mut rust_client, tenant, ts - 1000, ts + 1000, matcher).await;
        assert_eq!(rust_q, go_q);
        assert_eq!(rust_q.len(), 1);
        assert_eq!(rust_q[0].values, vec![DecodedVal::Float(ts, 99.5)]);
    });
}

#[test]
fn twin_process_multitenancy_disabled_no_auth_tenant_parity() {
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
    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();

    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    write_ingester_yaml(
        &go_yaml,
        go_http_port,
        go_grpc_port,
        &go_dir.join("tsdb"),
        &go_dir.join("bucket"),
        false,
        "single-tenant",
        "",
    );
    write_ingester_yaml(
        &rust_yaml,
        rust_http_port,
        rust_grpc_port,
        &rust_dir.join("tsdb"),
        &rust_dir.join("bucket"),
        false,
        "single-tenant",
        "",
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
        let mut go_client = IngesterClient::new(go_ch);
        let mut rust_client = IngesterClient::new(rust_ch);

        let ts = now_ms() - 5_000;
        let write_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "no_auth_metric"),
                    cortexpb::LabelPair::from_static("mode", "single"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 7.25,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        // Push WITHOUT x-scope-orgid metadata
        go_client
            .push(tonic::Request::new(write_req.clone()))
            .await
            .unwrap();
        rust_client
            .push(tonic::Request::new(write_req))
            .await
            .unwrap();

        // QueryStream WITHOUT x-scope-orgid metadata
        let q_req = ingesterpb::QueryRequest {
            start_timestamp_ms: ts - 1000,
            end_timestamp_ms: ts + 1000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".to_string(),
                value: "no_auth_metric".to_string(),
            }],
            streaming_chunks_batch_size: 4,
        };
        let go_q = run_query_stream_raw(&mut go_client, tonic::Request::new(q_req.clone())).await;
        let rust_q = run_query_stream_raw(&mut rust_client, tonic::Request::new(q_req)).await;
        assert_eq!(rust_q, go_q);
        assert_eq!(rust_q.len(), 1);
        assert_eq!(rust_q[0].values, vec![DecodedVal::Float(ts, 7.25)]);

        // Verify AllUserStats stored under "single-tenant" on both
        let go_all = go_client
            .all_user_stats(tonic::Request::new(ingesterpb::UserStatsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        let rust_all = rust_client
            .all_user_stats(tonic::Request::new(ingesterpb::UserStatsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(go_all.stats.len(), 1);
        assert_eq!(rust_all.stats.len(), 1);
        assert_eq!(go_all.stats[0].user_id, "single-tenant");
        assert_eq!(rust_all.stats[0].user_id, "single-tenant");
        assert_eq!(
            rust_all.stats[0].data.as_ref().map(|d| d.num_series),
            go_all.stats[0].data.as_ref().map(|d| d.num_series)
        );
        assert_eq!(rust_all.stats[0].data.as_ref().unwrap().num_series, 1);

        // UserStats without org header also resolves to "single-tenant"
        let go_us = go_client
            .user_stats(tonic::Request::new(ingesterpb::UserStatsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        let rust_us = rust_client
            .user_stats(tonic::Request::new(ingesterpb::UserStatsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rust_us.num_series, go_us.num_series);
        assert_eq!(rust_us.num_series, 1);
    });
}

fn encode_varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn encode_len_field(field_num: u32, bytes: &[u8], out: &mut Vec<u8>) {
    encode_varint(((field_num as u64) << 3) | 2, out);
    encode_varint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

fn build_raw_metadata_write_request(
    metric_type: u64,
    family: &[u8],
    help: &[u8],
    unit: &[u8],
) -> Vec<u8> {
    let mut meta = Vec::new();
    // field 1: type (varint, wire type 0)
    encode_varint(1 << 3, &mut meta);
    encode_varint(metric_type, &mut meta);
    // field 2: metric_family_name
    encode_len_field(2, family, &mut meta);
    // field 4: help (raw bytes, potentially non-UTF-8!)
    encode_len_field(4, help, &mut meta);
    // field 5: unit
    encode_len_field(5, unit, &mut meta);

    let mut wr = Vec::new();
    // field 3: repeated MetricMetadata metadata
    encode_len_field(3, &meta, &mut wr);
    wr
}

#[test]
fn twin_process_adversarial_edge_cases_parity() {
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
    let rust_http_port = free_tcp_port();
    let rust_grpc_port = free_tcp_port();

    let extra_limits = "  past_grace_period: 5m\n  out_of_order_time_window: 0s";
    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    write_ingester_yaml(
        &go_yaml,
        go_http_port,
        go_grpc_port,
        &go_dir.join("tsdb"),
        &go_dir.join("bucket"),
        true,
        "anonymous",
        extra_limits,
    );
    write_ingester_yaml(
        &rust_yaml,
        rust_http_port,
        rust_grpc_port,
        &rust_dir.join("tsdb"),
        &rust_dir.join("bucket"),
        true,
        "anonymous",
        extra_limits,
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

        let tenant = "adv-tenant";
        let now = now_ms();
        let t0 = now - 10_000;
        let t1 = now - 5_000;
        let t_too_old = now - 10 * 60 * 1000; // older than past_grace_period (5m)

        // (i) Duplicate timestamps & past_grace_period mixed with valid samples
        let seed_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "adv_dup_past"),
                    cortexpb::LabelPair::from_static("env", "prod"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 100.0,
                    timestamp_ms: t0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client
            .push(with_tenant(tenant, seed_req.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant(tenant, seed_req))
            .await
            .unwrap();

        // Identical duplicate timestamp + value => 200 OK on both
        let dup_same = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "adv_dup_past"),
                    cortexpb::LabelPair::from_static("env", "prod"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 100.0,
                    timestamp_ms: t0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client
            .push(with_tenant(tenant, dup_same.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant(tenant, dup_same))
            .await
            .unwrap();

        // Duplicate timestamp with DIFFERENT value => identical error & ErrorDetails parity
        let dup_diff = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "adv_dup_past"),
                    cortexpb::LabelPair::from_static("env", "prod"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 200.0,
                    timestamp_ms: t0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let go_dup_err = go_client
            .push(with_tenant(tenant, dup_diff.clone()))
            .await
            .unwrap_err();
        let rust_dup_err = rust_client
            .push(with_tenant(tenant, dup_diff))
            .await
            .unwrap_err();
        assert_eq!(rust_dup_err.code(), go_dup_err.code());
        let go_det = grpcutilpb::extract_cortex_error_details(&go_dup_err).unwrap();
        let rust_det = grpcutilpb::extract_cortex_error_details(&rust_dup_err).unwrap();
        assert_eq!(rust_det.cause, go_det.cause);
        assert_eq!(rust_det.soft, go_det.soft);
        assert_eq!(rust_det.rejected_samples, go_det.rejected_samples);

        // Mixed batch: SampleTooFarInPast + valid sample => 200 OK on both!
        let mixed_past_and_valid = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "adv_dup_past"),
                    cortexpb::LabelPair::from_static("env", "prod"),
                ],
                samples: vec![
                    cortexpb::Sample {
                        value: 999.0,
                        timestamp_ms: t_too_old,
                    },
                    cortexpb::Sample {
                        value: 101.0,
                        timestamp_ms: t1,
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client
            .push(with_tenant(tenant, mixed_past_and_valid.clone()))
            .await
            .unwrap();
        rust_client
            .push(with_tenant(tenant, mixed_past_and_valid))
            .await
            .unwrap();

        let matcher = vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "adv_dup_past".to_string(),
        }];
        let go_q = run_query_stream(
            &mut go_client,
            tenant,
            t_too_old - 1000,
            now + 1000,
            matcher.clone(),
        )
        .await;
        let rust_q = run_query_stream(
            &mut rust_client,
            tenant,
            t_too_old - 1000,
            now + 1000,
            matcher.clone(),
        )
        .await;
        assert_eq!(rust_q, go_q);
        assert_eq!(
            rust_q[0].values,
            vec![DecodedVal::Float(t0, 100.0), DecodedVal::Float(t1, 101.0)]
        );

        // (ii) Non-UTF-8 bytes in MetricMetadata.help
        let raw_meta_wr = build_raw_metadata_write_request(
            1, // COUNTER
            b"non_utf8_meta",
            b"help with invalid utf8 \xff\xfe bytes",
            b"bytes",
        );
        send_raw_grpc_unary(
            go_ch.clone(),
            "/cortex.Ingester/Push",
            Some(tenant),
            raw_meta_wr.clone(),
        )
        .await
        .expect("go must accept non-UTF-8 MetricMetadata.help");
        send_raw_grpc_unary(
            rust_ch.clone(),
            "/cortex.Ingester/Push",
            Some(tenant),
            raw_meta_wr,
        )
        .await
        .expect("rust must accept non-UTF-8 MetricMetadata.help");

        let rust_meta = rust_client
            .metrics_metadata(with_tenant(
                tenant,
                ingesterpb::MetricsMetadataRequest {
                    limit: 100,
                    metric: "non_utf8_meta".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rust_meta.metadata.len(), 1);
        assert_eq!(rust_meta.metadata[0].metric_family_name, "non_utf8_meta");
        assert_eq!(rust_meta.metadata[0].unit, "bytes");

        // Verify Go also stored the metadata (inspecting raw MetricsMetadataResponse bytes since
        // Go preserves raw non-UTF-8 bytes in proto3 string)
        let go_meta_req = ingesterpb::MetricsMetadataRequest {
            limit: 100,
            metric: "non_utf8_meta".to_string(),
            ..Default::default()
        }
        .encode_to_vec();
        let go_meta_raw = send_raw_grpc_unary(
            go_ch.clone(),
            "/cortex.Ingester/MetricsMetadata",
            Some(tenant),
            go_meta_req,
        )
        .await
        .unwrap();
        assert!(
            go_meta_raw.windows(b"non_utf8_meta".len()).any(|w| w == b"non_utf8_meta"),
            "go MetricsMetadataResponse missing non_utf8_meta"
        );

        // (iii) SIGKILL recovery with a deliberately torn multi-page WAL record on Rust WAL segment
        // Push an extra sample to a dedicated tenant so its WAL has clean committed series + samples.
        let wal_tenant = "wal-crash-tenant";
        let crash_samples: Vec<cortexpb::Sample> = (0..10)
            .map(|i| cortexpb::Sample {
                value: (i as f64) * 1.5,
                timestamp_ms: now - 20_000 + i * 1000,
            })
            .collect();
        rust_client
            .push(with_tenant(
                wal_tenant,
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "wal_crash_metric"),
                            cortexpb::LabelPair::from_static("id", "torn-tail"),
                        ],
                        samples: crash_samples.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        drop(rust_client);
        drop(rust_ch);
        rust_proc.sigkill_and_wait();

        // Find the latest WAL segment for wal-crash-tenant and append a torn multi-page record.
        let wal_dir = rust_dir.join("tsdb").join(wal_tenant).join("wal");
        let mut seg_paths: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|s| s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()))
            })
            .collect();
        seg_paths.sort();
        let last_seg = seg_paths.pop().expect("WAL segment exists after push");
        let seg_len = fs::metadata(&last_seg).unwrap().len() as usize;
        assert!(seg_len > 0, "WAL segment should be non-empty");

        let mut f = OpenOptions::new().append(true).open(&last_seg).unwrap();
        let mut rem = PAGE_SIZE - (seg_len % PAGE_SIZE);
        if rem < RECORD_HEADER_SIZE + 8 {
            // Pad remainder of current page with REC_PAGE_TERM zeros so REC_FIRST starts cleanly on next page
            f.write_all(&vec![0u8; rem]).unwrap();
            rem = PAGE_SIZE;
        }
        // Page N: valid REC_FIRST fragment filling the rest of the page
        let first_payload_len = rem - RECORD_HEADER_SIZE;
        let first_payload = vec![0xABu8; first_payload_len];
        let first_crc = crc32c::crc32c(&first_payload);
        let mut rec_first = Vec::with_capacity(rem);
        rec_first.push(REC_FIRST);
        rec_first.extend_from_slice(&(first_payload_len as u16).to_be_bytes());
        rec_first.extend_from_slice(&first_crc.to_be_bytes());
        rec_first.extend_from_slice(&first_payload);
        f.write_all(&rec_first).unwrap();

        // Page N+1: torn REC_LAST fragment (header claims 64 bytes, only 10 bytes present)
        let mut torn_last = Vec::new();
        torn_last.push(REC_LAST);
        torn_last.extend_from_slice(&64u16.to_be_bytes());
        torn_last.extend_from_slice(&0x1234_5678u32.to_be_bytes());
        torn_last.extend_from_slice(&[0xCDu8; 10]);
        f.write_all(&torn_last).unwrap();
        f.sync_all().unwrap();
        drop(f);

        // Respawn rust-mimir on the same directory; it must repair the torn WAL tail and reach /ready == 200.
        let rust_http_port2 = free_tcp_port();
        let rust_grpc_port2 = free_tcp_port();
        write_ingester_yaml(
            &rust_yaml,
            rust_http_port2,
            rust_grpc_port2,
            &rust_dir.join("tsdb"),
            &rust_dir.join("bucket"),
            true,
            "anonymous",
            extra_limits,
        );
        let mut rust_proc2 = ProcessGuard::spawn(
            "rust-mimir-recovered",
            &rust_mimir_release_bin(),
            &[
                "-target=ingester".to_string(),
                format!("-config.file={}", rust_yaml.display()),
            ],
            &rust_dir,
        );
        wait_for_ready(&http, rust_http_port2, &mut rust_proc2).await;

        // Confirm repair_wal actually ran and created the next clean segment (00000001)
        assert!(
            wal_dir.join("00000001").exists(),
            "expected repair_wal to rotate/create 00000001 after repairing torn tail"
        );

        let rust_ch2 = Channel::from_shared(format!("http://127.0.0.1:{rust_grpc_port2}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut rust_client2 = IngesterClient::new(rust_ch2);

        let wal_matcher = vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "wal_crash_metric".to_string(),
        }];
        let recovered = run_query_stream(
            &mut rust_client2,
            wal_tenant,
            now - 60_000,
            now + 60_000,
            wal_matcher.clone(),
        )
        .await;
        assert_eq!(recovered.len(), 1);
        let expected_vals: Vec<DecodedVal> = crash_samples
            .iter()
            .map(|s| DecodedVal::Float(s.timestamp_ms, s.value))
            .collect();
        assert_eq!(
            recovered[0].values, expected_vals,
            "all previously committed samples must survive torn multi-page WAL repair"
        );

        // Verify repaired WAL remains writable
        let t_post = now - 2_000;
        rust_client2
            .push(with_tenant(
                wal_tenant,
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "wal_crash_metric"),
                            cortexpb::LabelPair::from_static("id", "torn-tail"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 777.0,
                            timestamp_ms: t_post,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let after_post = run_query_stream(
            &mut rust_client2,
            wal_tenant,
            now - 60_000,
            now + 60_000,
            wal_matcher,
        )
        .await;
        assert_eq!(after_post[0].values.len(), expected_vals.len() + 1);
        assert_eq!(
            after_post[0].values.last(),
            Some(&DecodedVal::Float(t_post, 777.0))
        );

        rust_proc2.send_sigterm();
        let exit2 = rust_proc2.wait_for_exit(Duration::from_secs(15));
        assert_eq!(exit2.code(), Some(0));
    });
}
