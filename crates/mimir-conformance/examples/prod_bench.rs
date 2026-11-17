//! Realistic Mimir 3.2.0 Production Kafka `ingest_storage` Benchmark (Go vs Rust).
//!
//! Models the full production profile:
//! - Write path: Kafka `ingest_storage` v2 ONLY (`push_grpc_method_enabled: false`,
//!   `ingester-zone-a-0` owning partition 0 in the partition ring)
//! - Record Version = 2 (`V2CommonSymbols` offset 64 + RW2 symbol table + Zstd RecordBatch v2),
//!   ~500 samples/record, 10 K8s labels, ~95 metadata/record over 1,600 metric families,
//!   0.07% OOO samples, ~1%/h new-series churn, 2 tenants (`prod` 99.5%, `preprod` 0.5%)
//! - Historical local ULID blocks (`--local-blocks`, default 4, up to 28) on disk
//! - 4 Phases per engine (Go `target/go-mimir`, then Rust `target/release/mimir`):
//!   1. Preload & Unthrottled Backlog Catch-up + Go `/debug/pprof/heap` decomposition
//!   2. Steady-state 327k samples/s + concurrent bimodal gRPC read mix (small ~7, med ~700,
//!      heavy ~28k series, 42% repeated matchers, plus LabelNamesAndValues /
//!      LabelValuesCardinality / UserStats)
//!   3. Head Compaction Under Live Load (`POST /ingester/flush?wait=true` + 50ms RSS sawtooth)
//!   4. `SIGKILL` Cold Restart & 5s Kafka Backlog Catch-up
//!
//! Run via: `just prod-bench --series 2000000 --steady-secs 30`

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, CreateTopicsRequest,
    CreateTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin,
    rust_mimir_release_bin,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use mimir_proto::symbols::{
    v2_common_symbols, RECORD_VERSION_HEADER_KEY, V2_RECORD_SYMBOL_OFFSET,
};
use mimir_proto::zerocopy::from_write_request_to_rw2;
use mimir_storage::ingest::compression::{compress_record_batch, decompress_record_batch};
use mimir_storage::ingest::{
    record_version_header, KafkaClient, KafkaCompression, ProduceRecord, RecordVersion,
};
use mimir_tsdb::block::BlockMetaCompaction;
use mimir_tsdb::chunkenc::{EncodedChunk, XorChunk};
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::compact::{write_block_from_series, CompactionSeries};
use mimir_tsdb::labels::Labels;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const TWO_HOURS_MS: i64 = 2 * 60 * 60 * 1000;
const SAMPLES_PER_RECORD: usize = 500;
const METADATA_PER_RECORD: usize = 95;
const NUM_METRIC_FAMILIES: usize = 1_600;
const HEAVY_TARGET_SERIES: usize = 28_000;
const SERIES_PER_POD: usize = 7;
const BROKER_MAX_RETAINED_BYTES: usize = 320 * 1024 * 1024;

// ---------------------------------------------------------------------------
// CLI Config
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct ProdBenchConfig {
    series: usize,
    steady_secs: u64,
    local_blocks: usize,
    rate_samples_per_sec: usize,
    brokers: Option<String>,
    topic: String,
    run_go: bool,
    run_rust: bool,
    no_reads: bool,
    work_dir: Option<PathBuf>,
    perf_out: Option<String>,
}

impl Default for ProdBenchConfig {
    fn default() -> Self {
        Self {
            series: 2_000_000,
            steady_secs: 30,
            local_blocks: 4,
            rate_samples_per_sec: 327_000,
            brokers: None,
            topic: "mimir-prod-bench".to_string(),
            run_go: true,
            run_rust: true,
            no_reads: false,
            work_dir: None,
            perf_out: None,
        }
    }
}

fn parse_args() -> ProdBenchConfig {
    let mut cfg = ProdBenchConfig::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let next_str = |idx: &mut usize| -> String {
            *idx += 1;
            args.get(*idx).expect("missing flag value").clone()
        };
        let next_usize = |idx: &mut usize| -> usize {
            *idx += 1;
            args.get(*idx)
                .expect("missing flag value")
                .parse()
                .expect("invalid integer flag value")
        };
        match args[i].as_str() {
            "--series" => cfg.series = next_usize(&mut i),
            "--steady-secs" => cfg.steady_secs = next_usize(&mut i) as u64,
            "--local-blocks" => cfg.local_blocks = next_usize(&mut i),
            "--rate" => cfg.rate_samples_per_sec = next_usize(&mut i),
            "--brokers" => cfg.brokers = Some(next_str(&mut i)),
            "--topic" => cfg.topic = next_str(&mut i),
            "--go-only" => {
                cfg.run_go = true;
                cfg.run_rust = false;
            }
            "--rust-only" => {
                cfg.run_go = false;
                cfg.run_rust = true;
            }
            "--no-reads" => {
                cfg.no_reads = true;
            }
            "--work-dir" => cfg.work_dir = Some(PathBuf::from(next_str(&mut i))),
            "--perf-out" => cfg.perf_out = Some(next_str(&mut i)),
            other => panic!("unknown prod_bench flag: {other}"),
        }
        i += 1;
    }
    cfg
}

// ---------------------------------------------------------------------------
// Process + /proc/<pid>/{stat,status} + Prometheus /metrics helpers
// ---------------------------------------------------------------------------

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

    fn sigkill_and_reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
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

#[derive(Clone, Copy, Debug, Default)]
struct ProcStats {
    cpu_secs: f64,
    rss_anon_mib: f64,
    rss_file_mib: f64,
    vm_rss_mib: f64,
    vm_hwm_mib: f64,
}

fn read_proc_stats(pid: u32) -> ProcStats {
    let stat_raw = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let cpu_secs = if let Some(rparen) = stat_raw.rfind(')') {
        let after = &stat_raw[rparen + 2..];
        let fields: Vec<&str> = after.split_whitespace().collect();
        if fields.len() > 12 {
            let utime: u64 = fields[11].parse().unwrap_or(0);
            let stime: u64 = fields[12].parse().unwrap_or(0);
            (utime + stime) as f64 / 100.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let status_raw = fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let parse_kb = |line: &str, prefix: &str| -> Option<f64> {
        line.strip_prefix(prefix)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
            .map(|kb| kb as f64 / 1024.0)
    };

    let mut st = ProcStats {
        cpu_secs,
        ..Default::default()
    };
    for line in status_raw.lines() {
        if let Some(v) = parse_kb(line, "RssAnon:") {
            st.rss_anon_mib = v;
        } else if let Some(v) = parse_kb(line, "RssFile:") {
            st.rss_file_mib = v;
        } else if let Some(v) = parse_kb(line, "VmRSS:") {
            st.vm_rss_mib = v;
        } else if let Some(v) = parse_kb(line, "VmHWM:") {
            st.vm_hwm_mib = v;
        }
    }
    st
}

#[derive(Debug, Default, Clone)]
struct ScrapedMetrics {
    memory_series: f64,
    ingested_samples_total: f64,
    last_consumed_offset: Option<i64>,
    consume_delay_p50_s: Option<f64>,
    consume_delay_p99_s: Option<f64>,
    record_proc_p50_ms: Option<f64>,
    record_proc_p99_ms: Option<f64>,
    postings_cache_hits: Option<f64>,
    postings_cache_misses: Option<f64>,
}

fn parse_prom_label(labels_str: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=\"");
    let start = labels_str.find(&needle)? + needle.len();
    let rest = &labels_str[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn histogram_quantile_from_buckets(buckets: &BTreeMap<String, f64>, q: f64) -> Option<f64> {
    let mut pts: Vec<(f64, f64)> = Vec::new();
    let mut inf_count = None;
    for (le_str, &cnt) in buckets {
        if le_str == "+Inf" {
            inf_count = Some(cnt);
        } else if let Ok(v) = le_str.parse::<f64>() {
            pts.push((v, cnt));
        }
    }
    pts.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total = inf_count.or_else(|| pts.last().map(|p| p.1))?;
    if total <= 0.0 || pts.is_empty() {
        return None;
    }
    let rank = q * total;
    let mut prev_b = 0.0;
    let mut prev_c = 0.0;
    for &(upper, count) in &pts {
        if count >= rank {
            if count <= prev_c {
                return Some(upper);
            }
            let frac = (rank - prev_c) / (count - prev_c);
            return Some(prev_b + frac * (upper - prev_b));
        }
        prev_b = upper;
        prev_c = count;
    }
    pts.last().map(|p| p.0)
}

async fn scrape_metrics(http: &reqwest::Client, http_port: u16) -> Option<ScrapedMetrics> {
    let text = http
        .get(format!("http://127.0.0.1:{http_port}/metrics"))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;

    let mut out = ScrapedMetrics::default();
    let mut delay_buckets: BTreeMap<String, f64> = BTreeMap::new();
    let mut proc_buckets_reader: BTreeMap<String, f64> = BTreeMap::new();
    let mut proc_buckets_pusher: BTreeMap<String, f64> = BTreeMap::new();

    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((metric_and_labels, val_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(val) = val_str.trim().parse::<f64>() else {
            continue;
        };
        let (name, labels_str) = match metric_and_labels.find('{') {
            Some(idx) => (&metric_and_labels[..idx], &metric_and_labels[idx..]),
            None => (metric_and_labels, ""),
        };

        match name {
            "cortex_ingester_memory_series" => out.memory_series = val,
            "cortex_ingester_ingested_samples_total" => out.ingested_samples_total += val,
            "cortex_ingest_storage_reader_last_consumed_offset" => {
                out.last_consumed_offset = Some(val as i64);
            }
            "cortex_ingest_storage_reader_receive_delay_seconds_bucket" => {
                if let Some(le) = parse_prom_label(labels_str, "le") {
                    *delay_buckets.entry(le).or_insert(0.0) += val;
                }
            }
            "cortex_ingest_storage_reader_records_processing_time_seconds_bucket"
            | "cortex_ingest_storage_reader_batch_consume_duration_seconds_bucket"
            | "cortex_ingest_storage_reader_consume_duration_seconds_bucket" => {
                if let Some(le) = parse_prom_label(labels_str, "le") {
                    *proc_buckets_reader.entry(le).or_insert(0.0) += val;
                }
            }
            "cortex_ingest_storage_reader_pusher_processing_time_seconds_bucket"
            | "cortex_ingest_storage_pusher_processing_duration_seconds_bucket" => {
                if let Some(le) = parse_prom_label(labels_str, "le") {
                    *proc_buckets_pusher.entry(le).or_insert(0.0) += val;
                }
            }
            "cortex_ingester_tsdb_head_postings_for_matchers_cache_hits_total"
            | "cortex_ingester_tsdb_block_postings_for_matchers_cache_hits_total" => {
                *out.postings_cache_hits.get_or_insert(0.0) += val;
            }
            "cortex_ingester_tsdb_head_postings_for_matchers_cache_misses_total"
            | "cortex_ingester_tsdb_block_postings_for_matchers_cache_misses_total" => {
                *out.postings_cache_misses.get_or_insert(0.0) += val;
            }
            _ => {}
        }
    }

    out.consume_delay_p50_s = histogram_quantile_from_buckets(&delay_buckets, 0.50);
    out.consume_delay_p99_s = histogram_quantile_from_buckets(&delay_buckets, 0.99);

    let proc_buckets = if !proc_buckets_reader.is_empty() {
        &proc_buckets_reader
    } else {
        &proc_buckets_pusher
    };
    out.record_proc_p50_ms = histogram_quantile_from_buckets(proc_buckets, 0.50).map(|s| s * 1000.0);
    out.record_proc_p99_ms = histogram_quantile_from_buckets(proc_buckets, 0.99).map(|s| s * 1000.0);

    Some(out)
}

// ---------------------------------------------------------------------------
// High-Speed In-Process Async TCP Kafka v2 Broker (<400 MiB RAM ring)
// ---------------------------------------------------------------------------

struct StoredBatch {
    base_offset: i64,
    end_offset: i64,
    max_timestamp: i64,
    raw_batch: Bytes,
}

#[derive(Default)]
struct PartitionLog {
    batches: VecDeque<StoredBatch>,
    log_start_offset: i64,
    next_offset: i64,
    retained_bytes: usize,
    total_bytes_produced: u64,
    total_records_produced: u64,
    committed_offsets: HashMap<String, i64>,
}

struct InMemoryKafkaBroker {
    topic: String,
    port: u16,
    addr: String,
    log: Mutex<PartitionLog>,
    notify: Notify,
    space_notify: Notify,
    shutdown: AtomicBool,
}

impl InMemoryKafkaBroker {
    async fn start(topic: String) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind in-memory kafka broker");
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}");
        let broker = Arc::new(Self {
            topic,
            port,
            addr,
            log: Mutex::new(PartitionLog::default()),
            notify: Notify::new(),
            space_notify: Notify::new(),
            shutdown: AtomicBool::new(false),
        });

        let b = Arc::clone(&broker);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                if b.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let _ = stream.set_nodelay(true);
                let b_conn = Arc::clone(&b);
                tokio::spawn(async move {
                    if let Err(e) = handle_kafka_conn(b_conn, stream).await {
                        eprintln!("[broker] conn error: {e:#}");
                    }
                });
            }
        });

        broker
    }

    fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.notify.notify_waiters();
        self.space_notify.notify_waiters();
    }

    fn stats(&self) -> (i64, usize, u64, u64) {
        let g = self.log.lock();
        (
            g.next_offset,
            g.retained_bytes,
            g.total_records_produced,
            g.total_bytes_produced,
        )
    }

    fn next_offset(&self) -> i64 {
        self.log.lock().next_offset
    }

    /// Appends a pre-encoded Zstd RecordBatch v2 directly to the broker ring, patching
    /// `baseOffset` (bytes 0..8, which sits outside the RecordBatch CRC32C).
    /// Applies bounded ring eviction against committed offsets when > `BROKER_MAX_RETAINED_BYTES`.
    async fn append_encoded_records_batch(
        &self,
        records: Vec<ProduceRecord>,
        compression: KafkaCompression,
        respect_ring_cap: bool,
    ) -> i64 {
        let num_records = records.len() as i64;
        if num_records == 0 {
            return self.next_offset();
        }
        let max_ts = records
            .iter()
            .filter_map(|r| r.timestamp_ms)
            .max()
            .unwrap_or_else(now_ms);

        let proto_records: Vec<Record> = records
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                let mut headers = kafka_protocol::indexmap::IndexMap::with_capacity(r.headers.len());
                for (k, v) in r.headers {
                    headers.insert(StrBytes::from_string(k), Some(v));
                }
                Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    producer_id: NO_PRODUCER_ID,
                    producer_epoch: NO_PRODUCER_EPOCH,
                    timestamp_type: TimestampType::Creation,
                    offset: i as i64,
                    sequence: i as i32,
                    timestamp: r.timestamp_ms.unwrap_or(max_ts),
                    key: r.key,
                    value: r.value,
                    headers,
                }
            })
            .collect();

        let mut batch_buf = BytesMut::with_capacity(64 * 1024);
        let encode_opts = RecordEncodeOptions {
            version: 2,
            compression: compression.to_protocol(),
        };
        RecordBatchEncoder::encode_with_custom_compression(
            &mut batch_buf,
            proto_records.iter(),
            &encode_opts,
            Some(compress_record_batch),
        )
        .expect("encode kafka RecordBatch v2");

        // Wait briefly if un-evictable buffer exceeds cap
        if respect_ring_cap {
            loop {
                let should_wait = {
                    let mut g = self.log.lock();
                    evict_committed_batches(&mut g);
                    g.retained_bytes >= BROKER_MAX_RETAINED_BYTES
                };
                if !should_wait || self.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = self.space_notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        }

        let mut g = self.log.lock();
        let base_offset = g.next_offset;
        let end_offset = base_offset + num_records;
        // Patch baseOffset in bytes 0..8 (outside CRC32C)
        batch_buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
        let raw_batch = batch_buf.freeze();
        let blen = raw_batch.len();
        g.next_offset = end_offset;
        g.retained_bytes += blen;
        g.total_bytes_produced += blen as u64;
        g.total_records_produced += num_records as u64;
        g.batches.push_back(StoredBatch {
            base_offset,
            end_offset,
            max_timestamp: max_ts,
            raw_batch,
        });
        evict_committed_batches(&mut g);
        drop(g);
        self.notify.notify_waiters();
        end_offset - 1
    }
}

fn evict_committed_batches(g: &mut PartitionLog) {
    if g.retained_bytes <= BROKER_MAX_RETAINED_BYTES {
        return;
    }
    let min_committed = g.committed_offsets.values().copied().min().unwrap_or(-1);
    if min_committed < 0 {
        return;
    }
    let safe_floor = min_committed.saturating_sub(256);
    while g.retained_bytes > BROKER_MAX_RETAINED_BYTES / 2 {
        let Some(front) = g.batches.front() else {
            break;
        };
        if front.end_offset <= safe_floor {
            let popped = g.batches.pop_front().unwrap();
            g.retained_bytes = g.retained_bytes.saturating_sub(popped.raw_batch.len());
            g.log_start_offset = popped.end_offset;
        } else {
            break;
        }
    }
}

async fn handle_kafka_conn(broker: Arc<InMemoryKafkaBroker>, stream: TcpStream) -> anyhow::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let mut len_buf = [0u8; 4];

    loop {
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if !(4..=256 * 1024 * 1024).contains(&frame_len) {
            break;
        }
        let mut frame = BytesMut::zeroed(frame_len);
        if reader.read_exact(&mut frame).await.is_err() {
            break;
        }

        let api_key_raw = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let mut cursor = frame.freeze();

        let resp_bytes = dispatch_kafka_frame(&broker, api_key_raw, api_version, &mut cursor).await?;
        writer.write_all(&resp_bytes).await?;
    }
    Ok(())
}

fn encode_kafka_response<Resp: Encodable + HeaderVersion>(
    corr_id: i32,
    api_version: i16,
    resp_header_ver: i16,
    resp: &Resp,
) -> anyhow::Result<Bytes> {
    let mut out = BytesMut::with_capacity(256);
    out.put_u32(0);
    let mut hdr = ResponseHeader::default();
    hdr.correlation_id = corr_id;
    hdr.encode(&mut out, resp_header_ver)?;
    resp.encode(&mut out, api_version)?;
    let frame_len = (out.len() - 4) as u32;
    out[0..4].copy_from_slice(&frame_len.to_be_bytes());
    Ok(out.freeze())
}

async fn dispatch_kafka_frame(
    broker: &InMemoryKafkaBroker,
    api_key_raw: i16,
    api_version: i16,
    buf: &mut Bytes,
) -> anyhow::Result<Bytes> {
    let api_key = ApiKey::try_from(api_key_raw).map_err(|_| anyhow::anyhow!("unknown api key {api_key_raw}"))?;

    match api_key {
        ApiKey::ApiVersions => {
            let hdr = RequestHeader::decode(buf, ApiVersionsRequest::header_version(api_version))?;
            let _req = ApiVersionsRequest::decode(buf, api_version)?;
            let mut resp = ApiVersionsResponse::default();
            resp.error_code = 0;
            let specs = [
                (ApiKey::Produce as i16, 3, 9),
                (ApiKey::Fetch as i16, 4, 11),
                (ApiKey::ListOffsets as i16, 1, 7),
                (ApiKey::Metadata as i16, 1, 9),
                (ApiKey::OffsetCommit as i16, 2, 8),
                (ApiKey::OffsetFetch as i16, 1, 7),
                (ApiKey::FindCoordinator as i16, 0, 3),
                (ApiKey::ApiVersions as i16, 0, 3),
                (ApiKey::CreateTopics as i16, 0, 5),
            ];
            for (k, min_v, max_v) in specs {
                let mut av = ApiVersion::default();
                av.api_key = k;
                av.min_version = min_v;
                av.max_version = max_v;
                resp.api_keys.push(av);
            }
            // ApiVersionsResponse always uses ResponseHeader v0
            encode_kafka_response(hdr.correlation_id, api_version, 0, &resp)
        }
        ApiKey::Metadata => {
            let hdr = RequestHeader::decode(buf, MetadataRequest::header_version(api_version))?;
            let req = MetadataRequest::decode(buf, api_version)?;
            let mut resp = MetadataResponse::default();
            resp.cluster_id = Some(StrBytes::from_static_str("prod-bench-kafka"));
            resp.controller_id = BrokerId(0);

            let mut b = MetadataResponseBroker::default();
            b.node_id = BrokerId(0);
            b.host = StrBytes::from_static_str("127.0.0.1");
            b.port = broker.port as i32;
            resp.brokers.push(b);

            let requested_topics: Vec<String> = match req.topics {
                Some(list) if !list.is_empty() => list
                    .into_iter()
                    .filter_map(|t| t.name.map(|n| n.0.as_str().to_owned()))
                    .collect(),
                _ => vec![broker.topic.clone()],
            };

            for tname in requested_topics {
                let mut mt = MetadataResponseTopic::default();
                mt.name = Some(TopicName(StrBytes::from_string(tname.clone())));
                if tname == broker.topic {
                    mt.error_code = 0;
                    let mut mp = MetadataResponsePartition::default();
                    mp.error_code = 0;
                    mp.partition_index = 0;
                    mp.leader_id = BrokerId(0);
                    mp.leader_epoch = 0;
                    mp.replica_nodes = vec![BrokerId(0)];
                    mp.isr_nodes = vec![BrokerId(0)];
                    mt.partitions.push(mp);
                } else {
                    mt.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }
                resp.topics.push(mt);
            }
            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                MetadataResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::FindCoordinator => {
            let hdr =
                RequestHeader::decode(buf, FindCoordinatorRequest::header_version(api_version))?;
            let _req = FindCoordinatorRequest::decode(buf, api_version)?;
            let mut resp = FindCoordinatorResponse::default();
            resp.error_code = 0;
            resp.node_id = BrokerId(0);
            resp.host = StrBytes::from_static_str("127.0.0.1");
            resp.port = broker.port as i32;
            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                FindCoordinatorResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::ListOffsets => {
            let hdr = RequestHeader::decode(buf, ListOffsetsRequest::header_version(api_version))?;
            let req = ListOffsetsRequest::decode(buf, api_version)?;
            let mut resp = ListOffsetsResponse::default();
            let g = broker.log.lock();

            for rt in req.topics {
                let tname = rt.name.0.as_str().to_owned();
                let mut lt = ListOffsetsTopicResponse::default();
                lt.name = TopicName(StrBytes::from_string(tname.clone()));

                for rp in rt.partitions {
                    let mut lp = ListOffsetsPartitionResponse::default();
                    lp.partition_index = rp.partition_index;
                    if tname != broker.topic || rp.partition_index != 0 {
                        lp.error_code = 3;
                    } else if rp.timestamp == -2 {
                        lp.offset = g.log_start_offset;
                        lp.timestamp = -1;
                    } else if rp.timestamp == -1 {
                        lp.offset = g.next_offset;
                        lp.timestamp = -1;
                    } else {
                        let found = g
                            .batches
                            .iter()
                            .find(|b| b.max_timestamp >= rp.timestamp);
                        if let Some(b) = found {
                            lp.offset = b.base_offset;
                            lp.timestamp = b.max_timestamp;
                        } else {
                            lp.offset = -1;
                            lp.timestamp = -1;
                        }
                    }
                    lt.partitions.push(lp);
                }
                resp.topics.push(lt);
            }

            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                ListOffsetsResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::OffsetFetch => {
            let hdr = RequestHeader::decode(buf, OffsetFetchRequest::header_version(api_version))?;
            let req = OffsetFetchRequest::decode(buf, api_version)?;
            let group_id = req.group_id.0.as_str().to_owned();
            let mut resp = OffsetFetchResponse::default();
            let g = broker.log.lock();

            let topic_list: Vec<(String, Vec<i32>)> = match req.topics {
                Some(topics) => topics
                    .into_iter()
                    .map(|rt| (rt.name.0.as_str().to_owned(), rt.partition_indexes))
                    .collect(),
                None => vec![(broker.topic.clone(), vec![0])],
            };
            for (tname, pids) in topic_list {
                let mut ot = OffsetFetchResponseTopic::default();
                ot.name = TopicName(StrBytes::from_string(tname.clone()));
                for pid in pids {
                    let mut op = OffsetFetchResponsePartition::default();
                    op.partition_index = pid;
                    if tname == broker.topic && pid == 0 {
                        op.committed_offset =
                            g.committed_offsets.get(&group_id).copied().unwrap_or(-1);
                        op.committed_leader_epoch = -1;
                        op.error_code = 0;
                    } else {
                        op.committed_offset = -1;
                        op.error_code = 0;
                    }
                    ot.partitions.push(op);
                }
                resp.topics.push(ot);
            }
            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                OffsetFetchResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::OffsetCommit => {
            let hdr = RequestHeader::decode(buf, OffsetCommitRequest::header_version(api_version))?;
            let req = OffsetCommitRequest::decode(buf, api_version)?;
            let group_id = req.group_id.0.as_str().to_owned();
            let mut resp = OffsetCommitResponse::default();
            {
                let mut g = broker.log.lock();
                for rt in req.topics {
                    let tname = rt.name.0.as_str().to_owned();
                    let mut ct = OffsetCommitResponseTopic::default();
                    ct.name = TopicName(StrBytes::from_string(tname.clone()));
                    for rp in rt.partitions {
                        let mut cp = OffsetCommitResponsePartition::default();
                        cp.partition_index = rp.partition_index;
                        if tname == broker.topic && rp.partition_index == 0 {
                            g.committed_offsets
                                .insert(group_id.clone(), rp.committed_offset);
                            cp.error_code = 0;
                        } else {
                            cp.error_code = 3;
                        }
                        ct.partitions.push(cp);
                    }
                    resp.topics.push(ct);
                }
                evict_committed_batches(&mut g);
            }
            broker.space_notify.notify_waiters();

            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                OffsetCommitResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::Fetch => {
            let hdr = RequestHeader::decode(buf, FetchRequest::header_version(api_version))?;
            let req = FetchRequest::decode(buf, api_version)?;

            let mut target_fetch_offset = None;
            for ft in &req.topics {
                if ft.topic.0.as_str() == broker.topic {
                    if let Some(fp) = ft.partitions.iter().find(|p| p.partition == 0) {
                        target_fetch_offset = Some(fp.fetch_offset);
                    }
                }
            }

            if let Some(fo) = target_fetch_offset {
                let max_wait = Duration::from_millis((req.max_wait_ms.max(0) as u64).min(150));
                let deadline = Instant::now() + max_wait;
                loop {
                    let ready = {
                        let g = broker.log.lock();
                        fo < g.next_offset || fo < g.log_start_offset
                    };
                    if ready
                        || Instant::now() >= deadline
                        || broker.shutdown.load(Ordering::Relaxed)
                    {
                        break;
                    }
                    let rem = deadline.saturating_duration_since(Instant::now());
                    let _ = tokio::time::timeout(rem, broker.notify.notified()).await;
                }
            }

            let mut resp = FetchResponse::default();
            resp.session_id = 0;
            let global_max = (req.max_bytes.max(1) as usize).min(8 * 1024 * 1024);

            let g = broker.log.lock();
            for ft in req.topics {
                let tname = ft.topic.0.as_str().to_owned();
                let mut frt = FetchableTopicResponse::default();
                frt.topic = TopicName(StrBytes::from_string(tname.clone()));

                for fp in ft.partitions {
                    let mut pd = PartitionData::default();
                    pd.partition_index = fp.partition;
                    if tname != broker.topic || fp.partition != 0 {
                        pd.error_code = 3;
                    } else if fp.fetch_offset < g.log_start_offset {
                        pd.error_code = 1; // OFFSET_OUT_OF_RANGE
                        pd.high_watermark = g.next_offset;
                        pd.log_start_offset = g.log_start_offset;
                    } else {
                        pd.error_code = 0;
                        pd.high_watermark = g.next_offset;
                        pd.last_stable_offset = g.next_offset;
                        pd.log_start_offset = g.log_start_offset;

                        let part_max = (fp.partition_max_bytes.max(1) as usize).min(global_max);
                        let start_idx = g
                            .batches
                            .partition_point(|b| b.end_offset <= fp.fetch_offset);
                        if start_idx < g.batches.len() {
                            let mut out_bytes = BytesMut::new();
                            for b in g.batches.iter().skip(start_idx) {
                                if !out_bytes.is_empty()
                                    && out_bytes.len() + b.raw_batch.len() > part_max
                                {
                                    break;
                                }
                                out_bytes.extend_from_slice(&b.raw_batch);
                                if out_bytes.len() >= part_max {
                                    break;
                                }
                            }
                            if !out_bytes.is_empty() {
                                pd.records = Some(out_bytes.freeze());
                            }
                        }
                    }
                    frt.partitions.push(pd);
                }
                resp.responses.push(frt);
            }
            drop(g);

            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                FetchResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::Produce => {
            let hdr = RequestHeader::decode(buf, ProduceRequest::header_version(api_version))?;
            let req = ProduceRequest::decode(buf, api_version)?;
            let mut resp = ProduceResponse::default();
            for td in req.topic_data {
                let tname = td.name.0.as_str().to_owned();
                let mut tr = kafka_protocol::messages::produce_response::TopicProduceResponse::default();
                tr.name = TopicName(StrBytes::from_string(tname.clone()));
                for pd in td.partition_data {
                    let mut pr = kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
                    pr.index = pd.index;
                    if tname == broker.topic && pd.index == 0 {
                        if let Some(mut raw) = pd.records {
                            let records = RecordBatchDecoder::decode_with_custom_compression(
                                &mut raw,
                                Some(decompress_record_batch),
                            )?;
                            let num = records.len() as i64;
                            let g = broker.log.lock();
                            let base = g.next_offset;
                            drop(g);
                            // Re-encode only if needed via external producer; store raw if needed
                            let mut precs = Vec::with_capacity(records.len());
                            for r in records {
                                precs.push(ProduceRecord {
                                    key: r.key,
                                    value: r.value,
                                    headers: r
                                        .headers
                                        .into_iter()
                                        .map(|(k, v)| (k.as_str().to_owned(), v.unwrap_or_default()))
                                        .collect(),
                                    timestamp_ms: Some(r.timestamp),
                                });
                            }
                            drop(precs);
                            pr.base_offset = base;
                            let _ = num;
                        }
                    } else {
                        pr.error_code = 3;
                    }
                    tr.partition_responses.push(pr);
                }
                resp.responses.push(tr);
            }
            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                ProduceResponse::header_version(api_version),
                &resp,
            )
        }
        ApiKey::CreateTopics => {
            let hdr = RequestHeader::decode(buf, CreateTopicsRequest::header_version(api_version))?;
            let req = CreateTopicsRequest::decode(buf, api_version)?;
            let mut resp = CreateTopicsResponse::default();
            for t in req.topics {
                let mut ct = CreatableTopicResult::default();
                ct.name = t.name;
                ct.error_code = 0;
                resp.topics.push(ct);
            }
            encode_kafka_response(
                hdr.correlation_id,
                api_version,
                CreateTopicsResponse::header_version(api_version),
                &resp,
            )
        }
        other => {
            anyhow::bail!("unsupported kafka api key in prod_bench broker: {other:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// Realistic K8s 10-Label Series Generator & RW2 v2 Record Encoder
// ---------------------------------------------------------------------------

const CLUSTERS: &[&str] = &["prod-us-east-1", "prod-us-west-2", "prod-eu-west-1"];
const CONTAINERS: &[&str] = &["app", "envoy", "otel-sidecar", "worker", "proxy", "auth", "db-pool", "cache"];
const STATUS_CODES: &[&str] = &["200", "201", "204", "400", "404", "500", "503"];

#[derive(Clone, Debug)]
struct SeriesLayout {
    total_series: usize,
    heavy_count: usize,
    num_services: usize,
}

impl SeriesLayout {
    fn new(total_series: usize) -> Self {
        let heavy_count = HEAVY_TARGET_SERIES.min(total_series.max(1));
        let num_services = (total_series / 700).max(1);
        Self {
            total_series,
            heavy_count,
            num_services,
        }
    }

    #[inline]
    fn metric_idx(&self, s: usize) -> usize {
        if s < self.heavy_count {
            0
        } else {
            1 + ((s / SERIES_PER_POD) % (NUM_METRIC_FAMILIES - 1))
        }
    }

    #[inline]
    fn pod_idx(&self, s: usize) -> usize {
        s / SERIES_PER_POD
    }

    #[inline]
    fn service_idx(&self, s: usize) -> usize {
        (s / SERIES_PER_POD) % self.num_services
    }

    fn metric_name(idx: usize) -> String {
        format!("http_server_requests_total_{idx:04}")
    }

    fn pod_name(pod_idx: usize) -> String {
        format!("pod-prod-us-east-1-eks-compute-workload-pool-{pod_idx:08}")
    }

    fn service_name(svc_idx: usize) -> String {
        format!("svc-{svc_idx:05}")
    }

    fn build_labels(&self, s: usize) -> Vec<cortexpb::LabelPair> {
        let pod_idx = self.pod_idx(s);
        let svc_idx = self.service_idx(s);
        let ns_idx = svc_idx % 20;
        let node_idx = pod_idx % 500;
        let job_idx = svc_idx % 100;
        let cluster = CLUSTERS[pod_idx % CLUSTERS.len()];
        let container = CONTAINERS[pod_idx % CONTAINERS.len()];
        let status_code = STATUS_CODES[s % STATUS_CODES.len()];
        let metric = Self::metric_name(self.metric_idx(s));
        let pod = Self::pod_name(pod_idx);
        let svc = Self::service_name(svc_idx);
        let ns = format!("ns-{ns_idx:02}");
        let node = format!("ip-10-42-{}-{}.ec2.internal", (node_idx >> 8) & 0xff, node_idx & 0xff);
        let job = format!("{ns}/service-{job_idx:03}");
        let instance = format!(
            "10.42.{}.{}:8080",
            ((pod_idx >> 8) & 0xff),
            (pod_idx & 0xff)
        );

        // Strictly sorted lexicographically:
        // __name__, cluster, container, instance, job, namespace, node, pod, service, status_code
        vec![
            cortexpb::LabelPair::from_strs("__name__", metric),
            cortexpb::LabelPair::from_static("cluster", cluster),
            cortexpb::LabelPair::from_static("container", container),
            cortexpb::LabelPair::from_strs("instance", instance),
            cortexpb::LabelPair::from_strs("job", job),
            cortexpb::LabelPair::from_strs("namespace", ns),
            cortexpb::LabelPair::from_strs("node", node),
            cortexpb::LabelPair::from_strs("pod", pod),
            cortexpb::LabelPair::from_strs("service", svc),
            cortexpb::LabelPair::from_static("status_code", status_code),
        ]
    }
}

fn encode_v2_record(
    layout: &SeriesLayout,
    tenant: &str,
    items: &[(usize, i64, f64)],
    metadata_offset: usize,
) -> (ProduceRecord, usize) {
    let mut timeseries = Vec::with_capacity(items.len());
    for &(s, ts_ms, val) in items {
        timeseries.push(cortexpb::TimeSeries {
            labels: layout.build_labels(s),
            samples: vec![cortexpb::Sample {
                value: val,
                timestamp_ms: ts_ms,
            }],
            ..Default::default()
        });
    }

    let mut metadata = Vec::with_capacity(METADATA_PER_RECORD);
    for m in 0..METADATA_PER_RECORD {
        let mid = (metadata_offset + m) % NUM_METRIC_FAMILIES;
        metadata.push(cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: SeriesLayout::metric_name(mid),
            help: "Production HTTP server request counter".to_string(),
            unit: "requests".to_string(),
        });
    }

    let rw1 = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries,
        metadata,
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&rw1, V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()))
        .expect("encode rw2");
    let raw_bytes = rw2.encode_to_vec();
    let raw_len = raw_bytes.len();
    let hdr_val = Bytes::copy_from_slice(&record_version_header(RecordVersion::V2));

    let rec = ProduceRecord {
        key: Some(Bytes::copy_from_slice(tenant.as_bytes())),
        value: Some(Bytes::from(raw_bytes)),
        headers: vec![(RECORD_VERSION_HEADER_KEY.to_string(), hdr_val)],
        timestamp_ms: Some(now_ms()),
    };
    (rec, raw_len)
}

// ---------------------------------------------------------------------------
// Historical Local ULID Block Generator (parallel across blocks)
// ---------------------------------------------------------------------------

fn generate_local_blocks(
    tsdb_dir: &Path,
    layout: &SeriesLayout,
    num_blocks: usize,
    t_block_end: i64,
) {
    if num_blocks == 0 {
        return;
    }
    let prod_dir = tsdb_dir.join("prod");
    fs::create_dir_all(&prod_dir).unwrap();

    // Include all heavy (~28k), medium, and small queried series + up to 50k series in each block
    let block_series_count = layout.total_series.min(50_000);
    let workers = num_blocks.min(
        std::thread::available_parallelism()
            .map_or(4, |n| n.get())
            .clamp(1, 8),
    );
    let next_b = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let b = next_b.fetch_add(1, Ordering::Relaxed);
                if b >= num_blocks {
                    break;
                }
                let mint = t_block_end - ((num_blocks - b) as i64) * TWO_HOURS_MS;
                let maxt = mint + TWO_HOURS_MS;

                let mut comp_series = Vec::with_capacity(block_series_count);
                for s in 0..block_series_count {
                    let pairs = layout.build_labels(s);
                    let str_pairs: Vec<(String, String)> = pairs
                        .into_iter()
                        .map(|p| {
                            (
                                String::from_utf8_lossy(&p.name).into_owned(),
                                String::from_utf8_lossy(&p.value).into_owned(),
                            )
                        })
                        .collect();
                    let labels = Labels::from_pairs(
                        str_pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                    );

                    let mut xc = XorChunk::new();
                    {
                        let mut app = xc.appender().unwrap();
                        for step in 0..10i64 {
                            let ts = mint + 60_000 + step * 600_000;
                            app.append(0, ts, (s as f64) + (step as f64) * 1.5)
                                .unwrap();
                        }
                    }
                    comp_series.push(CompactionSeries {
                        labels,
                        chunks: vec![ChunkMeta {
                            ref_id: 0,
                            min_time: mint + 60_000,
                            max_time: mint + 60_000 + 9 * 600_000,
                            chunk: Some(EncodedChunk::Xor(xc)),
                        }],
                        tombstones: Vec::new(),
                    });
                }

                // Compaction requires sorted series by labels
                comp_series.sort_by(|a, b| a.labels.cmp(&b.labels));
                write_block_from_series(
                    &prod_dir,
                    mint,
                    maxt,
                    comp_series,
                    BlockMetaCompaction::default(),
                    false,
                    0,
                )
                .expect("write historical ULID block");
            });
        }
    });
}

// ---------------------------------------------------------------------------
// Config + Readiness + Go pprof
// ---------------------------------------------------------------------------

fn free_tcp_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn write_prod_yaml(
    path: &Path,
    http_port: u16,
    grpc_port: u16,
    broker_addr: &str,
    topic: &str,
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
    instance_id: ingester-zone-a-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    unregister_on_shutdown: true
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
  push_grpc_method_enabled: false
  active_series_metrics_enabled: true
ingest_storage:
  enabled: true
  kafka:
    address: {broker_addr}
    topic: {topic}
    consume_from_position_at_startup: last-offset
    consumer_group_offset_commit_interval: 100ms
    last_produced_offset_poll_interval: 50ms
    target_consumer_lag_at_startup: 0s
    max_consumer_lag_at_startup: 0s
    fetch_concurrency_max: 4
    ingestion_concurrency_max: 8
    ingestion_concurrency_batch_size: 150
    ingestion_concurrency_queue_capacity: 3
    ingestion_concurrency_target_flushes_per_shard: 40
    ingestion_concurrency_estimated_bytes_per_sample: 200
limits:
  out_of_order_time_window: 30m
  max_global_series_per_user: 0
  ingestion_rate: 10000000
  ingestion_burst_size: 10000000
  max_label_names_per_series: 64
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    retention_period: 720h
    ship_interval: 0s
    head_compaction_interval: 10m
    wal_compression_enabled: true
activity_tracker:
  filepath: ""
"#,
        bucket_dir.display(),
        tsdb_dir.display()
    );
    fs::write(path, yaml).unwrap();
}

async fn wait_for_ready(http: &reqwest::Client, http_port: u16, proc: &mut ProcessGuard) {
    let url = format!("http://127.0.0.1:{http_port}/ready");
    let deadline = Instant::now() + Duration::from_secs(120);
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
        if Instant::now() >= deadline {
            let err = proc.dump_stderr();
            panic!("{} /ready timeout (120s), stderr:\n{err}", proc.name);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn capture_go_pprof_heap(http: &reqwest::Client, http_port: u16, out_dir: &Path) {
    let resp = match http
        .get(format!("http://127.0.0.1:{http_port}/debug/pprof/heap"))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        other => {
            println!("  [Go pprof] failed to fetch /debug/pprof/heap: {other:?}");
            return;
        }
    };
    let bytes = resp.bytes().await.expect("read pprof heap bytes");
    let heap_path = out_dir.join("go_heap.pb.gz");
    fs::write(&heap_path, &bytes).expect("write go_heap.pb.gz");

    for flag in ["-inuse_space", "-alloc_space"] {
        let out = Command::new("go")
            .args([
                "tool",
                "pprof",
                "-top",
                "-nodecount=20",
                flag,
                &heap_path.to_string_lossy(),
            ])
            .output();
        match out {
            Ok(o) if o.status.success() => {
                let txt = String::from_utf8_lossy(&o.stdout);
                println!("  [Go pprof {flag}]:");
                for line in txt.lines().take(26) {
                    println!("    {line}");
                }
            }
            Ok(o) => {
                println!(
                    "  [Go pprof {flag}] failed: {}",
                    String::from_utf8_lossy(&o.stderr)
                );
            }
            Err(e) => {
                println!("  [Go pprof {flag}] spawn error: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Kafka Sink abstraction (In-Process Broker OR External Redpanda via KafkaClient)
// ---------------------------------------------------------------------------

enum KafkaSink {
    InProcess(Arc<InMemoryKafkaBroker>),
    External {
        client: Arc<KafkaClient>,
        topic: String,
        next_offset: AtomicI64,
        total_records: AtomicU64,
        total_wire_bytes: AtomicU64,
    },
}

impl KafkaSink {
    async fn produce_batch(&self, records: Vec<ProduceRecord>, respect_ring_cap: bool) -> i64 {
        match self {
            Self::InProcess(b) => {
                b.append_encoded_records_batch(records, KafkaCompression::Zstd, respect_ring_cap)
                    .await
            }
            Self::External {
                client,
                topic,
                next_offset,
                total_records,
                total_wire_bytes,
            } => {
                let count = records.len() as i64;
                let approx_wire: u64 = records
                    .iter()
                    .map(|r| r.value.as_ref().map_or(0, |v| v.len() as u64) / 4)
                    .sum();
                let base = client
                    .produce(topic, 0, KafkaCompression::Zstd, records)
                    .await
                    .expect("external kafka produce");
                let last = base + count - 1;
                next_offset.fetch_max(last + 1, Ordering::Relaxed);
                total_records.fetch_add(count as u64, Ordering::Relaxed);
                total_wire_bytes.fetch_add(approx_wire, Ordering::Relaxed);
                last
            }
        }
    }

    async fn last_produced_offset(&self) -> i64 {
        match self {
            Self::InProcess(b) => b.next_offset() - 1,
            Self::External { client, topic, .. } => client
                .fetch_partition_last_produced_offset(topic, 0)
                .await
                .unwrap_or(-1),
        }
    }
}

// ---------------------------------------------------------------------------
// Concurrent Bimodal Read Mix Worker
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ReadStats {
    qs_small_us: Vec<u64>,
    qs_med_us: Vec<u64>,
    qs_heavy_us: Vec<u64>,
    lnv_us: Vec<u64>,
    lvc_us: Vec<u64>,
    us_us: Vec<u64>,
    small_series_sum: u64,
    heavy_series_sum: u64,
    errors: u64,
}

fn with_tenant<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from(tenant).unwrap(),
    );
    req
}

#[allow(clippy::result_large_err)]
async fn run_one_query_stream(
    client: &mut IngesterClient<Channel>,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> Result<u64, tonic::Status> {
    let mut stream = client
        .query_stream(with_tenant(
            "prod",
            ingesterpb::QueryRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers,
                streaming_chunks_batch_size: 100,
            },
        ))
        .await?
        .into_inner();
    let mut series = 0u64;
    while let Some(item) = stream.next().await {
        let msg = item?;
        series += msg.streaming_series.len() as u64;
    }
    Ok(series)
}

fn spawn_read_workers(
    stop_flag: Arc<AtomicBool>,
    grpc_ch: Channel,
    layout: Arc<SeriesLayout>,
    start_ms: i64,
    end_ms: i64,
) -> tokio::task::JoinHandle<ReadStats> {
    tokio::spawn(async move {
        let stats = Arc::new(Mutex::new(ReadStats::default()));
        let mut handles = Vec::new();

        // Worker 1: QueryStream at 6.35 req/s (42% repeated matchers; bimodal 85% small, 12% med, 3% heavy + min 10 heavy)
        {
            let ch = grpc_ch.clone();
            let stop = Arc::clone(&stop_flag);
            let st = Arc::clone(&stats);
            let lay = Arc::clone(&layout);
            handles.push(tokio::spawn(async move {
                let mut client = IngesterClient::new(ch)
                    .max_decoding_message_size(128 * 1024 * 1024)
                    .max_encoding_message_size(128 * 1024 * 1024);
                let interval = Duration::from_secs_f64(1.0 / 6.35);
                let mut next_tick = Instant::now();
                let mut q_idx = 0usize;
                let max_pods = (lay.total_series / SERIES_PER_POD).max(1);
                let mut heavy_count = 0usize;

                while !stop.load(Ordering::Relaxed) {
                    let now_i = Instant::now();
                    if next_tick > now_i {
                        tokio::time::sleep(next_tick - now_i).await;
                    }
                    next_tick += interval;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }

                    // 42% repeated matcher set (cycle small fixed set of 8 queries)
                    let is_repeat = (q_idx % 100) < 42;
                    let kind_roll = (q_idx * 73) % 100;
                    // Guarantee >= 10 heavy samples even in short runs (every 10th query until 10)
                    let is_heavy = kind_roll >= 97 || (heavy_count < 10 && q_idx % 12 == 5);
                    let is_med = !is_heavy && kind_roll >= 85;

                    let (matchers, kind) = if is_heavy {
                        heavy_count += 1;
                        (
                            vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: "__name__".to_string(),
                                value: SeriesLayout::metric_name(0),
                            }],
                            2u8,
                        )
                    } else if is_med {
                        let svc_id = if is_repeat {
                            q_idx % 4
                        } else {
                            (q_idx * 17) % lay.num_services
                        };
                        (
                            vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: "service".to_string(),
                                value: SeriesLayout::service_name(svc_id),
                            }],
                            1u8,
                        )
                    } else {
                        let pod_id = if is_repeat {
                            10_000 + (q_idx % 8)
                        } else {
                            // Avoid heavy_count slice so __name__ matches the pod's metric
                            (lay.heavy_count / SERIES_PER_POD)
                                + ((q_idx * 131) % max_pods.saturating_sub(lay.heavy_count / SERIES_PER_POD).max(1))
                        }
                        .min(max_pods - 1);
                        let metric_idx = lay.metric_idx(pod_id * SERIES_PER_POD);
                        (
                            vec![
                                ingesterpb::LabelMatcher {
                                    r#type: ingesterpb::MatchType::Equal as i32,
                                    name: "__name__".to_string(),
                                    value: SeriesLayout::metric_name(metric_idx),
                                },
                                ingesterpb::LabelMatcher {
                                    r#type: ingesterpb::MatchType::Equal as i32,
                                    name: "pod".to_string(),
                                    value: SeriesLayout::pod_name(pod_id),
                                },
                            ],
                            0u8,
                        )
                    };
                    q_idx += 1;

                    let t0 = Instant::now();
                    match run_one_query_stream(&mut client, start_ms, end_ms, matchers).await {
                        Ok(sc) => {
                            let us = t0.elapsed().as_micros() as u64;
                            let mut g = st.lock();
                            match kind {
                                0 => {
                                    g.qs_small_us.push(us);
                                    g.small_series_sum += sc;
                                }
                                1 => g.qs_med_us.push(us),
                                _ => {
                                    g.qs_heavy_us.push(us);
                                    g.heavy_series_sum += sc;
                                }
                            }
                        }
                        Err(_) => {
                            st.lock().errors += 1;
                        }
                    }
                }
            }));
        }

        // Worker 2: LabelNamesAndValues (0.07/s, min 3 samples)
        {
            let ch = grpc_ch.clone();
            let stop = Arc::clone(&stop_flag);
            let st = Arc::clone(&stats);
            handles.push(tokio::spawn(async move {
                let mut client = IngesterClient::new(ch)
                    .max_decoding_message_size(128 * 1024 * 1024);
                let mut count = 0usize;
                while count < 3 || !stop.load(Ordering::Relaxed) {
                    if count > 0 {
                        let sleep_dur = if count < 3 {
                            Duration::from_secs(3)
                        } else {
                            Duration::from_secs_f64(1.0 / 0.07)
                        };
                        tokio::time::sleep(sleep_dur).await;
                    }
                    if count >= 3 && stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let t0 = Instant::now();
                    let res = client
                        .label_names_and_values(with_tenant(
                            "prod",
                            ingesterpb::LabelNamesAndValuesRequest {
                                matchers: vec![ingesterpb::LabelMatcher {
                                    r#type: ingesterpb::MatchType::Equal as i32,
                                    name: "namespace".to_string(),
                                    value: "ns-01".to_string(),
                                }],
                                count_method: ingesterpb::CountMethod::Active as i32,
                            },
                        ))
                        .await;
                    match res {
                        Ok(r) => {
                            let mut stream = r.into_inner();
                            while let Some(item) = stream.next().await {
                                if item.is_err() {
                                    st.lock().errors += 1;
                                    break;
                                }
                            }
                            st.lock().lnv_us.push(t0.elapsed().as_micros() as u64);
                        }
                        Err(_) => st.lock().errors += 1,
                    }
                    count += 1;
                }
            }));
        }

        // Worker 3: LabelValuesCardinality (0.08/s, min 3 samples)
        {
            let ch = grpc_ch.clone();
            let stop = Arc::clone(&stop_flag);
            let st = Arc::clone(&stats);
            handles.push(tokio::spawn(async move {
                let mut client = IngesterClient::new(ch)
                    .max_decoding_message_size(128 * 1024 * 1024);
                let mut count = 0usize;
                while count < 3 || !stop.load(Ordering::Relaxed) {
                    if count > 0 {
                        let sleep_dur = if count < 3 {
                            Duration::from_secs(3)
                        } else {
                            Duration::from_secs_f64(1.0 / 0.08)
                        };
                        tokio::time::sleep(sleep_dur).await;
                    }
                    if count >= 3 && stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let t0 = Instant::now();
                    let res = client
                        .label_values_cardinality(with_tenant(
                            "prod",
                            ingesterpb::LabelValuesCardinalityRequest {
                                label_names: vec!["namespace".to_string(), "cluster".to_string()],
                                matchers: Vec::new(),
                                count_method: ingesterpb::CountMethod::Active as i32,
                            },
                        ))
                        .await;
                    match res {
                        Ok(r) => {
                            let mut stream = r.into_inner();
                            while let Some(item) = stream.next().await {
                                if item.is_err() {
                                    st.lock().errors += 1;
                                    break;
                                }
                            }
                            st.lock().lvc_us.push(t0.elapsed().as_micros() as u64);
                        }
                        Err(_) => st.lock().errors += 1,
                    }
                    count += 1;
                }
            }));
        }

        // Worker 4: UserStats (0.08/s, min 3 samples)
        {
            let ch = grpc_ch;
            let stop = Arc::clone(&stop_flag);
            let st = Arc::clone(&stats);
            handles.push(tokio::spawn(async move {
                let mut client = IngesterClient::new(ch);
                let mut count = 0usize;
                while count < 3 || !stop.load(Ordering::Relaxed) {
                    if count > 0 {
                        let sleep_dur = if count < 3 {
                            Duration::from_secs(3)
                        } else {
                            Duration::from_secs_f64(1.0 / 0.08)
                        };
                        tokio::time::sleep(sleep_dur).await;
                    }
                    if count >= 3 && stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let t0 = Instant::now();
                    match client
                        .user_stats(with_tenant(
                            "prod",
                            ingesterpb::UserStatsRequest {
                                count_method: ingesterpb::CountMethod::InMemory as i32,
                            },
                        ))
                        .await
                    {
                        Ok(_) => st.lock().us_us.push(t0.elapsed().as_micros() as u64),
                        Err(_) => st.lock().errors += 1,
                    }
                    count += 1;
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
        Arc::try_unwrap(stats).unwrap_or_else(|_| panic!("stats ref")).into_inner()
    })
}

fn pct_ms(vals: &mut [u64], pct: f64) -> f64 {
    if vals.is_empty() {
        return 0.0;
    }
    vals.sort_unstable();
    let idx = (((vals.len() - 1) as f64) * pct).round() as usize;
    vals[idx.min(vals.len() - 1)] as f64 / 1000.0
}

// ---------------------------------------------------------------------------
// Engine Runner (4 Phases)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct EngineProdResult {
    name: &'static str,
    // Phase 1
    p1_wall_s: f64,
    p1_samples: usize,
    p1_records: usize,
    p1_raw_bytes_per_sample: f64,
    p1_wire_bytes_per_sample: f64,
    p1_stats: ProcStats,
    // Phase 2
    p2_wall_s: f64,
    p2_samples: usize,
    p2_cpu_us_per_sample: f64,
    p2_stats: ProcStats,
    p2_metrics: ScrapedMetrics,
    qs_small_p50_ms: f64,
    qs_small_p99_ms: f64,
    qs_small_avg_series: f64,
    qs_med_p50_ms: f64,
    qs_med_p99_ms: f64,
    qs_heavy_p50_ms: f64,
    qs_heavy_p99_ms: f64,
    qs_heavy_avg_series: f64,
    lnv_p50_ms: f64,
    lvc_p50_ms: f64,
    us_p50_ms: f64,
    read_errors: u64,
    // Phase 3
    p3_compaction_s: f64,
    p3_pre_rss_anon_mib: f64,
    p3_peak_rss_anon_mib: f64,
    p3_peak_vm_rss_mib: f64,
    // Phase 4
    p4_ready_s: f64,
    p4_catchup_s: f64,
    p4_stats: ProcStats,
}

type RecordPlanItem = (String, Vec<(usize, i64, f64)>, usize);

async fn produce_record_batches_parallel(
    sink: &Arc<KafkaSink>,
    layout: &Arc<SeriesLayout>,
    items: Vec<RecordPlanItem>,
    respect_ring_cap: bool,
) -> (usize, usize, u64) {
    let total_records = items.len();
    let total_raw = Arc::new(AtomicU64::new(0));
    let total_samples: usize = items.iter().map(|x| x.1.len()).sum();

    // Encode in parallel on blocking pool in chunks of 5 records per Kafka RecordBatch
    let chunk_size = 5usize;
    let chunks: Vec<Vec<RecordPlanItem>> = items
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let workers = std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .clamp(2, 8);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<ProduceRecord>>(32);
    let next_idx = Arc::new(AtomicUsize::new(0));
    let chunks_arc = Arc::new(chunks);

    let mut enc_handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let lay = Arc::clone(layout);
        let ch_arc = Arc::clone(&chunks_arc);
        let idx_ctr = Arc::clone(&next_idx);
        let raw_ctr = Arc::clone(&total_raw);
        let tx_c = tx.clone();
        enc_handles.push(tokio::task::spawn_blocking(move || {
            loop {
                let idx = idx_ctr.fetch_add(1, Ordering::Relaxed);
                let Some(batch) = ch_arc.get(idx) else {
                    break;
                };
                let mut precs = Vec::with_capacity(batch.len());
                let mut raw_sum = 0u64;
                for (tenant, pts, md_off) in batch {
                    let (pr, rlen) = encode_v2_record(&lay, tenant, pts, *md_off);
                    raw_sum += rlen as u64;
                    precs.push(pr);
                }
                raw_ctr.fetch_add(raw_sum, Ordering::Relaxed);
                if tx_c.blocking_send(precs).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);

    while let Some(batch) = rx.recv().await {
        sink.produce_batch(batch, respect_ring_cap).await;
    }
    for h in enc_handles {
        let _ = h.await;
    }
    (total_records, total_samples, total_raw.load(Ordering::Relaxed))
}

async fn run_engine(
    name: &'static str,
    is_go: bool,
    bin: &Path,
    cfg: &ProdBenchConfig,
    layout: &Arc<SeriesLayout>,
    base_work: &Path,
) -> EngineProdResult {
    let eng_dir = base_work.join(if is_go { "go" } else { "rust" });
    let tsdb_dir = eng_dir.join("tsdb");
    let bucket_dir = eng_dir.join("bucket");
    fs::create_dir_all(&tsdb_dir).unwrap();
    fs::create_dir_all(&bucket_dir).unwrap();

    let t_block_end = ((now_ms() - 2 * TWO_HOURS_MS) / TWO_HOURS_MS) * TWO_HOURS_MS;
    println!(
        "\n============================================================\n\
         [{}] Generating {} historical 2h ULID blocks in {}...",
        name,
        cfg.local_blocks,
        tsdb_dir.display()
    );
    let blk_t0 = Instant::now();
    generate_local_blocks(&tsdb_dir, layout, cfg.local_blocks, t_block_end);
    println!(
        "[{}] Local blocks ready in {:.2}s",
        name,
        blk_t0.elapsed().as_secs_f64()
    );

    // Setup Kafka broker / sink
    let unique_topic = format!(
        "{}-{}",
        cfg.topic,
        if is_go { "go" } else { "rust" }
    );
    let (sink, broker_addr, in_proc_broker): (
        Arc<KafkaSink>,
        String,
        Option<Arc<InMemoryKafkaBroker>>,
    ) = if let Some(ref ext_addr) = cfg.brokers {
        let kcfg = mimir_config::KafkaConfig {
            address: mimir_config::StringSliceCSV(vec![ext_addr.clone()]),
            ..Default::default()
        };
        let kc = Arc::new(KafkaClient::from_config(&kcfg).expect("connect external kafka"));
        kc.create_topics(&[&unique_topic], 1, 1)
            .await
            .expect("create topic on external kafka");
        (
            Arc::new(KafkaSink::External {
                client: kc,
                topic: unique_topic.clone(),
                next_offset: AtomicI64::new(0),
                total_records: AtomicU64::new(0),
                total_wire_bytes: AtomicU64::new(0),
            }),
            ext_addr.clone(),
            None,
        )
    } else {
        let b = InMemoryKafkaBroker::start(unique_topic.clone()).await;
        let addr = b.addr.clone();
        (Arc::new(KafkaSink::InProcess(Arc::clone(&b))), addr, Some(b))
    };

    // Timestamps:
    // - Local blocks cover [t_block_end - B*2h, t_block_end)
    // - Phase 1 20% extra first-chunk samples: t_p1_c1 = t_block_end + 30m
    // - Phase 1 main samples: t_p1_c2 = t_block_end + 2h + 1m (so 20% have 2 head chunks -> ~1.2 chunks/series!)
    let t_p1_c1 = t_block_end + 30 * 60 * 1000;
    let t_p1_c2 = t_block_end + TWO_HOURS_MS + 60_000;

    // Build Phase 1 preload record plan
    let extra_chunk_series = cfg.series / 5; // 20% get extra chunk
    let mut p1_records_plan: Vec<RecordPlanItem> = Vec::new();
    let mut rec_seq = 0usize;
    let mut s = 0usize;
    while s < extra_chunk_series {
        let end = (s + SAMPLES_PER_RECORD).min(extra_chunk_series);
        let tenant = if (rec_seq % 200) == 199 { "preprod" } else { "prod" };
        let pts: Vec<(usize, i64, f64)> = (s..end)
            .map(|idx| (idx, t_p1_c1, idx as f64 * 0.5))
            .collect();
        p1_records_plan.push((
            tenant.to_string(),
            pts,
            (rec_seq * METADATA_PER_RECORD) % NUM_METRIC_FAMILIES,
        ));
        rec_seq += 1;
        s = end;
    }
    let mut s = 0usize;
    while s < cfg.series {
        let end = (s + SAMPLES_PER_RECORD).min(cfg.series);
        let tenant = if (rec_seq % 200) == 199 { "preprod" } else { "prod" };
        let pts: Vec<(usize, i64, f64)> = (s..end)
            .map(|idx| (idx, t_p1_c2, idx as f64 + 1.0))
            .collect();
        p1_records_plan.push((
            tenant.to_string(),
            pts,
            (rec_seq * METADATA_PER_RECORD) % NUM_METRIC_FAMILIES,
        ));
        rec_seq += 1;
        s = end;
    }

    let p1_target_offset = if in_proc_broker.is_some() && cfg.series <= 15_000_000 {
        println!(
            "[{}] Phase 1: Pre-encoding {} preload records ({} series + 20% 2-chunk) into Kafka...",
            name,
            p1_records_plan.len(),
            cfg.series
        );
        let (nrec, nsamp, raw_b) =
            produce_record_batches_parallel(&sink, layout, p1_records_plan, false).await;
        let last_off = sink.last_produced_offset().await;
        println!(
            "[{}] Preloaded {} records ({} samples, {:.1} B/sample uncompressed) -> offset {}",
            name,
            nrec,
            nsamp,
            raw_b as f64 / nsamp as f64,
            last_off
        );
        (nrec, nsamp, raw_b, last_off)
    } else {
        // Streaming preload for external or ultra-large backlog
        let (nrec, nsamp, raw_b) =
            produce_record_batches_parallel(&sink, layout, p1_records_plan, true).await;
        let last_off = sink.last_produced_offset().await;
        (nrec, nsamp, raw_b, last_off)
    };
    let (p1_records, p1_samples, p1_raw_bytes, p1_last_offset) = p1_target_offset;
    let p1_wire_bytes = match &*sink {
        KafkaSink::InProcess(b) => b.stats().3,
        KafkaSink::External { total_wire_bytes, .. } => total_wire_bytes.load(Ordering::Relaxed),
    };

    let http_port = free_tcp_port();
    let grpc_port = free_tcp_port();
    let yaml_path = eng_dir.join("mimir.yaml");
    write_prod_yaml(
        &yaml_path,
        http_port,
        grpc_port,
        &broker_addr,
        &unique_topic,
        &tsdb_dir,
        &bucket_dir,
    );

    let bin_args = vec![
        "-target=ingester".to_string(),
        format!("-config.file={}", yaml_path.display()),
    ];

    // Spawn process and measure Phase 1 catch-up!
    let http = reqwest::Client::new();
    let p1_start = Instant::now();
    let mut proc = ProcessGuard::spawn(name, bin, &bin_args, &eng_dir);
    wait_for_ready(&http, http_port, &mut proc).await;
    let pid = proc.pid();

    let p1_deadline = Instant::now() + Duration::from_secs(600);
    let mut last_log = Instant::now();
    loop {
        if let Some(st) = proc.try_wait() {
            let err = proc.dump_stderr();
            panic!("{name} exited during Phase 1 ({st}):\n{err}");
        }
        if let Some(m) = scrape_metrics(&http, http_port).await {
            let consumed = m.last_consumed_offset.unwrap_or(-1);
            if last_log.elapsed() >= Duration::from_secs(5) {
                println!(
                    "[{name}] P1 progress: mem_series={:.0}/{} ingested={:.0} consumed_off={}/{}",
                    m.memory_series, cfg.series, m.ingested_samples_total, consumed, p1_last_offset
                );
                last_log = Instant::now();
            }
            if m.memory_series as usize >= cfg.series && consumed >= p1_last_offset {
                break;
            }
        }
        assert!(Instant::now() < p1_deadline, "{name} Phase 1 catch-up timeout");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let p1_wall = p1_start.elapsed().as_secs_f64();
    let p1_stats = read_proc_stats(pid);
    println!(
        "[{}] Phase 1 DONE in {:.2}s -> {:.0} samples/s ({:.0} rec/s) | RssAnon={:.1} MiB ({:.0} B/series) RssFile={:.1} MiB CPU={:.2}s",
        name,
        p1_wall,
        p1_samples as f64 / p1_wall,
        p1_records as f64 / p1_wall,
        p1_stats.rss_anon_mib,
        (p1_stats.rss_anon_mib * 1024.0 * 1024.0) / cfg.series as f64,
        p1_stats.rss_file_mib,
        p1_stats.cpu_secs
    );

    if is_go {
        capture_go_pprof_heap(&http, http_port, &eng_dir).await;
    }

    // Optional perf record during Phase 2 on Rust
    let _perf_guard = if !is_go {
        cfg.perf_out.as_ref().map(|out| {
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
                    out.clone(),
                ],
                &eng_dir,
            )
        })
    } else {
        None
    };

    // -----------------------------------------------------------------------
    // Phase 2: Steady-State Prod Traffic (327k samples/s) + Optional Bimodal Reads
    // -----------------------------------------------------------------------
    let stop_reads = Arc::new(AtomicBool::new(false));
    let read_handle = if cfg.no_reads {
        None
    } else {
        let grpc_ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .unwrap()
            .tcp_nodelay(true)
            .connect()
            .await
            .expect("connect ingester grpc");

        let query_start_ms = t_block_end - (cfg.local_blocks as i64) * TWO_HOURS_MS;
        let query_end_ms = t_p1_c2 + 7_200_000;
        Some(spawn_read_workers(
            Arc::clone(&stop_reads),
            grpc_ch,
            Arc::clone(layout),
            query_start_ms,
            query_end_ms,
        ))
    };

    // Steady-state producer task (runs through Phase 2 AND Phase 3 AND 5s post-Phase-3 WAL warm-up!)
    let stop_steady_prod = Arc::new(AtomicBool::new(false));
    let ooo_paused = Arc::new(AtomicBool::new(false));
    let steady_samples_sent = Arc::new(AtomicUsize::new(0));
    let last_in_order_ts = Arc::new(AtomicI64::new(t_p1_c2 + 15_000));

    let prod_handle = {
        let sink_c = Arc::clone(&sink);
        let lay_c = Arc::clone(layout);
        let stop_c = Arc::clone(&stop_steady_prod);
        let ooo_pause_c = Arc::clone(&ooo_paused);
        let sent_ctr = Arc::clone(&steady_samples_sent);
        let ts_atomic = Arc::clone(&last_in_order_ts);
        let target_rate = cfg.rate_samples_per_sec;
        let base_series = cfg.series;

        tokio::spawn(async move {
            let records_per_sec = (target_rate as f64) / (SAMPLES_PER_RECORD as f64);
            let batch_records = 5usize; // 2,500 samples per Kafka RecordBatch
            let batch_interval = Duration::from_secs_f64((batch_records as f64) / records_per_sec);

            let churn_per_sec = (base_series as f64 / 3600.0).max(1.0);
            let mut churn_accum = 0.0f64;
            let mut next_churn_id = base_series;
            let mut series_cursor = 0usize;
            let mut rec_idx = 0usize;
            let mut next_tick = Instant::now();

            let spawn_encode_batch =
                |sc_in: usize, ca_in: f64, cid_in: usize, r_in: usize, do_ooo: bool| {
                    let lay_enc = Arc::clone(&lay_c);
                    let ts_enc = Arc::clone(&ts_atomic);
                    // Advance deterministic cursor state synchronously so pipelined batches stay ordered.
                    let mut sc = sc_in;
                    let mut ca = ca_in;
                    let mut cid_next = cid_in;
                    let mut r_idx = r_in;
                    let mut batch_plan = Vec::with_capacity(batch_records);
                    for _ in 0..batch_records {
                        let tenant = if (r_idx % 200) == 199 { "preprod" } else { "prod" };
                        let round = sc / base_series;
                        let base_ts =
                            t_p1_c2 + 15_000 + (round as i64) * 15_000 + ((r_idx % 1000) as i64);
                        ts_enc.fetch_max(base_ts, Ordering::Relaxed);
                        ca += churn_per_sec / records_per_sec;
                        let churn_here = if ca >= 1.0 {
                            ca -= 1.0;
                            1usize
                        } else {
                            0usize
                        };
                        let mut pts = Vec::with_capacity(SAMPLES_PER_RECORD);
                        for j in 0..SAMPLES_PER_RECORD {
                            let s_id = if j == 0 && churn_here > 0 {
                                let cid = cid_next;
                                cid_next += 1;
                                cid
                            } else {
                                let id = sc % base_series;
                                sc += 1;
                                id
                            };
                            let ts = if do_ooo && j == 1 && r_idx.is_multiple_of(3) {
                                base_ts - 4 * 60 * 1000
                            } else {
                                base_ts
                            };
                            pts.push((s_id, ts, (round as f64) * 2.0 + (s_id as f64) * 0.01));
                        }
                        let meta_off = (r_idx * METADATA_PER_RECORD) % NUM_METRIC_FAMILIES;
                        batch_plan.push((tenant, pts, meta_off));
                        r_idx += 1;
                    }
                    let fut = tokio::task::spawn_blocking(move || {
                        let mut precs = Vec::with_capacity(batch_records);
                        let mut batch_samples = 0usize;
                        for (tenant, pts, meta_off) in batch_plan {
                            batch_samples += pts.len();
                            let (pr, _) = encode_v2_record(&lay_enc, tenant, &pts, meta_off);
                            precs.push(pr);
                        }
                        (precs, batch_samples)
                    });
                    (fut, sc, ca, cid_next, r_idx)
                };

            let mut inflight = VecDeque::with_capacity(4);
            while !stop_c.load(Ordering::Relaxed) {
                while inflight.len() < 3 {
                    let do_ooo = !ooo_pause_c.load(Ordering::Relaxed);
                    let (fut, sc, ca, cid, r) =
                        spawn_encode_batch(series_cursor, churn_accum, next_churn_id, rec_idx, do_ooo);
                    series_cursor = sc;
                    churn_accum = ca;
                    next_churn_id = cid;
                    rec_idx = r;
                    inflight.push_back(fut);
                }

                let now_i = Instant::now();
                if next_tick > now_i {
                    tokio::time::sleep(next_tick - now_i).await;
                } else {
                    tokio::task::yield_now().await;
                }
                next_tick += batch_interval;
                if stop_c.load(Ordering::Relaxed) {
                    break;
                }

                let (precs, batch_samples) = inflight
                    .pop_front()
                    .unwrap()
                    .await
                    .expect("encode_v2_record task");

                sink_c.produce_batch(precs, true).await;
                sent_ctr.fetch_add(batch_samples, Ordering::Relaxed);
            }
        })
    };

    let p2_start_cpu = read_proc_stats(pid).cpu_secs;
    let p2_start_scraped = scrape_metrics(&http, http_port)
        .await
        .expect("scrape_metrics at P2 start");
    let p2_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(cfg.steady_secs)).await;

    stop_reads.store(true, Ordering::Release);
    let mut read_stats = match read_handle {
        Some(h) => h.await.expect("read workers"),
        None => ReadStats::default(),
    };

    let p2_wall = p2_start.elapsed().as_secs_f64();
    let p2_stats = read_proc_stats(pid);
    let p2_metrics = scrape_metrics(&http, http_port)
        .await
        .expect("scrape_metrics at P2 end");
    let p2_ingested = (p2_metrics.ingested_samples_total - p2_start_scraped.ingested_samples_total)
        .max(1.0) as usize;
    let p2_cpu_delta = (p2_stats.cpu_secs - p2_start_cpu).max(0.0);
    let p2_cpu_us_per_sample = (p2_cpu_delta * 1_000_000.0) / (p2_ingested as f64);

    let qs_small_avg = if read_stats.qs_small_us.is_empty() {
        0.0
    } else {
        read_stats.small_series_sum as f64 / read_stats.qs_small_us.len() as f64
    };
    let qs_heavy_avg = if read_stats.qs_heavy_us.is_empty() {
        0.0
    } else {
        read_stats.heavy_series_sum as f64 / read_stats.qs_heavy_us.len() as f64
    };

    let qs_small_p50_ms = pct_ms(&mut read_stats.qs_small_us, 0.50);
    let qs_small_p99_ms = pct_ms(&mut read_stats.qs_small_us, 0.99);
    let qs_med_p50_ms = pct_ms(&mut read_stats.qs_med_us, 0.50);
    let qs_med_p99_ms = pct_ms(&mut read_stats.qs_med_us, 0.99);
    let qs_heavy_p50_ms = pct_ms(&mut read_stats.qs_heavy_us, 0.50);
    let qs_heavy_p99_ms = pct_ms(&mut read_stats.qs_heavy_us, 0.99);
    let lnv_p50_ms = pct_ms(&mut read_stats.lnv_us, 0.50);
    let lvc_p50_ms = pct_ms(&mut read_stats.lvc_us, 0.50);
    let us_p50_ms = pct_ms(&mut read_stats.us_us, 0.50);

    println!(
        "[{}] Phase 2 ({:.1}s, {} ingested) -> CPU={:.2} µs/sample | RssAnon={:.1} MiB ({:.0} B/series) RssFile={:.1} MiB | QS small({:.1}s) p50={:.2}ms p99={:.2}ms | QS heavy({:.0}s) p50={:.2}ms p99={:.2}ms | errs={}",
        name,
        p2_wall,
        p2_ingested,
        p2_cpu_us_per_sample,
        p2_stats.rss_anon_mib,
        (p2_stats.rss_anon_mib * 1024.0 * 1024.0) / p2_metrics.memory_series.max(1.0),
        p2_stats.rss_file_mib,
        qs_small_avg,
        qs_small_p50_ms,
        qs_small_p99_ms,
        qs_heavy_avg,
        qs_heavy_p50_ms,
        qs_heavy_p99_ms,
        read_stats.errors
    );

    // -----------------------------------------------------------------------
    // Phase 3: Head Compaction Under Live Load (POST /ingester/flush?wait=true)
    // -----------------------------------------------------------------------
    // Pause OOO injection across flush boundary so post-compaction min_valid_time isn't violated
    ooo_paused.store(true, Ordering::Release);
    let p3_pre_rss_anon = read_proc_stats(pid).rss_anon_mib;
    let peak_rss_anon = Arc::new(AtomicU64::new(p3_pre_rss_anon.to_bits()));
    let peak_vm_rss = Arc::new(AtomicU64::new(read_proc_stats(pid).vm_rss_mib.to_bits()));
    let stop_rss_poll = Arc::new(AtomicBool::new(false));

    let poll_handle = {
        let stop_c = Arc::clone(&stop_rss_poll);
        let pa = Arc::clone(&peak_rss_anon);
        let pv = Arc::clone(&peak_vm_rss);
        tokio::spawn(async move {
            while !stop_c.load(Ordering::Relaxed) {
                let st = read_proc_stats(pid);
                pa.fetch_max(st.rss_anon_mib.to_bits(), Ordering::Relaxed);
                pv.fetch_max(st.vm_rss_mib.to_bits(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };

    let p3_start = Instant::now();
    let flush_resp = http
        .post(format!("http://127.0.0.1:{http_port}/ingester/flush?wait=true"))
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .expect("flush request");
    assert_eq!(flush_resp.status().as_u16(), 204, "{name} flush status");
    let p3_compaction_s = p3_start.elapsed().as_secs_f64();
    stop_rss_poll.store(true, Ordering::Release);
    let _ = poll_handle.await;

    let p3_peak_rss_anon_mib = f64::from_bits(peak_rss_anon.load(Ordering::Relaxed));
    let p3_peak_vm_rss_mib = f64::from_bits(peak_vm_rss.load(Ordering::Relaxed));
    println!(
        "[{}] Phase 3 Compaction: wall={:.2}s | pre_RssAnon={:.1} MiB -> peak_RssAnon={:.1} MiB (+{:.1} MiB sawtooth) peak_VmRSS={:.1} MiB",
        name,
        p3_compaction_s,
        p3_pre_rss_anon,
        p3_peak_rss_anon_mib,
        (p3_peak_rss_anon_mib - p3_pre_rss_anon).max(0.0),
        p3_peak_vm_rss_mib
    );

    // -----------------------------------------------------------------------
    // Phase 4: Populate fresh WAL segments -> SIGKILL -> 5s Backlog -> Restart
    // -----------------------------------------------------------------------
    // Keep steady ingest running 5s so uncompacted WAL segments exist in wal/
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop_steady_prod.store(true, Ordering::Release);
    let _ = prod_handle.await;

    // Wait briefly for OffsetCommit (100ms interval) and WAL write
    tokio::time::sleep(Duration::from_millis(400)).await;
    let wal_dir = tsdb_dir.join("prod").join("wal");
    let wal_files = fs::read_dir(&wal_dir).map(|rd| rd.count()).unwrap_or(0);
    println!("[{}] Pre-SIGKILL wal/ files: {}", name, wal_files);

    // Kill -9!
    proc.sigkill_and_reap();
    drop(_perf_guard);

    // Enqueue 5-second Kafka backlog (~1.63M samples) while ingester is dead
    let backlog_samples = 5 * cfg.rate_samples_per_sec;
    let backlog_records = backlog_samples.div_ceil(SAMPLES_PER_RECORD);
    let mut p4_plan = Vec::with_capacity(backlog_records);
    let p4_base_ts = last_in_order_ts.load(Ordering::Relaxed) + 15_000;
    let mut cur_s = 0usize;
    for r in 0..backlog_records {
        let tenant = if (r % 200) == 199 { "preprod" } else { "prod" };
        let ts = p4_base_ts + (r as i64) * 5;
        let pts: Vec<(usize, i64, f64)> = (0..SAMPLES_PER_RECORD)
            .map(|_| {
                let id = cur_s % cfg.series;
                cur_s += 1;
                (id, ts, 999.0 + (r as f64))
            })
            .collect();
        p4_plan.push((
            tenant.to_string(),
            pts,
            (r * METADATA_PER_RECORD) % NUM_METRIC_FAMILIES,
        ));
    }
    // Wait for Phase 4 backlog encode & spawn proc2
    let (_, _, _) = produce_record_batches_parallel(&sink, layout, p4_plan, false).await;
    let p4_target_offset = sink.last_produced_offset().await;

    // Cold Restart on SAME data dir!
    let p4_spawn_t0 = Instant::now();
    let mut proc2 = ProcessGuard::spawn(name, bin, &bin_args, &eng_dir);
    wait_for_ready(&http, http_port, &mut proc2).await;
    let p4_ready_s = p4_spawn_t0.elapsed().as_secs_f64();
    let pid2 = proc2.pid();

    let p4_deadline = Instant::now() + Duration::from_secs(300);
    let mut last_p4_log = Instant::now();
    loop {
        if let Some(st) = proc2.try_wait() {
            let err = proc2.dump_stderr();
            panic!("{name} exited during Phase 4 catchup ({st}):\n{err}");
        }
        if let Some(m) = scrape_metrics(&http, http_port).await {
            if last_p4_log.elapsed() >= Duration::from_secs(5) {
                println!(
                    "[{name}] P4 progress: consumed_off={:?}/{} mem_series={:.0}",
                    m.last_consumed_offset, p4_target_offset, m.memory_series
                );
                last_p4_log = Instant::now();
            }
            if m.last_consumed_offset.unwrap_or(-1) >= p4_target_offset {
                break;
            }
        }
        assert!(Instant::now() < p4_deadline, "{name} Phase 4 catchup timeout");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let p4_catchup_s = p4_spawn_t0.elapsed().as_secs_f64();
    let p4_stats = read_proc_stats(pid2);
    println!(
        "[{}] Phase 4 SIGKILL Restart: /ready={:.2}s | 0-lag catchup={:.2}s | RssAnon={:.1} MiB",
        name, p4_ready_s, p4_catchup_s, p4_stats.rss_anon_mib
    );

    drop(proc2);
    if let Some(b) = in_proc_broker {
        b.stop();
    }

    EngineProdResult {
        name,
        p1_wall_s: p1_wall,
        p1_samples,
        p1_records,
        p1_raw_bytes_per_sample: p1_raw_bytes as f64 / p1_samples as f64,
        p1_wire_bytes_per_sample: p1_wire_bytes as f64 / p1_samples as f64,
        p1_stats,
        p2_wall_s: p2_wall,
        p2_samples: p2_ingested,
        p2_cpu_us_per_sample,
        p2_stats,
        p2_metrics,
        qs_small_p50_ms,
        qs_small_p99_ms,
        qs_small_avg_series: qs_small_avg,
        qs_med_p50_ms,
        qs_med_p99_ms,
        qs_heavy_p50_ms,
        qs_heavy_p99_ms,
        qs_heavy_avg_series: qs_heavy_avg,
        lnv_p50_ms,
        lvc_p50_ms,
        us_p50_ms,
        read_errors: read_stats.errors,
        p3_compaction_s,
        p3_pre_rss_anon_mib: p3_pre_rss_anon,
        p3_peak_rss_anon_mib,
        p3_peak_vm_rss_mib,
        p4_ready_s,
        p4_catchup_s,
        p4_stats,
    }
}

#[tokio::main]
async fn main() {
    let cfg = parse_args();
    ensure_go_oracle_built().expect("go-mimir must build");
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");

    let tmp_guard;
    let work_dir = match cfg.work_dir {
        Some(ref p) => {
            fs::create_dir_all(p).unwrap();
            p.clone()
        }
        None => {
            tmp_guard = tempfile::tempdir().unwrap();
            tmp_guard.path().to_path_buf()
        }
    };

    let layout = Arc::new(SeriesLayout::new(cfg.series));
    println!(
        "=== Mimir 3.2.0 Production Kafka `ingest_storage` Benchmark ===\n\
         Series: {} (prod 99.5% + preprod 0.5%) | Local 2h ULID blocks: {}\n\
         Steady rate: {} samples/s for {}s | Kafka: {} | Heavy query target: {} series",
        cfg.series,
        cfg.local_blocks,
        cfg.rate_samples_per_sec,
        cfg.steady_secs,
        cfg.brokers.as_deref().unwrap_or("in-process TCP Kafka v2 (Zstd)"),
        layout.heavy_count
    );

    let go_res = if cfg.run_go {
        Some(run_engine("Go Mimir", true, &go_mimir_bin(), &cfg, &layout, &work_dir).await)
    } else {
        None
    };

    let rust_res = if cfg.run_rust {
        Some(
            run_engine(
                "Rust mimir-rs",
                false,
                &rust_mimir_release_bin(),
                &cfg,
                &layout,
                &work_dir,
            )
            .await,
        )
    } else {
        None
    };

    let print_engine = |r: &EngineProdResult| {
        let p1_rss_per_series =
            (r.p1_stats.rss_anon_mib * 1024.0 * 1024.0) / (cfg.series as f64);
        let p2_rss_per_series = (r.p2_stats.rss_anon_mib * 1024.0 * 1024.0)
            / r.p2_metrics.memory_series.max(1.0);
        let cache_hit_pct = match (
            r.p2_metrics.postings_cache_hits,
            r.p2_metrics.postings_cache_misses,
        ) {
            (Some(h), Some(m)) if h + m > 0.0 => format!("{:.1}%", h * 100.0 / (h + m)),
            _ => "n/a".to_string(),
        };
        println!(
            "\n--- {} Summary ---\n\
             [Phase 1 Catch-up]  {:.2} s | {:.0} samples/s ({:.0} rec/s) | raw={:.1} B/samp wire={:.1} B/samp ({:.2}x)\n\
             [Phase 1 Memory]    RssAnon={:.1} MiB ({:.0} B/series) | RssFile={:.1} MiB | VmRSS={:.1} MiB | CPU={:.2} s\n\
             [Phase 2 Steady]    {:.1} s ({} samples) | CPU={:.2} µs/sample | RssAnon={:.1} MiB ({:.0} B/series) | RssFile={:.1} MiB | VmHWM={:.1} MiB\n\
             [Phase 2 Reads]     QS small (~{:.0}s): p50={:.2} ms p99={:.2} ms | QS med: p50={:.2} ms p99={:.2} ms | QS heavy (~{:.0}s): p50={:.2} ms p99={:.2} ms\n\
             [Phase 2 Reads+]    LNV p50={:.2} ms | LVC p50={:.2} ms | UserStats p50={:.2} ms | cache_hit={} | errors={}\n\
             [Phase 2 Kafka]     delay p50={:.3}s p99={:.3}s | proc p50={:.2}ms p99={:.2}ms\n\
             [Phase 3 Compact]   wall={:.2} s | pre_RssAnon={:.1} MiB -> peak_RssAnon={:.1} MiB (+{:.1} MiB sawtooth) | peak_VmRSS={:.1} MiB\n\
             [Phase 4 SIGKILL]   /ready={:.2} s | 0-lag catch-up={:.2} s | RssAnon={:.1} MiB",
            r.name,
            r.p1_wall_s,
            r.p1_samples as f64 / r.p1_wall_s,
            r.p1_records as f64 / r.p1_wall_s,
            r.p1_raw_bytes_per_sample,
            r.p1_wire_bytes_per_sample,
            r.p1_raw_bytes_per_sample / r.p1_wire_bytes_per_sample.max(0.01),
            r.p1_stats.rss_anon_mib,
            p1_rss_per_series,
            r.p1_stats.rss_file_mib,
            r.p1_stats.vm_rss_mib,
            r.p1_stats.cpu_secs,
            r.p2_wall_s,
            r.p2_samples,
            r.p2_cpu_us_per_sample,
            r.p2_stats.rss_anon_mib,
            p2_rss_per_series,
            r.p2_stats.rss_file_mib,
            r.p2_stats.vm_hwm_mib,
            r.qs_small_avg_series,
            r.qs_small_p50_ms,
            r.qs_small_p99_ms,
            r.qs_med_p50_ms,
            r.qs_med_p99_ms,
            r.qs_heavy_avg_series,
            r.qs_heavy_p50_ms,
            r.qs_heavy_p99_ms,
            r.lnv_p50_ms,
            r.lvc_p50_ms,
            r.us_p50_ms,
            cache_hit_pct,
            r.read_errors,
            r.p2_metrics.consume_delay_p50_s.unwrap_or(-1.0),
            r.p2_metrics.consume_delay_p99_s.unwrap_or(-1.0),
            r.p2_metrics.record_proc_p50_ms.unwrap_or(-1.0),
            r.p2_metrics.record_proc_p99_ms.unwrap_or(-1.0),
            r.p3_compaction_s,
            r.p3_pre_rss_anon_mib,
            r.p3_peak_rss_anon_mib,
            (r.p3_peak_rss_anon_mib - r.p3_pre_rss_anon_mib).max(0.0),
            r.p3_peak_vm_rss_mib,
            r.p4_ready_s,
            r.p4_catchup_s,
            r.p4_stats.rss_anon_mib,
        );
    };

    if let Some(ref g) = go_res {
        print_engine(g);
    }
    if let Some(ref r) = rust_res {
        print_engine(r);
    }
    if let (Some(g), Some(r)) = (go_res, rust_res) {
        let g_tp = g.p1_samples as f64 / g.p1_wall_s;
        let r_tp = r.p1_samples as f64 / r.p1_wall_s;
        println!("\n=== Production Head-to-Head (Go / Rust) ===");
        println!("Phase 1 catch-up speedup:     {:.2}x", r_tp / g_tp);
        println!(
            "Steady CPU reduction:         {:.2}x ({:.1}% less CPU)",
            g.p2_cpu_us_per_sample / r.p2_cpu_us_per_sample.max(0.001),
            (1.0 - r.p2_cpu_us_per_sample / g.p2_cpu_us_per_sample.max(0.001)) * 100.0
        );
        println!(
            "RssAnon heap reduction:       {:.2}x ({:.1}% less heap RAM)",
            g.p2_stats.rss_anon_mib / r.p2_stats.rss_anon_mib.max(0.01),
            (1.0 - r.p2_stats.rss_anon_mib / g.p2_stats.rss_anon_mib.max(0.01)) * 100.0
        );
        println!(
            "QueryStream small p99:        {:.2}x",
            g.qs_small_p99_ms / r.qs_small_p99_ms.max(0.001)
        );
        println!(
            "QueryStream heavy p99:        {:.2}x",
            g.qs_heavy_p99_ms / r.qs_heavy_p99_ms.max(0.001)
        );
        println!(
            "Head compaction speedup:      {:.2}x",
            g.p3_compaction_s / r.p3_compaction_s.max(0.001)
        );
        println!(
            "Compaction peak RssAnon red.: {:.2}x",
            g.p3_peak_rss_anon_mib / r.p3_peak_rss_anon_mib.max(0.01)
        );
        println!(
            "SIGKILL /ready speedup:       {:.2}x",
            g.p4_ready_s / r.p4_ready_s.max(0.001)
        );
        println!(
            "SIGKILL 0-lag catchup:        {:.2}x",
            g.p4_catchup_s / r.p4_catchup_s.max(0.001)
        );
    }
}
