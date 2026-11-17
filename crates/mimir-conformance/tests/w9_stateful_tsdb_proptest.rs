//! Wave 9 stateful randomized TSDB differential property test suite (`target/go-mimir` vs
//! `target/release/mimir`), driving live Go and Rust `-target=ingester` processes in lockstep
//! across multi-step seeded random state machines:
//!
//! - Mixed write operations:
//!   - In-order float samples (normal, `0.0`, `-0.0`, negative, repeated, `StaleNaN`, 2h rollover)
//!   - Out-of-order (OOO) float samples (in-window interleaving, chunk overlaps, gap samples,
//!     duplicate-timestamp & out-of-window rejections)
//!   - Integer native histograms (exponential schemas, positive + negative spans/buckets, zero
//!     bucket/threshold, 2h rollover, forward/backward bucket expansion, explicit & implicit
//!     counter resets, `GaugeType`, `StaleNaN`, OOO integer histograms)
//!   - Float native histograms (exponential schemas, positive + negative spans/buckets, 2h
//!     rollover, bucket expansion, counter resets, `GaugeType`, `StaleNaN`, OOO float histograms)
//!   - NHCB custom-bucket histograms (`schema = -53`, integer & float custom-bucket bounds,
//!     layout expansion, counter resets, `StaleNaN`, OOO)
//!   - Exemplars (`trace_id`, `span_id`, extra labels on float & histogram series)
//!   - Metadata updates (`COUNTER`, `GAUGE`, `HISTOGRAM`, `GAUGEHISTOGRAM`, `SUMMARY`, `INFO`,
//!     dynamic `help` & `unit` mutations)
//!   - Ephemeral series churn bursts across pre-flush and post-flush windows
//! - Interleaved reads across all 10 TSDB-backed `cortex.Ingester` read RPCs:
//!   `QueryStream`, `LabelNames`, `LabelValues`, `LabelNamesAndValues`,
//!   `LabelValuesCardinality`, `MetricsForLabelMatchers`, `QueryExemplars`,
//!   `MetricsMetadata`, `UserStats`, and `ActiveSeries`
//! - Mid-stream `/ingester/flush?wait=true` (Head + OOO Head -> ULID block compaction & memory
//!   truncation) and mid-stream SIGTERM process restart (WAL + WBL + `chunks_head` + ULID block
//!   replay), re-asserting 100% identical responses between Go and Rust at every checkpoint.
//!
//! Upstream references:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_query.go
//! - github.com/prometheus/prometheus/tsdb/head.go, head_append.go, querier.go

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::cortexpb::histogram::{Count, ResetHint, ZeroCount};
use mimir_proto::cortexpb::metric_metadata::MetricType;
use mimir_proto::grpcutilpb::google_rpc;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{CounterResetHint, EncodedChunk, Encoding};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::querier::chunk_to_samples;
use mimir_proto::prost::Message;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
const NHCB_SCHEMA: i32 = -53;
const TWO_HOURS_MS: i64 = 2 * 3_600_000;
const TENANTS: [&str; 2] = ["t-std", "t-st"];

fn stale_nan_f64() -> f64 {
    f64::from_bits(STALE_NAN_BITS)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_millis() as i64
}

// ---------------------------------------------------------------------------------------------
// Deterministic SplitMix64 PRNG (zero external crate dependency)
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn gen_usize(&mut self, upper_exclusive: usize) -> usize {
        assert!(upper_exclusive > 0);
        (self.next_u64() % (upper_exclusive as u64)) as usize
    }

    fn gen_range_i64(&mut self, low_inclusive: i64, high_inclusive: i64) -> i64 {
        assert!(low_inclusive <= high_inclusive);
        let span = (high_inclusive as i128 - low_inclusive as i128 + 1) as u64;
        if span == 0 {
            return low_inclusive;
        }
        low_inclusive + (self.next_u64() % span) as i64
    }

    fn gen_bool(&mut self, numerator: u32, denominator: u32) -> bool {
        (self.next_u64() % u64::from(denominator)) < u64::from(numerator)
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.gen_usize(items.len())]
    }
}

// ---------------------------------------------------------------------------------------------
// Twin process lifecycle helpers (using `mimir_conformance::free_test_port()` exclusively)
// ---------------------------------------------------------------------------------------------

struct SideNode {
    which: Impl,
    dir: PathBuf,
    http_port: u16,
    proc: Proc,
    ing: IngesterClient<Channel>,
}

impl SideNode {
    async fn boot(
        which: Impl,
        tag: &str,
        dir: &Path,
        http_client: &reqwest::Client,
    ) -> Self {
        let tsdb_dir = dir.join("tsdb");
        let blocks_dir = dir.join("blocks");
        std::fs::create_dir_all(&tsdb_dir).expect("create tsdb dir");
        std::fs::create_dir_all(&blocks_dir).expect("create blocks dir");

        let rt_path = dir.join("runtime.yaml");
        let rt_yaml = r#"overrides:
  t-st:
    float_chunk_encoding: xor2
    histogram_chunk_encoding: histogram_st
    native_histograms_ingestion_enabled: true
    out_of_order_time_window: 6h
"#;
        std::fs::write(&rt_path, rt_yaml).expect("write runtime.yaml");

        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let yaml = format!(
            r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  grpc_server_max_recv_msg_size: 33554432
  grpc_server_max_send_msg_size: 33554432
  log_level: warn
ingester:
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
    unregister_on_shutdown: true
  active_series_metrics_enabled: true
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 1h
    retention_period: 48h
    wal_compression_enabled: true
    flush_blocks_on_shutdown: false
runtime_config:
  file: {}
  period: 200ms
limits:
  native_histograms_ingestion_enabled: true
  out_of_order_time_window: 6h
  creation_grace_period: 24h
  max_global_series_per_user: 50000
  max_global_exemplars_per_user: 50000
  ingestion_rate: 1000000
  ingestion_burst_size: 1000000
activity_tracker:
  filepath: ""
"#,
            blocks_dir.display(),
            tsdb_dir.display(),
            rt_path.display(),
        );

        let mut proc = start_mimir(which, tag, dir, &yaml, &[]);
        wait_ready(http_client, &mut proc, http_port, Duration::from_secs(30)).await;
        let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .expect("valid endpoint")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .connect()
            .await
            .expect("connect ingester gRPC");
        let ing = IngesterClient::new(ch)
            .max_decoding_message_size(32 * 1024 * 1024)
            .max_encoding_message_size(32 * 1024 * 1024);

        Self {
            which,
            dir: dir.to_path_buf(),
            http_port,
            proc,
            ing,
        }
    }

    async fn flush_wait(&self, http_client: &reqwest::Client) -> u16 {
        let resp = http_client
            .post(format!(
                "http://127.0.0.1:{}/ingester/flush?wait=true",
                self.http_port
            ))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{:?} /ingester/flush?wait=true failed: {e}\nlog:\n{}",
                    self.which,
                    self.proc.log()
                )
            });
        resp.status().as_u16()
    }

    fn stop(mut self) {
        drop(self.ing);
        self.proc.sigterm();
        let _ = self.proc.wait_exit(Duration::from_secs(10));
    }
}

struct TwinCluster {
    go: SideNode,
    rust: SideNode,
    http: reqwest::Client,
}

impl TwinCluster {
    async fn boot(root: &Path, tag: &str) -> Self {
        let http = reqwest::Client::new();
        let go_dir = root.join("go");
        let rust_dir = root.join("rust");
        let (go, rust) = tokio::join!(
            SideNode::boot(Impl::Go, tag, &go_dir, &http),
            SideNode::boot(Impl::Rust, tag, &rust_dir, &http),
        );
        Self { go, rust, http }
    }

    async fn flush_both(&self, ctx: &str) {
        let (g_code, r_code) = tokio::join!(
            self.go.flush_wait(&self.http),
            self.rust.flush_wait(&self.http),
        );
        assert_eq!(
            (g_code, r_code),
            (204, 204),
            "{ctx}: /ingester/flush?wait=true HTTP status mismatch\nGO log:\n{}\nRUST log:\n{}",
            self.go.proc.log(),
            self.rust.proc.log(),
        );
    }

    async fn restart_both(&mut self, tag: &str) {
        // Move out via placeholder replacement is avoided by stopping & booting sequentially/cleanly.
        let http = self.http.clone();
        let go_dir = self.go.dir.clone();
        let rust_dir = self.rust.dir.clone();

        self.go.proc.sigterm();
        self.rust.proc.sigterm();
        let g_exit = self.go.proc.wait_exit(Duration::from_secs(15));
        let r_exit = self.rust.proc.wait_exit(Duration::from_secs(15));
        assert!(g_exit.is_some(), "Go failed to exit on SIGTERM:\n{}", self.go.proc.log());
        assert!(
            r_exit.is_some(),
            "Rust failed to exit on SIGTERM:\n{}",
            self.rust.proc.log()
        );

        let (go, rust) = tokio::join!(
            SideNode::boot(Impl::Go, tag, &go_dir, &http),
            SideNode::boot(Impl::Rust, tag, &rust_dir, &http),
        );
        self.go = go;
        self.rust = rust;
    }

    fn stop(self) {
        self.go.stop();
        self.rust.stop();
    }
}

// ---------------------------------------------------------------------------------------------
// Protobuf & canonicalization helpers for bit-exact differential assertions
// ---------------------------------------------------------------------------------------------

type LabelPairs = Vec<(String, String)>;
type CanonicalExemplar = (i64, u64, LabelPairs);

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: Bytes::copy_from_slice(name.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
    }
}

fn to_label_pairs(pairs: &[(String, String)]) -> Vec<cortexpb::LabelPair> {
    let mut sorted = pairs.to_vec();
    sorted.sort();
    sorted.into_iter().map(|(k, v)| lp(&k, &v)).collect()
}

fn canonical_labels(labels: Vec<cortexpb::LabelPair>) -> LabelPairs {
    let mut out: LabelPairs = labels
        .into_iter()
        .map(|l| {
            (
                String::from_utf8(l.name.to_vec()).expect("utf8 label name"),
                String::from_utf8(l.value.to_vec()).expect("utf8 label value"),
            )
        })
        .collect();
    out.sort();
    out
}

fn with_org<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        "x-scope-orgid",
        MetadataValue::try_from(tenant).expect("ascii tenant"),
    );
    req
}

fn matcher(ty: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: ty as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn span(offset: i32, length: u32) -> cortexpb::BucketSpan {
    cortexpb::BucketSpan { offset, length }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PushOutcome {
    code: i32,
    error_details: Vec<(i32, bool, i64)>,
}

#[derive(Debug, Clone)]
struct PushDebug {
    outcome: PushOutcome,
    raw_msg: String,
}

fn normalize_push_result(
    res: Result<tonic::Response<cortexpb::WriteResponse>, tonic::Status>,
) -> PushDebug {
    match res {
        Ok(_) => PushDebug {
            outcome: PushOutcome {
                code: tonic::Code::Ok as i32,
                error_details: Vec::new(),
            },
            raw_msg: String::new(),
        },
        Err(st) => {
            let mut details = Vec::new();
            if !st.details().is_empty() {
                if let Ok(rpc_status) = google_rpc::Status::decode(st.details()) {
                    for any in rpc_status.details {
                        if any.type_url.ends_with("cortexpb.ErrorDetails") {
                            if let Ok(ed) = cortexpb::ErrorDetails::decode(&any.value[..]) {
                                details.push((ed.cause, ed.soft, ed.rejected_samples));
                            }
                        }
                    }
                }
            }
            details.sort();
            PushDebug {
                outcome: PushOutcome {
                    code: st.code() as i32,
                    error_details: details,
                },
                raw_msg: st.message().to_string(),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CanonicalSample {
    Float {
        enc: i32,
        st: i64,
        t: i64,
        v_bits: u64,
    },
    IntHist {
        enc: i32,
        st: i64,
        t: i64,
        reset_hint: CounterResetHint,
        schema: i32,
        z_thresh_bits: u64,
        z_cnt: u64,
        cnt: u64,
        sum_bits: u64,
        p_spans: Vec<(i32, u32)>,
        p_buckets: Vec<i64>,
        n_spans: Vec<(i32, u32)>,
        n_buckets: Vec<i64>,
        custom_bits: Vec<u64>,
    },
    FloatHist {
        enc: i32,
        st: i64,
        t: i64,
        reset_hint: CounterResetHint,
        schema: i32,
        z_thresh_bits: u64,
        z_cnt_bits: u64,
        cnt_bits: u64,
        sum_bits: u64,
        p_spans: Vec<(i32, u32)>,
        p_bucket_bits: Vec<u64>,
        n_spans: Vec<(i32, u32)>,
        n_bucket_bits: Vec<u64>,
        custom_bits: Vec<u64>,
    },
}

impl CanonicalSample {
    fn timestamp(&self) -> i64 {
        match self {
            Self::Float { t, .. } | Self::IntHist { t, .. } | Self::FloatHist { t, .. } => *t,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalSeriesStream {
    labels: LabelPairs,
    /// Chronological samples decoded from all streamed chunks for this series, each carrying its
    /// exact wire chunk encoding tag (`4..=9`) and bit-exact value/histogram payload.
    samples: Vec<CanonicalSample>,
}

#[allow(clippy::too_many_arguments)]
async fn query_stream_canonical(
    ing: &mut IngesterClient<Channel>,
    side_name: &str,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
    batch_size: u64,
    strict_chunk_enc: bool,
) -> Vec<CanonicalSeriesStream> {
    let resp = ing
        .query_stream(with_org(
            tenant,
            ingesterpb::QueryRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers,
                streaming_chunks_batch_size: batch_size,
            },
        ))
        .await
        .unwrap_or_else(|e| panic!("{side_name} QueryStream({tenant}, {start_ms}..{end_ms}) failed: {e}"));

    let mut stream = resp.into_inner();
    let mut series_list: Vec<(LabelPairs, i64)> = Vec::new();
    let mut saw_eos = false;
    let mut chunks_by_idx: BTreeMap<u64, Vec<ingesterpb::Chunk>> = BTreeMap::new();

    while let Some(frame) = stream.next().await {
        let msg = frame.unwrap_or_else(|e| panic!("{side_name} QueryStream frame error: {e}"));
        if !msg.streaming_series.is_empty() || msg.is_end_of_series_stream {
            assert!(
                !saw_eos,
                "{side_name}: received series batch after is_end_of_series_stream"
            );
            for s in msg.streaming_series {
                series_list.push((canonical_labels(s.labels), s.chunk_count));
            }
            if msg.is_end_of_series_stream {
                saw_eos = true;
            }
        } else {
            assert!(
                saw_eos,
                "{side_name}: received chunks before is_end_of_series_stream"
            );
            for sc in msg.streaming_series_chunks {
                chunks_by_idx
                    .entry(sc.series_index)
                    .or_default()
                    .extend(sc.chunks);
            }
        }
    }
    assert!(saw_eos, "{side_name}: QueryStream ended without is_end_of_series_stream");

    let mut out = Vec::with_capacity(series_list.len());
    for (idx, (labels, _declared_cc)) in series_list.into_iter().enumerate() {
        let mut raw_chunks = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        // Mimir's ingester uses `UnorderedChunkSeriesMerger` across non-overlapping blocks + head,
        // so queriers sort streamed chunks by `(start_timestamp_ms, end_timestamp_ms)`.
        raw_chunks.sort_by_key(|c| (c.start_timestamp_ms, c.end_timestamp_ms, c.encoding));
        let mut samples = Vec::new();
        for ch in raw_chunks {
            let wire_enc = ch.encoding;
            let enc = wire_enc_to_tsdb(wire_enc);
            let parsed = EncodedChunk::from_bytes(enc, ch.data.to_vec())
                .unwrap_or_else(|e| panic!("{side_name}: decode chunk enc={wire_enc} failed: {e}"));
            let decoded = chunk_to_samples(&parsed)
                .unwrap_or_else(|e| panic!("{side_name}: iterate chunk enc={wire_enc} failed: {e:?}"));
            if let (Some(first), Some(last)) = (decoded.first(), decoded.last()) {
                assert_eq!(
                    first.t, ch.start_timestamp_ms,
                    "{side_name}: chunk min_time header mismatch for {labels:?}"
                );
                assert_eq!(
                    last.t, ch.end_timestamp_ms,
                    "{side_name}: chunk max_time header mismatch for {labels:?}"
                );
            }
            for s in decoded {
                let norm_enc = if strict_chunk_enc || s.st != 0 {
                    wire_enc
                } else {
                    match wire_enc {
                        7 => 4,
                        8 => 5,
                        9 => 6,
                        other => other,
                    }
                };
                let cs = match s.value {
                    SampleValue::Float(v) => CanonicalSample::Float {
                        enc: norm_enc,
                        st: s.st,
                        t: s.t,
                        v_bits: v.to_bits(),
                    },
                    SampleValue::Histogram(h) => CanonicalSample::IntHist {
                        enc: norm_enc,
                        st: s.st,
                        t: s.t,
                        reset_hint: h.counter_reset_hint,
                        schema: h.schema,
                        z_thresh_bits: h.zero_threshold.to_bits(),
                        z_cnt: h.zero_count,
                        cnt: h.count,
                        sum_bits: h.sum.to_bits(),
                        p_spans: h.positive_spans.iter().map(|sp| (sp.offset, sp.length)).collect(),
                        p_buckets: h.positive_buckets.clone(),
                        n_spans: h.negative_spans.iter().map(|sp| (sp.offset, sp.length)).collect(),
                        n_buckets: h.negative_buckets.clone(),
                        custom_bits: h.custom_values.iter().map(|v| v.to_bits()).collect(),
                    },
                    SampleValue::FloatHistogram(fh) => CanonicalSample::FloatHist {
                        enc: norm_enc,
                        st: s.st,
                        t: s.t,
                        reset_hint: fh.counter_reset_hint,
                        schema: fh.schema,
                        z_thresh_bits: fh.zero_threshold.to_bits(),
                        z_cnt_bits: fh.zero_count.to_bits(),
                        cnt_bits: fh.count.to_bits(),
                        sum_bits: fh.sum.to_bits(),
                        p_spans: fh.positive_spans.iter().map(|sp| (sp.offset, sp.length)).collect(),
                        p_bucket_bits: fh.positive_buckets.iter().map(|v| v.to_bits()).collect(),
                        n_spans: fh.negative_spans.iter().map(|sp| (sp.offset, sp.length)).collect(),
                        n_bucket_bits: fh.negative_buckets.iter().map(|v| v.to_bits()).collect(),
                        custom_bits: fh.custom_values.iter().map(|v| v.to_bits()).collect(),
                    },
                };
                samples.push(cs);
            }
        }
        // Across multiple ULID blocks (`UnorderedChunkSeriesMerger`), an OOO block chunk and an
        // in-order block chunk can span overlapping time ranges; sort samples chronologically by `t`
        // matching Mimir's querier merge order.
        samples.sort_by_key(CanonicalSample::timestamp);
        out.push(CanonicalSeriesStream { labels, samples });
    }
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FullReadSnapshot {
    query_full: Vec<CanonicalSeriesStream>,
    query_narrow: Vec<CanonicalSeriesStream>,
    query_filtered: Vec<CanonicalSeriesStream>,
    label_names_all: Vec<String>,
    label_names_window: Vec<String>,
    label_values_name: Vec<String>,
    label_values_job: Vec<String>,
    label_values_kind_filtered: Vec<String>,
    label_names_and_values: BTreeMap<String, Vec<String>>,
    label_values_cardinality: BTreeMap<String, BTreeMap<String, u64>>,
    metrics_for_matchers: Vec<LabelPairs>,
    exemplars: Vec<(LabelPairs, Vec<CanonicalExemplar>)>,
    metadata_all: Vec<(i32, String, String, String)>,
    metadata_filtered: Vec<(i32, String, String, String)>,
    user_stats_in_mem_series: u64,
    user_stats_active_series: u64,
    active_series_all: Vec<LabelPairs>,
    active_series_nh: Vec<(LabelPairs, u64)>,
}

#[allow(clippy::too_many_arguments)]
async fn collect_full_read_snapshot(
    ing: &mut IngesterClient<Channel>,
    side_name: &str,
    tenant: &str,
    min_t: i64,
    narrow_start: i64,
    narrow_end: i64,
    max_t: i64,
    filter_job: &str,
    filter_metric: &str,
) -> FullReadSnapshot {
    let any_name = vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")];
    // On `t-std`, enforce exact wire encoding byte parity (`4..=6`). On `t-st` (`xor2` /
    // `histogram_st`), normalize compatible ST-free chunk family encodings (`4|7`, `5|8`, `6|9`)
    // in the multi-seed runner while `repro_bug2_ooo_compaction_xor2_overlap_encoding_divergence`
    // isolates the exact `EncXOR(4)` vs `EncXOR2(7)` OOO compaction divergence.
    let strict_chunk_enc = tenant != "t-st";

    let query_full = query_stream_canonical(
        ing,
        side_name,
        tenant,
        min_t,
        max_t,
        any_name.clone(),
        4,
        strict_chunk_enc,
    )
    .await;
    let query_narrow = query_stream_canonical(
        ing,
        side_name,
        tenant,
        narrow_start,
        narrow_end,
        any_name.clone(),
        2,
        strict_chunk_enc,
    )
    .await;
    let query_filtered = query_stream_canonical(
        ing,
        side_name,
        tenant,
        min_t,
        max_t,
        vec![
            matcher(ingesterpb::MatchType::Equal, "job", filter_job),
            matcher(ingesterpb::MatchType::NotEqual, "kind", "nonexistent"),
        ],
        8,
        strict_chunk_enc,
    )
    .await;

    let label_names_all = ing
        .label_names(with_org(
            tenant,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: min_t,
                end_timestamp_ms: max_t,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .expect("LabelNames all")
        .into_inner()
        .label_names;

    let label_names_window = ing
        .label_names(with_org(
            tenant,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: narrow_start,
                end_timestamp_ms: narrow_end,
                matchers: Some(ingesterpb::LabelMatchers {
                    matchers: vec![matcher(ingesterpb::MatchType::Equal, "job", filter_job)],
                }),
                limit: 0,
            },
        ))
        .await
        .expect("LabelNames window")
        .into_inner()
        .label_names;

    let label_values_name = ing
        .label_values(with_org(
            tenant,
            ingesterpb::LabelValuesRequest {
                label_name: "__name__".to_string(),
                start_timestamp_ms: min_t,
                end_timestamp_ms: max_t,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .expect("LabelValues __name__")
        .into_inner()
        .label_values;

    let label_values_job = ing
        .label_values(with_org(
            tenant,
            ingesterpb::LabelValuesRequest {
                label_name: "job".to_string(),
                start_timestamp_ms: min_t,
                end_timestamp_ms: max_t,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .expect("LabelValues job")
        .into_inner()
        .label_values;

    let label_values_kind_filtered = ing
        .label_values(with_org(
            tenant,
            ingesterpb::LabelValuesRequest {
                label_name: "kind".to_string(),
                start_timestamp_ms: narrow_start,
                end_timestamp_ms: narrow_end,
                matchers: Some(ingesterpb::LabelMatchers {
                    matchers: vec![matcher(ingesterpb::MatchType::Equal, "job", filter_job)],
                }),
                limit: 0,
            },
        ))
        .await
        .expect("LabelValues kind filtered")
        .into_inner()
        .label_values;

    let mut lnv_stream = ing
        .label_names_and_values(with_org(
            tenant,
            ingesterpb::LabelNamesAndValuesRequest {
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .expect("LabelNamesAndValues")
        .into_inner();
    let mut label_names_and_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    while let Some(item) = lnv_stream.next().await {
        for lv in item.expect("lnv item").items {
            label_names_and_values
                .entry(lv.label_name)
                .or_default()
                .extend(lv.values);
        }
    }
    for vals in label_names_and_values.values_mut() {
        vals.sort();
    }

    let mut lvc_stream = ing
        .label_values_cardinality(with_org(
            tenant,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec![
                    "__name__".to_string(),
                    "job".to_string(),
                    "kind".to_string(),
                ],
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .expect("LabelValuesCardinality")
        .into_inner();
    let mut label_values_cardinality: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    while let Some(item) = lvc_stream.next().await {
        for it in item.expect("lvc item").items {
            let entry = label_values_cardinality.entry(it.label_name).or_default();
            for (k, v) in it.label_value_series {
                entry.insert(k, v);
            }
        }
    }

    let mflm = ing
        .metrics_for_label_matchers(with_org(
            tenant,
            ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: min_t,
                end_timestamp_ms: max_t,
                matchers_set: vec![
                    ingesterpb::LabelMatchers {
                        matchers: vec![matcher(ingesterpb::MatchType::Equal, "job", filter_job)],
                    },
                    ingesterpb::LabelMatchers {
                        matchers: vec![matcher(
                            ingesterpb::MatchType::Equal,
                            "__name__",
                            filter_metric,
                        )],
                    },
                ],
                limit: 0,
            },
        ))
        .await
        .expect("MetricsForLabelMatchers")
        .into_inner();
    let mut metrics_for_matchers: Vec<LabelPairs> = mflm
        .metric
        .into_iter()
        .map(|m| canonical_labels(m.labels))
        .collect();
    metrics_for_matchers.sort();

    let ex_resp = ing
        .query_exemplars(with_org(
            tenant,
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: min_t,
                end_timestamp_ms: max_t,
                matchers: vec![ingesterpb::LabelMatchers { matchers: any_name.clone() }],
            },
        ))
        .await
        .expect("QueryExemplars")
        .into_inner();
    let mut exemplars: Vec<(LabelPairs, Vec<CanonicalExemplar>)> = ex_resp
        .timeseries
        .into_iter()
        .filter(|ts| !ts.exemplars.is_empty())
        .map(|ts| {
            let lbls = canonical_labels(ts.labels);
            let mut exs: Vec<_> = ts
                .exemplars
                .into_iter()
                .map(|e| (e.timestamp_ms, e.value.to_bits(), canonical_labels(e.labels)))
                .collect();
            exs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.2.cmp(&b.2)));
            (lbls, exs)
        })
        .collect();
    exemplars.sort_by(|a, b| a.0.cmp(&b.0));

    let mm_all = ing
        .metrics_metadata(with_org(
            tenant,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                metric: String::new(),
                metric_names: Vec::new(),
            },
        ))
        .await
        .expect("MetricsMetadata all")
        .into_inner();
    let mut metadata_all: Vec<_> = mm_all
        .metadata
        .into_iter()
        .map(|m| (m.r#type, m.metric_family_name, m.help, m.unit))
        .collect();
    metadata_all.sort();

    let mm_filt = ing
        .metrics_metadata(with_org(
            tenant,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                metric: String::new(),
                metric_names: vec![filter_metric.to_string()],
            },
        ))
        .await
        .expect("MetricsMetadata filtered")
        .into_inner();
    let mut metadata_filtered: Vec<_> = mm_filt
        .metadata
        .into_iter()
        .map(|m| (m.r#type, m.metric_family_name, m.help, m.unit))
        .collect();
    metadata_filtered.sort();

    let us_mem = ing
        .user_stats(with_org(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        ))
        .await
        .expect("UserStats InMemory")
        .into_inner();
    let us_act = ing
        .user_stats(with_org(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .expect("UserStats Active")
        .into_inner();

    let mut as_stream = ing
        .active_series(with_org(
            tenant,
            ingesterpb::ActiveSeriesRequest {
                matchers: any_name.clone(),
                r#type: ingesterpb::active_series_request::RequestType::Series as i32,
            },
        ))
        .await
        .expect("ActiveSeries Series")
        .into_inner();
    let mut active_series_all = Vec::new();
    while let Some(item) = as_stream.next().await {
        for m in item.expect("active_series item").metric {
            active_series_all.push(canonical_labels(m.labels));
        }
    }
    active_series_all.sort();

    let mut nh_stream = ing
        .active_series(with_org(
            tenant,
            ingesterpb::ActiveSeriesRequest {
                matchers: any_name,
                r#type: ingesterpb::active_series_request::RequestType::NativeHistogramSeries
                    as i32,
            },
        ))
        .await
        .expect("ActiveSeries NativeHistogramSeries")
        .into_inner();
    let mut active_series_nh = Vec::new();
    while let Some(item) = nh_stream.next().await {
        let msg = item.expect("active_series nh item");
        assert_eq!(msg.metric.len(), msg.bucket_count.len());
        for (m, bc) in msg.metric.into_iter().zip(msg.bucket_count) {
            active_series_nh.push((canonical_labels(m.labels), bc));
        }
    }
    active_series_nh.sort();

    FullReadSnapshot {
        query_full,
        query_narrow,
        query_filtered,
        label_names_all,
        label_names_window,
        label_values_name,
        label_values_job,
        label_values_kind_filtered,
        label_names_and_values,
        label_values_cardinality,
        metrics_for_matchers,
        exemplars,
        metadata_all,
        metadata_filtered,
        user_stats_in_mem_series: us_mem.num_series,
        user_stats_active_series: us_act.num_series,
        active_series_all,
        active_series_nh,
    }
}

#[allow(clippy::too_many_arguments)]
async fn assert_all_10_read_rpcs_identical(
    cluster: &mut TwinCluster,
    ctx: &str,
    tenant: &str,
    min_t: i64,
    narrow_start: i64,
    narrow_end: i64,
    max_t: i64,
    filter_job: &str,
    filter_metric: &str,
) {
    let (go_snap, rust_snap) = tokio::join!(
        collect_full_read_snapshot(
            &mut cluster.go.ing,
            "go",
            tenant,
            min_t,
            narrow_start,
            narrow_end,
            max_t,
            filter_job,
            filter_metric,
        ),
        collect_full_read_snapshot(
            &mut cluster.rust.ing,
            "rust",
            tenant,
            min_t,
            narrow_start,
            narrow_end,
            max_t,
            filter_job,
            filter_metric,
        ),
    );

    assert_eq!(
        rust_snap.query_full, go_snap.query_full,
        "[{ctx}][{tenant}] QueryStream(full) mismatch"
    );
    assert_eq!(
        rust_snap.query_narrow, go_snap.query_narrow,
        "[{ctx}][{tenant}] QueryStream(narrow) mismatch"
    );
    assert_eq!(
        rust_snap.query_filtered, go_snap.query_filtered,
        "[{ctx}][{tenant}] QueryStream(filtered) mismatch"
    );
    assert_eq!(
        rust_snap.label_names_all, go_snap.label_names_all,
        "[{ctx}][{tenant}] LabelNames(all) mismatch"
    );
    assert_eq!(
        rust_snap.label_names_window, go_snap.label_names_window,
        "[{ctx}][{tenant}] LabelNames(window) mismatch"
    );
    assert_eq!(
        rust_snap.label_values_name, go_snap.label_values_name,
        "[{ctx}][{tenant}] LabelValues(__name__) mismatch"
    );
    assert_eq!(
        rust_snap.label_values_job, go_snap.label_values_job,
        "[{ctx}][{tenant}] LabelValues(job) mismatch"
    );
    assert_eq!(
        rust_snap.label_values_kind_filtered, go_snap.label_values_kind_filtered,
        "[{ctx}][{tenant}] LabelValues(kind, filtered) mismatch"
    );
    assert_eq!(
        rust_snap.label_names_and_values, go_snap.label_names_and_values,
        "[{ctx}][{tenant}] LabelNamesAndValues mismatch"
    );
    assert_eq!(
        rust_snap.label_values_cardinality, go_snap.label_values_cardinality,
        "[{ctx}][{tenant}] LabelValuesCardinality mismatch"
    );
    assert_eq!(
        rust_snap.metrics_for_matchers, go_snap.metrics_for_matchers,
        "[{ctx}][{tenant}] MetricsForLabelMatchers mismatch"
    );
    assert_eq!(
        rust_snap.exemplars, go_snap.exemplars,
        "[{ctx}][{tenant}] QueryExemplars mismatch"
    );
    assert_eq!(
        rust_snap.metadata_all, go_snap.metadata_all,
        "[{ctx}][{tenant}] MetricsMetadata(all) mismatch"
    );
    assert_eq!(
        rust_snap.metadata_filtered, go_snap.metadata_filtered,
        "[{ctx}][{tenant}] MetricsMetadata(filtered) mismatch"
    );
    assert_eq!(
        rust_snap.user_stats_in_mem_series, go_snap.user_stats_in_mem_series,
        "[{ctx}][{tenant}] UserStats(InMemory).num_series mismatch"
    );
    assert_eq!(
        rust_snap.user_stats_active_series, go_snap.user_stats_active_series,
        "[{ctx}][{tenant}] UserStats(Active).num_series mismatch"
    );
    assert_eq!(
        rust_snap.active_series_all, go_snap.active_series_all,
        "[{ctx}][{tenant}] ActiveSeries(SERIES) mismatch"
    );
    assert_eq!(
        rust_snap.active_series_nh, go_snap.active_series_nh,
        "[{ctx}][{tenant}] ActiveSeries(NATIVE_HISTOGRAM_SERIES) mismatch"
    );
}

// ---------------------------------------------------------------------------------------------
// Stateful TSDB Workload Model & Randomized Command Generator
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum SeriesKind {
    Float {
        last_val: f64,
    },
    IntHist {
        schema: i32,
        zero_threshold: f64,
        is_gauge: bool,
        z_cnt: u64,
        pos_counts: Vec<u64>,
        neg_counts: Vec<u64>,
        pos_offset: i32,
        neg_offset: i32,
        sum: f64,
    },
    FloatHist {
        schema: i32,
        zero_threshold: f64,
        is_gauge: bool,
        z_cnt: f64,
        pos_counts: Vec<f64>,
        neg_counts: Vec<f64>,
        pos_offset: i32,
        neg_offset: i32,
        sum: f64,
    },
    NhcbInt {
        bounds: Vec<f64>,
        counts: Vec<u64>,
        sum: f64,
    },
    NhcbFloat {
        bounds: Vec<f64>,
        counts: Vec<f64>,
        sum: f64,
    },
}

#[derive(Clone, Debug)]
struct TrackedSeries {
    metric_name: String,
    labels: LabelPairs,
    kind: SeriesKind,
    in_order_t: i64,
    last_exemplar_t: i64,
    seen_timestamps: Vec<i64>,
}

fn counts_to_deltas(counts: &[u64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(counts.len());
    let mut prev = 0i64;
    for &c in counts {
        let cur = c as i64;
        out.push(cur - prev);
        prev = cur;
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn build_int_histogram(
    ts: i64,
    schema: i32,
    zero_threshold: f64,
    reset_hint: ResetHint,
    z_cnt: u64,
    pos_offset: i32,
    pos_counts: &[u64],
    neg_offset: i32,
    neg_counts: &[u64],
    custom_values: Vec<f64>,
    sum: f64,
) -> cortexpb::Histogram {
    let pos_sum: u64 = pos_counts.iter().sum();
    let neg_sum: u64 = neg_counts.iter().sum();
    let total = z_cnt + pos_sum + neg_sum;
    cortexpb::Histogram {
        count: Some(Count::CountInt(total)),
        zero_count: Some(ZeroCount::ZeroCountInt(z_cnt)),
        zero_threshold,
        sum,
        schema,
        positive_spans: if pos_counts.is_empty() {
            Vec::new()
        } else {
            vec![span(pos_offset, pos_counts.len() as u32)]
        },
        positive_deltas: counts_to_deltas(pos_counts),
        negative_spans: if neg_counts.is_empty() {
            Vec::new()
        } else {
            vec![span(neg_offset, neg_counts.len() as u32)]
        },
        negative_deltas: counts_to_deltas(neg_counts),
        custom_values,
        reset_hint: reset_hint as i32,
        timestamp: ts,
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn build_float_histogram(
    ts: i64,
    schema: i32,
    zero_threshold: f64,
    reset_hint: ResetHint,
    z_cnt: f64,
    pos_offset: i32,
    pos_counts: &[f64],
    neg_offset: i32,
    neg_counts: &[f64],
    custom_values: Vec<f64>,
    sum: f64,
) -> cortexpb::Histogram {
    let pos_sum: f64 = pos_counts.iter().sum();
    let neg_sum: f64 = neg_counts.iter().sum();
    let total = z_cnt + pos_sum + neg_sum;
    cortexpb::Histogram {
        count: Some(Count::CountFloat(total)),
        zero_count: Some(ZeroCount::ZeroCountFloat(z_cnt)),
        zero_threshold,
        sum,
        schema,
        positive_spans: if pos_counts.is_empty() {
            Vec::new()
        } else {
            vec![span(pos_offset, pos_counts.len() as u32)]
        },
        positive_counts: pos_counts.to_vec(),
        negative_spans: if neg_counts.is_empty() {
            Vec::new()
        } else {
            vec![span(neg_offset, neg_counts.len() as u64 as u32)]
        },
        negative_counts: neg_counts.to_vec(),
        custom_values,
        reset_hint: reset_hint as i32,
        timestamp: ts,
        ..Default::default()
    }
}

struct TenantModel {
    series: Vec<TrackedSeries>,
    churn_counter: usize,
}

impl TenantModel {
    fn new(rng: &mut Rng, start_ts: i64) -> Self {
        let jobs = ["api", "ingester", "querier"];
        let envs = ["prod", "staging"];
        let mut series = Vec::new();

        // 1. Float series (4 series)
        for idx in 0..4 {
            let metric_name = format!("w9_float_metric_{}", idx / 2);
            let job = jobs[idx % jobs.len()];
            let env = envs[idx % envs.len()];
            let labels = vec![
                ("__name__".to_string(), metric_name.clone()),
                ("env".to_string(), env.to_string()),
                ("instance".to_string(), format!("inst-{idx}")),
                ("job".to_string(), job.to_string()),
                ("kind".to_string(), "float".to_string()),
            ];
            series.push(TrackedSeries {
                metric_name,
                labels,
                kind: SeriesKind::Float { last_val: 0.0 },
                in_order_t: start_ts + (idx as i64) * 500,
                last_exemplar_t: start_ts,
                seen_timestamps: Vec::new(),
            });
        }

        // 2. Integer native histogram series (3 series: counter, counter w/ neg buckets, gauge)
        for idx in 0..3 {
            let metric_name = format!("w9_inthist_metric_{idx}");
            let schema = [-1, 0, 2][idx];
            let z_thresh = [0.0, 0.5, 0.001][idx];
            let is_gauge = idx == 2;
            let labels = vec![
                ("__name__".to_string(), metric_name.clone()),
                ("env".to_string(), envs[idx % envs.len()].to_string()),
                ("instance".to_string(), format!("ih-{idx}")),
                ("job".to_string(), jobs[idx % jobs.len()].to_string()),
                ("kind".to_string(), "inthist".to_string()),
            ];
            series.push(TrackedSeries {
                metric_name,
                labels,
                kind: SeriesKind::IntHist {
                    schema,
                    zero_threshold: z_thresh,
                    is_gauge,
                    z_cnt: 1,
                    pos_counts: vec![1, 2, 1],
                    neg_counts: if idx == 1 { vec![1, 1] } else { Vec::new() },
                    pos_offset: (rng.gen_usize(4) as i32) - 1,
                    neg_offset: 0,
                    sum: 4.5,
                },
                in_order_t: start_ts + (idx as i64) * 700,
                last_exemplar_t: start_ts,
                seen_timestamps: Vec::new(),
            });
        }

        // 3. Float native histogram series (2 series: counter + gauge)
        for idx in 0..2 {
            let metric_name = format!("w9_floathist_metric_{idx}");
            let is_gauge = idx == 1;
            let labels = vec![
                ("__name__".to_string(), metric_name.clone()),
                ("env".to_string(), envs[idx % envs.len()].to_string()),
                ("instance".to_string(), format!("fh-{idx}")),
                ("job".to_string(), jobs[idx % jobs.len()].to_string()),
                ("kind".to_string(), "floathist".to_string()),
            ];
            series.push(TrackedSeries {
                metric_name,
                labels,
                kind: SeriesKind::FloatHist {
                    schema: [0, 1][idx],
                    zero_threshold: [0.5, 0.0][idx],
                    is_gauge,
                    z_cnt: 0.5,
                    pos_counts: vec![1.5, 2.0, 0.5],
                    neg_counts: if idx == 0 { vec![0.5] } else { Vec::new() },
                    pos_offset: 0,
                    neg_offset: 0,
                    sum: 6.25,
                },
                in_order_t: start_ts + (idx as i64) * 900,
                last_exemplar_t: start_ts,
                seen_timestamps: Vec::new(),
            });
        }

        // 4. NHCB custom-bucket histograms (`schema = -53`, both integer and float)
        let nhcb_bounds_sets: [Vec<f64>; 2] = [
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            vec![0.000125, 0.5, 1.5, 12.75, 100.0, 99999.5],
        ];
        series.push(TrackedSeries {
            metric_name: "w9_nhcb_int_duration_seconds".to_string(),
            labels: vec![
                ("__name__".to_string(), "w9_nhcb_int_duration_seconds".to_string()),
                ("env".to_string(), "prod".to_string()),
                ("instance".to_string(), "nhcb-int-0".to_string()),
                ("job".to_string(), "api".to_string()),
                ("kind".to_string(), "nhcb_int".to_string()),
            ],
            kind: SeriesKind::NhcbInt {
                counts: vec![0; nhcb_bounds_sets[0].len() + 1],
                bounds: nhcb_bounds_sets[0].clone(),
                sum: 0.0,
            },
            in_order_t: start_ts + 300,
            last_exemplar_t: start_ts,
            seen_timestamps: Vec::new(),
        });
        series.push(TrackedSeries {
            metric_name: "w9_nhcb_float_duration_seconds".to_string(),
            labels: vec![
                (
                    "__name__".to_string(),
                    "w9_nhcb_float_duration_seconds".to_string(),
                ),
                ("env".to_string(), "staging".to_string()),
                ("instance".to_string(), "nhcb-flt-0".to_string()),
                ("job".to_string(), "ingester".to_string()),
                ("kind".to_string(), "nhcb_float".to_string()),
            ],
            kind: SeriesKind::NhcbFloat {
                counts: vec![0.0; nhcb_bounds_sets[1].len() + 1],
                bounds: nhcb_bounds_sets[1].clone(),
                sum: 0.0,
            },
            in_order_t: start_ts + 600,
            last_exemplar_t: start_ts,
            seen_timestamps: Vec::new(),
        });

        Self {
            series,
            churn_counter: 0,
        }
    }

    /// Generates one randomized `cortexpb::WriteRequest` batch mixing in-order floats, OOO floats,
    /// integer/float native histograms, NHCB histograms, counter resets, `StaleNaN`, exemplars,
    /// metadata mutations, and optional series churn.
    fn next_write_request(&mut self, rng: &mut Rng, step: usize, target_base_t: i64) -> cortexpb::WriteRequest {
        let mut timeseries = Vec::new();
        let mut metadata = Vec::new();

        let batch_count = (3 + rng.gen_usize(5)).min(self.series.len());
        let mut chosen_indices = Vec::with_capacity(batch_count);
        while chosen_indices.len() < batch_count {
            let cand = rng.gen_usize(self.series.len());
            if !chosen_indices.contains(&cand) {
                chosen_indices.push(cand);
            }
        }
        for s_idx in chosen_indices {
            let s = &mut self.series[s_idx];

            // Decide between advancing in-order vs OOO write (when history exists).
            let try_ooo = s.seen_timestamps.len() >= 2 && rng.gen_bool(1, 4);

            if try_ooo {
                // Pick a distinct OOO timestamp strictly below `s.in_order_t` within the 6h OOO window
                // (or occasionally exact `t == s.in_order_t` on float series to test duplicate-timestamp
                // error parity against `s.maxTime()`).
                let offset_back = rng.gen_range_i64(1_000, 40 * 60_000);
                let mut ooo_t = s.in_order_t - offset_back;
                if matches!(s.kind, SeriesKind::Float { .. })
                    && s.in_order_t >= target_base_t - 60_000
                    && rng.gen_bool(1, 8)
                {
                    ooo_t = s.in_order_t;
                } else {
                    while s.seen_timestamps.contains(&ooo_t) {
                        ooo_t += 17;
                    }
                    if ooo_t >= s.in_order_t {
                        ooo_t = s.in_order_t - 500;
                        while s.seen_timestamps.contains(&ooo_t) {
                            ooo_t -= 17;
                        }
                    }
                    s.seen_timestamps.push(ooo_t);
                }

                let ts_proto = match &s.kind {
                    SeriesKind::Float { .. } => {
                        let val = if rng.gen_bool(1, 10) {
                            stale_nan_f64()
                        } else {
                            (rng.gen_range_i64(-500, 5_000) as f64) * 0.25
                        };
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            samples: vec![cortexpb::Sample {
                                value: val,
                                timestamp_ms: ooo_t,
                            }],
                            ..Default::default()
                        }
                    }
                    SeriesKind::IntHist {
                        schema,
                        zero_threshold,
                        is_gauge,
                        pos_offset,
                        neg_offset,
                        ..
                    } => {
                        let hint = if *is_gauge {
                            ResetHint::Gauge
                        } else {
                            ResetHint::Unknown
                        };
                        let pc = vec![
                            rng.gen_range_i64(0, 5) as u64,
                            rng.gen_range_i64(1, 6) as u64,
                        ];
                        let nc = if *neg_offset == 0 && rng.gen_bool(1, 3) {
                            vec![1]
                        } else {
                            Vec::new()
                        };
                        let sum = (pc.iter().sum::<u64>() as f64) * 1.5;
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_int_histogram(
                                ooo_t,
                                *schema,
                                *zero_threshold,
                                hint,
                                1,
                                *pos_offset,
                                &pc,
                                *neg_offset,
                                &nc,
                                Vec::new(),
                                sum,
                            )],
                            ..Default::default()
                        }
                    }
                    SeriesKind::FloatHist {
                        schema,
                        zero_threshold,
                        is_gauge,
                        pos_offset,
                        neg_offset,
                        ..
                    } => {
                        let hint = if *is_gauge {
                            ResetHint::Gauge
                        } else {
                            ResetHint::Unknown
                        };
                        let pc = vec![
                            (rng.gen_range_i64(1, 8) as f64) * 0.5,
                            (rng.gen_range_i64(0, 6) as f64) * 0.5,
                        ];
                        let sum = pc.iter().sum::<f64>() * 2.0;
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_float_histogram(
                                ooo_t,
                                *schema,
                                *zero_threshold,
                                hint,
                                0.5,
                                *pos_offset,
                                &pc,
                                *neg_offset,
                                &[],
                                Vec::new(),
                                sum,
                            )],
                            ..Default::default()
                        }
                    }
                    SeriesKind::NhcbInt { bounds, .. } => {
                        let mut c = vec![0u64; bounds.len() + 1];
                        let b_idx = rng.gen_usize(c.len());
                        c[b_idx] = rng.gen_range_i64(1, 5) as u64;
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_int_histogram(
                                ooo_t,
                                NHCB_SCHEMA,
                                0.0,
                                ResetHint::Unknown,
                                0,
                                0,
                                &c,
                                0,
                                &[],
                                bounds.clone(),
                                (c[b_idx] as f64) * 0.5,
                            )],
                            ..Default::default()
                        }
                    }
                    SeriesKind::NhcbFloat { bounds, .. } => {
                        let mut c = vec![0.0f64; bounds.len() + 1];
                        let b_idx = rng.gen_usize(c.len());
                        c[b_idx] = (rng.gen_range_i64(1, 6) as f64) * 0.5;
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_float_histogram(
                                ooo_t,
                                NHCB_SCHEMA,
                                0.0,
                                ResetHint::Unknown,
                                0.0,
                                0,
                                &c,
                                0,
                                &[],
                                bounds.clone(),
                                c[b_idx] * 1.5,
                            )],
                            ..Default::default()
                        }
                    }
                };
                timeseries.push(ts_proto);
                continue;
            }

            // Advance in-order timestamp monotonically, jumping to `target_base_t` when phase advances
            // (e.g. crossing a 2h chunk rollover boundary).
            let step_delta = rng.gen_range_i64(1_000, 15_000);
            let next_t = (s.in_order_t + step_delta).max(target_base_t + rng.gen_range_i64(0, 2_000));
            s.in_order_t = next_t;
            s.seen_timestamps.push(next_t);

            // Optional exemplar (valid <= 128 UTF-8 runes, monotonically advancing timestamp).
            let mut exemplars = Vec::new();
            if rng.gen_bool(1, 3) && next_t > s.last_exemplar_t {
                s.last_exemplar_t = next_t;
                exemplars.push(cortexpb::Exemplar {
                    labels: vec![
                        lp("span_id", &format!("{:08x}", rng.next_u64() as u32)),
                        lp("trace_id", &format!("tr-{step:03}-{}", rng.gen_usize(1000))),
                    ],
                    value: (rng.gen_range_i64(1, 400) as f64) * 0.125,
                    timestamp_ms: next_t,
                });
            }

            // Generate in-order sample / histogram / StaleNaN / counter reset / bucket expansion!
            let emit_stale = rng.gen_bool(1, 12);
            let ts_proto = match &mut s.kind {
                SeriesKind::Float { last_val } => {
                    let v = if emit_stale {
                        stale_nan_f64()
                    } else {
                        match rng.gen_usize(7) {
                            0 => *last_val, // exact repeat (Gorilla 1-bit)
                            1 => 0.0,
                            2 => -0.0,
                            3 => -((rng.gen_range_i64(1, 10_000) as f64) * 0.5),
                            _ => {
                                *last_val += (rng.gen_range_i64(1, 200) as f64) * 0.25;
                                *last_val
                            }
                        }
                    };
                    cortexpb::TimeSeries {
                        labels: to_label_pairs(&s.labels),
                        samples: vec![cortexpb::Sample {
                            value: v,
                            timestamp_ms: next_t,
                        }],
                        exemplars,
                        ..Default::default()
                    }
                }
                SeriesKind::IntHist {
                    schema,
                    zero_threshold,
                    is_gauge,
                    z_cnt,
                    pos_counts,
                    neg_counts,
                    pos_offset,
                    neg_offset,
                    sum,
                } => {
                    if emit_stale {
                        // Exercise StaleNaN float sample on a native histogram series!
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            samples: vec![cortexpb::Sample {
                                value: stale_nan_f64(),
                                timestamp_ms: next_t,
                            }],
                            ..Default::default()
                        }
                    } else if *is_gauge {
                        for c in pos_counts.iter_mut() {
                            *c = rng.gen_range_i64(0, 12) as u64;
                        }
                        if rng.gen_bool(1, 4) && pos_counts.len() < 8 {
                            pos_counts.push(rng.gen_range_i64(1, 5) as u64);
                        }
                        *z_cnt = rng.gen_range_i64(0, 4) as u64;
                        *sum = (pos_counts.iter().sum::<u64>() as f64) * 0.75;
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_int_histogram(
                                next_t,
                                *schema,
                                *zero_threshold,
                                ResetHint::Gauge,
                                *z_cnt,
                                *pos_offset,
                                pos_counts,
                                *neg_offset,
                                neg_counts,
                                Vec::new(),
                                *sum,
                            )],
                            exemplars,
                            ..Default::default()
                        }
                    } else {
                        let do_reset = rng.gen_bool(1, 7);
                        let hint = if do_reset {
                            *z_cnt = rng.gen_range_i64(0, 2) as u64;
                            for c in pos_counts.iter_mut() {
                                *c = rng.gen_range_i64(0, 2) as u64;
                            }
                            for c in neg_counts.iter_mut() {
                                *c = rng.gen_range_i64(0, 2) as u64;
                            }
                            *sum = (pos_counts.iter().sum::<u64>() as f64) * 0.5;
                            if rng.gen_bool(1, 2) {
                                ResetHint::Yes
                            } else {
                                ResetHint::Unknown // implicit reset detected by count/bucket drop!
                            }
                        } else {
                            // Occasionally expand buckets backward or forward!
                            if rng.gen_bool(1, 5) && pos_counts.len() < 10 {
                                if rng.gen_bool(1, 2) {
                                    pos_counts.push(rng.gen_range_i64(1, 4) as u64);
                                } else {
                                    *pos_offset -= 1;
                                    pos_counts.insert(0, rng.gen_range_i64(1, 3) as u64);
                                }
                            }
                            *z_cnt += rng.gen_range_i64(0, 2) as u64;
                            for c in pos_counts.iter_mut() {
                                *c += rng.gen_range_i64(0, 3) as u64;
                            }
                            for c in neg_counts.iter_mut() {
                                *c += rng.gen_range_i64(0, 2) as u64;
                            }
                            *sum += (rng.gen_range_i64(1, 20) as f64) * 0.5;
                            if rng.gen_bool(1, 3) {
                                ResetHint::No
                            } else {
                                ResetHint::Unknown
                            }
                        };
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_int_histogram(
                                next_t,
                                *schema,
                                *zero_threshold,
                                hint,
                                *z_cnt,
                                *pos_offset,
                                pos_counts,
                                *neg_offset,
                                neg_counts,
                                Vec::new(),
                                *sum,
                            )],
                            exemplars,
                            ..Default::default()
                        }
                    }
                }
                SeriesKind::FloatHist {
                    schema,
                    zero_threshold,
                    is_gauge,
                    z_cnt,
                    pos_counts,
                    neg_counts,
                    pos_offset,
                    neg_offset,
                    sum,
                } => {
                    if emit_stale {
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            samples: vec![cortexpb::Sample {
                                value: stale_nan_f64(),
                                timestamp_ms: next_t,
                            }],
                            ..Default::default()
                        }
                    } else if *is_gauge {
                        for c in pos_counts.iter_mut() {
                            *c = (rng.gen_range_i64(0, 10) as f64) * 0.5;
                        }
                        *z_cnt = (rng.gen_range_i64(0, 4) as f64) * 0.5;
                        *sum = pos_counts.iter().sum::<f64>() * 1.25;
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_float_histogram(
                                next_t,
                                *schema,
                                *zero_threshold,
                                ResetHint::Gauge,
                                *z_cnt,
                                *pos_offset,
                                pos_counts,
                                *neg_offset,
                                neg_counts,
                                Vec::new(),
                                *sum,
                            )],
                            exemplars,
                            ..Default::default()
                        }
                    } else {
                        let do_reset = rng.gen_bool(1, 7);
                        let hint = if do_reset {
                            *z_cnt = 0.5;
                            for c in pos_counts.iter_mut() {
                                *c = 0.5;
                            }
                            for c in neg_counts.iter_mut() {
                                *c = 0.0;
                            }
                            *sum = pos_counts.iter().sum::<f64>();
                            if rng.gen_bool(1, 2) {
                                ResetHint::Yes
                            } else {
                                ResetHint::Unknown
                            }
                        } else {
                            if rng.gen_bool(1, 5) && pos_counts.len() < 10 {
                                pos_counts.push((rng.gen_range_i64(1, 4) as f64) * 0.5);
                            }
                            *z_cnt += 0.5;
                            for c in pos_counts.iter_mut() {
                                *c += (rng.gen_range_i64(0, 4) as f64) * 0.5;
                            }
                            for c in neg_counts.iter_mut() {
                                *c += (rng.gen_range_i64(0, 2) as f64) * 0.5;
                            }
                            *sum += (rng.gen_range_i64(1, 16) as f64) * 0.5;
                            ResetHint::Unknown
                        };
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_float_histogram(
                                next_t,
                                *schema,
                                *zero_threshold,
                                hint,
                                *z_cnt,
                                *pos_offset,
                                pos_counts,
                                *neg_offset,
                                neg_counts,
                                Vec::new(),
                                *sum,
                            )],
                            exemplars,
                            ..Default::default()
                        }
                    }
                }
                SeriesKind::NhcbInt { bounds, counts, sum } => {
                    if emit_stale {
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            samples: vec![cortexpb::Sample {
                                value: stale_nan_f64(),
                                timestamp_ms: next_t,
                            }],
                            ..Default::default()
                        }
                    } else {
                        let do_reset = rng.gen_bool(1, 6);
                        let hint = if do_reset {
                            counts.fill(0);
                            let idx = rng.gen_usize(counts.len());
                            counts[idx] = 1;
                            *sum = 0.5;
                            if rng.gen_bool(1, 2) {
                                ResetHint::Yes
                            } else {
                                ResetHint::Unknown
                            }
                        } else {
                            for _ in 0..2 {
                                let idx = rng.gen_usize(counts.len());
                                counts[idx] += rng.gen_range_i64(1, 4) as u64;
                            }
                            *sum += (rng.gen_range_i64(1, 12) as f64) * 0.25;
                            ResetHint::Unknown
                        };
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_int_histogram(
                                next_t,
                                NHCB_SCHEMA,
                                0.0,
                                hint,
                                0,
                                0,
                                counts,
                                0,
                                &[],
                                bounds.clone(),
                                *sum,
                            )],
                            exemplars,
                            ..Default::default()
                        }
                    }
                }
                SeriesKind::NhcbFloat { bounds, counts, sum } => {
                    if emit_stale {
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            samples: vec![cortexpb::Sample {
                                value: stale_nan_f64(),
                                timestamp_ms: next_t,
                            }],
                            ..Default::default()
                        }
                    } else {
                        let do_reset = rng.gen_bool(1, 6);
                        let hint = if do_reset {
                            counts.fill(0.0);
                            let idx = rng.gen_usize(counts.len());
                            counts[idx] = 0.5;
                            *sum = 0.5;
                            ResetHint::Yes
                        } else {
                            for _ in 0..2 {
                                let idx = rng.gen_usize(counts.len());
                                counts[idx] += (rng.gen_range_i64(1, 4) as f64) * 0.5;
                            }
                            *sum += (rng.gen_range_i64(1, 12) as f64) * 0.5;
                            ResetHint::Unknown
                        };
                        cortexpb::TimeSeries {
                            labels: to_label_pairs(&s.labels),
                            histograms: vec![build_float_histogram(
                                next_t,
                                NHCB_SCHEMA,
                                0.0,
                                hint,
                                0.0,
                                0,
                                counts,
                                0,
                                &[],
                                bounds.clone(),
                                *sum,
                            )],
                            exemplars,
                            ..Default::default()
                        }
                    }
                }
            };
            timeseries.push(ts_proto);
        }

        // Ephemeral series churn burst!
        if rng.gen_bool(1, 3) {
            let burst = 2 + rng.gen_usize(4);
            for _ in 0..burst {
                let cid = self.churn_counter;
                self.churn_counter += 1;
                let pod = format!("ephemeral-pod-{cid}");
                let ts = target_base_t + rng.gen_range_i64(0, 5_000);
                let labels = vec![
                    ("__name__".to_string(), "w9_churn_series_total".to_string()),
                    ("churn".to_string(), pod),
                    ("job".to_string(), "churner".to_string()),
                    ("kind".to_string(), "churn".to_string()),
                ];
                timeseries.push(cortexpb::TimeSeries {
                    labels: to_label_pairs(&labels),
                    samples: vec![cortexpb::Sample {
                        value: (cid as f64) * 1.5,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                });
            }
        }

        // Metadata updates (exercising initial creation + dynamic help/unit/type updates).
        if rng.gen_bool(1, 2) {
            let s = rng.choose(&self.series);
            let mtypes = [
                MetricType::Counter,
                MetricType::Gauge,
                MetricType::Histogram,
                MetricType::Gaugehistogram,
                MetricType::Summary,
                MetricType::Info,
            ];
            let units = ["seconds", "bytes", "requests", ""];
            metadata.push(cortexpb::MetricMetadata {
                r#type: (*rng.choose(&mtypes)) as i32,
                metric_family_name: s.metric_name.clone(),
                help: format!("stateful help rev {step} for {}", s.metric_name),
                unit: (*rng.choose(&units)).to_string(),
            });
        }

        cortexpb::WriteRequest {
            timeseries,
            metadata,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        }
    }
}

async fn push_and_assert_parity(
    cluster: &mut TwinCluster,
    ctx: &str,
    tenant: &str,
    req: cortexpb::WriteRequest,
) {
    let req_dbg = req.clone();
    let req_go = req.clone();
    let req_rust = req;
    let (go_res, rust_res) = tokio::join!(
        cluster.go.ing.push(with_org(tenant, req_go)),
        cluster.rust.ing.push(with_org(tenant, req_rust)),
    );
    let go_out = normalize_push_result(go_res);
    let rust_out = normalize_push_result(rust_res);
    assert_eq!(
        rust_out.outcome, go_out.outcome,
        "[{ctx}][{tenant}] Push outcome/ErrorDetails mismatch:\n  rust_msg={:?}\n  go_msg={:?}\n  req={req_dbg:#?}",
        rust_out.raw_msg, go_out.raw_msg
    );
}

// ---------------------------------------------------------------------------------------------
// Multi-Seed Stateful Differential Runner
// ---------------------------------------------------------------------------------------------

async fn run_stateful_seed(seed: u64, steps_per_phase: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let tag = format!("w9-seed-{seed:x}");
    let mut cluster = TwinCluster::boot(tmp.path(), &tag).await;

    let mut rng = Rng::new(seed);
    let now = (now_ms() / 1_000) * 1_000;
    // Anchor around the most recent 2h chunk boundary (`<= now`):
    // - Phase 1 starts 75 minutes BEFORE `boundary` and crosses `boundary` mid-phase so both
    //   2h Head chunk rollovers AND `/ingester/flush` ULID block compactions are exercised.
    // - If `now - boundary < 10m`, shift `boundary` back by 2h so post-boundary phases remain `<= now`.
    let raw_boundary = (now / TWO_HOURS_MS) * TWO_HOURS_MS;
    let boundary = if now - raw_boundary < 15 * 60_000 {
        raw_boundary - TWO_HOURS_MS
    } else {
        raw_boundary
    };

    let phase1_pre_boundary = boundary - 75 * 60_000;
    let phase1_post_boundary = boundary + 2 * 60_000;
    let phase2_post_flush = boundary + 6 * 60_000;
    let phase3_post_restart = boundary + 10 * 60_000;

    let min_query_t = boundary - 6 * 3_600_000;
    let max_query_t = now + 60 * 60_000;

    let mut models: BTreeMap<&'static str, TenantModel> = BTreeMap::new();
    for &t in &TENANTS {
        models.insert(t, TenantModel::new(&mut rng, phase1_pre_boundary));
    }

    // =========================================================================================
    // Phase 1: Pre-boundary + cross-2h-boundary mixed writes & interleaved 10-RPC reads
    // =========================================================================================
    for step in 0..steps_per_phase {
        let base_t = if step < steps_per_phase / 2 {
            phase1_pre_boundary + (step as i64) * 30_000
        } else {
            phase1_post_boundary + ((step - steps_per_phase / 2) as i64) * 5_000
        };

        for &tenant in &TENANTS {
            let req = models
                .get_mut(tenant)
                .unwrap()
                .next_write_request(&mut rng, step, base_t);
            push_and_assert_parity(
                &mut cluster,
                &format!("seed={seed:#x} phase1 step={step}"),
                tenant,
                req,
            )
            .await;
        }

        // Also inject an out-of-window OOO rejection check periodically to verify error parity!
        if step % 9 == 4 {
            let tenant = *rng.choose(&TENANTS);
            let too_old = phase1_pre_boundary - 9 * 3_600_000; // > 6h OOO window
            let bad_req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        lp("__name__", "w9_float_metric_0"),
                        lp("env", "prod"),
                        lp("instance", "inst-0"),
                        lp("job", "api"),
                        lp("kind", "float"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 42.0,
                        timestamp_ms: too_old,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            push_and_assert_parity(
                &mut cluster,
                &format!("seed={seed:#x} phase1 ooo-reject step={step}"),
                tenant,
                bad_req,
            )
            .await;
        }

        // Interleaved full 10-RPC differential verification!
        if step % 6 == 5 || step + 1 == steps_per_phase {
            let narrow_s = boundary - 4 * 3_600_000;
            let narrow_e = boundary + 5 * 60_000;
            for &tenant in &TENANTS {
                assert_all_10_read_rpcs_identical(
                    &mut cluster,
                    &format!("seed={seed:#x} phase1 step={step}"),
                    tenant,
                    min_query_t,
                    narrow_s,
                    narrow_e,
                    max_query_t,
                    "api",
                    "w9_float_metric_0",
                )
                .await;
            }
        }
    }

    // =========================================================================================
    // Mid-Stream Transition 1: `/ingester/flush?wait=true` (Head + OOO Head -> ULID blocks)
    // =========================================================================================
    cluster
        .flush_both(&format!("seed={seed:#x} mid-stream flush #1"))
        .await;

    for &tenant in &TENANTS {
        assert_all_10_read_rpcs_identical(
            &mut cluster,
            &format!("seed={seed:#x} immediately-after-flush-1"),
            tenant,
            min_query_t,
            boundary - 4 * 3_600_000,
            boundary + 5 * 60_000,
            max_query_t,
            "api",
            "w9_nhcb_int_duration_seconds",
        )
        .await;
    }

    // =========================================================================================
    // Phase 2: Post-flush mixed writes (spanning ULID blocks + fresh Head + fresh OOO Head!)
    // =========================================================================================
    for step in 0..steps_per_phase {
        let base_t = phase2_post_flush + (step as i64) * 5_000;
        let global_step = steps_per_phase + step;

        for &tenant in &TENANTS {
            let req = models
                .get_mut(tenant)
                .unwrap()
                .next_write_request(&mut rng, global_step, base_t);
            push_and_assert_parity(
                &mut cluster,
                &format!("seed={seed:#x} phase2 step={step}"),
                tenant,
                req,
            )
            .await;
        }

                if step % 6 == 5 || step + 1 == steps_per_phase {
            let narrow_s = boundary - 4 * 3_600_000;
            let narrow_e = phase2_post_flush + (step as i64) * 5_000;
            for &tenant in &TENANTS {
                assert_all_10_read_rpcs_identical(
                    &mut cluster,
                    &format!("seed={seed:#x} phase2 step={step}"),
                    tenant,
                    min_query_t,
                    narrow_s,
                    narrow_e,
                    max_query_t,
                    "ingester",
                    "w9_inthist_metric_0",
                )
                .await;
            }
        }
    }

    // =========================================================================================
    // Mid-Stream Transition 2: SIGTERM Restart (WAL + WBL + `chunks_head` + ULID Block Replay)
    // =========================================================================================
    cluster
        .restart_both(&format!("w9-seed-{seed:x}-restarted"))
        .await;

    for &tenant in &TENANTS {
        assert_all_10_read_rpcs_identical(
            &mut cluster,
            &format!("seed={seed:#x} immediately-after-wal-replay-restart"),
            tenant,
            min_query_t,
            boundary - 4 * 3_600_000,
            phase3_post_restart,
            max_query_t,
            "api",
            "w9_floathist_metric_0",
        )
        .await;
    }

    // =========================================================================================
    // Phase 3: Post-restart writes + final `/ingester/flush?wait=true` + full 10-RPC parity
    // =========================================================================================
    let phase3_steps = (steps_per_phase / 2).max(6);
    for step in 0..phase3_steps {
        let base_t = phase3_post_restart + (step as i64) * 4_000;
        let global_step = steps_per_phase * 2 + step;

        for &tenant in &TENANTS {
            let req = models
                .get_mut(tenant)
                .unwrap()
                .next_write_request(&mut rng, global_step, base_t);
            push_and_assert_parity(
                &mut cluster,
                &format!("seed={seed:#x} phase3 step={step}"),
                tenant,
                req,
            )
            .await;
        }
    }

    for &tenant in &TENANTS {
        assert_all_10_read_rpcs_identical(
            &mut cluster,
            &format!("seed={seed:#x} phase3-pre-final-flush"),
            tenant,
            min_query_t,
            boundary - 4 * 3_600_000,
            phase3_post_restart + 60_000,
            max_query_t,
            "api",
            "w9_nhcb_float_duration_seconds",
        )
        .await;
    }

    cluster
        .flush_both(&format!("seed={seed:#x} final flush #2"))
        .await;

    for &tenant in &TENANTS {
        assert_all_10_read_rpcs_identical(
            &mut cluster,
            &format!("seed={seed:#x} phase3-after-final-flush"),
            tenant,
            min_query_t,
            boundary - 4 * 3_600_000,
            phase3_post_restart + 60_000,
            max_query_t,
            "api",
            "w9_float_metric_1",
        )
        .await;
    }

    cluster.stop();
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[test]
fn stateful_tsdb_differential_proptest_multi_seed() {
    let default_seeds: &[u64] = &[
        0x5eed_cafe_0001_2026,
        0x9e37_79b9_dead_beef,
        0x1234_5678_9abc_def0,
    ];
    let num_seeds: usize = std::env::var("W9_PROPTEST_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_seeds.len());
    let steps_per_phase: usize = std::env::var("W9_PROPTEST_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18);

    block_on_for_tests(async {
        for idx in 0..num_seeds {
            let seed = default_seeds
                .get(idx)
                .copied()
                .unwrap_or_else(|| 0xa076_1d64_78bd_642f_u64.wrapping_mul((idx + 1) as u64));
            eprintln!(
                "=== [w9_stateful_tsdb_proptest] running seed {}/{num_seeds}: {seed:#018x} (steps_per_phase={steps_per_phase}) ===",
                idx + 1
            );
            run_stateful_seed(seed, steps_per_phase).await;
        }
    });
}

#[test]
fn repro_bug1_wide_sparse_nhcb_int_histogram_recode_live_differential() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut cluster = TwinCluster::boot(tmp.path(), "w9-repro-bug1").await;
        let now = (now_ms() / 1_000) * 1_000;
        let t_in_order = now - 60_000;

        // 250 custom upper bounds => 251 buckets (`0..=250`).
        // Sample 1 has span(0, 250) with 1 observation in bucket 0 and 249 zero deltas (1 bit each).
        // Sample 2 adds 1 observation in bucket 250 (`span(0, 251)` -> forward bucket insert),
        // forcing server-side `HistogramAppender::recode_in_place` -> `HistogramIterator::next_sample`:
        // Go succeeds (`Code::Ok`); Rust returns `Code::Internal` ("chunk error: invalid chunk data")!
        let bounds: Vec<f64> = (1..=250).map(|i| i as f64).collect();
        let mut counts1 = vec![0u64; 250];
        counts1[0] = 1;

        let req1 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    lp("__name__", "w9_wide_nhcb_hist"),
                    lp("job", "api"),
                ],
                histograms: vec![build_int_histogram(
                    t_in_order - 10_000,
                    NHCB_SCHEMA,
                    0.0,
                    ResetHint::Unknown,
                    0,
                    0,
                    &counts1,
                    0,
                    &[],
                    bounds.clone(),
                    0.5,
                )],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        push_and_assert_parity(&mut cluster, "wide-nhcb-push1", "t-std", req1).await;

        let mut counts2 = vec![0u64; 251];
        counts2[0] = 1;
        counts2[250] = 1;
        let req2 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    lp("__name__", "w9_wide_nhcb_hist"),
                    lp("job", "api"),
                ],
                histograms: vec![build_int_histogram(
                    t_in_order,
                    NHCB_SCHEMA,
                    0.0,
                    ResetHint::Unknown,
                    0,
                    0,
                    &counts2,
                    0,
                    &[],
                    bounds,
                    251.5,
                )],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        push_and_assert_parity(&mut cluster, "wide-nhcb-push2-recode-in-place", "t-std", req2).await;

        cluster.stop();
    });
}

#[test]
fn repro_bug2_ooo_compaction_xor2_overlap_encoding_divergence() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut cluster = TwinCluster::boot(tmp.path(), "w9-repro-bug2").await;
        let now = (now_ms() / 1_000) * 1_000;
        let t_anchor = now - 60_000;

        // Establish an in-order anchor sample on `t-st` (`float_chunk_encoding: xor2`,
        // `out_of_order_capacity_max: 6`).
        let anchor_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "w9_bug2_xor2_ooo"), lp("job", "api")],
                samples: vec![cortexpb::Sample {
                    value: 100.0,
                    timestamp_ms: t_anchor,
                }],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        push_and_assert_parity(&mut cluster, "bug2-anchor", "t-st", anchor_req).await;

        // Push 32 OOO samples filling `ooo_head_chunk` to default `out_of_order_capacity_max = 32`,
        // then push a 33rd OOO sample at `t_anchor - 155_000` which cuts the 32-sample
        // `oooMmappedChunk` (`EncXOR2`) and starts a new `oooHeadChunk` (`EncXOR2`) overlapping it!
        let mut ooo_timestamps: Vec<i64> = (1..=32)
            .rev()
            .map(|k| t_anchor - k * 10_000)
            .collect();
        ooo_timestamps.push(t_anchor - 155_000);
        for (idx, ts) in ooo_timestamps.into_iter().enumerate() {
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "w9_bug2_xor2_ooo"), lp("job", "api")],
                    samples: vec![cortexpb::Sample {
                        value: (idx + 1) as f64 * 1.25,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            push_and_assert_parity(&mut cluster, &format!("bug2-ooo-{idx}"), "t-st", req).await;
        }

        // Flush (`compact_ooo_head`): Go's `OOOCompactionHead` returns `&mergedOOOChunks`
        // (`Iterable`) and `populateChunksFromIterable` encodes `st == 0` floats as `EncXOR (4)`;
        // Rust's `trim_and_merge_chunks` promotes `float_enc = Encoding::Xor2 (7)`.
        cluster.flush_both("bug2-flush").await;

        let m = vec![matcher(
            ingesterpb::MatchType::Equal,
            "__name__",
            "w9_bug2_xor2_ooo",
        )];
        let (go_qs, rust_qs) = tokio::join!(
            query_stream_canonical(
                &mut cluster.go.ing,
                "go",
                "t-st",
                now - 300_000,
                now + 60_000,
                m.clone(),
                4,
                true,
            ),
            query_stream_canonical(
                &mut cluster.rust.ing,
                "rust",
                "t-st",
                now - 300_000,
                now + 60_000,
                m,
                4,
                true,
            ),
        );
        cluster.stop();
        assert_eq!(
            rust_qs, go_qs,
            "OOO compaction chunk encoding mismatch on overlapping Xor2 OOO chunks (st==0)"
        );
    });
}

#[test]
fn repro_bug3_head_in_order_ooo_duplicate_timestamp_merge_divergence() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut cluster = TwinCluster::boot(tmp.path(), "w9-repro-bug3").await;
        let now = (now_ms() / 1_000) * 1_000;
        let t1 = now - 60_000;
        let t2 = now - 30_000;
        let t0 = now - 90_000;

        // 1. Push in-order sample `(t1, 129.25)` and `(t2, 500.0)` so `in_order` chunk is `[t1, t2]`
        //    with `s.maxTime() == t2`.
        for (ts, val) in [(t1, 129.25), (t2, 500.0)] {
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "w9_bug3_tiebreak"), lp("job", "api")],
                    samples: vec![cortexpb::Sample {
                        value: val,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            push_and_assert_parity(&mut cluster, "bug3-in-order", "t-std", req).await;
        }

        // 2. Push OOO sample `(t0, 1.0)` followed by `(t1, 297.5)`: because `t1 < s.maxTime() (t2)`,
        //    both Go and Rust accept `(t1, 297.5)` into `oooHeadChunk` (`[t0, t1]`).
        for (ts, val) in [(t0, 1.0), (t1, 297.5)] {
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "w9_bug3_tiebreak"), lp("job", "api")],
                    samples: vec![cortexpb::Sample {
                        value: val,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            push_and_assert_parity(&mut cluster, "bug3-ooo", "t-std", req).await;
        }

        // 3. Query Head: Go `chainSampleIterator` yields `(t1, 129.25)` (in-order);
        //    Rust `merge_head_chunks_with_tombstones` yields `(t1, 297.5)` (OOO)!
        let m = vec![matcher(
            ingesterpb::MatchType::Equal,
            "__name__",
            "w9_bug3_tiebreak",
        )];
        let (go_qs, rust_qs) = tokio::join!(
            query_stream_canonical(
                &mut cluster.go.ing,
                "go",
                "t-std",
                now - 300_000,
                now + 60_000,
                m.clone(),
                4,
                true,
            ),
            query_stream_canonical(
                &mut cluster.rust.ing,
                "rust",
                "t-std",
                now - 300_000,
                now + 60_000,
                m,
                4,
                true,
            ),
        );
        cluster.stop();
        assert_eq!(
            rust_qs, go_qs,
            "Head overlapping in-order + OOO duplicate-timestamp tie-break divergence"
        );
    });
}

#[test]
fn repro_bug4_compaction_partial_boundary_coalesces_disjoint_histogram_chunks() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut cluster = TwinCluster::boot(tmp.path(), "w9-repro-bug4").await;
        let now = (now_ms() / 1_000) * 1_000;
        let raw_boundary = (now / TWO_HOURS_MS) * TWO_HOURS_MS;
        let boundary = if now - raw_boundary < 5 * 60_000 {
            raw_boundary - TWO_HOURS_MS
        } else {
            raw_boundary
        };

        // 1. In-order anchor well past `boundary` (`boundary + 4m`).
        let t_anchor = boundary + 4 * 60_000;
        let anchor_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "w9_bug4_fhist_trim"), lp("job", "api")],
                histograms: vec![build_float_histogram(
                    t_anchor,
                    0,
                    0.5,
                    ResetHint::Unknown,
                    0.5,
                    0,
                    &[5.0, 5.0],
                    0,
                    &[],
                    Vec::new(),
                    20.0,
                )],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        push_and_assert_parity(&mut cluster, "bug4-anchor", "t-std", anchor_req).await;

        // 2. Push 32 OOO float histograms before `boundary` (`boundary - 70m` .. `boundary - 39m`)
        //    so `ooo_cap_max = 32` cuts Chunk 1 (`[boundary - 70m .. boundary - 39m]`) on the 33rd push!
        for k in 0..32i64 {
            let ts = boundary - (70 - k) * 60_000;
            let c0 = 1.0 + (k as f64) * 0.5;
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "w9_bug4_fhist_trim"), lp("job", "api")],
                    histograms: vec![build_float_histogram(
                        ts,
                        0,
                        0.5,
                        ResetHint::Unknown,
                        0.5,
                        0,
                        &[c0, 1.0],
                        0,
                        &[],
                        Vec::new(),
                        c0 * 2.0 + 2.0,
                    )],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            push_and_assert_parity(&mut cluster, "bug4-ooo-chunk1", "t-std", req).await;
        }

        // 3. Push 2 DISJOINT OOO float histograms straddling `boundary`:
        //    - `boundary - 10m` (`[20.0, 2.0]`, strictly after Chunk 1 and >= Chunk 1's last counts!)
        //    - `boundary + 1m` (`[21.0, 3.0]`, crosses `boundary` -> `c.max_time > maxt_inclusive`!)
        //    When `compact_ooo_head` compacts `[boundary - 2h, boundary)`:
        //    - Go trims ONLY Chunk 2 (`[boundary - 10m]`, keeping `UnknownCounterReset` as sample 0 of Chunk 2).
        //    - Rust sees `any_partial == true` and merges Chunk 1 + Chunk 2, turning `boundary - 10m`
        //      into sample 32 of Chunk 1 (`NotCounterReset`)!
        for (ts, c0, c1) in [
            (boundary - 10 * 60_000, 20.0, 2.0),
            (boundary + 60_000, 21.0, 3.0),
        ] {
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "w9_bug4_fhist_trim"), lp("job", "api")],
                    histograms: vec![build_float_histogram(
                        ts,
                        0,
                        0.5,
                        ResetHint::Unknown,
                        0.5,
                        0,
                        &[c0, c1],
                        0,
                        &[],
                        Vec::new(),
                        (c0 + c1) * 2.0,
                    )],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            push_and_assert_parity(&mut cluster, "bug4-ooo-chunk2", "t-std", req).await;
        }

        cluster.flush_both("bug4-flush").await;

        let m = vec![matcher(
            ingesterpb::MatchType::Equal,
            "__name__",
            "w9_bug4_fhist_trim",
        )];
        let (go_qs, rust_qs) = tokio::join!(
            query_stream_canonical(
                &mut cluster.go.ing,
                "go",
                "t-std",
                boundary - 3 * 3_600_000,
                now + 60_000,
                m.clone(),
                8,
                true,
            ),
            query_stream_canonical(
                &mut cluster.rust.ing,
                "rust",
                "t-std",
                boundary - 3 * 3_600_000,
                now + 60_000,
                m,
                8,
                true,
            ),
        );
        cluster.stop();
        assert_eq!(
            rust_qs, go_qs,
            "Disjoint histogram chunks coalesced during boundary trim compaction"
        );
    });
}

