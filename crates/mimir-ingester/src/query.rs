//! Read RPC implementations, read admission, and ingest_storage read consistency.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_query.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_search.go
//! - github.com/grafana/mimir/pkg/ingester/label_names_and_values.go
//! - github.com/grafana/mimir/pkg/ingester/active_series.go
//! - github.com/grafana/mimir/pkg/ingester/stats.go
//! - github.com/grafana/mimir/pkg/querier/api/consistency.go

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mimir_common::limiter::ReactivePermit;
use mimir_config::parse_go_duration;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_storage::ingest::PartitionOffsets;
use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::db::Tsdb;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::QueryChunkSeries;
use mimir_tsdb::search::{FuzzAlg, SearchHints, SearchOrdering, SearchResult};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::circuit_breaker::BreakerPermit;
use crate::errors::IngesterError;
use crate::ingester::{Ingester, INGEST_STORAGE_DEFAULT_READ_CONSISTENCY_TIMEOUT};
use crate::lookupplan::{self, BasicLookupPlan, LookupPlanner, NoopPlanner, SelectHints};
use crate::metrics::REASON_MAX_INFLIGHT_READ_REQUESTS;
use crate::mirror_querier::{
    ChunkBlockMeta, ChunkQuerier, ChunkSeries, ChunkSeriesSet, MirroredChunkQuerier, QuerierError,
};
use crate::user_tsdb::UserTsdb;

pub const QUERY_STREAM_BATCH_SIZE: usize = 128;
pub const FALLBACK_CHUNK_STREAM_BATCH_SIZE: u64 = 256;
pub const QUERY_STREAM_BATCH_MESSAGE_SIZE: usize = 1024 * 1024;
pub type QueryStreamBox = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<ingesterpb::QueryStreamResponse, Status>> + Send + 'static>,
>;
pub const LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES: usize = 1024 * 1024;
pub const LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES: usize = 1024 * 1024;
pub const ACTIVE_SERIES_MAX_SIZE_BYTES: usize = 1024 * 1024;
pub const SEARCH_BATCH_SIZE: usize = 256;

const ONLY_REPLICA_GRPC_MD_KEY: &str = "__only_replica__";
const X_ONLY_REPLICA_MD_KEY: &str = "x-only-replica";
const CONSISTENCY_LEVEL_MD_KEY: &str = "__consistency_level__";
const CONSISTENCY_OFFSETS_MD_KEY: &str = "__consistency_offsets__";
const CONSISTENCY_MAX_DELAY_MD_KEY: &str = "__consistency_max_delay__";

pub const READ_CONSISTENCY_STRONG: &str = "strong";
pub const READ_CONSISTENCY_EVENTUAL: &str = "eventual";

pub struct ReadPermit {
    started_at: Instant,
    breaker_permit: Option<BreakerPermit>,
    reactive_permit: Option<ReactivePermit>,
}

impl ReadPermit {
    pub fn finish(mut self, deadline_exceeded: bool) {
        let dur = self.started_at.elapsed();
        if let Some(bp) = self.breaker_permit.take() {
            bp.finish(dur, deadline_exceeded);
        }
        if let Some(rp) = self.reactive_permit.take() {
            rp.record();
        }
    }
}

pub fn is_only_replica_metadata(md: &MetadataMap) -> bool {
    let check = |k: &str| {
        md.get(k)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("true"))
    };
    check(ONLY_REPLICA_GRPC_MD_KEY) || check(X_ONLY_REPLICA_MD_KEY)
}

/// Parses `v1=<p>:<off>,...` or `v2=<rc>/<p>:<off>;<off>,...` consistency offsets header.
pub fn lookup_encoded_consistency_offsets(
    encoded: &str,
    read_compartment: i64,
    partition_id: i32,
) -> Option<PartitionOffsets> {
    if let Some(rest) = encoded.strip_prefix("v1=") {
        if read_compartment != 0 {
            return None;
        }
        let target_key = format!("{partition_id}:");
        for entry in rest.split(',') {
            if let Some(val_str) = entry.strip_prefix(&target_key) {
                if let Ok(off) = val_str.parse::<i64>() {
                    return Some(PartitionOffsets::new_single_cluster(off));
                }
            }
        }
        return None;
    }
    if let Some(rest) = encoded.strip_prefix("v2=") {
        let target_key = format!("{read_compartment}/{partition_id}:");
        for entry in rest.split(',') {
            if let Some(vals_str) = entry.strip_prefix(&target_key) {
                let mut cluster_offsets = Vec::new();
                for part in vals_str.split(';') {
                    let off = part.parse::<i64>().ok()?;
                    cluster_offsets.push(off);
                }
                return Some(PartitionOffsets::new_multi_cluster(cluster_offsets));
            }
        }
        return None;
    }
    None
}

pub type ShardAndMatchers = (Option<(u64, u64)>, Vec<LabelMatcher>);

/// Parses `__query_shard__` matcher from a slice of matchers, returning `(Option<(shard_idx, shard_count)>, filtered)`.
pub fn remove_shard_from_matchers(
    matchers: Vec<LabelMatcher>,
) -> Result<ShardAndMatchers, IngesterError> {
    for (idx, m) in matchers.iter().enumerate() {
        if m.name == "__query_shard__" && m.match_type == MatchType::Equal {
            let parts: Vec<&str> = m.value.split('_').collect();
            if parts.len() != 3 || parts[1] != "of" {
                return Err(IngesterError::BadRequest {
                    message: format!("invalid shard ID: {:?}", m.value),
                });
            }
            let one_based = parts[0].parse::<u64>().map_err(|_| IngesterError::BadRequest {
                message: format!("invalid shard ID: {:?}", m.value),
            })?;
            let count = parts[2].parse::<u64>().map_err(|_| IngesterError::BadRequest {
                message: format!("invalid shard ID: {:?}", m.value),
            })?;
            if one_based == 0 || count == 0 || one_based > count {
                return Err(IngesterError::BadRequest {
                    message: format!("invalid shard ID: {:?}", m.value),
                });
            }
            let mut filtered = Vec::with_capacity(matchers.len() - 1);
            for (i, item) in matchers.into_iter().enumerate() {
                if i != idx {
                    filtered.push(item);
                }
            }
            return Ok((Some((one_based - 1, count)), filtered));
        }
    }
    Ok((None, matchers))
}

pub fn from_proto_matchers(
    raw: &[ingesterpb::LabelMatcher],
) -> Result<Vec<LabelMatcher>, IngesterError> {
    let mut out = Vec::with_capacity(raw.len());
    for m in raw {
        let mt = match ingesterpb::MatchType::try_from(m.r#type) {
            Ok(ingesterpb::MatchType::Equal) => MatchType::Equal,
            Ok(ingesterpb::MatchType::NotEqual) => MatchType::NotEqual,
            Ok(ingesterpb::MatchType::RegexMatch) => MatchType::RegexMatch,
            Ok(ingesterpb::MatchType::RegexNoMatch) => MatchType::RegexNotMatch,
            Err(_) => {
                return Err(IngesterError::BadRequest {
                    message: format!("unknown match type {}", m.r#type),
                });
            }
        };
        let lm = LabelMatcher::new(mt, &m.name, &m.value).map_err(|e| {
            IngesterError::BadRequest {
                message: format!("invalid matcher: {e}"),
            }
        })?;
        out.push(lm);
    }
    Ok(out)
}

pub fn labels_to_proto_pairs(labels: &Labels) -> Vec<cortexpb::LabelPair> {
    let mut out = Vec::with_capacity(labels.len());
    for (name, value) in labels.iter_bytes_slices() {
        out.push(cortexpb::LabelPair { name, value });
    }
    out
}

/// Maps internal Prometheus TSDB chunk encoding to Mimir wire `chunk.Encoding` (`pkg/ingester/client/custom.go`).
pub fn tsdb_encoding_to_wire(enc: Encoding) -> i32 {
    match enc {
        Encoding::Xor => 4,             // PrometheusXorChunk
        Encoding::Histogram => 5,       // PrometheusHistogramChunk
        Encoding::FloatHistogram => 6,  // PrometheusFloatHistogramChunk
        Encoding::Xor2 => 7,            // PrometheusXor2Chunk
        Encoding::HistogramSt => 8,     // PrometheusHistogramSTChunk
        Encoding::FloatHistogramSt => 9, // PrometheusFloatHistogramSTChunk
        Encoding::None => 0,
    }
}

pub fn proto_to_params(
    wf: Option<&ingesterpb::SearchFilter>,
) -> Result<Option<SearchHints>, IngesterError> {
    let Some(wf) = wf else {
        return Ok(None);
    };
    let fuzz_alg = match ingesterpb::search_filter::FuzzAlg::try_from(wf.fuzz_alg) {
        Ok(ingesterpb::search_filter::FuzzAlg::Subsequence) => FuzzAlg::Subsequence,
        Ok(ingesterpb::search_filter::FuzzAlg::JaroWinkler) => FuzzAlg::JaroWinkler,
        Ok(ingesterpb::search_filter::FuzzAlg::SubstringLeft) => FuzzAlg::SubstringLeft,
        Ok(ingesterpb::search_filter::FuzzAlg::Substring) => FuzzAlg::Substring,
        Err(_) => {
            return Err(IngesterError::BadRequest {
                message: format!("unknown fuzz algorithm {}", wf.fuzz_alg),
            });
        }
    };
    let hints = SearchHints {
        terms: wf.terms.clone(),
        case_sensitive: !wf.case_insensitive,
        fuzz_alg,
        fuzz_threshold: wf.fuzz_threshold,
        ..Default::default()
    };
    hints.validate().map_err(|e| IngesterError::BadRequest {
        message: e.to_string(),
    })?;
    Ok(Some(hints))
}

pub fn build_search_hints(
    filter: Option<&ingesterpb::SearchFilter>,
    ordering: i32,
    limit: i64,
    wire_matchers: &[ingesterpb::LabelMatcher],
) -> Result<(SearchHints, Vec<LabelMatcher>), IngesterError> {
    let matchers = from_proto_matchers(wire_matchers)?;
    let parsed_filter = proto_to_params(filter)?;
    if limit < 0 {
        return Err(IngesterError::BadRequest {
            message: format!("limit must be >= 0, got {limit}"),
        });
    }
    let order_by = match ingesterpb::SearchOrdering::try_from(ordering) {
        Ok(ingesterpb::SearchOrdering::OrderByValueDesc) => SearchOrdering::ValueDesc,
        Ok(ingesterpb::SearchOrdering::OrderByScoreDesc) => SearchOrdering::ScoreDesc,
        _ => SearchOrdering::ValueAsc,
    };

    let mut hints = parsed_filter.unwrap_or_default();
    hints.order_by = order_by;
    hints.limit = (limit.min(isize::MAX as i64)) as usize;

    Ok((hints, matchers))
}

/// Iterator trait matching Prometheus `storage.SearchResultSet` (`pkg/ingester/ingester_search.go`).
pub trait SearchResultSet {
    fn next(&mut self) -> bool;
    fn at(&self) -> SearchResult;
    fn warnings(&self) -> &[String];
    fn err(&self) -> Option<String>;
}

pub struct VecSearchResultSet {
    results: Vec<SearchResult>,
    idx: usize,
    warnings: Vec<String>,
    err: Option<String>,
}

impl VecSearchResultSet {
    pub fn new(results: Vec<SearchResult>, warnings: Vec<String>, err: Option<String>) -> Self {
        Self {
            results,
            idx: 0,
            warnings,
            err,
        }
    }
}

impl SearchResultSet for VecSearchResultSet {
    fn next(&mut self) -> bool {
        if self.idx >= self.results.len() {
            return false;
        }
        self.idx += 1;
        true
    }

    fn at(&self) -> SearchResult {
        self.results[self.idx - 1].clone()
    }

    fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn err(&self) -> Option<String> {
        self.err.clone()
    }
}

/// Streams results from `rs` in batches of `SEARCH_BATCH_SIZE` (`pkg/ingester/ingester_search.go:streamSearchResults`).
pub fn stream_search_results(
    is_canceled: &dyn Fn() -> bool,
    rs: &mut dyn SearchResultSet,
    send: &mut dyn FnMut(ingesterpb::SearchResultBatch) -> Result<(), String>,
) -> Result<(), String> {
    if is_canceled() {
        return Err("context canceled".to_string());
    }
    let mut batch = ingesterpb::SearchResultBatch {
        results: Vec::with_capacity(SEARCH_BATCH_SIZE),
        warnings: Vec::new(),
    };
    while rs.next() {
        if is_canceled() {
            return Err("context canceled".to_string());
        }
        let v = rs.at();
        batch.results.push(ingesterpb::search_result_batch::Result {
            value: v.value,
            score: v.score,
        });
        if batch.results.len() >= SEARCH_BATCH_SIZE {
            let full = std::mem::replace(
                &mut batch,
                ingesterpb::SearchResultBatch {
                    results: Vec::with_capacity(SEARCH_BATCH_SIZE),
                    warnings: Vec::new(),
                },
            );
            send(full)?;
        }
    }
    if let Some(err) = rs.err() {
        return Err(err);
    }
    batch.warnings = rs.warnings().to_vec();
    if !batch.results.is_empty() || !batch.warnings.is_empty() {
        send(batch)?;
    }
    Ok(())
}

/// Async sink for streaming `QueryStreamResponse` batches incrementally (releasing label memory per batch).
pub trait QueryStreamSink: Send + Sync {
    fn send<'a>(
        &'a self,
        resp: ingesterpb::QueryStreamResponse,
    ) -> mimir_ring::kv::BoxFuture<'a, Result<(), IngesterError>>;
}

impl Ingester {
    pub fn check_read_overloaded(&self) -> Result<(), IngesterError> {
        let limiter_opt = self.utilization_based_limiter.read().clone();
        if let Some(limiter) = limiter_opt {
            if let Some(reason) = limiter.limiting_reason() {
                if !reason.is_empty() {
                    self.metrics
                        .utilization_limited_requests
                        .with_label_values(&[&reason])
                        .inc();
                    return Err(IngesterError::TooBusy);
                }
            }
        }
        Ok(())
    }

    pub async fn start_read_request(&self, md: &MetadataMap) -> Result<ReadPermit, IngesterError> {
        self.check_available_for_read()?;
        self.check_read_overloaded()?;
        let started_at = Instant::now();
        let only_replica = is_only_replica_metadata(md);

        let (breaker_permit, reactive_permit) = if !only_replica {
            if let Some(ref rl) = self.reactive_limiter.read {
                if !rl.can_acquire_permit() {
                    self.metrics
                        .rejected
                        .with_label_values(&[REASON_MAX_INFLIGHT_READ_REQUESTS])
                        .inc();
                    return Err(IngesterError::ReactiveLimiterExceeded {
                        message: "read reactive limiter exceeded".to_string(),
                    });
                }
            }
            let bp = self.circuit_breaker.try_acquire_read_permit()?;
            let rp = if let Some(ref rl) = self.reactive_limiter.read {
                match rl.acquire_permit().await {
                    Ok(p) => Some(p),
                    Err(e) => {
                        self.metrics
                            .rejected
                            .with_label_values(&[REASON_MAX_INFLIGHT_READ_REQUESTS])
                            .inc();
                        return Err(IngesterError::ReactiveLimiterExceeded {
                            message: e.to_string(),
                        });
                    }
                }
            } else {
                None
            };
            (bp, rp)
        } else {
            (None, None)
        };

        Ok(ReadPermit {
            started_at,
            breaker_permit,
            reactive_permit,
        })
    }

    pub async fn enforce_read_consistency(
        &self,
        md: &MetadataMap,
        user_id: &str,
    ) -> Result<(), IngesterError> {
        let reader_opt = self.ingest_reader.read().clone();
        let Some(reader) = reader_opt else {
            return Ok(());
        };

        let level = md
            .get(CONSISTENCY_LEVEL_MD_KEY)
            .and_then(|v| v.to_str().ok())
            .filter(|s| *s == READ_CONSISTENCY_STRONG || *s == READ_CONSISTENCY_EVENTUAL)
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                self.overrides
                    .for_tenant(user_id)
                    .ingest_storage_read_consistency
                    .clone()
            });

        if level == READ_CONSISTENCY_EVENTUAL {
            if let Some(delay_str) = md
                .get(CONSISTENCY_MAX_DELAY_MD_KEY)
                .and_then(|v| v.to_str().ok())
            {
                if let Ok(d_ns) = parse_go_duration(delay_str) {
                    if d_ns > 0 {
                        let std_d = Duration::from_nanos(d_ns as u64);
                        reader.enforce_read_max_delay(std_d).map_err(|e| {
                            IngesterError::Internal {
                                message: format!("enforce read consistency max delay: {e}"),
                            }
                        })?;
                    }
                }
            }
            return Ok(());
        }

        if level == READ_CONSISTENCY_STRONG {
            let timeout = INGEST_STORAGE_DEFAULT_READ_CONSISTENCY_TIMEOUT;
            if let Some(encoded) = md
                .get(CONSISTENCY_OFFSETS_MD_KEY)
                .and_then(|v| v.to_str().ok())
            {
                if let Some(offsets) = lookup_encoded_consistency_offsets(
                    encoded,
                    self.cfg.read_compartment_id,
                    self.ingest_partition_id,
                ) {
                    reader
                        .wait_read_consistency_until_offsets(&offsets, timeout)
                        .await
                        .map_err(|e| IngesterError::Internal {
                            message: format!("wait for read consistency: {e}"),
                        })?;
                    return Ok(());
                }
            }

            reader
                .wait_read_consistency_until_last_produced_offset(timeout)
                .await
                .map_err(|e| IngesterError::Internal {
                    message: format!("wait for read consistency: {e}"),
                })?;
        }

        Ok(())
    }

    pub async fn handle_query_stream(
        &self,
        md: &MetadataMap,
        user_id: &str,
        mut req: ingesterpb::QueryRequest,
    ) -> Result<Vec<ingesterpb::QueryStreamResponse>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self.query_stream_inner(md, user_id, &mut req).await;
        permit.finish(false);
        res
    }

    /// Bimodal `QueryStream` implementation:
    /// - Fast inline path when matching series <= 128, total chunks <= batch_limit, and payload < 1 MiB
    ///   (builds 1 or 2 `QueryStreamResponse`s synchronously without `tokio::spawn` or `mpsc::channel`).
    /// - Bounded `mpsc::channel(8)` incremental streamer for larger queries so peak memory stays flat.
    pub async fn handle_query_stream_bimodal(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        mut req: ingesterpb::QueryRequest,
    ) -> Result<QueryStreamBox, IngesterError> {
        let permit = self.start_read_request(md).await?;
        if req.streaming_chunks_batch_size == 0 {
            req.streaming_chunks_batch_size = FALLBACK_CHUNK_STREAM_BATCH_SIZE;
        }
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;

        self.metrics.queries.inc();
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            permit.finish(false);
            return Ok(Box::pin(tokio_stream::empty()));
        };

        let mut chunk_series = self.select_chunk_series_for_query(
            &db,
            user_id,
            req.start_timestamp_ms,
            req.end_timestamp_ms,
            shard,
            &matchers,
        )?;

        let batch_limit = req.streaming_chunks_batch_size as usize;
        let num_series = chunk_series.len();
        let total_chunks: usize = chunk_series.iter().map(|cs| cs.chunks.len()).sum();
        let est_chunk_bytes: usize = chunk_series
            .iter()
            .flat_map(|cs| cs.chunks.iter())
            .map(|cm| cm.chunk.as_ref().map_or(0, |c| c.as_bytes().len() + 32))
            .sum();

        if num_series <= QUERY_STREAM_BATCH_SIZE
            && total_chunks <= batch_limit
            && est_chunk_bytes < QUERY_STREAM_BATCH_MESSAGE_SIZE
        {
            if num_series == 0 {
                self.metrics
                    .queried_series
                    .with_label_values(&["merged_blocks"])
                    .observe(0.0);
                self.metrics.queried_samples.observe(0.0);
                permit.finish(false);
                let resp = ingesterpb::QueryStreamResponse {
                    streaming_series: Vec::new(),
                    is_end_of_series_stream: true,
                    streaming_series_chunks: Vec::new(),
                };
                return Ok(Box::pin(tokio_stream::iter(std::iter::once(Ok(resp)))));
            }

            let mut series_batch = Vec::with_capacity(num_series);
            for cs in chunk_series.iter_mut() {
                let labels = std::mem::take(&mut cs.labels);
                series_batch.push(ingesterpb::QueryStreamSeries {
                    labels: labels_to_proto_pairs(&labels),
                    chunk_count: cs.chunks.len() as i64,
                });
            }

            let mut responses: smallvec::SmallVec<
                [Result<ingesterpb::QueryStreamResponse, Status>; 2],
            > = smallvec::SmallVec::new();
            responses.push(Ok(ingesterpb::QueryStreamResponse {
                streaming_series: series_batch,
                is_end_of_series_stream: true,
                streaming_series_chunks: Vec::new(),
            }));

            let mut num_samples = 0usize;
            let mut chunks_batch = Vec::with_capacity(num_series);
            let mut batch_bytes = 0usize;

            for (series_idx, cs) in chunk_series.into_iter().enumerate() {
                let mut proto_chunks = Vec::with_capacity(cs.chunks.len());
                for cm in cs.chunks {
                    let Some(enc_chunk) = cm.chunk else {
                        permit.finish(false);
                        return Err(IngesterError::Internal {
                            message: "unfilled chunk returned from TSDB chunk querier".to_string(),
                        });
                    };
                    num_samples += enc_chunk.num_samples();
                    let wire_enc = tsdb_encoding_to_wire(enc_chunk.encoding());
                    let data = Bytes::from(enc_chunk.into_bytes());
                    proto_chunks.push(ingesterpb::Chunk {
                        start_timestamp_ms: cm.min_time,
                        end_timestamp_ms: cm.max_time,
                        encoding: wire_enc,
                        data,
                    });
                }

                let sc = ingesterpb::QueryStreamSeriesChunks {
                    series_index: series_idx as u64,
                    chunks: proto_chunks,
                };
                let msg_size = sc.encoded_len();

                if (batch_bytes > 0 && batch_bytes + msg_size > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                    || chunks_batch.len() >= batch_limit
                {
                    responses.push(Ok(ingesterpb::QueryStreamResponse {
                        streaming_series: Vec::new(),
                        is_end_of_series_stream: false,
                        streaming_series_chunks: std::mem::take(&mut chunks_batch),
                    }));
                    batch_bytes = 0;
                }

                chunks_batch.push(sc);
                batch_bytes += msg_size;
            }

            if !chunks_batch.is_empty() {
                responses.push(Ok(ingesterpb::QueryStreamResponse {
                    streaming_series: Vec::new(),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: chunks_batch,
                }));
            }

            self.metrics
                .queried_series
                .with_label_values(&["merged_blocks"])
                .observe(num_series as f64);
            self.metrics.queried_samples.observe(num_samples as f64);
            permit.finish(false);
            return Ok(Box::pin(tokio_stream::iter(responses)));
        }

        let (tx, rx) = mpsc::channel(8);
        let ingester = Arc::clone(self);
        tokio::spawn(async move {
            let res = ingester
                .stream_selected_query_batches(chunk_series, batch_limit, &tx)
                .await;
            if let Err(e) = res {
                let _ = tx.send(Err(e.map_read_error_to_status())).await;
            }
            permit.finish(false);
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    /// Streaming `QueryStream` implementation backed by a bounded `mpsc::channel(8)` so
    /// 128-series label batches and chunk batches are flushed and dropped incrementally
    /// without buffering the entire query result set in memory.
    pub async fn handle_query_stream_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        mut req: ingesterpb::QueryRequest,
    ) -> Result<ReceiverStream<Result<ingesterpb::QueryStreamResponse, Status>>, IngesterError>
    {
        let permit = self.start_read_request(md).await?;
        if req.streaming_chunks_batch_size == 0 {
            req.streaming_chunks_batch_size = FALLBACK_CHUNK_STREAM_BATCH_SIZE;
        }
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;

        self.metrics.queries.inc();
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            permit.finish(false);
            let (_tx, rx) = mpsc::channel(1);
            return Ok(ReceiverStream::new(rx));
        };

        let (tx, rx) = mpsc::channel(8);
        let ingester = Arc::clone(self);
        let user_id_owned = user_id.to_string();
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let batch_limit = req.streaming_chunks_batch_size as usize;

        tokio::spawn(async move {
            let res = ingester
                .stream_query_batches(
                    &db,
                    &user_id_owned,
                    start_ts,
                    end_ts,
                    batch_limit,
                    shard,
                    &matchers,
                    &tx,
                )
                .await;
            if let Err(e) = res {
                let _ = tx.send(Err(e.map_read_error_to_status())).await;
            }
            permit.finish(false);
        });

        Ok(ReceiverStream::new(rx))
    }

    #[allow(clippy::too_many_arguments)]
    fn select_chunk_series_for_query(
        &self,
        db: &UserTsdb,
        user_id: &str,
        start_timestamp_ms: i64,
        end_timestamp_ms: i64,
        shard: Option<(u64, u64)>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<QueryChunkSeries>, IngesterError> {
        let ooo_window = self
            .overrides
            .for_tenant(user_id)
            .out_of_order_time_window
            .0;
        let mut chunk_series = if self
            .blocks_cfg
            .tsdb
            .index_lookup_planning
            .index_lookup_planning_enabled
        {
            let planner: Arc<dyn LookupPlanner> = Arc::new(NoopPlanner);
            let mut planned_cq = PlannedTsdbChunkQuerier::new_with_unordered(
                Arc::clone(&db.db),
                start_timestamp_ms,
                end_timestamp_ms,
                planner,
                ooo_window > 0,
            );
            let select_hints = shard.map(|(shard_index, shard_count)| SelectHints {
                shard_index,
                shard_count,
            });
            let portion = self
                .blocks_cfg
                .tsdb
                .index_lookup_planning
                .index_lookup_planning_comparison_portion;
            if portion > 0.0 && rand::random::<f64>() <= portion {
                let mut mq = MirroredChunkQuerier::new_with_meta(
                    user_id,
                    self.metrics.index_lookup_comparison_outcomes.clone(),
                    start_timestamp_ms,
                    end_timestamp_ms,
                    ChunkBlockMeta::default(),
                    Box::new(planned_cq),
                );
                let mut ss = mq.select(true, select_hints.as_ref(), matchers);
                let mut out = Vec::new();
                while ss.next() {
                    if let Some(s) = ss.at() {
                        out.push(QueryChunkSeries {
                            labels: s.labels(),
                            chunks: s.chunks(),
                        });
                    }
                }
                if let Some(e) = ss.err() {
                    return Err(IngesterError::Internal {
                        message: format!("selecting series from MirroredChunkQuerier: {e}"),
                    });
                }
                drop(ss);
                let _ = mq.close();
                out
            } else {
                let mut ss = planned_cq.select(true, select_hints.as_ref(), matchers);
                let mut out = Vec::new();
                while ss.next() {
                    if let Some(s) = ss.at() {
                        out.push(QueryChunkSeries {
                            labels: s.labels(),
                            chunks: s.chunks(),
                        });
                    }
                }
                if let Some(e) = ss.err() {
                    return Err(IngesterError::Internal {
                        message: format!("selecting series from PlannedChunkQuerier: {e}"),
                    });
                }
                out
            }
        } else {
            let cq = if ooo_window > 0 {
                db.db
                    .unordered_chunk_querier(start_timestamp_ms, end_timestamp_ms)
            } else {
                db.db.chunk_querier(start_timestamp_ms, end_timestamp_ms)
            };
            cq.select(true, matchers)
                .map_err(|e| IngesterError::Internal {
                    message: format!("selecting series from ChunkQuerier: {e}"),
                })?
        };

        if let Some((shard_idx, shard_count)) = shard {
            chunk_series.retain(|cs| labels_stable_hash(&cs.labels) % shard_count == shard_idx);
        }
        Ok(chunk_series)
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_query_batches(
        &self,
        db: &UserTsdb,
        user_id: &str,
        start_timestamp_ms: i64,
        end_timestamp_ms: i64,
        batch_limit: usize,
        shard: Option<(u64, u64)>,
        matchers: &[LabelMatcher],
        tx: &mpsc::Sender<Result<ingesterpb::QueryStreamResponse, Status>>,
    ) -> Result<(), IngesterError> {
        let chunk_series = self.select_chunk_series_for_query(
            db,
            user_id,
            start_timestamp_ms,
            end_timestamp_ms,
            shard,
            matchers,
        )?;
        self.stream_selected_query_batches(chunk_series, batch_limit, tx)
            .await
    }

    async fn stream_selected_query_batches(
        &self,
        mut chunk_series: Vec<QueryChunkSeries>,
        batch_limit: usize,
        tx: &mpsc::Sender<Result<ingesterpb::QueryStreamResponse, Status>>,
    ) -> Result<(), IngesterError> {

        let num_series = chunk_series.len();
        let mut series_batch = Vec::with_capacity(QUERY_STREAM_BATCH_SIZE.min(num_series.max(1)));
        for cs in chunk_series.iter_mut() {
            let labels = std::mem::take(&mut cs.labels);
            series_batch.push(ingesterpb::QueryStreamSeries {
                labels: labels_to_proto_pairs(&labels),
                chunk_count: cs.chunks.len() as i64,
            });
            drop(labels);
            if series_batch.len() >= QUERY_STREAM_BATCH_SIZE {
                let resp = ingesterpb::QueryStreamResponse {
                    streaming_series: std::mem::take(&mut series_batch),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: Vec::new(),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return Ok(());
                }
            }
        }
        if tx
            .send(Ok(ingesterpb::QueryStreamResponse {
                streaming_series: series_batch,
                is_end_of_series_stream: true,
                streaming_series_chunks: Vec::new(),
            }))
            .await
            .is_err()
        {
            return Ok(());
        }

        let mut num_samples = 0usize;
        let mut chunks_batch = Vec::with_capacity(batch_limit.min(1024));
        let mut batch_bytes = 0usize;

        for (series_idx, cs) in chunk_series.into_iter().enumerate() {
            let mut proto_chunks = Vec::with_capacity(cs.chunks.len());
            for cm in cs.chunks {
                let Some(enc_chunk) = cm.chunk else {
                    return Err(IngesterError::Internal {
                        message: "unfilled chunk returned from TSDB chunk querier".to_string(),
                    });
                };
                num_samples += enc_chunk.num_samples();
                let wire_enc = tsdb_encoding_to_wire(enc_chunk.encoding());
                let data = Bytes::from(enc_chunk.into_bytes());
                proto_chunks.push(ingesterpb::Chunk {
                    start_timestamp_ms: cm.min_time,
                    end_timestamp_ms: cm.max_time,
                    encoding: wire_enc,
                    data,
                });
            }

            let sc = ingesterpb::QueryStreamSeriesChunks {
                series_index: series_idx as u64,
                chunks: proto_chunks,
            };
            let msg_size = sc.encoded_len();

            if (batch_bytes > 0 && batch_bytes + msg_size > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                || chunks_batch.len() >= batch_limit
            {
                let resp = ingesterpb::QueryStreamResponse {
                    streaming_series: Vec::new(),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: std::mem::take(&mut chunks_batch),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return Ok(());
                }
                batch_bytes = 0;
            }

            chunks_batch.push(sc);
            batch_bytes += msg_size;
        }

        if !chunks_batch.is_empty()
            && tx
                .send(Ok(ingesterpb::QueryStreamResponse {
                    streaming_series: Vec::new(),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: chunks_batch,
                }))
                .await
                .is_err()
        {
            return Ok(());
        }

        self.metrics
            .queried_series
            .with_label_values(&["merged_blocks"])
            .observe(num_series as f64);
        self.metrics.queried_samples.observe(num_samples as f64);

        Ok(())
    }

    async fn query_stream_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: &mut ingesterpb::QueryRequest,
    ) -> Result<Vec<ingesterpb::QueryStreamResponse>, IngesterError> {
        if req.streaming_chunks_batch_size == 0 {
            req.streaming_chunks_batch_size = FALLBACK_CHUNK_STREAM_BATCH_SIZE;
        }
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;

        self.metrics.queries.inc();
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(Vec::new());
        };

        let mut chunk_series = self.select_chunk_series_for_query(
            &db,
            user_id,
            req.start_timestamp_ms,
            req.end_timestamp_ms,
            shard,
            &matchers,
        )?;

        let num_series = chunk_series.len();
        let mut responses = Vec::with_capacity((num_series / QUERY_STREAM_BATCH_SIZE) + 8);
        let mut series_batch = Vec::with_capacity(QUERY_STREAM_BATCH_SIZE.min(num_series.max(1)));
        for cs in chunk_series.iter_mut() {
            let labels = std::mem::take(&mut cs.labels);
            series_batch.push(ingesterpb::QueryStreamSeries {
                labels: labels_to_proto_pairs(&labels),
                chunk_count: cs.chunks.len() as i64,
            });
            drop(labels);
            if series_batch.len() >= QUERY_STREAM_BATCH_SIZE {
                responses.push(ingesterpb::QueryStreamResponse {
                    streaming_series: std::mem::take(&mut series_batch),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: Vec::new(),
                });
            }
        }
        responses.push(ingesterpb::QueryStreamResponse {
            streaming_series: series_batch,
            is_end_of_series_stream: true,
            streaming_series_chunks: Vec::new(),
        });
        let mut num_samples = 0usize;

        // Phase 2: stream QueryStreamSeriesChunks batches respecting streaming_chunks_batch_size & 1MiB cap.
        let batch_limit = req.streaming_chunks_batch_size as usize;
        let mut chunks_batch = Vec::with_capacity(batch_limit.min(1024));
        let mut batch_bytes = 0usize;

        for (series_idx, cs) in chunk_series.into_iter().enumerate() {
            let mut proto_chunks = Vec::with_capacity(cs.chunks.len());
            for cm in cs.chunks {
                let Some(enc_chunk) = cm.chunk else {
                    return Err(IngesterError::Internal {
                        message: "unfilled chunk returned from TSDB chunk querier".to_string(),
                    });
                };
                num_samples += enc_chunk.num_samples();
                let wire_enc = tsdb_encoding_to_wire(enc_chunk.encoding());
                let data = Bytes::from(enc_chunk.into_bytes());
                proto_chunks.push(ingesterpb::Chunk {
                    start_timestamp_ms: cm.min_time,
                    end_timestamp_ms: cm.max_time,
                    encoding: wire_enc,
                    data,
                });
            }

            let sc = ingesterpb::QueryStreamSeriesChunks {
                series_index: series_idx as u64,
                chunks: proto_chunks,
            };
            let msg_size = sc.encoded_len();

            if (batch_bytes > 0 && batch_bytes + msg_size > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                || chunks_batch.len() >= batch_limit
            {
                responses.push(ingesterpb::QueryStreamResponse {
                    streaming_series: Vec::new(),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: std::mem::take(&mut chunks_batch),
                });
                batch_bytes = 0;
            }

            chunks_batch.push(sc);
            batch_bytes += msg_size;
        }

        if !chunks_batch.is_empty() {
            responses.push(ingesterpb::QueryStreamResponse {
                streaming_series: Vec::new(),
                is_end_of_series_stream: false,
                streaming_series_chunks: chunks_batch,
            });
        }

        self.metrics
            .queried_series
            .with_label_values(&["merged_blocks"])
            .observe(num_series as f64);
        self.metrics.queried_samples.observe(num_samples as f64);

        Ok(responses)
    }

    /// Streams Phase 1 (`QueryStreamSeries` batches ending with `is_end_of_series_stream: true`)
    /// from `db.chunk_querier(mint, maxt)` via `sink`, releasing each series' labels as each batch
    /// is flushed (`pkg/ingester/ingester_query.go:sendStreamingQuerySeries`).
    pub async fn send_streaming_query_series(
        &self,
        db: &UserTsdb,
        start_timestamp_ms: i64,
        end_timestamp_ms: i64,
        matchers: &[LabelMatcher],
        sink: &dyn QueryStreamSink,
    ) -> Result<Vec<QueryChunkSeries>, IngesterError> {
        let cq = db.db.chunk_querier(start_timestamp_ms, end_timestamp_ms);
        let mut chunk_series = cq
            .select(true, matchers)
            .map_err(|e| IngesterError::Internal {
                message: format!("selecting series from ChunkQuerier: {e}"),
            })?;
        self.send_streaming_query_series_from_chunk_series(&mut chunk_series, sink)
            .await?;
        Ok(chunk_series)
    }

    async fn send_streaming_query_series_from_chunk_series(
        &self,
        chunk_series: &mut [QueryChunkSeries],
        sink: &dyn QueryStreamSink,
    ) -> Result<usize, IngesterError> {
        let num_series = chunk_series.len();
        let mut series_batch = Vec::with_capacity(QUERY_STREAM_BATCH_SIZE);
        for cs in chunk_series.iter_mut() {
            // Release each series' labels as soon as it is serialized into the batch,
            // bounding peak label memory to ~QUERY_STREAM_BATCH_SIZE * labels_per_series.
            let labels = std::mem::take(&mut cs.labels);
            series_batch.push(ingesterpb::QueryStreamSeries {
                labels: labels_to_proto_pairs(&labels),
                chunk_count: cs.chunks.len() as i64,
            });
            drop(labels);
            if series_batch.len() >= QUERY_STREAM_BATCH_SIZE {
                sink.send(ingesterpb::QueryStreamResponse {
                    streaming_series: std::mem::take(&mut series_batch),
                    is_end_of_series_stream: false,
                    streaming_series_chunks: Vec::new(),
                })
                .await?;
            }
        }
        sink.send(ingesterpb::QueryStreamResponse {
            streaming_series: series_batch,
            is_end_of_series_stream: true,
            streaming_series_chunks: Vec::new(),
        })
        .await?;
        Ok(num_series)
    }

    pub async fn handle_query_exemplars(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ExemplarQueryRequest,
    ) -> Result<ingesterpb::ExemplarQueryResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self.query_exemplars_inner(md, user_id, req).await;
        permit.finish(false);
        res
    }

    async fn query_exemplars_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ExemplarQueryRequest,
    ) -> Result<ingesterpb::ExemplarQueryResponse, IngesterError> {
        let mut matcher_sets = Vec::with_capacity(req.matchers.len());
        for lm_set in &req.matchers {
            matcher_sets.push(from_proto_matchers(&lm_set.matchers)?);
        }
        self.metrics.queries.inc();
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(ingesterpb::ExemplarQueryResponse::default());
        };

        let slices: Vec<&[LabelMatcher]> = matcher_sets.iter().map(Vec::as_slice).collect();
        let results =
            db.db
                .select_exemplars(req.start_timestamp_ms, req.end_timestamp_ms, &slices);

        let mut num_exemplars = 0usize;
        let mut timeseries = Vec::with_capacity(results.len());
        for es in results {
            let exs: Vec<cortexpb::Exemplar> = es
                .exemplars
                .into_iter()
                .map(|ex| cortexpb::Exemplar {
                    labels: labels_to_proto_pairs(&ex.labels),
                    value: ex.value,
                    timestamp_ms: ex.timestamp_ms,
                })
                .collect();
            num_exemplars += exs.len();
            timeseries.push(cortexpb::TimeSeries {
                labels: labels_to_proto_pairs(&es.series_labels),
                samples: Vec::new(),
                exemplars: exs,
                histograms: Vec::new(),
                created_timestamp: 0,
            });
        }
        self.metrics.queried_exemplars.observe(num_exemplars as f64);

        Ok(ingesterpb::ExemplarQueryResponse { timeseries })
    }

    pub async fn handle_label_values(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesRequest,
    ) -> Result<ingesterpb::LabelValuesResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self.label_values_inner(md, user_id, req).await;
        permit.finish(false);
        res
    }

    async fn label_values_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesRequest,
    ) -> Result<ingesterpb::LabelValuesResponse, IngesterError> {
        let matchers = match req.matchers {
            Some(ref m) => from_proto_matchers(&m.matchers)?,
            None => Vec::new(),
        };
        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(ingesterpb::LabelValuesResponse::default());
        };
        let q = db.db.querier(req.start_timestamp_ms, req.end_timestamp_ms);
        let mut vals = q
            .label_values(&req.label_name, limit, &matchers)
            .map_err(|e| IngesterError::Internal {
                message: e.to_string(),
            })?;
        if let Some(lim) = limit {
            if vals.len() > lim {
                vals.truncate(lim);
            }
        }
        Ok(ingesterpb::LabelValuesResponse { label_values: vals })
    }

    pub async fn handle_label_names(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesRequest,
    ) -> Result<ingesterpb::LabelNamesResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self.label_names_inner(md, user_id, req).await;
        permit.finish(false);
        res
    }

    async fn label_names_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesRequest,
    ) -> Result<ingesterpb::LabelNamesResponse, IngesterError> {
        let matchers = match req.matchers {
            Some(ref m) => from_proto_matchers(&m.matchers)?,
            None => Vec::new(),
        };
        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(ingesterpb::LabelNamesResponse::default());
        };
        let q = db.db.querier(req.start_timestamp_ms, req.end_timestamp_ms);
        let mut names = q.label_names(&matchers).map_err(|e| IngesterError::Internal {
            message: e.to_string(),
        })?;
        if let Some(lim) = limit {
            if names.len() > lim {
                names.truncate(lim);
            }
        }
        Ok(ingesterpb::LabelNamesResponse { label_names: names })
    }

    pub async fn handle_user_stats(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::UserStatsRequest,
    ) -> Result<ingesterpb::UserStatsResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = async {
            self.enforce_read_consistency(md, user_id).await?;
            let Some(db) = self.get_tsdb(user_id) else {
                return Ok(ingesterpb::UserStatsResponse::default());
            };
            create_user_stats(&db, req.count_method)
        }
        .await;
        permit.finish(false);
        res
    }

    pub async fn handle_all_user_stats(
        &self,
        md: &MetadataMap,
        req: ingesterpb::UserStatsRequest,
    ) -> Result<ingesterpb::UsersStatsResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = (|| -> Result<ingesterpb::UsersStatsResponse, IngesterError> {
            let mut stats = Vec::with_capacity(self.tsdbs.len());
            for entry in self.tsdbs.iter() {
                let data = create_user_stats(entry.value(), req.count_method)?;
                stats.push(ingesterpb::UserIdStatsResponse {
                    user_id: entry.key().clone(),
                    data: Some(data),
                });
            }
            Ok(ingesterpb::UsersStatsResponse { stats })
        })();
        permit.finish(false);
        res
    }

    pub async fn handle_metrics_for_label_matchers(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::MetricsForLabelMatchersRequest,
    ) -> Result<ingesterpb::MetricsForLabelMatchersResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self
            .metrics_for_label_matchers_inner(md, user_id, req)
            .await;
        permit.finish(false);
        res
    }

    async fn metrics_for_label_matchers_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::MetricsForLabelMatchersRequest,
    ) -> Result<ingesterpb::MetricsForLabelMatchersResponse, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(ingesterpb::MetricsForLabelMatchersResponse::default());
        };
        let mut matchers_set = Vec::with_capacity(req.matchers_set.len());
        for lm_set in &req.matchers_set {
            matchers_set.push(from_proto_matchers(&lm_set.matchers)?);
        }

        let cq = db
            .db
            .chunk_querier(req.start_timestamp_ms, req.end_timestamp_ms);
        let mut merged_labels: BTreeSet<Labels> = BTreeSet::new();
        for matchers in &matchers_set {
            let series = cq
                .select_raw_labels(true, matchers)
                .map_err(|e| IngesterError::Internal {
                    message: e.to_string(),
                })?;
            merged_labels.extend(series);
        }
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            usize::MAX
        };
        let metric: Vec<cortexpb::Metric> = merged_labels
            .into_iter()
            .take(limit)
            .map(|lbls| cortexpb::Metric {
                labels: labels_to_proto_pairs(&lbls),
            })
            .collect();
        Ok(ingesterpb::MetricsForLabelMatchersResponse { metric })
    }

    pub async fn handle_metrics_metadata(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::MetricsMetadataRequest,
    ) -> Result<ingesterpb::MetricsMetadataResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = (|| -> Result<ingesterpb::MetricsMetadataResponse, IngesterError> {
            let Some(store) = self.get_user_metadata(user_id) else {
                return Ok(ingesterpb::MetricsMetadataResponse::default());
            };
            let metadata = store.query(
                &req.metric_names,
                &req.metric,
                req.limit,
                req.limit_per_metric,
            );
            Ok(ingesterpb::MetricsMetadataResponse { metadata })
        })();
        permit.finish(false);
        res
    }

    pub async fn handle_label_names_and_values(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesAndValuesRequest,
    ) -> Result<Vec<ingesterpb::LabelNamesAndValuesResponse>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self
            .label_names_and_values_inner(md, user_id, req)
            .await;
        permit.finish(false);
        res
    }

    async fn label_names_and_values_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesAndValuesRequest,
    ) -> Result<Vec<ingesterpb::LabelNamesAndValuesResponse>, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(Vec::new());
        };
        let matchers = from_proto_matchers(&req.matchers)?;
        let active_only = match ingesterpb::CountMethod::try_from(req.count_method) {
            Ok(ingesterpb::CountMethod::InMemory) => false,
            Ok(ingesterpb::CountMethod::Active) => true,
            Err(_) => {
                return Err(IngesterError::Internal {
                    message: format!("unknown count method {}", req.count_method),
                });
            }
        };

        let head = db.db.head();
        let label_names = head
            .label_names(&matchers)
            .map_err(|e| IngesterError::Internal {
                message: e.to_string(),
            })?;

        let mut batches = Vec::new();
        let mut resp = ingesterpb::LabelNamesAndValuesResponse::default();
        let mut resp_bytes = 0usize;

        for label_name in label_names {
            resp_bytes += label_name.len();
            if resp_bytes >= LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES && !resp.items.is_empty() {
                batches.push(std::mem::take(&mut resp));
                resp_bytes = label_name.len();
            }

            let values = head
                .label_values(&label_name, &matchers)
                .map_err(|e| IngesterError::Internal {
                    message: e.to_string(),
                })?;

            let mut filtered = Vec::with_capacity(values.len());
            if active_only {
                for val in values {
                    let is_active = if matchers.is_empty() {
                        head.postings_for_label_value(&label_name, &val)
                            .into_iter()
                            .any(|r| db.active_series.contains_ref(r))
                    } else {
                        let mut ms = matchers.clone();
                        ms.push(
                            LabelMatcher::new(MatchType::Equal, &label_name, &val)
                                .expect("valid eq matcher"),
                        );
                        head.postings_for_matchers(&ms)
                            .map_err(|e| IngesterError::Internal {
                                message: e.to_string(),
                            })?
                            .into_iter()
                            .any(|r| db.active_series.contains_ref(r))
                    };
                    if is_active {
                        filtered.push(val);
                    }
                }
            } else {
                filtered = values;
            }

            if filtered.is_empty() {
                continue;
            }

            let mut cur_vals = Vec::new();
            for (idx, val) in filtered.iter().enumerate() {
                resp_bytes += val.len();
                cur_vals.push(val.clone());
                let is_last = idx + 1 == filtered.len();
                if resp_bytes >= LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES {
                    resp.items.push(ingesterpb::LabelValues {
                        label_name: label_name.clone(),
                        values: std::mem::take(&mut cur_vals),
                    });
                    batches.push(std::mem::take(&mut resp));
                    resp_bytes = if is_last { 0 } else { label_name.len() };
                } else if is_last && !cur_vals.is_empty() {
                    resp.items.push(ingesterpb::LabelValues {
                        label_name: label_name.clone(),
                        values: std::mem::take(&mut cur_vals),
                    });
                }
            }
        }

        if !resp.items.is_empty() {
            batches.push(resp);
        }
        Ok(batches)
    }

    pub async fn handle_label_values_cardinality(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesCardinalityRequest,
    ) -> Result<Vec<ingesterpb::LabelValuesCardinalityResponse>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self
            .label_values_cardinality_inner(md, user_id, req)
            .await;
        permit.finish(false);
        res
    }

    async fn label_values_cardinality_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesCardinalityRequest,
    ) -> Result<Vec<ingesterpb::LabelValuesCardinalityResponse>, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(Vec::new());
        };
        let matchers = from_proto_matchers(&req.matchers)?;
        let active_only = match ingesterpb::CountMethod::try_from(req.count_method) {
            Ok(ingesterpb::CountMethod::InMemory) => false,
            Ok(ingesterpb::CountMethod::Active) => true,
            Err(_) => {
                return Err(IngesterError::Internal {
                    message: format!("unknown count method {}", req.count_method),
                });
            }
        };

        let head = db.db.head();
        let mut batches = Vec::new();
        let mut resp = ingesterpb::LabelValuesCardinalityResponse::default();
        let mut resp_bytes = 0usize;

        for lbl_name in &req.label_names {
            let lbl_values =
                head.label_values(lbl_name, &matchers)
                    .map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })?;

            let mut map: HashMap<String, u64> = HashMap::new();
            for val in lbl_values {
                let refs = if matchers.is_empty() {
                    head.postings_for_label_value(lbl_name, &val)
                } else {
                    let mut ms = matchers.clone();
                    ms.push(
                        LabelMatcher::new(MatchType::Equal, lbl_name, &val)
                            .expect("valid eq matcher"),
                    );
                    head.postings_for_matchers(&ms)
                        .map_err(|e| IngesterError::Internal {
                            message: e.to_string(),
                        })?
                };
                let count = if active_only {
                    refs.into_iter()
                        .filter(|r| db.active_series.contains_ref(*r))
                        .count() as u64
                } else {
                    refs.len() as u64
                };
                if count == 0 {
                    continue;
                }
                resp_bytes += val.len();
                map.insert(val, count);
                if resp_bytes >= LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES {
                    resp.items.push(ingesterpb::LabelValueSeriesCount {
                        label_name: lbl_name.clone(),
                        label_value_series: std::mem::take(&mut map),
                    });
                    batches.push(std::mem::take(&mut resp));
                    resp_bytes = 0;
                }
            }
            if !map.is_empty() {
                resp.items.push(ingesterpb::LabelValueSeriesCount {
                    label_name: lbl_name.clone(),
                    label_value_series: map,
                });
            }
        }
        if !resp.items.is_empty() {
            batches.push(resp);
        }
        Ok(batches)
    }

    pub async fn handle_active_series_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ActiveSeriesRequest,
    ) -> Result<ReceiverStream<Result<ingesterpb::ActiveSeriesResponse, Status>>, IngesterError>
    {
        let permit = self.start_read_request(md).await?;
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            permit.finish(false);
            let (_tx, rx) = mpsc::channel(1);
            return Ok(ReceiverStream::new(rx));
        };

        let (tx, rx) = mpsc::channel(8);
        let is_native_histogram = req.r#type
            == ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32;

        tokio::spawn(async move {
            let head = db.db.head();
            let refs = if matchers.is_empty() {
                Ok(Vec::new())
            } else {
                head.postings_for_matchers(&matchers)
                    .map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })
            };
            let refs = match refs {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(e.map_read_error_to_status())).await;
                    permit.finish(false);
                    return;
                }
            };

            let mut resp = ingesterpb::ActiveSeriesResponse::default();
            let mut current_size = 0usize;

            for ref_id in refs {
                let bucket_count = if is_native_histogram {
                    let Some(bc) = db.active_series.native_histogram_buckets(ref_id) else {
                        continue;
                    };
                    Some(bc)
                } else {
                    if !db.active_series.contains_ref(ref_id) {
                        continue;
                    }
                    None
                };

                let Some(lbls) = head.series_labels(ref_id) else {
                    continue;
                };
                if let Some((shard_idx, shard_count)) = shard {
                    if labels_stable_hash(&lbls) % shard_count != shard_idx {
                        continue;
                    }
                }

                let m = cortexpb::Metric {
                    labels: labels_to_proto_pairs(&lbls),
                };
                let m_size = m.encoded_len() + if is_native_histogram { 8 } else { 0 };
                if current_size + m_size > ACTIVE_SERIES_MAX_SIZE_BYTES && !resp.metric.is_empty()
                {
                    if tx.send(Ok(std::mem::take(&mut resp))).await.is_err() {
                        permit.finish(false);
                        return;
                    }
                    current_size = 0;
                }
                resp.metric.push(m);
                if let Some(bc) = bucket_count {
                    resp.bucket_count.push(bc);
                }
                current_size += m_size;
            }
            if !resp.metric.is_empty() {
                let _ = tx.send(Ok(resp)).await;
            }
            permit.finish(false);
        });

        Ok(ReceiverStream::new(rx))
    }

    pub async fn handle_active_series(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ActiveSeriesRequest,
    ) -> Result<Vec<ingesterpb::ActiveSeriesResponse>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self.active_series_inner(md, user_id, req).await;
        permit.finish(false);
        res
    }

    async fn active_series_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ActiveSeriesRequest,
    ) -> Result<Vec<ingesterpb::ActiveSeriesResponse>, IngesterError> {
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_tsdb(user_id) else {
            return Ok(Vec::new());
        };
        let is_native_histogram = req.r#type
            == ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32;
        let head = db.db.head();
        let refs = if matchers.is_empty() {
            Vec::new()
        } else {
            head.postings_for_matchers(&matchers)
                .map_err(|e| IngesterError::Internal {
                    message: e.to_string(),
                })?
        };

        let mut batches = Vec::new();
        let mut resp = ingesterpb::ActiveSeriesResponse::default();
        let mut current_size = 0usize;

        for ref_id in refs {
            let bucket_count = if is_native_histogram {
                let Some(bc) = db.active_series.native_histogram_buckets(ref_id) else {
                    continue;
                };
                Some(bc)
            } else {
                if !db.active_series.contains_ref(ref_id) {
                    continue;
                }
                None
            };

            let Some(lbls) = head.series_labels(ref_id) else {
                continue;
            };
            if let Some((shard_idx, shard_count)) = shard {
                if labels_stable_hash(&lbls) % shard_count != shard_idx {
                    continue;
                }
            }

            let m = cortexpb::Metric {
                labels: labels_to_proto_pairs(&lbls),
            };
            let m_size = m.encoded_len() + if is_native_histogram { 8 } else { 0 };
            if current_size + m_size > ACTIVE_SERIES_MAX_SIZE_BYTES && !resp.metric.is_empty() {
                batches.push(std::mem::take(&mut resp));
                current_size = 0;
            }
            resp.metric.push(m);
            if let Some(bc) = bucket_count {
                resp.bucket_count.push(bc);
            }
            current_size += m_size;
        }
        if !resp.metric.is_empty() {
            batches.push(resp);
        }
        Ok(batches)
    }

    pub async fn handle_search_label_names(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelNamesRequest,
    ) -> Result<Vec<ingesterpb::SearchResultBatch>, IngesterError> {
        let (hints, matchers) =
            build_search_hints(req.filter.as_ref(), req.ordering, req.limit, &req.matchers)?;
        let permit = self.start_read_request(md).await?;
        let res = async {
            self.enforce_read_consistency(md, user_id).await?;
            let Some(db) = self.get_tsdb(user_id) else {
                return Ok(Vec::new());
            };
            let q = db.db.querier(req.start_timestamp_ms, req.end_timestamp_ms);
            let results =
                q.search_label_names(&hints, &matchers)
                    .map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })?;
            let mut rs = VecSearchResultSet::new(results, Vec::new(), None);
            let mut batches = Vec::new();
            stream_search_results(
                &|| false,
                &mut rs,
                &mut |b| {
                    batches.push(b);
                    Ok(())
                },
            )
            .map_err(|message| IngesterError::Internal { message })?;
            Ok(batches)
        }
        .await;
        permit.finish(false);
        res
    }

    pub async fn handle_search_label_values(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelValuesRequest,
    ) -> Result<Vec<ingesterpb::SearchResultBatch>, IngesterError> {
        let (hints, matchers) =
            build_search_hints(req.filter.as_ref(), req.ordering, req.limit, &req.matchers)?;
        let permit = self.start_read_request(md).await?;
        let res = async {
            self.enforce_read_consistency(md, user_id).await?;
            let Some(db) = self.get_tsdb(user_id) else {
                return Ok(Vec::new());
            };
            let q = db.db.querier(req.start_timestamp_ms, req.end_timestamp_ms);
            let results = q
                .search_label_values(&req.name, &hints, &matchers)
                .map_err(|e| IngesterError::Internal {
                    message: e.to_string(),
                })?;
            let mut rs = VecSearchResultSet::new(results, Vec::new(), None);
            let mut batches = Vec::new();
            stream_search_results(
                &|| false,
                &mut rs,
                &mut |b| {
                    batches.push(b);
                    Ok(())
                },
            )
            .map_err(|message| IngesterError::Internal { message })?;
            Ok(batches)
        }
        .await;
        permit.finish(false);
        res
    }
}

fn create_user_stats(
    db: &UserTsdb,
    count_method: i32,
) -> Result<ingesterpb::UserStatsResponse, IngesterError> {
    let api_rate = db.ingested_api_samples.rate();
    let rule_rate = db.ingested_rule_samples.rate();
    let num_series = match ingesterpb::CountMethod::try_from(count_method) {
        Ok(ingesterpb::CountMethod::InMemory) => db.db.head().num_series(),
        Ok(ingesterpb::CountMethod::Active) => db.active_series.active().0 as u64,
        Err(_) => {
            return Err(IngesterError::Internal {
                message: format!("unknown count method {count_method}"),
            });
        }
    };
    Ok(ingesterpb::UserStatsResponse {
        ingestion_rate: api_rate + rule_rate,
        num_series,
        api_ingestion_rate: api_rate,
        rule_ingestion_rate: rule_rate,
    })
}

/// Ported from `prometheus/model/labels/sharding.go:StableHash`, the per-series shard hash
/// (`memSeries.shardHash`) used by the TSDB head `ShardedPostings`.
fn labels_stable_hash(lbls: &Labels) -> u64 {
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    for (name, value) in lbls.iter() {
        h.update(name.as_bytes());
        h.update(&[0xff]);
        h.update(value.as_bytes());
        h.update(&[0xff]);
    }
    h.digest()
}

#[allow(dead_code)]
fn _unused_btreemap_check(_: BTreeMap<String, Vec<String>>) {}

// ---------------------------------------------------------------------------
// Lookupplan + MirroredChunkQuerier TSDB Adapter
// ---------------------------------------------------------------------------

struct TsdbChunkSeriesItem {
    labels: Labels,
    chunks: Vec<ChunkMeta>,
}

impl ChunkSeries for TsdbChunkSeriesItem {
    fn labels(&self) -> Labels {
        self.labels.clone()
    }

    fn chunks(&self) -> Vec<ChunkMeta> {
        self.chunks.clone()
    }
}

struct VecChunkSeriesSet {
    items: Vec<Arc<dyn ChunkSeries>>,
    pos: usize,
    err: Option<QuerierError>,
}

impl ChunkSeriesSet for VecChunkSeriesSet {
    fn next(&mut self) -> bool {
        if self.err.is_some() || self.pos >= self.items.len() {
            return false;
        }
        self.pos += 1;
        true
    }

    fn at(&mut self) -> Option<Arc<dyn ChunkSeries>> {
        if self.pos == 0 || self.pos > self.items.len() {
            return None;
        }
        Some(Arc::clone(&self.items[self.pos - 1]))
    }

    fn err(&self) -> Option<QuerierError> {
        self.err.clone()
    }
}

pub struct PlannedTsdbChunkQuerier {
    db: Arc<Tsdb>,
    mint: i64,
    maxt: i64,
    planner: Arc<dyn LookupPlanner>,
    unordered: bool,
}

impl PlannedTsdbChunkQuerier {
    pub fn new(db: Arc<Tsdb>, mint: i64, maxt: i64, planner: Arc<dyn LookupPlanner>) -> Self {
        Self::new_with_unordered(db, mint, maxt, planner, false)
    }

    pub fn new_with_unordered(
        db: Arc<Tsdb>,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        unordered: bool,
    ) -> Self {
        Self {
            db,
            mint,
            maxt,
            planner,
            unordered,
        }
    }
}

impl ChunkQuerier for PlannedTsdbChunkQuerier {
    fn select(
        &mut self,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet> {
        let plan = if lookupplan::is_planning_disabled_in_context() {
            BasicLookupPlan::new(matchers.to_vec(), Vec::new())
        } else {
            let in_plan = BasicLookupPlan::new(matchers.to_vec(), Vec::new());
            match self.planner.plan_index_lookup(&in_plan, hints) {
                Ok(p) => p,
                Err(e) => {
                    return Box::new(VecChunkSeriesSet {
                        items: Vec::new(),
                        pos: 0,
                        err: Some(QuerierError::Other(e)),
                    });
                }
            }
        };

        let cq = if self.unordered {
            self.db.unordered_chunk_querier(self.mint, self.maxt)
        } else {
            self.db.chunk_querier(self.mint, self.maxt)
        };
        match cq.select(sort_series, &plan.index_matchers) {
            Ok(series) => {
                let items = series
                    .into_iter()
                    .filter(|cs| {
                        plan.scan_matchers
                            .iter()
                            .all(|m| m.matches(cs.labels.get(&m.name)))
                    })
                    .map(|cs| {
                        Arc::new(TsdbChunkSeriesItem {
                            labels: cs.labels,
                            chunks: cs.chunks,
                        }) as Arc<dyn ChunkSeries>
                    })
                    .collect();
                Box::new(VecChunkSeriesSet {
                    items,
                    pos: 0,
                    err: None,
                })
            }
            Err(e) => Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: Some(QuerierError::Other(e.to_string())),
            }),
        }
    }

    fn label_values(
        &mut self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        let cq = self.db.chunk_querier(self.mint, self.maxt);
        let vals = cq
            .label_values(name, None, matchers)
            .map_err(|e| QuerierError::Other(e.to_string()))?;
        Ok((vals, Vec::new()))
    }

    fn label_names(
        &mut self,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        let cq = self.db.chunk_querier(self.mint, self.maxt);
        let names = cq
            .label_names(matchers)
            .map_err(|e| QuerierError::Other(e.to_string()))?;
        Ok((names, Vec::new()))
    }

    fn close(&mut self) -> Result<(), QuerierError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// list_series (`pkg/ingester/list_series_test.go`)
// ---------------------------------------------------------------------------

pub trait SeriesPostings {
    fn next(&mut self) -> bool;
    fn at(&self) -> u64;
    fn err(&self) -> Option<String>;
}

pub trait SeriesIndexReader {
    fn series(&mut self, ref_id: u64) -> Result<Labels, String>;
}

pub struct ListPostings {
    refs: Vec<u64>,
    pos: usize,
}

impl ListPostings {
    pub fn new(refs: Vec<u64>) -> Self {
        Self { refs, pos: 0 }
    }
}

impl SeriesPostings for ListPostings {
    fn next(&mut self) -> bool {
        if self.pos >= self.refs.len() {
            return false;
        }
        self.pos += 1;
        true
    }

    fn at(&self) -> u64 {
        self.refs[self.pos - 1]
    }

    fn err(&self) -> Option<String> {
        None
    }
}

pub struct Series<P: SeriesPostings, I: SeriesIndexReader> {
    postings: P,
    idx: I,
    err: Option<String>,
}

impl<P: SeriesPostings, I: SeriesIndexReader> Series<P, I> {
    pub fn new(postings: P, idx: I) -> Self {
        Self {
            postings,
            idx,
            err: None,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.postings.next()
    }

    pub fn at(&mut self) -> Labels {
        match self.idx.series(self.postings.at()) {
            Ok(lbls) => lbls,
            Err(e) => {
                self.err = Some(format!("error getting series: {e}"));
                Labels::empty()
            }
        }
    }

    pub fn err(&self) -> Option<String> {
        if let Some(ref e) = self.err {
            if let Some(pe) = self.postings.err() {
                return Some(format!("error listing series: {e}; {pe}"));
            }
            return Some(format!("error listing series: {e}"));
        }
        self.postings.err()
    }
}

// ---------------------------------------------------------------------------
// label_names_and_values (`pkg/ingester/label_names_and_values.go`)
// ---------------------------------------------------------------------------

pub const CHECK_CONTEXT_ERROR_SERIES_COUNT: u64 = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LabelNamesAndValuesError {
    Canceled,
    PoolStopped,
    Other(String),
}

impl std::fmt::Display for LabelNamesAndValuesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Canceled => f.write_str("context canceled"),
            Self::PoolStopped => f.write_str("worker pool is stopped"),
            Self::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for LabelNamesAndValuesError {}

pub trait LabelIndexReader: Send + Sync {
    fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, LabelNamesAndValuesError>;
    fn label_values(
        &self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, LabelNamesAndValuesError>;
}

pub fn label_names_and_values_with_index<I, F, C>(
    index: &I,
    matchers: &[LabelMatcher],
    message_size_threshold: usize,
    is_canceled: C,
    mut filter: F,
) -> Result<Vec<ingesterpb::LabelNamesAndValuesResponse>, LabelNamesAndValuesError>
where
    I: LabelIndexReader,
    F: FnMut(&str, &str) -> Result<bool, LabelNamesAndValuesError>,
    C: Fn() -> bool,
{
    if is_canceled() {
        return Err(LabelNamesAndValuesError::Canceled);
    }
    let label_names = index.label_names(matchers)?;
    let mut batches = Vec::new();
    let mut response = ingesterpb::LabelNamesAndValuesResponse::default();
    let mut response_size_bytes = 0usize;

    for label_name in label_names {
        if is_canceled() {
            return Err(LabelNamesAndValuesError::Canceled);
        }
        let mut label_item = ingesterpb::LabelValues {
            label_name: label_name.clone(),
            values: Vec::new(),
        };
        response_size_bytes += label_name.len();
        if response_size_bytes >= message_size_threshold && !response.items.is_empty() {
            batches.push(std::mem::take(&mut response));
            response_size_bytes = label_name.len();
        }
        let raw_values = index.label_values(&label_name, matchers)?;
        let mut values = Vec::with_capacity(raw_values.len());
        for val in raw_values {
            if filter(&label_name, &val)? {
                values.push(val);
            }
        }

        let mut last_added_idx: isize = -1;
        for (i, val) in values.iter().enumerate() {
            response_size_bytes += val.len();
            if response_size_bytes >= message_size_threshold {
                let start = (last_added_idx + 1) as usize;
                label_item.values = values[start..=i].to_vec();
                last_added_idx = i as isize;
                response.items.push(ingesterpb::LabelValues {
                    label_name: label_item.label_name.clone(),
                    values: std::mem::take(&mut label_item.values),
                });
                batches.push(std::mem::take(&mut response));
                if i + 1 == values.len() {
                    response_size_bytes = 0;
                } else {
                    response_size_bytes = label_name.len();
                }
            } else if i + 1 == values.len() {
                let start = (last_added_idx + 1) as usize;
                label_item.values = values[start..=i].to_vec();
                response.items.push(ingesterpb::LabelValues {
                    label_name: label_item.label_name.clone(),
                    values: std::mem::take(&mut label_item.values),
                });
            }
        }
    }

    if !response.items.is_empty() {
        batches.push(response);
    }
    Ok(batches)
}

pub trait PostingsCursor: Send {
    fn next(&mut self) -> bool;
    fn at(&self) -> u64;
    fn err(&self) -> Option<LabelNamesAndValuesError>;
}

pub type PostingsForMatchersFn = Arc<
    dyn Fn(&[LabelMatcher]) -> Result<Box<dyn PostingsCursor>, LabelNamesAndValuesError>
        + Send
        + Sync,
>;

#[derive(Clone, Debug)]
pub struct LabelValueCountResult {
    pub val: String,
    pub count: u64,
    pub err: Option<LabelNamesAndValuesError>,
}

pub fn count_label_value_series(
    is_canceled: &(dyn Fn() -> bool + Send + Sync),
    lbl_name: &str,
    lbl_value: &str,
    matchers: &[LabelMatcher],
    postings_for_matchers_fn: &PostingsForMatchersFn,
) -> Result<u64, LabelNamesAndValuesError> {
    if is_canceled() {
        return Err(LabelNamesAndValuesError::Canceled);
    }
    let mut lbl_val_matchers = Vec::with_capacity(matchers.len() + 1);
    lbl_val_matchers.extend_from_slice(matchers);
    lbl_val_matchers.push(
        LabelMatcher::new(MatchType::Equal, lbl_name, lbl_value)
            .map_err(|e| LabelNamesAndValuesError::Other(e.to_string()))?,
    );

    let mut p = postings_for_matchers_fn(&lbl_val_matchers)?;
    let mut count = 0u64;
    while p.next() {
        count += 1;
        if count.is_multiple_of(CHECK_CONTEXT_ERROR_SERIES_COUNT) && is_canceled() {
            return Err(LabelNamesAndValuesError::Canceled);
        }
    }
    if let Some(e) = p.err() {
        return Err(e);
    }
    Ok(count)
}

type WorkerJob = Box<dyn FnOnce() + Send>;

/// Simple bounded compute worker pool matching `pkg/util/workerpool`.
pub struct ComputeWorkerPool {
    stopped: Arc<AtomicBool>,
    tx: Mutex<Option<mpsc::UnboundedSender<WorkerJob>>>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl ComputeWorkerPool {
    pub fn new(size: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<WorkerJob>();
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let mut handles = Vec::with_capacity(size.max(1));
        for _ in 0..size.max(1) {
            let rx_c = Arc::clone(&rx);
            handles.push(tokio::spawn(async move {
                loop {
                    let job = {
                        let mut guard = rx_c.lock().await;
                        guard.recv().await
                    };
                    let Some(job) = job else {
                        break;
                    };
                    let _ = tokio::task::spawn_blocking(job).await;
                }
            }));
        }
        Arc::new(Self {
            stopped: Arc::new(AtomicBool::new(false)),
            tx: Mutex::new(Some(tx)),
            handles: Mutex::new(handles),
        })
    }

    pub fn submit(
        &self,
        job: impl FnOnce() + Send + 'static,
    ) -> Result<(), LabelNamesAndValuesError> {
        if self.stopped.load(AtomicOrdering::Acquire) {
            return Err(LabelNamesAndValuesError::PoolStopped);
        }
        let guard = self.tx.lock();
        let Some(ref tx) = *guard else {
            return Err(LabelNamesAndValuesError::PoolStopped);
        };
        tx.send(Box::new(job))
            .map_err(|_| LabelNamesAndValuesError::PoolStopped)
    }

    pub async fn stop(&self) {
        self.stopped.store(true, AtomicOrdering::Release);
        self.tx.lock().take();
        let handles = std::mem::take(&mut *self.handles.lock());
        for h in handles {
            let _ = h.await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn compute_label_values_series_count(
    is_canceled: Arc<dyn Fn() -> bool + Send + Sync>,
    pool: Arc<ComputeWorkerPool>,
    chunk_size: usize,
    lbl_name: String,
    lbl_values: Vec<String>,
    matchers: Vec<LabelMatcher>,
    postings_for_matchers_fn: PostingsForMatchersFn,
) -> mpsc::Receiver<LabelValueCountResult> {
    let cap = lbl_values.len().max(1);
    let (tx, rx) = mpsc::channel(cap);
    if lbl_values.is_empty() {
        return rx;
    }

    tokio::spawn(async move {
        let mut wg_handles = Vec::new();
        let chunk_sz = chunk_size.max(1);
        let mut start = 0usize;
        while start < lbl_values.len() {
            if is_canceled() {
                let _ = tx
                    .send(LabelValueCountResult {
                        val: String::new(),
                        count: 0,
                        err: Some(LabelNamesAndValuesError::Canceled),
                    })
                    .await;
                break;
            }
            let end = (start + chunk_sz).min(lbl_values.len());
            let chunk_vals = lbl_values[start..end].to_vec();
            let cancel_c = Arc::clone(&is_canceled);
            let lbl_c = lbl_name.clone();
            let matchers_c = matchers.clone();
            let fn_c = Arc::clone(&postings_for_matchers_fn);
            let tx_c = tx.clone();
            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

            if let Err(e) = pool.submit(move || {
                for v in chunk_vals {
                    match count_label_value_series(
                        cancel_c.as_ref(),
                        &lbl_c,
                        &v,
                        &matchers_c,
                        &fn_c,
                    ) {
                        Ok(count) => {
                            let _ = tx_c.try_send(LabelValueCountResult {
                                val: v,
                                count,
                                err: None,
                            });
                        }
                        Err(err) => {
                            let _ = tx_c.try_send(LabelValueCountResult {
                                val: v,
                                count: 0,
                                err: Some(err),
                            });
                        }
                    }
                }
                let _ = done_tx.send(());
            }) {
                let _ = tx
                    .send(LabelValueCountResult {
                        val: String::new(),
                        count: 0,
                        err: Some(e),
                    })
                    .await;
                break;
            }
            wg_handles.push(done_rx);
            start = end;
        }
        for h in wg_handles {
            let _ = h.await;
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency_offsets_v1_and_v2_and_sharding_parse() {
        let off1 = lookup_encoded_consistency_offsets("v1=0:42,1:99", 0, 1).unwrap();
        assert_eq!(off1.num_kafka_clusters(), 1);
        assert_eq!(off1.for_kafka_cluster(0), 99);
        assert!(lookup_encoded_consistency_offsets("v1=0:42", 1, 0).is_none());

        let off2 = lookup_encoded_consistency_offsets("v2=0/2:10;20,1/2:30;40", 1, 2).unwrap();
        assert_eq!(off2.num_kafka_clusters(), 2);
        assert_eq!(off2.for_kafka_cluster(0), 30);
        assert_eq!(off2.for_kafka_cluster(1), 40);

        let matchers = vec![
            LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap(),
            LabelMatcher::new(MatchType::Equal, "__query_shard__", "2_of_4").unwrap(),
        ];
        let (shard, filtered) = remove_shard_from_matchers(matchers).unwrap();
        assert_eq!(shard, Some((1, 4)));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "__name__");
    }

    #[tokio::test]
    async fn bimodal_query_stream_matches_inline_and_channel_paths() {
        use tokio_stream::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ingest_cfg = mimir_config::IngestStorageConfig::default();
        let overrides = mimir_config::Overrides::new(mimir_config::LimitsConfig::default());

        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            ingest_cfg,
            overrides,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        // Push 150 series so we can test 0 (missing), 7 (inline fast path), and 150 (channel streaming path).
        let mut ts_list = Vec::with_capacity(150);
        for i in 0..150 {
            let group = if i < 7 { "small" } else { "large" };
            ts_list.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "bimodal_metric"),
                    cortexpb::LabelPair::from_strs("group", group),
                    cortexpb::LabelPair::from_strs("id", format!("{i:03}")),
                ],
                samples: vec![cortexpb::Sample {
                    value: i as f64,
                    timestamp_ms: 1_000,
                }],
                ..Default::default()
            });
        }
        let wr = cortexpb::WriteRequest {
            timeseries: ts_list,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        ing.push_to_storage("user-a", &wr).unwrap();

        let md = MetadataMap::new();
        // 1) 0 matching series
        let req0 = ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 2_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "missing_metric".into(),
            }],
            ..Default::default()
        };
        let mut s0 = ing
            .handle_query_stream_bimodal(&md, "user-a", req0.clone())
            .await
            .unwrap();
        let mut res0 = Vec::new();
        while let Some(item) = s0.next().await {
            res0.push(item.unwrap());
        }
        assert_eq!(res0.len(), 1);
        assert!(res0[0].is_end_of_series_stream);
        assert!(res0[0].streaming_series.is_empty());

        // 2) 7 matching series (inline fast path <= 128)
        let req7 = ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 2_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "group".into(),
                value: "small".into(),
            }],
            ..Default::default()
        };
        let expected7 = ing.handle_query_stream(&md, "user-a", req7.clone()).await.unwrap();
        let mut s7 = ing
            .handle_query_stream_bimodal(&md, "user-a", req7)
            .await
            .unwrap();
        let mut got7 = Vec::new();
        while let Some(item) = s7.next().await {
            got7.push(item.unwrap());
        }
        assert_eq!(got7.len(), 2);
        assert_eq!(got7, expected7);

        // 3) 150 matching series (channel streaming path > 128)
        let req150 = ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 2_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "bimodal_metric".into(),
            }],
            ..Default::default()
        };
        let expected150 = ing
            .handle_query_stream(&md, "user-a", req150.clone())
            .await
            .unwrap();
        let mut s150 = ing
            .handle_query_stream_bimodal(&md, "user-a", req150)
            .await
            .unwrap();
        let mut got150 = Vec::new();
        while let Some(item) = s150.next().await {
            got150.push(item.unwrap());
        }
        assert_eq!(got150, expected150);
    }
}
