//! Read RPC implementations, read admission, and ingest_storage read consistency.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_query.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_search.go
//! - github.com/grafana/mimir/pkg/ingester/label_names_and_values.go
//! - github.com/grafana/mimir/pkg/ingester/active_series.go
//! - github.com/grafana/mimir/pkg/ingester/stats.go
//! - github.com/grafana/mimir/pkg/querier/api/consistency.go

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mimir_common::limiter::ReactivePermit;
use mimir_config::parse_go_duration;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_proto::query_stream_enc::{
    encode_metrics_for_label_matchers_response, encoded_series_chunks_len, QueryStreamBatchBuilder,
    RawChunk,
};
use mimir_storage::ingest::partition_offset_watcher::OffsetWatcherError;
use mimir_storage::ingest::reader::ReaderError;
use mimir_storage::ingest::PartitionOffsets;
use mimir_tsdb::block::{Block, BlockReadGuard};
use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::compact::{
    compact_chunk_series_merge, merge_chunks_with_tombstones_and_float_encoding,
    merge_head_chunks_with_tombstones, reencode_chunk_without_tombstones,
};
use mimir_tsdb::db::Tsdb;
use mimir_tsdb::head::{HeadReadGuard, HeadReadIsolation};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::{
    BlockSelectStats, BlockSelectStatsSink, HeadQuerySnapshot, LazyStreamEvent, QueryChunkSeries,
    SeriesStatsFilter,
};
use mimir_tsdb::search::{FuzzAlg, SearchHints, SearchOrdering, SearchResult};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::circuit_breaker::BreakerPermit;
use crate::errors::IngesterError;
use crate::ingester::Ingester;
use crate::lookupplan::{self, BasicLookupPlan, LookupPlanner, QueryStats, SelectHints};
use crate::push::ADAPTIVE_LIMITER_EXCEEDED_MSG;
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
pub type RawQueryStreamBox =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, Status>> + Send + 'static>>;
pub const LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES: usize = 1024 * 1024;
pub const LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES: usize = 1024 * 1024;
pub const ACTIVE_SERIES_MAX_SIZE_BYTES: usize = 1024 * 1024;
pub const SEARCH_BATCH_SIZE: usize = 256;

pub(crate) const ONLY_REPLICA_GRPC_MD_KEY: &str = "__only_replica__";
const X_ONLY_REPLICA_MD_KEY: &str = "x-only-replica";
const CONSISTENCY_LEVEL_MD_KEY: &str = "__consistency_level__";
const CONSISTENCY_OFFSETS_MD_KEY: &str = "__consistency_offsets__";
const CONSISTENCY_MAX_DELAY_MD_KEY: &str = "__consistency_max_delay__";

pub const READ_CONSISTENCY_STRONG: &str = "strong";
pub const READ_CONSISTENCY_EVENTUAL: &str = "eventual";

tokio::task_local! {
    pub(crate) static READ_TAP_ADMITTED: bool;
}

pub struct ReadTapPermit {
    started_at: Instant,
    breaker_permit: Option<BreakerPermit>,
    reactive_permit: Option<ReactivePermit>,
    deadline_exceeded: Arc<AtomicBool>,
    prepared: bool,
    finished: bool,
}

impl ReadTapPermit {
    pub fn set_reactive_permit(&mut self, rp: ReactivePermit) {
        self.reactive_permit = Some(rp);
        self.prepared = true;
    }

    pub fn mark_prepared(&mut self) {
        self.prepared = true;
    }

    pub fn is_prepared(&self) -> bool {
        self.prepared
    }

    pub fn deadline_exceeded_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.deadline_exceeded)
    }

    pub fn abort(mut self) {
        self.abort_inner();
    }

    fn abort_inner(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        drop(self.breaker_permit.take());
        if let Some(rp) = self.reactive_permit.take() {
            rp.drop_permit();
        }
    }

    pub fn finish(mut self, canceled: bool) {
        self.finish_inner(canceled);
    }

    fn finish_inner(&mut self, canceled: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let dur = self.started_at.elapsed();
        let deadline = self.deadline_exceeded.load(AtomicOrdering::Relaxed);
        if let Some(bp) = self.breaker_permit.take() {
            bp.finish(dur, deadline);
        }
        if let Some(rp) = self.reactive_permit.take() {
            if canceled {
                rp.drop_permit();
            } else {
                rp.record();
            }
        }
    }
}

impl Drop for ReadTapPermit {
    fn drop(&mut self) {
        if self.prepared {
            self.finish_inner(true);
        } else {
            self.abort_inner();
        }
    }
}

pub struct ReadPermit {
    started_at: Instant,
    breaker_permit: Option<BreakerPermit>,
    reactive_permit: Option<ReactivePermit>,
}

impl ReadPermit {
    pub fn noop() -> Self {
        Self {
            started_at: Instant::now(),
            breaker_permit: None,
            reactive_permit: None,
        }
    }

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

impl Drop for ReadPermit {
    fn drop(&mut self) {
        // Go's read interceptor always calls `finish(err)`: an abandoned read (e.g. the handler
        // dropped because the client reset the stream or the gRPC deadline expired, where Go
        // sees `context.Canceled`) is still recorded by the circuit breaker.
        if let Some(bp) = self.breaker_permit.take() {
            bp.finish(self.started_at.elapsed(), false);
        }
        if let Some(rp) = self.reactive_permit.take() {
            rp.drop_permit();
        }
    }
}

/// Maps the reader's strong read consistency wait error to the Go error: the configured
/// `-ingest-storage.kafka.wait-strong-read-consistency-timeout` expiring is
/// `errWaitStrongReadConsistencyTimeoutExceeded` (wrapping `context.DeadlineExceeded`, so
/// `codes.DeadlineExceeded` and a read circuit breaker timeout). The request's own deadline
/// cancels the wait by dropping it (grpc-go resets the stream and cancels the handler context).
/// Go checks `errors.Is(err, context.DeadlineExceeded)`, which also holds for the
/// multi-cluster reader's `write compartment N: ...` wrapped / multierror results
/// ([`ReaderError::is_deadline_exceeded`]).
fn map_strong_read_consistency_wait_err(e: ReaderError) -> IngesterError {
    if !e.is_deadline_exceeded() {
        return IngesterError::Internal {
            message: format!("wait for read consistency: {e}"),
        };
    }
    let message = match e {
        // The caller's deadline expired first: Go's wait returns the context cause
        // `context.DeadlineExceeded` itself.
        ReaderError::Watcher(OffsetWatcherError::Timeout(_)) => {
            "wait for read consistency: context deadline exceeded".to_string()
        }
        e => format!("wait for read consistency: {e}"),
    };
    IngesterError::DeadlineExceeded { message }
}

/// Counts only the series of the requested query shard in the per-block select stats (Go
/// applies the shard inside the block querier).
fn shard_stats_filter(shard: Option<(u64, u64)>) -> Option<SeriesStatsFilter> {
    shard.map(|(idx, count)| {
        Arc::new(move |l: &Labels| labels_stable_hash(l) % count == idx) as SeriesStatsFilter
    })
}

/// Go `isDeadlineExceeded(err)` for the read circuit breaker's `finish`.
fn deadline_exceeded<T>(res: &Result<T, IngesterError>) -> bool {
    res.as_ref().err().is_some_and(IngesterError::is_deadline_exceeded)
}

pub fn is_only_replica_metadata(md: &MetadataMap) -> bool {
    let check = |k: &str| {
        md.get(k)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("true"))
    };
    check(ONLY_REPLICA_GRPC_MD_KEY) || check(X_ONLY_REPLICA_MD_KEY)
}

pub fn is_only_replica_headers(headers: &axum::http::HeaderMap) -> bool {
    let check = |k: &str| {
        headers
            .get(k)
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
            if parts.len() != 3
                || parts[1] != "of"
                || parts[0].starts_with('+')
                || parts[2].starts_with('+')
            {
                return Err(IngesterError::BadRequest {
                    message: format!("invalid shard ID: {:?}", m.value),
                });
            }
            let one_based = parts[0].parse::<u64>().map_err(|e| IngesterError::BadRequest {
                message: format!("invalid shard ID: {:?}: {e}", m.value),
            })?;
            let count = parts[2].parse::<u64>().map_err(|e| IngesterError::BadRequest {
                message: format!("invalid shard ID: {:?}: {e}", m.value),
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

/// Go `client.FromLabelMatchers`. Its errors are plain errors, which the read RPCs return as
/// `codes.Internal` (`mapReadErrorToErrorWithStatus`) with the Go text: `invalid matcher type` or
/// the Prometheus regex error (`error parsing regexp: ...`).
#[derive(Clone, Default)]
pub(crate) struct ReadErrorSink {
    pub(crate) log: Option<mimir_common::grpc_log::GrpcErrorLog>,
    pub(crate) deadline_flag: Option<Arc<AtomicBool>>,
}

impl ReadErrorSink {
    pub(crate) fn from_extensions(ext: &axum::http::Extensions) -> Self {
        Self {
            log: ext.get::<mimir_common::grpc_log::GrpcErrorLog>().cloned(),
            deadline_flag: ext
                .get::<crate::grpc::TapDeadlineFlag>()
                .map(|f| Arc::clone(&f.0)),
        }
    }

    pub(crate) fn map_read_error(&self, e: IngesterError) -> Status {
        if let Some(ref log) = self.log {
            log.set(mimir_common::grpc_log::GrpcErrorLogEntry {
                err: e.to_string(),
                keep: true,
                reason: None,
            });
            if e.is_deadline_exceeded() {
                log.set_instrumentation_code(tonic::Code::Unknown as i32);
            }
        }
        if e.is_deadline_exceeded() {
            if let Some(ref flag) = self.deadline_flag {
                flag.store(true, AtomicOrdering::Relaxed);
            }
        }
        e.map_read_error_to_status()
    }
}

pub(crate) fn validate_label_values_matchers(
    req: &ingesterpb::LabelValuesRequest,
) -> Result<Vec<LabelMatcher>, IngesterError> {
    match req.matchers {
        Some(ref m) => from_proto_matchers(&m.matchers),
        None => Ok(Vec::new()),
    }
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
                return Err(IngesterError::Internal {
                    message: "invalid matcher type".to_string(),
                });
            }
        };
        let lm = LabelMatcher::new(mt, &m.name, &m.value).map_err(|e| IngesterError::Internal {
            message: e.to_string(),
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
    // Go `SearchLabel{Names,Values}` return every buildSearchHints error as InvalidArgument.
    let matchers = from_proto_matchers(wire_matchers).map_err(|e| IngesterError::BadRequest {
        message: e.to_string(),
    })?;
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
    fn get_open_tsdb_for_query(
        &self,
        user_id: &str,
    ) -> Result<Option<Arc<crate::user_tsdb::UserTsdb>>, IngesterError> {
        Ok(self.get_tsdb(user_id))
    }
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

    /// Non-blocking gRPC `TapHandle` admission (`Ingester.StartReadRequest` + `grpc_call_check.go`).
    pub fn try_start_read_tap(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<ReadTapPermit, IngesterError> {
        self.check_available_for_read()?;
        self.check_read_overloaded()?;
        let started_at = Instant::now();
        let only_replica = is_only_replica_headers(headers);
        let breaker_permit = if !only_replica {
            if let Some(ref rl) = self.reactive_limiter.read {
                if !rl.can_acquire_permit() {
                    self.metrics
                        .rejected
                        .with_label_values(&[REASON_MAX_INFLIGHT_READ_REQUESTS])
                        .inc();
                    return Err(IngesterError::ReactiveLimiterExceeded {
                        message: ADAPTIVE_LIMITER_EXCEEDED_MSG.to_string(),
                    });
                }
            }
            self.circuit_breaker.try_acquire_read_permit()?
        } else {
            None
        };
        Ok(ReadTapPermit {
            started_at,
            breaker_permit,
            reactive_permit: None,
            deadline_exceeded: Arc::new(AtomicBool::new(false)),
            prepared: false,
            finished: false,
        })
    }

    /// Blocking reactive limiter permit acquisition (`Ingester.PrepareReadRequest`).
    pub async fn prepare_read_reactive(
        &self,
        headers: &axum::http::HeaderMap,
        deadline_ctx: Option<&crate::grpc::RpcDeadlineContext>,
    ) -> Result<Option<ReactivePermit>, IngesterError> {
        if is_only_replica_headers(headers) {
            return Ok(None);
        }
        let Some(ref rl) = self.reactive_limiter.read else {
            return Ok(None);
        };
        let res = match deadline_ctx {
            Some(ctx) if ctx.has_timed_out() => {
                ctx.mark_expired();
                Err("context deadline exceeded".to_string())
            }
            Some(ctx) => {
                if let Some(deadline) = ctx.deadline {
                    tokio::select! {
                        r = rl.acquire_permit() => r.map_err(|e| e.to_string()),
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                            ctx.mark_expired();
                            Err("context deadline exceeded".to_string())
                        }
                    }
                } else {
                    rl.acquire_permit().await.map_err(|e| e.to_string())
                }
            }
            None => rl.acquire_permit().await.map_err(|e| e.to_string()),
        };
        match res {
            Ok(p) => Ok(Some(p)),
            Err(message) => {
                self.metrics
                    .rejected
                    .with_label_values(&[REASON_MAX_INFLIGHT_READ_REQUESTS])
                    .inc();
                Err(IngesterError::ReactiveLimiterExceeded { message })
            }
        }
    }

    pub async fn start_read_request(&self, md: &MetadataMap) -> Result<ReadPermit, IngesterError> {
        if READ_TAP_ADMITTED.try_with(|v| *v).unwrap_or(false) {
            return Ok(ReadPermit::noop());
        }
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
                        message: ADAPTIVE_LIMITER_EXCEEDED_MSG.to_string(),
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
            if crate::grpc::current_rpc_deadline_context().is_some_and(|ctx| ctx.has_timed_out()) {
                return Err(IngesterError::DeadlineExceeded {
                    message: "wait for read consistency: context deadline exceeded".to_string(),
                });
            }
            let configured_timeout = Duration::from_nanos(
                self.ingest_cfg
                    .kafka
                    .wait_strong_read_consistency_timeout
                    .as_nanos()
                    .max(0) as u64,
            );
            let timeout = crate::grpc::effective_read_consistency_timeout(configured_timeout);
            if timeout.is_zero()
                && crate::grpc::current_rpc_deadline_context()
                    .is_some_and(|ctx| ctx.deadline.is_some())
            {
                return Err(IngesterError::DeadlineExceeded {
                    message: "wait for read consistency: context deadline exceeded".to_string(),
                });
            }
            let map_wait_err = map_strong_read_consistency_wait_err;
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
                        .map_err(map_wait_err)?;
                    return Ok(());
                }
            }

            reader
                .wait_read_consistency_until_last_produced_offset(timeout)
                .await
                .map_err(map_wait_err)?;
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
        permit.finish(deadline_exceeded(&res));
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
        req: ingesterpb::QueryRequest,
    ) -> Result<QueryStreamBox, IngesterError> {
        self.handle_query_stream_bimodal_with_sink(md, user_id, req, ReadErrorSink::default())
            .await
    }

    pub(crate) async fn handle_query_stream_bimodal_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        mut req: ingesterpb::QueryRequest,
        err_sink: ReadErrorSink,
    ) -> Result<QueryStreamBox, IngesterError> {
        let permit = self.start_read_request(md).await?;
        if req.streaming_chunks_batch_size == 0 {
            req.streaming_chunks_batch_size = FALLBACK_CHUNK_STREAM_BATCH_SIZE;
        }
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;

        self.metrics.queries.inc();
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }

        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                return Ok(Box::pin(tokio_stream::empty()));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };

        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let batch_limit = (req.streaming_chunks_batch_size.min(isize::MAX as u64)).max(1) as usize;
        let mut chunk_series = if !self
            .blocks_cfg
            .tsdb
            .index_lookup_planning
            .index_lookup_planning_enabled
            && self
                .blocks_cfg
                .tsdb
                .index_lookup_planning
                .index_lookup_planning_comparison_portion
                <= 0.0
        {
            const INLINE_CHUNK_LOAD_SERIES_LIMIT: usize = 256;
            const INLINE_CHUNK_LOAD_REFS_LIMIT: usize = 512;

            let ooo_window = self
                .overrides
                .for_tenant(user_id)
                .out_of_order_time_window
                .0;
            let unordered = ooo_window > 0;
            let select_stats = BlockSelectStatsSink::default();
            let cq = if unordered {
                db.db.unordered_chunk_querier(start_ts, end_ts)
            } else {
                db.db.chunk_querier(start_ts, end_ts)
            }
            .with_concurrent(shard.is_some())
            .with_filtered_select_stats(Arc::clone(&select_stats), shard_stats_filter(shard));
            match cq
                .try_select_adaptive(
                    true,
                    &matchers,
                    INLINE_CHUNK_LOAD_SERIES_LIMIT,
                    INLINE_CHUNK_LOAD_REFS_LIMIT,
                )
                .map_err(|e| IngesterError::Internal {
                    message: format!("selecting series from ChunkQuerier: {e}"),
                })? {
                Some(Ok(mut inline_series)) => {
                    self.observe_block_select_stats(
                        user_id,
                        &db,
                        &select_stats,
                        shard.is_some(),
                        &matchers,
                    );
                    if let Some((shard_idx, shard_count)) = shard {
                        inline_series
                            .retain(|cs| labels_stable_hash(&cs.labels) % shard_count == shard_idx);
                    }
                    inline_series
                }
                maybe_prepared => {
                    let prepared_opt = match maybe_prepared {
                        Some(Err(p)) => Some(p),
                        None => None,
                        Some(Ok(_)) => unreachable!(),
                    };
                    // Large or lock-contended query: stream lazily in O(1) chunk memory on a
                    // blocking worker using a bounded mpsc::channel(4).
                    let (tx, rx) = mpsc::channel(4);
                    let ingester = Arc::clone(self);
                    let db_c = Arc::clone(&db);
                    let user_c = user_id.to_owned();
                    let err_sink = err_sink.clone();
                    tokio::task::spawn_blocking(move || {
                        let cq_b = if unordered {
                            db_c.db.unordered_chunk_querier(start_ts, end_ts)
                        } else {
                            db_c.db.chunk_querier(start_ts, end_ts)
                        }
                        .with_concurrent(shard.is_some())
                        .with_filtered_select_stats(
                            Arc::clone(&select_stats),
                            shard_stats_filter(shard),
                        );
                        let prepared = match prepared_opt {
                            Some(p) => p,
                            None => match cq_b.prepare_select(&matchers) {
                                Ok(p) => p,
                                Err(e) => {
                                    let err = IngesterError::Internal {
                                        message: format!("selecting series from ChunkQuerier: {e}"),
                                    };
                                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                                    permit.finish(false);
                                    return;
                                }
                            },
                        };
                        let shard_pred = shard.map(|(shard_idx, shard_count)| {
                            move |lbls: &Labels| labels_stable_hash(lbls) % shard_count == shard_idx
                        });
                        let filter_ref: Option<&dyn Fn(&Labels) -> bool> =
                            shard_pred.as_ref().map(|f| f as &dyn Fn(&Labels) -> bool);

                        let mut num_series = 0usize;
                        let mut num_samples = 0usize;
                        let mut series_batch: Vec<ingesterpb::QueryStreamSeries> =
                            Vec::with_capacity(QUERY_STREAM_BATCH_SIZE);
                        let mut chunks_batch: Vec<ingesterpb::QueryStreamSeriesChunks> =
                            Vec::with_capacity(batch_limit.min(1024));
                        let mut batch_bytes = 0usize;
                        let mut receiver_closed = false;

                        let res = cq_b.stream_prepared_chunks_lazy(
                            prepared,
                            &matchers,
                            true,
                            filter_ref,
                            |ev| match ev {
                                LazyStreamEvent::Series(labels, chunk_count) => {
                                    num_series += 1;
                                    series_batch.push(ingesterpb::QueryStreamSeries {
                                        labels: labels_to_proto_pairs(labels),
                                        chunk_count: chunk_count as i64,
                                    });
                                    if series_batch.len() >= QUERY_STREAM_BATCH_SIZE {
                                        let resp = ingesterpb::QueryStreamResponse {
                                            streaming_series: std::mem::take(&mut series_batch),
                                            is_end_of_series_stream: false,
                                            streaming_series_chunks: Vec::new(),
                                        };
                                        if tx.blocking_send(Ok(resp)).is_err() {
                                            receiver_closed = true;
                                            return Ok(false);
                                        }
                                    }
                                    Ok(true)
                                }
                                LazyStreamEvent::SeriesEnd => {
                                    let resp = ingesterpb::QueryStreamResponse {
                                        streaming_series: std::mem::take(&mut series_batch),
                                        is_end_of_series_stream: true,
                                        streaming_series_chunks: Vec::new(),
                                    };
                                    if tx.blocking_send(Ok(resp)).is_err() {
                                        receiver_closed = true;
                                        return Ok(false);
                                    }
                                    Ok(true)
                                }
                                LazyStreamEvent::SeriesChunks(series_idx, chks) => {
                                    let mut proto_chunks = Vec::with_capacity(chks.len());
                                    for cm in chks {
                                        let Some(enc_chunk) = cm.chunk else {
                                            return Err(mimir_tsdb::querier::QuerierError::Chunk(
                                                mimir_tsdb::chunkenc::ChunkError::InvalidEncoding(0),
                                            ));
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

                                    if (batch_bytes > 0
                                        && batch_bytes + msg_size > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                                        || chunks_batch.len() >= batch_limit
                                    {
                                        let resp = ingesterpb::QueryStreamResponse {
                                            streaming_series: Vec::new(),
                                            is_end_of_series_stream: false,
                                            streaming_series_chunks: std::mem::take(
                                                &mut chunks_batch,
                                            ),
                                        };
                                        if tx.blocking_send(Ok(resp)).is_err() {
                                            receiver_closed = true;
                                            return Ok(false);
                                        }
                                        batch_bytes = 0;
                                    }
                                    chunks_batch.push(sc);
                                    batch_bytes += msg_size;
                                    Ok(true)
                                }
                            },
                        );
                        ingester.observe_block_select_stats(
                            &user_c,
                            &db_c,
                            &select_stats,
                            shard.is_some(),
                            &matchers,
                        );

                        match res {
                            Ok(()) => {
                                if !receiver_closed {
                                    if !chunks_batch.is_empty() {
                                        let _ = tx.blocking_send(Ok(ingesterpb::QueryStreamResponse {
                                            streaming_series: Vec::new(),
                                            is_end_of_series_stream: false,
                                            streaming_series_chunks: chunks_batch,
                                        }));
                                    }
                                    ingester
                                        .metrics
                                        .queried_series
                                        .with_label_values(&["merged_blocks"])
                                        .observe(num_series as f64);
                                    ingester.metrics.queried_samples.observe(num_samples as f64);
                                }
                            }
                            Err(e) => {
                                let err = IngesterError::Internal {
                                    message: format!("selecting series from ChunkQuerier: {e}"),
                                };
                                let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                            }
                        }
                        permit.finish(false);
                    });

                    return Ok(Box::pin(ReceiverStream::new(rx)));
                }
            }
        } else {
            let ing_c = Arc::clone(self);
            let db_c = Arc::clone(&db);
            let uid_c = user_id.to_string();
            tokio::task::spawn_blocking(move || {
                ing_c.select_chunk_series_for_query(
                    &db_c, &uid_c, start_ts, end_ts, shard, &matchers,
                )
            })
            .await
            .map_err(|e| IngesterError::Internal {
                message: format!("query task join error: {e}"),
            })??
        };

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
                let _ = tx.send(Err(err_sink.map_read_error(e))).await;
            }
            permit.finish(false);
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    /// Zero-alloc wire-encoding `QueryStream` implementation returning raw serialized
    /// `QueryStreamResponse` protobuf `Bytes` batches (served via `RawQueryStreamCodec` in `grpc.rs`).
    ///
    /// Eliminates per-series `Vec<cortexpb::LabelPair>`, per-label `Bytes::slice` atomic refcount
    /// ops, per-chunk `Vec<ingesterpb::Chunk>`, and `prost` double-pass encoding.
    pub async fn handle_query_stream_raw(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::QueryRequest,
    ) -> Result<RawQueryStreamBox, IngesterError> {
        self.handle_query_stream_raw_with_sink(md, user_id, req, ReadErrorSink::default())
            .await
    }

    pub(crate) async fn handle_query_stream_raw_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        mut req: ingesterpb::QueryRequest,
        err_sink: ReadErrorSink,
    ) -> Result<RawQueryStreamBox, IngesterError> {
        let permit = self.start_read_request(md).await?;
        if req.streaming_chunks_batch_size == 0 {
            req.streaming_chunks_batch_size = FALLBACK_CHUNK_STREAM_BATCH_SIZE;
        }
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;

        self.metrics.queries.inc();
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }

        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                return Ok(Box::pin(tokio_stream::empty()));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };

        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let batch_limit = (req.streaming_chunks_batch_size.min(isize::MAX as u64)).max(1) as usize;
        let chunk_series = if !self
            .blocks_cfg
            .tsdb
            .index_lookup_planning
            .index_lookup_planning_enabled
            && self
                .blocks_cfg
                .tsdb
                .index_lookup_planning
                .index_lookup_planning_comparison_portion
                <= 0.0
        {
            const INLINE_CHUNK_LOAD_SERIES_LIMIT: usize = 256;
            const INLINE_CHUNK_LOAD_REFS_LIMIT: usize = 512;

            let ooo_window = self
                .overrides
                .for_tenant(user_id)
                .out_of_order_time_window
                .0;
            let unordered = ooo_window > 0;
            let select_stats = BlockSelectStatsSink::default();
            let cq = if unordered {
                db.db.unordered_chunk_querier(start_ts, end_ts)
            } else {
                db.db.chunk_querier(start_ts, end_ts)
            }
            .with_concurrent(shard.is_some())
            .with_filtered_select_stats(Arc::clone(&select_stats), shard_stats_filter(shard));
            match cq
                .try_select_adaptive(
                    true,
                    &matchers,
                    INLINE_CHUNK_LOAD_SERIES_LIMIT,
                    INLINE_CHUNK_LOAD_REFS_LIMIT,
                )
                .map_err(|e| IngesterError::Internal {
                    message: format!("selecting series from ChunkQuerier: {e}"),
                })? {
                Some(Ok(mut inline_series)) => {
                    self.observe_block_select_stats(
                        user_id,
                        &db,
                        &select_stats,
                        shard.is_some(),
                        &matchers,
                    );
                    if let Some((shard_idx, shard_count)) = shard {
                        inline_series
                            .retain(|cs| labels_stable_hash(&cs.labels) % shard_count == shard_idx);
                    }
                    inline_series
                }
                maybe_prepared => {
                    let prepared_opt = match maybe_prepared {
                        Some(Err(p)) => Some(p),
                        None => None,
                        Some(Ok(_)) => unreachable!(),
                    };
                    let (tx, rx) = mpsc::channel(4);
                    let ingester = Arc::clone(self);
                    let db_c = Arc::clone(&db);
                    let user_c = user_id.to_owned();
                    let err_sink = err_sink.clone();
                    tokio::task::spawn_blocking(move || {
                        let cq_b = if unordered {
                            db_c.db.unordered_chunk_querier(start_ts, end_ts)
                        } else {
                            db_c.db.chunk_querier(start_ts, end_ts)
                        }
                        .with_concurrent(shard.is_some())
                        .with_filtered_select_stats(
                            Arc::clone(&select_stats),
                            shard_stats_filter(shard),
                        );
                        let prepared = match prepared_opt {
                            Some(p) => p,
                            None => match cq_b.prepare_select(&matchers) {
                                Ok(p) => p,
                                Err(e) => {
                                    let err = IngesterError::Internal {
                                        message: format!("selecting series from ChunkQuerier: {e}"),
                                    };
                                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                                    permit.finish(false);
                                    return;
                                }
                            },
                        };
                        let shard_pred = shard.map(|(shard_idx, shard_count)| {
                            move |lbls: &Labels| labels_stable_hash(lbls) % shard_count == shard_idx
                        });
                        let filter_ref: Option<&dyn Fn(&Labels) -> bool> =
                            shard_pred.as_ref().map(|f| f as &dyn Fn(&Labels) -> bool);

                        let mut num_series = 0usize;
                        let mut num_samples = 0usize;
                        let mut builder = QueryStreamBatchBuilder::with_capacity(64 * 1024);
                        let mut series_in_batch = 0usize;
                        let mut chunks_in_batch = 0usize;
                        let mut receiver_closed = false;

                        let res = cq_b.stream_prepared_chunks_lazy(
                            prepared,
                            &matchers,
                            true,
                            filter_ref,
                            |ev| match ev {
                                LazyStreamEvent::Series(labels, chunk_count) => {
                                    num_series += 1;
                                    if builder
                                        .push_series_stringlabels(
                                            labels.as_bytes(),
                                            chunk_count as i64,
                                        )
                                        .is_err()
                                    {
                                        return Err(mimir_tsdb::querier::QuerierError::Chunk(
                                            mimir_tsdb::chunkenc::ChunkError::InvalidEncoding(0),
                                        ));
                                    }
                                    series_in_batch += 1;
                                    if series_in_batch >= QUERY_STREAM_BATCH_SIZE {
                                        let bytes = builder.finish();
                                        series_in_batch = 0;
                                        if tx.blocking_send(Ok(bytes)).is_err() {
                                            receiver_closed = true;
                                            return Ok(false);
                                        }
                                    }
                                    Ok(true)
                                }
                                LazyStreamEvent::SeriesEnd => {
                                    builder.set_end_of_series_stream();
                                    let bytes = builder.finish();
                                    series_in_batch = 0;
                                    if tx.blocking_send(Ok(bytes)).is_err() {
                                        receiver_closed = true;
                                        return Ok(false);
                                    }
                                    Ok(true)
                                }
                                LazyStreamEvent::SeriesChunks(series_idx, chks) => {
                                    let mut raw_chunks: smallvec::SmallVec<[RawChunk<'_>; 4]> =
                                        smallvec::SmallVec::with_capacity(chks.len());
                                    for cm in &chks {
                                        let Some(ref enc_chunk) = cm.chunk else {
                                            return Err(mimir_tsdb::querier::QuerierError::Chunk(
                                                mimir_tsdb::chunkenc::ChunkError::InvalidEncoding(0),
                                            ));
                                        };
                                        num_samples += enc_chunk.num_samples();
                                        raw_chunks.push(RawChunk {
                                            encoding: tsdb_encoding_to_wire(enc_chunk.encoding()),
                                            min_timestamp_ms: cm.min_time,
                                            max_timestamp_ms: cm.max_time,
                                            data: enc_chunk.as_bytes(),
                                        });
                                    }
                                    let entry_len =
                                        encoded_series_chunks_len(series_idx as u64, &raw_chunks);
                                    if (chunks_in_batch > 0
                                        && builder.len() + entry_len
                                            > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                                        || chunks_in_batch >= batch_limit
                                    {
                                        let bytes = builder.finish();
                                        chunks_in_batch = 0;
                                        if tx.blocking_send(Ok(bytes)).is_err() {
                                            receiver_closed = true;
                                            return Ok(false);
                                        }
                                    }
                                    builder.push_series_chunks(series_idx as u64, &raw_chunks);
                                    chunks_in_batch += 1;
                                    Ok(true)
                                }
                            },
                        );
                        ingester.observe_block_select_stats(
                            &user_c,
                            &db_c,
                            &select_stats,
                            shard.is_some(),
                            &matchers,
                        );

                        match res {
                            Ok(()) => {
                                if !receiver_closed {
                                    if !builder.is_empty() {
                                        let _ = tx.blocking_send(Ok(builder.finish()));
                                    }
                                    ingester
                                        .metrics
                                        .queried_series
                                        .with_label_values(&["merged_blocks"])
                                        .observe(num_series as f64);
                                    ingester.metrics.queried_samples.observe(num_samples as f64);
                                }
                            }
                            Err(e) => {
                                let err = IngesterError::Internal {
                                    message: format!("selecting series from ChunkQuerier: {e}"),
                                };
                                let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                            }
                        }
                        permit.finish(false);
                    });

                    return Ok(Box::pin(ReceiverStream::new(rx)));
                }
            }
        } else {
            let ing_c = Arc::clone(self);
            let db_c = Arc::clone(&db);
            let uid_c = user_id.to_string();
            tokio::task::spawn_blocking(move || {
                ing_c.select_chunk_series_for_query(
                    &db_c, &uid_c, start_ts, end_ts, shard, &matchers,
                )
            })
            .await
            .map_err(|e| IngesterError::Internal {
                message: format!("query task join error: {e}"),
            })??
        };

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
            let mut builder = QueryStreamBatchBuilder::with_capacity(
                (est_chunk_bytes + num_series * 128).max(256),
            );
            if num_series == 0 {
                self.metrics
                    .queried_series
                    .with_label_values(&["merged_blocks"])
                    .observe(0.0);
                self.metrics.queried_samples.observe(0.0);
                permit.finish(false);
                builder.set_end_of_series_stream();
                return Ok(Box::pin(tokio_stream::iter(std::iter::once(Ok(
                    builder.finish(),
                )))));
            }

            for cs in &chunk_series {
                builder
                    .push_series_stringlabels(cs.labels.as_bytes(), cs.chunks.len() as i64)
                    .map_err(|e| IngesterError::Internal {
                        message: format!("encoding QueryStreamSeries: {e}"),
                    })?;
            }
            builder.set_end_of_series_stream();

            let mut responses: smallvec::SmallVec<[Result<Bytes, Status>; 2]> =
                smallvec::SmallVec::new();
            responses.push(Ok(builder.finish()));

            let mut num_samples = 0usize;
            let mut chunks_in_batch = 0usize;
            let mut raw_chunks: smallvec::SmallVec<[RawChunk<'_>; 4]> = smallvec::SmallVec::new();

            for (series_idx, cs) in chunk_series.iter().enumerate() {
                raw_chunks.clear();
                for cm in &cs.chunks {
                    let Some(ref enc_chunk) = cm.chunk else {
                        permit.finish(false);
                        return Err(IngesterError::Internal {
                            message: "unfilled chunk returned from TSDB chunk querier".to_string(),
                        });
                    };
                    num_samples += enc_chunk.num_samples();
                    raw_chunks.push(RawChunk {
                        encoding: tsdb_encoding_to_wire(enc_chunk.encoding()),
                        min_timestamp_ms: cm.min_time,
                        max_timestamp_ms: cm.max_time,
                        data: enc_chunk.as_bytes(),
                    });
                }
                let entry_len = encoded_series_chunks_len(series_idx as u64, &raw_chunks);
                if (chunks_in_batch > 0
                    && builder.len() + entry_len > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                    || chunks_in_batch >= batch_limit
                {
                    responses.push(Ok(builder.finish()));
                    chunks_in_batch = 0;
                }
                builder.push_series_chunks(series_idx as u64, &raw_chunks);
                chunks_in_batch += 1;
            }

            if !builder.is_empty() {
                responses.push(Ok(builder.finish()));
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
                .stream_selected_raw_query_batches(chunk_series, batch_limit, &tx)
                .await;
            if let Err(e) = res {
                let _ = tx.send(Err(err_sink.map_read_error(e))).await;
            }
            permit.finish(false);
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn stream_selected_raw_query_batches(
        &self,
        mut chunk_series: Vec<QueryChunkSeries>,
        batch_limit: usize,
        tx: &mpsc::Sender<Result<Bytes, Status>>,
    ) -> Result<(), IngesterError> {
        let num_series = chunk_series.len();
        let mut builder = QueryStreamBatchBuilder::with_capacity(64 * 1024);
        let mut series_in_batch = 0usize;
        for cs in chunk_series.iter_mut() {
            let labels = std::mem::take(&mut cs.labels);
            builder
                .push_series_stringlabels(labels.as_bytes(), cs.chunks.len() as i64)
                .map_err(|e| IngesterError::Internal {
                    message: format!("encoding QueryStreamSeries: {e}"),
                })?;
            drop(labels);
            series_in_batch += 1;
            if series_in_batch >= QUERY_STREAM_BATCH_SIZE {
                let bytes = builder.finish();
                series_in_batch = 0;
                if tx.send(Ok(bytes)).await.is_err() {
                    return Ok(());
                }
            }
        }
        builder.set_end_of_series_stream();
        if tx.send(Ok(builder.finish())).await.is_err() {
            return Ok(());
        }

        let mut num_samples = 0usize;
        let mut chunks_in_batch = 0usize;
        let mut raw_chunks: smallvec::SmallVec<[RawChunk<'_>; 4]> = smallvec::SmallVec::new();

        for (series_idx, cs) in chunk_series.iter().enumerate() {
            raw_chunks.clear();
            for cm in &cs.chunks {
                let Some(ref enc_chunk) = cm.chunk else {
                    return Err(IngesterError::Internal {
                        message: "unfilled chunk returned from TSDB chunk querier".to_string(),
                    });
                };
                num_samples += enc_chunk.num_samples();
                raw_chunks.push(RawChunk {
                    encoding: tsdb_encoding_to_wire(enc_chunk.encoding()),
                    min_timestamp_ms: cm.min_time,
                    max_timestamp_ms: cm.max_time,
                    data: enc_chunk.as_bytes(),
                });
            }
            let entry_len = encoded_series_chunks_len(series_idx as u64, &raw_chunks);
            if (chunks_in_batch > 0
                && builder.len() + entry_len > QUERY_STREAM_BATCH_MESSAGE_SIZE)
                || chunks_in_batch >= batch_limit
            {
                let bytes = builder.finish();
                chunks_in_batch = 0;
                if tx.send(Ok(bytes)).await.is_err() {
                    return Ok(());
                }
            }
            builder.push_series_chunks(series_idx as u64, &raw_chunks);
            chunks_in_batch += 1;
        }

        if !builder.is_empty() && tx.send(Ok(builder.finish())).await.is_err() {
            return Ok(());
        }

        self.metrics
            .queried_series
            .with_label_values(&["merged_blocks"])
            .observe(num_series as f64);
        self.metrics.queried_samples.observe(num_samples as f64);

        Ok(())
    }

    /// Streaming `QueryStream` implementation backed by a bounded `mpsc::channel(8)` so
    /// 128-series label batches and chunk batches are flushed and dropped incrementally
    /// without buffering the entire query result set in memory.
    pub async fn handle_query_stream_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::QueryRequest,
    ) -> Result<ReceiverStream<Result<ingesterpb::QueryStreamResponse, Status>>, IngesterError>
    {
        self.handle_query_stream_rx_with_sink(md, user_id, req, ReadErrorSink::default())
            .await
    }

    pub(crate) async fn handle_query_stream_rx_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        mut req: ingesterpb::QueryRequest,
        err_sink: ReadErrorSink,
    ) -> Result<ReceiverStream<Result<ingesterpb::QueryStreamResponse, Status>>, IngesterError>
    {
        let permit = self.start_read_request(md).await?;
        if req.streaming_chunks_batch_size == 0 {
            req.streaming_chunks_batch_size = FALLBACK_CHUNK_STREAM_BATCH_SIZE;
        }
        let matchers = from_proto_matchers(&req.matchers)?;
        let (shard, matchers) = remove_shard_from_matchers(matchers)?;

        self.metrics.queries.inc();
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }

        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                let (_tx, rx) = mpsc::channel(1);
                return Ok(ReceiverStream::new(rx));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };

        let (tx, rx) = mpsc::channel(8);
        let ingester = Arc::clone(self);
        let user_id_owned = user_id.to_string();
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let batch_limit = (req.streaming_chunks_batch_size.min(isize::MAX as u64)).max(1) as usize;

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
                let _ = tx.send(Err(err_sink.map_read_error(e))).await;
            }
            permit.finish(false);
        });

        Ok(ReceiverStream::new(rx))
    }

    /// `statsTrackingChunkSeriesSet.recordStatsToMetrics` for every block querier (Head and
    /// persisted blocks) of a `QueryStream` select. Estimates are only set by a cost-based plan;
    /// without one they are 0 like Go's unset `QueryStats`.
    ///
    /// The actual selected postings are only known to Go when the postings come through the
    /// PostingsForMatchers cache (`ActualSelectedPostingsClonerFactory`): for sharded selects, for
    /// `-blocks-storage.tsdb.{head,block}-postings-for-matchers-cache-force`, and never for shared
    /// head caches with invalidation and no metric name matcher. Otherwise 0 is observed.
    pub(crate) fn observe_block_select_stats(
        &self,
        user_id: &str,
        db: &UserTsdb,
        sink: &BlockSelectStatsSink,
        sharded: bool,
        matchers: &[LabelMatcher],
    ) {
        self.observe_block_select_stats_with_estimates(user_id, db, sink, &[], sharded, matchers);
    }

    pub(crate) fn observe_block_select_stats_with_estimates(
        &self,
        user_id: &str,
        db: &UserTsdb,
        sink: &BlockSelectStatsSink,
        estimates: &[PlannedSelectMeta],
        sharded: bool,
        matchers: &[LabelMatcher],
    ) {
        let stats = std::mem::take(&mut *sink.lock());
        if stats.is_empty() {
            return;
        }
        let tsdb_cfg = &self.blocks_cfg.tsdb;
        let default_has_metric_name =
            mimir_tsdb::postings_cache::exact_metric_name_from_matchers(matchers).is_some();
        let postings_tracked = |head: bool, has_metric_name: bool| {
            let force = if head {
                tsdb_cfg.head_postings_for_matchers_cache_force
            } else {
                tsdb_cfg.block_postings_for_matchers_cache_force
            };
            let shared_key_ok = !(head
                && tsdb_cfg.shared_postings_for_matchers_cache
                && tsdb_cfg.head_postings_for_matchers_cache_invalidation
                && !has_metric_name);
            (sharded || force) && shared_key_ok
        };
        let head_min_time = db.db.head().min_time();
        let block_range = db.db.head().options().chunk_range_ms.max(1);
        let index = self.metrics.queried_series.with_label_values(&["single_block_index"]);
        let filter = self.metrics.queried_series.with_label_values(&["single_block_filter"]);
        let lp = &self.lookup_plan_metrics;
        for (idx, mut st) in stats.into_iter().enumerate() {
            let meta = estimates.get(idx).copied().unwrap_or(PlannedSelectMeta {
                estimated_postings: 0,
                estimated_final: 0,
                is_secondary_unplanned: false,
                has_index_metric_name: default_has_metric_name,
            });
            if !meta.is_secondary_unplanned {
                if st.head {
                    self.metrics.queried_blocks.with_label_values(&["0"]).inc();
                } else if head_min_time == i64::MAX {
                    self.metrics
                        .queried_blocks
                        .with_label_values(&["unknown"])
                        .inc();
                } else {
                    let gen = (head_min_time.saturating_sub(st.block_min_time) / block_range).max(1);
                    if gen > 100 {
                        self.metrics
                            .queried_blocks
                            .with_label_values(&["100+"])
                            .inc();
                    } else {
                        self.metrics
                            .queried_blocks
                            .with_label_values(&[&gen.to_string()])
                            .inc();
                    }
                }
            }
            if st.head && st.overlaps_ooo {
                continue;
            }
            if !postings_tracked(st.head, meta.has_index_metric_name) {
                st.selected_postings = 0;
            }
            index.observe(st.selected_postings as f64);
            filter.observe(st.series as f64);
            if st.series > 0 {
                lp.filtered_ratio
                    .with_label_values(&[user_id])
                    .observe(st.selected_postings as f64 / st.series as f64);
                lp.final_cardinality_ratio
                    .with_label_values(&[user_id])
                    .observe(meta.estimated_final as f64 / st.series as f64);
            }
            if st.selected_postings > 0 {
                lp.intersection_size_ratio
                    .with_label_values(&[user_id])
                    .observe(meta.estimated_postings as f64 / st.selected_postings as f64);
            }
        }
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
        let unordered = ooo_window > 0;
        let planning_enabled = self
            .blocks_cfg
            .tsdb
            .index_lookup_planning
            .index_lookup_planning_enabled;
        let portion = self
            .blocks_cfg
            .tsdb
            .index_lookup_planning
            .index_lookup_planning_comparison_portion;
        let select_stats = BlockSelectStatsSink::default();
        let est_stats: EstimatesSink = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut chunk_series = if planning_enabled || portion > 0.0 {
            let select_hints = shard.map(|(shard_index, shard_count)| SelectHints {
                shard_index,
                shard_count,
            });
            let snapshot = db
                .db
                .try_snapshot_query_sources(start_timestamp_ms, end_timestamp_ms)
                .map_err(|e| IngesterError::Internal {
                    message: format!("selecting series from ChunkQuerier: {e}"),
                })?;
            // Keep `block_guards` declared before `blocks` so early returns drop `Arc<Block>`
            // mmaps before `BlockReadGuard` wakes `close_and_wait()`.
            let block_guards = snapshot.block_guards;
            let blocks = snapshot.blocks;
            let head_snap = snapshot.head;

            let run_cq = |cq: &mut dyn ChunkQuerier| -> Result<Vec<QueryChunkSeries>, IngesterError> {
                let mut ss = cq.select(true, select_hints.as_ref(), matchers);
                let mut out = Vec::new();
                while ss.next() {
                    if let Some(s) = ss.at() {
                        out.push(QueryChunkSeries {
                            labels: s.labels(),
                            chunks: s.chunks(),
                        });
                    }
                }
                let err = ss.err();
                drop(ss);
                let _ = cq.close();
                if let Some(e) = err {
                    return Err(IngesterError::Internal {
                        message: format!("selecting series from ChunkQuerier: {e}"),
                    });
                }
                Ok(out)
            };
            let wrap_mirrored = |planned_cq: PlannedTsdbChunkQuerier,
                                 meta: ChunkBlockMeta| -> Box<dyn ChunkQuerier> {
                if portion > 0.0 && rand::random::<f64>() <= portion {
                    Box::new(MirroredChunkQuerierAdapter(MirroredChunkQuerier::new_with_meta(
                        user_id,
                        self.metrics.index_lookup_comparison_outcomes.clone(),
                        start_timestamp_ms,
                        end_timestamp_ms,
                        meta,
                        Box::new(planned_cq),
                    )))
                } else {
                    Box::new(planned_cq)
                }
            };
            let run_planned = |planned_cq: PlannedTsdbChunkQuerier,
                               meta: ChunkBlockMeta| -> Result<Vec<QueryChunkSeries>, IngesterError> {
                let mut cq = wrap_mirrored(planned_cq, meta);
                run_cq(cq.as_mut())
            };

            let mut per_source: Vec<Vec<QueryChunkSeries>> = Vec::new();
            if let Some(hs) = head_snap {
                // Match Go `tsdb/db.go:2955-2986` + `ooo_head_read.go:708-760`:
                // create the RangeHead ChunkQuerier (optionally mirrored), then when OOO
                // overlaps wrap it in `HeadAndOOOChunkQuerier`, which runs `Select` once
                // through `LookupPlanner` over Head+OOO, merges overlapping Head clusters,
                // and closes the wrapped querier without calling its `select()` (`no_select`).
                let planner = db.head_lookup_planner();
                let head_overlaps_ooo = hs.overlaps_ooo;
                let head_meta = ChunkBlockMeta {
                    ulid: lookupplan::HEAD_ULID,
                    min_time: hs.min_time,
                    max_time: hs.max_time,
                };
                let pq = PlannedTsdbChunkQuerier::new_head_with_snapshot(
                    Arc::clone(&db.db),
                    start_timestamp_ms,
                    end_timestamp_ms,
                    Arc::clone(&planner),
                    unordered,
                    hs.clone(),
                )
                .with_select_stats(Arc::clone(&select_stats), shard_stats_filter(shard))
                .with_estimates_sink(Arc::clone(&est_stats));
                let mut head_cq = wrap_mirrored(pq, head_meta);
                if head_overlaps_ooo {
                    head_cq = Box::new(
                        HeadAndOOOChunkQuerier::new_with_snapshot(
                            Arc::clone(&db.db),
                            start_timestamp_ms,
                            end_timestamp_ms,
                            planner,
                            Some(head_cq),
                            hs,
                        )
                        .with_select_stats(Arc::clone(&select_stats), shard_stats_filter(shard))
                        .with_estimates_sink(Arc::clone(&est_stats)),
                    );
                }
                per_source.push(run_cq(head_cq.as_mut())?);
            }
            for (blk, block_guard) in blocks.into_iter().zip(block_guards) {
                let planner = db.block_lookup_planner(&blk);
                let m = blk.meta();
                let meta = ChunkBlockMeta {
                    ulid: m.ulid,
                    min_time: m.min_time,
                    max_time: m.max_time,
                };
                let pq = PlannedTsdbChunkQuerier::new_block_with_guard(
                    Arc::clone(&db.db),
                    blk,
                    block_guard,
                    start_timestamp_ms,
                    end_timestamp_ms,
                    planner,
                    unordered,
                )
                .with_select_stats(Arc::clone(&select_stats), shard_stats_filter(shard))
                .with_estimates_sink(Arc::clone(&est_stats));
                per_source.push(run_planned(pq, meta)?);
            }
            merge_per_block_chunk_series(
                per_source,
                unordered,
                db.db.head().float_chunk_encoding(),
            )?
        } else {
            let cq = if unordered {
                db.db
                    .unordered_chunk_querier(start_timestamp_ms, end_timestamp_ms)
            } else {
                db.db.chunk_querier(start_timestamp_ms, end_timestamp_ms)
            }
            .with_concurrent(shard.is_some())
            .with_filtered_select_stats(Arc::clone(&select_stats), shard_stats_filter(shard));
            cq.select(true, matchers)
                .map_err(|e| IngesterError::Internal {
                    message: format!("selecting series from ChunkQuerier: {e}"),
                })?
        };
        let est_vec = std::mem::take(&mut *est_stats.lock());
        self.observe_block_select_stats_with_estimates(
            user_id,
            db,
            &select_stats,
            &est_vec,
            shard.is_some(),
            matchers,
        );

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

        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
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
        let batch_limit = (req.streaming_chunks_batch_size.min(isize::MAX as u64)).max(1) as usize;
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
        permit.finish(deadline_exceeded(&res));
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

        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    pub(crate) async fn handle_label_values_with_matchers(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesRequest,
        matchers: Vec<LabelMatcher>,
    ) -> Result<ingesterpb::LabelValuesResponse, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self
            .label_values_inner_with_matchers(md, user_id, req, matchers)
            .await;
        permit.finish(deadline_exceeded(&res));
        res
    }

    async fn label_values_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesRequest,
    ) -> Result<ingesterpb::LabelValuesResponse, IngesterError> {
        let matchers = validate_label_values_matchers(&req)?;
        self.label_values_inner_with_matchers(md, user_id, req, matchers)
            .await
    }

    async fn label_values_inner_with_matchers(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesRequest,
        matchers: Vec<LabelMatcher>,
    ) -> Result<ingesterpb::LabelValuesResponse, IngesterError> {
        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(ingesterpb::LabelValuesResponse::default());
        };
        let label_name = req.label_name;
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let mut vals = tokio::task::spawn_blocking(move || {
            let q = db.db.querier(start_ts, end_ts);
            q.label_values(&label_name, limit, &matchers)
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("label_values task join error: {e}"),
        })?
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    async fn label_names_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesRequest,
    ) -> Result<ingesterpb::LabelNamesResponse, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(ingesterpb::LabelNamesResponse::default());
        };
        let matchers = match req.matchers {
            Some(ref m) => from_proto_matchers(&m.matchers)?,
            None => Vec::new(),
        };
        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let mut names = tokio::task::spawn_blocking(move || {
            let q = db.db.querier(start_ts, end_ts);
            q.label_names(&matchers)
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("label_names task join error: {e}"),
        })?
        .map_err(|e| IngesterError::Internal {
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
            let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
                return Ok(ingesterpb::UserStatsResponse::default());
            };
            create_user_stats(&db, req.count_method)
        }
        .await;
        permit.finish(deadline_exceeded(&res));
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
        permit.finish(deadline_exceeded(&res));
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    /// Zero-alloc wire-encoding `MetricsForLabelMatchers` returning raw serialized
    /// `MetricsForLabelMatchersResponse` protobuf `Bytes` (served via `RawMetricsForLabelMatchersSvc`).
    pub async fn handle_metrics_for_label_matchers_raw(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::MetricsForLabelMatchersRequest,
    ) -> Result<Bytes, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = self
            .metrics_for_label_matchers_raw_inner(md, user_id, req)
            .await;
        permit.finish(deadline_exceeded(&res));
        res
    }

    fn select_sorted_labels_for_matchers(
        db: &UserTsdb,
        start_ts: i64,
        end_ts: i64,
        matchers_set: &[Vec<LabelMatcher>],
        limit: usize,
    ) -> Result<Vec<Labels>, IngesterError> {
        if matchers_set.is_empty() {
            return Ok(Vec::new());
        }
        let snapshot = db
            .db
            .try_snapshot_query_sources(start_ts, end_ts)
            .map_err(|e| IngesterError::Internal {
                message: e.to_string(),
            })?;
        // Keep `block_guards` declared before `blocks` so block mmaps drop before guards.
        let _block_guards = snapshot.block_guards;
        let blocks = snapshot.blocks;
        let head_snap = snapshot.head;
        let head = head_snap.as_ref().map(|_| db.db.head());
        let head_iso = head_snap.as_ref().map(|hs| &hs.isolation);

        let head_planner = head.map(|_| db.head_lookup_planner());
        let block_planners: Vec<Arc<dyn LookupPlanner>> = blocks
            .iter()
            .map(|blk| db.block_lookup_planner(blk))
            .collect();

        let plan_for =
            |planner: &Arc<dyn LookupPlanner>, matchers: &[LabelMatcher]| -> BasicLookupPlan {
                let in_plan = BasicLookupPlan::new(matchers.to_vec(), Vec::new());
                match planner.plan_index_lookup(&in_plan, None) {
                    Ok(p) => p,
                    Err(_) => in_plan,
                }
            };

        if matchers_set.len() == 1 && blocks.is_empty() {
            let (Some(head), Some(planner)) = (head, head_planner.as_ref()) else {
                return Ok(Vec::new());
            };
            let plan = plan_for(planner, &matchers_set[0]);
            let refs = head
                .postings_for_matchers_with_concurrent(&plan.index_matchers, false)
                .map_err(|e| IngesterError::Internal {
                    message: e.to_string(),
                })?;
            let mut out = Vec::with_capacity(refs.len());
            for &r in refs.iter() {
                let labels_opt = match head_iso {
                    Some(iso) => {
                        head.series_has_chunks_in_range_with_isolation(r, start_ts, end_ts, iso)
                    }
                    None => head.series_has_chunks_in_range(r, start_ts, end_ts),
                };
                if let Some(labels) = labels_opt {
                    if plan
                        .scan_matchers
                        .iter()
                        .all(|m| m.matches(labels.get(&m.name)))
                    {
                        out.push(labels);
                    }
                }
            }
            if out.len() > 1 {
                mimir_tsdb::querier::sort_labels(&mut out);
            }
            if out.len() > limit {
                out.truncate(limit);
            }
            return Ok(out);
        }

        let mut dedup = hashbrown::HashSet::<Labels>::new();
        for matchers in matchers_set {
            if let (Some(head), Some(planner)) = (head, head_planner.as_ref()) {
                let plan = plan_for(planner, matchers);
                let refs = head
                    .postings_for_matchers_with_concurrent(&plan.index_matchers, false)
                    .map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })?;
                for &r in refs.iter() {
                    let labels_opt = match head_iso {
                        Some(iso) => {
                            head.series_has_chunks_in_range_with_isolation(r, start_ts, end_ts, iso)
                        }
                        None => head.series_has_chunks_in_range(r, start_ts, end_ts),
                    };
                    if let Some(labels) = labels_opt {
                        if plan
                            .scan_matchers
                            .iter()
                            .all(|m| m.matches(labels.get(&m.name)))
                        {
                            dedup.insert(labels);
                        }
                    }
                }
            }
            for (blk, planner) in blocks.iter().zip(&block_planners) {
                let plan = plan_for(planner, matchers);
                let idx = blk.index();
                let tombr = blk.tombstones();
                let refs = blk
                    .postings_for_matchers_with_concurrent(&plan.index_matchers, false)
                    .map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })?;
                for &r in refs.iter() {
                    let (labels, chks) = idx.series(r).map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })?;
                    if !plan
                        .scan_matchers
                        .iter()
                        .all(|m| m.matches(labels.get(&m.name)))
                    {
                        continue;
                    }
                    let tombs = tombr.get(r);
                    let any_chunk = chks.iter().any(|cm| {
                        cm.overlaps_closed_interval(start_ts, end_ts)
                            && !tombs.iter().any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
                    });
                    if any_chunk {
                        dedup.insert(labels);
                    }
                }
            }
        }
        let mut merged: Vec<Labels> = dedup.into_iter().collect();
        if limit < merged.len() {
            merged.select_nth_unstable(limit);
            merged.truncate(limit);
        }
        if merged.len() > 1 {
            mimir_tsdb::querier::sort_labels(&mut merged);
        }
        Ok(merged)
    }

    async fn metrics_for_label_matchers_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::MetricsForLabelMatchersRequest,
    ) -> Result<ingesterpb::MetricsForLabelMatchersResponse, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(ingesterpb::MetricsForLabelMatchersResponse::default());
        };
        let mut matchers_set = Vec::with_capacity(req.matchers_set.len());
        for lm_set in &req.matchers_set {
            matchers_set.push(from_proto_matchers(&lm_set.matchers)?);
        }
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            usize::MAX
        };

        tokio::task::spawn_blocking(move || {
            let sorted_labels =
                Self::select_sorted_labels_for_matchers(&db, start_ts, end_ts, &matchers_set, limit)?;
            let metric: Vec<cortexpb::Metric> = sorted_labels
                .into_iter()
                .map(|lbls| cortexpb::Metric {
                    labels: labels_to_proto_pairs(&lbls),
                })
                .collect();
            Ok(ingesterpb::MetricsForLabelMatchersResponse { metric })
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("metrics_for_label_matchers task join error: {e}"),
        })?
    }

    async fn metrics_for_label_matchers_raw_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::MetricsForLabelMatchersRequest,
    ) -> Result<Bytes, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(Bytes::new());
        };
        let mut matchers_set = Vec::with_capacity(req.matchers_set.len());
        for lm_set in &req.matchers_set {
            matchers_set.push(from_proto_matchers(&lm_set.matchers)?);
        }
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            usize::MAX
        };

        tokio::task::spawn_blocking(move || {
            let sorted_labels =
                Self::select_sorted_labels_for_matchers(&db, start_ts, end_ts, &matchers_set, limit)?;
            if sorted_labels.is_empty() {
                return Ok(Bytes::new());
            }
            let mut out = bytes::BytesMut::with_capacity(sorted_labels.len().saturating_mul(192));
            encode_metrics_for_label_matchers_response(
                &mut out,
                sorted_labels.iter().map(Labels::as_bytes),
            )
            .map_err(|e| IngesterError::Internal {
                message: format!("encode metrics_for_label_matchers: {e}"),
            })?;
            Ok(out.freeze())
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("metrics_for_label_matchers task join error: {e}"),
        })?
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    pub async fn handle_label_names_and_values_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesAndValuesRequest,
    ) -> Result<
        ReceiverStream<Result<ingesterpb::LabelNamesAndValuesResponse, Status>>,
        IngesterError,
    > {
        self.handle_label_names_and_values_rx_with_sink(
            md,
            user_id,
            req,
            ReadErrorSink::default(),
        )
        .await
    }

    pub(crate) async fn handle_label_names_and_values_rx_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesAndValuesRequest,
        err_sink: ReadErrorSink,
    ) -> Result<
        ReceiverStream<Result<ingesterpb::LabelNamesAndValuesResponse, Status>>,
        IngesterError,
    > {
        let permit = self.start_read_request(md).await?;
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }
        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                let (_tx, rx) = mpsc::channel(1);
                return Ok(ReceiverStream::new(rx));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };
        let matchers = match from_proto_matchers(&req.matchers) {
            Ok(m) => m,
            Err(e) => {
                permit.finish(false);
                return Err(e);
            }
        };
        let active_only = match ingesterpb::CountMethod::try_from(req.count_method) {
            Ok(ingesterpb::CountMethod::InMemory) => false,
            Ok(ingesterpb::CountMethod::Active) => true,
            Err(_) => {
                permit.finish(false);
                return Err(IngesterError::Internal {
                    message: format!("unknown count method \"{}\"", req.count_method),
                });
            }
        };
        let (tx, rx) = mpsc::channel(8);
        tokio::task::spawn_blocking(move || {
            let head = db.db.head();
            let base_refs_arc = if !matchers.is_empty() {
                match head.postings_for_matchers_direct_arc(&matchers) {
                    Ok(refs) if refs.is_empty() => {
                        permit.finish(false);
                        return;
                    }
                    Ok(refs) => Some(refs),
                    Err(e) => {
                        let err = IngesterError::Internal {
                            message: e.to_string(),
                        };
                        let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                        permit.finish(false);
                        return;
                    }
                }
            } else {
                None
            };
            // W3-09: match Go `LabelNamesAndValues(ACTIVE, matchers)`: filter postings by
            // `matchers` first (accelerated by dense bitset), then check if ANY series carrying
            // the label value is active.
            let ref_filter = base_refs_arc
                .as_deref()
                .map(mimir_tsdb::postings::DenseRefFilter::from_sorted_refs);
            let label_names = match head.label_names(&[]) {
                Ok(n) => n,
                Err(e) => {
                    let err = IngesterError::Internal {
                        message: e.to_string(),
                    };
                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                    permit.finish(false);
                    return;
                }
            };
            let mut resp = ingesterpb::LabelNamesAndValuesResponse::default();
            let mut resp_bytes = 0usize;
            for label_name in label_names {
                let mut filtered = Vec::new();
                if let Some(ref filter) = ref_filter {
                    head.visit_matching_values_postings_for_name(
                        &label_name,
                        |small, large| {
                            filter.matches_any(small, large)
                                && (!active_only
                                    || small.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r as u64, i64::MIN)
                                    })
                                    || large.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r, i64::MIN)
                                    }))
                        },
                        |val_str| {
                            if !val_str.is_empty() {
                                filtered.push(val_str.to_owned());
                            }
                        },
                    );
                } else {
                    head.visit_matching_values_postings_for_name(
                        &label_name,
                        |small, large| {
                            (!small.is_empty() || !large.is_empty())
                                && (!active_only
                                    || small.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r as u64, i64::MIN)
                                    })
                                    || large.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r, i64::MIN)
                                    }))
                        },
                        |val_str| {
                            if !val_str.is_empty() {
                                filtered.push(val_str.to_owned());
                            }
                        },
                    );
                }
                if filtered.is_empty() {
                    continue;
                }
                filtered.sort_unstable();
                resp_bytes += label_name.len();
                if resp_bytes >= LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES && !resp.items.is_empty() {
                    if tx.blocking_send(Ok(std::mem::take(&mut resp))).is_err() {
                        permit.finish(false);
                        return;
                    }
                    resp_bytes = label_name.len();
                }
                let mut cur_vals = Vec::new();
                for val in filtered {
                    resp_bytes += val.len();
                    cur_vals.push(val);
                    if resp_bytes >= LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES {
                        resp.items.push(ingesterpb::LabelValues {
                            label_name: label_name.clone(),
                            values: std::mem::take(&mut cur_vals),
                        });
                        if tx.blocking_send(Ok(std::mem::take(&mut resp))).is_err() {
                            permit.finish(false);
                            return;
                        }
                        resp_bytes = label_name.len();
                    }
                }
                if !cur_vals.is_empty() {
                    resp.items.push(ingesterpb::LabelValues {
                        label_name: label_name.clone(),
                        values: cur_vals,
                    });
                } else {
                    resp_bytes = 0;
                }
            }
            if !resp.items.is_empty() {
                let _ = tx.blocking_send(Ok(resp));
            }
            permit.finish(false);
        });
        Ok(ReceiverStream::new(rx))
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    async fn label_names_and_values_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelNamesAndValuesRequest,
    ) -> Result<Vec<ingesterpb::LabelNamesAndValuesResponse>, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(Vec::new());
        };
        let matchers = from_proto_matchers(&req.matchers)?;
        let active_only = match ingesterpb::CountMethod::try_from(req.count_method) {
            Ok(ingesterpb::CountMethod::InMemory) => false,
            Ok(ingesterpb::CountMethod::Active) => true,
            Err(_) => {
                return Err(IngesterError::Internal {
                    message: format!("unknown count method \"{}\"", req.count_method),
                });
            }
        };

        tokio::task::spawn_blocking(move || -> Result<Vec<ingesterpb::LabelNamesAndValuesResponse>, IngesterError> {
            let head = db.db.head();
            let base_refs_arc = if !matchers.is_empty() {
                let refs = head
                    .postings_for_matchers_direct_arc(&matchers)
                    .map_err(|e| IngesterError::Internal {
                        message: e.to_string(),
                    })?;
                if refs.is_empty() {
                    return Ok(Vec::new());
                }
                Some(refs)
            } else {
                None
            };
            // W3-09: match Go `LabelNamesAndValues(ACTIVE, matchers)`: filter postings by
            // `matchers` first (accelerated by dense bitset), then check if ANY series carrying
            // the label value is active.
            let ref_filter = base_refs_arc
                .as_deref()
                .map(mimir_tsdb::postings::DenseRefFilter::from_sorted_refs);

            let label_names = head
                .label_names(&[])
                .map_err(|e| IngesterError::Internal {
                    message: e.to_string(),
                })?;

            let mut batches = Vec::new();
            let mut resp = ingesterpb::LabelNamesAndValuesResponse::default();
            let mut resp_bytes = 0usize;

            for label_name in label_names {
                let mut filtered = Vec::new();
                if let Some(ref filter) = ref_filter {
                    head.visit_matching_values_postings_for_name(
                        &label_name,
                        |small, large| {
                            filter.matches_any(small, large)
                                && (!active_only
                                    || small.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r as u64, i64::MIN)
                                    })
                                    || large.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r, i64::MIN)
                                    }))
                        },
                        |val_str| {
                            if !val_str.is_empty() {
                                filtered.push(val_str.to_owned());
                            }
                        },
                    );
                } else {
                    head.visit_matching_values_postings_for_name(
                        &label_name,
                        |small, large| {
                            (!small.is_empty() || !large.is_empty())
                                && (!active_only
                                    || small.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r as u64, i64::MIN)
                                    })
                                    || large.iter().any(|&r| {
                                        db.active_series.is_ref_active_fast(r, i64::MIN)
                                    }))
                        },
                        |val_str| {
                            if !val_str.is_empty() {
                                filtered.push(val_str.to_owned());
                            }
                        },
                    );
                }

                if filtered.is_empty() {
                    continue;
                }
                filtered.sort_unstable();

                resp_bytes += label_name.len();
                if resp_bytes >= LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES && !resp.items.is_empty() {
                    batches.push(std::mem::take(&mut resp));
                    resp_bytes = label_name.len();
                }

                let mut cur_vals = Vec::new();
                for val in filtered {
                    resp_bytes += val.len();
                    cur_vals.push(val);
                    if resp_bytes >= LABEL_NAMES_AND_VALUES_TARGET_SIZE_BYTES {
                        resp.items.push(ingesterpb::LabelValues {
                            label_name: label_name.clone(),
                            values: std::mem::take(&mut cur_vals),
                        });
                        batches.push(std::mem::take(&mut resp));
                        resp_bytes = label_name.len();
                    }
                }
                if !cur_vals.is_empty() {
                    resp.items.push(ingesterpb::LabelValues {
                        label_name: label_name.clone(),
                        values: cur_vals,
                    });
                } else {
                    resp_bytes = 0;
                }
            }

            if !resp.items.is_empty() {
                batches.push(resp);
            }
            Ok(batches)
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("label_names_and_values task join error: {e}"),
        })?
    }

    pub async fn handle_label_values_cardinality_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesCardinalityRequest,
    ) -> Result<
        ReceiverStream<Result<ingesterpb::LabelValuesCardinalityResponse, Status>>,
        IngesterError,
    > {
        self.handle_label_values_cardinality_rx_with_sink(
            md,
            user_id,
            req,
            ReadErrorSink::default(),
        )
        .await
    }

    pub(crate) async fn handle_label_values_cardinality_rx_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesCardinalityRequest,
        err_sink: ReadErrorSink,
    ) -> Result<
        ReceiverStream<Result<ingesterpb::LabelValuesCardinalityResponse, Status>>,
        IngesterError,
    > {
        let permit = self.start_read_request(md).await?;
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }
        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                let (_tx, rx) = mpsc::channel(1);
                return Ok(ReceiverStream::new(rx));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };
        let matchers = match from_proto_matchers(&req.matchers) {
            Ok(m) => m,
            Err(e) => {
                permit.finish(false);
                return Err(e);
            }
        };
        let active_only = match ingesterpb::CountMethod::try_from(req.count_method) {
            Ok(ingesterpb::CountMethod::InMemory) => false,
            Ok(ingesterpb::CountMethod::Active) => true,
            Err(_) => {
                permit.finish(false);
                return Err(IngesterError::Internal {
                    message: format!("unknown count method \"{}\"", req.count_method),
                });
            }
        };
        let label_names = req.label_names;
        let pool = Arc::clone(&self.compute_worker_pool);
        let chunk_size = self.cfg.label_values_count.chunk_size.max(1) as usize;
        let tenant_id = user_id.to_string();
        let (tx, rx) = mpsc::channel(8);
        tokio::task::spawn_blocking(move || {
            struct CancelOnDrop(Arc<AtomicBool>);
            impl Drop for CancelOnDrop {
                fn drop(&mut self) {
                    self.0.store(true, AtomicOrdering::Release);
                }
            }
            let canceled = Arc::new(AtomicBool::new(false));
            let _cancel_guard = CancelOnDrop(Arc::clone(&canceled));
            let is_canceled: Arc<dyn Fn() -> bool + Send + Sync> = {
                let c = Arc::clone(&canceled);
                Arc::new(move || c.load(AtomicOrdering::Acquire))
            };
            let to_err = |e: LabelNamesAndValuesError| IngesterError::Internal {
                message: e.to_string(),
            };
            let head = db.db.head();
            let is_single_all_postings = matchers.len() == 1
                && matchers[0].name.is_empty()
                && matchers[0].value.is_empty();
            let eligible_refs: Option<Arc<[u64]>> = if !matchers.is_empty() {
                let base_refs = match head.postings_for_matchers_direct_arc(&matchers) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = IngesterError::Internal { message: e.to_string() };
                        let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                        permit.finish(false);
                        return;
                    }
                };
                if !base_refs.is_empty() {
                    for lbl_name in &label_names {
                        if is_single_all_postings || lbl_name.is_empty() {
                            match head.label_values(lbl_name, &matchers) {
                                Ok(vals) if !vals.is_empty() => {
                                    let err = IngesterError::Internal {
                                        message: "unexpected all postings".to_string(),
                                    };
                                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                                    permit.finish(false);
                                    return;
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    let err = IngesterError::Internal {
                                        message: e.to_string(),
                                    };
                                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                                    permit.finish(false);
                                    return;
                                }
                            }
                        }
                    }
                }
                if active_only {
                    let mut filtered = Vec::with_capacity(base_refs.len());
                    for &r in base_refs.iter() {
                        if db.active_series.is_ref_active_fast(r, i64::MIN) {
                            filtered.push(r);
                        }
                    }
                    if filtered.is_empty() {
                        permit.finish(false);
                        return;
                    }
                    Some(Arc::from(filtered))
                } else if base_refs.is_empty() {
                    permit.finish(false);
                    return;
                } else {
                    Some(base_refs)
                }
            } else {
                None
            };
            let mut resp = ingesterpb::LabelValuesCardinalityResponse::default();
            let mut resp_bytes = 0usize;
            for lbl_name in &label_names {
                let lbl_values = if let Some(ref elig) = eligible_refs {
                    if lbl_name.is_empty() {
                        if head.visit_postings_for_label_value("", "", |small, large| {
                            mimir_tsdb::postings::intersect_sorted_any(small, large, elig)
                        }) {
                            vec![String::new()]
                        } else {
                            Vec::new()
                        }
                    } else {
                        let mut vals = Vec::new();
                        head.visit_all_values_postings_for_name(lbl_name, |val_str, small, large| {
                            if !val_str.is_empty()
                                && mimir_tsdb::postings::intersect_sorted_any(small, large, elig)
                            {
                                vals.push(val_str.to_owned());
                            }
                        });
                        vals.sort_unstable();
                        vals
                    }
                } else {
                    match head.label_values(lbl_name, &[]) {
                        Ok(v) => v,
                        Err(e) => {
                            let err = IngesterError::Internal { message: e.to_string() };
                            let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                            permit.finish(false);
                            return;
                        }
                    }
                };
                let count_fn: LabelValueCountFn = {
                    let (db, eligible_refs, lbl_name) =
                        (Arc::clone(&db), eligible_refs.clone(), lbl_name.clone());
                    Arc::new(move |val: &str| -> Result<u64, LabelNamesAndValuesError> {
                        let head = db.db.head();
                        Ok(head.visit_postings_for_label_value(
                            &lbl_name,
                            val,
                            |small, large| match eligible_refs.as_deref() {
                                Some(elig) => {
                                    mimir_tsdb::postings::intersect_sorted_count(small, large, elig)
                                }
                                None => {
                                    if active_only {
                                        db.active_series.count_active_in_slices(small, large)
                                    } else {
                                        (small.len() + large.len()) as u64
                                    }
                                }
                            },
                        ))
                    })
                };
                let mut count_rx = compute_label_values_counts(
                    Arc::clone(&is_canceled),
                    &pool,
                    &tenant_id,
                    chunk_size,
                    Arc::new(lbl_values),
                    count_fn,
                );
                let mut item: Option<ingesterpb::LabelValueSeriesCount> = None;
                while let Some(res) = count_rx.blocking_recv() {
                    if let Some(e) = res.err {
                        let _ = tx.blocking_send(Err(err_sink.map_read_error(to_err(e))));
                        permit.finish(false);
                        return;
                    }
                    if res.count == 0 {
                        continue;
                    }
                    resp_bytes += res.val.len();
                    item.get_or_insert_with(|| ingesterpb::LabelValueSeriesCount {
                        label_name: lbl_name.clone(),
                        label_value_series: HashMap::new(),
                    })
                    .label_value_series
                    .insert(res.val, res.count);
                    if resp_bytes < LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES {
                        continue;
                    }
                    if let Some(it) = item.take() {
                        resp.items.push(it);
                    }
                    if tx.blocking_send(Ok(std::mem::take(&mut resp))).is_err() {
                        permit.finish(false);
                        return;
                    }
                    resp_bytes = 0;
                }
                if let Some(it) = item.take() {
                    resp.items.push(it);
                }
            }
            if !resp.items.is_empty() {
                let _ = tx.blocking_send(Ok(resp));
            }
            permit.finish(false);
        });
        Ok(ReceiverStream::new(rx))
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    async fn label_values_cardinality_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::LabelValuesCardinalityRequest,
    ) -> Result<Vec<ingesterpb::LabelValuesCardinalityResponse>, IngesterError> {
        self.enforce_read_consistency(md, user_id).await?;
        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(Vec::new());
        };
        let matchers = from_proto_matchers(&req.matchers)?;
        let active_only = match ingesterpb::CountMethod::try_from(req.count_method) {
            Ok(ingesterpb::CountMethod::InMemory) => false,
            Ok(ingesterpb::CountMethod::Active) => true,
            Err(_) => {
                return Err(IngesterError::Internal {
                    message: format!("unknown count method \"{}\"", req.count_method),
                });
            }
        };

        let label_names = req.label_names;
        let pool = Arc::clone(&self.compute_worker_pool);
        let chunk_size = self.cfg.label_values_count.chunk_size.max(1) as usize;
        let tenant_id = user_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<ingesterpb::LabelValuesCardinalityResponse>, IngesterError> {
            // Go: ctx canceled on return so in-flight chunk tasks on the shared pool bail out.
            struct CancelOnDrop(Arc<AtomicBool>);
            impl Drop for CancelOnDrop {
                fn drop(&mut self) {
                    self.0.store(true, AtomicOrdering::Release);
                }
            }
            let canceled = Arc::new(AtomicBool::new(false));
            let _cancel_guard = CancelOnDrop(Arc::clone(&canceled));
            let is_canceled: Arc<dyn Fn() -> bool + Send + Sync> = {
                let c = Arc::clone(&canceled);
                Arc::new(move || c.load(AtomicOrdering::Acquire))
            };
            let to_err = |e: LabelNamesAndValuesError| IngesterError::Internal {
                message: e.to_string(),
            };

            let head = db.db.head();
            let is_single_all_postings = matchers.len() == 1
                && matchers[0].name.is_empty()
                && matchers[0].value.is_empty();
            let eligible_refs: Option<Arc<[u64]>> = if !matchers.is_empty() {
                let base_refs =
                    head.postings_for_matchers_direct_arc(&matchers)
                        .map_err(|e| IngesterError::Internal {
                            message: e.to_string(),
                        })?;
                if !base_refs.is_empty() {
                    for lbl_name in &label_names {
                        if is_single_all_postings || lbl_name.is_empty() {
                            let vals = head.label_values(lbl_name, &matchers).map_err(|e| {
                                IngesterError::Internal {
                                    message: e.to_string(),
                                }
                            })?;
                            if !vals.is_empty() {
                                return Err(IngesterError::Internal {
                                    message: "unexpected all postings".to_string(),
                                });
                            }
                        }
                    }
                }
                if active_only {
                    let mut filtered = Vec::with_capacity(base_refs.len());
                    for &r in base_refs.iter() {
                        if db.active_series.is_ref_active_fast(r, i64::MIN) {
                            filtered.push(r);
                        }
                    }
                    if filtered.is_empty() {
                        return Ok(Vec::new());
                    }
                    Some(Arc::from(filtered))
                } else {
                    if base_refs.is_empty() {
                        return Ok(Vec::new());
                    }
                    Some(base_refs)
                }
            } else {
                None
            };

            let mut batches = Vec::new();
            let mut resp = ingesterpb::LabelValuesCardinalityResponse::default();
            let mut resp_bytes = 0usize;

            for lbl_name in &label_names {
                let lbl_values = if let Some(ref elig) = eligible_refs {
                    if lbl_name.is_empty() {
                        if head.visit_postings_for_label_value("", "", |small, large| {
                            mimir_tsdb::postings::intersect_sorted_any(small, large, elig)
                        }) {
                            vec![String::new()]
                        } else {
                            Vec::new()
                        }
                    } else {
                        let mut vals = Vec::new();
                        head.visit_all_values_postings_for_name(lbl_name, |val_str, small, large| {
                            if !val_str.is_empty()
                                && mimir_tsdb::postings::intersect_sorted_any(small, large, elig)
                            {
                                vals.push(val_str.to_owned());
                            }
                        });
                        vals.sort_unstable();
                        vals
                    }
                } else {
                    head.label_values(lbl_name, &[]).map_err(|e| {
                        IngesterError::Internal {
                            message: e.to_string(),
                        }
                    })?
                };

                let count_fn: LabelValueCountFn = {
                    let (db, eligible_refs, lbl_name) =
                        (Arc::clone(&db), eligible_refs.clone(), lbl_name.clone());
                    Arc::new(move |val: &str| -> Result<u64, LabelNamesAndValuesError> {
                        let head = db.db.head();
                        Ok(head.visit_postings_for_label_value(
                            &lbl_name,
                            val,
                            |small, large| match eligible_refs.as_deref() {
                                Some(elig) => {
                                    mimir_tsdb::postings::intersect_sorted_count(small, large, elig)
                                }
                                None => {
                                    if active_only {
                                        db.active_series.count_active_in_slices(small, large)
                                    } else {
                                        (small.len() + large.len()) as u64
                                    }
                                }
                            },
                        ))
                    })
                };

                // Go: labelValuesCardinality consumes computeLabelValuesSeriesCount results in
                // completion order, starting a new item after each flushed message.
                let mut rx = compute_label_values_counts(
                    Arc::clone(&is_canceled),
                    &pool,
                    &tenant_id,
                    chunk_size,
                    Arc::new(lbl_values),
                    count_fn,
                );
                let mut item: Option<ingesterpb::LabelValueSeriesCount> = None;
                while let Some(res) = rx.blocking_recv() {
                    if let Some(e) = res.err {
                        return Err(to_err(e));
                    }
                    if res.count == 0 {
                        continue;
                    }
                    resp_bytes += res.val.len();
                    item.get_or_insert_with(|| ingesterpb::LabelValueSeriesCount {
                        label_name: lbl_name.clone(),
                        label_value_series: HashMap::new(),
                    })
                    .label_value_series
                    .insert(res.val, res.count);
                    if resp_bytes < LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES {
                        continue;
                    }
                    if let Some(it) = item.take() {
                        resp.items.push(it);
                    }
                    batches.push(std::mem::take(&mut resp));
                    resp_bytes = 0;
                }
                if let Some(it) = item.take() {
                    resp.items.push(it);
                }
            }
            if !resp.items.is_empty() {
                batches.push(resp);
            }
            Ok(batches)
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("label_values_cardinality task join error: {e}"),
        })?
    }

    pub async fn handle_active_series_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ActiveSeriesRequest,
    ) -> Result<ReceiverStream<Result<ingesterpb::ActiveSeriesResponse, Status>>, IngesterError>
    {
        self.handle_active_series_rx_with_sink(md, user_id, req, ReadErrorSink::default())
            .await
    }

    pub(crate) async fn handle_active_series_rx_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ActiveSeriesRequest,
        err_sink: ReadErrorSink,
    ) -> Result<ReceiverStream<Result<ingesterpb::ActiveSeriesResponse, Status>>, IngesterError>
    {
        let permit = self.start_read_request(md).await?;
        let matchers = match active_series_matchers(&req.matchers) {
            Ok(m) => m,
            Err(e) => {
                permit.finish(false);
                return Err(e);
            }
        };
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }

        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                let (_tx, rx) = mpsc::channel(1);
                return Ok(ReceiverStream::new(rx));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };
        if !self.cfg.active_series.active_series_metrics_enabled {
            permit.finish(false);
            return Err(IngesterError::Internal {
                message: "error listing active series: active series tracker is not initialized"
                    .to_string(),
            });
        }

        let (shard, matchers) = match remove_shard_from_matchers(matchers) {
            Ok(v) => v,
            Err(e) => {
                permit.finish(false);
                return Err(IngesterError::Internal {
                    message: format!(
                        "error listing active series: error removing shard matcher: {e}"
                    ),
                });
            }
        };

        let (tx, rx) = mpsc::channel(8);
        let is_native_histogram = req.r#type
            == ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32;

        tokio::task::spawn_blocking(move || {
            let head = db.db.head();
            let refs = if matchers.is_empty() {
                Ok(Vec::new())
            } else {
                head.postings_for_matchers_direct(&matchers)
                    .map_err(|e| IngesterError::Internal {
                        message: format!("error listing active series: error getting postings: {e}"),
                    })
            };
            let refs = match refs {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(err_sink.map_read_error(e)));
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
                    if tx.blocking_send(Ok(std::mem::take(&mut resp))).is_err() {
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
                let _ = tx.blocking_send(Ok(resp));
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    async fn active_series_inner(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::ActiveSeriesRequest,
    ) -> Result<Vec<ingesterpb::ActiveSeriesResponse>, IngesterError> {
        let matchers = active_series_matchers(&req.matchers)?;
        self.enforce_read_consistency(md, user_id).await?;

        let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
            return Ok(Vec::new());
        };
        if !self.cfg.active_series.active_series_metrics_enabled {
            return Err(IngesterError::Internal {
                message: "error listing active series: active series tracker is not initialized"
                    .to_string(),
            });
        }
        let (shard, matchers) =
            remove_shard_from_matchers(matchers).map_err(|e| IngesterError::Internal {
                message: format!("error listing active series: error removing shard matcher: {e}"),
            })?;
        let is_native_histogram = req.r#type
            == ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32;
        tokio::task::spawn_blocking(move || {
            let head = db.db.head();
            let refs = if matchers.is_empty() {
                Vec::new()
            } else {
                head.postings_for_matchers_direct(&matchers)
                    .map_err(|e| IngesterError::Internal {
                        message: format!("error listing active series: error getting postings: {e}"),
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
                if current_size + m_size > ACTIVE_SERIES_MAX_SIZE_BYTES && !resp.metric.is_empty()
                {
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
        })
        .await
        .map_err(|e| IngesterError::Internal {
            message: format!("active_series task join error: {e}"),
        })?
    }

    /// `org_id` is the request's org ID; as in Go the search request is validated before the
    /// tenant ID (`tenant.TenantID`).
    pub async fn handle_search_label_names(
        &self,
        md: &MetadataMap,
        org_id: &str,
        req: ingesterpb::SearchLabelNamesRequest,
    ) -> Result<Vec<ingesterpb::SearchResultBatch>, IngesterError> {
        let (hints, matchers) =
            build_search_hints(req.filter.as_ref(), req.ordering, req.limit, &req.matchers)?;
        let user_id = &search_tenant_id(org_id)?;
        self.handle_search_label_names_with_hints(md, user_id, req, hints, matchers)
            .await
    }

    /// `SearchLabelNames` after its input was validated by [`build_search_hints`] (Go validates
    /// before the tenant and the deferred read error mapping).
    pub async fn handle_search_label_names_with_hints_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelNamesRequest,
        hints: SearchHints,
        matchers: Vec<LabelMatcher>,
    ) -> Result<ReceiverStream<Result<ingesterpb::SearchResultBatch, Status>>, IngesterError> {
        self.handle_search_label_names_with_hints_rx_with_sink(
            md,
            user_id,
            req,
            hints,
            matchers,
            ReadErrorSink::default(),
        )
        .await
    }

    pub(crate) async fn handle_search_label_names_with_hints_rx_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelNamesRequest,
        hints: SearchHints,
        matchers: Vec<LabelMatcher>,
        err_sink: ReadErrorSink,
    ) -> Result<ReceiverStream<Result<ingesterpb::SearchResultBatch, Status>>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }
        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                let (_tx, rx) = mpsc::channel(1);
                return Ok(ReceiverStream::new(rx));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let (tx, rx) = mpsc::channel(8);
        tokio::task::spawn_blocking(move || {
            let q = db.db.querier(start_ts, end_ts);
            let results = match q.search_label_names(&hints, &matchers) {
                Ok(r) => r,
                Err(e) => {
                    let err = IngesterError::Internal { message: e.to_string() };
                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                    permit.finish(false);
                    return;
                }
            };
            let mut rs = VecSearchResultSet::new(results, Vec::new(), None);
            let mut receiver_closed = false;
            let stream_res = stream_search_results(
                &|| false,
                &mut rs,
                &mut |b| {
                    if tx.blocking_send(Ok(b)).is_err() {
                        receiver_closed = true;
                        return Err("receiver closed".to_string());
                    }
                    Ok(())
                },
            );
            if let Err(message) = stream_res {
                if !receiver_closed {
                    let err = IngesterError::Internal { message };
                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                }
            }
            permit.finish(false);
        });
        Ok(ReceiverStream::new(rx))
    }

    pub async fn handle_search_label_names_with_hints(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelNamesRequest,
        hints: SearchHints,
        matchers: Vec<LabelMatcher>,
    ) -> Result<Vec<ingesterpb::SearchResultBatch>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = async {
            self.enforce_read_consistency(md, user_id).await?;
            let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
                return Ok(Vec::new());
            };
            let start_ts = req.start_timestamp_ms;
            let end_ts = req.end_timestamp_ms;
            let results = tokio::task::spawn_blocking(move || {
                let q = db.db.querier(start_ts, end_ts);
                q.search_label_names(&hints, &matchers)
            })
            .await
            .map_err(|e| IngesterError::Internal {
                message: format!("search_label_names task join error: {e}"),
            })?
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
        permit.finish(deadline_exceeded(&res));
        res
    }

    /// See [`Self::handle_search_label_names`] for `org_id`.
    pub async fn handle_search_label_values(
        &self,
        md: &MetadataMap,
        org_id: &str,
        req: ingesterpb::SearchLabelValuesRequest,
    ) -> Result<Vec<ingesterpb::SearchResultBatch>, IngesterError> {
        let (hints, matchers) =
            build_search_hints(req.filter.as_ref(), req.ordering, req.limit, &req.matchers)?;
        let user_id = &search_tenant_id(org_id)?;
        self.handle_search_label_values_with_hints(md, user_id, req, hints, matchers)
            .await
    }

    /// `SearchLabelValues` after its input was validated by [`build_search_hints`] (Go validates
    /// before the tenant and the deferred read error mapping).
    pub async fn handle_search_label_values_with_hints_rx(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelValuesRequest,
        hints: SearchHints,
        matchers: Vec<LabelMatcher>,
    ) -> Result<ReceiverStream<Result<ingesterpb::SearchResultBatch, Status>>, IngesterError> {
        self.handle_search_label_values_with_hints_rx_with_sink(
            md,
            user_id,
            req,
            hints,
            matchers,
            ReadErrorSink::default(),
        )
        .await
    }

    pub(crate) async fn handle_search_label_values_with_hints_rx_with_sink(
        self: &Arc<Self>,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelValuesRequest,
        hints: SearchHints,
        matchers: Vec<LabelMatcher>,
        err_sink: ReadErrorSink,
    ) -> Result<ReceiverStream<Result<ingesterpb::SearchResultBatch, Status>>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        if let Err(e) = self.enforce_read_consistency(md, user_id).await {
            permit.finish(e.is_deadline_exceeded());
            return Err(e);
        }
        let db = match self.get_open_tsdb_for_query(user_id) {
            Ok(Some(db)) => db,
            Ok(None) => {
                permit.finish(false);
                let (_tx, rx) = mpsc::channel(1);
                return Ok(ReceiverStream::new(rx));
            }
            Err(e) => {
                permit.finish(e.is_deadline_exceeded());
                return Err(e);
            }
        };
        let start_ts = req.start_timestamp_ms;
        let end_ts = req.end_timestamp_ms;
        let name = req.name;
        let (tx, rx) = mpsc::channel(8);
        tokio::task::spawn_blocking(move || {
            let q = db.db.querier(start_ts, end_ts);
            let results = match q.search_label_values(&name, &hints, &matchers) {
                Ok(r) => r,
                Err(e) => {
                    let err = IngesterError::Internal { message: e.to_string() };
                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                    permit.finish(false);
                    return;
                }
            };
            let mut rs = VecSearchResultSet::new(results, Vec::new(), None);
            let mut receiver_closed = false;
            let stream_res = stream_search_results(
                &|| false,
                &mut rs,
                &mut |b| {
                    if tx.blocking_send(Ok(b)).is_err() {
                        receiver_closed = true;
                        return Err("receiver closed".to_string());
                    }
                    Ok(())
                },
            );
            if let Err(message) = stream_res {
                if !receiver_closed {
                    let err = IngesterError::Internal { message };
                    let _ = tx.blocking_send(Err(err_sink.map_read_error(err)));
                }
            }
            permit.finish(false);
        });
        Ok(ReceiverStream::new(rx))
    }

    pub async fn handle_search_label_values_with_hints(
        &self,
        md: &MetadataMap,
        user_id: &str,
        req: ingesterpb::SearchLabelValuesRequest,
        hints: SearchHints,
        matchers: Vec<LabelMatcher>,
    ) -> Result<Vec<ingesterpb::SearchResultBatch>, IngesterError> {
        let permit = self.start_read_request(md).await?;
        let res = async {
            self.enforce_read_consistency(md, user_id).await?;
            let Some(db) = self.get_open_tsdb_for_query(user_id)? else {
                return Ok(Vec::new());
            };
            let start_ts = req.start_timestamp_ms;
            let end_ts = req.end_timestamp_ms;
            let name = req.name;
            let results = tokio::task::spawn_blocking(move || {
                let q = db.db.querier(start_ts, end_ts);
                q.search_label_values(&name, &hints, &matchers)
            })
            .await
            .map_err(|e| IngesterError::Internal {
                message: format!("search_label_values task join error: {e}"),
            })?
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
        permit.finish(deadline_exceeded(&res));
        res
    }
}

/// Go `ActiveSeries`: `fmt.Errorf("error parsing label matchers: %w", err)`.
fn active_series_matchers(
    raw: &[ingesterpb::LabelMatcher],
) -> Result<Vec<LabelMatcher>, IngesterError> {
    from_proto_matchers(raw).map_err(|e| IngesterError::Internal {
        message: format!("error parsing label matchers: {e}"),
    })
}

/// dskit `tenant.TenantID` inside the search handlers (plain error -> `codes.Internal`).
fn search_tenant_id(org_id: &str) -> Result<String, IngesterError> {
    mimir_common::tenant::parse_single_tenant_id(org_id).map_err(|e| IngesterError::Internal {
        message: e.to_string(),
    })
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
                message: format!("unknown count method \"{count_method}\""),
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

pub(crate) fn merge_per_block_chunk_series(
    mut sources: Vec<Vec<QueryChunkSeries>>,
    unordered: bool,
    float_encoding: Encoding,
) -> Result<Vec<QueryChunkSeries>, IngesterError> {
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    if sources.len() == 1 {
        let mut single = sources.pop().unwrap_or_default();
        single.sort_by(|a, b| a.labels.cmp(&b.labels));
        return Ok(single);
    }
    let mut by_labels: BTreeMap<Labels, Vec<ChunkMeta>> = BTreeMap::new();
    for src in sources {
        for cs in src {
            by_labels.entry(cs.labels).or_default().extend(cs.chunks);
        }
    }
    let mut out = Vec::with_capacity(by_labels.len());
    for (labels, mut chunks) in by_labels {
        if !unordered && chunks.len() > 1 {
            chunks.sort_by_key(|c| (c.min_time, c.max_time));
            let has_overlap = chunks.windows(2).any(|w| w[1].min_time <= w[0].max_time);
            if has_overlap {
                chunks = compact_chunk_series_merge(chunks, float_encoding).map_err(|e| {
                    IngesterError::Internal {
                        message: format!("merging block chunks: {e}"),
                    }
                })?;
            }
        }
        out.push(QueryChunkSeries { labels, chunks });
    }
    Ok(out)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlannedSelectMeta {
    pub estimated_postings: u64,
    pub estimated_final: u64,
    pub is_secondary_unplanned: bool,
    pub has_index_metric_name: bool,
}

pub type EstimatesSink = Arc<parking_lot::Mutex<Vec<PlannedSelectMeta>>>;

enum PlannedSource {
    All,
    HeadInOrder {
        guard: Option<HeadReadGuard>,
        isolation: HeadReadIsolation,
        overlaps_in_order: bool,
        overlaps_ooo: bool,
    },
    SingleBlock {
        block: Arc<Block>,
        guard: Option<BlockReadGuard>,
    },
}

pub struct PlannedTsdbChunkQuerier {
    db: Arc<Tsdb>,
    source: PlannedSource,
    mint: i64,
    maxt: i64,
    planner: Arc<dyn LookupPlanner>,
    unordered: bool,
    select_stats: Option<(BlockSelectStatsSink, Option<SeriesStatsFilter>)>,
    estimates_sink: Option<EstimatesSink>,
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
            source: PlannedSource::All,
            mint,
            maxt,
            planner,
            unordered,
            select_stats: None,
            estimates_sink: None,
        }
    }

    pub fn new_head(
        db: Arc<Tsdb>,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        unordered: bool,
    ) -> Self {
        match db.snapshot_head_query_source(mint, maxt) {
            Some(hs) => Self::new_head_with_snapshot(db, mint, maxt, planner, unordered, hs),
            None => Self {
                db,
                source: PlannedSource::HeadInOrder {
                    guard: None,
                    isolation: HeadReadIsolation::default(),
                    overlaps_in_order: false,
                    overlaps_ooo: false,
                },
                mint,
                maxt,
                planner,
                unordered,
                select_stats: None,
                estimates_sink: None,
            },
        }
    }

    pub fn new_head_with_snapshot(
        db: Arc<Tsdb>,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        unordered: bool,
        snap: HeadQuerySnapshot,
    ) -> Self {
        Self {
            db,
            source: PlannedSource::HeadInOrder {
                guard: Some(snap.guard),
                isolation: snap.isolation,
                overlaps_in_order: snap.overlaps_in_order,
                overlaps_ooo: snap.overlaps_ooo,
            },
            mint,
            maxt,
            planner,
            unordered,
            select_stats: None,
            estimates_sink: None,
        }
    }

    pub fn new_block(
        db: Arc<Tsdb>,
        block: Arc<Block>,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        unordered: bool,
    ) -> Self {
        let guard = Block::start_read(&block);
        Self {
            db,
            source: PlannedSource::SingleBlock { block, guard },
            mint,
            maxt,
            planner,
            unordered,
            select_stats: None,
            estimates_sink: None,
        }
    }

    pub fn new_block_with_guard(
        db: Arc<Tsdb>,
        block: Arc<Block>,
        guard: BlockReadGuard,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        unordered: bool,
    ) -> Self {
        Self {
            db,
            source: PlannedSource::SingleBlock {
                block,
                guard: Some(guard),
            },
            mint,
            maxt,
            planner,
            unordered,
            select_stats: None,
            estimates_sink: None,
        }
    }

    /// Records per block querier stats of every select into `sink` (counting only series
    /// accepted by `filter`).
    pub fn with_select_stats(
        mut self,
        sink: BlockSelectStatsSink,
        filter: Option<SeriesStatsFilter>,
    ) -> Self {
        self.select_stats = Some((sink, filter));
        self
    }

    pub fn with_estimates_sink(mut self, sink: EstimatesSink) -> Self {
        self.estimates_sink = Some(sink);
        self
    }

    fn record_planned_stats(
        &self,
        planning_disabled: bool,
        q_stats: &QueryStats,
        index_matchers: &[LabelMatcher],
        stat: BlockSelectStats,
    ) {
        if let Some((sink, _)) = &self.select_stats {
            sink.lock().push(stat);
            if let Some(ref est_sink) = self.estimates_sink {
                let (est_p, est_f) = if planning_disabled {
                    (0, 0)
                } else {
                    (
                        QueryStats::load_estimated_selected_postings(Some(q_stats)),
                        QueryStats::load_estimated_final_cardinality(Some(q_stats)),
                    )
                };
                est_sink.lock().push(PlannedSelectMeta {
                    estimated_postings: est_p,
                    estimated_final: est_f,
                    is_secondary_unplanned: planning_disabled,
                    has_index_metric_name:
                        mimir_tsdb::postings_cache::exact_metric_name_from_matchers(index_matchers)
                            .is_some(),
                });
            }
        }
    }

    fn select_head_in_order(
        &self,
        sort_series: bool,
        shard: Option<(u64, u64)>,
        plan: &BasicLookupPlan,
        planning_disabled: bool,
        q_stats: &QueryStats,
    ) -> Box<dyn ChunkSeriesSet> {
        let PlannedSource::HeadInOrder {
            guard,
            isolation,
            overlaps_in_order,
            overlaps_ooo,
        } = &self.source
        else {
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        };
        if guard.is_none() {
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        }
        let head = self.db.head();
        let head_ooo = *overlaps_ooo;
        let ino_mint = isolation.ino_mint;
        if !*overlaps_in_order || !head.initialized() || head.is_closed() {
            self.record_planned_stats(
                planning_disabled,
                q_stats,
                &plan.index_matchers,
                BlockSelectStats {
                    selected_postings: 0,
                    series: 0,
                    head: true,
                    overlaps_ooo: head_ooo,
                    block_min_time: 0,
                },
            );
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        }
        let refs = match head
            .postings_for_matchers_with_concurrent(&plan.index_matchers, shard.is_some())
        {
            Ok(r) => r,
            Err(e) => {
                return Box::new(VecChunkSeriesSet {
                    items: Vec::new(),
                    pos: 0,
                    err: Some(QuerierError::Other(e.to_string())),
                });
            }
        };
        let mut raw_items: Vec<TsdbChunkSeriesItem> = Vec::with_capacity(refs.len());
        let mut chks_buf = Vec::new();
        let mut tombs_buf = Vec::new();
        for &r in refs.iter() {
            let Some(labels) = head.series_labels(r) else {
                continue;
            };
            if let Some((shard_idx, shard_count)) = shard {
                if labels_stable_hash(&labels) % shard_count != shard_idx {
                    continue;
                }
            }
            if !plan
                .scan_matchers
                .iter()
                .all(|m| m.matches(labels.get(&m.name)))
            {
                continue;
            }
            chks_buf.clear();
            tombs_buf.clear();
            if let Err(e) = head.load_series_in_order_chunks_into_with_isolation(
                r,
                ino_mint,
                self.maxt,
                isolation,
                &mut chks_buf,
                &mut tombs_buf,
            ) {
                return Box::new(VecChunkSeriesSet {
                    items: Vec::new(),
                    pos: 0,
                    err: Some(QuerierError::Other(e.to_string())),
                });
            }
            if chks_buf.is_empty() {
                continue;
            }
            // Go `populateChunksFromIterable` (`tsdb/querier.go:1271`) uses `EncXOR` for
            // intra-head merges when `st == 0`, regardless of `float_chunk_encoding`.
            let final_chunks = match merge_head_chunks_with_tombstones(
                std::mem::take(&mut chks_buf),
                &tombs_buf,
            ) {
                Ok(c) => c,
                Err(e) => {
                    return Box::new(VecChunkSeriesSet {
                        items: Vec::new(),
                        pos: 0,
                        err: Some(QuerierError::Other(e.to_string())),
                    });
                }
            };
            if !final_chunks.is_empty() {
                raw_items.push(TsdbChunkSeriesItem {
                    labels,
                    chunks: final_chunks,
                });
            }
        }
        if sort_series {
            raw_items.sort_by(|a, b| a.labels.cmp(&b.labels));
        }
        let items: Vec<Arc<dyn ChunkSeries>> = raw_items
            .into_iter()
            .map(|it| Arc::new(it) as Arc<dyn ChunkSeries>)
            .collect();
        self.record_planned_stats(
            planning_disabled,
            q_stats,
            &plan.index_matchers,
            BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: items.len() as u64,
                head: true,
                overlaps_ooo: head_ooo,
                block_min_time: 0,
            },
        );
        Box::new(VecChunkSeriesSet {
            items,
            pos: 0,
            err: None,
        })
    }

    fn select_single_block(
        &self,
        sort_series: bool,
        shard: Option<(u64, u64)>,
        plan: &BasicLookupPlan,
        planning_disabled: bool,
        q_stats: &QueryStats,
    ) -> Box<dyn ChunkSeriesSet> {
        let PlannedSource::SingleBlock { block, guard } = &self.source else {
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        };
        if guard.is_none() || !block.overlaps_closed_interval(self.mint, self.maxt) {
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        }
        let refs = match block
            .postings_for_matchers_with_concurrent(&plan.index_matchers, shard.is_some())
        {
            Ok(r) => r,
            Err(e) => {
                return Box::new(VecChunkSeriesSet {
                    items: Vec::new(),
                    pos: 0,
                    err: Some(QuerierError::Other(e.to_string())),
                });
            }
        };
        let idx = block.index();
        let chkr = block.chunks();
        let tombr = block.tombstones();
        let float_enc = self.db.head().float_chunk_encoding();
        let mut raw_items: Vec<TsdbChunkSeriesItem> = Vec::with_capacity(refs.len());
        for &r in refs.iter() {
            let (labels, chks) = match idx.series(r) {
                Ok(v) => v,
                Err(e) => {
                    return Box::new(VecChunkSeriesSet {
                        items: Vec::new(),
                        pos: 0,
                        err: Some(QuerierError::Other(e.to_string())),
                    });
                }
            };
            if let Some((shard_idx, shard_count)) = shard {
                if labels_stable_hash(&labels) % shard_count != shard_idx {
                    continue;
                }
            }
            if !plan
                .scan_matchers
                .iter()
                .all(|m| m.matches(labels.get(&m.name)))
            {
                continue;
            }
            let tombs = tombr.get(r);
            let mut loaded = Vec::new();
            for mut cm in chks {
                if !cm.overlaps_closed_interval(self.mint, self.maxt) {
                    continue;
                }
                if tombs
                    .iter()
                    .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
                {
                    continue;
                }
                match chkr.chunk(cm.ref_id) {
                    Ok(chk) => cm.chunk = Some(chk),
                    Err(e) => {
                        return Box::new(VecChunkSeriesSet {
                            items: Vec::new(),
                            pos: 0,
                            err: Some(QuerierError::Other(e.to_string())),
                        });
                    }
                }
                loaded.push((cm, tombs.clone()));
            }
            if loaded.is_empty() {
                continue;
            }
            let final_chunks = if self.unordered {
                match merge_chunks_with_tombstones_and_float_encoding(loaded, true, float_enc) {
                    Ok(c) => c,
                    Err(e) => {
                        return Box::new(VecChunkSeriesSet {
                            items: Vec::new(),
                            pos: 0,
                            err: Some(QuerierError::Other(e.to_string())),
                        });
                    }
                }
            } else {
                let mut reencoded = Vec::with_capacity(loaded.len());
                for (cm, tombs_item) in loaded {
                    match reencode_chunk_without_tombstones(cm, &tombs_item) {
                        Ok(Some(c)) => reencoded.push(c),
                        Ok(None) => {}
                        Err(e) => {
                            return Box::new(VecChunkSeriesSet {
                                items: Vec::new(),
                                pos: 0,
                                err: Some(QuerierError::Other(e.to_string())),
                            });
                        }
                    }
                }
                match compact_chunk_series_merge(reencoded, float_enc) {
                    Ok(c) => c,
                    Err(e) => {
                        return Box::new(VecChunkSeriesSet {
                            items: Vec::new(),
                            pos: 0,
                            err: Some(QuerierError::Other(e.to_string())),
                        });
                    }
                }
            };
            if !final_chunks.is_empty() {
                raw_items.push(TsdbChunkSeriesItem {
                    labels,
                    chunks: final_chunks,
                });
            }
        }
        if sort_series {
            raw_items.sort_by(|a, b| a.labels.cmp(&b.labels));
        }
        let items: Vec<Arc<dyn ChunkSeries>> = raw_items
            .into_iter()
            .map(|it| Arc::new(it) as Arc<dyn ChunkSeries>)
            .collect();
        self.record_planned_stats(
            planning_disabled,
            q_stats,
            &plan.index_matchers,
            BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: items.len() as u64,
                head: false,
                overlaps_ooo: false,
                block_min_time: block.min_time(),
            },
        );
        Box::new(VecChunkSeriesSet {
            items,
            pos: 0,
            err: None,
        })
    }
}

impl ChunkQuerier for PlannedTsdbChunkQuerier {
    fn select(
        &mut self,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet> {
        let planning_disabled = lookupplan::is_planning_disabled_in_context();
        let q_stats = Arc::new(QueryStats::new());
        let shard = hints.and_then(|h| {
            if h.shard_count > 0 {
                Some((h.shard_index, h.shard_count))
            } else {
                None
            }
        });
        let plan = if planning_disabled {
            BasicLookupPlan::new(matchers.to_vec(), Vec::new())
        } else {
            let in_plan = BasicLookupPlan::new(matchers.to_vec(), Vec::new());
            match lookupplan::with_query_stats_context(Some(Arc::clone(&q_stats)), || {
                self.planner.plan_index_lookup(&in_plan, hints)
            }) {
                Ok(p) => p,
                Err(_) => in_plan,
            }
        };

        match &self.source {
            PlannedSource::HeadInOrder { .. } => {
                return self.select_head_in_order(
                    sort_series,
                    shard,
                    &plan,
                    planning_disabled,
                    &q_stats,
                );
            }
            PlannedSource::SingleBlock { .. } => {
                return self.select_single_block(
                    sort_series,
                    shard,
                    &plan,
                    planning_disabled,
                    &q_stats,
                );
            }
            PlannedSource::All => {}
        }

        let mut cq = if self.unordered {
            self.db.unordered_chunk_querier(self.mint, self.maxt)
        } else {
            self.db.chunk_querier(self.mint, self.maxt)
        }
        .with_concurrent(shard.is_some());
        let stats_len_before = self.select_stats.as_ref().map(|(s, _)| s.lock().len());
        if let Some((sink, filter)) = &self.select_stats {
            cq = cq.with_filtered_select_stats(Arc::clone(sink), filter.clone());
        }
        match cq.select(sort_series, &plan.index_matchers) {
            Ok(series) => {
                let items: Vec<Arc<dyn ChunkSeries>> = series
                    .into_iter()
                    .filter(|cs| {
                        shard.is_none_or(|(shard_idx, shard_count)| {
                            labels_stable_hash(&cs.labels) % shard_count == shard_idx
                        }) && plan
                            .scan_matchers
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
                if let (Some((sink, _)), Some(before)) = (&self.select_stats, stats_len_before) {
                    let mut g = sink.lock();
                    let added = g.len().saturating_sub(before);
                    if added == 1 && !plan.scan_matchers.is_empty() {
                        g[before].series = items.len() as u64;
                    }
                    if let Some(ref est_sink) = self.estimates_sink {
                        let (est_p, est_f) = if planning_disabled {
                            (0, 0)
                        } else {
                            (
                                QueryStats::load_estimated_selected_postings(Some(&q_stats)),
                                QueryStats::load_estimated_final_cardinality(Some(&q_stats)),
                            )
                        };
                        let has_index_metric_name =
                            mimir_tsdb::postings_cache::exact_metric_name_from_matchers(
                                &plan.index_matchers,
                            )
                            .is_some();
                        let mut eg = est_sink.lock();
                        for _ in 0..added {
                            eg.push(PlannedSelectMeta {
                                estimated_postings: est_p,
                                estimated_final: est_f,
                                is_secondary_unplanned: planning_disabled,
                                has_index_metric_name,
                            });
                        }
                    }
                }
                Box::new(VecChunkSeriesSet {
                    items,
                    pos: 0,
                    err: None,
                })
            }
            Err(e) => {
                if let (Some((sink, _)), Some(before)) = (&self.select_stats, stats_len_before) {
                    sink.lock().truncate(before);
                }
                Box::new(VecChunkSeriesSet {
                    items: Vec::new(),
                    pos: 0,
                    err: Some(QuerierError::Other(e.to_string())),
                })
            }
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
        match &mut self.source {
            PlannedSource::All => {}
            PlannedSource::HeadInOrder { guard, .. } => {
                *guard = None;
            }
            PlannedSource::SingleBlock { guard, .. } => {
                *guard = None;
            }
        }
        Ok(())
    }
}

pub struct MirroredChunkQuerierAdapter(pub MirroredChunkQuerier);

impl ChunkQuerier for MirroredChunkQuerierAdapter {
    fn select(
        &mut self,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet> {
        Box::new(self.0.select(sort_series, hints, matchers))
    }

    fn label_values(
        &mut self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        self.0.label_values(name, matchers)
    }

    fn label_names(
        &mut self,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        self.0.label_names(matchers)
    }

    fn close(&mut self) -> Result<(), QuerierError> {
        self.0.close()
    }
}

/// Port of Go `tsdb.HeadAndOOOChunkQuerier` (`tsdb/ooo_head_read.go:708-760`):
/// queries Head + OOO Head together once via `LookupPlanner`, merges overlapping
/// in-order and OOO Head chunks per cluster, and closes the wrapped inner Head querier
/// without invoking its `select()` (recording `outcome="no_select"` when mirrored).
pub struct HeadAndOOOChunkQuerier {
    db: Arc<Tsdb>,
    mint: i64,
    maxt: i64,
    planner: Arc<dyn LookupPlanner>,
    guard: Option<HeadReadGuard>,
    isolation: HeadReadIsolation,
    overlaps_in_order: bool,
    overlaps_ooo: bool,
    inner: Option<Box<dyn ChunkQuerier>>,
    select_stats: Option<(BlockSelectStatsSink, Option<SeriesStatsFilter>)>,
    estimates_sink: Option<EstimatesSink>,
}

impl HeadAndOOOChunkQuerier {
    pub fn new(
        db: Arc<Tsdb>,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        inner: Option<Box<dyn ChunkQuerier>>,
    ) -> Self {
        match db.snapshot_head_query_source(mint, maxt) {
            Some(hs) => Self::new_with_snapshot(db, mint, maxt, planner, inner, hs),
            None => Self {
                db,
                mint,
                maxt,
                planner,
                guard: None,
                isolation: HeadReadIsolation::default(),
                overlaps_in_order: false,
                overlaps_ooo: false,
                inner,
                select_stats: None,
                estimates_sink: None,
            },
        }
    }

    pub fn new_with_snapshot(
        db: Arc<Tsdb>,
        mint: i64,
        maxt: i64,
        planner: Arc<dyn LookupPlanner>,
        inner: Option<Box<dyn ChunkQuerier>>,
        snap: HeadQuerySnapshot,
    ) -> Self {
        Self {
            db,
            mint,
            maxt,
            planner,
            guard: Some(snap.guard),
            isolation: snap.isolation,
            overlaps_in_order: snap.overlaps_in_order,
            overlaps_ooo: snap.overlaps_ooo,
            inner,
            select_stats: None,
            estimates_sink: None,
        }
    }

    pub fn with_select_stats(
        mut self,
        sink: BlockSelectStatsSink,
        filter: Option<SeriesStatsFilter>,
    ) -> Self {
        self.select_stats = Some((sink, filter));
        self
    }

    pub fn with_estimates_sink(mut self, sink: EstimatesSink) -> Self {
        self.estimates_sink = Some(sink);
        self
    }

    fn record_planned_stats(
        &self,
        planning_disabled: bool,
        q_stats: &QueryStats,
        index_matchers: &[LabelMatcher],
        stat: BlockSelectStats,
    ) {
        if let Some((sink, _)) = &self.select_stats {
            sink.lock().push(stat);
            if let Some(ref est_sink) = self.estimates_sink {
                let (est_p, est_f) = if planning_disabled {
                    (0, 0)
                } else {
                    (
                        QueryStats::load_estimated_selected_postings(Some(q_stats)),
                        QueryStats::load_estimated_final_cardinality(Some(q_stats)),
                    )
                };
                est_sink.lock().push(PlannedSelectMeta {
                    estimated_postings: est_p,
                    estimated_final: est_f,
                    is_secondary_unplanned: planning_disabled,
                    has_index_metric_name:
                        mimir_tsdb::postings_cache::exact_metric_name_from_matchers(index_matchers)
                            .is_some(),
                });
            }
        }
    }
}

impl ChunkQuerier for HeadAndOOOChunkQuerier {
    fn select(
        &mut self,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet> {
        if self.guard.is_none() {
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        }
        let planning_disabled = lookupplan::is_planning_disabled_in_context();
        let q_stats = Arc::new(QueryStats::new());
        let shard = hints.and_then(|h| {
            if h.shard_count > 0 {
                Some((h.shard_index, h.shard_count))
            } else {
                None
            }
        });
        let plan = if planning_disabled {
            BasicLookupPlan::new(matchers.to_vec(), Vec::new())
        } else {
            let in_plan = BasicLookupPlan::new(matchers.to_vec(), Vec::new());
            match lookupplan::with_query_stats_context(Some(Arc::clone(&q_stats)), || {
                self.planner.plan_index_lookup(&in_plan, hints)
            }) {
                Ok(p) => p,
                Err(_) => in_plan,
            }
        };

        let head = self.db.head();
        let head_ooo = self.overlaps_ooo;
        if head.is_closed()
            || (!head_ooo && (!self.overlaps_in_order || !head.initialized()))
        {
            self.record_planned_stats(
                planning_disabled,
                &q_stats,
                &plan.index_matchers,
                BlockSelectStats {
                    selected_postings: 0,
                    series: 0,
                    head: true,
                    overlaps_ooo: head_ooo,
                    block_min_time: 0,
                },
            );
            return Box::new(VecChunkSeriesSet {
                items: Vec::new(),
                pos: 0,
                err: None,
            });
        }

        let refs = match head
            .postings_for_matchers_with_concurrent(&plan.index_matchers, shard.is_some())
        {
            Ok(r) => r,
            Err(e) => {
                return Box::new(VecChunkSeriesSet {
                    items: Vec::new(),
                    pos: 0,
                    err: Some(QuerierError::Other(e.to_string())),
                });
            }
        };

        let mut raw_items: Vec<TsdbChunkSeriesItem> = Vec::with_capacity(refs.len());
        for &r in refs.iter() {
            let Some(labels) = head.series_labels(r) else {
                continue;
            };
            if let Some((shard_idx, shard_count)) = shard {
                if labels_stable_hash(&labels) % shard_count != shard_idx {
                    continue;
                }
            }
            if !plan
                .scan_matchers
                .iter()
                .all(|m| m.matches(labels.get(&m.name)))
            {
                continue;
            }
            let final_chunks = match head.series_final_chunks_only_with_isolation(
                r,
                self.mint,
                self.maxt,
                &self.isolation,
            ) {
                Ok(c) => c,
                Err(e) => {
                    return Box::new(VecChunkSeriesSet {
                        items: Vec::new(),
                        pos: 0,
                        err: Some(QuerierError::Other(e.to_string())),
                    });
                }
            };
            if !final_chunks.is_empty() {
                raw_items.push(TsdbChunkSeriesItem {
                    labels,
                    chunks: final_chunks,
                });
            }
        }

        if sort_series {
            raw_items.sort_by(|a, b| a.labels.cmp(&b.labels));
        }
        let items: Vec<Arc<dyn ChunkSeries>> = raw_items
            .into_iter()
            .map(|it| Arc::new(it) as Arc<dyn ChunkSeries>)
            .collect();
        self.record_planned_stats(
            planning_disabled,
            &q_stats,
            &plan.index_matchers,
            BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: items.len() as u64,
                head: true,
                overlaps_ooo: head_ooo,
                block_min_time: 0,
            },
        );
        Box::new(VecChunkSeriesSet {
            items,
            pos: 0,
            err: None,
        })
    }

    fn label_values(
        &mut self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        match &mut self.inner {
            Some(q) => q.label_values(name, matchers),
            None => Ok((Vec::new(), Vec::new())),
        }
    }

    fn label_names(
        &mut self,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        match &mut self.inner {
            Some(q) => q.label_names(matchers),
            None => Ok((Vec::new(), Vec::new())),
        }
    }

    fn close(&mut self) -> Result<(), QuerierError> {
        self.guard = None;
        match &mut self.inner {
            Some(q) => q.close(),
            None => Ok(()),
        }
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

/// Shared tenant-fair compute worker pool (Go: `pkg/util/workerpool.Pool`).
pub use crate::workerpool::Pool as ComputeWorkerPool;

/// Go: `dimensionLabelValuesSeriesCount`.
pub const DIMENSION_LABEL_VALUES_SERIES_COUNT: &str = "label-values-series-count";

/// Counts the series for one label value.
pub type LabelValueCountFn =
    Arc<dyn Fn(&str) -> Result<u64, LabelNamesAndValuesError> + Send + Sync>;

/// Go: `computeLabelValuesSeriesCount`, generic over the per-value counting function.
///
/// Values are split into `chunk_size` chunks, each submitted to the shared pool on behalf of
/// `tenant_id`. Results arrive in completion order; the receiver ends once every submitted chunk
/// has finished. Submission stops at the first cancellation (`context canceled`) or pool error.
#[allow(clippy::too_many_arguments)]
fn submit_label_values_chunk(
    pool: &ComputeWorkerPool,
    tenant_id: &str,
    lbl_values: &Arc<Vec<String>>,
    range: std::ops::Range<usize>,
    is_canceled: &Arc<dyn Fn() -> bool + Send + Sync>,
    count_fn: &LabelValueCountFn,
    tx: &mpsc::Sender<LabelValueCountResult>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<(), crate::workerpool::SubmitError> {
    let (values, cancel, count_fn, tx_c) = (
        Arc::clone(lbl_values),
        Arc::clone(is_canceled),
        Arc::clone(count_fn),
        tx.clone(),
    );
    pool.submit_with_done(
        DIMENSION_LABEL_VALUES_SERIES_COUNT,
        tenant_id,
        move || {
            for v in &values[range] {
                let res = if cancel() {
                    Err(LabelNamesAndValuesError::Canceled)
                } else {
                    count_fn(v)
                };
                let msg = match res {
                    Ok(count) => LabelValueCountResult {
                        val: v.clone(),
                        count,
                        err: None,
                    },
                    Err(err) => LabelValueCountResult {
                        val: v.clone(),
                        count: 0,
                        err: Some(err),
                    },
                };
                if tx_c.blocking_send(msg).is_err() {
                    break;
                }
            }
            (tx_c, permit)
        },
        drop,
    )
}

/// Go: `computeLabelValuesSeriesCount`, generic over the per-value counting function.
///
/// Values are split into `chunk_size` chunks, each submitted to the shared pool on behalf of
/// `tenant_id`. Results arrive in completion order; the receiver ends once every submitted chunk
/// has finished. Submission stops at the first cancellation (`context canceled`) or pool error.
pub fn compute_label_values_counts(
    is_canceled: Arc<dyn Fn() -> bool + Send + Sync>,
    pool: &Arc<ComputeWorkerPool>,
    tenant_id: &str,
    chunk_size: usize,
    lbl_values: Arc<Vec<String>>,
    count_fn: LabelValueCountFn,
) -> mpsc::Receiver<LabelValueCountResult> {
    let chunk_size = chunk_size.max(1);
    let chan_cap = chunk_size.saturating_mul(4).clamp(64, 4096);
    let (tx, rx) = mpsc::channel(chan_cap);
    if lbl_values.is_empty() {
        return rx;
    }
    let pool = Arc::clone(pool);
    let tenant_id = tenant_id.to_string();
    let max_outstanding = (pool.workers() / 2).clamp(1, 16);
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let sem = Arc::new(tokio::sync::Semaphore::new(max_outstanding));
        handle.spawn(async move {
            let mut start = 0usize;
            while start < lbl_values.len() {
                if tx.is_closed() {
                    return;
                }
                if is_canceled() {
                    let _ = tx
                        .send(LabelValueCountResult {
                            val: String::new(),
                            count: 0,
                            err: Some(LabelNamesAndValuesError::Canceled),
                        })
                        .await;
                    return;
                }
                let permit = tokio::select! {
                    p = Arc::clone(&sem).acquire_owned() => match p {
                        Ok(p) => p,
                        Err(_) => return,
                    },
                    _ = tx.closed() => return,
                };
                let end = (start + chunk_size).min(lbl_values.len());
                if let Err(submit_err) = submit_label_values_chunk(
                    &pool,
                    &tenant_id,
                    &lbl_values,
                    start..end,
                    &is_canceled,
                    &count_fn,
                    &tx,
                    Some(permit),
                ) {
                    let err = match submit_err {
                        crate::workerpool::SubmitError::PoolStopped => {
                            LabelNamesAndValuesError::PoolStopped
                        }
                        crate::workerpool::SubmitError::InvalidTenantId => {
                            LabelNamesAndValuesError::Other(submit_err.to_string())
                        }
                    };
                    let _ = tx
                        .send(LabelValueCountResult {
                            val: String::new(),
                            count: 0,
                            err: Some(err),
                        })
                        .await;
                    return;
                }
                start = end;
            }
        });
    } else {
        std::thread::spawn(move || {
            let mut start = 0usize;
            while start < lbl_values.len() {
                if tx.is_closed() {
                    return;
                }
                if is_canceled() {
                    let _ = tx.blocking_send(LabelValueCountResult {
                        val: String::new(),
                        count: 0,
                        err: Some(LabelNamesAndValuesError::Canceled),
                    });
                    return;
                }
                let end = (start + chunk_size).min(lbl_values.len());
                if let Err(submit_err) = submit_label_values_chunk(
                    &pool,
                    &tenant_id,
                    &lbl_values,
                    start..end,
                    &is_canceled,
                    &count_fn,
                    &tx,
                    None,
                ) {
                    let err = match submit_err {
                        crate::workerpool::SubmitError::PoolStopped => {
                            LabelNamesAndValuesError::PoolStopped
                        }
                        crate::workerpool::SubmitError::InvalidTenantId => {
                            LabelNamesAndValuesError::Other(submit_err.to_string())
                        }
                    };
                    let _ = tx.blocking_send(LabelValueCountResult {
                        val: String::new(),
                        count: 0,
                        err: Some(err),
                    });
                    return;
                }
                start = end;
            }
        });
    }
    rx
}

/// Go: `computeLabelValuesSeriesCount` (postings-based counting via `countLabelValueSeries`).
#[allow(clippy::too_many_arguments)]
pub fn compute_label_values_series_count(
    is_canceled: Arc<dyn Fn() -> bool + Send + Sync>,
    pool: &Arc<ComputeWorkerPool>,
    tenant_id: &str,
    chunk_size: usize,
    lbl_name: String,
    lbl_values: Vec<String>,
    matchers: Vec<LabelMatcher>,
    postings_for_matchers_fn: PostingsForMatchersFn,
) -> mpsc::Receiver<LabelValueCountResult> {
    let cancel = Arc::clone(&is_canceled);
    let count_fn: LabelValueCountFn = Arc::new(move |v: &str| {
        count_label_value_series(
            cancel.as_ref(),
            &lbl_name,
            v,
            &matchers,
            &postings_for_matchers_fn,
        )
    });
    compute_label_values_counts(
        is_canceled,
        pool,
        tenant_id,
        chunk_size,
        Arc::new(lbl_values),
        count_fn,
    )
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

    #[tokio::test]
    async fn query_stream_bimodal_lazy_large_matches_expected_with_ooo_blocks_and_shard() {
        use futures::StreamExt;

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "lazy-stream-test".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
        let limits = mimir_config::LimitsConfig {
            out_of_order_time_window: mimir_config::ModelDuration::from_mins(30),
            ..Default::default()
        };
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(limits),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        // Write 500 series (> INLINE_CHUNK_LOAD_SERIES_LIMIT=256) with 2 timestamps so we can
        // compact the earlier window into an on-disk ULID block while keeping newer + OOO in Head.
        let mut ts_batch1 = Vec::with_capacity(500);
        let mut ts_batch2 = Vec::with_capacity(500);
        for i in 0..500usize {
            let labels = vec![
                cortexpb::LabelPair {
                    name: "__name__".into(),
                    value: "lazy_large_metric".into(),
                },
                cortexpb::LabelPair {
                    name: "idx".into(),
                    value: format!("{i:04}").into(),
                },
            ];
            ts_batch1.push(cortexpb::TimeSeries {
                labels: labels.clone(),
                samples: vec![cortexpb::Sample {
                    value: i as f64,
                    timestamp_ms: 1_000_000,
                }],
                ..Default::default()
            });
            ts_batch2.push(cortexpb::TimeSeries {
                labels,
                samples: vec![
                    cortexpb::Sample {
                        value: (i + 1) as f64,
                        timestamp_ms: 2_000_000,
                    },
                    // OOO sample for every 10th series overlapping the Head window
                    cortexpb::Sample {
                        value: (i + 2) as f64,
                        timestamp_ms: if i % 10 == 0 { 1_900_000 } else { 2_010_000 },
                    },
                ],
                ..Default::default()
            });
        }
        ing.push_to_storage(
            "user-lazy",
            &cortexpb::WriteRequest {
                timeseries: ts_batch1,
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .unwrap();

        // Compact first batch into an on-disk ULID block
        ing.get_tsdb("user-lazy")
            .unwrap()
            .db
            .compact_head_all()
            .unwrap();

        // Push second batch into Head (includes OOO samples)
        ing.push_to_storage(
            "user-lazy",
            &cortexpb::WriteRequest {
                timeseries: ts_batch2,
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .unwrap();

        let md = MetadataMap::new();
        // Unsharded 500-series lazy stream across Block + Head + OOO
        let req_all = ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 3_000_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "lazy_large_metric".into(),
            }],
            ..Default::default()
        };
        let expected_all = ing
            .handle_query_stream(&md, "user-lazy", req_all.clone())
            .await
            .unwrap();
        let mut s_all = ing
            .handle_query_stream_bimodal(&md, "user-lazy", req_all)
            .await
            .unwrap();
        let mut got_all = Vec::new();
        while let Some(item) = s_all.next().await {
            got_all.push(item.unwrap());
        }
        assert_eq!(got_all, expected_all);

        // Sharded 500-series lazy stream (`1_of_4`)
        let req_shard = ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 3_000_000,
            matchers: vec![
                ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".into(),
                    value: "lazy_large_metric".into(),
                },
                ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__query_shard__".into(),
                    value: "1_of_4".into(),
                },
            ],
            ..Default::default()
        };
        let expected_shard = ing
            .handle_query_stream(&md, "user-lazy", req_shard.clone())
            .await
            .unwrap();
        let mut s_shard = ing
            .handle_query_stream_bimodal(&md, "user-lazy", req_shard)
            .await
            .unwrap();
        let mut got_shard = Vec::new();
        while let Some(item) = s_shard.next().await {
            got_shard.push(item.unwrap());
        }
        assert_eq!(got_shard, expected_shard);
    }

    #[tokio::test]
    async fn wave3_active_lnv_shard_error_and_quoted_unknown_count_method() {
        use bytes::Bytes;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        cfg.active_series.active_series_metrics_enabled = true;
        cfg.active_series.active_series_metrics_idle_timeout =
            mimir_config::GoDuration(50_000_000); // 50ms
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let lp = |n: &str, v: &str| cortexpb::LabelPair {
            name: Bytes::copy_from_slice(n.as_bytes()),
            value: Bytes::copy_from_slice(v.as_bytes()),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Push `m_inactive{env="prod", only_inact="x"}` then expire it via purge(now + 1s).
        ing.push_to_storage(
            "u1",
            &cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        lp("__name__", "m_inactive"),
                        lp("env", "prod"),
                        lp("only_inact", "x"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .unwrap();
        ing.get_tsdb("u1")
            .unwrap()
            .active_series
            .purge(SystemTime::now() + Duration::from_secs(1));

        // Push `m_active{env="prod"}` so `env="prod"` is active on `m_active`.
        ing.push_to_storage(
            "u1",
            &cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "m_active"), lp("env", "prod")],
                    samples: vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: now + 1_000,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .unwrap();

        // W3-09: LabelNamesAndValues(ACTIVE, matchers=[__name__="m_inactive"]) must return {"env": ["prod"]}.
        let md = MetadataMap::new();
        let lnv = ing
            .handle_label_names_and_values(
                &md,
                "u1",
                ingesterpb::LabelNamesAndValuesRequest {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::Equal as i32,
                        name: "__name__".into(),
                        value: "m_inactive".into(),
                    }],
                    count_method: ingesterpb::CountMethod::Active as i32,
                },
            )
            .await
            .unwrap();
        assert_eq!(lnv.len(), 1);
        assert_eq!(lnv[0].items.len(), 1);
        assert_eq!(lnv[0].items[0].label_name, "env");
        assert_eq!(lnv[0].items[0].values, vec!["prod"]);

        // W3-13: ActiveSeries invalid `__query_shard__` carries Go's outer prefix.
        let bad_shard_req = ingesterpb::ActiveSeriesRequest {
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__query_shard__".into(),
                value: "bad".into(),
            }],
            r#type: 0,
        };
        let as_err = ing
            .handle_active_series(&md, "u1", bad_shard_req.clone())
            .await
            .unwrap_err();
        assert_eq!(
            as_err.to_string(),
            "error listing active series: error removing shard matcher: invalid shard ID: \"bad\""
        );
        let Err(rx_err) = ing.handle_active_series_rx(&md, "u1", bad_shard_req).await else {
            panic!("expected rx bad shard error");
        };
        assert_eq!(
            rx_err.to_string(),
            "error listing active series: error removing shard matcher: invalid shard ID: \"bad\""
        );

        // W3-14: Unknown CountMethod quotes the numeric value (`"99"`).
        let us_err = ing
            .handle_user_stats(&md, "u1", ingesterpb::UserStatsRequest { count_method: 99 })
            .await
            .unwrap_err();
        assert_eq!(us_err.to_string(), "unknown count method \"99\"");

        let lnv_err = ing
            .handle_label_names_and_values(
                &md,
                "u1",
                ingesterpb::LabelNamesAndValuesRequest {
                    matchers: vec![],
                    count_method: 99,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(lnv_err.to_string(), "unknown count method \"99\"");

        let lvc_err = ing
            .handle_label_values_cardinality(
                &md,
                "u1",
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec!["env".into()],
                    matchers: vec![],
                    count_method: 99,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(lvc_err.to_string(), "unknown count method \"99\"");
    }

    #[tokio::test]
    async fn metrics_for_label_matchers_dedups_sorts_and_respects_limit() {
        use tokio_stream::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let req = cortexpb::WriteRequest {
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "m_b"),
                        cortexpb::LabelPair::from_strs("env", "prod"),
                    ],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: 1_000,
                        value: 1.0,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "m_a"),
                        cortexpb::LabelPair::from_strs("env", "prod"),
                    ],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: 1_000,
                        value: 2.0,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "m_c"),
                        cortexpb::LabelPair::from_strs("env", "dev"),
                    ],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: 1_000,
                        value: 3.0,
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        ing.push_to_storage("u-mflm", &req).unwrap();

        let md = MetadataMap::new();
        let mflm = ing
            .handle_metrics_for_label_matchers(
                &md,
                "u-mflm",
                ingesterpb::MetricsForLabelMatchersRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: 2_000,
                    matchers_set: vec![
                        ingesterpb::LabelMatchers {
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: "env".into(),
                                value: "prod".into(),
                            }],
                        },
                        ingesterpb::LabelMatchers {
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::RegexMatch as i32,
                                name: "__name__".into(),
                                value: "m_[ab]".into(),
                            }],
                        },
                    ],
                    limit: 2,
                },
            )
            .await
            .unwrap();
        let names: Vec<String> = mflm
            .metric
            .iter()
            .map(|m| {
                m.labels
                    .iter()
                    .find(|l| l.name.as_ref() == b"__name__")
                    .map(|l| String::from_utf8_lossy(&l.value).into_owned())
                    .unwrap()
            })
            .collect();
        assert_eq!(names, vec!["m_a", "m_b"]);

        let mut rx = ing
            .handle_active_series_rx(
                &md,
                "u-mflm",
                ingesterpb::ActiveSeriesRequest {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::Equal as i32,
                        name: "env".into(),
                        value: "prod".into(),
                    }],
                    r#type: 0,
                },
            )
            .await
            .unwrap();
        let mut active_count = 0usize;
        while let Some(resp) = rx.next().await {
            active_count += resp.unwrap().metric.len();
        }
        assert_eq!(active_count, 2);
    }

    async fn single_worker_thread_id(pool: &ComputeWorkerPool) -> std::thread::ThreadId {
        let (tx, rx) = tokio::sync::oneshot::channel();
        pool.submit("probe", "probe", move || {
            let _ = tx.send(std::thread::current().id());
        })
        .unwrap();
        rx.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compute_label_values_counts_does_not_drop_errors_when_channel_is_full() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        // Case 1: Canceled after all 64 channel slots are already occupied.
        {
            let pool = Arc::new(ComputeWorkerPool::new(1, "test-full-cancel", None).unwrap());
            pool.start().unwrap();
            let worker_tid = single_worker_thread_id(&pool).await;
            let lbl_values: Vec<String> = (0..80).map(|i| i.to_string()).collect();
            let entered = Arc::new(AtomicUsize::new(0));
            let checks = Arc::new(AtomicUsize::new(0));
            let canceled = Arc::new(AtomicBool::new(false));

            let entered_c = Arc::clone(&entered);
            let count_fn: LabelValueCountFn = Arc::new(move |_| {
                entered_c.fetch_add(1, Ordering::AcqRel);
                Ok(1)
            });

            let checks_c = Arc::clone(&checks);
            let entered_chk = Arc::clone(&entered);
            let canceled_c = Arc::clone(&canceled);
            let is_canceled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
                if std::thread::current().id() == worker_tid {
                    return canceled_c.load(Ordering::Acquire);
                }
                if canceled_c.load(Ordering::Acquire) {
                    return true;
                }
                let n = checks_c.fetch_add(1, Ordering::AcqRel) + 1;
                if n == 66 {
                    // Item 64 only enters count_fn after items 0..63 are pushed into tx.
                    while entered_chk.load(Ordering::Acquire) < 65 {
                        std::thread::yield_now();
                    }
                    canceled_c.store(true, Ordering::Release);
                    return true;
                }
                false
            });

            let mut rx = compute_label_values_counts(
                is_canceled,
                &pool,
                "t1",
                1,
                Arc::new(lbl_values),
                count_fn,
            );
            while !canceled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }

            let mut saw_canceled = false;
            let mut ok_count = 0usize;
            while let Some(res) = rx.recv().await {
                match res.err {
                    None => ok_count += 1,
                    Some(LabelNamesAndValuesError::Canceled) => saw_canceled = true,
                    Some(other) => panic!("unexpected error: {other:?}"),
                }
            }
            assert!(ok_count >= 64, "expected channel to have been saturated: {ok_count}");
            assert!(saw_canceled, "Canceled error must not be dropped when channel is full");
            pool.stop();
        }

        // Case 2: PoolStopped after all 64 channel slots are already occupied.
        {
            let pool = Arc::new(ComputeWorkerPool::new(1, "test-full-stop", None).unwrap());
            pool.start().unwrap();
            let worker_tid = single_worker_thread_id(&pool).await;
            let lbl_values: Vec<String> = (0..80).map(|i| i.to_string()).collect();
            let entered = Arc::new(AtomicUsize::new(0));
            let checks = Arc::new(AtomicUsize::new(0));
            let stopped_triggered = Arc::new(AtomicBool::new(false));

            let entered_c = Arc::clone(&entered);
            let count_fn: LabelValueCountFn = Arc::new(move |_| {
                entered_c.fetch_add(1, Ordering::AcqRel);
                Ok(1)
            });

            let checks_c = Arc::clone(&checks);
            let entered_chk = Arc::clone(&entered);
            let pool_c = Arc::clone(&pool);
            let stopped_c = Arc::clone(&stopped_triggered);
            let is_canceled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
                if std::thread::current().id() == worker_tid {
                    return false;
                }
                let n = checks_c.fetch_add(1, Ordering::AcqRel) + 1;
                if n == 66 {
                    while entered_chk.load(Ordering::Acquire) < 65 {
                        std::thread::yield_now();
                    }
                    let pool_stop = Arc::clone(&pool_c);
                    std::thread::spawn(move || pool_stop.stop());
                    while pool_c.submit("probe", "t2", || {}).is_ok() {
                        std::thread::yield_now();
                    }
                    stopped_c.store(true, Ordering::Release);
                }
                false
            });

            let mut rx = compute_label_values_counts(
                is_canceled,
                &pool,
                "t1",
                1,
                Arc::new(lbl_values),
                count_fn,
            );
            while !stopped_triggered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }

            let mut saw_pool_stopped = false;
            let mut ok_count = 0usize;
            while let Some(res) = rx.recv().await {
                match res.err {
                    None => ok_count += 1,
                    Some(LabelNamesAndValuesError::PoolStopped) => saw_pool_stopped = true,
                    Some(other) => panic!("unexpected error: {other:?}"),
                }
            }
            assert!(ok_count >= 64, "expected channel to have been saturated: {ok_count}");
            assert!(saw_pool_stopped, "PoolStopped error must not be dropped when channel is full");
        }
    }

    #[test]
    fn w16_icw_02_merge_per_block_chunk_series_matches_go_tie_break_and_120_split() {
        use mimir_tsdb::chunkenc::{EncodedChunk, Encoding, Xor2Chunk, XorChunk};
        use mimir_tsdb::chunks_block::ChunkMeta;
        use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
        use mimir_tsdb::labels::Labels;

        let lbls = Labels::from_pairs([("__name__", "up"), ("job", "api")]);

        // Block A: [150, 200] with (150, 1.0), (200, 1.9)
        let mut c0 = XorChunk::new();
        {
            let mut app = c0.appender().unwrap();
            app.append(0, 150, 1.0).unwrap();
            app.append(0, 200, 1.9).unwrap();
        }
        // Block B: [150, 300] with (150, 2.0), (300, 3.0)
        let mut c1 = XorChunk::new();
        {
            let mut app = c1.appender().unwrap();
            app.append(0, 150, 2.0).unwrap();
            app.append(0, 300, 3.0).unwrap();
        }
        let s0 = vec![QueryChunkSeries {
            labels: lbls.clone(),
            chunks: vec![ChunkMeta {
                ref_id: 1,
                min_time: 150,
                max_time: 200,
                chunk: Some(EncodedChunk::Xor(c0)),
            }],
        }];
        let s1 = vec![QueryChunkSeries {
            labels: lbls.clone(),
            chunks: vec![ChunkMeta {
                ref_id: 1,
                min_time: 150,
                max_time: 300,
                chunk: Some(EncodedChunk::Xor(c1)),
            }],
        }];
        let merged = merge_per_block_chunk_series(vec![s1, s0], false, Encoding::Xor).unwrap();
        assert_eq!(merged.len(), 1);
        let samples: Vec<(i64, f64)> = merged[0]
            .chunks
            .iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        // c0=[150, 200] sorts before c1=[150, 300] by (min_time, max_time) and is passed LAST
        // to chain_sample_merge, so (150, 1.0) wins.
        assert_eq!(samples, vec![(150, 1.0), (200, 1.9), (300, 3.0)]);

        // Byte-identical EncXOR2 chunks elided verbatim without re-encoding even under Encoding::Xor.
        let mut dup = Xor2Chunk::new();
        {
            let mut app = dup.appender().unwrap();
            app.append(0, 100, 42.0).unwrap();
            app.append(0, 200, 43.0).unwrap();
        }
        let dup_meta = ChunkMeta {
            ref_id: 7,
            min_time: 100,
            max_time: 200,
            chunk: Some(EncodedChunk::Xor2(dup)),
        };
        let merged_dup = merge_per_block_chunk_series(
            vec![
                vec![QueryChunkSeries {
                    labels: lbls.clone(),
                    chunks: vec![dup_meta.clone()],
                }],
                vec![QueryChunkSeries {
                    labels: lbls,
                    chunks: vec![dup_meta],
                }],
            ],
            false,
            Encoding::Xor,
        )
        .unwrap();
        assert_eq!(merged_dup[0].chunks.len(), 1);
        assert_eq!(
            merged_dup[0].chunks[0].chunk.as_ref().unwrap().encoding(),
            Encoding::Xor2
        );
    }

    #[tokio::test]
    async fn planned_head_overlapping_xor2_chunks_merge_to_enc_xor() {
        use mimir_tsdb::chunkenc::Xor2Chunk;

        // 1) Verify directly that merge_head_chunks_with_tombstones (used by
        // PlannedTsdbChunkQuerier::select_head_in_order) emits EncXOR (1) on overlapping
        // Xor2 chunks with st==0.
        let mut c0 = Xor2Chunk::new();
        {
            let mut app = c0.appender().unwrap();
            app.append(0, 1_000, 1.0).unwrap();
            app.append(0, 3_000, 3.0).unwrap();
        }
        let mut c1 = Xor2Chunk::new();
        {
            let mut app = c1.appender().unwrap();
            app.append(0, 2_000, 2.0).unwrap();
            app.append(0, 4_000, 4.0).unwrap();
        }
        let merged = merge_head_chunks_with_tombstones(
            vec![
                ChunkMeta {
                    ref_id: 1,
                    min_time: 1_000,
                    max_time: 3_000,
                    chunk: Some(mimir_tsdb::chunkenc::EncodedChunk::Xor2(c0)),
                },
                ChunkMeta {
                    ref_id: 2,
                    min_time: 2_000,
                    max_time: 4_000,
                    chunk: Some(mimir_tsdb::chunkenc::EncodedChunk::Xor2(c1)),
                },
            ],
            &[],
        )
        .unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor);

        // 2) End-to-end QueryStream with float_chunk_encoding=xor2, enable_st_storage=false,
        // index_lookup_planning_enabled=true, and overlapping Head+OOO float chunks emits EncXOR (1).
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "planned-xor2-head".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
        blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
        let limits = mimir_config::LimitsConfig {
            float_chunk_encoding: "xor2".to_string(),
            out_of_order_time_window: mimir_config::ModelDuration::from_mins(30),
            ..Default::default()
        };
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(limits),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let now = 1_700_000_000_000i64;
        let user = "t-planned-xor2";
        ing.push_to_storage(
            user,
            &cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "planned_overlap_xor2"),
                        cortexpb::LabelPair::from_strs("job", "api"),
                    ],
                    samples: vec![
                        cortexpb::Sample { value: 1.0, timestamp_ms: now },
                        cortexpb::Sample { value: 3.0, timestamp_ms: now + 2_000 },
                    ],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .unwrap();
        ing.push_to_storage(
            user,
            &cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "planned_overlap_xor2"),
                        cortexpb::LabelPair::from_strs("job", "api"),
                    ],
                    samples: vec![cortexpb::Sample { value: 2.0, timestamp_ms: now + 1_000 }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .unwrap();

        let md = MetadataMap::new();
        let req = ingesterpb::QueryRequest {
            start_timestamp_ms: now - 1_000,
            end_timestamp_ms: now + 5_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "planned_overlap_xor2".into(),
            }],
            ..Default::default()
        };
        let res = ing.handle_query_stream(&md, user, req).await.unwrap();
        let chunks: Vec<_> = res
            .iter()
            .flat_map(|r| r.streaming_series_chunks.iter().flat_map(|sc| sc.chunks.iter()))
            .collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].encoding,
            tsdb_encoding_to_wire(Encoding::Xor),
            "planned Head merge with st==0 must emit wire PrometheusXorChunk (4), not PrometheusXor2Chunk (7)"
        );
    }

    #[tokio::test]
    async fn b04_label_values_cardinality_empty_label_name_with_matchers_errors() {
        use tokio_stream::StreamExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let mut ing_cfg = mimir_config::IngesterConfig::default();
        ing_cfg.ring.instance_id = "test-b04".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            ing_cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.compute_worker_pool.start().unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let user = "t-b04";
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        ing.push_to_storage(
            user,
            &cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "up"),
                        cortexpb::LabelPair::from_strs("job", "api"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .unwrap();

        let md = MetadataMap::new();
        for count_method in [
            ingesterpb::CountMethod::InMemory as i32,
            ingesterpb::CountMethod::Active as i32,
        ] {
            let req = ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec![String::new()],
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "job".into(),
                    value: "api".into(),
                }],
                count_method,
            };
            let err = ing
                .handle_label_values_cardinality(&md, user, req.clone())
                .await
                .unwrap_err();
            assert_eq!(err.map_read_error_to_status().code(), tonic::Code::Internal);
            assert!(
                err.to_string().contains("unexpected all postings"),
                "unexpected err: {err}"
            );

            let mut stream = ing
                .handle_label_values_cardinality_rx_with_sink(
                    &md,
                    user,
                    req,
                    ReadErrorSink::default(),
                )
                .await
                .unwrap();
            let first = stream.next().await.unwrap();
            let st = first.unwrap_err();
            assert_eq!(st.code(), tonic::Code::Internal);
            assert!(
                st.message().contains("unexpected all postings"),
                "unexpected stream status: {st:?}"
            );
        }

        // Empty matchers with label_names: [""] must still succeed (single all-postings fast path).
        let ok_res = ing
            .handle_label_values_cardinality(
                &md,
                user,
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec![String::new()],
                    matchers: Vec::new(),
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_res.len(), 1);
    }

    #[test]
    fn map_strong_read_consistency_wait_err_matches_go_multi_cluster_context_deadline() {
        let single = map_strong_read_consistency_wait_err(
            mimir_storage::ingest::ReaderError::ContextDeadlineExceeded,
        );
        assert!(single.is_deadline_exceeded());
        assert_eq!(
            single.to_string(),
            "wait for read consistency: context deadline exceeded"
        );

        let multi = map_strong_read_consistency_wait_err(
            mimir_storage::ingest::ReaderError::WriteCompartment {
                id: 0,
                source: Box::new(mimir_storage::ingest::ReaderError::ContextDeadlineExceeded),
            },
        );
        assert!(multi.is_deadline_exceeded());
        assert_eq!(
            multi.to_string(),
            "wait for read consistency: write compartment 0: context deadline exceeded"
        );
    }
}
