//! Kafka ingest_storage client, record v0-v2 codecs, readers, writers, and offset tracking.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/*
//! - github.com/grafana/mimir/pkg/ingester/offset_catalogue.go

pub mod client_consumer;
pub mod compression;
pub mod fetcher;
pub mod fixture_generator;
pub mod heap_merger;
pub mod kafka_client;
pub mod kgo_retry;
pub mod kprom;
pub mod labeled_registry;
pub mod offset_catalogue;
pub mod offset_file;
pub mod partition_offset_client;
pub mod partition_offset_reader;
pub mod partition_offset_watcher;
pub mod pusher;
pub mod reader;
pub mod reader_client;
pub mod transport;
pub mod util;
pub mod version;
pub mod writer;
pub mod writer_client;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::logging::{LogLevel, Logger};
use mimir_config::{KafkaConfig, StringSliceCSV};
use mimir_proto::ringpb::{PartitionDesc, PartitionRingDesc, PartitionState, PartitionTokenScheme};
use parking_lot::{Mutex, RwLock};
use prometheus::{Gauge, GaugeVec, Opts, Registry};
use tokio::sync::watch;

pub use compression::KafkaCompression;
pub use fetcher::{
    find_gaps_in_records, handle_kafka_fetch_err, ConcurrentFetchers, FetchResult, FetchWaiter,
    FetchWant, FetcherMetrics, KafkaFetchError, MetadataRefresher, OffsetRange, RangeErrorPolicy,
    CHOSEN_BROKER_DIED, FORCED_MIN_VALUE_FOR_MAX_BYTES, INITIAL_BYTES_PER_RECORD, UNKNOWN_BROKER,
};
pub use fixture_generator::{
    FixtureConfig, FixtureGenerator, FixtureTenantClassConfig,
};
pub use heap_merger::{
    merge_records_in_timestamp_order, HeapMerger, HeapMergerMetrics, MergeableRecord,
};
pub use kafka_client::{
    FetchPartitionResult, FetchedRecord, KafkaClient, KafkaClientError, ListedOffset,
    ProduceRecord, KAFKA_OFFSET_END, KAFKA_OFFSET_START,
};
pub use offset_catalogue::{
    merge_topics_partitions_offsets, read_offset_catalogue_from_file,
    write_offset_catalogue_to_file, OffsetCatalogue, OffsetCatalogueData, OffsetCatalogueError,
    OffsetCatalogueMetrics, OffsetWatermark, PartitionOffsets, PartitionsOffsets,
    TopicsPartitionsOffsets, OFFSET_CATALOGUE_FILENAME, OFFSET_CATALOGUE_VERSION,
};
pub use offset_file::{OffsetFile, OffsetFileError, OFFSET_FILE_VERSION};
pub use partition_offset_client::{PartitionOffsetClient, PartitionOffsetClientMetrics};
pub use partition_offset_reader::{
    GenericOffsetReader, GetPartitionIDsFn, MultiClusterOffsetsReader,
    SingleClusterPartitionOffsetReader, SingleClusterTopicOffsetsReader,
};
pub use partition_offset_watcher::{OffsetWatcherError, PartitionOffsetWatcher};
pub use pusher::{
    label_adapters_hash, request_contents, BatchingQueue, BatchingQueueDoneHandle,
    BatchingQueueMetrics, ErrorSampler, FlushableWriteRequest, LogPolicy, NoOpPreCommitNotifier,
    ParallelStoragePusher, ParallelStorageShards, PreCommitNotifier, PushErrorHandler, Pusher,
    PusherConsumer, PusherConsumerMetrics, PusherError, SequentialStoragePusher,
    StoragePusherMetrics,
};
pub use reader::{
    decode_fetched_record, AdmClient, DecodedKafkaRecord, KafkaAdmClient,
    MultiClusterPartitionReader, PartitionCommitter, ReaderError, ReaderMetrics, ReaderState,
    RecordBatchConsumer, RecordBatchConsumerFactory, SingleClusterPartitionReader,
    StrongReadConsistencyMetrics,
    ZeroCopyBatchConsumer, ZeroCopyRecordHandler, ERR_OFFSET_FILE_WRITE, READER_METRICS_PREFIX,
};
pub use reader_client::{
    new_kafka_reader_client, new_kafka_reader_client_metrics, KafkaReaderClient,
    READER_BROKER_MAX_READ_BYTES, READER_FETCH_MAX_BYTES, READER_FETCH_MAX_PARTITION_BYTES,
    READER_FETCH_MIN_BYTES,
};
pub use util::{
    base64_decode, base64_encode, create_topics, ingester_partition_id, request_json_from_socket,
    sasl_mechanism, sha256, sha512, CredentialCallback, IngestKafkaClient,
    KafkaClientExtendedMetrics, KafkaMskIamAuth, KafkaOauthbearerAuth, KafkaSaslSecretConfig,
    RecordedSpan, ResolvedSaslMechanism, ResultPromise, SampledOnlyPropagator, SampledOnlyTracer,
    ScramAlg, SpanRecorder, StaticSaslSecret, TraceSpanContext, TracedKafkaRecord,
};
pub use transport::{
    EstablishedConn, KafkaReadHalf, KafkaStream, KafkaTransport, KafkaWriteHalf, SaslAuthConfig,
};
pub use version::{
    deserialize_record_content, parse_record_version, record_version_header,
    record_version_header_raw, split_write_request_by_max_marshal_size,
    split_write_request_by_max_marshal_size_rw2, validate_record_version, RecordCodecError,
    RecordSerializer, RecordVersion, SerializedKafkaRecord, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
    MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT, PRODUCER_BATCH_MAX_BYTES,
};
pub use writer::{
    err_write_request_data_item_too_large_msg, is_write_request_empty,
    marshal_write_request_to_records, produce_results_err, successful_produce_records_stats,
    IngestWriter, PartitionWriteRequest, Writer, WriterError, WriterMetrics, WriterOptions,
    DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS,
};
pub use writer_client::{
    first_produce_err, kafka_producer_compression_from_config, KafkaProducer, KafkaProducerClient,
    KafkaWriterClient, KafkaWriterClientOptions, ProduceCancelSource, ProduceContext,
    ProduceResult, ProducerError, SlidingWindowSummary, WriterRecord,
    DEFAULT_METADATA_REFRESH_INTERVAL, DEFAULT_PRODUCER_LINGER, WRITER_METRICS_PREFIX,
};

// ---------------------------------------------------------------------------
// Compartments & KafkaConfig helpers (ported from pkg/compartments & pkg/storage/ingest/config.go)
// ---------------------------------------------------------------------------

pub const READ_COMPARTMENT_ID_PLACEHOLDER: &str = "<read-compartment-id>";
pub const WRITE_COMPARTMENT_ID_PLACEHOLDER: &str = "<write-compartment-id>";

pub fn with_read_compartment_suffix(s: &str, read_compartment_id: usize) -> String {
    format!("{s}-rc-{read_compartment_id}")
}

pub fn replace_write_compartment(s: &str, write_compartment_id: usize) -> String {
    s.replace(
        WRITE_COMPARTMENT_ID_PLACEHOLDER,
        &write_compartment_id.to_string(),
    )
}

/// Ported from `KafkaConfig.GetConsumerGroup` in `pkg/storage/ingest/config.go`.
pub fn get_consumer_group(cfg: &KafkaConfig, instance_id: &str, partition_id: i32) -> String {
    if cfg.consumer_group.is_empty() {
        return instance_id.to_string();
    }
    cfg.consumer_group
        .replace("<partition>", &partition_id.to_string())
}

/// Ported from `KafkaConfig.WriteCompartmentConfig` in `pkg/storage/ingest/config.go`.
pub fn write_compartment_config(cfg: &KafkaConfig, write_compartment_id: usize) -> KafkaConfig {
    let mut c = cfg.clone();
    c.address = StringSliceCSV(
        cfg.address
            .0
            .iter()
            .map(|a| replace_write_compartment(a, write_compartment_id))
            .collect(),
    );
    c.sasl_username = replace_write_compartment(&cfg.sasl_username, write_compartment_id);
    c.sasl_password = replace_write_compartment(&cfg.sasl_password, write_compartment_id);
    c
}

/// Ported from `divideBudget` in `pkg/storage/ingest/config.go`.
pub fn divide_budget(budget: i64, num_compartments: usize) -> i64 {
    if budget <= 0 || num_compartments <= 1 {
        return budget;
    }
    (budget / (num_compartments as i64)).max(1)
}

/// Ported from `WriteCompartmentConfigs` in `pkg/storage/ingest/config.go`.
pub fn write_compartment_configs(
    base: &KafkaConfig,
    num_compartments: usize,
    topic: &str,
) -> Vec<KafkaConfig> {
    let fetch_concurrency_max = divide_budget(base.fetch_concurrency_max, num_compartments);
    let max_buffered_bytes = divide_budget(base.max_buffered_bytes, num_compartments);
    let ingestion_concurrency_max =
        divide_budget(base.ingestion_concurrency_max, num_compartments);

    (0..num_compartments)
        .map(|i| {
            let mut c = write_compartment_config(base, i);
            c.topic = topic.to_string();
            c.fetch_concurrency_max = fetch_concurrency_max;
            c.max_buffered_bytes = max_buffered_bytes;
            c.ingestion_concurrency_max = ingestion_concurrency_max;
            c
        })
        .collect()
}

/// Mapped Warpstream client options matching `KafkaConfig.ToWarpstreamClientOptions` in `pkg/storage/ingest/config.go`.
#[derive(Debug, Clone, PartialEq)]
pub struct WarpstreamClientOptions {
    pub address: Vec<String>,
    pub topic: String,
    pub client_id: String,
    pub dial_timeout: Duration,
    pub write_timeout: Duration,
    pub linger: Duration,
    pub batch_max_bytes: i32,
    pub metadata_refresh_interval: Duration,
    pub cluster_stats_ttl: Duration,
    pub health_check_slow_multiplier: f64,
    pub health_check_max_slow_fraction: f64,
    pub health_check_faulty_threshold: f64,
    pub health_check_max_faulty_fraction: f64,
    pub hedger_min_hedge_delay: Duration,
    pub hedger_max_hedge_agents: i64,
    pub demoter_probe_interval: Duration,
    pub produce_request_timeout: Duration,
    pub produce_request_timeout_overhead: Duration,
    pub tls_enabled: bool,
    pub tls_insecure_skip_verify: bool,
}

pub fn to_warpstream_client_options(cfg: &KafkaConfig) -> Result<WarpstreamClientOptions, String> {
    if cfg.tls_enabled {
        cfg.tls_client_config()
            .get_tls_config()
            .map_err(|e| format!("invalid Kafka TLS config: {e}"))?;
    }

    let dial_timeout = Duration::from_nanos(cfg.dial_timeout.as_nanos().max(0) as u64);
    let write_timeout = Duration::from_nanos(cfg.write_timeout.as_nanos().max(0) as u64);
    let write_timeout_overhead =
        Duration::from_nanos(cfg.write_timeout_overhead.as_nanos().max(0) as u64);
    let produce_request_timeout = write_timeout.saturating_sub(write_timeout_overhead);

    Ok(WarpstreamClientOptions {
        address: cfg.address.0.clone(),
        topic: cfg.topic.clone(),
        client_id: cfg.client_id.clone(),
        dial_timeout,
        write_timeout,
        linger: DEFAULT_PRODUCER_LINGER,
        batch_max_bytes: PRODUCER_BATCH_MAX_BYTES as i32,
        metadata_refresh_interval: DEFAULT_METADATA_REFRESH_INTERVAL,
        cluster_stats_ttl: Duration::from_secs(1),
        health_check_slow_multiplier: cfg.warpstream_health_check_slow_multiplier,
        health_check_max_slow_fraction: cfg.warpstream_health_check_max_slow_fraction,
        health_check_faulty_threshold: cfg.warpstream_health_check_faulty_threshold,
        health_check_max_faulty_fraction: cfg.warpstream_health_check_max_faulty_fraction,
        hedger_min_hedge_delay: Duration::from_nanos(
            cfg.warpstream_hedge_min_delay.as_nanos().max(0) as u64,
        ),
        hedger_max_hedge_agents: cfg.warpstream_hedge_max_agents,
        demoter_probe_interval: Duration::from_nanos(
            cfg.warpstream_demoter_probe_interval.as_nanos().max(0) as u64,
        ),
        produce_request_timeout,
        produce_request_timeout_overhead: write_timeout_overhead,
        tls_enabled: cfg.tls_enabled,
        tls_insecure_skip_verify: cfg.tls_insecure_skip_verify,
    })
}

// ---------------------------------------------------------------------------
// KafkaLogger (ported from pkg/storage/ingest/logger.go)
// ---------------------------------------------------------------------------

pub struct KafkaLogger {
    logger: Arc<dyn Logger>,
}

impl KafkaLogger {
    pub fn new(logger: Arc<dyn Logger>) -> Self {
        Self { logger }
    }

    pub fn level(&self) -> LogLevel {
        LogLevel::Info
    }

    pub fn log(&self, lev: LogLevel, msg: &str, keyvals: &[(&str, &str)]) {
        let mut pairs = Vec::with_capacity(2 + keyvals.len());
        pairs.push(("component", "kafka_client"));
        pairs.push(("msg", msg));
        pairs.extend_from_slice(keyvals);
        self.logger.log(lev, &pairs);
    }
}

// ---------------------------------------------------------------------------
// PartitionRingWatchers (ported from pkg/storage/ingest/partition_ring_watchers.go)
// ---------------------------------------------------------------------------

pub const OPTIMAL_TOKENS_PER_PARTITION: usize = 512;
const TOTAL_TOKENS_COUNT: u64 = 1u64 << 32;
const MAX_ZONES_COUNT: usize = 3;

#[derive(Clone, Copy, Debug)]
struct RingToken {
    token: u32,
    prev_token: u32,
}

fn token_distance_u32(from: u32, to: u32) -> i64 {
    let diff = (to as i64) - (from as i64);
    if diff > 0 {
        diff
    } else {
        (TOTAL_TOKENS_COUNT as i64) + diff
    }
}

fn search_token_u32(tokens: &[u32], key: u32) -> usize {
    let i = tokens.partition_point(|&t| t <= key);
    if i >= tokens.len() {
        0
    } else {
        i
    }
}

/// Derived spread-minimizing token table for partition IDs `0..partitions`.
#[derive(Debug, Clone)]
pub struct PartitionTokenTable {
    tokens: Vec<Vec<u32>>,
}

impl PartitionTokenTable {
    pub fn new(partitions: i32) -> Result<Self, String> {
        if partitions < 0 {
            return Err(format!("negative partition count {partitions}"));
        }
        if partitions == 0 {
            return Ok(Self { tokens: Vec::new() });
        }

        let max_id = (partitions - 1) as usize;
        let token_distance = ((TOTAL_TOKENS_COUNT / OPTIMAL_TOKENS_PER_PARTITION as u64
            / MAX_ZONES_COUNT as u64)
            * MAX_ZONES_COUNT as u64) as u32;
        let mut first_tokens = Vec::with_capacity(OPTIMAL_TOKENS_PER_PARTITION);
        for i in 0..OPTIMAL_TOKENS_PER_PARTITION {
            first_tokens.push((i as u32).wrapping_mul(token_distance));
        }

        let mut tokens_by_id: Vec<Vec<u32>> = vec![Vec::new(); max_id + 1];
        tokens_by_id[0] = first_tokens.clone();
        if max_id > 0 {
            let mut queues: Vec<Vec<(f64, RingToken)>> = Vec::with_capacity(max_id + 1);
            let mut first_q = Vec::with_capacity(OPTIMAL_TOKENS_PER_PARTITION);
            let mut prev = first_tokens.len() - 1;
            let mut first_own = 0.0f64;
            for (idx, &tok) in first_tokens.iter().enumerate() {
                let d = token_distance_u32(first_tokens[prev], tok) as f64;
                first_own += d;
                first_q.push((
                    d,
                    RingToken {
                        token: tok,
                        prev_token: first_tokens[prev],
                    },
                ));
                prev = idx;
            }
            queues.push(first_q);

            let mut inst_owns: Vec<(usize, f64)> = vec![(0, first_own)];
            let max_zones = MAX_ZONES_COUNT as u32;
            let max_token_value =
                (((TOTAL_TOKENS_COUNT / MAX_ZONES_COUNT as u64) - 1) * MAX_ZONES_COUNT as u64)
                    as u32;

            for (i, slot) in tokens_by_id.iter_mut().enumerate().take(max_id + 1).skip(1) {
                let optimal_inst_own = (TOTAL_TOKENS_COUNT as f64) / ((i + 1) as f64);
                let mut curr_own = 0.0f64;
                let mut added = 0usize;
                let mut ignored: Vec<(usize, f64)> = Vec::new();
                let mut new_toks = Vec::with_capacity(OPTIMAL_TOKENS_PER_PARTITION);
                let mut curr_q = Vec::with_capacity(OPTIMAL_TOKENS_PER_PARTITION);

                while added < OPTIMAL_TOKENS_PER_PARTITION {
                    let rem = (OPTIMAL_TOKENS_PER_PARTITION - added) as u32;
                    let raw_opt = ((optimal_inst_own - curr_own) as u32) / rem;
                    let opt_tok_own = (raw_opt / max_zones) * max_zones;

                    // Pick highest-ownership non-ignored instance
                    let best_pos = inst_owns
                        .iter()
                        .enumerate()
                        .max_by(|a, b| {
                            a.1 .1
                                .total_cmp(&b.1 .1)
                                .then_with(|| b.1 .0.cmp(&a.1 .0))
                        })
                        .map(|(p, _)| p)
                        .ok_or_else(|| "no instance available".to_string())?;

                    let (hid, hown) = inst_owns[best_pos];
                    if hown <= opt_tok_own as f64 {
                        return Err("cannot add token".to_string());
                    }

                    let tok_best_pos = queues[hid]
                        .iter()
                        .enumerate()
                        .max_by(|a, b| a.1 .0.total_cmp(&b.1 .0))
                        .map(|(p, _)| p)
                        .ok_or_else(|| "empty token queue".to_string())?;

                    let (old_tok_own, rt) = queues[hid][tok_best_pos];
                    if old_tok_own <= opt_tok_own as f64 {
                        let popped = inst_owns.swap_remove(best_pos);
                        ignored.push(popped);
                        continue;
                    }

                    let offset = max_token_value.wrapping_sub(rt.prev_token);
                    let new_tok = if offset < opt_tok_own {
                        opt_tok_own.wrapping_sub(offset)
                    } else {
                        rt.prev_token.wrapping_add(opt_tok_own)
                    };
                    new_toks.push(new_tok);
                    curr_q.push((
                        token_distance_u32(rt.prev_token, new_tok) as f64,
                        RingToken {
                            token: new_tok,
                            prev_token: rt.prev_token,
                        },
                    ));

                    let new_tok_own = token_distance_u32(new_tok, rt.token) as f64;
                    curr_own += old_tok_own - new_tok_own;
                    queues[hid][tok_best_pos] = (
                        new_tok_own,
                        RingToken {
                            token: rt.token,
                            prev_token: new_tok,
                        },
                    );
                    inst_owns[best_pos].1 = hown - old_tok_own + new_tok_own;
                    added += 1;
                }

                *slot = new_toks;
                inst_owns.extend(ignored);
                queues.push(curr_q);
                inst_owns.push((i, curr_own));
            }
        }

        for t in &mut tokens_by_id {
            t.sort_unstable();
        }
        Ok(Self {
            tokens: tokens_by_id,
        })
    }

    pub fn tokens_for(&self, id: i32) -> Result<&[u32], String> {
        if id < 0 || (id as usize) >= self.tokens.len() {
            return Err(format!("partition {id} out of range {}", self.tokens.len()));
        }
        Ok(&self.tokens[id as usize])
    }
}

/// In-memory KV store for `PartitionRingDesc`, used by `PartitionRingWatchers`.
pub struct InMemoryPartitionKv {
    entries: RwLock<HashMap<String, PartitionRingDesc>>,
    notifiers: Mutex<HashMap<String, watch::Sender<u64>>>,
}

impl Default for InMemoryPartitionKv {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            notifiers: Mutex::new(HashMap::new()),
        }
    }
}

impl InMemoryPartitionKv {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            notifiers: Mutex::new(HashMap::new()),
        })
    }

    pub fn put(&self, key: &str, desc: PartitionRingDesc) {
        self.entries.write().insert(key.to_string(), desc);
        if let Some(tx) = self.notifiers.lock().get(key) {
            let next = *tx.borrow() + 1;
            let _ = tx.send(next);
        }
    }

    pub fn get(&self, key: &str) -> Option<PartitionRingDesc> {
        self.entries.read().get(key).cloned()
    }

    pub fn subscribe(&self, key: &str) -> watch::Receiver<u64> {
        let mut guard = self.notifiers.lock();
        guard
            .entry(key.to_string())
            .or_insert_with(|| watch::channel(0).0)
            .subscribe()
    }
}

/// Immutable snapshot of a partition ring watched by `PartitionRingWatcher`.
#[derive(Clone)]
pub struct PartitionRingSnapshot {
    desc: PartitionRingDesc,
    ring_tokens: Vec<u32>,
    token_table: Option<Arc<PartitionTokenTable>>,
}

impl PartitionRingSnapshot {
    pub fn new(
        desc: PartitionRingDesc,
        token_table: Option<Arc<PartitionTokenTable>>,
    ) -> Result<Self, String> {
        let mut ring_tokens = Vec::new();
        for (&id, part) in &desc.partitions {
            let toks = Self::resolve_tokens_for_part(&token_table, id, part)?;
            ring_tokens.extend_from_slice(toks);
        }
        ring_tokens.sort_unstable();
        Ok(Self {
            desc,
            ring_tokens,
            token_table,
        })
    }

    fn resolve_tokens_for_part<'a>(
        token_table: &'a Option<Arc<PartitionTokenTable>>,
        id: i32,
        part: &'a PartitionDesc,
    ) -> Result<&'a [u32], String> {
        match part.token_scheme() {
            PartitionTokenScheme::PartitionTokensStored => Ok(&part.tokens),
            PartitionTokenScheme::PartitionTokensSmt512 => {
                let tbl = token_table
                    .as_ref()
                    .ok_or_else(|| format!("cannot resolve tokens for partition {id}: no token generator"))?;
                tbl.tokens_for(id)
            }
        }
    }

    pub fn partitions_count(&self) -> usize {
        self.desc.partitions.len()
    }

    pub fn get_token_ranges_for_partition(&self, partition_id: i32) -> Result<Vec<u32>, String> {
        let part = self
            .desc
            .partitions
            .get(&partition_id)
            .ok_or_else(|| "partition does not exist".to_string())?;
        let tokens = Self::resolve_tokens_for_part(&self.token_table, partition_id, part)?;
        let mut ranges = Vec::with_capacity(2 * (tokens.len() + 1));

        let add_range = |ranges: &mut Vec<u32>, start: u32, end: u32| {
            if !ranges.is_empty() && ranges[ranges.len() - 1] == start.wrapping_sub(1) && start > 0
            {
                let last = ranges.len() - 1;
                ranges[last] = end;
            } else {
                ranges.push(start);
                ranges.push(end);
            }
        };

        let mut owns_last_range = false;
        let mut start_of_last_range = 0u32;

        for (iter, &t) in tokens.iter().enumerate() {
            let last_owned_token = t.wrapping_sub(1);
            let ix = search_token_u32(&self.ring_tokens, last_owned_token);
            if ix == 0 {
                if iter > 0 {
                    return Err("inconsistent tokens info".to_string());
                }
                let prev_ix = self.ring_tokens.len() - 1;
                owns_last_range = true;
                start_of_last_range = self.ring_tokens[prev_ix];
                if t > 0 {
                    add_range(&mut ranges, 0, last_owned_token);
                }
            } else {
                let prev_ix = ix - 1;
                add_range(&mut ranges, self.ring_tokens[prev_ix], last_owned_token);
            }
        }
        if owns_last_range {
            add_range(&mut ranges, start_of_last_range, u32::MAX);
        }
        Ok(ranges)
    }
}

pub fn add_partition_with_stored_tokens(
    desc: &mut PartitionRingDesc,
    id: i32,
    state: PartitionState,
    tokens: Vec<u32>,
) {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut part = PartitionDesc::default();
    part.set_state(state);
    part.state_timestamp = now_secs;
    part.tokens = tokens;
    part.set_token_scheme(PartitionTokenScheme::PartitionTokensStored);
    desc.partitions.insert(id, part);
}

pub fn add_partition_with_derived_tokens(
    desc: &mut PartitionRingDesc,
    id: i32,
    state: PartitionState,
) {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut part = PartitionDesc::default();
    part.set_state(state);
    part.state_timestamp = now_secs;
    part.set_token_scheme(PartitionTokenScheme::PartitionTokensSmt512);
    desc.partitions.insert(id, part);
}

pub struct PartitionRingWatcher {
    key: String,
    kv: Arc<InMemoryPartitionKv>,
    token_table: Option<Arc<PartitionTokenTable>>,
    ring: RwLock<Arc<PartitionRingSnapshot>>,
    _num_partitions: GaugeVec,
    _max_partition_id: Gauge,
    stop_tx: watch::Sender<bool>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl PartitionRingWatcher {
    pub fn new(
        name: &str,
        key: &str,
        kv: Arc<InMemoryPartitionKv>,
        token_table: Option<Arc<PartitionTokenTable>>,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        let mut const_labels = HashMap::new();
        const_labels.insert("name".to_string(), name.to_string());

        let num_partitions = GaugeVec::new(
            Opts::new(
                "partition_ring_partitions",
                "Number of partitions by state in the partitions ring.",
            )
            .const_labels(const_labels.clone()),
            &["state"],
        )
        .expect("valid metric");
        let max_partition_id = Gauge::with_opts(
            Opts::new(
                "partition_ring_max_partition_id",
                "Highest partition ID in the ring, or -1 when empty.",
            )
            .const_labels(const_labels),
        )
        .expect("valid metric");

        if let Some(r) = reg {
            r.register(Box::new(num_partitions.clone())).expect("register partition_ring_partitions without collision");
            r.register(Box::new(max_partition_id.clone())).expect("register partition_ring_max_partition_id without collision");
        }

        let empty = Arc::new(
            PartitionRingSnapshot::new(PartitionRingDesc::default(), token_table.clone())
                .expect("empty ring"),
        );
        let (stop_tx, _) = watch::channel(false);

        Arc::new(Self {
            key: key.to_string(),
            kv,
            token_table,
            ring: RwLock::new(empty),
            _num_partitions: num_partitions,
            _max_partition_id: max_partition_id,
            stop_tx,
            task: tokio::sync::Mutex::new(None),
        })
    }

    pub fn partition_ring(&self) -> Arc<PartitionRingSnapshot> {
        self.ring.read().clone()
    }

    pub async fn start(&self) -> Result<(), String> {
        let guard = self.task.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        if let Some(desc) = self.kv.get(&self.key) {
            let snap = Arc::new(PartitionRingSnapshot::new(desc, self.token_table.clone())?);
            *self.ring.write() = snap;
        }
        Ok(())
    }

    pub async fn start_arc(self: &Arc<Self>) -> Result<(), String> {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        if let Some(desc) = self.kv.get(&self.key) {
            let snap = Arc::new(PartitionRingSnapshot::new(desc, self.token_table.clone())?);
            *self.ring.write() = snap;
        }
        let me = Arc::clone(self);
        let mut sub = self.kv.subscribe(&self.key);
        let mut stop_rx = self.stop_tx.subscribe();
        let h = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    res = sub.changed() => {
                        if res.is_err() {
                            break;
                        }
                        if let Some(desc) = me.kv.get(&me.key) {
                            if let Ok(snap) = PartitionRingSnapshot::new(desc, me.token_table.clone()) {
                                *me.ring.write() = Arc::new(snap);
                            }
                        }
                    }
                }
            }
        });
        *guard = Some(h);
        Ok(())
    }

    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
    }
}

/// Manages one or more `PartitionRingWatcher`s (one per read compartment when enabled).
/// Ported from `NewPartitionRingWatchers` in `pkg/storage/ingest/partition_ring_watchers.go`.
pub struct PartitionRingWatchers {
    watchers: Vec<Arc<PartitionRingWatcher>>,
}

impl PartitionRingWatchers {
    pub fn new(
        compartments_enabled: bool,
        num_compartments: i32,
        ring_name: &str,
        ring_key: &str,
        kv: Arc<InMemoryPartitionKv>,
        token_table: Option<Arc<PartitionTokenTable>>,
        reg: Option<&Registry>,
    ) -> Result<Self, String> {
        let watchers = if !compartments_enabled {
            vec![PartitionRingWatcher::new(
                ring_name,
                ring_key,
                kv,
                token_table,
                reg,
            )]
        } else {
            if num_compartments < 1 {
                return Err(format!(
                    "the number of compartments must be at least 1, got {num_compartments}"
                ));
            }
            (0..num_compartments as usize)
                .map(|c| {
                    let name = with_read_compartment_suffix(ring_name, c);
                    let key = with_read_compartment_suffix(ring_key, c);
                    PartitionRingWatcher::new(&name, &key, Arc::clone(&kv), token_table.clone(), reg)
                })
                .collect()
        };

        Ok(Self { watchers })
    }

    pub fn count(&self) -> usize {
        self.watchers.len()
    }

    pub fn all(&self) -> &[Arc<PartitionRingWatcher>] {
        &self.watchers
    }

    pub fn watcher(&self, compartment_id: usize) -> &Arc<PartitionRingWatcher> {
        &self.watchers[compartment_id]
    }

    pub fn partition_ring(&self, compartment_id: usize) -> Arc<PartitionRingSnapshot> {
        self.watchers[compartment_id].partition_ring()
    }

    pub async fn start(&self) -> Result<(), String> {
        for w in &self.watchers {
            w.start_arc().await?;
        }
        Ok(())
    }

    pub async fn stop(&self) {
        for w in &self.watchers {
            w.stop().await;
        }
    }
}
