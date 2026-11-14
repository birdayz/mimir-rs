//! Kafka ingest_storage client, record v0-v2 codecs, readers, writers, and offset tracking.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/*
//! - github.com/grafana/mimir/pkg/ingester/offset_catalogue.go

pub mod compression;
pub mod heap_merger;
pub mod kafka_client;
pub mod offset_catalogue;
pub mod offset_file;
pub mod partition_offset_client;
pub mod partition_offset_reader;
pub mod partition_offset_watcher;
pub mod pusher;
pub mod reader;
pub mod reader_client;
pub mod util;
pub mod version;
pub mod writer;
pub mod writer_client;

pub use compression::KafkaCompression;
pub use heap_merger::{merge_records_in_timestamp_order, HeapMerger, HeapMergerMetrics, MergeableRecord};
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
    GenericOffsetReader, MultiClusterOffsetsReader, SingleClusterPartitionOffsetReader,
    SingleClusterTopicOffsetsReader,
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
    RecordBatchConsumer, SingleClusterPartitionReader, StrongReadConsistencyMetrics,
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
    SaslAuthConfig, ScramAlg, SpanRecorder, StaticSaslSecret, TraceSpanContext, TracedKafkaRecord,
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
