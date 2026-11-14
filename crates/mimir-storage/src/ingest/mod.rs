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
pub mod reader;
pub mod version;
pub mod writer;

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
pub use reader::{
    decode_fetched_record, DecodedKafkaRecord, MultiClusterPartitionReader, ReaderError,
    ReaderMetrics, RecordBatchConsumer, SingleClusterPartitionReader, ZeroCopyBatchConsumer,
    ZeroCopyRecordHandler,
};
pub use version::{
    deserialize_record_content, parse_record_version, record_version_header,
    validate_record_version, RecordCodecError, RecordSerializer, RecordVersion,
    SerializedKafkaRecord, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
    MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT, PRODUCER_BATCH_MAX_BYTES,
};
pub use writer::{IngestWriter, PartitionWriteRequest, WriterError, WriterMetrics};
