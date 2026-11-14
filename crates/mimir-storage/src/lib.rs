//! Object storage bucket client, Thanos shipper, and Kafka ingest_storage client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/*
//! - github.com/grafana/mimir/pkg/ingester/shipper.go
//! - github.com/grafana/mimir/pkg/storage/ingest/*

pub mod bucket;
pub mod ingest;
pub mod shipper;

pub use ulid;

pub use bucket::{
    read_tenant_deletion_mark, tenant_deletion_mark_exists, write_tenant_deletion_mark,
    BucketClient, BucketError, ObjectAttributes, TenantDeletionMark, DIR_DELIM,
    MIMIR_INTERNALS_PREFIX, TENANT_DELETION_MARK_PATH,
};
pub use shipper::{
    block_metas_from_oldest, gather_file_stats, get_segment_files, read_shipped_blocks,
    upload_block_to_bucket, write_shipper_meta_file, BlockDesc, BlockFile, BlockMeta,
    BlockMetaCompaction, BlockStats, ModelTime, Shipper, ShipperError, ShipperMeta,
    ShipperMetrics, SourceType, ThanosDownsample, ThanosMeta, CHUNKS_DIRNAME,
    COMPACTION_HINT_FROM_OUT_OF_ORDER, INDEX_FILENAME, META_FILENAME,
    OUT_OF_ORDER_EXTERNAL_LABEL, OUT_OF_ORDER_EXTERNAL_LABEL_VALUE, SHIPPER_META_FILENAME,
    SHIPPER_META_VERSION_1, SPARSE_INDEX_HEADER_FILENAME,
};
