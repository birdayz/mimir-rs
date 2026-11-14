//! Object storage bucket client, Thanos shipper, and Kafka ingest_storage client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/*
//! - github.com/grafana/mimir/pkg/ingester/shipper.go
//! - github.com/grafana/mimir/pkg/storage/ingest/*

pub mod bucket;
pub mod ingest;
pub mod shipper;
pub mod tsdb_storage;

pub use ulid;

pub use bucket::{
    read_tenant_deletion_mark, tenant_deletion_mark_exists, write_tenant_deletion_mark,
    BucketClient, BucketError, ObjectAttributes, TenantDeletionMark, DIR_DELIM,
    MIMIR_INTERNALS_PREFIX, TENANT_DELETION_MARK_PATH,
};
pub use shipper::{
    block_metas_from_oldest, delete_block, delete_no_compact_marker, download_meta,
    gather_file_stats, get_segment_files, is_block_dir, mark_for_deletion, mark_for_no_compact,
    read_shipped_blocks, upload_block, upload_block_to_bucket, write_shipper_meta_file, BlockDesc,
    BlockFile, BlockMeta, BlockMetaCompaction, BlockStats, DeletionMark, ModelTime, NoCompactMark,
    NoCompactReason, Shipper, ShipperError, ShipperMeta, ShipperMetrics, SourceType,
    ThanosDownsample, ThanosMeta, CHUNKS_DIRNAME, COMPACTION_HINT_FROM_OUT_OF_ORDER,
    DELETION_MARK_FILENAME, DELETION_MARK_VERSION_1, INDEX_FILENAME, META_FILENAME,
    NO_COMPACT_MARK_FILENAME, NO_COMPACT_MARK_VERSION_1, OUT_OF_ORDER_EXTERNAL_LABEL,
    OUT_OF_ORDER_EXTERNAL_LABEL_VALUE, SHIPPER_META_FILENAME, SHIPPER_META_VERSION_1,
    SPARSE_INDEX_HEADER_FILENAME, TSDB_VERSION_1,
};
pub use tsdb_storage::{
    all_users, hash_block_id, is_user_id_reserved, list_users, DurationList, TSDBMetrics,
    UsersScanner,
};
