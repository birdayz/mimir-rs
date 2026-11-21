//! Wave 26 Tier-1 Go-Oracle Expansion & Differential Conformance Suite.
//!
//! Differential tests against upstream Go Mimir packages via target/go-oracle:
//! - cmd/mimir/main.go, pkg/mimir/mimir.go (validate-config)
//! - pkg/mimir/runtime_config.go, pkg/util/validation/limits.go (load-runtime-config)
//! - pkg/mimirpb/mimir.pb.go, pkg/mimirpb/compat_rw2.go (decode-write-request)
//! - prometheus/tsdb/index/index.go (write-index-v1-fixture, verify-index-file)
//! - pkg/storage/ingest/version.go, pkg/storage/ingest/writer.go (serialize-ingest-records)

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use mimir_config::runtime_config::{
    DistributorInstanceLimits, RuntimeConfigManager, RuntimeConfigManagerOptions,
};
use mimir_config::{
    parse_cli_and_load_config, parse_go_duration, GoDuration, InstanceLimits, LimitsConfig,
    RuntimeConfigConfig, StringSliceCSV,
};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, DecodeWriteRequest, LabelJson, WriteRequestJson,
};
use mimir_proto::bytes::Bytes;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message as _;
use mimir_proto::symbols::{v2_common_symbols, RECORD_VERSION_HEADER_KEY, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{
    decode_classic_stream, decode_rw2_stream, decode_write_request,
    encode_write_request_rw2_bytes, encode_write_request_rw2_split_raw_bytes, DecodeOptions,
    Rw2ResolvedScratch, WriteRequestScratch,
};
use mimir_storage::ingest::version::{
    deserialize_record_content, parse_record_version, record_version_header, RecordSerializer,
    RecordVersion, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
};
use mimir_storage::ingest::writer::{
    err_write_request_data_item_too_large_msg, produce_results_err, WriterError,
};
use mimir_storage::ingest::writer_client::{ProduceResult, ProducerError, WriterRecord};
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::index::{IndexReader, IndexWriter, INDEX_FILENAME, INDEX_FORMAT_V1, INDEX_FORMAT_V2};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::postings::PostingsSource as _;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tempfile::tempdir;

// -------------------------------------------------------------------------------------------------
// Go-oracle DTOs for Wave 26 subcommands
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ValidateConfigReq<'a> {
    yaml: &'a str,
    args: &'a [&'a str],
}

#[derive(Debug, Deserialize)]
struct ValidateConfigResp {
    ok: bool,
    stage: String,
    error: String,
}

#[derive(Debug, Serialize)]
struct LoadRuntimeConfigReq<'a> {
    default_limits_yaml: &'a str,
    files: Vec<String>,
    loader: &'a str,
    query_store_after: &'a str,
}

#[derive(Debug, Deserialize)]
struct LoadRuntimeConfigResp {
    ok: bool,
    error: String,
    #[serde(default)]
    overrides_json: Option<BTreeMap<String, Option<JsonValue>>>,
}

#[derive(Debug, Serialize)]
struct IndexPathReq {
    path: String,
}

#[derive(Debug, Deserialize)]
struct IndexChunkMetaOut {
    ref_id: u64,
    min_time: i64,
    max_time: i64,
}

#[derive(Debug, Deserialize)]
struct IndexSeriesOut {
    ref_id: u64,
    labels: Vec<LabelJson>,
    chunks: Vec<IndexChunkMetaOut>,
}

#[derive(Debug, Deserialize)]
struct WriteIndexV1FixtureResp {
    version: u8,
    label_names: Vec<String>,
    sorted_label_values: BTreeMap<String, Vec<String>>,
    label_values: BTreeMap<String, Vec<String>>,
    postings: BTreeMap<String, Vec<u64>>,
    all_postings: Vec<u64>,
    series: Vec<IndexSeriesOut>,
}

#[derive(Debug, Deserialize)]
struct VerifyIndexFileResp {
    ok: bool,
    error: String,
    version: u8,
    #[serde(default)]
    label_names: Vec<String>,
    series_count: usize,
    #[serde(default)]
    query_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SerializeIngestRecordsReq<'a> {
    version: i32,
    user_id: &'a str,
    write_req_b64: String,
    max_record_bytes: usize,
    topic: &'a str,
    partition: i32,
}

#[derive(Debug, Deserialize)]
struct SerializeIngestRecordsResp {
    ok: bool,
    error: String,
    #[serde(default)]
    records_b64: Vec<String>,
    input_size: usize,
}

// -------------------------------------------------------------------------------------------------
// Deterministic PRNG & wire/stringlabels helpers
// -------------------------------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            0
        } else {
            (self.next_u64() as usize) % max
        }
    }
    fn mutate(&mut self, base: &[u8]) -> Vec<u8> {
        let mut buf = base.to_vec();
        if buf.is_empty() {
            buf.push((self.next_u64() & 0xff) as u8);
            return buf;
        }
        let op = self.next_usize(5);
        match op {
            0 => {
                // Single-byte flip
                let idx = self.next_usize(buf.len());
                buf[idx] ^= 1u8 + ((self.next_u64() % 255) as u8);
            }
            1 => {
                // Truncate
                let new_len = self.next_usize(buf.len());
                buf.truncate(new_len);
            }
            2 => {
                // Insert random byte
                let idx = self.next_usize(buf.len() + 1);
                buf.insert(idx, (self.next_u64() & 0xff) as u8);
            }
            3 => {
                // Delete slice
                if buf.len() > 2 {
                    let start = self.next_usize(buf.len() - 1);
                    let end = start + 1 + self.next_usize(buf.len() - start - 1).min(8);
                    buf.drain(start..end);
                }
            }
            _ => {
                // Multi-byte overwrite
                for _ in 0..3 {
                    let idx = self.next_usize(buf.len());
                    buf[idx] = (self.next_u64() & 0xff) as u8;
                }
            }
        }
        buf
    }
}

fn push_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            break;
        }
        out.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
}

fn push_tag(out: &mut Vec<u8>, field_num: u64, wire_type: u64) {
    push_varint(out, (field_num << 3) | wire_type);
}

fn push_bytes_field(out: &mut Vec<u8>, field_num: u64, data: &[u8]) {
    push_tag(out, field_num, 2);
    push_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn push_packed_u32_field(out: &mut Vec<u8>, field_num: u64, vals: &[u32]) {
    let mut inner = Vec::new();
    for &v in vals {
        push_varint(&mut inner, v as u64);
    }
    push_bytes_field(out, field_num, &inner);
}

fn build_ts_rw2_wire(
    labels_refs: &[u32],
    samples: &[(i64, f64)],
    exemplars_refs: &[&[u32]],
    metadata: Option<(i32, u32, u32)>,
    raw_extra: &[u8],
) -> Vec<u8> {
    let mut ts = Vec::new();
    if !labels_refs.is_empty() {
        push_packed_u32_field(&mut ts, 1, labels_refs);
    }
    for &(t, v) in samples {
        let mut s = Vec::new();
        push_tag(&mut s, 1, 1);
        s.extend_from_slice(&v.to_le_bytes());
        push_tag(&mut s, 2, 0);
        push_varint(&mut s, t as u64);
        push_bytes_field(&mut ts, 2, &s);
    }
    for ex_refs in exemplars_refs {
        let mut ex = Vec::new();
        if !ex_refs.is_empty() {
            push_packed_u32_field(&mut ex, 1, ex_refs);
        }
        push_tag(&mut ex, 2, 1);
        ex.extend_from_slice(&1.5f64.to_le_bytes());
        push_tag(&mut ex, 3, 0);
        push_varint(&mut ex, 1_700_000_000_000u64);
        push_bytes_field(&mut ts, 3, &ex);
    }
    if let Some((ty, help_ref, unit_ref)) = metadata {
        let mut md = Vec::new();
        if ty != 0 {
            push_tag(&mut md, 1, 0);
            push_varint(&mut md, ty as u64);
        }
        if help_ref != 0 {
            push_tag(&mut md, 3, 0);
            push_varint(&mut md, help_ref as u64);
        }
        if unit_ref != 0 {
            push_tag(&mut md, 4, 0);
            push_varint(&mut md, unit_ref as u64);
        }
        push_bytes_field(&mut ts, 5, &md);
    }
    ts.extend_from_slice(raw_extra);
    ts
}

// =================================================================================================
// Test 1: Validate Config Matrix (55+ valid & invalid config + CLI combinations)
// =================================================================================================

struct ValidateCase {
    name: &'static str,
    yaml: &'static str,
    args: &'static [&'static str],
}

#[test]
fn w26_oracle_validate_config_matrix() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let cases: Vec<ValidateCase> = vec![
        // --- Baseline ---
        ValidateCase {
            name: "baseline_ingester_valid",
            yaml: "target: ingester\n",
            args: &[],
        },
        ValidateCase {
            name: "baseline_all_valid",
            yaml: "",
            args: &["-target=ingester"],
        },
        // --- IngesterConfig: ring, partition ring, spread-minimizing, reactive limiter, label values count ---
        ValidateCase {
            name: "ingester_ring_invalid_kvstore",
            yaml: "target: ingester\ningester:\n  ring:\n    kvstore:\n      store: bogus_kv\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_ring_spread_minimizing_valid",
            yaml: "target: ingester\ningester:\n  ring:\n    token_generation_strategy: spread-minimizing\n    spread_minimizing_zones: zone-a,zone-b\n    instance_availability_zone: zone-a\n    instance_id: ingester-12\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_ring_spread_minimizing_i64_overflow",
            yaml: "target: ingester\ningester:\n  ring:\n    token_generation_strategy: spread-minimizing\n    spread_minimizing_zones: zone-a\n    instance_availability_zone: zone-a\n    instance_id: ingester-99999999999999999999\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_ring_spread_minimizing_newline_in_prefix",
            yaml: "target: ingester\ningester:\n  ring:\n    token_generation_strategy: spread-minimizing\n    spread_minimizing_zones: zone-a\n    instance_availability_zone: zone-a\n    instance_id: \"bad\\ningester-0\"\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_ring_spread_minimizing_bad_zone",
            yaml: "target: ingester\ningester:\n  ring:\n    token_generation_strategy: spread-minimizing\n    spread_minimizing_zones: zone-a\n    instance_availability_zone: zone-z\n    instance_id: ingester-0\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_partition_ring_valid",
            yaml: "target: ingester\ningester:\n  partition_ring:\n    max_derived_token_partitions: 32\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_partition_ring_zero_valid",
            yaml: "target: ingester\ningester:\n  partition_ring:\n    max_derived_token_partitions: 0\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_partition_ring_negative_invalid",
            yaml: "target: ingester\ningester:\n  partition_ring:\n    max_derived_token_partitions: -1\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_push_reactive_limiter_valid",
            yaml: "target: ingester\ningester:\n  push_reactive_limiter:\n    enabled: true\n    min_limit_factor: 1.5\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_push_reactive_limiter_invalid_factor",
            yaml: "target: ingester\ningester:\n  push_reactive_limiter:\n    enabled: true\n    min_limit_factor: 0.5\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_read_reactive_limiter_invalid_factor",
            yaml: "target: ingester\ningester:\n  read_reactive_limiter:\n    enabled: true\n    min_limit_factor: 0.9\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_label_values_count_valid",
            yaml: "target: ingester\ningester:\n  label_values_count:\n    chunk_size: 16\n",
            args: &[],
        },
        ValidateCase {
            name: "ingester_label_values_count_zero_invalid",
            yaml: "target: ingester\ningester:\n  label_values_count:\n    chunk_size: 0\n",
            args: &[],
        },
        // --- BlocksStorageConfig: S3, TSDB, all 4 bucket_store memcached caches ---
        ValidateCase {
            name: "blocks_s3_sts_endpoint_valid",
            yaml: "target: ingester\nblocks_storage:\n  backend: s3\n  s3:\n    sts_endpoint: https://sts.example.com:8443\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_s3_sts_endpoint_bad_port",
            yaml: "target: ingester\nblocks_storage:\n  backend: s3\n  s3:\n    sts_endpoint: \"https://example.com:notaport\"\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_s3_sts_endpoint_unclosed_ipv6",
            yaml: "target: ingester\nblocks_storage:\n  backend: s3\n  s3:\n    sts_endpoint: \"https://[::1\"\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_s3_kms_encryption_context_valid",
            yaml: "target: ingester\nblocks_storage:\n  backend: s3\n  s3:\n    sse:\n      type: SSE-KMS\n      kms_encryption_context: '{\"k\": \"v\"}'\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_s3_kms_encryption_context_json_null",
            yaml: "target: ingester\nblocks_storage:\n  backend: s3\n  s3:\n    sse:\n      type: SSE-KMS\n      kms_encryption_context: '{\"k\": null}'\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_s3_kms_encryption_context_invalid_json",
            yaml: "target: ingester\nblocks_storage:\n  backend: s3\n  s3:\n    sse:\n      type: SSE-KMS\n      kms_encryption_context: '{not_json'\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_tsdb_stripe_size_valid",
            yaml: "target: ingester\nblocks_storage:\n  tsdb:\n    stripe_size: 16384\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_tsdb_stripe_size_not_power_of_two",
            yaml: "target: ingester\nblocks_storage:\n  tsdb:\n    stripe_size: 100\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_tsdb_write_buffer_size_valid",
            yaml: "target: ingester\nblocks_storage:\n  tsdb:\n    head_chunks_write_buffer_size_bytes: 65536\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_tsdb_write_buffer_size_too_small",
            yaml: "target: ingester\nblocks_storage:\n  tsdb:\n    head_chunks_write_buffer_size_bytes: 1024\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_tsdb_write_buffer_size_not_multiple_1024",
            yaml: "target: ingester\nblocks_storage:\n  tsdb:\n    head_chunks_write_buffer_size_bytes: 65537\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_index_cache_memcached_valid",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    index_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_index_cache_memcached_missing_addrs",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    index_cache:\n      backend: memcached\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_index_cache_memcached_zero_lookup_period",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    index_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n        addresses_lookup_period: 0s\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_chunks_cache_memcached_valid",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    chunks_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_chunks_cache_memcached_zero_concurrency",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    chunks_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n        max_async_concurrency: 0\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_index_header_cache_memcached_valid",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    index_header_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_index_header_cache_memcached_zero_shards",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    index_header_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n        free_list_shards: 0\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_metadata_cache_memcached_valid",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    metadata_cache:\n      backend: memcached\n      memcached:\n        addresses: dns+memcached:11211\n",
            args: &[],
        },
        ValidateCase {
            name: "blocks_bucket_store_metadata_cache_memcached_missing_addrs",
            yaml: "target: ingester\nblocks_storage:\n  bucket_store:\n    metadata_cache:\n      backend: memcached\n",
            args: &[],
        },
        // --- IngestStorageConfig ---
        ValidateCase {
            name: "ingest_storage_disabled_empty_sasl_mechanism",
            yaml: "target: ingester\ningest_storage:\n  enabled: false\n  kafka:\n    sasl_mechanism: \"\"\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_disabled_invalid_sasl_mechanism",
            yaml: "target: ingester\ningest_storage:\n  enabled: false\n  kafka:\n    sasl_mechanism: INVALID\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_invalid_sasl_mechanism",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    sasl_mechanism: INVALID\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_sasl_plain",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    sasl_mechanism: PLAIN\n    sasl_username: user\n    sasl_password: secret\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_sasl_scram_sha_256",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    sasl_mechanism: SCRAM-SHA-256\n    sasl_username: user\n    sasl_password: secret\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_sasl_scram_sha_512",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    sasl_mechanism: SCRAM-SHA-512\n    sasl_username: user\n    sasl_password: secret\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_sasl_oauthbearer",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    sasl_mechanism: OAUTHBEARER\n    sasl_oauthbearer_token: tok123\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_sasl_aws_msk_iam",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    sasl_mechanism: AWS_MSK_IAM\n    sasl_msk_iam_access_key: ak\n    sasl_msk_iam_secret_key: sk\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_lag_bounds_valid",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    target_consumer_lag_at_startup: 2s\n    max_consumer_lag_at_startup: 15s\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_lag_bounds_inconsistent",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    target_consumer_lag_at_startup: 10s\n    max_consumer_lag_at_startup: 0s\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_lag_max_lt_target",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    target_consumer_lag_at_startup: 10s\n    max_consumer_lag_at_startup: 5s\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_fetch_max_wait_too_low",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    fetch_max_wait: 1s\n",
            args: &[],
        },
        ValidateCase {
            name: "ingest_storage_enabled_fetch_max_wait_too_high",
            yaml: "target: ingester\ningest_storage:\n  enabled: true\n  kafka:\n    address: 127.0.0.1:9092\n    topic: ingest\n    fetch_max_wait: 60s\n",
            args: &[],
        },
        // --- LimitsConfig ---
        ValidateCase {
            name: "limits_yaml_invalid_otel_strategy_with_cli_override",
            yaml: "target: ingester\nlimits:\n  otel_translation_strategy: NoUTF8EscapingWithSuffixes\n",
            args: &[
                "-validation.name-validation-scheme=utf8",
                "-distributor.otel-metric-suffixes-enabled=true",
            ],
        },
        ValidateCase {
            name: "limits_yaml_invalid_multiplier_with_cli_override",
            yaml: "target: ingester\nlimits:\n  max_estimated_fetched_chunks_per_query_multiplier: -1\n",
            args: &["-querier.max-estimated-fetched-chunks-per-query-multiplier=0"],
        },
        ValidateCase {
            name: "limits_negative_max_label_value_length_truncate",
            yaml: "target: ingester\nlimits:\n  max_label_value_length: -1\n  label_value_length_over_limit_strategy: truncate\n",
            args: &[],
        },
        ValidateCase {
            name: "limits_negative_max_label_value_length_drop",
            yaml: "target: ingester\nlimits:\n  max_label_value_length: -1\n  label_value_length_over_limit_strategy: drop\n",
            args: &[],
        },
        ValidateCase {
            name: "limits_active_series_custom_trackers_valid",
            yaml: "target: ingester\nlimits:\n  active_series_custom_trackers:\n    api_series: '{job=\"api\"}'\n",
            args: &[],
        },
        ValidateCase {
            name: "limits_active_series_custom_trackers_invalid_matcher",
            yaml: "target: ingester\nlimits:\n  active_series_custom_trackers:\n    bad: '{foo}'\n",
            args: &[],
        },
        ValidateCase {
            name: "limits_blocked_queries_valid",
            yaml: "target: ingester\nlimits:\n  blocked_queries:\n    - pattern: up\n",
            args: &[],
        },
        ValidateCase {
            name: "limits_blocked_queries_invalid_regex",
            yaml: "target: ingester\nlimits:\n  blocked_queries:\n    - pattern: \"[\"\n      regex: true\n",
            args: &[],
        },
        // --- ServerConfig ---
        ValidateCase {
            name: "server_cluster_validation_without_label",
            yaml: "target: ingester\nserver:\n  cluster_validation:\n    grpc:\n      enabled: true\n",
            args: &[],
        },
        ValidateCase {
            name: "server_openmetrics_without_instrumentation",
            yaml: "target: ingester\nserver:\n  enable_open_metrics_text_created_samples: true\n  register_instrumentation: false\n",
            args: &[],
        },
        // --- MemberlistConfig ---
        ValidateCase {
            name: "memberlist_zone_aware_routing_valid",
            yaml: "target: ingester\nmemberlist:\n  zone_aware_routing:\n    enabled: true\n    instance_availability_zone: zone-a\n    role: member\n",
            args: &[],
        },
        ValidateCase {
            name: "memberlist_zone_aware_routing_missing_zone",
            yaml: "target: ingester\nmemberlist:\n  zone_aware_routing:\n    enabled: true\n    role: member\n",
            args: &[],
        },
        ValidateCase {
            name: "memberlist_propagation_delay_tracker_invalid_interval",
            yaml: "target: ingester\nmemberlist:\n  propagation_delay_tracker:\n    enabled: true\n    beacon_interval: 0s\n",
            args: &[],
        },
        // --- Cross-module validation on -target=ingester ---
        ValidateCase {
            name: "cross_module_querier_invalid_engine",
            yaml: "target: ingester\nquerier:\n  query_engine: invalid_engine\n",
            args: &[],
        },
        ValidateCase {
            name: "cross_module_ruler_storage_invalid_backend",
            yaml: "target: ingester\nruler_storage:\n  backend: invalid_backend\n",
            args: &[],
        },
        ValidateCase {
            name: "cross_module_alertmanager_storage_invalid_backend",
            yaml: "target: ingester\nalertmanager_storage:\n  backend: invalid_backend\n",
            args: &[],
        },
        ValidateCase {
            name: "cross_module_distributor_invalid_reactive_limiter",
            yaml: "target: ingester\ndistributor:\n  reactive_limiter:\n    enabled: true\n    min_limit_factor: 0.5\n",
            args: &[],
        },
        ValidateCase {
            name: "cross_module_overrides_exporter_invalid_metric",
            yaml: "target: ingester\noverrides_exporter:\n  enabled_metrics: not_a_limit\n",
            args: &[],
        },
    ];

    assert!(cases.len() >= 55, "expected 55+ cases, got {}", cases.len());

    let dir = tempdir().expect("create tempdir");
    let mut mismatches = Vec::new();

    for (idx, tc) in cases.iter().enumerate() {
        let go_resp: ValidateConfigResp = invoke_oracle(
            "validate-config",
            &ValidateConfigReq {
                yaml: tc.yaml,
                args: tc.args,
            },
        )
        .unwrap_or_else(|e| panic!("go-oracle failed on case {idx} ({}): {e}", tc.name));

        let mut rust_args: Vec<String> = Vec::new();
        if !tc.yaml.is_empty() {
            let cfg_path = dir.path().join(format!("case_{idx:03}_{}.yaml", tc.name));
            fs::write(&cfg_path, tc.yaml).expect("write temp config");
            rust_args.push(format!("-config.file={}", cfg_path.display()));
        }
        rust_args.extend(tc.args.iter().map(|s| (*s).to_string()));

        let rust_res = parse_cli_and_load_config("mimir", &rust_args);
        let rust_ok = rust_res.is_ok();
        if rust_ok != go_resp.ok {
            mismatches.push(format!(
                "case {idx} ({}): go ok={} (stage={}, err={:?}) vs rust ok={} (err={:?})",
                tc.name,
                go_resp.ok,
                go_resp.stage,
                go_resp.error,
                rust_ok,
                rust_res.err().map(|e| e.to_string()),
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "validate-config parity mismatches ({}/{}):\n{}",
        mismatches.len(),
        cases.len(),
        mismatches.join("\n")
    );
}

// =================================================================================================
// Test 2: Runtime Config Overrides Parity
// =================================================================================================

struct RuntimeCase {
    name: &'static str,
    default_limits_yaml: &'static str,
    files_yaml: &'static [&'static str],
    loader: &'static str,
    query_store_after: &'static str,
}

fn load_default_limits_from_yaml(
    dir: &std::path::Path,
    idx: usize,
    limits_yaml: &str,
) -> LimitsConfig {
    if limits_yaml.trim().is_empty() {
        return LimitsConfig::default();
    }
    let indented: String = limits_yaml
        .lines()
        .map(|l| format!("  {l}\n"))
        .collect();
    let full_yaml = format!("target: ingester\nlimits:\n{indented}");
    let p = dir.join(format!("defaults_{idx}.yaml"));
    fs::write(&p, full_yaml).expect("write defaults yaml");
    let loaded = parse_cli_and_load_config("mimir", &[format!("-config.file={}", p.display())])
        .expect("default limits yaml must be valid");
    loaded.config.limits
}

#[test]
fn w26_oracle_runtime_config_overrides_parity() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let cases: Vec<RuntimeCase> = vec![
        RuntimeCase {
            name: "global_otel_underscore_with_omitted_tenant_otel_rejected",
            default_limits_yaml: "otel_metric_suffixes_enabled: true\notel_translation_strategy: UnderscoreEscapingWithSuffixes\n",
            files_yaml: &["overrides:\n  t1:\n    max_global_series_per_user: 100\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "global_otel_noutf8_with_omitted_tenant_otel_rejected",
            default_limits_yaml: "name_validation_scheme: utf8\notel_metric_suffixes_enabled: true\notel_translation_strategy: NoUTF8EscapingWithSuffixes\n",
            files_yaml: &["overrides:\n  t1:\n    max_global_series_per_user: 100\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "global_otel_with_explicit_tenant_otel_accepted",
            default_limits_yaml: "name_validation_scheme: utf8\notel_metric_suffixes_enabled: true\notel_translation_strategy: NoUTF8EscapingWithSuffixes\n",
            files_yaml: &["overrides:\n  t1:\n    max_global_series_per_user: 100\n    name_validation_scheme: utf8\n    otel_metric_suffixes_enabled: true\n  t_null: null\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "t_null_and_numeric_tenant_ids_yaml_loader",
            default_limits_yaml: "max_global_series_per_user: 500\n",
            files_yaml: &["overrides:\n  12345:\n    max_global_series_per_user: 42\n  t_null: null\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "t_null_and_numeric_tenant_ids_map_loader",
            default_limits_yaml: "max_global_series_per_user: 500\n",
            files_yaml: &["overrides:\n  12345:\n    max_global_series_per_user: 42\n  t_null: null\n"],
            loader: "map",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "cost_attribution_trackers_overlay_and_null_reset_yaml",
            default_limits_yaml: "cost_attribution_trackers:\n  global_base:\n    labels:\n      - input: team\n        output: team\nadditional_cost_attribution_trackers:\n  global_add:\n    labels:\n      - input: env\n        output: env\n",
            files_yaml: &[
                "overrides:\n  t_overlay:\n    max_global_series_per_user: 100\n    cost_attribution_trackers:\n      tenant_base:\n        labels:\n          - input: svc\n            output: svc\n    additional_cost_attribution_trackers:\n      tenant_add:\n        labels:\n          - input: region\n            output: region\n  t_null_ca:\n    cost_attribution_trackers: null\n",
            ],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "protected_namespaces_replacement_map_loader",
            default_limits_yaml: "ruler_protected_namespaces: ns1\nmax_global_series_per_user: 777\n",
            files_yaml: &[
                "overrides:\n  t_overlay:\n    ruler_protected_namespaces: ns2,ns3\n",
            ],
            loader: "map",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "multi_file_merge_yaml_vs_map",
            default_limits_yaml: "",
            files_yaml: &[
                "overrides:\n  t1:\n    max_global_series_per_user: 100\n    max_label_names_per_series: 25\n  t2:\n    max_global_series_per_user: 200\n",
                "overrides:\n  t1:\n    max_global_series_per_user: 333\n  t3: null\n",
            ],
            loader: "map",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "invalid_query_ingesters_within_vs_query_store_after",
            default_limits_yaml: "",
            files_yaml: &["overrides:\n  t1:\n    query_ingesters_within: 10h\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "invalid_per_tenant_blocked_queries_regex",
            default_limits_yaml: "",
            files_yaml: &["overrides:\n  t1:\n    blocked_queries:\n      - pattern: \"[\"\n        regex: true\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "invalid_per_tenant_custom_trackers_matcher",
            default_limits_yaml: "",
            files_yaml: &["overrides:\n  t1:\n    active_series_custom_trackers:\n      bad: \"{foo}\"\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
        RuntimeCase {
            name: "invalid_per_tenant_negative_label_value_length_truncate",
            default_limits_yaml: "",
            files_yaml: &["overrides:\n  t1:\n    max_label_value_length: -1\n    label_value_length_over_limit_strategy: truncate\n"],
            loader: "yaml",
            query_store_after: "12h",
        },
    ];

    let dir = tempdir().expect("create tempdir");
    let mut mismatches = Vec::new();

    for (idx, tc) in cases.iter().enumerate() {
        let mut file_paths = Vec::with_capacity(tc.files_yaml.len());
        for (fidx, contents) in tc.files_yaml.iter().enumerate() {
            let fp = dir.path().join(format!("rc_{idx}_{fidx}.yaml"));
            fs::write(&fp, contents).expect("write runtime config file");
            file_paths.push(fp.display().to_string());
        }

        let go_resp: LoadRuntimeConfigResp = invoke_oracle(
            "load-runtime-config",
            &LoadRuntimeConfigReq {
                default_limits_yaml: tc.default_limits_yaml,
                files: file_paths.clone(),
                loader: tc.loader,
                query_store_after: tc.query_store_after,
            },
        )
        .unwrap_or_else(|e| panic!("go-oracle load-runtime-config failed ({}): {e}", tc.name));

        let default_limits =
            load_default_limits_from_yaml(dir.path(), idx, tc.default_limits_yaml);
        let qsa = parse_go_duration(tc.query_store_after).expect("valid query_store_after");
        let rc_cfg = RuntimeConfigConfig {
            file: StringSliceCSV(file_paths),
            loader: tc.loader.to_string(),
            ..RuntimeConfigConfig::default()
        };

        let rust_mgr = RuntimeConfigManager::new_with_options(RuntimeConfigManagerOptions {
            cfg: rc_cfg,
            default_limits,
            default_ingester_limits: InstanceLimits::default(),
            default_distributor_limits: DistributorInstanceLimits::default(),
            querier_query_store_after: GoDuration(qsa),
            observer: None,
            http_observer: None,
        });

        let rust_ok = rust_mgr.is_ok();
        if rust_ok != go_resp.ok {
            mismatches.push(format!(
                "case {} ({}): go ok={} (err={:?}) vs rust ok={} (err={:?})",
                idx,
                tc.name,
                go_resp.ok,
                go_resp.error,
                rust_ok,
                rust_mgr.err().map(|e| e.to_string()),
            ));
            continue;
        }

        if let (Ok(mgr), Some(go_ov)) = (rust_mgr, go_resp.overrides_json) {
            let rust_vals = mgr.get_config();
            let go_keys: BTreeSet<&str> = go_ov.keys().map(String::as_str).collect();
            let rust_keys: BTreeSet<&str> =
                rust_vals.overrides.keys().map(String::as_str).collect();
            assert_eq!(
                rust_keys, go_keys,
                "case {} ({}): tenant key set mismatch",
                idx, tc.name
            );

            for (tenant, go_val_opt) in &go_ov {
                let rust_opt = rust_vals.overrides.get(tenant).expect("tenant exists");
                match (go_val_opt, rust_opt) {
                    (None, None) => {}
                    (Some(go_obj), Some(rust_lim)) => {
                        if let Some(go_max) = go_obj
                            .get("max_global_series_per_user")
                            .and_then(JsonValue::as_i64)
                        {
                            assert_eq!(
                                rust_lim.max_global_series_per_user, go_max,
                                "case {} ({}) tenant {tenant} max_global_series_per_user",
                                idx, tc.name
                            );
                        }
                        if let Some(go_ca) = go_obj
                            .get("cost_attribution_trackers")
                            .and_then(JsonValue::as_object)
                        {
                            let go_ca_keys: BTreeSet<&str> =
                                go_ca.keys().map(String::as_str).collect();
                            let rust_ca_keys: BTreeSet<&str> = rust_lim
                                .cost_attribution_trackers
                                .keys()
                                .map(String::as_str)
                                .collect();
                            assert_eq!(
                                rust_ca_keys, go_ca_keys,
                                "case {} ({}) tenant {tenant} cost_attribution_trackers keys",
                                idx, tc.name
                            );
                        } else {
                            assert!(
                                rust_lim.cost_attribution_trackers.is_empty(),
                                "case {} ({}) tenant {tenant} expected empty cost_attribution_trackers",
                                idx,
                                tc.name
                            );
                        }
                    }
                    other => panic!(
                        "case {} ({}) tenant {tenant} nullness mismatch: {other:?}",
                        idx, tc.name
                    ),
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "runtime-config parity mismatches ({}/{}):\n{}",
        mismatches.len(),
        cases.len(),
        mismatches.join("\n")
    );
}

// =================================================================================================
// Test 3: Adversarial RW2 & Classic Wire Differential
// =================================================================================================

fn check_wire_against_go(name: &str, mode_str: &str, raw: &[u8], mismatches: &mut Vec<String>) {
    let go_res = invoke_oracle::<_, WriteRequestJson>(
        "decode-write-request",
        &DecodeWriteRequest {
            mode: mode_str.to_string(),
            bytes_b64: B64.encode(raw),
        },
    );

    let opts = match mode_str {
        "classic" => DecodeOptions::classic(),
        "rw2" => DecodeOptions::rw2(),
        "kafka-v2" => DecodeOptions::kafka_v2(),
        other => panic!("unexpected mode {other}"),
    };

    let mut scratch = WriteRequestScratch::new();
    let rust_dec = decode_write_request(raw, opts, &mut scratch);

    let mut label_arena = Vec::new();
    let mut samples_arena = Vec::new();
    let mut stream_series_count = 0usize;
    let mut stream_err: Option<String> = None;

    if mode_str == "classic" {
        let mut s_scratch = WriteRequestScratch::new();
        if let Err(e) = decode_classic_stream(
            raw,
            opts,
            &mut s_scratch,
            &mut label_arena,
            &mut samples_arena,
            |_| stream_series_count += 1,
        ) {
            stream_err = Some(e.to_string());
        }
    } else {
        let mut s_scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut meta_out = Vec::new();
        if let Err(e) = decode_rw2_stream(
            raw,
            opts,
            &mut s_scratch,
            &mut resolved,
            &mut label_arena,
            &mut samples_arena,
            &mut meta_out,
            |_| stream_series_count += 1,
        ) {
            stream_err = Some(e.to_string());
        }
    }

    let go_ok = go_res.is_ok();
    let rust_dec_ok = rust_dec.is_ok();
    let rust_stream_ok = stream_err.is_none();

    if go_ok != rust_dec_ok || go_ok != rust_stream_ok {
        mismatches.push(format!(
            "{name} [{mode_str}]: go_ok={go_ok} ({:?}) vs rust_dec_ok={rust_dec_ok} ({:?}) vs rust_stream_ok={rust_stream_ok} ({:?})",
            go_res.err().map(|e| e.to_string()),
            rust_dec.err().map(|e| e.to_string()),
            stream_err,
        ));
        return;
    }

    if let (Ok(go_wr), Ok(rust_wr)) = (go_res, rust_dec) {
        if go_wr.timeseries.len() != rust_wr.timeseries_len()
            || go_wr.timeseries.len() != stream_series_count
        {
            mismatches.push(format!(
                "{name} [{mode_str}]: series count mismatch go={} rust_dec={} rust_stream={}",
                go_wr.timeseries.len(),
                rust_wr.timeseries_len(),
                stream_series_count
            ));
        }
        if go_wr.metadata.len() != rust_wr.metadata_len() {
            mismatches.push(format!(
                "{name} [{mode_str}]: metadata count mismatch go={} rust={}",
                go_wr.metadata.len(),
                rust_wr.metadata_len()
            ));
        }
    }
}

#[test]
fn w26_oracle_rw2_adversarial_wire_differential() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let mut mismatches = Vec::new();

    // 1. Field 5 before field 4 (RW2, local refs [1, 2]) -> Go rejects, Rust MUST reject.
    {
        let mut wire = Vec::new();
        let ts = build_ts_rw2_wire(&[1, 2], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts);
        push_bytes_field(&mut wire, 4, b"");
        push_bytes_field(&mut wire, 4, b"__name__");
        push_bytes_field(&mut wire, 4, b"up");
        check_wire_against_go("f5_before_f4_local_rw2", "rw2", &wire, &mut mismatches);
    }

    // 2. Field 5 before field 4 (KafkaV2, local refs [1, 65]) -> Go rejects, Rust MUST reject.
    {
        let mut wire = Vec::new();
        let ts = build_ts_rw2_wire(&[1, 65], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts);
        push_bytes_field(&mut wire, 4, b"");
        push_bytes_field(&mut wire, 4, b"custom_metric");
        check_wire_against_go("f5_before_f4_local_kv2", "kafka-v2", &wire, &mut mismatches);
    }

    // 3. Field 5 before field 4 (KafkaV2, common-only refs [1, 1]).
    {
        let mut wire = Vec::new();
        let ts = build_ts_rw2_wire(&[1, 1], &[(1000, 2.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts);
        push_bytes_field(&mut wire, 4, b"");
        check_wire_against_go("f5_before_f4_common_only_kv2", "kafka-v2", &wire, &mut mismatches);
    }

    // 4. Field 5 before field 4 (RW2, ref-0 only [0, 0]).
    {
        let mut wire = Vec::new();
        let ts = build_ts_rw2_wire(&[0, 0], &[(1000, 3.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts);
        push_bytes_field(&mut wire, 4, b"");
        check_wire_against_go("f5_before_f4_ref0_rw2", "rw2", &wire, &mut mismatches);
    }

    // 5. Interleaved field 4 -> field 5 -> field 4 -> field 5.
    {
        let mut wire = Vec::new();
        push_bytes_field(&mut wire, 4, b"");
        push_bytes_field(&mut wire, 4, b"__name__");
        push_bytes_field(&mut wire, 4, b"m1");
        let ts1 = build_ts_rw2_wire(&[1, 2], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts1);
        push_bytes_field(&mut wire, 4, b"m2");
        let ts2 = build_ts_rw2_wire(&[1, 3], &[(2000, 2.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts2);
        check_wire_against_go("interleaved_f4_f5_f4_f5_valid", "rw2", &wire, &mut mismatches);

        // Interleaved where first ts references symbol defined in later f4:
        let mut wire_bad = Vec::new();
        push_bytes_field(&mut wire_bad, 4, b"");
        push_bytes_field(&mut wire_bad, 4, b"__name__");
        let ts_oob = build_ts_rw2_wire(&[1, 2], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire_bad, 5, &ts_oob);
        push_bytes_field(&mut wire_bad, 4, b"m_late");
        check_wire_against_go(
            "interleaved_f4_f5_early_ref_later_f4",
            "rw2",
            &wire_bad,
            &mut mismatches,
        );
    }

    // 6. Field 5 before malformed field 4 (non-empty first symbol).
    {
        let mut wire = Vec::new();
        let ts = build_ts_rw2_wire(&[1, 2], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts);
        push_bytes_field(&mut wire, 4, b"not_empty");
        check_wire_against_go("f5_before_malformed_f4", "rw2", &wire, &mut mismatches);
    }

    // 7. Duplicate __name__ in labels_refs.
    {
        let mut wire = Vec::new();
        push_bytes_field(&mut wire, 4, b"");
        push_bytes_field(&mut wire, 4, b"__name__");
        push_bytes_field(&mut wire, 4, b"a");
        push_bytes_field(&mut wire, 4, b"b");
        let ts = build_ts_rw2_wire(&[1, 2, 1, 3], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire, 5, &ts);
        check_wire_against_go("dup_name_rw2", "rw2", &wire, &mut mismatches);
    }

    // 8. Empty MetadataRW2 (0x2a 0x00 inside TimeSeriesRW2).
    {
        let mut wire = Vec::new();
        push_bytes_field(&mut wire, 4, b"");
        push_bytes_field(&mut wire, 4, b"__name__");
        push_bytes_field(&mut wire, 4, b"up");
        let ts = build_ts_rw2_wire(&[1, 2], &[(1000, 1.0)], &[], Some((0, 0, 0)), &[]);
        push_bytes_field(&mut wire, 5, &ts);
        check_wire_against_go("empty_metadata_rw2", "rw2", &wire, &mut mismatches);
    }

    // 9. Odd & unpacked labels_refs.
    {
        let mut wire_odd = Vec::new();
        push_bytes_field(&mut wire_odd, 4, b"");
        push_bytes_field(&mut wire_odd, 4, b"__name__");
        push_bytes_field(&mut wire_odd, 4, b"up");
        let ts_odd = build_ts_rw2_wire(&[1, 2, 1], &[(1000, 1.0)], &[], None, &[]);
        push_bytes_field(&mut wire_odd, 5, &ts_odd);
        check_wire_against_go("odd_packed_labels_refs", "rw2", &wire_odd, &mut mismatches);

        // Unpacked wire_type=0 labels_refs: [field 1 varint 1, field 1 varint 2]
        let mut wire_unpacked = Vec::new();
        push_bytes_field(&mut wire_unpacked, 4, b"");
        push_bytes_field(&mut wire_unpacked, 4, b"__name__");
        push_bytes_field(&mut wire_unpacked, 4, b"up");
        let mut ts_unpacked = Vec::new();
        push_tag(&mut ts_unpacked, 1, 0);
        push_varint(&mut ts_unpacked, 1);
        push_tag(&mut ts_unpacked, 1, 0);
        push_varint(&mut ts_unpacked, 2);
        push_bytes_field(&mut wire_unpacked, 5, &ts_unpacked);
        check_wire_against_go("unpacked_labels_refs", "rw2", &wire_unpacked, &mut mismatches);

        // Unpacked single odd varint: [0x08, 0x01]
        let mut wire_unpacked_odd = Vec::new();
        push_bytes_field(&mut wire_unpacked_odd, 4, b"");
        push_bytes_field(&mut wire_unpacked_odd, 4, b"__name__");
        let ts_u_odd = vec![0x08u8, 0x01];
        push_bytes_field(&mut wire_unpacked_odd, 5, &ts_u_odd);
        check_wire_against_go(
            "unpacked_odd_labels_refs",
            "rw2",
            &wire_unpacked_odd,
            &mut mismatches,
        );
    }

    // 10. Non-UTF-8 symbols, exemplars, metadata across classic, rw2, kafka-v2.
    {
        // Classic raw non-UTF-8
        let mut lp1 = Vec::new();
        push_bytes_field(&mut lp1, 1, b"__name__");
        push_bytes_field(&mut lp1, 2, b"metric_\xff_name");
        let mut ts1 = Vec::new();
        push_bytes_field(&mut ts1, 1, &lp1);
        let mut samp = Vec::new();
        push_tag(&mut samp, 1, 1);
        samp.extend_from_slice(&42.0f64.to_le_bytes());
        push_tag(&mut samp, 2, 0);
        push_varint(&mut samp, 1000);
        push_bytes_field(&mut ts1, 2, &samp);
        let mut ex_lp = Vec::new();
        push_bytes_field(&mut ex_lp, 1, b"trace");
        push_bytes_field(&mut ex_lp, 2, b"\xfe\x80");
        let mut ex = Vec::new();
        push_bytes_field(&mut ex, 1, &ex_lp);
        push_tag(&mut ex, 2, 1);
        ex.extend_from_slice(&1.0f64.to_le_bytes());
        push_tag(&mut ex, 3, 0);
        push_varint(&mut ex, 1001);
        push_bytes_field(&mut ts1, 3, &ex);
        let mut classic_wire = Vec::new();
        push_bytes_field(&mut classic_wire, 1, &ts1);
        check_wire_against_go("non_utf8_classic", "classic", &classic_wire, &mut mismatches);

        // RW2 & KafkaV2 non-UTF-8 symbols + exemplar + metadata help/unit
        let mut rw2_wire = Vec::new();
        push_bytes_field(&mut rw2_wire, 4, b"");
        push_bytes_field(&mut rw2_wire, 4, b"__name__");
        push_bytes_field(&mut rw2_wire, 4, b"metric_\xff");
        push_bytes_field(&mut rw2_wire, 4, b"trace_\xfe");
        push_bytes_field(&mut rw2_wire, 4, b"help_\xfd");
        let ts_rw2 = build_ts_rw2_wire(&[1, 2], &[(1000, 1.0)], &[&[3, 2]], Some((1, 4, 0)), &[]);
        push_bytes_field(&mut rw2_wire, 5, &ts_rw2);
        check_wire_against_go("non_utf8_rw2", "rw2", &rw2_wire, &mut mismatches);

        let mut kv2_wire = Vec::new();
        push_bytes_field(&mut kv2_wire, 4, b"");
        push_bytes_field(&mut kv2_wire, 4, b"metric_\xff");
        push_bytes_field(&mut kv2_wire, 4, b"trace_\xfe");
        push_bytes_field(&mut kv2_wire, 4, b"help_\xfd");
        let ts_kv2 = build_ts_rw2_wire(
            &[1, 65],
            &[(1000, 1.0)],
            &[&[66, 65]],
            Some((1, 67, 0)),
            &[],
        );
        push_bytes_field(&mut kv2_wire, 5, &ts_kv2);
        check_wire_against_go("non_utf8_kafka_v2", "kafka-v2", &kv2_wire, &mut mismatches);
    }

    // 11. 200 deterministic mutated RW2 wire payloads.
    let base_req = cortexpb::WriteRequest {
        timeseries: vec![
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                    cortexpb::LabelPair::from_static("job", "api"),
                    cortexpb::LabelPair::from_static("method", "GET"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 12.5,
                    timestamp_ms: 1_700_000_000_000,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair::from_static("trace_id", "abc")],
                    value: 0.5,
                    timestamp_ms: 1_700_000_000_100,
                }],
                ..Default::default()
            },
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("instance", "10.0.0.1:8080"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1_700_000_000_000,
                }],
                ..Default::default()
            },
        ],
        metadata: vec![cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: "http_requests_total".to_string(),
            help: "total requests".to_string(),
            unit: "1".to_string(),
        }],
        ..Default::default()
    };

    let mut base_rw2 = Vec::new();
    encode_write_request_rw2_bytes(&base_req, 0, None, &mut base_rw2).unwrap();
    let mut base_kv2 = Vec::new();
    encode_write_request_rw2_bytes(
        &base_req,
        V2_RECORD_SYMBOL_OFFSET,
        Some(v2_common_symbols()),
        &mut base_kv2,
    )
    .unwrap();

    let mut rng = Rng::new(0x26_5eed_cafe_beef);
    for i in 0..200 {
        let (mode, base) = if i % 2 == 0 {
            ("rw2", &base_rw2[..])
        } else {
            ("kafka-v2", &base_kv2[..])
        };
        let mutated = rng.mutate(base);
        check_wire_against_go(&format!("mut_{i:03}"), mode, &mutated, &mut mismatches);
    }

    assert!(
        mismatches.is_empty(),
        "rw2 adversarial wire parity mismatches ({}):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

// =================================================================================================
// Test 4: TSDB Index FormatV1 & FormatV2 Differential + Corrupt-Byte Fuzzing
// =================================================================================================

#[test]
fn w26_oracle_tsdb_index_v1_v2_differential() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let dir = tempdir().expect("create tempdir");
    let v1_path: PathBuf = dir.path().join("index_v1");

    let go_v1: WriteIndexV1FixtureResp = invoke_oracle(
        "write-index-v1-fixture",
        &IndexPathReq {
            path: v1_path.display().to_string(),
        },
    )
    .expect("go-oracle write-index-v1-fixture");
    assert_eq!(go_v1.version, INDEX_FORMAT_V1);

    // Open V1 index in Rust and assert bit-for-bit parity.
    let reader_v1 = IndexReader::open(&v1_path).expect("Rust IndexReader must open FormatV1");
    assert_eq!(reader_v1.version(), INDEX_FORMAT_V1);
    assert_eq!(reader_v1.label_names(), go_v1.label_names);

    for name in &go_v1.label_names {
        let rust_vals = reader_v1
            .label_values(name)
            .expect("label_values on V1 index");
        assert_eq!(
            &rust_vals,
            go_v1.sorted_label_values.get(name).expect("go sorted values"),
            "V1 sorted label_values mismatch for {name}"
        );
        let go_set: BTreeSet<&str> = go_v1
            .label_values
            .get(name)
            .expect("go label values")
            .iter()
            .map(String::as_str)
            .collect();
        let rust_set: BTreeSet<&str> = rust_vals.iter().map(String::as_str).collect();
        assert_eq!(rust_set, go_set, "V1 label_values set mismatch for {name}");

        for val in &rust_vals {
            let key = format!("{name}={val}");
            let rust_p = reader_v1
                .postings(name, &[val.as_str()])
                .expect("postings on V1");
            assert_eq!(
                &rust_p,
                go_v1.postings.get(&key).expect("go postings"),
                "V1 postings mismatch for {key}"
            );
        }
    }

    let rust_multi_p = reader_v1
        .postings("job", &["alpha", "zeta"])
        .expect("multi-value postings on V1");
    assert_eq!(
        &rust_multi_p,
        go_v1.postings.get("job=alpha|zeta").expect("go multi postings"),
        "V1 multi-value postings mismatch"
    );

    let rust_all = reader_v1.all_postings().expect("all_postings on V1");
    assert_eq!(rust_all, go_v1.all_postings, "V1 all_postings mismatch");

    for go_s in &go_v1.series {
        let (rust_lbls, rust_chks) = reader_v1
            .series(go_s.ref_id)
            .unwrap_or_else(|e| panic!("V1 series({}) failed: {e}", go_s.ref_id));
        let rust_pairs: Vec<LabelJson> = rust_lbls
            .iter()
            .map(|(n, v)| LabelJson {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect();
        assert_eq!(rust_pairs, go_s.labels, "V1 series {} labels mismatch", go_s.ref_id);
        assert_eq!(
            rust_chks.len(),
            go_s.chunks.len(),
            "V1 series {} chunks len mismatch",
            go_s.ref_id
        );
        for (rc, gc) in rust_chks.iter().zip(&go_s.chunks) {
            assert_eq!(rc.ref_id, gc.ref_id);
            assert_eq!(rc.min_time, gc.min_time);
            assert_eq!(rc.max_time, gc.max_time);
        }
    }

    // Build a V2 index via Rust IndexWriter and verify Go opens it cleanly too.
    let v2_path = dir.path().join(INDEX_FILENAME);
    {
        let mut w = IndexWriter::new(&v2_path).expect("IndexWriter::new");
        let l1 = Labels::from_pairs([("__name__", "m1"), ("job", "api")]);
        let l2 = Labels::from_pairs([("__name__", "m2"), ("job", "db")]);
        for sym in ["", "__name__", "api", "db", "job", "m1", "m2"] {
            w.add_symbol(sym).expect("add_symbol");
        }
        w.add_series(
            0,
            &l1,
            &[ChunkMeta {
                ref_id: 10,
                min_time: 100,
                max_time: 200,
                chunk: None,
            }],
        )
        .expect("add_series 1");
        w.add_series(
            1,
            &l2,
            &[ChunkMeta {
                ref_id: 20,
                min_time: 300,
                max_time: 400,
                chunk: None,
            }],
        )
        .expect("add_series 2");
        w.close().expect("close v2 index");
    }

    let go_v2: VerifyIndexFileResp = invoke_oracle(
        "verify-index-file",
        &IndexPathReq {
            path: v2_path.display().to_string(),
        },
    )
    .expect("go verify-index-file on v2");
    assert!(go_v2.ok && go_v2.query_error.is_none(), "go v2 error: {go_v2:?}");
    assert_eq!(go_v2.version, INDEX_FORMAT_V2);
    assert_eq!(go_v2.series_count, 2);

    // Corrupt-byte differential fuzzing across both V1 and V2 index bytes.
    let v1_bytes = fs::read(&v1_path).expect("read v1 bytes");
    let v2_bytes = fs::read(&v2_path).expect("read v2 bytes");
    let mut rng = Rng::new(0x01de_c0de_2026);
    for iter in 0..32 {
        let base = if iter % 2 == 0 { &v1_bytes } else { &v2_bytes };
        let mut corrupt = base.clone();
        match iter % 4 {
            0 => {
                // Corrupt header / magic / version
                let pos = rng.next_usize(5);
                corrupt[pos] ^= 0xff;
            }
            1 => {
                // Corrupt TOC CRC or offset
                let pos = corrupt.len().saturating_sub(1 + rng.next_usize(52));
                corrupt[pos] ^= 0x55;
            }
            2 => {
                // Truncate index
                let len = corrupt.len().saturating_sub(1 + rng.next_usize(32));
                corrupt.truncate(len);
            }
            _ => {
                // Corrupt interior byte (symbols / series / postings)
                if corrupt.len() > 64 {
                    let pos = 5 + rng.next_usize(corrupt.len() - 57);
                    corrupt[pos] ^= 0x80;
                }
            }
        }
        let cp = dir.path().join(format!("corrupt_{iter}"));
        fs::write(&cp, &corrupt).expect("write corrupt index");

        let go_chk: VerifyIndexFileResp = invoke_oracle(
            "verify-index-file",
            &IndexPathReq {
                path: cp.display().to_string(),
            },
        )
        .expect("go verify-index-file");

        let rust_open = IndexReader::open(&cp);
        if !go_chk.ok {
            assert!(
                rust_open.is_err(),
                "iter {iter}: Go rejected index ({}) but Rust IndexReader::open succeeded",
                go_chk.error
            );
        } else if go_chk.query_error.is_none() {
            let r = rust_open.unwrap_or_else(|e| {
                panic!("iter {iter}: Go opened & queried cleanly but Rust failed open: {e}")
            });
            assert_eq!(r.version(), go_chk.version);
            assert_eq!(r.label_names(), go_chk.label_names);
            let all = r.all_postings().expect("rust all_postings");
            assert_eq!(all.len(), go_chk.series_count);
            for id in all {
                r.series(id).expect("rust series");
            }
        } else if let Ok(r) = rust_open {
            // Go opened header/TOC/symbols/table but hit a query error on all_postings/series;
            // verify Rust also surfaces an error when querying all_postings + all series.
            let q_res: Result<(), String> = (|| {
                let all = r.all_postings().map_err(|e| e.to_string())?;
                for id in all {
                    r.series(id).map_err(|e| e.to_string())?;
                }
                Ok(())
            })();
            assert!(
                q_res.is_err(),
                "iter {iter}: Go hit query error ({:?}) but Rust opened & queried without error",
                go_chk.query_error
            );
        }
    }
}

// =================================================================================================
// Test 5: Ingest RecordSerializer v0/v1/v2 Split + Oversized Item + Raw Non-UTF-8 Bytes
// =================================================================================================

#[test]
fn w26_oracle_ingest_record_serializer_v0_v1_v2_split_and_raw_bytes() {
    ensure_go_oracle_built().expect("go-oracle must build");

    // 1. Valid UTF-8 multi-series + metadata + exemplar request across v0, v1, v2
    let mut series = Vec::new();
    let mut metadata = Vec::new();
    for i in 0..5 {
        let name = format!("metric_{i}_total");
        series.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"__name__"),
                    value: Bytes::from(name.clone()),
                },
                cortexpb::LabelPair::from_static("job", "app"),
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"shard"),
                    value: Bytes::from(format!("s{i}")),
                },
            ],
            samples: vec![cortexpb::Sample {
                value: (i as f64) * 3.25 + 1.0,
                timestamp_ms: 1_700_000_000_000 + i * 1000,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![cortexpb::LabelPair {
                    name: Bytes::from_static(b"trace_id"),
                    value: Bytes::from(format!("trace-{i}")),
                }],
                value: 0.25,
                timestamp_ms: 1_700_000_000_050 + i * 1000,
            }],
            ..Default::default()
        });
        metadata.push(cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: name,
            help: format!("help text {i}"),
            unit: "bytes".to_string(),
        });
    }

    let utf8_req = cortexpb::WriteRequest {
        timeseries: series,
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        metadata,
        skip_label_validation: true,
        skip_label_count_validation: true,
        ..Default::default()
    };
    let utf8_wire = utf8_req.encode_to_vec();

    for version in [0i32, 1, 2] {
        for max_bytes in [100_000usize, 140, 1] {
            let go_resp: SerializeIngestRecordsResp = invoke_oracle(
                "serialize-ingest-records",
                &SerializeIngestRecordsReq {
                    version,
                    user_id: "tenant-a",
                    write_req_b64: B64.encode(&utf8_wire),
                    max_record_bytes: max_bytes,
                    topic: "ingest",
                    partition: 3,
                },
            )
            .expect("go-oracle serialize-ingest-records");
            assert!(
                go_resp.ok,
                "go serialize failed (v{version}, max={max_bytes}): {}",
                go_resp.error
            );

            let ser = RecordSerializer::from_i64(version as i64).expect("valid version");
            let (rust_recs, rust_input_size) = ser
                .to_records("ingest", 3, "tenant-a", &utf8_req, max_bytes)
                .expect("rust to_records");

            assert_eq!(
                rust_input_size, go_resp.input_size,
                "v{version} max={max_bytes} input_size"
            );
            assert_eq!(
                rust_recs.len(),
                go_resp.records_b64.len(),
                "v{version} max={max_bytes} split record count mismatch"
            );

            // Check Kafka Version header on Rust records
            for r in &rust_recs {
                let hdrs = r.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref()));
                assert_eq!(parse_record_version(hdrs), version);
                if version == 0 {
                    assert!(r.headers.is_empty());
                } else {
                    assert_eq!(r.headers[0].0, RECORD_VERSION_HEADER_KEY);
                    assert_eq!(
                        r.headers[0].1.as_ref(),
                        &record_version_header(RecordVersion::from_i64(version as i64).unwrap())
                    );
                }
            }

            // Cross-decode every Go-serialized record with Rust and verify all series & metadata sum up
            let mut decoded_series = 0usize;
            let mut decoded_metadata = 0usize;
            let mut scratch = WriteRequestScratch::new();
            for b64 in &go_resp.records_b64 {
                let rec_bytes = B64.decode(b64).expect("valid base64");
                let bwr = deserialize_record_content(&rec_bytes, version, &mut scratch)
                    .expect("rust deserialize_record_content on go record");
                decoded_series += bwr.timeseries_len();
                decoded_metadata += bwr.metadata_len();
                assert_eq!(bwr.source, utf8_req.source);
                assert_eq!(bwr.skip_label_validation, utf8_req.skip_label_validation);
                assert_eq!(
                    bwr.skip_label_count_validation,
                    utf8_req.skip_label_count_validation
                );
            }
            assert_eq!(decoded_series, utf8_req.timeseries.len());
            if version < 2 {
                assert_eq!(decoded_metadata, utf8_req.metadata.len());
            } else {
                assert_eq!(decoded_metadata, utf8_req.timeseries.len());
            }
        }
    }

    // 2. Raw non-UTF-8 bytes in series labels AND exemplar labels round-tripping through Go & Rust
    let raw_req = cortexpb::WriteRequest {
        timeseries: vec![
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "raw_metric_0"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"bad_\xff_key"),
                        value: Bytes::from_static(b"val_\xfe_\x80"),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 10.0,
                    timestamp_ms: 1_700_000_001_000,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: Bytes::from_static(b"ex_\xfd"),
                        value: Bytes::from_static(b"tr_\xfa\xfb"),
                    }],
                    value: 0.5,
                    timestamp_ms: 1_700_000_001_050,
                }],
                ..Default::default()
            },
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "raw_metric_1"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"bin_\x81"),
                        value: Bytes::from_static(b"data_\x90\x91"),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 20.0,
                    timestamp_ms: 1_700_000_002_000,
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let raw_wire = raw_req.encode_to_vec();

    for version in [0i32, 1, 2] {
        for max_bytes in [100_000usize, 70] {
            let go_resp: SerializeIngestRecordsResp = invoke_oracle(
                "serialize-ingest-records",
                &SerializeIngestRecordsReq {
                    version,
                    user_id: "tenant-raw",
                    write_req_b64: B64.encode(&raw_wire),
                    max_record_bytes: max_bytes,
                    topic: "ingest",
                    partition: 0,
                },
            )
            .expect("go serialize raw bytes");
            assert!(go_resp.ok, "go raw bytes serialize failed: {}", go_resp.error);

            let ser = RecordSerializer::from_i64(version as i64).unwrap();
            let (rust_recs, rust_input_size) = ser
                .to_records("ingest", 0, "tenant-raw", &raw_req, max_bytes)
                .expect("rust raw bytes to_records");
            assert_eq!(rust_input_size, go_resp.input_size);
            assert_eq!(
                rust_recs.len(),
                go_resp.records_b64.len(),
                "raw bytes v{version} max={max_bytes} split count"
            );

            if version == 2 {
                let raw_split = encode_write_request_rw2_split_raw_bytes(
                    &raw_req,
                    max_bytes,
                    V2_RECORD_SYMBOL_OFFSET,
                    Some(v2_common_symbols()),
                )
                .expect("encode_write_request_rw2_split_raw_bytes");
                assert_eq!(raw_split.len(), go_resp.records_b64.len());
            }

            // Decode Go's and Rust's emitted records in Rust and assert exact raw non-UTF-8 preservation
            let mut go_seen_labels: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
            let mut rust_seen_labels: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
            let mut go_seen_exemplar_labels: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
            let mut rust_seen_exemplar_labels: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
            let mut scratch = WriteRequestScratch::new();
            let mut resolved = Rw2ResolvedScratch::new();
            let mut label_arena = Vec::new();
            let mut samples_arena = Vec::new();

            for (go_b64, rust_rec) in go_resp.records_b64.iter().zip(&rust_recs) {
                let go_bytes = B64.decode(go_b64).unwrap();
                for (is_go, payload) in [
                    (true, go_bytes.as_slice()),
                    (false, rust_rec.value.as_ref()),
                ] {
                    let bwr = deserialize_record_content(payload, version, &mut scratch)
                        .expect("deserialize_record_content raw bytes");
                    for ts in bwr.timeseries() {
                        let pairs: Vec<(Vec<u8>, Vec<u8>)> = ts
                            .labels()
                            .map(|l| (l.name.to_vec(), l.value.to_vec()))
                            .collect();
                        if is_go {
                            go_seen_labels.push(pairs);
                        } else {
                            rust_seen_labels.push(pairs);
                        }
                        for ex in ts.exemplars() {
                            let ex_pairs: Vec<(Vec<u8>, Vec<u8>)> = ex
                                .labels()
                                .map(|l| (l.name.to_vec(), l.value.to_vec()))
                                .collect();
                            if is_go {
                                go_seen_exemplar_labels.push(ex_pairs);
                            } else {
                                rust_seen_exemplar_labels.push(ex_pairs);
                            }
                        }
                    }

                    // Also verify stream decoders flag bad_utf8 without error
                    label_arena.clear();
                    samples_arena.clear();
                    if version < 2 {
                        let mut found = 0usize;
                        decode_classic_stream(
                            payload,
                            DecodeOptions::classic(),
                            &mut scratch,
                            &mut label_arena,
                            &mut samples_arena,
                            |s| {
                                found += 1;
                                assert!(s.bad_utf8);
                            },
                        )
                        .expect("decode classic raw bytes");
                        assert!(found >= 1);
                    } else {
                        let mut meta_out = Vec::new();
                        let mut found = 0usize;
                        decode_rw2_stream(
                            payload,
                            DecodeOptions::kafka_v2(),
                            &mut scratch,
                            &mut resolved,
                            &mut label_arena,
                            &mut samples_arena,
                            &mut meta_out,
                            |s| {
                                found += 1;
                                assert!(s.bad_utf8);
                            },
                        )
                        .expect("decode rw2 raw bytes");
                        assert!(found >= 1);
                    }
                }
            }

            let expected_labels = vec![
                vec![
                    (b"__name__".to_vec(), b"raw_metric_0".to_vec()),
                    (b"bad_\xff_key".to_vec(), b"val_\xfe_\x80".to_vec()),
                ],
                vec![
                    (b"__name__".to_vec(), b"raw_metric_1".to_vec()),
                    (b"bin_\x81".to_vec(), b"data_\x90\x91".to_vec()),
                ],
            ];
            let expected_exemplars = vec![vec![(b"ex_\xfd".to_vec(), b"tr_\xfa\xfb".to_vec())]];
            assert_eq!(go_seen_labels, expected_labels);
            assert_eq!(rust_seen_labels, expected_labels);
            assert_eq!(go_seen_exemplar_labels, expected_exemplars);
            assert_eq!(rust_seen_exemplar_labels, expected_exemplars);
        }
    }

    // 3. Oversized single series (> MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT) & ErrWriteRequestDataItemTooLarge
    let huge_val = "x".repeat(MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT + 1024);
    let huge_req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "huge_series"),
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"payload"),
                    value: Bytes::from(huge_val),
                },
            ],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: 1_700_000_000_000,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let huge_wire = huge_req.encode_to_vec();

    let go_huge: SerializeIngestRecordsResp = invoke_oracle(
        "serialize-ingest-records",
        &SerializeIngestRecordsReq {
            version: 2,
            user_id: "tenant-huge",
            write_req_b64: B64.encode(&huge_wire),
            max_record_bytes: MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
            topic: "ingest",
            partition: 0,
        },
    )
    .expect("go serialize huge");
    assert!(go_huge.ok);
    assert_eq!(go_huge.records_b64.len(), 1);
    let go_huge_rec = B64.decode(&go_huge.records_b64[0]).unwrap();
    assert!(
        go_huge_rec.len() > MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
        "expected oversized record > MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT"
    );

    let (rust_huge_recs, rust_huge_input_size) = RecordSerializer::new(RecordVersion::V2)
        .to_records(
            "ingest",
            0,
            "tenant-huge",
            &huge_req,
            MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
        )
        .expect("rust serialize huge");
    assert_eq!(rust_huge_input_size, go_huge.input_size);
    assert_eq!(rust_huge_recs.len(), 1);
    assert!(rust_huge_recs[0].value.len() > MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT);

    // When broker rejects oversized record with MessageTooLarge, Writer maps to ErrWriteRequestDataItemTooLarge
    let err = produce_results_err(&[ProduceResult {
        record: WriterRecord {
            topic: "ingest".to_string(),
            partition: 0,
            key: Bytes::from_static(b"tenant-huge"),
            value: rust_huge_recs[0].value.clone(),
            headers: rust_huge_recs[0].headers.clone(),
            offset: 0,
            timestamp_ms: Some(0),
        },
        err: Some(ProducerError::MessageTooLarge),
    }])
    .unwrap_err();
    assert!(matches!(err, WriterError::WriteRequestDataItemTooLarge));
    assert_eq!(err.to_string(), err_write_request_data_item_too_large_msg());
    assert!(err.to_string().contains(&MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT.to_string()));
}
