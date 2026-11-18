//! Kafka ingest_storage record serialization & zero-copy deserialization (versions 0, 1, 2).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/version.go
//! - github.com/grafana/mimir/pkg/storage/ingest/writer.go

use bytes::Bytes;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::symbols::{
    v2_common_symbols, CommonSymbols, FastSymbolsTable, LATEST_RECORD_VERSION,
    MIN_PREALLOCATED_SYMBOLS_PER_REQUEST, RECORD_VERSION_HEADER_KEY, V2_RECORD_SYMBOL_OFFSET,
};
use mimir_proto::zerocopy::{
    decode_write_request, from_write_request_to_rw2, BorrowedWriteRequest, DecodeError,
    DecodeOptions, EncodeRw2Error, WriteRequestScratch,
};
use thiserror::Error;

pub const PRODUCER_BATCH_MAX_BYTES: usize = 16_000_000;
pub const MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT: usize = PRODUCER_BATCH_MAX_BYTES - 16_384;
pub const MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum RecordCodecError {
    #[error("unknown record version {0}")]
    UnknownVersion(i64),
    #[error("received a record with an unsupported version: {0}, max supported version: {LATEST_RECORD_VERSION}")]
    UnsupportedVersion(i32),
    #[error("rw2 encode error: {0}")]
    EncodeRw2(#[from] EncodeRw2Error),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("protobuf encode error: {0}")]
    ProstEncode(#[from] mimir_proto::prost::EncodeError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
}

impl RecordVersion {
    pub fn from_i64(v: i64) -> Result<Self, RecordCodecError> {
        match v {
            0 => Ok(RecordVersion::V0),
            1 => Ok(RecordVersion::V1),
            2 => Ok(RecordVersion::V2),
            other => Err(RecordCodecError::UnknownVersion(other)),
        }
    }

    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

pub fn validate_record_version(version: i64) -> Result<(), RecordCodecError> {
    RecordVersion::from_i64(version).map(|_| ())
}

pub fn record_version_header(version: RecordVersion) -> [u8; 4] {
    record_version_header_raw(version.as_u32())
}

pub fn record_version_header_raw(version: u32) -> [u8; 4] {
    version.to_be_bytes()
}

/// Extracts the record version from Kafka record headers.
/// Returns `0` if the `Version` header is absent (matching `ParseRecordVersion` in `version.go`).
pub fn parse_record_version<'a>(headers: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> i32 {
    for (k, v) in headers {
        if k == RECORD_VERSION_HEADER_KEY && v.len() >= 4 {
            return u32::from_be_bytes([v[0], v[1], v[2], v[3]]) as i32;
        }
    }
    0
}

#[derive(Debug, Clone)]
pub struct SerializedKafkaRecord {
    pub topic: String,
    pub partition: i32,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
}

/// Converts a `cortexpb::WriteRequest` into one or more serialized Kafka records for version 0, 1, or 2.
#[derive(Debug, Clone, Copy)]
pub struct RecordSerializer {
    pub version: RecordVersion,
}

impl RecordSerializer {
    pub fn new(version: RecordVersion) -> Self {
        Self { version }
    }

    pub fn from_i64(version: i64) -> Result<Self, RecordCodecError> {
        Ok(Self::new(RecordVersion::from_i64(version)?))
    }

    /// Serializes `req` into Kafka records, splitting if needed to stay under `max_size` bytes.
    /// Returns `(records, original_input_size_bytes)`.
    pub fn to_records(
        self,
        topic: &str,
        partition_id: int_or_i32!(),
        tenant_id: &str,
        req: &cortexpb::WriteRequest,
        max_size: usize,
    ) -> Result<(Vec<SerializedKafkaRecord>, usize), RecordCodecError> {
        let input_size = req.encoded_len();
        if req.timeseries.is_empty() && req.metadata.is_empty() && req.timeseries_rw2.is_empty() {
            return Ok((Vec::new(), input_size));
        }

        let key = Bytes::copy_from_slice(tenant_id.as_bytes());

        match self.version {
            RecordVersion::V0 => {
                let payloads = serialize_rw1_split(req, input_size, max_size)?;
                let records = payloads
                    .into_iter()
                    .map(|value| SerializedKafkaRecord {
                        topic: topic.to_owned(),
                        partition: partition_id,
                        key: key.clone(),
                        value,
                        headers: Vec::new(),
                    })
                    .collect();
                Ok((records, input_size))
            }
            RecordVersion::V1 => {
                let payloads = serialize_rw1_split(req, input_size, max_size)?;
                let hdr_val = Bytes::copy_from_slice(&record_version_header(RecordVersion::V1));
                let records = payloads
                    .into_iter()
                    .map(|value| SerializedKafkaRecord {
                        topic: topic.to_owned(),
                        partition: partition_id,
                        key: key.clone(),
                        value,
                        headers: vec![(RECORD_VERSION_HEADER_KEY.to_string(), hdr_val.clone())],
                    })
                    .collect();
                Ok((records, input_size))
            }
            RecordVersion::V2 => {
                let payloads = serialize_rw2_split(req, max_size)?;
                let hdr_val = Bytes::copy_from_slice(&record_version_header(RecordVersion::V2));
                let records = payloads
                    .into_iter()
                    .map(|value| SerializedKafkaRecord {
                        topic: topic.to_owned(),
                        partition: partition_id,
                        key: key.clone(),
                        value,
                        headers: vec![(RECORD_VERSION_HEADER_KEY.to_string(), hdr_val.clone())],
                    })
                    .collect();
                Ok((records, input_size))
            }
        }
    }
}

macro_rules! int_or_i32 {
    () => {
        i32
    };
}
use int_or_i32;

fn new_partial_write_request(req: &cortexpb::WriteRequest) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        source: req.source,
        skip_label_validation: req.skip_label_validation,
        skip_label_count_validation: req.skip_label_count_validation,
        ..Default::default()
    }
}

#[inline]
fn embedded_message_field_size(message_size: usize) -> usize {
    1 + message_size + mimir_proto::prost::length_delimiter_len(message_size)
}

/// Splits a RW1 `WriteRequest` into partial requests each <= `max_size` bytes (best effort).
/// Ported 1:1 from `mimirpb.SplitWriteRequestByMaxMarshalSize`.
pub fn split_write_request_by_max_marshal_size(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Vec<cortexpb::WriteRequest> {
    if max_size == 0 || req_size <= max_size {
        return vec![req.clone()];
    }

    let ts_reqs = split_timeseries_by_max_marshal_size(req, req_size, max_size);
    let md_reqs = split_metadata_by_max_marshal_size(req, req_size, max_size);

    if md_reqs.is_empty() {
        return ts_reqs;
    }
    if ts_reqs.is_empty() {
        return md_reqs;
    }
    let mut merged = Vec::with_capacity(ts_reqs.len() + md_reqs.len());
    merged.extend(ts_reqs);
    merged.extend(md_reqs);
    merged
}

fn split_timeseries_by_max_marshal_size(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Vec<cortexpb::WriteRequest> {
    if req.timeseries.is_empty() {
        return Vec::new();
    }
    let md_size: usize = req
        .metadata
        .iter()
        .map(|m| embedded_message_field_size(m.encoded_len()))
        .sum();
    let req_size_without_md = req_size.saturating_sub(md_size);
    if req_size_without_md <= max_size {
        let mut partial = new_partial_write_request(req);
        partial.timeseries = req.timeseries.clone();
        return vec![partial];
    }

    let mut out = Vec::new();
    let mut next_req = new_partial_write_request(req);
    let mut next_req_size = next_req.encoded_len();

    for ts in &req.timeseries {
        let series_field_size = embedded_message_field_size(ts.encoded_len());
        if next_req_size + series_field_size > max_size && !next_req.timeseries.is_empty() {
            out.push(next_req);
            next_req = new_partial_write_request(req);
            next_req_size = next_req.encoded_len();
        }
        next_req_size += series_field_size;
        next_req.timeseries.push(ts.clone());
    }
    if !next_req.timeseries.is_empty() {
        out.push(next_req);
    }
    out
}

fn split_metadata_by_max_marshal_size(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Vec<cortexpb::WriteRequest> {
    if req.metadata.is_empty() {
        return Vec::new();
    }
    let ts_size: usize = req
        .timeseries
        .iter()
        .map(|ts| embedded_message_field_size(ts.encoded_len()))
        .sum();
    let req_size_without_ts = req_size.saturating_sub(ts_size);
    if req_size_without_ts <= max_size {
        let mut partial = new_partial_write_request(req);
        partial.metadata = req.metadata.clone();
        return vec![partial];
    }

    let mut out = Vec::new();
    let mut next_req = new_partial_write_request(req);
    let mut next_req_size = next_req.encoded_len();

    for m in &req.metadata {
        let md_field_size = embedded_message_field_size(m.encoded_len());
        if next_req_size + md_field_size > max_size && !next_req.metadata.is_empty() {
            out.push(next_req);
            next_req = new_partial_write_request(req);
            next_req_size = next_req.encoded_len();
        }
        next_req_size += md_field_size;
        next_req.metadata.push(m.clone());
    }
    if !next_req.metadata.is_empty() {
        out.push(next_req);
    }
    out
}

fn resolve_ref<'a>(
    r: u32,
    symbols: &'a [String],
    common_symbols: &'a [String],
    offset: u32,
) -> &'a str {
    if r < offset {
        common_symbols.get(r as usize).map(String::as_str).unwrap_or("")
    } else {
        symbols
            .get((r - offset) as usize)
            .map(String::as_str)
            .unwrap_or("")
    }
}

fn resolved_symbol_size(r: u32, symbols: &[String], offset: u32) -> usize {
    if r == 0 {
        return 1 + mimir_proto::prost::length_delimiter_len(0);
    }
    if r < offset {
        return 0;
    }
    let l = symbols.get((r - offset) as usize).map(String::len).unwrap_or(0);
    1 + l + mimir_proto::prost::length_delimiter_len(l)
}

fn max_rw2_series_size_after_resymbolization(
    ts: &cortexpb::TimeSeriesRw2,
    symbols: &[String],
    offset: u32,
) -> (usize, usize) {
    let highest_possible = symbols.len() as u64 + offset as u64;
    let sym_upper = 1 + mimir_proto::prost::length_delimiter_len(highest_possible as usize);
    let mut series_size = ts.encoded_len();
    if ts.metadata.is_none() {
        // Go `TimeSeriesRW2.Metadata` is non-nullable (`(gogoproto.nullable) = false`), so Go's
        // `ts.Size()` always includes 2 bytes (`0x2a, 0x00`) for an empty `MetadataRW2`.
        series_size += 2;
    }
    let mut syms_size = 0usize;

    for &r in &ts.labels_refs {
        series_size += sym_upper.saturating_sub(mimir_proto::prost::length_delimiter_len(r as usize));
        syms_size += resolved_symbol_size(r, symbols, offset);
    }
    for ex in &ts.exemplars {
        for &r in &ex.labels_refs {
            series_size +=
                sym_upper.saturating_sub(mimir_proto::prost::length_delimiter_len(r as usize));
            syms_size += resolved_symbol_size(r, symbols, offset);
        }
    }
    let (help_ref, unit_ref) = ts
        .metadata
        .as_ref()
        .map_or((0, 0), |md| (md.help_ref, md.unit_ref));
    series_size +=
        sym_upper.saturating_sub(mimir_proto::prost::length_delimiter_len(help_ref as usize));
    syms_size += resolved_symbol_size(help_ref, symbols, offset);
    series_size +=
        sym_upper.saturating_sub(mimir_proto::prost::length_delimiter_len(unit_ref as usize));
    syms_size += resolved_symbol_size(unit_ref, symbols, offset);
    (series_size, syms_size)
}

fn resymbolize_timeseries_rw2(
    ts: &cortexpb::TimeSeriesRw2,
    orig_symbols: &[String],
    symbols: &mut FastSymbolsTable<'_>,
    common_symbols: &CommonSymbols,
) -> cortexpb::TimeSeriesRw2 {
    let offset = symbols.offset();
    let common_slice = common_symbols.get_slice();

    let labels_refs = ts
        .labels_refs
        .iter()
        .map(|&r| symbols.symbolize(resolve_ref(r, orig_symbols, common_slice, offset)))
        .collect();

    let exemplars = ts
        .exemplars
        .iter()
        .map(|ex| {
            let ex_refs = ex
                .labels_refs
                .iter()
                .map(|&r| symbols.symbolize(resolve_ref(r, orig_symbols, common_slice, offset)))
                .collect();
            cortexpb::ExemplarRw2 {
                labels_refs: ex_refs,
                value: ex.value,
                timestamp: ex.timestamp,
            }
        })
        .collect();

    let metadata = ts.metadata.map(|md| cortexpb::MetadataRw2 {
        r#type: md.r#type,
        help_ref: symbols.symbolize(resolve_ref(md.help_ref, orig_symbols, common_slice, offset)),
        unit_ref: symbols.symbolize(resolve_ref(md.unit_ref, orig_symbols, common_slice, offset)),
    });

    cortexpb::TimeSeriesRw2 {
        labels_refs,
        samples: ts.samples.clone(),
        histograms: ts.histograms.clone(),
        exemplars,
        metadata,
        created_timestamp: ts.created_timestamp,
    }
}

/// Splits an RW2 `WriteRequest` into partial RW2 requests each <= `max_size` bytes (best effort),
/// re-symbolizing each chunk against `common_symbols` and `offset`.
/// Ported 1:1 from `mimirpb.SplitWriteRequestByMaxMarshalSizeRW2`.
pub fn split_write_request_by_max_marshal_size_rw2(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
    offset: u32,
    common_symbols: &CommonSymbols,
) -> Vec<cortexpb::WriteRequest> {
    if max_size == 0 || req_size <= max_size {
        return vec![req.clone()];
    }
    if req.timeseries_rw2.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut next_symbols = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
    next_symbols.configure_common_symbols(offset, Some(common_symbols));

    let mut next_req = new_partial_write_request(req);
    let mut next_req_size = next_req.encoded_len();
    let mut next_req_symbols_size = 0usize;

    for ts in &req.timeseries_rw2 {
        let (series_size, syms_size) =
            max_rw2_series_size_after_resymbolization(ts, &req.symbols_rw2, offset);
        let series_field_size = embedded_message_field_size(series_size);

        if next_req_size + series_field_size + syms_size > max_size
            && !next_req.timeseries_rw2.is_empty()
        {
            next_req.symbols_rw2 = next_symbols.symbols();
            out.push(next_req);

            next_req = new_partial_write_request(req);
            next_req_size = next_req.encoded_len();
            next_req_symbols_size = 0;
            next_symbols.reset();
            next_symbols.configure_common_symbols(offset, Some(common_symbols));
        }

        let new_ts = resymbolize_timeseries_rw2(ts, &req.symbols_rw2, &mut next_symbols, common_symbols);
        next_req_size += embedded_message_field_size(new_ts.encoded_len());
        let new_syms_size = next_symbols.symbols_size_proto();
        next_req_size += new_syms_size.saturating_sub(next_req_symbols_size);
        next_req_symbols_size = new_syms_size;
        next_req.timeseries_rw2.push(new_ts);
    }

    if !next_req.timeseries_rw2.is_empty() {
        next_req.symbols_rw2 = next_symbols.symbols();
        out.push(next_req);
    }

    out
}

fn serialize_rw1_split(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Result<Vec<Bytes>, RecordCodecError> {
    let parts = split_write_request_by_max_marshal_size(req, req_size, max_size);
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
        let mut buf = Vec::with_capacity(part.encoded_len());
        part.encode(&mut buf)?;
        out.push(Bytes::from(buf));
    }
    Ok(out)
}

fn serialize_rw2_split(
    req: &cortexpb::WriteRequest,
    max_size: usize,
) -> Result<Vec<Bytes>, RecordCodecError> {
    let rw2 = from_write_request_to_rw2(req, V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()))?;
    let rw2_size = rw2.encoded_len();
    let parts = split_write_request_by_max_marshal_size_rw2(
        &rw2,
        rw2_size,
        max_size,
        V2_RECORD_SYMBOL_OFFSET,
        v2_common_symbols(),
    );
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
        let mut buf = Vec::with_capacity(part.encoded_len());
        part.encode(&mut buf)?;
        out.push(Bytes::from(buf));
    }
    Ok(out)
}

/// Zero-copy deserializes a Kafka record payload of version 0, 1, or 2 directly into `scratch`.
/// Ported from `DeserializeRecordContent` in `pkg/storage/ingest/version.go`.
pub fn deserialize_record_content<'a, 's>(
    content: &'a [u8],
    version: i32,
    scratch: &'s mut WriteRequestScratch,
) -> Result<BorrowedWriteRequest<'a, 's>, RecordCodecError> {
    match version {
        0 | 1 => Ok(decode_write_request(
            content,
            DecodeOptions::classic(),
            scratch,
        )?),
        2 => Ok(decode_write_request(
            content,
            DecodeOptions::kafka_v2(),
            scratch,
        )?),
        other => Err(RecordCodecError::UnsupportedVersion(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_write_request() -> cortexpb::WriteRequest {
        cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("job", "prometheus"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_serialize_and_deserialize_v0_v1_v2() {
        let req = sample_write_request();
        let mut scratch = WriteRequestScratch::new();

        for ver in [RecordVersion::V0, RecordVersion::V1, RecordVersion::V2] {
            let (recs, orig_size) = RecordSerializer::new(ver)
                .to_records("ingest", 3, "tenant-a", &req, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT)
                .unwrap();
            assert_eq!(recs.len(), 1);
            assert!(orig_size > 0);
            let rec = &recs[0];
            assert_eq!(rec.topic, "ingest");
            assert_eq!(rec.partition, 3);
            assert_eq!(rec.key.as_ref(), b"tenant-a");

            let parsed_ver = parse_record_version(
                rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
            );
            assert_eq!(parsed_ver, ver as i32);

            let decoded =
                deserialize_record_content(&rec.value, parsed_ver, &mut scratch).unwrap();
            assert_eq!(decoded.to_owned_write_request(), req);
        }
    }

    #[test]
    fn test_record_splitting() {
        let mut req = cortexpb::WriteRequest::default();
        for i in 0..20 {
            req.timeseries.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "metric_a"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"idx"),
                        value: Bytes::from(format!("series_{i:04}")),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: i as f64,
                    timestamp_ms: 1000 + i,
                }],
                ..Default::default()
            });
        }
        for ver in [RecordVersion::V1, RecordVersion::V2] {
            let (recs, _) = RecordSerializer::new(ver)
                .to_records("ingest", 0, "t1", &req, 200)
                .unwrap();
            assert!(recs.len() > 1);
            let mut scratch = WriteRequestScratch::new();
            let mut total_series = 0;
            for rec in &recs {
                let v = parse_record_version(
                    rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
                );
                let dec = deserialize_record_content(&rec.value, v, &mut scratch).unwrap();
                total_series += dec.timeseries_len();
            }
            assert_eq!(total_series, 20);
        }
    }

    #[test]
    fn test_max_rw2_series_size_accounts_for_empty_metadata_and_symbols_size_proto() {
        let ts = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 65],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: 1000,
            }],
            metadata: None,
            ..Default::default()
        };
        let symbols = vec!["".to_string(), "foo".to_string()];
        let (series_size, syms_size) =
            max_rw2_series_size_after_resymbolization(&ts, &symbols, 64);
        // Must include +2 for missing MetadataRW2 (0x2a 0x00) and 2*(sym_upper-1) + 4 for syms.
        let sym_upper = 1 + mimir_proto::prost::length_delimiter_len(symbols.len() + 64);
        assert!(series_size >= ts.encoded_len() + 2 + 2 * (sym_upper - 1));
        // help_ref=0 (2 bytes) + unit_ref=0 (2 bytes) + "foo" (5 bytes) = 9 bytes.
        assert_eq!(syms_size, 9);

        let mut table = FastSymbolsTable::new(10);
        table.configure_common_symbols(64, Some(v2_common_symbols()));
        assert_eq!(table.symbols_size_proto(), 0);
        table.symbolize("foo");
        // Tag 4 (1) + len varint (1) + "foo" (3) = 5 bytes, excluding "".
        assert_eq!(table.symbols_size_proto(), 5);
    }
}
