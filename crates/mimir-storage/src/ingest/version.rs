//! Kafka ingest_storage record serialization & zero-copy deserialization (versions 0, 1, 2).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/version.go
//! - github.com/grafana/mimir/pkg/storage/ingest/writer.go

use bytes::Bytes;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::symbols::{
    v2_common_symbols, LATEST_RECORD_VERSION, RECORD_VERSION_HEADER_KEY, V2_RECORD_SYMBOL_OFFSET,
};
use mimir_proto::zerocopy::{
    decode_write_request, encode_write_request_rw2_bytes, BorrowedWriteRequest, DecodeError,
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
    version.as_u32().to_be_bytes()
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

fn serialize_rw1_split(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Result<Vec<Bytes>, RecordCodecError> {
    if max_size == 0 || req_size <= max_size {
        let mut buf = Vec::with_capacity(req_size);
        req.encode(&mut buf)?;
        return Ok(vec![Bytes::from(buf)]);
    }

    let mut out = Vec::new();

    if !req.timeseries.is_empty() {
        let mut cur = cortexpb::WriteRequest {
            source: req.source,
            skip_label_validation: req.skip_label_validation,
            skip_label_count_validation: req.skip_label_count_validation,
            ..Default::default()
        };
        for ts in &req.timeseries {
            let item_len = ts.encoded_len() + 4;
            if !cur.timeseries.is_empty() && cur.encoded_len() + item_len > max_size {
                let mut buf = Vec::with_capacity(cur.encoded_len());
                cur.encode(&mut buf)?;
                out.push(Bytes::from(buf));
                cur.timeseries.clear();
            }
            cur.timeseries.push(ts.clone());
        }
        if !cur.timeseries.is_empty() {
            let mut buf = Vec::with_capacity(cur.encoded_len());
            cur.encode(&mut buf)?;
            out.push(Bytes::from(buf));
        }
    }

    if !req.metadata.is_empty() {
        let mut cur = cortexpb::WriteRequest {
            source: req.source,
            skip_label_validation: req.skip_label_validation,
            skip_label_count_validation: req.skip_label_count_validation,
            ..Default::default()
        };
        for m in &req.metadata {
            let item_len = m.encoded_len() + 4;
            if !cur.metadata.is_empty() && cur.encoded_len() + item_len > max_size {
                let mut buf = Vec::with_capacity(cur.encoded_len());
                cur.encode(&mut buf)?;
                out.push(Bytes::from(buf));
                cur.metadata.clear();
            }
            cur.metadata.push(m.clone());
        }
        if !cur.metadata.is_empty() {
            let mut buf = Vec::with_capacity(cur.encoded_len());
            cur.encode(&mut buf)?;
            out.push(Bytes::from(buf));
        }
    }

    Ok(out)
}

fn serialize_rw2_split(
    req: &cortexpb::WriteRequest,
    max_size: usize,
) -> Result<Vec<Bytes>, RecordCodecError> {
    let mut buf = Vec::new();
    encode_write_request_rw2_bytes(req, V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()), &mut buf)?;
    if max_size == 0 || buf.len() <= max_size || req.timeseries.len() <= 1 && req.metadata.len() <= 1
    {
        return Ok(vec![Bytes::from(buf)]);
    }

    // Split by timeseries/metadata and encode each chunk to RW2.
    let mut out = Vec::new();
    let mut cur = cortexpb::WriteRequest {
        source: req.source,
        skip_label_validation: req.skip_label_validation,
        skip_label_count_validation: req.skip_label_count_validation,
        ..Default::default()
    };

    for ts in &req.timeseries {
        cur.timeseries.push(ts.clone());
        buf.clear();
        encode_write_request_rw2_bytes(
            &cur,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
            &mut buf,
        )?;
        if buf.len() > max_size && cur.timeseries.len() > 1 {
            let popped = cur.timeseries.pop().unwrap();
            buf.clear();
            encode_write_request_rw2_bytes(
                &cur,
                V2_RECORD_SYMBOL_OFFSET,
                Some(v2_common_symbols()),
                &mut buf,
            )?;
            out.push(Bytes::copy_from_slice(&buf));
            cur.timeseries.clear();
            cur.timeseries.push(popped);
        }
    }

    for m in &req.metadata {
        cur.metadata.push(m.clone());
        buf.clear();
        encode_write_request_rw2_bytes(
            &cur,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
            &mut buf,
        )?;
        if buf.len() > max_size && (cur.timeseries.len() + cur.metadata.len()) > 1 {
            let popped = cur.metadata.pop().unwrap();
            buf.clear();
            encode_write_request_rw2_bytes(
                &cur,
                V2_RECORD_SYMBOL_OFFSET,
                Some(v2_common_symbols()),
                &mut buf,
            )?;
            out.push(Bytes::copy_from_slice(&buf));
            cur.timeseries.clear();
            cur.metadata.clear();
            cur.metadata.push(popped);
        }
    }

    if !cur.timeseries.is_empty() || !cur.metadata.is_empty() {
        buf.clear();
        encode_write_request_rw2_bytes(
            &cur,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_common_symbols()),
            &mut buf,
        )?;
        out.push(Bytes::copy_from_slice(&buf));
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
}
