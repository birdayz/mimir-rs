//! Zero-allocation protobuf wire encoder for `ingesterpb::QueryStreamResponse`.
//!
//! Encodes `QueryStreamSeries` directly from raw Prometheus `stringlabels` bytes or
//! `(name: &[u8], val: &[u8])` iterators, and `QueryStreamSeriesChunks` directly from
//! raw chunk metadata + `&[u8]` chunk payloads into `bytes::BytesMut` without
//! allocating `Vec<cortexpb::LabelPair>`, `Bytes::slice` refcounts, or `Vec<ingesterpb::Chunk>`.
//!
//! Ported from wire schema in `pkg/ingester/client/ingester.proto` (`QueryStreamResponse`).

use bytes::{BufMut, Bytes, BytesMut};

/// Error returned when encoding a `QueryStreamResponse` from raw `stringlabels` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QueryStreamEncodeError {
    #[error("truncated stringlabels buffer")]
    TruncatedStringLabels,
}

/// Borrowed raw TSDB chunk view for zero-alloc `QueryStreamResponse` encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawChunk<'a> {
    pub encoding: i32,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub data: &'a [u8],
}

/// Returns the number of bytes required to encode `v` as an unsigned protobuf base-128 varint.
#[inline(always)]
pub const fn varint_len(v: u64) -> usize {
    let bits = 64usize - (v | 1).leading_zeros() as usize;
    (bits * 9 + 64) >> 6
}

#[inline(always)]
fn write_varint(dst: &mut BytesMut, mut v: u64) {
    if v < 0x80 {
        dst.put_u8(v as u8);
        return;
    }
    let mut buf = [0u8; 10];
    let mut i = 0usize;
    while v >= 0x80 {
        buf[i] = (v as u8 & 0x7f) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = v as u8;
    dst.extend_from_slice(&buf[..=i]);
}

#[cold]
#[inline(never)]
fn decode_stringlabel_len_slow(
    data: &[u8],
    pos: usize,
) -> Result<(usize, usize), QueryStreamEncodeError> {
    let tail = data
        .get(pos + 1..pos + 4)
        .ok_or(QueryStreamEncodeError::TruncatedStringLabels)?;
    let len = (tail[0] as usize) | ((tail[1] as usize) << 8) | ((tail[2] as usize) << 16);
    Ok((len, pos + 4))
}

#[inline(always)]
fn decode_stringlabel_len(
    data: &[u8],
    pos: usize,
) -> Result<(usize, usize), QueryStreamEncodeError> {
    let b = *data
        .get(pos)
        .ok_or(QueryStreamEncodeError::TruncatedStringLabels)?;
    if b < 255 {
        Ok((b as usize, pos + 1))
    } else {
        decode_stringlabel_len_slow(data, pos)
    }
}

#[inline(always)]
fn label_pair_payload_len(name_len: usize, val_len: usize) -> usize {
    let k = if name_len == 0 {
        0
    } else {
        1 + varint_len(name_len as u64) + name_len
    };
    let v = if val_len == 0 {
        0
    } else {
        1 + varint_len(val_len as u64) + val_len
    };
    k + v
}

#[inline(always)]
fn write_label_pair(dst: &mut BytesMut, name: &[u8], val: &[u8], pair_payload_len: usize) {
    dst.put_u8(0x0a); // field 1 (QueryStreamSeries.labels), wire type 2
    write_varint(dst, pair_payload_len as u64);
    if !name.is_empty() {
        dst.put_u8(0x0a); // field 1 (LabelPair.name), wire type 2
        write_varint(dst, name.len() as u64);
        dst.extend_from_slice(name);
    }
    if !val.is_empty() {
        dst.put_u8(0x12); // field 2 (LabelPair.value), wire type 2
        write_varint(dst, val.len() as u64);
        dst.extend_from_slice(val);
    }
}

#[inline(always)]
fn raw_chunk_payload_len(chunk: &RawChunk<'_>) -> usize {
    let mut len = 0usize;
    if chunk.min_timestamp_ms != 0 {
        len += 1 + varint_len(chunk.min_timestamp_ms as u64);
    }
    if chunk.max_timestamp_ms != 0 {
        len += 1 + varint_len(chunk.max_timestamp_ms as u64);
    }
    if chunk.encoding != 0 {
        len += 1 + varint_len(chunk.encoding as i64 as u64);
    }
    if !chunk.data.is_empty() {
        len += 1 + varint_len(chunk.data.len() as u64) + chunk.data.len();
    }
    len
}

#[inline(always)]
fn write_raw_chunk(dst: &mut BytesMut, chunk: &RawChunk<'_>, payload_len: usize) {
    dst.put_u8(0x12); // field 2 (QueryStreamSeriesChunks.chunks), wire type 2
    write_varint(dst, payload_len as u64);
    if chunk.min_timestamp_ms != 0 {
        dst.put_u8(0x08); // field 1 (start_timestamp_ms), wire type 0
        write_varint(dst, chunk.min_timestamp_ms as u64);
    }
    if chunk.max_timestamp_ms != 0 {
        dst.put_u8(0x10); // field 2 (end_timestamp_ms), wire type 0
        write_varint(dst, chunk.max_timestamp_ms as u64);
    }
    if chunk.encoding != 0 {
        dst.put_u8(0x18); // field 3 (encoding), wire type 0
        write_varint(dst, chunk.encoding as i64 as u64);
    }
    if !chunk.data.is_empty() {
        dst.put_u8(0x22); // field 4 (data), wire type 2
        write_varint(dst, chunk.data.len() as u64);
        dst.extend_from_slice(chunk.data);
    }
}

#[inline(always)]
fn encode_stringlabels_message_single_pass(
    dst: &mut BytesMut,
    outer_tag: u8,
    stringlabels: &[u8],
    chunk_count: Option<i64>,
) -> Result<(), QueryStreamEncodeError> {
    let sl_len = stringlabels.len();
    // Every valid stringlabels buffer of length L encodes to <= 2*L bytes of LabelPairs.
    // Add 3 (outer tag + 2B varint placeholder) + 12 (optional chunk_count) + 16 slack.
    let max_needed = 3 + sl_len.saturating_mul(2) + 28;
    dst.reserve(max_needed);
    let start_len = dst.len();
    dst.put_u8(outer_tag);
    dst.put_u16(0);
    let body_start = dst.len();

    let mut pos = 0usize;
    while pos < sl_len {
        let b0 = stringlabels[pos];
        if b0 < 128 && pos + 1 + (b0 as usize) < sl_len {
            let klen = b0 as usize;
            let kstart = pos + 1;
            let kend = kstart + klen;
            let b1 = stringlabels[kend];
            if b1 < 128 && kend + 1 + (b1 as usize) <= sl_len {
                let vlen = b1 as usize;
                let vstart = kend + 1;
                let vend = vstart + vlen;
                pos = vend;
                if klen > 0 && vlen > 0 {
                    let pair_len = 4 + klen + vlen;
                    if pair_len < 128 {
                        let total = 6 + klen + vlen;
                        // SAFETY: `max_needed >= 3 + 2*sl_len + 28` guarantees spare capacity >= `total`.
                        // `BufMut::chunk_mut` returns `&mut UninitSlice` covering spare capacity
                        // `[dst.len()..dst.capacity()]` with valid Stacked-Borrows provenance.
                        unsafe {
                            let spare = BufMut::chunk_mut(dst);
                            debug_assert!(spare.len() >= total);
                            let ptr = spare.as_mut_ptr();
                            *ptr = 0x0a;
                            *ptr.add(1) = pair_len as u8;
                            *ptr.add(2) = 0x0a;
                            *ptr.add(3) = klen as u8;
                            std::ptr::copy_nonoverlapping(
                                stringlabels.as_ptr().add(kstart),
                                ptr.add(4),
                                klen,
                            );
                            *ptr.add(4 + klen) = 0x12;
                            *ptr.add(5 + klen) = vlen as u8;
                            std::ptr::copy_nonoverlapping(
                                stringlabels.as_ptr().add(vstart),
                                ptr.add(6 + klen),
                                vlen,
                            );
                            BufMut::advance_mut(dst, total);
                        }
                        continue;
                    }
                }
                let pair_len = label_pair_payload_len(klen, vlen);
                write_label_pair(dst, &stringlabels[kstart..kend], &stringlabels[vstart..vend], pair_len);
                continue;
            }
        }
        let (klen, kstart) = match decode_stringlabel_len(stringlabels, pos) {
            Ok(v) => v,
            Err(e) => {
                dst.truncate(start_len);
                return Err(e);
            }
        };
        let Some(kend) = kstart.checked_add(klen).filter(|&e| e <= sl_len) else {
            dst.truncate(start_len);
            return Err(QueryStreamEncodeError::TruncatedStringLabels);
        };
        let (vlen, vstart) = match decode_stringlabel_len(stringlabels, kend) {
            Ok(v) => v,
            Err(e) => {
                dst.truncate(start_len);
                return Err(e);
            }
        };
        let Some(vend) = vstart.checked_add(vlen).filter(|&e| e <= sl_len) else {
            dst.truncate(start_len);
            return Err(QueryStreamEncodeError::TruncatedStringLabels);
        };
        pos = vend;
        let pair_len = label_pair_payload_len(klen, vlen);
        write_label_pair(
            dst,
            &stringlabels[kstart..kend],
            &stringlabels[vstart..vend],
            pair_len,
        );
    }
    if let Some(cc) = chunk_count {
        if cc != 0 {
            dst.put_u8(0x10);
            write_varint(dst, cc as u64);
        }
    }

    let payload_len = dst.len() - body_start;
    let actual_vlen = varint_len(payload_len as u64);
    if actual_vlen == 2 {
        let v = payload_len as u64;
        dst[body_start - 2] = (v as u8 & 0x7f) | 0x80;
        dst[body_start - 1] = (v >> 7) as u8;
    } else if actual_vlen == 1 {
        dst[body_start - 2] = payload_len as u8;
        dst.copy_within(body_start.., body_start - 1);
        dst.truncate(dst.len() - 1);
    } else {
        let extra = actual_vlen - 2;
        let old_len = dst.len();
        dst.resize(old_len + extra, 0);
        dst.copy_within(body_start..old_len, body_start + extra);
        let mut v = payload_len as u64;
        let mut idx = body_start - 2;
        while v >= 0x80 {
            dst[idx] = (v as u8 & 0x7f) | 0x80;
            v >>= 7;
            idx += 1;
        }
        dst[idx] = v as u8;
    }
    Ok(())
}

/// Appends one `QueryStreamResponse.streaming_series` entry (field 3) encoded directly from
/// raw `stringlabels` bytes into `dst`.
pub fn encode_query_stream_series_stringlabels(
    dst: &mut BytesMut,
    stringlabels: &[u8],
    chunk_count: i64,
) -> Result<(), QueryStreamEncodeError> {
    encode_stringlabels_message_single_pass(dst, 0x1a, stringlabels, Some(chunk_count))
}

/// Appends one `MetricsForLabelMatchersResponse.metric` entry (field 1, tag `0x0a`) encoded
/// directly from raw `stringlabels` bytes into `dst` without allocating `Vec<LabelPair>`.
pub fn encode_metric_stringlabels(
    dst: &mut BytesMut,
    stringlabels: &[u8],
) -> Result<(), QueryStreamEncodeError> {
    encode_stringlabels_message_single_pass(dst, 0x0a, stringlabels, None)
}

/// Encodes a sequence of raw `stringlabels` byte slices into a single
/// `MetricsForLabelMatchersResponse` protobuf message in `dst`.
pub fn encode_metrics_for_label_matchers_response<'a, I>(
    dst: &mut BytesMut,
    metrics: I,
) -> Result<(), QueryStreamEncodeError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    for sl in metrics {
        encode_metric_stringlabels(dst, sl)?;
    }
    Ok(())
}

/// Appends one `QueryStreamResponse.streaming_series` entry (field 3) encoded directly from an
/// iterator of `(name, val)` byte slices using single-pass varint backpatching.
pub fn encode_query_stream_series_pairs<'a, I>(dst: &mut BytesMut, labels: I, chunk_count: i64)
where
    I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
{
    dst.reserve(256);
    dst.put_u8(0x1a); // field 3 (QueryStreamResponse.streaming_series), wire type 2
    // Reserve 2 bytes for the varint length (exact for 128..=16383 byte series, single copy_within otherwise).
    dst.put_u16(0);
    let body_start = dst.len();

    for (name, val) in labels {
        let pair_len = label_pair_payload_len(name.len(), val.len());
        write_label_pair(dst, name, val, pair_len);
    }
    if chunk_count != 0 {
        dst.put_u8(0x10); // field 2 (QueryStreamSeries.chunk_count), wire type 0
        write_varint(dst, chunk_count as u64);
    }

    let payload_len = dst.len() - body_start;
    let actual_vlen = varint_len(payload_len as u64);
    if actual_vlen == 2 {
        let v = payload_len as u64;
        dst[body_start - 2] = (v as u8 & 0x7f) | 0x80;
        dst[body_start - 1] = (v >> 7) as u8;
    } else if actual_vlen == 1 {
        dst[body_start - 2] = payload_len as u8;
        dst.copy_within(body_start.., body_start - 1);
        dst.truncate(dst.len() - 1);
    } else {
        let extra = actual_vlen - 2;
        let old_len = dst.len();
        dst.resize(old_len + extra, 0);
        dst.copy_within(body_start..old_len, body_start + extra);
        let mut v = payload_len as u64;
        let mut idx = body_start - 2;
        while v >= 0x80 {
            dst[idx] = (v as u8 & 0x7f) | 0x80;
            v >>= 7;
            idx += 1;
        }
        dst[idx] = v as u8;
    }
}

/// Returns the exact protobuf wire byte length of one `QueryStreamResponse.streaming_series_chunks`
/// entry (field 5 tag + varint length + payload).
#[inline]
pub fn encoded_series_chunks_len(series_index: u64, chunks: &[RawChunk<'_>]) -> usize {
    let mut sc_payload_len = if series_index != 0 {
        1 + varint_len(series_index)
    } else {
        0
    };
    for c in chunks {
        let clen = raw_chunk_payload_len(c);
        sc_payload_len += 1 + varint_len(clen as u64) + clen;
    }
    1 + varint_len(sc_payload_len as u64) + sc_payload_len
}

/// Appends one `QueryStreamResponse.streaming_series_chunks` entry (field 5) directly from raw
/// chunk metadata + `&[u8]` chunk bytes into `dst`.
pub fn encode_query_stream_series_chunks(
    dst: &mut BytesMut,
    series_index: u64,
    chunks: &[RawChunk<'_>],
) {
    let mut sc_payload_len = if series_index != 0 {
        1 + varint_len(series_index)
    } else {
        0
    };
    for c in chunks {
        let clen = raw_chunk_payload_len(c);
        sc_payload_len += 1 + varint_len(clen as u64) + clen;
    }

    dst.reserve(1 + varint_len(sc_payload_len as u64) + sc_payload_len);
    dst.put_u8(0x2a); // field 5 (QueryStreamResponse.streaming_series_chunks), wire type 2
    write_varint(dst, sc_payload_len as u64);

    if series_index != 0 {
        dst.put_u8(0x08); // field 1 (QueryStreamSeriesChunks.series_index), wire type 0
        write_varint(dst, series_index);
    }
    for c in chunks {
        let clen = raw_chunk_payload_len(c);
        write_raw_chunk(dst, c, clen);
    }
}

/// Encodes a batch of `(stringlabels_bytes, chunk_count)` series into a single `QueryStreamResponse`
/// protobuf message in `dst`.
pub fn encode_query_stream_timeseries_batch<'a, I>(
    dst: &mut BytesMut,
    series: I,
    is_end_of_series_stream: bool,
) -> Result<(), QueryStreamEncodeError>
where
    I: IntoIterator<Item = (&'a [u8], i64)>,
{
    for (stringlabels, chunk_count) in series {
        encode_query_stream_series_stringlabels(dst, stringlabels, chunk_count)?;
    }
    if is_end_of_series_stream {
        dst.extend_from_slice(&[0x20, 0x01]);
    }
    Ok(())
}

/// Encodes a batch of `(series_index, chunks)` entries into a single `QueryStreamResponse`
/// protobuf message in `dst`.
pub fn encode_query_stream_chunks_batch(
    dst: &mut BytesMut,
    series_chunks: &[(u64, &[RawChunk<'_>])],
) {
    for (series_index, chunks) in series_chunks {
        encode_query_stream_series_chunks(dst, *series_index, chunks);
    }
}

/// Reusable builder for zero-alloc `QueryStreamResponse` protobuf messages in `BytesMut`.
#[derive(Debug, Default)]
pub struct QueryStreamBatchBuilder {
    buf: BytesMut,
    end_of_series_written: bool,
}

impl QueryStreamBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            end_of_series_written: false,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear();
        self.end_of_series_written = false;
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn push_series_stringlabels(
        &mut self,
        stringlabels: &[u8],
        chunk_count: i64,
    ) -> Result<(), QueryStreamEncodeError> {
        encode_query_stream_series_stringlabels(&mut self.buf, stringlabels, chunk_count)
    }

    #[inline]
    pub fn push_series_pairs<'a, I>(&mut self, labels: I, chunk_count: i64)
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        encode_query_stream_series_pairs(&mut self.buf, labels, chunk_count);
    }

    #[inline]
    pub fn set_end_of_series_stream(&mut self) {
        if !self.end_of_series_written {
            self.buf.extend_from_slice(&[0x20, 0x01]);
            self.end_of_series_written = true;
        }
    }

    #[inline]
    pub fn push_series_chunks(&mut self, series_index: u64, chunks: &[RawChunk<'_>]) {
        encode_query_stream_series_chunks(&mut self.buf, series_index, chunks);
    }

    /// Splits off the encoded protobuf message as `Bytes`, retaining buffer capacity for reuse.
    #[inline]
    pub fn finish(&mut self) -> Bytes {
        self.end_of_series_written = false;
        self.buf.split().freeze()
    }

    /// Splits off the encoded protobuf message as `BytesMut`, retaining remaining spare capacity.
    #[inline]
    pub fn take_bytes_mut(&mut self) -> BytesMut {
        self.end_of_series_written = false;
        self.buf.split()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cortexpb, ingesterpb};
    use prost::Message;

    fn encode_stringlabels(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        for &(k, v) in pairs {
            for part in [k, v] {
                if part.len() < 255 {
                    buf.push(part.len() as u8);
                } else {
                    buf.push(255);
                    buf.push((part.len() & 0xff) as u8);
                    buf.push(((part.len() >> 8) & 0xff) as u8);
                    buf.push(((part.len() >> 16) & 0xff) as u8);
                }
                buf.extend_from_slice(part);
            }
        }
        buf
    }

    fn make_prost_series(pairs: &[(&[u8], &[u8])], chunk_count: i64) -> ingesterpb::QueryStreamSeries {
        ingesterpb::QueryStreamSeries {
            labels: pairs
                .iter()
                .map(|&(k, v)| cortexpb::LabelPair {
                    name: Bytes::copy_from_slice(k),
                    value: Bytes::copy_from_slice(v),
                })
                .collect(),
            chunk_count,
        }
    }

    #[test]
    fn test_varint_len_boundaries() {
        let mut test_vals = vec![0u64, 1, 127, 128, u64::MAX - 1, u64::MAX];
        for shift in 1..64 {
            let p = 1u64 << shift;
            test_vals.push(p - 1);
            test_vals.push(p);
            test_vals.push(p + 1);
        }
        for v in test_vals {
            let mut tmp = BytesMut::new();
            write_varint(&mut tmp, v);
            assert_eq!(tmp.len(), varint_len(v), "varint_len mismatch for {v}");
            assert_eq!(tmp.len(), prost::encoding::encoded_len_varint(v));
        }
    }

    #[test]
    fn test_series_batch_parity_empty_long_and_negative_counts() {
        let long_k = vec![b'k'; 300];
        let long_v = vec![b'v'; 17_000];
        let s1: Vec<(&[u8], &[u8])> = vec![];
        let s2: Vec<(&[u8], &[u8])> = vec![(b"", b""), (b"only_k", b""), (b"", b"only_v")];
        let s3: Vec<(&[u8], &[u8])> = vec![
            (b"__name__", b"http_requests_total"),
            (b"instance", b"10.0.0.1:8080"),
            (b"job", b"api"),
        ];
        let s4: Vec<(&[u8], &[u8])> = vec![(&long_k, &long_v)];

        let cases = vec![
            (s1, 0i64),
            (s2, -1i64),
            (s3, 42i64),
            (s4, i64::MIN),
        ];

        let encoded_sl: Vec<(Vec<u8>, i64)> = cases
            .iter()
            .map(|(pairs, cc)| (encode_stringlabels(pairs), *cc))
            .collect();

        for end_flag in [false, true] {
            let mut dst = BytesMut::new();
            encode_query_stream_timeseries_batch(
                &mut dst,
                encoded_sl.iter().map(|(b, cc)| (b.as_slice(), *cc)),
                end_flag,
            )
            .unwrap();

            let mut builder_pairs = QueryStreamBatchBuilder::with_capacity(4096);
            for (pairs, cc) in &cases {
                builder_pairs.push_series_pairs(pairs.iter().copied(), *cc);
            }
            if end_flag {
                builder_pairs.set_end_of_series_stream();
            }
            let pairs_bytes = builder_pairs.finish();

            let prost_msg = ingesterpb::QueryStreamResponse {
                streaming_series: cases
                    .iter()
                    .map(|(pairs, cc)| make_prost_series(pairs, *cc))
                    .collect(),
                is_end_of_series_stream: end_flag,
                streaming_series_chunks: Vec::new(),
            };
            let prost_bytes = prost_msg.encode_to_vec();

            assert_eq!(&dst[..], &prost_bytes[..], "stringlabels wire mismatch (end={end_flag})");
            assert_eq!(&pairs_bytes[..], &prost_bytes[..], "pairs wire mismatch (end={end_flag})");

            let decoded = ingesterpb::QueryStreamResponse::decode(&dst[..]).unwrap();
            assert_eq!(decoded, prost_msg);
        }
    }

    #[test]
    fn test_chunks_batch_parity_all_encodings_and_negative_timestamps() {
        let encodings = [0, 4, 5, 6, 7, 8, 9, -1, i32::MIN, i32::MAX];
        let payload_small = b"xor-chunk-bytes";
        let payload_large = vec![0xab; 4096];

        let mut raw_chunks = Vec::new();
        let mut prost_chunks = Vec::new();
        for (idx, &enc) in encodings.iter().enumerate() {
            let (min_ts, max_ts, data): (i64, i64, &[u8]) = match idx % 4 {
                0 => (0, 0, &[]),
                1 => (-1_700_000_000_000, -1_699_999_000_000, payload_small),
                2 => (i64::MIN, i64::MAX, &payload_large),
                _ => (-1, 0, b"\x00\x01\x02"),
            };
            raw_chunks.push(RawChunk {
                encoding: enc,
                min_timestamp_ms: min_ts,
                max_timestamp_ms: max_ts,
                data,
            });
            prost_chunks.push(ingesterpb::Chunk {
                start_timestamp_ms: min_ts,
                end_timestamp_ms: max_ts,
                encoding: enc,
                data: Bytes::copy_from_slice(data),
            });
        }

        let empty_slice: &[RawChunk<'_>] = &[];
        let mut builder = QueryStreamBatchBuilder::with_capacity(8192);
        builder.push_series_chunks(0, empty_slice);
        builder.push_series_chunks(1, &raw_chunks[..4]);
        builder.push_series_chunks(u64::MAX, &raw_chunks[4..]);
        let wire = builder.finish();

        let prost_msg = ingesterpb::QueryStreamResponse {
            streaming_series: Vec::new(),
            is_end_of_series_stream: false,
            streaming_series_chunks: vec![
                ingesterpb::QueryStreamSeriesChunks {
                    series_index: 0,
                    chunks: Vec::new(),
                },
                ingesterpb::QueryStreamSeriesChunks {
                    series_index: 1,
                    chunks: prost_chunks[..4].to_vec(),
                },
                ingesterpb::QueryStreamSeriesChunks {
                    series_index: u64::MAX,
                    chunks: prost_chunks[4..].to_vec(),
                },
            ],
        };
        let prost_wire = prost_msg.encode_to_vec();

        assert_eq!(&wire[..], &prost_wire[..]);
        let decoded = ingesterpb::QueryStreamResponse::decode(&wire[..]).unwrap();
        assert_eq!(decoded, prost_msg);
    }

    #[test]
    fn test_multi_mb_batch_parity() {
        let num_series = if cfg!(miri) { 24usize } else { 2000usize };
        let num_chunks = if cfg!(miri) { 8u64 } else { 1000u64 };
        let mut sl_items = Vec::with_capacity(num_series);
        let mut prost_series = Vec::with_capacity(num_series);
        for i in 0..num_series {
            let metric = format!("metric_{i}");
            let pod = format!("pod-{:05}", i);
            let pairs: Vec<(&[u8], &[u8])> = vec![
                (b"__name__", metric.as_bytes()),
                (b"cluster", b"prod-eu-west-1"),
                (b"namespace", b"monitoring"),
                (b"pod", pod.as_bytes()),
            ];
            sl_items.push((encode_stringlabels(&pairs), (i % 7) as i64));
            prost_series.push(make_prost_series(&pairs, (i % 7) as i64));
        }

        let big_chunk = vec![0x5a; 4096];
        let mut builder = QueryStreamBatchBuilder::with_capacity(if cfg!(miri) { 4096 } else { 64 * 1024 });
        for (sl, cc) in &sl_items {
            builder.push_series_stringlabels(sl, *cc).unwrap();
        }
        builder.set_end_of_series_stream();

        let mut prost_sc = Vec::with_capacity(num_chunks as usize);
        for i in 0..num_chunks {
            let rc = [RawChunk {
                encoding: 4,
                min_timestamp_ms: 1_700_000_000_000 + i as i64 * 1000,
                max_timestamp_ms: 1_700_000_060_000 + i as i64 * 1000,
                data: &big_chunk,
            }];
            builder.push_series_chunks(i, &rc);
            prost_sc.push(ingesterpb::QueryStreamSeriesChunks {
                series_index: i,
                chunks: vec![ingesterpb::Chunk {
                    start_timestamp_ms: rc[0].min_timestamp_ms,
                    end_timestamp_ms: rc[0].max_timestamp_ms,
                    encoding: 4,
                    data: Bytes::from_static(&[0x5a; 4096]),
                }],
            });
        }

        let wire = builder.finish();
        let min_wire_len = if cfg!(miri) { 32 * 1024 } else { 4 * 1024 * 1024 };
        assert!(wire.len() > min_wire_len);

        let prost_msg = ingesterpb::QueryStreamResponse {
            streaming_series: prost_series,
            is_end_of_series_stream: true,
            streaming_series_chunks: prost_sc,
        };
        let prost_wire = prost_msg.encode_to_vec();
        assert_eq!(&wire[..], &prost_wire[..]);

        let decoded = ingesterpb::QueryStreamResponse::decode(&wire[..]).unwrap();
        assert_eq!(decoded, prost_msg);
    }

    #[test]
    fn test_truncated_stringlabels_rejected() {
        let mut dst = BytesMut::new();
        assert_eq!(
            encode_query_stream_series_stringlabels(&mut dst, &[5, b'a', b'b'], 1),
            Err(QueryStreamEncodeError::TruncatedStringLabels)
        );
        assert!(dst.is_empty());
        assert_eq!(
            encode_query_stream_series_stringlabels(&mut dst, &[255, 10, 0], 1),
            Err(QueryStreamEncodeError::TruncatedStringLabels)
        );
        assert_eq!(
            encode_query_stream_series_stringlabels(&mut dst, &[1, b'k'], 1),
            Err(QueryStreamEncodeError::TruncatedStringLabels)
        );
    }

    #[test]
    fn test_metrics_for_label_matchers_raw_wire_parity() {
        let long_val = vec![b'x'; 300];
        let series_cases: Vec<Vec<(&[u8], &[u8])>> = vec![
            vec![],
            vec![(b"__name__", b"up")],
            vec![(b"__name__", b"http_requests_total"), (b"empty_val", b""), (b"job", b"api")],
            vec![(b"__name__", b"long_metric"), (b"payload", &long_val)],
        ];

        let mut sl_bufs = Vec::new();
        let mut prost_metrics = Vec::new();
        for pairs in &series_cases {
            sl_bufs.push(encode_stringlabels(pairs));
            prost_metrics.push(cortexpb::Metric {
                labels: pairs
                    .iter()
                    .map(|&(k, v)| cortexpb::LabelPair {
                        name: Bytes::copy_from_slice(k),
                        value: Bytes::copy_from_slice(v),
                    })
                    .collect(),
            });
        }

        let mut raw_dst = BytesMut::new();
        encode_metrics_for_label_matchers_response(
            &mut raw_dst,
            sl_bufs.iter().map(|v| v.as_slice()),
        )
        .unwrap();
        let prost_resp = ingesterpb::MetricsForLabelMatchersResponse {
            metric: prost_metrics,
        };
        let prost_wire = prost_resp.encode_to_vec();
        assert_eq!(&raw_dst[..], &prost_wire[..]);

        let decoded = ingesterpb::MetricsForLabelMatchersResponse::decode(&raw_dst[..]).unwrap();
        assert_eq!(decoded, prost_resp);

        // Empty response produces 0 bytes.
        let mut empty_dst = BytesMut::new();
        encode_metrics_for_label_matchers_response(&mut empty_dst, std::iter::empty()).unwrap();
        assert!(empty_dst.is_empty());
        assert!(ingesterpb::MetricsForLabelMatchersResponse::default()
            .encode_to_vec()
            .is_empty());
    }
}
