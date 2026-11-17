//! Gate 3 — 10-Decoder Differential & Corrupt-Byte Property/Fuzz Conformance Suite.
//!
//! Covers all 10 binary/wire/disk decoders with deterministic seeded property & mutation fuzzing:
//! 1. `mimir_proto::zerocopy::{decode_classic_stream, decode_write_request}` (+ `prost` & `go-oracle`)
//! 2. `mimir_proto::zerocopy::decode_rw2_stream` (RW2, Kafka v0/v1/v2, `V2CommonSymbols` + `go-oracle`)
//! 3. `mimir_proto::query_stream_enc::QueryStreamBatchBuilder` (+ `prost::Message::decode`)
//! 4. `mimir_proto::{snappy, s2}` bounded framed decoders (zip-bomb & multi-chunk roundtrip)
//! 5. `mimir_storage::ingest::kafka_client::KafkaClient::decode_fetched_partition` (RecordBatch v2)
//! 6. `mimir_tsdb::wlog::WlogReader` (multi-page torn/corrupt WAL records across None/Snappy/Zstd)
//! 7. `mimir_tsdb::{head_chunks::HeadChunksReader, chunks_block::BlockChunksReader}`
//! 8. `mimir_tsdb::index::IndexReader` (raw + CRC-recomputed TOC/symbols/postings/series mutations)
//! 9. `mimir_tsdb::chunkenc` iterators (`Xor`, `Xor2`, `Histogram`, `FloatHistogram`, `*St`)
//! 10. `mimir_ring::kv::memberlist::wire` (`MpReader`) + Snappy KV envelope codecs

#![allow(clippy::too_many_arguments)]

use std::io::Write;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, BytesB64Response, DecodeWriteRequest, ExemplarJson,
    HistogramJson, LabelJson, MetadataJson, SampleJson, SpanJson, TimeSeriesJson, WriteRequestJson,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use mimir_proto::query_stream_enc::{QueryStreamBatchBuilder, RawChunk};
use mimir_proto::ringpb::{
    InstanceDesc, InstanceState, PartitionDesc, PartitionRingDesc, PartitionState,
    PartitionTokenScheme,
};
use mimir_proto::s2;
use mimir_proto::snappy;
use mimir_proto::symbols::{v2_common_symbols, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{
    decode_classic_stream, decode_rw2_stream, decode_write_request, encode_write_request_rw2_bytes,
    DecodeMode, DecodeOptions, Rw2ResolvedScratch, WriteRequestScratch,
};
use mimir_ring::kv::memberlist::wire::{
    add_label_header, compress_payload, decode_ack_resp, decode_alive, decode_compound_message,
    decode_compressed_payload, decode_dead, decode_err_resp, decode_indirect_ping,
    decode_nack_resp, decode_ping, decode_push_pull, decode_user_msg_stream, decompress_buffer,
    encode_ack_resp, encode_alive, encode_dead, encode_err_resp, encode_indirect_ping,
    encode_nack_resp, encode_ping, encode_push_pull, encode_user_msg_stream, make_compound_message,
    remove_label_header, unwrap_crc32, wrap_crc32, AckResp, Alive, CompressionType as MlComp,
    Dead, ErrResp, IndirectPingReq, NackResp, NodeStateType, Ping, PushNodeState,
};
use mimir_ring::kv::ProtoSnappyCodec;
use mimir_ring::partition_ring::model::partition_ring_desc_codec;
use mimir_ring::ring::model::ring_desc_codec;
use mimir_storage::ingest::compression::{compress_record_batch, decompress_record_batch};
use mimir_storage::ingest::kafka_client::KafkaClient;
use mimir_storage::ingest::version::{
    deserialize_record_content, RecordSerializer, RecordVersion,
    MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
};
use mimir_tsdb::chunkenc::{
    FloatHistogram, FloatHistogramChunk, FloatHistogramIterator, FloatHistogramStChunk,
    FloatHistogramStIterator, Histogram, HistogramChunk, HistogramIterator, HistogramSpan,
    HistogramStChunk, HistogramStIterator, Xor2Chunk, Xor2Iterator, XorChunk, XorIterator,
};
use mimir_tsdb::chunks_block::{BlockChunkRef, BlockChunksReader, BlockChunksWriter, ChunkMeta};
use mimir_tsdb::head_chunks::{
    head_chunk_file_path, ChunkDiskMapperRef, HeadChunksReader, HeadChunksWriter,
};
use mimir_tsdb::index::{IndexReader, IndexToc, IndexWriter, INDEX_FILENAME};

const INDEX_TOC_LEN: usize = 6 * 8 + 4;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::wlog::{CompressionType as WalComp, WlogReader, WlogWriter};

// -------------------------------------------------------------------------------------------------
// Deterministic SplitMix64 PRNG & Byte Mutators
// -------------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[inline]
    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    #[inline]
    fn next_u8(&mut self) -> u8 {
        (self.next_u64() & 0xff) as u8
    }

    #[inline]
    fn gen_bool(&mut self) -> bool {
        (self.next_u64() & 1) == 1
    }

    #[inline]
    fn gen_range(&mut self, lo: usize, hi: usize) -> usize {
        debug_assert!(lo < hi);
        lo + ((self.next_u64() as usize) % (hi - lo))
    }

    fn gen_bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = if max_len == 0 {
            0
        } else {
            self.gen_range(0, max_len + 1)
        };
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(self.next_u8());
        }
        out
    }

    fn gen_ascii(&mut self, min_len: usize, max_len: usize) -> String {
        let len = self.gen_range(min_len, max_len + 1);
        let mut s = String::with_capacity(len);
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz_0123456789:-/.";
        for _ in 0..len {
            let b = ALPHABET[self.gen_range(0, ALPHABET.len())];
            s.push(b as char);
        }
        s
    }

    fn mutate_bytes(&mut self, valid: &[u8]) -> Vec<u8> {
        if valid.is_empty() {
            return self.gen_bytes(32);
        }
        let mut out = valid.to_vec();
        match self.gen_range(0, 5) {
            0 => {
                // Truncate
                let new_len = self.gen_range(0, out.len());
                out.truncate(new_len);
            }
            1 => {
                // 1..=4 bit flips
                let flips = self.gen_range(1, 5);
                for _ in 0..flips {
                    let idx = self.gen_range(0, out.len());
                    let bit = 1u8 << (self.gen_range(0, 8) as u8);
                    out[idx] ^= bit;
                }
            }
            2 => {
                // Byte overwrite burst
                let idx = self.gen_range(0, out.len());
                let cnt = self.gen_range(1, 9).min(out.len() - idx);
                for i in 0..cnt {
                    out[idx + i] = self.next_u8();
                }
            }
            3 => {
                // Insert random bytes
                let idx = self.gen_range(0, out.len() + 1);
                let extra = self.gen_bytes(16);
                out.splice(idx..idx, extra);
            }
            _ => {
                // Delete slice
                if out.len() > 1 {
                    let a = self.gen_range(0, out.len());
                    let b = self.gen_range(a, out.len());
                    out.drain(a..b);
                }
            }
        }
        out
    }
}

fn case_count(default: usize) -> usize {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.max(8))
        .unwrap_or(default)
}

fn gen_write_request_json(rng: &mut Rng, long_labels: bool) -> WriteRequestJson {
    let sources = ["API", "RULE", "OTLP"];
    let num_ts = rng.gen_range(0, 5);
    let mut timeseries = Vec::with_capacity(num_ts);
    for t_idx in 0..num_ts {
        let mut labels = vec![LabelJson {
            name: "__name__".to_string(),
            value: format!("fuzz_metric_{}", rng.gen_range(0, 12)),
        }];
        let extra = rng.gen_range(0, 5);
        for l_idx in 0..extra {
            let mut val = rng.gen_ascii(0, 20);
            if long_labels && l_idx == 0 && t_idx == 0 {
                val = "v".repeat(270);
            }
            // Keep label names unique and distinct from "__name__" so Go's builder doesn't drop/dedup weirdly
            labels.push(LabelJson {
                name: format!("lbl_{l_idx}_{}", rng.gen_ascii(1, 6)),
                value: val,
            });
        }
        // Sort labels by name and drop empty values so classic & RW2 canonical comparisons match Go.
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        labels.retain(|l| !l.value.is_empty());

        let base_ts = 1_700_000_000_000i64 + (rng.gen_range(0, 1000) as i64) * 1000;
        let num_samples = rng.gen_range(0, 4);
        let mut samples = Vec::with_capacity(num_samples);
        for s_idx in 0..num_samples {
            let v = match rng.gen_range(0, 5) {
                0 => 0.0,
                1 => -12.5,
                _ => ((rng.next_u64() % 10_000) as f64) / 8.0,
            };
            samples.push(SampleJson {
                timestamp_ms: base_ts + (s_idx as i64) * 15_000,
                value: v,
            });
        }

        let mut exemplars = Vec::new();
        if rng.gen_bool() {
            exemplars.push(ExemplarJson {
                labels: vec![LabelJson {
                    name: "trace_id".to_string(),
                    value: rng.gen_ascii(4, 16),
                }],
                value: 0.25,
                timestamp_ms: base_ts + 500,
            });
        }

        let mut histograms = Vec::new();
        if rng.gen_bool() {
            let is_float = rng.gen_bool();
            let nhcb = rng.gen_bool();
            let schema = if nhcb { -53 } else { rng.gen_range(0, 5) as i32 - 2 };
            let mut h = HistogramJson {
                timestamp_ms: base_ts + 1000,
                schema,
                zero_threshold: 0.001,
                sum: 12.5,
                reset_hint: (rng.gen_range(0, 4)) as u32,
                is_float,
                positive_spans: vec![SpanJson {
                    offset: 0,
                    length: 2,
                }],
                negative_spans: vec![SpanJson {
                    offset: -1,
                    length: 1,
                }],
                custom_values: if nhcb { vec![0.5, 1.5] } else { vec![] },
                ..Default::default()
            };
            if is_float {
                h.count_float = 10.5;
                h.zero_count_float = 1.5;
                h.positive_counts = vec![4.0, 5.0];
                h.negative_counts = vec![1.0];
            } else {
                h.count = 10;
                h.zero_count = 2;
                h.positive_deltas = vec![3, 1];
                h.negative_deltas = vec![1];
            }
            histograms.push(h);
        }

        timeseries.push(TimeSeriesJson {
            labels,
            samples,
            exemplars,
            histograms,
            created_timestamp: if rng.gen_bool() {
                base_ts - 60_000
            } else {
                0
            },
        });
    }

    let mtypes = ["COUNTER", "GAUGE", "HISTOGRAM"];
    let mut metadata = Vec::new();
    if !timeseries.is_empty() && rng.gen_bool() {
        let fam = timeseries[0]
            .labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.clone())
            .unwrap_or_else(|| "fuzz_m".to_string());
        metadata.push(MetadataJson {
            metric_type: mtypes[rng.gen_range(0, mtypes.len())].to_string(),
            metric_family_name: fam,
            help: rng.gen_ascii(0, 24),
            unit: rng.gen_ascii(0, 10),
        });
    }

    WriteRequestJson {
        mode: "classic".to_string(),
        source: sources[rng.gen_range(0, sources.len())].to_string(),
        skip_label_validation: rng.gen_bool(),
        skip_label_count_validation: rng.gen_bool(),
        timeseries,
        metadata,
    }
}

fn write_request_json_to_proto(req: &WriteRequestJson) -> cortexpb::WriteRequest {
    let source = match req.source.as_str() {
        "RULE" => cortexpb::write_request::SourceEnum::Rule as i32,
        "OTLP" => cortexpb::write_request::SourceEnum::Otlp as i32,
        _ => cortexpb::write_request::SourceEnum::Api as i32,
    };
    let metadata = req
        .metadata
        .iter()
        .map(|m| {
            let mt = cortexpb::metric_metadata::MetricType::from_str_name(&m.metric_type)
                .unwrap_or(cortexpb::metric_metadata::MetricType::Unknown)
                as i32;
            cortexpb::MetricMetadata {
                r#type: mt,
                metric_family_name: m.metric_family_name.clone(),
                help: m.help.clone(),
                unit: m.unit.clone(),
            }
        })
        .collect();
    let timeseries = req
        .timeseries
        .iter()
        .map(|ts| cortexpb::TimeSeries {
            labels: ts
                .labels
                .iter()
                .map(|l| cortexpb::LabelPair::from_strs(&l.name, &l.value))
                .collect(),
            samples: ts
                .samples
                .iter()
                .map(|s| cortexpb::Sample {
                    value: s.value,
                    timestamp_ms: s.timestamp_ms,
                })
                .collect(),
            exemplars: ts
                .exemplars
                .iter()
                .map(|e| cortexpb::Exemplar {
                    labels: e
                        .labels
                        .iter()
                        .map(|l| cortexpb::LabelPair::from_strs(&l.name, &l.value))
                        .collect(),
                    value: e.value,
                    timestamp_ms: e.timestamp_ms,
                })
                .collect(),
            histograms: ts
                .histograms
                .iter()
                .map(|h| {
                    let (count, zero_count) = if h.is_float {
                        (
                            Some(cortexpb::histogram::Count::CountFloat(h.count_float)),
                            Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(
                                h.zero_count_float,
                            )),
                        )
                    } else {
                        (
                            Some(cortexpb::histogram::Count::CountInt(h.count)),
                            Some(cortexpb::histogram::ZeroCount::ZeroCountInt(h.zero_count)),
                        )
                    };
                    cortexpb::Histogram {
                        sum: h.sum,
                        schema: h.schema,
                        zero_threshold: h.zero_threshold,
                        negative_spans: h
                            .negative_spans
                            .iter()
                            .map(|s| cortexpb::BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        negative_deltas: h.negative_deltas.clone(),
                        negative_counts: h.negative_counts.clone(),
                        positive_spans: h
                            .positive_spans
                            .iter()
                            .map(|s| cortexpb::BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        positive_deltas: h.positive_deltas.clone(),
                        positive_counts: h.positive_counts.clone(),
                        reset_hint: h.reset_hint as i32,
                        timestamp: h.timestamp_ms,
                        custom_values: h.custom_values.clone(),
                        count,
                        zero_count,
                    }
                })
                .collect(),
            created_timestamp: ts.created_timestamp,
        })
        .collect();
    cortexpb::WriteRequest {
        timeseries,
        source,
        metadata,
        skip_label_validation: req.skip_label_validation,
        skip_label_count_validation: req.skip_label_count_validation,
        ..Default::default()
    }
}

// =================================================================================================
// 1. mimir_proto::zerocopy::{decode_classic_stream, decode_write_request}
// =================================================================================================

#[test]
fn fuzz_01_zerocopy_classic_stream_and_write_request() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let mut rng = Rng::new(0x01_5eed_cafe_1001);
    let mut scratch_a = WriteRequestScratch::new();
    let mut scratch_b = WriteRequestScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();

    // A) Arbitrary & mutated protobuf bytes never panic/OOM and match between classic decoders.
    let raw_cases = case_count(512);
    for iter in 0..raw_cases {
        let buf = if iter % 3 == 0 {
            let valid = write_request_json_to_proto(&gen_write_request_json(&mut rng, false))
                .encode_to_vec();
            rng.mutate_bytes(&valid)
        } else {
            rng.gen_bytes(192)
        };
        let r_ref = decode_write_request(&buf, DecodeOptions::classic(), &mut scratch_a);
        label_bytes.clear();
        samples.clear();
        let mut streamed_series = 0usize;
        let r_stream = decode_classic_stream(
            &buf,
            DecodeOptions::classic(),
            &mut scratch_b,
            &mut label_bytes,
            &mut samples,
            |_| streamed_series += 1,
        );
        match (r_ref, r_stream) {
            (Ok(bwr), Ok(hdr)) => {
                assert_eq!(streamed_series, bwr.timeseries_len());
                assert_eq!(hdr.source, bwr.source);
                assert_eq!(hdr.skip_label_validation, bwr.skip_label_validation);
                assert_eq!(
                    hdr.skip_label_count_validation,
                    bwr.skip_label_count_validation
                );
                assert_eq!(
                    scratch_b.metadata(&buf).collect::<Vec<_>>(),
                    bwr.metadata().collect::<Vec<_>>()
                );
            }
            (Err(e1), Err(e2)) => {
                assert_eq!(e1, e2, "classic decoder error mismatch on iter {iter}");
                assert!(label_bytes.is_empty());
                assert!(samples.is_empty());
            }
            (Ok(_), Err(e)) => panic!("ref Ok, stream Err({e:?}) on iter {iter}"),
            (Err(e), Ok(_)) => panic!("ref Err({e:?}), stream Ok on iter {iter}"),
        }
    }

    // B) Structured randomized WriteRequest parity vs prost::Message::decode.
    let struct_cases = case_count(256);
    for iter in 0..struct_cases {
        let req = write_request_json_to_proto(&gen_write_request_json(&mut rng, iter % 7 == 0));
        let wire = req.encode_to_vec();
        let prost_dec = cortexpb::WriteRequest::decode(&wire[..]).unwrap();
        let zc_dec = decode_write_request(&wire, DecodeOptions::classic(), &mut scratch_a)
            .expect("classic zerocopy decode")
            .to_owned_write_request();
        assert_eq!(zc_dec, prost_dec, "prost parity mismatch on case {iter}");

        label_bytes.clear();
        samples.clear();
        let mut count = 0usize;
        decode_classic_stream(
            &wire,
            DecodeOptions::classic(),
            &mut scratch_b,
            &mut label_bytes,
            &mut samples,
            |_| count += 1,
        )
        .expect("classic stream decode");
        assert_eq!(count, prost_dec.timeseries.len());
    }

    // C) Differential parity vs Go oracle (classic encode + decode).
    let oracle_cases = case_count(24).min(32);
    for iter in 0..oracle_cases {
        let mut req_json = gen_write_request_json(&mut rng, iter % 4 == 0);
        req_json.mode = "classic".to_string();
        let go_enc: BytesB64Response =
            invoke_oracle("encode-write-request", &req_json).expect("go encode classic");
        let go_wire = B64.decode(&go_enc.bytes_b64).unwrap();

        let rust_owned = decode_write_request(&go_wire, DecodeOptions::classic(), &mut scratch_a)
            .expect("rust decode go classic")
            .to_owned_write_request();
        let expected = write_request_json_to_proto(&req_json);
        assert_eq!(rust_owned, expected, "go->rust classic mismatch on {iter}");

        let rust_wire = expected.encode_to_vec();
        let go_dec: WriteRequestJson = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: "classic".to_string(),
                bytes_b64: B64.encode(&rust_wire),
            },
        )
        .expect("go decode rust classic");
        assert_eq!(go_dec.timeseries.len(), expected.timeseries.len());
        assert_eq!(go_dec.metadata.len(), expected.metadata.len());
        assert_eq!(go_dec.source, req_json.source);
    }
}

// =================================================================================================
// 2. mimir_proto::zerocopy::decode_rw2_stream (RW2 & Kafka v0/v1/v2 + V2CommonSymbols)
// =================================================================================================

#[test]
fn fuzz_02_zerocopy_rw2_stream_kafka_v0_v1_v2() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let mut rng = Rng::new(0x02_5eed_cafe_2002);
    let mut scratch = WriteRequestScratch::new();
    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let v2_syms = v2_common_symbols();

    // A) Corrupt/truncated RW2 & KafkaV2 frames never panic/OOM.
    let raw_cases = case_count(512);
    for iter in 0..raw_cases {
        let buf = if iter % 3 == 0 {
            let req = write_request_json_to_proto(&gen_write_request_json(&mut rng, false));
            let mut wire = Vec::new();
            if iter % 2 == 0 {
                encode_write_request_rw2_bytes(&req, 0, None, &mut wire).unwrap();
            } else {
                encode_write_request_rw2_bytes(
                    &req,
                    V2_RECORD_SYMBOL_OFFSET,
                    Some(v2_syms),
                    &mut wire,
                )
                .unwrap();
            }
            rng.mutate_bytes(&wire)
        } else {
            rng.gen_bytes(192)
        };
        let custom_opts = DecodeOptions {
            mode: DecodeMode::Rw2Custom {
                offset: 4,
                common_symbols: &["", "__name__", "job", "up"],
            },
            ..DecodeOptions::rw2()
        };
        for mode in [
            DecodeOptions::rw2(),
            DecodeOptions::kafka_v2(),
            custom_opts,
        ] {
            let _ = decode_write_request(&buf, mode, &mut scratch);
            label_bytes.clear();
            samples.clear();
            let mut meta = Vec::new();
            let _ = decode_rw2_stream(
                &buf,
                mode,
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta,
                |_| {},
            );
        }
    }

    // B) Structured roundtrip across RW2 + Kafka RecordSerializer v0/v1/v2.
    let struct_cases = case_count(256);
    let ser_v0 = RecordSerializer::new(RecordVersion::V0);
    let ser_v1 = RecordSerializer::new(RecordVersion::V1);
    let ser_v2 = RecordSerializer::new(RecordVersion::V2);
    for iter in 0..struct_cases {
        let mut req = write_request_json_to_proto(&gen_write_request_json(&mut rng, iter % 8 == 0));
        // Metadata must match series in RW2; keep valid non-empty labels.
        let mut rw2_wire = Vec::new();
        encode_write_request_rw2_bytes(&req, 0, None, &mut rw2_wire).unwrap();
        let mut kv2_wire = Vec::new();
        encode_write_request_rw2_bytes(
            &req,
            V2_RECORD_SYMBOL_OFFSET,
            Some(v2_syms),
            &mut kv2_wire,
        )
        .unwrap();

        for (wire, opts) in [
            (&rw2_wire, DecodeOptions::rw2()),
            (&kv2_wire, DecodeOptions::kafka_v2()),
        ] {
            let mut cnt = 0usize;
            label_bytes.clear();
            samples.clear();
            let mut meta = Vec::new();
            let hdr = decode_rw2_stream(
                wire,
                opts,
                &mut scratch,
                &mut resolved,
                &mut label_bytes,
                &mut samples,
                &mut meta,
                |_| cnt += 1,
            )
            .expect("structured decode_rw2_stream");
            assert_eq!(cnt, req.timeseries.len());
            assert_eq!(hdr.source, req.source);

            let owned = decode_write_request(wire, opts, &mut scratch)
                .expect("structured decode_write_request rw2")
                .to_owned_write_request();
            assert_eq!(owned.timeseries.len(), req.timeseries.len());
        }

        // Kafka RecordSerializer v0/v1/v2 roundtrip -> deserialize_record_content
        if !req.timeseries.is_empty() {
            // Ensure labels exist on every timeseries for partitioner
            for ts in &mut req.timeseries {
                if ts.labels.is_empty() {
                    ts.labels
                        .push(cortexpb::LabelPair::from_static("__name__", "fallback"));
                }
            }
            let ver = match iter % 3 {
                0 => (&ser_v0, 0i32),
                1 => (&ser_v1, 1i32),
                _ => (&ser_v2, 2i32),
            };
            let (recs, _) = ver
                .0
                .to_records(
                    "ingest",
                    0,
                    "t-fuzz",
                    &req,
                    MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
                )
                .expect("serialize kafka record");
            for r in recs {
                let mut dec_scratch = WriteRequestScratch::new();
                let dec = deserialize_record_content(&r.value, ver.1, &mut dec_scratch)
                    .expect("deserialize_record_content");
                assert!(dec.timeseries_len() > 0 || req.timeseries.is_empty());
            }
        }
    }

    // C) Go oracle differential parity for rw2 and kafka-v2.
    let oracle_cases = case_count(24).min(24);
    for iter in 0..oracle_cases {
        let mode = if iter % 2 == 0 { "rw2" } else { "kafka-v2" };
        let opts = if mode == "rw2" {
            DecodeOptions::rw2()
        } else {
            DecodeOptions::kafka_v2()
        };
        let mut req_json = gen_write_request_json(&mut rng, iter % 5 == 0);
        req_json.mode = mode.to_string();
        let go_enc: BytesB64Response =
            invoke_oracle("encode-write-request", &req_json).expect("go encode rw2");
        let go_wire = B64.decode(&go_enc.bytes_b64).unwrap();

        label_bytes.clear();
        samples.clear();
        let mut stream_cnt = 0usize;
        let mut meta = Vec::new();
        decode_rw2_stream(
            &go_wire,
            opts,
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta,
            |_| stream_cnt += 1,
        )
        .expect("rust decode_rw2_stream on go wire");
        assert_eq!(stream_cnt, req_json.timeseries.len());

        let rust_proto = write_request_json_to_proto(&req_json);
        let mut rust_wire = Vec::new();
        if mode == "rw2" {
            encode_write_request_rw2_bytes(&rust_proto, 0, None, &mut rust_wire).unwrap();
        } else {
            encode_write_request_rw2_bytes(
                &rust_proto,
                V2_RECORD_SYMBOL_OFFSET,
                Some(v2_syms),
                &mut rust_wire,
            )
            .unwrap();
        }
        let go_dec: WriteRequestJson = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: mode.to_string(),
                bytes_b64: B64.encode(&rust_wire),
            },
        )
        .expect("go decode rust rw2 wire");
        assert_eq!(go_dec.timeseries.len(), rust_proto.timeseries.len());
    }
}

// =================================================================================================
// 3. mimir_proto::query_stream_enc::QueryStreamBatchBuilder
// =================================================================================================

#[test]
fn fuzz_03_query_stream_batch_builder_prost_parity() {
    let mut rng = Rng::new(0x03_5eed_cafe_3003);
    let cases = case_count(256);
    let encodings = [0, 1, 2, 3, 4, 5, 6, -1, i32::MIN, i32::MAX];

    for iter in 0..cases {
        let num_series = rng.gen_range(0, 10);
        let mut builder = QueryStreamBatchBuilder::with_capacity(256);
        let mut prost_series = Vec::with_capacity(num_series);

        for s_idx in 0..num_series {
            let num_labels = rng.gen_range(0, 7);
            let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(num_labels);
            for l_idx in 0..num_labels {
                let k = if iter % 11 == 0 && l_idx == 0 {
                    vec![b'k'; 260]
                } else if l_idx == 1 && rng.gen_bool() {
                    Vec::new()
                } else {
                    rng.gen_ascii(1, 20).into_bytes()
                };
                let v = if iter % 13 == 0 && l_idx == 0 {
                    vec![b'v'; 310]
                } else if rng.gen_range(0, 5) == 0 {
                    Vec::new()
                } else {
                    rng.gen_ascii(0, 32).into_bytes()
                };
                pairs.push((k, v));
            }
            let cc = match rng.gen_range(0, 5) {
                0 => 0,
                1 => -1,
                2 => i64::MIN,
                3 => i64::MAX,
                _ => (rng.next_u64() & 0xffff) as i64,
            };
            builder.push_series_pairs(
                pairs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
                cc,
            );
            prost_series.push(ingesterpb::QueryStreamSeries {
                labels: pairs
                    .iter()
                    .map(|(k, v)| cortexpb::LabelPair {
                        name: Bytes::copy_from_slice(k),
                        value: Bytes::copy_from_slice(v),
                    })
                    .collect(),
                chunk_count: cc,
            });
            let _ = s_idx;
        }

        let end_flag = rng.gen_bool();
        if end_flag {
            builder.set_end_of_series_stream();
        }

        let num_chunk_groups = rng.gen_range(0, 6);
        let mut prost_chunks = Vec::with_capacity(num_chunk_groups);
        for g_idx in 0..num_chunk_groups {
            let series_index = if g_idx == 0 && rng.gen_bool() {
                u64::MAX
            } else {
                rng.next_u64() % 1000
            };
            let nchunks = rng.gen_range(0, 5);
            let mut payloads = Vec::with_capacity(nchunks);
            let mut p_chunks = Vec::with_capacity(nchunks);
            for _ in 0..nchunks {
                let enc = encodings[rng.gen_range(0, encodings.len())];
                let (min_ts, max_ts) = match rng.gen_range(0, 4) {
                    0 => (i64::MIN, i64::MAX),
                    1 => (-1_700_000_000_000, -1_699_999_000_000),
                    2 => (-1, 0),
                    _ => (1_700_000_000_000, 1_700_000_120_000),
                };
                let data = rng.gen_bytes(512);
                p_chunks.push(ingesterpb::Chunk {
                    start_timestamp_ms: min_ts,
                    end_timestamp_ms: max_ts,
                    encoding: enc,
                    data: Bytes::copy_from_slice(&data),
                });
                payloads.push((enc, min_ts, max_ts, data));
            }
            let raw_refs: Vec<RawChunk<'_>> = payloads
                .iter()
                .map(|(enc, min_ts, max_ts, data)| RawChunk {
                    encoding: *enc,
                    min_timestamp_ms: *min_ts,
                    max_timestamp_ms: *max_ts,
                    data: data.as_slice(),
                })
                .collect();
            builder.push_series_chunks(series_index, &raw_refs);
            prost_chunks.push(ingesterpb::QueryStreamSeriesChunks {
                series_index,
                chunks: p_chunks,
            });
        }

        let wire = builder.finish();
        let expected_msg = ingesterpb::QueryStreamResponse {
            streaming_series: prost_series,
            is_end_of_series_stream: end_flag,
            streaming_series_chunks: prost_chunks,
        };
        assert_eq!(
            &wire[..],
            &expected_msg.encode_to_vec()[..],
            "wire mismatch on case {iter}"
        );
        let decoded = ingesterpb::QueryStreamResponse::decode(&wire[..])
            .expect("prost decode QueryStreamResponse");
        assert_eq!(decoded, expected_msg, "decoded mismatch on case {iter}");

        // Arbitrary/truncated stringlabels must fail cleanly without panicking.
        let mut b2 = QueryStreamBatchBuilder::with_capacity(64);
        let bad_sl = rng.gen_bytes(64);
        let _ = b2.push_series_stringlabels(&bad_sl, 1);
    }
}

// =================================================================================================
// 4. mimir_proto::{snappy, s2} bounded framed decoders
// =================================================================================================

#[test]
fn fuzz_04_snappy_and_s2_framed_decoders() {
    let mut rng = Rng::new(0x04_5eed_cafe_4004);
    let cases = case_count(512);

    for iter in 0..cases {
        let max_len = rng.gen_range(0, 513);
        let mut frame = match iter % 4 {
            0 => rng.gen_bytes(256),
            1 => {
                // Snappy zip-bomb exceeding max_len
                let big = vec![b'z'; max_len + 4096];
                let comp = snappy::compress_framed(&big).unwrap();
                if iter % 8 == 1 {
                    rng.mutate_bytes(&comp)
                } else {
                    comp
                }
            }
            2 => {
                // Malicious S2 frame with forged chunk lengths / varint uncompressed len
                let mut f = b"\xff\x06\x00\x00S2sTwO".to_vec();
                let ctype = [0x00u8, 0x01, 0x02, 0xfe, 0x80, 0x42][rng.gen_range(0, 6)];
                f.push(ctype);
                let body = rng.gen_bytes(64);
                let clen = if rng.gen_bool() {
                    body.len() + 4
                } else {
                    0x00ff_ffff
                };
                f.push((clen & 0xff) as u8);
                f.push(((clen >> 8) & 0xff) as u8);
                f.push(((clen >> 16) & 0xff) as u8);
                f.extend_from_slice(&rng.next_u32().to_le_bytes());
                f.extend_from_slice(&body);
                f
            }
            _ => {
                // Mutated Snappy framed stream
                let sample = rng.gen_bytes(300);
                let comp = snappy::compress_framed(&sample).unwrap();
                rng.mutate_bytes(&comp)
            }
        };

        // Ensure snappy bounded decoder never exceeds max_len and restores dst on Err.
        let prefix = b"pre:";
        let mut dst_snappy = prefix.to_vec();
        dst_snappy.reserve_exact(max_len + 16);
        let cap_before = dst_snappy.capacity();
        match snappy::decompress_framed_bounded_into(&frame, &mut dst_snappy, max_len) {
            Ok(n) => {
                assert!(n <= max_len, "snappy exceeded max_len: {n} > {max_len}");
                assert_eq!(dst_snappy.len(), prefix.len() + n);
                assert_eq!(&dst_snappy[..prefix.len()], prefix);
            }
            Err(_) => {
                assert_eq!(dst_snappy.as_slice(), prefix);
                assert_eq!(dst_snappy.capacity(), cap_before);
            }
        }

        // Same bounds check on S2 framed decoder.
        let mut dst_s2 = prefix.to_vec();
        match s2::decompress_framed_into(&frame, &mut dst_s2, max_len) {
            Ok(()) => {
                let n = dst_s2.len() - prefix.len();
                assert!(n <= max_len, "s2 exceeded max_len: {n} > {max_len}");
                assert_eq!(&dst_s2[..prefix.len()], prefix);
            }
            Err(_) => {
                assert!(dst_s2.len() <= prefix.len() + max_len);
            }
        }
        let _ = &mut frame;
    }

    // Multi-chunk roundtrips (> 64 KiB) for Snappy framed + S2 stream decoding.
    for size in [0usize, 1, 1024, 65_536, 140_000] {
        let mut data = vec![0u8; size];
        for (i, b) in data.iter_mut().enumerate() {
            *b = ((i * 31 + 7) & 0xff) as u8;
        }
        let comp = snappy::compress_framed(&data).unwrap();
        let mut out1 = Vec::new();
        let n1 = snappy::decompress_framed_bounded_into(&comp, &mut out1, size).unwrap();
        assert_eq!(n1, size);
        assert_eq!(out1, data);

        let mut out2 = Vec::new();
        s2::decompress_framed_into(&comp, &mut out2, size).unwrap();
        assert_eq!(out2.len(), size);
        assert_eq!(out2, data);
    }
}

// =================================================================================================
// 5. Kafka RecordBatch v2 fast decoder (mimir_storage::ingest::kafka_client::KafkaClient)
// =================================================================================================

fn encode_kafka_v2_batch(
    rng: &mut Rng,
    num_records: usize,
    base_offset: i64,
    comp: Compression,
) -> Bytes {
    let mut records = Vec::with_capacity(num_records);
    for i in 0..num_records {
        let mut headers = IndexMap::new();
        if rng.gen_bool() {
            headers.insert(
                StrBytes::from_static_str("v"),
                Some(Bytes::from_static(&[0, 0, 0, 2])),
            );
        }
        records.push(Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: base_offset + i as i64,
            sequence: i as i32,
            timestamp: 1_700_000_000_000 + (i as i64) * 100,
            key: if rng.gen_bool() {
                Some(Bytes::from(rng.gen_bytes(16)))
            } else {
                None
            },
            value: Some(Bytes::from(rng.gen_bytes(64))),
            headers,
        });
    }
    let mut buf = BytesMut::new();
    let opts = RecordEncodeOptions {
        version: 2,
        compression: comp,
    };
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        records.iter(),
        &opts,
        Some(compress_record_batch),
    )
    .expect("encode kafka v2 batch");
    buf.freeze()
}

#[test]
fn fuzz_05_kafka_record_batch_v2_fast_decoder() {
    let client = KafkaClient::new(
        vec!["127.0.0.1:9092".to_string()],
        "fuzz-client",
        "",
        Duration::from_secs(5),
        Duration::from_secs(5),
    )
    .expect("KafkaClient::new");
    let mut rng = Rng::new(0x05_5eed_cafe_5005);
    let compressions = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    // A) Differential roundtrip across all 5 codecs vs kafka_protocol::records::RecordBatchDecoder
    for (idx, &comp) in compressions.iter().enumerate() {
        for rep in 0..8usize {
            let nrecs = rng.gen_range(1, 6);
            let base_off = (idx * 10 + rep) as i64;
            let frame = encode_kafka_v2_batch(&mut rng, nrecs, base_off, comp);
            let (fast_recs, next_off, end_off) = client
                .decode_fetched_partition(frame.clone(), base_off)
                .expect("fast decode valid v2 batch");
            let mut ref_cursor = frame.clone();
            let ref_recs = RecordBatchDecoder::decode_with_custom_compression(
                &mut ref_cursor,
                Some(decompress_record_batch),
            )
            .expect("ref decode valid v2 batch");
            assert_eq!(fast_recs.len(), ref_recs.len());
            assert_eq!(end_off, Some(base_off + nrecs as i64 - 1));
            assert_eq!(next_off, base_off + nrecs as i64);
            for (f, r) in fast_recs.iter().zip(ref_recs.iter()) {
                assert_eq!(f.offset, r.offset);
                assert_eq!(f.timestamp_ms, r.timestamp);
                assert_eq!(f.key, r.key.clone().unwrap_or_default());
                assert_eq!(f.value, r.value.clone().unwrap_or_default());
            }
        }
    }

    // B) Mutated/corrupt frames never panic or OOM, and ANY bit-flip in CRC-protected bytes fails!
    let cases = case_count(512);
    for iter in 0..cases {
        let comp = compressions[iter % compressions.len()];
        let nrecs = rng.gen_range(1, 4);
        let valid = encode_kafka_v2_batch(&mut rng, nrecs, 0, comp);

        // 1. Corrupt CRC region (bytes 21..valid.len()) -> MUST return Err
        if valid.len() > 22 {
            let mut bad = valid.to_vec();
            let pos = if iter % 5 == 0 {
                // Flip control-batch attribute bit (offset 22, bit 5): CRC must still reject!
                22
            } else {
                rng.gen_range(21, bad.len())
            };
            bad[pos] ^= 1u8 << (rng.gen_range(0, 8) as u8);
            let res = client.decode_fetched_partition(Bytes::from(bad), 0);
            assert!(
                res.is_err(),
                "bit-flip at CRC-covered offset {pos} unexpectedly succeeded"
            );
        }

        // 2. Arbitrary random bytes / truncated / tampered header lengths never panic or OOM
        let junk = match iter % 4 {
            0 => rng.gen_bytes(160),
            1 => {
                let mut t = valid.to_vec();
                t.truncate(rng.gen_range(0, t.len()));
                t
            }
            2 => {
                // Craft v2 header with huge batchLength / recordsCount
                let mut b = valid.to_vec();
                b[8..12].copy_from_slice(&(i32::MAX).to_be_bytes());
                if b.len() > 61 {
                    b[57..61].copy_from_slice(&(i32::MAX).to_be_bytes());
                }
                b
            }
            _ => rng.mutate_bytes(&valid),
        };
        let _ = client.decode_fetched_partition(Bytes::from(junk), 0);
    }
}

// =================================================================================================
// 6. mimir_tsdb::wlog::WlogReader
// =================================================================================================

#[test]
fn fuzz_06_tsdb_wlog_reader() {
    let mut rng = Rng::new(0x06_5eed_cafe_6006);
    let comps = [WalComp::None, WalComp::Snappy, WalComp::Zstd];

    // Generate valid multi-page WAL segments (> 32 KiB record spanning REC_FIRST/MIDDLE/LAST)
    let mut templates: Vec<Vec<u8>> = Vec::with_capacity(comps.len());
    for &comp in &comps {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), 128 * 1024, comp).unwrap();
        let small = vec![0x41u8; 200];
        let multi_page: Vec<u8> = (0..48_000).map(|_| rng.next_u8()).collect();
        w.log(&[&small, &multi_page, &small]).unwrap();
        w.close().unwrap();
        let seg_bytes = std::fs::read(mimir_tsdb::wlog::segment_name(dir.path(), 0)).unwrap();
        assert!(seg_bytes.len() > 32 * 1024);
        templates.push(seg_bytes);
    }

    let cases = case_count(256);
    for iter in 0..cases {
        let buf = match iter % 4 {
            0 => rng.gen_bytes(512),
            1 => {
                // Torn multi-page WAL segment
                let tpl = &templates[iter % templates.len()];
                let cut = rng.gen_range(0, tpl.len());
                tpl[..cut].to_vec()
            }
            2 => {
                // Non-zero byte in page padding or bad record size
                let tpl = &templates[iter % templates.len()];
                rng.mutate_bytes(tpl)
            }
            _ => {
                // Crafted header claiming huge length or invalid rec type
                let mut b = vec![0u8; 64];
                b[0] = rng.next_u8();
                b[1..3].copy_from_slice(&u16::MAX.to_be_bytes());
                let crc = crc32c::crc32c(&b[7..]);
                b[3..7].copy_from_slice(&crc.to_be_bytes());
                b
            }
        };

        let mut r = WlogReader::from_reader(buf.as_slice());
        for _ in 0..1024 {
            match r.next_record() {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    }

    // Also test WlogReader::open_dir on a mutated segment file on disk.
    let dir = tempfile::tempdir().unwrap();
    let mut mutated = templates[0].clone();
    let last_idx = mutated.len() - 1;
    mutated[last_idx] ^= 0xff;
    std::fs::write(mimir_tsdb::wlog::segment_name(dir.path(), 0), &mutated).unwrap();
    let mut r = WlogReader::open_dir(dir.path()).unwrap();
    let mut saw_err = false;
    loop {
        match r.next_record() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                saw_err = true;
                break;
            }
        }
    }
    assert!(saw_err, "corrupted trailing WAL CRC must fail");
}

// =================================================================================================
// 7. mimir_tsdb::{head_chunks::HeadChunksReader, chunks_block::BlockChunksReader}
// =================================================================================================

#[test]
fn fuzz_07_tsdb_head_chunks_and_block_chunks_readers() {
    let mut rng = Rng::new(0x07_5eed_cafe_7007);

    // Prepare valid head chunk bytes + block chunk bytes as mutation templates
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, 1_000, 1.0).unwrap();
        app.append(0, 2_000, 2.5).unwrap();
    }
    let mut hc = HistogramChunk::new();
    {
        let mut app = hc.appender().unwrap();
        let h = Histogram {
            count: 5,
            sum: 7.5,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![2, 0],
            ..Default::default()
        };
        app.append_histogram(None, 0, 1_000, &h, true).unwrap();
    }
    let encoded_xor = mimir_tsdb::chunkenc::EncodedChunk::Xor(xc);
    let encoded_hist = mimir_tsdb::chunkenc::EncodedChunk::Histogram(hc);

    let head_tpl_dir = tempfile::tempdir().unwrap();
    let ref1: ChunkDiskMapperRef;
    {
        let mut hw = HeadChunksWriter::open(head_tpl_dir.path()).unwrap();
        ref1 = hw.write_chunk(1, 1000, 2000, &encoded_xor, false).unwrap();
        hw.write_chunk(2, 1000, 1000, &encoded_hist, true).unwrap();
        hw.close().unwrap();
    }
    let head_tpl_bytes = std::fs::read(head_chunk_file_path(head_tpl_dir.path(), 1)).unwrap();

    let blk_tpl_dir = tempfile::tempdir().unwrap();
    let blk_ref: u64;
    {
        let mut bw = BlockChunksWriter::open(blk_tpl_dir.path()).unwrap();
        let mut metas = vec![
            ChunkMeta {
                ref_id: 0,
                min_time: 1000,
                max_time: 2000,
                chunk: Some(encoded_xor.clone()),
            },
            ChunkMeta {
                ref_id: 0,
                min_time: 1000,
                max_time: 1000,
                chunk: Some(encoded_hist.clone()),
            },
        ];
        bw.write_chunks(&mut metas).unwrap();
        blk_ref = metas[0].ref_id;
        bw.close().unwrap();
    }
    let blk_tpl_bytes =
        std::fs::read(mimir_tsdb::chunks_block::block_chunk_file_path(blk_tpl_dir.path(), 1))
            .unwrap();

    let cases = case_count(256);
    let work_dir = tempfile::tempdir().unwrap();
    for iter in 0..cases {
        // Mutate HeadChunks file
        let h_bytes = match iter % 4 {
            0 => rng.gen_bytes(128),
            1 => rng.mutate_bytes(&head_tpl_bytes),
            2 => {
                // Mutate uvarint data_len to overflow / huge value after 8-byte header + 25-byte meta
                let mut b = head_tpl_bytes.clone();
                if b.len() > 40 {
                    for x in &mut b[33..38] {
                        *x = 0xff;
                    }
                }
                b
            }
            _ => {
                // Corrupt CRC at end of first chunk
                let mut b = head_tpl_bytes.clone();
                let last = b.len() - 1;
                b[last] ^= 0x55;
                b
            }
        };
        let h_dir = work_dir.path().join("hc");
        let _ = std::fs::create_dir_all(&h_dir);
        std::fs::write(head_chunk_file_path(&h_dir, 1), &h_bytes).unwrap();
        if let Ok(r) = HeadChunksReader::open_no_repair(&h_dir) {
            let _ = r.chunk(ref1);
            let _ = r.chunk(ChunkDiskMapperRef::new(99, 999999));
            let _ = r.iterate_all_chunks(|_| Ok(()));
        }
        let _ = HeadChunksReader::open(&h_dir);

        // Mutate BlockChunks file
        let b_bytes = match iter % 3 {
            0 => rng.gen_bytes(128),
            1 => rng.mutate_bytes(&blk_tpl_bytes),
            _ => {
                let mut b = blk_tpl_bytes.clone();
                if b.len() > 16 {
                    b[8..13].copy_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);
                }
                b
            }
        };
        let b_dir = work_dir.path().join("bc");
        let _ = std::fs::create_dir_all(&b_dir);
        std::fs::write(
            mimir_tsdb::chunks_block::block_chunk_file_path(&b_dir, 1),
            &b_bytes,
        )
        .unwrap();
        if let Ok(r) = BlockChunksReader::open(&b_dir) {
            let _ = r.chunk(blk_ref);
            let _ = r.chunk(BlockChunkRef::new(0, u32::MAX).0);
            let _ = r.chunk(BlockChunkRef::new(5, 0).0);
        }
    }
}

// =================================================================================================
// 8. mimir_tsdb::index::IndexReader
// =================================================================================================

fn recompute_decbuf_crc(buf: &mut [u8], off: usize) {
    if off + 8 > buf.len() {
        return;
    }
    let len = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
    let payload_start = off + 4;
    if let Some(payload_end) = payload_start.checked_add(len) {
        if payload_end + 4 <= buf.len() {
            let crc = crc32c::crc32c(&buf[payload_start..payload_end]);
            buf[payload_end..payload_end + 4].copy_from_slice(&crc.to_be_bytes());
        }
    }
}

#[test]
fn fuzz_08_tsdb_index_reader() {
    let mut rng = Rng::new(0x08_5eed_cafe_8008);

    // Build a valid TSDB v2 index file as mutation template
    let tpl_dir = tempfile::tempdir().unwrap();
    let tpl_path = tpl_dir.path().join(INDEX_FILENAME);
    {
        let mut w = IndexWriter::new(&tpl_path).unwrap();
        let syms = [
            "",
            "__name__",
            "api",
            "db",
            "http_requests",
            "job",
            "up",
        ];
        for s in syms {
            w.add_symbol(s).unwrap();
        }
        let l1 = Labels::from_pairs([("__name__", "http_requests"), ("job", "api")]);
        let l2 = Labels::from_pairs([("__name__", "up"), ("job", "db")]);
        w.add_series(
            0,
            &l1,
            &[ChunkMeta {
                ref_id: 8,
                min_time: 1000,
                max_time: 2000,
                chunk: None,
            }],
        )
        .unwrap();
        w.add_series(
            1,
            &l2,
            &[ChunkMeta {
                ref_id: 40,
                min_time: 1000,
                max_time: 3000,
                chunk: None,
            }],
        )
        .unwrap();
        w.close().unwrap();
    }
    let tpl_bytes = std::fs::read(&tpl_path).unwrap();
    let tpl_toc = IndexToc::parse(&tpl_bytes).unwrap();

    let work_dir = tempfile::tempdir().unwrap();
    let work_path = work_dir.path().join(INDEX_FILENAME);
    let cases = case_count(256);

    for iter in 0..cases {
        let buf = match iter % 5 {
            0 => rng.gen_bytes(128),
            1 => rng.mutate_bytes(&tpl_bytes),
            2 => {
                // Corrupt TOC offsets with valid TOC CRC
                let mut b = tpl_bytes.clone();
                let toc_off = b.len() - INDEX_TOC_LEN;
                let field_off = toc_off + rng.gen_range(0, 6) * 8;
                let bogus = rng.next_u64();
                b[field_off..field_off + 8].copy_from_slice(&bogus.to_be_bytes());
                let crc = crc32c::crc32c(&b[toc_off..toc_off + 48]);
                b[toc_off + 48..toc_off + 52].copy_from_slice(&crc.to_be_bytes());
                b
            }
            3 => {
                // Corrupt symbols or postings_table payload WITH recomputed section CRC
                let mut b = tpl_bytes.clone();
                let sec_off = if rng.gen_bool() {
                    tpl_toc.symbols as usize
                } else {
                    tpl_toc.postings_table as usize
                };
                if sec_off + 12 < b.len() {
                    let len = u32::from_be_bytes(b[sec_off..sec_off + 4].try_into().unwrap()) as usize;
                    if len > 0 && sec_off + 4 + len <= b.len() {
                        let mut_pos = sec_off + 4 + rng.gen_range(0, len);
                        b[mut_pos] ^= 0xff;
                        recompute_decbuf_crc(&mut b, sec_off);
                    }
                }
                b
            }
            _ => {
                // Corrupt series / postings section bytes
                let mut b = tpl_bytes.clone();
                let lo = tpl_toc.series as usize;
                let hi = (tpl_toc.postings_table as usize).max(lo + 1).min(b.len());
                if lo < hi {
                    let pos = rng.gen_range(lo, hi);
                    b[pos] ^= 0x7f;
                }
                b
            }
        };

        let _ = IndexToc::parse(&buf);
        let mut f = std::fs::File::create(&work_path).unwrap();
        f.write_all(&buf).unwrap();
        drop(f);

        if let Ok(r) = IndexReader::open(&work_path) {
            let _ = r.symbols();
            let _ = r.lookup_symbol(rng.next_u32());
            let _ = r.label_names();
            let _ = r.label_values("job");
            let _ = r.postings_for_all_values("__name__");
            if let Ok(postings) = r.postings("", &[""]) {
                for id in postings.into_iter().take(16) {
                    let _ = r.series(id);
                    let _ = r.series_labels(id);
                }
            }
            let _ = r.series(rng.next_u64());
        }
    }
}

// =================================================================================================
// 9. ChunkEnc iterators (Xor, Xor2, Histogram, FloatHistogram, HistogramSt, FloatHistogramSt)
// =================================================================================================

struct SampleChunks {
    xor: Vec<u8>,
    xor2: Vec<u8>,
    hist: Vec<u8>,
    fhist: Vec<u8>,
    hst: Vec<u8>,
    fhst: Vec<u8>,
}

fn build_sample_chunks() -> SampleChunks {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, 1_000, 1.25).unwrap();
        app.append(0, 2_000, 2.5).unwrap();
        app.append(0, 3_000, f64::from_bits(0x7ff0_0000_0000_0002))
            .unwrap();
    }
    let mut x2 = Xor2Chunk::new();
    {
        let mut app = x2.appender().unwrap();
        app.append(500, 1_000, 1.25).unwrap();
        app.append(500, 2_000, 2.5).unwrap();
        app.append(1_500, 3_000, 3.75).unwrap();
    }
    let h = Histogram {
        count: 7,
        sum: 12.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 2,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![3, -1],
        negative_spans: vec![HistogramSpan {
            offset: -1,
            length: 1,
        }],
        negative_buckets: vec![1],
        ..Default::default()
    };
    let mut hc = HistogramChunk::new();
    {
        let mut app = hc.appender().unwrap();
        app.append_histogram(None, 0, 1_000, &h, true).unwrap();
        app.append_histogram(None, 0, 2_000, &h, true).unwrap();
    }
    let mut hst = HistogramStChunk::new();
    {
        let mut app = hst.appender().unwrap();
        app.append_histogram(None, 500, 1_000, &h, true).unwrap();
        app.append_histogram(None, 500, 2_000, &h, true).unwrap();
    }
    let fh = FloatHistogram {
        count: 7.5,
        sum: 12.5,
        schema: -53,
        zero_threshold: 0.001,
        zero_count: 1.5,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![2.5, 3.5],
        custom_values: vec![0.1, 1.0],
        ..Default::default()
    };
    let mut fhc = FloatHistogramChunk::new();
    {
        let mut app = fhc.appender().unwrap();
        app.append_float_histogram(None, 0, 1_000, &fh, true).unwrap();
        app.append_float_histogram(None, 0, 2_000, &fh, true).unwrap();
    }
    let mut fhst = FloatHistogramStChunk::new();
    {
        let mut app = fhst.appender().unwrap();
        app.append_float_histogram(None, 500, 1_000, &fh, true).unwrap();
        app.append_float_histogram(None, 500, 2_000, &fh, true).unwrap();
    }

    SampleChunks {
        xor: xc.into_bytes(),
        xor2: x2.into_bytes(),
        hist: hc.into_bytes(),
        fhist: fhc.into_bytes(),
        hst: hst.into_bytes(),
        fhst: fhst.into_bytes(),
    }
}

#[test]
fn fuzz_09_tsdb_chunkenc_iterators_all_encodings() {
    let mut rng = Rng::new(0x09_5eed_cafe_9009);
    let SampleChunks {
        xor: t_xor,
        xor2: t_xor2,
        hist: t_hist,
        fhist: t_fhist,
        hst: t_hst,
        fhst: t_fhst,
    } = build_sample_chunks();
    let cases = case_count(512);

    let gen_chunk_input = |rng: &mut Rng, tpl: &[u8], iter: usize| -> Vec<u8> {
        match iter % 4 {
            0 => rng.gen_bytes(192),
            1 => rng.mutate_bytes(tpl),
            2 => {
                // Header with large num_total (e.g. u16::MAX) and short random bit body
                let mut b = rng.gen_bytes(48);
                if b.len() < 4 {
                    b.resize(4, 0);
                }
                b[0..2].copy_from_slice(&u16::MAX.to_be_bytes());
                b
            }
            _ => {
                // Truncate template
                let cut = rng.gen_range(0, tpl.len() + 1);
                tpl[..cut].to_vec()
            }
        }
    };

    for iter in 0..cases {
        let bx = gen_chunk_input(&mut rng, &t_xor, iter);
        let mut ix = XorIterator::new(&bx);
        for _ in 0..1024 {
            let _ = ix.at();
            match ix.next_sample() {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        let mut ix_seek = XorIterator::new(&bx);
        let _ = ix_seek.seek(1_500);

        let bx2 = gen_chunk_input(&mut rng, &t_xor2, iter);
        let mut ix2 = Xor2Iterator::new(&bx2);
        for _ in 0..1024 {
            let _ = ix2.at();
            let _ = ix2.at_st();
            match ix2.next_sample() {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        let mut ix2_seek = Xor2Iterator::new(&bx2);
        let _ = ix2_seek.seek(1_500);

        let bh = gen_chunk_input(&mut rng, &t_hist, iter);
        let mut ih = HistogramIterator::new(&bh);
        for _ in 0..1024 {
            match ih.next_histogram() {
                Ok(Some(_)) => {
                    let _ = ih.at_histogram();
                    let _ = ih.at_float_histogram();
                }
                Ok(None) | Err(_) => break,
            }
        }
        let mut ih_seek = HistogramIterator::new(&bh);
        let _ = ih_seek.seek(1_500);

        let bfh = gen_chunk_input(&mut rng, &t_fhist, iter);
        let mut ifh = FloatHistogramIterator::new(&bfh);
        for _ in 0..1024 {
            match ifh.next_float_histogram() {
                Ok(Some(_)) => {
                    let _ = ifh.at_float_histogram();
                }
                Ok(None) | Err(_) => break,
            }
        }
        let mut ifh_seek = FloatHistogramIterator::new(&bfh);
        let _ = ifh_seek.seek(1_500);

        let bhst = gen_chunk_input(&mut rng, &t_hst, iter);
        let mut ihst = HistogramStIterator::new(&bhst);
        for _ in 0..1024 {
            let _ = ihst.at_st();
            match ihst.next_histogram() {
                Ok(Some(_)) => {
                    let _ = ihst.at_histogram();
                    let _ = ihst.at_float_histogram();
                }
                Ok(None) | Err(_) => break,
            }
        }

        let bfhst = gen_chunk_input(&mut rng, &t_fhst, iter);
        let mut ifhst = FloatHistogramStIterator::new(&bfhst);
        for _ in 0..1024 {
            let _ = ifhst.at_st();
            match ifhst.next_float_histogram() {
                Ok(Some(_)) => {
                    let _ = ifhst.at_float_histogram();
                }
                Ok(None) | Err(_) => break,
            }
        }
    }
}

// =================================================================================================
// 10. Memberlist MpReader & Snappy KV envelope decoders (mimir-ring)
// =================================================================================================

#[test]
fn fuzz_10_memberlist_mp_reader_and_snappy_kv_envelope() {
    let mut rng = Rng::new(0x0a_5eed_cafe_a00a);

    // Valid memberlist wire templates
    let ping_enc = encode_ping(&Ping {
        seq_no: 42,
        node: "n1".to_string(),
        source_addr: vec![127, 0, 0, 1],
        source_port: 7946,
        source_node: "n0".to_string(),
    });
    let iping_enc = encode_indirect_ping(&IndirectPingReq {
        seq_no: 7,
        target: vec![10, 0, 0, 1],
        port: 7946,
        node: "n2".to_string(),
        nack: true,
        source_addr: vec![10, 0, 0, 2],
        source_port: 7946,
        source_node: "n0".to_string(),
    });
    let ack_enc = encode_ack_resp(&AckResp {
        seq_no: 1,
        payload: Some(b"ok".to_vec()),
    });
    let nack_enc = encode_nack_resp(&NackResp { seq_no: 2 });
    let err_enc = encode_err_resp(&ErrResp {
        error: "err".to_string(),
    });
    let alive_enc = encode_alive(&Alive {
        incarnation: 3,
        node: "n1".to_string(),
        addr: vec![127, 0, 0, 1],
        port: 7946,
        meta: Some(vec![1, 2]),
        vsn: vec![1, 5, 2, 0, 0, 0],
    });
    let dead_enc = encode_dead(&Dead {
        incarnation: 4,
        node: "n1".to_string(),
        from: "n0".to_string(),
    });
    let pp_enc = encode_push_pull(
        &[PushNodeState {
            name: "n1".to_string(),
            addr: vec![127, 0, 0, 1],
            port: 7946,
            meta: None,
            incarnation: 1,
            state: NodeStateType::Alive,
            vsn: vec![1, 5, 2],
        }],
        b"user-kv-state",
        true,
    );
    let umsg_enc = encode_user_msg_stream(b"hello-msg");
    let comp_enc = make_compound_message(&[&ping_enc, &ack_enc]);
    let lzw_enc = compress_payload(MlComp::Lzw, &ping_enc).unwrap();
    let crc_enc = wrap_crc32(&ping_enc);
    let lbl_enc = add_label_header(&ping_enc, "cluster-a").unwrap();

    let templates: [&[u8]; 13] = [
        &ping_enc[1..],
        &iping_enc[1..],
        &ack_enc[1..],
        &nack_enc[1..],
        &err_enc[1..],
        &alive_enc[1..],
        &dead_enc[1..],
        &pp_enc[1..],
        &umsg_enc[1..],
        &comp_enc[1..],
        &lzw_enc[1..],
        &crc_enc,
        &lbl_enc,
    ];

    let ring_codec = ring_desc_codec();
    let pring_codec = partition_ring_desc_codec();
    let ring_snappy = ProtoSnappyCodec::<mimir_proto::ringpb::Desc>::new("ringDesc");
    let pring_snappy = ProtoSnappyCodec::<PartitionRingDesc>::new("partitionRingDesc");

    let mut sample_ring = mimir_proto::ringpb::Desc::default();
    sample_ring.ingesters.insert(
        "ing-0".to_string(),
        InstanceDesc {
            addr: "127.0.0.1:9095".to_string(),
            timestamp: 1_700_000_000,
            state: InstanceState::Active as i32,
            tokens: vec![1, 1000, 2000],
            zone: "zone-a".to_string(),
            registered_timestamp: 1_700_000_000,
            read_only: false,
            read_only_updated_timestamp: 0,
            ..Default::default()
        },
    );
    let encoded_ring = ring_snappy.encode_msg(&sample_ring).unwrap();

    let mut sample_pring = PartitionRingDesc::default();
    sample_pring.partitions.insert(
        0,
        PartitionDesc {
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 1_700_000_000,
            tokens: vec![10, 20],
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
            ..Default::default()
        },
    );
    let encoded_pring = pring_snappy.encode_msg(&sample_pring).unwrap();

    let sample_kv_pair = KeyValuePair {
        key: "collectors/ring".to_string(),
        value: encoded_ring.clone().into(),
        codec: "ringDesc".to_string(),
        deleted: false,
        ..Default::default()
    }
    .encode_to_vec();

    let cases = case_count(512);
    for iter in 0..cases {
        let mp_buf = match iter % 5 {
            0 => rng.gen_bytes(160),
            1 => {
                // Deeply nested MessagePack fixarray/fixmap (must reject without stack overflow)
                let depth = rng.gen_range(32, 160);
                let marker = if rng.gen_bool() { 0x91u8 } else { 0x81u8 };
                let mut v = vec![marker; depth];
                v.push(0x00);
                // Wrap in valid 1-key map with unknown key to trigger skip_value()
                let mut wrapped = vec![0x81, 0xa1, b'X'];
                wrapped.extend_from_slice(&v);
                wrapped
            }
            2 => {
                // Malicious ext32 / arr32 / map32 / str32 / bin32 bomb lengths
                let tags = [0xc9u8, 0xdd, 0xdf, 0xdb, 0xc6];
                let tag = tags[rng.gen_range(0, tags.len())];
                let mut v = vec![0x81, 0xa1, b'X', tag];
                v.extend_from_slice(&u32::MAX.to_be_bytes());
                v.extend_from_slice(&rng.gen_bytes(8));
                v
            }
            3 => rng.mutate_bytes(templates[iter % templates.len()]),
            _ => {
                // Random push_pull header with huge Nodes / UserStateLen
                let mut v = vec![0x82];
                v.extend_from_slice(&[0xa5]);
                v.extend_from_slice(b"Nodes");
                v.extend_from_slice(&[0xce]);
                v.extend_from_slice(&u32::MAX.to_be_bytes());
                v.extend_from_slice(&[0xac]);
                v.extend_from_slice(b"UserStateLen");
                v.extend_from_slice(&[0xce]);
                v.extend_from_slice(&u32::MAX.to_be_bytes());
                v
            }
        };

        let _ = decode_ping(&mp_buf);
        let _ = decode_indirect_ping(&mp_buf);
        let _ = decode_ack_resp(&mp_buf);
        let _ = decode_nack_resp(&mp_buf);
        let _ = decode_err_resp(&mp_buf);
        let _ = decode_alive(&mp_buf);
        let _ = decode_dead(&mp_buf);
        let _ = decode_compressed_payload(&mp_buf);
        let _ = decode_push_pull(&mp_buf);
        let _ = decode_user_msg_stream(&mp_buf);
        let _ = decode_compound_message(&mp_buf);
        let _ = decompress_buffer(MlComp::Lzw, &mp_buf);
        let _ = unwrap_crc32(&mp_buf);
        let _ = remove_label_header(&mp_buf);

        // Snappy KV envelope & KeyValuePair protobuf fuzzing
        let kv_buf = match iter % 4 {
            0 => rng.gen_bytes(160),
            1 => rng.mutate_bytes(&encoded_ring),
            2 => rng.mutate_bytes(&encoded_pring),
            _ => rng.mutate_bytes(&sample_kv_pair),
        };
        let _ = KeyValuePair::decode(&kv_buf[..]);
        let _ = ring_codec.decode(&kv_buf);
        let _ = pring_codec.decode(&kv_buf);
        let _ = ring_snappy.decode_msg(&kv_buf);
        let _ = pring_snappy.decode_msg(&kv_buf);
    }

    // Roundtrip sanity on Snappy KV envelopes
    assert_eq!(ring_snappy.decode_msg(&encoded_ring).unwrap(), sample_ring);
    assert_eq!(
        pring_snappy.decode_msg(&encoded_pring).unwrap(),
        sample_pring
    );
}
