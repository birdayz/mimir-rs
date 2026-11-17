use std::hint::black_box;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mimir_proto::cortexpb;
use mimir_proto::query_stream_enc::{QueryStreamBatchBuilder, RawChunk};
use mimir_proto::s2;
use mimir_proto::snappy;
use mimir_proto::symbols::{v2_common_symbols, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{
    decode_classic_stream, decode_rw2_stream, encode_write_request_rw2_bytes, DecodeOptions,
    Rw2ResolvedScratch, WriteRequestScratch,
};
use prost::Message as _;

fn make_write_request(num_series: usize) -> cortexpb::WriteRequest {
    let metrics = [
        "http_server_requests_total",
        "http_client_requests_total",
        "grpc_server_handled_total",
        "container_cpu_usage_seconds_total",
        "process_network_bytes_total",
    ];
    let namespaces = [
        "prod-api",
        "prod-checkout",
        "prod-auth",
        "prod-billing",
        "prod-search",
    ];
    let containers = ["app", "envoy", "otel-sidecar", "worker"];
    let statuses = ["200", "201", "400", "404", "500"];

    let mut timeseries = Vec::with_capacity(num_series);
    for k in 0..num_series {
        let metric = metrics[k % metrics.len()];
        let ns = namespaces[(k / 5) % namespaces.len()];
        let pod = format!("svc-{}", k / 25);
        let container = containers[k % containers.len()];
        let instance = format!("10.42.{}.{}:8080", (k >> 8) & 0xff, k & 0xff);
        let job = format!("{ns}/service");
        let status_code = statuses[k % statuses.len()];

        timeseries.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", metric),
                cortexpb::LabelPair::from_static("cluster", "prod-us-east-1"),
                cortexpb::LabelPair::from_static("container", container),
                cortexpb::LabelPair::from_strs("instance", &instance),
                cortexpb::LabelPair::from_strs("job", &job),
                cortexpb::LabelPair::from_static("namespace", ns),
                cortexpb::LabelPair::from_strs("pod", &pod),
                cortexpb::LabelPair::from_static("status_code", status_code),
            ],
            samples: vec![cortexpb::Sample {
                value: (k as f64) * 1.5,
                timestamp_ms: 1_700_000_000_000 + (k as i64),
            }],
            exemplars: Vec::new(),
            histograms: Vec::new(),
            created_timestamp: 0,
        });
    }
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries,
        ..Default::default()
    }
}

fn encode_stringlabels(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
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

fn make_s2_stream(payload: &[u8]) -> Vec<u8> {
    // Emit S2 stream identifier ("S2sTwO") + Snappy-compatible compressed chunks (<= 64 KiB each)
    let mut out = Vec::with_capacity(payload.len() / 2 + 32);
    out.extend_from_slice(b"\xff\x06\x00\x00S2sTwO");
    let mut enc = snap::raw::Encoder::new();
    for block in payload.chunks(65_536) {
        let compressed = enc.compress_vec(block).expect("snappy raw compress");
        let crc = s2::masked_crc32c(block);
        let chunk_len = 4 + compressed.len();
        out.push(0x00);
        out.push((chunk_len & 0xff) as u8);
        out.push(((chunk_len >> 8) & 0xff) as u8);
        out.push(((chunk_len >> 16) & 0xff) as u8);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&compressed);
    }
    out
}

fn bench_decode_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codecs/decode_stream");
    let num_series = 1_000usize;
    let req = make_write_request(num_series);
    let classic_bytes = req.encode_to_vec();
    let mut rw2_bytes = Vec::new();
    encode_write_request_rw2_bytes(
        &req,
        V2_RECORD_SYMBOL_OFFSET,
        Some(v2_common_symbols()),
        &mut rw2_bytes,
    )
    .expect("encode rw2");

    group.throughput(Throughput::Elements(num_series as u64));

    group.bench_function("decode_classic_stream_1k", |b| {
        let mut scratch = WriteRequestScratch::new();
        let mut label_arena = Vec::with_capacity(128 * 1024);
        let mut samples_arena = Vec::with_capacity(num_series);
        b.iter(|| {
            label_arena.clear();
            samples_arena.clear();
            let mut sum = 0u64;
            let hdr = decode_classic_stream(
                black_box(&classic_bytes),
                DecodeOptions::classic(),
                &mut scratch,
                &mut label_arena,
                &mut samples_arena,
                |s| {
                    sum = sum.wrapping_add(s.prom_hash).wrapping_add(s.samples_range.len() as u64);
                },
            )
            .expect("decode_classic_stream");
            black_box((hdr, sum));
        });
    });

    group.bench_function("decode_rw2_stream_kafka_v2_1k", |b| {
        let mut scratch = WriteRequestScratch::new();
        let mut resolved_scratch = Rw2ResolvedScratch::new();
        let mut label_arena = Vec::with_capacity(128 * 1024);
        let mut samples_arena = Vec::with_capacity(num_series);
        b.iter(|| {
            label_arena.clear();
            samples_arena.clear();
            let mut meta_out = Vec::new();
            let mut count = 0usize;
            let hdr = decode_rw2_stream(
                black_box(&rw2_bytes),
                DecodeOptions::kafka_v2(),
                &mut scratch,
                &mut resolved_scratch,
                &mut label_arena,
                &mut samples_arena,
                &mut meta_out,
                |s| {
                    count += s.encoded_labels.len() + s.samples_range.len();
                },
            )
            .expect("decode_rw2_stream");
            black_box((hdr, count));
        });
    });

    group.finish();
}

fn bench_grpc_frame_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codecs/grpc_frame_compression");
    let req = make_write_request(1_000);
    let payload = req.encode_to_vec();
    let snappy_framed = snappy::encode_grpc_frame(&payload, true).expect("snappy encode");
    let s2_stream = make_s2_stream(&payload);
    let mut s2_grpc_framed = Vec::with_capacity(5 + s2_stream.len());
    s2_grpc_framed.push(1);
    s2_grpc_framed.extend_from_slice(&(s2_stream.len() as u32).to_be_bytes());
    s2_grpc_framed.extend_from_slice(&s2_stream);
    let s2_grpc_framed = Bytes::from(s2_grpc_framed);

    group.throughput(Throughput::Bytes(payload.len() as u64));

    group.bench_function("snappy_grpc_frame_compress", |b| {
        b.iter(|| {
            let out = snappy::encode_grpc_frame(black_box(&payload), true).expect("encode");
            black_box(out);
        });
    });

    group.bench_function("snappy_grpc_frame_decompress", |b| {
        b.iter(|| {
            let out = snappy::decode_grpc_frame(black_box(&snappy_framed)).expect("decode");
            black_box(out);
        });
    });

    group.bench_function("snappy_grpc_frame_roundtrip", |b| {
        b.iter(|| {
            let enc = snappy::encode_grpc_frame(black_box(&payload), true).expect("encode");
            let dec = snappy::decode_grpc_frame(&enc).expect("decode");
            black_box(dec);
        });
    });

    group.bench_function("s2_grpc_frame_decompress", |b| {
        let mut out = Vec::with_capacity(payload.len());
        b.iter(|| {
            out.clear();
            let raw = &s2_grpc_framed[5..];
            s2::decompress_framed_into(black_box(raw), &mut out, payload.len() + 1024)
                .expect("s2 decode");
            black_box(out.len());
        });
    });

    group.bench_function("s2_grpc_frame_compress_and_decompress", |b| {
        let mut out = Vec::with_capacity(payload.len());
        b.iter(|| {
            let s2_payload = make_s2_stream(black_box(&payload));
            out.clear();
            s2::decompress_framed_into(&s2_payload, &mut out, payload.len() + 1024)
                .expect("s2 decode");
            black_box(out.len());
        });
    });

    group.finish();
}

fn bench_query_stream_batch_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codecs/query_stream_batch_builder");

    for &series_count in &[1_000usize, 25_000usize] {
        let series_labels: Vec<Vec<u8>> = (0..series_count)
            .map(|i| {
                let pod = format!("api-pod-{i:05}");
                let instance = format!("10.10.{}.{}:8080", (i >> 8) & 0xff, i & 0xff);
                encode_stringlabels(&[
                    (b"__name__", b"http_requests_total"),
                    (b"cluster", b"prod-us-east-1"),
                    (b"instance", instance.as_bytes()),
                    (b"job", b"api-server"),
                    (b"namespace", b"prod-api"),
                    (b"pod", pod.as_bytes()),
                ])
            })
            .collect();
        let chunk_bytes = vec![0xabu8; 128];
        let raw_chunks = [
            RawChunk {
                min_timestamp_ms: 1_700_000_000_000,
                max_timestamp_ms: 1_700_000_600_000,
                encoding: 1,
                data: &chunk_bytes,
            },
            RawChunk {
                min_timestamp_ms: 1_700_000_600_001,
                max_timestamp_ms: 1_700_001_200_000,
                encoding: 1,
                data: &chunk_bytes,
            },
        ];

        group.throughput(Throughput::Elements(series_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{series_count}_series")),
            &series_count,
            |b, _| {
                let mut builder = QueryStreamBatchBuilder::with_capacity(series_count * 384);
                b.iter(|| {
                    builder.clear();
                    for sl in &series_labels {
                        builder
                            .push_series_stringlabels(black_box(sl), raw_chunks.len() as i64)
                            .expect("push_series");
                    }
                    builder.set_end_of_series_stream();
                    for idx in 0..series_labels.len() {
                        builder.push_series_chunks(idx as u64, black_box(&raw_chunks));
                    }
                    let bytes = builder.finish();
                    black_box(bytes);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_streams,
    bench_grpc_frame_compression,
    bench_query_stream_batch_builder
);
criterion_main!(benches);
