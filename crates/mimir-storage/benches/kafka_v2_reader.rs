use std::hint::black_box;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use mimir_proto::cortexpb;
use mimir_proto::zerocopy::WriteRequestScratch;
use mimir_storage::ingest::compression::{compress_record_batch, decompress_record_batch};
use mimir_storage::ingest::kafka_client::{FetchedRecord, KafkaClient};
use mimir_storage::ingest::reader::decode_fetched_record;
use mimir_storage::ingest::version::{
    RecordSerializer, RecordVersion, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
};

fn build_write_request(num_series: usize, base_ts: i64) -> cortexpb::WriteRequest {
    let metrics = [
        "http_server_requests_total",
        "http_client_requests_total",
        "grpc_server_handled_total",
        "container_cpu_usage_seconds_total",
    ];
    let namespaces = ["prod-api", "prod-checkout", "prod-auth", "prod-billing"];
    let mut timeseries = Vec::with_capacity(num_series);
    for k in 0..num_series {
        let ns = namespaces[k % namespaces.len()];
        let pod = format!("pod-{}", k / 10);
        let instance = format!("10.42.{}.{}:8080", (k >> 8) & 0xff, k & 0xff);
        timeseries.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", metrics[k % metrics.len()]),
                cortexpb::LabelPair::from_static("cluster", "prod-us-east-1"),
                cortexpb::LabelPair::from_strs("instance", &instance),
                cortexpb::LabelPair::from_static("job", "api-service"),
                cortexpb::LabelPair::from_static("namespace", ns),
                cortexpb::LabelPair::from_strs("pod", &pod),
                cortexpb::LabelPair::from_static("status_code", "200"),
            ],
            samples: vec![cortexpb::Sample {
                value: (k as f64) * 1.25,
                timestamp_ms: base_ts + k as i64,
            }],
            ..Default::default()
        });
    }
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries,
        ..Default::default()
    }
}

fn build_v2_record_batch(
    records_per_batch: usize,
    series_per_record: usize,
    compression: Compression,
) -> (Bytes, Bytes, Vec<FetchedRecord>, usize) {
    let serializer = RecordSerializer::new(RecordVersion::V2);
    let base_ts = 1_700_000_000_000i64;
    let mut kafka_records = Vec::with_capacity(records_per_batch);
    let mut raw_records_section = BytesMut::new();

    for idx in 0..records_per_batch {
        let req = build_write_request(series_per_record, base_ts + (idx as i64) * 1_000);
        let (serialized, _) = serializer
            .to_records(
                "ingest",
                0,
                "tenant-bench",
                &req,
                MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
            )
            .expect("RecordSerializer v2");
        let rec = &serialized[0];
        let mut headers = IndexMap::new();
        for (hk, hv) in &rec.headers {
            headers.insert(StrBytes::from_string(hk.clone()), Some(hv.clone()));
        }
        raw_records_section.extend_from_slice(&rec.value);
        kafka_records.push(Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: idx as i64,
            sequence: idx as i32,
            timestamp: base_ts + (idx as i64) * 100,
            key: Some(rec.key.clone()),
            value: Some(rec.value.clone()),
            headers,
        });
    }

    let mut frame_buf = BytesMut::new();
    let opts = RecordEncodeOptions {
        version: 2,
        compression,
    };
    RecordBatchEncoder::encode_with_custom_compression(
        &mut frame_buf,
        kafka_records.iter(),
        &opts,
        Some(compress_record_batch),
    )
    .expect("RecordBatchEncoder");
    let frame = frame_buf.freeze();

    let mut compressed_payload_buf = BytesMut::new();
    let mut src_mut = BytesMut::from(raw_records_section.as_ref());
    compress_record_batch(&mut src_mut, &mut compressed_payload_buf, compression)
        .expect("compress_record_batch");

    let client = KafkaClient::new(
        vec!["127.0.0.1:9092".to_string()],
        "bench-client",
        "",
        Duration::from_secs(5),
        Duration::from_secs(5),
    )
    .expect("KafkaClient::new");
    let (decoded_records, _, _) = client
        .decode_fetched_partition(frame.clone(), 0)
        .expect("initial decode");

    (
        frame,
        compressed_payload_buf.freeze(),
        decoded_records,
        raw_records_section.len(),
    )
}

fn bench_kafka_v2_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_v2_reader");

    // 20 Kafka records * 100 series/record = 2,000 series per batch
    let records_per_batch = 20usize;
    let series_per_record = 100usize;
    let total_series = records_per_batch * series_per_record;

    let (frame_none, _, decoded_records, raw_bytes_len) =
        build_v2_record_batch(records_per_batch, series_per_record, Compression::None);
    let (frame_snappy, comp_snappy, _, _) =
        build_v2_record_batch(records_per_batch, series_per_record, Compression::Snappy);
    let (frame_zstd, comp_zstd, _, _) =
        build_v2_record_batch(records_per_batch, series_per_record, Compression::Zstd);

    let client = KafkaClient::new(
        vec!["127.0.0.1:9092".to_string()],
        "bench-client",
        "",
        Duration::from_secs(5),
        Duration::from_secs(5),
    )
    .expect("KafkaClient::new");

    group.throughput(Throughput::Bytes(raw_bytes_len as u64));

    group.bench_function("record_batch_v2_fast_frame_decode_none", |b| {
        b.iter(|| {
            let (recs, hwm, _) = client
                .decode_fetched_partition(black_box(frame_none.clone()), 0)
                .expect("decode none");
            black_box((recs.len(), hwm));
        });
    });

    group.bench_function("record_batch_v2_fast_frame_decode_snappy", |b| {
        b.iter(|| {
            let (recs, hwm, _) = client
                .decode_fetched_partition(black_box(frame_snappy.clone()), 0)
                .expect("decode snappy");
            black_box((recs.len(), hwm));
        });
    });

    group.bench_function("record_batch_v2_fast_frame_decode_zstd", |b| {
        b.iter(|| {
            let (recs, hwm, _) = client
                .decode_fetched_partition(black_box(frame_zstd.clone()), 0)
                .expect("decode zstd");
            black_box((recs.len(), hwm));
        });
    });

    group.bench_function("snappy_batch_decompress", |b| {
        b.iter(|| {
            let mut slice = black_box(comp_snappy.clone());
            let out =
                decompress_record_batch(&mut slice, Compression::Snappy).expect("snappy decomp");
            black_box(out.len());
        });
    });

    group.bench_function("zstd_batch_decompress", |b| {
        b.iter(|| {
            let mut slice = black_box(comp_zstd.clone());
            let out = decompress_record_batch(&mut slice, Compression::Zstd).expect("zstd decomp");
            black_box(out.len());
        });
    });

    group.throughput(Throughput::Elements(total_series as u64));
    group.bench_function("rw2_v2_symbolised_batch_decode_2k_series", |b| {
        let mut scratch = WriteRequestScratch::new();
        b.iter(|| {
            let mut total = 0usize;
            for rec in &decoded_records {
                let dec = decode_fetched_record(black_box(rec), &mut scratch)
                    .expect("decode_fetched_record");
                total += dec.write_request.timeseries_len();
            }
            black_box(total);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_kafka_v2_reader);
criterion_main!(benches);
