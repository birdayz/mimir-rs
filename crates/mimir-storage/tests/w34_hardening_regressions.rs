use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;
use kafka_protocol::messages::{
    api_versions_response::ApiVersion,
    fetch_response::{FetchableTopicResponse, PartitionData},
    metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    },
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
    MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader,
    ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::ResponseError;
use mimir_config::{
    AzureConfig, BucketConfig, GCSConfig, GoDuration, KafkaConfig, S3BucketLookupType, S3Config,
    StorageBackendConfig, StringSliceCSV,
};
use mimir_storage::bucket::exthttp::parse_content_range;
use mimir_storage::bucket::s3::credentials::{split_credential_process_args, Expiry};
use mimir_storage::bucket::{
    BucketClient, BucketError, ObjectUploadOptions, ObjectVersion, UploadCondition,
};
use mimir_storage::ingest::compression::{
    decompress_record_batch, MAX_DECOMPRESSED_KAFKA_BATCH_BYTES,
};
use mimir_storage::ingest::fetcher::{ConcurrentFetchers, FetcherMetrics, RangeErrorPolicy};
use mimir_storage::ingest::kafka_client::{
    process_fetch_partition_batches, KafkaClient, KafkaClientError, KgoBrokerConn,
};
use mimir_storage::ingest::transport::{KafkaTransport, SaslAuthConfig};
use mimir_storage::ingest::writer::ingest_writer_retriable_err;
use mimir_storage::ingest::writer_client::{
    KafkaProducerClient, KafkaWriterClient, KafkaWriterClientOptions, WriterRecord,
};
use mimir_storage::ingest::{GenericOffsetReader, PartitionOffsetClient};
use mimir_storage::shipper::atomic_write_file;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn read_kafka_frame(rd: &mut (impl AsyncReadExt + Unpin)) -> Option<Bytes> {
    let mut len_buf = [0u8; 4];
    if rd.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    if rd.read_exact(&mut buf).await.is_err() {
        return None;
    }
    Some(Bytes::from(buf))
}

fn encode_kafka_response<R: Encodable>(
    api_key: ApiKey,
    api_ver: i16,
    corr_id: i32,
    resp: &R,
) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.put_u32(0);
    let mut hdr = ResponseHeader::default();
    hdr.correlation_id = corr_id;
    hdr.encode(&mut out, api_key.response_header_version(api_ver))
        .unwrap();
    resp.encode(&mut out, api_ver).unwrap();
    let frame_len = (out.len() - 4) as u32;
    out[0..4].copy_from_slice(&frame_len.to_be_bytes());
    out.to_vec()
}

fn write_uvarint(out: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        out.put_u8(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    out.put_u8(v as u8);
}

fn write_svarint(out: &mut BytesMut, v: i64) {
    let zz = ((v << 1) ^ (v >> 63)) as u64;
    write_uvarint(out, zz);
}

fn build_v2_batch_bytes(
    base_offset: i64,
    last_offset_delta: i32,
    attributes: i16,
    records: &[(&[u8], &[u8])],
) -> Bytes {
    let mut recs_buf = BytesMut::new();
    for (idx, (k, v)) in records.iter().enumerate() {
        let mut rec = BytesMut::new();
        rec.put_u8(0);
        rec.put_u8(0);
        let od = ((idx as i64) << 1) as u64;
        write_uvarint(&mut rec, od);
        write_svarint(&mut rec, k.len() as i64);
        rec.extend_from_slice(k);
        write_svarint(&mut rec, v.len() as i64);
        rec.extend_from_slice(v);
        write_svarint(&mut rec, 0);
        write_svarint(&mut recs_buf, rec.len() as i64);
        recs_buf.extend_from_slice(&rec);
    }

    let mut batch = BytesMut::new();
    batch.put_i64(base_offset);
    let batch_length = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + recs_buf.len();
    batch.put_i32(batch_length as i32);
    batch.put_i32(0);
    batch.put_u8(2);
    let crc_pos = batch.len();
    batch.put_u32(0);
    let crc_start = batch.len();
    batch.put_i16(attributes);
    batch.put_i32(last_offset_delta);
    batch.put_i64(1_700_000_000_000);
    batch.put_i64(1_700_000_000_000);
    batch.put_i64(-1);
    batch.put_i16(-1);
    batch.put_i32(-1);
    batch.put_i32(records.len() as i32);
    batch.extend_from_slice(&recs_buf);
    let crc = crc32c::crc32c(&batch[crc_start..]);
    batch[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    batch.freeze()
}

#[tokio::test]
async fn kafka_tombstones_discard_late_replies_without_poisoning_siblings_and_saturate_offsets() {
    let (client_io, mut server_io) = tokio::io::duplex(64 * 1024);
    let conn = KgoBrokerConn::from_duplex(client_io);

    let srv = tokio::spawn(async move {
        let frame1 = read_kafka_frame(&mut server_io).await.unwrap();
        let mut f1 = frame1;
        let hdr1 =
            RequestHeader::decode(&mut f1, ApiKey::Metadata.request_header_version(4)).unwrap();

        let frame2 = read_kafka_frame(&mut server_io).await.unwrap();
        let mut f2 = frame2;
        let hdr2 =
            RequestHeader::decode(&mut f2, ApiKey::Metadata.request_header_version(4)).unwrap();

        let mut meta = MetadataResponse::default();
        let mut b = MetadataResponseBroker::default();
        b.node_id = BrokerId(1);
        b.host = StrBytes::from_static_str("127.0.0.1");
        b.port = 9092;
        meta.brokers.push(b);

        let r1 = encode_kafka_response(ApiKey::Metadata, 4, hdr1.correlation_id, &meta);
        let r2 = encode_kafka_response(ApiKey::Metadata, 4, hdr2.correlation_id, &meta);
        server_io.write_all(&r1).await.unwrap();
        server_io.write_all(&r2).await.unwrap();
        server_io.flush().await.unwrap();
        server_io
    });

    let err1 = conn
        .request_metadata_with_timeout(Duration::from_millis(30))
        .await
        .unwrap_err();
    assert!(
        matches!(err1, KafkaClientError::RequestTimeout(_)),
        "expected timeout, got {err1:?}"
    );
    assert_eq!(conn.pending_len(), 1);

    let ok2 = conn
        .request_metadata_with_timeout(Duration::from_secs(2))
        .await
        .expect("sibling request must NOT be poisoned by late timed-out response");
    assert_eq!(ok2.brokers.len(), 1);
    assert_eq!(conn.pending_len(), 0);
    let _keep = srv.await.unwrap();

    let (client_io2, _server_io2) = tokio::io::duplex(64 * 1024);
    let conn2 = KgoBrokerConn::from_duplex(client_io2);
    for _ in 0..=65 {
        let _ = conn2
            .request_metadata_with_timeout(Duration::from_millis(1))
            .await;
    }
    assert!(
        conn2.is_dead(),
        "connection must retire once stale tombstones exceed cap"
    );

    let batch = build_v2_batch_bytes(i64::MAX - 2, 2, 0, &[(b"k", b"v")]);
    let (recs, next_off, end_off) = process_fetch_partition_batches(batch, i64::MAX - 2).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(end_off, Some(i64::MAX));
    assert_eq!(next_off, i64::MAX);
}

#[test]
fn compression_rejects_corrupt_5byte_snappy_xerial_and_14byte_zstd_without_huge_alloc() {
    let mut bad_snappy = Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x01]);
    let err = decompress_record_batch(
        &mut bad_snappy,
        kafka_protocol::records::Compression::Snappy,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("exceeds maximum expansion"),
        "expected expansion cap rejection, got: {err}"
    );

    let mut xerial = Vec::new();
    xerial.extend_from_slice(b"\x82SNAPPY\0");
    xerial.extend_from_slice(&1u32.to_be_bytes());
    xerial.extend_from_slice(&1u32.to_be_bytes());
    xerial.extend_from_slice(&5u32.to_be_bytes());
    xerial.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x01]);
    let mut bad_xerial = Bytes::from(xerial);
    let err_x = decompress_record_batch(
        &mut bad_xerial,
        kafka_protocol::records::Compression::Snappy,
    )
    .unwrap_err();
    assert!(
        err_x.to_string().contains("exceeds maximum expansion"),
        "expected xerial expansion cap rejection, got: {err_x}"
    );

    let mut zstd_hdr = Vec::new();
    zstd_hdr.extend_from_slice(&0xFD2FB528u32.to_le_bytes());
    zstd_hdr.push(0xE0);
    zstd_hdr.extend_from_slice(&(64u64 * 1024 * 1024).to_le_bytes());
    zstd_hdr.push(0x00);
    let mut bad_zstd = Bytes::from(zstd_hdr);
    let err_z = decompress_record_batch(
        &mut bad_zstd,
        kafka_protocol::records::Compression::Zstd,
    )
    .unwrap_err();
    assert!(
        !err_z.to_string().is_empty() && MAX_DECOMPRESSED_KAFKA_BATCH_BYTES > 64 * 1024 * 1024
    );
}

#[tokio::test]
async fn writer_client_serializes_per_partition_under_retries_and_does_not_retry_first_read_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port() as i32;
    let first_produce = Arc::new(AtomicBool::new(true));
    let observed_values = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let obs_c = Arc::clone(&observed_values);
    let fp_c = Arc::clone(&first_produce);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let obs = Arc::clone(&obs_c);
            let fp = Arc::clone(&fp_c);
            tokio::spawn(async move {
                let (mut rd, mut wr) = sock.split();
                while let Some(frame) = read_kafka_frame(&mut rd).await {
                    let mut buf = frame;
                    let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                    let ver = i16::from_be_bytes([buf[2], buf[3]]);
                    let api_key = ApiKey::try_from(key_i16).unwrap();
                    let hdr = RequestHeader::decode(&mut buf, api_key.request_header_version(ver))
                        .unwrap();
                    let bytes = match api_key {
                        ApiKey::ApiVersions => {
                            let _ = ApiVersionsRequest::decode(&mut buf, ver);
                            let mut resp = ApiVersionsResponse::default();
                            for (k, mn, mx) in [
                                (ApiKey::ApiVersions as i16, 0, 3),
                                (ApiKey::Metadata as i16, 1, 12),
                                (ApiKey::Produce as i16, 3, 9),
                            ] {
                                let mut av = ApiVersion::default();
                                av.api_key = k;
                                av.min_version = mn;
                                av.max_version = mx;
                                resp.api_keys.push(av);
                            }
                            encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                        }
                        ApiKey::Metadata => {
                            let _ = MetadataRequest::decode(&mut buf, ver);
                            let mut resp = MetadataResponse::default();
                            let mut b = MetadataResponseBroker::default();
                            b.node_id = BrokerId(0);
                            b.host = StrBytes::from_static_str("127.0.0.1");
                            b.port = port;
                            resp.brokers.push(b);
                            resp.controller_id = BrokerId(0);
                            let mut mt = MetadataResponseTopic::default();
                            mt.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                            let mut mp = MetadataResponsePartition::default();
                            mp.partition_index = 0;
                            mp.leader_id = BrokerId(0);
                            mt.partitions.push(mp);
                            resp.topics.push(mt);
                            encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                        }
                        ApiKey::Produce => {
                            let req = ProduceRequest::decode(&mut buf, ver).unwrap();
                            let mut resp = ProduceResponse::default();
                            for rt in &req.topic_data {
                                let mut tr = TopicProduceResponse::default();
                                tr.name = rt.name.clone();
                                for rp in &rt.partition_data {
                                    let mut pr = PartitionProduceResponse::default();
                                    pr.index = rp.index;
                                    if fp.swap(false, Ordering::SeqCst) {
                                        pr.error_code = ResponseError::NotLeaderOrFollower.code();
                                    } else {
                                        if let Some(raw) = &rp.records {
                                            if let Ok((recs, _, _)) =
                                                process_fetch_partition_batches(raw.clone(), 0)
                                            {
                                                let mut g = obs.lock();
                                                for r in recs {
                                                    g.push(r.value.to_vec());
                                                }
                                            }
                                        }
                                        pr.error_code = 0;
                                        pr.base_offset = obs.lock().len() as i64;
                                    }
                                    tr.partition_responses.push(pr);
                                }
                                resp.responses.push(tr);
                            }
                            encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                        }
                        _ => continue,
                    };
                    if wr.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let mut cfg = KafkaConfig {
        address: StringSliceCSV(vec![addr.to_string()]),
        topic: "ingest".to_string(),
        write_timeout: GoDuration::from_secs(3),
        ..Default::default()
    };
    cfg.dial_timeout = GoDuration::from_secs(2);

    let client = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )
    .unwrap();

    let (tx1, rx1) = tokio::sync::oneshot::channel();
    client.produce(
        WriterRecord {
            topic: "ingest".to_string(),
            partition: 0,
            key: Bytes::from_static(b"k1"),
            value: Bytes::from_static(b"first"),
            ..Default::default()
        },
        Box::new(move |_, res| {
            let _ = tx1.send(res);
        }),
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    let (tx2, rx2) = tokio::sync::oneshot::channel();
    client.produce(
        WriterRecord {
            topic: "ingest".to_string(),
            partition: 0,
            key: Bytes::from_static(b"k2"),
            value: Bytes::from_static(b"second"),
            ..Default::default()
        },
        Box::new(move |_, res| {
            let _ = tx2.send(res);
        }),
    );

    rx1.await.unwrap().expect("first batch succeeds after retry");
    rx2.await.unwrap().expect("second batch succeeds after first");
    assert_eq!(
        *observed_values.lock(),
        vec![b"first".to_vec(), b"second".to_vec()],
        "retry of batch 1 must NEVER allow batch 2 to overtake on same partition"
    );
    client.close();
    srv.abort();

    assert!(ingest_writer_retriable_err(&KafkaClientError::ConnectionClosed));
    assert!(!ingest_writer_retriable_err(&KafkaClientError::Sasl("bad".into())));
    assert!(!ingest_writer_retriable_err(&KafkaClientError::FirstReadEof("eof".into())));

    let eof_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let eof_addr = eof_listener.local_addr().unwrap();
    let eof_srv = tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = eof_listener.accept().await else {
                break;
            };
            drop(sock);
        }
    });
    let mut eof_cfg = KafkaConfig {
        address: StringSliceCSV(vec![eof_addr.to_string()]),
        topic: "ingest".to_string(),
        write_timeout: GoDuration::from_secs(5),
        ..Default::default()
    };
    eof_cfg.dial_timeout = GoDuration::from_secs(2);
    let eof_client = KafkaWriterClient::new(
        &eof_cfg,
        1,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )
    .unwrap();
    let start = std::time::Instant::now();
    let res = eof_client
        .produce_sync(WriterRecord {
            topic: "ingest".to_string(),
            partition: 0,
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            ..Default::default()
        })
        .await;
    assert!(res.is_err());
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "FirstReadEof must fail fast instead of wildcard-retrying for full 5s write_timeout"
    );
    eof_client.close();
    eof_srv.abort();
}

#[tokio::test]
async fn fetcher_advances_past_control_batches_and_flushes_buffered_on_out_of_range() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port() as i32;

    let control_batch = build_v2_batch_bytes(0, 0, 0x20, &[(b"ctrl", b"abort")]);
    let data_batch = build_v2_batch_bytes(1, 0, 0x00, &[(b"k1", b"v1")]);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let ctrl = control_batch.clone();
            let data = data_batch.clone();
            tokio::spawn(async move {
                let (mut rd, mut wr) = sock.split();
                while let Some(frame) = read_kafka_frame(&mut rd).await {
                    let mut buf = frame;
                    let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                    let ver = i16::from_be_bytes([buf[2], buf[3]]);
                    let api_key = ApiKey::try_from(key_i16).unwrap();
                    let hdr = RequestHeader::decode(&mut buf, api_key.request_header_version(ver))
                        .unwrap();
                    let bytes = match api_key {
                        ApiKey::ApiVersions => {
                            let _ = ApiVersionsRequest::decode(&mut buf, ver);
                            let mut resp = ApiVersionsResponse::default();
                            for (k, mn, mx) in [
                                (ApiKey::ApiVersions as i16, 0, 3),
                                (ApiKey::Metadata as i16, 1, 12),
                                (ApiKey::Fetch as i16, 4, 12),
                            ] {
                                let mut av = ApiVersion::default();
                                av.api_key = k;
                                av.min_version = mn;
                                av.max_version = mx;
                                resp.api_keys.push(av);
                            }
                            encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                        }
                        ApiKey::Metadata => {
                            let _ = MetadataRequest::decode(&mut buf, ver);
                            let mut resp = MetadataResponse::default();
                            let mut b = MetadataResponseBroker::default();
                            b.node_id = BrokerId(0);
                            b.host = StrBytes::from_static_str("127.0.0.1");
                            b.port = port;
                            resp.brokers.push(b);
                            resp.controller_id = BrokerId(0);
                            let mut mt = MetadataResponseTopic::default();
                            mt.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                            let mut mp = MetadataResponsePartition::default();
                            mp.partition_index = 0;
                            mp.leader_id = BrokerId(0);
                            mt.partitions.push(mp);
                            resp.topics.push(mt);
                            encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                        }
                        ApiKey::Fetch => {
                            let req = FetchRequest::decode(&mut buf, ver).unwrap();
                            let fetch_off = req.topics[0].partitions[0].fetch_offset;
                            let mut resp = FetchResponse::default();
                            let mut tr = FetchableTopicResponse::default();
                            tr.topic = TopicName(StrBytes::from_static_str("ingest"));
                            let mut pr = PartitionData::default();
                            pr.partition_index = 0;
                            pr.high_watermark = 2;
                            pr.log_start_offset = 0;
                            if fetch_off == 0 {
                                pr.records = Some(ctrl.clone());
                            } else if fetch_off == 1 {
                                pr.records = Some(data.clone());
                            } else {
                                pr.error_code = ResponseError::OffsetOutOfRange.code();
                            }
                            tr.partitions.push(pr);
                            resp.responses.push(tr);
                            encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                        }
                        _ => continue,
                    };
                    if wr.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let cfg = KafkaConfig {
        address: StringSliceCSV(vec![addr.to_string()]),
        topic: "ingest".to_string(),
        dial_timeout: GoDuration::from_secs(2),
        ..Default::default()
    };
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    let oc = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, None);
    let start_reader = Arc::new(GenericOffsetReader::new(
        Arc::new(|| Box::pin(async { Ok(0) })),
        Duration::from_secs(60),
    ));
    start_reader.start().await;

    let fetchers = ConcurrentFetchers::new(
        client,
        "ingest",
        0,
        0,
        1,
        1024 * 1024,
        false,
        Duration::from_millis(50),
        &oc,
        RangeErrorPolicy::ResumeFromStart,
        Arc::clone(&start_reader),
        mimir_common::backoff::BackoffConfig {
            min_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(50),
            max_retries: 0,
        },
        FetcherMetrics::new(None),
    )
    .await
    .unwrap();
    fetchers.start().await;

    let fr = tokio::time::timeout(
        Duration::from_secs(2),
        fetchers.poll_fetches(Duration::from_secs(2)),
    )
    .await
    .expect("fetcher must advance past control-only batch without spinning infinitely")
    .expect("must return FetchResult with data record at offset 1");
    assert_eq!(fr.records.len(), 1);
    assert_eq!(fr.records[0].offset, 1);
    assert_eq!(fr.records[0].value.as_ref(), b"v1");
    fetchers.stop().await;
    start_reader.stop().await;
    srv.abort();
}

#[tokio::test]
async fn transport_scram_caps_iterations_runs_off_worker_and_redacts_expected_sig() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let (mut rd, mut wr) = sock.split();
        while let Some(frame) = read_kafka_frame(&mut rd).await {
            let mut buf = frame;
            let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
            let ver = i16::from_be_bytes([buf[2], buf[3]]);
            let api_key = ApiKey::try_from(key_i16).unwrap();
            let hdr = RequestHeader::decode(&mut buf, api_key.request_header_version(ver)).unwrap();
            let bytes = match api_key {
                ApiKey::ApiVersions => {
                    let _ = ApiVersionsRequest::decode(&mut buf, ver);
                    let mut resp = ApiVersionsResponse::default();
                    for (k, mn, mx) in [
                        (ApiKey::ApiVersions as i16, 0, 3),
                        (ApiKey::SaslHandshake as i16, 0, 1),
                        (ApiKey::SaslAuthenticate as i16, 0, 2),
                    ] {
                        let mut av = ApiVersion::default();
                        av.api_key = k;
                        av.min_version = mn;
                        av.max_version = mx;
                        resp.api_keys.push(av);
                    }
                    encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                }
                ApiKey::SaslHandshake => {
                    let _ = SaslHandshakeRequest::decode(&mut buf, ver);
                    let mut resp = SaslHandshakeResponse::default();
                    resp.mechanisms
                        .push(StrBytes::from_static_str("SCRAM-SHA-256"));
                    encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                }
                ApiKey::SaslAuthenticate => {
                    let req = SaslAuthenticateRequest::decode(&mut buf, ver).unwrap();
                    let msg = String::from_utf8_lossy(&req.auth_bytes);
                    let nonce = msg.split("r=").nth(1).unwrap_or("n");
                    let server_first = format!("r={nonce}srv,s=c2FsdA==,i=2000000");
                    let mut resp = SaslAuthenticateResponse::default();
                    resp.auth_bytes = Bytes::from(server_first);
                    encode_kafka_response(api_key, ver, hdr.correlation_id, &resp)
                }
                _ => continue,
            };
            if wr.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let transport = KafkaTransport::plaintext(Duration::from_secs(2)).with_sasl(
        SaslAuthConfig::ScramSha256 {
            username: "u".into(),
            password: "p".into(),
        },
    );
    let err = transport
        .connect(&addr.to_string(), "c1", Duration::from_secs(2))
        .await
        .err()
        .expect("SCRAM iteration cap must error");
    let msg = err.to_string();
    assert!(
        msg.contains("exceeds maximum 1000000"),
        "expected SCRAM iteration cap error, got: {msg}"
    );
    srv.abort();
}

#[tokio::test]
async fn s3_gcs_azure_bucket_and_shipper_hardening() {
    let requests = Arc::new(Mutex::new(Vec::<(String, HashMap<String, String>)>::new()));
    let reqs_c = Arc::clone(&requests);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s3_srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let reqs = Arc::clone(&reqs_c);
            tokio::spawn(async move {
                loop {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let hdr_end = loop {
                        let Ok(n) = sock.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
                    let mut lines = head.lines();
                    let req_line = lines.next().unwrap_or("").to_string();
                    let mut hdrs = HashMap::new();
                    let mut content_len = 0usize;
                    for line in lines {
                        if let Some((k, v)) = line.split_once(':') {
                            let kl = k.trim().to_ascii_lowercase();
                            let vl = v.trim().to_string();
                            if kl == "content-length" {
                                content_len = vl.parse().unwrap_or(0);
                            }
                            hdrs.insert(kl, vl);
                        }
                    }
                    let mut body_read = buf.len() - (hdr_end + 4);
                    while body_read < content_len {
                        let Ok(n) = sock.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        body_read += n;
                    }
                    reqs.lock().push((req_line.clone(), hdrs));
                    let resp = if req_line.starts_with("HEAD /bkt/missing") {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else if req_line.starts_with("HEAD ") {
                        "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nETag: \"e1\"\r\n\r\n"
                            .to_string()
                    } else if req_line.contains("delimiter=") && req_line.contains("prefix=loop") {
                        let xml = "<ListBucketResult><Name>bkt</Name><IsTruncated>true</IsTruncated></ListBucketResult>";
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{xml}",
                            xml.len()
                        )
                    } else if req_line.contains("delimiter=") && req_line.contains("prefix=bad") {
                        let xml = "<ListBucketResult><Name>bkt</Name><Contents><Key>bad/..</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{xml}",
                            xml.len()
                        )
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"e1\"\r\n\r\n"
                            .to_string()
                    };
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });

    let cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "s3".into(),
            s3: S3Config {
                endpoint: addr.to_string(),
                region: "us-east-1".into(),
                bucket_name: "bkt".into(),
                access_key_id: "ak".into(),
                secret_access_key: "sk".into(),
                insecure: true,
                bucket_lookup_type: S3BucketLookupType::Path,
                list_objects_version: "v1".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let bkt = BucketClient::new_client(&cfg, "ingester", None).unwrap();

    let empty = bkt.get_range("exists.bin", 0, 0).await.unwrap();
    assert!(empty.is_empty());
    let missing = bkt.get_range("missing.bin", 0, 0).await.unwrap_err();
    assert!(missing.is_not_found());

    bkt.upload_with_options(
        "obj.json",
        Bytes::from_static(b"{}"),
        ObjectUploadOptions {
            content_type: Some("application/json".into()),
            condition: Some(UploadCondition::IfNotExists),
        },
    )
    .await
    .unwrap();
    bkt.upload_with_options(
        "obj2.json",
        Bytes::from_static(b"{}"),
        ObjectUploadOptions {
            content_type: None,
            condition: Some(UploadCondition::IfMatch(ObjectVersion::ETag(
                r#""e1""#.into(),
            ))),
        },
    )
    .await
    .unwrap();

    {
        let log = requests.lock();
        assert!(log
            .iter()
            .any(|(r, _)| r.starts_with("HEAD /bkt/exists.bin")));
        assert!(!log
            .iter()
            .any(|(_, h)| h.get("range").is_some_and(|v| v.contains("--"))));
        let put1 = log
            .iter()
            .find(|(r, _)| r.starts_with("PUT /bkt/obj.json"))
            .unwrap();
        assert_eq!(
            put1.1.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(put1.1.get("if-none-match").map(String::as_str), Some("*"));
        let put2 = log
            .iter()
            .find(|(r, _)| r.starts_with("PUT /bkt/obj2.json"))
            .unwrap();
        assert_eq!(
            put2.1.get("if-match").map(String::as_str),
            Some(r#""e1""#)
        );
    }

    let iter_err = bkt.iter_dir("loop").await.unwrap_err();
    assert!(
        iter_err.to_string().contains("did not advance marker"),
        "expected non-advancing V1 marker error, got: {iter_err}"
    );

    let bad_dst = tempfile::tempdir().unwrap();
    let dl_err = bkt
        .download_dir("bad", "bad", bad_dst.path(), Default::default())
        .await
        .unwrap_err();
    assert!(
        matches!(dl_err, BucketError::InvalidPath(_, _)),
        "expected InvalidPath for .. basename, got: {dl_err:?}"
    );
    s3_srv.abort();

    assert_eq!(
        split_credential_process_args("/usr/local/bin/cred-helper").unwrap(),
        vec!["/usr/local/bin/cred-helper"]
    );
    assert_eq!(
        split_credential_process_args(r#"'/path with spaces/helper' --profile "my prof""#)
            .unwrap(),
        vec!["/path with spaces/helper", "--profile", "my prof"]
    );
    let mut exp = Expiry::default();
    let past = Utc::now() - chrono::Duration::seconds(60);
    exp.set_expiration(past);
    assert_eq!(exp.effective_expiration(), Some(past));

    let mem = BucketClient::new_in_memory("mem");
    mem.upload("ok/a.txt", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let dst = tempfile::tempdir().unwrap();
    mem.download_dir("ok", "ok", dst.path(), Default::default())
        .await
        .unwrap();
    assert_eq!(std::fs::read(dst.path().join("a.txt")).unwrap(), b"hello");

    let ship_dir = tempfile::tempdir().unwrap();
    let target = Arc::new(ship_dir.path().join("shipper.meta.json"));
    let mut handles = Vec::new();
    for i in 0..16 {
        let t = Arc::clone(&target);
        handles.push(std::thread::spawn(move || {
            for r in 0..5 {
                let payload = format!(r#"{{"i":{i},"r":{r}}}"#);
                atomic_write_file(&t, payload.as_bytes()).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let leftover: Vec<_> = std::fs::read_dir(ship_dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(leftover, vec!["shipper.meta.json".to_string()]);

    assert!(parse_content_range("bytes 10-5/20").is_none());
    assert!(parse_content_range(&format!("bytes 0-{}/{}", usize::MAX, usize::MAX)).is_none());
    assert_eq!(parse_content_range("bytes 2-5/10"), Some((2..6, 10)));

    let gcs_calls = Arc::new(AtomicUsize::new(0));
    let gcs_c = Arc::clone(&gcs_calls);
    let gcs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gcs_addr = gcs_listener.local_addr().unwrap();
    let gcs_srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = gcs_listener.accept().await else {
                break;
            };
            let cnt = Arc::clone(&gcs_c);
            tokio::spawn(async move {
                let mut tmp = [0u8; 4096];
                let _ = sock.read(&mut tmp).await;
                cnt.fetch_add(1, Ordering::SeqCst);
                let _ = sock
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    let gcs_sa = format!(
        r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{gcs_addr}","disable_oauth":true}}"#
    );
    let gcs_store = mimir_storage::bucket::gcs::new_store(&GCSConfig {
        bucket_name: "bkt".into(),
        service_account: gcs_sa,
        max_retries: 3,
        enable_upload_retries: false,
        ..Default::default()
    })
    .unwrap();
    let gcs_path = object_store::path::Path::from("k");
    assert!(gcs_store
        .put_opts(
            &gcs_path,
            PutPayload::from_static(b"v"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            }
        )
        .await
        .is_err());
    assert_eq!(
        gcs_calls.swap(0, Ordering::SeqCst),
        1,
        "GCS non-idempotent Create must not retry 503"
    );
    assert!(gcs_store.delete(&gcs_path).await.is_err());
    assert_eq!(
        gcs_calls.swap(0, Ordering::SeqCst),
        1,
        "GCS non-idempotent DELETE must not retry 503"
    );
    assert!(gcs_store.head(&gcs_path).await.is_err());
    assert_eq!(gcs_calls.load(Ordering::SeqCst), 3);
    gcs_srv.abort();

    let az_calls = Arc::new(AtomicUsize::new(0));
    let az_c = Arc::clone(&az_calls);
    let az_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let az_addr = az_listener.local_addr().unwrap();
    let az_srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = az_listener.accept().await else {
                break;
            };
            let cnt = Arc::clone(&az_c);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                loop {
                    let Ok(n) = sock.read(&mut tmp).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&buf);
                let resp = if head.contains("restype=container") {
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                } else {
                    cnt.fetch_add(1, Ordering::SeqCst);
                    "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                };
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    let az_cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "azure".into(),
            azure: AzureConfig {
                account_name: "devstoreaccount1".into(),
                container_name: "blocks".into(),
                max_retries: 2,
                connection_string: format!(
                    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{az_addr}/devstoreaccount1;"
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let az_bkt =
        tokio::task::spawn_blocking(move || BucketClient::new_client(&az_cfg, "ingester", None))
            .await
            .unwrap()
            .unwrap();
    let az_res = az_bkt
        .upload_with_options(
            "k",
            Bytes::from_static(b"v"),
            ObjectUploadOptions {
                content_type: None,
                condition: Some(UploadCondition::IfNotExists),
            },
        )
        .await;
    assert!(az_res.is_err());
    assert_eq!(
        az_calls.swap(0, Ordering::SeqCst),
        1,
        "Azure non-idempotent IfNotExists must not retry 503"
    );
    assert!(az_bkt.download("k").await.is_err());
    assert!(az_calls.load(Ordering::SeqCst) > 1, "idempotent Azure download must retry 503");
    az_srv.abort();
}
