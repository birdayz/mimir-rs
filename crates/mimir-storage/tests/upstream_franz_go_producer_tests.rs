//! Ported upstream github.com/twmb/franz-go/pkg/kgo producer, sink, batch encoding,
//! promise ring backpressure, and transaction tests.

#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use helpers::{create_test_kafka_config, FakeKafkaCluster, HookAction};
use kafka_protocol::error::ResponseError;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::write_txn_markers_request::{
    WritableTxnMarker, WritableTxnMarkerTopic,
};
use kafka_protocol::messages::{
    ProduceRequest, ProduceResponse, ProducerId, TopicName, WriteTxnMarkersRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use mimir_storage::ingest::compression::{compress_record_batch, KafkaCompression};
use mimir_storage::ingest::kgo_producer_sink::{
    append_legacy_message_set, shard_write_txn_markers, NullBatchProduceRequest,
    ProducerTopicsRegistry, PromiseChainFlushHarness, RecBuf, Sink, WirePromisedRec, WireRecBatch,
};
use mimir_storage::ingest::ring::PromiseRing;
use mimir_storage::ingest::writer_client::{
    first_produce_err, KafkaProducer, KafkaProducerClient, KafkaWriterClient,
    KafkaWriterClientOptions, ProduceCancelSource, ProduceContext, ProducerError, WriterRecord,
};

// ---------------------------------------------------------------------------
// produce_request_encode_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_encode_test.go::TestProduceRequestNullBatchEncoding
#[test]
fn test_produce_request_null_batch_encoding() {
    for version in [8i16, 9] {
        let req = NullBatchProduceRequest {
            version,
            acks: -1,
            timeout_ms: 1000,
            topics: vec![("t".to_string(), vec![0, 1])],
        };
        let mut raw = Vec::new();
        req.append_to(&mut raw);
        let mut buf = Bytes::from(raw);
        let decoded = ProduceRequest::decode(&mut buf, version)
            .unwrap_or_else(|e| panic!("v{version} decode failed: {e}"));
        assert!(buf.is_empty(), "v{version} leftover bytes {}", buf.len());
        assert_eq!(decoded.topic_data.len(), 1);
        assert_eq!(decoded.topic_data[0].partition_data.len(), 2);
        for p in &decoded.topic_data[0].partition_data {
            assert!(p.records.is_none(), "v{version} partition {} records not null", p.index);
        }
    }
}

// ---------------------------------------------------------------------------
// produce_request_test.go (8 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestClient_Produce
#[tokio::test]
async fn test_client_produce() {
    let cluster = FakeKafkaCluster::start(1, "prod-topic").await;
    let mut cfg = create_test_kafka_config(&cluster.addr, "prod-topic");
    cfg.producer_max_buffered_bytes = 5000;
    let wc = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )
    .unwrap();
    let producer = KafkaProducer::new(wc, 5000, None);

    let num_workers = 50usize;
    let total_recs = 4_000usize;
    let counter = Arc::new(AtomicUsize::new(0));
    let write_ok = Arc::new(AtomicUsize::new(0));
    let write_err = Arc::new(AtomicUsize::new(0));
    let unexpected = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(num_workers);
    for w in 0..num_workers {
        let p = Arc::clone(&producer);
        let counter = Arc::clone(&counter);
        let write_ok = Arc::clone(&write_ok);
        let write_err = Arc::clone(&write_err);
        let unexpected = Arc::clone(&unexpected);
        handles.push(tokio::spawn(async move {
            loop {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                if idx >= total_recs {
                    break;
                }
                let val_len = (idx * 73 + w * 17) % 1000;
                let rec = WriterRecord {
                    topic: "prod-topic".into(),
                    partition: 0,
                    key: Bytes::from_static(b"test"),
                    value: Bytes::from(vec![b'x'; val_len]),
                    ..Default::default()
                };
                let res = p.produce_sync(&ProduceContext::background(), vec![rec]).await;
                match first_produce_err(&res) {
                    None => {
                        write_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(ProducerError::MaxBuffered) => {
                        write_err.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(other) => {
                        eprintln!("unexpected produce error: {other}");
                        unexpected.store(true, Ordering::SeqCst);
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert!(!unexpected.load(Ordering::SeqCst));
    assert!(write_ok.load(Ordering::Relaxed) > 0);
    assert_eq!(
        write_ok.load(Ordering::Relaxed) + write_err.load(Ordering::Relaxed),
        total_recs
    );
    producer.close();
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestIssue769
#[tokio::test]
async fn test_issue_769() {
    let cluster = FakeKafkaCluster::start(1, "t769").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t769");
    let wc = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )
    .unwrap();
    let producer = KafkaProducer::new(wc, 1 << 20, None);

    // Pre-canceled context must bail immediately without waiting for network.
    let (cancel_src, ctx) = ProduceCancelSource::new();
    cancel_src.cancel();
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        producer.produce_sync(
            &ctx,
            vec![WriterRecord {
                topic: "t769".into(),
                value: Bytes::from_static(b"foo"),
                ..Default::default()
            }],
        ),
    )
    .await
    .expect("must fail promptly");
    assert!(matches!(
        first_produce_err(&res),
        Some(ProducerError::CancelledBeforeProducing(_))
    ));

    // Normal produce succeeds, then a second canceled produce fails promptly.
    let ok_res = producer
        .produce_sync(
            &ProduceContext::background(),
            vec![WriterRecord {
                topic: "t769".into(),
                value: Bytes::from_static(b"foo"),
                ..Default::default()
            }],
        )
        .await;
    assert!(first_produce_err(&ok_res).is_none());

    let res2 = tokio::time::timeout(
        Duration::from_secs(3),
        producer.produce_sync(
            &ctx,
            vec![WriterRecord {
                topic: "t769".into(),
                value: Bytes::from_static(b"foo"),
                ..Default::default()
            }],
        ),
    )
    .await
    .expect("must fail promptly");
    assert!(matches!(
        first_produce_err(&res2),
        Some(ProducerError::CancelledBeforeProducing(_))
    ));

    producer.close();
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestIssueProduceSyncManualPartitionNegativeDoesNotPanic
#[tokio::test]
async fn test_issue_produce_sync_manual_partition_negative_does_not_panic() {
    let cluster = FakeKafkaCluster::start(1, "t-neg-part").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t-neg-part");
    let wc = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: false,
        },
    )
    .unwrap();

    let warmup = wc
        .produce_sync(WriterRecord {
            topic: "t-neg-part".into(),
            partition: 0,
            value: Bytes::from_static(b"warmup"),
            ..Default::default()
        })
        .await;
    assert!(warmup.is_ok());

    let err = wc
        .produce_sync(WriterRecord {
            topic: "t-neg-part".into(),
            partition: -1,
            value: Bytes::from_static(b"bad"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("invalid record partitioning choice"),
        "unexpected error: {err}"
    );
    wc.close();
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestIssue831
#[tokio::test]
async fn test_issue_831() {
    let cluster = FakeKafkaCluster::start(1, "t831").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t831");
    let wc = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )
    .unwrap();
    let producer = KafkaProducer::new(wc, 1024, None);

    let mut handles = Vec::with_capacity(500);
    for _ in 0..500 {
        let p = Arc::clone(&producer);
        handles.push(tokio::spawn(async move {
            let (cancel, ctx) = ProduceCancelSource::new();
            cancel.cancel();
            let res = p
                .produce_sync(
                    &ctx,
                    vec![WriterRecord {
                        topic: "t831".into(),
                        value: Bytes::from_static(b"foo"),
                        ..Default::default()
                    }],
                )
                .await;
            assert!(first_produce_err(&res).is_some());
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), futures::future::join_all(handles))
        .await
        .expect("500 canceled produces must complete without stalling");

    producer.close();
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestPromisedRecAppendTo
#[test]
fn test_promised_rec_append_to() {
    let mut headers = IndexMap::new();
    headers.insert(StrBytes::from_static_str("header key 1"), Some(Bytes::from_static(b"header value 1")));
    headers.insert(StrBytes::from_static_str("header key 2"), Some(Bytes::from_static(b"header value 2")));
    let krec0 = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: 0,
        timestamp: 0,
        key: None,
        value: None,
        headers: IndexMap::new(),
    };
    let krec1 = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 3,
        sequence: 3,
        timestamp: 2,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers,
    };
    let mut kbuf0 = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut kbuf0,
        [&krec0],
        &RecordEncodeOptions { version: 2, compression: Compression::None },
        None::<fn(&mut BytesMut, &mut BytesMut, Compression) -> anyhow::Result<()>>,
    )
    .unwrap();
    let rec0_len = kbuf0.len() - 61;
    let mut kbuf = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut kbuf,
        [&krec0, &krec1],
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
        None::<fn(&mut BytesMut, &mut BytesMut, Compression) -> anyhow::Result<()>>,
    )
    .unwrap();
    // Extract krec1 (timestampDelta=2, offsetDelta=3) after header + krec0.
    let exp = &kbuf[61 + rec0_len..];

    let pr = WirePromisedRec {
        key: Some(b"key".to_vec()),
        value: Some(b"value".to_vec()),
        headers: vec![
            ("header key 1".into(), Some(b"header value 1".to_vec())),
            ("header key 2".into(), Some(b"header value 2".to_vec())),
        ],
        timestamp_delta: 2,
    };
    let mut got = Vec::new();
    pr.append_to(&mut got, 3);
    assert_eq!(got.as_slice(), exp);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestRecBatchAppendTo
#[test]
fn test_rec_batch_append_to() {
    let mut h1 = IndexMap::new();
    h1.insert(StrBytes::from_static_str("header key 1"), Some(Bytes::from_static(b"header value 1")));
    h1.insert(StrBytes::from_static_str("header key 2"), Some(Bytes::from_static(b"header value 2")));

    let recs = [
        Record {
            transactional: true,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: 12,
            producer_epoch: 11,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: 10,
            timestamp: 22,
            key: Some(Bytes::from_static(b"key 1")),
            value: Some(Bytes::from_static(b"value 1")),
            headers: h1,
        },
        Record {
            transactional: true,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: 12,
            producer_epoch: 11,
            timestamp_type: TimestampType::Creation,
            offset: 1,
            sequence: 11,
            timestamp: 24,
            key: Some(Bytes::from_static(b"key 2")),
            value: Some(Bytes::from_static(b"value 2")),
            headers: IndexMap::new(),
        },
    ];

    let our_batch = WireRecBatch {
        seq: 10,
        first_timestamp: 22,
        max_timestamp_delta: 2,
        records: vec![
            WirePromisedRec {
                key: Some(b"key 1".to_vec()),
                value: Some(b"value 1".to_vec()),
                headers: vec![
                    ("header key 1".into(), Some(b"header value 1".to_vec())),
                    ("header key 2".into(), Some(b"header value 2".to_vec())),
                ],
                timestamp_delta: 0,
            },
            WirePromisedRec {
                key: Some(b"key 2".to_vec()),
                value: Some(b"value 2".to_vec()),
                headers: vec![],
                timestamp_delta: 2,
            },
        ],
    };

    for (comp_k, comp_p) in [
        (KafkaCompression::None, Compression::None),
        (KafkaCompression::Snappy, Compression::Snappy),
    ] {
        let mut expected_batch = BytesMut::new();
        RecordBatchEncoder::encode_with_custom_compression(
            &mut expected_batch,
            recs.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: comp_p,
            },
            Some(compress_record_batch),
        )
        .unwrap();

        for version in [8i16, 9, 10, 11] {
            let mut got_full = Vec::new();
            our_batch.append_to(&mut got_full, version, 12, 11, true, comp_k);
            let mut slice = &got_full[..];
            let wire_len = if version >= 9 {
                let mut v = 0u64;
                let mut shift = 0;
                loop {
                    let b = slice[0];
                    slice = &slice[1..];
                    v |= ((b & 0x7f) as u64) << shift;
                    if b < 0x80 {
                        break;
                    }
                    shift += 7;
                }
                (v - 1) as usize
            } else {
                let l = i32::from_be_bytes(slice[..4].try_into().unwrap()) as usize;
                slice = &slice[4..];
                l
            };
            assert_eq!(wire_len, slice.len(), "v{version} {comp_k:?}");
            assert_eq!(slice, expected_batch.as_ref(), "v{version} {comp_k:?}");

            // Round-trip inside ProduceRequest
            let mut req = ProduceRequest::default();
            req.acks = -1;
            req.timeout_ms = 1000;
            let mut tp = TopicProduceData::default();
            tp.name = TopicName(StrBytes::from_static_str("t"));
            let mut pp = PartitionProduceData::default();
            pp.index = 0;
            pp.records = Some(Bytes::copy_from_slice(slice));
            tp.partition_data.push(pp);
            req.topic_data.push(tp);
            let mut wire = BytesMut::new();
            req.encode(&mut wire, version).unwrap();
            let mut dec_buf = wire.freeze();
            let decoded = ProduceRequest::decode(&mut dec_buf, version).unwrap();
            assert_eq!(
                decoded.topic_data[0].partition_data[0].records.as_deref(),
                Some(expected_batch.as_ref())
            );
        }
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestMessageSetAppendTo
#[test]
fn test_message_set_append_to() {
    for magic in [0i8, 1] {
        for gzip in [false, true] {
            let mut raw = Vec::new();
            append_legacy_message_set(
                &mut raw,
                magic,
                &[
                    (b"key 1", b"value 1", 22),
                    (b"key 2", b"value 2", 24),
                ],
                gzip,
            );
            let (recs, next, _) =
                mimir_storage::ingest::kafka_client::process_fetch_partition_batches(
                    Bytes::from(raw),
                    0,
                )
                .unwrap();
            assert_eq!(recs.len(), 2, "magic={magic} gzip={gzip}");
            assert_eq!(recs[0].value.as_ref(), b"value 1");
            assert_eq!(recs[1].value.as_ref(), b"value 2");
            assert_eq!(next, 2);
        }
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/produce_request_test.go::TestClient_ProduceLargeMessages
#[tokio::test]
async fn test_client_produce_large_messages() {
    let cluster = FakeKafkaCluster::start(1, "large-topic").await;
    let cfg = create_test_kafka_config(&cluster.addr, "large-topic");

    // 1) Client-side MessageTooLarge
    {
        let record_size = 50 * 1024;
        let wc = KafkaWriterClient::new(
            &cfg,
            2,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: true,
            },
        )
        .unwrap();
        wc.set_batch_max_bytes(record_size - 100);
        let err = wc
            .produce_sync(WriterRecord {
                topic: "large-topic".into(),
                partition: 0,
                value: Bytes::from(vec![b'x'; record_size]),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(err.is_message_too_large(), "got {err:?}");
        assert!(
            err.to_string().contains(&format!("uncompressed_bytes={record_size}")),
            "got: {err}"
        );
        wc.close();
    }

    // 2) Broker-side MessageTooLarge
    {
        *cluster.hooks.produce.lock() = Some(Arc::new(|_req| {
            Box::pin(async {
                let mut resp = ProduceResponse::default();
                let mut tr = TopicProduceResponse::default();
                tr.name = TopicName(StrBytes::from_static_str("large-topic"));
                let mut pr = PartitionProduceResponse::default();
                pr.index = 0;
                pr.error_code = ResponseError::MessageTooLarge.code();
                tr.partition_responses.push(pr);
                resp.responses.push(tr);
                HookAction::Respond(resp)
            })
        }));

        let record_size = 64 * 1024;
        let wc = KafkaWriterClient::new(
            &cfg,
            2,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: true,
            },
        )
        .unwrap();
        wc.set_batch_max_bytes(record_size + 1024);
        let err = wc
            .produce_sync(WriterRecord {
                topic: "large-topic".into(),
                partition: 0,
                value: Bytes::from(vec![b'x'; record_size]),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(err.is_message_too_large(), "got {err:?}");
        let msg = err.to_string();
        assert!(msg.contains("uncompressed_bytes="), "got: {msg}");
        assert!(msg.contains(" compressed_bytes="), "got: {msg}");
        wc.close();
    }

    cluster.close();
}

// ---------------------------------------------------------------------------
// producer_sweep_test.go (5 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/producer_sweep_test.go::TestAuditFullPromiseRingBoundAndConvergence
#[test]
fn test_audit_full_promise_ring_bound_and_convergence() {
    let max_len = 8usize;
    let bounded: Arc<PromiseRing<u32>> = Arc::new(PromiseRing::with_max_len(max_len));
    let gated_mu = Arc::new(std::sync::Mutex::new(false));
    let gated_cv = Arc::new(std::sync::Condvar::new());
    let drained_count = Arc::new(AtomicUsize::new(0));
    let via_goroutine = Arc::new(AtomicBool::new(false));

    fn spawn_worker(
        ring: Arc<PromiseRing<u32>>,
        gm: Arc<std::sync::Mutex<bool>>,
        gc: Arc<std::sync::Condvar>,
        dc: Arc<AtomicUsize>,
        vg: Arc<AtomicBool>,
    ) {
        std::thread::spawn(move || {
            let mut open = gm.lock().unwrap();
            while !*open {
                open = gc.wait(open).unwrap();
            }
            drop(open);
            let mut spawned_from_promise = false;
            loop {
                dc.fetch_add(1, Ordering::Relaxed);
                if !spawned_from_promise {
                    spawned_from_promise = true;
                    let r2 = Arc::clone(&ring);
                    let gm2 = Arc::clone(&gm);
                    let gc2 = Arc::clone(&gc);
                    let dc2 = Arc::clone(&dc);
                    let vg2 = Arc::clone(&vg);
                    std::thread::spawn(move || {
                        let (f, _) = r2.push(777);
                        vg2.store(true, Ordering::SeqCst);
                        if f {
                            spawn_worker(r2, gm2, gc2, dc2, vg2);
                        }
                    });
                }
                let (_, more, _) = ring.drop_peek();
                if !more {
                    break;
                }
            }
        });
    }

    let (first, _) = bounded.push(1);
    assert!(first);
    spawn_worker(
        Arc::clone(&bounded),
        Arc::clone(&gated_mu),
        Arc::clone(&gated_cv),
        Arc::clone(&drained_count),
        Arc::clone(&via_goroutine),
    );

    let extra = 64usize;
    let b_fill = Arc::clone(&bounded);
    let gm_f = Arc::clone(&gated_mu);
    let gc_f = Arc::clone(&gated_cv);
    let dc_f = Arc::clone(&drained_count);
    let vg_f = Arc::clone(&via_goroutine);
    let filler = std::thread::spawn(move || {
        for i in 0..(max_len + extra) {
            let (f, _) = b_fill.push(i as u32 + 2);
            if f {
                spawn_worker(
                    Arc::clone(&b_fill),
                    Arc::clone(&gm_f),
                    Arc::clone(&gc_f),
                    Arc::clone(&dc_f),
                    Arc::clone(&vg_f),
                );
            }
        }
    });

    let start = std::time::Instant::now();
    while bounded.len() < max_len {
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(30));
    assert!(bounded.len() <= max_len, "ring grew past max_len");

    let (tx_int, rx_int) = std::sync::mpsc::channel();
    let b_int = Arc::clone(&bounded);
    let int_h = std::thread::spawn(move || {
        b_int.push_force(999);
        tx_int.send(()).unwrap();
    });
    rx_int
        .recv_timeout(Duration::from_secs(2))
        .expect("push_force must not park");

    *gated_mu.lock().unwrap() = true;
    gated_cv.notify_all();
    filler.join().unwrap();
    int_h.join().unwrap();

    let wait_start = std::time::Instant::now();
    while !via_goroutine.load(Ordering::SeqCst) || !bounded.is_empty() {
        assert!(wait_start.elapsed() < Duration::from_secs(5), "ring never drained");
        std::thread::sleep(Duration::from_millis(1));
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/producer_sweep_test.go::TestAuditFlushNotStarvedByPromiseChain
#[test]
fn test_audit_flush_not_starved_by_promise_chain() {
    let harness = Arc::new(PromiseChainFlushHarness::default());
    let mid_drain = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let stop_chain = Arc::new(AtomicBool::new(false));

    {
        let md = Arc::clone(&mid_drain);
        let sc = Arc::clone(&stop_chain);
        *harness.on_broadcast.lock().unwrap() = Some(Box::new(move |more_queued| {
            if more_queued {
                sc.store(true, Ordering::SeqCst);
                *md.0.lock().unwrap() = true;
                md.1.notify_all();
            }
        }));
    }

    *harness.buffered_records.lock().unwrap() = 1;
    let h_flush = Arc::clone(&harness);
    let (tx_flush, rx_flush) = std::sync::mpsc::channel();
    let flush_thread = std::thread::spawn(move || {
        h_flush.flush();
        tx_flush.send(()).unwrap();
    });

    while harness.flushing.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }

    // Seed promise chain on ring so moreQueued == true when counted record finishes.
    harness.ring.push(Arc::new(|| {}));
    harness.finish_one_buffered();

    let (lock, cv) = &*mid_drain;
    let guard = lock.lock().unwrap();
    let (guard, timeout) = cv.wait_timeout_while(guard, Duration::from_secs(5), |&mut ok| !ok).unwrap();
    drop(guard);
    assert!(!timeout.timed_out(), "expected mid-drain broadcast");
    assert!(stop_chain.load(Ordering::SeqCst));
    rx_flush
        .recv_timeout(Duration::from_secs(5))
        .expect("Flush must wake promptly on mid-drain broadcast");
    flush_thread.join().unwrap();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/producer_sweep_test.go::TestAuditPurgeVsProduceUnknownTopicNoOrphan
#[test]
fn test_audit_purge_vs_produce_unknown_topic_no_orphan() {
    let reg = Arc::new(ProducerTopicsRegistry::default());
    let (tx1, rx1) = std::sync::mpsc::channel::<String>();
    reg.produce_unknown("U", Box::new(move |e| { let _ = tx1.send(e.to_string()); }));

    // Hold sink_rec_bufs_mu while purging.
    let sink_guard = reg.sink_rec_bufs_mu.lock().unwrap();
    let reg_p = Arc::clone(&reg);
    let (tx_purge, rx_purge) = std::sync::mpsc::channel();
    let purge_h = std::thread::spawn(move || {
        reg_p.purge_topics(&["K", "U"]);
        tx_purge.send(()).unwrap();
    });

    assert_eq!(rx1.recv_timeout(Duration::from_secs(3)).unwrap(), "errPurged");

    let (tx2, rx2) = std::sync::mpsc::channel::<String>();
    let reg_u2 = Arc::clone(&reg);
    let u2_h = std::thread::spawn(move || {
        reg_u2.produce_unknown("U", Box::new(move |e| { let _ = tx2.send(e.to_string()); }));
    });
    std::thread::sleep(Duration::from_millis(40));
    drop(sink_guard);
    rx_purge.recv_timeout(Duration::from_secs(3)).unwrap();
    purge_h.join().unwrap();
    u2_h.join().unwrap();

    // Invariant: if unknown_topics has "U", topics_mu MUST also contain "U".
    let has_unk = reg.unknown_topics_mu.lock().unwrap().contains_key("U");
    let has_topic = reg.topics_mu.lock().unwrap().contains("U");
    assert!(has_unk && has_topic, "unknown_topics={has_unk} topics={has_topic}");
    drop(rx2);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/producer_sweep_test.go::TestAuditFailedRecordSentinels
#[tokio::test]
async fn test_audit_failed_record_sentinels() {
    let cluster = FakeKafkaCluster::start(1, "t-sentinel").await;
    let cfg = create_test_kafka_config(&cluster.addr, "t-sentinel");
    let wc = KafkaWriterClient::new(
        &cfg,
        1,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: true,
            disable_linger: true,
        },
    )
    .unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    wc.produce(
        WriterRecord {
            topic: String::new(),
            partition: 0,
            offset: 42,
            ..Default::default()
        },
        Box::new(move |rec, res| {
            let _ = tx.send((rec, res));
        }),
    );
    let (rec, res) = rx.await.unwrap();
    assert!(res.is_err());
    assert_eq!(rec.offset, -1, "failed record must carry -1 sentinel offset");
    wc.close();
    cluster.close();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/producer_sweep_test.go::TestAuditFlushWokenByBlockedProduceCancel
#[test]
fn test_audit_flush_woken_by_blocked_produce_cancel() {
    let harness = Arc::new(PromiseChainFlushHarness::default());
    for i in 0..200 {
        *harness.buffered_records.lock().unwrap() = 1;
        harness.blocked.store(1, Ordering::SeqCst);

        let h_flush = Arc::clone(&harness);
        let (tx_flush, rx_flush) = std::sync::mpsc::channel();
        let ft = std::thread::spawn(move || {
            h_flush.flush();
            tx_flush.send(()).unwrap();
        });
        while harness.flushing.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        *harness.buffered_records.lock().unwrap() = 0;
        harness.cancel_blocked_produce();

        rx_flush
            .recv_timeout(Duration::from_secs(3))
            .unwrap_or_else(|_| panic!("iteration {i}: flush hung after blocked produce cancel"));
        ft.join().unwrap();
    }
}

// ---------------------------------------------------------------------------
// sink_resweep_test.go (1 test) & sink_unstage_test.go (3 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/sink_resweep_test.go::TestAuditTxnRecheckRewindUnmarksAddedToTxn
#[test]
fn test_audit_txn_recheck_rewind_unmarks_added_to_txn() {
    let sink = Sink::new(11, true);
    let r_new = RecBuf::new("t", 0);
    let r_old = RecBuf::new("t", 1);
    r_old.lock().unwrap().added_to_txn.store(true, Ordering::SeqCst);
    r_new.lock().unwrap().buffer_one();
    r_old.lock().unwrap().buffer_one();
    sink.add_rec_buf(Arc::clone(&r_new));
    sink.add_rec_buf(Arc::clone(&r_old));

    let (req, txn_req) = sink.create_req(5, 0);
    let txn_req = txn_req.expect("expected AddPartitionsToTxn");
    assert_eq!(req.batches.len(), 2);
    assert!(txn_req.contains("t", 0));
    assert!(!txn_req.contains("t", 1));
    assert!(r_new.lock().unwrap().added_to_txn.load(Ordering::SeqCst));
    assert!(r_old.lock().unwrap().added_to_txn.load(Ordering::SeqCst));
    assert_eq!(r_new.lock().unwrap().inflight, 1);
    assert_eq!(r_old.lock().unwrap().inflight, 1);

    req.undo_staged_batches(Some(&txn_req));
    assert!(!r_new.lock().unwrap().added_to_txn.load(Ordering::SeqCst));
    assert!(r_old.lock().unwrap().added_to_txn.load(Ordering::SeqCst));
    assert_eq!(r_new.lock().unwrap().inflight, 0);
    assert_eq!(r_old.lock().unwrap().inflight, 0);
    assert_eq!(r_new.lock().unwrap().batch_drain_idx, 0);
    assert_eq!(r_old.lock().unwrap().batch_drain_idx, 0);
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/sink_unstage_test.go::TestUndoStagedBatchesWithInflight
#[test]
fn test_undo_staged_batches_with_inflight() {
    let sink = Sink::new(9, false);
    let r = RecBuf::new("t", 0);
    sink.add_rec_buf(Arc::clone(&r));

    r.lock().unwrap().buffer_one();
    let (_req_a, _) = sink.create_req(5, 0);
    r.lock().unwrap().ok_on_sink = true;
    r.lock().unwrap().buffer_one();
    let (_req_b, _) = sink.create_req(5, 0);
    r.lock().unwrap().buffer_one();
    let (req_c, _) = sink.create_req(5, 0);

    {
        let rb = r.lock().unwrap();
        assert_eq!((rb.batch_drain_idx, rb.inflight, rb.seq), (3, 3, 3));
    }

    req_c.undo_staged_batches(None);
    {
        let rb = r.lock().unwrap();
        assert_eq!((rb.batch_drain_idx, rb.inflight, rb.seq), (2, 2, 2));
    }

    {
        let mut rb = r.lock().unwrap();
        Sink::finish_batch(&mut rb);
        Sink::finish_batch(&mut rb);
        assert_eq!((rb.batch_drain_idx, rb.inflight, rb.batches.len()), (0, 0, 1));
    }

    let (req_c2, _) = sink.create_req(5, 0);
    assert_eq!(req_c2.batches.len(), 1);
    let rb = r.lock().unwrap();
    assert_eq!((rb.batch_drain_idx, rb.inflight, rb.seq), (1, 1, 3));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/sink_unstage_test.go::TestUndoStagedBatchesAfterFailAllRecords
#[test]
fn test_undo_staged_batches_after_fail_all_records() {
    let sink = Sink::new(9, false);
    let r = RecBuf::new("t", 0);
    sink.add_rec_buf(Arc::clone(&r));

    r.lock().unwrap().buffer_one();
    let (req, _) = sink.create_req(5, 0);
    Sink::fail_all_records(&mut r.lock().unwrap());
    req.undo_staged_batches(None);

    let rb = r.lock().unwrap();
    assert_eq!(rb.batch_drain_idx, 0);
    assert_eq!(rb.inflight, 0);
    assert!(rb.batches.is_empty());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/sink_unstage_test.go::TestCreateReqSkipsPendingSeqResetWithInflight
#[test]
fn test_create_req_skips_pending_seq_reset_with_inflight() {
    let sink = Sink::new(9, false);
    let r = RecBuf::new("t", 0);
    sink.add_rec_buf(Arc::clone(&r));

    r.lock().unwrap().buffer_one();
    let (_req_a, _) = sink.create_req(5, 0);
    r.lock().unwrap().ok_on_sink = true;
    r.lock().unwrap().buffer_one();

    // A sequence reset lands while b0 is still inflight; staging b1 now must be skipped.
    r.lock().unwrap().need_seq_reset = true;
    let (req_empty, _) = sink.create_req(5, 0);
    assert!(req_empty.batches.is_empty());

    // Finish b0 -> inflight becomes 0 -> next create_req consumes reset and stages b1 with seq 0!
    Sink::finish_batch(&mut r.lock().unwrap());
    let (req_next, _) = sink.create_req(5, 0);
    assert_eq!(req_next.batches.len(), 1);
    assert_eq!(req_next.batches[0].1, 0);
    assert!(!r.lock().unwrap().need_seq_reset);
}

// ---------------------------------------------------------------------------
// txn_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/txn_test.go::TestTxnEtl
#[tokio::test]
async fn test_txn_etl() {
    let cluster = FakeKafkaCluster::start(3, "txn-src").await;
    let cfg = create_test_kafka_config(&cluster.addr, "txn-src");
    let client = mimir_storage::ingest::KafkaClient::from_config(&cfg).unwrap();
    client.create_topics(&["txn-mid", "txn-dst"], 3, 1).await.unwrap();
    let wc = KafkaWriterClient::new(
        &cfg,
        4,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: true,
            disable_linger: true,
        },
    )
    .unwrap();
    let producer = KafkaProducer::new(wc, 1 << 20, None);

    const N: usize = 300;
    // Stage 1: transactional producer writes N records to txn-src across 3 partitions.
    let mut recs = Vec::with_capacity(N);
    for i in 0..N {
        recs.push(WriterRecord {
            topic: "txn-src".into(),
            partition: (i % 3) as i32,
            key: Bytes::from(i.to_string()),
            value: Bytes::from_static(b"payload-body"),
            ..Default::default()
        });
    }
    let res = producer.produce_sync(&ProduceContext::background(), recs).await;
    assert!(first_produce_err(&res).is_none());

    // Stage 2: ETL consumer reads from txn-src, produces to txn-mid, commits offsets.
    let mut seen_src = HashSet::new();
    for p in 0..3 {
        let fetched = client
            .fetch("txn-src", p, 0, Duration::from_millis(100), 1, 1024 * 1024)
            .await
            .unwrap();
        let mut forward = Vec::new();
        for r in fetched.records {
            let k: usize = std::str::from_utf8(&r.key).unwrap().parse().unwrap();
            assert!(seen_src.insert(k), "duplicate record {k}");
            assert_eq!(r.value.as_ref(), b"payload-body");
            forward.push(WriterRecord {
                topic: "txn-mid".into(),
                partition: p,
                key: r.key,
                value: r.value,
                ..Default::default()
            });
        }
        let fres = producer.produce_sync(&ProduceContext::background(), forward).await;
        assert!(first_produce_err(&fres).is_none());
        client
            .commit_offset("txn-etl-g1", "txn-src", p, fetched.next_fetch_offset)
            .await
            .unwrap();
    }
    assert_eq!(seen_src.len(), N);

    // Stage 3: verify committed offsets and downstream completeness on txn-mid.
    let mut seen_mid = HashSet::new();
    for p in 0..3 {
        let off = client.fetch_committed_offset("txn-etl-g1", "txn-src", p).await.unwrap();
        assert_eq!(off, Some((N / 3) as i64));
        let fetched = client
            .fetch("txn-mid", p, 0, Duration::from_millis(100), 1, 1024 * 1024)
            .await
            .unwrap();
        for r in fetched.records {
            let k: usize = std::str::from_utf8(&r.key).unwrap().parse().unwrap();
            assert!(seen_mid.insert(k));
        }
    }
    assert_eq!(seen_mid.len(), N);

    producer.close();
    client.close().await;
    cluster.close();
}

// ---------------------------------------------------------------------------
// client_resweep_test.go (1 test)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/client_resweep_test.go::TestAuditWriteTxnMarkersPreservesCoordinatorEpoch
#[test]
fn test_audit_write_txn_markers_preserves_coordinator_epoch() {
    let mut req = WriteTxnMarkersRequest::default();
    let mut m = WritableTxnMarker::default();
    m.producer_id = ProducerId(1);
    m.producer_epoch = 2;
    m.transaction_result = true;
    m.coordinator_epoch = 5;
    let mut mt = WritableTxnMarkerTopic::default();
    mt.name = TopicName(StrBytes::from_static_str("foo"));
    mt.partition_indexes = vec![0];
    m.topics.push(mt);
    req.markers.push(m);

    let leaders = HashMap::from([(("foo".to_string(), 0), 1)]);
    let sharded = shard_write_txn_markers(&req, &leaders);
    assert_eq!(sharded.len(), 1);
    let wreq = sharded.get(&1).unwrap();
    assert_eq!(wreq.markers.len(), 1);
    assert_eq!(wreq.markers[0].coordinator_epoch, 5);
    assert_eq!(wreq.markers[0].producer_id.0, 1);
    assert_eq!(wreq.markers[0].producer_epoch, 2);
    assert!(wreq.markers[0].transaction_result);
}
