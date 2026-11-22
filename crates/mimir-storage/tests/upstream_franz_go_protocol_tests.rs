//! Ported upstream `github.com/twmb/franz-go/pkg/kgo` protocol, compression, config, error
//! classification, partitioner, ring, legacy message set, record formatter, and metrics tests.

use std::collections::BTreeSet;
use std::io::{self, Cursor};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use kafka_protocol::records::Compression;
use mimir_storage::ingest::compression::{
    decompress_record_batch_with_limit, xerial_decode, CompressFlag, CompressionCodecConfig,
    CompressionError, Compressor, DecompressPool, Decompressor, KafkaCompression,
};
use mimir_storage::ingest::kafka_client::process_fetch_partition_batches;
use mimir_storage::ingest::kgo_retry::{
    is_decompress_err, is_retryable_broker_err, DecompressError, FirstReadEofKind,
};
use mimir_storage::ingest::partitioner::{
    murmur2_kafka, sarama_hasher, LeastBackupPartitioner, Partitioner, PartitionerRecord,
    TopicBackupIter, TopicPartitioner, UniformBytesPartitioner,
};
use mimir_storage::ingest::record_formatter::{
    FormattedFetchPartition, FormattedRecord, FormattedRecordHeader, ReaderReadErr, RecordAttrs,
    RecordFormatter, RecordReader,
};
use mimir_storage::ingest::ring::{PromiseRing, MIN_RING_CAP};
use mimir_storage::ingest::KafkaClientError;

// ---------------------------------------------------------------------------
// compression_test.go (8 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestCompressionCodecLZ4WithSpecifiedLevel
#[test]
fn test_compression_codec_lz4_with_specified_level() {
    let codec = CompressionCodecConfig::lz4().with_level(512);
    let comp = Compressor::new(&[codec]).unwrap().unwrap();
    let (out, used) = comp.compress(b"hello lz4 level 512", CompressFlag::NONE).unwrap();
    assert_eq!(used, KafkaCompression::Lz4);
    let got = Decompressor.decompress(&out, used).unwrap();
    assert_eq!(got, b"hello lz4 level 512");
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestNewCompressor
#[test]
fn test_new_compressor() {
    let cases: Vec<(Vec<CompressionCodecConfig>, bool)> = vec![
        (vec![CompressionCodecConfig { codec: -1, level: 0 }], true),
        (vec![CompressionCodecConfig { codec: 5, level: 0 }], true),
        (vec![CompressionCodecConfig::none()], false),
        (vec![CompressionCodecConfig::gzip(), CompressionCodecConfig::none()], false),
        (vec![CompressionCodecConfig::snappy(), CompressionCodecConfig::none()], false),
        (vec![CompressionCodecConfig::lz4()], false),
        (vec![CompressionCodecConfig::zstd()], false),
        (vec![CompressionCodecConfig::zstd(), CompressionCodecConfig::lz4()], false),
        (vec![CompressionCodecConfig::gzip().with_level(127)], false),
        (vec![CompressionCodecConfig::lz4().with_level(127)], false),
        (vec![CompressionCodecConfig::zstd().with_level(127)], false),
        (
            vec![
                CompressionCodecConfig::zstd(),
                CompressionCodecConfig::zstd(),
                CompressionCodecConfig::lz4(),
                CompressionCodecConfig::snappy(),
                CompressionCodecConfig::gzip().with_level(1),
            ],
            false,
        ),
    ];
    for (i, (codecs, fail)) in cases.into_iter().enumerate() {
        let res = Compressor::new(&codecs);
        assert_eq!(res.is_err(), fail, "case #{i}");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestGzipCompressionLevels
#[test]
fn test_gzip_compression_levels() {
    let chunk = b"abcdefghijklmno pqrs tuvwxy   z0123456789 the quick brown fox ";
    let mut input = Vec::with_capacity(chunk.len() * 2048);
    for _ in 0..2048 {
        input.extend_from_slice(chunk);
    }

    let c1 = Compressor::new(&[CompressionCodecConfig::gzip().with_level(1)]).unwrap().unwrap();
    let c9 = Compressor::new(&[CompressionCodecConfig::gzip().with_level(9)]).unwrap().unwrap();
    let c_def = Compressor::new(&[CompressionCodecConfig::gzip()]).unwrap().unwrap();
    let c_bad = Compressor::new(&[CompressionCodecConfig::gzip().with_level(127)]).unwrap().unwrap();

    let (out1, u1) = c1.compress(&input, CompressFlag::NONE).unwrap();
    let (out9, u9) = c9.compress(&input, CompressFlag::NONE).unwrap();
    let (out_def, udef) = c_def.compress(&input, CompressFlag::NONE).unwrap();
    let (out_bad, ubad) = c_bad.compress(&input, CompressFlag::NONE).unwrap();

    assert_eq!(u1, KafkaCompression::Gzip);
    assert_eq!(u9, KafkaCompression::Gzip);
    assert_eq!(udef, KafkaCompression::Gzip);
    assert_eq!(ubad, KafkaCompression::Gzip);
    assert_ne!(out1, out9, "BestSpeed and BestCompression must differ");
    assert_eq!(out_bad, out_def, "invalid level must fall back to default");

    for (i, out) in [out1, out9, out_def, out_bad].into_iter().enumerate() {
        let got = Decompressor.decompress(&out, KafkaCompression::Gzip).unwrap();
        assert_eq!(got, input, "roundtrip #{i}");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestCompressDecompress
#[test]
fn test_compress_decompress() {
    let inputs = [
        b"aB3d".to_vec(),
        b"aB3dEf90Gh12Ij34Kl56Mn78Op90Qr12".to_vec(),
        b"0123456789abcdef".repeat(16),
    ];
    let codec_sets: Vec<Vec<CompressionCodecConfig>> = vec![
        vec![CompressionCodecConfig::none()],
        vec![CompressionCodecConfig::gzip()],
        vec![CompressionCodecConfig::snappy()],
        vec![CompressionCodecConfig::lz4()],
        vec![CompressionCodecConfig::zstd()],
        vec![CompressionCodecConfig::zstd(), CompressionCodecConfig::lz4()],
    ];

    for flag in [CompressFlag::NONE, CompressFlag::DISABLE_ZSTD] {
        for codecs in &codec_sets {
            let Some(comp) = Compressor::new(codecs).unwrap() else {
                continue;
            };
            let mut handles = Vec::new();
            for _ in 0..3 {
                let comp = comp.clone();
                let inputs = inputs.clone();
                handles.push(thread::spawn(move || {
                    for inp in &inputs {
                        let (c, used) = comp.compress(inp, flag).unwrap();
                        if flag == CompressFlag::DISABLE_ZSTD {
                            assert_ne!(used, KafkaCompression::Zstd);
                        }
                        let dec = Decompressor.decompress(&c, used).unwrap();
                        assert_eq!(&dec, inp);
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        }
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestDecompressBombBounded
#[test]
fn test_decompress_bomb_bounded() {
    let max_bound = 1 << 20;
    let huge = vec![0u8; 8 << 20];
    for codec in [
        KafkaCompression::Gzip,
        KafkaCompression::Snappy,
        KafkaCompression::Lz4,
        KafkaCompression::Zstd,
    ] {
        let comp = Compressor::new(&[CompressionCodecConfig {
            codec: match codec {
                KafkaCompression::None => 0,
                KafkaCompression::Gzip => 1,
                KafkaCompression::Snappy => 2,
                KafkaCompression::Lz4 => 3,
                KafkaCompression::Zstd => 4,
            },
            level: 0,
        }])
        .unwrap()
        .unwrap();
        let (compressed, used) = comp.compress(&huge, CompressFlag::NONE).unwrap();
        assert_eq!(used, codec);
        let mut b = Bytes::from(compressed);
        let err = decompress_record_batch_with_limit(&mut b, codec.to_protocol(), max_bound)
            .unwrap_err();
        assert_eq!(
            err.downcast_ref::<CompressionError>(),
            Some(&CompressionError::DecompressedSizeExceeded { limit: max_bound })
        );
    }

    let small = b"abc123 ".repeat(512);
    for codec in [
        KafkaCompression::Gzip,
        KafkaCompression::Snappy,
        KafkaCompression::Lz4,
        KafkaCompression::Zstd,
    ] {
        let comp = Compressor::new(&[CompressionCodecConfig {
            codec: match codec {
                KafkaCompression::None => 0,
                KafkaCompression::Gzip => 1,
                KafkaCompression::Snappy => 2,
                KafkaCompression::Lz4 => 3,
                KafkaCompression::Zstd => 4,
            },
            level: 0,
        }])
        .unwrap()
        .unwrap();
        let (compressed, used) = comp.compress(&small, CompressFlag::NONE).unwrap();
        let mut b = Bytes::from(compressed);
        let got = decompress_record_batch_with_limit(&mut b, used.to_protocol(), max_bound).unwrap();
        assert_eq!(got.as_ref(), small.as_slice());
    }

    let chunk = snap::raw::Encoder::new()
        .compress_vec(&vec![0u8; 768 << 10])
        .unwrap();
    let mut xer = Vec::new();
    xer.extend_from_slice(b"\x82SNAPPY\x00");
    xer.extend_from_slice(&[0u8; 8]);
    for _ in 0..2 {
        xer.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
        xer.extend_from_slice(&chunk);
    }
    let mut b = Bytes::from(xer);
    let err = decompress_record_batch_with_limit(&mut b, Compression::Snappy, max_bound).unwrap_err();
    assert_eq!(
        err.downcast_ref::<CompressionError>(),
        Some(&CompressionError::DecompressedSizeExceeded { limit: max_bound })
    );
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestDecompressPools
#[test]
fn test_decompress_pools() {
    struct TestPool {
        len: usize,
        cap: usize,
        given_ptr: Mutex<Option<usize>>,
        puts: Mutex<Vec<Vec<u8>>>,
    }
    impl DecompressPool for TestPool {
        fn get_decompress_bytes(&self, _compressed: &[u8], _codec: KafkaCompression) -> Vec<u8> {
            let mut v = vec![0xAAu8; self.cap];
            v.truncate(self.len);
            *self.given_ptr.lock().unwrap() = Some(v.as_ptr() as usize);
            v
        }
        fn put_decompress_bytes(&self, buf: Vec<u8>) {
            self.puts.lock().unwrap().push(buf);
        }
    }

    let in_a = b"batch A 0123456789 ".repeat(4000);
    let xerial_chunk = snap::raw::Encoder::new().compress_vec(&in_a).unwrap();
    let mut xerial_src = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01".to_vec();
    xerial_src.extend_from_slice(&(xerial_chunk.len() as u32).to_be_bytes());
    xerial_src.extend_from_slice(&xerial_chunk);

    let mut inputs = Vec::new();
    for (name, codec_id, codec) in [
        ("gzip", 1, KafkaCompression::Gzip),
        ("snappy", 2, KafkaCompression::Snappy),
        ("lz4", 3, KafkaCompression::Lz4),
        ("zstd", 4, KafkaCompression::Zstd),
    ] {
        let comp = Compressor::new(&[CompressionCodecConfig { codec: codec_id, level: 0 }])
            .unwrap()
            .unwrap();
        let (src, _) = comp.compress(&in_a, CompressFlag::NONE).unwrap();
        inputs.push((name, codec, src));
    }
    inputs.push(("snappy-xerial", KafkaCompression::Snappy, xerial_src));

    let garbage = [0xffu8, 0x00, 0x13, 0x37].repeat(8);
    for (pt_name, len, cap, corrupt) in [
        ("nopool", 0usize, 0usize, false),
        ("fits", 0, 2 * in_a.len(), false),
        ("fits-with-len", 64, 2 * in_a.len(), false),
        ("too-small", 0, 16, false),
        ("corrupt", 16, 2 * in_a.len(), true),
    ] {
        for (cname, codec, src) in &inputs {
            let pool = TestPool {
                len,
                cap,
                given_ptr: Mutex::new(None),
                puts: Mutex::new(Vec::new()),
            };
            let pool_ref: Option<&dyn DecompressPool> = if cap > 0 { Some(&pool) } else { None };
            if corrupt {
                let mut bad = garbage.clone();
                if *cname == "snappy-xerial" {
                    let mut x = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01".to_vec();
                    x.extend_from_slice(&garbage);
                    bad = x;
                }
                let res = Decompressor.decompress_with_limit(&bad, *codec, usize::MAX, pool_ref);
                assert!(res.is_err(), "{pt_name}/{cname} corrupt must fail");
                let puts = pool.puts.lock().unwrap();
                assert_eq!(puts.len(), 1);
                assert_eq!(puts[0].len(), len);
                assert_eq!(puts[0].as_ptr() as usize, pool.given_ptr.lock().unwrap().unwrap());
                assert!(puts[0].iter().all(|&b| b == 0), "must be zeroed before put");
                continue;
            }
            let got = Decompressor
                .decompress_with_limit(src, *codec, usize::MAX, pool_ref)
                .unwrap();
            assert_eq!(got, in_a, "{pt_name}/{cname}");
            if cap > 0 {
                assert!(pool.puts.lock().unwrap().is_empty());
                let in_pool = got.as_ptr() as usize == pool.given_ptr.lock().unwrap().unwrap();
                assert_eq!(in_pool, cap >= in_a.len(), "{pt_name}/{cname}");
            }
        }
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::TestDecompressZstdHugeClaim
#[test]
fn test_decompress_zstd_huge_claim() {
    let mut frame = vec![0x28u8, 0xb5, 0x2f, 0xfd, 0xc0, 0x00];
    frame.extend_from_slice(&(8u64 << 30).to_le_bytes());
    let err = Decompressor
        .decompress(&frame, KafkaCompression::Zstd)
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CompressionError>(),
            Some(CompressionError::DecompressedSizeExceeded { .. })
        ),
        "expected DecompressedSizeExceeded, got {err:?}"
    );
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/compression_test.go::Test_xerialDecode
#[test]
fn test_xerial_decode() {
    let cases = [
        (
            "Compressed data ok",
            "glNOQVBQWQAAAAABAAAAAQAAAA8NMEhlbGxvLCBXb3JsZCE=",
            Some(b"Hello, World!".as_slice()),
        ),
        (
            "Compressed data without header",
            "/wYAAHNOYVBwWQERAACChVPDSGVsbG8sIFdvcmxkIQ==",
            None,
        ),
        (
            "Compressed data less than minimum length, malformed",
            "glNOQVBQWQAAAAABAAAAAQAAAA==",
            None,
        ),
        (
            "Compressed data not the advertised length",
            "glNOQVBQWQAAAAABAAAAAQAAAA8NMEhlbGxvLCBXb3Js",
            None,
        ),
    ];
    for (name, b64, want) in cases {
        let data = base64::engine::general_purpose::STANDARD.decode(b64).unwrap();
        let res = xerial_decode(None, &data, 1 << 20);
        match want {
            Some(exp) => assert_eq!(res.unwrap().as_slice(), exp, "{name}"),
            None => assert!(res.is_err(), "{name} should fail"),
        }
    }
}

// ---------------------------------------------------------------------------
// errors_classify_test.go (2 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/errors_classify_test.go::TestUnexpectedEOFRetriable
#[test]
fn test_unexpected_eof_retriable() {
    let ueof = KafkaClientError::Io(io::Error::from(io::ErrorKind::UnexpectedEof));
    assert!(is_retryable_broker_err(&ueof));
    assert!(!is_retryable_broker_err(&KafkaClientError::FirstReadEof(
        FirstReadEofKind::Sasl.to_string(),
    )));
    assert!(!is_retryable_broker_err(&KafkaClientError::FirstReadEof(
        "unexpected eof".into(),
    )));
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/errors_classify_test.go::TestIsDecompressErr
#[test]
fn test_is_decompress_err() {
    #[derive(Debug)]
    struct Wrapped(DecompressError);
    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "process failed: {}", self.0)
        }
    }
    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    let direct = DecompressError {
        source: Box::new(io::Error::from(io::ErrorKind::UnexpectedEof)),
    };
    assert!(is_decompress_err(Some(&direct)));

    let wrapped = Wrapped(DecompressError {
        source: Box::new(io::Error::from(io::ErrorKind::UnexpectedEof)),
    });
    assert!(is_decompress_err(Some(&wrapped)));

    let plain = io::Error::from(io::ErrorKind::UnexpectedEof);
    assert!(!is_decompress_err(Some(&plain)));
    assert!(!is_decompress_err(None));
}

// ---------------------------------------------------------------------------
// partitioner_test.go (4 tests)
// ---------------------------------------------------------------------------

struct TestBackupIter {
    backups: Vec<i64>,
}

impl TopicBackupIter for TestBackupIter {
    fn next(&mut self) -> (usize, i64) {
        let last = self.backups.len() - 1;
        let b = self.backups.pop().unwrap();
        (last, b)
    }
    fn rem(&self) -> usize {
        self.backups.len()
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/partitioner_test.go::TestLeastBackupPicksLeast
#[test]
fn test_least_backup_picks_least() {
    let mut tp = LeastBackupPartitioner.for_topic("t");
    let rec = PartitionerRecord::default();
    for i in 0..200 {
        tp.on_new_batch();
        let mut iter = TestBackupIter { backups: vec![3, 0, 7] };
        let pick = tp.partition_by_backup(&rec, 3, &mut iter);
        assert_eq!(pick, 1, "iteration {i}: picked {pick}, want 1");
    }

    let mut seen = BTreeSet::new();
    for _ in 0..300 {
        tp.on_new_batch();
        let mut iter = TestBackupIter { backups: vec![2, 2, 2] };
        seen.insert(tp.partition_by_backup(&rec, 3, &mut iter));
    }
    for p in 0..3 {
        assert!(seen.contains(&p), "never picked partition {p}");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/partitioner_test.go::TestAuditUniformBytesAdaptiveShrinkStalePartition
#[test]
fn test_audit_uniform_bytes_adaptive_shrink_stale_partition() {
    let ubp = UniformBytesPartitioner::new(1 << 30, true, false, None);
    let mut p = ubp.for_topic_concrete("t");
    p.on_part = 5;
    p.bytes = 0;

    let rec = PartitionerRecord::default();
    let mut iter = TestBackupIter { backups: vec![-1, 10, 20] };
    let pick = p.partition_by_backup(&rec, 3, &mut iter);
    assert!((0..3).contains(&pick), "out-of-range pick {pick}");

    p.on_part = -1;
    p.bytes = 0;
    let mut iter = TestBackupIter { backups: vec![1, 2, 3] };
    let pick = p.partition_by_backup(&rec, 3, &mut iter);
    assert!((0..3).contains(&pick), "out-of-range normal pick {pick}");
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/partitioner_test.go::TestMurmur2KafkaVectors
#[test]
fn test_murmur2_kafka_vectors() {
    let cases = [
        ("21", -973932308i32),
        ("foobar", -790332482),
        ("a-little-bit-long-string", -985981536),
        ("a-little-bit-longer-string", -1486304829),
        ("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
        ("abc", 479470107),
    ];
    for (inp, want) in cases {
        let got = murmur2_kafka(inp.as_bytes()) as i32;
        assert_eq!(got, want, "murmur2({inp:?})");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/partitioner_test.go::TestSaramaHasherPinnedPlacement
#[test]
fn test_sarama_hasher_pinned_placement() {
    let h = sarama_hasher(|_| 0x8000_0001);
    let want = if usize::BITS == 32 { 7 } else { 9 };
    assert_eq!(h(&[], 10), want);

    let h2 = sarama_hasher(|_| 12345);
    assert_eq!(h2(&[], 10), 5);
}

// ---------------------------------------------------------------------------
// ring_test.go (3 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/ring_test.go::TestRing
#[test]
fn test_ring() {
    let assert_push = |r: &PromiseRing<i32>, elem: i32, exp_first: bool, exp_dead: bool| {
        assert_eq!(r.push(elem), (exp_first, exp_dead));
    };
    let assert_drop = |r: &PromiseRing<i32>, exp_next: Option<i32>, exp_more: bool, exp_dead: bool| {
        assert_eq!(r.drop_peek(), (exp_next, exp_more, exp_dead));
    };

    {
        let r = PromiseRing::new();
        assert_push(&r, 1, true, false);
        assert_push(&r, 2, false, false);
        assert_push(&r, 3, false, false);
        assert_drop(&r, Some(2), true, false);
        assert_drop(&r, Some(3), true, false);
        assert_drop(&r, None, false, false);
    }

    {
        let r = PromiseRing::new();
        assert_push(&r, 1, true, false);
        for i in 2..10 {
            assert_push(&r, i, false, false);
            assert_drop(&r, Some(i), true, false);
        }
        assert_drop(&r, None, false, false);
    }

    {
        let r = PromiseRing::new();
        for i in 1..=10 {
            assert_push(&r, i, i == 1, false);
        }
        assert!(r.capacity() > MIN_RING_CAP);
        for i in 1..=9 {
            assert_drop(&r, Some(i + 1), true, false);
        }
        assert_drop(&r, None, false, false);
        assert_eq!(r.capacity(), MIN_RING_CAP);
    }

    {
        let r = PromiseRing::new();
        assert_push(&r, 1, true, false);
        assert_push(&r, 2, false, false);
        assert_push(&r, 3, false, false);
        r.die();
        assert_push(&r, 4, false, true);
        assert_drop(&r, Some(2), true, true);
    }

    {
        let r = PromiseRing::new();
        for i in 1..=10 {
            assert_push(&r, i, i == 1, false);
        }
        for i in 11..=1000 {
            assert_push(&r, i, false, false);
            assert_drop(&r, Some(i - 9), true, false);
        }
        assert!(r.capacity() <= 32, "cap {}", r.capacity());
    }

    {
        let r = PromiseRing::new();
        for i in 1..=1000 {
            assert_push(&r, i, i == 1, false);
        }
        assert!(r.capacity() >= 1000);
        for _ in 1..=996 {
            r.drop_peek();
        }
        assert_eq!(r.capacity(), MIN_RING_CAP);
        assert_drop(&r, Some(998), true, false);
        assert_drop(&r, Some(999), true, false);
        assert_drop(&r, Some(1000), true, false);
        assert_drop(&r, None, false, false);
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/ring_test.go::TestRingMaxLen
#[test]
fn test_ring_max_len() {
    let r = Arc::new(PromiseRing::with_max_len(3));
    for i in 0..3 {
        let (first, dead) = r.push(i);
        assert!(!dead);
        assert_eq!(first, i == 0);
    }
    let (tx_started, rx_started) = std::sync::mpsc::channel();
    let (tx_done, rx_done) = std::sync::mpsc::channel();
    let r2 = Arc::clone(&r);
    let h = thread::spawn(move || {
        tx_started.send(()).unwrap();
        r2.push(99);
        tx_done.send(()).unwrap();
    });
    rx_started.recv().unwrap();
    assert!(rx_done.recv_timeout(Duration::from_millis(80)).is_err());
    r.drop_peek();
    rx_done.recv_timeout(Duration::from_secs(1)).unwrap();
    h.join().unwrap();
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/ring_test.go::TestRingMaxLenDie
#[test]
fn test_ring_max_len_die() {
    let r = Arc::new(PromiseRing::with_max_len(1));
    r.push(0);
    let (tx_done, rx_done) = std::sync::mpsc::channel();
    let r2 = Arc::clone(&r);
    let h = thread::spawn(move || {
        let (_, dead) = r2.push(99);
        tx_done.send(dead).unwrap();
    });
    thread::sleep(Duration::from_millis(80));
    r.die();
    let dead = rx_done.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(dead);
    h.join().unwrap();
}

// ---------------------------------------------------------------------------
// legacy_message_set_test.go (2 tests)
// ---------------------------------------------------------------------------

fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if (crc & 1) != 0 {
                0xedb8_8320 ^ (crc >> 1)
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn encode_legacy_msg(magic: i8, offset: i64, attrs: i8, ts: i64, value: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(magic as u8);
    body.push(attrs as u8);
    if magic == 1 {
        body.extend_from_slice(&ts.to_be_bytes());
    }
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&(value.len() as i32).to_be_bytes());
    body.extend_from_slice(value);

    let mut msg = Vec::new();
    msg.extend_from_slice(&offset.to_be_bytes());
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&crc32_ieee(&body).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

fn wrap_legacy_set(magic: i8, wrapper_offset: i64, inner: &[Vec<u8>]) -> Vec<u8> {
    let mut raw = Vec::new();
    for item in inner {
        raw.extend_from_slice(item);
    }
    use std::io::Write;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&raw).unwrap();
    let compressed = gz.finish().unwrap();
    encode_legacy_msg(magic, wrapper_offset, 0x01, 0, &compressed)
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/legacy_message_set_test.go::TestLegacyMessageSetInnerOffsets
#[test]
fn test_legacy_message_set_inner_offsets() {
    let cases = [
        (
            "v1 compacted gap",
            10i64,
            wrap_legacy_set(
                1,
                20,
                &[
                    encode_legacy_msg(1, 0, 0, 0, b"a"),
                    encode_legacy_msg(1, 1, 0, 0, b"b"),
                    encode_legacy_msg(1, 2, 0, 0, b"c"),
                    encode_legacy_msg(1, 10, 0, 0, b"d"),
                ],
            ),
            Some(vec![10i64, 11, 12, 20]),
        ),
        (
            "v1 contiguous",
            10,
            wrap_legacy_set(
                1,
                12,
                &[
                    encode_legacy_msg(1, 0, 0, 0, b"a"),
                    encode_legacy_msg(1, 1, 0, 0, b"b"),
                    encode_legacy_msg(1, 2, 0, 0, b"c"),
                ],
            ),
            Some(vec![10, 11, 12]),
        ),
        (
            "v1 mid-set resume",
            12,
            wrap_legacy_set(
                1,
                20,
                &[
                    encode_legacy_msg(1, 0, 0, 0, b"a"),
                    encode_legacy_msg(1, 1, 0, 0, b"b"),
                    encode_legacy_msg(1, 2, 0, 0, b"c"),
                    encode_legacy_msg(1, 10, 0, 0, b"d"),
                ],
            ),
            Some(vec![12, 20]),
        ),
        (
            "v1 wrapper offset zero",
            0,
            wrap_legacy_set(
                1,
                0,
                &[
                    encode_legacy_msg(1, 0, 0, 0, b"a"),
                    encode_legacy_msg(1, 1, 0, 0, b"b"),
                ],
            ),
            Some(vec![0, 1]),
        ),
        (
            "v1 wrapper below last inner",
            0,
            wrap_legacy_set(
                1,
                5,
                &[
                    encode_legacy_msg(1, 0, 0, 0, b"a"),
                    encode_legacy_msg(1, 10, 0, 0, b"b"),
                ],
            ),
            None,
        ),
        (
            "v0 compacted gap",
            10,
            wrap_legacy_set(
                0,
                20,
                &[
                    encode_legacy_msg(0, 10, 0, 0, b"a"),
                    encode_legacy_msg(0, 11, 0, 0, b"b"),
                    encode_legacy_msg(0, 12, 0, 0, b"c"),
                    encode_legacy_msg(0, 20, 0, 0, b"d"),
                ],
            ),
            Some(vec![10, 11, 12, 20]),
        ),
    ];

    for (name, ask, set, want) in cases {
        let res = process_fetch_partition_batches(Bytes::from(set), ask);
        match want {
            None => assert!(res.is_err(), "{name} expected err"),
            Some(want_offsets) => {
                let (recs, next, _) = res.unwrap();
                let got: Vec<i64> = recs.iter().map(|r| r.offset).collect();
                assert_eq!(got, want_offsets, "{name}");
                assert_eq!(next, *want_offsets.last().unwrap() + 1, "{name}");
            }
        }
    }
}

fn encode_v2_batch(first_offset: i64, last_offset_delta: i32, num_records: i32, records: &[u8]) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&first_offset.to_be_bytes());
    raw.extend_from_slice(&0i32.to_be_bytes());
    raw.extend_from_slice(&0i32.to_be_bytes());
    raw.push(2);
    raw.extend_from_slice(&0u32.to_be_bytes());
    raw.extend_from_slice(&0i16.to_be_bytes());
    raw.extend_from_slice(&last_offset_delta.to_be_bytes());
    raw.extend_from_slice(&0i64.to_be_bytes());
    raw.extend_from_slice(&0i64.to_be_bytes());
    raw.extend_from_slice(&(-1i64).to_be_bytes());
    raw.extend_from_slice(&(-1i16).to_be_bytes());
    raw.extend_from_slice(&(-1i32).to_be_bytes());
    raw.extend_from_slice(&num_records.to_be_bytes());
    raw.extend_from_slice(records);
    let batch_len = (raw.len() - 12) as u32;
    raw[8..12].copy_from_slice(&batch_len.to_be_bytes());
    let crc = crc32c::crc32c(&raw[21..]);
    raw[17..21].copy_from_slice(&crc.to_be_bytes());
    raw
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/legacy_message_set_test.go::TestEmptyBatchClaimingRecords
#[test]
fn test_empty_batch_claiming_records() {
    let corrupt = encode_v2_batch(10, 4, 5, &[]);
    assert!(process_fetch_partition_batches(Bytes::from(corrupt), 10).is_err());

    let empty_ok = encode_v2_batch(10, 4, 0, &[]);
    let (recs, next, _) = process_fetch_partition_batches(Bytes::from(empty_ok), 10).unwrap();
    assert!(recs.is_empty());
    assert_eq!(next, 15);
}

// ---------------------------------------------------------------------------
// record_formatter_test.go (7 tests)
// ---------------------------------------------------------------------------

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestRecordFormatter
#[test]
fn test_record_formatter() {
    let r = FormattedRecord {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        headers: vec![
            FormattedRecordHeader { key: "H1".into(), value: b"V1".to_vec() },
            FormattedRecordHeader { key: "h2".into(), value: b"v2".to_vec() },
        ],
        timestamp_ms: 17_000,
        topic: "topictopictopictopictopict".into(),
        partition: 3,
        attrs: RecordAttrs(0b1011_0011),
        producer_epoch: 1,
        producer_id: 791,
        leader_epoch: -1,
        offset: 343,
        delivery_count: 3,
        acquisition_deadline_ms: Some(42_000),
    };
    let p = FormattedFetchPartition {
        high_watermark: 999,
        last_stable_offset: 666,
        log_start_offset: 333,
    };

    type FmtCase<'a> = (&'a str, &'a [u8], Option<&'a [u8]>);
    let cases: &[FmtCase<'_>] = &[
        ("%v", b"value", None),
        ("%v{}", b"value", None),
        (
            "%T{hex16}%t %V{ascii} %v %V{little16} %k %K{big32} %o",
            b"001atopictopictopictopictopict 5 value \x05\x00 key \x00\x00\x00\x03 343",
            None,
        ),
        ("%[ %| %]", b"<nil> <nil> <nil>", Some(b"333 666 999")),
        ("%d{strftime## %a ##} %d %d{ascii}", b" Thu  17000 17000", None),
        (
            "%T{ascii} %T{hex64} %T{hex32} %T{hex16} %T{hex8} %T{hex4} %T{hex}",
            b"26 000000000000001a 0000001a 001a 1a a 1a",
            None,
        ),
        (
            "%K{big64} %K{big32} %K{big16} %K{big8}",
            b"\x00\x00\x00\x00\x00\x00\x00\x03 \x00\x00\x00\x03 \x00\x03 \x03",
            None,
        ),
        (
            "%K{little64} %K{little32} %K{little16} %K{little8}",
            b"\x03\x00\x00\x00\x00\x00\x00\x00 \x03\x00\x00\x00 \x03\x00 \x03",
            None,
        ),
        (r"\t\r\n\\\x00 %{%}%%", b"\t\r\n\\\x00 {}%", None),
        (r"\x00\x7f\x80\xfe\xff", b"\x00\x7f\x80\xfe\xff", None),
        (
            "%T %K %V %H %p %o %e %i %x %y",
            b"26 3 5 2 3 343 -1 1 791 1",
            Some(b"26 3 5 2 3 343 -1 2 791 1"),
        ),
        ("%k{base64} %k{hex}", b"a2V5 6b6579", None),
        ("%v{base64} %v{base64raw}", b"dmFsdWU= dmFsdWU", None),
        ("%H %h{ %K{ascii} %k %v %V } %k %v", b"2  2 H1 V1 2  2 h2 v2 2  key value", None),
        ("%a{compression}", b"lz4", None),
        (
            "%a{compression;number} %a{transactional-bit;bool} %a{control-bit;hex8} %a{timestamp-type;hex8}",
            b"3 true 01 ff",
            None,
        ),
        (
            "%a{compression} %a{transactional-bit} %a{control-bit} %a{timestamp-type}",
            b"lz4 1 1 -1",
            None,
        ),
        ("%D", b"3", None),
        ("%D{hex8}", b"03", None),
        ("%A", b"42000", None),
        ("%A{strftime[%F]}", b"1970-01-01", None),
    ];

    for (layout, exp_r, exp_p_opt) in cases {
        let f = RecordFormatter::new(layout).unwrap();
        let mut got_r = Vec::new();
        f.append_record(&mut got_r, Some(&r));
        assert_eq!(got_r.as_slice(), *exp_r, "R[{layout}]");
        let mut got_p = Vec::new();
        f.append_partition_record(&mut got_p, Some(&p), Some(&r));
        assert_eq!(got_p.as_slice(), exp_p_opt.unwrap_or(exp_r), "P[{layout}]");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestRecordFormatterUnpack
#[test]
fn test_record_formatter_unpack() {
    let cases: &[(&str, &[u8], &str)] = &[
        ("%v{unpack#c#}", b"foo", "f"),
        ("%v{unpack#cbs#}", b"foo", "f111o"),
        ("%v{unpack#cBs#}", b"foo", "f111o"),
        ("%v{unpack[ch]}", b"f\xff\xff", "f-1"),
        ("%v{unpack[cH]}", b"f\xff\xff", "f65535"),
        ("%v{unpack[ci]}", b"f\xff\xff\xff\xff\xff\xff\xff\xff", "f-1"),
        ("%v{unpack[cI]}", b"f\xff\xff\xff\xff\xff\xff\xff\xff", "f4294967295"),
        ("%v{unpack{{{cq}}}}", b"f\xff\xff\xff\xff\xff\xff\xff\xff", "f-1"),
        ("%v{unpack((cQ))}", b"f\xff\xff\xff\xff\xff\xff\xff\xff", "f18446744073709551615"),
        (
            "%v{unpack[x<xH.xx>Hxx.xxHxx.xx<xxHxx$]}",
            b"\x00\x01 \x00\x01 \x00\x01 \x00\x01",
            "256 1 1 256",
        ),
    ];
    for (layout, inp, exp) in cases {
        let f = RecordFormatter::new(layout).unwrap();
        let mut got = Vec::new();
        let rec = FormattedRecord { value: inp.to_vec(), ..Default::default() };
        f.append_record(&mut got, Some(&rec));
        assert_eq!(String::from_utf8_lossy(&got), *exp, "{layout}");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestRecordReader
#[test]
fn test_record_reader() {
    let cases: Vec<(&str, &[u8], Vec<FormattedRecord>, bool)> = vec![
        ("%v", b"foo bar biz\nbaz", vec![FormattedRecord::string("foo bar biz\nbaz")], false),
        ("%v{}", b"foo bar biz\nbaz", vec![FormattedRecord::string("foo bar biz\nbaz")], false),
        ("%k %v", b"foo bar biz\nbaz", vec![FormattedRecord::key_string("foo", "bar biz\nbaz")], false),
        (
            "%k %v\n",
            b"foo bar biz\nbaz \n biz\n",
            vec![
                FormattedRecord::key_string("foo", "bar biz"),
                FormattedRecord::key_string("baz", ""),
                FormattedRecord::key_string("", "biz"),
            ],
            false,
        ),
        (
            "%t %k %v",
            b"foo bar biz",
            vec![FormattedRecord {
                topic: "foo".into(),
                key: b"bar".to_vec(),
                value: b"biz".to_vec(),
                ..Default::default()
            }],
            false,
        ),
        (
            "%T%t %K%k %V{byte}%v",
            b"3foo 3bar \x03biz",
            vec![FormattedRecord {
                topic: "foo".into(),
                key: b"bar".to_vec(),
                value: b"biz".to_vec(),
                ..Default::default()
            }],
            false,
        ),
        ("%K{ascii}%k", b"3foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{hex64}%k", b"0000000000000003foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{hex32}%k", b"00000003foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{hex16}%k", b"0003foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{hex8}%k", b"03foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{hex4}%k", b"3foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{big64}%k", b"\x00\x00\x00\x00\x00\x00\x00\x03foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{little64}%k", b"\x03\x00\x00\x00\x00\x00\x00\x00foo", vec![FormattedRecord::key_string("foo", "")], false),
        ("%K{3}%kgap%V{3}%v", b"foogapbar", vec![FormattedRecord::key_string("foo", "bar")], false),
        (r"\t\r\n\\\x00 %{%}%% %v", b"\t\r\n\\\x00 {}% foo", vec![FormattedRecord::string("foo")], false),
        (
            "%H{2}%V{ascii}%v%h{%V%v%K%k}",
            b"3foo1v1k2vv2kk",
            vec![FormattedRecord {
                value: b"foo".to_vec(),
                headers: vec![
                    FormattedRecordHeader { key: "k".into(), value: b"v".to_vec() },
                    FormattedRecordHeader { key: "kk".into(), value: b"vv".to_vec() },
                ],
                ..Default::default()
            }],
            false,
        ),
        ("%V{bool}%v", b"truee", vec![FormattedRecord::string("e")], false),
        ("%V %v{hex}asdf", b"6 6b6579asdf", vec![FormattedRecord::string("key")], false),
        ("%V %v{base64}asdf", b"4 a2V5asdf", vec![FormattedRecord::string("key")], false),
        (
            "%p %o %e %d %x %y{hex8}",
            b"3 4 5 17000 6 0a",
            vec![FormattedRecord {
                partition: 3,
                offset: 4,
                leader_epoch: 5,
                timestamp_ms: 17000,
                producer_id: 6,
                producer_epoch: 10,
                ..Default::default()
            }],
            false,
        ),
        (
            r"%v{re[\d{2}]}",
            b"2345",
            vec![FormattedRecord::string("23"), FormattedRecord::string("45")],
            false,
        ),
        (
            r"%K{3}%v{re[.*?\d]}%k",
            b"abcdefg[1aaad2bbb",
            vec![
                FormattedRecord::key_string("aaa", "abcdefg[1"),
                FormattedRecord::key_string("bbb", "d2"),
            ],
            false,
        ),
        ("%v\n", b"foo", vec![], true),
        ("%V{4}%v", b"fo", vec![], true),
        ("%o{hex8}", b"az", vec![], true),
        (
            r"%v{json} %k{json}",
            br#"{"foo": "bar"} true"#,
            vec![FormattedRecord::key_string("true", r#"{"foo":"bar"}"#)],
            false,
        ),
        (
            r"%v{json}%k{json}",
            b"3null",
            vec![FormattedRecord::key_string("null", "3")],
            false,
        ),
    ];

    for (layout, inp, exp, exp_err) in cases {
        let mut r = RecordReader::new(Cursor::new(inp), layout).unwrap();
        for (i, want) in exp.iter().enumerate() {
            let got = r.read_record().unwrap_or_else(|e| panic!("{layout} #{i}: {e}"));
            assert_eq!(&got, want, "{layout} #{i}");
        }
        let end = r.read_record();
        if exp_err {
            assert!(!matches!(end, Err(ReaderReadErr::Eof)), "{layout} expected non-EOF err, got {end:?}");
        } else {
            assert_eq!(end, Err(ReaderReadErr::Eof), "{layout} expected EOF");
        }
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestRecordReaderJson
#[test]
fn test_record_reader_json() {
    let nested_obj = format!("{}{{}}{}", "{\"f\":".repeat(1000), "}".repeat(1000));
    let nested_arr = format!("{}{}", "[".repeat(1000), "]".repeat(1000));
    let tests: Vec<(&str, bool)> = vec![
        ("", false),
        ("   ", false),
        (" z", false),
        (" 1  ", true),
        ("   {}", true),
        ("   []", true),
        ("   true", true),
        ("   null", true),
        (r#"   "n""#, true),
        (r#""foo""#, true),
        (r#" "\uaaaa" "#, true),
        (r#" "\"#, false),
        (r#" "\z"#, false),
        (r#" "\uazaa" "#, false),
        ("1", true),
        ("  0 ", true),
        (" 0e1 ", true),
        (" 0.1 ", true),
        ("1.", false),
        ("-0", true),
        ("1e6", true),
        ("-1e+6", true),
        ("-z", false),
        ("-", false),
        ("1e", false),
        ("1e+", false),
        (" 1e.1 ", false),
        ("{}", true),
        (r#"{"foo": 3}"#, true),
        (&nested_obj, true),
        (r#"{"foo": 2,"fudge}"#, false),
        (r#"{{"foo": }}"#, false),
        ("[]", true),
        ("[[]]", true),
        ("[ 1, {}]", true),
        (&nested_arr, true),
        ("[", false),
        ("[1,", false),
        ("[1a]", false),
        ("true", true),
        ("tru", false),
        ("false", true),
        ("fals", false),
        ("null ", true),
        ("nul", false),
    ];

    for (inp, valid) in tests {
        let mut r = RecordReader::new(Cursor::new(inp.as_bytes()), "%v{json}").unwrap();
        let res = r.read_record();
        assert_eq!(res.is_ok(), valid, "json input {inp:?}: got {res:?}");
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestNewRecordReaderRejectsBadLayouts
#[test]
fn test_new_record_reader_rejects_bad_layouts() {
    for layout in [
        "%q",
        "%qx",
        "%t%T",
        "%v%V",
        "%k%K",
        "%p{3}",
        "%T{3}",
        "%p{3}%o{4}",
    ] {
        assert!(
            RecordReader::new(Cursor::new(b""), layout).is_err(),
            "layout {layout:?} should be rejected"
        );
    }
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestRecordReaderOverflowingSizeNoPanic
#[test]
fn test_record_reader_overflowing_size_no_panic() {
    let mut r = RecordReader::new(Cursor::new(b"18446744073709551615x"), "%V%v").unwrap();
    assert!(r.read_record().is_err());
}

// Upstream: github.com/twmb/franz-go/pkg/kgo/record_formatter_test.go::TestRecordReaderTruncatedFixedSizeNoPanic
#[test]
fn test_record_reader_truncated_fixed_size_no_panic() {
    let cases: &[(&str, &[u8])] = &[
        ("%p{big32}%o{big64}", b"\x00\x00\x00\x01"),
        ("%p{big32}%o{little64}", b"\x00\x00\x00\x01"),
        ("%o{big64}%p{big32}", b"\x00\x00\x00\x00\x00\x00\x00\x01"),
        ("%k:%e{big16}", b"key:"),
        ("%k:%p{byte}", b"key:"),
    ];
    for (layout, inp) in cases {
        let mut r = RecordReader::new(Cursor::new(inp), layout).unwrap();
        assert_eq!(r.read_record(), Err(ReaderReadErr::UnexpectedEof), "{layout}");
    }
}

