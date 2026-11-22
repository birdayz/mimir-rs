//! Faithful port of upstream mimir-prometheus tests for:
//! - tsdb/fileutil (5 tests)
//! - tsdb/record (17 tests)
//! - tsdb/chunks (23 tests)
//! - tsdb/wlog (37 tests)
//!
//! Total: 82 tests.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use mimir_tsdb::chunkenc::histogram_model::{
    CounterResetHint, FloatHistogram, Histogram, HistogramSpan, CUSTOM_BUCKETS_SCHEMA,
    EXPONENTIAL_SCHEMA_MAX,
};
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding, XorChunk};
use mimir_tsdb::chunks_block::{
    block_chunk_file_path, chunk_from_samples, encoded_size_in_bytes, BlockChunksReader,
    BlockChunksWriter, BlockSample, BlockSampleValue,
};
use mimir_tsdb::encoding::fileutil::{
    aligned_block, current_file_offset, dir_size, enable_direct_io, file_direct_io_rqmts,
    is_aligned, DirectIOWriter, FileLock, ERR_WRITER_INVALID,
};
use mimir_tsdb::encoding::{Encbuf, EncodingError};
use mimir_tsdb::head_chunks::{
    head_chunk_file_path, read_chunk_header_and_close, repair_last_chunk_file_with_remover,
    ChunkDiskMapper, ChunkDiskMapperRef, ChunkPayload, ChunkWriteJob, ChunkWriteQueue,
    HeadChunksError, WriteChunkFn, WriteJobQueue, CRC_SIZE, DEFAULT_WRITE_BUFFER_SIZE,
    HEAD_CHUNK_FILE_HEADER_SIZE, MAGIC_CHUNKS_SIZE, MAGIC_HEAD_CHUNKS, MAX_HEAD_CHUNK_META_SIZE,
    OUT_OF_ORDER_MASK,
};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::metrics::WAL_METRICS_PREFIX;
use mimir_tsdb::record::{
    BuffersPool, MetricType, RecordDecoder, RecordEncoder, RecordError, RecordType, RefExemplar,
    RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample, RefSeries, Tombstone,
};
use mimir_tsdb::wlog::{
    checkpoint_dir, compress_wal_record, create_checkpoint, create_segment_file, decompress_record_into,
    delete_checkpoints, delete_temp_checkpoints, last_checkpoint, read_min_valid_time, segment_name,
    segments_range, CompressionType, LiveReader, LiveReaderMetrics, SegmentBufReader, Watcher,
    WatcherMetrics, WlogError, WlogReader, WlogWriter, WriteFaultAction, WriteTo, PAGE_SIZE,
    RECORD_HEADER_SIZE, REC_FIRST, REC_FULL, REC_LAST, REC_MIDDLE, REC_PAGE_TERM,
};

// ============================================================================
// Shared deterministic helpers
// ============================================================================

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() as usize) % bound
    }
    fn next_i64(&mut self) -> i64 {
        (self.next_u64() & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }
    fn next_f64(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64) / ((1u64 << 53) as f64)
    }
    fn fill_bytes(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let b = self.next_u64().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&b[..n]);
        }
    }
}

fn generate_test_histogram(seed: usize) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 2 + seed as u64,
        count: 5 + (seed as u64) * 4,
        sum: 18.4 * (seed as f64 + 1.0),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: Vec::new(),
        positive_buckets: vec![seed as i64 + 1, 1, -1, 0],
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn generate_test_float_histogram(seed: usize) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 2.0 + seed as f64,
        count: 5.0 + (seed as f64) * 4.0,
        sum: 18.4 * (seed as f64 + 1.0),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: Vec::new(),
        positive_buckets: vec![seed as f64 + 1.0, 1.0, -1.0, 0.0],
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn make_custom_bucket_histogram(seed: usize) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.001,
        zero_count: 2 + seed as u64,
        count: 5 + (seed as u64) * 4,
        sum: 18.4 * (seed as f64 + 1.0),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: Vec::new(),
        positive_buckets: vec![seed as i64 + 1, 1, -1, 0],
        negative_buckets: Vec::new(),
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
    }
}

fn all_compressions() -> [CompressionType; 3] {
    [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd]
}

// ============================================================================
// Package 1: tsdb/fileutil (5 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/fileutil/direct_io_writer_test.go::TestDirectIOFile
#[test]
fn test_direct_io_file() {
    let dir = tempfile::tempdir().unwrap();
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.path().join("test"))
        .unwrap();
    enable_direct_io(&f).unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/fileutil/direct_io_writer_test.go::TestAlignedBlockEarlyPanic
#[test]
fn test_aligned_block_early_panic() {
    let dir = tempfile::tempdir().unwrap();
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.path().join("foo"))
        .unwrap();
    let align_rqmts = file_direct_io_rqmts(&f).unwrap();

    for (desc, size) in [("Zero size", 0usize), ("Size not multiple of offset alignment", 9973usize)] {
        let rqmts = align_rqmts;
        let res = std::panic::catch_unwind(move || {
            let _ = aligned_block(size, &rqmts);
        });
        assert!(res.is_err(), "expected panic for {desc}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/fileutil/direct_io_writer_test.go::TestAlignedBloc
#[test]
fn test_aligned_bloc() {
    let dir = tempfile::tempdir().unwrap();
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.path().join("foo"))
        .unwrap();
    let align_rqmts = file_direct_io_rqmts(&f).unwrap();
    let block = aligned_block(5 * align_rqmts.offset_align, &align_rqmts);
    assert!(is_aligned(&block, &align_rqmts));
    assert_eq!(block.len(), 5 * align_rqmts.offset_align);
    assert!(!is_aligned(&block[1..], &align_rqmts));
}

// Upstream: github.com/prometheus/prometheus/tsdb/fileutil/direct_io_writer_test.go::TestDirectIOWriter
#[test]
fn test_direct_io_writer() {
    let probe_dir = tempfile::tempdir().unwrap();
    let probe_f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(probe_dir.path().join("foo"))
        .unwrap();
    let align_rqmts = file_direct_io_rqmts(&probe_f).unwrap();
    let oa = align_rqmts.offset_align;

    struct Case {
        name: &'static str,
        initial_offset: usize,
        buffer_size: usize,
        data_size: usize,
        written_bytes: usize,
        should_invalidate: bool,
    }

    let cases = [
        Case {
            name: "data equal to buffer",
            initial_offset: 0,
            buffer_size: 8 * oa,
            data_size: 8 * oa,
            written_bytes: 8 * oa,
            should_invalidate: false,
        },
        Case {
            name: "data exceeds buffer",
            initial_offset: 0,
            buffer_size: 4 * oa,
            data_size: 64 * oa,
            written_bytes: 64 * oa,
            should_invalidate: false,
        },
        Case {
            name: "data exceeds buffer + final offset unaligned",
            initial_offset: 0,
            buffer_size: 2 * oa,
            data_size: 4 * oa + 33,
            written_bytes: 4 * oa + oa,
            should_invalidate: true,
        },
        Case {
            name: "data smaller than buffer",
            initial_offset: 0,
            buffer_size: 8 * oa,
            data_size: 3 * oa,
            written_bytes: 3 * oa,
            should_invalidate: false,
        },
        Case {
            name: "data smaller than buffer + final offset unaligned",
            initial_offset: 0,
            buffer_size: 4 * oa,
            data_size: oa + 70,
            written_bytes: oa + oa,
            should_invalidate: true,
        },
        Case {
            name: "offset aligned",
            initial_offset: oa,
            buffer_size: 8 * oa,
            data_size: oa,
            written_bytes: oa,
            should_invalidate: false,
        },
        Case {
            name: "initial offset unaligned + final offset unaligned",
            initial_offset: 8,
            buffer_size: 8 * oa,
            data_size: 64 * oa,
            written_bytes: 64 * oa + (oa - 8),
            should_invalidate: true,
        },
        Case {
            name: "offset unaligned + final offset aligned",
            initial_offset: 8,
            buffer_size: 4 * oa,
            data_size: 4 * oa + (oa - 8),
            written_bytes: 4 * oa + (oa - 8),
            should_invalidate: false,
        },
        Case {
            name: "empty data",
            initial_offset: 0,
            buffer_size: 4 * oa,
            data_size: 0,
            written_bytes: 0,
            should_invalidate: false,
        },
    ];

    for tc in cases {
        let dir = tempfile::tempdir().unwrap();
        let file_name = dir.path().join("test");
        let data: Vec<u8> = (0..tc.data_size).map(|i| (i % 251) as u8).collect();

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&file_name)
            .unwrap();
        if tc.initial_offset != 0 {
            f.seek(SeekFrom::Start(tc.initial_offset as u64)).unwrap();
        }

        let mut w = DirectIOWriter::new(f, tc.buffer_size).unwrap();
        let n = w.write(&data).unwrap();
        assert_eq!(n, tc.data_size, "{}", tc.name);
        w.flush().unwrap();

        let curr_offset = current_file_offset(w.file_mut()).unwrap();
        assert_eq!(curr_offset, tc.data_size + tc.initial_offset, "{}", tc.name);

        let file_bytes = fs::read(&file_name).unwrap();
        if tc.data_size > 0 {
            assert_eq!(
                file_bytes.len(),
                tc.written_bytes + tc.initial_offset,
                "{}",
                tc.name
            );
            assert_eq!(
                &file_bytes[tc.initial_offset..tc.data_size + tc.initial_offset],
                data.as_slice(),
                "{}",
                tc.name
            );
        } else {
            assert!(file_bytes.is_empty(), "{}", tc.name);
        }

        if tc.should_invalidate {
            assert!(w.is_invalid(), "{}", tc.name);
            let err = w.flush().unwrap_err();
            assert!(err.to_string().contains(ERR_WRITER_INVALID) || err.kind() == io::ErrorKind::InvalidInput);
            assert!(w.write(&[]).is_err(), "{}", tc.name);
        } else {
            assert!(!w.is_invalid(), "{}", tc.name);
            w.flush().unwrap();
            assert_eq!(w.write(&[]).unwrap(), 0, "{}", tc.name);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/fileutil/flock_test.go::TestLocking
#[test]
fn test_locking() {
    let dir = tempfile::tempdir().unwrap();
    let file_name = dir.path().join("LOCK");
    assert!(fs::metadata(&file_name).is_err());

    let (lock, existed) = FileLock::flock(&file_name).unwrap();
    assert!(!existed);
    assert!(fs::metadata(&file_name).is_ok());

    let err = FileLock::flock(&file_name).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

    lock.release().unwrap();
    assert!(fs::metadata(&file_name).is_ok());

    let (lock, existed) = FileLock::flock(&file_name).unwrap();
    assert!(existed);
    lock.release().unwrap();
}

// ============================================================================
// Package 2: tsdb/record (17 tests)
// ============================================================================

#[derive(Clone, Copy)]
enum RefSamplesCase {
    Realistic1000,
    Realistic1000VariableST,
    Realistic1000ConstST,
    WorstCase1000,
    WorstCase1000ST,
}

fn gen_test_ref_samples_case(case: RefSamplesCase) -> Vec<RefSample> {
    let mut rng = XorShift64::new(0xABCD_1234_5678_90EF);
    let mut out = Vec::with_capacity(1000);
    let base_t = 1_700_000_000_000i64;
    for i in 0..1000i64 {
        let (ref_id, t, st, v) = match case {
            RefSamplesCase::Realistic1000 => (i as u64, base_t + (i / 100), 0, (i as f64) * 1.5),
            RefSamplesCase::Realistic1000VariableST => (
                i as u64,
                base_t + i * 1000,
                base_t + i * 1000 - 100 - (i % 7),
                (i as f64) * 1.5,
            ),
            RefSamplesCase::Realistic1000ConstST => {
                (i as u64, base_t + i * 1000, base_t - 5000, (i as f64) * 1.5)
            }
            RefSamplesCase::WorstCase1000 => (
                rng.next_u64() & 0x7FFF_FFFF,
                rng.next_i64(),
                0,
                rng.next_f64(),
            ),
            RefSamplesCase::WorstCase1000ST => (
                rng.next_u64() & 0x7FFF_FFFF,
                rng.next_i64(),
                rng.next_i64(),
                rng.next_f64(),
            ),
        };
        out.push(RefSample { ref_id, st, t, v });
    }
    out
}

fn zero_out_sts(samples: &[RefSample]) -> Vec<RefSample> {
    samples
        .iter()
        .map(|s| RefSample { st: 0, ..*s })
        .collect()
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/bench_test.go::TestEncodeDecode
#[test]
fn test_encode_decode() {
    for enable_st_storage in [false, true] {
        for case in [
            RefSamplesCase::Realistic1000,
            RefSamplesCase::Realistic1000VariableST,
            RefSamplesCase::Realistic1000ConstST,
            RefSamplesCase::WorstCase1000,
            RefSamplesCase::WorstCase1000ST,
        ] {
            let dec = RecordDecoder::new();
            let enc = RecordEncoder::new(enable_st_storage);
            let s = gen_test_ref_samples_case(case);
            let expected = if enable_st_storage {
                s.clone()
            } else {
                zero_out_sts(&s)
            };

            let got = dec.samples(&enc.samples(&s, Vec::new())).unwrap();
            assert_eq!(got, expected);

            // With byte buffer (append!)
            let buf = vec![0u8; 10];
            let appended = enc.samples(&s, buf);
            let got = dec.samples(&appended[10..]).unwrap();
            assert_eq!(got, expected);

            // With sample slice reuse
            let mut samples = Vec::with_capacity(s.len() + 1);
            dec.samples_into(&enc.samples(&s, Vec::new()), &mut samples)
                .unwrap();
            assert_eq!(samples, expected);

            // With zstd compression
            let raw = enc.samples(&s, Vec::new());
            let mut comp_buf = Vec::new();
            let compressed = compress_wal_record(&raw, CompressionType::Zstd, &mut comp_buf).unwrap();
            let mut decomp = Vec::new();
            let payload = if compressed {
                decompress_record_into(CompressionType::Zstd, &comp_buf, &mut decomp).unwrap();
                &decomp
            } else {
                &raw
            };
            let got = dec.samples(payload).unwrap();
            assert_eq!(got, expected);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/buffers_test.go::TestBuffersPool_PtrClear
#[test]
fn test_buffers_pool_ptr_clear() {
    let pool = BuffersPool::new();

    let mut h = pool.get_histograms(1);
    h.push(Some(RefHistogramSample {
        ref_id: 1,
        st: 0,
        t: 100,
        h: Histogram {
            schema: 1,
            ..Default::default()
        },
    }));
    pool.put_histograms(h);

    let h2 = pool.get_histograms(1);
    assert!(h2.is_empty());
    assert!(h2.capacity() >= 1);

    let mut fh = pool.get_float_histograms(1);
    fh.push(Some(RefFloatHistogramSample {
        ref_id: 1,
        st: 0,
        t: 100,
        fh: FloatHistogram {
            schema: 1,
            ..Default::default()
        },
    }));
    pool.put_float_histograms(fh);

    let fh2 = pool.get_float_histograms(1);
    assert!(fh2.is_empty());
    assert!(fh2.capacity() >= 1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_EncodeDecode
#[test]
fn test_record_encode_decode() {
    let mut enc = RecordEncoder::new(false);
    let dec = RecordDecoder::new();

    let series = vec![
        RefSeries {
            ref_id: 100,
            labels: Labels::from_pairs([("123", "456"), ("abc", "def")]),
        },
        RefSeries {
            ref_id: 1,
            labels: Labels::from_pairs([("1234", "4567"), ("abc", "def2")]),
        },
        RefSeries {
            ref_id: 435245,
            labels: Labels::from_pairs([("foo", "bar"), ("xyz", "def")]),
        },
    ];
    let dec_series = dec.series(&enc.series(&series, Vec::new())).unwrap();
    assert_eq!(dec_series, series);

    let metadata = vec![
        RefMetadata {
            ref_id: 100,
            metric_type: MetricType::Counter as u8,
            unit: String::new(),
            help: "some magic counter".to_string(),
        },
        RefMetadata {
            ref_id: 1,
            metric_type: MetricType::Counter as u8,
            unit: "seconds".to_string(),
            help: "CPU time counter".to_string(),
        },
        RefMetadata {
            ref_id: 147741,
            metric_type: MetricType::Gauge as u8,
            unit: "percentage".to_string(),
            help: "current memory usage".to_string(),
        },
    ];
    let dec_meta = dec.metadata(&enc.metadata(&metadata, Vec::new())).unwrap();
    assert_eq!(dec_meta, metadata);

    // Without ST
    let samples = vec![
        RefSample { ref_id: 0, st: 0, t: 12423423, v: 1.2345 },
        RefSample { ref_id: 123, st: 0, t: -1231, v: -123.0 },
        RefSample { ref_id: 2, st: 0, t: 0, v: 99999.0 },
    ];
    let encoded = enc.samples(&samples, Vec::new());
    assert_eq!(dec.record_type(&encoded), RecordType::Samples);
    assert_eq!(dec.samples(&encoded).unwrap(), samples);

    // V2 without ST
    enc = RecordEncoder::new(true);
    let encoded = enc.samples(&samples, Vec::new());
    assert_eq!(dec.record_type(&encoded), RecordType::SamplesV2);
    assert_eq!(dec.samples(&encoded).unwrap(), samples);

    // With ST
    let samples_with_st = vec![
        RefSample { ref_id: 0, st: 14, t: 12423423, v: 1.2345 },
        RefSample { ref_id: 123, st: 14, t: -1231, v: -123.0 },
        RefSample { ref_id: 2, st: 14, t: 0, v: 99999.0 },
    ];
    let encoded = enc.samples(&samples_with_st, Vec::new());
    assert_eq!(dec.record_type(&encoded), RecordType::SamplesV2);
    assert_eq!(dec.samples(&encoded).unwrap(), samples_with_st);

    // With ST (ST[i] == T[i-1])
    let samples_delta_st = vec![
        RefSample { ref_id: 0, st: 12423300, t: 12423400, v: 1.2345 },
        RefSample { ref_id: 123, st: 12423400, t: 12423500, v: -123.0 },
        RefSample { ref_id: 2, st: 12423500, t: 12423600, v: 99999.0 },
    ];
    assert_eq!(dec.samples(&enc.samples(&samples_delta_st, Vec::new())).unwrap(), samples_delta_st);

    // With ST (ST[i] == ST[i-1])
    let samples_const_st = vec![
        RefSample { ref_id: 0, st: 12423300, t: 12423400, v: 1.2345 },
        RefSample { ref_id: 123, st: 12423300, t: 12423500, v: -123.0 },
        RefSample { ref_id: 2, st: 12423300, t: 12423600, v: 99999.0 },
    ];
    assert_eq!(dec.samples(&enc.samples(&samples_const_st, Vec::new())).unwrap(), samples_const_st);

    // Tombstones
    let tstones = vec![
        Tombstone { ref_id: 123, mint: -1000, maxt: 1231231 },
        Tombstone { ref_id: 123, mint: 5000, maxt: 0 },
        Tombstone { ref_id: 13, mint: -1000, maxt: -11 },
        Tombstone { ref_id: 13, mint: 5000, maxt: 1000 },
    ];
    assert_eq!(dec.tombstones(&enc.tombstones(&tstones, Vec::new())).unwrap(), tstones);

    // Exemplars
    let exemplars = vec![
        RefExemplar {
            ref_id: 0,
            t: 12423423,
            v: 1.2345,
            labels: Labels::from_pairs([("trace_id", "qwerty")]),
        },
        RefExemplar {
            ref_id: 123,
            t: -1231,
            v: -123.0,
            labels: Labels::from_pairs([("trace_id", "asdf")]),
        },
        RefExemplar {
            ref_id: 2,
            t: 0,
            v: 99999.0,
            labels: Labels::from_pairs([("trace_id", "zxcv")]),
        },
    ];
    assert_eq!(dec.exemplars(&enc.exemplars(&exemplars, Vec::new())).unwrap(), exemplars);

    // Histograms V1
    enc = RecordEncoder::new(false);
    let mut histograms = vec![
        RefHistogramSample {
            ref_id: 56,
            st: 0,
            t: 1234,
            h: generate_test_histogram(0),
        },
        RefHistogramSample {
            ref_id: 42,
            st: 0,
            t: 5678,
            h: Histogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 4,
                count: 11,
                sum: 35.5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 2 },
                    HistogramSpan { offset: 2, length: 2 },
                ],
                negative_spans: vec![
                    HistogramSpan { offset: 0, length: 1 },
                    HistogramSpan { offset: 1, length: 2 },
                ],
                positive_buckets: vec![1, 1, -1, 0],
                negative_buckets: vec![1, 2, -1],
                custom_values: Vec::new(),
            },
        },
        RefHistogramSample {
            ref_id: 67,
            st: 0,
            t: 5678,
            h: make_custom_bucket_histogram(0),
        },
    ];
    let (hist_samples, cb_hists) = enc.histogram_samples(&histograms, Vec::new());
    let cb_hist_samples = enc.custom_buckets_histogram_samples(&cb_hists, Vec::new());
    let mut dec_hists = dec.histogram_samples(&hist_samples).unwrap();
    dec_hists.extend(dec.histogram_samples(&cb_hist_samples).unwrap());
    assert_eq!(dec_hists, histograms);

    // Float histograms V1
    let mut float_histograms: Vec<RefFloatHistogramSample> = histograms
        .iter()
        .map(|h| RefFloatHistogramSample {
            ref_id: h.ref_id,
            st: 0,
            t: h.t,
            fh: h.h.to_float(),
        })
        .collect();
    let (fh_samples, cb_fhs) = enc.float_histogram_samples(&float_histograms, Vec::new());
    let cb_fh_samples = enc.custom_buckets_float_histogram_samples(&cb_fhs, Vec::new());
    let mut dec_fhs = dec.float_histogram_samples(&fh_samples).unwrap();
    dec_fhs.extend(dec.float_histogram_samples(&cb_fh_samples).unwrap());
    assert_eq!(dec_fhs, float_histograms);

    // Gauge histograms V2
    enc = RecordEncoder::new(true);
    for h in &mut histograms {
        h.h.counter_reset_hint = CounterResetHint::GaugeType;
        h.st = 1000;
    }
    let (h2, left) = enc.histogram_samples(&histograms, Vec::new());
    assert!(left.is_empty());
    assert_eq!(dec.histogram_samples(&h2).unwrap(), histograms);

    for fh in &mut float_histograms {
        fh.fh.counter_reset_hint = CounterResetHint::GaugeType;
        fh.st = 1000;
    }
    let (fh2, left_fh) = enc.float_histogram_samples(&float_histograms, Vec::new());
    assert!(left_fh.is_empty());
    assert_eq!(dec.float_histogram_samples(&fh2).unwrap(), float_histograms);

    // Empty slices
    for st in [false, true] {
        let e = RecordEncoder::new(st);
        let (hb, cb) = e.histogram_samples(&[], Vec::new());
        assert!(cb.is_empty());
        assert!(dec.histogram_samples(&hb).unwrap().is_empty());

        let (fhb, cbf) = e.float_histogram_samples(&[], Vec::new());
        assert!(cbf.is_empty());
        assert!(dec.float_histogram_samples(&fhb).unwrap().is_empty());
    }

    // V1 all custom bucket
    let enc_v1 = RecordEncoder::new(false);
    let all_custom = vec![
        RefHistogramSample {
            ref_id: 56,
            st: 0,
            t: 1234,
            h: make_custom_bucket_histogram(0),
        },
        RefHistogramSample {
            ref_id: 67,
            st: 0,
            t: 5678,
            h: make_custom_bucket_histogram(0),
        },
    ];
    let (hb, cb) = enc_v1.histogram_samples(&all_custom, Vec::new());
    assert!(hb.is_empty());
    assert_eq!(cb.len(), 2);
    let cb_buf = enc_v1.custom_buckets_histogram_samples(&cb, Vec::new());
    assert_eq!(dec.histogram_samples(&cb_buf).unwrap(), all_custom);

    let all_custom_float: Vec<RefFloatHistogramSample> = all_custom
        .iter()
        .map(|h| RefFloatHistogramSample {
            ref_id: h.ref_id,
            st: 0,
            t: h.t,
            fh: h.h.to_float(),
        })
        .collect();
    let (fhb, cbf) = enc_v1.float_histogram_samples(&all_custom_float, Vec::new());
    assert!(fhb.is_empty());
    assert_eq!(cbf.len(), 2);
    let cbf_buf = enc_v1.custom_buckets_float_histogram_samples(&cbf, Vec::new());
    assert_eq!(dec.float_histogram_samples(&cbf_buf).unwrap(), all_custom_float);

    // V1 backward compat ST=0
    let (v1h, v1cb) = enc_v1.histogram_samples(&histograms, Vec::new());
    let v1cb_buf = enc_v1.custom_buckets_histogram_samples(&v1cb, Vec::new());
    for h in dec
        .histogram_samples(&v1h)
        .unwrap()
        .into_iter()
        .chain(dec.histogram_samples(&v1cb_buf).unwrap())
    {
        assert_eq!(h.st, 0);
    }

    let (v1fh, v1cbf) = enc_v1.float_histogram_samples(&float_histograms, Vec::new());
    let v1cbf_buf = enc_v1.custom_buckets_float_histogram_samples(&v1cbf, Vec::new());
    for fh in dec
        .float_histogram_samples(&v1fh)
        .unwrap()
        .into_iter()
        .chain(dec.float_histogram_samples(&v1cbf_buf).unwrap())
    {
        assert_eq!(fh.st, 0);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_MinValidTime
#[test]
fn test_record_min_valid_time() {
    let enc = RecordEncoder::new(false);
    let dec = RecordDecoder::new();
    for mint in [0i64, 1, -1, 12345, i64::MIN, i64::MAX] {
        let encoded = enc.min_valid_time(mint, Vec::new());
        assert_eq!(dec.record_type(&encoded), RecordType::MinValidTime);
        assert_eq!(dec.min_valid_time(&encoded).unwrap(), mint);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_MinValidTimeDecodeWrongType
#[test]
fn test_record_min_valid_time_decode_wrong_type() {
    let enc = RecordEncoder::new(false);
    let dec = RecordDecoder::new();
    assert!(dec.min_valid_time(&enc.series(&[], Vec::new())).is_err());
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_V1MixedRegularAndCustomBucketHistogramPermutations
#[test]
fn test_record_v1_mixed_regular_and_custom_bucket_histogram_permutations() {
    let dec = RecordDecoder::new();
    let enc = RecordEncoder::new(false);

    let regular_a = generate_test_histogram(0);
    let regular_b = Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 4,
        count: 11,
        sum: 35.5,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 2 },
        ],
        negative_spans: vec![
            HistogramSpan { offset: 0, length: 1 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 1, -1, 0],
        negative_buckets: vec![1, 2, -1],
        custom_values: Vec::new(),
    };
    let custom = make_custom_bucket_histogram(1);

    let cases = [
        vec!["regularA", "custom"],
        vec!["custom", "regularA"],
        vec!["regularA", "custom", "regularB"],
        vec!["custom", "regularA", "custom"],
    ];

    for kinds in cases {
        let int_samples: Vec<RefHistogramSample> = kinds
            .iter()
            .enumerate()
            .map(|(i, &k)| RefHistogramSample {
                ref_id: (100 + i * 11) as u64,
                st: 0,
                t: (1000 + i * 250) as i64,
                h: match k {
                    "regularA" => regular_a.clone(),
                    "regularB" => regular_b.clone(),
                    "custom" => custom.clone(),
                    _ => unreachable!(),
                },
            })
            .collect();
        let want_reg: Vec<_> = int_samples
            .iter()
            .filter(|s| !s.h.uses_custom_buckets())
            .cloned()
            .collect();
        let want_cb: Vec<_> = int_samples
            .iter()
            .filter(|s| s.h.uses_custom_buckets())
            .cloned()
            .collect();

        let (hist_buf, cb_refs) = enc.histogram_samples(&int_samples, Vec::new());
        let cb_buf = enc.custom_buckets_histogram_samples(&cb_refs, Vec::new());
        assert_eq!(dec.histogram_samples(&hist_buf).unwrap(), want_reg);
        assert_eq!(dec.histogram_samples(&cb_buf).unwrap(), want_cb);

        let float_samples: Vec<RefFloatHistogramSample> = int_samples
            .iter()
            .map(|s| RefFloatHistogramSample {
                ref_id: s.ref_id,
                st: 0,
                t: s.t,
                fh: s.h.to_float(),
            })
            .collect();
        let want_reg_f: Vec<_> = float_samples
            .iter()
            .filter(|s| !s.fh.uses_custom_buckets())
            .cloned()
            .collect();
        let want_cb_f: Vec<_> = float_samples
            .iter()
            .filter(|s| s.fh.uses_custom_buckets())
            .cloned()
            .collect();

        let (f_buf, cbf_refs) = enc.float_histogram_samples(&float_samples, Vec::new());
        let cbf_buf = enc.custom_buckets_float_histogram_samples(&cbf_refs, Vec::new());
        assert_eq!(dec.float_histogram_samples(&f_buf).unwrap(), want_reg_f);
        assert_eq!(dec.float_histogram_samples(&cbf_buf).unwrap(), want_cb_f);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_V2HistogramRoundTrip
#[test]
fn test_record_v2_histogram_round_trip() {
    let dec = RecordDecoder::new();
    let enc = RecordEncoder::new(true);

    let int_samples = vec![
        RefHistogramSample {
            ref_id: 100,
            st: 1000,
            t: 1000,
            h: generate_test_histogram(0),
        },
        RefHistogramSample {
            ref_id: 111,
            st: 1000,
            t: 1250,
            h: make_custom_bucket_histogram(0),
        },
        RefHistogramSample {
            ref_id: 122,
            st: 1234,
            t: 1500,
            h: generate_test_histogram(1),
        },
    ];
    let (hist_buf, leftover) = enc.histogram_samples(&int_samples, Vec::new());
    assert!(leftover.is_empty());
    assert_eq!(dec.record_type(&hist_buf), RecordType::HistogramSamplesV2);
    assert_eq!(dec.histogram_samples(&hist_buf).unwrap(), int_samples);

    let float_samples: Vec<RefFloatHistogramSample> = int_samples
        .iter()
        .map(|s| RefFloatHistogramSample {
            ref_id: s.ref_id,
            st: s.st,
            t: s.t,
            fh: s.h.to_float(),
        })
        .collect();
    let (f_buf, left_f) = enc.float_histogram_samples(&float_samples, Vec::new());
    assert!(left_f.is_empty());
    assert_eq!(dec.record_type(&f_buf), RecordType::FloatHistogramSamplesV2);
    assert_eq!(dec.float_histogram_samples(&f_buf).unwrap(), float_samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_DecodeInvalidHistogramSchema
#[test]
fn test_record_decode_invalid_histogram_schema() {
    let dec = RecordDecoder::new();
    for st in [false, true] {
        for schema in [-100i32, 100i32] {
            let enc = RecordEncoder::new(st);
            let mut h = generate_test_histogram(0);
            h.schema = schema;
            let samples = vec![RefHistogramSample {
                ref_id: 56,
                st: 0,
                t: 1234,
                h,
            }];
            let (buf, _) = enc.histogram_samples(&samples, Vec::new());
            let decoded = dec.histogram_samples(&buf).unwrap();
            assert!(decoded.is_empty());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_DecodeInvalidFloatHistogramSchema
#[test]
fn test_record_decode_invalid_float_histogram_schema() {
    let dec = RecordDecoder::new();
    for st in [false, true] {
        for schema in [-100i32, 100i32] {
            let enc = RecordEncoder::new(st);
            let mut fh = generate_test_float_histogram(0);
            fh.schema = schema;
            let samples = vec![RefFloatHistogramSample {
                ref_id: 56,
                st: 0,
                t: 1234,
                fh,
            }];
            let (buf, _) = enc.float_histogram_samples(&samples, Vec::new());
            let decoded = dec.float_histogram_samples(&buf).unwrap();
            assert!(decoded.is_empty());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_DecodeV2UnknownFirstHistogramSchema
#[test]
fn test_record_decode_v2_unknown_first_histogram_schema() {
    let enc = RecordEncoder::new(true);
    let dec = RecordDecoder::new();
    let mut bad = generate_test_histogram(0);
    bad.schema = -100;
    let good = generate_test_histogram(1);
    let samples = vec![
        RefHistogramSample {
            ref_id: 56,
            st: 1000,
            t: 1234,
            h: bad,
        },
        RefHistogramSample {
            ref_id: 42,
            st: 1000,
            t: 5678,
            h: good,
        },
    ];
    let (buf, _) = enc.histogram_samples(&samples, Vec::new());
    let decoded = dec.histogram_samples(&buf).unwrap();
    assert_eq!(decoded, samples[1..]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_DecodeV2UnknownFirstFloatHistogramSchema
#[test]
fn test_record_decode_v2_unknown_first_float_histogram_schema() {
    let enc = RecordEncoder::new(true);
    let dec = RecordDecoder::new();
    let mut bad = generate_test_float_histogram(0);
    bad.schema = -100;
    let good = generate_test_float_histogram(1);
    let samples = vec![
        RefFloatHistogramSample {
            ref_id: 56,
            st: 1000,
            t: 1234,
            fh: bad,
        },
        RefFloatHistogramSample {
            ref_id: 42,
            st: 1000,
            t: 5678,
            fh: good,
        },
    ];
    let (buf, _) = enc.float_histogram_samples(&samples, Vec::new());
    let decoded = dec.float_histogram_samples(&buf).unwrap();
    assert_eq!(decoded, samples[1..]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_DecodeTooHighResolutionHistogramSchema
#[test]
fn test_record_decode_too_high_resolution_histogram_schema() {
    let dec = RecordDecoder::new();
    for st in [false, true] {
        for schema in [9i32, 52i32] {
            let enc = RecordEncoder::new(st);
            let mut h = generate_test_histogram(0);
            h.schema = schema;
            let samples = vec![RefHistogramSample {
                ref_id: 56,
                st: 0,
                t: 1234,
                h,
            }];
            let (buf, _) = enc.histogram_samples(&samples, Vec::new());
            let decoded = dec.histogram_samples(&buf).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].h.schema, EXPONENTIAL_SCHEMA_MAX);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_DecodeTooHighResolutionFloatHistogramSchema
#[test]
fn test_record_decode_too_high_resolution_float_histogram_schema() {
    let dec = RecordDecoder::new();
    for st in [false, true] {
        for schema in [9i32, 52i32] {
            let enc = RecordEncoder::new(st);
            let mut fh = generate_test_float_histogram(0);
            fh.schema = schema;
            let samples = vec![RefFloatHistogramSample {
                ref_id: 56,
                st: 0,
                t: 1234,
                fh,
            }];
            let (buf, _) = enc.float_histogram_samples(&samples, Vec::new());
            let decoded = dec.float_histogram_samples(&buf).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].fh.schema, EXPONENTIAL_SCHEMA_MAX);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_Corrupted
#[test]
fn test_record_corrupted() {
    let dec = RecordDecoder::new();
    for st in [false, true] {
        let enc = RecordEncoder::new(st);

        let series = vec![RefSeries {
            ref_id: 100,
            labels: Labels::from_pairs([("123", "456"), ("abc", "def")]),
        }];
        let err = dec.series(&enc.series(&series, Vec::new())[..8]).unwrap_err();
        assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));

        let samples = vec![RefSample {
            ref_id: 0,
            st: 0,
            t: 12423423,
            v: 1.2345,
        }];
        let err = dec.samples(&enc.samples(&samples, Vec::new())[..8]).unwrap_err();
        assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));

        let tstones = vec![
            Tombstone { ref_id: 123, mint: -1000, maxt: 1231231 },
            Tombstone { ref_id: 123, mint: 5000, maxt: 0 },
        ];
        let err = dec.tombstones(&enc.tombstones(&tstones, Vec::new())[..8]).unwrap_err();
        assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));

        let exemplars = vec![RefExemplar {
            ref_id: 0,
            t: 12423423,
            v: 1.2345,
            labels: Labels::from_pairs([("trace_id", "asdf")]),
        }];
        let err = dec.exemplars(&enc.exemplars(&exemplars, Vec::new())[..8]).unwrap_err();
        assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));

        let meta = vec![RefMetadata {
            ref_id: 147,
            metric_type: MetricType::Counter as u8,
            unit: "unit".to_string(),
            help: "help".to_string(),
        }];
        let err = dec.metadata(&enc.metadata(&meta, Vec::new())[..8]).unwrap_err();
        assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));

        let hists = vec![
            RefHistogramSample {
                ref_id: 56,
                st: 0,
                t: 1234,
                h: generate_test_histogram(0),
            },
            RefHistogramSample {
                ref_id: 67,
                st: 0,
                t: 5678,
                h: make_custom_bucket_histogram(0),
            },
        ];
        let (h_buf, cb_h) = enc.histogram_samples(&hists, Vec::new());
        let err = dec.histogram_samples(&h_buf[..8]).unwrap_err();
        assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));
        if !cb_h.is_empty() {
            let cb_buf = enc.custom_buckets_histogram_samples(&cb_h, Vec::new());
            let err = dec.histogram_samples(&cb_buf[..8]).unwrap_err();
            assert!(matches!(err, RecordError::Encoding(EncodingError::InvalidSize)));
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_Type
#[test]
fn test_record_type() {
    let mut enc = RecordEncoder::new(false);
    let dec = RecordDecoder::new();

    assert_eq!(
        dec.record_type(&enc.series(&[RefSeries { ref_id: 100, labels: Labels::from_pairs([("abc", "123")]) }], Vec::new())),
        RecordType::Series
    );
    assert_eq!(
        dec.record_type(&enc.samples(&[RefSample { ref_id: 123, st: 0, t: 12345, v: 1.2345 }], Vec::new())),
        RecordType::Samples
    );

    enc = RecordEncoder::new(true);
    assert_eq!(
        dec.record_type(&enc.samples(&[RefSample { ref_id: 123, st: 0, t: 12345, v: 1.2345 }], Vec::new())),
        RecordType::SamplesV2
    );
    assert_eq!(
        dec.record_type(&enc.samples(&[RefSample { ref_id: 123, st: 1, t: 12345, v: 1.2345 }], Vec::new())),
        RecordType::SamplesV2
    );
    assert_eq!(
        dec.record_type(&enc.tombstones(&[Tombstone { ref_id: 1, mint: 1, maxt: 2 }], Vec::new())),
        RecordType::Tombstones
    );
    assert_eq!(
        dec.record_type(&enc.metadata(&[RefMetadata { ref_id: 147, metric_type: MetricType::Counter as u8, unit: "unit".into(), help: "help".into() }], Vec::new())),
        RecordType::Metadata
    );

    let hists = vec![
        RefHistogramSample { ref_id: 56, st: 0, t: 1234, h: generate_test_histogram(0) },
        RefHistogramSample { ref_id: 67, st: 0, t: 5678, h: make_custom_bucket_histogram(0) },
    ];
    enc = RecordEncoder::new(false);
    let (h1, cb) = enc.histogram_samples(&hists, Vec::new());
    assert_eq!(dec.record_type(&h1), RecordType::HistogramSamples);
    let cb1 = enc.custom_buckets_histogram_samples(&cb, Vec::new());
    assert_eq!(dec.record_type(&cb1), RecordType::CustomBucketsHistogramSamples);

    enc = RecordEncoder::new(true);
    let (h2, left) = enc.histogram_samples(&hists, Vec::new());
    assert!(left.is_empty());
    assert_eq!(dec.record_type(&h2), RecordType::HistogramSamplesV2);

    let fhists: Vec<_> = hists
        .iter()
        .map(|h| RefFloatHistogramSample { ref_id: h.ref_id, st: 0, t: h.t, fh: h.h.to_float() })
        .collect();
    let (fh2, left_f) = enc.float_histogram_samples(&fhists, Vec::new());
    assert!(left_f.is_empty());
    assert_eq!(dec.record_type(&fh2), RecordType::FloatHistogramSamplesV2);

    assert_eq!(dec.record_type(&[]), RecordType::Unknown);
    assert_eq!(dec.record_type(&[0]), RecordType::Unknown);
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_MetadataDecodeUnknownExtraFields
#[test]
fn test_record_metadata_decode_unknown_extra_fields() {
    let mut enc = Encbuf::new();
    let dec = RecordDecoder::new();

    enc.put_byte(RecordType::Metadata as u8);

    // Entry 1
    enc.put_uvarint64(101);
    enc.put_byte(MetricType::Counter as u8);
    enc.put_uvarint(2);
    enc.put_uvarint_str("UNIT");
    enc.put_uvarint_str("");
    enc.put_uvarint_str("HELP");
    enc.put_uvarint_str("some magic counter");

    // Entry 2
    enc.put_uvarint64(99);
    enc.put_byte(MetricType::Counter as u8);
    enc.put_uvarint(3);
    enc.put_uvarint_str("UNIT");
    enc.put_uvarint_str("seconds");
    enc.put_uvarint_str("HELP");
    enc.put_uvarint_str("CPU time counter");
    enc.put_uvarint_str("an extra field name to be skipped");
    enc.put_uvarint_str("with its value");

    // Entry 3
    enc.put_uvarint64(47250);
    enc.put_byte(MetricType::Gauge as u8);
    enc.put_uvarint(4);
    enc.put_uvarint_str("extra name one");
    enc.put_uvarint_str("extra value one");
    enc.put_uvarint_str("HELP");
    enc.put_uvarint_str("current memory usage");
    enc.put_uvarint_str("extra name two");
    enc.put_uvarint_str("extra value two");
    enc.put_uvarint_str("UNIT");
    enc.put_uvarint_str("percentage");

    let expected = vec![
        RefMetadata {
            ref_id: 101,
            metric_type: MetricType::Counter as u8,
            unit: String::new(),
            help: "some magic counter".to_string(),
        },
        RefMetadata {
            ref_id: 99,
            metric_type: MetricType::Counter as u8,
            unit: "seconds".to_string(),
            help: "CPU time counter".to_string(),
        },
        RefMetadata {
            ref_id: 47250,
            metric_type: MetricType::Gauge as u8,
            unit: "percentage".to_string(),
            help: "current memory usage".to_string(),
        },
    ];
    assert_eq!(dec.metadata(enc.as_slice()).unwrap(), expected);
}

// Upstream: github.com/prometheus/prometheus/tsdb/record/record_test.go::TestRecord_MetadataInternsStrings
#[test]
fn test_record_metadata_interns_strings() {
    let enc = RecordEncoder::new(false);
    let dec = RecordDecoder::new();

    let input = vec![
        RefMetadata {
            ref_id: 1,
            metric_type: MetricType::Counter as u8,
            unit: "seconds".to_string(),
            help: "Total CPU time spent in seconds.".to_string(),
        },
        RefMetadata {
            ref_id: 2,
            metric_type: MetricType::Counter as u8,
            unit: "seconds".to_string(),
            help: "Total CPU time spent in seconds.".to_string(),
        },
    ];
    let mut raw1 = enc.metadata(&input, Vec::new());
    let out1 = dec.metadata_interned(&raw1).unwrap();
    assert_eq!(out1.len(), 2);
    assert!(Arc::ptr_eq(&out1[0].unit, &out1[1].unit));
    assert!(Arc::ptr_eq(&out1[0].help, &out1[1].help));

    raw1.fill(b'X');
    assert_eq!(&*out1[0].unit, "seconds");
    assert_eq!(&*out1[0].help, "Total CPU time spent in seconds.");

    let raw2 = enc.metadata(&input, Vec::new());
    let out2 = dec.metadata_interned(&raw2).unwrap();
    assert!(Arc::ptr_eq(&out1[0].unit, &out2[0].unit));
    assert!(Arc::ptr_eq(&out1[0].help, &out2[0].help));
}

// ============================================================================
// Package 3: tsdb/chunks (23 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunks_test.go::TestReaderWithInvalidBuffer
#[test]
fn test_reader_with_invalid_buffer() {
    let r = BlockChunksReader::from_raw_bytes(vec![vec![0x81, 0x81, 0x81, 0x81, 0x81, 0x81]]);
    assert!(r.chunk(0).is_err());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunks_test.go::TestWriterWithDefaultSegmentSize
#[test]
fn test_writer_with_default_segment_size() {
    let mut chk1 = chunk_from_samples(&[
        BlockSample { st: 0, t: 10, value: BlockSampleValue::Float(11.0) },
        BlockSample { st: 0, t: 20, value: BlockSampleValue::Float(12.0) },
        BlockSample { st: 0, t: 30, value: BlockSampleValue::Float(13.0) },
    ])
    .unwrap();
    let mut chk2 = chunk_from_samples(&[
        BlockSample { st: 0, t: 40, value: BlockSampleValue::Histogram(generate_test_histogram(1)) },
        BlockSample { st: 0, t: 50, value: BlockSampleValue::Histogram(generate_test_histogram(2)) },
        BlockSample { st: 0, t: 60, value: BlockSampleValue::Histogram(generate_test_histogram(3)) },
    ])
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let mut w = BlockChunksWriter::open(dir.path()).unwrap();
    w.write_chunks(std::slice::from_mut(&mut chk1)).unwrap();
    w.write_chunks(std::slice::from_mut(&mut chk2)).unwrap();
    w.close().unwrap();

    let count = fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(count, 1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunks_test.go::TestChunkFromSamplesWithST
#[test]
fn test_chunk_from_samples_with_st() {
    let samples = vec![
        BlockSample { st: 5, t: 10, value: BlockSampleValue::Float(11.0) },
        BlockSample { st: 15, t: 20, value: BlockSampleValue::Float(12.0) },
        BlockSample { st: 25, t: 30, value: BlockSampleValue::Float(13.0) },
    ];
    let meta = chunk_from_samples(&samples).unwrap();
    assert_eq!(meta.min_time, 10);
    assert_eq!(meta.max_time, 30);
    let EncodedChunk::Xor2(chk) = meta.chunk.unwrap() else {
        panic!("expected Xor2 chunk with ST");
    };
    let mut it = chk.iter();
    let mut idx = 0usize;
    while let Some((ts, v)) = it.next_sample().unwrap() {
        assert_eq!(it.at_st(), samples[idx].st);
        assert_eq!(ts, samples[idx].t);
        let BlockSampleValue::Float(exp_v) = samples[idx].value else { unreachable!() };
        assert_eq!(v, exp_v);
        idx += 1;
    }
    assert_eq!(idx, samples.len());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunks_test.go::TestChunkSizeInBytes
#[test]
fn test_chunk_size_in_bytes() {
    let mut m_float = chunk_from_samples(&[
        BlockSample { st: 0, t: 10, value: BlockSampleValue::Float(11.0) },
        BlockSample { st: 0, t: 20, value: BlockSampleValue::Float(12.0) },
        BlockSample { st: 0, t: 30, value: BlockSampleValue::Float(13.0) },
    ])
    .unwrap();
    let mut m_hist = chunk_from_samples(&[
        BlockSample { st: 0, t: 40, value: BlockSampleValue::Histogram(generate_test_histogram(1)) },
        BlockSample { st: 0, t: 50, value: BlockSampleValue::Histogram(generate_test_histogram(2)) },
        BlockSample { st: 0, t: 60, value: BlockSampleValue::Histogram(generate_test_histogram(3)) },
    ])
    .unwrap();
    let mut m_fhist = chunk_from_samples(&[
        BlockSample { st: 0, t: 70, value: BlockSampleValue::FloatHistogram(generate_test_float_histogram(1)) },
        BlockSample { st: 0, t: 80, value: BlockSampleValue::FloatHistogram(generate_test_float_histogram(2)) },
        BlockSample { st: 0, t: 90, value: BlockSampleValue::FloatHistogram(generate_test_float_histogram(3)) },
    ])
    .unwrap();

    let expected_sz = mimir_tsdb::chunks_block::SEGMENT_HEADER_SIZE
        + encoded_size_in_bytes(m_float.chunk.as_ref().unwrap())
        + encoded_size_in_bytes(m_hist.chunk.as_ref().unwrap())
        + encoded_size_in_bytes(m_fhist.chunk.as_ref().unwrap());

    let dir = tempfile::tempdir().unwrap();
    let mut w = BlockChunksWriter::with_segment_size(dir.path(), 1024).unwrap();
    let mut metas = vec![m_float.clone(), m_hist.clone(), m_fhist.clone()];
    w.write_chunks(&mut metas).unwrap();
    w.close().unwrap();

    m_float.ref_id = metas[0].ref_id;
    m_hist.ref_id = metas[1].ref_id;
    m_fhist.ref_id = metas[2].ref_id;

    let actual_sz = fs::metadata(block_chunk_file_path(dir.path(), 1)).unwrap().len() as usize;
    assert_eq!(actual_sz, expected_sz);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunk_write_queue_test.go::TestChunkWriteQueue_GettingChunkFromQueue
#[test]
fn test_chunk_write_queue_getting_chunk_from_queue() {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let gate2 = Arc::clone(&gate);
    let writer: WriteChunkFn = Arc::new(move |_, _, _, _, _, _, _| {
        let (lk, cv) = &*gate2;
        let mut g = lk.lock().unwrap();
        while !*g {
            g = cv.wait(g).unwrap();
        }
        Ok(())
    });

    let q = ChunkWriteQueue::new(None, 1000, writer);
    let test_chunk = EncodedChunk::Xor(XorChunk::new());
    let ref_id = ChunkDiskMapperRef(0);
    q.add_job(ChunkWriteJob {
        chk: Some(ChunkPayload::Encoded(test_chunk.clone())),
        r#ref: ref_id,
        ..Default::default()
    })
    .unwrap();

    let got = q.get(ref_id).unwrap();
    assert_eq!(got.as_encoded(), Some(&test_chunk));

    {
        let (lk, cv) = &*gate;
        *lk.lock().unwrap() = true;
        cv.notify_all();
    }
    q.stop();
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunk_write_queue_test.go::TestChunkWriteQueue_WritingThroughQueue
#[test]
fn test_chunk_write_queue_writing_through_queue() {
    #[derive(Default)]
    struct Captured {
        series_ref: u64,
        mint: i64,
        maxt: i64,
        chk: Option<EncodedChunk>,
        ref_id: ChunkDiskMapperRef,
        cut_file: bool,
    }
    let captured = Arc::new(Mutex::new(Captured::default()));
    let cap2 = Arc::clone(&captured);

    let writer: WriteChunkFn = Arc::new(move |series_ref, mint, maxt, chk, ref_id, _, cut_file| {
        let mut g = cap2.lock().unwrap();
        g.series_ref = series_ref;
        g.mint = mint;
        g.maxt = maxt;
        g.chk = chk.and_then(|p| p.as_encoded().cloned());
        g.ref_id = ref_id;
        g.cut_file = cut_file;
        Ok(())
    });

    let q = ChunkWriteQueue::new(None, 1000, writer);
    let chunk = EncodedChunk::Xor(XorChunk::new());
    let ref_id = ChunkDiskMapperRef::new(321, 123);
    let done = Arc::new((Mutex::new(false), Condvar::new()));
    let done2 = Arc::clone(&done);

    q.add_job(ChunkWriteJob {
        series_ref: 1,
        mint: 2,
        maxt: 3,
        chk: Some(ChunkPayload::Encoded(chunk.clone())),
        r#ref: ref_id,
        cut_file: true,
        is_ooo: false,
        callback: Some(Box::new(move |_| {
            let (lk, cv) = &*done2;
            *lk.lock().unwrap() = true;
            cv.notify_all();
        })),
    })
    .unwrap();

    let (lk, cv) = &*done;
    let mut g = lk.lock().unwrap();
    while !*g {
        g = cv.wait(g).unwrap();
    }
    q.stop();

    let cap = captured.lock().unwrap();
    assert_eq!(cap.series_ref, 1);
    assert_eq!(cap.mint, 2);
    assert_eq!(cap.maxt, 3);
    assert_eq!(cap.chk.as_ref(), Some(&chunk));
    assert_eq!(cap.ref_id, ref_id);
    assert!(cap.cut_file);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunk_write_queue_test.go::TestChunkWriteQueue_WrappingAroundSizeLimit
#[test]
fn test_chunk_write_queue_wrapping_around_size_limit() {
    let size_limit = 100usize;
    let permits = Arc::new((Mutex::new(0usize), Condvar::new()));
    let permits_w = Arc::clone(&permits);
    let closed_flag = Arc::new(AtomicBool::new(false));
    let closed_w = Arc::clone(&closed_flag);

    let writer: WriteChunkFn = Arc::new(move |_, _, _, _, _, _, _| {
        let (lk, cv) = &*permits_w;
        let mut g = lk.lock().unwrap();
        while *g == 0 && !closed_w.load(Ordering::SeqCst) {
            g = cv.wait(g).unwrap();
        }
        if *g > 0 {
            *g -= 1;
        }
        Ok(())
    });

    let q = Arc::new(ChunkWriteQueue::new(None, size_limit, writer));
    let ref_counter = Arc::new(AtomicU64::new(0));
    let cb_done = Arc::new(AtomicUsize::new(0));

    let add_chunk = |q: &ChunkWriteQueue| {
        let r = ChunkDiskMapperRef(ref_counter.fetch_add(1, Ordering::SeqCst));
        let cb_d = Arc::clone(&cb_done);
        q.add_job(ChunkWriteJob {
            r#ref: r,
            callback: Some(Box::new(move |res| {
                assert!(res.is_ok());
                cb_d.fetch_add(1, Ordering::SeqCst);
            })),
            ..Default::default()
        })
        .unwrap();
    };
    let unblock = || {
        let (lk, cv) = &*permits;
        *lk.lock().unwrap() += 1;
        cv.notify_one();
    };

    for _ in 0..size_limit / 2 {
        add_chunk(&q);
    }
    for _ in 0..size_limit / 2 {
        unblock();
    }
    // Wait until first half drained before testing full queue.
    let start = Instant::now();
    while cb_done.load(Ordering::SeqCst) < size_limit / 2 && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(1));
    }

    for _ in 0..=size_limit {
        add_chunk(&q);
    }
    // Wait briefly so worker pops 1st item and blocks in writer.
    let start = Instant::now();
    while !q.queue_is_full() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(q.queue_is_full());

    let added = Arc::new(AtomicBool::new(false));
    let added2 = Arc::clone(&added);
    let q2 = Arc::clone(&q);
    let r_extra = ChunkDiskMapperRef(ref_counter.fetch_add(1, Ordering::SeqCst));
    let cb_d2 = Arc::clone(&cb_done);
    let h = thread::spawn(move || {
        q2.add_job(ChunkWriteJob {
            r#ref: r_extra,
            callback: Some(Box::new(move |res| {
                assert!(res.is_ok());
                cb_d2.fetch_add(1, Ordering::SeqCst);
            })),
            ..Default::default()
        })
        .unwrap();
        added2.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));
    assert!(!added.load(Ordering::SeqCst));

    unblock();
    h.join().unwrap();
    assert!(added.load(Ordering::SeqCst));

    let start = Instant::now();
    while !q.queue_is_full() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(q.queue_is_full());

    for _ in 0..=size_limit {
        assert!(!q.queue_is_empty());
        unblock();
    }
    let start = Instant::now();
    while !q.queue_is_empty() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(q.queue_is_empty());

    closed_flag.store(true, Ordering::SeqCst);
    permits.1.notify_all();
    q.stop();
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/chunk_write_queue_test.go::TestChunkWriteQueue_HandlerErrorViaCallback
#[test]
fn test_chunk_write_queue_handler_error_via_callback() {
    let writer: WriteChunkFn = Arc::new(|_, _, _, _, _, _, _| Err(HeadChunksError::Closed));
    let q = ChunkWriteQueue::new(None, 1, writer);
    let got = Arc::new((Mutex::new(None::<HeadChunksError>), Condvar::new()));
    let got2 = Arc::clone(&got);
    q.add_job(ChunkWriteJob {
        callback: Some(Box::new(move |res| {
            let (lk, cv) = &*got2;
            *lk.lock().unwrap() = res.err();
            cv.notify_all();
        })),
        ..Default::default()
    })
    .unwrap();

    let (lk, cv) = &*got;
    let mut g = lk.lock().unwrap();
    while g.is_none() {
        g = cv.wait(g).unwrap();
    }
    assert!(matches!(*g, Some(HeadChunksError::Closed)));
    q.stop();
}

fn random_xor_chunk(rng: &mut XorShift64) -> EncodedChunk {
    let mut xc = XorChunk::new();
    let len = rng.next_usize(120);
    {
        let mut app = xc.appender().unwrap();
        let mut t = 0i64;
        for _ in 0..len {
            t += 1 + (rng.next_usize(100) as i64);
            app.append(0, t, rng.next_f64()).unwrap();
        }
    }
    EncodedChunk::Xor(xc)
}

fn run_head_chunks_with_queue_sizes<F>(f: F)
where
    F: Fn(usize),
{
    for q_size in [0usize, 1_000_000usize] {
        f(q_size);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestChunkDiskMapper_WriteChunk_Chunk_IterateChunks
#[test]
fn test_chunk_disk_mapper_write_chunk_chunk_iterate_chunks() {
    run_head_chunks_with_queue_sizes(|q_size| {
        let dir = tempfile::tempdir().unwrap();
        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();

        #[derive(Clone)]
        struct Expected {
            series_ref: u64,
            chunk_ref: ChunkDiskMapperRef,
            mint: i64,
            maxt: i64,
            num_samples: u16,
            chunk: EncodedChunk,
            is_ooo: bool,
        }

        let mut expected_bytes = Vec::new();
        let mut next_chunk_offset = HEAD_CHUNK_FILE_HEADER_SIZE as u64;
        let mut expected_data = Vec::new();
        let mut rng = XorShift64::new(0x1234_5678);
        let mut total_chunks = 0usize;

        while hrw.cur_file_sequence() < 3 || hrw.write_buf_buffered() == 0 {
            let mut add_chunks = |num_chunks: usize| {
                for _ in 0..num_chunks {
                    let series_ref = rng.next_u64() & 0x7FFF_FFFF;
                    let mint = (total_chunks as i64) * 1000 + 1;
                    let maxt = (total_chunks as i64 + 1) * 1000;
                    let chunk = random_xor_chunk(&mut rng);
                    let is_ooo = rng.next_usize(2) == 0;
                    total_chunks += 1;

                    let done = Arc::new((Mutex::new(None::<Result<(), HeadChunksError>>), Condvar::new()));
                    let done2 = Arc::clone(&done);
                    let chk_ref = hrw.write_chunk(series_ref, mint, maxt, chunk.clone(), is_ooo, move |res| {
                        let (lk, cv) = &*done2;
                        *lk.lock().unwrap() = Some(res);
                        cv.notify_all();
                    });
                    let (lk, cv) = &*done;
                    let mut g = lk.lock().unwrap();
                    while g.is_none() {
                        g = cv.wait(g).unwrap();
                    }
                    g.take().unwrap().unwrap();

                    expected_data.push(Expected {
                        series_ref,
                        chunk_ref: chk_ref,
                        mint,
                        maxt,
                        num_samples: chunk.num_samples() as u16,
                        chunk: chunk.clone(),
                        is_ooo,
                    });

                    if hrw.cur_file_sequence() == 1 {
                        assert_eq!(chk_ref, ChunkDiskMapperRef::new(1, next_chunk_offset as u32));
                        let mut meta_buf = [0u8; MAX_HEAD_CHUNK_META_SIZE];
                        meta_buf[0..8].copy_from_slice(&series_ref.to_be_bytes());
                        meta_buf[8..16].copy_from_slice(&mint.to_be_bytes());
                        meta_buf[16..24].copy_from_slice(&maxt.to_be_bytes());
                        let mut enc = chunk.encoding().as_u8();
                        if is_ooo {
                            enc |= OUT_OF_ORDER_MASK;
                        }
                        meta_buf[24] = enc;
                        let ulen = mimir_tsdb::encoding::put_uvarint(&mut meta_buf[25..], chunk.as_bytes().len() as u64);
                        let hdr_len = 25 + ulen;
                        let crc = crc32c::crc32c_append(crc32c::crc32c(&meta_buf[..hdr_len]), chunk.as_bytes());
                        expected_bytes.extend_from_slice(&meta_buf[..hdr_len]);
                        expected_bytes.extend_from_slice(chunk.as_bytes());
                        expected_bytes.extend_from_slice(&crc.to_be_bytes());
                        next_chunk_offset += (hdr_len + chunk.as_bytes().len() + CRC_SIZE) as u64;
                    }
                }
            };
            add_chunks(100);
            hrw.cut_new_file();
            add_chunks(10);
        }

        assert_eq!(hrw.mmapped_file_count(), 3);
        let actual_bytes = fs::read(head_chunk_file_path(dir.path(), 1)).unwrap();
        assert_eq!(
            u32::from_be_bytes(actual_bytes[0..MAGIC_CHUNKS_SIZE].try_into().unwrap()),
            MAGIC_HEAD_CHUNKS
        );
        let file_end = HEAD_CHUNK_FILE_HEADER_SIZE + expected_bytes.len();
        assert_eq!(&actual_bytes[HEAD_CHUNK_FILE_HEADER_SIZE..file_end], expected_bytes.as_slice());

        for exp in &expected_data {
            let act = hrw.chunk(exp.chunk_ref).unwrap();
            assert_eq!(act.as_bytes(), exp.chunk.as_bytes());
        }

        hrw.close().unwrap();
        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut idx = 0usize;
        hrw.iterate_all_chunks_raw(|rec, _| {
            let exp = &expected_data[idx];
            assert_eq!(rec.series_ref, exp.series_ref);
            assert_eq!(rec.chunk_ref, exp.chunk_ref);
            assert_eq!(rec.mint, exp.mint);
            assert_eq!(rec.maxt, exp.maxt);
            assert_eq!(rec.num_samples, exp.num_samples);
            assert_eq!(rec.is_ooo, exp.is_ooo);
            let act = hrw.chunk(exp.chunk_ref).unwrap();
            assert_eq!(act.as_bytes(), exp.chunk.as_bytes());
            idx += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(idx, expected_data.len());
        hrw.close().unwrap();
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestChunkDiskMapper_WriteUnsupportedChunk_Chunk_IterateChunks
#[test]
fn test_chunk_disk_mapper_write_unsupported_chunk_chunk_iterate_chunks() {
    const ENC_UNSUPPORTED_XOR: u8 = (Encoding::Xor as u8) | 0b0100_0000;
    run_head_chunks_with_queue_sizes(|q_size| {
        let dir = tempfile::tempdir().unwrap();
        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut rng = XorShift64::new(0xBEEF);
        let uc = random_xor_chunk(&mut rng);
        let uc_bytes = uc.as_bytes().to_vec();

        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let done2 = Arc::clone(&done);
        let uc_ref = hrw.write_payload(
            42,
            1,
            1000,
            ChunkPayload::Raw {
                encoding: ENC_UNSUPPORTED_XOR,
                bytes: uc_bytes,
            },
            false,
            move |res| {
                res.unwrap();
                let (lk, cv) = &*done2;
                *lk.lock().unwrap() = true;
                cv.notify_all();
            },
        );
        let (lk, cv) = &*done;
        let mut g = lk.lock().unwrap();
        while !*g {
            g = cv.wait(g).unwrap();
        }
        assert_eq!(hrw.mmapped_file_count(), 1);
        hrw.close().unwrap();

        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut seen = 0usize;
        hrw.iterate_all_chunks_raw(|rec, raw_enc| {
            assert_eq!(rec.series_ref, 42);
            assert_eq!(rec.chunk_ref, uc_ref);
            assert_eq!(rec.mint, 1);
            assert_eq!(rec.maxt, 1000);
            assert_eq!(raw_enc, ENC_UNSUPPORTED_XOR);

            let err = hrw.chunk(rec.chunk_ref).unwrap_err();
            assert!(err.to_string().contains("invalid chunk encoding \"<unknown>\""));
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
        hrw.close().unwrap();
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestChunkDiskMapper_Truncate
#[test]
fn test_chunk_disk_mapper_truncate() {
    run_head_chunks_with_queue_sizes(|q_size| {
        let dir = tempfile::tempdir().unwrap();
        let mut hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut time_range = 0i64;
        let mut rng = XorShift64::new(0x1111);

        let add_chunk = |hrw: &ChunkDiskMapper, time_range: &mut i64, rng: &mut XorShift64| {
            let mint = *time_range + 1;
            let maxt = *time_range + 99;
            let done = Arc::new((Mutex::new(false), Condvar::new()));
            let done2 = Arc::clone(&done);
            hrw.write_chunk(1, mint, maxt, random_xor_chunk(rng), false, move |res| {
                res.unwrap();
                let (lk, cv) = &*done2;
                *lk.lock().unwrap() = true;
                cv.notify_all();
            });
            let (lk, cv) = &*done;
            let mut g = lk.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
            *time_range += 100;
        };

        let verify_files = |hrw: &ChunkDiskMapper, expected: &[usize]| {
            let count = fs::read_dir(hrw.dir()).unwrap().count();
            assert_eq!(count, expected.len());
            assert_eq!(hrw.mmapped_file_count(), expected.len());
            for &seq in expected {
                assert!(hrw.has_mmapped_file(seq), "missing mmapped seq {seq}");
            }
        };

        for _ in 1..=7 {
            hrw.cut_new_file();
            add_chunk(&hrw, &mut time_range, &mut rng);
        }
        verify_files(&hrw, &[1, 2, 3, 4, 5, 6, 7]);

        hrw.truncate(3).unwrap();
        add_chunk(&hrw, &mut time_range, &mut rng);
        verify_files(&hrw, &[3, 4, 5, 6, 7, 8]);

        hrw.close().unwrap();
        hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        verify_files(&hrw, &[3, 4, 5, 6, 7, 8]);

        add_chunk(&hrw, &mut time_range, &mut rng);
        verify_files(&hrw, &[3, 4, 5, 6, 7, 8, 9]);

        hrw.truncate(6).unwrap();
        verify_files(&hrw, &[6, 7, 8, 9]);

        hrw.truncate(6).unwrap();
        verify_files(&hrw, &[6, 7, 8, 9]);

        add_chunk(&hrw, &mut time_range, &mut rng);
        verify_files(&hrw, &[6, 7, 8, 9, 10]);

        hrw.truncate(8).unwrap();
        verify_files(&hrw, &[8, 9, 10]);

        hrw.truncate(10).unwrap();
        add_chunk(&hrw, &mut time_range, &mut rng);
        verify_files(&hrw, &[10, 11]);

        hrw.close().unwrap();
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestChunkDiskMapper_Truncate_PreservesFileSequence
#[test]
fn test_chunk_disk_mapper_truncate_preserves_file_sequence() {
    run_head_chunks_with_queue_sizes(|q_size| {
        let dir = tempfile::tempdir().unwrap();
        let mut hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut time_range = 0i64;
        let mut rng = XorShift64::new(0x2222);

        let add_chunk = |hrw: &ChunkDiskMapper, time_range: &mut i64, rng: &mut XorShift64| {
            let mint = *time_range + 1;
            let maxt = *time_range + 99;
            let done = Arc::new((Mutex::new(false), Condvar::new()));
            let done2 = Arc::clone(&done);
            hrw.write_chunk(1, mint, maxt, random_xor_chunk(rng), false, move |res| {
                res.unwrap();
                let (lk, cv) = &*done2;
                *lk.lock().unwrap() = true;
                cv.notify_all();
            });
            let (lk, cv) = &*done;
            let mut g = lk.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
            *time_range += 100;
        };

        let verify_files = |hrw: &ChunkDiskMapper, expected: &[usize]| {
            assert_eq!(fs::read_dir(hrw.dir()).unwrap().count(), expected.len());
            assert_eq!(hrw.mmapped_file_count(), expected.len());
            for &seq in expected {
                assert!(hrw.has_mmapped_file(seq));
            }
        };

        add_chunk(&hrw, &mut time_range, &mut rng); // 1
        hrw.cut_empty_file_for_test().unwrap(); // 2
        add_chunk(&hrw, &mut time_range, &mut rng);
        hrw.cut_empty_file_for_test().unwrap(); // 3
        add_chunk(&hrw, &mut time_range, &mut rng);
        hrw.cut_empty_file_for_test().unwrap(); // 4 (empty)
        hrw.cut_empty_file_for_test().unwrap(); // 5
        add_chunk(&hrw, &mut time_range, &mut rng);
        hrw.cut_empty_file_for_test().unwrap(); // 6 (empty)

        verify_files(&hrw, &[1, 2, 3, 4, 5, 6]);

        hrw.truncate(3).unwrap();
        verify_files(&hrw, &[3, 4, 5, 6]);

        add_chunk(&hrw, &mut time_range, &mut rng);
        verify_files(&hrw, &[3, 4, 5, 6]);

        hrw.truncate(5).unwrap();
        add_chunk(&hrw, &mut time_range, &mut rng);
        verify_files(&hrw, &[5, 6, 7]);

        hrw.close().unwrap();
        hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        verify_files(&hrw, &[5, 6, 7]);
        hrw.close().unwrap();
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestChunkDiskMapper_Truncate_WriteQueueRaceCondition
#[test]
fn test_chunk_disk_mapper_truncate_write_queue_race_condition() {
    let dir = tempfile::tempdir().unwrap();
    let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, 1_000_000, None).unwrap();
    let q = hrw.write_queue().unwrap();
    let orig = q.write_chunk_fn();
    q.set_write_chunk_for_test(Arc::new(move |s, mint, maxt, chk, ref_id, is_ooo, cut| {
        thread::sleep(Duration::from_millis(100));
        orig(s, mint, maxt, chk, ref_id, is_ooo, cut)
    }));

    let done = Arc::new((Mutex::new(0usize), Condvar::new()));
    let mut rng = XorShift64::new(0x3333);

    let d1 = Arc::clone(&done);
    let r1 = hrw.write_chunk(1, 0, 10, random_xor_chunk(&mut rng), false, move |res| {
        res.unwrap();
        let (lk, cv) = &*d1;
        *lk.lock().unwrap() += 1;
        cv.notify_all();
    });
    assert_eq!(r1.unpack().0, 1);

    hrw.truncate(1).unwrap();
    hrw.cut_new_file();

    let d2 = Arc::clone(&done);
    let r2 = hrw.write_chunk(1, 0, 10, random_xor_chunk(&mut rng), false, move |res| {
        res.unwrap();
        let (lk, cv) = &*d2;
        *lk.lock().unwrap() += 1;
        cv.notify_all();
    });
    assert_eq!(r2.unpack().0, 2);

    let (lk, cv) = &*done;
    let mut g = lk.lock().unwrap();
    while *g < 2 {
        g = cv.wait(g).unwrap();
    }
    hrw.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestHeadReadWriter_TruncateAfterFailedIterateChunks
#[test]
fn test_head_read_writer_truncate_after_failed_iterate_chunks() {
    run_head_chunks_with_queue_sizes(|q_size| {
        let dir = tempfile::tempdir().unwrap();
        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut rng = XorShift64::new(0x4444);
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let done2 = Arc::clone(&done);
        hrw.write_chunk(1, 0, 1000, random_xor_chunk(&mut rng), false, move |res| {
            res.unwrap();
            let (lk, cv) = &*done2;
            *lk.lock().unwrap() = true;
            cv.notify_all();
        });
        let (lk, cv) = &*done;
        let mut g = lk.lock().unwrap();
        while !*g {
            g = cv.wait(g).unwrap();
        }
        hrw.close().unwrap();

        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let err = hrw
            .iterate_all_chunks_raw(|_, _| Err(HeadChunksError::CannotHandle("random error".into())))
            .unwrap_err();
        assert!(err.to_string().contains("random error"));
        hrw.truncate(2000).unwrap();
        hrw.close().unwrap();
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_test.go::TestHeadReadWriter_ReadRepairOnEmptyLastFile
#[test]
fn test_head_read_writer_read_repair_on_empty_last_file() {
    run_head_chunks_with_queue_sizes(|q_size| {
        let dir = tempfile::tempdir().unwrap();
        let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
        let mut time_range = 0i64;
        let mut rng = XorShift64::new(0x5555);

        let mut add_chunk = || {
            let mint = time_range + 1;
            let maxt = time_range + 99;
            let done = Arc::new((Mutex::new(false), Condvar::new()));
            let done2 = Arc::clone(&done);
            hrw.write_chunk(1, mint, maxt, random_xor_chunk(&mut rng), false, move |res| {
                res.unwrap();
                let (lk, cv) = &*done2;
                *lk.lock().unwrap() = true;
                cv.notify_all();
            });
            let (lk, cv) = &*done;
            let mut g = lk.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
            time_range += 100;
        };

        add_chunk(); // 1
        hrw.cut_new_file();
        add_chunk(); // 2
        hrw.cut_new_file();
        add_chunk(); // 3
        assert_eq!(hrw.mmapped_file_count(), 3);
        let last_file = 3usize;
        hrw.close().unwrap();

        let write_corrupt = |b: &[u8]| {
            let fname = head_chunk_file_path(dir.path(), last_file + 1);
            fs::write(&fname, b).unwrap();
        };

        let check_repair = || {
            let hrw = ChunkDiskMapper::open(dir.path(), DEFAULT_WRITE_BUFFER_SIZE, q_size, None).unwrap();
            assert_eq!(hrw.mmapped_file_count(), 3);
            for seq in 1..=3 {
                assert!(hrw.has_mmapped_file(seq));
            }
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 3);
            hrw.close().unwrap();
        };

        write_corrupt(&[]);
        check_repair();

        write_corrupt(&[0, 0, 0, 0, 0, 0, 0, 0]);
        check_repair();

        write_corrupt(&[1, 2]);
        check_repair();

        check_repair();

        struct FailReader(Arc<AtomicBool>);
        impl Read for FailReader {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"))
            }
        }
        impl Drop for FailReader {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let closed = Arc::new(AtomicBool::new(false));
        let rdr = FailReader(Arc::clone(&closed));
        let mut buf = [0u8; MAGIC_CHUNKS_SIZE];
        let err = read_chunk_header_and_close(rdr, &mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(closed.load(Ordering::SeqCst));
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/head_chunks_windows_test.go::TestRepairLastChunkFile_RetriesSharingViolation
#[test]
fn test_repair_last_chunk_file_retries_sharing_violation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001");
    fs::write(&path, []).unwrap();

    let mut files = BTreeMap::new();
    files.insert(1usize, path.clone());

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts2 = Arc::clone(&attempts);
    let sharing_blocked = Arc::new(AtomicBool::new(true));
    let blocked2 = Arc::clone(&sharing_blocked);

    // Release the simulated sharing lock right after the first failed attempt.
    let remover = move |p: &Path| -> io::Result<()> {
        let n = attempts2.fetch_add(1, Ordering::SeqCst);
        if n == 0 && blocked2.load(Ordering::SeqCst) {
            blocked2.store(false, Ordering::SeqCst);
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "sharing violation"));
        }
        fs::remove_file(p)
    };
    let is_retryable = |e: &io::Error| e.kind() == io::ErrorKind::PermissionDenied;

    repair_last_chunk_file_with_remover(&mut files, remover, is_retryable).unwrap();
    assert!(files.is_empty());
    assert!(!path.exists());
    assert!(attempts.load(Ordering::SeqCst) >= 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueuePushPopSingleGoroutine
#[test]
fn test_queue_push_pop_single_goroutine() {
    let mut rng = XorShift64::new(0x7777_8888);
    let max_size = if cfg!(miri) { 50 } else { 500 };
    let max_iters = if cfg!(miri) { 20 } else { 50 };

    for max_count in 1..max_size {
        let seg_sz = 1 + rng.next_usize(max_count);
        let queue = WriteJobQueue::new(max_count, seg_sz);
        let mut elements = 0usize;
        let mut last_write_id = 0u64;
        let mut last_read_id = 0u64;

        for _ in 0..max_iters {
            if elements < max_count {
                let mut to_write = rng.next_usize(max_count - elements);
                if to_write == 0 {
                    to_write = 1;
                }
                for _ in 0..to_write {
                    last_write_id += 1;
                    assert!(queue.push(ChunkWriteJob {
                        series_ref: last_write_id,
                        ..Default::default()
                    }));
                    elements += 1;
                }
            }
            if elements > 0 {
                let mut to_read = rng.next_usize(elements);
                if to_read == 0 {
                    to_read = 1;
                }
                for _ in 0..to_read {
                    last_read_id += 1;
                    let j = queue.pop().unwrap();
                    assert_eq!(j.series_ref, last_read_id);
                    elements -= 1;
                }
            }
            assert_eq!(queue.length(), elements);
            queue.assert_invariants();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueuePushBlocksOnFullQueue
#[test]
fn test_queue_push_blocks_on_full_queue() {
    let queue = Arc::new(WriteJobQueue::new(5, 5));
    let q2 = Arc::clone(&queue);
    let (tx, rx) = std::sync::mpsc::channel();

    let h = thread::spawn(move || {
        for s in 1..=5 {
            assert!(q2.push(ChunkWriteJob { series_ref: s, ..Default::default() }));
        }
        tx.send(Instant::now()).unwrap();
        assert!(q2.push(ChunkWriteJob { series_ref: 6, ..Default::default() }));
        tx.send(Instant::now()).unwrap();
    });

    let time_before_push = rx.recv().unwrap();
    let delay = Duration::from_millis(100);
    assert!(rx.recv_timeout(delay).is_err());

    let pop_time = Instant::now();
    let j = queue.pop().unwrap();
    assert_eq!(j.series_ref, 1);

    let time_after_push = rx.recv().unwrap();
    assert!(time_after_push >= pop_time);
    assert!(time_after_push.duration_since(time_before_push) >= delay);
    h.join().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueuePopBlocksOnEmptyQueue
#[test]
fn test_queue_pop_blocks_on_empty_queue() {
    let queue = Arc::new(WriteJobQueue::new(5, 5));
    let q2 = Arc::clone(&queue);
    let (tx, rx) = std::sync::mpsc::channel();

    let h = thread::spawn(move || {
        let j = q2.pop().unwrap();
        assert_eq!(j.series_ref, 1);
        tx.send(Instant::now()).unwrap();
        let j = q2.pop().unwrap();
        assert_eq!(j.series_ref, 2);
        tx.send(Instant::now()).unwrap();
    });

    assert!(queue.push(ChunkWriteJob { series_ref: 1, ..Default::default() }));
    let time_before_pop = rx.recv().unwrap();
    let delay = Duration::from_millis(100);
    assert!(rx.recv_timeout(delay).is_err());

    let push_time = Instant::now();
    assert!(queue.push(ChunkWriteJob { series_ref: 2, ..Default::default() }));
    let time_after_pop = rx.recv().unwrap();
    assert!(time_after_pop >= push_time);
    assert!(time_after_pop.duration_since(time_before_pop) >= delay);
    h.join().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueuePopUnblocksOnClose
#[test]
fn test_queue_pop_unblocks_on_close() {
    let queue = Arc::new(WriteJobQueue::new(5, 5));
    let q2 = Arc::clone(&queue);
    let (tx, rx) = std::sync::mpsc::channel();

    let h = thread::spawn(move || {
        let j = q2.pop().unwrap();
        assert_eq!(j.series_ref, 1);
        tx.send(Instant::now()).unwrap();
        assert!(q2.pop().is_none());
        tx.send(Instant::now()).unwrap();
    });

    assert!(queue.push(ChunkWriteJob { series_ref: 1, ..Default::default() }));
    let time_before_pop = rx.recv().unwrap();
    let delay = Duration::from_millis(100);
    assert!(rx.recv_timeout(delay).is_err());

    let close_time = Instant::now();
    queue.close();
    let time_after_pop = rx.recv().unwrap();
    assert!(time_after_pop >= close_time);
    assert!(time_after_pop.duration_since(time_before_pop) >= delay);
    h.join().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueuePopAfterCloseReturnsAllElements
#[test]
fn test_queue_pop_after_close_returns_all_elements() {
    const COUNT: u64 = 10;
    let queue = WriteJobQueue::new(COUNT as usize, COUNT as usize);
    for i in 0..COUNT {
        assert!(queue.push(ChunkWriteJob { series_ref: i, ..Default::default() }));
    }
    queue.close();
    assert!(!queue.push(ChunkWriteJob { series_ref: 11111, ..Default::default() }));
    for i in 0..COUNT {
        assert_eq!(queue.pop().unwrap().series_ref, i);
    }
    assert!(queue.pop().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueuePushPopManyGoroutines
#[test]
fn test_queue_push_pop_many_goroutines() {
    const READERS: usize = 5;
    const WRITERS: usize = 10;
    const WRITES: usize = 500;

    let queue = Arc::new(WriteJobQueue::new(1024, 64));
    let refs = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let mut reader_handles = Vec::new();
    for _ in 0..READERS {
        let q = Arc::clone(&queue);
        let refs = Arc::clone(&refs);
        reader_handles.push(thread::spawn(move || {
            while let Some(j) = q.pop() {
                refs.lock().unwrap().insert(j.series_ref);
            }
        }));
    }

    let id = Arc::new(AtomicU64::new(0));
    let mut writer_handles = Vec::new();
    for _ in 0..WRITERS {
        let q = Arc::clone(&queue);
        let id = Arc::clone(&id);
        writer_handles.push(thread::spawn(move || {
            for _ in 0..WRITES {
                let r = id.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(q.push(ChunkWriteJob { series_ref: r, ..Default::default() }));
            }
        }));
    }
    for h in writer_handles {
        h.join().unwrap();
    }
    queue.close();
    for h in reader_handles {
        h.join().unwrap();
    }
    assert_eq!(refs.lock().unwrap().len(), WRITERS * WRITES);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunks/queue_test.go::TestQueueSegmentIsKeptEvenIfEmpty
#[test]
fn test_queue_segment_is_kept_even_if_empty() {
    let queue = WriteJobQueue::new(1024, 64);
    assert!(queue.push(ChunkWriteJob { series_ref: 1, ..Default::default() }));
    assert!(queue.pop().is_some());
    assert_eq!(queue.first_segment_indices(), Some((1, 1)));
}

// ============================================================================
// Package 4: tsdb/wlog (37 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestLastCheckpoint
#[test]
fn test_last_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    assert!(last_checkpoint(dir.path()).unwrap().is_none());

    fs::create_dir_all(dir.path().join("checkpoint.0000")).unwrap();
    let (p, k) = last_checkpoint(dir.path()).unwrap().unwrap();
    assert_eq!(p, dir.path().join("checkpoint.0000"));
    assert_eq!(k, 0);

    fs::create_dir_all(dir.path().join("checkpoint.xyz")).unwrap();
    let (p, k) = last_checkpoint(dir.path()).unwrap().unwrap();
    assert_eq!(p, dir.path().join("checkpoint.0000"));
    assert_eq!(k, 0);

    fs::create_dir_all(dir.path().join("checkpoint.1")).unwrap();
    let (p, k) = last_checkpoint(dir.path()).unwrap().unwrap();
    assert_eq!(p, dir.path().join("checkpoint.1"));
    assert_eq!(k, 1);

    fs::create_dir_all(dir.path().join("checkpoint.1000")).unwrap();
    let (p, k) = last_checkpoint(dir.path()).unwrap().unwrap();
    assert_eq!(p, dir.path().join("checkpoint.1000"));
    assert_eq!(k, 1000);

    fs::create_dir_all(dir.path().join("checkpoint.99999999")).unwrap();
    let (p, k) = last_checkpoint(dir.path()).unwrap().unwrap();
    assert_eq!(p, dir.path().join("checkpoint.99999999"));
    assert_eq!(k, 99999999);

    fs::create_dir_all(dir.path().join("checkpoint.100000000")).unwrap();
    let (p, k) = last_checkpoint(dir.path()).unwrap().unwrap();
    assert_eq!(p, dir.path().join("checkpoint.100000000"));
    assert_eq!(k, 100000000);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestDeleteCheckpoints
#[test]
fn test_delete_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    delete_checkpoints(dir.path(), 0).unwrap();

    for name in ["checkpoint.00", "checkpoint.01", "checkpoint.02", "checkpoint.03"] {
        fs::create_dir_all(dir.path().join(name)).unwrap();
    }
    delete_checkpoints(dir.path(), 2).unwrap();
    let mut names: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["checkpoint.02", "checkpoint.03"]);

    for name in ["checkpoint.99999999", "checkpoint.100000000", "checkpoint.100000001"] {
        fs::create_dir_all(dir.path().join(name)).unwrap();
    }
    delete_checkpoints(dir.path(), 100000000).unwrap();
    let mut names: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["checkpoint.100000000", "checkpoint.100000001"]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestReadMinValidTime_NoCheckpointYet
#[test]
fn test_read_min_valid_time_no_checkpoint_yet() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(read_min_valid_time(dir.path()).unwrap(), None);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestReadMinValidTime_AfterCheckpoint
#[test]
fn test_read_min_valid_time_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
    create_checkpoint(dir.path(), 0, 1000, |_| true, 100, false, CompressionType::None).unwrap();
    assert_eq!(read_min_valid_time(dir.path()).unwrap(), Some(100));
    w.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestReadMinValidTime_ReflectsOnlyLatestCheckpoint
#[test]
fn test_read_min_valid_time_reflects_only_latest_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
    create_checkpoint(dir.path(), 0, 1000, |_| true, 100, false, CompressionType::None).unwrap();
    create_checkpoint(dir.path(), 1001, 2000, |_| true, 200, false, CompressionType::None).unwrap();

    assert_eq!(read_min_valid_time(dir.path()).unwrap(), Some(200));

    let (cp_dir, _) = last_checkpoint(dir.path()).unwrap().unwrap();
    let mut r = WlogReader::open_dir(&cp_dir).unwrap();
    let dec = RecordDecoder::new();
    let mut mvt_count = 0usize;
    while let Some(rec) = r.next_record().unwrap() {
        if dec.record_type(rec) == RecordType::MinValidTime {
            mvt_count += 1;
        }
    }
    assert_eq!(mvt_count, 1);
    w.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpoint_MinValidTimeNeverRegressesAcrossRestarts
#[test]
fn test_checkpoint_min_valid_time_never_regresses_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
    create_checkpoint(dir.path(), 0, 1000, |_| true, 5000, false, CompressionType::None).unwrap();
    assert_eq!(read_min_valid_time(dir.path()).unwrap(), Some(5000));

    create_checkpoint(dir.path(), 1001, 2000, |_| true, 100, false, CompressionType::None).unwrap();
    assert_eq!(read_min_valid_time(dir.path()).unwrap(), Some(5000));
    w.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestReadMinValidTime_OlderCheckpointWithoutRecord
#[test]
fn test_read_min_valid_time_older_checkpoint_without_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = WlogWriter::open(dir.path().join("checkpoint.0000"), CompressionType::None).unwrap();
    let enc = RecordEncoder::new(false);
    let rec = enc.series(
        &[RefSeries {
            ref_id: 0,
            labels: Labels::from_pairs([("a", "b")]),
        }],
        Vec::new(),
    );
    w.log(&[&rec]).unwrap();
    w.close().unwrap();

    assert_eq!(read_min_valid_time(dir.path()).unwrap(), None);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpoint
#[test]
fn test_checkpoint() {
    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let enc = RecordEncoder::new(enable_st);

            drop(create_segment_file(dir.path(), 100).unwrap());

            {
                let mut cp99 = WlogWriter::open(dir.path().join("checkpoint.0099"), compress).unwrap();
                let srec = enc.series(
                    &[
                        RefSeries { ref_id: 0, labels: Labels::from_pairs([("a", "b"), ("c", "0")]) },
                        RefSeries { ref_id: 1, labels: Labels::from_pairs([("a", "b"), ("c", "1")]) },
                    ],
                    Vec::new(),
                );
                cp99.log(&[&srec]).unwrap();
                cp99.log(&[&[255u8]]).unwrap();
                cp99.close().unwrap();
            }

            let mut w = WlogWriter::with_segment_size(dir.path(), 128 * 1024, compress).unwrap();
            let (mut samples_in_wal, mut hists_in_wal, mut fhists_in_wal) = (0usize, 0usize, 0usize);
            let mut last = 0i64;
            let mut i = 0usize;
            loop {
                let (_, n) = segments_range(dir.path()).unwrap().unwrap();
                if n >= 106 {
                    break;
                }
                if i == 0 {
                    let s = enc.series(
                        &[
                            RefSeries { ref_id: 2, labels: Labels::from_pairs([("a", "b"), ("c", "2")]) },
                            RefSeries { ref_id: 3, labels: Labels::from_pairs([("a", "b"), ("c", "3")]) },
                            RefSeries { ref_id: 4, labels: Labels::from_pairs([("a", "b"), ("c", "4")]) },
                            RefSeries { ref_id: 5, labels: Labels::from_pairs([("a", "b"), ("c", "5")]) },
                        ],
                        Vec::new(),
                    );
                    w.log(&[&s]).unwrap();
                    let m = enc.metadata(
                        &[
                            RefMetadata { ref_id: 2, metric_type: 0, unit: "unit".into(), help: "help".into() },
                            RefMetadata { ref_id: 3, metric_type: 0, unit: "unit".into(), help: "help".into() },
                            RefMetadata { ref_id: 4, metric_type: 0, unit: "unit".into(), help: "help".into() },
                            RefMetadata { ref_id: 5, metric_type: 0, unit: "unit".into(), help: "help".into() },
                        ],
                        Vec::new(),
                    );
                    w.log(&[&m]).unwrap();
                }

                let samples = [
                    RefSample { ref_id: 0, st: last - 1, t: last, v: i as f64 },
                    RefSample { ref_id: 1, st: last + 9999, t: last + 10000, v: i as f64 },
                    RefSample { ref_id: 2, st: last + 19999, t: last + 20000, v: i as f64 },
                    RefSample { ref_id: 3, st: last + 29999, t: last + 30000, v: i as f64 },
                ];
                let s_rec = enc.samples(&samples, Vec::new());
                w.log(&[&s_rec]).unwrap();
                samples_in_wal += 4;

                let h = generate_test_histogram(i);
                let hs = [
                    RefHistogramSample { ref_id: 0, st: last - 1, t: last, h: h.clone() },
                    RefHistogramSample { ref_id: 1, st: last + 9999, t: last + 10000, h: h.clone() },
                    RefHistogramSample { ref_id: 2, st: last + 19999, t: last + 20000, h: h.clone() },
                    RefHistogramSample { ref_id: 3, st: last + 29999, t: last + 30000, h },
                ];
                let (h_rec, _) = enc.histogram_samples(&hs, Vec::new());
                w.log(&[&h_rec]).unwrap();
                hists_in_wal += 4;

                let mut cbh = make_custom_bucket_histogram(i);
                cbh.positive_spans.clear();
                cbh.positive_buckets.clear();
                let cbhs = [
                    RefHistogramSample { ref_id: 0, st: last - 1, t: last, h: cbh.clone() },
                    RefHistogramSample { ref_id: 1, st: last + 9999, t: last + 10000, h: cbh.clone() },
                    RefHistogramSample { ref_id: 2, st: last + 19999, t: last + 20000, h: cbh.clone() },
                    RefHistogramSample { ref_id: 3, st: last + 29999, t: last + 30000, h: cbh },
                ];
                let cb_refs: Vec<_> = cbhs.iter().collect();
                let cbh_rec = enc.custom_buckets_histogram_samples(&cb_refs, Vec::new());
                w.log(&[&cbh_rec]).unwrap();
                hists_in_wal += 4;

                let fh = generate_test_float_histogram(i);
                let fhs = [
                    RefFloatHistogramSample { ref_id: 0, st: last - 1, t: last, fh: fh.clone() },
                    RefFloatHistogramSample { ref_id: 1, st: last + 9999, t: last + 10000, fh: fh.clone() },
                    RefFloatHistogramSample { ref_id: 2, st: last + 19999, t: last + 20000, fh: fh.clone() },
                    RefFloatHistogramSample { ref_id: 3, st: last + 29999, t: last + 30000, fh },
                ];
                let (fh_rec, _) = enc.float_histogram_samples(&fhs, Vec::new());
                w.log(&[&fh_rec]).unwrap();
                fhists_in_wal += 4;

                let mut cbh_f = make_custom_bucket_histogram(i);
                cbh_f.positive_spans.clear();
                cbh_f.positive_buckets.clear();
                let cbfh = cbh_f.to_float();
                let cbfhs = [
                    RefFloatHistogramSample { ref_id: 0, st: last - 1, t: last, fh: cbfh.clone() },
                    RefFloatHistogramSample { ref_id: 1, st: last + 9999, t: last + 10000, fh: cbfh.clone() },
                    RefFloatHistogramSample { ref_id: 2, st: last + 19999, t: last + 20000, fh: cbfh.clone() },
                    RefFloatHistogramSample { ref_id: 3, st: last + 29999, t: last + 30000, fh: cbfh },
                ];
                let cbf_refs: Vec<_> = cbfhs.iter().collect();
                let cbfh_rec = enc.custom_buckets_float_histogram_samples(&cbf_refs, Vec::new());
                w.log(&[&cbfh_rec]).unwrap();
                fhists_in_wal += 4;

                let ex_rec = enc.exemplars(
                    &[RefExemplar {
                        ref_id: 1,
                        t: last,
                        v: i as f64,
                        labels: Labels::from_pairs([("trace_id", format!("trace-{i}").as_str())]),
                    }],
                    Vec::new(),
                );
                w.log(&[&ex_rec]).unwrap();

                let ls = last.to_string();
                let m_rec = enc.metadata(
                    &[
                        RefMetadata { ref_id: 0, metric_type: 0, unit: ls.clone(), help: ls.clone() },
                        RefMetadata { ref_id: 1, metric_type: 0, unit: ls.clone(), help: ls.clone() },
                        RefMetadata { ref_id: 2, metric_type: 0, unit: ls.clone(), help: ls.clone() },
                        RefMetadata { ref_id: 3, metric_type: 0, unit: ls.clone(), help: ls },
                    ],
                    Vec::new(),
                );
                w.log(&[&m_rec]).unwrap();

                last += 100;
                i += 1;
            }
            let dir_path = w.dir().to_path_buf();
            w.close().unwrap();

            let stats = create_checkpoint(&dir_path, 100, 106, |x| x % 2 == 0, last / 2, enable_st, compress).unwrap();
            for entry in fs::read_dir(&dir_path).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name().into_string().unwrap();
                if let Ok(idx) = name.parse::<usize>() {
                    if idx < 107 {
                        fs::remove_file(entry.path()).unwrap();
                    }
                }
            }
            delete_checkpoints(&dir_path, 106).unwrap();

            assert_eq!(stats.total_samples, hists_in_wal + fhists_in_wal + samples_in_wal);
            assert!(stats.dropped_samples > 0);

            let entries: Vec<_> = fs::read_dir(&dir_path)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .collect();
            assert_eq!(entries, vec!["checkpoint.00000106"]);

            let mut r = WlogReader::open_dir(dir_path.join("checkpoint.00000106")).unwrap();
            let dec = RecordDecoder::new();
            let first_rec = r.next_record().unwrap().unwrap();
            assert_eq!(dec.record_type(first_rec), RecordType::MinValidTime);
            assert_eq!(dec.min_valid_time(first_rec).unwrap(), last / 2);

            let mut series = Vec::new();
            let mut metadata = Vec::new();
            let (mut s_cp, mut h_cp, mut fh_cp) = (0usize, 0usize, 0usize);

            while let Some(rec) = r.next_record().unwrap() {
                match dec.record_type(rec) {
                    RecordType::Series => dec.series_into(rec, &mut series).unwrap(),
                    RecordType::Samples | RecordType::SamplesV2 => {
                        let samples = dec.samples(rec).unwrap();
                        for s in &samples {
                            assert!(s.t >= last / 2);
                            if enable_st {
                                assert_eq!(s.st, s.t - 1);
                            } else {
                                assert_eq!(s.st, 0);
                            }
                        }
                        s_cp += samples.len();
                    }
                    RecordType::HistogramSamples
                    | RecordType::CustomBucketsHistogramSamples
                    | RecordType::HistogramSamplesV2 => {
                        let hs = dec.histogram_samples(rec).unwrap();
                        for h in &hs {
                            assert!(h.t >= last / 2);
                            if enable_st {
                                assert_eq!(h.st, h.t - 1);
                            } else {
                                assert_eq!(h.st, 0);
                            }
                        }
                        h_cp += hs.len();
                    }
                    RecordType::FloatHistogramSamples
                    | RecordType::CustomBucketsFloatHistogramSamples
                    | RecordType::FloatHistogramSamplesV2 => {
                        let fhs = dec.float_histogram_samples(rec).unwrap();
                        for h in &fhs {
                            assert!(h.t >= last / 2);
                            if enable_st {
                                assert_eq!(h.st, h.t - 1);
                            } else {
                                assert_eq!(h.st, 0);
                            }
                        }
                        fh_cp += fhs.len();
                    }
                    RecordType::Exemplars => {
                        for e in dec.exemplars(rec).unwrap() {
                            assert!(e.t >= last / 2);
                        }
                    }
                    RecordType::Metadata => dec.metadata_into(rec, &mut metadata).unwrap(),
                    _ => {}
                }
            }

            let s_ratio = (s_cp as f64) / (samples_in_wal as f64);
            let h_ratio = (h_cp as f64) / (hists_in_wal as f64);
            let fh_ratio = (fh_cp as f64) / (fhists_in_wal as f64);
            assert!(s_ratio > 0.5 && s_ratio < 0.8, "enable_st={enable_st} compress={compress:?} last={last} s_cp={s_cp}/{samples_in_wal} s_ratio={s_ratio}");
            assert!(h_ratio > 0.5 && h_ratio < 0.8, "enable_st={enable_st} compress={compress:?} last={last} h_cp={h_cp}/{hists_in_wal} h_ratio={h_ratio}");
            assert!(fh_ratio > 0.5 && fh_ratio < 0.8, "enable_st={enable_st} compress={compress:?} last={last} fh_cp={fh_cp}/{fhists_in_wal} fh_ratio={fh_ratio}");

            assert_eq!(
                series,
                vec![
                    RefSeries { ref_id: 0, labels: Labels::from_pairs([("a", "b"), ("c", "0")]) },
                    RefSeries { ref_id: 2, labels: Labels::from_pairs([("a", "b"), ("c", "2")]) },
                    RefSeries { ref_id: 4, labels: Labels::from_pairs([("a", "b"), ("c", "4")]) },
                ]
            );
            metadata.sort_by_key(|m| m.ref_id);
            let exp_last = (last - 100).to_string();
            assert_eq!(
                metadata,
                vec![
                    RefMetadata { ref_id: 0, metric_type: 0, unit: exp_last.clone(), help: exp_last.clone() },
                    RefMetadata { ref_id: 2, metric_type: 0, unit: exp_last.clone(), help: exp_last },
                    RefMetadata { ref_id: 4, metric_type: 0, unit: "unit".into(), help: "help".into() },
                ]
            );
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpointMetadataAcrossBatches
#[test]
fn test_checkpoint_metadata_across_batches() {
    const METADATA_BATCH_SIZE: usize = 1000;
    const FLUSH_THRESHOLD: usize = 1024 * 1024;
    const NUM_SERIES: usize = 2 * METADATA_BATCH_SIZE + 1;

    let dir = tempfile::tempdir().unwrap();
    let enc = RecordEncoder::new(false);
    let mut w = WlogWriter::with_segment_size(dir.path(), 8 * 1024 * 1024, CompressionType::None).unwrap();

    let mut expected = Vec::with_capacity(NUM_SERIES);
    for i in 0..NUM_SERIES {
        let m = RefMetadata {
            ref_id: i as u64,
            metric_type: 0,
            unit: "seconds".to_string(),
            help: format!("{}{}", "x".repeat(FLUSH_THRESHOLD / METADATA_BATCH_SIZE), i),
        };
        expected.push(m.clone());
        let srec = enc.series(
            &[RefSeries {
                ref_id: i as u64,
                labels: Labels::from_pairs([("a", i.to_string().as_str())]),
            }],
            Vec::new(),
        );
        let mrec = enc.metadata(&[m], Vec::new());
        w.log(&[&srec, &mrec]).unwrap();
    }
    let (first, last) = segments_range(dir.path()).unwrap().unwrap();
    w.close().unwrap();

    let stats = create_checkpoint(dir.path(), first, last, |_| true, 0, false, CompressionType::None).unwrap();
    assert_eq!(stats.total_metadata, NUM_SERIES);

    let mut r = WlogReader::open_dir(checkpoint_dir(dir.path(), last)).unwrap();
    let dec = RecordDecoder::new();
    let mut got = Vec::new();
    let mut batch_sizes = Vec::new();
    while let Some(rec) = r.next_record().unwrap() {
        if dec.record_type(rec) != RecordType::Metadata {
            continue;
        }
        let batch = dec.metadata(rec).unwrap();
        batch_sizes.push(batch.len());
        got.extend(batch);
    }
    assert_eq!(batch_sizes, vec![METADATA_BATCH_SIZE, METADATA_BATCH_SIZE, 1]);
    got.sort_by_key(|m| m.ref_id);
    assert_eq!(got, expected);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpoint_Tombstones
#[test]
fn test_checkpoint_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    drop(create_segment_file(dir.path(), 0).unwrap());

    let mut w = WlogWriter::with_segment_size(dir.path(), 128 * 1024, CompressionType::None).unwrap();
    let enc = RecordEncoder::new(false);
    let rec = enc.tombstones(
        &[
            Tombstone { ref_id: 1, mint: i64::MIN, maxt: i64::MAX },
            Tombstone { ref_id: 2, mint: i64::MIN, maxt: i64::MAX },
            Tombstone { ref_id: 3, mint: 0, maxt: 100 },
            Tombstone { ref_id: 4, mint: 0, maxt: 5 },
            Tombstone { ref_id: 5, mint: 1000, maxt: i64::MAX },
            Tombstone { ref_id: 6, mint: 0, maxt: 100 },
        ],
        Vec::new(),
    );
    w.log(&[&rec]).unwrap();
    let (first, last) = segments_range(dir.path()).unwrap().unwrap();
    w.next_segment().unwrap();
    w.close().unwrap();

    create_checkpoint(
        dir.path(),
        first,
        last,
        |id| id == 2 || id == 3 || id == 4,
        10,
        false,
        CompressionType::None,
    )
    .unwrap();

    let (cp_dir, _) = last_checkpoint(dir.path()).unwrap().unwrap();
    let mut r = WlogReader::open_dir(&cp_dir).unwrap();
    let dec = RecordDecoder::new();
    let mut stones = Vec::new();
    while let Some(rec) = r.next_record().unwrap() {
        if dec.record_type(rec) == RecordType::Tombstones {
            dec.tombstones_into(rec, &mut stones).unwrap();
        }
    }
    assert_eq!(
        stones,
        vec![
            Tombstone { ref_id: 2, mint: i64::MIN, maxt: i64::MAX },
            Tombstone { ref_id: 3, mint: 0, maxt: 100 },
        ]
    );
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpointV2HistogramsToV1
#[test]
fn test_checkpoint_v2_histograms_to_v1() {
    let exp_h = generate_test_histogram(0);
    let cb_h = make_custom_bucket_histogram(0);

    let dir = tempfile::tempdir().unwrap();
    let enc_v2 = RecordEncoder::new(true);
    let mut w = WlogWriter::with_segment_size(dir.path(), 128 * 1024, CompressionType::None).unwrap();

    let s_rec = enc_v2.series(
        &[
            RefSeries { ref_id: 0, labels: Labels::from_pairs([("a", "b"), ("c", "exp0")]) },
            RefSeries { ref_id: 1, labels: Labels::from_pairs([("a", "b"), ("c", "cb1")]) },
            RefSeries { ref_id: 2, labels: Labels::from_pairs([("a", "b"), ("c", "exp2")]) },
            RefSeries { ref_id: 3, labels: Labels::from_pairs([("a", "b"), ("c", "cb3")]) },
        ],
        Vec::new(),
    );
    w.log(&[&s_rec]).unwrap();

    let hist_samples = vec![
        RefHistogramSample { ref_id: 0, st: 0, t: 1000, h: exp_h.clone() },
        RefHistogramSample { ref_id: 1, st: 0, t: 1000, h: cb_h.clone() },
        RefHistogramSample { ref_id: 0, st: 0, t: 2000, h: exp_h },
        RefHistogramSample { ref_id: 1, st: 0, t: 2000, h: cb_h },
    ];
    let (h_rec, leftover) = enc_v2.histogram_samples(&hist_samples, Vec::new());
    assert!(leftover.is_empty());
    w.log(&[&h_rec]).unwrap();

    let fhist_samples: Vec<_> = hist_samples
        .iter()
        .map(|h| RefFloatHistogramSample {
            ref_id: h.ref_id + 2,
            st: 0,
            t: h.t,
            fh: h.h.to_float(),
        })
        .collect();
    let (fh_rec, fleft) = enc_v2.float_histogram_samples(&fhist_samples, Vec::new());
    assert!(fleft.is_empty());
    w.log(&[&fh_rec]).unwrap();
    w.close().unwrap();

    let (_, last) = segments_range(dir.path()).unwrap().unwrap();
    let stats = create_checkpoint(dir.path(), 0, last, |_| true, 0, false, CompressionType::None).unwrap();
    assert_eq!(stats.total_samples, hist_samples.len() + fhist_samples.len());
    assert_eq!(stats.dropped_samples, 0);

    let mut r = WlogReader::open_dir(checkpoint_dir(dir.path(), last)).unwrap();
    let dec = RecordDecoder::new();
    let mut counts: HashMap<RecordType, usize> = HashMap::new();
    while let Some(rec) = r.next_record().unwrap() {
        let rt = dec.record_type(rec);
        match rt {
            RecordType::HistogramSamples => {
                let hs = dec.histogram_samples(rec).unwrap();
                for h in &hs {
                    assert_eq!(h.ref_id, 0);
                    assert!(!h.h.uses_custom_buckets());
                }
                *counts.entry(rt).or_default() += hs.len();
            }
            RecordType::CustomBucketsHistogramSamples => {
                let hs = dec.histogram_samples(rec).unwrap();
                for h in &hs {
                    assert_eq!(h.ref_id, 1);
                    assert!(h.h.uses_custom_buckets());
                }
                *counts.entry(rt).or_default() += hs.len();
            }
            RecordType::FloatHistogramSamples => {
                let fhs = dec.float_histogram_samples(rec).unwrap();
                for h in &fhs {
                    assert_eq!(h.ref_id, 2);
                    assert!(!h.fh.uses_custom_buckets());
                }
                *counts.entry(rt).or_default() += fhs.len();
            }
            RecordType::CustomBucketsFloatHistogramSamples => {
                let fhs = dec.float_histogram_samples(rec).unwrap();
                for h in &fhs {
                    assert_eq!(h.ref_id, 3);
                    assert!(h.fh.uses_custom_buckets());
                }
                *counts.entry(rt).or_default() += fhs.len();
            }
            RecordType::HistogramSamplesV2 | RecordType::FloatHistogramSamplesV2 => {
                panic!("unexpected V2 record in V1 checkpoint");
            }
            _ => {}
        }
    }
    assert_eq!(counts.get(&RecordType::HistogramSamples), Some(&2));
    assert_eq!(counts.get(&RecordType::CustomBucketsHistogramSamples), Some(&2));
    assert_eq!(counts.get(&RecordType::FloatHistogramSamples), Some(&2));
    assert_eq!(counts.get(&RecordType::CustomBucketsFloatHistogramSamples), Some(&2));
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpointNoTmpFolderAfterError
#[test]
fn test_checkpoint_no_tmp_folder_after_error() {
    for st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), 64 * 1024, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(st);
        let rec = enc.series(
            &[RefSeries {
                ref_id: 0,
                labels: Labels::from_pairs([("a", "b"), ("c", "2")]),
            }],
            Vec::new(),
        );
        w.log(&[&rec]).unwrap();
        w.close().unwrap();

        let mut f = OpenOptions::new().write(true).open(segment_name(dir.path(), 0)).unwrap();
        f.seek(SeekFrom::Start(1)).unwrap();
        f.write_all(&[42]).unwrap();
        drop(f);

        assert!(create_checkpoint(dir.path(), 0, 1, |_| true, 0, st, CompressionType::None).is_err());

        for entry in fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            assert!(!name.ends_with(".tmp"), "leftover tmp folder: {name}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestCheckpointDeletesTemporaryCheckpoints
#[test]
fn test_checkpoint_deletes_temporary_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("checkpoint.00001000.tmp")).unwrap();
    let w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
    create_checkpoint(dir.path(), 0, 1000, |_| true, 1000, false, CompressionType::None).unwrap();
    w.close().unwrap();

    let dirs: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            e.file_type().unwrap().is_dir().then(|| e.file_name().into_string().unwrap())
        })
        .collect();
    assert_eq!(dirs, vec!["checkpoint.00001000"]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/checkpoint_test.go::TestDeleteTempCheckpoints
#[test]
fn test_delete_temp_checkpoints() {
    let cases = [
        (vec![], vec![]),
        (vec!["checkpoint.00001000.tmp"], vec![]),
        (
            vec!["checkpoint.00000001.tmp", "checkpoint.00001000.tmp", "checkpoint.00002000.tmp"],
            vec![],
        ),
        (
            vec![
                "checkpoint.00000001",
                "checkpoint.00000001.tmp",
                "checkpoint.00001000.tmp",
                "checkpoint.00002000",
            ],
            vec!["checkpoint.00000001", "checkpoint.00002000"],
        ),
    ];
    for (create, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        for name in create {
            fs::create_dir_all(dir.path().join(name)).unwrap();
        }
        delete_temp_checkpoints(dir.path()).unwrap();
        let mut actual: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let e = e.unwrap();
                e.file_type().unwrap().is_dir().then(|| e.file_name().into_string().unwrap())
            })
            .collect();
        actual.sort();
        assert_eq!(actual, expected);
    }
}

fn encoded_wal_record(t: u8, b: &[u8]) -> Vec<u8> {
    if t == REC_PAGE_TERM {
        let mut out = Vec::with_capacity(1 + b.len());
        out.push(0);
        out.extend_from_slice(b);
        return out;
    }
    let mut out = vec![0u8; RECORD_HEADER_SIZE + b.len()];
    out[0] = t;
    out[1..3].copy_from_slice(&(b.len() as u16).to_be_bytes());
    out[3..7].copy_from_slice(&crc32c::crc32c(b).to_be_bytes());
    out[RECORD_HEADER_SIZE..].copy_from_slice(b);
    out
}

type ReaderTestCase = (Vec<(u8, Vec<u8>)>, Vec<Vec<u8>>, bool);

fn reader_test_cases() -> Vec<ReaderTestCase> {
    let data: Vec<u8> = (0..100_000usize).map(|i| (i % 251) as u8).collect();
    vec![
        (
            vec![
                (REC_FULL, data[0..200].to_vec()),
                (REC_FIRST, data[200..300].to_vec()),
                (REC_LAST, data[300..400].to_vec()),
                (REC_FIRST, data[400..800].to_vec()),
                (REC_MIDDLE, data[800..900].to_vec()),
                (REC_PAGE_TERM, vec![0u8; PAGE_SIZE - 900 - RECORD_HEADER_SIZE * 5 - 1]),
                (REC_LAST, data[900..900].to_vec()),
                (REC_FIRST, data[900..1000].to_vec()),
                (REC_MIDDLE, data[1000..1200].to_vec()),
                (REC_MIDDLE, data[1200..30000].to_vec()),
                (REC_MIDDLE, data[30000..30001].to_vec()),
                (REC_MIDDLE, data[30001..30001].to_vec()),
                (REC_LAST, data[30001..32000].to_vec()),
            ],
            vec![
                data[0..200].to_vec(),
                data[200..400].to_vec(),
                data[400..900].to_vec(),
                data[900..32000].to_vec(),
            ],
            false,
        ),
        (
            vec![(REC_FULL, data[0..PAGE_SIZE - RECORD_HEADER_SIZE].to_vec())],
            vec![data[0..PAGE_SIZE - RECORD_HEADER_SIZE].to_vec()],
            false,
        ),
        (
            vec![(REC_FULL, data[0..PAGE_SIZE + 1].to_vec())],
            vec![],
            true,
        ),
        (
            vec![
                (REC_FULL, data[0..PAGE_SIZE / 2].to_vec()),
                (REC_FULL, data[0..PAGE_SIZE / 2].to_vec()),
            ],
            vec![data[0..PAGE_SIZE / 2].to_vec(), data[0..PAGE_SIZE / 2].to_vec()],
            false,
        ),
        (vec![(REC_MIDDLE, data[0..200].to_vec())], vec![], true),
        (vec![(REC_LAST, data[0..200].to_vec())], vec![], true),
        (
            vec![(REC_FIRST, data[0..200].to_vec()), (REC_FULL, data[200..400].to_vec())],
            vec![],
            true,
        ),
        (
            vec![
                (REC_FIRST, data[0..100].to_vec()),
                (REC_MIDDLE, data[100..200].to_vec()),
                (REC_FULL, data[200..400].to_vec()),
            ],
            vec![],
            true,
        ),
        (
            vec![(REC_FULL, data[0..100].to_vec()), (REC_PAGE_TERM, {
                let mut v = vec![0u8; PAGE_SIZE - RECORD_HEADER_SIZE - 102];
                v.push(1);
                v
            })],
            vec![data[0..100].to_vec()],
            true,
        ),
    ]
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestReader
#[test]
fn test_reader() {
    for (i, (recs, exp, fail)) in reader_test_cases().into_iter().enumerate() {
        let mut buf = Vec::new();
        for (t, b) in &recs {
            buf.extend_from_slice(&encoded_wal_record(*t, b));
        }

        {
            let mut r = WlogReader::from_reader(buf.as_slice());
            let mut got = Vec::new();
            let mut err = None;
            loop {
                match r.next_record() {
                    Ok(Some(rec)) => got.push(rec.to_vec()),
                    Ok(None) => break,
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                }
            }
            assert_eq!(got, exp, "WlogReader case {i}");
            assert_eq!(err.is_some(), fail, "WlogReader case {i} fail={fail}");
        }

        {
            let mut lr = LiveReader::new(Some(LiveReaderMetrics::new(None)), buf.as_slice());
            lr.eof_non_err = true;
            let mut got = Vec::new();
            while lr.next_record() {
                got.push(lr.record().to_vec());
            }
            assert_eq!(got, exp, "LiveReader case {i}");
            assert_eq!(lr.err().is_some(), fail, "LiveReader case {i} fail={fail}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestReader_Live
#[test]
fn test_reader_live() {
    for (i, (recs, exp, fail)) in reader_test_cases().into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("live_{i}"));
        let mut write_fd = OpenOptions::new().create(true).write(true).truncate(true).open(&path).unwrap();
        let read_fd = File::open(&path).unwrap();

        let recs_bg = recs.clone();
        let h = thread::spawn(move || {
            for (t, b) in recs_bg {
                write_fd.write_all(&encoded_wal_record(t, &b)).unwrap();
                write_fd.flush().unwrap();
                thread::yield_now();
            }
            drop(write_fd);
        });

        let mut reader = LiveReader::new(Some(LiveReaderMetrics::new(None)), read_fd);
        for e in &exp {
            while !reader.next_record() {
                assert!(LiveReader::<File>::is_eof_err(reader.err().unwrap()), "case {i}");
                thread::yield_now();
            }
            assert_eq!(reader.record(), e.as_slice(), "case {i}");
        }
        h.join().unwrap();
        assert!(!reader.next_record());
        if fail {
            assert!(reader.err().is_some(), "case {i} expected error");
        }
    }
}

fn generate_random_entries(w: &mut WlogWriter, rng: &mut XorShift64, count: usize) -> Vec<Vec<u8>> {
    let mut records = Vec::with_capacity(count);
    let mut batch: Vec<Vec<u8>> = Vec::new();
    for i in 0..count {
        let sz = match i % 5 {
            0 | 1 => 50,
            2 | 3 => PAGE_SIZE,
            _ => PAGE_SIZE * 8,
        };
        let len = rng.next_usize(sz);
        let mut rec = vec![0u8; len];
        rng.fill_bytes(&mut rec);
        records.push(rec.clone());
        batch.push(rec);
        if rng.next_usize(4) < 3 {
            let slices: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
            w.log(&slices).unwrap();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let slices: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
        w.log(&slices).unwrap();
    }
    records
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestReaderFuzz
#[test]
fn test_reader_fuzz() {
    let count = if cfg!(miri) { 40 } else { 500 };
    for compress in all_compressions() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), 128 * PAGE_SIZE, compress).unwrap();
        let mut rng = XorShift64::new(0xDEAD_BEEF_1234);
        let expected = generate_random_entries(&mut w, &mut rng, count);
        w.close().unwrap();

        {
            let mut r = WlogReader::open_dir(dir.path()).unwrap();
            for exp in &expected {
                let got = r.next_record().unwrap().unwrap();
                assert_eq!(got, exp.as_slice());
            }
            assert!(r.next_record().unwrap().is_none());
        }

        {
            let sbr = SegmentBufReader::open_dir(dir.path()).unwrap();
            let mut lr = LiveReader::new(None, sbr);
            lr.eof_non_err = true;
            for exp in &expected {
                assert!(lr.next_record());
                assert_eq!(lr.record(), exp.as_slice());
            }
            assert!(!lr.next_record());
            assert!(lr.err().is_none());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestReaderFuzz_Live
#[test]
fn test_reader_fuzz_live() {
    let count = if cfg!(miri) { 40 } else { 500 };
    for compress in all_compressions() {
        let dir = tempfile::tempdir().unwrap();
        let w = Arc::new(Mutex::new(
            WlogWriter::with_segment_size(dir.path(), 128 * PAGE_SIZE, compress).unwrap(),
        ));
        let w_bg = Arc::clone(&w);
        let expected = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let exp_bg = Arc::clone(&expected);
        let done = Arc::new(AtomicBool::new(false));
        let done_bg = Arc::clone(&done);

        let h = thread::spawn(move || {
            let mut rng = XorShift64::new(0xCAFE_BABE);
            for i in 0..count {
                let sz = match i % 5 {
                    0 | 1 => 50,
                    2 | 3 => PAGE_SIZE,
                    _ => PAGE_SIZE * 8,
                };
                let len = rng.next_usize(sz);
                let mut rec = vec![0u8; len];
                rng.fill_bytes(&mut rec);
                exp_bg.lock().unwrap().push(rec.clone());
                w_bg.lock().unwrap().log(&[&rec]).unwrap();
            }
            thread::sleep(Duration::from_millis(50));
            done_bg.store(true, Ordering::SeqCst);
        });

        let mut cur_seg = segments_range(dir.path()).unwrap().unwrap().0;
        let mut lr = LiveReader::new(None, File::open(segment_name(dir.path(), cur_seg)).unwrap());
        let mut read_idx = 0usize;

        loop {
            while lr.next_record() {
                let exp_rec = expected.lock().unwrap()[read_idx].clone();
                assert_eq!(lr.record(), exp_rec.as_slice());
                read_idx += 1;
            }
            assert!(LiveReader::<File>::is_eof_err(lr.err().unwrap()));

            let (_, last) = segments_range(dir.path()).unwrap().unwrap();
            if last > cur_seg {
                while lr.next_record() {
                    let exp_rec = expected.lock().unwrap()[read_idx].clone();
                    assert_eq!(lr.record(), exp_rec.as_slice());
                    read_idx += 1;
                }
                let seg_len = fs::metadata(segment_name(dir.path(), cur_seg)).unwrap().len() as usize;
                assert_eq!(lr.offset(), seg_len);
                cur_seg += 1;
                lr = LiveReader::new(None, File::open(segment_name(dir.path(), cur_seg)).unwrap());
                continue;
            }
            if done.load(Ordering::SeqCst) {
                while lr.next_record() {
                    let exp_rec = expected.lock().unwrap()[read_idx].clone();
                    assert_eq!(lr.record(), exp_rec.as_slice());
                    read_idx += 1;
                }
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        h.join().unwrap();
        assert_eq!(read_idx, count);
        w.lock().unwrap().close_mut().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestLiveReaderCorrupt_ShortFile
#[test]
fn test_live_reader_corrupt_short_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE, CompressionType::None).unwrap();
    let rec = vec![7u8; PAGE_SIZE - RECORD_HEADER_SIZE];
    w.log(&[&rec]).unwrap();
    w.close().unwrap();

    let f = OpenOptions::new().write(true).open(segment_name(dir.path(), 0)).unwrap();
    f.set_len((PAGE_SIZE / 2) as u64).unwrap();
    drop(f);

    let (m, _) = segments_range(dir.path()).unwrap().unwrap();
    let mut r = LiveReader::new(None, File::open(segment_name(dir.path(), m)).unwrap());
    assert!(!r.next_record());
    assert!(LiveReader::<File>::is_eof_err(r.err().unwrap()));
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestLiveReaderCorrupt_RecordTooLongAndShort
#[test]
fn test_live_reader_corrupt_record_too_long_and_short() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE * 2, CompressionType::None).unwrap();
    let rec = vec![9u8; PAGE_SIZE - RECORD_HEADER_SIZE];
    w.log(&[&rec]).unwrap();
    w.close().unwrap();

    let mut f = OpenOptions::new().write(true).open(segment_name(dir.path(), 0)).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&[REC_FULL, 0xFF, 0xFF]).unwrap();
    drop(f);

    let (m, _) = segments_range(dir.path()).unwrap().unwrap();
    let mut r = LiveReader::new(
        Some(LiveReaderMetrics::new(None)),
        File::open(segment_name(dir.path(), m)).unwrap(),
    );
    assert!(!r.next_record());
    let err = r.err().unwrap().to_string();
    assert!(err.contains("record length greater than a single page: 65542 > 32768"), "got {err}");
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/reader_test.go::TestReaderData
#[test]
fn test_reader_data() {
    let dir = match std::env::var("WALDIR") {
        Ok(d) if !d.is_empty() => d,
        _ => return,
    };
    let mut r = WlogReader::open_dir(&dir).unwrap();
    while r.next_record().unwrap().is_some() {}
    let sbr = SegmentBufReader::open_dir(Path::new(&dir)).unwrap();
    let mut lr = LiveReader::new(None, sbr);
    lr.eof_non_err = true;
    while lr.next_record() {}
    assert!(lr.err().is_none());
}

// ============================================================================
// watcher_test.go mock & 8 tests
// ============================================================================

#[derive(Default)]
struct WriteToMockInner {
    series_stored: Vec<RefSeries>,
    metadata_stored: Vec<RefMetadata>,
    samples_appended: Vec<RefSample>,
    exemplars_appended: Vec<RefExemplar>,
    histograms_appended: Vec<RefHistogramSample>,
    float_histograms_appended: Vec<RefFloatHistogramSample>,

    series_stores: usize,
    metadata_stores: usize,
    sample_appends: usize,
    exemplar_appends: usize,
    histogram_appends: usize,
    float_histograms_appends: usize,

    series_segment_indexes: HashMap<u64, usize>,
}

struct WriteToMock {
    inner: Mutex<WriteToMockInner>,
    delay: Duration,
}

impl WriteToMock {
    fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(WriteToMockInner::default()),
            delay,
        })
    }

    fn check_num_series(&self) -> usize {
        self.inner.lock().unwrap().series_segment_indexes.len()
    }
}

impl WriteTo for WriteToMock {
    fn append(&self, samples: &[RefSample]) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.sample_appends += 1;
        g.samples_appended.extend_from_slice(samples);
        drop(g);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        true
    }

    fn append_exemplars(&self, exemplars: &[RefExemplar]) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.exemplar_appends += 1;
        g.exemplars_appended.extend_from_slice(exemplars);
        drop(g);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        true
    }

    fn append_histograms(&self, histograms: &[RefHistogramSample]) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.histogram_appends += 1;
        g.histograms_appended.extend_from_slice(histograms);
        drop(g);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        true
    }

    fn append_float_histograms(&self, float_histograms: &[RefFloatHistogramSample]) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.float_histograms_appends += 1;
        g.float_histograms_appended.extend_from_slice(float_histograms);
        drop(g);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        true
    }

    fn store_series(&self, series: &[RefSeries], index: usize) {
        let mut g = self.inner.lock().unwrap();
        g.series_stores += 1;
        g.series_stored.extend_from_slice(series);
        for s in series {
            g.series_segment_indexes.insert(s.ref_id, index);
        }
        drop(g);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
    }

    fn store_metadata(&self, meta: &[RefMetadata]) {
        let mut g = self.inner.lock().unwrap();
        g.metadata_stores += 1;
        g.metadata_stored.extend_from_slice(meta);
        drop(g);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
    }

    fn update_series_segment(&self, series: &[RefSeries], index: usize) {
        let mut g = self.inner.lock().unwrap();
        for s in series {
            g.series_segment_indexes.insert(s.ref_id, index);
        }
    }

    fn series_reset(&self, index: usize) {
        let mut g = self.inner.lock().unwrap();
        g.series_segment_indexes.retain(|_, v| *v >= index);
    }
}

fn wait_eventually<F: FnMut() -> bool>(mut cond: F, timeout: Duration) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < timeout, "condition not met within {timeout:?}");
        thread::sleep(Duration::from_millis(20));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestWatcher_Tail
#[test]
fn test_watcher_tail() {
    const BATCHES: usize = 3;
    const SERIES_PER_BATCH: usize = 100;
    const EXEMPLARS_PER_SERIES: usize = 2;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let now = SystemTime::now();
            let now_ms = now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64;
            let future_ms = now_ms + 5000;
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            let enc = RecordEncoder::new(enable_st);

            struct Batch {
                series: Vec<RefSeries>,
                meta: Vec<RefMetadata>,
                samples: Vec<RefSample>,
                hists: Vec<RefHistogramSample>,
                fhists: Vec<RefFloatHistogramSample>,
                cb_hists: Vec<RefHistogramSample>,
                cb_fhists: Vec<RefFloatHistogramSample>,
                exemplars: Vec<RefExemplar>,
            }
            let mut batches = Vec::with_capacity(BATCHES);
            for b in 0..BATCHES {
                let mut series = Vec::new();
                let mut meta = Vec::new();
                let mut samples = Vec::new();
                let mut hists = Vec::new();
                let mut fhists = Vec::new();
                let mut cb_hists = Vec::new();
                let mut cb_fhists = Vec::new();
                let mut exemplars = Vec::new();
                for s in 0..SERIES_PER_BATCH {
                    let ref_id = (b * SERIES_PER_BATCH + s) as u64;
                    let name = format!("metric_{ref_id}");
                    series.push(RefSeries {
                        ref_id,
                        labels: Labels::from_pairs([("__name__", name.as_str())]),
                    });
                    meta.push(RefMetadata {
                        ref_id,
                        metric_type: MetricType::Counter as u8,
                        unit: "seconds".into(),
                        help: "help".into(),
                    });
                    for k in 0..10i64 {
                        samples.push(RefSample {
                            ref_id,
                            st: if enable_st { future_ms - 10 } else { 0 },
                            t: future_ms + k,
                            v: k as f64,
                        });
                    }
                    for k in 0..5i64 {
                        let h = generate_test_histogram(k as usize);
                        let cbh = make_custom_bucket_histogram(k as usize);
                        hists.push(RefHistogramSample {
                            ref_id,
                            st: if enable_st { future_ms - 10 } else { 0 },
                            t: future_ms + k,
                            h: h.clone(),
                        });
                        fhists.push(RefFloatHistogramSample {
                            ref_id,
                            st: if enable_st { future_ms - 10 } else { 0 },
                            t: future_ms + k,
                            fh: h.to_float(),
                        });
                        cb_hists.push(RefHistogramSample {
                            ref_id,
                            st: if enable_st { future_ms - 10 } else { 0 },
                            t: future_ms + k,
                            h: cbh.clone(),
                        });
                        cb_fhists.push(RefFloatHistogramSample {
                            ref_id,
                            st: if enable_st { future_ms - 10 } else { 0 },
                            t: future_ms + k,
                            fh: cbh.to_float(),
                        });
                    }
                    for k in 0..EXEMPLARS_PER_SERIES {
                        exemplars.push(RefExemplar {
                            ref_id,
                            t: future_ms + k as i64,
                            v: k as f64,
                            labels: Labels::from_pairs([("trace_id", format!("t-{k}").as_str())]),
                        });
                    }
                }
                batches.push(Batch {
                    series,
                    meta,
                    samples,
                    hists,
                    fhists,
                    cb_hists,
                    cb_fhists,
                    exemplars,
                });
            }

            let mut w = WlogWriter::with_segment_size(&wdir, 128 * PAGE_SIZE, compress).unwrap();
            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "test",
                Arc::clone(&wt),
                dir.path(),
                true,
                true,
                true,
            );
            watcher.set_start_time(now);
            watcher.start();

            let pre = enc.samples(
                &[
                    RefSample { ref_id: 1, st: 0, t: now_ms - 1000, v: 123.0 },
                    RefSample { ref_id: 2, st: 0, t: now_ms - 1000, v: 123.1 },
                ],
                Vec::new(),
            );
            w.log(&[&pre]).unwrap();

            for batch in &batches {
                let s_rec = enc.series(&batch.series, Vec::new());
                let m_rec = enc.metadata(&batch.meta, Vec::new());
                let sm_rec = enc.samples(&batch.samples, Vec::new());
                let (h_rec, _) = enc.histogram_samples(&batch.hists, Vec::new());
                let (fh_rec, _) = enc.float_histogram_samples(&batch.fhists, Vec::new());
                let cb_refs: Vec<_> = batch.cb_hists.iter().collect();
                let cbh_rec = enc.custom_buckets_histogram_samples(&cb_refs, Vec::new());
                let cbf_refs: Vec<_> = batch.cb_fhists.iter().collect();
                let cbfh_rec = enc.custom_buckets_float_histogram_samples(&cbf_refs, Vec::new());
                let ex_rec = enc.exemplars(&batch.exemplars, Vec::new());

                w.log(&[&s_rec]).unwrap();
                w.log(&[&m_rec]).unwrap();
                w.log(&[&sm_rec]).unwrap();
                w.log(&[&h_rec]).unwrap();
                w.log(&[&fh_rec]).unwrap();
                w.log(&[&cbh_rec]).unwrap();
                w.log(&[&cbfh_rec]).unwrap();
                w.log(&[&ex_rec]).unwrap();
                watcher.notify();
            }

            wait_eventually(
                || wt.inner.lock().unwrap().exemplar_appends >= BATCHES,
                Duration::from_secs(30),
            );
            watcher.stop();
            w.close().unwrap();

            let g = wt.inner.lock().unwrap();
            assert_eq!(g.series_stores, BATCHES);
            assert_eq!(g.metadata_stores, BATCHES);
            assert_eq!(g.sample_appends, BATCHES);
            assert_eq!(g.histogram_appends, 2 * BATCHES);
            assert_eq!(g.float_histograms_appends, 2 * BATCHES);
            assert_eq!(g.exemplar_appends, BATCHES);
            for (i, b) in batches.iter().enumerate() {
                let sec = b.series.len();
                assert_eq!(&g.series_stored[i * sec..(i + 1) * sec], b.series.as_slice());
                let msec = b.meta.len();
                assert_eq!(&g.metadata_stored[i * msec..(i + 1) * msec], b.meta.as_slice());
                let ssec = b.samples.len();
                assert_eq!(&g.samples_appended[i * ssec..(i + 1) * ssec], b.samples.as_slice());
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestReadToEndNoCheckpoint
#[test]
fn test_read_to_end_no_checkpoint() {
    const SERIES_COUNT: usize = 10;
    const SAMPLES_COUNT: usize = 250;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            let mut w = WlogWriter::with_segment_size(&wdir, 128 * PAGE_SIZE, compress).unwrap();
            let enc = RecordEncoder::new(enable_st);
            let mut rng = XorShift64::new(0x1010);
            let mut recs = Vec::new();

            for i in 0..SERIES_COUNT {
                recs.push(enc.series(
                    &[RefSeries {
                        ref_id: i as u64,
                        labels: Labels::from_pairs([("__name__", format!("metric_{i}").as_str())]),
                    }],
                    Vec::new(),
                ));
                for j in 0..SAMPLES_COUNT {
                    recs.push(enc.samples(
                        &[RefSample {
                            ref_id: j as u64,
                            st: 0,
                            t: i as i64,
                            v: i as f64,
                        }],
                        Vec::new(),
                    ));
                    if rng.next_usize(4) < 3 {
                        let slices: Vec<&[u8]> = recs.iter().map(|v| v.as_slice()).collect();
                        w.log(&slices).unwrap();
                        recs.clear();
                    }
                }
            }
            if !recs.is_empty() {
                let slices: Vec<&[u8]> = recs.iter().map(|v| v.as_slice()).collect();
                w.log(&slices).unwrap();
            }

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "",
                Arc::clone(&wt),
                dir.path(),
                false,
                false,
                false,
            );
            watcher.set_read_timeout(Duration::from_secs(1));
            watcher.start();
            wait_eventually(|| wt.check_num_series() == SERIES_COUNT, Duration::from_secs(15));
            watcher.stop();
            w.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestReadToEndWithCheckpoint
#[test]
fn test_read_to_end_with_checkpoint() {
    const SEGMENT_SIZE: usize = 32 * 1024;
    const SERIES_COUNT: usize = 10;
    const SAMPLES_COUNT: usize = 250;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            let enc = RecordEncoder::new(enable_st);
            let mut w = WlogWriter::with_segment_size(&wdir, SEGMENT_SIZE, compress).unwrap();
            let mut rng = XorShift64::new(0x2020);

            for i in 0..SERIES_COUNT {
                let ref_id = (i + 100) as u64;
                let s = enc.series(
                    &[RefSeries {
                        ref_id,
                        labels: Labels::from_pairs([("__name__", format!("metric_{i}").as_str())]),
                    }],
                    Vec::new(),
                );
                w.log(&[&s]).unwrap();
                w.log(&[&[255u8]]).unwrap();
                for _ in 0..SAMPLES_COUNT {
                    let inner = rng.next_usize(ref_id as usize + 1) as u64;
                    let sm = enc.samples(&[RefSample { ref_id: inner, st: 0, t: i as i64, v: i as f64 }], Vec::new());
                    w.log(&[&sm]).unwrap();
                }
            }
            create_checkpoint(&wdir, 0, 1, |_| true, 0, enable_st, compress).unwrap();
            w.truncate(1).unwrap();

            for i in 0..SERIES_COUNT {
                let s = enc.series(
                    &[RefSeries {
                        ref_id: i as u64,
                        labels: Labels::from_pairs([("__name__", format!("metric_{i}").as_str())]),
                    }],
                    Vec::new(),
                );
                w.log(&[&s]).unwrap();
                for j in 0..SAMPLES_COUNT {
                    let sm = enc.samples(&[RefSample { ref_id: j as u64, st: 0, t: i as i64, v: i as f64 }], Vec::new());
                    w.log(&[&sm]).unwrap();
                }
            }

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "",
                Arc::clone(&wt),
                dir.path(),
                false,
                false,
                false,
            );
            watcher.set_read_timeout(Duration::from_secs(1));
            watcher.start();
            wait_eventually(|| wt.check_num_series() == SERIES_COUNT * 2, Duration::from_secs(15));
            watcher.stop();
            w.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestReadCheckpoint
#[test]
fn test_read_checkpoint() {
    const SERIES_COUNT: usize = 10;
    const SAMPLES_COUNT: usize = 250;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            drop(create_segment_file(&wdir, 30).unwrap());

            let enc = RecordEncoder::new(enable_st);
            let mut w = WlogWriter::with_segment_size(&wdir, 128 * PAGE_SIZE, compress).unwrap();
            let mut rng = XorShift64::new(0x3030);

            for i in 0..SERIES_COUNT {
                let ref_id = (i + 100) as u64;
                let s = enc.series(
                    &[RefSeries {
                        ref_id,
                        labels: Labels::from_pairs([("__name__", format!("metric_{i}").as_str())]),
                    }],
                    Vec::new(),
                );
                w.log(&[&s]).unwrap();
                for _ in 0..SAMPLES_COUNT {
                    let inner = rng.next_usize(ref_id as usize + 1) as u64;
                    let sm = enc.samples(&[RefSample { ref_id: inner, st: 0, t: i as i64, v: i as f64 }], Vec::new());
                    w.log(&[&sm]).unwrap();
                }
            }
            w.next_segment().unwrap();
            create_checkpoint(&wdir, 30, 31, |_| true, 0, enable_st, compress).unwrap();
            w.truncate(32).unwrap();

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "",
                Arc::clone(&wt),
                dir.path(),
                false,
                false,
                false,
            );
            watcher.start();
            wait_eventually(|| wt.check_num_series() >= SERIES_COUNT, Duration::from_secs(10));
            watcher.stop();
            assert_eq!(wt.check_num_series(), SERIES_COUNT);
            w.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestReadCheckpointMultipleSegments
#[test]
fn test_read_checkpoint_multiple_segments() {
    const SERIES_COUNT: usize = 40;
    const SAMPLES_COUNT: usize = 500;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            let enc = RecordEncoder::new(enable_st);
            let mut w = WlogWriter::with_segment_size(&wdir, PAGE_SIZE, compress).unwrap();
            let mut rng = XorShift64::new(0x4040);

            for j in 0..SERIES_COUNT {
                let s = enc.series(
                    &[RefSeries {
                        ref_id: j as u64,
                        labels: Labels::from_pairs([("__name__", "metric_0")]),
                    }],
                    Vec::new(),
                );
                w.log(&[&s]).unwrap();
                for _ in 0..SAMPLES_COUNT {
                    let inner = rng.next_usize(j + 1) as u64;
                    let sm = enc.samples(&[RefSample { ref_id: inner, st: 0, t: 0, v: 0.0 }], Vec::new());
                    w.log(&[&sm]).unwrap();
                }
            }
            w.close().unwrap();

            let cp_dir = wdir.join("checkpoint.000004");
            fs::create_dir(&cp_dir).unwrap();
            for i in 0..=4 {
                fs::rename(segment_name(&wdir, i), segment_name(&cp_dir, i)).unwrap();
            }

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "",
                Arc::clone(&wt),
                dir.path(),
                false,
                false,
                false,
            );
            let (last_cp, _) = last_checkpoint(watcher.wal_dir()).unwrap().unwrap();
            watcher.read_checkpoint(&last_cp, false).unwrap();
            assert!(wt.check_num_series() > 0);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestCheckpointSeriesReset
#[test]
fn test_checkpoint_series_reset() {
    const SEGMENT_SIZE: usize = 64 * 1024;
    const SERIES_COUNT: usize = 30;
    const SAMPLES_COUNT: usize = 700;

    let cases = [
        (CompressionType::None, false),
        (CompressionType::Snappy, false),
        (CompressionType::None, true),
        (CompressionType::Snappy, true),
    ];

    for (compress, enable_st) in cases {
        let dir = tempfile::tempdir().unwrap();
        let wdir = dir.path().join("wal");
        fs::create_dir_all(&wdir).unwrap();
        let enc = RecordEncoder::new(enable_st);
        let mut w = WlogWriter::with_segment_size(&wdir, SEGMENT_SIZE, compress).unwrap();
        let mut rng = XorShift64::new(0x5050);

        for i in 0..SERIES_COUNT {
            let ref_id = (i + 100) as u64;
            let s = enc.series(
                &[RefSeries {
                    ref_id,
                    labels: Labels::from_pairs([("__name__", format!("metric_{i}").as_str())]),
                }],
                Vec::new(),
            );
            w.log(&[&s]).unwrap();
            for _ in 0..SAMPLES_COUNT {
                let inner = rng.next_usize(ref_id as usize + 1) as u64;
                let sm = enc.samples(&[RefSample { ref_id: inner, st: 0, t: i as i64, v: i as f64 }], Vec::new());
                w.log(&[&sm]).unwrap();
            }
        }

        let wt = WriteToMock::new(Duration::ZERO);
        let watcher = Watcher::new(
            Some(WatcherMetrics::new(None)),
            None,
            "",
            Arc::clone(&wt),
            dir.path(),
            false,
            false,
            false,
        );
        watcher.set_read_timeout(Duration::from_secs(1));
        watcher.start();
        wait_eventually(|| wt.check_num_series() == SERIES_COUNT, Duration::from_secs(10));

        // Simulate lagging second watcher stopping at segment 1.
        let restart_wt = WriteToMock::new(Duration::ZERO);
        let restart_watcher = Watcher::new(
            Some(WatcherMetrics::new(None)),
            None,
            "",
            Arc::clone(&restart_wt),
            dir.path(),
            false,
            false,
            false,
        );
        restart_watcher.set_max_segment(Some(1));
        restart_watcher.run().unwrap();
        assert!(restart_wt.check_num_series() > 0 && restart_wt.check_num_series() < SERIES_COUNT);

        create_checkpoint(&wdir, 2, 4, |_| true, 0, true, compress).unwrap();
        w.truncate(5).unwrap();

        let (_, cpi) = last_checkpoint(&wdir).unwrap().unwrap();
        // When current segment <= checkpoint index, garbage_collect_series skips without recording.
        watcher.garbage_collect_series(cpi).unwrap();
        watcher.garbage_collect_series(cpi + 1).unwrap();
        watcher.stop();

        wait_eventually(|| wt.check_num_series() < SERIES_COUNT, Duration::from_secs(10));

        // Restarting watcher on a checkpoint missed by GC calls series_reset.
        restart_watcher.set_max_segment(Some(cpi + 1));
        restart_watcher.run().unwrap();
        for (&_ref_id, &seg) in &restart_wt.inner.lock().unwrap().series_segment_indexes {
            assert!(seg >= cpi, "series from segment {seg} was not garbage collected (cpi={cpi})");
        }
        w.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestRun_StartupTime
#[test]
fn test_run_startup_time() {
    const SEGMENTS: usize = 20;
    const SERIES_COUNT: usize = 40;
    const SAMPLES_COUNT: usize = 500;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            let enc = RecordEncoder::new(enable_st);
            let mut w = WlogWriter::with_segment_size(&wdir, PAGE_SIZE, compress).unwrap();
            let mut rng = XorShift64::new(0x6060);

            for i in 0..SEGMENTS {
                for j in 0..SERIES_COUNT {
                    let ref_id = (j + i * 100) as u64;
                    let s = enc.series(
                        &[RefSeries {
                            ref_id,
                            labels: Labels::from_pairs([("__name__", format!("metric_{i}").as_str())]),
                        }],
                        Vec::new(),
                    );
                    w.log(&[&s]).unwrap();
                    for _ in 0..SAMPLES_COUNT {
                        let inner = rng.next_usize(ref_id as usize + 1) as u64;
                        let sm = enc.samples(&[RefSample { ref_id: inner, st: 0, t: i as i64, v: i as f64 }], Vec::new());
                        w.log(&[&sm]).unwrap();
                    }
                }
            }
            w.close().unwrap();

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "",
                Arc::clone(&wt),
                dir.path(),
                false,
                false,
                false,
            );
            watcher.set_max_segment(Some(SEGMENTS));
            let start = Instant::now();
            watcher.run().unwrap();
            assert!(start.elapsed() < Duration::from_secs(15));
        }
    }
}

fn generate_wal_records_for_watcher(
    w: &mut WlogWriter,
    segment: usize,
    series_count: usize,
    samples_count: usize,
    enable_st: bool,
) {
    let enc = RecordEncoder::new(enable_st);
    let mut rng = XorShift64::new(0x7070 + segment as u64);
    for j in 0..series_count {
        let ref_id = (j + segment * 100) as u64;
        let s = enc.series(
            &[RefSeries {
                ref_id,
                labels: Labels::from_pairs([("__name__", format!("metric_{segment}").as_str())]),
            }],
            Vec::new(),
        );
        w.log(&[&s]).unwrap();
        for _ in 0..samples_count {
            let inner = rng.next_usize(ref_id as usize + 1) as u64;
            let sm = enc.samples(
                &[RefSample {
                    ref_id: inner,
                    st: 0,
                    t: segment as i64,
                    v: segment as f64,
                }],
                Vec::new(),
            );
            w.log(&[&sm]).unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/watcher_test.go::TestRun_AvoidNotifyWhenBehind
#[test]
fn test_run_avoid_notify_when_behind() {
    const SEGMENTS_TO_WRITE: usize = 5;
    const SEGMENTS_TO_READ: usize = SEGMENTS_TO_WRITE - 1;
    const SERIES_COUNT: usize = 10;
    const SAMPLES_COUNT: usize = 50;

    for enable_st in [false, true] {
        for compress in all_compressions() {
            let dir = tempfile::tempdir().unwrap();
            let wdir = dir.path().join("wal");
            fs::create_dir(&wdir).unwrap();
            let mut w = WlogWriter::with_segment_size(&wdir, PAGE_SIZE, compress).unwrap();
            generate_wal_records_for_watcher(&mut w, 0, SERIES_COUNT, SAMPLES_COUNT, enable_st);
            w.next_segment().unwrap();

            let wt = WriteToMock::new(Duration::from_millis(1));
            let watcher = Watcher::new(
                Some(WatcherMetrics::new(None)),
                None,
                "",
                Arc::clone(&wt),
                dir.path(),
                false,
                false,
                false,
            );
            watcher.set_max_segment(Some(SEGMENTS_TO_READ));

            let watcher_bg = Arc::clone(&watcher);
            let h = thread::spawn(move || {
                let start = Instant::now();
                watcher_bg.run().unwrap();
                assert!(start.elapsed() < Duration::from_secs(15));
            });

            wait_eventually(|| wt.check_num_series() == SERIES_COUNT, Duration::from_secs(10));

            for i in 1..SEGMENTS_TO_WRITE {
                generate_wal_records_for_watcher(&mut w, i, SERIES_COUNT, SAMPLES_COUNT, enable_st);
                w.next_segment().unwrap();
            }
            h.join().unwrap();
            assert_eq!(wt.check_num_series(), (SEGMENTS_TO_READ + 1) * SERIES_COUNT);
            assert_eq!(
                wt.inner.lock().unwrap().samples_appended.len(),
                SEGMENTS_TO_READ * SERIES_COUNT * SAMPLES_COUNT
            );
            w.close().unwrap();
        }
    }
}

// ============================================================================
// wlog_test.go 8 tests
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestWALRepair_ReadingError
#[test]
fn test_wal_repair_reading_error() {
    struct Case {
        name: &'static str,
        corr_sgm: usize,
        corr_fn: fn(&mut File),
        intact_recs: usize,
    }
    let cases = [
        Case {
            name: "torn_last_record",
            corr_sgm: 2,
            corr_fn: |f| {
                f.seek(SeekFrom::Start((PAGE_SIZE * 2) as u64)).unwrap();
                f.write_all(&[REC_FIRST]).unwrap();
            },
            intact_recs: 8,
        },
        Case {
            name: "bad_header",
            corr_sgm: 1,
            corr_fn: |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                f.write_all(&[REC_PAGE_TERM]).unwrap();
            },
            intact_recs: 4,
        },
        Case {
            name: "bad_fragment_sequence",
            corr_sgm: 1,
            corr_fn: |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                f.write_all(&[REC_LAST]).unwrap();
            },
            intact_recs: 4,
        },
        Case {
            name: "bad_fragment_flag",
            corr_sgm: 1,
            corr_fn: |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                f.write_all(&[123]).unwrap();
            },
            intact_recs: 4,
        },
        Case {
            name: "bad_checksum",
            corr_sgm: 1,
            corr_fn: |f| {
                f.seek(SeekFrom::Start((PAGE_SIZE + 4) as u64)).unwrap();
                f.write_all(&[0]).unwrap();
            },
            intact_recs: 4,
        },
        Case {
            name: "bad_length",
            corr_sgm: 1,
            corr_fn: |f| {
                f.seek(SeekFrom::Start((PAGE_SIZE + 2) as u64)).unwrap();
                f.write_all(&[0]).unwrap();
            },
            intact_recs: 4,
        },
        Case {
            name: "bad_content",
            corr_sgm: 1,
            corr_fn: |f| {
                f.seek(SeekFrom::Start((PAGE_SIZE + 100) as u64)).unwrap();
                f.write_all(b"beef").unwrap();
            },
            intact_recs: 4,
        },
    ];

    for tc in cases {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 3 * PAGE_SIZE;
        let mut w = WlogWriter::with_segment_size(dir.path(), seg_size, CompressionType::None).unwrap();
        let mut records = Vec::new();
        for i in 1..=9u8 {
            let mut b = vec![0u8; PAGE_SIZE - RECORD_HEADER_SIZE];
            b[0] = i;
            w.log(&[&b]).unwrap();
            records.push(b);
        }
        let (first, last) = segments_range(dir.path()).unwrap().unwrap();
        assert_eq!(1 + last - first, 3, "{}", tc.name);
        w.close().unwrap();

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_name(dir.path(), tc.corr_sgm))
            .unwrap();
        (tc.corr_fn)(&mut f);
        drop(f);

        let mut w = WlogWriter::with_segment_size(dir.path(), seg_size, CompressionType::None).unwrap();
        let (first, last) = segments_range(dir.path()).unwrap().unwrap();
        'seg_loop: for i in first..=last {
            let mut r = WlogReader::open_segment(dir.path(), i).unwrap();
            loop {
                match r.next_record() {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(WlogError::Corruption { segment: Some(cs), offset: co, .. }) => {
                        w.repair(cs, co).unwrap();
                        break 'seg_loop;
                    }
                    Err(e) => panic!("unexpected err: {e}"),
                }
            }
        }
        w.close().unwrap();

        let mut r = WlogReader::open_dir(dir.path()).unwrap();
        let mut result = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            result.push(rec.to_vec());
        }
        assert_eq!(result.len(), tc.intact_recs, "{}", tc.name);
        for (i, rec) in result.iter().enumerate() {
            assert_eq!(rec, &records[i], "{} record {i}", tc.name);
        }

        let (_, last) = segments_range(dir.path()).unwrap().unwrap();
        assert_eq!(last, tc.corr_sgm + 1, "{}", tc.name);
        assert_eq!(fs::metadata(segment_name(dir.path(), last)).unwrap().len(), 0, "{}", tc.name);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestCorruptAndCarryOn
#[test]
fn test_corrupt_and_carry_on() {
    let dir = tempfile::tempdir().unwrap();
    let segment_size = PAGE_SIZE * 3;
    let record_size = (PAGE_SIZE / 3) - RECORD_HEADER_SIZE;
    let mut rng = XorShift64::new(0x8080);

    {
        let mut w = WlogWriter::with_segment_size(dir.path(), segment_size, CompressionType::None).unwrap();
        for _ in 0..18 {
            let mut buf = vec![0u8; record_size];
            rng.fill_bytes(&mut buf);
            w.log(&[&buf]).unwrap();
        }
        w.close().unwrap();
    }

    let (first, last) = segments_range(dir.path()).unwrap().unwrap();
    for s in first..=last {
        assert_eq!(
            fs::metadata(segment_name(dir.path(), s)).unwrap().len(),
            segment_size as u64
        );
    }

    {
        let f = OpenOptions::new().write(true).open(segment_name(dir.path(), 0)).unwrap();
        f.set_len((segment_size / 2) as u64).unwrap();
    }

    let (corr_seg, corr_off) = {
        let mut r = WlogReader::open_dir(dir.path()).unwrap();
        for _ in 0..4 {
            assert_eq!(r.next_record().unwrap().unwrap().len(), record_size);
        }
        match r.next_record().unwrap_err() {
            WlogError::Corruption { segment: Some(cs), offset: co, .. } => (cs, co),
            e => panic!("expected corruption, got {e:?}"),
        }
    };

    {
        let mut w = WlogWriter::with_segment_size(dir.path(), segment_size, CompressionType::None).unwrap();
        w.repair(corr_seg, corr_off).unwrap();
        assert_eq!(w.current_segment_index(), Some(1));
        assert_eq!(w.done_pages(), 0);
        for _ in 0..5 {
            let mut buf = vec![0u8; record_size];
            rng.fill_bytes(&mut buf);
            w.log(&[&buf]).unwrap();
        }
        w.close().unwrap();
    }

    let mut r = WlogReader::open_dir(dir.path()).unwrap();
    let mut count = 0usize;
    while let Some(rec) = r.next_record().unwrap() {
        assert_eq!(rec.len(), record_size);
        count += 1;
    }
    assert_eq!(count, 9);
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestClose
#[test]
fn test_close() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE, CompressionType::None).unwrap();
    w.close_mut().unwrap();
    assert!(matches!(w.close_mut(), Err(WlogError::Closed)));
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestSegmentMetric
#[test]
fn test_segment_metric() {
    let segment_size = PAGE_SIZE;
    let record_size = (PAGE_SIZE / 2) - RECORD_HEADER_SIZE;
    let dir = tempfile::tempdir().unwrap();
    let reg = prometheus::Registry::new();
    let mut w = WlogWriter::with_registry(
        dir.path(),
        segment_size,
        CompressionType::None,
        Some(reg.clone()),
        WAL_METRICS_PREFIX,
    )
    .unwrap();

    let gauge_val = || -> f64 {
        reg.gather()
            .into_iter()
            .find(|mf| mf.get_name() == format!("{WAL_METRICS_PREFIX}segment_current"))
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value()
    };

    let initial_segment = gauge_val();
    let mut rng = XorShift64::new(0x9090);
    for _ in 0..3 {
        let mut buf = vec![0u8; record_size];
        rng.fill_bytes(&mut buf);
        w.log(&[&buf]).unwrap();
    }
    assert_eq!(gauge_val(), initial_segment + 1.0);
    w.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestCompression
#[test]
fn test_compression() {
    let bootstrap = |compress: CompressionType| {
        const RECORD_SIZE: usize = (PAGE_SIZE / 2) - RECORD_HEADER_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE, compress).unwrap();
        let buf = vec![0u8; RECORD_SIZE];
        for _ in 0..100 {
            w.log(&[&buf]).unwrap();
        }
        w.close().unwrap();
        dir
    };

    let dir_uncompressed = bootstrap(CompressionType::None);
    let uncompressed_size = dir_size(dir_uncompressed.path()).unwrap();

    for compress in [CompressionType::Snappy, CompressionType::Zstd] {
        let dir_compressed = bootstrap(compress);
        let compressed_size = dir_size(dir_compressed.path()).unwrap();
        assert!(
            (uncompressed_size as f64) * 0.75 > (compressed_size as f64),
            "{compress:?}: uncompressed={uncompressed_size}, compressed={compressed_size}"
        );
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestLogPartialWrite
#[test]
fn test_log_partial_write() {
    let segment_size = PAGE_SIZE * 2;
    let record = [1u8, 2, 3, 4, 5];
    let recs_per_page = PAGE_SIZE / (RECORD_HEADER_SIZE + record.len());
    let tests = [
        ("first record in page", 10usize, 1usize),
        ("middle record in page", 10usize, 3usize),
        ("last record of page", recs_per_page + 10, recs_per_page),
    ];

    for (name, num_records, faulty_record) in tests {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), segment_size, CompressionType::None).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_hook = Arc::clone(&count);
        w.set_write_fault_hook(Some(Arc::new(move |_| {
            let n = count_hook.fetch_add(1, Ordering::SeqCst) + 1;
            if n == faulty_record {
                WriteFaultAction::PartialThenError {
                    prefix_len: 2,
                    err_kind: io::ErrorKind::UnexpectedEof,
                }
            } else {
                WriteFaultAction::PassThrough
            }
        })));

        for i in 1..=num_records {
            let res = w.log(&[&record]);
            if i == faulty_record {
                let err = res.unwrap_err();
                assert!(matches!(err, WlogError::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof), "{name}");
                // Rust WlogWriter atomically rolls back torn writes; retry the record after recovery.
                w.log(&[&record]).unwrap();
            } else {
                res.unwrap();
            }
        }
        w.close().unwrap();

        let mut r = WlogReader::open_segment(dir.path(), 0).unwrap();
        for i in 0..num_records {
            let got = r.next_record().unwrap().unwrap_or_else(|| panic!("{name} missing rec {i}"));
            assert_eq!(got, &record, "{name}");
        }
        assert!(r.next_record().unwrap().is_none(), "{name}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestUnregisterMetrics
#[test]
fn test_unregister_metrics() {
    let reg = prometheus::Registry::new();
    for _ in 0..2 {
        let dir = tempfile::tempdir().unwrap();
        let wl = WlogWriter::with_registry(
            dir.path(),
            mimir_tsdb::wlog::DEFAULT_SEGMENT_SIZE,
            CompressionType::None,
            Some(reg.clone()),
            WAL_METRICS_PREFIX,
        )
        .unwrap();
        wl.close().unwrap();
    }
    // Verify a third registration on the same registry still succeeds cleanly without AlreadyReg.
    let dir3 = tempfile::tempdir().unwrap();
    let wl3 = WlogWriter::with_registry(
        dir3.path(),
        mimir_tsdb::wlog::DEFAULT_SEGMENT_SIZE,
        CompressionType::None,
        Some(reg.clone()),
        WAL_METRICS_PREFIX,
    )
    .unwrap();
    assert!(!reg.gather().is_empty());
    wl3.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/wlog/wlog_test.go::TestSyncSegmentsUntilCurrent
#[test]
fn test_sync_segments_until_current() {
    // Subtest 1: sync succeeds with active segments
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE, CompressionType::None).unwrap();
        let rec = vec![0u8; PAGE_SIZE / 2];
        for _ in 0..10 {
            w.log(&[&rec]).unwrap();
        }
        w.fsync_segments_until_current().unwrap();
        w.close().unwrap();
    }

    // Subtest 2: sync fails when WAL is closed
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE, CompressionType::None).unwrap();
        let rec = vec![0u8; PAGE_SIZE / 2];
        for _ in 0..10 {
            w.log(&[&rec]).unwrap();
        }
        w.close_mut().unwrap();
        let err = w.fsync_segments_until_current().unwrap_err();
        assert_eq!(err.to_string(), "I/O error: unable to fsync segments: write log is closed");
    }
}

