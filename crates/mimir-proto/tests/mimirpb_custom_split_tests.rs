use std::sync::Mutex;
use std::time::Duration;

use mimir_proto::compat::{
    from_float_histogram_to_histogram_proto, from_histogram_to_histogram_proto, LabelAdapter,
    PromFloatHistogram, PromHistogram, PromSpan,
};
use mimir_proto::cortexpb;
use mimir_proto::ref_leaks::{
    BufferHolder, CustomCodec, InstrumentRefLeaksConfig, WriteRequestWithBuffers,
};
use mimir_proto::split::{
    embedded_message_field_size, max_rw2_series_size_after_resymbolization,
    resymbolize_timeseries_rw2, split_write_request_by_max_marshal_size,
    split_write_request_by_max_marshal_size_rw2,
};
use mimir_proto::symbols::{CommonSymbols, FastSymbolsTable};
use mimir_proto::timeseries::{
    clear_exemplars, copy_to_yolo_string, deep_copy_timeseries,
    set_timeseries_unmarshal_caching_enabled, PoolExemplar, PoolTimeSeries, PreallocTimeseries,
    TimeSeriesPool,
};
use mimir_proto::zerocopy::from_write_request_to_rw2;
use prometheus::Registry;
use prost::Message as _;
use rand::{Rng, SeedableRng};

static UNMARSHAL_CACHE_LOCK: Mutex<()> = Mutex::new(());

fn generate_test_histogram(i: i64) -> PromHistogram {
    PromHistogram {
        count: 12 + (i * 9) as u64,
        zero_count: 2 + i as u64,
        zero_threshold: 0.001,
        sum: 18.4 * ((i + 1) as f64),
        schema: 1,
        positive_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        negative_buckets: vec![i + 1, 1, -1, 0],
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }
}

fn generate_test_float_histogram(i: i64) -> PromFloatHistogram {
    PromFloatHistogram {
        count: 12.0 + ((i * 9) as f64),
        zero_count: 2.0 + (i as f64),
        zero_threshold: 0.001,
        sum: 18.4 * ((i + 1) as f64),
        schema: 1,
        positive_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        negative_spans: vec![
            PromSpan { offset: 0, length: 2 },
            PromSpan { offset: 1, length: 2 },
        ],
        negative_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }
}

// Upstream: pkg/mimirpb/custom_test.go::TestWriteRequest_MinTimestamp
#[test]
fn test_write_request_min_timestamp() {
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            samples: vec![cortexpb::Sample { timestamp_ms: 10, ..Default::default() }],
            exemplars: vec![cortexpb::Exemplar { timestamp_ms: 20, ..Default::default() }],
            histograms: vec![cortexpb::Histogram { timestamp: 30, ..Default::default() }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(req.min_timestamp(), 10);

    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::Exemplar { timestamp_ms: 10, ..Default::default() }],
            histograms: vec![cortexpb::Histogram { timestamp: 30, ..Default::default() }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(req.min_timestamp(), 10);

    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::Exemplar { timestamp_ms: 30, ..Default::default() }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(req.min_timestamp(), 10);
}

// Upstream: pkg/mimirpb/custom_test.go::TestWriteRequest_IsEmpty
#[test]
fn test_write_request_is_empty() {
    let both = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            ..Default::default()
        }],
        metadata: vec![cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: "test_metric".to_string(),
            help: "This is a test metric.".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(!both.is_empty());

    let only_ts = cortexpb::WriteRequest {
        timeseries: both.timeseries.clone(),
        ..Default::default()
    };
    assert!(!only_ts.is_empty());

    let only_meta = cortexpb::WriteRequest {
        metadata: both.metadata.clone(),
        ..Default::default()
    };
    assert!(!only_meta.is_empty());

    let empty = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        skip_label_validation: true,
        ..Default::default()
    };
    assert!(empty.is_empty());
}

// Upstream: pkg/mimirpb/custom_test.go::TestWriteRequest_FieldSize
#[test]
fn test_write_request_field_size() {
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::Exemplar { timestamp_ms: 30, ..Default::default() }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            ..Default::default()
        }],
        metadata: vec![cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: "test_metric".to_string(),
            help: "This is a test metric.".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let orig_size = req.encoded_len();
    let metadata_size = req.metadata_size();
    let timeseries_size = req.timeseries_size();
    assert!(metadata_size < orig_size);
    assert!(timeseries_size < orig_size);
    assert_eq!(metadata_size + timeseries_size, orig_size);

    let mut req_no_ts = req.clone();
    req_no_ts.timeseries.clear();
    assert_eq!(orig_size - timeseries_size, req_no_ts.encoded_len());

    let mut req_no_meta = req.clone();
    req_no_meta.metadata.clear();
    assert_eq!(orig_size - metadata_size, req_no_meta.encoded_len());

    let reqv2 = cortexpb::WriteRequest {
        symbols_rw2: vec![
            "".to_string(),
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
        ],
        timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::ExemplarRw2 { timestamp: 30, ..Default::default() }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let orig_size = reqv2.encoded_len();
    let ts_size = reqv2.timeseries_rw2_size();
    let sym_size = reqv2.symbols_rw2_size();
    assert!(ts_size < orig_size);
    assert!(sym_size < orig_size);
    assert_eq!(ts_size + sym_size, orig_size);

    let mut reqv2_no_ts = reqv2.clone();
    reqv2_no_ts.timeseries_rw2.clear();
    assert_eq!(orig_size - ts_size, reqv2_no_ts.encoded_len());

    let mut reqv2_no_sym = reqv2;
    reqv2_no_sym.symbols_rw2.clear();
    assert_eq!(orig_size - sym_size, reqv2_no_sym.encoded_len());
}

// Upstream: pkg/mimirpb/custom_test.go::TestIsFloatHistogram
#[test]
fn test_is_float_histogram() {
    let int_h = from_histogram_to_histogram_proto(
        0,
        &PromHistogram {
            sum: 0.0,
            count: 0,
            zero_threshold: 0.001,
            schema: 2,
            positive_spans: vec![
                PromSpan { offset: 0, length: 1 },
                PromSpan { offset: 3, length: 1 },
                PromSpan { offset: 2, length: 2 },
            ],
            positive_buckets: vec![0, 0, 0, 0],
            ..Default::default()
        },
    );
    assert!(!int_h.is_float_histogram());

    let float_h = from_float_histogram_to_histogram_proto(
        0,
        &PromFloatHistogram {
            sum: 0.0,
            count: 0.0,
            zero_threshold: 0.001,
            schema: 2,
            positive_spans: vec![
                PromSpan { offset: 0, length: 1 },
                PromSpan { offset: 3, length: 1 },
                PromSpan { offset: 2, length: 2 },
            ],
            positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
            ..Default::default()
        },
    );
    assert!(float_h.is_float_histogram());
}

// Upstream: pkg/mimirpb/custom_test.go::TestCodecV2_Unmarshal
#[test]
fn test_codec_v2_unmarshal() {
    let codec = CustomCodec::new(&InstrumentRefLeaksConfig::default(), None);
    let orig_req = cortexpb::WriteRequest::default();
    let data = codec.marshal(&orig_req);
    let mut req = WriteRequestWithBuffers::default();
    codec.unmarshal(&data, &mut req).unwrap();
    assert_eq!(req.inner, orig_req);
    assert!(req.buffer().is_some());
    req.free_buffer();
}

// Upstream: pkg/mimirpb/custom_test.go::TestHistogram_BucketsCount
#[test]
fn test_histogram_buckets_count() {
    assert_eq!(cortexpb::Histogram::default().bucket_count(), 0);
    assert_eq!(
        from_histogram_to_histogram_proto(0, &generate_test_histogram(0)).bucket_count(),
        8
    );
    assert_eq!(
        from_float_histogram_to_histogram_proto(0, &generate_test_float_histogram(0)).bucket_count(),
        8
    );
}

// Upstream: pkg/mimirpb/custom_test.go::TestBufferHolder_FreeBuffer_Idempotent
#[test]
fn test_buffer_holder_free_buffer_idempotent() {
    let codec = CustomCodec::new(&InstrumentRefLeaksConfig::default(), None);
    let data = codec.marshal(&cortexpb::WriteRequest::default());
    let mut req = WriteRequestWithBuffers::default();
    codec.unmarshal(&data, &mut req).unwrap();
    assert!(req.buffer().is_some());

    req.free_buffer();
    // Second FreeBuffer is a no-op and does not panic.
    req.free_buffer();
    assert!(req.buffer().is_none());
}

// Upstream: pkg/mimirpb/custom_test.go::TestAddSourceBufferHolder_DeduplicationByBuffer
#[test]
fn test_add_source_buffer_holder_deduplication_by_buffer() {
    let codec = CustomCodec::new(&InstrumentRefLeaksConfig::default(), None);
    let data = codec.marshal(&cortexpb::WriteRequest::default());
    let mut source_req = WriteRequestWithBuffers::default();
    codec.unmarshal(&data, &mut source_req).unwrap();
    let buf = source_req.buffer().unwrap().clone();

    let mut dest_req = WriteRequestWithBuffers::default();
    dest_req.add_source_buffer_holder(&source_req.buffer_holder);

    let second_holder = BufferHolder::new(Some(buf));
    dest_req.add_source_buffer_holder(&second_holder);

    assert_eq!(dest_req.source_buffer_holders.as_ref().unwrap().len(), 1);
    dest_req.free_buffer();
    source_req.free_buffer();
}

// Upstream: pkg/mimirpb/custom_test.go::TestWriteRequest_FreeBuffer_WithSourceHolders_Idempotent
#[test]
fn test_write_request_free_buffer_with_source_holders_idempotent() {
    let codec = CustomCodec::new(&InstrumentRefLeaksConfig::default(), None);
    let data = codec.marshal(&cortexpb::WriteRequest::default());
    let mut source_req = WriteRequestWithBuffers::default();
    codec.unmarshal(&data, &mut source_req).unwrap();
    assert!(source_req.buffer().is_some());

    let mut dest_req = WriteRequestWithBuffers::default();
    dest_req.add_source_buffer_holder(&source_req.buffer_holder);
    assert_eq!(dest_req.source_buffer_holders.as_ref().unwrap().len(), 1);

    dest_req.free_buffer();
    dest_req.free_buffer();
    assert!(dest_req.source_buffer_holders.is_none());
    source_req.free_buffer();
}

// Upstream: pkg/mimirpb/custom_test.go::TestAddSourceBufferHolder_AfterFreeBuffer
#[test]
fn test_add_source_buffer_holder_after_free_buffer() {
    let codec = CustomCodec::new(&InstrumentRefLeaksConfig::default(), None);
    let data = codec.marshal(&cortexpb::WriteRequest::default());
    let mut source_req = WriteRequestWithBuffers::default();
    codec.unmarshal(&data, &mut source_req).unwrap();
    assert!(source_req.buffer().is_some());

    source_req.free_buffer();
    assert!(source_req.buffer().is_none());

    let mut dest_req = WriteRequestWithBuffers::default();
    dest_req.add_source_buffer_holder(&source_req.buffer_holder);
    assert!(dest_req.source_buffer_holders.is_none());
}

// Upstream: pkg/mimirpb/prealloc_rw2_test.go::TestWriteRequestRW2Conversion
#[test]
fn test_write_request_rw2_conversion() {
    use cortexpb::metadata_rw2::MetricType as Rw2Type;
    use cortexpb::metric_metadata::MetricType as Rw1Type;
    use cortexpb::write_request::SourceEnum;

    // Preserves meta options
    let req = cortexpb::WriteRequest {
        skip_label_validation: true,
        skip_label_count_validation: true,
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&req, 0, None).unwrap();
    assert!(rw2.skip_label_validation);
    assert!(rw2.skip_label_count_validation);

    // Source preserved
    for src in [SourceEnum::Api as i32, SourceEnum::Rule as i32, 0] {
        let r = cortexpb::WriteRequest {
            source: src,
            ..Default::default()
        };
        let conv = from_write_request_to_rw2(&r, 0, None).unwrap();
        assert_eq!(conv.source, src);
    }

    // CreatedTimestamp preserved
    let r = cortexpb::WriteRequest {
        timeseries: vec![
            cortexpb::TimeSeries { created_timestamp: 1234567890, ..Default::default() },
            cortexpb::TimeSeries { created_timestamp: 1234567900, ..Default::default() },
        ],
        ..Default::default()
    };
    let conv = from_write_request_to_rw2(&r, 0, None).unwrap();
    assert_eq!(conv.timeseries_rw2.len(), 2);
    assert_eq!(conv.timeseries_rw2[0].created_timestamp, 1234567890);
    assert_eq!(conv.timeseries_rw2[1].created_timestamp, 1234567900);

    // Samples
    let r = cortexpb::WriteRequest {
        timeseries: vec![
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "my_cool_series"),
                    cortexpb::LabelPair::from_static("job", "foo/bar"),
                ],
                samples: vec![
                    cortexpb::Sample { value: 123.0, timestamp_ms: 1234567890 },
                    cortexpb::Sample { value: 456.0, timestamp_ms: 1234567900 },
                ],
                ..Default::default()
            },
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "my_cool_series"),
                    cortexpb::LabelPair::from_static("job", "asdf/jkl"),
                ],
                samples: vec![
                    cortexpb::Sample { value: 135.0, timestamp_ms: 1234567890 },
                    cortexpb::Sample { value: 246.0, timestamp_ms: 1234567900 },
                ],
                ..Default::default()
            },
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "my_other_cool_series"),
                    cortexpb::LabelPair::from_static("job", "foo/bar"),
                ],
                samples: vec![
                    cortexpb::Sample { value: 112.0, timestamp_ms: 1234567890 },
                    cortexpb::Sample { value: 358.0, timestamp_ms: 1234567900 },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&r, 0, None).unwrap();
    assert!(rw2.timeseries.is_empty());
    assert_eq!(
        rw2.symbols_rw2,
        vec!["", "__name__", "my_cool_series", "job", "foo/bar", "asdf/jkl", "my_other_cool_series"]
    );
    assert_eq!(rw2.timeseries_rw2[0].labels_refs, vec![1, 2, 3, 4]);
    assert_eq!(rw2.timeseries_rw2[1].labels_refs, vec![1, 2, 3, 5]);
    assert_eq!(rw2.timeseries_rw2[2].labels_refs, vec![1, 6, 3, 4]);

    // Metadata matching + unmatched + conflicting + deduplication
    let r = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "my_cool_series"),
                cortexpb::LabelPair::from_static("job", "foo/bar"),
            ],
            samples: vec![cortexpb::Sample { value: 123.0, timestamp_ms: 1234567890 }],
            ..Default::default()
        }],
        metadata: vec![
            cortexpb::MetricMetadata {
                r#type: Rw1Type::Counter as i32,
                metric_family_name: "my_cool_series".to_string(),
                help: "old".to_string(),
                unit: "megawatts".to_string(),
            },
            cortexpb::MetricMetadata {
                r#type: Rw1Type::Counter as i32,
                metric_family_name: "my_cool_series".to_string(),
                help: "old".to_string(),
                unit: "megawatts".to_string(),
            },
            cortexpb::MetricMetadata {
                r#type: Rw1Type::Gauge as i32,
                metric_family_name: "my_cool_series".to_string(),
                help: "new".to_string(),
                unit: "megawatts".to_string(),
            },
            cortexpb::MetricMetadata {
                r#type: Rw1Type::Unknown as i32,
                metric_family_name: "my_cool_series_2".to_string(),
                help: "other".to_string(),
                unit: "gigawatts".to_string(),
            },
        ],
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&r, 0, None).unwrap();
    assert!(rw2.metadata.is_empty());
    assert_eq!(rw2.timeseries_rw2.len(), 3);
    assert_eq!(rw2.timeseries_rw2[0].metadata.unwrap().r#type, Rw2Type::Counter as i32);
    assert_eq!(rw2.timeseries_rw2[1].metadata.unwrap().r#type, Rw2Type::Gauge as i32);
    assert_eq!(rw2.timeseries_rw2[2].metadata.unwrap().r#type, Rw2Type::Unspecified as i32);
}

// Upstream: pkg/mimirpb/prealloc_rw2_test.go::TestExemplarConversion
#[test]
fn test_exemplar_conversion() {
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("__name__", "m")],
            exemplars: vec![
                cortexpb::Exemplar {
                    labels: vec![
                        cortexpb::LabelPair::from_static("trace_id", "abc123"),
                        cortexpb::LabelPair::from_static("span_id", "def456"),
                    ],
                    value: 42.5,
                    timestamp_ms: 1234567890,
                },
                cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair::from_static("trace_id", "xyz")],
                    value: 0.0,
                    timestamp_ms: 0,
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&req, 0, None).unwrap();
    let exs = &rw2.timeseries_rw2[0].exemplars;
    assert_eq!(exs.len(), 2);
    assert_eq!(exs[0].value, 42.5);
    assert_eq!(exs[0].timestamp, 1234567890);
    assert_eq!(exs[0].labels_refs.len(), 4);
    assert_eq!(exs[1].value, 0.0);
    assert_eq!(exs[1].timestamp, 0);
}

// Upstream: pkg/mimirpb/prealloc_rw2_test.go::TestMetricMetadataConversion
#[test]
fn test_metric_metadata_conversion() {
    let req = cortexpb::WriteRequest {
        metadata: vec![
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "m1".to_string(),
                help: "help text".to_string(),
                unit: "unit text".to_string(),
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                metric_family_name: "m2".to_string(),
                help: String::new(),
                unit: String::new(),
            },
        ],
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&req, 0, None).unwrap();
    assert_eq!(rw2.timeseries_rw2.len(), 2);
    let m1 = rw2.timeseries_rw2[0].metadata.unwrap();
    assert_eq!(m1.r#type, cortexpb::metadata_rw2::MetricType::Counter as i32);
    assert_eq!(rw2.symbols_rw2[m1.help_ref as usize], "help text");
    assert_eq!(rw2.symbols_rw2[m1.unit_ref as usize], "unit text");

    let m2 = rw2.timeseries_rw2[1].metadata.unwrap();
    assert_eq!(m2.r#type, cortexpb::metadata_rw2::MetricType::Gauge as i32);
    assert_eq!(m2.help_ref, 0);
    assert_eq!(m2.unit_ref, 0);
}

// Upstream: pkg/mimirpb/prealloc_rw2_test.go::TestWriteRequestRW2Conversion_WriteRequestHasChanged
#[test]
fn test_write_request_rw2_conversion_write_request_has_changed() {
    // Verify all protobuf fields of cortexpb::WriteRequest are accounted for by RW2 conversion.
    let rw1 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries::default()],
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        metadata: vec![cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: "m".to_string(),
            help: "h".to_string(),
            unit: "u".to_string(),
        }],
        symbols_rw2: Vec::new(),
        timeseries_rw2: Vec::new(),
        skip_label_validation: true,
        skip_label_count_validation: true,
    };
    let rw2 = from_write_request_to_rw2(&rw1, 0, None).unwrap();
    assert_eq!(rw2.source, rw1.source);
    assert_eq!(rw2.skip_label_validation, rw1.skip_label_validation);
    assert_eq!(rw2.skip_label_count_validation, rw1.skip_label_count_validation);
    assert!(!rw2.symbols_rw2.is_empty());
    assert!(!rw2.timeseries_rw2.is_empty());
}

// Upstream: pkg/mimirpb/ref_leaks_test.go::TestInstrumentRefLeaks
#[test]
fn test_instrument_ref_leaks() {
    let reg = Registry::new();
    let cfg = InstrumentRefLeaksConfig {
        percentage: 100.0,
        before_reuse_period: Duration::ZERO,
        max_inflight_instrumented_bytes: 0,
    };
    cfg.validate().unwrap();
    let codec = CustomCodec::new(&cfg, Some(&reg));

    let src = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("labelName", "labelValue")],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1234,
                value: 1337.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let buf = codec.marshal(&src);

    let mut req = WriteRequestWithBuffers::default();
    codec.unmarshal(&buf, &mut req).unwrap();

    assert_eq!(codec.tracker().instrumented_buffers_total(), 1.0);
    assert!(codec.tracker().inflight_instrumented_bytes() > 0.0);

    let leaking_label_name = req.tracked_label_refs[0].0.clone();
    assert_eq!(leaking_label_name.as_str(), "labelName");

    req.free_buffer();

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = leaking_label_name.as_str();
    }));
    let panic_payload = res.expect_err("reading leaked ref after FreeBuffer must fault");
    let msg = panic_payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic_payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("");
    assert_eq!(msg, "runtime error: invalid memory address or nil pointer dereference");

    assert_eq!(codec.tracker().instrumented_buffers_total(), 1.0);
    assert_eq!(codec.tracker().inflight_instrumented_bytes(), 0.0);

    let mut req2 = WriteRequestWithBuffers::default();
    codec.unmarshal(&buf, &mut req2).unwrap();
    assert_eq!(codec.tracker().instrumented_buffers_total(), 2.0);
    assert!(codec.tracker().inflight_instrumented_bytes() > 0.0);
    req2.free_buffer();
    assert_eq!(codec.tracker().instrumented_buffers_total(), 2.0);
    assert_eq!(codec.tracker().inflight_instrumented_bytes(), 0.0);
}

fn test_req_v2_static() -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        skip_label_validation: true,
        symbols_rw2: vec![
            "".to_string(),
            "__name__".to_string(),
            "series_1".to_string(),
            "pod".to_string(),
            "test-application-123456".to_string(),
            "This is the first test metric.".to_string(),
            "series_2".to_string(),
            "This is the second test metric.".to_string(),
            "series_3".to_string(),
            "This is the third test metric.".to_string(),
        ],
        timeseries_rw2: vec![
            cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
                exemplars: vec![cortexpb::ExemplarRw2 { timestamp: 30, ..Default::default() }],
                histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                    help_ref: 5,
                    unit_ref: 0,
                }),
                ..Default::default()
            },
            cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1, 6, 3, 4],
                samples: vec![cortexpb::Sample { timestamp_ms: 30, ..Default::default() }],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                    help_ref: 7,
                    unit_ref: 0,
                }),
                ..Default::default()
            },
            cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1, 8],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                    help_ref: 9,
                    unit_ref: 0,
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn merge_rw2s(partials: &[cortexpb::WriteRequest]) -> cortexpb::WriteRequest {
    let mut st = FastSymbolsTable::new(100);
    let mut ts_out = Vec::new();
    for partial in partials {
        for ts in &partial.timeseries_rw2 {
            let new_lbls: Vec<u32> = ts
                .labels_refs
                .iter()
                .map(|&r| st.symbolize(&partial.symbols_rw2[r as usize]))
                .collect();
            let meta = ts.metadata.map(|m| cortexpb::MetadataRw2 {
                r#type: m.r#type,
                help_ref: st.symbolize(&partial.symbols_rw2[m.help_ref as usize]),
                unit_ref: st.symbolize(&partial.symbols_rw2[m.unit_ref as usize]),
            });
            let new_exs: Vec<cortexpb::ExemplarRw2> = ts
                .exemplars
                .iter()
                .map(|ex| cortexpb::ExemplarRw2 {
                    labels_refs: ex
                        .labels_refs
                        .iter()
                        .map(|&r| st.symbolize(&partial.symbols_rw2[r as usize]))
                        .collect(),
                    value: ex.value,
                    timestamp: ex.timestamp,
                })
                .collect();
            ts_out.push(cortexpb::TimeSeriesRw2 {
                labels_refs: new_lbls,
                samples: ts.samples.clone(),
                histograms: ts.histograms.clone(),
                exemplars: new_exs,
                metadata: meta,
                created_timestamp: ts.created_timestamp,
            });
        }
    }
    cortexpb::WriteRequest {
        timeseries_rw2: ts_out,
        symbols_rw2: st.symbols(),
        source: partials[0].source,
        skip_label_validation: partials[0].skip_label_validation,
        skip_label_count_validation: partials[0].skip_label_count_validation,
        ..Default::default()
    }
}

fn generate_write_request(
    num_series: usize,
    num_labels_per_series: usize,
    num_samples_per_series: usize,
    num_metadata: usize,
) -> cortexpb::WriteRequest {
    let mut timeseries = Vec::with_capacity(num_series);
    for i in 0..num_series {
        let mut labels = Vec::with_capacity(num_labels_per_series);
        labels.push(cortexpb::LabelPair::from_strs("__name__", format!("series_{i}")));
        for l in 1..num_labels_per_series {
            labels.push(cortexpb::LabelPair::from_strs(
                format!("label_{l}"),
                format!("this-is-the-value-of-label-{l}"),
            ));
        }
        let samples = (0..num_samples_per_series)
            .map(|s| cortexpb::Sample {
                timestamp_ms: s as i64,
                value: s as f64,
            })
            .collect();
        let exemplars = vec![cortexpb::Exemplar {
            labels: vec![cortexpb::LabelPair::from_strs(
                "trace_id",
                format!("the-trace-id-for-{i}"),
            )],
            timestamp_ms: i as i64,
            value: i as f64,
        }];
        timeseries.push(cortexpb::TimeSeries {
            labels,
            samples,
            exemplars,
            ..Default::default()
        });
    }
    let metadata = (0..num_metadata)
        .map(|i| cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: format!("series_{i}"),
            help: format!("this is the help description for series {i}"),
            unit: "seconds".to_string(),
        })
        .collect();
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        skip_label_validation: true,
        timeseries,
        metadata,
        ..Default::default()
    }
}

// Upstream: pkg/mimirpb/split_test.go::TestSplitWriteRequestByMaxMarshalSize
#[test]
fn test_split_write_request_by_max_marshal_size() {
    let reqv1 = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        skip_label_validation: true,
        timeseries: vec![
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "series_1"),
                    cortexpb::LabelPair::from_static("pod", "test-application-123456"),
                ],
                samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
                exemplars: vec![cortexpb::Exemplar { timestamp_ms: 30, ..Default::default() }],
                histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
                ..Default::default()
            },
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "series_2"),
                    cortexpb::LabelPair::from_static("pod", "test-application-123456"),
                ],
                samples: vec![cortexpb::Sample { timestamp_ms: 30, ..Default::default() }],
                ..Default::default()
            },
        ],
        metadata: vec![
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_1".to_string(),
                help: "This is the first test metric.".to_string(),
                ..Default::default()
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_2".to_string(),
                help: "This is the second test metric.".to_string(),
                ..Default::default()
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_3".to_string(),
                help: "This is the third test metric.".to_string(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    // Less than limit -> returns single request
    let partials = split_write_request_by_max_marshal_size(&reqv1, reqv1.encoded_len(), 100_000);
    assert_eq!(partials, vec![reqv1.clone()]);

    let reqv2 = test_req_v2_static();
    let partials = split_write_request_by_max_marshal_size_rw2(&reqv2, reqv2.encoded_len(), 100_000, 0, None);
    assert_eq!(partials, vec![reqv2.clone()]);

    // Non-positive limit -> returns single request
    for limit in [0isize, -1] {
        let p1 = split_write_request_by_max_marshal_size(&reqv1, reqv1.encoded_len(), limit);
        assert_eq!(p1, vec![reqv1.clone()]);
        let p2 = split_write_request_by_max_marshal_size_rw2(&reqv2, reqv2.encoded_len(), limit, 0, None);
        assert_eq!(p2, vec![reqv2.clone()]);
    }

    // Split honoring size limit (RW1)
    let partials = split_write_request_by_max_marshal_size(&reqv1, reqv1.encoded_len(), 100);
    assert_eq!(partials.len(), 4);
    assert_eq!(partials[0].timeseries.len(), 1);
    assert_eq!(partials[1].timeseries.len(), 1);
    assert_eq!(partials[2].metadata.len(), 2);
    assert_eq!(partials[3].metadata.len(), 1);
    for p in &partials {
        assert!(p.encoded_len() <= 100);
    }

    // Split honoring size limit (RW2)
    let partials = split_write_request_by_max_marshal_size_rw2(&reqv2, reqv2.encoded_len(), 150, 0, None);
    assert_eq!(partials.len(), 3);
    for p in &partials {
        assert!(p.encoded_len() <= 150);
    }

    // Bin-packing RW2
    let partials = split_write_request_by_max_marshal_size_rw2(&reqv2, reqv2.encoded_len(), 220, 0, None);
    assert_eq!(partials.len(), 2);
    assert_eq!(partials[0].timeseries_rw2.len().max(partials[1].timeseries_rw2.len()), 2);
    for p in &partials {
        assert!(p.encoded_len() <= 220);
    }
    assert_eq!(reqv2.encode_to_vec(), merge_rw2s(&partials).encode_to_vec());

    // Limit < size(symbols)
    let partials = split_write_request_by_max_marshal_size_rw2(&reqv2, reqv2.encoded_len(), 50, 0, None);
    assert_eq!(partials.len(), 3);
    for p in &partials {
        assert!(p.encoded_len() > 50);
    }

    // Limit smaller than each entity -> 1 entity per partial
    let partials = split_write_request_by_max_marshal_size(&reqv1, reqv1.encoded_len(), 10);
    assert_eq!(partials.len(), 5);
    for p in &partials {
        assert!(p.encoded_len() > 10);
    }

    // Embedded message framing accounted for (RW2 with common symbols)
    let common_symbols = CommonSymbols::from_static(&["", "__name__"]);
    let samples: Vec<cortexpb::Sample> = (1..=12)
        .map(|i| cortexpb::Sample {
            timestamp_ms: i,
            value: i as f64,
        })
        .collect();
    let rw2_framed = cortexpb::WriteRequest {
        symbols_rw2: vec![
            "".to_string(),
            "series_a".to_string(),
            "help_a".to_string(),
            "unit_a".to_string(),
            "series_b".to_string(),
            "help_b".to_string(),
            "unit_b".to_string(),
        ],
        timeseries_rw2: vec![
            cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1, 65],
                samples: samples.clone(),
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                    help_ref: 66,
                    unit_ref: 67,
                }),
                ..Default::default()
            },
            cortexpb::TimeSeriesRw2 {
                labels_refs: vec![1, 68],
                samples,
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                    help_ref: 69,
                    unit_ref: 70,
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let limit = (rw2_framed.encoded_len() - 1) as isize;
    let rw2_partials = split_write_request_by_max_marshal_size_rw2(
        &rw2_framed,
        rw2_framed.encoded_len(),
        limit,
        64,
        Some(&common_symbols),
    );
    assert_eq!(rw2_partials.len(), 2);
    for p in &rw2_partials {
        assert!(p.encoded_len() <= limit as usize);
    }

    // Does not mutate input
    let orig_rw2 = test_req_v2_static();
    let mut_check = test_req_v2_static();
    let _ = split_write_request_by_max_marshal_size_rw2(&mut_check, mut_check.encoded_len(), 220, 0, None);
    assert_eq!(mut_check, orig_rw2);
}

// Upstream: pkg/mimirpb/split_test.go::TestSplitWriteRequestByMaxMarshalSize_Fuzzy
#[test]
fn test_split_write_request_by_max_marshal_size_fuzzy() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5eed1234);
    let num_runs = 200;

    // RW1 fuzzy
    for r in 0..num_runs {
        let cap = (r + 1).min(50);
        let num_series = rng.gen_range(1..=cap);
        let num_labels = rng.gen_range(1..=cap);
        let num_samples = rng.gen_range(1..=cap);
        let num_meta = rng.gen_range(1..=cap);

        let req = generate_write_request(num_series, num_labels, num_samples, num_meta);
        let max_size = (req.encoded_len() / rng.gen_range(1..=10)) as isize;
        let partials = split_write_request_by_max_marshal_size(&req, req.encoded_len(), max_size);

        let mut merged = cortexpb::WriteRequest {
            source: partials[0].source,
            skip_label_validation: partials[0].skip_label_validation,
            ..Default::default()
        };
        for p in partials {
            if p.encoded_len() as isize > max_size && max_size > 0 {
                assert_eq!(p.timeseries.len() + p.metadata.len(), 1);
            }
            merged.timeseries.extend(p.timeseries);
            merged.metadata.extend(p.metadata);
        }
        assert_eq!(req, merged);
    }

    // RW2 fuzzy
    for r in 0..num_runs {
        let cap = (r + 1).min(50);
        let num_series = rng.gen_range(1..=cap);
        let num_labels = rng.gen_range(1..=cap);
        let num_samples = rng.gen_range(1..=cap);
        let num_meta = rng.gen_range(1..=cap);

        let rw1 = generate_write_request(num_series, num_labels, num_samples, num_meta);
        let req = from_write_request_to_rw2(&rw1, 0, None).unwrap();
        let req_cpy = req.clone();

        let max_size = (req.encoded_len() / rng.gen_range(1..=10)) as isize;
        let partials =
            split_write_request_by_max_marshal_size_rw2(&req, req.encoded_len(), max_size, 0, None);
        for p in &partials {
            if p.encoded_len() as isize > max_size && max_size > 0 {
                assert_eq!(p.timeseries_rw2.len(), 1);
            }
        }
        let merged = merge_rw2s(&partials);
        assert_eq!(req_cpy, merged);
    }
}

// Upstream: pkg/mimirpb/split_test.go::TestSplitWriteRequestByMaxMarshalSize_WriteRequestHasChanged
#[test]
fn test_split_write_request_by_max_marshal_size_write_request_has_changed() {
    let req = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        skip_label_validation: true,
        skip_label_count_validation: true,
        timeseries: vec![cortexpb::TimeSeries::default()],
        metadata: vec![cortexpb::MetricMetadata::default()],
        ..Default::default()
    };
    let partials = split_write_request_by_max_marshal_size(&req, req.encoded_len(), 1);
    for p in partials {
        assert_eq!(p.source, req.source);
        assert_eq!(p.skip_label_validation, req.skip_label_validation);
        assert_eq!(p.skip_label_count_validation, req.skip_label_count_validation);
    }
}

// Upstream: pkg/mimirpb/split_test.go::TestRW2SymbolSplitting
#[test]
fn test_rw2_symbol_splitting() {
    // 1. symbols size matches proto size
    {
        let syms: Vec<String> = vec![
            "", "__name__", "series_1", "pod", "test-application-123456", "trace_id", "12345",
            "Help Text",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let ts = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                timestamp: 30,
                labels_refs: vec![5, 6],
                ..Default::default()
            }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: 7,
                unit_ref: 0,
            }),
            ..Default::default()
        };
        let (_, symbols_size) = max_rw2_series_size_after_resymbolization(&ts, &syms, 0);
        let req = cortexpb::WriteRequest {
            symbols_rw2: syms,
            ..Default::default()
        };
        assert_eq!(req.symbols_rw2_size(), symbols_size);
    }

    // 2. symbols size only considers referenced symbols
    {
        let syms: Vec<String> = vec![
            "", "__name__", "series_1", "series_2", "series_3", "pod", "test-application-123456",
            "trace_id", "12345", "Help Text", "unrelated text",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let ts = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 4, 5, 6],
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                timestamp: 30,
                labels_refs: vec![7, 8],
                ..Default::default()
            }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: 9,
                unit_ref: 0,
            }),
            ..Default::default()
        };
        let (_, symbols_size) = max_rw2_series_size_after_resymbolization(&ts, &syms, 0);
        let req = cortexpb::WriteRequest {
            symbols_rw2: vec![
                syms[0].clone(),
                syms[1].clone(),
                syms[4].clone(),
                syms[5].clone(),
                syms[6].clone(),
                syms[7].clone(),
                syms[8].clone(),
                syms[9].clone(),
            ],
            ..Default::default()
        };
        assert_eq!(req.symbols_rw2_size(), symbols_size);
    }

    // 3. upper bound grows with possible symbol magnitude
    {
        let syms: Vec<String> = vec![
            "", "__name__", "series_1", "pod", "test-application-123456", "trace_id", "12345",
            "Help Text",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let ts = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                timestamp: 30,
                labels_refs: vec![5, 6],
                ..Default::default()
            }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: 7,
                unit_ref: 0,
            }),
            ..Default::default()
        };
        let actual_size = ts.encoded_len();
        let (small_refs, _) = max_rw2_series_size_after_resymbolization(&ts, &syms, 0);
        assert!(small_refs > actual_size);
        assert!(small_refs < 2 * actual_size);

        let (big_refs, _) = max_rw2_series_size_after_resymbolization(&ts, &syms, 10000);
        assert!(big_refs > small_refs);
        assert!(big_refs < 2 * actual_size);
    }

    // 4. resymbolizeTimeSeriesRW2
    {
        let mut new_table = FastSymbolsTable::new(100);
        let syms: Vec<String> = vec![
            "", "unrelated1", "unrelated2", "unrelated3", "__name__", "series_1", "pod",
            "test-application-123456", "trace_id", "12345", "Help Text",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let ts = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![4, 5, 6, 7],
            samples: vec![cortexpb::Sample { timestamp_ms: 20, ..Default::default() }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                timestamp: 30,
                labels_refs: vec![8, 9],
                ..Default::default()
            }],
            histograms: vec![cortexpb::Histogram { timestamp: 10, ..Default::default() }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: 10,
                unit_ref: 0,
            }),
            ..Default::default()
        };
        let resym = resymbolize_timeseries_rw2(&ts, &syms, None, &mut new_table);
        assert_eq!(
            new_table.symbols(),
            vec![
                "",
                "__name__",
                "series_1",
                "pod",
                "test-application-123456",
                "trace_id",
                "12345",
                "Help Text",
            ]
        );
        assert_eq!(resym.labels_refs, vec![1, 2, 3, 4]);
        assert_eq!(resym.exemplars[0].labels_refs, vec![5, 6]);
        assert_eq!(resym.metadata.unwrap().help_ref, 7);
        assert_eq!(embedded_message_field_size(ts.encoded_len()), embedded_message_field_size(resym.encoded_len()));
    }
}

// Upstream: pkg/mimirpb/timeseries_pools_test.go::TestTimeseriesFromPool
#[test]
fn test_timeseries_from_pool() {
    let pool = TimeSeriesPool::new();
    let first = pool.timeseries_from_pool();
    let second = pool.timeseries_from_pool();
    assert_ne!(&*first as *const PoolTimeSeries, &*second as *const PoolTimeSeries);

    let mut ts = pool.timeseries_from_pool();
    ts.labels.push(LabelAdapter::new("foo", "bar"));
    ts.samples.push(cortexpb::Sample { value: 1.0, timestamp_ms: 2 });
    pool.reuse_timeseries(ts);
    let reused = pool.timeseries_from_pool();
    assert!(reused.labels.is_empty());
    assert!(reused.samples.is_empty());

    let dirty_cases = vec![
        PoolTimeSeries {
            labels: vec![LabelAdapter::new("foo", "bar")],
            ..PoolTimeSeries::new()
        },
        PoolTimeSeries {
            samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: 2 }],
            ..PoolTimeSeries::new()
        },
        PoolTimeSeries {
            histograms: vec![cortexpb::Histogram { sum: 1.0, ..Default::default() }],
            ..PoolTimeSeries::new()
        },
        PoolTimeSeries {
            exemplars: vec![PoolExemplar { value: 1.0, timestamp_ms: 2, labels: Vec::new() }],
            ..PoolTimeSeries::new()
        },
        PoolTimeSeries {
            created_timestamp: 1234567890,
            ..PoolTimeSeries::new()
        },
        PoolTimeSeries {
            skip_unmarshaling_exemplars: true,
            ..PoolTimeSeries::new()
        },
    ];
    for dirty in dirty_cases {
        pool.drain_timeseries_for_test();
        pool.put_raw_timeseries_for_test(Box::new(dirty));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.timeseries_from_pool();
        }));
        assert!(res.is_err(), "expected panic on dirty pooled TimeSeries");
    }
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestLabelAdapter_Marshal
#[test]
fn test_label_adapter_marshal() {
    let cases = [
        LabelAdapter::new("foo", "bar"),
        LabelAdapter::new("very long label name", "very long label value"),
        LabelAdapter::new("", "foo"),
        LabelAdapter::default(),
    ];
    for bs in &cases {
        let bytes = bs.marshal().unwrap();
        let mut lbs = LabelAdapter::default();
        lbs.unmarshal(&bytes).unwrap();
        assert_eq!(&lbs, bs);
        assert_eq!(bytes.len(), bs.encoded_len());
    }
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseriesSliceFromPool
#[test]
fn test_prealloc_timeseries_slice_from_pool() {
    let pool = TimeSeriesPool::new();
    let first = pool.prealloc_timeseries_slice_from_pool();
    let second = pool.prealloc_timeseries_slice_from_pool();
    assert_ne!(first.as_ptr(), second.as_ptr());

    let mut slice = pool.prealloc_timeseries_slice_from_pool();
    slice.push(PreallocTimeseries::from_timeseries(PoolTimeSeries::new()));
    pool.reuse_slice(slice);

    let reused = pool.prealloc_timeseries_slice_from_pool();
    assert_eq!(reused.len(), 0);
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestCopyToYoloString
#[test]
fn test_copy_to_yolo_string() {
    let orig = "testString";
    let mut new_buf = vec![0u8; orig.len()];
    let buf_ptr = new_buf.as_ptr() as usize;
    let (safe_copy, remaining) = copy_to_yolo_string(&mut new_buf, orig);
    assert_eq!(safe_copy, orig);
    assert_ne!(safe_copy.as_ptr() as usize, orig.as_ptr() as usize);
    assert_eq!(safe_copy.as_ptr() as usize, buf_ptr);
    assert!(remaining.is_empty());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestDeepCopyTimeseries
#[test]
fn test_deep_copy_timeseries() {
    let src = PreallocTimeseries::from_timeseries(PoolTimeSeries {
        labels: vec![
            LabelAdapter::new("sampleLabel1", "sampleValue1"),
            LabelAdapter::new("sampleLabel2", "sampleValue2"),
        ],
        samples: vec![
            cortexpb::Sample { value: 1.0, timestamp_ms: 2 },
            cortexpb::Sample { value: 3.0, timestamp_ms: 4 },
        ],
        histograms: vec![cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountInt(35)),
            sum: 108.0,
            schema: cortexpb::NATIVE_HISTOGRAMS_WITH_CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.01,
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
            negative_spans: vec![
                cortexpb::BucketSpan { offset: -1, length: 1 },
                cortexpb::BucketSpan { offset: -2, length: 1 },
            ],
            negative_deltas: vec![7, 3],
            negative_counts: vec![8.0, 9.0],
            positive_spans: vec![
                cortexpb::BucketSpan { offset: 0, length: 1 },
                cortexpb::BucketSpan { offset: 2, length: 1 },
            ],
            positive_deltas: vec![2, 21],
            positive_counts: vec![6.0, 7.0],
            reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
            timestamp: 4 * 60_000 - 1,
            custom_values: vec![3.0, 4.0],
        }],
        exemplars: vec![PoolExemplar {
            value: 1.0,
            timestamp_ms: 2,
            labels: vec![
                LabelAdapter::new("exemplarLabel1", "exemplarValue1"),
                LabelAdapter::new("exemplarLabel2", "exemplarValue2"),
            ],
        }],
        created_timestamp: 1234567890,
        skip_unmarshaling_exemplars: true,
    });

    let dst = deep_copy_timeseries(PreallocTimeseries::default(), &src, true, true);
    assert_eq!(dst.ts(), src.ts());
    assert_ne!(
        src.ts() as *const PoolTimeSeries,
        dst.ts() as *const PoolTimeSeries
    );
    assert_ne!(src.ts().labels.as_ptr(), dst.ts().labels.as_ptr());
    assert_ne!(src.ts().samples.as_ptr(), dst.ts().samples.as_ptr());
    assert_ne!(src.ts().histograms.as_ptr(), dst.ts().histograms.as_ptr());
    assert_ne!(src.ts().exemplars.as_ptr(), dst.ts().exemplars.as_ptr());

    let dst_stripped = deep_copy_timeseries(PreallocTimeseries::default(), &src, false, false);
    assert!(dst_stripped.ts().exemplars.is_empty());
    assert!(dst_stripped.ts().histograms.is_empty());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestDeepCopyTimeseriesExemplars
#[test]
fn test_deep_copy_timeseries_exemplars() {
    let mut ts = PoolTimeSeries {
        labels: vec![
            LabelAdapter::new("sampleLabel1", "sampleValue1"),
            LabelAdapter::new("sampleLabel2", "sampleValue2"),
        ],
        samples: vec![
            cortexpb::Sample { value: 1.0, timestamp_ms: 2 },
            cortexpb::Sample { value: 3.0, timestamp_ms: 4 },
        ],
        ..PoolTimeSeries::new()
    };
    for _ in 0..100 {
        ts.exemplars.push(PoolExemplar {
            value: 1.0,
            timestamp_ms: 2,
            labels: vec![
                LabelAdapter::new("exemplarLabel1", "exemplarValue1"),
                LabelAdapter::new("exemplarLabel2", "exemplarValue2"),
            ],
        });
    }
    let src = PreallocTimeseries::from_timeseries(ts);
    let dst1 = deep_copy_timeseries(PreallocTimeseries::default(), &src, false, false);
    let dst2 = deep_copy_timeseries(PreallocTimeseries::default(), &src, false, true);
    assert!(dst1.yolo_slice.as_ref().unwrap().capacity() < dst2.yolo_slice.as_ref().unwrap().capacity());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestDeepCopyTimeseriesCopiesAllFields
#[test]
fn test_deep_copy_timeseries_copies_all_fields() {
    let src = PreallocTimeseries::from_timeseries(PoolTimeSeries {
        labels: vec![LabelAdapter::new("label1", "value1")],
        samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: 2 }],
        exemplars: vec![PoolExemplar {
            value: 1.0,
            timestamp_ms: 2,
            labels: vec![LabelAdapter::new("e1", "v1")],
        }],
        histograms: vec![cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountInt(10)),
            sum: 1.0,
            schema: 1,
            zero_threshold: 0.01,
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
            negative_spans: vec![cortexpb::BucketSpan { offset: 1, length: 2 }],
            negative_deltas: vec![1, 2],
            negative_counts: vec![1.0, 2.0],
            positive_spans: vec![cortexpb::BucketSpan { offset: 3, length: 4 }],
            positive_deltas: vec![3, 4],
            positive_counts: vec![3.0, 4.0],
            reset_hint: cortexpb::histogram::ResetHint::Yes as i32,
            timestamp: 100,
            custom_values: vec![5.0, 6.0],
        }],
        created_timestamp: 1234567890,
        skip_unmarshaling_exemplars: true,
    });
    let dst = deep_copy_timeseries(PreallocTimeseries::default(), &src, true, true);
    assert_eq!(dst.ts(), src.ts());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseries_Unmarshal
#[test]
fn test_prealloc_timeseries_unmarshal() {
    let _g = UNMARSHAL_CACHE_LOCK.lock().unwrap();
    let src = PreallocTimeseries::from_timeseries(PoolTimeSeries {
        labels: vec![
            LabelAdapter::new("sampleLabel1", "sampleValue1"),
            LabelAdapter::new("sampleLabel2", "sampleValue2"),
        ],
        samples: vec![
            cortexpb::Sample { value: 1.0, timestamp_ms: 2 },
            cortexpb::Sample { value: 3.0, timestamp_ms: 4 },
        ],
        ..PoolTimeSeries::new()
    });
    let data = src.marshal();

    set_timeseries_unmarshal_caching_enabled(false);
    let mut msg = PreallocTimeseries::default();
    msg.unmarshal(&data).unwrap();
    assert_eq!(msg.ts(), src.ts());
    assert!(msg.marshalled_data.is_none());

    set_timeseries_unmarshal_caching_enabled(true);
    let mut msg = PreallocTimeseries::default();
    msg.unmarshal(&data).unwrap();
    assert_eq!(msg.ts(), src.ts());
    let correct_marshaled = msg.marshalled_data.clone().unwrap();

    let random_data: Vec<u8> = (0..100).map(|i| (i * 7 + 3) as u8).collect();
    msg.marshalled_data = Some(random_data.clone());
    assert_eq!(msg.size(), random_data.len());
    assert_eq!(msg.marshal(), random_data);

    let mut out = vec![0u8; 2 * msg.size()];
    let n = msg.marshal_to(&mut out).unwrap();
    assert_eq!(n, msg.size());
    assert_eq!(&out[..n], &random_data);

    let mut out2 = vec![0u8; msg.size()];
    let n2 = msg.marshal_to_sized_buffer(&mut out2).unwrap();
    assert_eq!(n2, out2.len());
    assert_eq!(out2, random_data);

    msg.clear_unmarshal_data();
    assert!(msg.marshalled_data.is_none());
    assert_eq!(msg.marshal(), correct_marshaled);

    let mut out3 = vec![0u8; 2 * msg.size()];
    let n3 = msg.marshal_to(&mut out3).unwrap();
    assert_eq!(n3, msg.size());
    assert_eq!(&out3[..n3], &correct_marshaled);
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseries_SortLabelsIfNeeded
#[test]
fn test_prealloc_timeseries_sort_labels_if_needed() {
    let mut sorted = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("bar", "baz"),
                LabelAdapter::new("cluster", "cluster"),
                LabelAdapter::new("sample", "1"),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    sorted.sort_labels_if_needed();
    assert!(sorted.marshalled_data.is_some());

    let mut unsorted = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("sample", "1"),
                LabelAdapter::new("cluster", "cluster"),
                LabelAdapter::new("bar", "baz"),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    unsorted.sort_labels_if_needed();
    assert!(unsorted.ts().labels.windows(2).all(|w| w[0].name <= w[1].name));
    assert!(unsorted.marshalled_data.is_none());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseries_RemoveLabel
#[test]
fn test_prealloc_timeseries_remove_label() {
    let mut p = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("bar", "baz"),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    p.remove_label("bar");
    assert_eq!(p.ts().labels, vec![LabelAdapter::new("__name__", "foo")]);
    assert!(p.marshalled_data.is_none());

    let mut p2 = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("bar", "baz"),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    p2.remove_label("foo");
    assert_eq!(
        p2.ts().labels,
        vec![
            LabelAdapter::new("__name__", "foo"),
            LabelAdapter::new("bar", "baz"),
        ]
    );
    assert!(p2.marshalled_data.is_some());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseries_RemoveEmptyLabelValues
#[test]
fn test_prealloc_timeseries_remove_empty_label_values() {
    let mut p = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("empty1", ""),
                LabelAdapter::new("bar", "baz"),
                LabelAdapter::new("empty2", ""),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    p.remove_empty_label_values();
    assert_eq!(
        p.ts().labels,
        vec![
            LabelAdapter::new("__name__", "foo"),
            LabelAdapter::new("bar", "baz"),
        ]
    );
    assert!(p.marshalled_data.is_none());

    let mut p2 = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("bar", "baz"),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    p2.remove_empty_label_values();
    assert!(p2.marshalled_data.is_some());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseries_SetLabels
#[test]
fn test_prealloc_timeseries_set_labels() {
    let mut p = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            labels: vec![
                LabelAdapter::new("__name__", "foo"),
                LabelAdapter::new("bar", "baz"),
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    let expected = vec![
        LabelAdapter::new("__name__", "hello"),
        LabelAdapter::new("lbl", "world"),
    ];
    p.set_labels(expected.clone());
    assert_eq!(p.ts().labels, expected);
    assert!(p.marshalled_data.is_none());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestPreallocTimeseries_ResizeExemplars
#[test]
fn test_prealloc_timeseries_resize_exemplars() {
    let exemplars = (0..10)
        .map(|i| PoolExemplar {
            labels: vec![
                LabelAdapter::new("trace", "1"),
                LabelAdapter::new("service", "A"),
            ],
            value: 1.0,
            timestamp_ms: i,
        })
        .collect();
    let mut p = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            exemplars,
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    p.resize_exemplars(5);
    assert_eq!(p.ts().exemplars.len(), 5);
    assert!(p.marshalled_data.is_none());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestClearExemplars
#[test]
fn test_clear_exemplars() {
    let mut ts = PoolTimeSeries {
        exemplars: vec![
            PoolExemplar {
                labels: vec![LabelAdapter::new("trace", "1"), LabelAdapter::new("service", "A")],
                value: 1.0,
                timestamp_ms: 2,
            },
            PoolExemplar {
                labels: vec![LabelAdapter::new("trace", "2"), LabelAdapter::new("service", "B")],
                value: 2.0,
                timestamp_ms: 3,
            },
        ],
        ..PoolTimeSeries::new()
    };
    clear_exemplars(&mut ts);
    assert!(ts.exemplars.is_empty());
    assert!(ts.exemplars.capacity() >= 2);

    let mut ts_large = PoolTimeSeries {
        exemplars: (0..11)
            .map(|_| PoolExemplar {
                labels: vec![LabelAdapter::new("trace", "1"), LabelAdapter::new("service", "A")],
                value: 1.0,
                timestamp_ms: 2,
            })
            .collect(),
        ..PoolTimeSeries::new()
    };
    clear_exemplars(&mut ts_large);
    assert!(ts_large.exemplars.is_empty());
    assert_eq!(ts_large.exemplars.capacity(), 0);
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestSortExemplars
#[test]
fn test_sort_exemplars() {
    let mut p = PreallocTimeseries {
        timeseries: Some(Box::new(PoolTimeSeries {
            exemplars: vec![
                PoolExemplar {
                    labels: vec![LabelAdapter::new("trace", "1"), LabelAdapter::new("service", "A")],
                    value: 1.0,
                    timestamp_ms: 3,
                },
                PoolExemplar {
                    labels: vec![LabelAdapter::new("trace", "2"), LabelAdapter::new("service", "B")],
                    value: 2.0,
                    timestamp_ms: 2,
                },
            ],
            ..PoolTimeSeries::new()
        })),
        marshalled_data: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    p.sort_exemplars();
    assert_eq!(p.ts().exemplars.len(), 2);
    assert_eq!(p.ts().exemplars[0].timestamp_ms, 2);
    assert_eq!(p.ts().exemplars[1].timestamp_ms, 3);
    assert!(p.marshalled_data.is_none());
}

// Upstream: pkg/mimirpb/timeseries_test.go::TestTimeSeries_MakeReferencesSafeToRetain
#[test]
fn test_time_series_make_references_safe_to_retain() {
    let mut ts = PoolTimeSeries {
        labels: vec![LabelAdapter::new("name", "value")],
        exemplars: vec![PoolExemplar {
            labels: vec![LabelAdapter::new("name", "value")],
            value: 0.0,
            timestamp_ms: 0,
        }],
        ..PoolTimeSeries::new()
    };
    ts.make_references_safe_to_retain();
    assert_eq!(ts.labels[0].name, "name");
    assert_eq!(ts.labels[0].value, "value");
    assert_eq!(ts.exemplars[0].labels[0].name, "name");
    assert_eq!(ts.exemplars[0].labels[0].value, "value");
}
