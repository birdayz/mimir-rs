#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use helpers::{mock_metric_metadata, mock_timeseries_with_all};
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::symbols::{
    v2_common_symbols, FastSymbolsTable, MIN_PREALLOCATED_SYMBOLS_PER_REQUEST,
    RECORD_VERSION_HEADER_KEY, V2_RECORD_SYMBOL_OFFSET,
};
use mimir_proto::zerocopy::WriteRequestScratch;
use mimir_storage::ingest::{
    deserialize_record_content, parse_record_version, record_version_header_raw,
    RecordSerializer, RecordVersion,
};

// Upstream: pkg/storage/ingest/version_test.go::TestV2SymbolsCompat
#[test]
fn test_v2_symbols_compat() {
    let cs = v2_common_symbols();

    // "v2 symbols cannot be larger than v2 offset"
    assert!(cs.get_slice().len() <= V2_RECORD_SYMBOL_OFFSET as usize);

    // "the zeroth symbol in v2 symbols must be empty string"
    assert!(cs.get_slice()[0].is_empty());
    assert_eq!(cs.get_ref(""), Some(0));
}

// Upstream: pkg/storage/ingest/version_test.go::TestRecordVersionHeader
#[test]
fn test_record_version_header() {
    // "no version header is assumed to be v0"
    let parsed_version = parse_record_version(std::iter::empty::<(&str, &[u8])>());
    assert_eq!(parsed_version, 0);

    for version in [0u32, 1, 255] {
        let hdr = record_version_header_raw(version);
        let parsed = parse_record_version([(RECORD_VERSION_HEADER_KEY, hdr.as_slice())]);
        assert_eq!(parsed, version as i32);
    }
}

// Upstream: pkg/storage/ingest/version_test.go::TestDeserializeRecordContent
#[test]
fn test_deserialize_record_content() {
    let mut scratch = WriteRequestScratch::new();

    // "invalid version"
    {
        let err = deserialize_record_content(&[], 255, &mut scratch)
            .err()
            .expect("expected unsupported version error");
        assert!(
            err.to_string().contains("unsupported version"),
            "unexpected error: {err}"
        );
    }

    // "v0"
    {
        let reqv0 = cortexpb::WriteRequest {
            timeseries: vec![
                mock_timeseries_with_all("series_0"),
                mock_timeseries_with_all("series_1"),
                mock_timeseries_with_all("series_2"),
            ],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            metadata: vec![
                mock_metric_metadata("series_0"),
                mock_metric_metadata("series_1"),
                mock_metric_metadata("series_2"),
            ],
            ..Default::default()
        };
        let mut v0_bytes = Vec::new();
        reqv0.encode(&mut v0_bytes).unwrap();
        let decoded = deserialize_record_content(&v0_bytes, 0, &mut scratch).unwrap();
        assert_eq!(decoded.to_owned_write_request(), reqv0);
    }

    // "v1"
    {
        let reqv1 = cortexpb::WriteRequest {
            timeseries: vec![
                mock_timeseries_with_all("series_0"),
                mock_timeseries_with_all("series_1"),
                mock_timeseries_with_all("series_2"),
            ],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            metadata: vec![
                mock_metric_metadata("series_0"),
                mock_metric_metadata("series_1"),
                mock_metric_metadata("series_2"),
            ],
            ..Default::default()
        };
        let mut v1_bytes = Vec::new();
        reqv1.encode(&mut v1_bytes).unwrap();
        let decoded = deserialize_record_content(&v1_bytes, 1, &mut scratch).unwrap();
        assert_eq!(decoded.to_owned_write_request(), reqv1);
    }

    // "v2"
    {
        let mut syms = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
        syms.configure_common_symbols(V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()));

        let ts_rw2 = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![
                syms.symbolize("__name__"),
                syms.symbolize("test_metric_total"),
                syms.symbolize("job"),
                syms.symbolize("test_job"),
            ],
            samples: vec![cortexpb::Sample {
                value: 123.456,
                timestamp_ms: 1234567890,
            }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                value: 123.456,
                timestamp: 1234567890,
                labels_refs: vec![
                    syms.symbolize("__name__"),
                    syms.symbolize("test_metric_total"),
                    syms.symbolize("traceID"),
                    syms.symbolize("1234567890abcdef"),
                ],
            }],
            histograms: vec![cortexpb::Histogram {
                timestamp: 1234567890,
                count: Some(cortexpb::histogram::Count::CountInt(10)),
                sum: 100.0,
                schema: 3,
                zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
                positive_spans: vec![cortexpb::BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_deltas: vec![1],
                negative_spans: vec![cortexpb::BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                negative_deltas: vec![1],
                ..Default::default()
            }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: syms.symbolize("Help for test_metric_total"),
                unit_ref: syms.symbolize("seconds"),
            }),
            created_timestamp: 0,
        };
        let reqv2 = cortexpb::WriteRequestRw2 {
            symbols: syms.symbols(),
            timeseries: vec![ts_rw2],
        };
        // Symbols should not contain common labels "__name__" and "job"
        assert_eq!(
            reqv2.symbols,
            vec![
                "",
                "test_metric_total",
                "test_job",
                "traceID",
                "1234567890abcdef",
                "Help for test_metric_total",
                "seconds",
            ]
        );
        let mut v2_bytes = Vec::new();
        reqv2.encode(&mut v2_bytes).unwrap();

        let decoded = deserialize_record_content(&v2_bytes, 2, &mut scratch)
            .unwrap()
            .to_owned_write_request();
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(
            decoded.timeseries[0].labels,
            vec![
                cortexpb::LabelPair::from_static("__name__", "test_metric_total"),
                cortexpb::LabelPair::from_static("job", "test_job"),
            ]
        );
        assert_eq!(
            decoded.timeseries[0].samples,
            vec![cortexpb::Sample {
                value: 123.456,
                timestamp_ms: 1234567890,
            }]
        );
        assert_eq!(
            decoded.timeseries[0].exemplars,
            vec![cortexpb::Exemplar {
                value: 123.456,
                timestamp_ms: 1234567890,
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "test_metric_total"),
                    cortexpb::LabelPair::from_static("traceID", "1234567890abcdef"),
                ],
            }]
        );
        assert_eq!(decoded.timeseries[0].histograms.len(), 1);
        assert_eq!(
            decoded.metadata,
            vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "test_metric_total".to_string(),
                help: "Help for test_metric_total".to_string(),
                unit: "seconds".to_string(),
            }]
        );
    }

    // "v2 skips metadata normalization"
    {
        let mut syms = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
        syms.configure_common_symbols(V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()));

        let ts_rw2 = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![
                syms.symbolize("__name__"),
                syms.symbolize("test_histogram_seconds_bucket"),
            ],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Histogram as i32,
                help_ref: syms.symbolize("Help for test_histogram_seconds_bucket"),
                unit_ref: syms.symbolize("seconds"),
            }),
            ..Default::default()
        };
        let reqv2 = cortexpb::WriteRequestRw2 {
            symbols: syms.symbols(),
            timeseries: vec![ts_rw2],
        };
        assert_eq!(
            reqv2.symbols,
            vec![
                "",
                "test_histogram_seconds_bucket",
                "Help for test_histogram_seconds_bucket",
                "seconds",
            ]
        );
        let mut v2_bytes = Vec::new();
        reqv2.encode(&mut v2_bytes).unwrap();

        let decoded = deserialize_record_content(&v2_bytes, 2, &mut scratch)
            .unwrap()
            .to_owned_write_request();
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(
            decoded.timeseries[0].labels,
            vec![cortexpb::LabelPair::from_static(
                "__name__",
                "test_histogram_seconds_bucket"
            )]
        );
        assert!(decoded.timeseries[0].samples.is_empty());
        assert!(decoded.timeseries[0].exemplars.is_empty());
        assert!(decoded.timeseries[0].histograms.is_empty());
        assert_eq!(
            decoded.metadata,
            vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                metric_family_name: "test_histogram_seconds_bucket".to_string(),
                help: "Help for test_histogram_seconds_bucket".to_string(),
                unit: "seconds".to_string(),
            }]
        );
    }

    // "v2 preserves conflicting metadata"
    {
        let mut syms = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
        syms.configure_common_symbols(V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()));

        let ts1 = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![
                syms.symbolize("__name__"),
                syms.symbolize("test_histogram_seconds_bucket"),
            ],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Histogram as i32,
                help_ref: syms.symbolize("Help for test_histogram_seconds_bucket"),
                unit_ref: syms.symbolize("seconds"),
            }),
            ..Default::default()
        };
        let ts2 = cortexpb::TimeSeriesRw2 {
            labels_refs: vec![
                syms.symbolize("__name__"),
                syms.symbolize("test_histogram_seconds_bucket"),
            ],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Gauge as i32,
                help_ref: syms.symbolize("Help for test_histogram_seconds_bucket, but different"),
                unit_ref: syms.symbolize("seconds"),
            }),
            ..Default::default()
        };
        let reqv2 = cortexpb::WriteRequestRw2 {
            symbols: syms.symbols(),
            timeseries: vec![ts1, ts2],
        };
        assert_eq!(
            reqv2.symbols,
            vec![
                "",
                "test_histogram_seconds_bucket",
                "Help for test_histogram_seconds_bucket",
                "seconds",
                "Help for test_histogram_seconds_bucket, but different",
            ]
        );
        let mut v2_bytes = Vec::new();
        reqv2.encode(&mut v2_bytes).unwrap();

        let decoded = deserialize_record_content(&v2_bytes, 2, &mut scratch)
            .unwrap()
            .to_owned_write_request();
        assert_eq!(decoded.timeseries.len(), 2);
        assert_eq!(
            decoded.metadata,
            vec![
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                    metric_family_name: "test_histogram_seconds_bucket".to_string(),
                    help: "Help for test_histogram_seconds_bucket".to_string(),
                    unit: "seconds".to_string(),
                },
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                    metric_family_name: "test_histogram_seconds_bucket".to_string(),
                    help: "Help for test_histogram_seconds_bucket, but different".to_string(),
                    unit: "seconds".to_string(),
                },
            ]
        );
    }
}

// Upstream: pkg/storage/ingest/version_test.go::TestRecordSerializer
#[test]
fn test_record_serializer() {
    let req = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        skip_label_validation: true,
        timeseries: vec![
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "series_1"),
                    cortexpb::LabelPair::from_static("pod", "test-application-123456"),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 20,
                    value: 0.0,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    timestamp_ms: 30,
                    value: 0.0,
                    labels: Vec::new(),
                }],
                histograms: vec![cortexpb::Histogram {
                    timestamp: 10,
                    ..Default::default()
                }],
                ..Default::default()
            },
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "series_2"),
                    cortexpb::LabelPair::from_static("pod", "test-application-123456"),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 30,
                    value: 0.0,
                }],
                ..Default::default()
            },
        ],
        metadata: vec![
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_1".to_string(),
                help: "This is the first test metric.".to_string(),
                unit: String::new(),
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_2".to_string(),
                help: "This is the second test metric.".to_string(),
                unit: String::new(),
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_3".to_string(),
                help: "This is the third test metric.".to_string(),
                unit: String::new(),
            },
        ],
        ..Default::default()
    };

    let input_size = req.encoded_len();
    let (records, returned_size) = RecordSerializer::new(RecordVersion::V2)
        .to_records("test", 1234, "user-1", &req, 100_000)
        .unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];

    // Verify that ToRecords returns the input size (RW1), not the output size (RW2).
    assert_eq!(returned_size, input_size);
    assert_eq!(record.topic, "test");
    assert_eq!(record.partition, 1234);
    assert_eq!(
        parse_record_version(
            record
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref()))
        ),
        2
    );

    let mut scratch = WriteRequestScratch::new();
    let result_req = deserialize_record_content(&record.value, 2, &mut scratch)
        .unwrap()
        .to_owned_write_request();

    assert_eq!(result_req.timeseries.len(), 3);
    assert_eq!(result_req.timeseries[0..2], req.timeseries[..]);
    assert_eq!(
        result_req.timeseries[2],
        cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("__name__", "series_3")],
            ..Default::default()
        }
    );
    assert!(result_req.symbols_rw2.is_empty());
    assert!(result_req.timeseries_rw2.is_empty());
    for md in &req.metadata {
        assert!(result_req.metadata.contains(md));
    }
    assert_eq!(result_req.metadata.len(), req.metadata.len());
}
