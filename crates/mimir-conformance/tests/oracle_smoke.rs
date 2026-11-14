use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, run_go_mimir, BytesB64Response, ChunkResponse,
    ChunkSampleJson, DecodeChunkRequest, DecodeWriteRequest, EncodeChunkRequest, ExemplarJson,
    HashSeriesRequest, HashSeriesResponse, HistogramJson, LabelJson, MetadataJson, SampleJson,
    SpanJson, TimeSeriesJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest, VerifyTsdbDirResponse,
    WriteRequestJson, WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};

#[test]
fn go_oracle_smoke_roundtrip() {
    ensure_go_oracle_built().expect("go-oracle and go-mimir must build");

    // 1. Check go-mimir -version
    let version_out = run_go_mimir(&["-version"]).expect("run go-mimir -version");
    assert!(
        version_out.status.success(),
        "go-mimir -version failed: {}",
        String::from_utf8_lossy(&version_out.stderr)
    );
    let version_stdout = String::from_utf8_lossy(&version_out.stdout);
    assert!(
        version_stdout.contains("Mimir"),
        "unexpected -version output: {version_stdout}"
    );

    // 2. Check hash-series
    let hash_resp: HashSeriesResponse = invoke_oracle(
        "hash-series",
        &HashSeriesRequest {
            user_id: "tenant-1".to_string(),
            metric_name: "http_requests_total".to_string(),
            labels: vec![
                LabelJson {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                LabelJson {
                    name: "job".to_string(),
                    value: "api".to_string(),
                },
            ],
        },
    )
    .expect("hash-series");
    assert_ne!(hash_resp.shard_by_user, 0);
    assert_ne!(hash_resp.shard_by_metric_name, 0);
    assert_ne!(hash_resp.shard_by_all_labels, 0);
    assert_ne!(hash_resp.prom_labels_hash, 0);

    // 3. Check encode-write-request / decode-write-request across classic, rw2, kafka-v2
    for mode in ["classic", "rw2", "kafka-v2"] {
        let req = WriteRequestJson {
            mode: mode.to_string(),
            source: "API".to_string(),
            skip_label_validation: false,
            skip_label_count_validation: false,
            timeseries: vec![TimeSeriesJson {
                labels: vec![
                    LabelJson {
                        name: "__name__".to_string(),
                        value: "cortex_request_duration_seconds_bucket".to_string(),
                    },
                    LabelJson {
                        name: "job".to_string(),
                        value: "mimir".to_string(),
                    },
                ],
                samples: vec![SampleJson {
                    timestamp_ms: 1_700_000_000_000,
                    value: 42.5,
                }],
                exemplars: vec![ExemplarJson {
                    labels: vec![LabelJson {
                        name: "trace_id".to_string(),
                        value: "abc123".to_string(),
                    }],
                    value: 1.25,
                    timestamp_ms: 1_700_000_000_000,
                }],
                histograms: vec![],
                created_timestamp: 0,
            }],
            metadata: vec![MetadataJson {
                metric_type: "COUNTER".to_string(),
                metric_family_name: "cortex_request_duration_seconds_bucket".to_string(),
                help: "Request duration".to_string(),
                unit: "seconds".to_string(),
            }],
        };

        let encoded: BytesB64Response =
            invoke_oracle("encode-write-request", &req).expect("encode-write-request");
        assert!(!encoded.bytes_b64.is_empty());

        let decoded: WriteRequestJson = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: mode.to_string(),
                bytes_b64: encoded.bytes_b64,
            },
        )
        .expect("decode-write-request");
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].samples, req.timeseries[0].samples);
        assert_eq!(decoded.timeseries[0].exemplars, req.timeseries[0].exemplars);
        assert_eq!(decoded.metadata, req.metadata);
    }

    // 4. Check encode-chunk / decode-chunk (XOR float)
    let samples = vec![
        ChunkSampleJson {
            st: 0,
            t: 1_000,
            v: 1.5,
        },
        ChunkSampleJson {
            st: 0,
            t: 2_000,
            v: 2.25,
        },
        ChunkSampleJson {
            st: 0,
            t: 3_000,
            v: 3.75,
        },
    ];
    let enc_chunk: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: 1, // EncXOR
            samples: samples.clone(),
            histograms: vec![],
        },
    )
    .expect("encode-chunk");
    assert_eq!(enc_chunk.encoding, 1);
    assert_eq!(enc_chunk.num_samples, 3);
    assert!(!enc_chunk.bytes_b64.is_empty());

    let dec_chunk: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: 1,
            bytes_b64: enc_chunk.bytes_b64,
        },
    )
    .expect("decode-chunk");
    assert_eq!(dec_chunk.num_samples, 3);
    assert_eq!(dec_chunk.samples, samples);

    // 5a. Check write-tsdb-fixture / verify-tsdb-dir (WAL / Head without compaction: preserves samples, histograms, and exemplars)
    let tmp_wal = tempfile::tempdir().expect("tempdir wal");
    let dir_wal = tmp_wal.path().to_string_lossy().to_string();

    let fixture_series = vec![
        TsdbSeriesFixtureJson {
            labels: vec![
                LabelJson {
                    name: "__name__".to_string(),
                    value: "hist_metric".to_string(),
                },
                LabelJson {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            samples: vec![],
            histograms: vec![HistogramJson {
                timestamp_ms: 1_700_000_000_000,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 5,
                sum: 12.5,
                reset_hint: 1,
                is_float: false,
                positive_spans: vec![SpanJson {
                    offset: 0,
                    length: 2,
                }],
                positive_deltas: vec![2, -1],
                ..Default::default()
            }],
            exemplars: vec![],
        },
        TsdbSeriesFixtureJson {
            labels: vec![
                LabelJson {
                    name: "__name__".to_string(),
                    value: "up".to_string(),
                },
                LabelJson {
                    name: "job".to_string(),
                    value: "ingester".to_string(),
                },
            ],
            samples: vec![
                ChunkSampleJson {
                    st: 0,
                    t: 1_700_000_000_000,
                    v: 1.0,
                },
                ChunkSampleJson {
                    st: 0,
                    t: 1_700_000_015_000,
                    v: 2.0,
                },
            ],
            histograms: vec![],
            exemplars: vec![ExemplarJson {
                labels: vec![LabelJson {
                    name: "trace_id".to_string(),
                    value: "deadbeef".to_string(),
                }],
                value: 2.0,
                timestamp_ms: 1_700_000_015_000,
            }],
        },
    ];

    let write_wal_resp: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_wal.clone(),
            compact: false,
            out_of_order_time_window_ms: 0,
            series: fixture_series.clone(),
        },
    )
    .expect("write-tsdb-fixture wal");
    assert_eq!(write_wal_resp.status, "ok");
    assert!(write_wal_resp.blocks.is_empty());

    let verify_wal_resp: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir_wal,
            read_only: false,
        },
    )
    .expect("verify-tsdb-dir wal");
    assert!(verify_wal_resp.block_ulids.is_empty());
    assert_eq!(verify_wal_resp.head_series, 2);
    assert_eq!(verify_wal_resp.series.len(), 2);
    assert_eq!(verify_wal_resp.series[0].histograms.len(), 1);
    assert_eq!(verify_wal_resp.series[1].samples.len(), 2);
    assert_eq!(verify_wal_resp.series[1].exemplars.len(), 1);

    // 5b. Check write-tsdb-fixture / verify-tsdb-dir (compacted block)
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir_str = tmp.path().to_string_lossy().to_string();

    let write_resp: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_str.clone(),
            compact: true,
            out_of_order_time_window_ms: 0,
            series: fixture_series,
        },
    )
    .expect("write-tsdb-fixture compact");
    assert_eq!(write_resp.status, "ok");
    assert_eq!(write_resp.blocks.len(), 1);

    let verify_resp: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir_str,
            read_only: false,
        },
    )
    .expect("verify-tsdb-dir compact");
    assert_eq!(verify_resp.block_ulids, write_resp.blocks);
    assert_eq!(verify_resp.series.len(), 2);
    assert_eq!(verify_resp.series[0].histograms.len(), 1);
    assert_eq!(verify_resp.series[1].samples.len(), 2);
}
