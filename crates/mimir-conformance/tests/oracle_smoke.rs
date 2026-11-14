use std::collections::HashMap;
use std::net::TcpStream;

use base64::Engine;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, run_go_mimir, BytesB64Response, ChunkResponse,
    ChunkSampleJson, DecodeChunkRequest, DecodeWriteRequest, EncodeChunkRequest, ExemplarJson,
    HashSeriesRequest, HashSeriesResponse, HistogramJson, KfakeBrokerRequest, KfakePartitionOffset,
    KfakeRecordInput, LabelJson, MetadataJson, PartitionRingLookupRequest,
    PartitionRingLookupResponse, PartitionTokensRequest, PartitionTokensResponse, RingLookupRequest,
    RingLookupResponse, RingMergeRequest, RingMergeResponse, SampleJson, SpanJson,
    SpawnedKfakeBroker, TimeSeriesJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest,
    VerifyTsdbDirResponse, WriteRequestJson, WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_proto::prost::Message;
use mimir_proto::ringpb::{
    Desc, InstanceDesc, InstanceState, OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc,
    PartitionState, PartitionTokenScheme,
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

#[test]
fn wave2_oracle_smoke() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let b64 = base64::engine::general_purpose::STANDARD;

    // 1. partition-tokens
    let pt_resp: PartitionTokensResponse = invoke_oracle(
        "partition-tokens",
        &PartitionTokensRequest { partitions: 3 },
    )
    .expect("partition-tokens");
    assert_eq!(pt_resp.tokens_by_partition.len(), 3);
    for toks in &pt_resp.tokens_by_partition {
        assert_eq!(toks.len(), 512);
        assert!(toks.windows(2).all(|w| w[0] < w[1]));
    }

    // 2. ring-lookup
    let now_sec = 1_700_000_000_i64;
    let mut ingesters = HashMap::new();
    ingesters.insert(
        "ing-0".to_string(),
        InstanceDesc {
            id: "ing-0".to_string(),
            addr: "10.0.0.1:9095".to_string(),
            timestamp: now_sec - 5,
            state: InstanceState::Active as i32,
            tokens: vec![100, 1000],
            zone: "zone-a".to_string(),
            ..Default::default()
        },
    );
    ingesters.insert(
        "ing-1".to_string(),
        InstanceDesc {
            id: "ing-1".to_string(),
            addr: "10.0.0.2:9095".to_string(),
            timestamp: now_sec - 5,
            state: InstanceState::Active as i32,
            tokens: vec![500, 2000],
            zone: "zone-b".to_string(),
            ..Default::default()
        },
    );
    ingesters.insert(
        "ing-2".to_string(),
        InstanceDesc {
            id: "ing-2".to_string(),
            addr: "10.0.0.3:9095".to_string(),
            timestamp: now_sec - 5,
            state: InstanceState::Active as i32,
            tokens: vec![800, 3000],
            zone: "zone-c".to_string(),
            ..Default::default()
        },
    );
    let desc = Desc { ingesters };
    let desc_b64 = b64.encode(desc.encode_to_vec());

    let lookup_resp: RingLookupResponse = invoke_oracle(
        "ring-lookup",
        &RingLookupRequest {
            desc_b64: desc_b64.clone(),
            replication_factor: 3,
            heartbeat_timeout_ms: 60_000,
            zone_awareness_enabled: true,
            op: "Write".to_string(),
            token: 150,
            now_unix_sec: now_sec,
        },
    )
    .expect("ring-lookup");
    assert!(lookup_resp.error.is_none(), "{:?}", lookup_resp.error);
    assert_eq!(lookup_resp.instance_ids, vec!["ing-1", "ing-2", "ing-0"]);
    assert_eq!(lookup_resp.max_errors, 1);

    // Deterministic unhealthy check via now_unix_sec far in future
    let unhealthy_resp: RingLookupResponse = invoke_oracle(
        "ring-lookup",
        &RingLookupRequest {
            desc_b64: desc_b64.clone(),
            replication_factor: 3,
            heartbeat_timeout_ms: 60_000,
            zone_awareness_enabled: false,
            op: "Write".to_string(),
            token: 150,
            now_unix_sec: now_sec + 3600,
        },
    )
    .expect("ring-lookup unhealthy");
    assert!(unhealthy_resp.error.is_some());
    assert!(unhealthy_resp.instance_ids.is_empty());

    // 3. ring-merge
    let local_desc = Desc {
        ingesters: HashMap::from([(
            "ing-0".to_string(),
            InstanceDesc {
                id: "ing-0".to_string(),
                addr: "10.0.0.1:9095".to_string(),
                timestamp: 100,
                state: InstanceState::Active as i32,
                tokens: vec![10, 20],
                ..Default::default()
            },
        )]),
    };
    let remote_desc = Desc {
        ingesters: HashMap::from([(
            "ing-1".to_string(),
            InstanceDesc {
                id: "ing-1".to_string(),
                addr: "10.0.0.2:9095".to_string(),
                timestamp: 200,
                state: InstanceState::Active as i32,
                tokens: vec![30, 40],
                ..Default::default()
            },
        )]),
    };
    let merge_resp: RingMergeResponse = invoke_oracle(
        "ring-merge",
        &RingMergeRequest {
            local_b64: b64.encode(local_desc.encode_to_vec()),
            remote_b64: b64.encode(remote_desc.encode_to_vec()),
            cas: false,
        },
    )
    .expect("ring-merge");
    let merged = Desc::decode(
        b64.decode(&merge_resp.merged_b64)
            .expect("decode merged_b64")
            .as_slice(),
    )
    .expect("decode merged proto");
    assert_eq!(merged.ingesters.len(), 2);
    assert!(merge_resp.change_b64.is_some());

    // 4. partition-ring-lookup
    let mut pr_desc = PartitionRingDesc::default();
    for pid in 0..3_i32 {
        pr_desc.partitions.insert(
            pid,
            PartitionDesc {
                id: pid,
                tokens: vec![],
                state: PartitionState::PartitionActive as i32,
                state_timestamp: 100,
                state_change_locked: false,
                state_change_locked_timestamp: 0,
                token_scheme: PartitionTokenScheme::PartitionTokensSmt512 as i32,
            },
        );
        pr_desc.owners.insert(
            format!("ingester-{pid}"),
            OwnerDesc {
                owned_partition: pid,
                state: OwnerState::OwnerActive as i32,
                updated_timestamp: 100,
            },
        );
    }
    let pr_resp: PartitionRingLookupResponse = invoke_oracle(
        "partition-ring-lookup",
        &PartitionRingLookupRequest {
            desc_b64: b64.encode(pr_desc.encode_to_vec()),
            max_derived_token_partitions: 4,
            token: 123_456_789,
            op: "Write".to_string(),
        },
    )
    .expect("partition-ring-lookup");
    assert!(pr_resp.error.is_none(), "{:?}", pr_resp.error);
    let pid = pr_resp.partition_id.expect("partition_id");
    assert!((0..3).contains(&pid));
    assert_eq!(pr_resp.owners, vec![format!("ingester-{pid}")]);

    // 5. SpawnedKfakeBroker (v2 + snappy + zstd)
    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest".to_string(),
        partitions: 4,
        compression: Some("snappy".to_string()),
        records: vec![
            KfakeRecordInput {
                partition: 0,
                tenant: "user-1".to_string(),
                version: 2,
                compression: None,
                write_request: WriteRequestJson {
                    source: "API".to_string(),
                    timeseries: vec![TimeSeriesJson {
                        labels: vec![LabelJson {
                            name: "__name__".to_string(),
                            value: "up".to_string(),
                        }],
                        samples: vec![SampleJson {
                            timestamp_ms: 1_700_000_000_000,
                            value: 1.0,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            KfakeRecordInput {
                partition: 2,
                tenant: "user-2".to_string(),
                version: 2,
                compression: Some("zstd".to_string()),
                write_request: WriteRequestJson {
                    source: "API".to_string(),
                    timeseries: vec![TimeSeriesJson {
                        labels: vec![LabelJson {
                            name: "__name__".to_string(),
                            value: "http_requests_total".to_string(),
                        }],
                        samples: vec![SampleJson {
                            timestamp_ms: 1_700_000_001_000,
                            value: 5.0,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
        ],
    })
    .expect("SpawnedKfakeBroker::spawn");

    assert_eq!(broker.topic(), "ingest");
    assert!(broker.broker_addr().starts_with("127.0.0.1:"));
    assert_eq!(
        broker.offsets(),
        &[
            KfakePartitionOffset {
                partition: 0,
                start_offset: 0,
                end_offset: 1
            },
            KfakePartitionOffset {
                partition: 1,
                start_offset: 0,
                end_offset: 0
            },
            KfakePartitionOffset {
                partition: 2,
                start_offset: 0,
                end_offset: 1
            },
            KfakePartitionOffset {
                partition: 3,
                start_offset: 0,
                end_offset: 0
            },
        ]
    );

    // Verify broker TCP listener is live while guard is held.
    let conn = TcpStream::connect(broker.broker_addr()).expect("connect to kfake broker");
    drop(conn);
    drop(broker);
}
