//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_http_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_compartments_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_ingest_storage_test.go

mod compaction_ring_helpers;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use compaction_ring_helpers::fake_kafka::HookAction;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{FetchResponse, ListOffsetsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use mimir_common::services::{Service, State};
use mimir_common::shutdownmarker;
use mimir_config::{CompartmentsConfig, GoDuration, StringSliceCSV};
use mimir_ingester::compaction::time_until_compaction;
use mimir_ingester::http::ingester_router;
use mimir_ingester::ingest_storage::ingester_partition_id_from_instance_id;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ringpb::{Desc, InstanceState, OwnerState, PartitionRingDesc, PartitionState};
use mimir_ring::kv::CasOutcome;
use mimir_ring::partition_ring::model::{PartitionRingDescExt, PartitionRingError};
use mimir_ring::partition_ring::PartitionRingEditor;
use mimir_ring::ring::model::DescExt;
use mimir_ring::ring::replication_strategy::WRITE;
use mimir_ring::token_gen::{SpreadMinimizingTokenGenerator, TokenGenerator};
use mimir_storage::ingest::IngestWriter;
use prometheus::Registry;
use tonic::metadata::MetadataMap;
use tower::ServiceExt;

use compaction_ring_helpers::{
    create_test_compartments_ingester, create_test_ingester_with_ingest_storage, create_test_ring,
    default_ingester_test_config, default_limits_test_config, get_instance_state_in_ring,
    get_instance_token_count, poll_until, run_test_query, INGEST_RING_KEY, PARTITION_RING_KEY,
    USER_ID,
};

fn parse_utc_system_time(s: &str) -> SystemTime {
    // Format: "YYYY-MM-DDTHH:MM:SSZ"
    let date_part = &s[0..10];
    let time_part = &s[11..19];
    let d: Vec<i64> = date_part.split('-').map(|p| p.parse().unwrap()).collect();
    let t: Vec<i64> = time_part.split(':').map(|p| p.parse().unwrap()).collect();
    let (y, m, day) = (d[0], d[1], d[2]);
    let y_adj = y - if m <= 2 { 1 } else { 0 };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let secs = days * 86_400 + t[0] * 3600 + t[1] * 60 + t[2];
    UNIX_EPOCH + Duration::from_secs(secs as u64)
}

fn single_series_write_request(name: &str, ts: i64, val: f64) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", name)],
            samples: vec![cortexpb::Sample {
                value: val,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn gauge_value(reg: &Registry, metric_name: &str, label: Option<(&str, &str)>) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() != metric_name {
            continue;
        }
        for m in mf.get_metric() {
            if let Some((lk, lv)) = label {
                if !m
                    .get_label()
                    .iter()
                    .any(|lp| lp.get_name() == lk && lp.get_value() == lv)
                {
                    continue;
                }
            }
            return Some(m.get_gauge().get_value());
        }
    }
    None
}

fn counter_value(reg: &Registry, metric_name: &str) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == metric_name {
            if let Some(m) = mf.get_metric().first() {
                return m.get_counter().get_value();
            }
        }
    }
    0.0
}

// Upstream: pkg/ingester/ingester_http_test.go::TestIngester_FlushHandler_WhileStarting
#[tokio::test]
async fn test_ingester_flush_handler_while_starting() {
    const EXPECTED_SERIES_TO_REPLAY: i64 = 2;

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(1);

    let reg = Registry::new();
    let h = create_test_ingester_with_ingest_storage(
        cfg,
        default_limits_test_config(),
        None,
        None,
        Duration::from_millis(10),
        Some(&reg),
    )
    .await;
    h.ingester
        .set_head_compaction_interval_while_starting(Duration::from_millis(100));
    h.ingester.set_head_compaction_interval_jitter_enabled(false);

    let fetch_count = Arc::new(AtomicI64::new(0));
    let fc_c = Arc::clone(&fetch_count);
    *h.clusters[0].hooks.fetch.lock() = Some(Arc::new(move |_req| {
        let c = Arc::clone(&fc_c);
        Box::pin(async move {
            c.fetch_add(1, Ordering::AcqRel);
            HookAction::PassThrough
        })
    }));

    let topic_name = h.ingester.ingest_cfg.kafka.topic.clone();
    let pid = h.ingester.ingest_partition_id;
    *h.clusters[0].hooks.list_offsets.lock() = Some(Arc::new(move |req| {
        let tname = topic_name.clone();
        Box::pin(async move {
            let is_end = req
                .topics
                .iter()
                .all(|t| t.partitions.iter().all(|p| p.timestamp == -1));
            if !is_end {
                return HookAction::PassThrough;
            }
            let mut resp = ListOffsetsResponse::default();
            let mut lt = ListOffsetsTopicResponse::default();
            lt.name = TopicName(StrBytes::from_string(tname));
            let mut lp = ListOffsetsPartitionResponse::default();
            lp.partition_index = pid;
            lp.error_code = 0;
            lp.offset = EXPECTED_SERIES_TO_REPLAY;
            lt.partitions.push(lp);
            resp.topics.push(lt);
            HookAction::Respond(resp)
        })
    }));

    let writer = IngestWriter::from_config(&h.ingester.ingest_cfg.kafka, None).unwrap();
    writer
        .write_sync(
            &h.ingester.ingest_cfg.kafka.topic,
            pid,
            USER_ID,
            &single_series_write_request("series_1", 1000, 10.0),
        )
        .await
        .unwrap();

    h.ingester.start_async().await.unwrap();

    let fetched = poll_until(Duration::from_secs(5), || async {
        fetch_count.load(Ordering::Acquire) > 0
    })
    .await;
    assert!(fetched, "expected fetch requests while starting");
    assert_eq!(h.ingester.state(), State::Starting);
    assert_eq!(h.ingester.compaction_service_state(), State::Running);

    let router = ingester_router(Arc::clone(&h.ingester));
    let resp = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/ingester/flush?wait=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Unblock ingester by writing second series
    writer
        .write_sync(
            &h.ingester.ingest_cfg.kafka.topic,
            pid,
            USER_ID,
            &single_series_write_request("series_2", 1000, 10.0),
        )
        .await
        .unwrap();

    let running = poll_until(Duration::from_secs(5), || async {
        h.ingester.state() == State::Running
    })
    .await;
    assert!(running, "expected ingester to transition to Running");

    h.cleanup().await;
}

// Upstream: pkg/ingester/ingester_compartments_test.go::TestIngester_Compartments_RegistersPartitionInReadCompartmentRing
#[tokio::test]
async fn test_ingester_compartments_registers_partition_in_read_compartment_ring() {
    const READ_COMPARTMENT_ID: i64 = 1;

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.ring.instance_id = "ingester-zone-a-0".to_string();
    cfg.ingester.read_compartment_id = READ_COMPARTMENT_ID;

    let mut compartments = CompartmentsConfig {
        enabled: true,
        ..Default::default()
    };
    compartments.read.num_compartments = 2;
    compartments.write.num_compartments = 1;

    let h = create_test_compartments_ingester(
        cfg,
        compartments,
        default_limits_test_config(),
        None,
        Duration::from_millis(10),
        None,
    )
    .await;

    h.ingester.start().await.unwrap();

    let compartment_key = format!("{PARTITION_RING_KEY}-rc-{READ_COMPARTMENT_ID}");
    let count_partitions = |key: String| {
        let kv = Arc::clone(&h.partition_kv);
        async move {
            let val = kv.get(&key).await.unwrap();
            val.and_then(|a| a.downcast_ref::<PartitionRingDesc>().map(|d| d.partitions.len()))
                .unwrap_or(0)
        }
    };

    let registered = poll_until(Duration::from_secs(5), || {
        let k = compartment_key.clone();
        async move { count_partitions(k).await == 1 }
    })
    .await;
    assert!(registered, "partition should register under compartment key");
    assert_eq!(count_partitions(PARTITION_RING_KEY.to_string()).await, 0);

    h.cleanup().await;
}

// Upstream: pkg/ingester/ingester_compartments_test.go::TestIngester_Compartments_ShouldConsumeReadCompartmentTopicAtStartup
#[tokio::test]
async fn test_ingester_compartments_should_consume_read_compartment_topic_at_startup() {
    const READ_COMPARTMENT_ID: i64 = 1;
    const NUM_WRITE_COMPARTMENTS: usize = 2;
    const EXPECTED_SERIES_TO_REPLAY: i64 = 2;

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.owned_series_update_interval = GoDuration::from_millis(100);
    cfg.ingester.active_series.active_series_metrics_update_period = GoDuration::from_millis(100);
    cfg.ingester.read_compartment_id = READ_COMPARTMENT_ID;
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(1);

    let mut compartments = CompartmentsConfig {
        enabled: true,
        ..Default::default()
    };
    compartments.read.num_compartments = 2;
    compartments.write.num_compartments = NUM_WRITE_COMPARTMENTS as i64;

    let reg = Registry::new();
    let h = create_test_compartments_ingester(
        cfg,
        compartments,
        default_limits_test_config(),
        None,
        Duration::from_millis(10),
        Some(&reg),
    )
    .await;
    h.ingester
        .set_head_compaction_interval_while_starting(Duration::from_millis(100));
    h.ingester.set_head_compaction_interval_jitter_enabled(false);
    h.ingester.set_min_compaction_loop_delay(Duration::ZERO);
    h.ingester
        .set_limit_metrics_update_period(Duration::from_millis(100));

    assert_eq!(h.clusters.len(), NUM_WRITE_COMPARTMENTS);
    let consumed_topic = format!("mimir-rc-{READ_COMPARTMENT_ID}");
    let pid = h.ingester.ingest_partition_id;

    let fetch_count = Arc::new(AtomicI64::new(0));
    for cluster in &h.clusters {
        let fc_c = Arc::clone(&fetch_count);
        *cluster.hooks.fetch.lock() = Some(Arc::new(move |_req| {
            let c = Arc::clone(&fc_c);
            Box::pin(async move {
                c.fetch_add(1, Ordering::AcqRel);
                HookAction::PassThrough
            })
        }));

        let tname = consumed_topic.clone();
        *cluster.hooks.list_offsets.lock() = Some(Arc::new(move |req| {
            let tn = tname.clone();
            Box::pin(async move {
                let is_end = req
                    .topics
                    .iter()
                    .all(|t| t.partitions.iter().all(|p| p.timestamp == -1));
                if !is_end {
                    return HookAction::PassThrough;
                }
                let mut resp = ListOffsetsResponse::default();
                let mut lt = ListOffsetsTopicResponse::default();
                lt.name = TopicName(StrBytes::from_string(tn));
                let mut lp = ListOffsetsPartitionResponse::default();
                lp.partition_index = pid;
                lp.error_code = 0;
                lp.offset = EXPECTED_SERIES_TO_REPLAY;
                lt.partitions.push(lp);
                resp.topics.push(lt);
                HookAction::Respond(resp)
            })
        }));
    }

    let mut writers = Vec::with_capacity(NUM_WRITE_COMPARTMENTS);
    for cluster in &h.clusters {
        let mut wcfg = h.ingester.ingest_cfg.kafka.clone();
        wcfg.address = StringSliceCSV(vec![cluster.addr.clone()]);
        wcfg.topic = consumed_topic.clone();
        let w = IngestWriter::from_config(&wcfg, None).unwrap();
        w.write_sync(
            &consumed_topic,
            pid,
            USER_ID,
            &single_series_write_request("series_1", 1000, 10.0),
        )
        .await
        .unwrap();
        writers.push(w);
    }

    // Pre-populate rings to simulate restart
    let toks = SpreadMinimizingTokenGenerator::new(
        &h.ingester.cfg.ring.instance_id,
        &h.ingester.cfg.ring.instance_availability_zone,
        &["zone-a".to_string()],
        false,
    )
    .map(|g| g.generate_tokens(512, &[]))
    .unwrap_or_else(|_| {
        mimir_ring::token_gen::RandomTokenGenerator::new().generate_tokens(512, &[])
    });
    let ing_id = h.ingester.cfg.ring.instance_id.clone();
    let ing_addr = h.ingester.cfg.ring.instance_addr.clone();
    let ing_zone = h.ingester.cfg.ring.instance_availability_zone.clone();
    h.ring
        .kv_client()
        .unwrap()
        .cas(
            INGEST_RING_KEY,
            Box::new(move |in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<Desc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_ingester(
                    &ing_id,
                    &ing_addr,
                    &ing_zone,
                    toks.clone(),
                    InstanceState::Leaving,
                    Some(SystemTime::now()),
                    false,
                    None,
                    Default::default(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let compartment_key = format!("{PARTITION_RING_KEY}-rc-{READ_COMPARTMENT_ID}");
    let ing_id2 = h.ingester.cfg.ring.instance_id.clone();
    h.partition_kv
        .cas(
            &compartment_key,
            Box::new(move |in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_partition(pid, PartitionState::PartitionActive, SystemTime::now());
                desc.add_or_update_owner(&ing_id2, OwnerState::OwnerDeleted, pid, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    h.ingester.start_async().await.unwrap();

    let fetched = poll_until(Duration::from_secs(5), || async {
        fetch_count.load(Ordering::Acquire) >= NUM_WRITE_COMPARTMENTS as i64
    })
    .await;
    assert!(fetched, "expected fetches from every write compartment");

    // Verify internal services are running while starting
    let services_running = poll_until(Duration::from_secs(2), || async {
        h.ingester.compaction_service_state() == State::Running
            && h.ingester.owned_series_service_state() == State::Running
            && h.ingester.metrics_updater_service_state() == State::Running
            && h.ingester.metadata_purger_service_state() == State::Running
    })
    .await;
    assert!(services_running);

    let comp_triggered = poll_until(Duration::from_secs(2), || async {
        counter_value(&reg, "cortex_ingester_tsdb_compactions_triggered_total") > 0.0
    })
    .await;
    assert!(comp_triggered, "expected compactions triggered during startup");

    let metrics_ok = poll_until(Duration::from_secs(2), || async {
        gauge_value(&reg, "cortex_ingester_active_series", Some(("user", USER_ID))) == Some(1.0)
            && gauge_value(&reg, "cortex_ingester_owned_series", Some(("user", USER_ID)))
                == Some(1.0)
    })
    .await;
    assert!(metrics_ok, "expected active and owned series metrics == 1");

    let (first_iv, std_iv) = h
        .ingester
        .compaction_service_interval(SystemTime::now(), &h.ring.zones());
    assert_eq!(first_iv, Duration::from_millis(100));
    assert_eq!(std_iv, Duration::from_millis(100));

    assert_eq!(h.ingester.state(), State::Starting);
    assert_eq!(h.ingester.lifecycler_service_state(), State::New);

    // Unblock both clusters
    for w in &writers {
        w.write_sync(
            &consumed_topic,
            pid,
            USER_ID,
            &single_series_write_request("series_2", 1000, 10.0),
        )
        .await
        .unwrap();
    }

    let is_running = poll_until(Duration::from_secs(5), || async {
        h.ingester.state() == State::Running
    })
    .await;
    assert!(is_running, "expected ingester to reach Running");
    assert_eq!(h.ingester.lifecycler_service_state(), State::Running);

    let active_in_ring = poll_until(Duration::from_secs(2), || async {
        h.ring
            .get_instance_state(&h.ingester.cfg.ring.instance_id)
            .ok()
            == Some(InstanceState::Active)
    })
    .await;
    assert!(active_in_ring);

    let owner_ok = poll_until(Duration::from_secs(2), || async {
        h.watcher.partition_ring().partition_owner_ids(pid)
            == vec![h.ingester.cfg.ring.instance_id.clone()]
    })
    .await;
    assert!(owner_ok);

    h.cleanup().await;
}

// Upstream: pkg/ingester/ingester_compartments_test.go::TestIngester_Compartments_QueryStream_ReadConsistency
#[tokio::test]
async fn test_ingester_compartments_query_stream_read_consistency() {
    const NUM_WRITE_COMPARTMENTS: usize = 2;

    let cases = [
        ("eventual read consistency", "eventual", None, 0usize),
        ("strong read consistency without offsets", "strong", None, NUM_WRITE_COMPARTMENTS),
        (
            "strong read consistency with offsets",
            "strong",
            Some("v2=1/0:-2;-2"),
            0,
        ),
        (
            "strong read consistency with offsets for a different read compartment falls back to last produced",
            "strong",
            Some("v2=0/0:-2;-2"),
            NUM_WRITE_COMPARTMENTS,
        ),
    ];

    for (name, level, offsets_hdr, expected_series) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.ingester.read_compartment_id = 1;

        let mut compartments = CompartmentsConfig {
            enabled: true,
            ..Default::default()
        };
        compartments.read.num_compartments = 2;
        compartments.write.num_compartments = NUM_WRITE_COMPARTMENTS as i64;

        let mut limits = default_limits_test_config();
        limits.ingest_storage_read_consistency = level.to_string();

        let h = create_test_compartments_ingester(
            cfg,
            compartments,
            limits,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;

        let fail_fetch = Arc::new(AtomicBool::new(true));
        for cluster in &h.clusters {
            let ff = Arc::clone(&fail_fetch);
            *cluster.hooks.fetch.lock() = Some(Arc::new(move |req| {
                let ff_c = Arc::clone(&ff);
                Box::pin(async move {
                    if ff_c.load(Ordering::Acquire) {
                        let mut resp = FetchResponse::default();
                        for ft in &req.topics {
                            let mut fr = FetchableTopicResponse::default();
                            fr.topic = ft.topic.clone();
                            for fp in &ft.partitions {
                                let mut pd = PartitionData::default();
                                pd.partition_index = fp.partition;
                                pd.error_code = 1;
                                fr.partitions.push(pd);
                            }
                            resp.responses.push(fr);
                        }
                        return HookAction::Respond(resp);
                    }
                    HookAction::PassThrough
                })
            }));
        }

        h.ingester.start().await.unwrap();
        assert!(
            poll_until(Duration::from_secs(2), || async {
                h.ring.get_all_healthy(WRITE).map(|rs| rs.instances.len()).unwrap_or(0) == 1
            })
            .await
        );

        let consumed_topic = "mimir-rc-1";
        let pid = h.ingester.ingest_partition_id;
        for (wc_id, cluster) in h.clusters.iter().enumerate() {
            let mut wcfg = h.ingester.ingest_cfg.kafka.clone();
            wcfg.address = StringSliceCSV(vec![cluster.addr.clone()]);
            wcfg.topic = consumed_topic.to_string();
            let w = IngestWriter::from_config(&wcfg, None).unwrap();
            w.write_sync(
                consumed_topic,
                pid,
                USER_ID,
                &single_series_write_request(&format!("series_wc_{wc_id}"), 1000, 10.0),
            )
            .await
            .unwrap();
        }

        let mut md = MetadataMap::new();
        if let Some(hdr) = offsets_hdr {
            md.insert("__consistency_offsets__", hdr.parse().unwrap());
        }

        let ing_c = Arc::clone(&h.ingester);
        let issued = Arc::new(tokio::sync::Notify::new());
        let issued_c = Arc::clone(&issued);
        let qh = tokio::spawn(async move {
            issued_c.notify_one();
            run_test_query(
                &ing_c,
                &md,
                USER_ID,
                ingesterpb::MatchType::RegexMatch,
                "__name__",
                "series_wc_.*",
            )
            .await
            .unwrap()
        });

        issued.notified().await;
        fail_fetch.store(false, Ordering::Release);
        let res = qh.await.unwrap();
        assert_eq!(res.len(), expected_series, "case {name}");

        h.cleanup().await;
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_Startup_PartitionRingActiveBlocksOnInstanceRingActive
#[tokio::test]
async fn test_ingester_startup_partition_ring_active_blocks_on_instance_ring_active() {
    let tmp0 = tempfile::tempdir().unwrap();
    let tmp1 = tempfile::tempdir().unwrap();

    let mut cfg0 = default_ingester_test_config(tmp0.path());
    cfg0.ingester.ring.token_generation_strategy = "spread-minimizing".to_string();
    cfg0.ingester.ring.instance_availability_zone = "zone-a".to_string();
    cfg0.ingester.ring.spread_minimizing_zones = StringSliceCSV(vec!["zone-a".to_string()]);
    cfg0.ingester.ring.spread_minimizing_join_ring_in_order = true;
    cfg0.ingester.ring.instance_id = "ingester-zone-a-0".to_string();

    let mut cfg1 = default_ingester_test_config(tmp1.path());
    cfg1.ingester.ring = cfg0.ingester.ring.clone();
    cfg1.ingester.ring.instance_id = "ingester-zone-a-1".to_string();
    cfg1.ring_kv = Arc::clone(&cfg0.ring_kv);

    let (ring, ring_svc) = create_test_ring(&cfg1.ingester.ring, Arc::clone(&cfg1.ring_kv), None).await;

    // Register i0 in PENDING with empty tokens so i1 blocks in auto_join.
    let i0_id = cfg0.ingester.ring.instance_id.clone();
    let i0_addr = cfg0.ingester.ring.instance_addr.clone();
    let i0_zone = cfg0.ingester.ring.instance_availability_zone.clone();
    ring.kv_client()
        .unwrap()
        .cas(
            INGEST_RING_KEY,
            Box::new(move |in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<Desc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_ingester(
                    &i0_id,
                    &i0_addr,
                    &i0_zone,
                    vec![],
                    InstanceState::Pending,
                    Some(SystemTime::now()),
                    false,
                    None,
                    Default::default(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();
    assert!(
        poll_until(Duration::from_secs(2), || async {
            ring.get_instance_state("ingester-zone-a-0").ok() == Some(InstanceState::Pending)
        })
        .await
    );

    let h1 = create_test_ingester_with_ingest_storage(
        cfg1,
        default_limits_test_config(),
        Some((Arc::clone(&ring), Arc::clone(&ring_svc))),
        None,
        Duration::from_millis(10),
        None,
    )
    .await;

    h1.ingester.start_async().await.unwrap();

    // Wait 1s while i1 is blocked waiting for i0 to claim tokens
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(h1.ingester.state(), State::Starting);
    assert_eq!(
        ring.get_instance_state("ingester-zone-a-1").ok(),
        Some(InstanceState::Pending)
    );
    assert_eq!(h1.ingester.partition_lifecycler_service_state(), State::New);
    let pstate = h1
        .ingester
        .partition_lifecycler
        .as_ref()
        .unwrap()
        .get_partition_state()
        .await;
    assert!(matches!(pstate, Err(PartitionRingError::PartitionDoesNotExist)));

    let stop_err = h1.ingester.stop().await.unwrap_err();
    assert!(
        stop_err
            .to_string()
            .contains("failed to wait for instance to be active in ring"),
        "unexpected stop error: {stop_err}"
    );

    h1.watcher_svc.stop_async();
    let _ = h1.watcher_svc.await_terminated().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
    for c in h1.clusters {
        c.close();
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_ShouldReplayPartitionAtStartupAndThenJoinTheRings
#[tokio::test]
async fn test_ingester_should_replay_partition_at_startup_and_then_join_the_rings() {
    const EXPECTED_SERIES_TO_REPLAY: i64 = 2;

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.owned_series_update_interval = GoDuration::from_millis(100);
    cfg.ingester.active_series.active_series_metrics_update_period = GoDuration::from_millis(100);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(1);

    let reg = Registry::new();
    let h = create_test_ingester_with_ingest_storage(
        cfg,
        default_limits_test_config(),
        None,
        None,
        Duration::from_millis(10),
        Some(&reg),
    )
    .await;
    h.ingester
        .set_head_compaction_interval_while_starting(Duration::from_millis(100));
    h.ingester.set_head_compaction_interval_jitter_enabled(false);
    h.ingester.set_min_compaction_loop_delay(Duration::ZERO);
    h.ingester
        .set_limit_metrics_update_period(Duration::from_millis(100));

    let fetch_count = Arc::new(AtomicI64::new(0));
    let fc_c = Arc::clone(&fetch_count);
    *h.clusters[0].hooks.fetch.lock() = Some(Arc::new(move |_req| {
        let c = Arc::clone(&fc_c);
        Box::pin(async move {
            c.fetch_add(1, Ordering::AcqRel);
            HookAction::PassThrough
        })
    }));

    let topic_name = h.ingester.ingest_cfg.kafka.topic.clone();
    let pid = h.ingester.ingest_partition_id;
    *h.clusters[0].hooks.list_offsets.lock() = Some(Arc::new(move |req| {
        let tname = topic_name.clone();
        Box::pin(async move {
            let is_end = req
                .topics
                .iter()
                .all(|t| t.partitions.iter().all(|p| p.timestamp == -1));
            if !is_end {
                return HookAction::PassThrough;
            }
            let mut resp = ListOffsetsResponse::default();
            let mut lt = ListOffsetsTopicResponse::default();
            lt.name = TopicName(StrBytes::from_string(tname));
            let mut lp = ListOffsetsPartitionResponse::default();
            lp.partition_index = pid;
            lp.error_code = 0;
            lp.offset = EXPECTED_SERIES_TO_REPLAY;
            lt.partitions.push(lp);
            resp.topics.push(lt);
            HookAction::Respond(resp)
        })
    }));

    let writer = IngestWriter::from_config(&h.ingester.ingest_cfg.kafka, None).unwrap();
    writer
        .write_sync(
            &h.ingester.ingest_cfg.kafka.topic,
            pid,
            USER_ID,
            &single_series_write_request("series_1", 1000, 10.0),
        )
        .await
        .unwrap();

    // Simulate restart: instance LEAVING in ring, partition ACTIVE + owner DELETED in partition ring.
    let toks = mimir_ring::token_gen::RandomTokenGenerator::new().generate_tokens(512, &[]);
    let ing_id = h.ingester.cfg.ring.instance_id.clone();
    let ing_addr = h.ingester.cfg.ring.instance_addr.clone();
    let ing_zone = h.ingester.cfg.ring.instance_availability_zone.clone();
    h.ring
        .kv_client()
        .unwrap()
        .cas(
            INGEST_RING_KEY,
            Box::new(move |in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<Desc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_ingester(
                    &ing_id,
                    &ing_addr,
                    &ing_zone,
                    toks.clone(),
                    InstanceState::Leaving,
                    Some(SystemTime::now()),
                    false,
                    None,
                    Default::default(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let ing_id2 = h.ingester.cfg.ring.instance_id.clone();
    h.partition_kv
        .cas(
            PARTITION_RING_KEY,
            Box::new(move |in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_partition(pid, PartitionState::PartitionActive, SystemTime::now());
                desc.add_or_update_owner(&ing_id2, OwnerState::OwnerDeleted, pid, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    h.ingester.start_async().await.unwrap();

    assert!(
        poll_until(Duration::from_secs(5), || async {
            fetch_count.load(Ordering::Acquire) > 0
        })
        .await
    );

    assert!(
        poll_until(Duration::from_secs(2), || async {
            h.ingester.compaction_service_state() == State::Running
                && h.ingester.owned_series_service_state() == State::Running
                && h.ingester.metrics_updater_service_state() == State::Running
                && h.ingester.metadata_purger_service_state() == State::Running
        })
        .await
    );

    assert!(
        poll_until(Duration::from_secs(2), || async {
            counter_value(&reg, "cortex_ingester_tsdb_compactions_triggered_total") > 0.0
        })
        .await
    );

    assert!(
        poll_until(Duration::from_secs(2), || async {
            gauge_value(&reg, "cortex_ingester_active_series", Some(("user", USER_ID))) == Some(1.0)
                && gauge_value(&reg, "cortex_ingester_owned_series", Some(("user", USER_ID)))
                    == Some(1.0)
        })
        .await
    );

    let (first_iv, std_iv) = h
        .ingester
        .compaction_service_interval(SystemTime::now(), &h.ring.zones());
    assert_eq!(first_iv, Duration::from_millis(100));
    assert_eq!(std_iv, Duration::from_millis(100));

    assert_eq!(h.ingester.state(), State::Starting);
    assert_eq!(h.ingester.lifecycler_service_state(), State::New);

    writer
        .write_sync(
            &h.ingester.ingest_cfg.kafka.topic,
            pid,
            USER_ID,
            &single_series_write_request("series_2", 1000, 10.0),
        )
        .await
        .unwrap();

    assert!(
        poll_until(Duration::from_secs(5), || async {
            h.ingester.state() == State::Running
        })
        .await
    );
    assert_eq!(h.ingester.lifecycler_service_state(), State::Running);

    assert!(
        poll_until(Duration::from_secs(2), || async {
            h.ring
                .get_instance_state(&h.ingester.cfg.ring.instance_id)
                .ok()
                == Some(InstanceState::Active)
        })
        .await
    );
    assert!(
        poll_until(Duration::from_secs(2), || async {
            h.watcher.partition_ring().partition_owner_ids(pid)
                == vec![h.ingester.cfg.ring.instance_id.clone()]
        })
        .await
    );

    h.cleanup().await;
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_QueryStream_IngestStorageReadConsistency
#[tokio::test]
async fn test_ingester_query_stream_ingest_storage_read_consistency() {
    let cases = [
        ("eventual read consistency", "eventual", None, 0usize),
        ("strong read consistency without offsets", "strong", None, 1),
        (
            "strong read consistency with offsets (v1 encoding)",
            "strong",
            Some("v1=0:-2"),
            0,
        ),
        (
            "strong read consistency with offsets (v2 encoding)",
            "strong",
            Some("v2=0/0:-2"),
            0,
        ),
    ];

    for (name, level, offsets_hdr, expected_series) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = default_ingester_test_config(tmp.path());
        let mut limits = default_limits_test_config();
        limits.ingest_storage_read_consistency = level.to_string();

        let h = create_test_ingester_with_ingest_storage(
            cfg,
            limits,
            None,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;

        let fail_fetch = Arc::new(AtomicBool::new(true));
        let ff_c = Arc::clone(&fail_fetch);
        *h.clusters[0].hooks.fetch.lock() = Some(Arc::new(move |req| {
            let ff = Arc::clone(&ff_c);
            Box::pin(async move {
                if ff.load(Ordering::Acquire) {
                    let mut resp = FetchResponse::default();
                    for ft in &req.topics {
                        let mut fr = FetchableTopicResponse::default();
                        fr.topic = ft.topic.clone();
                        for fp in &ft.partitions {
                            let mut pd = PartitionData::default();
                            pd.partition_index = fp.partition;
                            pd.error_code = 1;
                            fr.partitions.push(pd);
                        }
                        resp.responses.push(fr);
                    }
                    return HookAction::Respond(resp);
                }
                HookAction::PassThrough
            })
        }));

        h.ingester.start().await.unwrap();
        assert!(
            poll_until(Duration::from_secs(2), || async {
                h.ring.get_all_healthy(WRITE).map(|rs| rs.instances.len()).unwrap_or(0) == 1
            })
            .await
        );

        let writer = IngestWriter::from_config(&h.ingester.ingest_cfg.kafka, None).unwrap();
        let pid = h.ingester.ingest_partition_id;
        writer
            .write_sync(
                &h.ingester.ingest_cfg.kafka.topic,
                pid,
                USER_ID,
                &single_series_write_request("series_1", 1000, 10.0),
            )
            .await
            .unwrap();

        let mut md = MetadataMap::new();
        if let Some(hdr) = offsets_hdr {
            md.insert("__consistency_offsets__", hdr.parse().unwrap());
        }

        let ing_c = Arc::clone(&h.ingester);
        let issued = Arc::new(tokio::sync::Notify::new());
        let issued_c = Arc::clone(&issued);
        let qh = tokio::spawn(async move {
            issued_c.notify_one();
            run_test_query(
                &ing_c,
                &md,
                USER_ID,
                ingesterpb::MatchType::Equal,
                "__name__",
                "series_1",
            )
            .await
            .unwrap()
        });

        issued.notified().await;
        fail_fetch.store(false, Ordering::Release);
        let res = qh.await.unwrap();
        assert_eq!(res.len(), expected_series, "case {name}");

        h.cleanup().await;
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_PrepareShutdownHandler_IngestStorageSupport
#[tokio::test]
async fn test_ingester_prepare_shutdown_handler_ingest_storage_support() {
    for num_tokens in [512i64, 0] {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.ingester.ring.num_tokens = num_tokens;
        let reg = Registry::new();

        let h = create_test_ingester_with_ingest_storage(
            cfg,
            default_limits_test_config(),
            None,
            None,
            Duration::from_millis(10),
            Some(&reg),
        )
        .await;
        h.ingester.start().await.unwrap();

        assert!(
            poll_until(Duration::from_secs(2), || async {
                h.ring.get_all_healthy(WRITE).map(|rs| rs.instances.len()).unwrap_or(0) == 1
            })
            .await
        );
        let tok_cnt = get_instance_token_count(
            h.ring.kv_client().unwrap().as_ref(),
            INGEST_RING_KEY,
            &h.ingester.cfg.ring.instance_id,
        )
        .await;
        assert_eq!(tok_cnt, Some(num_tokens as usize));

        let router = ingester_router(Arc::clone(&h.ingester));

        // 1. DELETE is not allowed when ingest storage is enabled
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/ingester/prepare-shutdown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            gauge_value(&reg, "cortex_ingester_prepare_shutdown_requested", None),
            Some(0.0)
        );

        // 2. POST prepare-shutdown sets gauge and removes owner on shutdown
        assert_eq!(
            get_instance_state_in_ring(
                h.ring.kv_client().unwrap().as_ref(),
                INGEST_RING_KEY,
                &h.ingester.cfg.ring.instance_id
            )
            .await,
            Some(InstanceState::Active)
        );

        let resp = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/ingester/prepare-shutdown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            gauge_value(&reg, "cortex_ingester_prepare_shutdown_requested", None),
            Some(1.0)
        );

        assert_eq!(
            get_instance_state_in_ring(
                h.ring.kv_client().unwrap().as_ref(),
                INGEST_RING_KEY,
                &h.ingester.cfg.ring.instance_id
            )
            .await,
            Some(InstanceState::Active)
        );
        assert!(
            poll_until(Duration::from_secs(2), || async {
                h.watcher.partition_ring().partition_owner_ids(0)
                    == vec!["ingester-zone-a-0".to_string()]
            })
            .await
        );

        h.ingester.stop().await.unwrap();

        assert!(
            poll_until(Duration::from_secs(2), || async {
                h.watcher
                    .partition_ring()
                    .partition_owner_ids(0)
                    .is_empty()
            })
            .await
        );
        assert!(
            get_instance_state_in_ring(
                h.ring.kv_client().unwrap().as_ref(),
                INGEST_RING_KEY,
                &h.ingester.cfg.ring.instance_id
            )
            .await
            .is_none()
        );

        h.watcher_svc.stop_async();
        let _ = h.watcher_svc.await_terminated().await;
        h.ring_svc.stop_async();
        let _ = h.ring_svc.await_terminated().await;
        for c in h.clusters {
            c.close();
        }
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_PreparePartitionDownscaleHandler
#[tokio::test]
async fn test_ingester_prepare_partition_downscale_handler() {
    for num_tokens in [512i64, 0] {
        let setup = |min_owners: i64| async move {
            let tmp = tempfile::tempdir().unwrap();
            let mut cfg = default_ingester_test_config(tmp.path());
            cfg.ingester.ring.num_tokens = num_tokens;
            cfg.ingester.partition_ring.min_partition_owners_count = min_owners;
            cfg.ingester.partition_ring.min_partition_owners_duration = GoDuration::ZERO;

            let h = create_test_ingester_with_ingest_storage(
                cfg,
                default_limits_test_config(),
                None,
                None,
                Duration::from_millis(10),
                None,
            )
            .await;
            h.ingester.start().await.unwrap();
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.ring.get_all_healthy(WRITE).map(|rs| rs.instances.len()).unwrap_or(0) == 1
                })
                .await
            );
            let editor = PartitionRingEditor::new(PARTITION_RING_KEY, Arc::clone(&h.partition_kv));
            (tmp, h, editor)
        };

        // 1. POST switches partition to INACTIVE
        {
            let (_tmp, h, _ed) = setup(1).await;
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().active_partition_ids() == vec![0]
                })
                .await
            );
            let resp = ingester_router(Arc::clone(&h.ingester))
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().inactive_partition_ids() == vec![0]
                })
                .await
            );
            h.cleanup().await;
        }

        // 2. DELETE after POST switches partition back to ACTIVE
        {
            let (_tmp, h, _ed) = setup(1).await;
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().active_partition_ids() == vec![0]
                })
                .await
            );
            let router = ingester_router(Arc::clone(&h.ingester));
            let resp = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().inactive_partition_ids() == vec![0]
                })
                .await
            );
            let resp = router
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().active_partition_ids() == vec![0]
                })
                .await
            );
            h.cleanup().await;
        }

        // 3. POST rejected if partition is PENDING
        {
            let (_tmp, h, _ed) = setup(2).await;
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().pending_partition_ids() == vec![0]
                })
                .await
            );
            let resp = ingester_router(Arc::clone(&h.ingester))
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::CONFLICT);
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().pending_partition_ids() == vec![0]
                })
                .await
            );
            h.cleanup().await;
        }

        // 4. DELETE ignored if partition is PENDING
        {
            let (_tmp, h, _ed) = setup(2).await;
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().pending_partition_ids() == vec![0]
                })
                .await
            );
            let resp = ingester_router(Arc::clone(&h.ingester))
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().pending_partition_ids() == vec![0]
                })
                .await
            );
            h.cleanup().await;
        }

        // 5. POST rejected if partition state change is locked
        {
            let (_tmp, h, ed) = setup(1).await;
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().active_partition_ids() == vec![0]
                })
                .await
            );
            ed.set_partition_state_change_lock(0, true).await.unwrap();
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher
                        .partition_ring()
                        .partitions()
                        .into_iter()
                        .any(|p| p.id == 0 && p.state_change_locked)
                })
                .await
            );
            let resp = ingester_router(Arc::clone(&h.ingester))
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::CONFLICT);
            assert_eq!(h.watcher.partition_ring().active_partition_ids(), vec![0]);
            h.cleanup().await;
        }

        // 6. DELETE rejected if partition state change is locked
        {
            let (_tmp, h, ed) = setup(1).await;
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().active_partition_ids() == vec![0]
                })
                .await
            );
            let router = ingester_router(Arc::clone(&h.ingester));
            let resp = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher.partition_ring().inactive_partition_ids() == vec![0]
                })
                .await
            );
            ed.set_partition_state_change_lock(0, true).await.unwrap();
            assert!(
                poll_until(Duration::from_secs(5), || async {
                    h.watcher
                        .partition_ring()
                        .partitions()
                        .into_iter()
                        .any(|p| p.id == 0 && p.state_change_locked)
                })
                .await
            );
            let resp = router
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri("/ingester/prepare-partition-downscale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::CONFLICT);
            assert_eq!(h.watcher.partition_ring().inactive_partition_ids(), vec![0]);
            h.cleanup().await;
        }
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_ShouldNotCreatePartitionIfThereIsShutdownMarker
#[tokio::test]
async fn test_ingester_should_not_create_partition_if_there_is_shutdown_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_ingester_test_config(tmp.path());
    let polling_interval = Duration::from_millis(10);

    let h = create_test_ingester_with_ingest_storage(
        cfg,
        default_limits_test_config(),
        None,
        None,
        polling_interval,
        None,
    )
    .await;

    std::fs::create_dir_all(&h.ingester.blocks_cfg.tsdb.dir).unwrap();
    shutdownmarker::create(shutdownmarker::get_path(&h.ingester.blocks_cfg.tsdb.dir)).unwrap();

    h.ingester.start_async().await.unwrap();

    tokio::time::sleep(10 * polling_interval).await;

    assert_eq!(h.ingester.state(), State::Starting);
    assert!(h.watcher.partition_ring().partition_ids().is_empty());
    let pid = ingester_partition_id_from_instance_id(&h.ingester.cfg.ring.instance_id).unwrap();
    assert!(h
        .watcher
        .partition_ring()
        .partition_owner_ids(pid)
        .is_empty());

    h.cleanup().await;
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_compactionServiceInterval
#[tokio::test]
async fn test_ingester_compaction_service_interval() {
    let fake_now = parse_utc_system_time("2025-07-21T10:27:00Z");

    // 1. when starting without jittering enabled
    {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(1);
        let h = create_test_ingester_with_ingest_storage(
            cfg,
            default_limits_test_config(),
            None,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;
        h.ingester.set_head_compaction_interval_jitter_enabled(false);
        std::fs::create_dir_all(&h.ingester.blocks_cfg.tsdb.dir).unwrap();
        shutdownmarker::create(shutdownmarker::get_path(&h.ingester.blocks_cfg.tsdb.dir))
            .unwrap();

        h.ingester.start_async().await.unwrap();

        let (first, standard) = h.ingester.compaction_service_interval(fake_now, &[]);
        assert_eq!(first, Duration::from_secs(30));
        assert_eq!(standard, Duration::from_secs(30));
        h.cleanup().await;
    }

    // 2. when starting with jittering enabled
    {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(1);
        let h = create_test_ingester_with_ingest_storage(
            cfg,
            default_limits_test_config(),
            None,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;
        h.ingester.set_head_compaction_interval_jitter_enabled(true);
        std::fs::create_dir_all(&h.ingester.blocks_cfg.tsdb.dir).unwrap();
        shutdownmarker::create(shutdownmarker::get_path(&h.ingester.blocks_cfg.tsdb.dir))
            .unwrap();
        h.ingester.start_async().await.unwrap();

        let (first, standard) = h.ingester.compaction_service_interval(fake_now, &[]);
        assert!(first < Duration::from_secs(30));
        assert_eq!(standard, Duration::from_secs(30));
        h.cleanup().await;
    }

    // 3. zone 1 out of 2 with jittering and zone awareness enabled at a 20m interval
    {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(20);
        cfg.ingester.ring.zone_awareness_enabled = true;
        cfg.ingester.ring.instance_availability_zone = "zone-a".to_string();
        let h = create_test_ingester_with_ingest_storage(
            cfg,
            default_limits_test_config(),
            None,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;
        h.ingester.set_head_compaction_interval_jitter_enabled(true);

        let zones = vec!["zone-a".to_string(), "zone-b".to_string()];
        let (first, standard) = h.ingester.compaction_service_interval(fake_now, &zones);
        assert!(first >= Duration::from_secs(13 * 60));
        assert!(first <= Duration::from_secs(18 * 60));
        assert_eq!(standard, Duration::from_secs(20 * 60));
        h.cleanup().await;
    }

    // 4. zone 2 out of 2 with jittering and zone awareness enabled at a 20m interval
    {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(20);
        cfg.ingester.ring.zone_awareness_enabled = true;
        cfg.ingester.ring.instance_availability_zone = "zone-b".to_string();
        let h = create_test_ingester_with_ingest_storage(
            cfg,
            default_limits_test_config(),
            None,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;
        h.ingester.set_head_compaction_interval_jitter_enabled(true);

        let zones = vec!["zone-a".to_string(), "zone-b".to_string()];
        let (first, standard) = h.ingester.compaction_service_interval(fake_now, &zones);
        assert!(first >= Duration::from_secs(3 * 60));
        assert!(first <= Duration::from_secs(8 * 60));
        assert_eq!(standard, Duration::from_secs(20 * 60));
        h.cleanup().await;
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_timeToNextZoneAwareCompaction
#[tokio::test]
async fn test_ingester_time_to_next_zone_aware_compaction() {
    let default_interval = Duration::from_secs(15 * 60);
    let fake_now = parse_utc_system_time("2025-07-21T10:27:00Z");

    let cases: Vec<(&str, bool, &str, Vec<&str>, Duration)> = vec![
        (
            "zone awareness disabled",
            false,
            "zone-a",
            vec!["zone-a", "zone-b"],
            default_interval,
        ),
        (
            "empty instance zone",
            true,
            "",
            vec!["zone-a", "zone-b"],
            default_interval,
        ),
        (
            "zone awareness disabled and empty instance zone",
            false,
            "",
            vec!["zone-a", "zone-b"],
            default_interval,
        ),
        ("single zone", true, "zone-a", vec!["zone-a"], default_interval),
        ("empty zones list", true, "zone-a", vec![], default_interval),
        (
            "current zone not found in zones list",
            true,
            "zone-missing",
            vec!["zone-a", "zone-b"],
            default_interval,
        ),
        (
            "two zones - first zone",
            true,
            "zone-a",
            vec!["zone-a", "zone-b"],
            Duration::from_secs(3 * 60),
        ),
        (
            "two zones - second zone",
            true,
            "zone-b",
            vec!["zone-a", "zone-b"],
            Duration::from_secs(10 * 60 + 30),
        ),
        (
            "three zones - first zone",
            true,
            "zone-a",
            vec!["zone-a", "zone-b", "zone-c"],
            Duration::from_secs(3 * 60),
        ),
        (
            "three zones - middle zone",
            true,
            "zone-b",
            vec!["zone-a", "zone-b", "zone-c"],
            Duration::from_secs(8 * 60),
        ),
        (
            "three zones - last zone",
            true,
            "zone-c",
            vec!["zone-a", "zone-b", "zone-c"],
            Duration::from_secs(13 * 60),
        ),
        (
            "many zones - verify staggering by picking one random",
            true,
            "zone-c",
            vec!["zone-a", "zone-b", "zone-c", "zone-d", "zone-e"],
            Duration::from_secs(9 * 60),
        ),
    ];

    for (name, za_enabled, inst_zone, zones, expected) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.ingester.ring.zone_awareness_enabled = za_enabled;
        cfg.ingester.ring.instance_availability_zone = inst_zone.to_string();
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_mins(15);

        let h = create_test_ingester_with_ingest_storage(
            cfg,
            default_limits_test_config(),
            None,
            None,
            Duration::from_millis(10),
            None,
        )
        .await;
        let z_strs: Vec<String> = zones.into_iter().map(ToString::to_string).collect();
        let actual = h.ingester.time_to_next_zone_aware_compaction(fake_now, &z_strs);
        assert_eq!(actual, expected, "case {name}");
        h.cleanup().await;
    }
}

// Upstream: pkg/ingester/ingester_ingest_storage_test.go::TestIngester_timeUntilCompaction
#[test]
fn test_ingester_time_until_compaction() {
    let fake_now = parse_utc_system_time("2025-07-21T10:00:00Z");
    let iv = Duration::from_secs(15 * 60);
    let offset_7m30s = Duration::from_secs(7 * 60 + 30);

    let cases = [
        (
            "no offset zone and starting at :00 gives us interval as-is",
            fake_now,
            iv,
            Duration::ZERO,
            Duration::from_secs(15 * 60),
        ),
        (
            "with offset zone of 7m30s and starting at :00 gives interval of 7m30s",
            fake_now,
            iv,
            offset_7m30s,
            offset_7m30s,
        ),
        (
            "with offset zone of 15m and equal to interval gives us offset interval as-is",
            fake_now,
            iv,
            iv,
            iv,
        ),
        (
            "with offset zone of 7m30s and starting at :10 gives us offset 12m30s",
            fake_now + Duration::from_secs(10 * 60),
            iv,
            offset_7m30s,
            Duration::from_secs(12 * 60 + 30),
        ),
        (
            "with offset zone of 7m30s, current time around the second interval",
            fake_now + Duration::from_secs(26 * 60),
            iv,
            offset_7m30s,
            Duration::from_secs(11 * 60 + 30),
        ),
        (
            "with offset zone of 7m30s, current time around the third interval",
            fake_now + Duration::from_secs(39 * 60 + 13),
            iv,
            offset_7m30s,
            Duration::from_secs(13 * 60 + 17),
        ),
        (
            "with offset zone of 7m30s, current time around the fourth interval",
            fake_now + Duration::from_secs(52 * 60 + 29),
            iv,
            offset_7m30s,
            Duration::from_secs(1),
        ),
        (
            "with offset zone of 7m30s, current time around past the fourth interval",
            fake_now + Duration::from_secs(58 * 60 + 40),
            iv,
            offset_7m30s,
            Duration::from_secs(8 * 60 + 50),
        ),
    ];

    for (name, now, interval, zone_offset, expected) in cases {
        let actual = time_until_compaction(now, interval, zone_offset);
        assert_eq!(actual, expected, "case {name}");
    }
}
