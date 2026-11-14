mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::stream::BoxStream;
use mimir_common::services::{start_and_await_running, stop_and_await_terminated, ServiceContext};
use mimir_config::{
    BlocksStorageConfig, BucketConfig, GoDuration, IngestStorageConfig, IngesterConfig, InstanceLimits,
    LimitsConfig, Overrides, RuntimeConfigConfig, RuntimeConfigManager, StringSliceCSV,
};
use mimir_ingester::errors::IngesterError;
use mimir_ingester::limiter::{IngesterRingLimiterStrategy, Limiter, NoopRingLimiterStrategy};
use mimir_ingester::metrics::IngesterMetrics;
use mimir_ingester::owned_series::{
    secondary_tsdb_hash_for_user, OwnedSeriesIngesterRingStrategy,
    OwnedSeriesPartitionRingStrategy, OwnedSeriesRingStrategy, OwnedSeriesService,
    RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION,
    RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED,
    RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED, RECOMPUTE_OWNED_SERIES_REASON_NEW_USER,
    RECOMPUTE_OWNED_SERIES_REASON_RING_CHANGED, RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED,
};
use mimir_ingester::user_tsdb::{next_forced_head_compaction_range, TsdbState, UserTsdb};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState, PartitionRingDesc, PartitionState};
use mimir_proto::sharding::shard_by_all_labels;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{cas_typed, KvClient};
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionRing, PartitionRingDescExt, PartitionRingOptions,
    PartitionRingWatcher,
};
use mimir_ring::prng::{shuffle_shard_seed, GoRand};
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{ring_desc_codec, Ring, RingSettings};
use mimir_storage::bucket::BucketClient;
use mimir_storage::ingest::{
    read_offset_catalogue_from_file, OffsetCatalogue, OffsetCatalogueError,
    OffsetCatalogueMetrics, OffsetWatermark, PartitionOffsets, OFFSET_CATALOGUE_FILENAME,
    OFFSET_CATALOGUE_VERSION,
};
use mimir_storage::shipper::{
    block_metas_from_oldest, download_meta, read_shipped_blocks, BlockMeta,
    BlockMetaCompaction, BlockStats, Shipper, ShipperMetrics, SourceType, ThanosMeta,
    CHUNKS_DIRNAME, OUT_OF_ORDER_EXTERNAL_LABEL, OUT_OF_ORDER_EXTERNAL_LABEL_VALUE,
};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use prometheus::Registry;
use tempfile::TempDir;
use ulid::Ulid;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value, mock_write_request,
    prepare_ingester_with_kv_and_overrides, start_and_wait_healthy,
};


fn add_ingester_to_desc(
    d: &mut Desc,
    id: &str,
    addr: &str,
    zone: &str,
    tokens: Vec<u32>,
    state: InstanceState,
) {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    d.ingesters.insert(
        id.to_string(),
        InstanceDesc {
            id: id.to_string(),
            addr: addr.to_string(),
            zone: zone.to_string(),
            tokens,
            state: state as i32,
            timestamp: now_secs,
            registered_timestamp: now_secs,
            read_only: false,
            read_only_updated_timestamp: 0,
            versions: HashMap::new(),
        },
    );
}

fn mk_block_meta(
    ulid: Ulid,
    min_time: i64,
    max_time: i64,
    num_samples: u64,
    compaction: BlockMetaCompaction,
    thanos: ThanosMeta,
) -> BlockMeta {
    BlockMeta {
        ulid,
        min_time,
        max_time,
        stats: BlockStats {
            num_samples,
            ..Default::default()
        },
        compaction,
        version: 1,
        out_of_order: false,
        thanos,
    }
}

fn create_block_on_disk(blocks_dir: &Path, id: Ulid, meta: &BlockMeta) {
    let bdir = blocks_dir.join(id.to_string());
    fs::create_dir_all(bdir.join(CHUNKS_DIRNAME)).unwrap();
    meta.write_to_dir(&bdir).unwrap();
    fs::write(bdir.join("index"), b"").unwrap();
}

// ============================================================================
// pkg/ingester/offset_catalogue_test.go (1 test)
// ============================================================================

// Upstream: pkg/ingester/offset_catalogue_test.go::TestOffsetCatalogue
#[test]
fn test_offset_catalogue() {
    let user_id = "user-1";

    // 1) no existing catalogue file
    {
        let dir = TempDir::new().unwrap();
        let id = Ulid::from_parts(1, 0);
        create_block_on_disk(
            dir.path(),
            id,
            &mk_block_meta(id, 0, 0, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
        );
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        c.sync(&PartitionOffsets::new_multi_cluster(vec![100, 250, -1]))
            .unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(data.version, OFFSET_CATALOGUE_VERSION);
        let mut expected = BTreeMap::new();
        expected.insert(
            0,
            OffsetWatermark {
                partition: 1,
                offset: 100,
            },
        );
        expected.insert(
            1,
            OffsetWatermark {
                partition: 1,
                offset: 250,
            },
        );
        assert_eq!(data.data.get(&id.to_string()), Some(&expected));
    }

    // 2) preserves existing entries
    {
        let dir = TempDir::new().unwrap();
        let id1 = Ulid::from_parts(1, 0);
        let id2 = Ulid::from_parts(2, 0);
        create_block_on_disk(
            dir.path(),
            id1,
            &mk_block_meta(id1, 0, 0, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
        );
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        c.sync(&PartitionOffsets::new_multi_cluster(vec![50, 300]))
            .unwrap();

        create_block_on_disk(
            dir.path(),
            id2,
            &mk_block_meta(id2, 0, 0, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
        );
        c.sync(&PartitionOffsets::new_multi_cluster(vec![200, 400]))
            .unwrap();

        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(
            data.data.get(&id1.to_string()).unwrap()[&0],
            OffsetWatermark {
                partition: 1,
                offset: 50
            }
        );
        assert_eq!(
            data.data.get(&id1.to_string()).unwrap()[&1],
            OffsetWatermark {
                partition: 1,
                offset: 300
            }
        );
        assert_eq!(
            data.data.get(&id2.to_string()).unwrap()[&0],
            OffsetWatermark {
                partition: 1,
                offset: 200
            }
        );
        assert_eq!(
            data.data.get(&id2.to_string()).unwrap()[&1],
            OffsetWatermark {
                partition: 1,
                offset: 400
            }
        );
    }

    // 3) drops deleted blocks
    {
        let dir = TempDir::new().unwrap();
        let id1 = Ulid::from_parts(1, 0);
        let id2 = Ulid::from_parts(2, 0);
        for id in [id1, id2] {
            create_block_on_disk(
                dir.path(),
                id,
                &mk_block_meta(id, 0, 0, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
            );
        }
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        c.sync(&PartitionOffsets::new_single_cluster(10)).unwrap();
        fs::remove_dir_all(dir.path().join(id1.to_string())).unwrap();
        c.sync(&PartitionOffsets::new_single_cluster(20)).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert!(!data.data.contains_key(&id1.to_string()));
        assert!(data.data.contains_key(&id2.to_string()));
    }

    // 4) no blocks
    {
        let dir = TempDir::new().unwrap();
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        c.sync(&PartitionOffsets::new_single_cluster(42)).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert!(data.data.is_empty());
    }

    // 5) rebuilds older catalogue
    {
        let dir = TempDir::new().unwrap();
        let id = Ulid::from_parts(1, 0);
        create_block_on_disk(
            dir.path(),
            id,
            &mk_block_meta(id, 0, 0, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
        );
        let content = format!(
            r#"{{"version":1,"updated_at":0,"data":{{"{}":{{"partition":1,"offset":50}}}}}}"#,
            id
        );
        fs::write(dir.path().join(OFFSET_CATALOGUE_FILENAME), content).unwrap();
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        c.sync(&PartitionOffsets::new_multi_cluster(vec![100, 250]))
            .unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(data.version, OFFSET_CATALOGUE_VERSION);
        assert_eq!(
            data.data.get(&id.to_string()).unwrap()[&0],
            OffsetWatermark {
                partition: 1,
                offset: 100
            }
        );
        assert_eq!(
            data.data.get(&id.to_string()).unwrap()[&1],
            OffsetWatermark {
                partition: 1,
                offset: 250
            }
        );
    }

    // 6) skips unset offsets
    {
        let dir = TempDir::new().unwrap();
        let id = Ulid::from_parts(1, 0);
        create_block_on_disk(
            dir.path(),
            id,
            &mk_block_meta(id, 0, 0, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
        );
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        assert!(matches!(
            c.sync(&PartitionOffsets::new_multi_cluster(vec![-1, -1])),
            Err(OffsetCatalogueError::EmptyOffsets)
        ));
        assert!(read_offset_catalogue_from_file(dir.path()).is_err());

        c.sync(&PartitionOffsets::new_multi_cluster(vec![50, -1]))
            .unwrap();
        let d1 = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(d1.data[&id.to_string()].len(), 1);
        assert_eq!(
            d1.data[&id.to_string()][&0],
            OffsetWatermark {
                partition: 1,
                offset: 50
            }
        );

        c.sync(&PartitionOffsets::new_multi_cluster(vec![200, 400]))
            .unwrap();
        let d2 = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(
            d2.data[&id.to_string()][&0],
            OffsetWatermark {
                partition: 1,
                offset: 50
            }
        );
        assert_eq!(
            d2.data[&id.to_string()][&1],
            OffsetWatermark {
                partition: 1,
                offset: 400
            }
        );
    }

    // 7) overwrites version mismatch
    {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join(OFFSET_CATALOGUE_FILENAME),
            r#"{"version":99,"updated_at":0,"data":{}}"#,
        )
        .unwrap();
        assert!(matches!(
            read_offset_catalogue_from_file(dir.path()),
            Err(OffsetCatalogueError::UnexpectedVersion { .. })
        ));
        let c = OffsetCatalogue::new(dir.path(), user_id, 1, OffsetCatalogueMetrics::new(None));
        c.sync(&PartitionOffsets::new_single_cluster(42)).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert!(data.data.is_empty());
    }
}

// ============================================================================
// pkg/ingester/ooo_chunk_file_sequence_reset_test.go (1 test)
// ============================================================================

// Upstream: pkg/ingester/ooo_chunk_file_sequence_reset_test.go::TestOOOSamplesAfterChunkFileSequenceReset
#[test]
fn test_ooo_samples_after_chunk_file_sequence_reset() {
    const MINUTE: i64 = 60_000;
    let want_minutes = [100i64, 101, 102, 103, 104];

    for head_gc_first in [true, false] {
        let dir = TempDir::new().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 2 * 3600 * 1000;
        opts.head.ooo_cap_max = 4;

        let lbls = [("__name__", "test_metric")];
        let append_minutes = |db: &Tsdb, mins: &[i64]| {
            let mut app = db.appender();
            let mut r = None;
            for &m in mins {
                let id = app.append_float(r, &lbls, m * MINUTE, m as f64).unwrap();
                r = Some(id);
            }
            app.commit().unwrap();
        };
        let query_minutes = |db: &Tsdb| -> Vec<i64> {
            let q = db.querier(want_minutes[0] * MINUTE, want_minutes[4] * MINUTE);
            let m = LabelMatcher::new(MatchType::Equal, "__name__", "test_metric").unwrap();
            let series = q.select(false, &[m]).unwrap();
            let mut got = Vec::new();
            for s in series {
                for sample in s.samples {
                    if matches!(sample.value, SampleValue::Float(_)) {
                        got.push(sample.t / MINUTE);
                    }
                }
            }
            got
        };
        let chunk_files = || -> usize {
            let p = dir.path().join("chunks_head");
            if !p.exists() {
                return 0;
            }
            fs::read_dir(p).unwrap().count()
        };

        // First run: m-map an OOO chunk by compacting the OOO head.
        {
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            append_minutes(&db, &[150]);
            append_minutes(&db, &[90]);
            db.compact_ooo_head().unwrap();
            assert_eq!(chunk_files(), 1);
            db.close().unwrap();
        }

        // Second run: restart deletes compacted chunk file on OOO compaction, then append new OOO samples.
        {
            let db = Tsdb::open(dir.path(), opts).unwrap();
            db.compact_ooo_head().unwrap();
            assert_eq!(chunk_files(), 0);

            append_minutes(&db, &want_minutes);
            assert!(chunk_files() > 0);
            assert_eq!(query_minutes(&db), want_minutes);

            if head_gc_first {
                let h = db.head();
                let _ = db.compact_head_range(h.min_time(), h.max_time().saturating_add(1));
            }
            db.compact_ooo_head().unwrap();
            assert_eq!(query_minutes(&db), want_minutes);
            db.close().unwrap();
        }
    }
}

// ============================================================================
// pkg/ingester/owned_series_test.go (7 tests)
// ============================================================================

const OWNED_SERVICE_SERIES_COUNT: usize = 10;
const OWNED_SERVICE_TEST_USER_LIMIT: usize = 10_000;

fn generate_series_with_tokens_at(test_user: &str, start_ms: i64) -> (Vec<(Labels, i64)>, Vec<u32>) {
    let mut series = Vec::with_capacity(OWNED_SERVICE_SERIES_COUNT);
    let mut tokens = Vec::with_capacity(OWNED_SERVICE_SERIES_COUNT);
    for idx in 0..OWNED_SERVICE_SERIES_COUNT {
        let lbl_name = format!("lbl_{idx:05}");
        let lbls = Labels::from_pairs([("__name__", "test"), (lbl_name.as_str(), "value")]);
        let tok = shard_by_all_labels(test_user, lbls.iter());
        series.push((lbls, start_ms + idx as i64));
        tokens.push(tok);
    }
    (series, tokens)
}

fn user_token(user: &str, zone: &str, skip: usize) -> u32 {
    let mut rng = GoRand::new(shuffle_shard_seed(user, zone));
    for _ in 0..skip {
        let _ = rng.uint32();
    }
    rng.uint32()
}

fn write_runtime_limits_file(
    path: &Path,
    overrides: &BTreeMap<String, (i64, i64)>, // user -> (max_global_series, shard_size)
    is_partition_ring: bool,
) {
    let mut yaml = String::from("overrides:\n");
    for (uid, (max_series, shard_size)) in overrides {
        yaml.push_str(&format!("  \"{uid}\":\n"));
        yaml.push_str(&format!("    max_global_series_per_user: {max_series}\n"));
        if is_partition_ring {
            yaml.push_str(&format!(
                "    ingestion_partitions_tenant_shard_size: {shard_size}\n"
            ));
        } else {
            yaml.push_str(&format!("    ingestion_tenant_shard_size: {shard_size}\n"));
        }
    }
    fs::write(path, yaml).unwrap();
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesServiceWithIngesterRing
#[tokio::test]
async fn test_owned_series_service_with_ingester_ring() {
    let test_user = "test-user";
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let (series_to_write, mut series_tokens) = generate_series_with_tokens_at(test_user, now_ms);
    series_tokens.sort_unstable();
    for w in series_tokens.windows(2) {
        assert!(w[1] > w[0]);
    }

    #[derive(Clone, Copy)]
    enum Case {
        EmptyIngester,
        NewUserTrigger,
        NewUserWalReplay,
        Shard1ScaleUpDownNoMove,
        Shard1ScaleUpDownMove,
        Shard0ScaleUpDown,
        AddPendingIngester,
        Shard0ToIngesterCount,
        ShardLtIngestersUpDown,
        ShardLtIngestersStartOther,
        SeriesLimitUpDown,
        LimitAndShardTandem,
        EarlyCompactionTrigger,
        PreviousRingCheckFailed,
    }

    let cases = [
        (Case::EmptyIngester, 0, false),
        (Case::NewUserTrigger, 0, false),
        (Case::NewUserWalReplay, 0, false),
        (Case::Shard1ScaleUpDownNoMove, 1, false),
        (Case::Shard1ScaleUpDownMove, 1, true),
        (Case::Shard0ScaleUpDown, 0, false),
        (Case::AddPendingIngester, 0, false),
        (Case::Shard0ToIngesterCount, 0, false),
        (Case::ShardLtIngestersUpDown, 1, false),
        (Case::ShardLtIngestersStartOther, 1, true),
        (Case::SeriesLimitUpDown, 0, false),
        (Case::LimitAndShardTandem, 1, false),
        (Case::EarlyCompactionTrigger, 0, false),
        (Case::PreviousRingCheckFailed, 0, false),
    ];

    for (case, init_shard, swap_shard_order) in cases {
        let dir = TempDir::new().unwrap();
        let rt_file = dir.path().join("runtime.yaml");
        let mut rt_map = BTreeMap::new();
        if !matches!(case, Case::EmptyIngester) {
            rt_map.insert(
                test_user.to_string(),
                (OWNED_SERVICE_TEST_USER_LIMIT as i64, init_shard),
            );
        }
        write_runtime_limits_file(&rt_file, &rt_map, false);

        let rt_cfg = RuntimeConfigConfig {
            file: StringSliceCSV(vec![rt_file.to_string_lossy().to_string()]),
            period: GoDuration::from_secs(3600),
            ..Default::default()
        };
        let rt_mgr = Arc::new(
            RuntimeConfigManager::new(
                rt_cfg,
                default_limits_test_config(),
                InstanceLimits::default(),
            )
            .unwrap(),
        );
        let overrides = rt_mgr.overrides();

        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

        let mut cfg = IngesterConfig::default();
        cfg.ring.instance_id = "first-ingester".to_string();
        cfg.ring.instance_addr = "localhost".to_string();
        cfg.ring.num_tokens = (OWNED_SERVICE_SERIES_COUNT / 2 + 1) as i64;
        cfg.ring.zone_awareness_enabled = true;
        cfg.ring.instance_availability_zone = "zone".to_string();
        cfg.ring.replication_factor = 1;
        cfg.ring.heartbeat_period = GoDuration::from_millis(50);
        cfg.ring.min_ready_duration = GoDuration::from_millis(10);
        cfg.ring.final_sleep = GoDuration(0);
        cfg.ring.unregister_on_shutdown = false;
        cfg.ring.kvstore.store = "inmemory".to_string();
        cfg.active_series.active_series_metrics_enabled = true;

        let mut blocks_cfg = BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.path().join("tsdb").to_string_lossy().to_string();
        blocks_cfg.tsdb.ship_interval = GoDuration(0);

        let skip_first = if swap_shard_order { 1 } else { 0 };
        let mut first_tokens: Vec<u32> = series_tokens[..OWNED_SERVICE_SERIES_COUNT / 2]
            .iter()
            .map(|t| t + 1)
            .collect();
        first_tokens.push(user_token(test_user, "zone", skip_first) + 1);
        first_tokens.sort_unstable();

        cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
            add_ingester_to_desc(&mut d, "first-ingester", "localhost", "zone", first_tokens.clone(), InstanceState::Active);
            Ok(Some((d, true)))
    })
        .await
        .unwrap();

        let (ing, ring) = prepare_ingester_with_kv_and_overrides(
            cfg.clone(),
            blocks_cfg.clone(),
            IngestStorageConfig::default(),
            overrides.clone(),
            Arc::clone(&kv),
            None,
        );
        start_and_wait_healthy(&ing).await;

        let mk_oss = |ing_ref: &Arc<mimir_ingester::Ingester>, ring_ref: &Arc<Ring>| {
            let strat = Arc::new(OwnedSeriesIngesterRingStrategy::new(
                "first-ingester",
                Arc::clone(ring_ref),
                ing_ref.overrides.clone(),
            ));
            let i_u = Arc::clone(ing_ref);
            let i_g = Arc::clone(ing_ref);
            OwnedSeriesService::new(
                Duration::from_secs(600),
                strat,
                None,
                Arc::new(move || i_u.get_tsdb_users()),
                Arc::new(move |u| i_g.get_tsdb(u)),
            )
        };
        let oss = mk_oss(&ing, &ring);

        let push_all = |ing_ref: Arc<mimir_ingester::Ingester>,
                        items: Vec<(Labels, i64)>| async move {
            for (lbls, ts) in items {
                ing_ref
                    .push_grpc(test_user, &mock_write_request(&lbls, 0.0, ts))
                    .await
                    .unwrap();
            }
        };

        let register_second = || async {
            let skip_second = if swap_shard_order { 0 } else { 1 };
            let mut toks: Vec<u32> = series_tokens[OWNED_SERVICE_SERIES_COUNT / 2..]
                .iter()
                .map(|t| t + 1)
                .collect();
            toks.push(user_token(test_user, "zone", skip_second) + 1);
            toks.sort_unstable();
            cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
                add_ingester_to_desc(&mut d, "second-ingester", "localhost", "zone", toks.clone(), InstanceState::Active);
                Ok(Some((d, true)))
    })
            .await
            .unwrap();
            // Wait for ring watcher to observe second-ingester
            let dl = tokio::time::Instant::now() + Duration::from_secs(2);
            while ring.instances_count() < 2 && tokio::time::Instant::now() < dl {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(ring.instances_count(), 2);
        };

        let remove_second = || async {
            cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
                d.ingesters.remove("second-ingester");
                Ok(Some((d, true)))
    })
            .await
            .unwrap();
            let dl = tokio::time::Instant::now() + Duration::from_secs(2);
            while ring.instances_count() > 1 && tokio::time::Instant::now() < dl {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(ring.instances_count(), 1);
        };

        match case {
            Case::EmptyIngester => {
                assert_eq!(oss.update_all_tenants(false), 0);
            }
            Case::NewUserTrigger => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                let db = ing.get_tsdb(test_user).unwrap();
                let os = db.owned_series_state();
                assert_eq!(
                    (os.owned_series_count, os.shard_size, os.local_series_limit),
                    (OWNED_SERVICE_SERIES_COUNT, 0, OWNED_SERVICE_TEST_USER_LIMIT)
                );
                assert_eq!(db.active_series.active().0, OWNED_SERVICE_SERIES_COUNT);
                assert_eq!(
                    db.peek_reason_for_recompute_owned_series().as_deref(),
                    Some(RECOMPUTE_OWNED_SERIES_REASON_NEW_USER)
                );
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_NEW_USER]
                );
                assert!(db.peek_reason_for_recompute_owned_series().is_none());
                assert_eq!(oss.update_all_tenants(false), 0);
            }
            Case::NewUserWalReplay => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                ing.stop().await.unwrap();

                let (ing2, _) = prepare_ingester_with_kv_and_overrides(
                    cfg,
                    blocks_cfg,
                    IngestStorageConfig::default(),
                    overrides.clone(),
                    Arc::clone(&kv),
                    None,
                );
                start_and_wait_healthy(&ing2).await;
                let db2 = ing2.get_tsdb(test_user).unwrap();
                let os2 = db2.owned_series_state();
                assert_eq!(
                    (os2.owned_series_count, os2.shard_size, os2.local_series_limit),
                    (OWNED_SERVICE_SERIES_COUNT, 0, OWNED_SERVICE_TEST_USER_LIMIT)
                );
                assert_eq!(
                    db2.peek_reason_for_recompute_owned_series().as_deref(),
                    Some(RECOMPUTE_OWNED_SERIES_REASON_NEW_USER)
                );
                assert_eq!(db2.active_series.active().0, 0);
                let _ = ing2.stop().await;
                continue;
            }
            Case::Shard1ScaleUpDownNoMove => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;
                assert_eq!(oss.update_all_tenants(false), 0);
                assert_eq!(oss.update_all_tenants(true), 0);
                remove_second().await;
                assert_eq!(oss.update_all_tenants(false), 0);
                assert_eq!(oss.update_all_tenants(true), 0);
                let db = ing.get_tsdb(test_user).unwrap();
                assert_eq!(db.owned_series_state().owned_series_count, OWNED_SERVICE_SERIES_COUNT);
            }
            Case::Shard1ScaleUpDownMove => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;
                assert_eq!(oss.update_all_tenants(false), 0);
                assert_eq!(oss.update_all_tenants(true), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_RING_CHANGED]
                );
                let db = ing.get_tsdb(test_user).unwrap();
                assert_eq!(db.owned_series_state().owned_series_count, 0);
                assert_eq!(db.active_series.active().0, 0);

                remove_second().await;
                assert_eq!(oss.update_all_tenants(false), 0);
                assert_eq!(oss.update_all_tenants(true), 1);
                assert_eq!(db.owned_series_state().owned_series_count, OWNED_SERVICE_SERIES_COUNT);
                assert_eq!(db.active_series.active().0, 0);
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(db.active_series.active().0, OWNED_SERVICE_SERIES_COUNT);
            }
            Case::Shard0ScaleUpDown => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;
                assert_eq!(oss.update_all_tenants(true), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED]
                );
                let db = ing.get_tsdb(test_user).unwrap();
                assert_eq!(
                    db.owned_series_state().owned_series_count,
                    OWNED_SERVICE_SERIES_COUNT / 2
                );
                assert_eq!(
                    db.owned_series_state().local_series_limit,
                    OWNED_SERVICE_TEST_USER_LIMIT / 2
                );
                remove_second().await;
                assert_eq!(oss.update_all_tenants(true), 1);
                assert_eq!(db.owned_series_state().owned_series_count, OWNED_SERVICE_SERIES_COUNT);
            }
            Case::AddPendingIngester => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
                    add_ingester_to_desc(&mut d, "second-ingester", "localhost", "zone", Vec::new(), InstanceState::Pending);
                    Ok(Some((d, true)))
    })
                .await
                .unwrap();
                let dl = tokio::time::Instant::now() + Duration::from_secs(2);
                while ring.instances_count() < 2 && tokio::time::Instant::now() < dl {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert_eq!(oss.update_all_tenants(true), 0);
            }
            Case::Shard0ToIngesterCount => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;
                assert_eq!(oss.update_all_tenants(true), 1);

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 2),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();

                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED]
                );
                let db = ing.get_tsdb(test_user).unwrap();
                assert_eq!(db.owned_series_state().shard_size, 2);
            }
            Case::ShardLtIngestersUpDown => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 2),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();

                assert_eq!(oss.update_all_tenants(false), 1);
                let db = ing.get_tsdb(test_user).unwrap();
                assert_eq!(
                    db.owned_series_state().owned_series_count,
                    OWNED_SERVICE_SERIES_COUNT / 2
                );

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 1),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(db.owned_series_state().owned_series_count, OWNED_SERVICE_SERIES_COUNT);
            }
            Case::ShardLtIngestersStartOther => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;
                assert_eq!(oss.update_all_tenants(true), 1);
                let db = ing.get_tsdb(test_user).unwrap();
                assert_eq!(db.owned_series_state().owned_series_count, 0);

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 2),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    db.owned_series_state().owned_series_count,
                    OWNED_SERVICE_SERIES_COUNT / 2
                );

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 1),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(db.owned_series_state().owned_series_count, 0);
            }
            Case::SeriesLimitUpDown => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                let db = ing.get_tsdb(test_user).unwrap();

                rt_map.insert(
                    test_user.to_string(),
                    ((OWNED_SERVICE_TEST_USER_LIMIT * 2) as i64, 0),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED]
                );
                assert_eq!(
                    db.owned_series_state().local_series_limit,
                    OWNED_SERVICE_TEST_USER_LIMIT * 2
                );

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 0),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    db.owned_series_state().local_series_limit,
                    OWNED_SERVICE_TEST_USER_LIMIT
                );
            }
            Case::LimitAndShardTandem => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                register_second().await;
                let db = ing.get_tsdb(test_user).unwrap();

                rt_map.insert(
                    test_user.to_string(),
                    ((OWNED_SERVICE_TEST_USER_LIMIT * 2) as i64, 2),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED]
                );
                assert_eq!(
                    (
                        db.owned_series_state().owned_series_count,
                        db.owned_series_state().shard_size,
                        db.owned_series_state().local_series_limit
                    ),
                    (
                        OWNED_SERVICE_SERIES_COUNT / 2,
                        2,
                        OWNED_SERVICE_TEST_USER_LIMIT
                    )
                );

                rt_map.insert(
                    test_user.to_string(),
                    (OWNED_SERVICE_TEST_USER_LIMIT as i64, 1),
                );
                write_runtime_limits_file(&rt_file, &rt_map, false);
                rt_mgr.reload().unwrap();
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    (
                        db.owned_series_state().owned_series_count,
                        db.owned_series_state().shard_size,
                        db.owned_series_state().local_series_limit
                    ),
                    (OWNED_SERVICE_SERIES_COUNT, 1, OWNED_SERVICE_TEST_USER_LIMIT)
                );
            }
            Case::EarlyCompactionTrigger => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                let db = ing.get_tsdb(test_user).unwrap();
                let max_before = db.db.head().max_time();
                ing.compact_blocks(true, max_before + 60_000, None).await;
                assert_eq!(db.db.head().num_series(), 0);
                assert_eq!(
                    db.peek_reason_for_recompute_owned_series().as_deref(),
                    Some(RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION)
                );
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION]
                );
                assert_eq!(db.owned_series_state().owned_series_count, 0);
                assert_eq!(db.active_series.active().0, OWNED_SERVICE_SERIES_COUNT);

                let (next_series, _) = generate_series_with_tokens_at(test_user, max_before + 1);
                push_all(Arc::clone(&ing), next_series).await;
                assert_eq!(db.active_series.active().0, OWNED_SERVICE_SERIES_COUNT);
            }
            Case::PreviousRingCheckFailed => {
                push_all(Arc::clone(&ing), series_to_write.clone()).await;
                assert_eq!(oss.update_all_tenants(false), 1);
                let db = ing.get_tsdb(test_user).unwrap();
                db.trigger_recompute_owned_series(
                    RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED,
                );
                assert_eq!(oss.update_all_tenants(false), 1);
                assert_eq!(
                    oss.last_update_reasons(),
                    vec![RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED]
                );
            }
        }
        let _ = ing.stop().await;
    }
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesPartitionsTestUserShuffleSharding
#[test]
fn test_owned_series_partitions_test_user_shuffle_sharding() {
    let user = "test-user-123";
    let (_, tokens) = generate_series_with_tokens_at(user, 1000);

    type ShardCase<'a> = (&'a [i32], usize, &'a [i32], &'a [(i32, usize)]);
    let cases: &[ShardCase<'_>] = &[
        (&[1], 1, &[1], &[(1, 10)]),
        (&[1, 2], 1, &[2], &[(2, 10)]),
        (&[1, 2], 2, &[1, 2], &[(1, 2), (2, 8)]),
        (&[0, 1], 1, &[1], &[(1, 10)]),
        (&[0, 1], 2, &[0, 1], &[(0, 3), (1, 7)]),
    ];

    for &(partitions, shard_size, exp_parts, exp_counts) in cases {
        let mut desc = PartitionRingDesc::default();
        for &pid in partitions {
            desc.add_partition(pid, PartitionState::PartitionActive, SystemTime::now());
        }
        let pr = PartitionRing::new(desc).unwrap();
        let nr = pr.shuffle_shard(user, shard_size).unwrap();
        assert_eq!(nr.partition_ids(), exp_parts);

        let mut counts: BTreeMap<i32, usize> = BTreeMap::new();
        for &k in &tokens {
            let pid = nr.active_partition_for_key(k).unwrap();
            *counts.entry(pid).or_insert(0) += 1;
        }
        let expected_map: BTreeMap<i32, usize> = exp_counts.iter().copied().collect();
        assert_eq!(counts, expected_map);
    }
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesServiceWithPartitionsRing
#[tokio::test]
async fn test_owned_series_service_with_partitions_ring() {
    let user = "test-user-123";
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let (series_to_write, _) = generate_series_with_tokens_at(user, now_ms);

    // 1) empty ingester & partition ring strategy tests covering 0/1/2 partitions, shard size changes, limits changes, and WAL replay
    let dir = TempDir::new().unwrap();
    let rt_file = dir.path().join("runtime.yaml");
    let mut rt_map = BTreeMap::new();
    rt_map.insert(user.to_string(), (OWNED_SERVICE_TEST_USER_LIMIT as i64, 0));
    write_runtime_limits_file(&rt_file, &rt_map, true);

    let rt_mgr = Arc::new(
        RuntimeConfigManager::new(
            RuntimeConfigConfig {
                file: StringSliceCSV(vec![rt_file.to_string_lossy().to_string()]),
                period: GoDuration::from_secs(3600),
                ..Default::default()
            },
            default_limits_test_config(),
            InstanceLimits::default(),
        )
        .unwrap(),
    );
    let overrides = rt_mgr.overrides();

    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    cas_typed::<PartitionRingDesc, _>(kv.as_ref(), "ingester-partitions", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        d.add_partition(0, PartitionState::PartitionActive, SystemTime::now());
        Ok(Some((d, true)))
    })
    .await
    .unwrap();

    let watcher = PartitionRingWatcher::new(
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv),
        PartitionRingOptions::default(),
        None,
    );
    let w_svc = Arc::clone(&watcher).into_service();
    start_and_await_running(ServiceContext::background(), w_svc.as_ref())
        .await
        .unwrap();

    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let (ing, _) = prepare_ingester_with_kv_and_overrides(
        cfg.clone(),
        blocks_cfg.clone(),
        ingest_cfg,
        overrides.clone(),
        Arc::new(InMemoryKv::new(ring_desc_codec())),
        None,
    );
    start_and_wait_healthy(&ing).await;

    let i_u = Arc::clone(&ing);
    let i_g = Arc::clone(&ing);
    let oss = OwnedSeriesService::new(
        Duration::from_secs(600),
        Arc::new(OwnedSeriesPartitionRingStrategy::new(
            0,
            Arc::clone(&watcher),
            overrides.clone(),
        )),
        None,
        Arc::new(move || i_u.get_tsdb_users()),
        Arc::new(move |u| i_g.get_tsdb(u)),
    );

    // Empty ingester
    assert_eq!(oss.update_all_tenants(false), 0);

    // Push all series -> new user trigger
    for (lbls, ts) in &series_to_write {
        ing.push_grpc(user, &mock_write_request(lbls, 0.0, *ts))
            .await
            .unwrap();
    }
    assert_eq!(oss.update_all_tenants(false), 1);
    assert_eq!(
        oss.last_update_reasons(),
        vec![RECOMPUTE_OWNED_SERIES_REASON_NEW_USER]
    );
    let db = ing.get_tsdb(user).unwrap();
    assert_eq!(
        db.owned_series_state().owned_series_count,
        OWNED_SERVICE_SERIES_COUNT
    );

    // Add partition 1 -> split 3/7 across partitions 0 & 1
    cas_typed::<PartitionRingDesc, _>(kv.as_ref(), "ingester-partitions", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        d.add_partition(1, PartitionState::PartitionActive, SystemTime::now());
        Ok(Some((d, true)))
    })
    .await
    .unwrap();
    let dl = tokio::time::Instant::now() + Duration::from_secs(2);
    while watcher.partition_ring().active_partition_ids().len() < 2
        && tokio::time::Instant::now() < dl
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // With shard size=0, local limit changes -> recomputes and partition 0 owns 3 series
    // Update overrides via Limiter-compatible check: here OwnedSeriesPartitionRingStrategy reads overrides;
    // since shard size=0 uses both partitions, local_limit in UserTsdb uses ingester's limiter.
    assert_eq!(oss.update_all_tenants(true), 1);
    assert_eq!(db.owned_series_state().owned_series_count, 3);
    assert_eq!(db.active_series.active().0, 3);

    // Change shard size to 1 -> partition 1 owns all 10 (partition 0 owns 0)
    rt_map.insert(user.to_string(), (OWNED_SERVICE_TEST_USER_LIMIT as i64, 1));
    write_runtime_limits_file(&rt_file, &rt_map, true);
    rt_mgr.reload().unwrap();
    assert_eq!(oss.update_all_tenants(false), 1);
    assert_eq!(
        oss.last_update_reasons(),
        vec![RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED]
    );
    assert_eq!(db.owned_series_state().owned_series_count, 0);

    // Change shard size to 2 -> partition 0 owns 3 again
    rt_map.insert(user.to_string(), (OWNED_SERVICE_TEST_USER_LIMIT as i64, 2));
    write_runtime_limits_file(&rt_file, &rt_map, true);
    rt_mgr.reload().unwrap();
    assert_eq!(oss.update_all_tenants(false), 1);
    assert_eq!(db.owned_series_state().owned_series_count, 3);

    // Remove partition 1 -> partition 0 owns all 10 again
    cas_typed::<PartitionRingDesc, _>(kv.as_ref(), "ingester-partitions", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        d.remove_partition(1);
        Ok(Some((d, true)))
    })
    .await
    .unwrap();
    let dl2 = tokio::time::Instant::now() + Duration::from_secs(2);
    while watcher.partition_ring().active_partition_ids().len() > 1
        && tokio::time::Instant::now() < dl2
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(oss.update_all_tenants(true), 1);
    assert_eq!(
        db.owned_series_state().owned_series_count,
        OWNED_SERVICE_SERIES_COUNT
    );

    let _ = stop_and_await_terminated(ServiceContext::background(), w_svc.as_ref()).await;
    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesStartsQuicklyWithEmptyIngesterRing
#[tokio::test]
async fn test_owned_series_starts_quickly_with_empty_ingester_ring() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let settings = RingSettings {
        heartbeat_timeout: Duration::from_secs(60),
        replication_factor: 1,
        zone_awareness_enabled: true,
        excluded_zones: Vec::new(),
        subring_cache_disabled: false
    };
    let ring = Ring::with_store_and_strategy(
        settings,
        "ingester",
        "collectors/ring",
        Arc::clone(&kv),
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .unwrap();
    let r_svc = Arc::clone(&ring).into_service();
    start_and_await_running(ServiceContext::background(), r_svc.as_ref())
        .await
        .unwrap();

    let strat = Arc::new(OwnedSeriesIngesterRingStrategy::new(
        "ingester",
        Arc::clone(&ring),
        Overrides::new(default_limits_test_config()),
    ));
    let checks = Arc::new(AtomicUsize::new(0));
    let checks_c = Arc::clone(&checks);
    let oss = OwnedSeriesService::new(
        Duration::from_secs(10),
        strat,
        None,
        Arc::new(move || {
            checks_c.fetch_add(1, Ordering::AcqRel);
            Vec::new()
        }),
        Arc::new(|_| None),
    );
    oss.start_and_await_running().await.unwrap();
    assert_eq!(checks.load(Ordering::Acquire), 0);

    cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        add_ingester_to_desc(
            &mut d,
            "an-instance",
            "localhost:11111",
            "zone",
            vec![1, 2, 3],
            InstanceState::Active,
        );
        Ok(Some((d, true)))
    })
    .await
    .unwrap();

    let dl = tokio::time::Instant::now() + Duration::from_secs(2);
    while checks.load(Ordering::Acquire) == 0 && tokio::time::Instant::now() < dl {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(checks.load(Ordering::Acquire) > 0);
    oss.stop_and_await_terminated().await.unwrap();
    let _ = stop_and_await_terminated(ServiceContext::background(), r_svc.as_ref()).await;
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesStartsQuicklyWithEmptyPartitionsRing
#[tokio::test]
async fn test_owned_series_starts_quickly_with_empty_partitions_ring() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let watcher = PartitionRingWatcher::new(
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv),
        PartitionRingOptions::default(),
        None,
    );
    let w_svc = Arc::clone(&watcher).into_service();
    start_and_await_running(ServiceContext::background(), w_svc.as_ref())
        .await
        .unwrap();

    let strat = Arc::new(OwnedSeriesPartitionRingStrategy::new(
        5,
        Arc::clone(&watcher),
        Overrides::new(default_limits_test_config()),
    ));
    let checks = Arc::new(AtomicUsize::new(0));
    let checks_c = Arc::clone(&checks);
    let oss = OwnedSeriesService::new(
        Duration::from_secs(10),
        strat,
        None,
        Arc::new(move || {
            checks_c.fetch_add(1, Ordering::AcqRel);
            Vec::new()
        }),
        Arc::new(|_| None),
    );
    oss.start_and_await_running().await.unwrap();
    assert_eq!(checks.load(Ordering::Acquire), 0);

    cas_typed::<PartitionRingDesc, _>(kv.as_ref(), "ingester-partitions", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        d.add_partition(6, PartitionState::PartitionActive, SystemTime::now());
        Ok(Some((d, true)))
    })
    .await
    .unwrap();

    let dl = tokio::time::Instant::now() + Duration::from_secs(2);
    while checks.load(Ordering::Acquire) == 0 && tokio::time::Instant::now() < dl {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(checks.load(Ordering::Acquire) > 0);
    oss.stop_and_await_terminated().await.unwrap();
    let _ = stop_and_await_terminated(ServiceContext::background(), w_svc.as_ref()).await;
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesIngesterRingStrategyRingChanged
#[tokio::test]
async fn test_owned_series_ingester_ring_strategy_ring_changed() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let settings = RingSettings {
        heartbeat_timeout: Duration::from_secs(60),
        replication_factor: 3,
        zone_awareness_enabled: true,
        excluded_zones: Vec::new(),
        subring_cache_disabled: false
    };
    let ring = Ring::with_store_and_strategy(
        settings,
        "ingester",
        "collectors/ring",
        Arc::clone(&kv),
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .unwrap();
    let r_svc = Arc::clone(&ring).into_service();
    start_and_await_running(ServiceContext::background(), r_svc.as_ref())
        .await
        .unwrap();

    let id1 = "first ingester";
    let id2 = "second instance";
    let strat = OwnedSeriesIngesterRingStrategy::new(
        id1,
        Arc::clone(&ring),
        Overrides::new(default_limits_test_config()),
    );

    cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        add_ingester_to_desc(
            &mut d,
            id1,
            "localhost:11111",
            "zone",
            vec![1, 2, 3],
            InstanceState::Active,
        );
        Ok(Some((d, true)))
    })
    .await
    .unwrap();
    let dl = tokio::time::Instant::now() + Duration::from_secs(2);
    while ring.instances_count() < 1 && tokio::time::Instant::now() < dl {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        add_ingester_to_desc(
            &mut d,
            id2,
            "localhost:22222",
            "zone",
            vec![4, 5, 6],
            InstanceState::Active,
        );
        Ok(Some((d, true)))
    })
    .await
    .unwrap();
    let dl2 = tokio::time::Instant::now() + Duration::from_secs(2);
    while ring.instances_count() < 2 && tokio::time::Instant::now() < dl2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(strat.check_ring_for_changes().unwrap());

    // Change of state is not interesting
    cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        if let Some(inst) = d.ingesters.get_mut(id2) {
            inst.state = InstanceState::Leaving as i32;
        }
        Ok(Some((d, true)))
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!strat.check_ring_for_changes().unwrap());

    // Removal of instance
    cas_typed::<Desc, _>(kv.as_ref(), "collectors/ring", move |cur| {
        let mut d = cur.cloned().unwrap_or_default();
        d.ingesters.remove(id2);
        Ok(Some((d, true)))
    })
    .await
    .unwrap();
    let dl3 = tokio::time::Instant::now() + Duration::from_secs(2);
    while ring.instances_count() > 1 && tokio::time::Instant::now() < dl3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(strat.check_ring_for_changes().unwrap());

    let _ = stop_and_await_terminated(ServiceContext::background(), r_svc.as_ref()).await;
}

// Upstream: pkg/ingester/owned_series_test.go::TestOwnedSeriesPartitionsRingStrategyRingChanged
#[tokio::test]
async fn test_owned_series_partitions_ring_strategy_ring_changed() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let watcher = PartitionRingWatcher::new(
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv),
        PartitionRingOptions::default(),
        None,
    );
    let w_svc = Arc::clone(&watcher).into_service();
    start_and_await_running(ServiceContext::background(), w_svc.as_ref())
        .await
        .unwrap();

    let strat = OwnedSeriesPartitionRingStrategy::new(
        1,
        Arc::clone(&watcher),
        Overrides::new(default_limits_test_config()),
    );

    async fn update_pr(
        kv: &Arc<dyn KvClient>,
        mut f: Box<dyn FnMut(&mut PartitionRingDesc) + Send>,
    ) {
        cas_typed::<PartitionRingDesc, _>(kv.as_ref(), "ingester-partitions", move |cur| {
            let mut d = cur.cloned().unwrap_or_default();
            f(&mut d);
            Ok(Some((d, true)))
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // 1: Active
    update_pr(&kv, Box::new(|d| {
        d.add_partition(1, PartitionState::PartitionActive, SystemTime::now());
    }))
    .await;
    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    // 2: Active
    update_pr(&kv, Box::new(|d| {
        d.add_partition(2, PartitionState::PartitionActive, SystemTime::now());
    }))
    .await;
    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    // 3: Inactive
    update_pr(&kv, Box::new(|d| {
        d.add_partition(3, PartitionState::PartitionInactive, SystemTime::now());
    }))
    .await;
    assert!(!strat.check_ring_for_changes().unwrap());

    // 4: Pending
    update_pr(&kv, Box::new(|d| {
        d.add_partition(4, PartitionState::PartitionPending, SystemTime::now());
    }))
    .await;
    assert!(!strat.check_ring_for_changes().unwrap());

    // 3 -> Active
    update_pr(&kv, Box::new(|d| {
        let _ = d.update_partition_state(3, PartitionState::PartitionActive, SystemTime::now());
    }))
    .await;
    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    // 1 -> Inactive
    update_pr(&kv, Box::new(|d| {
        let _ = d.update_partition_state(1, PartitionState::PartitionInactive, SystemTime::now());
    }))
    .await;
    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    // 1 -> Pending
    update_pr(&kv, Box::new(|d| {
        let _ = d.update_partition_state(1, PartitionState::PartitionPending, SystemTime::now());
    }))
    .await;
    assert!(!strat.check_ring_for_changes().unwrap());

    // 1 -> Active, 2 -> Inactive
    update_pr(&kv, Box::new(|d| {
        let _ = d.update_partition_state(1, PartitionState::PartitionActive, SystemTime::now());
        let _ = d.update_partition_state(2, PartitionState::PartitionInactive, SystemTime::now());
    }))
    .await;
    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    // remove 3 (active)
    update_pr(&kv, Box::new(|d| {
        d.remove_partition(3);
    }))
    .await;
    assert!(strat.check_ring_for_changes().unwrap());
    assert!(!strat.check_ring_for_changes().unwrap());

    // remove 2 (inactive)
    update_pr(&kv, Box::new(|d| {
        d.remove_partition(2);
    }))
    .await;
    assert!(!strat.check_ring_for_changes().unwrap());

    let _ = stop_and_await_terminated(ServiceContext::background(), w_svc.as_ref()).await;
}

// ============================================================================
// pkg/ingester/shipper_test.go (5 tests)
// ============================================================================

// Upstream: pkg/ingester/shipper_test.go::TestShipper
#[tokio::test]
async fn test_shipper() {
    let blocks_dir = TempDir::new().unwrap();
    let bucket_dir = TempDir::new().unwrap();
    let reg = Registry::new();

    let bkt = BucketClient::from_config(&BucketConfig {
        backend_cfg: mimir_config::StorageBackendConfig {
            backend: "filesystem".to_string(),
            filesystem: mimir_config::FilesystemConfig {
                dir: bucket_dir.path().to_string_lossy().to_string(),
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    let s = Shipper::new(
        "",
        blocks_dir.path(),
        bkt.clone(),
        SourceType::Test,
        false,
        ShipperMetrics::new(Some(&reg)),
    );

    assert!(read_shipped_blocks(blocks_dir.path()).unwrap().is_empty());

    let id1 = Ulid::from_parts(1, 0);
    assert_eq!(
        metric_value(&reg, "cortex_ingester_shipper_last_successful_upload_timestamp_seconds", &[]),
        Some(0.0)
    );

    let mut labels1 = BTreeMap::new();
    labels1.insert("a".into(), "b".into());
    create_block_on_disk(
        blocks_dir.path(),
        id1,
        &mk_block_meta(id1, 1000, 2000, 100, BlockMetaCompaction::default(), ThanosMeta { labels: labels1, ..Default::default() }),
    );
    assert_eq!(s.sync().await.unwrap(), 1);
    assert!(
        metric_value(
            &reg,
            "cortex_ingester_shipper_last_successful_upload_timestamp_seconds",
            &[]
        )
        .unwrap()
            > 0.0
    );
    let shipped1 = read_shipped_blocks(blocks_dir.path()).unwrap();
    assert_eq!(shipped1.len(), 1);
    assert!(shipped1.contains_key(&id1));

    // No new block -> 0 uploaded
    assert_eq!(s.sync().await.unwrap(), 0);

    // Block without external labels
    let id2 = Ulid::from_parts(2, 0);
    create_block_on_disk(
        blocks_dir.path(),
        id2,
        &mk_block_meta(id2, 2000, 4000, 100, BlockMetaCompaction::default(), ThanosMeta::default()),
    );
    assert_eq!(s.sync().await.unwrap(), 1);
    let shipped2 = read_shipped_blocks(blocks_dir.path()).unwrap();
    assert_eq!(shipped2.len(), 2);
    assert!(shipped2.contains_key(&id1));
    assert!(shipped2.contains_key(&id2));

    // Block with 0 samples is not uploaded
    let id3 = Ulid::from_parts(3, 0);
    create_block_on_disk(
        blocks_dir.path(),
        id3,
        &mk_block_meta(id3, 2000, 4000, 0, BlockMetaCompaction::default(), ThanosMeta::default()),
    );
    assert_eq!(s.sync().await.unwrap(), 0);

    let meta = download_meta(&bkt, id1).await.unwrap();
    assert!(!meta.thanos.files.is_empty());
}

#[derive(Debug)]
struct DeceivingUploadStore {
    inner: LocalFileSystem,
    fail_once_on_meta: AtomicBool,
}

impl fmt::Display for DeceivingUploadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeceivingUploadStore")
    }
}

impl ObjectStore for DeceivingUploadStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let loc_str = location.to_string();
        let location = location.clone();
        Box::pin(async move {
            let res = self.inner.put_opts(&location, payload, opts).await?;
            if loc_str.ends_with("/meta.json") && self.fail_once_on_meta.load(Ordering::Acquire) {
                return Err(object_store::Error::Generic {
                    store: "DeceivingUploadStore",
                    source: "base name matches, will fail upload".into(),
                });
            }
            Ok(res)
        })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        opts: PutMultipartOpts,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.put_multipart_opts(location, opts)
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        options: GetOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get_opts(location, options)
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.delete(location)
    }

    fn list<'life0>(
        &'life0 self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'life0, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjPath>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<ListResult>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_with_delimiter(prefix)
    }

    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjPath,
        to: &'life2 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.copy(from, to)
    }

    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjPath,
        to: &'life2 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.copy_if_not_exists(from, to)
    }
}

// Upstream: pkg/ingester/shipper_test.go::TestShipper_DeceivingUploadErrors
#[tokio::test]
async fn test_shipper_deceiving_upload_errors() {
    let blocks_dir = TempDir::new().unwrap();
    let bucket_dir = TempDir::new().unwrap();
    let store = Arc::new(DeceivingUploadStore {
        inner: LocalFileSystem::new_with_prefix(bucket_dir.path()).unwrap(),
        fail_once_on_meta: AtomicBool::new(true),
    });
    let bkt = BucketClient::from_object_store(store, "test", "filesystem");
    let s = Shipper::new(
        "",
        blocks_dir.path(),
        bkt,
        SourceType::Test,
        false,
        ShipperMetrics::new(None),
    );

    let id1 = Ulid::from_parts(1, 0);
    create_block_on_disk(
        blocks_dir.path(),
        id1,
        &mk_block_meta(id1, 1000, 2000, 100, BlockMetaCompaction::default(), ThanosMeta::default()),
    );

    // First sync fails on meta.json upload even though it landed in bucket
    assert!(s.sync().await.is_err());

    // Second sync sees meta.json already exists and reports 1 uploaded without re-uploading
    assert_eq!(s.sync().await.unwrap(), 1);
}

// Upstream: pkg/ingester/shipper_test.go::TestIterBlockMetas
#[test]
fn test_iter_block_metas() {
    let dir = TempDir::new().unwrap();
    for (part, min_t, max_t) in [(1u64, 1000, 2000), (2, 4000, 5000), (3, 2000, 3000)] {
        let id = Ulid::from_parts(part, 0);
        fs::create_dir(dir.path().join(id.to_string())).unwrap();
        mk_block_meta(id, min_t, max_t, 0, BlockMetaCompaction::default(), ThanosMeta::default())
        .write_to_dir(dir.path().join(id.to_string()))
        .unwrap();
    }
    let metas = block_metas_from_oldest(dir.path()).unwrap();
    assert!(metas.windows(2).all(|w| w[0].min_time <= w[1].min_time));
}

// Upstream: pkg/ingester/shipper_test.go::TestShipperAddsSegmentFiles
#[tokio::test]
async fn test_shipper_adds_segment_files() {
    let dir = TempDir::new().unwrap();
    let bkt = BucketClient::from_object_store(Arc::new(InMemory::new()), "test", "inmemory");
    let s = Shipper::new(
        "",
        dir.path(),
        bkt.clone(),
        SourceType::Test,
        false,
        ShipperMetrics::new(None),
    );

    let id = Ulid::from_parts(1, 0);
    let block_dir = dir.path().join(id.to_string());
    let chunks_dir = block_dir.join(CHUNKS_DIRNAME);
    fs::create_dir_all(&chunks_dir).unwrap();
    mk_block_meta(id, 1000, 2000, 1000, BlockMetaCompaction::default(), ThanosMeta::default())
    .write_to_dir(&block_dir)
    .unwrap();
    fs::write(block_dir.join("index"), b"index file").unwrap();
    let seg = "00001";
    fs::write(chunks_dir.join(seg), b"hello world").unwrap();

    assert_eq!(s.sync().await.unwrap(), 1);
    let meta = download_meta(&bkt, id).await.unwrap();
    assert_eq!(meta.thanos.segment_files, vec![seg.to_string()]);
}

// Upstream: pkg/ingester/shipper_test.go::TestShipper_AddOOOLabel
#[tokio::test]
async fn test_shipper_add_ooo_label() {
    let cases = [
        (false, false, false, BTreeMap::new()),
        (true, false, false, BTreeMap::new()),
        (false, true, true, BTreeMap::new()),
        (
            true,
            true,
            true,
            BTreeMap::from([(
                OUT_OF_ORDER_EXTERNAL_LABEL.to_string(),
                OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string(),
            )]),
        ),
        (
            true,
            true,
            true,
            BTreeMap::from([
                ("a".to_string(), "b".to_string()),
                (
                    OUT_OF_ORDER_EXTERNAL_LABEL.to_string(),
                    OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string(),
                ),
            ]),
        ),
    ];

    for (idx, (add_ooo, is_ooo, exp_hint, exp_labels)) in cases.into_iter().enumerate() {
        let blocks_dir = TempDir::new().unwrap();
        let bkt = BucketClient::from_object_store(Arc::new(InMemory::new()), "test", "inmemory");
        let s = Shipper::new(
            "",
            blocks_dir.path(),
            bkt.clone(),
            SourceType::Test,
            add_ooo,
            ShipperMetrics::new(None),
        );

        let id = Ulid::from_parts(1, 0);
        let mut comp = BlockMetaCompaction::default();
        if is_ooo {
            comp.set_out_of_order();
        }
        let mut init_labels = BTreeMap::new();
        if idx == 4 {
            init_labels.insert("a".to_string(), "b".to_string());
        }
        create_block_on_disk(
            blocks_dir.path(),
            id,
            &mk_block_meta(id, 1000, 2000, 100, comp, ThanosMeta { labels: init_labels, ..Default::default() }),
        );
        assert_eq!(s.sync().await.unwrap(), 1);
        let read_meta = download_meta(&bkt, id).await.unwrap();
        assert_eq!(read_meta.compaction.from_out_of_order(), exp_hint);
        assert_eq!(read_meta.thanos.labels, exp_labels);
    }
}

// ============================================================================
// pkg/ingester/user_tsdb_test.go (6 tests)
// ============================================================================

fn open_standalone_user_tsdb(
    dir: &Path,
    user_id: &str,
    limits: LimitsConfig,
    use_owned_for_limits: bool,
    early_non_owned: bool,
) -> Arc<UserTsdb> {
    let (mut cfg, blocks_cfg, _) = default_ingester_test_config(dir);
    cfg.use_ingester_owned_series_for_limits = use_owned_for_limits;
    cfg.early_compaction_non_owned_series_enabled = early_non_owned;
    let overrides = Overrides::new(limits.clone());
    let limiter = Arc::new(Limiter::new(overrides.clone(), Arc::new(NoopRingLimiterStrategy)));
    let reg = Registry::new();
    let metrics = IngesterMetrics::new(
        Some(&reg),
        false,
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        || 0.0,
        || None,
        || 0.0,
        || 0.0,
        || 0.0,
    );
    UserTsdb::open(
        user_id.to_string(),
        &cfg,
        &blocks_cfg,
        overrides,
        limiter,
        None,
        ShipperMetrics::new(Some(&reg)),
        Some(&reg),
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        Arc::new(|| 0),
        metrics.rejected.clone(),
        HashSet::new(),
        0,
        0,
    )
    .unwrap()
}

// Upstream: pkg/ingester/user_tsdb_test.go::TestUserTSDB_acquireAppendLock
#[tokio::test]
async fn test_user_tsdb_acquire_append_lock() {
    let dir = TempDir::new().unwrap();
    let db = open_standalone_user_tsdb(dir.path(), "test", default_limits_test_config(), false, false);

    // 1) allows acquiring lock during forced compaction if timestamp doesn't conflict
    {
        let g = db.acquire_append_lock(20).unwrap();
        drop(g);
        let (ok, _) = db.change_state_to_forced_compaction(TsdbState::Active, 20);
        assert!(ok);
        assert!(matches!(
            db.acquire_append_lock(20),
            Err(IngesterError::TsdbUnavailable { .. })
        ));
        let g2 = db.acquire_append_lock(21).unwrap();
        drop(g2);
        let (ok2, _) = db.change_state(TsdbState::ForceCompacting, TsdbState::Active);
        assert!(ok2);
    }

    // 2) counts all acquired locks in in-flight appends
    {
        let g_active = db.acquire_append_lock(20).unwrap();
        let (ok, _) = db.change_state_to_forced_compaction(TsdbState::Active, 20);
        assert!(ok);
        let g_fc = db.acquire_append_lock(21).unwrap();

        let db_c = Arc::clone(&db);
        let wait_fut = tokio::spawn(async move {
            db_c.wait_in_flight_appends().await;
        });

        drop(g_active);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!wait_fut.is_finished());

        drop(g_fc);
        tokio::time::timeout(Duration::from_millis(200), wait_fut)
            .await
            .unwrap()
            .unwrap();
        let _ = db.change_state(TsdbState::ForceCompacting, TsdbState::Active);
    }

    // 3) wait_in_flight_before_forced_compaction completes once active locks release
    {
        let g_active = db.acquire_append_lock(20).unwrap();
        let (ok, _) = db.change_state_to_forced_compaction(TsdbState::Active, 20);
        assert!(ok);
        let g_fc = db.acquire_append_lock(21).unwrap();

        let db_c = Arc::clone(&db);
        let wait_fut = tokio::spawn(async move {
            db_c.wait_in_flight_before_forced_compaction().await;
        });

        drop(g_active);
        tokio::time::timeout(Duration::from_millis(200), wait_fut)
            .await
            .unwrap()
            .unwrap();
        drop(g_fc);
    }
    db.close().unwrap();
}

// Upstream: pkg/ingester/user_tsdb_test.go::TestUserTSDB_addPendingNonOwnedRefs
#[test]
fn test_user_tsdb_add_pending_non_owned_refs() {
    let dir = TempDir::new().unwrap();
    let db = open_standalone_user_tsdb(dir.path(), "test", default_limits_test_config(), false, true);

    let set_of = |refs: &[u64]| -> HashSet<u64> { refs.iter().copied().collect() };
    let keys_of = |m: &HashMap<u64, Instant>| -> HashSet<u64> { m.keys().copied().collect() };

    // 1) initial snapshot populates timestamps
    let before = Instant::now();
    db.add_pending_non_owned_refs(&set_of(&[1, 2, 3]));
    let after = Instant::now();
    let snap = db.pending_non_owned_refs_snapshot();
    assert_eq!(keys_of(&snap), set_of(&[1, 2, 3]));
    for ts in snap.values() {
        assert!(*ts >= before && *ts <= after);
    }

    // 2) re-queuing same snapshot leaves timestamps untouched
    std::thread::sleep(Duration::from_millis(2));
    db.add_pending_non_owned_refs(&set_of(&[1, 2, 3]));
    assert_eq!(db.pending_non_owned_refs_snapshot(), snap);

    // 3) adding new ref stamps only the new ref
    let prev1 = snap[&1];
    std::thread::sleep(Duration::from_millis(2));
    db.add_pending_non_owned_refs(&set_of(&[1, 2, 3, 4]));
    let snap2 = db.pending_non_owned_refs_snapshot();
    assert_eq!(keys_of(&snap2), set_of(&[1, 2, 3, 4]));
    assert_eq!(snap2[&1], prev1);
    assert!(snap2[&4] > prev1);

    // 4) refs absent from snapshot are dropped without retouching survivors
    let ts1 = snap2[&1];
    let ts3 = snap2[&3];
    std::thread::sleep(Duration::from_millis(2));
    db.add_pending_non_owned_refs(&set_of(&[1, 3]));
    let snap3 = db.pending_non_owned_refs_snapshot();
    assert_eq!(keys_of(&snap3), set_of(&[1, 3]));
    assert_eq!(snap3[&1], ts1);
    assert_eq!(snap3[&3], ts3);

    // 5) drops + additions reconcile
    std::thread::sleep(Duration::from_millis(2));
    db.add_pending_non_owned_refs(&set_of(&[1, 3, 4]));
    let snap4 = db.pending_non_owned_refs_snapshot();
    assert_eq!(keys_of(&snap4), set_of(&[1, 3, 4]));
    assert_eq!(snap4[&1], ts1);
    assert!(snap4[&4] > ts1);

    // 6) empty snapshot drops everything
    db.add_pending_non_owned_refs(&HashSet::new());
    assert!(db.pending_non_owned_refs_snapshot().is_empty());

    // 7) full replacement
    db.add_pending_non_owned_refs(&set_of(&[1, 2, 3]));
    let orig1 = db.pending_non_owned_refs_snapshot()[&1];
    std::thread::sleep(Duration::from_millis(2));
    db.add_pending_non_owned_refs(&set_of(&[4, 5, 6]));
    let snap5 = db.pending_non_owned_refs_snapshot();
    assert_eq!(keys_of(&snap5), set_of(&[4, 5, 6]));
    for r in [4, 5, 6] {
        assert!(snap5[&r] > orig1);
    }

    db.close().unwrap();
}

// Upstream: pkg/ingester/user_tsdb_test.go::TestUserTSDB_takePendingNonOwnedRefs
#[test]
fn test_user_tsdb_take_pending_non_owned_refs() {
    let dir = TempDir::new().unwrap();
    let db = open_standalone_user_tsdb(dir.path(), "test", default_limits_test_config(), false, true);
    let set_of = |refs: &[u64]| -> HashSet<u64> { refs.iter().copied().collect() };

    // 1) empty queue returns empty
    assert!(db.take_pending_non_owned_refs(Instant::now()).is_empty());

    // 2) grace not elapsed leaves queue intact
    let before_add = Instant::now();
    std::thread::sleep(Duration::from_millis(2));
    db.add_pending_non_owned_refs(&set_of(&[1, 2, 3]));
    assert!(db.take_pending_non_owned_refs(before_add).is_empty());
    assert_eq!(db.pending_non_owned_refs_snapshot().len(), 3);

    // 3) grace elapsed drains all
    let mut got = db.take_pending_non_owned_refs(Instant::now() + Duration::from_secs(3600));
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);
    assert!(db.pending_non_owned_refs_snapshot().is_empty());

    // 4) exact boundary (not_after == stamped) succeeds
    db.add_pending_non_owned_refs(&set_of(&[42]));
    let boundary = db.pending_non_owned_refs_snapshot()[&42];
    assert_eq!(db.take_pending_non_owned_refs(boundary), vec![42]);
    assert!(db.pending_non_owned_refs_snapshot().is_empty());

    // 5) partial drain retains newer refs
    db.add_pending_non_owned_refs(&set_of(&[1, 2]));
    std::thread::sleep(Duration::from_millis(4));
    let between = Instant::now();
    std::thread::sleep(Duration::from_millis(4));
    db.add_pending_non_owned_refs(&set_of(&[1, 2, 3]));
    let mut partial = db.take_pending_non_owned_refs(between);
    partial.sort_unstable();
    assert_eq!(partial, vec![1, 2]);
    assert_eq!(
        db.pending_non_owned_refs_snapshot().keys().copied().collect::<Vec<_>>(),
        vec![3]
    );

    let rest = db.take_pending_non_owned_refs(Instant::now() + Duration::from_secs(3600));
    assert_eq!(rest, vec![3]);
    assert!(db.pending_non_owned_refs_snapshot().is_empty());

    db.close().unwrap();
}

// Upstream: pkg/ingester/user_tsdb_test.go::TestNextForcedHeadCompactionRange
#[test]
fn test_next_forced_head_compaction_range() {
    let block_duration = 10i64;
    let cases = [
        (12, 18, 50, Some((12, 18, true))),
        (12, 21, 50, Some((12, 19, false))),
        (12, 19, 15, Some((12, 15, true))),
        (12, 21, 15, Some((12, 15, true))),
        (12, 21, 11, None),
        (12, 21, 12, Some((12, 12, true))),
        (i64::MAX, 20, 12, None),
        (0, i64::MIN, 12, None),
    ];

    for (h_min, h_max, forced_max, expected) in cases {
        let got = next_forced_head_compaction_range(block_duration, h_min, h_max, forced_max);
        assert_eq!(got, expected);
    }
}

// Upstream: pkg/ingester/user_tsdb_test.go::TestGetSeriesCountAndMinLocalLimit
#[test]
fn test_get_series_count_and_min_local_limit() {
    let dir = TempDir::new().unwrap();
    let db = open_standalone_user_tsdb(dir.path(), "test", default_limits_test_config(), false, false);

    let mut app = db.db.appender();
    app.append_float(None, &[("__name__", "hello"), ("a", "world")], 10, 20.0)
        .unwrap();
    app.commit().unwrap();

    db.set_owned_series_state(mimir_ingester::user_tsdb::OwnedSeriesState {
        owned_series_count: 555,
        shard_size: 0,
        local_series_limit: 10000,
    });

    db.set_use_owned_series_for_limits(false);
    assert_eq!(db.get_series_count_and_min_local_limit(), (1, 0));

    db.set_use_owned_series_for_limits(true);
    assert_eq!(db.get_series_count_and_min_local_limit(), (555, 10000));

    db.close().unwrap();
}

// Upstream: pkg/ingester/user_tsdb_test.go::TestRecomputeOwnedSeries
#[test]
fn test_recompute_owned_series() {
    let limits = LimitsConfig {
        max_global_series_per_user: 0,
        ..Default::default()
    };
    let overrides = Overrides::new(limits.clone());
    let ring_lim = Arc::new(Limiter::new(
        overrides.clone(),
        Arc::new(IngesterRingLimiterStrategy::new_opt(
            None,
            3,
            true,
            "zone",
            overrides,
        )),
    ));

    // 1) happy path
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test", limits.clone(), false, true);
        let _ = &ring_lim;
        let (ok, attempts) = db.recompute_owned_series_with_compute_fn(5, "test", || 10);
        assert!(ok);
        assert_eq!(attempts, 1);
        let os = db.owned_series_state();
        assert_eq!((os.owned_series_count, os.shard_size, os.local_series_limit), (10, 5, i32::MAX as usize));
        db.close().unwrap();
    }

    // 2) increase during computation within limit (500 <= 1000)
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test", limits.clone(), false, true);
        let (ok, attempts) = db.recompute_owned_series_with_compute_fn(5, "test", || {
            db.add_owned_series_count(500);
            10
        });
        assert!(ok);
        assert_eq!(attempts, 1);
        assert_eq!(db.owned_series_state().owned_series_count, 10);
        db.close().unwrap();
    }

    // 3) last increase is within limit (attempt 3 succeeds)
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test", limits.clone(), false, true);
        let mut mods = vec![2000usize, 2000, 1000];
        let (ok, attempts) = db.recompute_owned_series_with_compute_fn(5, "test", || {
            let d = mods.remove(0);
            db.add_owned_series_count(d);
            10
        });
        assert!(ok);
        assert_eq!(attempts, 3);
        db.close().unwrap();
    }

    // 4) all increases exceed limit
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test", limits, false, true);
        let (ok, attempts) = db.recompute_owned_series_with_compute_fn(5, "test", || {
            db.add_owned_series_count(2000);
            10
        });
        assert!(!ok);
        assert_eq!(attempts, 3);
        db.close().unwrap();
    }

    // 5) compute_owned_series with no token ranges queues all series
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test-user", default_limits_test_config(), false, true);
        let mut app = db.db.appender();
        app.append_float(None, &[("__name__", "metric_a")], 100, 1.0)
            .unwrap();
        app.commit().unwrap();
        assert_eq!(db.compute_owned_series(), 0);
        assert_eq!(
            db.take_pending_non_owned_refs(Instant::now() + Duration::from_secs(3600))
                .len(),
            1
        );
        assert_eq!(db.db.head().num_series(), 1);
        db.close().unwrap();
    }

    // 6) empty head + no token ranges queues nothing
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test-user", default_limits_test_config(), false, true);
        assert_eq!(db.compute_owned_series(), 0);
        assert!(db
            .take_pending_non_owned_refs(Instant::now() + Duration::from_secs(3600))
            .is_empty());
        db.close().unwrap();
    }

    // 7) all series owned queues nothing
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test-user", default_limits_test_config(), false, true);
        let mut app = db.db.appender();
        app.append_float(None, &[("__name__", "metric_a")], 100, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "metric_b")], 200, 2.0)
            .unwrap();
        app.commit().unwrap();
        db.update_token_ranges(vec![0, u32::MAX]);
        assert_eq!(db.compute_owned_series(), 2);
        assert!(db
            .take_pending_non_owned_refs(Instant::now() + Duration::from_secs(3600))
            .is_empty());
        db.close().unwrap();
    }

    // 8) partial ownership queues only non-owned refs
    {
        let dir = TempDir::new().unwrap();
        let db = open_standalone_user_tsdb(dir.path(), "test-user", default_limits_test_config(), false, true);
        let la = Labels::from_pairs([("__name__", "metric_a")]);
        let lb = Labels::from_pairs([("__name__", "metric_b")]);
        let mut app = db.db.appender();
        app.append_float(None, &[("__name__", "metric_a")], 100, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "metric_b")], 200, 2.0)
            .unwrap();
        app.commit().unwrap();
        let ha = secondary_tsdb_hash_for_user("test-user", &la);
        let hb = secondary_tsdb_hash_for_user("test-user", &lb);
        assert_ne!(ha, hb);
        db.update_token_ranges(vec![0, ha.min(hb)]);
        assert_eq!(db.compute_owned_series(), 1);
        assert_eq!(
            db.take_pending_non_owned_refs(Instant::now() + Duration::from_secs(3600))
                .len(),
            1
        );
        assert_eq!(db.db.head().num_series(), 2);
        db.close().unwrap();
    }
}
