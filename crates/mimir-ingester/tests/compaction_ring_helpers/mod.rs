#![allow(dead_code)]

pub mod fake_kafka;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fake_kafka::FakeKafkaCluster;
use mimir_common::services::{ManagedBasicService, Service, ServiceContext};
use mimir_config::limits::LimitsConfig;
use mimir_config::{
    BlocksStorageConfig, CompartmentsConfig, GoDuration, IngestStorageConfig, IngesterConfig,
    Overrides, StringSliceCSV,
};
use mimir_ingester::ingester_ring_lifecycler::IngesterLifecycler;
use mimir_ingester::{Ingester, TokenlessLifecycler};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ringpb::{Desc, InstanceState, PartitionRingDesc};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::lifecycler::{BasicLifecyclerConfig, Lifecycler, LifecyclerConfig};
use mimir_ring::partition_ring::model::partition_ring_desc_codec;
use mimir_ring::partition_ring::{
    PartitionInstanceLifecycler, PartitionInstanceLifecyclerConfig, PartitionRingOptions,
    PartitionRingWatcher,
};
use mimir_ring::ring::model::{ring_desc_codec, InstanceDescExt};
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{Ring, RingSettings};
use mimir_storage::ingest::WRITE_COMPARTMENT_ID_PLACEHOLDER;
use mimir_tsdb::block::Block;
use mimir_tsdb::chunkenc::EncodedChunk;
use mimir_tsdb::compact::decode_chunk_samples;
use prometheus::Registry;
use tokio::net::TcpListener;
use tonic::metadata::MetadataMap;
use ulid::Ulid;

pub const INGEST_RING_NAME: &str = "ingester";
pub const INGEST_RING_KEY: &str = "ring";
pub const PARTITION_RING_NAME: &str = "ingester-partitions";
pub const PARTITION_RING_KEY: &str = "ingester-partitions";
pub const USER_ID: &str = "1";

pub fn default_limits_test_config() -> LimitsConfig {
    LimitsConfig::default()
}

pub struct TestIngesterConfigs {
    pub ingester: IngesterConfig,
    pub blocks: BlocksStorageConfig,
    pub ingest: IngestStorageConfig,
    pub ring_kv: Arc<dyn KvClient>,
}

pub fn default_ingester_test_config(root_dir: &Path) -> TestIngesterConfigs {
    let tsdb_dir = root_dir.join("tsdb");
    let bucket_dir = root_dir.join("bucket");
    let _ = std::fs::create_dir_all(&tsdb_dir);
    let _ = std::fs::create_dir_all(&bucket_dir);

    let ring_kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let mut ingester = IngesterConfig::default();
    ingester.ring.kvstore.store = "inmemory".to_string();
    ingester.ring.num_tokens = 1;
    ingester.ring.instance_port = 0;
    ingester.ring.instance_addr = "localhost".to_string();
    ingester.ring.instance_id = "localhost".to_string();
    ingester.ring.final_sleep = GoDuration::ZERO;
    ingester.ring.min_ready_duration = GoDuration::from_millis(100);
    ingester.ring.heartbeat_period = GoDuration::from_millis(500);
    ingester.active_series.active_series_metrics_enabled = true;

    let mut blocks = BlocksStorageConfig::default();
    blocks.tsdb.dir = tsdb_dir.to_string_lossy().to_string();
    blocks.tsdb.ship_interval = GoDuration::ZERO;
    blocks.bucket.backend_cfg.backend = "filesystem".to_string();
    blocks.bucket.backend_cfg.filesystem.dir = bucket_dir.to_string_lossy().to_string();

    let ingest = IngestStorageConfig::default();

    TestIngesterConfigs {
        ingester,
        blocks,
        ingest,
        ring_kv,
    }
}

pub async fn create_test_ring(
    cfg: &mimir_config::RingConfig,
    kv: Arc<dyn KvClient>,
    reg: Option<&Registry>,
) -> (Arc<Ring>, Arc<ManagedBasicService>) {
    let r = Ring::with_store_and_strategy(
        RingSettings::from(cfg),
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        kv,
        Arc::new(DefaultReplicationStrategy),
        reg,
    )
    .expect("create test ring");
    let svc = Arc::clone(&r).into_service();
    svc.start_async(ServiceContext::background())
        .await
        .expect("start ring");
    svc.await_running().await.expect("await ring");
    (r, svc)
}

pub async fn prepare_ingester_with_blocks_storage(
    cfg: TestIngesterConfigs,
    limits: Option<LimitsConfig>,
    reg: Option<&Registry>,
) -> (
    Arc<Ingester>,
    Arc<Ring>,
    Arc<ManagedBasicService>,
) {
    let lim = limits.unwrap_or_else(default_limits_test_config);
    let overrides = Overrides::new(lim);
    let (ring, ring_svc) = create_test_ring(&cfg.ingester.ring, Arc::clone(&cfg.ring_kv), reg).await;

    let lc_cfg = LifecyclerConfig::from_ring_config(&cfg.ingester.ring, 0).expect("lc cfg");
    let lc = Lifecycler::with_store(
        lc_cfg,
        Arc::clone(&cfg.ring_kv),
        None,
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        cfg.blocks.tsdb.flush_blocks_on_shutdown,
        reg,
    );

    let ing = Ingester::new(
        cfg.ingester,
        cfg.blocks,
        cfg.ingest,
        overrides,
        Some(Arc::clone(&ring)),
        Some(lc),
        None,
        None,
        None,
        reg,
    )
    .expect("create ingester");
    (ing, ring, ring_svc)
}

pub async fn start_and_wait_healthy(ingester: &Arc<Ingester>, ring: &Ring) {
    ingester.start().await.expect("start ingester");
    let id = &ingester.cfg.ring.instance_id;
    let ok = poll_until(Duration::from_secs(5), || async {
        ring.get_instance_state(id).ok() == Some(InstanceState::Active)
    })
    .await;
    assert!(ok, "timed out waiting for instance {id} to become ACTIVE");
}

pub struct IngestStorageHarness {
    pub ingester: Arc<Ingester>,
    pub ring: Arc<Ring>,
    pub ring_svc: Arc<ManagedBasicService>,
    pub clusters: Vec<Arc<FakeKafkaCluster>>,
    pub watcher: Arc<PartitionRingWatcher>,
    pub watcher_svc: Arc<ManagedBasicService>,
    pub partition_kv: Arc<dyn KvClient>,
}

impl IngestStorageHarness {
    pub async fn cleanup(self) {
        let _ = self.ingester.stop().await;
        self.watcher_svc.stop_async();
        let _ = self.watcher_svc.await_terminated().await;
        self.ring_svc.stop_async();
        let _ = self.ring_svc.await_terminated().await;
        for c in self.clusters {
            c.close();
        }
    }
}

pub async fn create_test_ingester_with_ingest_storage(
    mut cfg: TestIngesterConfigs,
    limits: LimitsConfig,
    existing_ring: Option<(Arc<Ring>, Arc<ManagedBasicService>)>,
    partition_kv_opt: Option<Arc<dyn KvClient>>,
    polling_interval: Duration,
    reg: Option<&Registry>,
) -> IngestStorageHarness {
    if cfg.ingester.ring.instance_id.is_empty() || cfg.ingester.ring.instance_id == "localhost" {
        cfg.ingester.ring.instance_id = "ingester-zone-a-0".to_string();
    }

    let (ring, ring_svc) = match existing_ring {
        Some(pair) => pair,
        None => create_test_ring(&cfg.ingester.ring, Arc::clone(&cfg.ring_kv), reg).await,
    };

    let topic = "mimir".to_string();
    let cluster = FakeKafkaCluster::start(10, &topic).await;

    cfg.ingester.push_grpc_method_enabled = false;
    cfg.ingest.enabled = true;
    cfg.ingest.kafka.topic = topic;
    cfg.ingest.kafka.address = StringSliceCSV(vec![cluster.addr.clone()]);
    cfg.ingest.kafka.last_produced_offset_poll_interval = GoDuration::from_millis(100);
    cfg.ingest.kafka.auto_create_topic_enabled = false;

    let partition_kv = partition_kv_opt
        .unwrap_or_else(|| Arc::new(InMemoryKv::new(partition_ring_desc_codec())));

    let watcher = PartitionRingWatcher::new(
        PARTITION_RING_NAME,
        PARTITION_RING_KEY,
        Arc::clone(&partition_kv),
        PartitionRingOptions::default(),
        reg,
    );
    let watcher_svc = Arc::clone(&watcher).into_service();
    watcher_svc
        .start_async(ServiceContext::background())
        .await
        .expect("start watcher");
    watcher_svc.await_running().await.expect("await watcher");

    let pid = mimir_ingester::ingest_storage::ingester_partition_id_from_instance_id(
        &cfg.ingester.ring.instance_id,
    )
    .expect("partition id");
    let plc_cfg = PartitionInstanceLifecyclerConfig::from_partition_ring_config(
        &cfg.ingester.partition_ring,
        pid,
        &cfg.ingester.ring.instance_id,
        polling_interval,
    );
    let plc = PartitionInstanceLifecycler::new(
        plc_cfg,
        PARTITION_RING_NAME,
        PARTITION_RING_KEY,
        Arc::clone(&partition_kv),
        reg,
    )
    .expect("create plc");

    let overrides = Overrides::new(limits);

    let lc_dyn: Arc<dyn IngesterLifecycler> = if cfg.ingester.ring.num_tokens == 0 {
        let bcfg = BasicLifecyclerConfig::from_ring_config_tokenless(&cfg.ingester.ring, 0)
            .expect("tokenless cfg");
        TokenlessLifecycler::new(
            bcfg,
            INGEST_RING_NAME,
            INGEST_RING_KEY,
            Arc::clone(&cfg.ring_kv),
            cfg.ingester.ring.min_ready_duration.as_std().unwrap_or(Duration::ZERO),
            cfg.ingester.ring.final_sleep.as_std().unwrap_or(Duration::ZERO),
            cfg.blocks.tsdb.flush_blocks_on_shutdown,
            None,
            reg,
        )
    } else {
        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg.ingester.ring, 0).expect("lc cfg");
        Lifecycler::with_store(
            lc_cfg,
            Arc::clone(&cfg.ring_kv),
            None,
            INGEST_RING_NAME,
            INGEST_RING_KEY,
            cfg.blocks.tsdb.flush_blocks_on_shutdown,
            reg,
        )
    };

    let ingester = Ingester::new_with_ingester_lifecycler(
        cfg.ingester,
        cfg.blocks,
        cfg.ingest,
        overrides,
        Some(Arc::clone(&ring)),
        Some(lc_dyn),
        Some(Arc::clone(&watcher)),
        Some(plc),
        None,
        reg,
    )
    .expect("create ingester");
    ingester.set_head_compaction_interval_jitter_enabled(false);

    IngestStorageHarness {
        ingester,
        ring,
        ring_svc,
        clusters: vec![cluster],
        watcher,
        watcher_svc,
        partition_kv,
    }
}

pub async fn create_test_compartments_ingester(
    mut cfg: TestIngesterConfigs,
    compartments: CompartmentsConfig,
    limits: LimitsConfig,
    partition_kv_opt: Option<Arc<dyn KvClient>>,
    polling_interval: Duration,
    reg: Option<&Registry>,
) -> IngestStorageHarness {
    assert!(compartments.enabled, "compartments must be enabled");
    if cfg.ingester.ring.instance_id.is_empty() || cfg.ingester.ring.instance_id == "localhost" {
        cfg.ingester.ring.instance_id = "ingester-zone-a-0".to_string();
    }

    let (ring, ring_svc) = create_test_ring(&cfg.ingester.ring, Arc::clone(&cfg.ring_kv), reg).await;

    let rc_id = cfg.ingester.read_compartment_id.max(0) as usize;
    let num_wc = compartments.write.num_compartments.max(1) as usize;
    let topic_template = "mimir-rc-<read-compartment-id>".to_string();
    let consumed_topic = topic_template.replace("<read-compartment-id>", &rc_id.to_string());

    let (address_template, clusters) =
        start_templatable_kafka_clusters(10, num_wc, &consumed_topic).await;

    cfg.ingester.push_grpc_method_enabled = false;
    cfg.ingest.enabled = true;
    cfg.ingest.kafka.topic = topic_template;
    cfg.ingest.kafka.address = StringSliceCSV(vec![address_template]);
    cfg.ingest.kafka.last_produced_offset_poll_interval = GoDuration::from_millis(100);
    cfg.ingest.kafka.auto_create_topic_enabled = false;

    let partition_kv = partition_kv_opt
        .unwrap_or_else(|| Arc::new(InMemoryKv::new(partition_ring_desc_codec())));

    let rc_ring_name = format!("{PARTITION_RING_NAME}-rc-{rc_id}");
    let rc_ring_key = format!("{PARTITION_RING_KEY}-rc-{rc_id}");

    let watcher = PartitionRingWatcher::new(
        &rc_ring_name,
        &rc_ring_key,
        Arc::clone(&partition_kv),
        PartitionRingOptions::default(),
        reg,
    );
    let watcher_svc = Arc::clone(&watcher).into_service();
    watcher_svc
        .start_async(ServiceContext::background())
        .await
        .expect("start watcher");
    watcher_svc.await_running().await.expect("await watcher");

    let pid = mimir_ingester::ingest_storage::ingester_partition_id_from_instance_id(
        &cfg.ingester.ring.instance_id,
    )
    .expect("partition id");
    let plc_cfg = PartitionInstanceLifecyclerConfig::from_partition_ring_config(
        &cfg.ingester.partition_ring,
        pid,
        &cfg.ingester.ring.instance_id,
        polling_interval,
    );
    let plc = PartitionInstanceLifecycler::new(
        plc_cfg,
        &rc_ring_name,
        &rc_ring_key,
        Arc::clone(&partition_kv),
        reg,
    )
    .expect("create plc");

    let overrides = Overrides::new(limits);
    let lc_cfg = LifecyclerConfig::from_ring_config(&cfg.ingester.ring, 0).expect("lc cfg");
    let lc = Lifecycler::with_store(
        lc_cfg,
        Arc::clone(&cfg.ring_kv),
        None,
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        cfg.blocks.tsdb.flush_blocks_on_shutdown,
        reg,
    );

    let ingester = Ingester::new(
        cfg.ingester,
        cfg.blocks,
        cfg.ingest,
        overrides,
        Some(Arc::clone(&ring)),
        Some(lc),
        Some(Arc::clone(&watcher)),
        Some(plc),
        None,
        reg,
    )
    .expect("create ingester");
    ingester.set_compartments(compartments);
    ingester.set_head_compaction_interval_jitter_enabled(false);

    IngestStorageHarness {
        ingester,
        ring,
        ring_svc,
        clusters,
        watcher,
        watcher_svc,
        partition_kv,
    }
}

async fn start_templatable_kafka_clusters(
    num_partitions: i32,
    num_clusters: usize,
    topic: &str,
) -> (String, Vec<Arc<FakeKafkaCluster>>) {
    for _ in 0..100 {
        let seed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = (seed.local_addr().unwrap().port() / 10) * 10;
        drop(seed);
        if base < 1024 || (base as usize + num_clusters) > 65535 {
            continue;
        }
        let mut listeners = Vec::with_capacity(num_clusters);
        for w in 0..num_clusters {
            match TcpListener::bind(format!("127.0.0.1:{}", base + w as u16)).await {
                Ok(ln) => listeners.push(ln),
                Err(_) => break,
            }
        }
        if listeners.len() < num_clusters {
            continue;
        }
        let clusters: Vec<Arc<FakeKafkaCluster>> = listeners
            .into_iter()
            .map(|ln| FakeKafkaCluster::start_with_listener(ln, num_partitions, topic))
            .collect();
        let template = format!("127.0.0.1:{}{}", base / 10, WRITE_COMPARTMENT_ID_PLACEHOLDER);
        return (template, clusters);
    }
    panic!("could not bind consecutive ports for templatable Kafka clusters");
}

pub type TestSeriesSpec<'a> = (&'a [(&'a str, &'a str)], &'a [(i64, f64)]);
pub type QuerySeriesResult = (BTreeMap<String, String>, Vec<(i64, f64)>);

pub async fn push_series_to_ingester(
    ingester: &Ingester,
    user_id: &str,
    series: &[TestSeriesSpec<'_>],
) -> Result<(), mimir_ingester::IngesterError> {
    let timeseries = series
        .iter()
        .map(|(lbls, samples)| cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(k, v))
                .collect(),
            samples: samples
                .iter()
                .map(|&(ts, val)| cortexpb::Sample {
                    value: val,
                    timestamp_ms: ts,
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    let req = cortexpb::WriteRequest {
        timeseries,
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    ingester.push_grpc(user_id, &req).await
}

pub async fn run_test_query(
    ingester: &Ingester,
    md: &MetadataMap,
    user_id: &str,
    match_type: ingesterpb::MatchType,
    name: &str,
    value: &str,
) -> Result<Vec<QuerySeriesResult>, mimir_ingester::IngesterError> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: i64::MIN,
        end_timestamp_ms: i64::MAX,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: match_type as i32,
            name: name.to_string(),
            value: value.to_string(),
        }],
        ..Default::default()
    };
    let responses = ingester.handle_query_stream(md, user_id, req).await?;

    let mut series_labels: Vec<BTreeMap<String, String>> = Vec::new();
    let mut series_samples: Vec<Vec<(i64, f64)>> = Vec::new();

    for resp in responses {
        for s in resp.streaming_series {
            let map: BTreeMap<String, String> = s
                .labels
                .into_iter()
                .map(|lp| {
                    (
                        String::from_utf8_lossy(&lp.name).into_owned(),
                        String::from_utf8_lossy(&lp.value).into_owned(),
                    )
                })
                .collect();
            series_labels.push(map);
            series_samples.push(Vec::new());
        }
        for sc in resp.streaming_series_chunks {
            let idx = sc.series_index as usize;
            for ch in sc.chunks {
                if ch.encoding == 4 {
                    let xc = mimir_tsdb::chunkenc::XorChunk::from_bytes(ch.data.to_vec());
                    let samples = decode_chunk_samples(&EncodedChunk::Xor(xc))
                        .expect("decode xor samples");
                    for sample in samples {
                        if let mimir_tsdb::compact::SampleValue::Float(v) = sample.value {
                            series_samples[idx].push((sample.t, v));
                        }
                    }
                }
            }
        }
    }

    let mut out = Vec::with_capacity(series_labels.len());
    for (lbls, mut vals) in series_labels.into_iter().zip(series_samples) {
        vals.sort_by_key(|p| p.0);
        out.push((lbls, vals));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub fn list_blocks_in_dir(dir: &Path) -> Vec<Ulid> {
    if !dir.exists() {
        return Vec::new();
    }
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("dir entry");
        if !entry.path().is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(uid) = Ulid::from_string(name) {
                ids.push(uid);
            }
        }
    }
    ids.sort();
    ids
}

pub fn read_metric_samples_from_block_dir(
    block_dir: &Path,
    metric_name: &str,
) -> Vec<QuerySeriesResult> {
    let blk = Block::open(block_dir).expect("open block");
    let ir = blk.index();
    let cr = blk.chunks();
    let postings = ir
        .postings("__name__", &[metric_name])
        .expect("postings");
    let mut out = Vec::new();
    for ref_id in postings {
        let (lbls, chks) = ir.series(ref_id).expect("series");
        let map: BTreeMap<String, String> = lbls
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut vals = Vec::new();
        for cm in chks {
            let enc = cr.chunk(cm.ref_id).expect("chunk");
            let samples = decode_chunk_samples(&enc).expect("decode chunk");
            for s in samples {
                if let mimir_tsdb::compact::SampleValue::Float(v) = s.value {
                    vals.push((s.t, v));
                }
            }
        }
        out.push((map, vals));
    }
    out
}

pub async fn poll_until<F, Fut>(timeout: Duration, mut pred: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if pred().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn get_instance_state_in_ring(
    kv: &dyn KvClient,
    ring_key: &str,
    instance_id: &str,
) -> Option<InstanceState> {
    let val = kv.get(ring_key).await.expect("kv get")?;
    let desc = val.downcast_ref::<Desc>().expect("ring.Desc");
    desc.ingesters
        .get(instance_id)
        .map(|i| i.instance_state())
}

pub async fn get_instance_token_count(
    kv: &dyn KvClient,
    ring_key: &str,
    instance_id: &str,
) -> Option<usize> {
    let val = kv.get(ring_key).await.expect("kv get")?;
    let desc = val.downcast_ref::<Desc>().expect("ring.Desc");
    desc.ingesters.get(instance_id).map(|i| i.tokens.len())
}

pub fn create_partition_ring_desc_with_partition(
    pid: i32,
    owner_id: &str,
) -> PartitionRingDesc {
    let mut desc = PartitionRingDesc::default();
    use mimir_proto::ringpb::{OwnerState, PartitionState};
    use mimir_ring::partition_ring::model::PartitionRingDescExt;
    let now = std::time::SystemTime::now();
    desc.add_partition(pid, PartitionState::PartitionActive, now);
    desc.add_or_update_owner(owner_id, OwnerState::OwnerDeleted, pid, now);
    desc
}
