#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::{start_and_await_running, ServiceContext, State};
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, LimitsConfig, Overrides,
};
use mimir_proto::cortexpb;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::lifecycler::Lifecycler;
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{ring_desc_codec, Ring, RingSettings};
use mimir_tsdb::labels::Labels;
use prometheus::Registry;

use mimir_ingester::Ingester;

pub const TEST_USER_ID: &str = "1";

pub fn default_limits_test_config() -> LimitsConfig {
    LimitsConfig::default()
}

pub fn default_ingester_test_config(dir: &Path) -> (IngesterConfig, BlocksStorageConfig, IngestStorageConfig) {
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_addr = "localhost".to_string();
    cfg.ring.instance_id = "localhost".to_string();
    cfg.ring.num_tokens = 1;
    cfg.ring.final_sleep = GoDuration(0);
    cfg.ring.min_ready_duration = GoDuration::from_millis(10);
    cfg.ring.heartbeat_period = GoDuration::from_millis(50);
    cfg.ring.kvstore.store = "inmemory".to_string();
    cfg.active_series.active_series_metrics_enabled = true;

    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.to_string_lossy().to_string();
    blocks_cfg.tsdb.ship_interval = GoDuration(0);

    let ingest_cfg = IngestStorageConfig::default();

    (cfg, blocks_cfg, ingest_cfg)
}

pub fn create_ring_and_lifecycler_with_kv(
    cfg: &IngesterConfig,
    kv: Arc<dyn KvClient>,
) -> (Arc<Ring>, Arc<Lifecycler>) {
    let mut settings = RingSettings::from(&cfg.ring);
    if settings.replication_factor == 0 {
        settings.replication_factor = 1;
    }
    let ring = Ring::with_store_and_strategy(
        settings,
        "ingester",
        "collectors/ring",
        Arc::clone(&kv),
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .expect("create ring");

    let lc_cfg = mimir_ring::lifecycler::LifecyclerConfig::from_ring_config(&cfg.ring, 0)
        .expect("lifecycler cfg");
    let lc = Lifecycler::with_store(
        lc_cfg,
        kv,
        None,
        "ingester",
        "collectors/ring",
        false,
        None,
    );
    (ring, lc)
}

pub fn prepare_ingester_with_blocks_storage(
    _dir: &Path,
    mut cfg: IngesterConfig,
    blocks_cfg: BlocksStorageConfig,
    ingest_cfg: IngestStorageConfig,
    limits: LimitsConfig,
    registry: Option<&Registry>,
) -> (Arc<Ingester>, Arc<Ring>, Arc<dyn KvClient>) {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    cfg.ring.kvstore.store = "inmemory".to_string();
    let (ring, lc) = create_ring_and_lifecycler_with_kv(&cfg, Arc::clone(&kv));
    let overrides = Overrides::new(limits);
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        overrides,
        Some(Arc::clone(&ring)),
        Some(lc),
        None,
        None,
        None,
        registry,
    )
    .expect("create ingester");
    (ing, ring, kv)
}

pub fn prepare_ingester_with_kv_and_overrides(
    cfg: IngesterConfig,
    blocks_cfg: BlocksStorageConfig,
    ingest_cfg: IngestStorageConfig,
    overrides: Overrides,
    kv: Arc<dyn KvClient>,
    registry: Option<&Registry>,
) -> (Arc<Ingester>, Arc<Ring>) {
    let (ring, lc) = create_ring_and_lifecycler_with_kv(&cfg, kv);
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        overrides,
        Some(Arc::clone(&ring)),
        Some(lc),
        None,
        None,
        None,
        registry,
    )
    .expect("create ingester");
    (ing, ring)
}

pub async fn start_ring(ring: &Arc<Ring>) {
    let svc = Arc::clone(ring).into_service();
    let ctx = ServiceContext::background();
    start_and_await_running(ctx, svc.as_ref())
        .await
        .expect("start ring service");
}

pub async fn start_and_wait_healthy(ing: &Arc<Ingester>) {
    if let Some(ref ring) = ing.instance_ring {
        start_ring(ring).await;
    }
    ing.start().await.expect("ingester start");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while ing.state() != State::Running {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for ingester Running state");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn mock_write_request(lbls: &Labels, val: f64, ts_ms: i64) -> cortexpb::WriteRequest {
    let pairs: Vec<cortexpb::LabelPair> = lbls
        .iter()
        .map(|(k, v)| cortexpb::LabelPair::from_strs(k, v))
        .collect();
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: pairs,
            samples: vec![cortexpb::Sample {
                value: val,
                timestamp_ms: ts_ms,
            }],
            exemplars: Vec::new(),
            histograms: Vec::new(),
            created_timestamp: 0,
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

pub fn metric_value(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let families = reg.gather();
    let expected: HashMap<&str, &str> = labels.iter().copied().collect();
    for mf in families {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let mut actual = HashMap::new();
            for lp in m.get_label() {
                actual.insert(lp.get_name(), lp.get_value());
            }
            if actual == expected {
                if m.has_counter() {
                    return Some(m.get_counter().get_value());
                }
                if m.has_gauge() {
                    return Some(m.get_gauge().get_value());
                }
            }
        }
    }
    None
}

pub fn metric_count(reg: &Registry, name: &str) -> usize {
    reg.gather()
        .into_iter()
        .filter(|mf| mf.get_name() == name)
        .map(|mf| mf.get_metric().len())
        .sum()
}
