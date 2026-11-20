//! Wave 21 Regression Tests:
//! - W21-DF-01: Non-client-streaming gRPC request cardinality (0 / 2+ frames) & per-frame bounds across identity/gzip/zstd/snappy/s2
//! - W21-DF-02: Per-block CostBasedPlanner cached once per ULID block, BlockReadGuard & HeadReadGuard pinned across MirroredChunkQuerier, Head OOO mirrored outside
//! - W21-DF-03: RefLeaksTracker instruments Kafka v0/v1/v2 records, custom Collector drains expired buffers on /metrics collect(), 1000-slot cap, OS page size
//! - W21-DF-08: ReactiveLimiter::acquire_permit races against RpcDeadlineContext and increments rejected_requests_total
//! - W21-DF-09: SampleTracker preserves empty wire label values (foo="" -> "") and flushes discarded attributions on mid-series hard error
//! - W21-DF-10: PostingsForMatchersCache invalidations_total gated until after Tsdb::open + initial db.compact() and not incremented on Head GC
//! - W21-DF-11: OwnedSeriesService offloads update_all_tenants to spawn_blocking with cancellation check & logs ring errors at error!
//! - W21-APS-01: Gorilla mux GET routes reject HEAD with 405 without triggering side effects
//! - W21-APS-06: label_churn_handler offloaded to spawn_blocking, parsed via go_parse_query, signed pre-1970 since_ms

mod common;

use std::collections::HashSet;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::to_bytes;
use axum::http;
use bytes::Bytes;
use mimir_common::services::{ServiceContext, State};
use mimir_config::{
    CostAttributionLabel, CostAttributionTrackerConfig, GoDuration, LimitsConfig, ModelDuration,
    Overrides,
};
use mimir_ingester::costattribution::Manager as CostAttributionManager;
use mimir_ingester::grpc::RpcDeadlineContext;
use mimir_ingester::http::ingester_router;
use mimir_ingester::ingest_storage::IngesterShardedBatchConsumer;
use mimir_ingester::lookupplan::{
    BlockIndexReader, LookupPlanner, NoopPlanner, PlannerFactoryTrait, PlannerProvider,
    PlanningBlockMeta,
};
use mimir_ingester::mirror_querier::{
    ChunkBlockMeta, ChunkSeriesSet, MirroredChunkQuerier,
};
use mimir_ingester::owned_series::{OwnedSeriesRingStrategy, OwnedSeriesService};
use mimir_ingester::query::PlannedTsdbChunkQuerier;
use mimir_ingester::user_tsdb::{SharedTsdbMetrics, UserTsdb};
use mimir_storage::shipper::ShipperMetrics;
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_proto::ref_leaks::InstrumentRefLeaksConfig;

use mimir_storage::ingest::pusher::PusherConsumerMetrics;
use mimir_storage::ingest::version::{RecordSerializer, RecordVersion};
use mimir_storage::ingest::FetchedRecord;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::{CounterVec, Encoder as _, Opts, Registry, TextEncoder};
use tempfile::TempDir;
use tonic::codec::CompressionEncoding;
use tower::ServiceExt;

use common::{
    default_ingester_test_config, default_limits_test_config,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

struct SingleChunkBody(Option<Bytes>);

impl hyper::body::Body for SingleChunkBody {
    type Data = Bytes;
    type Error = tonic::Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.0.take() {
            Some(b) => Poll::Ready(Some(Ok(hyper::body::Frame::data(b)))),
            None => Poll::Ready(None),
        }
    }
}

fn new_running_ingester(
    dir: &std::path::Path,
    limits: LimitsConfig,
    reg: Option<&Registry>,
) -> Arc<Ingester> {
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir);
    cfg.push_grpc_method_enabled = true;
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(limits),
        None,
        None,
        None,
        None,
        None,
        reg,
    )
    .expect("create ingester");
    ing.set_state_for_test(State::Running);
    ing
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn unframed(payload: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Bytes::from(out)
}

fn gzip_framed(payload: &[u8]) -> Bytes {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(payload).unwrap();
    let compressed = enc.finish().unwrap();
    let mut out = Vec::with_capacity(5 + compressed.len());
    out.push(1);
    out.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    Bytes::from(out)
}

fn zstd_framed(payload: &[u8]) -> Bytes {
    let compressed = zstd::stream::encode_all(payload, 0).unwrap();
    let mut out = Vec::with_capacity(5 + compressed.len());
    out.push(1);
    out.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    Bytes::from(out)
}

fn push_req_bytes(metric: &str, ts_ms: i64) -> Vec<u8> {
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", metric)],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts_ms,
                value: 42.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec()
}

fn render_metrics(reg: &Registry) -> String {
    let mut buf = Vec::new();
    TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

// ---------------------------------------------------------------------------
// 1. [W21-DF-01] Non-client-streaming gRPC cardinality & per-frame bounds
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w21_df_01_non_client_streaming_grpc_cardinality_and_per_frame_bounds() {
    const NO_MSG: &str =
        "cardinality violation: received no request message from non-client-streaming RPC";
    const MULTI_MSG: &str =
        "cardinality violation: received multiple request messages for non-client-streaming RPC";

    let dir = TempDir::new().unwrap();
    let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
    let max_decode = 1024usize;
    let svc = IngesterService::new(Arc::clone(&ing))
        .into_server()
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(max_decode);

    let ts = now_ms();
    let p1 = push_req_bytes("w21_df01_m1", ts);
    let p2 = push_req_bytes("w21_df01_m2", ts);

    type FrameEncoder = (&'static str, fn(&[u8]) -> Bytes);
    let encodings: [FrameEncoder; 5] = [
        ("identity", unframed),
        ("gzip", gzip_framed),
        ("zstd", zstd_framed),
        ("snappy", |b| {
            mimir_proto::snappy::encode_grpc_frame(b, true).unwrap()
        }),
        ("s2", |b| {
            mimir_proto::snappy::encode_grpc_frame(b, true).unwrap()
        }),
    ];

    for (enc, mk_frame) in encodings {
        // 0 frames -> cardinality violation (0 request messages) across Push, QueryStream, LabelNames
        for uri in [
            "/cortex.Ingester/Push",
            "/cortex.Ingester/QueryStream",
            "/cortex.Ingester/LabelNames",
        ] {
            let mut builder = http::Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .header("x-scope-orgid", "tenant-df01");
            if enc != "identity" {
                builder = builder.header("grpc-encoding", enc);
            }
            let req = builder
                .body(tonic::body::BoxBody::new(SingleChunkBody(None)))
                .unwrap();
            let resp = svc.clone().oneshot(req).await.unwrap();
            let status = tonic::Status::from_header_map(resp.headers()).expect("grpc-status");
            assert_eq!(status.code(), tonic::Code::Internal, "enc={enc} uri={uri}");
            assert_eq!(status.message(), NO_MSG, "enc={enc} uri={uri}");
        }

        // 2 concatenated valid frames -> cardinality violation BEFORE invoking Push handler (0 series committed)
        let f1 = mk_frame(&p1);
        let f2 = mk_frame(&p2);
        let mut two = Vec::with_capacity(f1.len() + f2.len());
        two.extend_from_slice(&f1);
        two.extend_from_slice(&f2);
        let mut builder = http::Request::builder()
            .method("POST")
            .uri("/cortex.Ingester/Push")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-scope-orgid", "tenant-df01");
        if enc != "identity" {
            builder = builder.header("grpc-encoding", enc);
        }
        let req = builder
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
                two,
            )))))
            .unwrap();
        let resp = svc.clone().oneshot(req).await.unwrap();
        let status = tonic::Status::from_header_map(resp.headers()).expect("grpc-status");
        assert_eq!(status.code(), tonic::Code::Internal, "enc={enc}: {status:?}");
        assert_eq!(status.message(), MULTI_MSG, "enc={enc}");
        // Verify handler was NOT invoked on frame 1!
        assert!(
            ing.get_tsdb("tenant-df01")
                .is_none_or(|db| db.db.head().num_series() == 0),
            "enc={enc} must not ingest frame 1 when frame 2 is present"
        );

        // 2 frames on QueryStream & LabelNames -> also rejected before handler
        let q_bytes = ingesterpb::QueryRequest {
            start_timestamp_ms: ts - 1000,
            end_timestamp_ms: ts + 1000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".to_string(),
                value: "w21_df01_m1".to_string(),
            }],
            streaming_chunks_batch_size: 0,
        }
        .encode_to_vec();
        let qf = mk_frame(&q_bytes);
        let mut two_q = Vec::with_capacity(qf.len() * 2);
        two_q.extend_from_slice(&qf);
        two_q.extend_from_slice(&qf);
        let mut builder = http::Request::builder()
            .method("POST")
            .uri("/cortex.Ingester/QueryStream")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-scope-orgid", "tenant-df01");
        if enc != "identity" {
            builder = builder.header("grpc-encoding", enc);
        }
        let req = builder
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
                two_q,
            )))))
            .unwrap();
        let resp = svc.clone().oneshot(req).await.unwrap();
        let status = tonic::Status::from_header_map(resp.headers()).expect("grpc-status");
        assert_eq!(status.code(), tonic::Code::Internal, "QueryStream enc={enc}");
        assert_eq!(status.message(), MULTI_MSG, "QueryStream enc={enc}");
    }

    // 2 snappy/s2 frames with 600 wire bytes each (total 1210 > 1029, each frame <= 1024)
    // must fail with Internal cardinality violation, NOT ResourceExhausted!
    let mut raw_600 = Vec::with_capacity(600);
    raw_600.push(0); // uncompressed flag inside snappy/s2 request
    raw_600.extend_from_slice(&600u32.to_be_bytes());
    raw_600.resize(605, 0x41);
    let mut two_600 = Vec::with_capacity(1210);
    two_600.extend_from_slice(&raw_600);
    two_600.extend_from_slice(&raw_600);
    for enc in ["snappy", "s2"] {
        let req = http::Request::builder()
            .method("POST")
            .uri("/cortex.Ingester/Push")
            .header("content-type", "application/grpc")
            .header("grpc-encoding", enc)
            .header("te", "trailers")
            .header("x-scope-orgid", "tenant-df01")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
                two_600.clone(),
            )))))
            .unwrap();
        let resp = svc.clone().oneshot(req).await.unwrap();
        let status = tonic::Status::from_header_map(resp.headers()).expect("grpc-status");
        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "per-frame wire cap enc={enc}: {status:?}"
        );
        assert_eq!(status.message(), MULTI_MSG, "per-frame wire cap enc={enc}");
    }

    // Decompressed bomb on frame 2 for gzip and zstd must fail with ResourceExhausted
    let small_gz = gzip_framed(&p1);
    let bomb_gz = gzip_framed(&vec![0u8; 2048]);
    let mut gz_bomb_two = Vec::new();
    gz_bomb_two.extend_from_slice(&small_gz);
    gz_bomb_two.extend_from_slice(&bomb_gz);
    let req_gz = http::Request::builder()
        .method("POST")
        .uri("/cortex.Ingester/Push")
        .header("content-type", "application/grpc")
        .header("grpc-encoding", "gzip")
        .header("te", "trailers")
        .header("x-scope-orgid", "tenant-df01")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
            gz_bomb_two,
        )))))
        .unwrap();
    let resp_gz = svc.clone().oneshot(req_gz).await.unwrap();
    let status_gz = tonic::Status::from_header_map(resp_gz.headers()).expect("grpc-status");
    assert_eq!(status_gz.code(), tonic::Code::ResourceExhausted);

    let small_zs = zstd_framed(&p1);
    let bomb_zs = zstd_framed(&vec![0u8; 2048]);
    let mut zs_bomb_two = Vec::new();
    zs_bomb_two.extend_from_slice(&small_zs);
    zs_bomb_two.extend_from_slice(&bomb_zs);
    let req_zs = http::Request::builder()
        .method("POST")
        .uri("/cortex.Ingester/Push")
        .header("content-type", "application/grpc")
        .header("grpc-encoding", "zstd")
        .header("te", "trailers")
        .header("x-scope-orgid", "tenant-df01")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
            zs_bomb_two,
        )))))
        .unwrap();
    let resp_zs = svc.clone().oneshot(req_zs).await.unwrap();
    let status_zs = tonic::Status::from_header_map(resp_zs.headers()).expect("grpc-status");
    assert_eq!(status_zs.code(), tonic::Code::ResourceExhausted);
}

// ---------------------------------------------------------------------------
// 2. [W21-DF-02] Block planner caching, BlockReadGuard/HeadReadGuard, OOO outside mirror
// ---------------------------------------------------------------------------
struct CountingPlannerFactory {
    calls: AtomicUsize,
}

impl PlannerFactoryTrait for CountingPlannerFactory {
    fn create_planner(
        &self,
        _meta: &PlanningBlockMeta,
        _reader: &dyn BlockIndexReader,
    ) -> Arc<dyn LookupPlanner> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Arc::new(NoopPlanner)
    }
}

#[tokio::test]
async fn w21_df_02_block_planner_cached_and_guards_held_and_ooo_mirrored_outside() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_comparison_portion = 1.0;

    let mut limits = default_limits_test_config();
    limits.out_of_order_time_window = ModelDuration(3600 * 1_000_000_000);

    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    // Part A: block_lookup_planner caches once per opened ULID block and prunes on block unload.
    let cache_dir = TempDir::new().unwrap();
    let (_, mut cache_bcfg, _) = default_ingester_test_config(cache_dir.path());
    cache_bcfg.tsdb.ship_interval = GoDuration(0);
    let cache_ov = Overrides::new(default_limits_test_config());
    let cache_lim = Arc::new(mimir_ingester::limiter::Limiter::new(
        cache_ov.clone(),
        Arc::new(mimir_ingester::limiter::NoopRingLimiterStrategy),
    ));
    let cache_reg = Registry::new();
    let cache_db = UserTsdb::open(
        "user-df02-cache".to_string(),
        &mimir_config::IngesterConfig::default(),
        &cache_bcfg,
        cache_ov,
        cache_lim,
        None,
        ShipperMetrics::new(Some(&cache_reg)),
        &SharedTsdbMetrics::new(Some(&cache_reg)),
        None,
        1,
        Arc::new(AtomicI64::new(0)),
        Arc::new(|| 0),
        CounterVec::new(Opts::new("rej_df02", "h"), &["reason"]).unwrap(),
        HashSet::new(),
        -1,
        1,
        None,
    )
    .unwrap();
    let factory = Arc::new(CountingPlannerFactory {
        calls: AtomicUsize::new(0),
    });
    cache_db.set_planner_provider(Arc::new(PlannerProvider::new(
        Arc::clone(&factory) as Arc<dyn PlannerFactoryTrait>,
    )));
    {
        let mut app = cache_db.db.appender();
        app.append_float(None, &[("__name__", "m_blk"), ("job", "a")], 1_000, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "m_blk"), ("job", "a")], 5_000, 2.0)
            .unwrap();
        app.commit().unwrap();
    }
    let cache_ulid = cache_db.db.compact_head_all().unwrap().expect("compacted block");
    let cache_blk = Arc::clone(&cache_db.db.blocks()[0]);
    assert_eq!(cache_blk.meta().ulid, cache_ulid);
    let _ = cache_db.block_lookup_planner(&cache_blk);
    let _ = cache_db.block_lookup_planner(&cache_blk);
    let _ = cache_db.block_lookup_planner(&cache_blk);
    assert_eq!(
        factory.calls.load(Ordering::SeqCst),
        1,
        "PlannerFactory::create_planner must execute at most once per opened ULID block"
    );

    // Remove cache_ulid on disk and reload_blocks() to trigger on_blocks_reload pruning.
    std::fs::remove_dir_all(cache_dir.path().join("user-df02-cache").join(cache_ulid.to_string())).unwrap();
    cache_db.db.reload_blocks().unwrap();
    assert!(cache_db.db.blocks().iter().all(|b| b.meta().ulid != cache_ulid));
    // Because cache_ulid was pruned on on_blocks_reload, querying its planner again invokes create_planner.
    let _ = cache_db.block_lookup_planner(&cache_blk);
    assert_eq!(
        factory.calls.load(Ordering::SeqCst),
        2,
        "on_blocks_reload must prune unloaded ULID blocks from block_planners cache"
    );

    let user = "user-df02";
    let db = ing.get_or_create_tsdb(user).unwrap();
    {
        let mut app = db.db.appender();
        app.append_float(None, &[("__name__", "m_blk"), ("job", "a")], 1_000, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "m_blk"), ("job", "a")], 5_000, 2.0)
            .unwrap();
        app.commit().unwrap();
    }
    let ulid = db.db.compact_head_all().unwrap().expect("compacted block");
    let blocks = db.db.blocks();
    assert_eq!(blocks.len(), 1);
    let blk = Arc::clone(&blocks[0]);
    assert_eq!(blk.meta().ulid, ulid);

    // Part B: BlockReadGuard stays pinned across MirroredChunkQuerier::select and close()
    // even when Block::close_and_wait() is invoked concurrently between select() and close().
    let outcomes = CounterVec::new(
        Opts::new("test_df02_outcomes", "test"),
        &["outcome"],
    )
    .unwrap();
    let pq = PlannedTsdbChunkQuerier::new_block(
        Arc::clone(&db.db),
        Arc::clone(&blk),
        0,
        10_000,
        db.block_lookup_planner(&blk),
        false,
    );
    let mut mq = MirroredChunkQuerier::new_with_meta(
        user,
        outcomes.clone(),
        0,
        10_000,
        ChunkBlockMeta {
            ulid,
            min_time: blk.meta().min_time,
            max_time: blk.meta().max_time,
        },
        Box::new(pq),
    );
    let matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "m_blk").unwrap()];
    let mut ss = mq.select(true, None, &matchers);
    let mut count = 0usize;
    while ss.next() {
        assert!(ss.at().is_some());
        count += 1;
    }
    assert!(ss.err().is_none());
    assert_eq!(count, 1);
    drop(ss);

    // Spawn concurrent close_and_wait() while mq is between select() and close().
    let blk_close = Arc::clone(&blk);
    let close_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let close_done_bg = Arc::clone(&close_done);
    let waiter = std::thread::spawn(move || {
        blk_close.close_and_wait();
        close_done_bg.store(true, Ordering::Release);
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !close_done.load(Ordering::Acquire),
        "Block::close_and_wait must wait while PlannedTsdbChunkQuerier holds BlockReadGuard"
    );
    mq.close().unwrap();
    waiter.join().unwrap();
    assert!(close_done.load(Ordering::Acquire));
    assert_eq!(
        outcomes.with_label_values(&["success"]).get() as u64,
        1,
        "mirrored block query must succeed with no false-positive extra_series during concurrent close_and_wait"
    );
    assert_eq!(
        outcomes.with_label_values(&["extra_series"]).get() as u64,
        0
    );

    // Part C: Head in-order mirrored inside MirroredChunkQuerier while OOO Head chunks are merged outside.
    // Push in-order sample at 100_000 (inside [0, 120_000]) and m_head_ooo_new at 180_000 (outside [0, 120_000]).
    {
        let mut app = db.db.appender();
        app.append_float(None, &[("__name__", "m_head_ino")], 100_000, 10.0)
            .unwrap();
        app.append_float(None, &[("__name__", "m_head_ooo_new")], 180_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }
    // Directly test concurrent OOO Head write into [0, 120_000] between mq.select() and mq.close()!
    let head_pq = PlannedTsdbChunkQuerier::new_head(
        Arc::clone(&db.db),
        0,
        120_000,
        db.head_lookup_planner(),
        false,
    );
    let mut head_mq = MirroredChunkQuerier::new_with_meta(
        user,
        outcomes.clone(),
        0,
        120_000,
        ChunkBlockMeta::default(),
        Box::new(head_pq),
    );
    let any_matcher = [LabelMatcher::new(MatchType::RegexMatch, "__name__", "m_head_.*").unwrap()];
    let mut hss = head_mq.select(true, None, &any_matcher);
    while hss.next() {
        assert!(hss.at().is_some());
    }
    assert!(hss.err().is_none());
    drop(hss);

    // Now write an OOO sample on m_head_ooo_new at t=50_000 (inside [0, 120_000], <= finished_at_ms)!
    {
        let mut app = db.db.appender();
        app.append_float(None, &[("__name__", "m_head_ooo_new")], 50_000, 99.0)
            .unwrap();
        app.commit().unwrap();
    }
    head_mq.close().unwrap();
    assert_eq!(
        outcomes.with_label_values(&["missing_series"]).get() as u64,
        0,
        "OOO series committed between select() and close() must NOT cause false-positive missing_series"
    );
    assert_eq!(outcomes.with_label_values(&["success"]).get() as u64, 2);

    // Verify full handle_query_stream for [0, 120_000] returns BOTH in-order and OOO Head series!
    let qs = ing
        .handle_query_stream(
            &tonic::metadata::MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 120_000,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::RegexMatch as i32,
                    name: "__name__".to_string(),
                    value: "m_head_.*".to_string(),
                }],
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    let names: HashSet<String> = qs
        .iter()
        .flat_map(|r| r.streaming_series.iter())
        .filter_map(|s| {
            s.labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| String::from_utf8_lossy(&l.value).into_owned())
        })
        .collect();
    assert!(names.contains("m_head_ino"));
    assert!(names.contains("m_head_ooo_new"));

    let _ = ing.stop().await;
}

// ---------------------------------------------------------------------------
// 3. [W21-DF-03] RefLeaksTracker on Kafka records, /metrics idle drain, 1000 cap, page size
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w21_df_03_ref_leaks_kafka_records_collector_drain_cap_1000_and_page_size() {
    // Part A: Kafka ingest_storage records (v0, v1, v2 instrumented; unsupported v99 skipped)
    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let ing = new_running_ingester(dir.path(), default_limits_test_config(), Some(&reg));
    let leak_cfg = InstrumentRefLeaksConfig {
        percentage: 100.0,
        before_reuse_period: Duration::from_secs(60),
        max_inflight_instrumented_bytes: 100 * 1024 * 1024,
    };
    let tracker = leak_cfg.tracker(Some(&reg));
    ing.set_ref_leaks_tracker(tracker.clone());

    let consumer = IngesterShardedBatchConsumer::new(
        Arc::clone(&ing),
        mimir_config::IngestStorageConfig::default().kafka,
        PusherConsumerMetrics::new(Some(&reg)),
    );

    let ts = now_ms();
    let mk_wr = |name: &str| cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", name)],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts,
                value: 1.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let rec_v0 = {
        let (s, _) = RecordSerializer::new(RecordVersion::V0)
            .to_records("topic", 0, "t-leaks", &mk_wr("m_v0"), 64 << 20)
            .unwrap();
        FetchedRecord {
            key: s[0].key.clone(),
            value: s[0].value.clone(),
            headers: s[0].headers.clone(),
            offset: 1,
            timestamp_ms: ts,
        }
    };
    let rec_v1 = {
        let (s, _) = RecordSerializer::new(RecordVersion::V1)
            .to_records("topic", 0, "t-leaks", &mk_wr("m_v1"), 64 << 20)
            .unwrap();
        FetchedRecord {
            key: s[0].key.clone(),
            value: s[0].value.clone(),
            headers: s[0].headers.clone(),
            offset: 2,
            timestamp_ms: ts,
        }
    };
    let rec_v2 = {
        let (s, _) = RecordSerializer::new(RecordVersion::V2)
            .to_records("topic", 0, "t-leaks", &mk_wr("m_v2"), 64 << 20)
            .unwrap();
        FetchedRecord {
            key: s[0].key.clone(),
            value: s[0].value.clone(),
            headers: s[0].headers.clone(),
            offset: 3,
            timestamp_ms: ts,
        }
    };
    let rec_v99 = FetchedRecord {
        key: Bytes::from_static(b"t-leaks"),
        value: Bytes::from_static(b"ignored"),
        headers: vec![("Version".to_string(), Bytes::copy_from_slice(&99u32.to_be_bytes()))],
        offset: 4,
        timestamp_ms: ts,
    };

    consumer
        .consume_records_sync(&[rec_v0, rec_v1, rec_v2, rec_v99])
        .unwrap();
    assert_eq!(tracker.instrumented_buffers_total(), 3.0);
    let inflight = tracker.inflight_instrumented_bytes() as usize;
    assert!(inflight >= 3 * 4096 && inflight.is_multiple_of(3));
    let expected_page = inflight / 3;
    assert!(expected_page.is_power_of_two());

    // Part B: idle /metrics collect() drains expired delayed_unmap buffers without new traffic
    let reg_idle = Registry::new();
    let cfg_idle = InstrumentRefLeaksConfig {
        percentage: 100.0,
        before_reuse_period: Duration::from_millis(30),
        max_inflight_instrumented_bytes: 10 * 1024 * 1024,
    };
    let tracker_idle = cfg_idle.tracker(Some(&reg_idle));
    let tb = tracker_idle.maybe_instrument_ref_leaks(b"hello").unwrap();
    tb.free();
    let before = render_metrics(&reg_idle);
    assert!(before.contains(&format!(
        "mimir_reference_leaks_inflight_instrumented_bytes {expected_page}"
    )));
    tokio::time::sleep(Duration::from_millis(60)).await;
    // Gather directly via Registry (simulating /metrics scrape while idle)
    let after = render_metrics(&reg_idle);
    assert!(
        after.contains("mimir_reference_leaks_inflight_instrumented_bytes 0"),
        "expected /metrics collect() to drain expired buffers: {after}"
    );

    // Part C: 1000-slot unmapQueue cap unmaps extras immediately
    let reg_cap = Registry::new();
    let cfg_cap = InstrumentRefLeaksConfig {
        percentage: 100.0,
        before_reuse_period: Duration::from_secs(600),
        max_inflight_instrumented_bytes: 0,
    };
    let tracker_cap = cfg_cap.tracker(Some(&reg_cap));
    for _ in 0..1005 {
        let tb = tracker_cap.maybe_instrument_ref_leaks(b"x").unwrap();
        tb.free();
    }
    assert_eq!(tracker_cap.instrumented_buffers_total(), 1005.0);
    assert_eq!(
        tracker_cap.inflight_instrumented_bytes(),
        (1000 * expected_page) as f64,
        "delayed_unmap must cap at 1000 entries and unmap extras immediately"
    );
}

// ---------------------------------------------------------------------------
// 4. [W21-DF-08] ReactiveLimiter deadline expiry increments rejected_requests_total
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w21_df_08_reactive_limiter_deadline_expiry_increments_rejected_requests() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    for rl in [&mut cfg.push_reactive_limiter, &mut cfg.read_reactive_limiter] {
        rl.enabled = true;
        rl.initial_limit = 1;
        rl.min_limit = 1;
        rl.max_limit = 1;
    }
    let reg = Registry::new();
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        Some(&reg),
    )
    .unwrap();
    ing.set_state_for_test(State::Running);

    // Saturate both push and read reactive limiters (capacity 1)
    let _held_push = ing.reactive_limiter.push.as_ref().unwrap().acquire_permit().await.unwrap();
    let _held_read = ing.reactive_limiter.read.as_ref().unwrap().acquire_permit().await.unwrap();

    let svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // Push with 25ms RpcDeadlineContext
    let push_body = unframed(&push_req_bytes("m_df08", now_ms()));
    let mut push_req = http::Request::builder()
        .method("POST")
        .uri("/cortex.Ingester/Push")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-scope-orgid", "t-df08")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(push_body))))
        .unwrap();
    push_req
        .extensions_mut()
        .insert(RpcDeadlineContext::new(Some(Instant::now() + Duration::from_millis(25))));
    let push_resp = tokio::time::timeout(Duration::from_secs(2), svc.clone().oneshot(push_req))
        .await
        .expect("push must not block past deadline")
        .unwrap();
    let push_status = tonic::Status::from_header_map(push_resp.headers()).expect("grpc-status");
    assert_eq!(push_status.code(), tonic::Code::Unavailable);

    // QueryStream (read) with 25ms RpcDeadlineContext
    let q_body = unframed(&ingesterpb::QueryRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 1000,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "m_df08".to_string(),
        }],
        streaming_chunks_batch_size: 0,
    }
    .encode_to_vec());
    let mut read_req = http::Request::builder()
        .method("POST")
        .uri("/cortex.Ingester/QueryStream")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-scope-orgid", "t-df08")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(q_body))))
        .unwrap();
    read_req
        .extensions_mut()
        .insert(RpcDeadlineContext::new(Some(Instant::now() + Duration::from_millis(25))));
    let read_resp = tokio::time::timeout(Duration::from_secs(2), svc.oneshot(read_req))
        .await
        .expect("read must not block past deadline")
        .unwrap();
    let read_status = tonic::Status::from_header_map(read_resp.headers()).expect("grpc-status");
    assert_eq!(read_status.code(), tonic::Code::Unavailable);

    let metrics_txt = render_metrics(&reg);
    assert!(
        metrics_txt.contains(
            r#"cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests"} 1"#
        ),
        "metrics:
{metrics_txt}"
    );
    assert!(
        metrics_txt.contains(
            r#"cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_read_requests"} 1"#
        ),
        "metrics:
{metrics_txt}"
    );
}

// ---------------------------------------------------------------------------
// 5. [W21-DF-09] Empty wire label values & mid-series early-error attribution
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w21_df_09_cost_attribution_empty_label_value_and_early_error_flush() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;

    let mut limits = default_limits_test_config();
    limits.max_global_series_per_user = 1;
    limits.cost_attribution_trackers.insert(
        "cost-attribution".to_string(),
        CostAttributionTrackerConfig {
            labels: vec![CostAttributionLabel {
                input: "team".to_string(),
                output: "team".to_string(),
            }],
            internal: false,
        },
    );
    let overrides = Overrides::new(limits);
    let reg = Registry::new();
    let cost_reg = Registry::new();
    let cost_mgr = CostAttributionManager::new(
        Duration::from_secs(60),
        overrides.clone(),
        Some(&reg),
        &cost_reg,
    )
    .unwrap();
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        overrides,
        None,
        None,
        None,
        None,
        None,
        Some(&reg),
    )
    .unwrap();
    ing.set_state_for_test(State::Running);
    ing.set_cost_attribution_manager(Some(Arc::clone(&cost_mgr)));

    let ts = now_ms();
    // Establish Head min_append_time by pushing 1 valid series (hits max_global_series_per_user = 1).
    let init_wr = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "init_m"),
                cortexpb::LabelPair::from_static("team", "seed"),
            ],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts,
                value: 1.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    ing.push_grpc("u-df09", &init_wr).await.unwrap();

    // Part A: Push {__name__="m_empty", team=""} with sample-too-far-in-future on BOTH raw & borrowed paths.
    let empty_team_wr = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "m_empty"),
                cortexpb::LabelPair::from_static("team", ""),
            ],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts + 10 * 3600 * 1000,
                value: 2.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let _ = ing.push_grpc("u-df09", &empty_team_wr).await;
    let _ = ing
        .push_grpc_raw("u-df09", Bytes::from(empty_team_wr.encode_to_vec()))
        .await;

    // Part B: Mid-series hard error: sample 1 is sample-too-far-in-future, sample 2 hits MaxSeriesPerUser!
    let mid_err_wr = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "m_hard_err"),
                cortexpb::LabelPair::from_static("team", "alpha"),
            ],
            samples: vec![
                cortexpb::Sample {
                    timestamp_ms: ts + 10 * 3600 * 1000,
                    value: 1.0,
                },
                cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 2.0,
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(ing.push_grpc("u-df09", &mid_err_wr).await.is_err());
    assert!(ing
        .push_grpc_raw("u-df09", Bytes::from(mid_err_wr.encode_to_vec()))
        .await
        .is_err());

    let cost_txt = render_metrics(&cost_reg);
    assert!(
        cost_txt.contains(
            r#"cortex_discarded_attributed_samples_total{reason="sample-too-far-in-future",team="",tenant="u-df09",tracker="cost-attribution"} 2"#
        ),
        "empty wire label value must be attributed as empty string, not __missing__:\n{cost_txt}"
    );
    assert!(
        !cost_txt.contains(r#"team="__missing__""#),
        "must not misattribute empty team value as __missing__:\n{cost_txt}"
    );
    assert!(
        cost_txt.contains(
            r#"cortex_discarded_attributed_samples_total{reason="sample-too-far-in-future",team="alpha",tenant="u-df09",tracker="cost-attribution"} 2"#
        ),
        "mid-series hard error must still flush discarded sample attribution:\n{cost_txt}"
    );
}

// ---------------------------------------------------------------------------
// 6. [W21-DF-10] Postings cache invalidations gated until after open + compact
// ---------------------------------------------------------------------------
#[test]
fn w21_df_10_postings_cache_invalidations_gated_until_after_open_and_compact() {
    let dir = TempDir::new().unwrap();
    let (_, mut blocks_cfg, _) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.ship_interval = GoDuration(0);
    blocks_cfg.tsdb.shared_postings_for_matchers_cache = true;
    blocks_cfg.tsdb.head_postings_for_matchers_cache_invalidation = true;
    blocks_cfg.tsdb.head_postings_for_matchers_cache_versions = 4096;

    let overrides = Overrides::new(default_limits_test_config());
    let limiter = Arc::new(mimir_ingester::limiter::Limiter::new(
        overrides.clone(),
        Arc::new(mimir_ingester::limiter::NoopRingLimiterStrategy),
    ));

    let inv_count = |reg: &Registry| -> u64 {
        reg.gather()
            .into_iter()
            .find(|mf| {
                mf.get_name()
                    == "cortex_ingester_tsdb_head_postings_for_matchers_cache_invalidations_total"
            })
            .map(|mf| mf.get_metric()[0].get_counter().get_value() as u64)
            .unwrap_or(0)
    };

    // Session 1: push 2 series -> 2 invalidations
    {
        let reg1 = Registry::new();
        let shared1 = SharedTsdbMetrics::new(Some(&reg1));
        let db1 = UserTsdb::open(
            "t-df10".to_string(),
            &mimir_config::IngesterConfig::default(),
            &blocks_cfg,
            overrides.clone(),
            Arc::clone(&limiter),
            None,
            ShipperMetrics::new(Some(&reg1)),
            &shared1,
            Some(Arc::new(Registry::new())),
            1,
            Arc::new(AtomicI64::new(0)),
            Arc::new(|| 0),
            CounterVec::new(Opts::new("rej1", "h"), &["reason"]).unwrap(),
            HashSet::new(),
            -1,
            1,
            None,
        )
        .unwrap();
        let mut app = db1.db.appender();
        app.append_float(None, &[("__name__", "m1")], 1_000, 1.0).unwrap();
        app.append_float(None, &[("__name__", "m2")], 2_000, 2.0).unwrap();
        app.commit().unwrap();
        assert_eq!(inv_count(&reg1), 2);
        db1.db.close().unwrap();
    }

    // Session 2: reopen on same WAL dir -> WAL replay + initial compact() must report 0 invalidations!
    let reg2 = Registry::new();
    let shared2 = SharedTsdbMetrics::new(Some(&reg2));
    let db2 = UserTsdb::open(
        "t-df10".to_string(),
        &mimir_config::IngesterConfig::default(),
        &blocks_cfg,
        overrides,
        limiter,
        None,
        ShipperMetrics::new(Some(&reg2)),
        &shared2,
        Some(Arc::new(Registry::new())),
        1,
        Arc::new(AtomicI64::new(0)),
        Arc::new(|| 0),
        CounterVec::new(Opts::new("rej2", "h"), &["reason"]).unwrap(),
        HashSet::new(),
        -1,
        1,
        None,
    )
    .unwrap();
    assert_eq!(
        inv_count(&reg2),
        0,
        "WAL replay + initial compact must not increment head postings cache invalidations_total"
    );

    // Post-open creation increments by 1
    {
        let mut app = db2.db.appender();
        app.append_float(None, &[("__name__", "m3")], 10_000_000, 3.0)
            .unwrap();
        app.commit().unwrap();
    }
    assert_eq!(inv_count(&reg2), 1);

    // Head GC deletion of <= 64 series must NOT increment invalidations_total
    db2.db.head().truncate_memory(5_000_000).unwrap();
    assert_eq!(
        inv_count(&reg2),
        1,
        "Head GC deletion must not increment head postings cache invalidations_total"
    );
}

// ---------------------------------------------------------------------------
// 7. [W21-DF-11] OwnedSeriesService cancellation + error! logging
// ---------------------------------------------------------------------------
struct ErrorRingStrategy;

impl OwnedSeriesRingStrategy for ErrorRingStrategy {
    fn check_ring_for_changes(&self) -> Result<bool, String> {
        Err("simulated ring read error".to_string())
    }

    fn shard_size_for_user(&self, _tenant: &str) -> usize {
        1
    }

    fn token_ranges_for_user(&self, _user_id: &str, _shard_size: usize) -> Result<Vec<u32>, String> {
        Ok(vec![0, u32::MAX])
    }

    fn owner_key_and_value(&self) -> (&'static str, String) {
        ("ingester", "test-0".to_string())
    }
}

struct EventRecorder(Arc<StdMutex<Vec<(tracing::Level, String)>>>);

impl tracing::Subscriber for EventRecorder {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut msg = String::new();
        struct V<'a>(&'a mut String);
        impl tracing::field::Visit for V<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    use std::fmt::Write;
                    let _ = write!(self.0, "{value:?}");
                }
            }
        }
        event.record(&mut V(&mut msg));
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), msg));
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn w21_df_11_owned_series_cancellable_and_logs_ring_error_at_error_level() {
    let dir = TempDir::new().unwrap();
    let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
    let ts = now_ms();
    ing.push_grpc("u1", &cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("__name__", "os_m")],
            samples: vec![cortexpb::Sample { timestamp_ms: ts, value: 1.0 }],
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .unwrap();

    let events = Arc::new(StdMutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(EventRecorder(Arc::clone(&events)));

    // 1. Canceled ServiceContext exits update_all_tenants_with_ctx immediately without updating tenants
    let ing_users = Arc::clone(&ing);
    let ing_get = Arc::clone(&ing);
    let oss = OwnedSeriesService::new(
        Duration::from_millis(20),
        Arc::new(ErrorRingStrategy),
        None,
        Arc::new(move || ing_users.tsdbs.iter().map(|e| e.key().clone()).collect()),
        Arc::new(move |u| ing_get.get_tsdb(u)),
    );
    let canceled_ctx = ServiceContext::background();
    canceled_ctx.cancel();
    assert_eq!(oss.update_all_tenants_with_ctx(true, Some(&canceled_ctx)), 0);

    // 2. Running logs error! on check_ring_for_changes() failure.
    struct EmptyThenErrStrategy(AtomicUsize);
    impl OwnedSeriesRingStrategy for EmptyThenErrStrategy {
        fn check_ring_for_changes(&self) -> Result<bool, String> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("ring is empty".to_string())
            } else {
                Err("kv store unavailable".to_string())
            }
        }
        fn shard_size_for_user(&self, _: &str) -> usize {
            1
        }
        fn token_ranges_for_user(&self, _: &str, _: usize) -> Result<Vec<u32>, String> {
            Ok(vec![0, u32::MAX])
        }
        fn owner_key_and_value(&self) -> (&'static str, String) {
            ("ingester", "test-0".to_string())
        }
    }

    let oss2 = OwnedSeriesService::new(
        Duration::from_millis(20),
        Arc::new(EmptyThenErrStrategy(AtomicUsize::new(0))),
        None,
        Arc::new(Vec::new),
        Arc::new(|_| None),
    );
    oss2.start_and_await_running().await.unwrap();
    tokio::time::sleep(Duration::from_millis(160)).await;
    oss2.stop_and_await_terminated().await.unwrap();

    let logged_error = events.lock().unwrap().iter().any(|(lvl, msg)| {
        *lvl == tracing::Level::ERROR && msg.contains("can't check ring for updates")
    });
    assert!(
        logged_error,
        "expected ERROR log 'can't check ring for updates', got: {:?}",
        *events.lock().unwrap()
    );
}

// ---------------------------------------------------------------------------
// 8. [W21-APS-01] Gorilla mux GET-only routes reject HEAD with 405 without side effects
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w21_aps_01_head_requests_return_405_without_side_effects() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    cfg.label_churn_enabled = true;
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_state_for_test(State::Running);

    let ts = now_ms();
    ing.push_grpc("t-aps01", &cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("__name__", "aps01_m")],
            samples: vec![cortexpb::Sample { timestamp_ms: ts, value: 1.0 }],
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .unwrap();

    let app = ingester_router(Arc::clone(&ing));

    for path in [
        "/ingester/flush",
        "/ingester/prepare-shutdown",
        "/ingester/prepare-partition-downscale",
        "/ingester/prepare-instance-ring-downscale",
        "/ingester/unregister-on-shutdown",
        "/ingester/tsdb_metrics",
        "/ingester/tenants",
        "/ingester/tsdb/t-aps01",
        "/ingester/label_churn/t-aps01",
    ] {
        let resp = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("HEAD")
                    .uri(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            http::StatusCode::METHOD_NOT_ALLOWED,
            "HEAD {path} must return 405 Method Not Allowed"
        );
    }

    // Verify HEAD /ingester/flush did NOT compact/flush TSDB into blocks!
    assert!(ing.get_tsdb("t-aps01").unwrap().db.blocks().is_empty());

    // Verify GET still works
    let resp_get = app
        .oneshot(
            http::Request::builder()
                .method("GET")
                .uri("/ingester/tenants")
                .header("accept", "application/json")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_get.status(), http::StatusCode::OK);
}

// ---------------------------------------------------------------------------
// 9. [W21-APS-06] label_churn_handler: go_parse_query, signed pre-1970 since_ms, spawn_blocking
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w21_aps_06_label_churn_go_parse_query_and_pre_1970_since_ms() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    cfg.label_churn_enabled = true;
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_state_for_test(State::Running);

    let ts = now_ms();
    ing.push_grpc("t-aps06", &cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "aps06_m"),
                cortexpb::LabelPair::from_static("env", "prod"),
            ],
            samples: vec![cortexpb::Sample { timestamp_ms: ts, value: 1.0 }],
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .unwrap();

    let db = ing.get_tsdb("t-aps06").unwrap();
    // Record a pre-1970 timestamp (-5_000 ms) AND a recent timestamp so both default 30m and pre-1970 100y work!
    db.series_ref_history.record(-5_000, 0);
    db.series_ref_history.record(ts - 1_000, 0);

    let app = ingester_router(Arc::clone(&ing));

    // Semicolon / invalid percent-escape dropped by go_parse_query -> falls back to default 200 OK
    for bad_q in ["lookback=%ZZ", "lookback=10m;x=1", "limit=5;x=1"] {
        let resp = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("GET")
                    .uri(format!("/ingester/label_churn/t-aps06?{bad_q}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            http::StatusCode::OK,
            "query={bad_q} should match Go url.ParseQuery fallback 200 OK"
        );
    }

    // Remove the recent record by replacing history? Wait: test pre-1970 on a second tenant with ONLY pre-1970 record!
    ing.push_grpc("t-aps06-pre", &cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("__name__", "aps06_pre")],
            samples: vec![cortexpb::Sample { timestamp_ms: ts, value: 1.0 }],
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .unwrap();
    let db_pre = ing.get_tsdb("t-aps06-pre").unwrap();
    db_pre.series_ref_history.record(-5_000, 0);

    // With default last_series_removal_ms=0, 100y (since_ms < 0) sees last_removal (0) > since_ms -> 503 CompactedTooRecently!
    let resp_comp = app
        .clone()
        .oneshot(
            http::Request::builder()
                .method("GET")
                .uri("/ingester/label_churn/t-aps06-pre?lookback=876000h")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status_comp = resp_comp.status();
    let body_comp = to_bytes(resp_comp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(status_comp, http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        String::from_utf8_lossy(&body_comp).contains("compaction removed series"),
        "expected CompactedTooRecently proving signed since_ms < 0, got: {}",
        String::from_utf8_lossy(&body_comp)
    );

    // With last_series_removal_ms = i64::MIN, pre-1970 signed since_ms finds -5_000ms record -> 200 OK!
    db_pre
        .last_series_removal_ms
        .store(i64::MIN, Ordering::Release);
    let resp_pre = app
        .oneshot(
            http::Request::builder()
                .method("GET")
                .uri("/ingester/label_churn/t-aps06-pre?lookback=876000h")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status_pre = resp_pre.status();
    let body_pre = to_bytes(resp_pre.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status_pre,
        http::StatusCode::OK,
        "pre-1970 signed since_ms must find -5000ms record, got: {}",
        String::from_utf8_lossy(&body_pre)
    );
}
