//! Wave 22 Regression Tests:
//! - W22-RES-02: QueryStream with index_lookup_planning + mirror enabled and OOO Head chunks
//!   matches Go HeadAndOOOChunkQuerier (cluster merge + Native Histogram CounterResetHeader,
//!   single LookupPlanner + PostingsForMatchers call, mirror outcome="no_select").

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mimir_config::ModelDuration;
use mimir_ingester::lookupplan::{BasicLookupPlan, LookupPlanner, SelectHints};
use mimir_ingester::mirror_querier::ChunkQuerier;
use mimir_ingester::query::HeadAndOOOChunkQuerier;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_tsdb::chunkenc::{CounterResetHeader, EncodedChunk, HistogramChunk};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::{Registry, TextEncoder};
use tempfile::TempDir;
use tonic::metadata::MetadataMap;

use common::{
    default_ingester_test_config, default_limits_test_config,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

fn mk_int_hist_proto(ts: i64, count: u64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
        zero_threshold: 0.001,
        sum: count as f64 * 1.5,
        schema: 0,
        positive_spans: vec![cortexpb::BucketSpan {
            offset: 0,
            length: 1,
        }],
        positive_deltas: vec![count as i64],
        reset_hint: cortexpb::histogram::ResetHint::No as i32,
        timestamp: ts,
        ..Default::default()
    }
}

fn counter_value(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                let all_match = labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == *k && lp.get_value() == *v)
                });
                if all_match {
                    return m.get_counter().get_value();
                }
            }
        }
    }
    0.0
}

struct SplitScanPlanner {
    calls: Arc<AtomicUsize>,
}

impl LookupPlanner for SplitScanPlanner {
    fn plan_index_lookup(
        &self,
        plan: &BasicLookupPlan,
        _hints: Option<&SelectHints>,
    ) -> Result<BasicLookupPlan, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut index_matchers = Vec::new();
        let mut scan_matchers = Vec::new();
        for m in &plan.index_matchers {
            if m.match_type == MatchType::RegexMatch {
                scan_matchers.push(m.clone());
            } else {
                index_matchers.push(m.clone());
            }
        }
        Ok(BasicLookupPlan::new(index_matchers, scan_matchers))
    }

    fn planner_kind(&self) -> &'static str {
        "split-scan-test"
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w22_res_02_query_stream_planning_and_mirror_head_ooo_parity() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 1.0;
    blocks_cfg.tsdb.head_postings_for_matchers_cache_force = true;

    let mut limits = default_limits_test_config();
    limits.out_of_order_time_window = ModelDuration(3600 * 1_000_000_000);

    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user = "user-w22-res02";
    let labels = vec![
        cortexpb::LabelPair::from_strs("__name__", "w22_hist"),
        cortexpb::LabelPair::from_strs("env", "prod"),
        cortexpb::LabelPair::from_strs("job", "api"),
    ];

    // Push in-order native histogram samples at t=1000 (count=10) and t=2000 (count=20).
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: labels.clone(),
                histograms: vec![mk_int_hist_proto(1_000, 10), mk_int_hist_proto(2_000, 20)],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Push OOO native histogram sample at t=1500 (count=25), creating a counter reset
    // between (t=1500, count=25) and (t=2000, count=20) once merged!
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: labels.clone(),
                histograms: vec![mk_int_hist_proto(1_500, 25)],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // 1. Query [0, 3_000] overlapping OOO Head:
    //    - Must merge overlapping in-order + OOO Head chunks into 2 chunks:
    //      Chunk 1: [(t=1000, count=10), (t=1500, count=25)]
    //      Chunk 2: [(t=2000, count=20)] with CounterResetHeader::CounterReset!
    //    - Must hit postings_for_matchers_cache_requests_total ONLY ONCE (not twice).
    //    - Must record mirror outcome="no_select" (since HeadAndOOOChunkQuerier bypasses
    //      inner RangeHead Select), and increment queried_blocks{generation="0"}.
    let res = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 3_000,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "w22_hist".to_string(),
                }],
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();

    let all_series: Vec<&ingesterpb::QueryStreamSeries> =
        res.iter().flat_map(|r| r.streaming_series.iter()).collect();
    assert_eq!(all_series.len(), 1);
    let all_chunks: Vec<&ingesterpb::Chunk> = res
        .iter()
        .flat_map(|r| r.streaming_series_chunks.iter())
        .flat_map(|sc| sc.chunks.iter())
        .collect();
    assert_eq!(
        all_chunks.len(),
        2,
        "overlapping in-order + OOO histogram Head chunks must merge and split cleanly at counter reset into 2 chunks"
    );

    let chk0 = HistogramChunk::from_bytes(all_chunks[0].data.to_vec());
    let chk1 = HistogramChunk::from_bytes(all_chunks[1].data.to_vec());
    assert_eq!(
        chk1.counter_reset_header(),
        CounterResetHeader::CounterReset,
        "chunk containing t=2000 (count=20 < OOO t=1500 count=25) must carry CounterResetHeader::CounterReset"
    );
    let s0 = decode_chunk_samples(&EncodedChunk::Histogram(chk0)).unwrap();
    let s1 = decode_chunk_samples(&EncodedChunk::Histogram(chk1)).unwrap();
    let counts0: Vec<(i64, u64)> = s0
        .iter()
        .map(|s| match &s.value {
            SampleValue::Histogram(h) => (s.t, h.count),
            other => panic!("expected histogram, got {other:?}"),
        })
        .collect();
    let counts1: Vec<(i64, u64)> = s1
        .iter()
        .map(|s| match &s.value {
            SampleValue::Histogram(h) => (s.t, h.count),
            other => panic!("expected histogram, got {other:?}"),
        })
        .collect();
    assert_eq!(counts0, vec![(1_000, 10), (1_500, 25)]);
    assert_eq!(counts1, vec![(2_000, 20)]);

    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_tsdb_head_postings_for_matchers_cache_requests_total",
            &[]
        ),
        1.0,
        "HeadAndOOOChunkQuerier must query Head PostingsForMatchersCache only once"
    );
    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "no_select"), ("user", user)]
        ),
        1.0,
        "MirroredChunkQuerier wrapped under HeadAndOOOChunkQuerier must record outcome=no_select"
    );
    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "success"), ("user", user)]
        ),
        0.0,
    );
    assert_eq!(
        counter_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        1.0,
    );

    // 2. Query [1_800, 3_000] NOT overlapping OOO Head (t=1500):
    //    MirroredChunkQuerier IS invoked on RangeHead and must record outcome="success".
    let res_no_ooo = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 1_800,
                end_timestamp_ms: 3_000,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "w22_hist".to_string(),
                }],
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    let no_ooo_chunks: usize = res_no_ooo
        .iter()
        .flat_map(|r| r.streaming_series_chunks.iter())
        .map(|sc| sc.chunks.len())
        .sum();
    assert_eq!(no_ooo_chunks, 1);
    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "success"), ("user", user)]
        ),
        1.0,
    );

    // 3. Verify HeadAndOOOChunkQuerier invokes LookupPlanner and applies scan_matchers:
    let db = ing.get_tsdb(user).unwrap();
    {
        let mut app = db.db.appender();
        app.append_float(
            None,
            &[("__name__", "w22_hist"), ("env", "dev"), ("job", "api")],
            1_200,
            42.0,
        )
        .unwrap();
        app.commit().unwrap();
    }
    let planner_calls = Arc::new(AtomicUsize::new(0));
    let planner: Arc<dyn LookupPlanner> = Arc::new(SplitScanPlanner {
        calls: Arc::clone(&planner_calls),
    });

    let mut q_ooo = HeadAndOOOChunkQuerier::new(
        Arc::clone(&db.db),
        0,
        3_000,
        planner,
        None,
    );
    let matchers = [
        LabelMatcher::new(MatchType::Equal, "__name__", "w22_hist").unwrap(),
        LabelMatcher::new(MatchType::RegexMatch, "env", "prod|staging").unwrap(),
    ];
    let mut ss = q_ooo.select(true, None, &matchers);
    let mut matched_envs = Vec::new();
    while ss.next() {
        let s = ss.at().unwrap();
        matched_envs.push(s.labels().get("env").to_string());
    }
    assert!(ss.err().is_none());
    drop(ss);
    q_ooo.close().unwrap();

    assert_eq!(planner_calls.load(Ordering::SeqCst), 1);
    assert_eq!(matched_envs, vec!["prod".to_string()]);

    let _ = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
    let _ = ing.stop().await;
}
