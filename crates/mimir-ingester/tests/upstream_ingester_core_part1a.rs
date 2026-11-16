//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_test.go
//! (lines 103..1931 and lines 4600..4747).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mimir_common::services::State;
use mimir_config::{
    BlocksStorageConfig, CircuitBreakerConfig, GoDuration, IngestStorageConfig, IngesterConfig,
    InstanceLimits, LimitsConfig, ModelDuration, Overrides,
};
use mimir_ingester::ingester::UtilizationBasedLimiter;
use mimir_ingester::metrics::IngesterMetrics;
use mimir_ingester::push::PushPermit;
use mimir_ingester::{Ingester, IngesterError};
use mimir_proto::compat::LabelAdapter;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::timeseries::{new_write_request, to_write_request};
use mimir_proto::zerocopy::{
    decode_write_request, encode_write_request_rw2_bytes, DecodeOptions, WriteRequestScratch,
};
use mimir_tsdb::chunkenc::{CounterResetHint, EncodedChunk, Encoding, Histogram, HistogramSpan};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use prometheus::proto::{MetricFamily, MetricType};
use prometheus::Registry;
use tonic::metadata::MetadataMap;
use tonic::Code;

const USER_ID: &str = "test";
const METRIC_NAME_LABEL: &str = "__name__";

fn default_ingester_test_config(tmp: &Path) -> (IngesterConfig, BlocksStorageConfig) {
    let mut cfg = IngesterConfig::default();
    cfg.active_series.active_series_metrics_enabled = true;
    cfg.ring.instance_id = "localhost".to_string();
    cfg.ring.replication_factor = 1;

    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.join("tsdb").to_string_lossy().into_owned();
    blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
    blocks_cfg.bucket.backend_cfg.filesystem.dir =
        tmp.join("bucket").to_string_lossy().into_owned();
    blocks_cfg.tsdb.head_compaction_interval_jitter_enabled = false;
    std::fs::create_dir_all(&blocks_cfg.tsdb.dir).unwrap();
    std::fs::create_dir_all(&blocks_cfg.bucket.backend_cfg.filesystem.dir).unwrap();
    (cfg, blocks_cfg)
}

fn default_limits_test_config() -> LimitsConfig {
    LimitsConfig {
        max_global_series_per_user: 0,
        max_global_series_per_metric: 0,
        max_global_metadata_per_user: 0,
        max_global_metadata_per_metric: 0,
        ..Default::default()
    }
}

async fn prepare_ingester_with_blocks_storage_and_limits(
    cfg: IngesterConfig,
    blocks_cfg: BlocksStorageConfig,
    limits: LimitsConfig,
    registry: Option<&Registry>,
) -> Arc<Ingester> {
    let ov = Overrides::new(limits);
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        ov,
        None,
        None,
        None,
        None,
        None,
        registry,
    )
    .expect("create ingester");
    ing.start().await.expect("start ingester");
    ing
}

fn generate_test_histogram(i: i64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: (2 + i) as u64,
        count: (12 + i * 9) as u64,
        sum: 18.4 * (i + 1) as f64,
        positive_spans: vec![
            HistogramSpan {
                offset: 0,
                length: 2,
            },
            HistogramSpan {
                offset: 1,
                length: 2,
            },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![
            HistogramSpan {
                offset: 0,
                length: 2,
            },
            HistogramSpan {
                offset: 1,
                length: 2,
            },
        ],
        negative_buckets: vec![i + 1, 1, -1, 0],
        custom_values: Vec::new(),
    }
}

fn generate_test_gauge_histogram(i: i64) -> Histogram {
    let mut h = generate_test_histogram(i);
    h.counter_reset_hint = CounterResetHint::GaugeType;
    h
}

fn int_histogram_to_proto(ts: i64, h: &Histogram) -> cortexpb::Histogram {
    let spans = |ss: &[HistogramSpan]| -> Vec<cortexpb::BucketSpan> {
        ss.iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect()
    };
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(h.count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(h.zero_count)),
        zero_threshold: h.zero_threshold,
        sum: h.sum,
        schema: h.schema,
        positive_spans: spans(&h.positive_spans),
        positive_deltas: h.positive_buckets.clone(),
        negative_spans: spans(&h.negative_spans),
        negative_deltas: h.negative_buckets.clone(),
        reset_hint: h.counter_reset_hint as i32,
        timestamp: ts,
        custom_values: h.custom_values.clone(),
        ..Default::default()
    }
}

fn add_histogram_series(
    req: &mut cortexpb::WriteRequest,
    lbls: &[Vec<LabelAdapter>],
    histograms: &[cortexpb::Histogram],
    exemplars: Option<&[Option<cortexpb::Exemplar>]>,
) {
    for (i, h) in histograms.iter().enumerate() {
        let mut ts = cortexpb::TimeSeries {
            labels: lbls[i]
                .iter()
                .map(|l| cortexpb::LabelPair::from_strs(&l.name, &l.value))
                .collect(),
            histograms: vec![h.clone()],
            ..Default::default()
        };
        if let Some(exs) = exemplars {
            if let Some(Some(ex)) = exs.get(i) {
                ts.exemplars.push(ex.clone());
            }
        }
        req.timeseries.push(ts);
    }
}

fn add_exemplars_at(req: &mut cortexpb::WriteRequest, idx: usize, exemplars: &[cortexpb::Exemplar]) {
    req.timeseries[idx].exemplars.extend_from_slice(exemplars);
}

fn metric_value(families: &[MetricFamily], name: &str) -> Option<f64> {
    let mf = families.iter().find(|f| f.get_name() == name)?;
    let m = mf.get_metric().first()?;
    match mf.get_field_type() {
        MetricType::COUNTER => Some(m.get_counter().get_value()),
        MetricType::GAUGE => Some(m.get_gauge().get_value()),
        _ => None,
    }
}

// ============================================================================
// 1. TestIncrementDecrementIdleCompactionConcurrent
// ============================================================================

// Upstream: pkg/ingester/ingester_test.go::TestIncrementDecrementIdleCompactionConcurrent
#[test]
fn test_increment_decrement_idle_compaction_concurrent() {
    let reg = Registry::new();
    let inflight_req = Arc::new(AtomicI64::new(0));
    let inflight_bytes = Arc::new(AtomicI64::new(0));
    let metrics = Arc::new(IngesterMetrics::new(
        Some(&reg),
        false,
        inflight_req,
        inflight_bytes,
        || 0.0,
        || None,
        || 0.0,
        || 0.0,
        || 0.0,
    ));

    let families = reg.gather();
    assert_eq!(
        metric_value(&families, "cortex_ingester_tsdb_forced_compactions_in_progress"),
        Some(0.0)
    );

    const NUM_GOROUTINES: usize = 100;
    let mut handles = Vec::with_capacity(NUM_GOROUTINES);
    for _ in 0..NUM_GOROUTINES {
        let m = Arc::clone(&metrics);
        handles.push(std::thread::spawn(move || {
            m.increase_forced_compactions();
            std::thread::sleep(Duration::from_millis(10));
            m.decrease_forced_compactions();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(metrics.forced_compactions_count(), 0);
    let families = reg.gather();
    assert_eq!(
        metric_value(&families, "cortex_ingester_tsdb_forced_compactions_in_progress"),
        Some(0.0)
    );
}

// ============================================================================
// 2. TestConfig_Validate
// ============================================================================

// Upstream: pkg/ingester/ingester_test.go::TestConfig_Validate
#[test]
fn test_config_validate() {
    struct Case {
        name: &'static str,
        compartments_enabled: bool,
        num_read_compartments: i64,
        setup: fn(&mut IngesterConfig),
        expected_error: &'static str,
    }

    let cases = [
        Case {
            name: "default config passes validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |_| {},
            expected_error: "",
        },
        Case {
            name: "negative error sample rate fails validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.error_sample_rate = -1;
            },
            expected_error: "error sample rate cannot be a negative number",
        },
        Case {
            name: "tokenless mode with gRPC push enabled fails validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.ring.num_tokens = 0;
                cfg.push_grpc_method_enabled = true;
            },
            expected_error: "ring tokens can only be disabled when gRPC push is disabled",
        },
        Case {
            name: "tokenless mode with gRPC push disabled passes validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.ring.num_tokens = 0;
                cfg.push_grpc_method_enabled = false;
            },
            expected_error: "",
        },
        Case {
            name: "compartments disabled with a non-zero read compartment ID fails validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.read_compartment_id = 1;
            },
            expected_error: "ingester read compartment ID must be 0 when compartments are disabled",
        },
        Case {
            name: "compartments enabled with read compartment ID 0 passes validation",
            compartments_enabled: true,
            num_read_compartments: 2,
            setup: |cfg| {
                cfg.read_compartment_id = 0;
            },
            expected_error: "",
        },
        Case {
            name: "compartments enabled with an in-range read compartment ID passes validation",
            compartments_enabled: true,
            num_read_compartments: 2,
            setup: |cfg| {
                cfg.read_compartment_id = 1;
            },
            expected_error: "",
        },
        Case {
            name: "compartments enabled with a negative read compartment ID fails validation",
            compartments_enabled: true,
            num_read_compartments: 2,
            setup: |cfg| {
                cfg.read_compartment_id = -1;
            },
            expected_error: "ingester read compartment ID -1 is out of range [0, 2)",
        },
        Case {
            name: "compartments enabled with an out-of-range read compartment ID fails validation",
            compartments_enabled: true,
            num_read_compartments: 2,
            setup: |cfg| {
                cfg.read_compartment_id = 2;
            },
            expected_error: "ingester read compartment ID 2 is out of range [0, 2)",
        },
        Case {
            name: "positive max derived token partitions passes validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.partition_ring.max_derived_token_partitions = 128;
            },
            expected_error: "",
        },
        Case {
            name: "negative max derived token partitions fails validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.partition_ring.max_derived_token_partitions = -1;
            },
            expected_error: "-ingester.partition-ring.max-derived-token-partitions must be between 0 and 2147483647, got -1",
        },
        Case {
            name: "max derived token partitions above math.MaxInt32 fails validation",
            compartments_enabled: false,
            num_read_compartments: 0,
            setup: |cfg| {
                cfg.partition_ring.max_derived_token_partitions = (i32::MAX as i64) + 1;
            },
            expected_error: "-ingester.partition-ring.max-derived-token-partitions must be between 0 and 2147483647, got 2147483648",
        },
    ];

    for tc in cases {
        let mut cfg = IngesterConfig::default();
        (tc.setup)(&mut cfg);
        let res = cfg.validate(tc.compartments_enabled, tc.num_read_compartments);
        if tc.expected_error.is_empty() {
            assert!(res.is_ok(), "{}: unexpected err {:?}", tc.name, res);
        } else {
            let err = res.expect_err(&format!("{}: expected error", tc.name));
            let msg = err.to_string();
            assert!(
                msg.contains(tc.expected_error),
                "{}: error '{msg}' does not contain '{}'",
                tc.name,
                tc.expected_error
            );
        }
    }
}

// ============================================================================
// 3. TestIngester_StartPushRequest
// ============================================================================

// Upstream: pkg/ingester/ingester_test.go::TestIngester_StartPushRequest
#[tokio::test]
async fn test_ingester_start_push_request() {
    const REQ_SIZE: i64 = 10;

    #[derive(Clone, Copy)]
    enum ExpectedErr {
        Unavailable,
        CircuitBreakerOpen,
        InstanceLimitReached,
    }

    struct Case {
        name: &'static str,
        failing_ingester: bool,
        cb_open: bool,
        cb_initial_delay: Duration,
        instance_limit_reached: bool,
        has_existing_state: bool,
        expected_status: bool,
        expected_inflight_reqs: i64,
        expected_inflight_bytes: i64,
        expected_err: Option<ExpectedErr>,
    }

    let cases = [
        Case {
            name: "fail if ingester is not available for push",
            failing_ingester: true,
            cb_open: false,
            cb_initial_delay: Duration::ZERO,
            instance_limit_reached: false,
            has_existing_state: false,
            expected_status: false,
            expected_inflight_reqs: 0,
            expected_inflight_bytes: 0,
            expected_err: Some(ExpectedErr::Unavailable),
        },
        Case {
            name: "fail if circuit breaker is open",
            failing_ingester: false,
            cb_open: true,
            cb_initial_delay: Duration::ZERO,
            instance_limit_reached: false,
            has_existing_state: false,
            expected_status: false,
            expected_inflight_reqs: 0,
            expected_inflight_bytes: 0,
            expected_err: Some(ExpectedErr::CircuitBreakerOpen),
        },
        Case {
            name: "fail if instance limit is reached",
            failing_ingester: false,
            cb_open: false,
            cb_initial_delay: Duration::ZERO,
            instance_limit_reached: true,
            has_existing_state: false,
            expected_status: false,
            expected_inflight_reqs: 0,
            expected_inflight_bytes: 0,
            expected_err: Some(ExpectedErr::InstanceLimitReached),
        },
        Case {
            name: "do not fail if circuit breaker is not active",
            failing_ingester: false,
            cb_open: false,
            cb_initial_delay: Duration::from_secs(60),
            instance_limit_reached: false,
            has_existing_state: false,
            expected_status: true,
            expected_inflight_reqs: 1,
            expected_inflight_bytes: REQ_SIZE,
            expected_err: None,
        },
        Case {
            name: "do not fail and return the same context if it already contains a pushRequestState",
            failing_ingester: false,
            cb_open: false,
            cb_initial_delay: Duration::ZERO,
            instance_limit_reached: false,
            has_existing_state: true,
            expected_status: false,
            expected_inflight_reqs: 0,
            expected_inflight_bytes: 0,
            expected_err: None,
        },
        Case {
            name: "do not fail and add pushRequestState to the context if everything is ok",
            failing_ingester: false,
            cb_open: false,
            cb_initial_delay: Duration::ZERO,
            instance_limit_reached: false,
            has_existing_state: false,
            expected_status: true,
            expected_inflight_reqs: 1,
            expected_inflight_bytes: REQ_SIZE,
            expected_err: None,
        },
    ];

    for tc in cases {
        let tmp = tempfile::tempdir().unwrap();
        let (mut cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
        cfg.push_circuit_breaker = CircuitBreakerConfig {
            enabled: true,
            initial_delay: GoDuration::from_nanos(tc.cb_initial_delay.as_nanos() as i64),
            cooldown_period: GoDuration::from_secs(10),
            ..Default::default()
        };

        let limits = default_limits_test_config();
        let ing = prepare_ingester_with_blocks_storage_and_limits(
            cfg,
            blocks_cfg,
            limits,
            None,
        )
        .await;

        let instance_limits = if tc.instance_limit_reached {
            InstanceLimits {
                max_inflight_push_requests_bytes: 1,
                ..Default::default()
            }
        } else {
            InstanceLimits::default()
        };
        ing.set_instance_limits_fn(Arc::new(move || Some(instance_limits.clone())));

        if tc.failing_ingester {
            ing.stop().await.unwrap();
            assert_eq!(ing.state(), State::Terminated);
        } else {
            assert_eq!(ing.state(), State::Running);
        }

        if let Some(ref push_cb) = ing.circuit_breaker.push {
            if tc.cb_open {
                push_cb.force_open();
            } else {
                push_cb.force_close();
            }
        }

        let dummy_existing = PushPermit {
            started_at: Instant::now(),
            breaker_permit: None,
            reactive_permit: None,
            inflight_permit: None,
            ingester: &ing,
            req_size: REQ_SIZE,
            finished: true,
        };

        let existing_opt = if tc.has_existing_state {
            Some(&dummy_existing)
        } else {
            None
        };

        let res = ing.start_push_request_with_state(existing_opt, REQ_SIZE).await;
        match res {
            Ok((permit_opt, should_finish)) => {
                assert!(
                    tc.expected_err.is_none(),
                    "{}: expected error, got Ok",
                    tc.name
                );
                assert_eq!(should_finish, tc.expected_status, "{}", tc.name);
                assert_eq!(
                    ing.inflight_push_requests.load(Ordering::Acquire),
                    tc.expected_inflight_reqs,
                    "{}",
                    tc.name
                );
                assert_eq!(
                    ing.inflight_push_requests_bytes.load(Ordering::Acquire),
                    tc.expected_inflight_bytes,
                    "{}",
                    tc.name
                );
                if let Some(permit) = permit_opt {
                    permit.finish(None);
                }
            }
            Err(err) => {
                assert_eq!(
                    ing.inflight_push_requests.load(Ordering::Acquire),
                    tc.expected_inflight_reqs,
                    "{}",
                    tc.name
                );
                assert_eq!(
                    ing.inflight_push_requests_bytes.load(Ordering::Acquire),
                    tc.expected_inflight_bytes,
                    "{}",
                    tc.name
                );
                match tc.expected_err {
                    Some(ExpectedErr::Unavailable) => {
                        assert!(
                            matches!(err, IngesterError::Unavailable { .. }),
                            "{}: got {err:?}",
                            tc.name
                        );
                    }
                    Some(ExpectedErr::CircuitBreakerOpen) => {
                        assert!(
                            matches!(err, IngesterError::CircuitBreakerOpen { .. }),
                            "{}: got {err:?}",
                            tc.name
                        );
                    }
                    Some(ExpectedErr::InstanceLimitReached) => {
                        assert!(
                            matches!(err, IngesterError::InstanceLimitReached { .. }),
                            "{}: got {err:?}",
                            tc.name
                        );
                    }
                    None => panic!("{}: unexpected err {err:?}", tc.name),
                }
            }
        }

        // Prevent dummy_existing from being left with anything to leak (no-op drop).
        std::mem::forget(dummy_existing);
        let _ = ing.stop().await;
    }
}

// ============================================================================
// 4. TestIngester_StartReadRequest
// ============================================================================

struct FakeUtilizationLimiter(&'static str);

impl UtilizationBasedLimiter for FakeUtilizationLimiter {
    fn limiting_reason(&self) -> Option<String> {
        Some(self.0.to_string())
    }
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_StartReadRequest
#[tokio::test]
async fn test_ingester_start_read_request() {
    #[derive(Clone, Copy)]
    enum ExpectedErr {
        Unavailable,
        TooBusy,
        CircuitBreakerOpen,
    }

    enum Action {
        None,
        StopIngester,
        SetOverloaded,
        OpenReadCb,
        OpenPushCb,
        DeactivateReadCb,
    }

    struct Case {
        name: &'static str,
        action: Action,
        expected_acquired_permit_count: i64,
        expected_err: Option<ExpectedErr>,
    }

    let cases = [
        Case {
            name: "fail if ingester is not available for read, and do not acquire a permit",
            action: Action::StopIngester,
            expected_acquired_permit_count: 0,
            expected_err: Some(ExpectedErr::Unavailable),
        },
        Case {
            name: "fail if ingester is overloaded, and do not acquire a permit",
            action: Action::SetOverloaded,
            expected_acquired_permit_count: 0,
            expected_err: Some(ExpectedErr::TooBusy),
        },
        Case {
            name: "fail if read circuit breaker is open, and do not acquire a permit",
            action: Action::OpenReadCb,
            expected_acquired_permit_count: 0,
            expected_err: Some(ExpectedErr::CircuitBreakerOpen),
        },
        Case {
            name: "fail if push circuit breaker is open, and do not acquire a permit",
            action: Action::OpenPushCb,
            expected_acquired_permit_count: 0,
            expected_err: Some(ExpectedErr::CircuitBreakerOpen),
        },
        Case {
            name: "do not fail if read circuit breaker is not active, and do not acquire a permit",
            action: Action::DeactivateReadCb,
            expected_acquired_permit_count: 0,
            expected_err: None,
        },
        Case {
            name: "do not fail if everything is ok, and acquire a permit",
            action: Action::None,
            expected_acquired_permit_count: 1,
            expected_err: None,
        },
    ];

    for tc in cases {
        let tmp = tempfile::tempdir().unwrap();
        let (mut cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
        cfg.read_circuit_breaker = CircuitBreakerConfig {
            enabled: true,
            cooldown_period: GoDuration::from_secs(10),
            request_timeout: GoDuration::from_secs(30),
            ..Default::default()
        };
        cfg.push_circuit_breaker = CircuitBreakerConfig {
            enabled: true,
            cooldown_period: GoDuration::from_secs(10),
            request_timeout: GoDuration::from_secs(2),
            ..Default::default()
        };

        let ing = prepare_ingester_with_blocks_storage_and_limits(
            cfg,
            blocks_cfg,
            default_limits_test_config(),
            None,
        )
        .await;
        assert_eq!(ing.state(), State::Running);

        let read_cb = ing
            .circuit_breaker
            .read
            .as_ref()
            .expect("read cb enabled")
            .clone();
        read_cb.force_close();

        match tc.action {
            Action::None => {}
            Action::StopIngester => {
                ing.stop().await.unwrap();
            }
            Action::SetOverloaded => {
                ing.set_utilization_based_limiter(Some(Arc::new(FakeUtilizationLimiter("cpu"))));
            }
            Action::OpenReadCb => {
                read_cb.force_open();
            }
            Action::OpenPushCb => {
                ing.circuit_breaker
                    .push
                    .as_ref()
                    .expect("push cb enabled")
                    .force_open();
            }
            Action::DeactivateReadCb => {
                read_cb.deactivate();
            }
        }

        let res = ing.start_read_request(&MetadataMap::new()).await;
        assert_eq!(
            read_cb.acquired_permits(),
            tc.expected_acquired_permit_count,
            "{}",
            tc.name
        );

        match res {
            Ok(permit) => {
                assert!(tc.expected_err.is_none(), "{}: expected error", tc.name);
                let expected_success_count = read_cb.acquired_permits();
                permit.finish(false);
                assert_eq!(
                    read_cb.recorded_successes(),
                    expected_success_count,
                    "{}",
                    tc.name
                );
                assert_eq!(read_cb.recorded_failures(), 0, "{}", tc.name);
            }
            Err(err) => {
                match tc.expected_err {
                    Some(ExpectedErr::Unavailable) => {
                        assert!(
                            matches!(err, IngesterError::Unavailable { .. }),
                            "{}: got {err:?}",
                            tc.name
                        );
                    }
                    Some(ExpectedErr::TooBusy) => {
                        assert!(
                            matches!(err, IngesterError::TooBusy),
                            "{}: got {err:?}",
                            tc.name
                        );
                    }
                    Some(ExpectedErr::CircuitBreakerOpen) => {
                        assert!(
                            matches!(err, IngesterError::CircuitBreakerOpen { .. }),
                            "{}: got {err:?}",
                            tc.name
                        );
                    }
                    None => panic!("{}: unexpected err {err:?}", tc.name),
                }
            }
        }

        let _ = ing.stop().await;
    }
}

// ============================================================================
// 5. TestIngester_Push (lines 569..1931 and runner lines 4600..4747)
// ============================================================================

#[derive(Clone, Debug)]
struct ExpectedStream {
    metric: BTreeMap<String, String>,
    values: Vec<(i64, f64)>,
    histograms: Vec<(i64, Histogram)>,
}

impl PartialEq for ExpectedStream {
    fn eq(&self, other: &Self) -> bool {
        self.metric == other.metric
            && self.values == other.values
            && self.histograms.len() == other.histograms.len()
            && self
                .histograms
                .iter()
                .zip(other.histograms.iter())
                .all(|((t1, h1), (t2, h2))| t1 == t2 && h1.prom_equals(h2))
    }
}

type ExpectedExemplar = (Vec<(String, String)>, i64, f64);
type SeriesSamplesAndHistograms = (Vec<(i64, f64)>, Vec<(i64, Histogram)>);

#[derive(Clone, Debug, PartialEq)]
struct ExpectedExemplarSeries {
    labels: Vec<(String, String)>,
    exemplars: Vec<ExpectedExemplar>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedMetadata {
    family_name: String,
    help: String,
    unit: String,
    metric_type: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushWireMode {
    Rw1,
    Rw2,
}

#[derive(Clone, Debug)]
struct ExpectedPushError {
    code: Code,
    cause: cortexpb::ErrorCause,
    soft: bool,
    rejected_samples: i64,
    message: String,
}

struct PushTestCase {
    name: &'static str,
    reqs: Vec<cortexpb::WriteRequest>,
    expected_err: Option<ExpectedPushError>,
    expected_ingested: Vec<ExpectedStream>,
    expected_metadata_ingested: Vec<ExpectedMetadata>,
    expected_exemplars_ingested: Vec<ExpectedExemplarSeries>,
    expected_exemplars_dropped_count: usize,
    expected_metrics: &'static str,
    additional_metrics: Vec<&'static str>,
    disable_active_series: bool,
    max_exemplars: usize,
    max_metadata_per_user: usize,
    max_metadata_per_metric: usize,
    native_histograms: bool,
    allow_ooo: bool,
    ignore_ooo_exemplars: bool,
    past_grace_period: Duration,
}

impl Default for PushTestCase {
    fn default() -> Self {
        Self {
            name: "",
            reqs: Vec::new(),
            expected_err: None,
            expected_ingested: Vec::new(),
            expected_metadata_ingested: Vec::new(),
            expected_exemplars_ingested: Vec::new(),
            expected_exemplars_dropped_count: 0,
            expected_metrics: "",
            additional_metrics: Vec::new(),
            disable_active_series: false,
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: false,
            allow_ooo: false,
            ignore_ooo_exemplars: false,
            past_grace_period: Duration::ZERO,
        }
    }
}

fn decode_wire_chunk(wire_enc: i32, data: &[u8]) -> EncodedChunk {
    let enc = match wire_enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire encoding: {other}"),
    };
    EncodedChunk::from_bytes(enc, data.to_vec()).expect("decode chunk")
}

fn decode_querystream_responses(responses: &[ingesterpb::QueryStreamResponse]) -> Vec<ExpectedStream> {
    let mut series_labels: Vec<BTreeMap<String, String>> = Vec::new();
    let mut series_order: Vec<BTreeMap<String, String>> = Vec::new();
    let mut by_series: HashMap<usize, SeriesSamplesAndHistograms> = HashMap::new();

    for resp in responses {
        for s in &resp.streaming_series {
            let metric: BTreeMap<String, String> = s
                .labels
                .iter()
                .map(|lp| {
                    (
                        String::from_utf8_lossy(&lp.name).into_owned(),
                        String::from_utf8_lossy(&lp.value).into_owned(),
                    )
                })
                .collect();
            series_order.push(metric.clone());
            series_labels.push(metric);
        }
        for sc in &resp.streaming_series_chunks {
            let idx = sc.series_index as usize;
            let entry = by_series.entry(idx).or_default();
            for chk in &sc.chunks {
                let ec = decode_wire_chunk(chk.encoding, &chk.data);
                let decoded = decode_chunk_samples(&ec).expect("decode samples");
                for ds in decoded {
                    match ds.value {
                        SampleValue::Float(v) => entry.0.push((ds.t, v)),
                        SampleValue::Histogram(h) => entry.1.push((ds.t, *h)),
                        SampleValue::FloatHistogram(_) => panic!("unexpected float histogram"),
                    }
                }
            }
        }
    }

    series_order
        .into_iter()
        .enumerate()
        .filter_map(|(idx, metric)| {
            by_series.remove(&idx).map(|(values, histograms)| ExpectedStream {
                metric,
                values,
                histograms,
            })
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
struct ParsedMetricSample {
    labels: BTreeMap<String, String>,
    value: f64,
}

#[derive(Clone, Debug)]
struct ParsedMetricFamily {
    metric_type: MetricType,
    samples: Vec<ParsedMetricSample>,
}

fn parse_prom_text_labels(raw: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return map;
    }
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').expect("k=v label pair");
        let k = k.trim().to_string();
        let v = v.trim().trim_matches('"').to_string();
        map.insert(k, v);
    }
    map
}

fn parse_expected_prom_text(text: &str) -> BTreeMap<String, ParsedMetricFamily> {
    let mut types: HashMap<String, MetricType> = HashMap::new();
    let mut out: BTreeMap<String, ParsedMetricFamily> = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            let name = parts.next().expect("metric name").to_string();
            let ty_str = parts.next().expect("metric type");
            let ty = match ty_str {
                "counter" => MetricType::COUNTER,
                "gauge" => MetricType::GAUGE,
                "histogram" => MetricType::HISTOGRAM,
                "summary" => MetricType::SUMMARY,
                _ => panic!("unknown metric type {ty_str}"),
            };
            types.insert(name.clone(), ty);
            out.entry(name).or_insert_with(|| ParsedMetricFamily {
                metric_type: ty,
                samples: Vec::new(),
            });
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        // Sample line: `metric_name{...} 123` or `metric_name 123`
        let (name, labels, val_str) = if let Some(brace_start) = line.find('{') {
            let brace_end = line.rfind('}').expect("closing brace");
            let name = line[..brace_start].trim().to_string();
            let lbls = parse_prom_text_labels(&line[brace_start + 1..brace_end]);
            let val_str = line[brace_end + 1..].trim();
            (name, lbls, val_str)
        } else {
            let mut parts = line.split_whitespace();
            let name = parts.next().expect("name").to_string();
            let val_str = parts.next().expect("val");
            (name, BTreeMap::new(), val_str)
        };
        let value: f64 = val_str.parse().expect("float metric value");
        let ty = types.get(&name).copied().unwrap_or(MetricType::GAUGE);
        let entry = out.entry(name).or_insert_with(|| ParsedMetricFamily {
            metric_type: ty,
            samples: Vec::new(),
        });
        entry.samples.push(ParsedMetricSample { labels, value });
    }
    out
}

fn gather_and_compare(
    families: &[MetricFamily],
    expected_text: &str,
    checked_metric_names: &[&str],
    ctx_name: &str,
) {
    let expected = parse_expected_prom_text(expected_text);
    let actual_map: HashMap<&str, &MetricFamily> =
        families.iter().map(|mf| (mf.get_name(), mf)).collect();

    for &metric_name in checked_metric_names {
        let exp_family = expected.get(metric_name);
        let act_family = actual_map.get(metric_name).copied();

        match (exp_family, act_family) {
            (None, None) => {}
            (None, Some(mf)) => {
                assert!(
                    mf.get_metric().is_empty(),
                    "{ctx_name}: unexpected samples for {metric_name}: {:?}",
                    mf.get_metric()
                );
            }
            (Some(exp), None) => {
                assert!(
                    exp.samples.is_empty(),
                    "{ctx_name}: missing metric family {metric_name} (expected {:?})",
                    exp.samples
                );
            }
            (Some(exp), Some(mf)) => {
                assert_eq!(
                    mf.get_field_type(),
                    exp.metric_type,
                    "{ctx_name}: metric type mismatch for {metric_name}"
                );
                let mut act_samples: Vec<ParsedMetricSample> = mf
                    .get_metric()
                    .iter()
                    .map(|m| {
                        let labels: BTreeMap<String, String> = m
                            .get_label()
                            .iter()
                            .map(|lp| (lp.get_name().to_string(), lp.get_value().to_string()))
                            .collect();
                        let value = match mf.get_field_type() {
                            MetricType::COUNTER => m.get_counter().get_value(),
                            MetricType::GAUGE => m.get_gauge().get_value(),
                            _ => panic!("unsupported metric type for {metric_name}"),
                        };
                        ParsedMetricSample { labels, value }
                    })
                    .collect();
                let mut exp_samples = exp.samples.clone();
                act_samples.sort_by(|a, b| a.labels.cmp(&b.labels));
                exp_samples.sort_by(|a, b| a.labels.cmp(&b.labels));
                assert_eq!(
                    act_samples.len(),
                    exp_samples.len(),
                    "{ctx_name}: sample count mismatch for {metric_name}: actual={act_samples:?}, expected={exp_samples:?}"
                );
                for (a, e) in act_samples.iter().zip(exp_samples.iter()) {
                    assert_eq!(a.labels, e.labels, "{ctx_name}: labels mismatch for {metric_name}");
                    assert!(
                        (a.value - e.value).abs() < 1e-9,
                        "{ctx_name}: value mismatch for {metric_name} {:?}: actual={}, expected={}",
                        a.labels,
                        a.value,
                        e.value
                    );
                }
            }
        }
    }
}

async fn run_push_test_case(tc: &PushTestCase, mode: PushWireMode) {
    let ctx_name = format!("{} ({mode:?})", tc.name);
    let tmp = tempfile::tempdir().unwrap();
    let registry = Registry::new();

    let (mut cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    cfg.active_series.active_series_metrics_enabled = !tc.disable_active_series;

    let mut limits = default_limits_test_config();
    limits.max_global_exemplars_per_user = tc.max_exemplars as i64;
    limits.max_global_metadata_per_user = tc.max_metadata_per_user as i64;
    limits.max_global_metadata_per_metric = tc.max_metadata_per_metric as i64;
    limits.native_histograms_ingestion_enabled = tc.native_histograms;
    limits.ignore_ooo_exemplars = tc.ignore_ooo_exemplars;
    limits.past_grace_period = ModelDuration(tc.past_grace_period.as_nanos() as i64);

    let ooo_time_window_secs = if tc.allow_ooo {
        limits.out_of_order_time_window = ModelDuration(Duration::from_secs(3600).as_nanos() as i64);
        3600i64
    } else {
        0i64
    };

    let ing = prepare_ingester_with_blocks_storage_and_limits(
        cfg,
        blocks_cfg,
        limits,
        Some(&registry),
    )
    .await;

    let appended_samples_before = ing.appended_samples.load(Ordering::Relaxed);
    let appended_exemplars_before = ing.appended_exemplars.load(Ordering::Relaxed);

    // Push timeseries (both RW1 and RW2 paths)
    for (idx, req) in tc.reqs.iter().enumerate() {
        let res = match mode {
            PushWireMode::Rw1 => ing.push_grpc(USER_ID, req).await,
            PushWireMode::Rw2 => {
                let mut rw2_buf = Vec::new();
                encode_write_request_rw2_bytes(req, 0, None, &mut rw2_buf)
                    .expect("encode rw2");
                let mut scratch = WriteRequestScratch::new();
                let borrowed = decode_write_request(&rw2_buf, DecodeOptions::rw2(), &mut scratch)
                    .expect("decode rw2");
                ing.push_borrowed_to_storage(USER_ID, &borrowed, rw2_buf.len() as i64)
            }
        };

        if idx < tc.reqs.len() - 1 {
            assert!(res.is_ok(), "{ctx_name}: req[{idx}] unexpected err: {res:?}");
        } else if let Some(ref exp_err) = tc.expected_err {
            let err = res.expect_err(&format!("{ctx_name}: expected error on final req"));
            let status = err.map_push_error_to_status();
            assert_eq!(status.code(), exp_err.code, "{ctx_name}: status code");
            let details = mimir_proto::grpcutilpb::extract_cortex_error_details(&status)
                .expect("cortex ErrorDetails");
            assert_eq!(details.cause, exp_err.cause as i32, "{ctx_name}: ErrorDetails.cause");
            assert_eq!(details.soft, exp_err.soft, "{ctx_name}: ErrorDetails.soft");
            assert_eq!(
                details.rejected_samples, exp_err.rejected_samples,
                "{ctx_name}: ErrorDetails.rejected_samples"
            );
            assert_eq!(status.message(), exp_err.message, "{ctx_name}: error message");
        } else {
            assert!(res.is_ok(), "{ctx_name}: unexpected err on final req: {res:?}");
        }
    }

    // Read back samples to see what has been really ingested
    let md = MetadataMap::new();
    let stream_res = ing
        .handle_query_stream(
            &md,
            USER_ID,
            ingesterpb::QueryRequest {
                start_timestamp_ms: i64::MIN,
                end_timestamp_ms: i64::MAX,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::RegexMatch as i32,
                    name: METRIC_NAME_LABEL.to_string(),
                    value: ".*".to_string(),
                }],
                ..Default::default()
            },
        )
        .await
        .expect("query_stream");
    let got_streams = decode_querystream_responses(&stream_res);
    assert_eq!(got_streams, tc.expected_ingested, "{ctx_name}: ingested samples");

    // Read back exemplars to see what has been really ingested
    let exemplar_res = ing
        .handle_query_exemplars(
            &md,
            USER_ID,
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: i64::MIN,
                end_timestamp_ms: i64::MAX,
                matchers: vec![ingesterpb::LabelMatchers {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::RegexMatch as i32,
                        name: METRIC_NAME_LABEL.to_string(),
                        value: ".*".to_string(),
                    }],
                }],
            },
        )
        .await
        .expect("query_exemplars");
    let got_exemplars: Vec<ExpectedExemplarSeries> = exemplar_res
        .timeseries
        .iter()
        .map(|ts| ExpectedExemplarSeries {
            labels: ts
                .labels
                .iter()
                .map(|l| {
                    (
                        String::from_utf8_lossy(&l.name).into_owned(),
                        String::from_utf8_lossy(&l.value).into_owned(),
                    )
                })
                .collect(),
            exemplars: ts
                .exemplars
                .iter()
                .map(|ex| {
                    (
                        ex.labels
                            .iter()
                            .map(|l| {
                                (
                                    String::from_utf8_lossy(&l.name).into_owned(),
                                    String::from_utf8_lossy(&l.value).into_owned(),
                                )
                            })
                            .collect(),
                        ex.timestamp_ms,
                        ex.value,
                    )
                })
                .collect(),
        })
        .collect();
    assert_eq!(
        got_exemplars, tc.expected_exemplars_ingested,
        "{ctx_name}: ingested exemplars"
    );

    // Read back metadata (order is never guaranteed)
    let meta_res = ing
        .handle_metrics_metadata(
            &md,
            USER_ID,
            mimir_proto::client::custom::default_metrics_metadata_request(),
        )
        .await
        .expect("metrics_metadata");
    let mut got_meta: Vec<ExpectedMetadata> = meta_res
        .metadata
        .iter()
        .map(|m| ExpectedMetadata {
            family_name: m.metric_family_name.clone(),
            help: m.help.clone(),
            unit: m.unit.clone(),
            metric_type: m.r#type,
        })
        .collect();
    let mut exp_meta = tc.expected_metadata_ingested.clone();
    got_meta.sort();
    exp_meta.sort();
    assert_eq!(got_meta, exp_meta, "{ctx_name}: ingested metadata");

    if !tc.disable_active_series {
        ing.update_active_series_metrics(SystemTime::now());
    }

    let base_metric_names = vec![
        "cortex_ingester_ingested_samples_total",
        "cortex_ingester_ingested_samples_failures_total",
        "cortex_ingester_memory_series",
        "cortex_ingester_memory_users",
        "cortex_ingester_memory_series_created_total",
        "cortex_ingester_memory_series_removed_total",
        "cortex_discarded_samples_total",
        "cortex_ingester_active_series",
        "cortex_ingester_active_native_histogram_series",
        "cortex_ingester_active_native_histogram_buckets",
        "cortex_ingester_tsdb_head_min_timestamp_seconds",
        "cortex_ingester_tsdb_head_max_timestamp_seconds",
        "cortex_ingester_series_with_floats_and_histograms_total",
    ];
    let mut checked_names = base_metric_names;
    checked_names.extend(tc.additional_metrics.iter().copied());

    let families = registry.gather();
    gather_and_compare(&families, tc.expected_metrics, &checked_names, &ctx_name);

    // Check anonymous usage stats
    let expected_tenants_count = if tc.expected_ingested.is_empty() { 0 } else { 1 };
    let mut expected_samples_count = 0i64;
    for stream in &tc.expected_ingested {
        expected_samples_count += (stream.values.len() + stream.histograms.len()) as i64;
    }
    let mut expected_exemplars_count = tc.expected_exemplars_dropped_count as i64;
    for series in &tc.expected_exemplars_ingested {
        expected_exemplars_count += series.exemplars.len() as i64;
    }

    let stats = ing.update_usage_stats();
    if !tc.disable_active_series {
        assert_eq!(
            stats.active_series,
            tc.expected_ingested.len() as i64,
            "{ctx_name}: active_series"
        );
    }
    assert_eq!(
        stats.memory_series,
        tc.expected_ingested.len() as i64,
        "{ctx_name}: memory_series"
    );
    assert_eq!(
        stats.memory_users, expected_tenants_count,
        "{ctx_name}: memory_users"
    );
    assert_eq!(
        stats.appended_samples,
        expected_samples_count + appended_samples_before,
        "{ctx_name}: appended_samples"
    );
    assert_eq!(
        stats.appended_exemplars,
        expected_exemplars_count + appended_exemplars_before,
        "{ctx_name}: appended_exemplars"
    );
    assert_eq!(
        stats.tenants_with_out_of_order_enabled == 1,
        tc.allow_ooo,
        "{ctx_name}: tenants_with_out_of_order_enabled"
    );
    assert_eq!(
        stats.min_out_of_order_time_window_seconds, ooo_time_window_secs,
        "{ctx_name}: min_out_of_order_time_window_seconds"
    );
    assert_eq!(
        stats.max_out_of_order_time_window_seconds, ooo_time_window_secs,
        "{ctx_name}: max_out_of_order_time_window_seconds"
    );

    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push
#[tokio::test]
async fn test_ingester_push() {
    let metric_label_adapters = vec![LabelAdapter::new(METRIC_NAME_LABEL, "test")];
    let metric_label_set = BTreeMap::from([(METRIC_NAME_LABEL.to_string(), "test".to_string())]);
    let metric_labels_pairs = vec![(METRIC_NAME_LABEL.to_string(), "test".to_string())];
    let trace_id = |v: &str| vec![("traceID".to_string(), v.to_string())];
    let ex_proto = |val: f64, ts_ms: i64, trace: &str| cortexpb::Exemplar {
        labels: vec![cortexpb::LabelPair::from_strs("traceID", trace)],
        value: val,
        timestamp_ms: ts_ms,
    };
    let ts_float_only = |val: f64, ts_ms: i64| cortexpb::TimeSeries {
        labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
        samples: vec![cortexpb::Sample {
            value: val,
            timestamp_ms: ts_ms,
        }],
        ..Default::default()
    };
    let ts_exemplar_only = |exs: Vec<cortexpb::Exemplar>| cortexpb::TimeSeries {
        labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
        exemplars: exs,
        ..Default::default()
    };

    let cases = vec![
        // 1. "should succeed on valid series and metadata"
        PushTestCase {
            name: "should succeed on valid series and metadata",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_label_adapters),
                    &[cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                    None,
                    vec![cortexpb::MetricMetadata {
                        metric_family_name: "metric_name_1".to_string(),
                        help: "a help for metric_name_1".to_string(),
                        unit: String::new(),
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    }],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request(
                    std::slice::from_ref(&metric_label_adapters),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 10,
                    }],
                    None,
                    vec![cortexpb::MetricMetadata {
                        metric_family_name: "metric_name_2".to_string(),
                        help: "a help for metric_name_2".to_string(),
                        unit: String::new(),
                        r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                    }],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: vec![(9, 1.0), (10, 2.0)],
                histograms: Vec::new(),
            }],
            expected_metadata_ingested: vec![
                ExpectedMetadata {
                    family_name: "metric_name_2".to_string(),
                    help: "a help for metric_name_2".to_string(),
                    unit: String::new(),
                    metric_type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                },
                ExpectedMetadata {
                    family_name: "metric_name_1".to_string(),
                    help: "a help for metric_name_1".to_string(),
                    unit: String::new(),
                    metric_type: cortexpb::metric_metadata::MetricType::Counter as i32,
                },
            ],
            additional_metrics: vec![
                "cortex_ingester_memory_metadata",
                "cortex_ingester_memory_metadata_created_total",
                "cortex_ingester_ingested_metadata_total",
                "cortex_ingester_ingested_metadata_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
                # TYPE cortex_ingester_ingested_metadata_failures_total counter
                cortex_ingester_ingested_metadata_failures_total 0
                # HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
                # TYPE cortex_ingester_ingested_metadata_total counter
                cortex_ingester_ingested_metadata_total 2
                # HELP cortex_ingester_memory_metadata The current number of metadata in memory.
                # TYPE cortex_ingester_memory_metadata gauge
                cortex_ingester_memory_metadata 2
                # HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
                # TYPE cortex_ingester_memory_metadata_created_total counter
                cortex_ingester_memory_metadata_created_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#,
            ..Default::default()
        },
        // 2. "should succeed on valid series with histograms and metadata"
        PushTestCase {
            name: "should succeed on valid series with histograms and metadata",
            native_histograms: true,
            reqs: vec![
                {
                    let mut r = new_write_request(
                        vec![cortexpb::MetricMetadata {
                            metric_family_name: "metric_name_1".to_string(),
                            help: "a help for metric_name_1".to_string(),
                            unit: String::new(),
                            r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                        }],
                        cortexpb::write_request::SourceEnum::Api,
                    );
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(1, &generate_test_histogram(1))],
                        None,
                    );
                    r
                },
                {
                    let mut r = new_write_request(
                        vec![cortexpb::MetricMetadata {
                            metric_family_name: "metric_name_2".to_string(),
                            help: "a help for metric_name_2".to_string(),
                            unit: String::new(),
                            r#type: cortexpb::metric_metadata::MetricType::Gaugehistogram as i32,
                        }],
                        cortexpb::write_request::SourceEnum::Api,
                    );
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(2, &generate_test_gauge_histogram(2))],
                        None,
                    );
                    r
                },
            ],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: Vec::new(),
                histograms: vec![
                    (1, generate_test_histogram(1)),
                    (2, generate_test_gauge_histogram(2)),
                ],
            }],
            expected_metadata_ingested: vec![
                ExpectedMetadata {
                    family_name: "metric_name_2".to_string(),
                    help: "a help for metric_name_2".to_string(),
                    unit: String::new(),
                    metric_type: cortexpb::metric_metadata::MetricType::Gaugehistogram as i32,
                },
                ExpectedMetadata {
                    family_name: "metric_name_1".to_string(),
                    help: "a help for metric_name_1".to_string(),
                    unit: String::new(),
                    metric_type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                },
            ],
            additional_metrics: vec![
                "cortex_ingester_memory_metadata",
                "cortex_ingester_memory_metadata_created_total",
                "cortex_ingester_ingested_metadata_total",
                "cortex_ingester_ingested_metadata_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
                # TYPE cortex_ingester_ingested_metadata_failures_total counter
                cortex_ingester_ingested_metadata_failures_total 0
                # HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
                # TYPE cortex_ingester_ingested_metadata_total counter
                cortex_ingester_ingested_metadata_total 2
                # HELP cortex_ingester_memory_metadata The current number of metadata in memory.
                # TYPE cortex_ingester_memory_metadata gauge
                cortex_ingester_memory_metadata 2
                # HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
                # TYPE cortex_ingester_memory_metadata_created_total counter
                cortex_ingester_memory_metadata_created_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
                # TYPE cortex_ingester_active_native_histogram_series gauge
                cortex_ingester_active_native_histogram_series{user="test"} 1
                # HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
                # TYPE cortex_ingester_active_native_histogram_buckets gauge
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.001
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.002
            "#,
            ..Default::default()
        },
        // 3. "should succeed on new float series with an exemplar"
        PushTestCase {
            name: "should succeed on new float series with an exemplar",
            max_exemplars: 2,
            reqs: vec![to_write_request(
                std::slice::from_ref(&metric_label_adapters),
                &[cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 9,
                }],
                Some(&[Some(ex_proto(1000.0, 1000, "123"))]),
                Vec::new(),
                cortexpb::write_request::SourceEnum::Api,
            )],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: vec![(9, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 1
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 0
            "#,
            ..Default::default()
        },
        // 4. "should succeed on new float series and an exemplar"
        PushTestCase {
            name: "should succeed on new float series and an exemplar",
            max_exemplars: 2,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![
                    ts_float_only(1.0, 9),
                    ts_exemplar_only(vec![ex_proto(1000.0, 1000, "123")]),
                ],
                ..Default::default()
            }],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: vec![(9, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 1
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
            "#,
            ..Default::default()
        },
        // 5. "should succeed on existing float series with an exemplar"
        PushTestCase {
            name: "should succeed on existing float series with an exemplar",
            max_exemplars: 2,
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_label_adapters),
                    &[cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                    None,
                    Vec::new(),
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request(
                    std::slice::from_ref(&metric_label_adapters),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 10,
                    }],
                    Some(&[Some(ex_proto(1000.0, 1000, "123"))]),
                    Vec::new(),
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: vec![(9, 1.0), (10, 2.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 0
            "#,
            ..Default::default()
        },
        // 6. "should soft fail on new float series and an exemplar and an out of order exemplar"
        PushTestCase {
            name: "should soft fail on new float series and an exemplar and an out of order exemplar",
            max_exemplars: 2,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![
                        ts_float_only(1.0, 9),
                        ts_exemplar_only(vec![ex_proto(1000.0, 1000, "123")]),
                    ],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![ts_exemplar_only(vec![ex_proto(2000.0, 500, "333")])],
                    ..Default::default()
                },
            ],
            expected_err: Some(ExpectedPushError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                message: "user=test: err: out of order exemplar. timestamp=1970-01-01T00:00:00.5Z, series=test, exemplar={traceID=\"333\"}".to_string(),
            }),
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: vec![(9, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 1
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 1
            "#,
            ..Default::default()
        },
        // 7. "should succeed on new float series and an exemplar and an out of order exemplar but OOO exemplars are ignored"
        PushTestCase {
            name: "should succeed on new float series and an exemplar and an out of order exemplar but OOO exemplars are ignored",
            max_exemplars: 2,
            ignore_ooo_exemplars: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![
                        ts_float_only(1.0, 9),
                        ts_exemplar_only(vec![ex_proto(1000.0, 1000, "123")]),
                    ],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![ts_exemplar_only(vec![ex_proto(2000.0, 500, "333")])],
                    ..Default::default()
                },
            ],
            expected_err: None,
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: vec![(9, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 1
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 1
            "#,
            ..Default::default()
        },
        // 8. "should succeed on new histogram series with an exemplar"
        PushTestCase {
            name: "should succeed on new histogram series with an exemplar",
            max_exemplars: 2,
            native_histograms: true,
            reqs: vec![{
                let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                add_histogram_series(
                    &mut r,
                    std::slice::from_ref(&metric_label_adapters),
                    &[int_histogram_to_proto(9, &generate_test_histogram(1))],
                    Some(&[Some(ex_proto(1000.0, 1000, "123"))]),
                );
                r
            }],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: Vec::new(),
                histograms: vec![(9, generate_test_histogram(1))],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 1
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
                # TYPE cortex_ingester_active_native_histogram_buckets gauge
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                # HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
                # TYPE cortex_ingester_active_native_histogram_series gauge
                cortex_ingester_active_native_histogram_series{user="test"} 1
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 0
            "#,
            ..Default::default()
        },
        // 9. "should succeed on new histogram series and an exemplar"
        PushTestCase {
            name: "should succeed on new histogram series and an exemplar",
            max_exemplars: 2,
            native_histograms: true,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
                        histograms: vec![int_histogram_to_proto(9, &generate_test_histogram(1))],
                        ..Default::default()
                    },
                    ts_exemplar_only(vec![ex_proto(1000.0, 1000, "123")]),
                ],
                ..Default::default()
            }],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: Vec::new(),
                histograms: vec![(9, generate_test_histogram(1))],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 1
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
                # TYPE cortex_ingester_active_native_histogram_buckets gauge
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                # HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
                # TYPE cortex_ingester_active_native_histogram_series gauge
                cortex_ingester_active_native_histogram_series{user="test"} 1
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 0
            "#,
            ..Default::default()
        },
        // 10. "should succeed on existing histogram series with an exemplar"
        PushTestCase {
            name: "should succeed on existing histogram series with an exemplar",
            max_exemplars: 2,
            native_histograms: true,
            reqs: vec![
                {
                    let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(9, &generate_test_histogram(1))],
                        None,
                    );
                    r
                },
                {
                    let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(10, &generate_test_histogram(2))],
                        Some(&[Some(ex_proto(1000.0, 1000, "123"))]),
                    );
                    r
                },
            ],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: Vec::new(),
                histograms: vec![
                    (9, generate_test_histogram(1)),
                    (10, generate_test_histogram(2)),
                ],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![(trace_id("123"), 1000, 1000.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
                # TYPE cortex_ingester_active_native_histogram_buckets gauge
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                # HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
                # TYPE cortex_ingester_active_native_histogram_series gauge
                cortex_ingester_active_native_histogram_series{user="test"} 1
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 1
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 1
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 0
            "#,
            ..Default::default()
        },
        // 11. "should succeed on existing histogram series with multiple exemplars"
        PushTestCase {
            name: "should succeed on existing histogram series with multiple exemplars",
            max_exemplars: 2,
            native_histograms: true,
            reqs: vec![
                {
                    let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(9, &generate_test_histogram(1))],
                        None,
                    );
                    r
                },
                {
                    let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(10, &generate_test_histogram(2))],
                        None,
                    );
                    add_exemplars_at(
                        &mut r,
                        0,
                        &[ex_proto(1000.0, 1000, "123"), ex_proto(2000.0, 2000, "456")],
                    );
                    r
                },
            ],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: Vec::new(),
                histograms: vec![
                    (9, generate_test_histogram(1)),
                    (10, generate_test_histogram(2)),
                ],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![
                    (trace_id("123"), 1000, 1000.0),
                    (trace_id("456"), 2000, 2000.0),
                ],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
                # TYPE cortex_ingester_active_native_histogram_buckets gauge
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                # HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
                # TYPE cortex_ingester_active_native_histogram_series gauge
                cortex_ingester_active_native_histogram_series{user="test"} 1
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 2
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 2
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 2
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 0
            "#,
            ..Default::default()
        },
        // 12. "should succeed on existing histogram series with partial updated exemplars"
        PushTestCase {
            name: "should succeed on existing histogram series with partial updated exemplars",
            max_exemplars: 2,
            native_histograms: true,
            reqs: vec![
                {
                    let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(9, &generate_test_histogram(1))],
                        None,
                    );
                    add_exemplars_at(
                        &mut r,
                        0,
                        &[ex_proto(1000.0, 1000, "123"), ex_proto(2000.0, 2000, "456")],
                    );
                    r
                },
                {
                    let mut r = new_write_request(Vec::new(), cortexpb::write_request::SourceEnum::Api);
                    add_histogram_series(
                        &mut r,
                        std::slice::from_ref(&metric_label_adapters),
                        &[int_histogram_to_proto(10, &generate_test_histogram(2))],
                        None,
                    );
                    add_exemplars_at(
                        &mut r,
                        0,
                        &[
                            ex_proto(1500.0, 1500, "333"),
                            ex_proto(2000.0, 2000, "456"),
                            ex_proto(3000.0, 3000, "789"),
                        ],
                    );
                    r
                },
            ],
            expected_ingested: vec![ExpectedStream {
                metric: metric_label_set.clone(),
                values: Vec::new(),
                histograms: vec![
                    (9, generate_test_histogram(1)),
                    (10, generate_test_histogram(2)),
                ],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: metric_labels_pairs.clone(),
                exemplars: vec![
                    (trace_id("456"), 2000, 2000.0),
                    (trace_id("789"), 3000, 3000.0),
                ],
            }],
            expected_exemplars_dropped_count: 2,
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                # HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
                # TYPE cortex_ingester_ingested_samples_total counter
                cortex_ingester_ingested_samples_total{user="test"} 2
                # HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
                # TYPE cortex_ingester_ingested_samples_failures_total counter
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                # HELP cortex_ingester_memory_users The current number of users in memory.
                # TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 1
                # HELP cortex_ingester_memory_series The current number of series in memory.
                # TYPE cortex_ingester_memory_series gauge
                cortex_ingester_memory_series 1
                # HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
                # TYPE cortex_ingester_memory_series_created_total counter
                cortex_ingester_memory_series_created_total{user="test"} 1
                # HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
                # TYPE cortex_ingester_memory_series_removed_total counter
                cortex_ingester_memory_series_removed_total{user="test"} 0
                # HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
                # TYPE cortex_ingester_active_native_histogram_buckets gauge
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                # HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
                # TYPE cortex_ingester_active_native_histogram_series gauge
                cortex_ingester_active_native_histogram_series{user="test"} 1
                # HELP cortex_ingester_active_series Number of currently active series per user.
                # TYPE cortex_ingester_active_series gauge
                cortex_ingester_active_series{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 3
                # HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 2
                # HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
                # TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                # HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage.
                # TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 2
                # HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
                # TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                # HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                # HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
                # TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                # HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
                # TYPE cortex_ingester_ingested_exemplars_total counter
                cortex_ingester_ingested_exemplars_total 4
                # HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
                # TYPE cortex_ingester_ingested_exemplars_failures_total counter
                cortex_ingester_ingested_exemplars_failures_total 1
            "#,
            ..Default::default()
        },
    ];

    for tc in &cases {
        for mode in [PushWireMode::Rw1, PushWireMode::Rw2] {
            run_push_test_case(tc, mode).await;
        }
    }
}
