//! Ported upstream dskit/ring core tests (part 3: replication_set and replication_set_tracker).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/replication_set_test.go
//! - github.com/grafana/dskit/ring/replication_set_tracker_test.go

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_config::GoDuration;
use mimir_proto::ringpb::{InstanceDesc, InstanceState};
use mimir_ring::ring::replication_set::{
    context_with_available_replicas, do_multi_until_quorum_inner, do_until_quorum,
    do_until_quorum_without_successful_context_cancellation, get_available_replicas,
    has_replication_set_changed, has_replication_set_changed_without_state,
    has_replication_set_changed_without_state_or_addr, DoUntilQuorumConfig, SimpleError,
};
use mimir_ring::ring::replication_set_tracker::{
    unique_zone_count, BoxDynError, CancelCauseFunc, CancellationError, ContextError,
    DefaultContextTracker, DefaultResultTracker, ErrResultNotNeeded, InflightInstanceTracker,
    ReplicationSetContextTracker, ReplicationSetResultTracker, RingContext, TrackerLogger,
    ZoneAwareContextTracker, ZoneAwareResultTracker,
};
use mimir_ring::ring::replication_strategy::ReplicationSet;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Barrier, Notify};

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogRecord {
    level: String,
    msg: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct TestTrackerLogger {
    messages: Mutex<Vec<LogRecord>>,
}

impl TestTrackerLogger {
    fn contains(&self, level: &str, msg: &str, fields: &[(&str, &str)]) -> bool {
        let msgs = self.messages.lock();
        msgs.iter().any(|r| {
            r.level == level
                && r.msg == msg
                && fields
                    .iter()
                    .all(|(k, v)| r.fields.get(*k).map(String::as_str) == Some(*v))
        })
    }

    fn all(&self) -> Vec<LogRecord> {
        self.messages.lock().clone()
    }
}

impl TrackerLogger for TestTrackerLogger {
    fn log(&self, level: &str, msg: &str, fields: &[(&str, String)]) {
        let mut map = BTreeMap::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        self.messages.lock().push(LogRecord {
            level: level.to_string(),
            msg: msg.to_string(),
            fields: map,
        });
    }
}

struct TestContextKey;

fn inst_addr(addr: &str) -> InstanceDesc {
    InstanceDesc {
        addr: addr.to_string(),
        ..Default::default()
    }
}

fn inst_addr_zone(addr: &str, zone: &str) -> InstanceDesc {
    InstanceDesc {
        addr: addr.to_string(),
        zone: zone.to_string(),
        ..Default::default()
    }
}

struct CleanupTracker {
    max_expected: usize,
    successful_cancelled: bool,
    received: AtomicUsize,
    tx: mpsc::UnboundedSender<String>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<String>>,
    cleaned_up: Mutex<Vec<String>>,
    contexts: Mutex<HashMap<String, RingContext>>,
    cancels: Mutex<HashMap<String, CancelCauseFunc>>,
}

impl CleanupTracker {
    fn new(max_expected: usize, successful_cancelled: bool) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            max_expected,
            successful_cancelled,
            received: AtomicUsize::new(0),
            tx,
            rx: tokio::sync::Mutex::new(rx),
            cleaned_up: Mutex::new(Vec::new()),
            contexts: Mutex::new(HashMap::new()),
            cancels: Mutex::new(HashMap::new()),
        })
    }

    fn track_call(&self, ctx: &RingContext, inst: &InstanceDesc, cancel: Option<CancelCauseFunc>) {
        self.contexts.lock().insert(inst.addr.clone(), ctx.clone());
        if let Some(c) = cancel {
            self.cancels.lock().insert(inst.addr.clone(), c);
        }
    }

    fn called_instances(&self) -> Vec<String> {
        self.contexts.lock().keys().cloned().collect()
    }

    fn cleanup(&self, res: String) {
        let count = self.received.fetch_add(1, Ordering::AcqRel) + 1;
        assert!(
            count <= self.max_expected,
            "received more cleanup calls ({count}) than expected ({})",
            self.max_expected
        );
        let _ = self.tx.send(res);
    }

    async fn collect(&self, expected: usize) -> Vec<String> {
        let mut out = Vec::with_capacity(expected);
        let mut rx = self.rx.lock().await;
        while out.len() < expected {
            match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
                Ok(Some(v)) => out.push(v),
                _ => panic!("timed out collecting cleanup calls: got {out:?}, expected {expected}"),
            }
        }
        *self.cleaned_up.lock() = out.clone();
        out
    }

    fn assert_correct(&self, successful: &[String], failed: &[String]) {
        let cleaned = self.cleaned_up.lock().clone();
        let ctxs = self.contexts.lock();
        for inst in successful {
            assert!(!failed.contains(inst));
            assert!(!cleaned.contains(inst), "{inst} returned but cleaned up");
            let ctx = &ctxs[inst];
            if self.successful_cancelled {
                assert!(ctx.is_canceled(), "expected {inst} context cancelled");
            } else {
                assert!(!ctx.is_canceled(), "expected {inst} context NOT cancelled");
            }
        }
        for inst in failed {
            assert!(cleaned.contains(inst), "{inst} failed/unused but not cleaned up");
            let ctx = &ctxs[inst];
            assert!(ctx.is_canceled(), "expected cleaned-up {inst} context cancelled");
        }
        let mut c_sorted = cleaned;
        let mut f_sorted = failed.to_vec();
        c_sorted.sort();
        f_sorted.sort();
        assert_eq!(c_sorted, f_sorted);
    }
}

fn assert_elements_match(mut actual: Vec<String>, mut expected: Vec<String>) {
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

// ============================================================================
// replication_set_test.go (27)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestReplicationSet_GetAddresses
#[test]
fn test_replication_set_get_addresses() {
    assert!(ReplicationSet::default().get_addresses().is_empty());
    let rs = ReplicationSet {
        instances: vec![inst_addr("127.0.0.1"), inst_addr("127.0.0.2"), inst_addr("127.0.0.3")],
        ..Default::default()
    };
    assert_elements_match(
        rs.get_addresses(),
        vec!["127.0.0.1".into(), "127.0.0.2".into(), "127.0.0.3".into()],
    );
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestReplicationSet_GetAddressesWithout
#[test]
fn test_replication_set_get_addresses_without() {
    assert!(ReplicationSet::default().get_addresses_without("127.0.0.1").is_empty());
    let rs = ReplicationSet {
        instances: vec![inst_addr("127.0.0.1"), inst_addr("127.0.0.2"), inst_addr("127.0.0.3")],
        ..Default::default()
    };
    assert_elements_match(
        rs.get_addresses_without("127.0.0.4"),
        vec!["127.0.0.1".into(), "127.0.0.2".into(), "127.0.0.3".into()],
    );
    assert_elements_match(
        rs.get_addresses_without("127.0.0.2"),
        vec!["127.0.0.1".into(), "127.0.0.3".into()],
    );
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestReplicationSet_Do
#[tokio::test]
async fn test_replication_set_do() {
    // 1. max errors = 0, no errors
    {
        let r = ReplicationSet {
            instances: vec![InstanceDesc::default()],
            ..Default::default()
        };
        let got = r
            .do_with_delay(&RingContext::background(), Duration::ZERO, |_, _| async {
                Ok(1i32)
            })
            .await
            .unwrap();
        assert_eq!(got, vec![1]);
    }
    // 2. max errors = 0, fail on 1 error out of 1
    {
        let r = ReplicationSet {
            instances: vec![InstanceDesc::default()],
            ..Default::default()
        };
        let err = r
            .do_with_delay(&RingContext::background(), Duration::ZERO, |_, _| async {
                Err::<i32, _>(Arc::new(SimpleError("failed".into())) as BoxDynError)
            })
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "failed");
    }
    // 3. fail on 1 error out of 3 (last call fails)
    {
        let count = Arc::new(AtomicI32::new(0));
        let r = ReplicationSet {
            instances: vec![InstanceDesc::default(); 3],
            ..Default::default()
        };
        let err = r
            .do_with_delay(&RingContext::background(), Duration::ZERO, move |_, _| {
                let c = Arc::clone(&count);
                async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if c.fetch_add(1, Ordering::AcqRel) + 1 > 2 {
                        Err(Arc::new(SimpleError("failed".into())) as BoxDynError)
                    } else {
                        Ok(1i32)
                    }
                }
            })
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "failed");
    }
    // 4. max errors = 1, fail on 3 errors out of 5 with delay
    {
        let count = Arc::new(AtomicI32::new(0));
        let r = ReplicationSet {
            instances: vec![InstanceDesc::default(); 5],
            max_errors: 1,
            ..Default::default()
        };
        let err = r
            .do_with_delay(&RingContext::background(), Duration::from_millis(100), move |_, _| {
                let c = Arc::clone(&count);
                async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if c.fetch_add(1, Ordering::AcqRel) + 1 > 2 {
                        Err(Arc::new(SimpleError("failed".into())) as BoxDynError)
                    } else {
                        Ok(1i32)
                    }
                }
            })
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "failed");
    }
    // 5. context canceled
    {
        let r = ReplicationSet {
            instances: vec![InstanceDesc::default(); 3],
            max_errors: 1,
            ..Default::default()
        };
        let (ctx, cancel) = RingContext::background().with_cancel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel.cancel(None);
        });
        let err = r
            .do_with_delay(&ctx, Duration::ZERO, |_, _| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(1i32)
            })
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "context canceled");
    }
    // 6. zone-aware scenarios
    for (name, instances, max_unavail, fail_zones, exp_len, exp_err) in [
        ("all succeed", vec![inst_addr_zone("", "zone1"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone3")], 0, vec![], Some(3), false),
        ("1/3 zones fail", vec![inst_addr_zone("", "zone1"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone3")], 1, vec!["zone1"], Some(2), false),
        ("2/3 zones fail", vec![inst_addr_zone("", "zone1"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone3")], 1, vec!["zone1", "zone2"], None, true),
        ("1/3 zones fail 6 inst", vec![inst_addr_zone("", "zone1"), inst_addr_zone("", "zone1"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone3"), inst_addr_zone("", "zone3")], 1, vec!["zone1"], Some(4), false),
        ("3/5 zones fail", vec![inst_addr_zone("", "zone1"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone3"), inst_addr_zone("", "zone4"), inst_addr_zone("", "zone5")], 2, vec!["zone1", "zone2", "zone3"], None, true),
        ("2/5 zones fail 10 inst", vec![inst_addr_zone("", "zone1"), inst_addr_zone("", "zone1"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone2"), inst_addr_zone("", "zone3"), inst_addr_zone("", "zone3"), inst_addr_zone("", "zone4"), inst_addr_zone("", "zone4"), inst_addr_zone("", "zone5"), inst_addr_zone("", "zone5")], 2, vec!["zone1", "zone5"], Some(6), false),
    ] {
        let r = ReplicationSet {
            instances,
            max_unavailable_zones: max_unavail,
            ..Default::default()
        };
        let fail_zones: Vec<String> = fail_zones.into_iter().map(String::from).collect();
        let res = r
            .do_with_delay(&RingContext::background(), Duration::ZERO, move |_, ing| {
                let fz = fail_zones.clone();
                async move {
                    if fz.contains(&ing.zone) {
                        Err(Arc::new(SimpleError("zone failed".into())) as BoxDynError)
                    } else {
                        Ok(1i32)
                    }
                }
            })
            .await;
        if exp_err {
            assert_eq!(res.unwrap_err().to_string(), "zone failed", "{name}");
        } else {
            assert_eq!(res.unwrap().len(), exp_len.unwrap(), "{name}");
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation
#[tokio::test]
async fn test_do_until_quorum_without_successful_context_cancellation() {
    #[derive(Clone)]
    enum Mode {
        Success,
        Fail,
        FailZoneB,
    }

    let cases = [
        ("no replicas 0/0", ReplicationSet::default(), Mode::Success, vec![], None, 0),
        ("no replicas max_errors=1", ReplicationSet { max_errors: 1, ..Default::default() }, Mode::Success, vec![], None, 0),
        ("no replicas max_unavail=1", ReplicationSet { max_unavailable_zones: 1, ..Default::default() }, Mode::Success, vec![], None, 0),
        ("one replica succeeds", ReplicationSet { instances: vec![inst_addr_zone("replica-1", "zone-1")], ..Default::default() }, Mode::Success, vec!["replica-1"], None, 0),
        ("one replica fails", ReplicationSet { instances: vec![inst_addr_zone("replica-1", "zone-1")], ..Default::default() }, Mode::Fail, vec![], Some("this is the error for replica-1"), 0),
        ("one replica max_errors=1 succeeds", ReplicationSet { instances: vec![inst_addr_zone("replica-1", "zone-1")], max_errors: 1, ..Default::default() }, Mode::Success, vec![], None, 1),
        ("one replica max_errors=1 fails", ReplicationSet { instances: vec![inst_addr_zone("replica-1", "zone-1")], max_errors: 1, ..Default::default() }, Mode::Fail, vec![], None, 0),
        ("one replica max_unavail=1 succeeds", ReplicationSet { instances: vec![inst_addr_zone("replica-1", "zone-1")], max_unavailable_zones: 1, ..Default::default() }, Mode::Success, vec![], None, 1),
        ("one replica max_unavail=1 fails", ReplicationSet { instances: vec![inst_addr_zone("replica-1", "zone-1")], max_unavailable_zones: 1, ..Default::default() }, Mode::Fail, vec![], None, 0),
        (
            "total zone failure",
            ReplicationSet {
                instances: vec![
                    inst_addr_zone("zone-a-replica-1", "zone-a"),
                    inst_addr_zone("zone-a-replica-2", "zone-a"),
                    inst_addr_zone("zone-b-replica-1", "zone-b"),
                    inst_addr_zone("zone-b-replica-2", "zone-b"),
                    inst_addr_zone("zone-c-replica-1", "zone-c"),
                    inst_addr_zone("zone-c-replica-2", "zone-c"),
                ],
                max_unavailable_zones: 1,
                ..Default::default()
            },
            Mode::FailZoneB,
            vec!["zone-a-replica-1", "zone-a-replica-2", "zone-c-replica-1", "zone-c-replica-2"],
            None,
            0,
        ),
    ];

    for (name, mut rs, mode, exp_results, exp_err, max_cleanup) in cases {
        rs.partition_id = 123;
        for minimize in [true, false] {
            let logger = Arc::new(TestTrackerLogger::default());
            let tracker = CleanupTracker::new(max_cleanup, false);
            let successful = Arc::new(Mutex::new(Vec::<String>::new()));

            let cfg = DoUntilQuorumConfig {
                minimize_requests: minimize,
                logger: Some(logger.clone()),
                ..Default::default()
            };
            let tr_c = Arc::clone(&tracker);
            let tr_cl = Arc::clone(&tracker);
            let succ_c = Arc::clone(&successful);
            let mode_c = mode.clone();
            let res = do_until_quorum_without_successful_context_cancellation(
                &RingContext::background(),
                &rs,
                &cfg,
                move |ctx, desc, cancel| {
                    let tr = Arc::clone(&tr_c);
                    let sc = Arc::clone(&succ_c);
                    let m = mode_c.clone();
                    async move {
                        tr.track_call(&ctx, &desc, Some(cancel));
                        let r = match m {
                            Mode::Success => Ok(desc.addr.clone()),
                            Mode::Fail => Err(Arc::new(SimpleError(format!("this is the error for {}", desc.addr))) as BoxDynError),
                            Mode::FailZoneB => {
                                if desc.zone == "zone-b" {
                                    Err(Arc::new(SimpleError(format!("this is the error for {}", desc.addr))) as BoxDynError)
                                } else {
                                    Ok(desc.addr.clone())
                                }
                            }
                        };
                        if r.is_ok() {
                            sc.lock().push(desc.addr);
                        }
                        r
                    }
                },
                move |s| tr_cl.cleanup(s),
            )
            .await;

            let exp_res_vec: Vec<String> = exp_results.iter().map(|s| (*s).to_string()).collect();
            match exp_err {
                None => assert_elements_match(res.unwrap(), exp_res_vec.clone()),
                Some(e) => assert_eq!(res.unwrap_err().to_string(), e, "{name}"),
            }

            let exp_cleanup: Vec<String> = successful
                .lock()
                .iter()
                .filter(|a| !exp_res_vec.contains(a))
                .cloned()
                .collect();
            tracker.collect(exp_cleanup.len()).await;
            tracker.assert_correct(&exp_res_vec, &exp_cleanup);

            if exp_err.is_none() {
                assert!(logger.contains("debug", "quorum reached", &[("partition_id", "123")]));
            } else {
                assert!(logger.contains(
                    "error",
                    "cancelling all outstanding requests because quorum cannot be reached",
                    &[("partition_id", "123")]
                ));
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_MultipleUnavailableZones
#[tokio::test]
async fn test_do_until_quorum_multiple_unavailable_zones() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr_zone("zone-a-replica-1", "zone-a"),
            inst_addr_zone("zone-a-replica-2", "zone-a"),
            inst_addr_zone("zone-b-replica-1", "zone-b"),
            inst_addr_zone("zone-b-replica-2", "zone-b"),
            inst_addr_zone("zone-c-replica-1", "zone-c"),
            inst_addr_zone("zone-c-replica-2", "zone-c"),
        ],
        max_unavailable_zones: 1,
        partition_id: 123,
        ..Default::default()
    };

    for minimize in [true, false] {
        let logger = Arc::new(TestTrackerLogger::default());
        let tracker = CleanupTracker::new(3, false);
        let exp_cleanup = Arc::new(Mutex::new(Vec::<String>::new()));

        let cfg = DoUntilQuorumConfig {
            minimize_requests: minimize,
            logger: Some(logger.clone()),
            ..Default::default()
        };
        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let ec_c = Arc::clone(&exp_cleanup);
        let err = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            move |ctx, desc, cancel| {
                let tr = Arc::clone(&tr_c);
                let ec = Arc::clone(&ec_c);
                async move {
                    tr.track_call(&ctx, &desc, Some(cancel));
                    if desc.addr.ends_with("replica-1") {
                        Err(Arc::new(SimpleError("error from a replica-1 instance".into())) as BoxDynError)
                    } else {
                        ec.lock().push(desc.addr.clone());
                        Ok(desc.addr)
                    }
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "error from a replica-1 instance");
        let exp = exp_cleanup.lock().clone();
        tracker.collect(exp.len()).await;
        tracker.assert_correct(&[], &exp);
        assert!(logger.contains(
            "error",
            "cancelling all outstanding requests because quorum cannot be reached",
            &[("partition_id", "123")]
        ));
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_PartialZoneFailure
#[tokio::test]
async fn test_do_until_quorum_partial_zone_failure() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr_zone("zone-a-replica-1", "zone-a"),
            inst_addr_zone("zone-a-replica-2", "zone-a"),
            inst_addr_zone("zone-b-replica-1", "zone-b"),
            inst_addr_zone("zone-b-replica-2", "zone-b"),
            inst_addr_zone("zone-c-replica-1", "zone-c"),
            inst_addr_zone("zone-c-replica-2", "zone-c"),
        ],
        max_unavailable_zones: 1,
        ..Default::default()
    };
    let expected: Vec<String> = vec![
        "zone-a-replica-1".into(),
        "zone-a-replica-2".into(),
        "zone-c-replica-1".into(),
        "zone-c-replica-2".into(),
    ];

    for minimize in [true, false] {
        for set_term in [true, false] {
            let tracker = CleanupTracker::new(1, false);
            let zb1_cleanup = Arc::new(AtomicBool::new(false));
            let cfg = DoUntilQuorumConfig {
                minimize_requests: minimize,
                is_terminal_error: if set_term {
                    Some(Arc::new(|_| false))
                } else {
                    None
                },
                ..Default::default()
            };
            let tr_c = Arc::clone(&tracker);
            let tr_cl = Arc::clone(&tracker);
            let zb_c = Arc::clone(&zb1_cleanup);
            let res = do_until_quorum_without_successful_context_cancellation(
                &RingContext::background(),
                &rs,
                &cfg,
                move |ctx, desc, cancel| {
                    let tr = Arc::clone(&tr_c);
                    let zb = Arc::clone(&zb_c);
                    async move {
                        tr.track_call(&ctx, &desc, Some(cancel));
                        if desc.addr == "zone-b-replica-1" {
                            zb.store(true, Ordering::Release);
                        }
                        if desc.addr == "zone-b-replica-2" {
                            return Err(Arc::new(SimpleError(format!("this is the error for {}", desc.addr))) as BoxDynError);
                        }
                        Ok(desc.addr)
                    }
                },
                move |s| tr_cl.cleanup(s),
            )
            .await
            .unwrap();
            assert_elements_match(res, expected.clone());
            if zb1_cleanup.load(Ordering::Acquire) {
                tracker.collect(1).await;
                tracker.assert_correct(&expected, &["zone-b-replica-1".into()]);
            } else {
                tracker.collect(0).await;
                tracker.assert_correct(&expected, &[]);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_TerminalError
#[tokio::test]
async fn test_do_until_quorum_terminal_error() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr_zone("zone-a-replica-1", "zone-a"),
            inst_addr_zone("zone-a-replica-2", "zone-a"),
            inst_addr_zone("zone-b-replica-1", "zone-b"),
            inst_addr_zone("zone-b-replica-2", "zone-b"),
            inst_addr_zone("zone-c-replica-1", "zone-c"),
            inst_addr_zone("zone-c-replica-2", "zone-c"),
        ],
        max_unavailable_zones: 1,
        ..Default::default()
    };
    let tracker = CleanupTracker::new(3, false);
    let called = Arc::new(Mutex::new(Vec::<String>::new()));

    let cfg = DoUntilQuorumConfig {
        minimize_requests: true,
        is_terminal_error: Some(Arc::new(|e| e.to_string() == "this is the terminal error")),
        ..Default::default()
    };
    let tr_c = Arc::clone(&tracker);
    let tr_cl = Arc::clone(&tracker);
    let called_c = Arc::clone(&called);
    let err = do_until_quorum_without_successful_context_cancellation(
        &RingContext::background(),
        &rs,
        &cfg,
        move |ctx, desc, cancel| {
            let tr = Arc::clone(&tr_c);
            let cl = Arc::clone(&called_c);
            async move {
                tr.track_call(&ctx, &desc, Some(cancel));
                let len = {
                    let mut g = cl.lock();
                    g.push(desc.addr.clone());
                    g.len()
                };
                if len == 4 {
                    Err(Arc::new(SimpleError("this is the terminal error".into())) as BoxDynError)
                } else {
                    Ok(desc.addr)
                }
            }
        },
        move |s| tr_cl.cleanup(s),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "this is the terminal error");
    let called_list = called.lock().clone();
    assert_eq!(called_list.len(), 4);
    tracker.collect(3).await;
    tracker.assert_correct(&[], &called_list[..3]);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_ZoneSorting
#[tokio::test]
async fn test_do_until_quorum_zone_sorting() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr_zone("zone-a-replica-1", "zone-a"),
            inst_addr_zone("zone-a-replica-2", "zone-a"),
            inst_addr_zone("zone-b-replica-1", "zone-b"),
            inst_addr_zone("zone-b-replica-2", "zone-b"),
            inst_addr_zone("zone-c-replica-1", "zone-c"),
            inst_addr_zone("zone-c-replica-2", "zone-c"),
            inst_addr_zone("zone-d-replica-1", "zone-d"),
            inst_addr_zone("zone-d-replica-2", "zone-d"),
        ],
        max_unavailable_zones: 2,
        ..Default::default()
    };
    let tracker = CleanupTracker::new(0, false);
    let called = Arc::new(Mutex::new(Vec::<String>::new()));

    let cfg = DoUntilQuorumConfig {
        minimize_requests: true,
        zone_sorter: Some(Arc::new(|_| {
            vec!["zone-b".into(), "zone-d".into(), "zone-a".into(), "zone-c".into()]
        })),
        ..Default::default()
    };
    let tr_c = Arc::clone(&tracker);
    let tr_cl = Arc::clone(&tracker);
    let called_c = Arc::clone(&called);
    let results = do_until_quorum_without_successful_context_cancellation(
        &RingContext::background(),
        &rs,
        &cfg,
        move |ctx, desc, cancel| {
            let tr = Arc::clone(&tr_c);
            let cl = Arc::clone(&called_c);
            async move {
                tr.track_call(&ctx, &desc, Some(cancel));
                cl.lock().push(desc.addr.clone());
                Ok(desc.addr)
            }
        },
        move |s| tr_cl.cleanup(s),
    )
    .await
    .unwrap();
    assert_elements_match(
        results.clone(),
        vec![
            "zone-b-replica-1".into(),
            "zone-b-replica-2".into(),
            "zone-d-replica-1".into(),
            "zone-d-replica-2".into(),
        ],
    );
    assert_elements_match(results.clone(), called.lock().clone());
    tracker.collect(0).await;
    tracker.assert_correct(&results, &[]);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_CancelsEntireZoneImmediatelyOnSingleFailure
#[tokio::test]
async fn test_do_until_quorum_cancels_entire_zone_immediately_on_single_failure() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr_zone("zone-a-replica-1", "zone-a"),
            inst_addr_zone("zone-a-replica-2", "zone-a"),
            inst_addr_zone("zone-b-replica-1", "zone-b"),
            inst_addr_zone("zone-b-replica-2", "zone-b"),
            inst_addr_zone("zone-c-replica-1", "zone-c"),
            inst_addr_zone("zone-c-replica-2", "zone-c"),
        ],
        max_unavailable_zones: 1,
        ..Default::default()
    };

    let tracker = CleanupTracker::new(1, false);
    let failing_zone = Arc::new(Mutex::new(String::new()));
    let saw_cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_notify = Arc::new(Notify::new());
    let failing_barrier = Arc::new(Barrier::new(2));

    let cfg = DoUntilQuorumConfig {
        minimize_requests: true,
        ..Default::default()
    };
    let tr_c = Arc::clone(&tracker);
    let tr_cl = Arc::clone(&tracker);
    let fz_c = Arc::clone(&failing_zone);
    let sc_c = Arc::clone(&saw_cancelled);
    let cn_c = Arc::clone(&cancelled_notify);
    let fb_c = Arc::clone(&failing_barrier);

    let results = do_until_quorum_without_successful_context_cancellation(
        &RingContext::background(),
        &rs,
        &cfg,
        move |ctx, desc, cancel| {
            let tr = Arc::clone(&tr_c);
            let fz = Arc::clone(&fz_c);
            let sc = Arc::clone(&sc_c);
            let cn = Arc::clone(&cn_c);
            let fb = Arc::clone(&fb_c);
            async move {
                tr.track_call(&ctx, &desc, Some(cancel));
                let zone_name = {
                    let mut g = fz.lock();
                    if g.is_empty() {
                        *g = desc.zone.clone();
                    }
                    g.clone()
                };
                if desc.addr == format!("{zone_name}-replica-1") {
                    fb.wait().await;
                    return Err(Arc::new(SimpleError("this is the failing instance".into())) as BoxDynError);
                } else if desc.addr == format!("{zone_name}-replica-2") {
                    fb.wait().await;
                    tokio::select! {
                        _ = ctx.done() => {
                            sc.store(true, Ordering::Release);
                            cn.notify_waiters();
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            cn.notify_waiters();
                            return Err(Arc::new(SimpleError("timed out waiting for cancellation".into())) as BoxDynError);
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cn.notified() => {}
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {
                            return Err(Arc::new(SimpleError("timed out waiting for failing zone".into())) as BoxDynError);
                        }
                    }
                }
                Ok(desc.addr)
            }
        },
        move |s| tr_cl.cleanup(s),
    )
    .await
    .unwrap();

    assert!(saw_cancelled.load(Ordering::Acquire));
    let fz = failing_zone.lock().clone();
    tracker.collect(1).await;
    tracker.assert_correct(&results, &[format!("{fz}-replica-2")]);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_RunsCallsInParallel
#[tokio::test]
async fn test_do_until_quorum_runs_calls_in_parallel() {
    for minimize in [true, false] {
        let rs = ReplicationSet {
            instances: vec![inst_addr("replica-1"), inst_addr("replica-2")],
            ..Default::default()
        };
        let barrier = Arc::new(Barrier::new(2));
        let cfg = DoUntilQuorumConfig {
            minimize_requests: minimize,
            ..Default::default()
        };
        let results = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            move |_, desc, _| {
                let b = Arc::clone(&barrier);
                async move {
                    b.wait().await;
                    Ok(desc.addr)
                }
            },
            |_| {},
        )
        .await
        .unwrap();
        assert_elements_match(results, vec!["replica-1".into(), "replica-2".into()]);
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_ReturnsMinimumResultSetForZoneAwareWhenAllSucceed
#[tokio::test]
async fn test_do_until_quorum_returns_minimum_result_set_for_zone_aware_when_all_succeed() {
    let instances = vec![
        inst_addr_zone("zone-a-replica-1", "zone-a"),
        inst_addr_zone("zone-a-replica-2", "zone-a"),
        inst_addr_zone("zone-b-replica-1", "zone-b"),
        inst_addr_zone("zone-b-replica-2", "zone-b"),
        inst_addr_zone("zone-c-replica-1", "zone-c"),
        inst_addr_zone("zone-c-replica-2", "zone-c"),
    ];
    for minimize in [true, false] {
        let rs = ReplicationSet {
            instances: instances.clone(),
            max_unavailable_zones: 1,
            ..Default::default()
        };
        let tracker = CleanupTracker::new(2, false);
        let called = Arc::new(Mutex::new(Vec::<InstanceDesc>::new()));
        let cfg = DoUntilQuorumConfig {
            minimize_requests: minimize,
            ..Default::default()
        };
        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let called_c = Arc::clone(&called);
        let results = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            move |ctx, desc, cancel| {
                let tr = Arc::clone(&tr_c);
                let cl = Arc::clone(&called_c);
                async move {
                    tr.track_call(&ctx, &desc, Some(cancel));
                    cl.lock().push(desc.clone());
                    Ok(desc.addr)
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 4);
        let called_list = called.lock().clone();
        if minimize {
            assert_eq!(unique_zone_count(&called_list), 2);
        }
        let exp_cleanup: Vec<String> = called_list
            .iter()
            .filter(|i| !results.contains(&i.addr))
            .map(|i| i.addr.clone())
            .collect();
        tracker.collect(exp_cleanup.len()).await;
        tracker.assert_correct(&results, &exp_cleanup);
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_ReturnsMinimumResultSetForNonZoneAwareWhenAllSucceed
#[tokio::test]
async fn test_do_until_quorum_returns_minimum_result_set_for_non_zone_aware_when_all_succeed() {
    let instances = vec![
        inst_addr_zone("zone-a-replica-1", "zone-a"),
        inst_addr_zone("zone-a-replica-2", "zone-a"),
        inst_addr_zone("zone-b-replica-1", "zone-b"),
        inst_addr_zone("zone-b-replica-2", "zone-b"),
        inst_addr_zone("zone-c-replica-1", "zone-c"),
        inst_addr_zone("zone-c-replica-2", "zone-c"),
    ];
    for minimize in [true, false] {
        let rs = ReplicationSet {
            instances: instances.clone(),
            max_errors: 1,
            ..Default::default()
        };
        let tracker = CleanupTracker::new(if minimize { 0 } else { 1 }, false);
        let called = Arc::new(Mutex::new(Vec::<String>::new()));
        let cfg = DoUntilQuorumConfig {
            minimize_requests: minimize,
            ..Default::default()
        };
        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let called_c = Arc::clone(&called);
        let results = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            move |ctx, desc, cancel| {
                let tr = Arc::clone(&tr_c);
                let cl = Arc::clone(&called_c);
                async move {
                    tr.track_call(&ctx, &desc, Some(cancel));
                    cl.lock().push(desc.addr.clone());
                    Ok(desc.addr)
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 5);
        let called_list = called.lock().clone();
        if minimize {
            assert_eq!(called_list.len(), 5);
        }
        let exp_cleanup: Vec<String> = called_list
            .iter()
            .filter(|a| !results.contains(a))
            .cloned()
            .collect();
        tracker.collect(exp_cleanup.len()).await;
        tracker.assert_correct(&results, &exp_cleanup);
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_DoesNotWaitForUnnecessarySlowResponses
#[tokio::test]
async fn test_do_until_quorum_does_not_wait_for_unnecessary_slow_responses() {
    let cases = [
        (
            ReplicationSet {
                instances: vec![inst_addr("instance-1"), inst_addr("instance-2-slow"), inst_addr("instance-3")],
                max_errors: 1,
                ..Default::default()
            },
            vec!["instance-1", "instance-3"],
            1usize,
        ),
        (
            ReplicationSet {
                instances: vec![
                    inst_addr_zone("zone-a-instance-1", "zone-a"),
                    inst_addr_zone("zone-a-instance-2-slow", "zone-a"),
                    inst_addr_zone("zone-b-instance-1", "zone-b"),
                    inst_addr_zone("zone-b-instance-2", "zone-b"),
                    inst_addr_zone("zone-c-instance-1", "zone-c"),
                    inst_addr_zone("zone-c-instance-2", "zone-c"),
                ],
                max_unavailable_zones: 1,
                ..Default::default()
            },
            vec!["zone-b-instance-1", "zone-b-instance-2", "zone-c-instance-1", "zone-c-instance-2"],
            2usize,
        ),
    ];

    for (rs, expected, max_cleanup) in cases {
        let tracker = CleanupTracker::new(max_cleanup, false);
        let wait_notify = Arc::new(Notify::new());
        let done_flag = Arc::new(AtomicBool::new(false));
        let called = Arc::new(Mutex::new(Vec::<String>::new()));
        let cfg = DoUntilQuorumConfig {
            minimize_requests: false,
            ..Default::default()
        };
        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let wn_c = Arc::clone(&wait_notify);
        let df_c = Arc::clone(&done_flag);
        let cl_c = Arc::clone(&called);

        let actual = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            move |ctx, desc, cancel| {
                let tr = Arc::clone(&tr_c);
                let wn = Arc::clone(&wn_c);
                let df = Arc::clone(&df_c);
                let cl = Arc::clone(&cl_c);
                async move {
                    tr.track_call(&ctx, &desc, Some(cancel));
                    cl.lock().push(desc.addr.clone());
                    if desc.addr.ends_with("-slow") && !df.load(Ordering::Acquire) {
                        tokio::select! {
                            _ = wn.notified() => {}
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                                return Err(Arc::new(SimpleError("waited for slow response".into())) as BoxDynError);
                            }
                        }
                    }
                    Ok(desc.addr)
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap();

        done_flag.store(true, Ordering::Release);
        wait_notify.notify_waiters();

        let exp_vec: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
        assert_elements_match(actual.clone(), exp_vec.clone());
        let exp_cleanup: Vec<String> = called
            .lock()
            .iter()
            .filter(|a| !actual.contains(a))
            .cloned()
            .collect();
        tracker.collect(exp_cleanup.len()).await;
        tracker.assert_correct(&exp_vec, &exp_cleanup);
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_ParentContextHandling_WithoutMinimizeRequests
#[tokio::test]
async fn test_do_until_quorum_parent_context_handling_without_minimize_requests() {
    let base_ctx = RingContext::background().with_value::<TestContextKey, String>("this-is-the-value-from-the-parent".into());
    let (parent_ctx, parent_cancel) = base_ctx.with_cancel();
    let logger = Arc::new(TestTrackerLogger::default());
    let rs = ReplicationSet {
        instances: vec![inst_addr("instance-1"), inst_addr("instance-2"), inst_addr("instance-3")],
        partition_id: 123,
        ..Default::default()
    };
    let tracker = CleanupTracker::new(3, false);
    let cfg = DoUntilQuorumConfig {
        minimize_requests: false,
        logger: Some(logger.clone()),
        ..Default::default()
    };
    let tr_c = Arc::clone(&tracker);
    let tr_cl = Arc::clone(&tracker);
    let pc_c = parent_cancel.clone();

    let err = do_until_quorum_without_successful_context_cancellation(
        &parent_ctx,
        &rs,
        &cfg,
        move |ctx, desc, cancel| {
            let tr = Arc::clone(&tr_c);
            let pc = pc_c.clone();
            async move {
                tr.track_call(&ctx, &desc, Some(cancel));
                assert_eq!(
                    ctx.value::<TestContextKey, String>().as_deref().map(String::as_str),
                    Some("this-is-the-value-from-the-parent")
                );
                if desc.addr == "instance-1" {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        pc.cancel(None);
                    });
                } else {
                    tokio::select! {
                        _ = ctx.done() => {}
                        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("timed out waiting for cancellation"),
                    }
                }
                Ok(desc.addr)
            }
        },
        move |s| tr_cl.cleanup(s),
    )
    .await
    .unwrap_err();

    assert_eq!(err.to_string(), "context canceled");
    tracker.collect(3).await;
    tracker.assert_correct(&[], &["instance-1".into(), "instance-2".into(), "instance-3".into()]);
    assert!(logger.contains(
        "debug",
        "parent context done, returning",
        &[("err", "context canceled"), ("partition_id", "123")]
    ));
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_ParentContextHandling_WithMinimizeRequests
#[tokio::test]
async fn test_do_until_quorum_parent_context_handling_with_minimize_requests() {
    let cases = [
        ReplicationSet {
            instances: vec![inst_addr_zone("instance-1", "zone-a"), inst_addr_zone("instance-2", "zone-b"), inst_addr_zone("instance-3", "zone-c")],
            max_unavailable_zones: 1,
            partition_id: 123,
            ..Default::default()
        },
        ReplicationSet {
            instances: vec![inst_addr("instance-1"), inst_addr("instance-2"), inst_addr("instance-3")],
            max_errors: 1,
            partition_id: 123,
            ..Default::default()
        },
    ];

    for rs in cases {
        let base_ctx = RingContext::background().with_value::<TestContextKey, String>("this-is-the-value-from-the-parent".into());
        let (parent_ctx, cancel) = base_ctx.with_cancel();
        let tracker = CleanupTracker::new(2, false);
        let logger = Arc::new(TestTrackerLogger::default());
        let started = Arc::new(AtomicUsize::new(0));
        let all_started = Arc::new(Notify::new());
        let cfg = DoUntilQuorumConfig {
            minimize_requests: true,
            logger: Some(logger.clone()),
            ..Default::default()
        };

        let st_c = Arc::clone(&started);
        let as_c = Arc::clone(&all_started);
        let cancel_c = cancel.clone();
        tokio::spawn(async move {
            while st_c.load(Ordering::Acquire) < 2 {
                as_c.notified().await;
            }
            cancel_c.cancel(None);
        });

        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let st_c2 = Arc::clone(&started);
        let as_c2 = Arc::clone(&all_started);
        let err = do_until_quorum_without_successful_context_cancellation(
            &parent_ctx,
            &rs,
            &cfg,
            move |ctx, desc, cancel| {
                let tr = Arc::clone(&tr_c);
                let st = Arc::clone(&st_c2);
                let as_n = Arc::clone(&as_c2);
                async move {
                    tr.track_call(&ctx, &desc, Some(cancel));
                    assert_eq!(
                        ctx.value::<TestContextKey, String>().as_deref().map(String::as_str),
                        Some("this-is-the-value-from-the-parent")
                    );
                    if st.fetch_add(1, Ordering::AcqRel) + 1 >= 2 {
                        as_n.notify_waiters();
                    }
                    tokio::select! {
                        _ = ctx.done() => {}
                        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("timed out"),
                    }
                    Ok(desc.addr)
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "context canceled");
        let called = tracker.called_instances();
        assert_eq!(called.len(), 2);
        tracker.collect(2).await;
        tracker.assert_correct(&[], &called);
        let logs = logger.all();
        let last = logs.last().unwrap();
        assert_eq!(last.msg, "parent context done, returning");
        assert_eq!(last.fields.get("partition_id").map(String::as_str), Some("123"));
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_InstanceContextHandling
#[tokio::test]
async fn test_do_until_quorum_instance_context_handling() {
    let cases = [
        (
            ReplicationSet {
                instances: vec![
                    inst_addr_zone("instance-1", "zone-a"),
                    inst_addr_zone("instance-2", "zone-a"),
                    inst_addr_zone("instance-3", "zone-b"),
                    inst_addr_zone("instance-4", "zone-b"),
                    inst_addr_zone("instance-5", "zone-c"),
                    inst_addr_zone("instance-6", "zone-c"),
                ],
                max_unavailable_zones: 1,
                ..Default::default()
            },
            4usize,
        ),
        (
            ReplicationSet {
                instances: vec![inst_addr("instance-1"), inst_addr("instance-2"), inst_addr("instance-3")],
                max_errors: 1,
                ..Default::default()
            },
            2usize,
        ),
    ];

    for (rs, exp_count) in cases {
        let tracker = CleanupTracker::new(0, false);
        let cfg = DoUntilQuorumConfig {
            minimize_requests: true,
            ..Default::default()
        };
        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let results = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            move |ctx, desc, cancel| {
                let tr = Arc::clone(&tr_c);
                async move {
                    tr.track_call(&ctx, &desc, Some(cancel));
                    Ok(desc.addr)
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), exp_count);
        tracker.collect(0).await;
        tracker.assert_correct(&results, &[]);

        let first_inst = &results[0];
        let cancel = tracker.cancels.lock().get(first_inst).unwrap().clone();
        cancel.cancel(Some(Arc::new(CancellationError::new("cancelling first instance"))));

        let ctxs = tracker.contexts.lock();
        for addr in &results {
            let ctx = &ctxs[addr];
            if addr == first_inst {
                assert_eq!(ctx.err(), Some(ContextError::Canceled));
            } else {
                assert_eq!(ctx.err(), None);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_InvalidHedgingConfig
#[tokio::test]
async fn test_do_until_quorum_invalid_hedging_config() {
    let cfg = DoUntilQuorumConfig {
        minimize_requests: true,
        hedging_delay: GoDuration::from_secs(-1),
        ..Default::default()
    };
    let err = do_until_quorum_without_successful_context_cancellation(
        &RingContext::background(),
        &ReplicationSet::default(),
        &cfg,
        |_, _, _| async { panic!("should never call f") as Result<String, BoxDynError> },
        |_| panic!("should never call cleanup"),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid DoUntilQuorumConfig: HedgingDelay must be non-negative"
    );
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_Hedging_ZoneAware
#[tokio::test]
async fn test_do_until_quorum_hedging_zone_aware() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr_zone("instance-1", "zone-a"),
            inst_addr_zone("instance-2", "zone-a"),
            inst_addr_zone("instance-3", "zone-b"),
            inst_addr_zone("instance-4", "zone-b"),
            inst_addr_zone("instance-5", "zone-c"),
            inst_addr_zone("instance-6", "zone-c"),
            inst_addr_zone("instance-7", "zone-d"),
            inst_addr_zone("instance-8", "zone-d"),
        ],
        max_unavailable_zones: 2,
        ..Default::default()
    };
    let tracker = CleanupTracker::new(4, false);
    let barrier = Arc::new(Barrier::new(8));
    let invocations = Arc::new(Mutex::new(Vec::<(InstanceDesc, Duration)>::new()));
    let started_at = Instant::now();
    let hedging = Duration::from_secs(1);

    let cfg = DoUntilQuorumConfig {
        minimize_requests: true,
        hedging_delay: GoDuration::from_secs(1),
        ..Default::default()
    };
    let tr_c = Arc::clone(&tracker);
    let tr_cl = Arc::clone(&tracker);
    let inv_c = Arc::clone(&invocations);
    let bar_c = Arc::clone(&barrier);

    let successful = do_until_quorum_without_successful_context_cancellation(
        &RingContext::background(),
        &rs,
        &cfg,
        move |ctx, desc, cancel| {
            let tr = Arc::clone(&tr_c);
            let inv = Arc::clone(&inv_c);
            let b = Arc::clone(&bar_c);
            async move {
                let elapsed = started_at.elapsed();
                tr.track_call(&ctx, &desc, Some(cancel));
                inv.lock().push((desc.clone(), elapsed));
                b.wait().await;
                Ok(desc.addr)
            }
        },
        move |s| tr_cl.cleanup(s),
    )
    .await
    .unwrap();
    assert_eq!(successful.len(), 4);
    let cleaned = tracker.collect(4).await;
    tracker.assert_correct(&successful, &cleaned);

    let inv = invocations.lock();
    assert_eq!(unique_zone_count(&inv[0..4].iter().map(|(d, _)| d.clone()).collect::<Vec<_>>()), 2);
    assert_eq!(unique_zone_count(&inv[4..6].iter().map(|(d, _)| d.clone()).collect::<Vec<_>>()), 1);
    assert_eq!(unique_zone_count(&inv[6..8].iter().map(|(d, _)| d.clone()).collect::<Vec<_>>()), 1);

    let tol = Duration::from_millis(200);
    for (_, d) in &inv[0..4] {
        assert!(*d < tol, "got {d:?}");
    }
    for (_, d) in &inv[4..6] {
        assert!(d.abs_diff(hedging) < tol, "got {d:?}");
    }
    for (_, d) in &inv[6..8] {
        assert!(d.abs_diff(2 * hedging) < tol, "got {d:?}");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_Hedging_NonZoneAware
#[tokio::test]
async fn test_do_until_quorum_hedging_non_zone_aware() {
    let rs = ReplicationSet {
        instances: vec![
            inst_addr("instance-1"),
            inst_addr("instance-2"),
            inst_addr("instance-3"),
            inst_addr("instance-4"),
        ],
        max_errors: 2,
        ..Default::default()
    };
    let tracker = CleanupTracker::new(2, false);
    let barrier = Arc::new(Barrier::new(4));
    let invocations = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let started_at = Instant::now();
    let hedging = Duration::from_secs(1);

    let cfg = DoUntilQuorumConfig {
        minimize_requests: true,
        hedging_delay: GoDuration::from_secs(1),
        ..Default::default()
    };
    let tr_c = Arc::clone(&tracker);
    let tr_cl = Arc::clone(&tracker);
    let inv_c = Arc::clone(&invocations);
    let bar_c = Arc::clone(&barrier);

    let successful = do_until_quorum_without_successful_context_cancellation(
        &RingContext::background(),
        &rs,
        &cfg,
        move |ctx, desc, cancel| {
            let tr = Arc::clone(&tr_c);
            let inv = Arc::clone(&inv_c);
            let b = Arc::clone(&bar_c);
            async move {
                let elapsed = started_at.elapsed();
                tr.track_call(&ctx, &desc, Some(cancel));
                inv.lock().push(elapsed);
                b.wait().await;
                Ok(desc.addr)
            }
        },
        move |s| tr_cl.cleanup(s),
    )
    .await
    .unwrap();
    assert_eq!(successful.len(), 2);
    let cleaned = tracker.collect(2).await;
    tracker.assert_correct(&successful, &cleaned);

    let inv = invocations.lock();
    let tol = Duration::from_millis(200);
    assert!(inv[0] < tol);
    assert!(inv[1] < tol);
    assert!(inv[2].abs_diff(hedging) < tol);
    assert!(inv[3].abs_diff(2 * hedging) < tol);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorumWithoutSuccessfulContextCancellation_LoggingWithNoFailingInstancesOrZones
#[tokio::test]
async fn test_do_until_quorum_logging_with_no_failing_instances_or_zones() {
    let instances = vec![
        inst_addr_zone("instance-1", "zone-a"),
        inst_addr_zone("instance-2", "zone-a"),
        inst_addr_zone("instance-3", "zone-b"),
        inst_addr_zone("instance-4", "zone-b"),
        inst_addr_zone("instance-5", "zone-c"),
        inst_addr_zone("instance-6", "zone-c"),
    ];

    for zone_aware in [true, false] {
        let rs = ReplicationSet {
            instances: instances.clone(),
            zone_awareness_enabled: zone_aware,
            ..Default::default()
        };
        let logger = Arc::new(TestTrackerLogger::default());
        let cfg = DoUntilQuorumConfig {
            logger: Some(logger.clone()),
            ..Default::default()
        };
        let results = do_until_quorum_without_successful_context_cancellation(
            &RingContext::background(),
            &rs,
            &cfg,
            |_, desc, _| async move { Ok(desc.addr) },
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 6);

        let expected_msg = if zone_aware {
            "starting requests to zone"
        } else {
            "starting request to instance"
        };
        let forbidden_msg = if zone_aware {
            "starting request to instance"
        } else {
            "starting requests to zone"
        };
        let logs = logger.all();
        assert!(!logs.iter().any(|l| l.msg == forbidden_msg));
        let count = logs.iter().filter(|l| l.msg == expected_msg).count();
        assert_eq!(count, if zone_aware { 3 } else { 6 });
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoUntilQuorum_InstanceContextHandling
#[tokio::test]
async fn test_do_until_quorum_instance_context_handling_with_cancel() {
    let cases = [
        (
            ReplicationSet {
                instances: vec![
                    inst_addr_zone("instance-1", "zone-a"),
                    inst_addr_zone("instance-2", "zone-a"),
                    inst_addr_zone("instance-3", "zone-b"),
                    inst_addr_zone("instance-4", "zone-b"),
                    inst_addr_zone("instance-5", "zone-c"),
                    inst_addr_zone("instance-6", "zone-c"),
                ],
                max_unavailable_zones: 1,
                ..Default::default()
            },
            4usize,
        ),
        (
            ReplicationSet {
                instances: vec![inst_addr("instance-1"), inst_addr("instance-2"), inst_addr("instance-3")],
                max_errors: 1,
                ..Default::default()
            },
            2usize,
        ),
    ];

    for (rs, exp_count) in cases {
        let ctx = RingContext::background()
            .with_value::<TestContextKey, String>("this-is-the-value-from-the-parent".into());
        let tracker = CleanupTracker::new(0, true);
        let cfg = DoUntilQuorumConfig {
            minimize_requests: true,
            ..Default::default()
        };
        let tr_c = Arc::clone(&tracker);
        let tr_cl = Arc::clone(&tracker);
        let results = do_until_quorum(
            &ctx,
            &rs,
            &cfg,
            move |ictx, desc| {
                let tr = Arc::clone(&tr_c);
                async move {
                    tr.track_call(&ictx, &desc, None);
                    assert_eq!(
                        ictx.value::<TestContextKey, String>().as_deref().map(String::as_str),
                        Some("this-is-the-value-from-the-parent")
                    );
                    Ok(desc.addr)
                }
            },
            move |s| tr_cl.cleanup(s),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), exp_count);
        tracker.collect(0).await;
        tracker.assert_correct(&results, &[]);
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestDoMultiUntilQuorumWithoutSuccessfulContextCancellation
#[tokio::test]
async fn test_do_multi_until_quorum_without_successful_context_cancellation() {
    let cfg = DoUntilQuorumConfig::default();
    let sets = vec![
        ReplicationSet {
            instances: vec![inst_addr("instance-1"), inst_addr("instance-2")],
            ..Default::default()
        },
        ReplicationSet {
            instances: vec![inst_addr("instance-3")],
            ..Default::default()
        },
    ];

    // 1. cancels before returning
    {
        let (res, wctx) = do_multi_until_quorum_inner(
            &RingContext::background(),
            &sets,
            &cfg,
            |ctx, inst, cancel| async move {
                assert_eq!(ctx.err(), None);
                cancel.cancel(None);
                assert_eq!(ctx.err(), Some(ContextError::Canceled));
                Ok(inst.addr)
            },
            |_| {},
        )
        .await;
        assert_elements_match(
            res.unwrap(),
            vec!["instance-1".into(), "instance-2".into(), "instance-3".into()],
        );
        assert_eq!(wctx.err(), Some(ContextError::Canceled));
        assert_eq!(wctx.cause().unwrap().to_string(), "all requests completed");
    }

    // 2. cancels after returning
    {
        let tracked = Arc::new(Mutex::new(Vec::<(RingContext, CancelCauseFunc)>::new()));
        let tr_c = Arc::clone(&tracked);
        let (res, wctx) = do_multi_until_quorum_inner(
            &RingContext::background(),
            &sets,
            &cfg,
            move |ctx, inst, cancel| {
                let tr = Arc::clone(&tr_c);
                async move {
                    tr.lock().push((ctx, cancel));
                    Ok(inst.addr)
                }
            },
            |_| {},
        )
        .await;
        assert_elements_match(
            res.unwrap(),
            vec!["instance-1".into(), "instance-2".into(), "instance-3".into()],
        );
        let list = tracked.lock();
        for (ctx, cancel) in list.iter() {
            assert_eq!(wctx.err(), None);
            assert_eq!(ctx.err(), None);
            cancel.cancel(None);
        }
        assert_eq!(wctx.err(), Some(ContextError::Canceled));
        assert_eq!(wctx.cause().unwrap().to_string(), "all requests completed");
    }

    // 3. cancels twice (idempotent)
    {
        let tracked = Arc::new(Mutex::new(Vec::<(RingContext, CancelCauseFunc)>::new()));
        let tr_c = Arc::clone(&tracked);
        let (res, wctx) = do_multi_until_quorum_inner(
            &RingContext::background(),
            &sets,
            &cfg,
            move |ctx, inst, cancel| {
                let tr = Arc::clone(&tr_c);
                async move {
                    tr.lock().push((ctx, cancel));
                    Ok(inst.addr)
                }
            },
            |_| {},
        )
        .await;
        assert_eq!(res.unwrap().len(), 3);
        let list = tracked.lock();
        for (ctx, cancel) in list.iter() {
            assert_eq!(wctx.err(), None);
            assert_eq!(ctx.err(), None);
            cancel.cancel(None);
            cancel.cancel(None);
        }
        assert_eq!(wctx.err(), Some(ContextError::Canceled));
        assert_eq!(wctx.cause().unwrap().to_string(), "all requests completed");
    }

    // 4. partial failure but quorum reached
    for minimize in [false, true] {
        let cfg_m = DoUntilQuorumConfig {
            minimize_requests: minimize,
            ..Default::default()
        };
        let sets_m = vec![
            ReplicationSet {
                instances: vec![inst_addr("instance-fail"), inst_addr("instance-success")],
                max_errors: 1,
                ..Default::default()
            },
            ReplicationSet {
                instances: vec![inst_addr("instance-fail"), inst_addr("instance-success")],
                max_errors: 1,
                ..Default::default()
            },
        ];
        let (res, wctx) = do_multi_until_quorum_inner(
            &RingContext::background(),
            &sets_m,
            &cfg_m,
            |ctx, inst, cancel| async move {
                if inst.addr == "instance-fail" {
                    cancel.cancel(None);
                    return Err(Arc::new(SimpleError("mocked error".into())) as BoxDynError);
                }
                let out = tokio::select! {
                    _ = ctx.done() => panic!("context must not be canceled"),
                    _ = tokio::time::sleep(Duration::from_millis(300)) => Ok(inst.addr),
                };
                cancel.cancel(None);
                out
            },
            |_| {},
        )
        .await;
        assert_eq!(res.unwrap(), vec!["instance-success", "instance-success"]);
        assert_eq!(wctx.err(), Some(ContextError::Canceled));
    }

    // 5. break on first error
    for failing in ["instance-1", "instance-2", "instance-3"] {
        let started = Arc::new(Barrier::new(3));
        let ended = Arc::new(AtomicUsize::new(0));
        let ended_n = Arc::new(Notify::new());
        let st_c = Arc::clone(&started);
        let en_c = Arc::clone(&ended);
        let enn_c = Arc::clone(&ended_n);
        let fail_str = failing.to_string();
        let (res, wctx) = do_multi_until_quorum_inner(
            &RingContext::background(),
            &sets,
            &cfg,
            move |ctx, inst, cancel| {
                let st = Arc::clone(&st_c);
                let en = Arc::clone(&en_c);
                let enn = Arc::clone(&enn_c);
                let fs = fail_str.clone();
                async move {
                    st.wait().await;
                    let r = if inst.addr == fs {
                        Err(Arc::new(SimpleError("mocked error".into())) as BoxDynError)
                    } else {
                        tokio::select! {
                            _ = ctx.done() => Err(ctx.cause().unwrap()),
                            _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("expected cancellation"),
                        }
                    };
                    cancel.cancel(None);
                    en.fetch_add(1, Ordering::AcqRel);
                    enn.notify_waiters();
                    r
                }
            },
            |_: String| {},
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), "mocked error");
        assert_eq!(wctx.err(), Some(ContextError::Canceled));
        assert!(wctx.cause().unwrap().to_string().contains("mocked error"));
        while ended.load(Ordering::Acquire) < 3 {
            ended_n.notified().await;
        }
    }

    // 6. caller context canceled
    {
        let (caller_ctx, caller_cancel) = RingContext::background().with_cancel();
        let started = Arc::new(AtomicUsize::new(0));
        let ended = Arc::new(AtomicUsize::new(0));
        let s_notify = Arc::new(Notify::new());
        let e_notify = Arc::new(Notify::new());

        let s_c = Arc::clone(&started);
        let sn_c = Arc::clone(&s_notify);
        tokio::spawn(async move {
            while s_c.load(Ordering::Acquire) < 3 {
                sn_c.notified().await;
            }
            caller_cancel.cancel(None);
        });

        let s_c2 = Arc::clone(&started);
        let sn_c2 = Arc::clone(&s_notify);
        let e_c2 = Arc::clone(&ended);
        let en_c2 = Arc::clone(&e_notify);
        let (res, wctx) = do_multi_until_quorum_inner(
            &caller_ctx,
            &sets,
            &cfg,
            move |ctx, _inst, _cancel| {
                let s = Arc::clone(&s_c2);
                let sn = Arc::clone(&sn_c2);
                let e = Arc::clone(&e_c2);
                let en = Arc::clone(&en_c2);
                async move {
                    s.fetch_add(1, Ordering::AcqRel);
                    sn.notify_waiters();
                    let r: Result<String, BoxDynError> = tokio::select! {
                        _ = ctx.done() => Err(ctx.cause().unwrap()),
                        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("expected cancel"),
                    };
                    e.fetch_add(1, Ordering::AcqRel);
                    en.notify_waiters();
                    r
                }
            },
            |_: String| {},
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), "context canceled");
        assert_eq!(wctx.err(), Some(ContextError::Canceled));
        while ended.load(Ordering::Acquire) < 3 {
            e_notify.notified().await;
        }
    }
}

fn rs_changes_initial() -> ReplicationSet {
    ReplicationSet {
        instances: vec![
            InstanceDesc { id: "ingester-0".into(), ..Default::default() },
            InstanceDesc { id: "ingester-1".into(), ..Default::default() },
            InstanceDesc { id: "ingester-2".into(), ..Default::default() },
        ],
        ..Default::default()
    }
}

fn rs_changes_cases() -> Vec<(&'static str, ReplicationSet, bool, bool, bool)> {
    vec![
        (
            "timestamp changed",
            ReplicationSet {
                instances: vec![
                    InstanceDesc { id: "ingester-0".into(), timestamp: 3_600_000_000, ..Default::default() },
                    InstanceDesc { id: "ingester-1".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-2".into(), ..Default::default() },
                ],
                ..Default::default()
            },
            false,
            false,
            false,
        ),
        (
            "state changed",
            ReplicationSet {
                instances: vec![
                    InstanceDesc { id: "ingester-0".into(), state: InstanceState::Pending as i32, ..Default::default() },
                    InstanceDesc { id: "ingester-1".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-2".into(), ..Default::default() },
                ],
                ..Default::default()
            },
            true,
            false,
            false,
        ),
        (
            "addr changed",
            ReplicationSet {
                instances: vec![
                    InstanceDesc { id: "ingester-0".into(), addr: "127.0.0.1".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-1".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-2".into(), ..Default::default() },
                ],
                ..Default::default()
            },
            true,
            true,
            false,
        ),
        (
            "more instances",
            ReplicationSet {
                instances: vec![
                    InstanceDesc { id: "ingester-0".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-1".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-2".into(), ..Default::default() },
                    InstanceDesc { id: "ingester-4".into(), ..Default::default() },
                ],
                ..Default::default()
            },
            true,
            true,
            true,
        ),
    ]
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestHasReplicationSetChanged_IgnoresTimeStamp
#[test]
fn test_has_replication_set_changed_ignores_timestamp() {
    let init = rs_changes_initial();
    for (name, next, exp, _, _) in rs_changes_cases() {
        assert_eq!(has_replication_set_changed(&init, &next), exp, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestHasReplicationSetChangedWithoutState_IgnoresTimeStampAndState
#[test]
fn test_has_replication_set_changed_without_state_ignores_timestamp_and_state() {
    let init = rs_changes_initial();
    for (name, next, _, exp, _) in rs_changes_cases() {
        assert_eq!(has_replication_set_changed_without_state(&init, &next), exp, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestHasReplicationSetChangedWithoutStateOrAddr_IgnoresTimeStampAndStateAndAddr
#[test]
fn test_has_replication_set_changed_without_state_or_addr_ignores_timestamp_and_state_and_addr() {
    let init = rs_changes_initial();
    for (name, next, _, _, exp) in rs_changes_cases() {
        assert_eq!(has_replication_set_changed_without_state_or_addr(&init, &next), exp, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestReplicationSet_ZoneCount
#[test]
fn test_replication_set_zone_count() {
    let cases = [
        (vec![], 0usize),
        (vec![inst_addr("instance-1")], 1),
        (vec![inst_addr("instance-1"), inst_addr("instance-2"), inst_addr("instance-3")], 1),
        (vec![inst_addr_zone("instance-1", "zone-a")], 1),
        (vec![inst_addr_zone("instance-1", "zone-a"), inst_addr_zone("instance-2", "zone-a"), inst_addr_zone("instance-3", "zone-a")], 1),
        (vec![inst_addr_zone("instance-1", "zone-a"), inst_addr_zone("instance-2", "zone-b"), inst_addr_zone("instance-3", "zone-c")], 3),
        (
            vec![
                inst_addr_zone("za1", "zone-a"), inst_addr_zone("za2", "zone-a"), inst_addr_zone("za3", "zone-a"),
                inst_addr_zone("zb1", "zone-b"), inst_addr_zone("zb2", "zone-b"), inst_addr_zone("zb3", "zone-b"),
                inst_addr_zone("zc1", "zone-c"), inst_addr_zone("zc2", "zone-c"), inst_addr_zone("zc3", "zone-c"),
            ],
            3,
        ),
    ];
    for (instances, exp) in cases {
        let r = ReplicationSet {
            instances,
            ..Default::default()
        };
        assert_eq!(r.zone_count(), exp);
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_test.go::TestContextWithAvailableReplicas
#[test]
fn test_context_with_available_replicas() {
    let ctx = context_with_available_replicas(&RingContext::background(), 5);
    assert_eq!(get_available_replicas(&ctx), Some(5));
    assert_eq!(get_available_replicas(&RingContext::background()), None);
}

// ============================================================================
// replication_set_tracker_test.go (23)
// ============================================================================

fn inst_id_addr(id: &str, addr: &str) -> InstanceDesc {
    InstanceDesc {
        id: id.to_string(),
        addr: addr.to_string(),
        ..Default::default()
    }
}

async fn poll_until(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker
#[test]
fn test_default_result_tracker() {
    let instances = vec![
        inst_addr("127.0.0.1"),
        inst_addr("127.0.0.2"),
        inst_addr("127.0.0.3"),
        inst_addr("127.0.0.4"),
    ];
    let err = SimpleError("test".into());

    // 1. no instances
    {
        let t = DefaultResultTracker::new(vec![], 0, None);
        assert!(t.succeeded());
        assert!(!t.failed());
    }
    // 2. all succeed, max_errors = 0
    {
        let t = DefaultResultTracker::new(instances.clone(), 0, None);
        for i in 0..3 {
            assert!(!t.succeeded());
            assert!(!t.failed());
            t.done(Some(i), None);
        }
        t.done(Some(3), None);
        assert!(t.succeeded());
        assert!(!t.failed());
        for i in 0..4 {
            assert!(t.should_include_result_from(i));
        }
    }
    // 3. 1 failing, max_errors = 1
    {
        let t = DefaultResultTracker::new(instances.clone(), 1, None);
        t.done(Some(0), None);
        t.done(Some(1), Some(&err));
        t.done(Some(2), None);
        assert!(!t.succeeded());
        assert!(!t.failed());
        t.done(Some(3), None);
        assert!(t.succeeded());
        assert!(!t.failed());
    }
    // 4. fail on 1st with max_errors = 0
    {
        let t = DefaultResultTracker::new(instances.clone(), 0, None);
        t.done(Some(0), None);
        t.done(Some(1), Some(&err));
        assert!(!t.succeeded());
        assert!(t.failed());
    }
    // 5. fail on 2nd with max_errors = 1
    {
        let t = DefaultResultTracker::new(instances.clone(), 1, None);
        t.done(Some(0), None);
        t.done(Some(1), Some(&err));
        assert!(!t.failed());
        t.done(Some(2), Some(&err));
        assert!(t.failed());
    }
    // 6. fail on 3rd with max_errors = 2
    {
        let t = DefaultResultTracker::new(instances, 2, None);
        t.done(Some(0), None);
        t.done(Some(1), Some(&err));
        t.done(Some(2), Some(&err));
        assert!(!t.failed());
        t.done(Some(3), Some(&err));
        assert!(t.failed());
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_AwaitStart_ContextCancelled
#[tokio::test]
async fn test_default_result_tracker_await_start_context_cancelled() {
    let t = DefaultResultTracker::new(vec![inst_addr_zone("127.0.0.1", "zone-a")], 0, None);
    let (ctx, cancel) = RingContext::background().with_cancel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel(None);
    });
    let err = t.await_start(&ctx, 0).await.unwrap_err();
    assert_eq!(err.to_string(), "context canceled");
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartAllRequests
#[tokio::test]
async fn test_default_result_tracker_start_all_requests() {
    let instances = vec![
        inst_id_addr("instance-1", "127.0.0.1"),
        inst_id_addr("instance-2", "127.0.0.2"),
        inst_id_addr("instance-3", "127.0.0.3"),
        inst_id_addr("instance-4", "127.0.0.4"),
    ];
    let logger = Arc::new(TestTrackerLogger::default());
    let t = DefaultResultTracker::new(instances.clone(), 1, Some(logger.clone()));
    t.start_all_requests();

    for (idx, inst) in instances.iter().enumerate() {
        t.await_start(&RingContext::background(), idx).await.unwrap();
        assert!(logger.contains(
            "debug",
            "starting request to instance",
            &[
                ("reason", "initial requests"),
                ("instanceAddr", &inst.addr),
                ("instanceID", &inst.id),
            ]
        ));
    }
    assert_eq!(logger.all().len(), 4);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartMinimumRequests_NoFailingRequests
#[tokio::test]
async fn test_default_result_tracker_start_minimum_requests_no_failing() {
    let instances = vec![
        inst_id_addr("instance-1", "127.0.0.1"),
        inst_id_addr("instance-2", "127.0.0.2"),
        inst_id_addr("instance-3", "127.0.0.3"),
        inst_id_addr("instance-4", "127.0.0.4"),
    ];
    let counts: Vec<Arc<AtomicUsize>> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let iterations = 400usize;

    for _ in 0..iterations {
        let logger = Arc::new(TestTrackerLogger::default());
        let t = Arc::new(DefaultResultTracker::new(instances.clone(), 1, Some(logger.clone())));
        t.start_minimum_requests();

        let results = Arc::new(Mutex::new(HashMap::<usize, Result<(), BoxDynError>>::new()));
        for (idx, count_slot) in counts.iter().enumerate() {
            let tc = Arc::clone(&t);
            let rc = Arc::clone(&results);
            let cnt = Arc::clone(count_slot);
            tokio::spawn(async move {
                let r = tc.await_start(&RingContext::background(), idx).await;
                if r.is_ok() {
                    cnt.fetch_add(1, Ordering::AcqRel);
                }
                rc.lock().insert(idx, r);
            });
        }

        assert!(poll_until(Duration::from_secs(1), || results.lock().len() == 3).await);
        assert_eq!(results.lock().values().filter(|r| r.is_ok()).count(), 3);

        t.done(None, None);
        t.done(None, None);
        t.done(None, None);

        assert!(poll_until(Duration::from_secs(1), || results.lock().len() == 4).await);
        assert_eq!(results.lock().values().filter(|r| r.is_ok()).count(), 3);
        assert!(t.succeeded());
        assert_eq!(logger.all().len(), 3);
    }

    let expected = (iterations as f64) * 0.75;
    for c in &counts {
        let v = c.load(Ordering::Acquire) as f64;
        assert!((v - expected).abs() <= expected * 0.25, "got {v}, expected ~{expected}");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartMinimumRequests_FailingRequestsBelowMaximumAllowed
#[tokio::test]
async fn test_default_result_tracker_start_minimum_requests_failing_below_max() {
    let instances = vec![
        inst_id_addr("instance-1", "127.0.0.1"),
        inst_id_addr("instance-2", "127.0.0.2"),
        inst_id_addr("instance-3", "127.0.0.3"),
        inst_id_addr("instance-4", "127.0.0.4"),
    ];
    let logger = Arc::new(TestTrackerLogger::default());
    let t = Arc::new(DefaultResultTracker::new(instances.clone(), 2, Some(logger.clone())));
    t.start_minimum_requests();

    let released = Arc::new(Mutex::new(Vec::<usize>::new()));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..4 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            let len = {
                let mut g = rel.lock();
                g.push(idx);
                if r.is_ok() {
                    sig.fetch_add(1, Ordering::AcqRel);
                }
                g.len()
            };
            if len == 2 {
                tc.done(Some(idx), Some(&SimpleError("something went wrong".into())));
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 3).await);
    assert_eq!(signalled.load(Ordering::Acquire), 3);

    t.done(None, None);
    t.done(None, None);

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 4).await);
    assert_eq!(signalled.load(Ordering::Acquire), 3);
    assert!(t.succeeded());
    assert_eq!(logger.all().len(), 4);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartMinimumRequests_FailingRequestsEqualToMaximumAllowed
#[tokio::test]
async fn test_default_result_tracker_start_minimum_requests_failing_equal_to_max() {
    let instances = vec![
        inst_addr("127.0.0.1"),
        inst_addr("127.0.0.2"),
        inst_addr("127.0.0.3"),
        inst_addr("127.0.0.4"),
    ];
    let t = Arc::new(DefaultResultTracker::new(instances, 2, None));
    t.start_minimum_requests();

    let released = Arc::new(AtomicUsize::new(0));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..4 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            let n = rel.fetch_add(1, Ordering::AcqRel) + 1;
            if r.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
            if n <= 2 {
                tc.done(Some(idx), Some(&SimpleError("something went wrong".into())));
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.load(Ordering::Acquire) == 4).await);
    t.done(None, None);
    t.done(None, None);
    assert_eq!(signalled.load(Ordering::Acquire), 4);
    assert!(t.succeeded());
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartMinimumRequests_MoreFailingRequestsThanMaximumAllowed
#[tokio::test]
async fn test_default_result_tracker_start_minimum_requests_more_failing_than_max() {
    let instances = vec![
        inst_addr("127.0.0.1"),
        inst_addr("127.0.0.2"),
        inst_addr("127.0.0.3"),
        inst_addr("127.0.0.4"),
    ];
    let t = Arc::new(DefaultResultTracker::new(instances, 1, None));
    t.start_minimum_requests();

    let released = Arc::new(AtomicUsize::new(0));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..4 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            let n = rel.fetch_add(1, Ordering::AcqRel) + 1;
            if r.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
            if n <= 2 {
                tc.done(Some(idx), Some(&SimpleError("something went wrong".into())));
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.load(Ordering::Acquire) == 4).await);
    t.done(None, None);
    t.done(None, None);
    assert_eq!(signalled.load(Ordering::Acquire), 4);
    assert!(t.failed());
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartMinimumRequests_MaxErrorsIsNumberOfInstances
#[tokio::test]
async fn test_default_result_tracker_start_minimum_requests_max_errors_is_num_instances() {
    let t = DefaultResultTracker::new(vec![inst_addr("127.0.0.1")], 1, None);
    t.start_minimum_requests();
    let err = t.await_start(&RingContext::background(), 0).await.unwrap_err();
    assert_eq!(err.to_string(), ErrResultNotNeeded.to_string());
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultResultTracker_StartAdditionalRequests
#[tokio::test]
async fn test_default_result_tracker_start_additional_requests() {
    let instances = vec![
        inst_id_addr("instance-1", "127.0.0.1"),
        inst_id_addr("instance-2", "127.0.0.2"),
        inst_id_addr("instance-3", "127.0.0.3"),
        inst_id_addr("instance-4", "127.0.0.4"),
    ];
    let logger = Arc::new(TestTrackerLogger::default());
    let t = Arc::new(DefaultResultTracker::new(instances, 2, Some(logger.clone())));
    t.start_minimum_requests();

    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..4 {
        let tc = Arc::clone(&t);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            if tc.await_start(&RingContext::background(), idx).await.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || signalled.load(Ordering::Acquire) == 2).await);
    assert_eq!(logger.all().len(), 2);

    t.start_additional_requests();
    assert!(poll_until(Duration::from_secs(1), || signalled.load(Ordering::Acquire) == 3).await);
    assert_eq!(logger.all().len(), 3);

    t.start_additional_requests();
    assert!(poll_until(Duration::from_secs(1), || signalled.load(Ordering::Acquire) == 4).await);
    assert_eq!(logger.all().len(), 4);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestDefaultContextTracker
#[test]
fn test_default_context_tracker() {
    let parent_ctx = RingContext::background()
        .with_value::<TestContextKey, String>("this-is-the-value-from-the-parent".into());
    let t = DefaultContextTracker::new(parent_ctx, 4);

    let (c1, _) = t.context_for(0);
    let (c2, cancel2) = t.context_for(1);
    let (c3, _) = t.context_for(2);
    let (c4, _) = t.context_for(3);

    for c in [&c1, &c2, &c3, &c4] {
        assert_eq!(
            c.value::<TestContextKey, String>().as_deref().map(String::as_str),
            Some("this-is-the-value-from-the-parent")
        );
        assert_eq!(c.err(), None);
    }

    t.cancel_context_for(0, Some(Arc::new(CancellationError::new("instance 1 cancellation cause"))));
    assert_eq!(c1.err(), Some(ContextError::Canceled));
    assert_eq!(c1.cause().unwrap().to_string(), "context canceled: instance 1 cancellation cause");
    for c in [&c2, &c3, &c4] {
        assert_eq!(c.err(), None);
    }

    cancel2.cancel(Some(Arc::new(CancellationError::new("instance 2 cancellation cause"))));
    assert_eq!(c2.err(), Some(ContextError::Canceled));
    assert_eq!(c2.cause().unwrap().to_string(), "context canceled: instance 2 cancellation cause");
    for c in [&c3, &c4] {
        assert_eq!(c.err(), None);
    }

    t.cancel_all_contexts(Some(Arc::new(CancellationError::new("remaining instances cancellation cause"))));
    for c in [&c3, &c4] {
        assert_eq!(c.err(), Some(ContextError::Canceled));
        assert_eq!(c.cause().unwrap().to_string(), "context canceled: remaining instances cancellation cause");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker
#[test]
fn test_zone_aware_result_tracker() {
    let instances = vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-a"),
        inst_addr_zone("127.0.0.3", "zone-b"),
        inst_addr_zone("127.0.0.4", "zone-b"),
        inst_addr_zone("127.0.0.5", "zone-c"),
        inst_addr_zone("127.0.0.6", "zone-c"),
    ];
    let err = SimpleError("test".into());

    // 1. no instances
    {
        let t = ZoneAwareResultTracker::new(vec![], 0, None, None);
        assert!(t.succeeded());
        assert!(!t.failed());
    }
    // 2. all succeed max_unavail = 0
    {
        let t = ZoneAwareResultTracker::new(instances[..3].to_vec(), 0, None, None);
        t.done(Some(0), None);
        t.done(Some(1), None);
        assert!(!t.succeeded());
        t.done(Some(2), None);
        assert!(t.succeeded());
        assert!(!t.failed());
        assert!(t.should_include_result_from(0));
        assert!(t.should_include_result_from(1));
        assert!(t.should_include_result_from(2));
    }
    // 3. fail on 1st failing instance when max_unavail = 0
    {
        let t = ZoneAwareResultTracker::new(instances.clone(), 0, None, None);
        t.done(Some(0), None);
        t.done(Some(1), Some(&err));
        assert!(!t.succeeded());
        assert!(t.failed());
    }
    // 4. succeed with 2 failures in same zone when max_unavail = 1
    {
        let t = ZoneAwareResultTracker::new(instances.clone(), 1, None, None);
        t.done(Some(0), Some(&err));
        t.done(Some(1), Some(&err));
        assert!(!t.failed());
        for i in 2..5 {
            t.done(Some(i), None);
            assert!(!t.succeeded());
        }
        t.done(Some(5), None);
        assert!(t.succeeded());
        assert!(!t.failed());
        assert!(!t.should_include_result_from(0));
        assert!(!t.should_include_result_from(1));
        for i in 2..6 {
            assert!(t.should_include_result_from(i));
        }
    }
    // 5. succeed early from all zones - 1 when max_unavail = 1
    {
        let t = ZoneAwareResultTracker::new(instances.clone(), 1, None, None);
        for i in 0..4 {
            t.done(Some(i), None);
        }
        assert!(t.succeeded());
        assert!(!t.failed());
        for i in 0..4 {
            assert!(t.should_include_result_from(i));
        }
        assert!(!t.should_include_result_from(4));
        assert!(!t.should_include_result_from(5));
    }
    // 6. succeed on 2 failing zones when max_unavail = 2
    {
        let t = ZoneAwareResultTracker::new(instances.clone(), 2, None, None);
        for i in 0..4 {
            t.done(Some(i), Some(&err));
            assert!(!t.failed());
        }
        t.done(Some(4), None);
        t.done(Some(5), None);
        assert!(t.succeeded());
        assert!(!t.failed());
        for i in 0..4 {
            assert!(!t.should_include_result_from(i));
        }
        assert!(t.should_include_result_from(4));
        assert!(t.should_include_result_from(5));
    }
    // 7. succeed early from all zones - 2 when max_unavail = 2
    {
        let t = ZoneAwareResultTracker::new(instances, 2, None, None);
        t.done(Some(0), None);
        t.done(Some(2), None);
        assert!(!t.succeeded());
        t.done(Some(1), None);
        assert!(t.succeeded());
        assert!(t.should_include_result_from(0));
        assert!(t.should_include_result_from(1));
        for i in 2..6 {
            assert!(!t.should_include_result_from(i));
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_AwaitStart_ContextCancelled
#[tokio::test]
async fn test_zone_aware_result_tracker_await_start_context_cancelled() {
    let t = ZoneAwareResultTracker::new(vec![inst_addr_zone("127.0.0.1", "zone-a")], 0, None, None);
    let (ctx, cancel) = RingContext::background().with_cancel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel(None);
    });
    let err = t.await_start(&ctx, 0).await.unwrap_err();
    assert_eq!(err.to_string(), "context canceled");
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartAllRequests
#[tokio::test]
async fn test_zone_aware_result_tracker_start_all_requests() {
    let instances = vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-a"),
        inst_addr_zone("127.0.0.3", "zone-b"),
        inst_addr_zone("127.0.0.4", "zone-b"),
        inst_addr_zone("127.0.0.5", "zone-c"),
        inst_addr_zone("127.0.0.6", "zone-c"),
    ];
    let logger = Arc::new(TestTrackerLogger::default());
    let t = ZoneAwareResultTracker::new(instances, 1, None, Some(logger.clone()));
    t.start_all_requests();

    for idx in 0..6 {
        t.await_start(&RingContext::background(), idx).await.unwrap();
    }
    assert_eq!(logger.all().len(), 3);
    for z in ["zone-a", "zone-b", "zone-c"] {
        assert!(logger.contains(
            "debug",
            "starting requests to zone",
            &[("reason", "initial requests"), ("zone", z)]
        ));
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_NoFailingRequests
#[tokio::test]
async fn test_zone_aware_result_tracker_start_minimum_requests_no_failing() {
    let instances = vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-a"),
        inst_addr_zone("127.0.0.3", "zone-b"),
        inst_addr_zone("127.0.0.4", "zone-b"),
        inst_addr_zone("127.0.0.5", "zone-c"),
        inst_addr_zone("127.0.0.6", "zone-c"),
    ];
    let zone_counts: [Arc<AtomicUsize>; 3] = Default::default();
    let iterations = 300usize;

    for _ in 0..iterations {
        let logger = Arc::new(TestTrackerLogger::default());
        let t = Arc::new(ZoneAwareResultTracker::new(
            instances.clone(),
            1,
            None,
            Some(logger.clone()),
        ));
        t.start_minimum_requests();

        let results = Arc::new(Mutex::new(HashMap::<usize, Result<(), BoxDynError>>::new()));
        for idx in 0..6 {
            let tc = Arc::clone(&t);
            let rc = Arc::clone(&results);
            tokio::spawn(async move {
                let r = tc.await_start(&RingContext::background(), idx).await;
                rc.lock().insert(idx, r);
            });
        }

        assert!(poll_until(Duration::from_secs(1), || results.lock().len() == 4).await);
        assert_eq!(results.lock().values().filter(|r| r.is_ok()).count(), 4);

        let keys: Vec<usize> = results.lock().keys().copied().collect();
        if keys.contains(&0) {
            zone_counts[0].fetch_add(1, Ordering::AcqRel);
            t.done(Some(0), None);
            t.done(Some(1), None);
        }
        if keys.contains(&2) {
            zone_counts[1].fetch_add(1, Ordering::AcqRel);
            t.done(Some(2), None);
            t.done(Some(3), None);
        }
        if keys.contains(&4) {
            zone_counts[2].fetch_add(1, Ordering::AcqRel);
            t.done(Some(4), None);
            t.done(Some(5), None);
        }

        assert!(t.succeeded());
        assert!(poll_until(Duration::from_secs(1), || results.lock().len() == 6).await);
        assert_eq!(results.lock().values().filter(|r| r.is_ok()).count(), 4);
        assert_eq!(logger.all().len(), 2);
    }

    let expected = (iterations as f64) * (2.0 / 3.0);
    for zc in &zone_counts {
        let v = zc.load(Ordering::Acquire) as f64;
        assert!((v - expected).abs() <= expected * 0.25, "got {v}, expected ~{expected}");
    }
}

fn eight_zone_instances() -> Vec<InstanceDesc> {
    vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-a"),
        inst_addr_zone("127.0.0.3", "zone-b"),
        inst_addr_zone("127.0.0.4", "zone-b"),
        inst_addr_zone("127.0.0.5", "zone-c"),
        inst_addr_zone("127.0.0.6", "zone-c"),
        inst_addr_zone("127.0.0.7", "zone-d"),
        inst_addr_zone("127.0.0.8", "zone-d"),
    ]
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesLessThanMaximumAllowed_SingleFailingRequestInZone
#[tokio::test]
async fn test_zone_aware_result_tracker_failing_zones_less_than_max_single_failure() {
    let instances = eight_zone_instances();
    let logger = Arc::new(TestTrackerLogger::default());
    let t = Arc::new(ZoneAwareResultTracker::new(
        instances.clone(),
        2,
        None,
        Some(logger.clone()),
    ));
    t.start_minimum_requests();

    let released = Arc::new(Mutex::new(Vec::<usize>::new()));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..8 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            rel.lock().push(idx);
            if r.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 4).await);
    assert_eq!(signalled.load(Ordering::Acquire), 4);

    let first_idx = released.lock()[0];
    t.done(Some(first_idx), Some(&SimpleError("something went wrong".into())));

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 6).await);
    assert_eq!(signalled.load(Ordering::Acquire), 6);
    assert!(logger.contains(
        "warn",
        "request to instance has failed, zone cannot contribute to quorum",
        &[("zone", &instances[first_idx].zone), ("err", "something went wrong")]
    ));

    let rem: Vec<usize> = released.lock()[1..].to_vec();
    for idx in rem {
        t.done(Some(idx), None);
    }
    assert!(t.succeeded());
    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 8).await);
    assert_eq!(signalled.load(Ordering::Acquire), 6);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesLessThanMaximumAllowed_MultipleFailingRequestsInSingleZone
#[tokio::test]
async fn test_zone_aware_result_tracker_failing_zones_less_than_max_multiple_failures_in_zone() {
    let instances = eight_zone_instances();
    let t = Arc::new(ZoneAwareResultTracker::new(instances.clone(), 2, None, None));
    t.start_minimum_requests();

    let released = Arc::new(Mutex::new(Vec::<usize>::new()));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..8 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            rel.lock().push(idx);
            if r.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 4).await);
    let failing_idx = released.lock()[0];
    t.done(Some(failing_idx), Some(&SimpleError("something went wrong".into())));

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 6).await);
    assert_eq!(signalled.load(Ordering::Acquire), 6);

    let other_in_zone = released
        .lock()
        .iter()
        .copied()
        .find(|&i| i != failing_idx && instances[i].zone == instances[failing_idx].zone)
        .unwrap();
    t.done(Some(other_in_zone), Some(&SimpleError("something else went wrong".into())));

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(released.lock().len(), 6);

    let rem: Vec<usize> = released
        .lock()
        .iter()
        .copied()
        .filter(|&i| i != failing_idx && i != other_in_zone)
        .collect();
    for idx in rem {
        t.done(Some(idx), None);
    }
    assert!(t.succeeded());
    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 8).await);
    assert_eq!(signalled.load(Ordering::Acquire), 6);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesEqualToMaximumAllowed
#[tokio::test]
async fn test_zone_aware_result_tracker_failing_zones_equal_to_max() {
    let instances = eight_zone_instances();
    let t = Arc::new(ZoneAwareResultTracker::new(instances.clone(), 2, None, None));
    t.start_minimum_requests();

    let released = Arc::new(Mutex::new(Vec::<usize>::new()));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..8 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            rel.lock().push(idx);
            if r.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 4).await);
    let first_idx = released.lock()[0];
    t.done(Some(first_idx), Some(&SimpleError("err1".into())));
    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 6).await);

    let second_idx = released
        .lock()
        .iter()
        .copied()
        .find(|&i| instances[i].zone != instances[first_idx].zone)
        .unwrap();
    t.done(Some(second_idx), Some(&SimpleError("err2".into())));
    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 8).await);
    assert_eq!(signalled.load(Ordering::Acquire), 8);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesGreaterThanMaximumAllowed
#[tokio::test]
async fn test_zone_aware_result_tracker_failing_zones_greater_than_max() {
    let instances = eight_zone_instances();
    let t = Arc::new(ZoneAwareResultTracker::new(instances, 2, None, None));
    t.start_minimum_requests();

    let released = Arc::new(Mutex::new(Vec::<usize>::new()));
    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..8 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            let r = tc.await_start(&RingContext::background(), idx).await;
            rel.lock().push(idx);
            if r.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 4).await);
    let initial: Vec<usize> = released.lock().clone();
    for idx in initial {
        t.done(Some(idx), Some(&SimpleError("err".into())));
    }
    assert!(poll_until(Duration::from_secs(1), || released.lock().len() == 8).await);
    assert_eq!(signalled.load(Ordering::Acquire), 8);

    let fifth = released.lock()[4];
    t.done(Some(fifth), Some(&SimpleError("err3".into())));
    assert!(t.failed());
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_MaxUnavailableZonesIsNumberOfZones
#[tokio::test]
async fn test_zone_aware_result_tracker_max_unavailable_zones_is_num_zones() {
    let instances = vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-a"),
    ];
    let t = ZoneAwareResultTracker::new(instances, 1, None, None);
    t.start_minimum_requests();

    for idx in 0..2 {
        let err = t.await_start(&RingContext::background(), idx).await.unwrap_err();
        assert_eq!(err.to_string(), ErrResultNotNeeded.to_string());
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartMinimumRequests_ZoneSorting
#[tokio::test]
async fn test_zone_aware_result_tracker_zone_sorting() {
    let instances = vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-b"),
        inst_addr_zone("127.0.0.3", "zone-c"),
        inst_addr_zone("127.0.0.4", "zone-d"),
    ];
    let released = Arc::new(Mutex::new([false; 4]));
    let t = Arc::new(ZoneAwareResultTracker::new(
        instances,
        2,
        Some(Arc::new(|_| {
            vec!["zone-c".into(), "zone-a".into(), "zone-d".into(), "zone-b".into()]
        })),
        None,
    ));
    t.start_minimum_requests();

    for idx in 0..4 {
        let tc = Arc::clone(&t);
        let rel = Arc::clone(&released);
        tokio::spawn(async move {
            tc.await_start(&RingContext::background(), idx).await.unwrap();
            rel.lock()[idx] = true;
        });
    }

    assert!(poll_until(Duration::from_secs(1), || *released.lock() == [true, false, true, false]).await);
    t.done(Some(0), Some(&SimpleError("zone A failed".into())));
    assert!(poll_until(Duration::from_secs(1), || *released.lock() == [true, false, true, true]).await);
    t.done(Some(3), Some(&SimpleError("zone D failed".into())));
    assert!(poll_until(Duration::from_secs(1), || *released.lock() == [true, true, true, true]).await);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareResultTracker_StartAdditionalRequests
#[tokio::test]
async fn test_zone_aware_result_tracker_start_additional_requests() {
    let instances = eight_zone_instances();
    let logger = Arc::new(TestTrackerLogger::default());
    let t = Arc::new(ZoneAwareResultTracker::new(instances.clone(), 2, None, Some(logger.clone())));
    t.start_minimum_requests();

    let signalled = Arc::new(AtomicUsize::new(0));
    for idx in 0..8 {
        let tc = Arc::clone(&t);
        let sig = Arc::clone(&signalled);
        tokio::spawn(async move {
            if tc.await_start(&RingContext::background(), idx).await.is_ok() {
                sig.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    assert!(poll_until(Duration::from_secs(1), || signalled.load(Ordering::Acquire) == 4).await);
    assert_eq!(logger.all().len(), 2);

    t.start_additional_requests();
    assert!(poll_until(Duration::from_secs(1), || signalled.load(Ordering::Acquire) == 6).await);
    assert_eq!(logger.all().len(), 3);

    t.start_additional_requests();
    assert!(poll_until(Duration::from_secs(1), || signalled.load(Ordering::Acquire) == 8).await);
    assert_eq!(logger.all().len(), 4);
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestZoneAwareContextTracker
#[test]
fn test_zone_aware_context_tracker() {
    let instances = vec![
        inst_addr_zone("127.0.0.1", "zone-a"),
        inst_addr_zone("127.0.0.2", "zone-a"),
        inst_addr_zone("127.0.0.3", "zone-b"),
        inst_addr_zone("127.0.0.4", "zone-b"),
        inst_addr_zone("127.0.0.5", "zone-c"),
        inst_addr_zone("127.0.0.6", "zone-c"),
    ];
    let parent_ctx = RingContext::background()
        .with_value::<TestContextKey, String>("this-is-the-value-from-the-parent".into());
    let t = ZoneAwareContextTracker::new(parent_ctx, instances);

    let (c0, _) = t.context_for(0);
    let (c1, _) = t.context_for(1);
    let (c2, cancel2) = t.context_for(2);
    let (c3, _) = t.context_for(3);
    let (c4, _) = t.context_for(4);
    let (c5, _) = t.context_for(5);

    for c in [&c0, &c1, &c2, &c3, &c4, &c5] {
        assert_eq!(
            c.value::<TestContextKey, String>().as_deref().map(String::as_str),
            Some("this-is-the-value-from-the-parent")
        );
        assert_eq!(c.err(), None);
    }

    t.cancel_context_for(0, Some(Arc::new(CancellationError::new("instance 1 cancellation cause"))));
    assert_eq!(c0.err(), Some(ContextError::Canceled));
    assert_eq!(c1.err(), Some(ContextError::Canceled));
    assert_eq!(c0.cause().unwrap().to_string(), "context canceled: instance 1 cancellation cause");
    assert_eq!(c1.cause().unwrap().to_string(), "context canceled: instance 1 cancellation cause");
    for c in [&c2, &c3, &c4, &c5] {
        assert_eq!(c.err(), None);
    }

    cancel2.cancel(Some(Arc::new(CancellationError::new("instance 3 cancellation cause"))));
    assert_eq!(c2.err(), Some(ContextError::Canceled));
    for c in [&c3, &c4, &c5] {
        assert_eq!(c.err(), None);
    }

    t.cancel_all_contexts(Some(Arc::new(CancellationError::new("remaining instances cancellation cause"))));
    for c in [&c3, &c4, &c5] {
        assert_eq!(c.err(), Some(ContextError::Canceled));
        assert_eq!(c.cause().unwrap().to_string(), "context canceled: remaining instances cancellation cause");
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_set_tracker_test.go::TestInflightInstanceTracker
#[test]
fn test_inflight_instance_tracker() {
    let sets = vec![
        ReplicationSet {
            instances: vec![inst_id_addr("instance-1", ""), inst_id_addr("instance-2", "")],
            ..Default::default()
        },
        ReplicationSet {
            instances: vec![inst_id_addr("instance-3", "")],
            ..Default::default()
        },
    ];

    // 1. addInstance idempotent
    {
        let inf = InflightInstanceTracker::new(&sets);
        inf.add_instance(0, 0);
        inf.add_instance(0, 0);
        inf.all_instances_added();
        assert!(!inf.all_instances_completed());
        inf.remove_instance(0, 0);
        assert!(inf.all_instances_completed());
    }
    // 2. removeInstance idempotent
    {
        let inf = InflightInstanceTracker::new(&sets);
        inf.add_instance(0, 0);
        inf.all_instances_added();
        assert!(!inf.all_instances_completed());
        for _ in 0..2 {
            inf.remove_instance(0, 0);
            assert!(inf.all_instances_completed());
        }
    }
    // 3. allInstancesCompleted after all removed and allInstancesAdded
    {
        let inf = InflightInstanceTracker::new(&sets);
        inf.add_instance(0, 0);
        inf.add_instance(0, 1);
        inf.add_instance(1, 0);
        inf.all_instances_added();
        assert!(!inf.all_instances_completed());
        inf.remove_instance(0, 0);
        assert!(!inf.all_instances_completed());
        inf.remove_instance(0, 1);
        assert!(!inf.all_instances_completed());
        inf.remove_instance(1, 0);
        assert!(inf.all_instances_completed());
    }
    // 4. allInstancesCompleted false until allInstancesAdded
    {
        let inf = InflightInstanceTracker::new(&sets);
        inf.add_instance(0, 0);
        inf.add_instance(0, 1);
        inf.add_instance(1, 0);
        assert!(!inf.all_instances_completed());
        inf.remove_instance(0, 0);
        inf.remove_instance(0, 1);
        inf.remove_instance(1, 0);
        assert!(!inf.all_instances_completed());
        inf.all_instances_added();
        assert!(inf.all_instances_completed());
    }
    let _ = HashSet::<String>::new();
}
