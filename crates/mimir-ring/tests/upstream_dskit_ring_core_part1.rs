//! Ported upstream dskit/ring core tests (part 1: batch, merge, model, queue, strategy, http, ticker, tokens).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/batch_test.go
//! - github.com/grafana/dskit/ring/merge_test.go
//! - github.com/grafana/dskit/ring/model_test.go
//! - github.com/grafana/dskit/ring/ownership_priority_queue_test.go
//! - github.com/grafana/dskit/ring/replication_strategy_test.go
//! - github.com/grafana/dskit/ring/ring_http_test.go
//! - github.com/grafana/dskit/ring/ticker_test.go
//! - github.com/grafana/dskit/ring/token_generator_test.go
//! - github.com/grafana/dskit/ring/tokens_test.go

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::to_bytes;
use axum::http::{HeaderMap, Request, StatusCode};
use mimir_common::services::{start_and_await_running, stop_and_await_terminated, ServiceContext};
use mimir_config::server::MemberlistConfig;
use mimir_proto::grpcutilpb::status_with_http_response;
use mimir_proto::httpgrpcpb::HttpResponse;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::{get_typed, CasOutcome, KvClient};
use mimir_ring::ring::batch::{is_http_status_4xx, BatchTracker, ItemTracker};
use mimir_ring::ring::http::{RingAccess, RingPageHandler, StatusPageConfig};
use mimir_ring::ring::model::{
    merge_tokens, merge_tokens_by_zone, ring_desc_codec, system_time_to_unix_secs, CompareResult,
    DescExt, InstanceDescExt, MAX_FUTURE_TIMESTAMP_SKEW,
};
use mimir_ring::ring::replication_set_tracker::BoxDynError;
use mimir_ring::ring::replication_strategy::{
    DefaultReplicationStrategy, IgnoreUnhealthyInstancesReplicationStrategy, ReplicationStrategy,
    READ, REPORTING, WRITE,
};
use mimir_ring::ring::ticker::new_disableable_ticker;
use mimir_ring::token_gen::{
    load_tokens_from_file, marshal_tokens, new_ring_instance_ownership_info,
    new_ring_token_ownership_info, store_tokens_to_file, tokens_equals, unmarshal_tokens,
    OwnershipPriorityQueue, RandomTokenGenerator, RingToken, TokenGenerator,
};
use prometheus::Registry;
use rand::Rng;
use regex::Regex;

fn httpgrpc_error(code: i32, msg: &str) -> BoxDynError {
    let resp = HttpResponse {
        code,
        headers: Vec::new(),
        body: msg.as_bytes().to_vec().into(),
    };
    Arc::new(status_with_http_response(&resp, msg))
}

// ============================================================================
// batch_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/batch_test.go::TestBatchTracker_Record
#[tokio::test]
async fn test_batch_tracker_record() {
    let min_success = 2;
    let max_failure = 1;
    let replication_factor = 3;

    let execute_scenario = |scenarios: Vec<(Vec<usize>, Option<BoxDynError>)>| async move {
        let it1 = Arc::new(ItemTracker::new(min_success, max_failure, replication_factor));
        let it2 = Arc::new(ItemTracker::new(min_success, max_failure, replication_factor));
        let trackers = [it1, it2];
        let (bt, done_rx, err_rx) = BatchTracker::new(trackers.len() as i32);

        let bt_c = Arc::clone(&bt);
        let trackers_c = trackers.clone();
        tokio::spawn(async move {
            for (idxs, err) in scenarios {
                let slice: Vec<Arc<ItemTracker>> =
                    idxs.into_iter().map(|i| Arc::clone(&trackers_c[i])).collect();
                bt_c.record(&slice, err, &is_http_status_4xx);
            }
        });

        tokio::select! {
            res = err_rx => Err(res.unwrap()),
            _ = done_rx => Ok(()),
        }
    };

    let err_4xx = httpgrpc_error(400, "this is an error");
    let bad_scenario = vec![
        (vec![0], None),
        (vec![1], Some(Arc::clone(&err_4xx))),
        (vec![0], None),
        (vec![0], None),
        (vec![1], Some(Arc::clone(&err_4xx))),
        (vec![1], Some(Arc::clone(&err_4xx))),
    ];
    assert!(execute_scenario(bad_scenario).await.is_err());

    let good_scenario = vec![
        (vec![0], None),
        (vec![1], Some(Arc::clone(&err_4xx))),
        (vec![1], Some(Arc::clone(&err_4xx))),
        (vec![0], None),
        (vec![0], None),
        (vec![1], Some(Arc::clone(&err_4xx))),
    ];
    assert!(execute_scenario(good_scenario).await.is_err());
}

// ============================================================================
// merge_test.go (8)
// ============================================================================

fn inst(addr: &str, ts: i64, state: InstanceState, tokens: Vec<u32>) -> InstanceDesc {
    InstanceDesc {
        addr: addr.to_string(),
        timestamp: ts,
        state: state as i32,
        tokens,
        ..Default::default()
    }
}

fn desc_from(items: Vec<(&str, InstanceDesc)>) -> Desc {
    Desc {
        ingesters: items
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn merge_descs(ring1: &mut Desc, ring2: &mut Desc) -> Option<Desc> {
    ring1.merge_with_time(ring2, false, SystemTime::now())
}

fn merge_local_cas(ring1: &mut Desc, ring2: &mut Desc, now_unix: i64) -> Option<Desc> {
    ring1.merge_with_time(ring2, true, UNIX_EPOCH + Duration::from_secs(now_unix as u64))
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestNormalizationAndConflictResolution
#[test]
fn test_normalization_and_conflict_resolution() {
    let now = system_time_to_unix_secs(SystemTime::now());
    let mut first = desc_from(vec![
        ("Ing 1", inst("addr1", now, InstanceState::Active, vec![50, 40, 40, 30])),
        (
            "Ing 2",
            inst("addr2", 123456, InstanceState::Leaving, vec![100, 5, 5, 100, 100, 200, 20, 10]),
        ),
        ("Ing 3", inst("addr3", now, InstanceState::Left, vec![100, 200, 300])),
        ("Ing 4", inst("addr4", now, InstanceState::Leaving, vec![30, 40, 50])),
        ("Unknown", inst("", 0, InstanceState::Active, vec![100])),
    ]);
    let mut second = desc_from(vec![(
        "Unknown",
        inst("", now + 10, InstanceState::Active, vec![1000, 2000]),
    )]);

    let change = first.merge_with_time(&mut second, false, SystemTime::now());

    assert_eq!(
        first,
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", 123456, InstanceState::Leaving, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now, InstanceState::Left, vec![])),
            ("Ing 4", inst("addr4", now, InstanceState::Leaving, vec![])),
            ("Unknown", inst("", now + 10, InstanceState::Active, vec![1000, 2000])),
        ])
    );
    assert_eq!(
        change,
        Some(desc_from(vec![(
            "Unknown",
            inst("", now + 10, InstanceState::Active, vec![1000, 2000]),
        )]))
    );
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestMerge
#[test]
fn test_merge() {
    let now = system_time_to_unix_secs(SystemTime::now());
    let first_ring = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Joining, vec![5, 10, 20, 100, 200])),
        ])
    };
    let second_ring = || {
        desc_from(vec![
            ("Ing 3", inst("addr3", now + 5, InstanceState::Active, vec![150, 250, 350])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
        ])
    };
    let third_ring = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Leaving, vec![30, 40, 50])),
            ("Ing 3", inst("addr3", now + 10, InstanceState::Active, vec![150, 250, 350])),
        ])
    };
    let expected_1_2 = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now + 5, InstanceState::Active, vec![150, 250, 350])),
        ])
    };
    let expected_1_2_3 = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Leaving, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now + 10, InstanceState::Active, vec![150, 250, 350])),
        ])
    };
    let fourth_ring = || {
        desc_from(vec![(
            "Ing 1",
            inst("addr1", now + 10, InstanceState::Left, vec![30, 40, 50]),
        )])
    };
    let expected_1_2_3_4 = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Left, vec![])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now + 10, InstanceState::Active, vec![150, 250, 350])),
        ])
    };

    {
        let mut our = first_ring();
        let ch = merge_descs(&mut our, &mut second_ring());
        assert_eq!(our, expected_1_2());
        assert_eq!(ch, Some(second_ring()));
    }
    {
        let mut our = expected_1_2();
        let ch = merge_descs(&mut our, &mut second_ring());
        assert_eq!(our, expected_1_2());
        assert_eq!(ch, None);
    }
    {
        let mut our = second_ring();
        let ch = merge_descs(&mut our, &mut first_ring());
        assert_eq!(our, expected_1_2());
        assert_eq!(
            ch,
            Some(desc_from(vec![(
                "Ing 1",
                inst("addr1", now, InstanceState::Active, vec![30, 40, 50]),
            )]))
        );
    }
    {
        let mut our1 = first_ring();
        merge_descs(&mut our1, &mut second_ring());
        merge_descs(&mut our1, &mut third_ring());
        assert_eq!(our1, expected_1_2_3());

        let mut our2 = second_ring();
        merge_descs(&mut our2, &mut third_ring());
        merge_descs(&mut our2, &mut first_ring());
        assert_eq!(our2, expected_1_2_3());
    }
    {
        let mut out = expected_1_2_3();
        let ch = merge_descs(&mut out, &mut fourth_ring());
        assert_eq!(out, expected_1_2_3_4());
        assert_eq!(
            ch,
            Some(desc_from(vec![(
                "Ing 1",
                inst("addr1", now + 10, InstanceState::Left, vec![]),
            )]))
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestTokensTakeover
#[test]
fn test_tokens_takeover() {
    let now = system_time_to_unix_secs(SystemTime::now());
    let first = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Joining, vec![5, 10, 20])),
        ])
    };
    let second = || {
        desc_from(vec![
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20])),
            ("Ing 3", inst("addr3", now + 5, InstanceState::Leaving, vec![5, 10, 20, 100, 200])),
        ])
    };
    let merged = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20])),
            ("Ing 3", inst("addr3", now + 5, InstanceState::Leaving, vec![100, 200])),
        ])
    };

    let mut our = first();
    let ch = merge_descs(&mut our, &mut second());
    assert_eq!(our, merged());
    assert_eq!(
        ch,
        Some(desc_from(vec![
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20])),
            ("Ing 3", inst("addr3", now + 5, InstanceState::Leaving, vec![100, 200])),
        ]))
    );

    let mut our2 = merged();
    let ch2 = merge_descs(&mut our2, &mut second());
    assert_eq!(our2, merged());
    assert_eq!(ch2, None);

    let mut our3 = second();
    let ch3 = merge_descs(&mut our3, &mut first());
    assert_eq!(our3, merged());
    assert_eq!(
        ch3,
        Some(desc_from(vec![(
            "Ing 1",
            inst("addr1", now, InstanceState::Active, vec![30, 40, 50]),
        )]))
    );
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestMergeLeft
#[test]
fn test_merge_left() {
    let now = system_time_to_unix_secs(SystemTime::now());
    let first_ring = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Joining, vec![5, 10, 20, 100, 200])),
        ])
    };
    let first_not_norm = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Joining, vec![20, 10, 5, 10, 20, 100, 200, 100])),
        ])
    };
    let second_ring = || desc_from(vec![("Ing 2", inst("addr2", now, InstanceState::Left, vec![]))]);
    let second_not_norm = || {
        desc_from(vec![(
            "Ing 2",
            inst("addr2", now, InstanceState::Left, vec![5, 10, 20, 100, 200]),
        )])
    };
    let expected_1_2 = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Left, vec![])),
        ])
    };
    let third_ring = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Leaving, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Joining, vec![5, 10, 20, 100, 200])),
        ])
    };
    let expected_1_2_3 = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Leaving, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Left, vec![])),
        ])
    };

    for mut rhs in [second_ring(), second_not_norm()] {
        let mut our = first_ring();
        let ch = merge_descs(&mut our, &mut rhs);
        assert_eq!(our, expected_1_2());
        assert_eq!(ch, Some(second_ring()));
    }
    {
        let mut our = expected_1_2();
        let ch = merge_descs(&mut our, &mut second_ring());
        assert_eq!(our, expected_1_2());
        assert_eq!(ch, None);
    }
    for mut rhs in [first_ring(), first_not_norm()] {
        let mut our = second_ring();
        let ch = merge_descs(&mut our, &mut rhs);
        assert_eq!(our, expected_1_2());
        assert_eq!(
            ch,
            Some(desc_from(vec![(
                "Ing 1",
                inst("addr1", now, InstanceState::Active, vec![30, 40, 50]),
            )]))
        );
    }
    {
        let mut our1 = first_ring();
        merge_descs(&mut our1, &mut second_ring());
        merge_descs(&mut our1, &mut third_ring());
        assert_eq!(our1, expected_1_2_3());

        let mut our2 = second_ring();
        merge_descs(&mut our2, &mut third_ring());
        merge_descs(&mut our2, &mut first_ring());
        assert_eq!(our2, expected_1_2_3());
    }
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestMergeRemoveMissing
#[test]
fn test_merge_remove_missing() {
    let now = system_time_to_unix_secs(SystemTime::now());
    let first = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now, InstanceState::Joining, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now, InstanceState::Leaving, vec![5, 10, 20, 100, 200])),
        ])
    };
    let second = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
        ])
    };
    let expected = || {
        desc_from(vec![
            ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now + 3, InstanceState::Left, vec![])),
        ])
    };

    {
        let mut our = first();
        let ch = merge_local_cas(&mut our, &mut second(), now + 3);
        assert_eq!(our, expected());
        assert_eq!(
            ch,
            Some(desc_from(vec![
                ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
                ("Ing 3", inst("addr3", now + 3, InstanceState::Left, vec![])),
            ]))
        );
    }
    {
        let mut our = expected();
        let ch = merge_local_cas(&mut our, &mut second(), now + 10);
        assert_eq!(our, expected());
        assert_eq!(ch, None);
    }
    {
        let mut our = second();
        let ch = merge_local_cas(&mut our, &mut first(), now + 3);
        assert_eq!(
            our,
            desc_from(vec![
                ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
                ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
                ("Ing 3", inst("addr3", now, InstanceState::Leaving, vec![])),
            ])
        );
        assert_eq!(
            ch,
            Some(desc_from(vec![(
                "Ing 3",
                inst("addr3", now, InstanceState::Leaving, vec![]),
            )]))
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestMergeMissingIntoLeft
#[test]
fn test_merge_missing_into_left() {
    let now = system_time_to_unix_secs(SystemTime::now());
    let mut ring1 = desc_from(vec![
        ("Ing 1", inst("addr1", now, InstanceState::Active, vec![30, 40, 50])),
        ("Ing 2", inst("addr2", now + 5, InstanceState::Active, vec![5, 10, 20, 100, 200])),
        ("Ing 3", inst("addr3", now, InstanceState::Left, vec![])),
    ]);
    let mut ring2 = desc_from(vec![
        ("Ing 1", inst("addr1", now + 10, InstanceState::Active, vec![30, 40, 50])),
        ("Ing 2", inst("addr2", now + 10, InstanceState::Active, vec![5, 10, 20, 100, 200])),
    ]);
    let ch = merge_local_cas(&mut ring1, &mut ring2, now + 10);
    assert_eq!(
        ring1,
        desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 10, InstanceState::Active, vec![5, 10, 20, 100, 200])),
            ("Ing 3", inst("addr3", now, InstanceState::Left, vec![])),
        ])
    );
    assert_eq!(
        ch,
        Some(desc_from(vec![
            ("Ing 1", inst("addr1", now + 10, InstanceState::Active, vec![30, 40, 50])),
            ("Ing 2", inst("addr2", now + 10, InstanceState::Active, vec![5, 10, 20, 100, 200])),
        ]))
    );
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestMergeFutureTimestamps
#[test]
fn test_merge_future_timestamps() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
    let valid = system_time_to_unix_secs(now);
    let within_skew = system_time_to_unix_secs(now + MAX_FUTURE_TIMESTAMP_SKEW);
    let skewed = system_time_to_unix_secs(now + Duration::from_secs(90 * 24 * 3600));

    type MergeCase<'a> = (&'a str, Desc, Desc, bool, Desc, Option<Desc>);
    let cases: Vec<MergeCase<'_>> = vec![
        (
            "incoming entry too far in the future is ignored",
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Leaving, vec![30, 40, 50]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            None,
        ),
        (
            "incoming new entry too far in the future is not added",
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 2", inst("addr2", skewed, InstanceState::Active, vec![5, 10, 20]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            None,
        ),
        (
            "incoming LEFT entry too far in the future is ignored",
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Left, vec![]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            None,
        ),
        (
            "incoming entry in the future within the allowed skew is accepted",
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", within_skew, InstanceState::Leaving, vec![30, 40, 50]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", within_skew, InstanceState::Leaving, vec![30, 40, 50]))]),
            Some(desc_from(vec![("Ing 1", inst("addr1", within_skew, InstanceState::Leaving, vec![30, 40, 50]))])),
        ),
        (
            "local entry too far in the future is replaced by an older incoming entry",
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            Some(desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))])),
        ),
        (
            "local entry too far in the future is replaced by an older incoming LEFT entry",
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Left, vec![]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Left, vec![]))]),
            Some(desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Left, vec![]))])),
        ),
        (
            "local entry too far in the future is not replaced by itself",
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50]))]),
            false,
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50]))]),
            None,
        ),
        (
            "local CAS removing an entry too far in the future marks it as LEFT now",
            desc_from(vec![
                ("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50])),
                ("Ing 2", inst("addr2", valid, InstanceState::Active, vec![5, 10, 20])),
            ]),
            desc_from(vec![("Ing 2", inst("addr2", valid, InstanceState::Active, vec![5, 10, 20]))]),
            true,
            desc_from(vec![
                ("Ing 1", inst("addr1", valid, InstanceState::Left, vec![])),
                ("Ing 2", inst("addr2", valid, InstanceState::Active, vec![5, 10, 20])),
            ]),
            Some(desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Left, vec![]))])),
        ),
        (
            "local CAS ignores an incoming entry too far in the future",
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            desc_from(vec![("Ing 1", inst("addr1", skewed, InstanceState::Active, vec![30, 40, 50]))]),
            true,
            desc_from(vec![("Ing 1", inst("addr1", valid, InstanceState::Active, vec![30, 40, 50]))]),
            None,
        ),
    ];

    for (name, mut local, mut incoming, local_cas, expected_local, expected_change) in cases {
        let ch = local.merge_with_time(&mut incoming, local_cas, now);
        assert_eq!(local, expected_local, "{name}");
        assert_eq!(ch, expected_change, "{name}");
    }
}

// Upstream: github.com/grafana/dskit/ring/merge_test.go::TestMergeFutureTimestamps_MemberlistCAS
#[tokio::test]
async fn test_merge_future_timestamps_memberlist_cas() {
    const KEY: &str = "ring";
    let codec = ring_desc_codec();
    let cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: "merge-future-ts".to_string(),
        ..Default::default()
    };

    let store = MemberlistKv::new(
        cfg,
        vec![codec.clone()],
        "",
        Some(&Registry::new()),
        KvOptions::default(),
    )
    .unwrap();
    let svc = store.service();
    start_and_await_running(ServiceContext::background(), &*svc)
        .await
        .unwrap();
    let client = MemberlistKvClient::new(store.clone(), codec).unwrap();

    let skewed = system_time_to_unix_secs(SystemTime::now() + Duration::from_secs(90 * 24 * 3600));
    client
        .cas(
            KEY,
            Box::new(move |_| {
                Ok(CasOutcome::UpdateOnce(Box::new(desc_from(vec![(
                    "ing-1",
                    inst("addr1", skewed, InstanceState::Active, vec![1, 2, 3]),
                )]))))
            }),
        )
        .await
        .unwrap();

    let heartbeat = system_time_to_unix_secs(SystemTime::now());
    client
        .cas(
            KEY,
            Box::new(move |cur| {
                let mut desc = cur.unwrap().downcast_ref::<Desc>().unwrap().clone();
                desc.ingesters.get_mut("ing-1").unwrap().timestamp = heartbeat;
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let val: Desc = get_typed(&client, KEY).await.unwrap().unwrap();
    assert_eq!(val.ingesters["ing-1"].timestamp, heartbeat);

    stop_and_await_terminated(ServiceContext::background(), &*svc)
        .await
        .unwrap();
}

// ============================================================================
// model_test.go (12)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestInstanceDesc_IsHealthy_ForIngesterOperations
#[test]
fn test_instance_desc_is_healthy_for_ingester_operations() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let cases = [
        (
            "ACTIVE within timeout",
            InstanceDesc {
                state: InstanceState::Active as i32,
                timestamp: now_s - 30,
                ..Default::default()
            },
            Duration::from_secs(60),
            true,
            true,
            true,
        ),
        (
            "ACTIVE past timeout",
            InstanceDesc {
                state: InstanceState::Active as i32,
                timestamp: now_s - 90,
                ..Default::default()
            },
            Duration::from_secs(60),
            false,
            false,
            false,
        ),
        (
            "JOINING within timeout",
            InstanceDesc {
                state: InstanceState::Joining as i32,
                timestamp: now_s - 30,
                ..Default::default()
            },
            Duration::from_secs(60),
            false,
            false,
            true,
        ),
        (
            "LEAVING within timeout",
            InstanceDesc {
                state: InstanceState::Leaving as i32,
                timestamp: now_s - 30,
                ..Default::default()
            },
            Duration::from_secs(60),
            false,
            true,
            true,
        ),
    ];

    for (name, ing, timeout, w, r, rep) in cases {
        assert_eq!(ing.is_healthy(WRITE, timeout, now), w, "{name} WRITE");
        assert_eq!(ing.is_healthy(READ, timeout, now), r, "{name} READ");
        assert_eq!(ing.is_healthy(REPORTING, timeout, now), rep, "{name} REPORTING");
    }
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestInstanceDesc_GetRegisteredAt
#[test]
fn test_instance_desc_get_registered_at() {
    let d_zero = InstanceDesc {
        registered_timestamp: 0,
        ..Default::default()
    };
    assert_eq!(d_zero.get_registered_at(), None);

    let ts = UNIX_EPOCH + Duration::from_secs(10_000_000);
    let d_set = InstanceDesc {
        registered_timestamp: 10_000_000,
        ..Default::default()
    };
    assert_eq!(d_set.get_registered_at(), Some(ts));
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestInstanceDesc_GetLastHeartbeatAt
#[test]
fn test_instance_desc_get_last_heartbeat_at() {
    let d_zero = InstanceDesc {
        timestamp: 0,
        ..Default::default()
    };
    assert_eq!(d_zero.get_last_heartbeat_at(), None);

    let ts = UNIX_EPOCH + Duration::from_secs(10_000_000);
    let d_set = InstanceDesc {
        timestamp: 10_000_000,
        ..Default::default()
    };
    assert_eq!(d_set.get_last_heartbeat_at(), Some(ts));
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestClaimTokensFromNormalizedToNormalized
#[test]
fn test_claim_tokens_from_normalized_to_normalized() {
    let mut r = desc_from(vec![
        ("first", InstanceDesc { tokens: vec![100, 200, 300], ..Default::default() }),
        ("second", InstanceDesc::default()),
    ]);
    let result = r.claim_tokens("first", "second");
    assert_eq!(result, vec![100, 200, 300]);
    assert_eq!(
        r,
        desc_from(vec![
            ("first", InstanceDesc::default()),
            ("second", InstanceDesc { tokens: vec![100, 200, 300], ..Default::default() }),
        ])
    );
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestDesc_Ready
#[test]
fn test_desc_ready() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let r = desc_from(vec![(
        "ing1",
        InstanceDesc {
            tokens: vec![100, 200, 300],
            state: InstanceState::Active as i32,
            timestamp: now_s,
            ..Default::default()
        },
    )]);
    assert!(r.is_ready(now, Duration::from_secs(10)).is_ok());
    assert!(r
        .is_ready(now + Duration::from_secs(300), Duration::from_secs(10))
        .is_err());

    let mut r2 = desc_from(vec![(
        "ing1",
        InstanceDesc {
            state: InstanceState::Active as i32,
            timestamp: now_s,
            ..Default::default()
        },
    )]);
    assert!(r2.is_ready(now, Duration::from_secs(10)).is_err());

    r2.ingesters.insert(
        "some ingester".to_string(),
        InstanceDesc {
            tokens: vec![12345],
            timestamp: now_s,
            ..Default::default()
        },
    );
    assert!(r2.is_ready(now, Duration::from_secs(10)).is_ok());
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestDesc_getTokensByZone
#[test]
fn test_desc_get_tokens_by_zone() {
    let empty = Desc::default();
    assert!(empty.get_tokens_by_zone().is_empty());

    let single = desc_from(vec![
        ("instance-1", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![1, 5], ..Default::default() }),
        ("instance-2", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![2, 4], ..Default::default() }),
        ("instance-3", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![3, 6], ..Default::default() }),
    ]);
    assert_eq!(
        single.get_tokens_by_zone(),
        HashMap::from([(String::new(), vec![1, 2, 3, 4, 5, 6])])
    );

    let multi = desc_from(vec![
        ("instance-1", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![1, 5], zone: "zone-1".into(), ..Default::default() }),
        ("instance-2", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![2, 4], zone: "zone-1".into(), ..Default::default() }),
        ("instance-3", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![3, 6], zone: "zone-2".into(), ..Default::default() }),
    ]);
    assert_eq!(
        multi.get_tokens_by_zone(),
        HashMap::from([
            ("zone-1".to_string(), vec![1, 2, 4, 5]),
            ("zone-2".to_string(), vec![3, 6]),
        ])
    );
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestDesc_TokensFor
#[test]
fn test_desc_tokens_for() {
    let empty = Desc::default();
    assert_eq!(empty.tokens_for("instance-1"), (vec![], vec![]));

    let single = desc_from(vec![
        ("instance-1", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![1, 5], ..Default::default() }),
        ("instance-2", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![2, 4], ..Default::default() }),
        ("instance-3", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![3, 6], ..Default::default() }),
    ]);
    assert_eq!(single.tokens_for("instance-1"), (vec![1, 5], vec![1, 2, 3, 4, 5, 6]));

    let multi = desc_from(vec![
        ("instance-1", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![1, 5], zone: "zone-1".into(), ..Default::default() }),
        ("instance-2", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![2, 4], zone: "zone-1".into(), ..Default::default() }),
        ("instance-3", InstanceDesc { addr: "127.0.0.1".into(), tokens: vec![3, 6], zone: "zone-2".into(), ..Default::default() }),
    ]);
    assert_eq!(multi.tokens_for("instance-1"), (vec![1, 5], vec![1, 2, 3, 4, 5, 6]));
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestDesc_RingsCompare
#[test]
fn test_desc_rings_compare() {
    let cases = [
        ("two empty", Desc::default(), Desc::default(), CompareResult::Equal),
        (
            "same single",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() })]),
            CompareResult::Equal,
        ),
        (
            "different timestamp",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), timestamp: 123456, ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), timestamp: 789012, ..Default::default() })]),
            CompareResult::EqualButStatesAndTimestamps,
        ),
        (
            "different state",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), state: InstanceState::Active as i32, ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), state: InstanceState::Joining as i32, ..Default::default() })]),
            CompareResult::EqualButStatesAndTimestamps,
        ),
        (
            "different registered_timestamp",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), registered_timestamp: 1, ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), registered_timestamp: 2, ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "different read_only",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), read_only: true, ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "different read_only_updated_timestamp",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), read_only_updated_timestamp: 0, ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), read_only_updated_timestamp: 100, ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "different zone",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), zone: "one".into(), ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), zone: "two".into(), ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "different addr",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr2".into(), ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "more instances",
            desc_from(vec![
                ("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() }),
                ("ing2", InstanceDesc { addr: "ing2".into(), ..Default::default() }),
            ]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "different tokens",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), tokens: vec![1, 2, 3], ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), ..Default::default() })]),
            CompareResult::Different,
        ),
        (
            "different tokens 2",
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), tokens: vec![1, 2, 3], ..Default::default() })]),
            desc_from(vec![("ing1", InstanceDesc { addr: "addr1".into(), tokens: vec![1, 2, 4], ..Default::default() })]),
            CompareResult::Different,
        ),
    ];

    for (name, r1, r2, expected) in cases {
        assert_eq!(r1.ring_compare(&r2), expected, "{name} r1->r2");
        assert_eq!(r2.ring_compare(&r1), expected, "{name} r2->r1");
    }
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestDesc_RingCompare_SharedTokenStorage
#[test]
fn test_desc_ring_compare_shared_token_storage() {
    let tokens = vec![1u32, 2, 3];
    let mut d1 = desc_from(vec![(
        "ing1",
        InstanceDesc {
            addr: "addr1".into(),
            tokens: tokens.clone(),
            ..Default::default()
        },
    )]);

    let mut d2 = d1.clone();
    assert_eq!(d1.ring_compare(&d2), CompareResult::Equal);
    d2.ingesters.get_mut("ing1").unwrap().timestamp += 1;
    assert_eq!(d1.ring_compare(&d2), CompareResult::EqualButStatesAndTimestamps);

    let d_slice = desc_from(vec![(
        "ing1",
        InstanceDesc {
            addr: "addr1".into(),
            tokens: tokens[..2].to_vec(),
            ..Default::default()
        },
    )]);
    assert_eq!(d1.ring_compare(&d_slice), CompareResult::Different);

    let d_cloned = desc_from(vec![(
        "ing1",
        InstanceDesc {
            addr: "addr1".into(),
            tokens: tokens.clone(),
            ..Default::default()
        },
    )]);
    assert_eq!(d1.ring_compare(&d_cloned), CompareResult::Equal);

    d1.ingesters.insert(
        "ing2".into(),
        InstanceDesc {
            addr: "addr2".into(),
            tokens: vec![4, 5, 6],
            ..Default::default()
        },
    );
    let mut d3 = d1.clone();
    d3.ingesters.get_mut("ing2").unwrap().tokens = vec![4, 5, 7];
    assert_eq!(d1.ring_compare(&d3), CompareResult::Different);
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestMergeTokens
#[test]
fn test_merge_tokens() {
    assert_eq!(merge_tokens(&[]), Vec::<u32>::new());
    assert_eq!(merge_tokens(&[vec![1, 3, 4, 8]]), vec![1, 3, 4, 8]);
    assert_eq!(
        merge_tokens(&[vec![1, 3, 4, 8], vec![0, 2, 6, 9], vec![5, 7, 10, 11]]),
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    );
    assert_eq!(
        merge_tokens(&[vec![1, 3, 4, 8], vec![], vec![0, 2, 6, 9], vec![], vec![5, 7, 10, 11]]),
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    );
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestMergeTokensByZone
#[test]
fn test_merge_tokens_by_zone() {
    assert!(merge_tokens_by_zone(&HashMap::new()).is_empty());

    let single = HashMap::from([(
        "zone-1".to_string(),
        vec![vec![1, 3, 4, 8], vec![2, 5, 6, 7]],
    )]);
    assert_eq!(
        merge_tokens_by_zone(&single),
        HashMap::from([("zone-1".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8])])
    );

    let multi = HashMap::from([
        ("zone-1".to_string(), vec![vec![1, 3, 4, 8], vec![2, 5, 6, 7]]),
        ("zone-2".to_string(), vec![vec![3, 5], vec![2, 4]]),
    ]);
    assert_eq!(
        merge_tokens_by_zone(&multi),
        HashMap::from([
            ("zone-1".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8]),
            ("zone-2".to_string(), vec![2, 3, 4, 5]),
        ])
    );
}

// Upstream: github.com/grafana/dskit/ring/model_test.go::TestDesc_Clone
#[test]
fn test_desc_clone() {
    let empty = Desc::default();
    assert_eq!(empty.clone(), empty);

    let orig = Desc {
        ingesters: HashMap::from([
            (
                "ing1".to_string(),
                InstanceDesc {
                    addr: "addr1".into(),
                    timestamp: 123456,
                    state: InstanceState::Leaving as i32,
                    tokens: vec![1, 2, 3],
                    zone: "zone1".into(),
                    registered_timestamp: 234567,
                    id: "ing1".into(),
                    read_only_updated_timestamp: 345678,
                    read_only: true,
                    versions: HashMap::from([(1, 2)]),
                },
            ),
            (
                "ing2".to_string(),
                InstanceDesc {
                    addr: "addr2".into(),
                    tokens: vec![4, 5, 6],
                    ..Default::default()
                },
            ),
        ]),
    };

    let mut clone = orig.clone();
    assert_eq!(orig, clone);

    clone.ingesters.insert(
        "ing3".into(),
        InstanceDesc {
            addr: "addr3".into(),
            ..Default::default()
        },
    );
    clone.ingesters.remove("ing2");
    clone.ingesters.get_mut("ing1").unwrap().state = InstanceState::Active as i32;

    assert_eq!(orig.ingesters.len(), 2);
    assert!(!orig.ingesters.contains_key("ing3"));
    assert!(orig.ingesters.contains_key("ing2"));
    assert_eq!(orig.ingesters["ing1"].state, InstanceState::Leaving as i32);
}

// ============================================================================
// ownership_priority_queue_test.go (3)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/ownership_priority_queue_test.go::TestOwnershipPriorityQueue_EqualOwnershipOfRingTokens
#[test]
fn test_ownership_priority_queue_equal_ownership_of_ring_tokens() {
    let mut pq: OwnershipPriorityQueue<RingToken> = OwnershipPriorityQueue::with_capacity(3);
    let first = new_ring_token_ownership_info(60, 40);
    let second = new_ring_token_ownership_info(40, 20);
    let third = new_ring_token_ownership_info(20, 10);

    pq.push(first);
    pq.push(second);
    pq.push(third);

    assert_eq!(pq.pop().unwrap(), first);
    assert_eq!(pq.pop().unwrap(), second);
    assert_eq!(pq.pop().unwrap(), third);
}

// Upstream: github.com/grafana/dskit/ring/ownership_priority_queue_test.go::TestOwnershipPriorityQueue_EqualOwnershipOfRingInstances
#[test]
fn test_ownership_priority_queue_equal_ownership_of_ring_instances() {
    let mut pq: OwnershipPriorityQueue<usize> = OwnershipPriorityQueue::with_capacity(3);
    let first = new_ring_instance_ownership_info(10, 40.0);
    let second = new_ring_instance_ownership_info(8, 40.0);
    let third = new_ring_instance_ownership_info(9, 15.0);

    pq.push(first);
    pq.push(second);
    pq.push(third);

    assert_eq!(pq.pop().unwrap(), first);
    assert_eq!(pq.pop().unwrap(), second);
    assert_eq!(pq.pop().unwrap(), third);
}

// Upstream: github.com/grafana/dskit/ring/ownership_priority_queue_test.go::TestOwnershipPriorityQueue_PushPopPeek
#[test]
fn test_ownership_priority_queue_push_pop_peek() {
    let size = 10;
    let mut pq: OwnershipPriorityQueue<usize> = OwnershipPriorityQueue::with_capacity(size);
    let mut rng = rand::thread_rng();
    let mut min_weight = f64::from(u32::MAX);
    let mut max_weight = 0.0_f64;

    for _ in 0..size {
        let w = (1000.0 - 100.0) * rng.gen::<f64>() + 100.0;
        max_weight = max_weight.max(w);
        min_weight = min_weight.min(w);
        pq.push(new_ring_instance_ownership_info(rng.gen::<usize>(), w));
    }

    assert_eq!(pq.peek().unwrap().ownership, max_weight);

    let new_max = max_weight + 1.0;
    pq.push(new_ring_instance_ownership_info(11, new_max));
    assert_eq!(pq.peek().unwrap().ownership, new_max);

    let new_min = min_weight - 1.0;
    pq.push(new_ring_instance_ownership_info(12, new_min));

    let item = pq.pop().unwrap();
    assert_eq!(item.ownership, new_max);
    assert_eq!(pq.peek().unwrap().ownership, max_weight);

    let mut curr_weight = f64::MAX;
    while pq.len() > 1 {
        let popped = pq.pop().unwrap();
        assert!(popped.ownership <= curr_weight);
        curr_weight = popped.ownership;
    }

    let last = pq.pop().unwrap();
    assert_eq!(last.ownership, new_min);
}

// ============================================================================
// replication_strategy_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/replication_strategy_test.go::TestRingReplicationStrategy
#[test]
fn test_ring_replication_strategy() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let cases = [
        (1usize, 1usize, 0usize, Some(0usize), None),
        (1, 0, 1, None, Some("at least 1 live replicas required, could only find 0 - unhealthy instances: dead1")),
        (3, 2, 0, Some(0), None),
        (3, 3, 0, Some(1), None),
        (3, 2, 1, Some(0), None),
        (3, 1, 2, None, Some("at least 2 live replicas required, could only find 1 - unhealthy instances: dead1,dead2")),
        (3, 4, 0, Some(1), None),
        (3, 3, 1, Some(0), None),
        (3, 2, 2, None, Some("at least 3 live replicas required, could only find 2 - unhealthy instances: dead1,dead2")),
    ];

    let strategy = DefaultReplicationStrategy;
    for (rf, live, dead, exp_max_fail, exp_err) in cases {
        let mut ingesters = Vec::new();
        for _ in 0..live {
            ingesters.push(InstanceDesc {
                timestamp: now_s,
                ..Default::default()
            });
        }
        for i in 0..dead {
            ingesters.push(InstanceDesc {
                addr: format!("dead{}", i + 1),
                ..Default::default()
            });
        }
        let res = strategy.filter(ingesters, READ, rf, Duration::from_secs(100), false, now);
        match exp_err {
            None => {
                let (healthy, max_fail) = res.unwrap();
                assert_eq!(healthy.len(), live);
                assert_eq!(max_fail, exp_max_fail.unwrap());
            }
            Some(msg) => {
                assert_eq!(res.unwrap_err().to_string(), msg);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/replication_strategy_test.go::TestIgnoreUnhealthyInstancesReplicationStrategy
#[test]
fn test_ignore_unhealthy_instances_replication_strategy() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let cases = [
        (1usize, 0usize, 0usize, None),
        (2, 1, 1, None),
        (2, 2, 1, None),
        (0, 3, 0, Some("at least 1 healthy replica required, could only find 0 - unhealthy instances: dead1,dead2,dead3")),
    ];

    let strategy = IgnoreUnhealthyInstancesReplicationStrategy;
    for (live, dead, exp_max_fail, exp_err) in cases {
        let mut ingesters = Vec::new();
        for _ in 0..live {
            ingesters.push(InstanceDesc {
                timestamp: now_s,
                ..Default::default()
            });
        }
        for i in 0..dead {
            ingesters.push(InstanceDesc {
                addr: format!("dead{}", i + 1),
                ..Default::default()
            });
        }
        let res = strategy.filter(ingesters, READ, 3, Duration::from_secs(100), false, now);
        match exp_err {
            None => {
                let (healthy, max_fail) = res.unwrap();
                assert_eq!(healthy.len(), live);
                assert_eq!(max_fail, exp_max_fail);
            }
            Some(msg) => {
                assert_eq!(res.unwrap_err().to_string(), msg);
            }
        }
    }
}

// ============================================================================
// ring_http_test.go (1)
// ============================================================================

struct FakeRingAccess {
    desc: Desc,
    hb_timeout: Duration,
}

impl RingAccess for FakeRingAccess {
    fn get_ring_desc(&self) -> Desc {
        self.desc.clone()
    }

    fn heartbeat_timeout(&self) -> Duration {
        self.hb_timeout
    }

    fn kv_client(&self) -> Option<&Arc<dyn KvClient>> {
        None
    }

    fn ring_key(&self) -> &str {
        "ring"
    }
}

async fn run_handler_get(handler: &RingPageHandler, raw_query: Option<&str>) -> (StatusCode, String, String) {
    let headers = HeaderMap::new();
    let resp = handler.handle_get(&headers, raw_query).await;
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, ct, String::from_utf8(bytes.to_vec()).unwrap())
}

// Upstream: github.com/grafana/dskit/ring/ring_http_test.go::TestRingPageHandler_handle
#[tokio::test]
async fn test_ring_page_handler_handle() {
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let access = Arc::new(FakeRingAccess {
        hb_timeout: Duration::from_secs(10),
        desc: Desc {
            ingesters: HashMap::from([
                (
                    "1".to_string(),
                    InstanceDesc {
                        id: "1".into(),
                        zone: "zone-a".into(),
                        state: InstanceState::Active as i32,
                        addr: "addr-a".into(),
                        timestamp: now_s,
                        tokens: vec![1_000_000, 3_000_000, 6_000_000],
                        versions: HashMap::from([(1, 2), (3, 4)]),
                        ..Default::default()
                    },
                ),
                (
                    "2".to_string(),
                    InstanceDesc {
                        id: "2".into(),
                        zone: "zone-b".into(),
                        state: InstanceState::Active as i32,
                        addr: "addr-b".into(),
                        timestamp: now_s,
                        tokens: vec![2_000_000, 4_000_000, 5_000_000, 7_000_000],
                        versions: HashMap::from([(1, 3), (3, 5)]),
                        ..Default::default()
                    },
                ),
            ]),
        },
    });
    let component_names = HashMap::from([(1u64, "Component X".to_string())]);
    let handler = RingPageHandler::new(
        access.clone(),
        Duration::from_secs(10),
        StatusPageConfig {
            hide_tokens_ui_elements: false,
            show_versions: true,
            component_names: component_names.clone(),
            instance_link: None,
        },
    );

    // 1. displays instance info
    {
        let (code, ct, body) = run_handler_get(&handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(ct, "text/html");
        assert!(Regex::new(r"(?m)<td>\s*1\s*</td>\s*<td>\s*zone-a\s*</td>\s*<td>\s*ACTIVE\s*</td>\s*<td>\s*addr-a\s*</td>").unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<td>\s*3\s*</td>\s*<td>\s*100%\s*</td>\s*<td>\s*1 \(Component X\): v2\s*<br/>\s*3: v4\s*<br/>\s*</td>").unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<td>\s*2\s*</td>\s*<td>\s*zone-b\s*</td>\s*<td>\s*ACTIVE\s*</td>\s*<td>\s*addr-b\s*</td>").unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<td>\s*4\s*</td>\s*<td>\s*100%\s*</td>\s*<td>\s*1 \(Component X\): v3\s*<br/>\s*3: v5\s*<br/>\s*</td>").unwrap().is_match(&body));
    }

    // 2. always renders the read-only state and keeps the read-only updated timestamp
    {
        let ro_change = now_s - 3600;
        let ro_access = Arc::new(FakeRingAccess {
            hb_timeout: Duration::from_secs(10),
            desc: Desc {
                ingesters: HashMap::from([
                    (
                        "read-only".to_string(),
                        InstanceDesc {
                            zone: "zone-a".into(),
                            state: InstanceState::Active as i32,
                            addr: "addr-ro".into(),
                            timestamp: now_s,
                            read_only: true,
                            read_only_updated_timestamp: ro_change,
                            ..Default::default()
                        },
                    ),
                    (
                        "read-write-was-read-only".to_string(),
                        InstanceDesc {
                            zone: "zone-b".into(),
                            state: InstanceState::Active as i32,
                            addr: "addr-rw".into(),
                            timestamp: now_s,
                            read_only: false,
                            read_only_updated_timestamp: ro_change,
                            ..Default::default()
                        },
                    ),
                    (
                        "never-read-only".to_string(),
                        InstanceDesc {
                            zone: "zone-c".into(),
                            state: InstanceState::Active as i32,
                            addr: "addr-never".into(),
                            timestamp: now_s,
                            read_only: false,
                            ..Default::default()
                        },
                    ),
                ]),
            },
        });
        let ro_handler = RingPageHandler::new(ro_access, Duration::from_secs(10), StatusPageConfig::default());
        let (code, _, body) = run_handler_get(&ro_handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert!(Regex::new(r"(?m)<td>\s*addr-ro\s*</td>\s*<td>\s*</td>\s*<td>\s*true\s*</td>\s*<td>\s*.+ago.+\s*</td>").unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<td>\s*addr-rw\s*</td>\s*<td>\s*</td>\s*<td>\s*false\s*</td>\s*<td>\s*\d{4}-\d{2}-\d{2}T.+Z\s*</td>").unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<td>\s*addr-never\s*</td>\s*<td>\s*</td>\s*<td>\s*false\s*</td>\s*<td>\s*</td>").unwrap().is_match(&body));
    }

    // 3. displays Show Tokens button by default
    {
        let (code, ct, body) = run_handler_get(&handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(ct, "text/html");
        assert!(body.contains(r#"<input type="button" value="Show Tokens" onclick="window.location.href = '?tokens=true'"/>"#));
    }

    // 4. displays tokens when Show Tokens is enabled
    {
        let (code, ct, body) = run_handler_get(&handler, Some("tokens=true")).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(ct, "text/html");
        assert!(body.contains(r#"<input type="button" value="Hide Tokens" onclick="window.location.href = '?tokens=false' "/>"#));
        assert!(Regex::new(r"(?m)<h2>\s*Instance: 1\s*</h2>\s*<p>\s*Tokens:<br/>\s*1000000\s*3000000\s*6000000\s*</p>").unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<h2>\s*Instance: 2\s*</h2>\s*<p>\s*Tokens:<br/>\s*2000000\s*4000000\s*5000000\s*7000000\s*</p>").unwrap().is_match(&body));
    }

    // 5-6. hides token columns and Show Tokens button when disabled
    let token_disabled_handler = RingPageHandler::new(
        access.clone(),
        Duration::from_secs(10),
        StatusPageConfig {
            hide_tokens_ui_elements: true,
            show_versions: true,
            component_names: component_names.clone(),
            instance_link: None,
        },
    );
    {
        let (code, ct, body) = run_handler_get(&token_disabled_handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(ct, "text/html");
        assert!(!Regex::new(r"(?m)<th>\s*Tokens\s*</th>\s*<th>\s*Ownership\s*</th>").unwrap().is_match(&body));
        assert!(!Regex::new(r"(?m)<td>\s*3\s*</td>\s*<td>\s*100%\s*</td>").unwrap().is_match(&body));
        assert!(!Regex::new(r"(?m)<td>\s*4\s*</td>\s*<td>\s*100%\s*</td>").unwrap().is_match(&body));
        assert!(!body.contains(r#"input type="button" value="Show Tokens""#));
    }

    // 7. hides versions column when disabled
    let versions_disabled_handler = RingPageHandler::new(
        access.clone(),
        Duration::from_secs(10),
        StatusPageConfig {
            hide_tokens_ui_elements: false,
            show_versions: false,
            component_names,
            instance_link: None,
        },
    );
    {
        let (code, ct, body) = run_handler_get(&versions_disabled_handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(ct, "text/html");
        assert!(!Regex::new(r"(?m)<th>\s*Versions\s*</th>").unwrap().is_match(&body));
        assert!(!Regex::new(r"(?m)<td>\s*1 \(Component X\): v2\s*<br/>\s*3: v4\s*<br/>\s*</td>").unwrap().is_match(&body));
        assert!(!Regex::new(r"(?m)<td>\s*1 \(Component X\): v3\s*<br/>\s*3: v5\s*<br/>\s*</td>").unwrap().is_match(&body));
    }

    // 8. displays instances without links by default
    {
        let (code, _, body) = run_handler_get(&handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert!(!body.contains("<a href="));
    }

    // 9. links instances when InstanceLink is set
    let link_handler = RingPageHandler::new(
        access,
        Duration::from_secs(10),
        StatusPageConfig {
            instance_link: Some(Arc::new(|id, inst| {
                if id == "2" {
                    String::new()
                } else {
                    format!("http://{}/instance/{}", inst.addr, id)
                }
            })),
            ..Default::default()
        },
    );
    {
        let (code, _, body) = run_handler_get(&link_handler, None).await;
        assert_eq!(code, StatusCode::OK);
        assert!(Regex::new(r#"(?m)<td>\s*<a href="http://addr-a/instance/1">1</a>\s*</td>\s*<td>\s*zone-a\s*</td>"#).unwrap().is_match(&body));
        assert!(Regex::new(r"(?m)<td>\s*2\s*</td>\s*<td>\s*zone-b\s*</td>").unwrap().is_match(&body));
    }
    let _ = (BTreeMap::<u64, u64>::new(), Ordering::SeqCst, Request::builder());
}

// ============================================================================
// ticker_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/ticker_test.go::TestNewDisableableTicker_Enabled
#[tokio::test]
async fn test_new_disableable_ticker_enabled() {
    let (stop, ch) = new_disableable_ticker(Duration::from_millis(10));
    let mut rx = ch.expect("enabled ticker must return channel");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(rx.try_recv().is_ok(), "ticker should have ticked when enabled");
    stop.stop();
}

// Upstream: github.com/grafana/dskit/ring/ticker_test.go::TestNewDisableableTicker_Disabled
#[tokio::test]
async fn test_new_disableable_ticker_disabled() {
    let (stop, ch) = new_disableable_ticker(Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(ch.is_none(), "disabled ticker must not return channel");
    stop.stop();
}

// ============================================================================
// token_generator_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/token_generator_test.go::TestRandomTokenGenerator_GenerateTokens
#[test]
fn test_random_token_generator_generate_tokens() {
    let gen = RandomTokenGenerator::new();
    let tokens = gen.generate_tokens(1_000_000, &[]);
    let mut dups = HashMap::with_capacity(tokens.len());
    for (ix, &v) in tokens.iter().enumerate() {
        if let Some(ox) = dups.insert(v, ix) {
            panic!("Found duplicate token {v}, tokens[{ix}]={}, tokens[{ox}]={}", tokens[ix], tokens[ox]);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/token_generator_test.go::TestRandomTokenGenerator_IgnoresOldTokens
#[test]
fn test_random_token_generator_ignores_old_tokens() {
    let gen = RandomTokenGenerator::new();
    let first = gen.generate_tokens(1_000_000, &[]);
    let second = gen.generate_tokens(1_000_000, &first);
    let dups: HashSet<u32> = first.iter().copied().collect();
    for v in &second {
        assert!(!dups.contains(v), "GenerateTokens returned old token {v}");
    }
}

// ============================================================================
// tokens_test.go (3)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/tokens_test.go::TestTokens_Serialization
#[test]
fn test_tokens_serialization() {
    let mut rng = rand::thread_rng();
    let mut tokens = vec![0u32; 512];
    for _ in 0..512 {
        tokens.push(rng.gen_range(0..=i32::MAX as u32));
    }
    let bytes = marshal_tokens(&tokens).unwrap();
    let unmarshaled = unmarshal_tokens(&bytes).unwrap();
    assert_eq!(tokens, unmarshaled);
}

// Upstream: github.com/grafana/dskit/ring/tokens_test.go::TestTokens_Equals
#[test]
fn test_tokens_equals() {
    let cases = [
        (vec![], vec![], true),
        (vec![1, 2, 3], vec![1, 2, 3], true),
        (vec![1, 2, 3], vec![3, 2, 1], true),
        (vec![1, 2], vec![1, 2, 3], false),
    ];
    for (first, second, expected) in cases {
        assert_eq!(tokens_equals(&first, &second), expected);
        assert_eq!(tokens_equals(&second, &first), expected);
    }
}

// Upstream: github.com/grafana/dskit/ring/tokens_test.go::TestLoadTokensFromFile_ShouldGuaranteeSortedTokens
#[test]
fn test_load_tokens_from_file_should_guarantee_sorted_tokens() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("tokens");
    let orig = vec![1u32, 5, 3];
    store_tokens_to_file(&orig, &path).unwrap();
    let actual = load_tokens_from_file(&path).unwrap();
    assert_eq!(actual, vec![1, 3, 5]);
}
