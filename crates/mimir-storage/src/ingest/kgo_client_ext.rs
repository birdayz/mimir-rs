//! Client metadata cache, FindCoordinator sharder, coordinator registry, KIP-951 safe move,
//! and share group consumer state ported from franz-go/pkg/kgo.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use kafka_protocol::messages::FindCoordinatorRequest;

// ---------------------------------------------------------------------------
// Metadata cache with by_id cleanup & eviction (client.go / client_sweep_test.go)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CachedMetaTopic {
    pub topic: String,
    pub topic_id: [u8; 16],
    pub when: Instant,
}

#[derive(Debug, Default)]
pub struct ClientMetaCache {
    pub topics: HashMap<String, CachedMetaTopic>,
    pub by_id: HashMap<[u8; 16], String>,
    pub metadata_min_age: Duration,
}

impl ClientMetaCache {
    pub fn new(metadata_min_age: Duration) -> Self {
        Self {
            topics: HashMap::new(),
            by_id: HashMap::new(),
            metadata_min_age,
        }
    }

    /// Ported from Client.storeCachedMeta: drops stale by_id mappings on topic ID change and
    /// prunes aged entries only when prune=true and req_topics is non-empty.
    pub fn store_cached_meta(
        &mut self,
        req_topics: &[&str],
        resp_topics: &[(&str, [u8; 16])],
        prune: bool,
        now: Instant,
    ) {
        let resp_set: HashSet<&str> = resp_topics.iter().map(|(t, _)| *t).collect();

        if prune && !req_topics.is_empty() {
            let min_age = self.metadata_min_age;
            let mut evicted_ids = Vec::new();
            self.topics.retain(|name, ct| {
                let keep = resp_set.contains(name.as_str())
                    || now.saturating_duration_since(ct.when) < min_age;
                if !keep && ct.topic_id != [0u8; 16] {
                    evicted_ids.push(ct.topic_id);
                }
                keep
            });
            for id in evicted_ids {
                self.by_id.remove(&id);
            }
        }

        for &(name, id) in resp_topics {
            if let Some(prev) = self.topics.get(name) {
                if prev.topic_id != id && prev.topic_id != [0u8; 16] {
                    self.by_id.remove(&prev.topic_id);
                }
            }
            if id != [0u8; 16] {
                self.by_id.insert(id, name.to_string());
            }
            self.topics.insert(
                name.to_string(),
                CachedMetaTopic {
                    topic: name.to_string(),
                    topic_id: id,
                    when: now,
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// FindCoordinator sharder non-mutation (sharder_mutation_test.go)
// ---------------------------------------------------------------------------

/// Ported from findCoordinatorSharder.shard: deduplicates keys into a new Vec without mutating
/// the caller's FindCoordinatorRequest.
pub fn shard_find_coordinator_request(req: &FindCoordinatorRequest) -> Vec<String> {
    let mut seen = HashSet::with_capacity(req.coordinator_keys.len());
    let mut dedup = Vec::with_capacity(req.coordinator_keys.len());
    for k in &req.coordinator_keys {
        let s = k.as_str().to_string();
        if seen.insert(s.clone()) {
            dedup.push(s);
        }
    }
    dedup
}

// ---------------------------------------------------------------------------
// Coordinator load race guard (coordinator_race_test.go)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoordinatorKey {
    pub name: String,
    pub kind: i8,
}

pub struct CoordinatorLoad {
    state: Mutex<Option<i32>>,
    cond: Condvar,
}

impl Default for CoordinatorLoad {
    fn default() -> Self {
        Self::new()
    }
}

impl CoordinatorLoad {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
            cond: Condvar::new(),
        }
    }

    pub fn publish_node(&self, node: i32) {
        let mut g = self.state.lock().unwrap();
        *g = Some(node);
        self.cond.notify_all();
    }

    pub fn wait_node(&self) -> i32 {
        let mut g = self.state.lock().unwrap();
        while g.is_none() {
            g = self.cond.wait(g).unwrap();
        }
        g.unwrap()
    }
}

#[derive(Default)]
pub struct CoordinatorRegistry {
    pub coordinators: Mutex<HashMap<CoordinatorKey, Arc<CoordinatorLoad>>>,
}

impl CoordinatorRegistry {
    /// Ported from Client.deleteStaleCoordinatorsByNode: waits for publication before reading node.
    pub fn delete_stale_coordinators_by_node(&self, node: i32) {
        let entries: Vec<(CoordinatorKey, Arc<CoordinatorLoad>)> = {
            let map = self.coordinators.lock().unwrap();
            map.iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect()
        };
        let mut to_delete = Vec::new();
        for (k, v) in entries {
            if v.wait_node() == node {
                to_delete.push(k);
            }
        }
        let mut map = self.coordinators.lock().unwrap();
        for k in to_delete {
            map.remove(&k);
        }
    }
}

// ---------------------------------------------------------------------------
// KIP-951 safe move after purge + re-add (topics_and_partitions_test.go)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Kip951RecBuf {
    pub topic: String,
    pub partition: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub has_sink: bool,
}

#[derive(Debug, Clone)]
pub struct Kip951MoveTarget {
    pub leader: i32,
    pub leader_epoch: i32,
}

#[derive(Default)]
pub struct Kip951TopicsPartitions {
    pub topics: Mutex<HashMap<String, Vec<Arc<Mutex<Kip951RecBuf>>>>>,
}

impl Kip951TopicsPartitions {
    pub fn apply_moves(&self, moves: &[(Arc<Mutex<Kip951RecBuf>>, Kip951MoveTarget)]) {
        let map = self.topics.lock().unwrap();
        for (staged, target) in moves {
            let (topic, part) = {
                let s = staged.lock().unwrap();
                (s.topic.clone(), s.partition)
            };
            let Some(parts) = map.get(&topic) else {
                continue;
            };
            let Ok(idx) = usize::try_from(part) else {
                continue;
            };
            let Some(current) = parts.get(idx) else {
                continue;
            };
            if !Arc::ptr_eq(current, staged) {
                continue;
            }
            let mut rb = current.lock().unwrap();
            rb.leader = target.leader;
            rb.leader_epoch = target.leader_epoch;
        }
    }
}

// ---------------------------------------------------------------------------
// Share consumer cursor migration & ack state (share_resweep_test.go / share_test.go)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ShareSource {
    pub node_id: i32,
    pub cursors: Mutex<Vec<Arc<ShareCursor>>>,
}

impl ShareSource {
    pub fn new(node_id: i32) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            cursors: Mutex::new(Vec::new()),
        })
    }

    pub fn create_share_req(&self) -> Vec<(String, i32)> {
        let guard = self.cursors.lock().unwrap();
        guard
            .iter()
            .filter(|c| c.assigned.load(Ordering::SeqCst) && !c.moving.load(Ordering::SeqCst))
            .map(|c| (c.topic.clone(), c.partition))
            .collect()
    }
}

#[derive(Debug)]
pub struct ShareCursor {
    pub topic: String,
    pub topic_id: [u8; 16],
    pub partition: i32,
    pub assigned: AtomicBool,
    pub moving: AtomicBool,
    pub source: Mutex<Arc<ShareSource>>,
}

impl ShareCursor {
    pub fn new(topic: &str, topic_id: [u8; 16], partition: i32, source: Arc<ShareSource>) -> Arc<Self> {
        Arc::new(Self {
            topic: topic.to_string(),
            topic_id,
            partition,
            assigned: AtomicBool::new(false),
            moving: AtomicBool::new(false),
            source: Mutex::new(source),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ShareMove {
    pub cursor: Arc<ShareCursor>,
    pub target_source: Arc<ShareSource>,
}

pub struct ShareConsumerCoordinator {
    pub workers: AtomicUsize,
    pub workers_mu: Mutex<()>,
    pub workers_cond: Condvar,
    pub sources_mu: Mutex<()>,
    pub metadata_mu: Mutex<()>,
}

impl Default for ShareConsumerCoordinator {
    fn default() -> Self {
        Self {
            workers: AtomicUsize::new(0),
            workers_mu: Mutex::new(()),
            workers_cond: Condvar::new(),
            sources_mu: Mutex::new(()),
            metadata_mu: Mutex::new(()),
        }
    }
}

impl ShareConsumerCoordinator {
    pub fn inc_worker(&self) {
        let _g = self.workers_mu.lock().unwrap();
        self.workers.fetch_add(1, Ordering::SeqCst);
    }

    pub fn dec_worker(&self) {
        let _g = self.workers_mu.lock().unwrap();
        if self.workers.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.workers_cond.notify_all();
        }
    }

    pub fn apply_moves(self: &Arc<Self>, moves: Vec<ShareMove>) {
        // Mark all cursors moving synchronously BEFORE spawning the migration worker.
        for mv in &moves {
            mv.cursor.moving.store(true, Ordering::SeqCst);
        }
        self.inc_worker();
        let this = Arc::clone(self);
        std::thread::spawn(move || {
            let _meta = this.metadata_mu.lock().unwrap();
            let _src = this.sources_mu.lock().unwrap();
            for mv in moves {
                let old_src = {
                    let mut sg = mv.cursor.source.lock().unwrap();
                    let prev = Arc::clone(&*sg);
                    *sg = Arc::clone(&mv.target_source);
                    prev
                };
                old_src
                    .cursors
                    .lock()
                    .unwrap()
                    .retain(|c| !Arc::ptr_eq(c, &mv.cursor));
                mv.target_source
                    .cursors
                    .lock()
                    .unwrap()
                    .push(Arc::clone(&mv.cursor));
                mv.cursor.moving.store(false, Ordering::SeqCst);
            }
            drop(_src);
            drop(_meta);
            this.dec_worker();
        });
    }

    pub fn migrate_share_cursor_to(&self, cursor: &Arc<ShareCursor>, target_source: Arc<ShareSource>) {
        self.inc_worker();
        let _src = self.sources_mu.lock().unwrap();
        let old_src = {
            let mut sg = cursor.source.lock().unwrap();
            let prev = Arc::clone(&*sg);
            *sg = Arc::clone(&target_source);
            prev
        };
        old_src
            .cursors
            .lock()
            .unwrap()
            .retain(|c| !Arc::ptr_eq(c, cursor));
        target_source
            .cursors
            .lock()
            .unwrap()
            .push(Arc::clone(cursor));
        drop(_src);
        self.dec_worker();
    }

    pub fn wait_leave_barrier(&self, timeout: Duration) -> bool {
        let g = self.workers_mu.lock().unwrap();
        let (_g, res) = self
            .workers_cond
            .wait_timeout_while(g, timeout, |_| self.workers.load(Ordering::SeqCst) > 0)
            .unwrap();
        !res.timed_out()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAckStatus {
    Accept,
    Release,
    Reject,
}

#[derive(Debug, Clone)]
pub struct ShareRecordState {
    pub key: usize,
    pub value: Vec<u8>,
    pub delivery_count: i32,
    pub ack: Option<ShareAckStatus>,
    pub archived: bool,
}

/// In-memory KIP-932 share group broker partition state for faithful share group behavior tests.
#[derive(Debug, Default)]
pub struct ShareGroupBrokerState {
    pub records: Mutex<Vec<ShareRecordState>>,
    pub spso: AtomicI32,
    pub committed_acks: Mutex<BTreeMap<usize, ShareAckStatus>>,
}

impl ShareGroupBrokerState {
    pub fn produce(&self, key: usize, value: &[u8]) {
        self.records.lock().unwrap().push(ShareRecordState {
            key,
            value: value.to_vec(),
            delivery_count: 0,
            ack: None,
            archived: false,
        });
    }

    pub fn acquire_batch(&self, max_records: usize) -> Vec<(usize, usize, Vec<u8>, i32)> {
        let mut recs = self.records.lock().unwrap();
        let mut out = Vec::new();
        for (idx, r) in recs.iter_mut().enumerate() {
            if out.len() >= max_records {
                break;
            }
            if !r.archived && r.ack.is_none() {
                r.delivery_count += 1;
                r.ack = Some(ShareAckStatus::Release); // acquired/in-flight until acked or released
                out.push((idx, r.key, r.value.clone(), r.delivery_count));
            }
        }
        out
    }

    pub fn apply_ack(&self, idx: usize, status: ShareAckStatus) {
        let mut recs = self.records.lock().unwrap();
        if let Some(r) = recs.get_mut(idx) {
            match status {
                ShareAckStatus::Accept => {
                    r.ack = Some(ShareAckStatus::Accept);
                    r.archived = true;
                    self.committed_acks.lock().unwrap().insert(r.key, ShareAckStatus::Accept);
                }
                ShareAckStatus::Reject => {
                    r.ack = Some(ShareAckStatus::Reject);
                    r.archived = true;
                    self.committed_acks.lock().unwrap().insert(r.key, ShareAckStatus::Reject);
                }
                ShareAckStatus::Release => {
                    r.ack = None;
                }
            }
        }
        // Advance SPSO past contiguous archived records.
        let mut spso = 0i32;
        for r in recs.iter() {
            if r.archived {
                spso += 1;
            } else {
                break;
            }
        }
        self.spso.store(spso, Ordering::SeqCst);
    }

    pub fn reset_spso(&self, new_offset: i32) {
        let mut recs = self.records.lock().unwrap();
        for (i, r) in recs.iter_mut().enumerate() {
            if (i as i32) >= new_offset {
                r.archived = false;
                r.ack = None;
            }
        }
        self.spso.store(new_offset, Ordering::SeqCst);
    }
}
