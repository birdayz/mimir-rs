//! Memberlist gossip propagation delay tracker and codec.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/propagation_tracker.go
//! - github.com/grafana/dskit/kv/memberlist/propagation_tracker_model.go

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext};
use mimir_config::server::PropagationDelayTrackerConfig;
use mimir_proto::memberlistpb::{BeaconDesc, PropagationDelayTrackerDesc};
use parking_lot::RwLock;
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use rand::Rng;
use tracing::{info, warn};

use super::client::{MemberlistKv, MemberlistKvClient};
use super::gofmt::{go_sharp_v_str, GoJson};
use super::mergeable::Mergeable;
use crate::kv::{CasOutcome, Codec, KvClient, ProtoSnappyCodec};

pub const PROPAGATION_DELAY_TRACKER_KEY: &str = "memberlist-propagation-delay-tracker";
pub const PROPAGATION_DELAY_TRACKER_CODEC_ID: &str = "propagationDelayTracker";

pub fn propagation_delay_tracker_codec() -> Arc<dyn Codec> {
    Arc::new(
        ProtoSnappyCodec::<PropagationDelayTrackerDesc>::new(PROPAGATION_DELAY_TRACKER_CODEC_ID)
            .with_mergeable(
                |d| Box::new(d),
                |m| m.as_any().downcast_ref::<PropagationDelayTrackerDesc>(),
            ),
    )
}

fn system_time_to_unix_millis(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn unix_millis_before(ts_millis: i64, limit: SystemTime) -> bool {
    if ts_millis >= 0 {
        match UNIX_EPOCH.checked_add(Duration::from_millis(ts_millis as u64)) {
            Some(t) => t < limit,
            None => false,
        }
    } else {
        match UNIX_EPOCH.checked_sub(Duration::from_millis(ts_millis.unsigned_abs())) {
            Some(t) => t < limit,
            None => true,
        }
    }
}

pub(crate) fn merge_propagation_tracker_with_time(
    dst: &mut PropagationDelayTrackerDesc,
    other: &PropagationDelayTrackerDesc,
    local_cas: bool,
    now: SystemTime,
) -> Option<PropagationDelayTrackerDesc> {
    let mut change = PropagationDelayTrackerDesc::default();

    for (&beacon_id, other_beacon) in &other.beacons {
        let changed = match dst.beacons.get(&beacon_id) {
            None => true,
            Some(this_beacon) => {
                other_beacon.published_at > this_beacon.published_at
                    || (other_beacon.published_at == this_beacon.published_at
                        && other_beacon.deleted_at > this_beacon.deleted_at)
            }
        };
        if changed {
            dst.beacons.insert(beacon_id, other_beacon.clone());
            change.beacons.insert(beacon_id, other_beacon.clone());
        }
    }

    if local_cas {
        let now_ms = system_time_to_unix_millis(now);
        for (&beacon_id, this_beacon) in &mut dst.beacons {
            if !other.beacons.contains_key(&beacon_id) && this_beacon.deleted_at == 0 {
                this_beacon.deleted_at = now_ms;
                change.beacons.insert(beacon_id, this_beacon.clone());
            }
        }
    }

    if change.beacons.is_empty() {
        None
    } else {
        Some(change)
    }
}

impl Mergeable for PropagationDelayTrackerDesc {
    fn merge(
        &mut self,
        other: &mut dyn Mergeable,
        local_cas: bool,
    ) -> Result<Option<Box<dyn Mergeable>>, String> {
        let other_desc = other
            .as_any()
            .downcast_ref::<PropagationDelayTrackerDesc>()
            .ok_or_else(|| {
                "expected *PropagationDelayTrackerDesc, got a different Mergeable".to_string()
            })?;
        Ok(
            merge_propagation_tracker_with_time(self, other_desc, local_cas, SystemTime::now())
                .map(|c| Box::new(c) as Box<dyn Mergeable>),
        )
    }

    fn merge_content(&self) -> Vec<String> {
        self.beacons.keys().map(|id| id.to_string()).collect()
    }

    fn remove_tombstones(&mut self, limit: Option<SystemTime>) -> (usize, usize) {
        let mut total = 0usize;
        let mut removed = 0usize;
        self.beacons.retain(|_, beacon| {
            if beacon.deleted_at != 0 {
                let expired = match limit {
                    None => true,
                    Some(lim) => unix_millis_before(beacon.deleted_at, lim),
                };
                if expired {
                    removed += 1;
                    return false;
                }
                total += 1;
            }
            true
        });
        (total, removed)
    }

    fn clone_mergeable(&self) -> Box<dyn Mergeable> {
        Box::new(self.clone())
    }

    fn to_go_json(&self) -> GoJson {
        let entries = self
            .beacons
            .iter()
            .map(|(k, b)| {
                let mut fields = Vec::new();
                if b.published_at != 0 {
                    fields.push(("published_at".into(), GoJson::int(b.published_at)));
                }
                if b.deleted_at != 0 {
                    fields.push(("deleted_at".into(), GoJson::int(b.deleted_at)));
                }
                if !b.published_by.is_empty() {
                    fields.push(("published_by".into(), GoJson::str(&b.published_by)));
                }
                (k.to_string(), GoJson::Object(fields))
            })
            .collect();
        GoJson::Object(vec![("beacons".into(), GoJson::sorted_map(entries))])
    }

    fn go_string(&self) -> String {
        let mut keys: Vec<u64> = self.beacons.keys().copied().collect();
        keys.sort_unstable();
        let mut s = String::from("&memberlist.PropagationDelayTrackerDesc{Beacons:map[uint64]BeaconDesc{");
        for k in keys {
            let b = &self.beacons[&k];
            s.push_str(&format!(
                "{k}: memberlist.BeaconDesc{{PublishedAt:{}, DeletedAt:{}, PublishedBy:{}}},",
                b.published_at,
                b.deleted_at,
                go_sharp_v_str(&b.published_by)
            ));
        }
        s.push_str("},}");
        s
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}

pub(crate) struct PropagationDelayTracker {
    cfg: PropagationDelayTrackerConfig,
    node_name: String,
    kv: Weak<MemberlistKv>,
    codec: Arc<dyn Codec>,
    seen_beacons: RwLock<HashSet<u64>>,
    pending_beacon_id: AtomicU64,
    startup_time_ms: AtomicI64,
    propagation_delay: Histogram,
    beacons_published_total: Counter,
    beacons_received_total: Counter,
    service: OnceLock<Arc<ManagedBasicService>>,
}

impl PropagationDelayTracker {
    pub fn new(
        kv: Weak<MemberlistKv>,
        cfg: PropagationDelayTrackerConfig,
        node_name: String,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        let propagation_delay = Histogram::with_opts(
            HistogramOpts::new(
                "memberlist_propagation_tracker_delay_seconds",
                "Time from beacon publish to receive.",
            )
            .buckets(prometheus::DEFAULT_BUCKETS.to_vec()),
        )
        .expect("valid histogram");
        let beacons_published_total = Counter::with_opts(Opts::new(
            "memberlist_propagation_tracker_beacons_published_total",
            "Total number of beacons published by this node.",
        ))
        .expect("valid counter");
        let beacons_received_total = Counter::with_opts(Opts::new(
            "memberlist_propagation_tracker_beacons_received_total",
            "Total number of unique beacons received.",
        ))
        .expect("valid counter");

        if let Some(r) = reg {
            let _ = r.register(Box::new(propagation_delay.clone()));
            let _ = r.register(Box::new(beacons_published_total.clone()));
            let _ = r.register(Box::new(beacons_received_total.clone()));
        }

        let tracker = Arc::new(Self {
            cfg,
            node_name,
            kv,
            codec: propagation_delay_tracker_codec(),
            seen_beacons: RwLock::new(HashSet::new()),
            pending_beacon_id: AtomicU64::new(0),
            startup_time_ms: AtomicI64::new(0),
            propagation_delay,
            beacons_published_total,
            beacons_received_total,
            service: OnceLock::new(),
        });

        let w = Arc::downgrade(&tracker);
        let svc = basic_service(
            None,
            Some(Box::new(move |ctx| {
                let w = w.clone();
                Box::pin(async move {
                    if let Some(t) = w.upgrade() {
                        t.running(ctx).await;
                    }
                    Ok(())
                })
            })),
            None,
        )
        .with_name("propagation-delay-tracker");
        let _ = tracker.service.set(svc);
        tracker
    }

    pub fn service(&self) -> Arc<ManagedBasicService> {
        Arc::clone(self.service.get().expect("service initialized in new()"))
    }

    async fn running(self: Arc<Self>, ctx: ServiceContext) {
        self.startup_time_ms
            .store(system_time_to_unix_millis(SystemTime::now()), Ordering::Release);
        let beacon_interval = self
            .cfg
            .beacon_interval
            .as_std()
            .unwrap_or(Duration::from_secs(60));
        let beacon_lifetime = self
            .cfg
            .beacon_lifetime
            .as_std()
            .unwrap_or(Duration::from_secs(600));
        info!(
            ?beacon_interval,
            ?beacon_lifetime,
            "propagation delay tracker started"
        );

        let watch_ctx = ctx.child();
        let watch_self = Arc::clone(&self);
        let watch_ctx_c = watch_ctx.clone();
        let watch_handle = tokio::spawn(async move {
            watch_self.watch_beacons(watch_ctx_c).await;
        });

        let initial_nanos = rand::thread_rng().gen_range(1..=(beacon_interval.as_nanos().max(1) as u64));
        let mut next_tick = tokio::time::Instant::now() + Duration::from_nanos(initial_nanos);

        loop {
            tokio::select! {
                biased;
                _ = ctx.canceled() => break,
                _ = tokio::time::sleep_until(next_tick) => {
                    next_tick = tokio::time::Instant::now() + beacon_interval;
                    tokio::select! {
                        biased;
                        _ = ctx.canceled() => break,
                        _ = self.on_beacon_interval() => {}
                    }
                }
            }
        }

        watch_ctx.cancel();
        if tokio::time::timeout(Duration::from_secs(10), watch_handle)
            .await
            .is_err()
        {
            warn!("timed out waiting for watch goroutine to finish");
        }
    }

    async fn watch_beacons(self: Arc<Self>, ctx: ServiceContext) {
        let mut boff = Backoff::new(BackoffConfig {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            max_retries: 0,
        });

        while boff.ongoing() && !ctx.is_canceled() {
            let Some(kv) = self.kv.upgrade() else {
                return;
            };
            let Ok(client) = MemberlistKvClient::new(kv, Arc::clone(&self.codec)) else {
                return;
            };
            let tracker = Arc::clone(&self);
            client
                .watch_key(
                    PROPAGATION_DELAY_TRACKER_KEY,
                    ctx.clone(),
                    Box::new(move |val| {
                        if let Ok(desc) = val.downcast::<PropagationDelayTrackerDesc>() {
                            tracker.on_beacons_received(&desc);
                        }
                        true
                    }),
                )
                .await;

            if ctx.is_canceled() {
                return;
            }
            tokio::select! {
                _ = ctx.canceled() => return,
                _ = boff.wait() => {}
            }
        }
    }

    fn already_seen(&self, beacon_id: u64) -> bool {
        self.seen_beacons.read().contains(&beacon_id)
    }

    fn mark_as_seen(&self, beacon_id: u64) {
        self.seen_beacons.write().insert(beacon_id);
    }

    fn cleanup_seen_beacons(&self, desc: &PropagationDelayTrackerDesc) {
        let pending_id = self.pending_beacon_id.load(Ordering::Acquire);
        self.seen_beacons.write().retain(|id| {
            if *id == pending_id {
                return true;
            }
            desc.beacons.get(id).is_some_and(|b| b.deleted_at == 0)
        });
    }

    pub(crate) fn on_beacons_received(&self, desc: &PropagationDelayTrackerDesc) {
        let now = SystemTime::now();
        let startup_ms = self.startup_time_ms.load(Ordering::Acquire);
        let warn_thresh = self
            .cfg
            .log_beacons_latency_longer_than
            .as_std()
            .unwrap_or(Duration::ZERO);
        let mut received_count = 0u64;

        for (&beacon_id, beacon) in &desc.beacons {
            if beacon.deleted_at != 0 {
                continue;
            }
            if self.already_seen(beacon_id) {
                continue;
            }
            self.mark_as_seen(beacon_id);

            if beacon.published_at < startup_ms {
                continue;
            }

            received_count += 1;
            let published = if beacon.published_at >= 0 {
                UNIX_EPOCH + Duration::from_millis(beacon.published_at as u64)
            } else {
                UNIX_EPOCH
            };
            if let Ok(delay) = now.duration_since(published) {
                self.propagation_delay.observe(delay.as_secs_f64());
                if !warn_thresh.is_zero() && delay >= warn_thresh {
                    warn!(
                        beacon_id,
                        published_by = %beacon.published_by,
                        ?delay,
                        "high latency beacon detected"
                    );
                }
            }
        }

        if received_count > 0 {
            self.beacons_received_total.inc_by(received_count as f64);
        }

        self.cleanup_seen_beacons(desc);
    }

    async fn on_beacon_interval(&self) {
        let Some(kv) = self.kv.upgrade() else {
            return;
        };
        let Ok(client) = MemberlistKvClient::new(kv, Arc::clone(&self.codec)) else {
            return;
        };

        let val = match client.get(PROPAGATION_DELAY_TRACKER_KEY).await {
            Ok(v) => v,
            Err(err) => {
                warn!(%err, "failed to get beacons");
                return;
            }
        };
        let desc = val
            .and_then(|any| any.downcast::<PropagationDelayTrackerDesc>().ok())
            .map(|b| *b)
            .unwrap_or_default();

        let now = SystemTime::now();
        let beacon_lifetime = self
            .cfg
            .beacon_lifetime
            .as_std()
            .unwrap_or(Duration::from_secs(600));
        let beacon_interval = self
            .cfg
            .beacon_interval
            .as_std()
            .unwrap_or(Duration::from_secs(60));

        let mut beacons_to_delete = Vec::new();
        let mut has_recent_beacon = false;

        for (&beacon_id, beacon) in &desc.beacons {
            if beacon.deleted_at != 0 {
                continue;
            }
            let published = if beacon.published_at >= 0 {
                UNIX_EPOCH + Duration::from_millis(beacon.published_at as u64)
            } else {
                UNIX_EPOCH
            };
            let age = now.duration_since(published).unwrap_or(Duration::ZERO);
            if age >= beacon_lifetime {
                beacons_to_delete.push(beacon_id);
            } else if age < beacon_interval {
                has_recent_beacon = true;
            }
        }

        if !beacons_to_delete.is_empty() {
            self.delete_beacons(&client, beacons_to_delete).await;
        }
        if !has_recent_beacon {
            self.publish_beacon(&client).await;
        }
    }

    async fn publish_beacon(&self, client: &MemberlistKvClient) {
        let mut beacon_id: u64 = rand::thread_rng().gen();
        if beacon_id == 0 {
            beacon_id = 1;
        }
        let now_ms = system_time_to_unix_millis(SystemTime::now());
        let node_name = self.node_name.clone();

        self.pending_beacon_id.store(beacon_id, Ordering::Release);
        self.mark_as_seen(beacon_id);

        let res = client
            .cas(
                PROPAGATION_DELAY_TRACKER_KEY,
                Box::new(move |in_val| {
                    let mut desc = in_val
                        .and_then(|a| a.downcast_ref::<PropagationDelayTrackerDesc>())
                        .cloned()
                        .unwrap_or_default();
                    if desc.beacons.contains_key(&beacon_id) {
                        return Ok(CasOutcome::NoChange);
                    }
                    desc.beacons.insert(
                        beacon_id,
                        BeaconDesc {
                            published_at: now_ms,
                            deleted_at: 0,
                            published_by: node_name.clone(),
                        },
                    );
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await;

        self.pending_beacon_id.store(0, Ordering::Release);
        if let Err(err) = res {
            warn!(%err, "failed to publish beacon");
            return;
        }
        self.beacons_published_total.inc();
    }

    async fn delete_beacons(&self, client: &MemberlistKvClient, beacon_ids: Vec<u64>) {
        let res = client
            .cas(
                PROPAGATION_DELAY_TRACKER_KEY,
                Box::new(move |in_val| {
                    let mut desc = in_val
                        .and_then(|a| a.downcast_ref::<PropagationDelayTrackerDesc>())
                        .cloned()
                        .unwrap_or_default();
                    let now_ms = system_time_to_unix_millis(SystemTime::now());
                    let mut changed = false;
                    for &id in &beacon_ids {
                        if let Some(b) = desc.beacons.get_mut(&id) {
                            if b.deleted_at == 0 {
                                b.deleted_at = now_ms;
                                changed = true;
                            }
                        }
                    }
                    if !changed {
                        return Ok(CasOutcome::NoChange);
                    }
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await;

        if let Err(err) = res {
            warn!(%err, "failed to mark beacons as deleted");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_config::server::MemberlistConfig;
    use mimir_config::GoDuration;

    #[test]
    fn c08_propagation_tracker_desc_merge_tombstones_and_codec() {
        let codec = propagation_delay_tracker_codec();
        assert_eq!(codec.codec_id(), "propagationDelayTracker");

        let mut d1 = PropagationDelayTrackerDesc::default();
        d1.beacons.insert(
            1,
            BeaconDesc {
                published_at: 1000,
                deleted_at: 0,
                published_by: "node-a".into(),
            },
        );
        let mut d2 = PropagationDelayTrackerDesc::default();
        d2.beacons.insert(
            1,
            BeaconDesc {
                published_at: 1000,
                deleted_at: 2000,
                published_by: "node-a".into(),
            },
        );
        d2.beacons.insert(
            2,
            BeaconDesc {
                published_at: 3000,
                deleted_at: 0,
                published_by: "node-b".into(),
            },
        );

        let bytes = codec.encode(&d2).unwrap();
        let decoded = codec.decode(&bytes).unwrap();
        let dec = decoded
            .downcast_ref::<PropagationDelayTrackerDesc>()
            .unwrap();
        assert_eq!(dec, &d2);

        let ch = d1.merge(&mut d2.clone(), false).unwrap().unwrap();
        let ch_desc = ch
            .as_any()
            .downcast_ref::<PropagationDelayTrackerDesc>()
            .unwrap();
        assert_eq!(ch_desc.beacons.len(), 2);
        assert_eq!(d1.beacons[&1].deleted_at, 2000);
        assert_eq!(d1.beacons[&2].published_by, "node-b");

        let limit = UNIX_EPOCH + Duration::from_millis(2500);
        assert_eq!(d1.remove_tombstones(Some(limit)), (0, 1));
        assert!(!d1.beacons.contains_key(&1));
        assert!(d1.beacons.contains_key(&2));
    }

    #[tokio::test]
    async fn c08_memberlist_kv_runs_propagation_delay_tracker_and_emits_metrics() {
        let mut cfg = MemberlistConfig {
            node_name: "pt-node".into(),
            bind_addr: vec!["127.0.0.1".into()],
            bind_port: 0,
            ..Default::default()
        };
        cfg.propagation_delay_tracker.enabled = true;
        cfg.propagation_delay_tracker.beacon_interval = GoDuration::from_millis(25);
        cfg.propagation_delay_tracker.beacon_lifetime = GoDuration::from_secs(60);

        let reg = Registry::new();
        let kv = MemberlistKv::start(cfg, Vec::new(), "", Some(&reg))
            .await
            .expect("MemberlistKv with propagation_delay_tracker.enabled must start");

        assert!(kv.get_codec(PROPAGATION_DELAY_TRACKER_CODEC_ID).is_some());

        let mut published = false;
        for _ in 0..40 {
            for mf in reg.gather() {
                if mf.get_name() == "memberlist_propagation_tracker_beacons_published_total" {
                    if let Some(m) = mf.get_metric().first() {
                        if m.get_counter().get_value() >= 1.0 {
                            published = true;
                        }
                    }
                }
            }
            if published {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(published, "expected tracker to publish at least one beacon");

        let mut incoming = PropagationDelayTrackerDesc::default();
        incoming.beacons.insert(
            42,
            BeaconDesc {
                published_at: system_time_to_unix_millis(SystemTime::now()),
                deleted_at: 0,
                published_by: "remote-node".into(),
            },
        );
        let codec = propagation_delay_tracker_codec();
        let encoded = codec.encode(&incoming).unwrap();
        let (_, new_ver, _, _) = kv
            .merge_bytes_value_for_key(
                PROPAGATION_DELAY_TRACKER_KEY,
                &encoded,
                codec.as_ref(),
                false,
                None,
            )
            .unwrap();
        assert!(new_ver > 0);
        kv.notify_watchers(PROPAGATION_DELAY_TRACKER_KEY);

        let mut received = false;
        let mut delay_count = 0u64;
        for _ in 0..40 {
            for mf in reg.gather() {
                if mf.get_name() == "memberlist_propagation_tracker_beacons_received_total" {
                    if let Some(m) = mf.get_metric().first() {
                        if m.get_counter().get_value() >= 1.0 {
                            received = true;
                        }
                    }
                }
                if mf.get_name() == "memberlist_propagation_tracker_delay_seconds" {
                    if let Some(m) = mf.get_metric().first() {
                        delay_count = m.get_histogram().get_sample_count();
                    }
                }
            }
            if received && delay_count >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(received, "expected beacons_received_total >= 1");
        assert!(delay_count >= 1, "expected delay_seconds sample_count >= 1");

        kv.shutdown().await;
    }
}
