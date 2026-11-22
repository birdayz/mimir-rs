//! SWIM membership state machine: probing (direct + indirect), suspicion, dead/left handling,
//! refutation, reaping, gossip and push/pull scheduling, leave.
//!
//! Ported from github.com/hashicorp/memberlist (grafana fork 8c522db348af): state.go
//! (`schedule`, `probe`, `probeNode`, `resetNodes`, `gossip`, `pushPull`, `aliveNode`,
//! `suspectNode`, `deadNode`, `mergeState`, `refute`, `setProbeChannels`, `setAckHandler`,
//! `invokeAckHandler`, `invokeNackHandler`), memberlist.go (`setAlive`, `Leave`, `NumMembers`,
//! `checkBroadcastQueueDepth`), broadcast.go (`memberlistBroadcast`, `getBroadcasts`),
//! net.go (`handlePing`, `handleIndirectPing`, `sendMsg`, `rawSendMsgPacket`) and util.go
//! (`kRandomNodes`, `moveDeadNodes`, `suspicionTimeout`, `pushPullScale`).
//!
//! It lives in a child module of `client` so it can use `MemberlistKv`'s private state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use rand::seq::SliceRandom;
use rand::Rng;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};

use super::super::queue::Broadcast;
use super::super::suspicion::Suspicion;
use super::super::wire::{
    encode_ack_resp, encode_alive, encode_dead, encode_indirect_ping, encode_nack_resp,
    encode_ping, encode_suspect, make_compound_message, make_compound_messages, AckResp, Alive,
    Dead, IndirectPingReq, MessageType, NackResp, NodeStateType, Ping, PushNodeState, Suspect,
    COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD, USER_MSG_OVERHEAD,
};
use super::{format_ip_port, ip_to_bytes, MemberlistKv, NodeState};

/// Go `pushPullScaleThreshold`.
const PUSH_PULL_SCALE_THRESHOLD: usize = 32;

/// Memberlist protocol timing (Go `memberlist.Config` as built by dskit `buildMemberlistConfig`).
/// Go `moveDeadNodes`.
pub fn move_dead_nodes(nodes: &mut [NodeState], gossip_to_the_dead_time: Duration) -> usize {
    let mut num_dead = 0usize;
    let n = nodes.len();
    let mut i = 0usize;
    while i < n - num_dead {
        if !nodes[i].dead_or_left() {
            i += 1;
            continue;
        }
        let elapsed = SystemTime::now()
            .duration_since(nodes[i].state_change)
            .unwrap_or(Duration::ZERO);
        if elapsed <= gossip_to_the_dead_time {
            i += 1;
            continue;
        }
        nodes.swap(i, n - num_dead - 1);
        num_dead += 1;
    }
    n - num_dead
}

pub fn shuffle_nodes<T>(nodes: &mut [T]) {
    nodes.shuffle(&mut rand::thread_rng());
}

pub fn random_offset(n: usize) -> usize {
    if n == 0 {
        0
    } else {
        rand::thread_rng().gen_range(0..n)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Timing {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub indirect_checks: usize,
    pub suspicion_mult: usize,
    pub suspicion_max_timeout_mult: usize,
    pub awareness_max_multiplier: i32,
    pub gossip_interval: Duration,
    pub gossip_nodes: usize,
    pub gossip_to_the_dead_time: Duration,
    pub dead_node_reclaim_time: Duration,
    pub push_pull_interval: Duration,
    pub push_pull_nodes: usize,
    pub retransmit_mult: usize,
    pub tcp_timeout: Duration,
    pub handoff_queue_depth: usize,
    pub queue_check_interval: Duration,
}

/// Node table: Go `m.nodes` (probe order) + `m.nodeMap` + `m.nodeTimers`.
#[derive(Default)]
pub(crate) struct NodeTable {
    pub order: Vec<String>,
    pub map: HashMap<String, NodeState>,
    pub timers: HashMap<String, Arc<Suspicion>>,
}

impl NodeTable {
    pub fn ordered(&self) -> Vec<&NodeState> {
        self.order.iter().filter_map(|n| self.map.get(n)).collect()
    }
}

/// Go `ackMessage`.
pub(crate) struct AckMessage {
    pub complete: bool,
    pub payload: Vec<u8>,
    pub timestamp: Instant,
}

pub type AckCallback = Box<dyn FnOnce(&[u8], Instant) + Send>;

/// Go `ackHandler`.
pub(crate) struct AckHandler {
    pub ack: AckCallback,
    pub nack: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Go `memberlistBroadcast`: named by node, replaces older broadcasts about the same node.
pub struct MemberlistBroadcast {
    pub node: String,
    pub msg: Vec<u8>,
    pub notify: Option<mpsc::Sender<()>>,
}

impl Broadcast for MemberlistBroadcast {
    fn invalidates(&self, other: &dyn Broadcast) -> bool {
        other
            .as_any()
            .downcast_ref::<MemberlistBroadcast>()
            .is_some_and(|mb| self.node == mb.node)
    }
    fn message(&self) -> &[u8] {
        &self.msg
    }
    fn finished(&self) {
        if let Some(tx) = &self.notify {
            let _ = tx.try_send(());
        }
    }
    fn name(&self) -> Option<&str> {
        Some(&self.node)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go `suspicionTimeout`.
pub fn suspicion_timeout(suspicion_mult: usize, n: usize, interval: Duration) -> Duration {
    let node_scale = (n.max(1) as f64).log10().max(1.0);
    // Go: time.Duration(suspicionMult) * time.Duration(nodeScale*1000) * interval / 1000
    let ns = suspicion_mult as u128 * ((node_scale * 1000.0) as u128) * interval.as_nanos() / 1000;
    Duration::from_nanos(ns as u64)
}

/// Go `pushPullScale`.
pub fn push_pull_scale(interval: Duration, n: usize) -> Duration {
    if n <= PUSH_PULL_SCALE_THRESHOLD {
        return interval;
    }
    let mult = ((n as f64).log2() - (PUSH_PULL_SCALE_THRESHOLD as f64).log2()).ceil() + 1.0;
    interval * mult as u32
}

fn random_stagger(d: Duration) -> Duration {
    if d.is_zero() {
        return d;
    }
    Duration::from_nanos(rand::thread_rng().gen_range(0..d.as_nanos().max(1) as u64))
}

/// Go `time.NewTicker` + `triggerFunc`: starts an interval ticker immediately into a
/// 1-slot buffered channel (dropping ticks only when a tick is already queued), waits a
/// random stagger in `[0, interval)`, then invokes `f` on each delivered tick.
///
/// Using a 1-slot channel prevents `probe_node` (which sleeps `probe_interval` on failure)
/// from skipping an entire extra `probe_interval` on every failed probe.
fn spawn_trigger<F, Fut>(
    interval: Duration,
    cancel: mimir_common::services::ServiceContext,
    mut f: F,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let cancel_tick = cancel.clone();
    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(start, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel_tick.canceled() => return,
                _ = ticker.tick() => {
                    let _ = tx.try_send(());
                }
            }
        }
    });
    tokio::spawn(async move {
        tokio::select! {
            _ = cancel.canceled() => return,
            _ = tokio::time::sleep(random_stagger(interval)) => {}
        }
        loop {
            tokio::select! {
                _ = cancel.canceled() => return,
                tick = rx.recv() => {
                    if tick.is_none() {
                        return;
                    }
                    f().await;
                }
            }
        }
    });
}

fn state_metrics_string(s: NodeStateType) -> &'static str {
    match s {
        NodeStateType::Alive => "alive",
        NodeStateType::Suspect => "suspect",
        NodeStateType::Dead => "dead",
        NodeStateType::Left => "left",
    }
}

impl MemberlistKv {
    pub(crate) fn self_arc(&self) -> Option<Arc<MemberlistKv>> {
        self.me.upgrade()
    }

    pub(crate) fn has_left(&self) -> bool {
        self.leave.load(Ordering::Acquire)
    }

    /// Go `estNumNodes`.
    pub(crate) fn est_num_nodes(&self) -> usize {
        self.num_nodes.load(Ordering::Acquire) as usize
    }

    /// Go `NumMembers`: nodes that are not dead or left (including ourselves).
    pub fn num_members(&self) -> usize {
        self.nodes
            .read()
            .map
            .values()
            .filter(|n| !n.dead_or_left())
            .count()
    }

    /// Go `GetHealthScore`.
    pub fn health_score(&self) -> i32 {
        self.awareness.get_health_score()
    }

    fn next_seq_no(&self) -> u32 {
        self.sequence_num.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
    }

    fn next_incarnation(&self) -> u32 {
        self.incarnation.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
    }

    fn skip_incarnation(&self, offset: u32) -> u32 {
        self.incarnation.fetch_add(offset, Ordering::AcqRel).wrapping_add(offset)
    }

    /// Go `encodeBroadcastNotify` / `queueBroadcast`.
    pub(crate) fn queue_ml_broadcast(&self, node: &str, msg: Vec<u8>, notify: Option<mpsc::Sender<()>>) {
        self.ml_broadcasts.queue_broadcast(Arc::new(MemberlistBroadcast {
            node: node.to_string(),
            msg,
            notify,
        }));
    }

    /// Go `kRandomNodes` (with the optional node selection delegate).
    pub(crate) fn k_random_nodes(
        &self,
        k: usize,
        use_selection: bool,
        exclude: impl Fn(&NodeState) -> bool,
    ) -> Vec<NodeState> {
        let table = self.nodes.read();
        let mut nodes: Vec<&NodeState> = table.ordered();
        let mut k_nodes: Vec<NodeState> = Vec::with_capacity(k);
        if use_selection {
            let selection = if let Some(sel) = &self.node_selection_delegate {
                Some(sel.select_nodes(&nodes))
            } else {
                self.zone_select.as_ref().map(|sel| sel.select_nodes(&nodes))
            };
            if let Some((selected, preferred)) = selection {
                if let Some(p) = preferred {
                    if k > 0 && !exclude(nodes[p]) {
                        k_nodes.push(nodes[p].clone());
                    }
                }
                nodes = selected.into_iter().map(|i| nodes[i]).collect();
            }
        }
        let n = nodes.len();
        if n == 0 {
            return k_nodes;
        }
        let mut rng = rand::thread_rng();
        let mut i = 0;
        while i < 3 * n && k_nodes.len() < k {
            i += 1;
            let state = nodes[rng.gen_range(0..n)];
            if exclude(state) {
                continue;
            }
            if k_nodes.iter().any(|kn| kn.name == state.name) {
                continue;
            }
            k_nodes.push(state.clone());
        }
        k_nodes
    }

    // --- scheduling -------------------------------------------------------------------------

    /// Go `schedule`: probe, push/pull and gossip tickers plus the broadcast-queue sampler.
    pub(crate) fn schedule(self: &Arc<Self>) {
        let t = self.timing.clone();
        if !t.probe_interval.is_zero() {
            let kv = Arc::clone(self);
            spawn_trigger(t.probe_interval, self.shutdown_ctx.clone(), move || {
                let kv = Arc::clone(&kv);
                async move { kv.probe().await }
            });
        }
        if !t.push_pull_interval.is_zero() {
            let kv = Arc::clone(self);
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.canceled() => return,
                    _ = tokio::time::sleep(random_stagger(t.push_pull_interval)) => {}
                }
                loop {
                    let tick = push_pull_scale(t.push_pull_interval, kv.est_num_nodes());
                    tokio::select! {
                        _ = cancel.canceled() => return,
                        _ = tokio::time::sleep(tick) => kv.push_pull().await,
                    }
                }
            });
        }
        if !t.gossip_interval.is_zero() && t.gossip_nodes > 0 {
            let kv = Arc::clone(self);
            spawn_trigger(t.gossip_interval, self.shutdown_ctx.clone(), move || {
                let kv = Arc::clone(&kv);
                async move { kv.gossip().await }
            });
        }
        // Go `checkBroadcastQueueDepth`.
        {
            let kv = Arc::clone(self);
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => return,
                        _ = tokio::time::sleep(t.queue_check_interval) => {
                            let numq = kv.ml_broadcasts.num_queued();
                            kv.go_metrics.add_sample(&["memberlist", "queue", "broadcasts"], numq as f64, &[]);
                        }
                    }
                }
            });
        }
    }

    // --- failure detection ------------------------------------------------------------------

    /// Go `probe`: one round of failure detection.
    pub(crate) async fn probe(&self) {
        let mut num_check = 0usize;
        loop {
            let node = {
                let table = self.nodes.read();
                if num_check >= table.order.len() {
                    return;
                }
                let mut idx = self.probe_index.lock();
                if *idx >= table.order.len() {
                    drop(table);
                    *idx = 0;
                    drop(idx);
                    self.reset_nodes();
                    num_check += 1;
                    continue;
                }
                let name = &table.order[*idx];
                *idx += 1;
                let Some(node) = table.map.get(name) else {
                    num_check += 1;
                    continue;
                };
                if node.name == self.node_name || node.dead_or_left() {
                    num_check += 1;
                    continue;
                }
                node.clone()
            };
            self.probe_node(&node).await;
            return;
        }
    }

    /// Go `setProbeChannels`.
    pub(crate) fn set_probe_channels(
        &self,
        seq_no: u32,
        ack_tx: mpsc::Sender<AckMessage>,
        nack_tx: Option<mpsc::Sender<()>>,
        timeout: Duration,
    ) {
        let ack_tx2 = ack_tx.clone();
        let handler = AckHandler {
            ack: Box::new(move |payload, timestamp| {
                let _ = ack_tx2.try_send(AckMessage {
                    complete: true,
                    payload: payload.to_vec(),
                    timestamp,
                });
            }),
            nack: nack_tx.map(|tx| {
                Arc::new(move || {
                    let _ = tx.try_send(());
                }) as Arc<dyn Fn() + Send + Sync>
            }),
        };
        self.ack_handlers.lock().insert(seq_no, handler);
        let me = self.me.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if let Some(kv) = me.upgrade() {
                kv.ack_handlers.lock().remove(&seq_no);
            }
            let _ = ack_tx.try_send(AckMessage {
                complete: false,
                payload: Vec::new(),
                timestamp: Instant::now(),
            });
        });
    }

    /// Go `setAckHandler`.
    pub(crate) fn set_ack_handler_public(
        &self,
        seq_no: u32,
        ack: AckCallback,
        timeout: Duration,
    ) {
        self.ack_handlers
            .lock()
            .insert(seq_no, AckHandler { ack, nack: None });
        let me = self.me.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if let Some(kv) = me.upgrade() {
                kv.ack_handlers.lock().remove(&seq_no);
            }
        });
    }

    /// Go `invokeAckHandler`.
    pub(crate) fn invoke_ack_handler(&self, ack: &AckResp, timestamp: Instant) {
        let h = self.ack_handlers.lock().remove(&ack.seq_no);
        if let Some(h) = h {
            (h.ack)(ack.payload.as_deref().unwrap_or(&[]), timestamp);
        }
    }

    /// Go `invokeNackHandler`.
    pub(crate) fn invoke_nack_handler(&self, nack: &NackResp) {
        let nack_fn = self
            .ack_handlers
            .lock()
            .get(&nack.seq_no)
            .and_then(|h| h.nack.clone());
        if let Some(f) = nack_fn {
            f();
        }
    }

    fn self_ping(&self, seq_no: u32, node: &str) -> Ping {
        let (addr, port) = self.advertise();
        Ping {
            seq_no,
            node: node.to_string(),
            source_addr: ip_to_bytes(addr),
            source_port: port,
            source_node: self.node_name.clone(),
        }
    }

    /// Go `probeNode`: single round of failure checking on `node`.
    pub(crate) async fn probe_node(&self, node: &NodeState) {
        let started = Instant::now();
        let probe_interval = self.awareness.scale_timeout(self.timing.probe_interval);
        if probe_interval > self.timing.probe_interval {
            self.go_metrics
                .incr_counter(&["memberlist", "degraded", "probe"], 1.0, &[]);
        }
        let ping = self.self_ping(self.next_seq_no(), &node.name);
        let cap = self.timing.indirect_checks + 1;
        let (ack_tx, mut ack_rx) = mpsc::channel::<AckMessage>(cap);
        let (nack_tx, mut nack_rx) = mpsc::channel::<()>(cap);
        self.set_probe_channels(ping.seq_no, ack_tx.clone(), Some(nack_tx), probe_interval);

        let addr = node.address();
        let mut awareness_delta: i32;
        if node.state == NodeStateType::Alive {
            // dskit's TCP transport writes packets asynchronously and never reports a remote failure.
            let _ = self.send_msg(&addr, encode_ping(&ping)).await;
        } else {
            let s = Suspect {
                incarnation: node.incarnation,
                node: node.name.clone(),
                from: self.node_name.clone(),
            };
            let ping_buf = encode_ping(&ping);
            let sus_buf = encode_suspect(&s);
            let compound = make_compound_message(&[&ping_buf, &sus_buf]);
            let _ = self.raw_send_msg_packet(&addr, Some(node), compound).await;
        }
        awareness_delta = -1;

        // Wait for response or probe timeout.
        let probe_timeout = self.timing.probe_timeout;
        let deadline = started + probe_interval;
        tokio::select! {
            v = ack_rx.recv() => {
                if let Some(v) = v {
                    if v.complete {
                        if let Some(pd) = &self.ping_delegate {
                            let rtt = v.timestamp.saturating_duration_since(started);
                            pd.notify_ping_complete(node, rtt, &v.payload);
                        }
                        self.awareness.apply_delta(awareness_delta);
                        self.go_metrics.measure_since(&["memberlist", "probeNode"], started, &[]);
                        return;
                    }
                    let _ = ack_tx.try_send(v);
                }
            }
            _ = tokio::time::sleep(probe_timeout) => {
                debug!(node = %node.name, "memberlist: Failed UDP ping (timeout reached)");
            }
        }

        // Indirect probes via random live peers (no node selection, as in Go).
        let k_nodes = self.k_random_nodes(self.timing.indirect_checks, false, |n| {
            n.name == self.node_name || n.name == node.name || n.state != NodeStateType::Alive
        });
        let mut expected_nacks = 0;
        let (self_addr, self_port) = self.advertise();
        for peer in &k_nodes {
            let nack = peer.pmax() >= 4;
            if nack {
                expected_nacks += 1;
            }
            let ind = IndirectPingReq {
                seq_no: ping.seq_no,
                target: node.addr.clone(),
                port: node.port,
                node: node.name.clone(),
                nack,
                source_addr: ip_to_bytes(self_addr),
                source_port: self_port,
                source_node: self.node_name.clone(),
            };
            let _ = self.send_msg(&peer.address(), encode_indirect_ping(&ind)).await;
        }

        let disable_tcp = self.disable_tcp_pings
            || self
                .disable_tcp_pings_for_node
                .as_ref()
                .is_some_and(|f| f(&node.name));
        let fallback_rx = if !disable_tcp && node.pmax() >= 3 {
            if let Some(kv) = self.self_arc() {
                let (ftx, frx) = tokio::sync::oneshot::channel();
                let target_addr = node.address();
                let target_name = node.name.clone();
                let ping_clone = ping.clone();
                tokio::spawn(async move {
                    let ok = kv
                        .send_ping_and_wait_for_ack(&target_addr, &target_name, ping_clone, deadline)
                        .await
                        .unwrap_or(false);
                    let _ = ftx.send(ok);
                });
                Some(frx)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(v) = ack_rx.recv().await {
            if v.complete {
                self.awareness.apply_delta(awareness_delta);
                self.go_metrics.measure_since(&["memberlist", "probeNode"], started, &[]);
                return;
            }
        }

        if let Some(frx) = fallback_rx {
            if let Ok(true) = frx.await {
                warn!(node = %node.name, "memberlist: Was able to connect over TCP but UDP probes failed");
                self.awareness.apply_delta(awareness_delta);
                self.go_metrics.measure_since(&["memberlist", "probeNode"], started, &[]);
                return;
            }
        }

        awareness_delta = 0;
        if expected_nacks > 0 {
            let mut nack_count = 0;
            while nack_rx.try_recv().is_ok() {
                nack_count += 1;
            }
            if nack_count < expected_nacks {
                awareness_delta += expected_nacks - nack_count;
            }
        } else {
            awareness_delta += 1;
        }
        self.awareness.apply_delta(awareness_delta);

        info!(node = %node.name, "memberlist: Suspect node has failed, no acks received");
        self.suspect_node(&Suspect {
            incarnation: node.incarnation,
            node: node.name.clone(),
            from: self.node_name.clone(),
        });
        self.go_metrics.measure_since(&["memberlist", "probeNode"], started, &[]);
    }

    /// Go `resetNodes`: reap dead nodes past GossipToTheDeadTime and shuffle the probe order.
    pub(crate) fn reset_nodes(&self) {
        let mut table = self.nodes.write();
        let gtd = self.timing.gossip_to_the_dead_time;
        let now = SystemTime::now();
        let reap: Vec<String> = table
            .order
            .iter()
            .filter(|name| {
                table.map.get(*name).is_some_and(|n| {
                    n.dead_or_left()
                        && now.duration_since(n.state_change).unwrap_or(Duration::ZERO) > gtd
                })
            })
            .cloned()
            .collect();
        for name in &reap {
            table.map.remove(name);
        }
        table.order.retain(|n| !reap.contains(n));
        self.num_nodes.store(table.order.len() as u32, Ordering::Release);
        table.order.shuffle(&mut rand::thread_rng());
    }

    // --- gossip / push-pull -----------------------------------------------------------------

    /// Go `gossip`.
    pub(crate) async fn gossip(&self) {
        let started = Instant::now();
        let gtd = self.timing.gossip_to_the_dead_time;
        let now = SystemTime::now();
        let k_nodes = self.k_random_nodes(self.timing.gossip_nodes, true, |n| {
            if n.name == self.node_name {
                return true;
            }
            match n.state {
                NodeStateType::Alive | NodeStateType::Suspect => false,
                NodeStateType::Dead => now.duration_since(n.state_change).unwrap_or(Duration::ZERO) > gtd,
                NodeStateType::Left => true,
            }
        });
        let enc_overhead = if self.encryption_enabled() && self.gossip_verify_outgoing {
            super::super::security::encrypt_overhead(self.encryption_version())
        } else {
            0
        };
        let bytes_avail = self
            .udp_buffer_size
            .saturating_sub(COMPOUND_HEADER_OVERHEAD + self.label_overhead() + enc_overhead);
        for node in k_nodes {
            let msgs = self.get_broadcasts(COMPOUND_OVERHEAD, bytes_avail);
            if msgs.is_empty() {
                break;
            }
            let addr = node.address();
            if msgs.len() == 1 {
                let msg = msgs.into_iter().next().expect("one");
                if let Err(err) = self.raw_send_msg_packet(&addr, Some(&node), msg).await {
                    error!(%addr, %err, "memberlist: Failed to send gossip");
                }
            } else {
                for compound in make_compound_messages(&msgs) {
                    if let Err(err) = self.raw_send_msg_packet(&addr, Some(&node), compound).await {
                        error!(%addr, %err, "memberlist: Failed to send gossip");
                    }
                }
            }
        }
        self.go_metrics.measure_since(&["memberlist", "gossip"], started, &[]);
    }

    /// Go `pushPull`.
    pub(crate) async fn push_pull(&self) {
        let num = self.timing.push_pull_nodes.max(1);
        let nodes = self.k_random_nodes(num, true, |n| {
            n.name == self.node_name || n.state != NodeStateType::Alive
        });
        for node in nodes {
            if let Err(err) = self.push_pull_node(&node.address(), false).await {
                error!(node = %node.name, %err, "memberlist: Push/Pull failed");
            }
        }
    }

    /// Go `getBroadcasts`: memberlist broadcasts first, then the delegate's (dskit KV) ones.
    pub(crate) fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Vec<u8>> {
        let mut to_send = self.ml_broadcasts.get_broadcasts(overhead, limit);
        let used: usize = to_send.iter().map(|m| m.len() + overhead).sum();
        let avail = limit as i64 - used as i64;
        if avail > (overhead + USER_MSG_OVERHEAD) as i64 {
            for msg in self.delegate_get_broadcasts(overhead + USER_MSG_OVERHEAD, avail as usize) {
                let mut buf = Vec::with_capacity(msg.len() + 1);
                buf.push(MessageType::User as u8);
                buf.extend_from_slice(&msg);
                to_send.push(buf);
            }
        }
        to_send
    }

    // --- message handlers --------------------------------------------------------------------

    /// Go `handlePing`.
    pub(crate) async fn handle_ping(&self, p: Ping, from: &str) {
        if !p.node.is_empty() && p.node != self.node_name {
            warn!(node = %p.node, from, "memberlist: Got ping for unexpected node");
            return;
        }
        let payload = self.ping_delegate.as_ref().map(|pd| pd.ack_payload());
        let ack = AckResp {
            seq_no: p.seq_no,
            payload,
        };
        let addr = if !p.source_addr.is_empty() && p.source_port > 0 {
            format_ip_port(&p.source_addr, p.source_port)
        } else {
            from.to_string()
        };
        if let Err(err) = self.send_msg(&addr, encode_ack_resp(&ack)).await {
            error!(%err, from, "memberlist: Failed to send ack");
        }
    }

    /// Go `handleIndirectPing`.
    pub(crate) async fn handle_indirect_ping(&self, mut ind: IndirectPingReq, from: &str) {
        if ind.port == 0 {
            ind.port = self.bind_port();
        }
        let local_seq = self.next_seq_no();
        let ping = self.self_ping(local_seq, &ind.node);
        let ind_addr = if !ind.source_addr.is_empty() && ind.source_port > 0 {
            format_ip_port(&ind.source_addr, ind.source_port)
        } else {
            from.to_string()
        };
        // Only an actual Ack response cancels the Nack timer (Go closes cancelCh inside
        // respHandler; expiring the ackHandler on timeout must NOT cancel sending NackResp).
        let acked = Arc::new((AtomicBool::new(false), Notify::new()));
        {
            let me = self.me.clone();
            let ind_addr = ind_addr.clone();
            let seq = ind.seq_no;
            let acked_cb = Arc::clone(&acked);
            self.set_ack_handler_public(
                local_seq,
                Box::new(move |_payload, _ts| {
                    acked_cb.0.store(true, Ordering::Release);
                    acked_cb.1.notify_waiters();
                    if let Some(kv) = me.upgrade() {
                        tokio::spawn(async move {
                            let ack = AckResp { seq_no: seq, payload: None };
                            if let Err(err) = kv.send_msg(&ind_addr, encode_ack_resp(&ack)).await {
                                error!(%err, "memberlist: Failed to forward ack");
                            }
                        });
                    }
                }),
                self.timing.probe_timeout,
            );
        }
        let addr = format_ip_port(&ind.target, ind.port);
        if let Err(err) = self.send_msg(&addr, encode_ping(&ping)).await {
            error!(%err, "memberlist: Failed to send indirect ping");
        }
        if ind.nack {
            let me = self.me.clone();
            let timeout = self.timing.probe_timeout;
            let seq = ind.seq_no;
            tokio::spawn(async move {
                let notified = acked.1.notified();
                tokio::pin!(notified);
                if acked.0.load(Ordering::Acquire) {
                    return;
                }
                tokio::select! {
                    _ = &mut notified => {}
                    _ = tokio::time::sleep(timeout) => {
                        if acked.0.load(Ordering::Acquire) {
                            return;
                        }
                        if let Some(kv) = me.upgrade() {
                            let nack = NackResp { seq_no: seq };
                            if let Err(err) = kv.send_msg(&ind_addr, encode_nack_resp(&nack)).await {
                                error!(%err, "memberlist: Failed to send nack");
                            }
                        }
                    }
                }
            });
        }
    }

    // --- state transitions -------------------------------------------------------------------

    /// Go `refute`.
    fn refute(&self, me: &mut NodeState, accused_inc: u32) {
        let mut inc = self.next_incarnation();
        if accused_inc >= inc {
            inc = self.skip_incarnation(accused_inc.wrapping_sub(inc).wrapping_add(1));
        }
        me.incarnation = inc;
        self.awareness.apply_delta(1);
        let a = Alive {
            incarnation: inc,
            node: me.name.clone(),
            addr: me.addr.clone(),
            port: me.port,
            meta: me.meta.clone(),
            vsn: me.vsn.clone(),
        };
        self.queue_ml_broadcast(&me.name, encode_alive(&a), None);
    }

    /// Go `aliveNode`.
    pub(crate) fn alive_node(&self, a: &Alive, notify: Option<mpsc::Sender<()>>, bootstrap: bool) {
        if self.has_left() && a.node == self.node_name {
            return;
        }
        if a.vsn.len() >= 3 {
            let (pmin, pmax, pcur) = (a.vsn[0], a.vsn[1], a.vsn[2]);
            if pmin == 0 || pmax == 0 || pmin > pmax {
                warn!(node = %a.node, pmin, pcur, pmax, "memberlist: Ignoring an alive message because protocol version(s) are wrong");
                return;
            }
        }
        if let Some(alive_del) = &self.alive_delegate {
            if a.vsn.len() < 6 {
                warn!(node = %a.node, "memberlist: ignoring alive message because Vsn is not present");
                return;
            }
            let node = NodeState {
                name: a.node.clone(),
                addr: a.addr.clone(),
                port: a.port,
                meta: a.meta.clone(),
                incarnation: a.incarnation,
                state: NodeStateType::Alive,
                state_change: SystemTime::now(),
                vsn: a.vsn.clone(),
            };
            if let Err(err) = alive_del.notify_alive(&node) {
                warn!(node = %a.node, %err, "memberlist: ignoring alive message");
                return;
            }
        }

        enum PendingEvent {
            Join(NodeState),
            Update(NodeState),
        }

        let pending_event = {
            let mut table = self.nodes.write();
            if self.has_left() && a.node == self.node_name {
                return;
            }

            let mut updates_node = false;
            if !table.map.contains_key(&a.node) {
                if let Some(cidrs) = self.cidrs_allowed.as_deref() {
                    if let Some(ip) = super::bytes_to_ip(&a.addr) {
                        if let Err(err) = super::super::security::ip_allowed(Some(cidrs), ip) {
                            warn!(node = %a.node, %err, "memberlist: Rejected node");
                            return;
                        }
                    }
                }
                let state = NodeState {
                    name: a.node.clone(),
                    addr: a.addr.clone(),
                    port: a.port,
                    meta: a.meta.clone(),
                    incarnation: 0,
                    state: NodeStateType::Dead,
                    state_change: SystemTime::now(),
                    vsn: if a.vsn.len() > 5 { a.vsn.clone() } else { vec![0; 6] },
                };
                table.map.insert(a.node.clone(), state);
                let n = table.order.len();
                let offset = if n == 0 { 0 } else { rand::thread_rng().gen_range(0..n as u32) as usize };
                table.order.push(a.node.clone());
                table.order.swap(offset, n);
                self.num_nodes.fetch_add(1, Ordering::AcqRel);
            } else {
                let state = table.map.get(&a.node).expect("present");
                if state.addr != a.addr || state.port != a.port {
                    if let Some(cidrs) = self.cidrs_allowed.as_deref() {
                        if let Some(ip) = super::bytes_to_ip(&a.addr) {
                            if let Err(err) = super::super::security::ip_allowed(Some(cidrs), ip) {
                                warn!(node = %a.node, %err, "memberlist: Rejected IP update");
                                return;
                            }
                        }
                    }
                    let reclaim = self.timing.dead_node_reclaim_time;
                    let can_reclaim = !reclaim.is_zero()
                        && SystemTime::now()
                            .duration_since(state.state_change)
                            .unwrap_or(Duration::ZERO)
                            > reclaim;
                    if state.state == NodeStateType::Left
                        || (state.state == NodeStateType::Dead && can_reclaim)
                    {
                        info!(node = %state.name, "memberlist: Updating address for left or failed node");
                        updates_node = true;
                    } else {
                        error!(
                            node = %state.name,
                            mine = %state.address(),
                            theirs = %format_ip_port(&a.addr, a.port),
                            "memberlist: Conflicting address"
                        );
                        let existing_clone = state.clone();
                        let other = NodeState {
                            name: a.node.clone(),
                            addr: a.addr.clone(),
                            port: a.port,
                            meta: a.meta.clone(),
                            incarnation: a.incarnation,
                            state: NodeStateType::Alive,
                            state_change: SystemTime::now(),
                            vsn: if a.vsn.len() > 5 { a.vsn.clone() } else { vec![0; 6] },
                        };
                        drop(table);
                        if let Some(cd) = &self.conflict_delegate {
                            cd.notify_conflict(&existing_clone, &other);
                        }
                        return;
                    }
                }
            }

            let is_local = a.node == self.node_name;
            let state_inc = table.map[&a.node].incarnation;
            if a.incarnation <= state_inc && !is_local && !updates_node {
                return;
            }
            if a.incarnation < state_inc && is_local {
                return;
            }
            if let Some(t) = table.timers.remove(&a.node) {
                t.stop();
            }

            let state = table.map.get_mut(&a.node).expect("present");
            let old_state = state.state;
            let old_meta = state.meta.clone();
            if !bootstrap && is_local {
                if a.incarnation == state.incarnation
                    && a.meta.as_deref().unwrap_or_default() == state.meta.as_deref().unwrap_or_default()
                    && a.vsn == state.vsn
                {
                    return;
                }
                self.refute(state, a.incarnation);
                warn!(node = %a.node, "memberlist: Refuting an alive message");
            } else {
                self.queue_ml_broadcast(&a.node, encode_alive(a), notify);
                if !a.vsn.is_empty() {
                    state.vsn = a.vsn.clone();
                }
                state.incarnation = a.incarnation;
                state.meta = a.meta.clone();
                state.addr = a.addr.clone();
                state.port = a.port;
                if state.state != NodeStateType::Alive {
                    state.state = NodeStateType::Alive;
                    state.state_change = SystemTime::now();
                }
            }
            self.go_metrics
                .incr_counter(&["memberlist", "msg", "alive"], 1.0, &[]);
            if old_state == NodeStateType::Dead || old_state == NodeStateType::Left {
                Some(PendingEvent::Join(state.clone()))
            } else if old_meta.as_deref().unwrap_or_default()
                != state.meta.as_deref().unwrap_or_default()
            {
                Some(PendingEvent::Update(state.clone()))
            } else {
                None
            }
        };

        if let Some(ev) = pending_event {
            let ed = self.event_delegate.read().clone();
            if let Some(ed) = ed {
                match ev {
                    PendingEvent::Join(n) => ed.notify_join(&n),
                    PendingEvent::Update(n) => ed.notify_update(&n),
                }
            }
        }
    }

    /// Go `suspectNode`.
    pub(crate) fn suspect_node(&self, s: &Suspect) {
        let mut table = self.nodes.write();
        let Some(state) = table.map.get(&s.node) else {
            return;
        };
        if s.incarnation < state.incarnation {
            return;
        }
        if let Some(timer) = table.timers.get(&s.node).cloned() {
            if timer.confirm(&s.from) {
                self.queue_ml_broadcast(&s.node, encode_suspect(s), None);
            }
            return;
        }
        if state.state != NodeStateType::Alive {
            return;
        }
        if state.name == self.node_name {
            let state = table.map.get_mut(&s.node).expect("present");
            self.refute(state, s.incarnation);
            warn!(from = %s.from, "memberlist: Refuting a suspect message");
            return;
        }
        self.queue_ml_broadcast(&s.node, encode_suspect(s), None);
        self.go_metrics
            .incr_counter(&["memberlist", "msg", "suspect"], 1.0, &[]);

        let change_time = SystemTime::now();
        {
            let state = table.map.get_mut(&s.node).expect("present");
            state.incarnation = s.incarnation;
            state.state = NodeStateType::Suspect;
            state.state_change = change_time;
        }

        let mut k = self.timing.suspicion_mult as i32 - 2;
        let n = self.est_num_nodes();
        if (n as i32) - 2 < k {
            k = 0;
        }
        let min = suspicion_timeout(self.timing.suspicion_mult, n, self.timing.probe_interval);
        let max = min * self.timing.suspicion_max_timeout_mult as u32;
        let me = self.me.clone();
        let node_name = s.node.clone();
        let f = Box::new(move |num_confirmations: i32| {
            let Some(kv) = me.upgrade() else {
                return;
            };
            let d = {
                let table = kv.nodes.read();
                match table.map.get(&node_name) {
                    Some(st) if st.state == NodeStateType::Suspect && st.state_change == change_time => {
                        Some(Dead {
                            incarnation: st.incarnation,
                            node: st.name.clone(),
                            from: kv.node_name.clone(),
                        })
                    }
                    _ => None,
                }
            };
            if let Some(d) = d {
                if k > 0 && num_confirmations < k {
                    kv.go_metrics
                        .incr_counter(&["memberlist", "degraded", "timeout"], 1.0, &[]);
                }
                info!(node = %d.node, num_confirmations, "memberlist: Marking node as failed, suspect timeout reached");
                kv.dead_node(&d);
            }
        });
        table
            .timers
            .insert(s.node.clone(), Suspicion::new(&s.from, k, min, max, f));
    }

    /// Go `deadNode`.
    pub(crate) fn dead_node(&self, d: &Dead) {
        let mut table = self.nodes.write();
        let Some(state) = table.map.get(&d.node) else {
            return;
        };
        if d.incarnation < state.incarnation {
            return;
        }
        if let Some(t) = table.timers.remove(&d.node) {
            t.stop();
        }
        let state = table.map.get_mut(&d.node).expect("present");
        if state.dead_or_left() {
            return;
        }
        if state.name == self.node_name {
            if !self.has_left() {
                self.refute(state, d.incarnation);
                warn!(from = %d.from, "memberlist: Refuting a dead message");
                return;
            }
            let notify = self.leave_broadcast_tx.clone();
            self.queue_ml_broadcast(&d.node, encode_dead(d), Some(notify));
        } else {
            self.queue_ml_broadcast(&d.node, encode_dead(d), None);
        }
        self.go_metrics.incr_counter(&["memberlist", "msg", "dead"], 1.0, &[]);
        state.incarnation = d.incarnation;
        state.state = if d.node == d.from {
            NodeStateType::Left
        } else {
            NodeStateType::Dead
        };
        state.state_change = SystemTime::now();
        let left_node = state.clone();
        drop(table);
        let ed = self.event_delegate.read().clone();
        if let Some(ed) = ed {
            ed.notify_leave(&left_node);
        }
    }

    /// Go `mergeState`.
    pub(crate) fn merge_state(&self, remote: &[PushNodeState]) {
        for r in remote {
            match r.state {
                NodeStateType::Alive => self.alive_node(
                    &Alive {
                        incarnation: r.incarnation,
                        node: r.name.clone(),
                        addr: r.addr.clone(),
                        port: r.port,
                        meta: r.meta.clone(),
                        vsn: r.vsn.clone(),
                    },
                    None,
                    false,
                ),
                NodeStateType::Left => self.dead_node(&Dead {
                    incarnation: r.incarnation,
                    node: r.name.clone(),
                    from: r.name.clone(),
                }),
                NodeStateType::Dead | NodeStateType::Suspect => self.suspect_node(&Suspect {
                    incarnation: r.incarnation,
                    node: r.name.clone(),
                    from: self.node_name.clone(),
                }),
            }
        }
    }

    /// Go `verifyProtocol`: all remote and local nodes must speak a common protocol version.
    pub(crate) fn verify_protocol(&self, remote: &[PushNodeState]) -> Result<(), String> {
        let table = self.nodes.read();
        let mut maxpmin = 0u8;
        let mut minpmax = u8::MAX;
        let mut maxdmin = 0u8;
        let mut mindmax = u8::MAX;
        let vsn6 = |v: &[u8]| -> [u8; 6] {
            let mut out = [0u8; 6];
            for (i, b) in v.iter().take(6).enumerate() {
                out[i] = *b;
            }
            out
        };
        for rn in remote {
            if rn.state != NodeStateType::Alive || rn.vsn.is_empty() {
                continue;
            }
            let v = vsn6(&rn.vsn);
            maxpmin = maxpmin.max(v[0]);
            minpmax = minpmax.min(v[1]);
            maxdmin = maxdmin.max(v[3]);
            mindmax = mindmax.min(v[4]);
        }
        for n in table.map.values() {
            if n.state != NodeStateType::Alive {
                continue;
            }
            let v = vsn6(&n.vsn);
            maxpmin = maxpmin.max(v[0]);
            minpmax = minpmax.min(v[1]);
            maxdmin = maxdmin.max(v[3]);
            mindmax = mindmax.min(v[4]);
        }
        for rn in remote {
            let v = vsn6(&rn.vsn);
            let (pcur, dcur) = (v[2], v[5]);
            if pcur < maxpmin || pcur > minpmax {
                return Err(format!(
                    "Node '{}' protocol version ({pcur}) is incompatible: [{maxpmin}, {minpmax}]",
                    rn.name
                ));
            }
            if dcur < maxdmin || dcur > mindmax {
                return Err(format!(
                    "Node '{}' delegate protocol version ({dcur}) is incompatible: [{maxdmin}, {mindmax}]",
                    rn.name
                ));
            }
        }
        for n in table.map.values() {
            let v = vsn6(&n.vsn);
            let (pcur, dcur) = (v[2], v[5]);
            if pcur < maxpmin || pcur > minpmax {
                return Err(format!(
                    "Node '{}' protocol version ({pcur}) is incompatible: [{maxpmin}, {minpmax}]",
                    n.name
                ));
            }
            if dcur < maxdmin || dcur > mindmax {
                return Err(format!(
                    "Node '{}' delegate protocol version ({dcur}) is incompatible: [{maxdmin}, {mindmax}]",
                    n.name
                ));
            }
        }
        Ok(())
    }

    /// Go `sendLocalState` node-instance gauges.
    pub(crate) fn record_node_instances(&self, nodes: &[PushNodeState]) {
        let mut counts: HashMap<&'static str, usize> = HashMap::new();
        for s in [NodeStateType::Alive, NodeStateType::Left, NodeStateType::Dead, NodeStateType::Suspect] {
            counts.insert(state_metrics_string(s), 0);
        }
        for n in nodes {
            *counts.entry(state_metrics_string(n.state)).or_default() += 1;
        }
        for (state, cnt) in counts {
            self.go_metrics.set_gauge(
                &["memberlist", "node", "instances"],
                cnt as f64,
                &[("node_state", state)],
            );
        }
    }

    /// Go `Leave`: broadcast our own death and wait for it to be gossiped (or the timeout).
    pub(crate) async fn leave(&self, timeout: Duration) -> Result<(), String> {
        if self.leave.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let me = {
            let table = self.nodes.read();
            table.map.get(&self.node_name).cloned()
        };
        let Some(me) = me else {
            warn!("memberlist: Leave but we're not in the node map.");
            return Ok(());
        };
        self.dead_node(&Dead {
            incarnation: me.incarnation,
            node: me.name.clone(),
            from: me.name.clone(),
        });
        let any_alive = self
            .nodes
            .read()
            .map
            .values()
            .any(|n| !n.dead_or_left() && n.name != self.node_name);
        if any_alive {
            let mut rx = self.leave_broadcast_rx.lock().await;
            if timeout.is_zero() {
                let _ = rx.recv().await;
            } else if tokio::time::timeout(timeout, rx.recv()).await.is_err() {
                return Err("timeout waiting for leave broadcast".to_string());
            }
        }
        Ok(())
    }
}

impl NodeState {
    /// Go `Node.PMax`.
    pub fn pmax(&self) -> u8 {
        self.vsn.get(1).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::KvOptions;
    use super::*;
    use mimir_config::server::MemberlistConfig;

    #[tokio::test]
    async fn refute_at_u32_max_wraps_without_panic() {
        let cfg = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            node_name: "refute-u32-max".to_string(),
            randomize_node_name: false,
            ..Default::default()
        };
        let kv = MemberlistKv::new(cfg, vec![], "", None, KvOptions::default()).unwrap();
        let node_name = kv.node_name.clone();
        kv.alive_node(
            &Alive {
                incarnation: u32::MAX,
                node: node_name.clone(),
                addr: vec![127, 0, 0, 1],
                port: 7946,
                meta: None,
                vsn: vec![1, 5, 2, 1, 2, 1],
            },
            None,
            true,
        );
        // Set local incarnation to u32::MAX so next_incarnation() wraps to 0 while accused_inc == u32::MAX.
        kv.incarnation.store(u32::MAX, Ordering::Release);
        kv.suspect_node(&Suspect {
            incarnation: u32::MAX,
            node: node_name.clone(),
            from: "accuser".to_string(),
        });
        let table = kv.nodes.read();
        let me = table.map.get(&node_name).expect("local node present");
        assert_eq!(me.state, NodeStateType::Alive);
    }

    #[tokio::test]
    async fn spawn_trigger_preserves_one_buffered_tick_like_go_ticker() {
        use std::sync::atomic::AtomicUsize;

        let cancel = mimir_common::services::ServiceContext::background();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_ref = Arc::clone(&calls);
        let interval = Duration::from_millis(60);
        spawn_trigger(interval, cancel.clone(), move || {
            let calls = Arc::clone(&calls_ref);
            async move {
                let n = calls.fetch_add(1, Ordering::AcqRel);
                if n == 0 {
                    // Simulate a failed probe finishing slightly after ProbeInterval.
                    tokio::time::sleep(Duration::from_millis(75)).await;
                }
            }
        });
        // With 1-slot buffering like Go's time.NewTicker + triggerFunc:
        // t=60ms fires call 0 (finishes at 135ms), t=120ms tick was buffered so call 1
        // runs immediately at 135ms, and t=180ms fires call 2.
        // Under MissedTickBehavior::Skip, call 1 would wait until 180ms and call 2 until 240ms.
        tokio::time::sleep(Duration::from_millis(215)).await;
        cancel.cancel();
        let count = calls.load(Ordering::Acquire);
        assert!(
            count >= 3,
            "expected at least 3 trigger calls in 215ms (1-slot buffered tick), got {count}"
        );
    }
}
