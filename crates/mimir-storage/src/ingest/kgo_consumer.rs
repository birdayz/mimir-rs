//! Consumer helpers, KIP-848 group signals, fetchManager concurrency gating, pollWaitState
//! rebalance gate, regex/topic pause/purge tracking, and Fetches::each_topic grouping.
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/{consumer.go,consumer_group_848.go,record_and_fetch.go}

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use regex::Regex;
use tokio::sync::{mpsc, oneshot};

use super::kafka_client::FetchedRecord;

#[derive(Debug, Clone, Default)]
pub struct KgoFetchPartition {
    pub partition: i32,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub records: Vec<FetchedRecord>,
}

#[derive(Debug, Clone, Default)]
pub struct KgoFetchTopic {
    pub topic: String,
    pub topic_id: [u8; 16],
    pub partitions: Vec<KgoFetchPartition>,
}

#[derive(Debug, Clone, Default)]
pub struct KgoFetch {
    pub topics: Vec<KgoFetchTopic>,
}

/// Ported from Fetches.EachTopic: groups FetchTopic across multiple broker fetches while
/// preserving topic_id.
pub fn fetches_each_topic<F>(fetches: &[KgoFetch], mut f: F)
where
    F: FnMut(KgoFetchTopic),
{
    if fetches.len() == 1 {
        for ft in &fetches[0].topics {
            f(ft.clone());
        }
        return;
    }
    let mut grouped: HashMap<String, KgoFetchTopic> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for fetch in fetches {
        for ft in &fetch.topics {
            let entry = grouped.entry(ft.topic.clone()).or_insert_with(|| {
                order.push(ft.topic.clone());
                KgoFetchTopic {
                    topic: ft.topic.clone(),
                    topic_id: ft.topic_id,
                    partitions: Vec::new(),
                }
            });
            if entry.topic_id == [0u8; 16] && ft.topic_id != [0u8; 16] {
                entry.topic_id = ft.topic_id;
            }
            entry.partitions.extend(ft.partitions.iter().cloned());
        }
    }
    for name in order {
        if let Some(ft) = grouped.remove(&name) {
            f(ft);
        }
    }
}

pub struct Group848Signals {
    pub is_848: bool,
    pub rejoin_tx: mpsc::Sender<String>,
    pub heartbeat_force_tx: mpsc::Sender<Box<dyn FnOnce(Option<String>) + Send>>,
}

impl Group848Signals {
    pub fn signal_subscription_change(&self, why: &str) {
        if self.is_848 {
            let _ = self.heartbeat_force_tx.try_send(Box::new(|_| {}));
        } else {
            let _ = self.rejoin_tx.try_send(why.to_string());
        }
    }
}

/// Ported from shouldNotify848Restart.
pub fn should_notify_848_restart(restarts: i64, retries: i64) -> bool {
    if retries <= 0 {
        return true;
    }
    restarts % retries == 0
}

pub struct FetchManager {
    desire_tx: mpsc::UnboundedSender<oneshot::Sender<mpsc::Sender<bool>>>,
    cancel_tx: mpsc::UnboundedSender<oneshot::Sender<mpsc::Sender<bool>>>,
}

impl FetchManager {
    pub fn start(
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
        poll_active: Arc<AtomicBool>,
        mut poll_wake: mpsc::Receiver<()>,
        allowed_fetches: isize,
    ) -> Self {
        let (desire_tx, mut desire_rx) =
            mpsc::unbounded_channel::<oneshot::Sender<mpsc::Sender<bool>>>();
        let (cancel_tx, mut cancel_fetch_rx) =
            mpsc::unbounded_channel::<oneshot::Sender<mpsc::Sender<bool>>>();
        let (done_tx, mut done_rx) = mpsc::channel::<bool>(64);

        tokio::spawn(async move {
            let mut active_fetches: isize = 0;
            let mut want_fetch: Vec<oneshot::Sender<mpsc::Sender<bool>>> = Vec::new();
            let mut want_quit = *cancel_rx.borrow();
            let mut cancel_active = !want_quit;

            loop {
                tokio::select! {
                    biased;
                    Some(reg) = desire_rx.recv() => {
                        want_fetch.push(reg);
                    }
                    Some(c) = cancel_fetch_rx.recv() => {
                        if !want_fetch.is_empty() {
                            want_fetch.remove(0);
                        } else {
                            active_fetches -= 1;
                        }
                        drop(c);
                    }
                    Some(_) = done_rx.recv() => {
                        active_fetches -= 1;
                    }
                    _ = cancel_rx.changed(), if cancel_active => {
                        if *cancel_rx.borrow() {
                            want_quit = true;
                            cancel_active = false;
                        }
                    }
                    Some(()) = poll_wake.recv() => {}
                }

                let poll_allowed = poll_active.load(Ordering::SeqCst);
                if !want_quit
                    && !want_fetch.is_empty()
                    && (active_fetches < allowed_fetches
                        || (poll_allowed && active_fetches == 0)
                        || allowed_fetches < 0)
                {
                    let grant = want_fetch.remove(0);
                    let _ = grant.send(done_tx.clone());
                    active_fetches += 1;
                    continue;
                }

                if want_quit && active_fetches == 0 && want_fetch.is_empty() {
                    break;
                }
            }
        });

        Self { desire_tx, cancel_tx }
    }

    pub fn desire_fetch(
        &self,
        grant_tx: oneshot::Sender<mpsc::Sender<bool>>,
    ) -> Result<(), mpsc::error::SendError<oneshot::Sender<mpsc::Sender<bool>>>> {
        self.desire_tx.send(grant_tx)
    }

    pub fn cancel_fetch(&self, grant_tx: oneshot::Sender<mpsc::Sender<bool>>) {
        let _ = self.cancel_tx.send(grant_tx);
    }
}

pub struct PollWaitGate {
    block_rebalance_on_poll: bool,
    state: Mutex<u64>,
    cond: Condvar,
}

impl PollWaitGate {
    pub fn new(block_rebalance_on_poll: bool) -> Self {
        Self {
            block_rebalance_on_poll,
            state: Mutex::new(0),
            cond: Condvar::new(),
        }
    }

    pub fn state(&self) -> u64 {
        *self.state.lock().unwrap()
    }

    pub fn wait_and_add_poller(&self) {
        if !self.block_rebalance_on_poll {
            return;
        }
        let mut s = self.state.lock().unwrap();
        if (*s & (u32::MAX as u64)) == 0 {
            while (*s >> 32) != 0 {
                s = self.cond.wait(s).unwrap();
            }
        }
        *s += 1;
    }

    pub fn unadd_poller(&self) {
        if !self.block_rebalance_on_poll {
            return;
        }
        let mut s = self.state.lock().unwrap();
        if (*s & (u32::MAX as u64)) == 0 {
            return;
        }
        *s -= 1;
        if (*s & (u32::MAX as u64)) == 0 {
            self.cond.notify_all();
        }
    }

    pub fn allow_rebalance(&self) {
        if !self.block_rebalance_on_poll {
            return;
        }
        let mut s = self.state.lock().unwrap();
        *s &= !(u32::MAX as u64);
        self.cond.notify_all();
    }

    pub fn wait_and_add_rebalance(&self) {
        if !self.block_rebalance_on_poll {
            return;
        }
        let mut s = self.state.lock().unwrap();
        *s += 1u64 << 32;
        while (*s & (u32::MAX as u64)) != 0 {
            s = self.cond.wait(s).unwrap();
        }
    }

    pub fn unadd_rebalance(&self) {
        if !self.block_rebalance_on_poll {
            return;
        }
        let mut s = self.state.lock().unwrap();
        *s -= 1u64 << 32;
        if (*s >> 32) == 0 {
            self.cond.notify_all();
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PausedPartitions {
    pub all: bool,
    pub partitions: HashSet<i32>,
}

#[derive(Debug, Default)]
pub struct PausedTopics {
    pub map: HashMap<String, PausedPartitions>,
}

impl PausedTopics {
    pub fn add_topics(&mut self, topics: &[&str]) {
        for &t in topics {
            self.map.entry(t.to_string()).or_default().all = true;
        }
    }

    pub fn add_partitions(&mut self, parts: &HashMap<String, Vec<i32>>) {
        for (t, ps) in parts {
            let entry = self.map.entry(t.clone()).or_default();
            for &p in ps {
                entry.partitions.insert(p);
            }
        }
    }

    /// Ported from pausedTopics.delTopics: writes modified struct back and keeps partition pauses.
    pub fn del_topics(&mut self, topics: &[&str]) {
        for &t in topics {
            if let Some(entry) = self.map.get_mut(t) {
                entry.all = false;
                if entry.partitions.is_empty() {
                    self.map.remove(t);
                }
            }
        }
    }

    pub fn del_partitions(&mut self, parts: &HashMap<String, Vec<i32>>) {
        for (t, ps) in parts {
            if let Some(entry) = self.map.get_mut(t) {
                for p in ps {
                    entry.partitions.remove(p);
                }
                if !entry.all && entry.partitions.is_empty() {
                    self.map.remove(t);
                }
            }
        }
    }

    pub fn is_paused(&self, topic: &str, partition: i32) -> bool {
        self.map
            .get(topic)
            .is_some_and(|p| p.all || p.partitions.contains(&partition))
    }
}

pub trait KgoHook: Send + Sync {
    fn on_new_client(&self) {}
    fn on_poll_start(&self) {}
    fn hook_kind(&self) -> &'static str {
        "hook"
    }
}

pub trait HookPollStart: Send + Sync {
    fn on_poll_start(&self);
}

impl<T: HookPollStart> KgoHook for T {
    fn on_poll_start(&self) {
        HookPollStart::on_poll_start(self);
    }
    fn hook_kind(&self) -> &'static str {
        "HookPollStart"
    }
}

#[derive(Clone)]
pub enum HookInput {
    Hook(Arc<dyn KgoHookWrapper>),
    Slice(Vec<HookInput>),
    Invalid(&'static str),
}

pub trait KgoHookWrapper: Send + Sync {
    fn id(&self) -> usize;
    fn as_dyn(&self) -> &dyn KgoHook;
}

pub fn process_hooks(inputs: Vec<HookInput>) -> Result<Vec<Arc<dyn KgoHookWrapper>>, String> {
    let mut out = Vec::new();
    for item in inputs {
        match item {
            HookInput::Hook(h) => out.push(h),
            HookInput::Slice(nested) => out.extend(process_hooks(nested)?),
            HookInput::Invalid(msg) => return Err(format!("invalid hook: {msg}")),
        }
    }
    Ok(out)
}

#[derive(Debug, Default)]
pub struct RegexEvaluationLog {
    pub added: Vec<String>,
    pub excluded: Vec<String>,
    pub skipped_internal: Vec<String>,
}

pub fn evaluate_regex_topics(
    include_patterns: &[Regex],
    exclude_patterns: &[Regex],
    cluster_topics: &[(String, bool)],
) -> (BTreeSet<String>, RegexEvaluationLog) {
    let mut matched = BTreeSet::new();
    let mut log = RegexEvaluationLog::default();
    for (name, is_internal) in cluster_topics {
        if *is_internal {
            if include_patterns.iter().any(|re| re.is_match(name)) {
                log.skipped_internal.push(name.clone());
            }
            continue;
        }
        if !include_patterns.iter().any(|re| re.is_match(name)) {
            continue;
        }
        if exclude_patterns.iter().any(|re| re.is_match(name)) {
            log.excluded.push(name.clone());
            continue;
        }
        log.added.push(name.clone());
        matched.insert(name.clone());
    }
    (matched, log)
}

#[derive(Debug, Default)]
pub struct PollCounter {
    pub count: AtomicU64,
}
