//! ReplicationSet execution helpers: Do, DoUntilQuorum, DoMultiUntilQuorum, change detection.
//!
//! Ported from github.com/grafana/dskit/ring/replication_set.go

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use mimir_config::GoDuration;
use mimir_proto::ringpb::{InstanceDesc, InstanceState};
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};

use super::replication_set_tracker::{
    BoxDynError, CancelCauseFunc, CancellationError, ContextError, DefaultContextTracker,
    DefaultResultTracker, ReplicationSetContextTracker, ReplicationSetResultTracker, RingContext,
    TrackerLogger, ZoneAwareContextTracker, ZoneAwareResultTracker, ZoneSorter,
    InflightInstanceTracker, NoopTrackerLogger,
};
use super::replication_strategy::ReplicationSet;

struct AvailableReplicasKey;

#[derive(Debug, Clone)]
pub struct SimpleError(pub String);

impl std::fmt::Display for SimpleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SimpleError {}

pub fn context_with_available_replicas(ctx: &RingContext, available_replicas: usize) -> RingContext {
    ctx.with_value::<AvailableReplicasKey, usize>(available_replicas)
}

pub fn get_available_replicas(ctx: &RingContext) -> Option<usize> {
    ctx.value::<AvailableReplicasKey, usize>().map(|v| *v)
}

impl ReplicationSet {
    /// Returns whether the replication set includes the replica with the provided addr.
    pub fn includes(&self, addr: &str) -> bool {
        self.instances.iter().any(|i| i.addr == addr)
    }

    /// Returns the IDs of all instances within the replication set.
    pub fn get_ids(&self) -> Vec<String> {
        self.instances.iter().map(|i| i.id.clone()).collect()
    }

    /// Returns the addresses of all instances within the replication set.
    pub fn get_addresses(&self) -> Vec<String> {
        self.instances.iter().map(|i| i.addr.clone()).collect()
    }

    /// Returns the addresses of all instances within the replication set excluding `exclude`.
    pub fn get_addresses_without(&self, exclude: &str) -> Vec<String> {
        self.instances
            .iter()
            .filter(|i| i.addr != exclude)
            .map(|i| i.addr.clone())
            .collect()
    }

    /// Returns the number of unique zones represented by instances within this replication set.
    pub fn zone_count(&self) -> usize {
        let mut zones: Vec<&str> = Vec::new();
        for inst in &self.instances {
            if !zones.contains(&inst.zone.as_str()) {
                zones.push(inst.zone.as_str());
            }
        }
        zones.len()
    }

    /// Runs `f` in parallel for all replicas in the set, erroring if failures exceed
    /// `max_errors` (or `max_unavailable_zones`) and returning early once quorum succeeds.
    pub async fn do_with_delay<T, F, Fut>(
        &self,
        ctx: &RingContext,
        delay: Duration,
        f: F,
    ) -> Result<Vec<T>, BoxDynError>
    where
        T: Send + 'static,
        F: Fn(RingContext, InstanceDesc) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, BoxDynError>> + Send + 'static,
    {
        let tracker: Arc<dyn ReplicationSetResultTracker> = if self.max_unavailable_zones > 0 {
            Arc::new(ZoneAwareResultTracker::new(
                self.instances.clone(),
                self.max_unavailable_zones,
                None,
                None,
            ))
        } else {
            Arc::new(DefaultResultTracker::new(
                self.instances.clone(),
                self.max_errors,
                None,
            ))
        };

        let (tx, mut rx) = mpsc::channel::<(usize, Result<T, BoxDynError>)>(self.instances.len().max(1));
        let force_notify = Arc::new(Notify::new());
        let (child_ctx, cancel) = ctx.with_cancel();
        let f = Arc::new(f);

        struct CancelOnDrop(CancelCauseFunc);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                self.0.cancel(None);
            }
        }
        let _guard = CancelOnDrop(cancel);

        let num_instances = self.instances.len();
        let max_errors = self.max_errors;
        let max_unavail_zones = self.max_unavailable_zones;

        for (i, inst) in self.instances.iter().cloned().enumerate() {
            let ic = child_ctx.clone();
            let tx_c = tx.clone();
            let f_c = Arc::clone(&f);
            let fn_c = Arc::clone(&force_notify);
            tokio::spawn(async move {
                if !delay.is_zero()
                    && max_unavail_zones == 0
                    && i >= num_instances.saturating_sub(max_errors)
                {
                    tokio::select! {
                        biased;
                        _ = ic.done() => return,
                        _ = fn_c.notified() => {}
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                let res = f_c(ic, inst).await;
                let _ = tx_c.send((i, res)).await;
            });
        }
        drop(tx);

        let mut results = Vec::with_capacity(self.instances.len());
        while !tracker.succeeded() {
            tokio::select! {
                biased;
                _ = child_ctx.done() => {
                    return Err(child_ctx.cause().unwrap_or_else(|| Arc::new(ContextError::Canceled)));
                }
                maybe_res = rx.recv() => {
                    let Some((idx, res)) = maybe_res else {
                        break;
                    };
                    match res {
                        Ok(val) => {
                            tracker.done(Some(idx), None);
                            results.push(val);
                        }
                        Err(e) => {
                            tracker.done(Some(idx), Some(e.as_ref()));
                            if tracker.failed() {
                                return Err(e);
                            }
                            if !delay.is_zero() && max_unavail_zones == 0 {
                                force_notify.notify_one();
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

pub type TerminalErrorFn = Arc<dyn Fn(&dyn std::error::Error) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub struct DoUntilQuorumConfig {
    pub minimize_requests: bool,
    pub hedging_delay: GoDuration,
    pub logger: Option<Arc<dyn TrackerLogger>>,
    pub is_terminal_error: Option<TerminalErrorFn>,
    pub zone_sorter: Option<ZoneSorter>,
    pub include_replica_count: bool,
}

impl DoUntilQuorumConfig {
    pub fn validate(&self) -> Result<(), BoxDynError> {
        if self.hedging_delay.as_nanos() < 0 {
            return Err(Arc::new(SimpleError(
                "invalid DoUntilQuorumConfig: HedgingDelay must be non-negative".to_string(),
            )));
        }
        Ok(())
    }
}

pub async fn do_until_quorum<T, F, Fut, C>(
    ctx: &RingContext,
    r: &ReplicationSet,
    cfg: &DoUntilQuorumConfig,
    f: F,
    cleanup_func: C,
) -> Result<Vec<T>, BoxDynError>
where
    T: Send + 'static,
    F: Fn(RingContext, InstanceDesc) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, BoxDynError>> + Send + 'static,
    C: Fn(T) + Send + Sync + 'static,
{
    let (child_ctx, cancel) = ctx.with_cancel();
    struct CancelOnDrop(CancelCauseFunc);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.cancel(None);
        }
    }
    let _guard = CancelOnDrop(cancel);

    let f = Arc::new(f);
    let wrapped = move |c: RingContext, inst: InstanceDesc, _cancel: CancelCauseFunc| {
        let f_c = Arc::clone(&f);
        async move { f_c(c, inst).await }
    };

    do_until_quorum_without_successful_context_cancellation(&child_ctx, r, cfg, wrapped, cleanup_func)
        .await
}

pub async fn do_until_quorum_without_successful_context_cancellation<T, F, Fut, C>(
    ctx: &RingContext,
    r: &ReplicationSet,
    cfg: &DoUntilQuorumConfig,
    f: F,
    cleanup_func: C,
) -> Result<Vec<T>, BoxDynError>
where
    T: Send + 'static,
    F: Fn(RingContext, InstanceDesc, CancelCauseFunc) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, BoxDynError>> + Send + 'static,
    C: Fn(T) + Send + Sync + 'static,
{
    cfg.validate()?;

    if r.zone_awareness_enabled && r.max_errors > 0 {
        return Err(Arc::new(SimpleError(format!(
            "invalid ReplicationSet: MaxErrors is non-zero (is {}) and ZoneAwarenessEnabled is true",
            r.max_errors
        ))));
    }

    let logger: Arc<dyn TrackerLogger> = match &cfg.logger {
        Some(l) => Arc::clone(l),
        None => Arc::new(NoopTrackerLogger),
    };

    let ctx = if cfg.include_replica_count {
        context_with_available_replicas(ctx, r.instances.len())
    } else {
        ctx.clone()
    };

    let partition_id = r.partition_id;
    let log_with_pid = |level: &str, msg: &str, mut fields: Vec<(&str, String)>| {
        if partition_id != -1 {
            fields.push(("partition_id", partition_id.to_string()));
        }
        logger.log(level, msg, &fields);
    };

    let (results_tx, mut results_rx) =
        mpsc::channel::<(usize, Result<T, BoxDynError>)>(r.instances.len().max(1));
    let mut results_remaining = r.instances.len();
    let cleanup_arc = Arc::new(cleanup_func);

    let zone_aware = r.max_unavailable_zones > 0 || r.zone_awareness_enabled;
    let result_tracker: Arc<dyn ReplicationSetResultTracker> = if zone_aware {
        Arc::new(ZoneAwareResultTracker::new(
            r.instances.clone(),
            r.max_unavailable_zones,
            cfg.zone_sorter.clone(),
            Some(Arc::clone(&logger)),
        ))
    } else {
        Arc::new(DefaultResultTracker::new(
            r.instances.clone(),
            r.max_errors,
            Some(Arc::clone(&logger)),
        ))
    };

    let context_tracker: Arc<dyn ReplicationSetContextTracker> = if zone_aware {
        Arc::new(ZoneAwareContextTracker::new(
            ctx.clone(),
            r.instances.clone(),
        ))
    } else {
        Arc::new(DefaultContextTracker::new(ctx.clone(), r.instances.len()))
    };

    if cfg.minimize_requests {
        result_tracker.start_minimum_requests();
    } else {
        result_tracker.start_all_requests();
    }

    let f = Arc::new(f);
    for (idx, inst) in r.instances.iter().cloned().enumerate() {
        let (inst_ctx, inst_cancel) = context_tracker.context_for(idx);
        let rt = Arc::clone(&result_tracker);
        let tx = results_tx.clone();
        let f_c = Arc::clone(&f);
        tokio::spawn(async move {
            if let Err(err) = rt.await_start(&inst_ctx, idx).await {
                let _ = tx.send((idx, Err(err))).await;
                return;
            }
            let res = f_c(inst_ctx, inst, inst_cancel).await;
            let _ = tx.send((idx, res)).await;
        });
    }
    drop(results_tx);

    let mut results_map: HashMap<usize, T> = HashMap::with_capacity(r.instances.len());

    let spawn_drain = |mut rx: mpsc::Receiver<(usize, Result<T, BoxDynError>)>,
                       mut remaining: usize,
                       cleanup: Arc<C>| {
        if remaining > 0 {
            tokio::spawn(async move {
                while remaining > 0 {
                    match rx.recv().await {
                        Some((_, res)) => {
                            remaining -= 1;
                            if let Ok(val) = res {
                                cleanup(val);
                            }
                        }
                        None => break,
                    }
                }
            });
        }
    };

    let hedging_dur = cfg.hedging_delay.as_std().unwrap_or(Duration::ZERO);
    let mut hedging_interval = if !hedging_dur.is_zero() {
        let start = tokio::time::Instant::now() + hedging_dur;
        Some(tokio::time::interval_at(start, hedging_dur))
    } else {
        None
    };

    while !result_tracker.succeeded() {
        tokio::select! {
            biased;
            _ = ctx.done() => {
                let err = ctx.cause().unwrap_or_else(|| Arc::new(ContextError::Canceled));
                log_with_pid(
                    "debug",
                    "parent context done, returning",
                    vec![("err", err.to_string())],
                );
                for (_, val) in results_map.drain() {
                    cleanup_arc(val);
                }
                spawn_drain(results_rx, results_remaining, cleanup_arc);
                return Err(err);
            }
            _ = async {
                if let Some(ref mut iv) = hedging_interval {
                    iv.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                result_tracker.start_additional_requests();
            }
            maybe_res = results_rx.recv() => {
                let Some((idx, res)) = maybe_res else {
                    break;
                };
                results_remaining -= 1;

                match res {
                    Ok(val) => {
                        result_tracker.done(Some(idx), None);
                        results_map.insert(idx, val);
                    }
                    Err(err) => {
                        if let Some(ref is_term) = cfg.is_terminal_error {
                            if is_term(err.as_ref()) {
                                log_with_pid(
                                    "warn",
                                    "cancelling all outstanding requests because a terminal error occurred",
                                    vec![("err", err.to_string())],
                                );
                                let cause: BoxDynError = Arc::new(CancellationError::new("a terminal error occurred"));
                                context_tracker.cancel_all_contexts(Some(cause));
                                for (_, val) in results_map.drain() {
                                    cleanup_arc(val);
                                }
                                spawn_drain(results_rx, results_remaining, cleanup_arc);
                                return Err(err);
                            }
                        }

                        result_tracker.done(Some(idx), Some(err.as_ref()));
                        let cause: BoxDynError = Arc::new(CancellationError::new(format!(
                            "this instance returned an error: {err}"
                        )));
                        context_tracker.cancel_context_for(idx, Some(cause));

                        if result_tracker.failed() {
                            log_with_pid(
                                "error",
                                "cancelling all outstanding requests because quorum cannot be reached",
                                vec![],
                            );
                            let cause: BoxDynError = Arc::new(CancellationError::new("quorum cannot be reached"));
                            context_tracker.cancel_all_contexts(Some(cause));
                            for (_, val) in results_map.drain() {
                                cleanup_arc(val);
                            }
                            spawn_drain(results_rx, results_remaining, cleanup_arc);
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    log_with_pid("debug", "quorum reached", vec![]);

    let mut results = Vec::with_capacity(r.instances.len());
    for idx in 0..r.instances.len() {
        if let Some(val) = results_map.remove(&idx) {
            if result_tracker.should_include_result_from(idx) {
                results.push(val);
            } else {
                let cause: BoxDynError = Arc::new(CancellationError::new(
                    "quorum reached, result not required from this instance",
                ));
                context_tracker.cancel_context_for(idx, Some(cause));
                cleanup_arc(val);
            }
        } else {
            let cause: BoxDynError = Arc::new(CancellationError::new(
                "quorum reached, result not required from this instance",
            ));
            context_tracker.cancel_context_for(idx, Some(cause));
        }
    }

    spawn_drain(results_rx, results_remaining, cleanup_arc);
    Ok(results)
}

pub async fn do_multi_until_quorum_without_successful_context_cancellation<T, F, Fut, C>(
    ctx: &RingContext,
    sets: &[ReplicationSet],
    cfg: &DoUntilQuorumConfig,
    f: F,
    cleanup_func: C,
) -> Result<Vec<T>, BoxDynError>
where
    T: Send + 'static,
    F: Fn(RingContext, InstanceDesc, CancelCauseFunc) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, BoxDynError>> + Send + 'static,
    C: Fn(T) + Send + Sync + 'static,
{
    if sets.is_empty() {
        return Err(Arc::new(SimpleError("no replication sets".to_string())));
    }
    if sets.len() == 1 {
        return do_until_quorum_without_successful_context_cancellation(
            ctx,
            &sets[0],
            cfg,
            f,
            cleanup_func,
        )
        .await;
    }
    let (res, _workers_ctx) =
        do_multi_until_quorum_inner(ctx, sets, cfg, f, cleanup_func).await;
    res
}

pub async fn do_multi_until_quorum_inner<T, F, Fut, C>(
    ctx: &RingContext,
    sets: &[ReplicationSet],
    cfg: &DoUntilQuorumConfig,
    f: F,
    cleanup_func: C,
) -> (Result<Vec<T>, BoxDynError>, RingContext)
where
    T: Send + 'static,
    F: Fn(RingContext, InstanceDesc, CancelCauseFunc) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, BoxDynError>> + Send + 'static,
    C: Fn(T) + Send + Sync + 'static,
{
    let total_cap: usize = sets.iter().map(|s| s.instances.len()).sum();
    let return_results = Arc::new(Mutex::new(Vec::with_capacity(total_cap)));
    let return_err: Arc<OnceLock<BoxDynError>> = Arc::new(OnceLock::new());
    let (workers_ctx, cancel_workers_ctx) = ctx.with_cancel_cause();
    let inflight_tracker = Arc::new(InflightInstanceTracker::new(sets));
    let f = Arc::new(f);
    let cleanup = Arc::new(cleanup_func);

    let mut handles = Vec::with_capacity(sets.len());
    for (idx, set) in sets.iter().cloned().enumerate() {
        let wctx = workers_ctx.clone();
        let cfg_c = cfg.clone();
        let f_c = Arc::clone(&f);
        let cleanup_c = Arc::clone(&cleanup);
        let ret_res_c = Arc::clone(&return_results);
        let ret_err_c = Arc::clone(&return_err);
        let cancel_w_c = cancel_workers_ctx.clone();
        let inflight_c = Arc::clone(&inflight_tracker);

        handles.push(tokio::spawn(async move {
            let instance_idx_counter = Arc::new(Mutex::new(0usize));
            let _ = &instance_idx_counter;

            // Map each spawned instance by addr/index via per-instance index lookup.
            let set_instances = set.instances.clone();
            let wrapped_fn = {
                let f_inner = Arc::clone(&f_c);
                let inflight_inner = Arc::clone(&inflight_c);
                let cancel_w_inner = cancel_w_c.clone();
                let set_instances = set_instances.clone();
                move |ictx: RingContext, inst: InstanceDesc, cancel_ctx: CancelCauseFunc| {
                    let pos = set_instances
                        .iter()
                        .position(|d| d.addr == inst.addr && d.id == inst.id && d.zone == inst.zone)
                        .unwrap_or(0);
                    inflight_inner.add_instance(idx, pos);

                    let inflight_rm = Arc::clone(&inflight_inner);
                    let cancel_w_rm = cancel_w_inner.clone();
                    let cancel_orig = cancel_ctx.clone();
                    let f_call = Arc::clone(&f_inner);
                    async move {
                        let (hook_ctx, hook_cancel) = ictx.with_cancel_cause_hook(move |cause| {
                            cancel_orig.cancel(cause);
                            inflight_rm.remove_instance(idx, pos);
                            if inflight_rm.all_instances_completed() {
                                cancel_w_rm.cancel(Some(Arc::new(SimpleError(
                                    "all requests completed".to_string(),
                                ))));
                            }
                        });
                        f_call(hook_ctx, inst, hook_cancel).await
                    }
                }
            };

            match do_until_quorum_without_successful_context_cancellation(
                &wctx,
                &set,
                &cfg_c,
                wrapped_fn,
                move |v| cleanup_c(v),
            )
            .await
            {
                Ok(mut set_results) => {
                    ret_res_c.lock().append(&mut set_results);
                }
                Err(set_err) => {
                    if ret_err_c.set(Arc::clone(&set_err)).is_ok() {
                        cancel_w_c.cancel(Some(Arc::new(SimpleError(format!(
                            "request canceled because quorum was not reached in another replication set due to error: {set_err}"
                        )))));
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    if let Some(err) = return_err.get() {
        return (Err(Arc::clone(err)), workers_ctx);
    }

    inflight_tracker.all_instances_added();
    if inflight_tracker.all_instances_completed() {
        cancel_workers_ctx.cancel(Some(Arc::new(SimpleError(
            "all requests completed".to_string(),
        ))));
    }

    let results = std::mem::take(&mut *return_results.lock());
    (Ok(results), workers_ctx)
}

pub fn has_replication_set_changed(before: &ReplicationSet, after: &ReplicationSet) -> bool {
    has_replication_set_changed_excluding(before, after, |i| {
        i.timestamp = 0;
    })
}

pub fn has_replication_set_changed_without_state(
    before: &ReplicationSet,
    after: &ReplicationSet,
) -> bool {
    has_replication_set_changed_excluding(before, after, |i| {
        i.timestamp = 0;
        i.state = InstanceState::Pending as i32;
    })
}

pub fn has_replication_set_changed_without_state_or_addr(
    before: &ReplicationSet,
    after: &ReplicationSet,
) -> bool {
    has_replication_set_changed_excluding(before, after, |i| {
        i.timestamp = 0;
        i.state = InstanceState::Pending as i32;
        i.addr.clear();
    })
}

fn has_replication_set_changed_excluding(
    before: &ReplicationSet,
    after: &ReplicationSet,
    exclude: impl Fn(&mut InstanceDesc),
) -> bool {
    if before.instances.len() != after.instances.len() {
        return true;
    }

    let mut before_sorted = before.instances.clone();
    let mut after_sorted = after.instances.clone();
    before_sorted.sort_by(|a, b| a.id.cmp(&b.id));
    after_sorted.sort_by(|a, b| a.id.cmp(&b.id));

    for (mut b, mut a) in before_sorted.into_iter().zip(after_sorted) {
        exclude(&mut a);
        exclude(&mut b);
        if b != a {
            return true;
        }
    }
    false
}
