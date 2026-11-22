//! Ported from github.com/grafana/dskit/concurrency/{limited_concurrency_singleflight,runner,worker}.go.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify, Semaphore};

use crate::multierror::{BoxError, MultiError, NonNilMultiError};
use crate::services::ServiceContext;

// ---------------------------------------------------------------------------
// limited_concurrency_singleflight.go
// ---------------------------------------------------------------------------

/// Ensures that across all concurrent calls to `for_each_not_in_flight`, only up to
/// `max_concurrent` tokens are executing concurrently, and duplicate in-flight tokens are skipped.
pub struct LimitedConcurrencySingleFlight {
    inflight_tokens: Arc<Mutex<HashSet<String>>>,
    inflight_calls: AtomicUsize,
    calls_done_notify: Notify,
    semaphore: Arc<Semaphore>,
}

impl LimitedConcurrencySingleFlight {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            inflight_tokens: Arc::new(Mutex::new(HashSet::new())),
            inflight_calls: AtomicUsize::new(0),
            calls_done_notify: Notify::new(),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    /// Waits until there are no in-flight calls to `for_each_not_in_flight`.
    pub async fn wait(&self) {
        loop {
            if self.inflight_calls.load(Ordering::Acquire) == 0 {
                return;
            }
            let notified = self.calls_done_notify.notified();
            if self.inflight_calls.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Invokes `f` for every token in `tokens` not already in flight.
    pub async fn for_each_not_in_flight<F, Fut>(
        &self,
        ctx: &ServiceContext,
        tokens: &[String],
        f: F,
    ) -> Result<(), NonNilMultiError>
    where
        F: Fn(ServiceContext, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.inflight_calls.fetch_add(1, Ordering::AcqRel);
        struct CallGuard<'a>(&'a LimitedConcurrencySingleFlight);
        impl Drop for CallGuard<'_> {
            fn drop(&mut self) {
                if self.0.inflight_calls.fetch_sub(1, Ordering::AcqRel) == 1 {
                    self.0.calls_done_notify.notify_waiters();
                }
            }
        }
        let _guard = CallGuard(self);

        let tokens_not_inflight = self.tokens_not_in_flight(tokens);
        if tokens_not_inflight.is_empty() {
            return Ok(());
        }

        let errs = Arc::new(Mutex::new(MultiError::default()));
        let mut handles = Vec::with_capacity(tokens_not_inflight.len());
        let f = Arc::new(f);

        for token in tokens_not_inflight {
            let permit = tokio::select! {
                biased;
                _ = ctx.canceled() => {
                    self.token_processed(&token);
                    continue;
                }
                res = self.semaphore.clone().acquire_owned() => {
                    match res {
                        Ok(p) => p,
                        Err(_) => {
                            self.token_processed(&token);
                            continue;
                        }
                    }
                }
            };

            if ctx.is_canceled() {
                self.token_processed(&token);
                drop(permit);
                continue;
            }

            let ctx_c = ctx.clone();
            let f_c = Arc::clone(&f);
            let errs_c = Arc::clone(&errs);
            let inflight_c = Arc::clone(&self.inflight_tokens);
            let token_c = token.clone();
            let handle = tokio::spawn(async move {
                let res = f_c(ctx_c, token_c.clone()).await;
                if let Err(err) = res {
                    errs_c.lock().add(err);
                }
                inflight_c.lock().remove(&token_c);
                drop(permit);
            });
            handles.push(handle);
        }

        for h in handles {
            let _ = h.await;
        }

        let maybe_err = errs.lock().clone().into_err();
        match maybe_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn tokens_not_in_flight(&self, tokens: &[String]) -> Vec<String> {
        let mut not_inflight = Vec::with_capacity(tokens.len());
        let mut guard = self.inflight_tokens.lock();
        for t in tokens {
            if guard.contains(t) {
                continue;
            }
            guard.insert(t.clone());
            not_inflight.push(t.clone());
        }
        not_inflight
    }

    fn token_processed(&self, token: &str) {
        self.inflight_tokens.lock().remove(token);
    }
}

// ---------------------------------------------------------------------------
// runner.go
// ---------------------------------------------------------------------------

/// Runs `user_fn` for each user ID up to `concurrency` workers, continuing on errors and
/// returning all errors combined.
pub async fn for_each_user<F, Fut>(
    ctx: &ServiceContext,
    user_ids: &[String],
    concurrency: usize,
    user_fn: F,
) -> Result<(), BoxError>
where
    F: Fn(ServiceContext, String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    if user_ids.is_empty() {
        return Ok(());
    }
    let workers = concurrency.max(1).min(user_ids.len());
    let idx = Arc::new(AtomicUsize::new(0));
    let users: Arc<Vec<String>> = Arc::new(user_ids.to_vec());
    let errs = Arc::new(Mutex::new(MultiError::default()));
    let user_fn = Arc::new(user_fn);

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let ctx_c = ctx.clone();
        let idx_c = Arc::clone(&idx);
        let users_c = Arc::clone(&users);
        let errs_c = Arc::clone(&errs);
        let fn_c = Arc::clone(&user_fn);
        handles.push(tokio::spawn(async move {
            loop {
                if ctx_c.is_canceled() {
                    break;
                }
                let i = idx_c.fetch_add(1, Ordering::AcqRel);
                if i >= users_c.len() {
                    break;
                }
                if let Err(err) = fn_c(ctx_c.clone(), users_c[i].clone()).await {
                    errs_c.lock().add(err);
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    if let Some(ctx_err) = ctx.err() {
        return Err(Arc::new(ctx_err));
    }
    let maybe_err = errs.lock().clone().into_err();
    match maybe_err {
        Some(e) => Err(Arc::new(e)),
        None => Ok(()),
    }
}

/// Runs `job_fn` for each job index in `0..jobs` up to `concurrency` workers.
/// If `concurrency == 0`, runs all jobs concurrently.
/// Breaks on first error encountered and cancels the `ServiceContext` passed to callbacks before returning.
pub async fn for_each_job<F, Fut>(
    ctx: &ServiceContext,
    jobs: usize,
    concurrency: usize,
    job_fn: F,
) -> Result<(), BoxError>
where
    F: Fn(ServiceContext, usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    if jobs == 0 {
        return Ok(());
    }
    if jobs == 1 {
        let job_ctx = ctx.child();
        let res = job_fn(job_ctx.clone(), 0).await;
        job_ctx.cancel();
        return res;
    }

    let concurrency = if concurrency == 0 { jobs } else { concurrency };
    let workers = concurrency.min(jobs);
    let group_ctx = ctx.child();
    let next_idx = Arc::new(AtomicUsize::new(0));
    let first_err: Arc<Mutex<Option<BoxError>>> = Arc::new(Mutex::new(None));
    let job_fn = Arc::new(job_fn);

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let gctx = group_ctx.clone();
        let idx_c = Arc::clone(&next_idx);
        let err_c = Arc::clone(&first_err);
        let fn_c = Arc::clone(&job_fn);
        handles.push(tokio::spawn(async move {
            while !gctx.is_canceled() {
                let i = idx_c.fetch_add(1, Ordering::AcqRel);
                if i >= jobs {
                    return;
                }
                if let Err(e) = fn_c(gctx.clone(), i).await {
                    let mut guard = err_c.lock();
                    if guard.is_none() {
                        *guard = Some(e);
                    }
                    gctx.cancel();
                    return;
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    group_ctx.cancel();

    let maybe_err = first_err.lock().take();
    match maybe_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Deprecated alias in Go (`ForEach` over a slice of jobs).
pub async fn for_each<J, F, Fut>(
    ctx: &ServiceContext,
    jobs: &[J],
    concurrency: usize,
    job_fn: F,
) -> Result<(), BoxError>
where
    J: Clone + Send + Sync + 'static,
    F: Fn(ServiceContext, J) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    let jobs_arc = Arc::new(jobs.to_vec());
    let fn_arc = Arc::new(job_fn);
    for_each_job(ctx, jobs_arc.len(), concurrency, move |c, idx| {
        let jobs_c = Arc::clone(&jobs_arc);
        let fn_c = Arc::clone(&fn_arc);
        async move { fn_c(c, jobs_c[idx].clone()).await }
    })
    .await
}

pub fn create_jobs_from_strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| (*s).to_string()).collect()
}

/// Runs `job_fn` concurrently for each job and merges all returned `Vec<R>` results.
pub async fn for_each_job_merge_results<J, R, F, Fut>(
    ctx: &ServiceContext,
    jobs: &[J],
    concurrency: usize,
    job_fn: F,
) -> Result<Vec<R>, BoxError>
where
    J: Clone + Send + Sync + 'static,
    R: Send + 'static,
    F: Fn(ServiceContext, J) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<R>, BoxError>> + Send,
{
    let results = Arc::new(Mutex::new(Vec::with_capacity(jobs.len())));
    let jobs_arc = Arc::new(jobs.to_vec());
    let fn_arc = Arc::new(job_fn);
    let results_c = Arc::clone(&results);

    for_each_job(ctx, jobs_arc.len(), concurrency, move |c, idx| {
        let jobs_c = Arc::clone(&jobs_arc);
        let fn_c = Arc::clone(&fn_arc);
        let res_c = Arc::clone(&results_c);
        async move {
            let mut job_res = fn_c(c, jobs_c[idx].clone()).await?;
            res_c.lock().append(&mut job_res);
            Ok(())
        }
    })
    .await?;

    let mut guard = results.lock();
    Ok(std::mem::take(&mut *guard))
}

// ---------------------------------------------------------------------------
// worker.go
// ---------------------------------------------------------------------------

type JobFn = Box<dyn FnOnce() + Send + 'static>;

/// Reusable worker pool matching `dskit/concurrency.ReusableGoroutinesPool`.
/// Workers are created lazily on demand up to `size`. If all workers are busy,
/// `go` spawns an overflow task to run the workload immediately.
pub struct ReusableGoroutinesPool {
    idle_waiters: Arc<Mutex<VecDeque<oneshot::Sender<JobFn>>>>,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
    pending: AtomicI64,
    active_workers: Arc<AtomicUsize>,
}

impl ReusableGoroutinesPool {
    pub fn new(size: usize) -> Self {
        Self {
            idle_waiters: Arc::new(Mutex::new(VecDeque::with_capacity(size))),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
            pending: AtomicI64::new(size as i64),
            active_workers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn active_worker_count(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
    }

    pub fn go<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut job: JobFn = Box::new(f);
        loop {
            let waiter = self.idle_waiters.lock().pop_front();
            match waiter {
                Some(tx) => match tx.send(job) {
                    Ok(()) => return,
                    Err(returned) => job = returned,
                },
                None => break,
            }
        }
        self.spawn_or_overflow(job);
    }

    fn spawn_or_overflow(&self, job: JobFn) {
        let prev = self.pending.fetch_sub(1, Ordering::AcqRel);
        if prev > 0 {
            self.new_worker(job);
            return;
        } else if prev < i64::MIN / 2 {
            self.pending.store(0, Ordering::Release);
        }
        let active = Arc::clone(&self.active_workers);
        active.fetch_add(1, Ordering::AcqRel);
        tokio::task::spawn_blocking(move || {
            job();
            active.fetch_sub(1, Ordering::AcqRel);
        });
    }

    fn new_worker(&self, first_job: JobFn) {
        let waiters = Arc::clone(&self.idle_waiters);
        let closed = Arc::clone(&self.closed);
        let notify = Arc::clone(&self.close_notify);
        let active = Arc::clone(&self.active_workers);
        active.fetch_add(1, Ordering::AcqRel);

        tokio::spawn(async move {
            tokio::task::spawn_blocking(first_job).await.ok();
            loop {
                if closed.load(Ordering::Acquire) {
                    active.fetch_sub(1, Ordering::AcqRel);
                    return;
                }
                let (tx, rx) = oneshot::channel();
                {
                    let mut guard = waiters.lock();
                    if closed.load(Ordering::Acquire) {
                        active.fetch_sub(1, Ordering::AcqRel);
                        return;
                    }
                    guard.push_back(tx);
                }
                let next = tokio::select! {
                    biased;
                    _ = notify.notified() => None,
                    res = rx => res.ok(),
                };
                match next {
                    Some(job) => {
                        tokio::task::spawn_blocking(job).await.ok();
                    }
                    None => {
                        active.fetch_sub(1, Ordering::AcqRel);
                        return;
                    }
                }
            }
        });
    }

    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.idle_waiters.lock().clear();
            self.close_notify.notify_waiters();
        }
    }
}
