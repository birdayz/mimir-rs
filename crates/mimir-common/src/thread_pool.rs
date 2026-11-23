//! Persistent scoped worker pool avoiding per-batch OS thread creation/teardown (`pthread_create` + `munmap`).
//!
//! Ported / engineered as internal execution infrastructure for scoped parallel CPU work.

use std::any::Any;
use std::cell::RefCell;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

#[cfg(not(miri))]
mod sync_impl {
    pub use parking_lot::{Condvar, Mutex, MutexGuard};

    #[inline]
    pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock()
    }

    #[inline]
    pub fn into_inner<T>(m: Mutex<T>) -> T {
        m.into_inner()
    }

    #[inline]
    pub fn wait<'a, T>(cv: &Condvar, mut guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        cv.wait(&mut guard);
        guard
    }
}

#[cfg(miri)]
mod sync_impl {
    pub use std::sync::{Condvar, Mutex, MutexGuard};

    #[inline]
    pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    pub fn into_inner<T>(m: Mutex<T>) -> T {
        m.into_inner().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    pub fn wait<'a, T>(cv: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        cv.wait(guard).unwrap_or_else(|e| e.into_inner())
    }
}

use sync_impl::{Condvar, Mutex};

thread_local! {
    static ACTIVE_POOLS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

struct TaskDescriptor {
    func: unsafe fn(*const (), usize),
    ctx: *const (),
    num_jobs: usize,
    workers_to_use: usize,
    claimed_workers: AtomicUsize,
    next_job: AtomicUsize,
    remaining_workers: AtomicUsize,
    panic: Mutex<Option<Box<dyn Any + Send>>>,
}

// SAFETY: `TaskDescriptor` is only accessed while the caller is blocked inside
// `scoped_execute` and `F: Sync`.
unsafe impl Send for TaskDescriptor {}
unsafe impl Sync for TaskDescriptor {}

struct PoolState {
    generation: u64,
    shutdown: bool,
    task: Option<*const TaskDescriptor>,
}

// SAFETY: `task` points to a stack-pinned `TaskDescriptor` valid for the duration of
// the generation until `remaining_workers` reaches 0 and `task` is cleared to `None`.
unsafe impl Send for PoolState {}

struct SharedPool {
    state: Mutex<PoolState>,
    work_cvar: Condvar,
    done_mu: Mutex<()>,
    done_cvar: Condvar,
}

/// A persistent pool of parked OS threads supporting safe scoped fork-join execution
/// without `std::thread::scope` thread creation or stack `mmap`/`munmap` TLB shootdowns.
pub struct ScopedWorkerPool {
    exec_lock: Mutex<()>,
    shared: Arc<SharedPool>,
    handles: Vec<JoinHandle<()>>,
    num_workers: usize,
}

impl ScopedWorkerPool {
    /// Spawns `num_workers` persistent worker threads named `<name_prefix>-<idx>`.
    pub fn new(name_prefix: &str, num_workers: usize) -> Self {
        let shared = Arc::new(SharedPool {
            state: Mutex::new(PoolState {
                generation: 0,
                shutdown: false,
                task: None,
            }),
            work_cvar: Condvar::new(),
            done_mu: Mutex::new(()),
            done_cvar: Condvar::new(),
        });

        let mut handles = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let shared_clone = Arc::clone(&shared);
            let name = format!("{name_prefix}-{worker_id}");
            let handle = thread::Builder::new()
                .name(name)
                .spawn(move || worker_loop(shared_clone))
                .expect("spawn persistent worker pool thread");
            handles.push(handle);
        }

        crate::record_scoped_worker_pool_threads_added(num_workers);
        Self {
            exec_lock: Mutex::new(()),
            shared,
            handles,
            num_workers,
        }
    }

    /// Returns the number of persistent background worker threads in this pool
    /// (excluding the calling thread, which also participates in `scoped_execute`).
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Executes `f(job_idx)` for every `job_idx in 0..num_jobs` across the calling thread
    /// and up to `min(num_jobs - 1, num_workers)` persistent worker threads.
    ///
    /// Blocks until all jobs complete (even if a panic occurs) before returning or
    /// propagating the first panic to the caller.
    pub fn scoped_execute<F>(&self, num_jobs: usize, f: F)
    where
        F: Fn(usize) + Sync,
    {
        if num_jobs == 0 {
            return;
        }
        let shared_addr = Arc::as_ptr(&self.shared) as usize;
        let is_reentrant = ACTIVE_POOLS.with(|p| p.borrow().contains(&shared_addr));
        if num_jobs == 1 || self.num_workers == 0 || is_reentrant {
            for idx in 0..num_jobs {
                f(idx);
            }
            return;
        }

        struct ActivePoolGuard {
            active: bool,
        }
        impl Drop for ActivePoolGuard {
            fn drop(&mut self) {
                if self.active {
                    ACTIVE_POOLS.with(|p| {
                        p.borrow_mut().pop();
                    });
                }
            }
        }

        unsafe fn invoke_fn<F: Fn(usize) + Sync>(ctx: *const (), idx: usize) {
            // SAFETY: `ctx` points to `f` on the caller's stack, alive until
            // `remaining_workers` reaches 0 before `scoped_execute` returns.
            let f = unsafe { &*(ctx as *const F) };
            f(idx);
        }

        let _exec_guard = sync_impl::lock(&self.exec_lock);
        ACTIVE_POOLS.with(|p| p.borrow_mut().push(shared_addr));
        let _active_guard = ActivePoolGuard { active: true };
        let workers_to_use = (num_jobs - 1).min(self.num_workers);
        let task = TaskDescriptor {
            func: invoke_fn::<F>,
            ctx: (&f as *const F).cast(),
            num_jobs,
            workers_to_use,
            claimed_workers: AtomicUsize::new(0),
            next_job: AtomicUsize::new(0),
            remaining_workers: AtomicUsize::new(workers_to_use),
            panic: Mutex::new(None),
        };

        struct WaitAndClearGuard<'a> {
            shared: &'a SharedPool,
            task: &'a TaskDescriptor,
        }

        impl Drop for WaitAndClearGuard<'_> {
            fn drop(&mut self) {
                if self.task.remaining_workers.load(Ordering::Acquire) != 0 {
                    let mut done_guard = sync_impl::lock(&self.shared.done_mu);
                    while self.task.remaining_workers.load(Ordering::Acquire) != 0 {
                        done_guard = sync_impl::wait(&self.shared.done_cvar, done_guard);
                    }
                }
                let mut st = sync_impl::lock(&self.shared.state);
                st.task = None;
            }
        }

        let wait_guard = WaitAndClearGuard {
            shared: &self.shared,
            task: &task,
        };

        crate::record_scoped_worker_pool_execution();
        {
            let mut st = sync_impl::lock(&self.shared.state);
            st.task = Some(&task as *const TaskDescriptor);
            st.generation = st.generation.wrapping_add(1);
        }
        // Wake all workers so non-FIFO condvars or spurious/in-flight wakeups
        // never strand a slot; extra workers see `claimed_workers >= workers_to_use`
        // and return to wait.
        self.shared.work_cvar.notify_all();

        let caller_res = catch_unwind(AssertUnwindSafe(|| loop {
            let idx = task.next_job.fetch_add(1, Ordering::Relaxed);
            if idx >= num_jobs {
                break;
            }
            f(idx);
        }));

        // Ensure all workers finish touching `task` and `f` before inspecting panics or returning.
        drop(wait_guard);

        if let Err(payload) = caller_res {
            resume_unwind(payload);
        }
        if let Some(payload) = sync_impl::into_inner(task.panic) {
            resume_unwind(payload);
        }
    }
}

impl Drop for ScopedWorkerPool {
    fn drop(&mut self) {
        {
            let mut st = sync_impl::lock(&self.shared.state);
            st.shutdown = true;
            st.generation = st.generation.wrapping_add(1);
        }
        self.shared.work_cvar.notify_all();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
        crate::record_scoped_worker_pool_threads_removed(self.num_workers);
    }
}

fn worker_loop(shared: Arc<SharedPool>) {
    ACTIVE_POOLS.with(|p| p.borrow_mut().push(Arc::as_ptr(&shared) as usize));
    let mut seen_gen = 0u64;
    loop {
        let task_ptr: *const TaskDescriptor = {
            let mut st = sync_impl::lock(&shared.state);
            loop {
                if st.shutdown {
                    return;
                }
                if st.generation != seen_gen {
                    seen_gen = st.generation;
                    if let Some(ptr) = st.task {
                        // SAFETY: caller holds `WaitAndClearGuard` keeping `*ptr` alive until
                        // `remaining_workers` reaches 0.
                        let task = unsafe { &*ptr };
                        if task.claimed_workers.fetch_add(1, Ordering::Relaxed)
                            < task.workers_to_use
                        {
                            break ptr;
                        }
                    }
                }
                st = sync_impl::wait(&shared.work_cvar, st);
            }
        };

        struct WorkerExit<'a> {
            task: *const TaskDescriptor,
            shared: &'a SharedPool,
        }
        impl Drop for WorkerExit<'_> {
            fn drop(&mut self) {
                // SAFETY: `task` remains valid until `remaining_workers` reaches 0;
                // we do not hold any `&TaskDescriptor` reference across or after `fetch_sub`.
                let prev = unsafe {
                    let ptr = std::ptr::addr_of!((*self.task).remaining_workers);
                    (&*ptr).fetch_sub(1, Ordering::AcqRel)
                };
                if prev == 1 {
                    let _g = sync_impl::lock(&self.shared.done_mu);
                    self.shared.done_cvar.notify_all();
                }
            }
        }
        let _exit = WorkerExit {
            task: task_ptr,
            shared: &shared,
        };

        let res = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: `_exit` has not decremented `remaining_workers` yet, so `task_ptr`
            // remains valid for the entire duration of this closure.
            let task = unsafe { &*task_ptr };
            loop {
                let idx = task.next_job.fetch_add(1, Ordering::Relaxed);
                if idx >= task.num_jobs {
                    break;
                }
                // SAFETY: `func` and `ctx` remain valid while `remaining_workers > 0`.
                unsafe { (task.func)(task.ctx, idx) };
            }
        }));
        if let Err(payload) = res {
            // SAFETY: `_exit` has not dropped yet, so `remaining_workers > 0`.
            let panic_mu = unsafe { &*std::ptr::addr_of!((*task_ptr).panic) };
            let mut p = sync_impl::lock(panic_mu);
            if p.is_none() {
                *p = Some(payload);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI64;

    #[test]
    fn scoped_execute_runs_all_jobs_and_reuses_threads() {
        #[derive(Clone, Copy)]
        struct SyncMutPtr(*mut usize);
        // SAFETY: callers only write to disjoint `idx` offsets.
        unsafe impl Send for SyncMutPtr {}
        unsafe impl Sync for SyncMutPtr {}

        let pool = ScopedWorkerPool::new("test-pool", 4);
        let mut results = [0usize; 64];
        for round in 0..20 {
            let ptr = SyncMutPtr(results.as_mut_ptr());
            let num_jobs = (round % 17) + 1;
            pool.scoped_execute(num_jobs, |idx| {
                // SAFETY: each `idx` is disjoint and `< num_jobs <= 64`.
                let base = ptr;
                unsafe {
                    *base.0.add(idx) = idx + round;
                }
            });
            for (idx, val) in results[..num_jobs].iter().enumerate() {
                assert_eq!(*val, idx + round);
            }
        }
    }

    #[test]
    fn scoped_execute_handles_zero_and_reentrancy() {
        let pool = ScopedWorkerPool::new("test-reentrant", 3);
        let sum = AtomicI64::new(0);
        pool.scoped_execute(0, |_| {
            sum.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(sum.load(Ordering::Relaxed), 0);

        pool.scoped_execute(6, |outer| {
            pool.scoped_execute(3, |inner| {
                sum.fetch_add((outer * 10 + inner + 1) as i64, Ordering::Relaxed);
            });
        });
        let expected: i64 = (0..6)
            .flat_map(|o| (0..3).map(move |i| (o * 10 + i + 1) as i64))
            .sum();
        assert_eq!(sum.load(Ordering::Relaxed), expected);
    }

    #[test]
    fn scoped_execute_propagates_worker_panic_after_joining_all_jobs() {
        let pool = ScopedWorkerPool::new("test-panic", 3);
        let completed = AtomicUsize::new(0);
        let res = catch_unwind(AssertUnwindSafe(|| {
            pool.scoped_execute(16, |idx| {
                if idx == 7 {
                    panic!("boom-7");
                }
                completed.fetch_add(1, Ordering::Relaxed);
            });
        }));
        assert!(res.is_err());
        // Pool remains usable after a panic.
        let ok_count = AtomicUsize::new(0);
        pool.scoped_execute(8, |_| {
            ok_count.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(ok_count.load(Ordering::Relaxed), 8);
    }
}
