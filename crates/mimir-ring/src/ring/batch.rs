//! Quorum-tracked batch RPC execution against keys in the ring.
//!
//! Ported from github.com/grafana/dskit/ring/batch.go

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use mimir_proto::grpcutil::error_to_status_code;
use mimir_proto::ringpb::InstanceDesc;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::replication_set::SimpleError;
use super::replication_set_tracker::{BoxDynError, ContextError, RingContext};
use super::replication_strategy::{Operation, ReplicationSet};
use super::{Ring, RingError};

pub trait DoBatchRing: Send + Sync {
    fn get_replication_set(&self, key: u32, op: Operation) -> Result<ReplicationSet, RingError>;
    fn replication_factor(&self) -> usize;
    fn instances_count(&self) -> usize;
}

impl DoBatchRing for Ring {
    fn get_replication_set(&self, key: u32, op: Operation) -> Result<ReplicationSet, RingError> {
        self.get(key, op)
    }

    fn replication_factor(&self) -> usize {
        self.replication_factor()
    }

    fn instances_count(&self) -> usize {
        self.instances_count()
    }
}

pub type IsClientErrorFn =
    Arc<dyn Fn(&(dyn std::error::Error + 'static)) -> bool + Send + Sync>;
pub type CleanupFn = Arc<dyn Fn() + Send + Sync>;

pub fn is_http_status_4xx(err: &(dyn std::error::Error + 'static)) -> bool {
    error_to_status_code(Some(err)) / 100 == 4
}

pub struct ItemTracker {
    pub min_success: i32,
    pub max_failures: i32,
    pub succeeded: AtomicI32,
    pub failed_client: AtomicI32,
    pub failed_server: AtomicI32,
    pub remaining: AtomicI32,
    err: Mutex<Option<BoxDynError>>,
}

impl ItemTracker {
    pub fn new(min_success: i32, max_failures: i32, remaining: i32) -> Self {
        Self {
            min_success,
            max_failures,
            succeeded: AtomicI32::new(0),
            failed_client: AtomicI32::new(0),
            failed_server: AtomicI32::new(0),
            remaining: AtomicI32::new(remaining),
            err: Mutex::new(None),
        }
    }

    pub fn record_error(
        &self,
        err: BoxDynError,
        is_client_error: &dyn Fn(&(dyn std::error::Error + 'static)) -> bool,
    ) -> i32 {
        let is_client = is_client_error(err.as_ref());
        *self.err.lock() = Some(err);
        if is_client {
            self.failed_client.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            self.failed_server.fetch_add(1, Ordering::AcqRel) + 1
        }
    }

    pub fn load_err(&self) -> Option<BoxDynError> {
        self.err.lock().clone()
    }
}

pub struct BatchTracker {
    rpcs_pending: AtomicI32,
    rpcs_failed: AtomicI32,
    done_tx: Mutex<Option<oneshot::Sender<()>>>,
    err_tx: Mutex<Option<oneshot::Sender<BoxDynError>>>,
}

impl BatchTracker {
    pub fn new(num_items: i32) -> (Arc<Self>, oneshot::Receiver<()>, oneshot::Receiver<BoxDynError>) {
        let (done_tx, done_rx) = oneshot::channel();
        let (err_tx, err_rx) = oneshot::channel();
        (
            Arc::new(Self {
                rpcs_pending: AtomicI32::new(num_items),
                rpcs_failed: AtomicI32::new(0),
                done_tx: Mutex::new(Some(done_tx)),
                err_tx: Mutex::new(Some(err_tx)),
            }),
            done_rx,
            err_rx,
        )
    }

    pub fn record(
        &self,
        item_trackers: &[Arc<ItemTracker>],
        err: Option<BoxDynError>,
        is_client_error: &dyn Fn(&(dyn std::error::Error + 'static)) -> bool,
    ) {
        for it in item_trackers {
            if let Some(ref e) = err {
                let err_count = it.record_error(Arc::clone(e), is_client_error);
                if (err_count > it.max_failures
                    || it.remaining.fetch_sub(1, Ordering::AcqRel) - 1 == 0)
                    && self.rpcs_failed.fetch_add(1, Ordering::AcqRel) + 1 == 1
                {
                    if let Some(tx) = self.err_tx.lock().take() {
                        let _ = tx.send(Arc::clone(e));
                    }
                }
            } else {
                let succeeded = it.succeeded.fetch_add(1, Ordering::AcqRel) + 1;
                if succeeded == it.min_success {
                    if self.rpcs_pending.fetch_sub(1, Ordering::AcqRel) - 1 == 0 {
                        if let Some(tx) = self.done_tx.lock().take() {
                            let _ = tx.send(());
                        }
                    }
                    continue;
                }
                if succeeded < it.min_success
                    && it.remaining.fetch_sub(1, Ordering::AcqRel) - 1 == 0
                    && self.rpcs_failed.fetch_add(1, Ordering::AcqRel) + 1 == 1
                {
                    if let Some(tx) = self.err_tx.lock().take() {
                        if let Some(stored) = it.load_err() {
                            let _ = tx.send(stored);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct DoBatchOptions {
    pub cleanup: Option<CleanupFn>,
    pub is_client_error: Option<IsClientErrorFn>,
}

pub async fn do_batch<F, Fut>(
    ctx: &RingContext,
    op: Operation,
    r: &dyn DoBatchRing,
    keys: &[u32],
    callback: F,
    cleanup: Option<CleanupFn>,
) -> Result<(), BoxDynError>
where
    F: Fn(InstanceDesc, Vec<usize>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxDynError>> + Send + 'static,
{
    do_batch_with_options(
        ctx,
        op,
        r,
        keys,
        callback,
        DoBatchOptions {
            cleanup,
            is_client_error: Some(Arc::new(is_http_status_4xx)),
        },
    )
    .await
}

pub async fn do_batch_with_options<F, Fut>(
    ctx: &RingContext,
    op: Operation,
    r: &dyn DoBatchRing,
    keys: &[u32],
    callback: F,
    opts: DoBatchOptions,
) -> Result<(), BoxDynError>
where
    F: Fn(InstanceDesc, Vec<usize>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxDynError>> + Send + 'static,
{
    let cleanup: CleanupFn = opts.cleanup.unwrap_or_else(|| Arc::new(|| {}));
    let is_client_error: IsClientErrorFn = opts
        .is_client_error
        .unwrap_or_else(|| Arc::new(is_http_status_4xx));

    let instances_count = r.instances_count();
    if instances_count == 0 {
        cleanup();
        return Err(Arc::new(SimpleError(
            "DoBatch: InstancesCount <= 0".to_string(),
        )));
    }

    let expected_per_inst = keys
        .len()
        .saturating_mul(r.replication_factor().saturating_add(1))
        / instances_count;
    let mut item_trackers: Vec<Arc<ItemTracker>> = Vec::with_capacity(keys.len());

    struct BatchInstance {
        desc: InstanceDesc,
        item_trackers: Vec<Arc<ItemTracker>>,
        indexes: Vec<usize>,
    }
    let mut instances: HashMap<String, BatchInstance> = HashMap::with_capacity(instances_count);

    for (i, &key) in keys.iter().enumerate() {
        if i % 10_000 == 0 {
            if let Some(cause) = ctx.cause() {
                cleanup();
                return Err(cause);
            }
        }

        let rs = match r.get_replication_set(key, op) {
            Ok(rs) => rs,
            Err(e) => {
                cleanup();
                return Err(Arc::new(e));
            }
        };

        let min_success = rs.instances.len().saturating_sub(rs.max_errors) as i32;
        let max_failures = rs.max_errors as i32;
        let it = Arc::new(ItemTracker::new(
            min_success,
            max_failures,
            rs.instances.len() as i32,
        ));
        for desc in rs.instances {
            let entry = instances.entry(desc.addr.clone()).or_insert_with(|| BatchInstance {
                desc,
                item_trackers: Vec::with_capacity(expected_per_inst),
                indexes: Vec::with_capacity(expected_per_inst),
            });
            entry.item_trackers.push(Arc::clone(&it));
            entry.indexes.push(i);
        }
        item_trackers.push(it);
    }

    if let Some(cause) = ctx.cause() {
        cleanup();
        return Err(cause);
    }

    let (tracker, done_rx, err_rx) = BatchTracker::new(item_trackers.len() as i32);
    let callback = Arc::new(callback);
    let mut handles = Vec::with_capacity(instances.len());

    for (_, inst) in instances {
        let cb = Arc::clone(&callback);
        let tr = Arc::clone(&tracker);
        let ice = Arc::clone(&is_client_error);
        handles.push(tokio::spawn(async move {
            let res = cb(inst.desc, inst.indexes).await;
            tr.record(&inst.item_trackers, res.err(), ice.as_ref());
        }));
    }

    tokio::spawn(async move {
        for h in handles {
            let _ = h.await;
        }
        cleanup();
    });

    tokio::select! {
        biased;
        res = err_rx => {
            match res {
                Ok(e) => Err(e),
                Err(_) => Err(Arc::new(SimpleError("batch error channel closed".to_string()))),
            }
        }
        _ = done_rx => Ok(()),
        _ = ctx.done() => {
            Err(ctx.cause().unwrap_or_else(|| Arc::new(ContextError::Canceled)))
        }
    }
}
