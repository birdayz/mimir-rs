//! Ingester ring lifecycler interface and tokenless lifecycler implementation.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_ring_lifecycler.go

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mimir_common::services::ManagedBasicService;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::{BoxFuture, KvClient};
use mimir_ring::lifecycler::{
    BasicLifecycler, BasicLifecyclerConfig, BasicLifecyclerDelegate, LeaveOnStoppingDelegate,
    Lifecycler, LifecyclerError,
};
use parking_lot::Mutex;
use prometheus::Registry;
use tracing::info;

pub type FlushFn = Arc<dyn Fn() + Send + Sync>;

/// Interface satisfied by both [`Lifecycler`] and [`TokenlessLifecycler`]
/// (`pkg/ingester/ingester_ring_lifecycler.go:ingesterLifecycler`).
pub trait IngesterLifecycler: Send + Sync {
    fn get_state(&self) -> InstanceState;
    fn get_read_only_state(&self) -> (bool, Option<SystemTime>);
    fn change_read_only_state(&self, read_only: bool) -> BoxFuture<'_, Result<(), LifecyclerError>>;
    fn check_ready(&self) -> BoxFuture<'_, Result<(), String>>;
    fn set_unregister_on_shutdown(&self, enabled: bool);
    fn should_unregister_on_shutdown(&self) -> bool;
    fn set_flush_on_shutdown(&self, enabled: bool);
    fn flush_on_shutdown(&self) -> bool;
    fn into_managed_service(self: Arc<Self>) -> Arc<ManagedBasicService>;
}

impl IngesterLifecycler for Lifecycler {
    fn get_state(&self) -> InstanceState {
        self.get_state()
    }

    fn get_read_only_state(&self) -> (bool, Option<SystemTime>) {
        self.get_read_only_state()
    }

    fn change_read_only_state(&self, read_only: bool) -> BoxFuture<'_, Result<(), LifecyclerError>> {
        Box::pin(Lifecycler::change_read_only_state(self, read_only))
    }

    fn check_ready(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(Lifecycler::check_ready(self))
    }

    fn set_unregister_on_shutdown(&self, enabled: bool) {
        self.set_unregister_on_shutdown(enabled);
    }

    fn should_unregister_on_shutdown(&self) -> bool {
        self.should_unregister_on_shutdown()
    }

    fn set_flush_on_shutdown(&self, enabled: bool) {
        self.set_flush_on_shutdown(enabled);
    }

    fn flush_on_shutdown(&self) -> bool {
        self.flush_on_shutdown()
    }

    fn into_managed_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        Lifecycler::into_service(self)
    }
}

struct TokenlessReadyState {
    ready: bool,
    ready_since: Option<Instant>,
}

struct TokenlessDelegateInner {
    final_sleep: Duration,
    flush: Option<FlushFn>,
    flush_on_shutdown: Arc<AtomicBool>,
}

impl BasicLifecyclerDelegate for TokenlessDelegateInner {
    fn on_ring_instance_register(
        &self,
        _lifecycler: &BasicLifecycler,
        _ring_desc: &Desc,
        instance_exists: bool,
        instance_id: &str,
        _instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>) {
        info!(
            instance = instance_id,
            exists = instance_exists,
            "registering tokenless ingester instance"
        );
        (InstanceState::Active, Vec::new())
    }

    fn on_ring_instance_tokens(&self, _lifecycler: &BasicLifecycler, _tokens: &[u32]) {}

    fn on_ring_instance_stopping<'a>(
        &'a self,
        _lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if self.flush_on_shutdown.load(Ordering::Acquire) {
                if let Some(ref flush) = self.flush {
                    info!("flushing blocks before shutdown");
                    flush();
                }
            }
            if !self.final_sleep.is_zero() {
                info!(
                    final_sleep = %mimir_config::format_go_duration(
                        self.final_sleep.as_nanos().min(i64::MAX as u128) as i64
                    ),
                    "lifecycler entering final sleep before shutdown"
                );
                tokio::time::sleep(self.final_sleep).await;
            }
        })
    }

    fn on_ring_instance_heartbeat(
        &self,
        _lifecycler: &BasicLifecycler,
        _ring_desc: &mut Desc,
        _instance_desc: &mut InstanceDesc,
    ) {
    }
}

/// Tokenless lifecycler wrapping [`BasicLifecycler`] with [`LeaveOnStoppingDelegate`]
/// (`pkg/ingester/ingester_ring_lifecycler.go:tokenlessLifecycler`).
pub struct TokenlessLifecycler {
    basic: Arc<BasicLifecycler>,
    min_ready_duration: Duration,
    flush_on_shutdown: Arc<AtomicBool>,
    ready_mu: Mutex<TokenlessReadyState>,
}

impl TokenlessLifecycler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: BasicLifecyclerConfig,
        ring_name: &str,
        ring_key: &str,
        ring_kv: Arc<dyn KvClient>,
        min_ready_duration: Duration,
        final_sleep: Duration,
        flush_on_shutdown: bool,
        flush: Option<FlushFn>,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        let flush_flag = Arc::new(AtomicBool::new(flush_on_shutdown));
        let inner = Arc::new(TokenlessDelegateInner {
            final_sleep,
            flush,
            flush_on_shutdown: Arc::clone(&flush_flag),
        });
        let delegate: Arc<dyn BasicLifecyclerDelegate> =
            Arc::new(LeaveOnStoppingDelegate::new(inner));
        let basic = BasicLifecycler::new(cfg, ring_name, ring_key, ring_kv, delegate, reg);

        Arc::new(Self {
            basic,
            min_ready_duration,
            flush_on_shutdown: flush_flag,
            ready_mu: Mutex::new(TokenlessReadyState {
                ready: false,
                ready_since: None,
            }),
        })
    }

    pub fn basic(&self) -> &Arc<BasicLifecycler> {
        &self.basic
    }

    fn check_readiness(&self) -> Result<(), String> {
        if !self.basic.is_registered() {
            return Err("instance not registered in the ring".to_string());
        }
        let state = self.basic.get_state();
        if state != InstanceState::Active {
            return Err(format!(
                "instance not in ACTIVE state, current state: {:?}",
                state
            ));
        }
        Ok(())
    }

    /// Synchronous readiness check (useful in tests with deterministic timing).
    pub fn check_ready_sync(&self, now: Instant) -> Result<(), String> {
        let mut ready = self.ready_mu.lock();
        if ready.ready {
            return Ok(());
        }
        if let Err(err) = self.check_readiness() {
            ready.ready_since = None;
            return Err(err);
        }
        let since = *ready.ready_since.get_or_insert(now);
        if now.saturating_duration_since(since) < self.min_ready_duration {
            return Err(format!(
                "waiting for {:?} after being ready",
                self.min_ready_duration
            ));
        }
        ready.ready = true;
        Ok(())
    }
}

impl IngesterLifecycler for TokenlessLifecycler {
    fn get_state(&self) -> InstanceState {
        self.basic.get_state()
    }

    fn get_read_only_state(&self) -> (bool, Option<SystemTime>) {
        self.basic.get_read_only_state()
    }

    fn change_read_only_state(&self, read_only: bool) -> BoxFuture<'_, Result<(), LifecyclerError>> {
        Box::pin(self.basic.change_read_only_state(read_only))
    }

    fn check_ready(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move { self.check_ready_sync(Instant::now()) })
    }

    fn set_unregister_on_shutdown(&self, enabled: bool) {
        self.basic
            .set_keep_instance_in_the_ring_on_shutdown(!enabled);
    }

    fn should_unregister_on_shutdown(&self) -> bool {
        !self.basic.should_keep_instance_in_the_ring_on_shutdown()
    }

    fn set_flush_on_shutdown(&self, enabled: bool) {
        self.flush_on_shutdown.store(enabled, Ordering::Release);
    }

    fn flush_on_shutdown(&self) -> bool {
        self.flush_on_shutdown.load(Ordering::Acquire)
    }

    fn into_managed_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        Arc::clone(&self.basic).into_service()
    }
}
