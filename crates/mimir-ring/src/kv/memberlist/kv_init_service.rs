//! Lazy-initializing memberlist KV service wrapper.
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/kv_init_service.go

use std::sync::{Arc, OnceLock};

use mimir_common::services::{
    basic_service, FnListener, ManagedBasicService, Service, ServiceContext, ServiceError,
};
use mimir_config::server::MemberlistConfig;
use prometheus::Registry;
use tokio::sync::{watch, Mutex};

use super::client::{KvOptions, MemberlistKv};
use crate::dns::DnsProvider;
use crate::kv::{Codec, KvError};

/// Port of `dskit/kv/memberlist.KVInitService`.
pub struct KvInitService {
    cfg: MemberlistConfig,
    codecs: Vec<Arc<dyn Codec>>,
    dns_provider: Option<Arc<dyn DnsProvider>>,
    registry: Option<Registry>,
    init: OnceLock<Result<Arc<MemberlistKv>, String>>,
    init_lock: Mutex<()>,
    failure_tx: watch::Sender<Option<String>>,
    failure_rx: watch::Receiver<Option<String>>,
    service: OnceLock<Arc<ManagedBasicService>>,
}

impl KvInitService {
    pub fn new(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        dns_provider: Option<Arc<dyn DnsProvider>>,
        registry: Option<Registry>,
    ) -> Arc<Self> {
        let (failure_tx, failure_rx) = watch::channel(None);
        let this = Arc::new(Self {
            cfg,
            codecs,
            dns_provider,
            registry,
            init: OnceLock::new(),
            init_lock: Mutex::new(()),
            failure_tx,
            failure_rx,
            service: OnceLock::new(),
        });

        let w = Arc::downgrade(&this);
        let w_stop = Arc::downgrade(&this);
        let svc = basic_service(
            None,
            Some(Box::new(move |ctx: ServiceContext| {
                let w = w.clone();
                Box::pin(async move {
                    let Some(s) = w.upgrade() else {
                        ctx.canceled().await;
                        return Ok(());
                    };
                    s.running(ctx).await
                })
            })),
            Some(Box::new(move |_| {
                let w = w_stop.clone();
                Box::pin(async move {
                    if let Some(s) = w.upgrade() {
                        s.stopping().await?;
                    }
                    Ok(())
                })
            })),
        )
        .with_name("memberlist KV service");
        let _ = this.service.set(svc);
        this
    }

    pub fn service(&self) -> Arc<ManagedBasicService> {
        Arc::clone(self.service.get().expect("service initialized in new()"))
    }

    async fn running(&self, ctx: ServiceContext) -> Result<(), ServiceError> {
        let mut rx = self.failure_rx.clone();
        loop {
            if ctx.is_canceled() {
                return Ok(());
            }
            if let Some(err) = rx.borrow_and_update().clone() {
                return Err(ServiceError::failure(err));
            }
            tokio::select! {
                biased;
                _ = ctx.canceled() => return Ok(()),
                res = rx.changed() => {
                    if res.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn get_memberlist_kv(&self) -> Result<Arc<MemberlistKv>, KvError> {
        if self.cfg.watch_prefix_buffer_size <= 0 {
            return Err(KvError::InvalidConfig(
                "invalid WatchPrefixBufferSize: must be greater than 0".to_string(),
            ));
        }

        if let Some(res) = self.init.get() {
            return res.clone().map_err(KvError::Other);
        }

        let _guard = self.init_lock.lock().await;
        if let Some(res) = self.init.get() {
            return res.clone().map_err(KvError::Other);
        }

        let opts = KvOptions {
            dns_provider: self.dns_provider.clone(),
            ..Default::default()
        };
        let kv = match MemberlistKv::new(
            self.cfg.clone(),
            self.codecs.clone(),
            "",
            self.registry.as_ref(),
            opts,
        ) {
            Ok(kv) => kv,
            Err(e) => {
                let msg = e.to_string();
                let _ = self.init.set(Err(msg.clone()));
                return Err(KvError::Other(msg));
            }
        };

        let tx = self.failure_tx.clone();
        let listener = FnListener::new(
            None,
            None,
            None,
            None,
            Some(Box::new(move |_from, failure| {
                let msg = failure.to_string();
                tx.send_if_modified(|cur| {
                    if cur.is_none() {
                        *cur = Some(msg);
                        true
                    } else {
                        false
                    }
                });
            })),
        );
        let _ = kv.service().add_listener(listener);

        let _ = self.init.set(Ok(Arc::clone(&kv)));
        if let Err(e) = kv.service().start_async(ServiceContext::background()).await {
            return Err(KvError::Other(e.to_string()));
        }
        Ok(kv)
    }

    pub fn get_kv(&self) -> Option<Arc<MemberlistKv>> {
        self.init.get().and_then(|r| r.as_ref().ok().cloned())
    }

    pub async fn stopping(&self) -> Result<(), ServiceError> {
        if let Some(kv) = self.get_kv() {
            let svc = kv.service();
            svc.stop_async();
            let _ = svc.await_terminated().await;
        }
        Ok(())
    }
}
