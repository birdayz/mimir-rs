//! Lazy-initializing memberlist KV service wrapper.
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/kv_init_service.go

use std::sync::{Arc, OnceLock};

use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext};
use mimir_config::server::MemberlistConfig;
use prometheus::Registry;
use tokio::sync::Mutex;

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
    service: OnceLock<Arc<ManagedBasicService>>,
}

impl KvInitService {
    pub fn new(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        dns_provider: Option<Arc<dyn DnsProvider>>,
        registry: Option<Registry>,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            cfg,
            codecs,
            dns_provider,
            registry,
            init: OnceLock::new(),
            init_lock: Mutex::new(()),
            service: OnceLock::new(),
        });

        let w = Arc::downgrade(&this);
        let w_stop = Arc::downgrade(&this);
        let svc = basic_service(
            None,
            Some(Box::new(move |ctx: ServiceContext| {
                let _ = w;
                Box::pin(async move {
                    ctx.canceled().await;
                    Ok(())
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
        let res = MemberlistKv::start_with_options(
            self.cfg.clone(),
            self.codecs.clone(),
            "",
            self.registry.as_ref(),
            opts,
        )
        .await
        .map_err(|e| e.to_string());
        let _ = self.init.set(res.clone());
        res.map_err(KvError::Other)
    }

    pub fn get_kv(&self) -> Option<Arc<MemberlistKv>> {
        self.init.get().and_then(|r| r.as_ref().ok().cloned())
    }

    pub async fn stopping(&self) -> Result<(), mimir_common::services::ServiceError> {
        if let Some(kv) = self.get_kv() {
            kv.shutdown().await;
        }
        Ok(())
    }
}
