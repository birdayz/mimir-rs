//! Key-value store abstractions, codecs, and backends.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/client.go
//! - github.com/grafana/dskit/kv/codec/codec.go

pub mod consul;
pub mod etcd;
pub mod inmemory;
pub mod memberlist;
pub mod multi;
pub mod prefix;

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use mimir_common::services::ServiceContext;
use mimir_config::ingester::KVStoreConfig;
use thiserror::Error;

use self::memberlist::mergeable::Mergeable;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("codec error: {0}")]
    Codec(String),
    #[error("invalid value type: {0}")]
    InvalidType(String),
    #[error("CAS failed for key {key}: {reason}")]
    CasFailed { key: String, reason: String },
    #[error("key not found: {0}")]
    NotFound(String),
    #[error("invalid KV store config: {0}")]
    InvalidConfig(String),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unexpected state: {0}")]
    UnexpectedState(String),
    #[error("context canceled")]
    Canceled,
    #[error("{0}")]
    Other(String),
}

/// Result of a CAS callback.
pub enum CasOutcome {
    /// Store the returned value.
    Update(Box<dyn Any + Send + Sync>),
    /// Leave the stored value unchanged and succeed immediately.
    NoChange,
}

/// Error returned by a CAS callback, indicating whether CAS should retry.
#[derive(Debug)]
pub struct CasCallbackError {
    pub err: KvError,
    pub retry: bool,
}

impl CasCallbackError {
    pub fn retryable(err: impl Into<KvError>) -> Self {
        Self {
            err: err.into(),
            retry: true,
        }
    }

    pub fn fatal(err: impl Into<KvError>) -> Self {
        Self {
            err: err.into(),
            retry: false,
        }
    }
}

pub type CasFn = Box<
    dyn FnMut(Option<&(dyn Any + Send + Sync)>) -> Result<CasOutcome, CasCallbackError> + Send,
>;

/// Codec for serializing/deserializing values stored in a KV store.
pub trait Codec: Send + Sync {
    fn codec_id(&self) -> &str;
    fn decode(&self, bytes: &[u8]) -> Result<Box<dyn Any + Send + Sync>, KvError>;
    fn encode(&self, msg: &(dyn Any + Send + Sync)) -> Result<Vec<u8>, KvError>;

    /// Decodes `bytes` into a [`Mergeable`] value (used by `memberlist` KV).
    fn decode_mergeable(&self, _bytes: &[u8]) -> Result<Box<dyn Mergeable>, KvError> {
        Err(KvError::Codec(format!(
            "codec {} does not support Mergeable",
            self.codec_id()
        )))
    }

    /// Encodes a [`Mergeable`] value into wire bytes (used by `memberlist` KV).
    fn encode_mergeable(&self, _msg: &dyn Mergeable) -> Result<Vec<u8>, KvError> {
        Err(KvError::Codec(format!(
            "codec {} does not support Mergeable",
            self.codec_id()
        )))
    }
}

/// Proto + Snappy block codec matching `github.com/grafana/dskit/kv/codec/codec.go`.
///
/// IMPORTANT: Uses Snappy BLOCK format (`snap::raw::Encoder` / `snap::raw::Decoder`),
/// NOT Snappy framed stream!
pub struct ProtoSnappyCodec<M> {
    id: String,
    post_decode: Option<fn(&mut M)>,
    to_mergeable: Option<fn(M) -> Box<dyn Mergeable>>,
    from_mergeable: Option<fn(&dyn Mergeable) -> Option<&M>>,
}

impl<M> Clone for ProtoSnappyCodec<M> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            post_decode: self.post_decode,
            to_mergeable: self.to_mergeable,
            from_mergeable: self.from_mergeable,
        }
    }
}

impl<M> ProtoSnappyCodec<M>
where
    M: prost::Message + Default + Send + Sync + 'static,
{
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            post_decode: None,
            to_mergeable: None,
            from_mergeable: None,
        }
    }

    pub fn with_post_decode(mut self, f: fn(&mut M)) -> Self {
        self.post_decode = Some(f);
        self
    }

    pub fn with_mergeable(
        mut self,
        to_mergeable: fn(M) -> Box<dyn Mergeable>,
        from_mergeable: fn(&dyn Mergeable) -> Option<&M>,
    ) -> Self {
        self.to_mergeable = Some(to_mergeable);
        self.from_mergeable = Some(from_mergeable);
        self
    }

    pub fn decode_msg(&self, bytes: &[u8]) -> Result<M, KvError> {
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(bytes)
            .map_err(|e| KvError::Codec(format!("snappy decode: {e}")))?;
        let mut msg = M::decode(decompressed.as_slice())
            .map_err(|e| KvError::Codec(format!("proto decode: {e}")))?;
        if let Some(post) = self.post_decode {
            post(&mut msg);
        }
        Ok(msg)
    }

    pub fn encode_msg(&self, msg: &M) -> Result<Vec<u8>, KvError> {
        let raw = msg.encode_to_vec();
        snap::raw::Encoder::new()
            .compress_vec(&raw)
            .map_err(|e| KvError::Codec(format!("snappy encode: {e}")))
    }
}

impl<M> Codec for ProtoSnappyCodec<M>
where
    M: prost::Message + Default + Send + Sync + 'static,
{
    fn codec_id(&self) -> &str {
        &self.id
    }

    fn decode(&self, bytes: &[u8]) -> Result<Box<dyn Any + Send + Sync>, KvError> {
        Ok(Box::new(self.decode_msg(bytes)?))
    }

    fn encode(&self, msg: &(dyn Any + Send + Sync)) -> Result<Vec<u8>, KvError> {
        let typed = msg.downcast_ref::<M>().ok_or_else(|| {
            KvError::InvalidType(format!("expected {} for codec {}", std::any::type_name::<M>(), self.id))
        })?;
        self.encode_msg(typed)
    }

    fn decode_mergeable(&self, bytes: &[u8]) -> Result<Box<dyn Mergeable>, KvError> {
        let Some(to_m) = self.to_mergeable else {
            return Err(KvError::Codec(format!(
                "codec {} does not support Mergeable",
                self.id
            )));
        };
        let msg = self.decode_msg(bytes)?;
        Ok(to_m(msg))
    }

    fn encode_mergeable(&self, msg: &dyn Mergeable) -> Result<Vec<u8>, KvError> {
        let Some(from_m) = self.from_mergeable else {
            return Err(KvError::Codec(format!(
                "codec {} does not support Mergeable",
                self.id
            )));
        };
        let typed = from_m(msg).ok_or_else(|| {
            KvError::InvalidType(format!("expected {} for codec {}", std::any::type_name::<M>(), self.id))
        })?;
        self.encode_msg(typed)
    }
}

/// String codec matching `dskit/kv/codec.String`.
#[derive(Clone, Copy, Debug, Default)]
pub struct StringCodec;

impl Codec for StringCodec {
    fn codec_id(&self) -> &str {
        "string"
    }

    fn decode(&self, bytes: &[u8]) -> Result<Box<dyn Any + Send + Sync>, KvError> {
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e| KvError::Codec(format!("utf8 decode: {e}")))?;
        Ok(Box::new(s))
    }

    fn encode(&self, msg: &(dyn Any + Send + Sync)) -> Result<Vec<u8>, KvError> {
        let s = msg
            .downcast_ref::<String>()
            .ok_or_else(|| KvError::InvalidType("expected String".to_string()))?;
        Ok(s.as_bytes().to_vec())
    }
}

pub type WatchKeyFn = Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>;
pub type WatchPrefixFn = Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>;

/// Async key-value store client matching `github.com/grafana/dskit/kv.Client`.
pub trait KvClient: Send + Sync {
    /// Lists all keys under `prefix`. Returned keys include `prefix`.
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>>;

    /// Gets the decoded value for `key`, or `None` if it does not exist.
    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>>;

    /// Best-effort deletion of `key`.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>>;

    /// Compare-And-Swap with retry loop.
    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>>;

    /// Watches `key` for changes until `cancel` is canceled or `f` returns `false`.
    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        f: WatchKeyFn,
    ) -> BoxFuture<'a, ()>;

    /// Watches all keys with `prefix` for changes until `cancel` is canceled or `f` returns `false`.
    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        f: WatchPrefixFn,
    ) -> BoxFuture<'a, ()>;
}

/// Typed convenience helpers on [`KvClient`].
pub async fn get_typed<T: Clone + Send + Sync + 'static>(
    client: &dyn KvClient,
    key: &str,
) -> Result<Option<T>, KvError> {
    match client.get(key).await? {
        None => Ok(None),
        Some(boxed) => boxed
            .downcast::<T>()
            .map(|b| Some(*b))
            .map_err(|_| KvError::InvalidType(std::any::type_name::<T>().to_string())),
    }
}

pub async fn cas_typed<T, F>(client: &dyn KvClient, key: &str, mut f: F) -> Result<(), KvError>
where
    T: Clone + Send + Sync + 'static,
    F: FnMut(Option<&T>) -> Result<Option<(T, bool)>, CasCallbackError> + Send + 'static,
{
    let cb: CasFn = Box::new(move |current| {
        let typed = match current {
            None => None,
            Some(any) => Some(
                any.downcast_ref::<T>()
                    .ok_or_else(|| CasCallbackError::fatal(KvError::InvalidType(std::any::type_name::<T>().to_string())))?,
            ),
        };
        match f(typed)? {
            None => Ok(CasOutcome::NoChange),
            Some((val, _retry)) => Ok(CasOutcome::Update(Box::new(val))),
        }
    });
    client.cas(key, cb).await
}

/// Builds a [`KvClient`] from [`KVStoreConfig`].
pub fn build_kv_client(
    cfg: &KVStoreConfig,
    codec: Arc<dyn Codec>,
    memberlist_kv: Option<Arc<memberlist::MemberlistKv>>,
    reg: Option<&prometheus::Registry>,
) -> Result<Arc<dyn KvClient>, KvError> {
    build_kv_client_with_options(cfg, codec, memberlist_kv, reg, KvClientOptions::default())
}

/// Per-client options of `kv.NewClient` that are not part of the YAML config.
#[derive(Clone, Default)]
pub struct KvClientOptions {
    /// `kv.RegistererWithKVName`: value of the `kv_name` label on the client's metrics
    /// (currently applied to the `multikv_*` metrics).
    pub kv_name: Option<String>,
    /// Metric name prefix of the registerer the client is created with (e.g. `cortex_` for
    /// `prometheus.WrapRegistererWithPrefix("cortex_", ...)`).
    pub metrics_prefix: String,
    /// `MultiConfig.ConfigProvider`: a `multi` store watches it for runtime
    /// `multi_kv_config` changes (primary switch, mirroring toggle).
    pub multi_config_provider: Option<multi::MultiConfigProvider>,
}

/// Like [`build_kv_client`], with [`KvClientOptions`].
pub fn build_kv_client_with_options(
    cfg: &KVStoreConfig,
    codec: Arc<dyn Codec>,
    memberlist_kv: Option<Arc<memberlist::MemberlistKv>>,
    reg: Option<&prometheus::Registry>,
    opts: KvClientOptions,
) -> Result<Arc<dyn KvClient>, KvError> {
    let client = create_client(
        &cfg.store,
        &cfg.prefix,
        cfg,
        codec,
        memberlist_kv,
        reg,
        opts,
    )?;
    Ok(client)
}

fn create_client(
    backend: &str,
    prefix: &str,
    cfg: &KVStoreConfig,
    codec: Arc<dyn Codec>,
    memberlist_kv: Option<Arc<memberlist::MemberlistKv>>,
    reg: Option<&prometheus::Registry>,
    opts: KvClientOptions,
) -> Result<Arc<dyn KvClient>, KvError> {
    let base: Arc<dyn KvClient> = match backend {
        "inmemory" => Arc::new(inmemory::InMemoryKv::shared_with_codec(codec)),
        "consul" => Arc::new(consul::ConsulKv::new(cfg.consul.clone(), codec, reg)?),
        "etcd" => Arc::new(etcd::EtcdKv::new(cfg.etcd.clone(), codec)?),
        "memberlist" => {
            let ml = memberlist_kv.ok_or_else(|| {
                KvError::InvalidConfig("memberlist KV store requested but not initialized".to_string())
            })?;
            Arc::new(memberlist::MemberlistKvClient::new(ml, codec)?)
        }
        "multi" => {
            if cfg.multi.primary.is_empty() || cfg.multi.secondary.is_empty() {
                return Err(KvError::InvalidConfig(
                    "primary or secondary store not set".to_string(),
                ));
            }
            if cfg.multi.primary == "multi" || cfg.multi.secondary == "multi" {
                return Err(KvError::InvalidConfig(
                    "primary and secondary stores cannot be multi-stores".to_string(),
                ));
            }
            if cfg.multi.primary == cfg.multi.secondary {
                return Err(KvError::InvalidConfig(
                    "primary and secondary stores must be different".to_string(),
                ));
            }
            let primary = create_client(
                &cfg.multi.primary,
                "",
                cfg,
                codec.clone(),
                memberlist_kv.clone(),
                reg,
                KvClientOptions::default(),
            )?;
            let secondary = create_client(
                &cfg.multi.secondary,
                "",
                cfg,
                codec.clone(),
                memberlist_kv,
                reg,
                KvClientOptions::default(),
            )?;
            multi::MultiKv::new_with_options(
                cfg.multi.clone(),
                vec![
                    multi::NamedKvClient {
                        name: cfg.multi.primary.clone(),
                        client: primary,
                    },
                    multi::NamedKvClient {
                        name: cfg.multi.secondary.clone(),
                        client: secondary,
                    },
                ],
                reg,
                multi::MultiKvOptions {
                    codec: Some(codec),
                    config_provider: opts.multi_config_provider,
                    kv_name: opts.kv_name,
                    metrics_prefix: opts.metrics_prefix,
                },
            )
        }
        other => {
            return Err(KvError::InvalidConfig(format!(
                "invalid KV store type: {other}"
            )));
        }
    };

    if prefix.is_empty() {
        Ok(base)
    } else {
        Ok(Arc::new(prefix::PrefixedKvClient::new(
            base,
            prefix.to_string(),
        )))
    }
}
