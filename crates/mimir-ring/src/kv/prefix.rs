//! Prefixed KV client wrapper.
//!
//! Ported from github.com/grafana/dskit/kv/prefix.go

use std::any::Any;
use std::sync::Arc;

use mimir_common::services::ServiceContext;

use super::{BoxFuture, CasFn, KvClient, KvError};

pub struct PrefixedKvClient {
    prefix: String,
    inner: Arc<dyn KvClient>,
}

impl PrefixedKvClient {
    pub fn new(inner: Arc<dyn KvClient>, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            inner,
        }
    }
}

impl KvClient for PrefixedKvClient {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let full = format!("{}{}", self.prefix, prefix);
            let mut keys = self.inner.list(&full).await?;
            for k in &mut keys {
                if let Some(stripped) = k.strip_prefix(&self.prefix) {
                    *k = stripped.to_string();
                }
            }
            Ok(keys)
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let full = format!("{}{}", self.prefix, key);
            self.inner.get(&full).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let full = format!("{}{}", self.prefix, key);
            self.inner.delete(&full).await
        })
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let full = format!("{}{}", self.prefix, key);
            self.inner.cas(&full, f).await
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let full = format!("{}{}", self.prefix, key);
            self.inner.watch_key(&full, cancel, f).await
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let full = format!("{}{}", self.prefix, prefix);
            let own_prefix = self.prefix.clone();
            self.inner
                .watch_prefix(
                    &full,
                    cancel,
                    Box::new(move |k, v| {
                        let stripped = k.strip_prefix(&own_prefix).unwrap_or(k);
                        f(stripped, v)
                    }),
                )
                .await
        })
    }
}
