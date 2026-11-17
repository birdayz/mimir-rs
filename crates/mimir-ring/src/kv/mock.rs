//! The `mock` KV backend: a client that does nothing (testing only).
//!
//! Ported from github.com/grafana/dskit/kv/mock.go (`mockClient`, `buildMockClient`)

use std::any::Any;

use mimir_common::services::ServiceContext;

use super::{BoxFuture, CasFn, KvClient, KvError, WatchKeyFn, WatchPrefixFn};

/// Go `kv.mockClient`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MockKv;

impl MockKv {
    /// Go `buildMockClient`.
    pub fn new() -> Self {
        tracing::warn!("created mockClient for testing only");
        MockKv
    }
}

impl KvClient for MockKv {
    fn list<'a>(&'a self, _prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get<'a>(
        &'a self,
        _key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        // Go returns `"", nil`.
        Box::pin(async { Ok(Some(Box::new(String::new()) as Box<dyn Any + Send + Sync>)) })
    }

    fn delete<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async { Ok(()) })
    }

    fn cas<'a>(&'a self, _key: &'a str, _f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async { Ok(()) })
    }

    fn watch_key<'a>(
        &'a self,
        _key: &'a str,
        _cancel: ServiceContext,
        _f: WatchKeyFn,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn watch_prefix<'a>(
        &'a self,
        _prefix: &'a str,
        _cancel: ServiceContext,
        _f: WatchPrefixFn,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}
