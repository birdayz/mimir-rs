//! Ported from github.com/grafana/mimir/pkg/mimirpb/symbols.go
//! and github.com/grafana/mimir/pkg/storage/ingest/version.go

use std::collections::HashMap;
use std::sync::OnceLock;

pub const RECORD_VERSION_HEADER_KEY: &str = "Version";
pub const LATEST_RECORD_VERSION: u32 = 2;
pub const V2_RECORD_SYMBOL_OFFSET: u32 = 64;

pub const MIN_PREALLOCATED_SYMBOLS_PER_REQUEST: usize = 100;
pub const MAX_PREALLOCATED_SYMBOLS_PER_REQUEST: usize = 100_000;

/// Built-in common symbols table for Kafka `ingest_storage` Record Version 2.
///
/// *IMPORTANT*: V2 is a stabilized wire format; this table cannot be changed without a version bump.
/// Ported verbatim from `V2CommonSymbols` in `pkg/storage/ingest/version.go`.
pub const V2_COMMON_SYMBOLS_LIST: &[&str] = &[
    // RW2.0 Spec: The first element of the symbols table MUST be an empty string.
    // This ensures that empty/missing refs still map to empty string.
    "",
    // Prometheus/Mimir symbols
    "__name__",
    "__aggregation__",
    "<aggregated>",
    "le",
    "component",
    "cortex_request_duration_seconds_bucket",
    "storage_operation_duration_seconds_bucket",
    // Grafana Labs products
    "grafana",
    "asserts_env",
    "asserts_request_context",
    "asserts_source",
    "asserts_entity_type",
    "asserts_request_type",
    // General symbols
    "name",
    "image",
    // Kubernetes symbols
    "cluster",
    "namespace",
    "pod",
    "job",
    "instance",
    "container",
    "replicaset",
    // Networking, HTTP
    "interface",
    "status_code",
    "resource",
    "operation",
    "method",
    // Common tools
    "kube-system",
    "kube-system/cadvisor",
    "node-exporter",
    "node-exporter/node-exporter",
    "kube-system/kubelet",
    "kube-system/node-local-dns",
    "kube-state-metrics/kube-state-metrics",
    "default/kubernetes",
];

/// Shared, immutable pre-loaded symbols table for RW2/Kafka-v2 encoding & decoding.
#[derive(Debug, Clone)]
pub struct CommonSymbols {
    symbols: Vec<String>,
    map: HashMap<String, u32>,
}

impl CommonSymbols {
    pub fn new(symbols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let symbols: Vec<String> = symbols.into_iter().map(Into::into).collect();
        let mut map = HashMap::with_capacity(symbols.len());
        for (idx, s) in symbols.iter().enumerate() {
            map.insert(s.clone(), idx as u32);
        }
        Self { symbols, map }
    }

    pub fn from_static(symbols: &[&str]) -> Self {
        Self::new(symbols.iter().copied())
    }

    #[inline]
    pub fn get_slice(&self) -> &[String] {
        &self.symbols
    }

    #[inline]
    pub fn get_ref(&self, s: &str) -> Option<u32> {
        self.map.get(s).copied()
    }

    #[inline]
    pub fn get_symbol(&self, r: u32) -> Option<&str> {
        self.symbols.get(r as usize).map(|s| s.as_str())
    }
}

/// Returns the static `CommonSymbols` instance matching `ingest.V2CommonSymbols`.
pub fn v2_common_symbols() -> &'static CommonSymbols {
    static V2: OnceLock<CommonSymbols> = OnceLock::new();
    V2.get_or_init(|| CommonSymbols::from_static(V2_COMMON_SYMBOLS_LIST))
}

/// Optimized symbol table matching `mimirpb.FastSymbolsTable` in `pkg/mimirpb/symbols.go`.
#[derive(Debug, Clone)]
pub struct FastSymbolsTable<'a> {
    symbols_map: HashMap<String, u32>,
    symbols_map_capacity_lower_bound: usize,
    common_symbols: Option<&'a CommonSymbols>,
    offset: u32,
}

impl<'a> Default for FastSymbolsTable<'a> {
    fn default() -> Self {
        Self::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST)
    }
}

impl<'a> FastSymbolsTable<'a> {
    pub fn new(capacity_hint: usize) -> Self {
        Self {
            symbols_map: HashMap::with_capacity(capacity_hint),
            symbols_map_capacity_lower_bound: capacity_hint,
            common_symbols: None,
            offset: 0,
        }
    }

    pub fn configure_common_symbols(
        &mut self,
        offset: u32,
        common_symbols: Option<&'a CommonSymbols>,
    ) {
        self.offset = offset;
        self.common_symbols = common_symbols;
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn symbolize(&mut self, s: &str) -> u32 {
        if s.is_empty() {
            // 0 means empty string, even if an offset is provided.
            return 0;
        }
        if let Some(cs) = self.common_symbols {
            if let Some(r) = cs.get_ref(s) {
                return r;
            }
        }
        if let Some(&r) = self.symbols_map.get(s) {
            return r;
        }
        let sym_map_len = self.symbols_map.len();
        let r = (sym_map_len as u32) + self.offset + 1;
        self.symbols_map.insert(s.to_owned(), r);
        if sym_map_len + 1 > self.symbols_map_capacity_lower_bound {
            self.symbols_map_capacity_lower_bound = sym_map_len + 1;
        }
        r
    }

    #[inline]
    pub fn count_symbols(&self) -> usize {
        self.symbols_map.len() + 1
    }

    #[inline]
    pub fn cap_lower_bound(&self) -> usize {
        self.symbols_map_capacity_lower_bound
    }

    pub fn symbols(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.count_symbols());
        self.symbols_into(&mut out);
        out
    }

    pub fn symbols_into(&self, prealloc: &mut Vec<String>) {
        prealloc.clear();
        prealloc.resize(self.count_symbols(), String::new());
        for (k, &v) in &self.symbols_map {
            let idx = (v - self.offset) as usize;
            prealloc[idx] = k.clone();
        }
    }

    pub fn symbols_into_drain(&mut self, prealloc: &mut Vec<String>) {
        prealloc.clear();
        prealloc.resize(self.count_symbols(), String::new());
        let offset = self.offset;
        for (k, v) in self.symbols_map.drain() {
            let idx = (v - offset) as usize;
            prealloc[idx] = k;
        }
        self.offset = 0;
        self.common_symbols = None;
    }

    pub fn reset(&mut self) {
        self.symbols_map.clear();
        self.offset = 0;
        self.common_symbols = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_symbols_table_basic() {
        // Ported from TestSymbolsTable in pkg/mimirpb/symbols_test.go
        let mut s = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
        assert_eq!(s.symbols(), vec![""]);
        assert_eq!(s.symbolize(""), 0);
        assert_eq!(s.symbols(), vec![""]);

        assert_eq!(s.symbolize("abc"), 1);
        assert_eq!(s.symbols(), vec!["", "abc"]);

        assert_eq!(s.symbolize("__name__"), 2);
        assert_eq!(s.symbols(), vec!["", "abc", "__name__"]);

        assert_eq!(s.symbolize("foo"), 3);
        assert_eq!(s.symbols(), vec!["", "abc", "__name__", "foo"]);

        s.reset();
        assert_eq!(s.symbols(), vec![""]);
        assert_eq!(s.symbolize(""), 0);
        assert_eq!(s.symbolize("__name__"), 1);
        assert_eq!(s.symbols(), vec!["", "__name__"]);
        assert_eq!(s.symbolize("abc"), 2);
        assert_eq!(s.symbols(), vec!["", "__name__", "abc"]);
    }

    #[test]
    fn test_fast_symbols_table_offset_no_common() {
        let mut s = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
        s.configure_common_symbols(128, None);

        assert_eq!(s.symbols(), vec![""]);
        assert_eq!(s.symbolize(""), 0);
        assert_eq!(s.symbols(), vec![""]);

        assert_eq!(s.symbolize("abc"), 129);
        assert_eq!(s.symbols(), vec!["", "abc"]);

        assert_eq!(s.symbolize("__name__"), 130);
        assert_eq!(s.symbols(), vec!["", "abc", "__name__"]);

        s.reset();
        assert_eq!(s.offset(), 0);
    }

    #[test]
    fn test_fast_symbols_table_common_symbols() {
        let cs = CommonSymbols::from_static(&["", "__name__", "__aggregation__"]);
        let mut s = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
        s.configure_common_symbols(8, Some(&cs));

        assert_eq!(s.symbols(), vec![""]);
        assert_eq!(s.symbolize(""), 0);
        assert_eq!(s.symbols(), vec![""]);

        assert_eq!(s.symbolize("abc"), 9);
        assert_eq!(s.symbols(), vec!["", "abc"]);

        assert_eq!(s.symbolize("__name__"), 1);
        assert_eq!(s.symbols(), vec!["", "abc"]);

        assert_eq!(s.symbolize("def"), 10);
        assert_eq!(s.symbols(), vec!["", "abc", "def"]);

        assert_eq!(s.symbolize("__aggregation__"), 2);
        assert_eq!(s.symbols(), vec!["", "abc", "def"]);

        s.reset();
        assert!(s.common_symbols.is_none());
    }

    #[test]
    fn test_fast_symbols_table_capacity_estimation() {
        let mut s = FastSymbolsTable::new(50);
        assert_eq!(s.cap_lower_bound(), 50);

        s.symbolize("abc");
        s.symbolize("def");
        s.symbolize("ghi");
        assert_eq!(s.cap_lower_bound(), 50);

        s.reset();
        assert_eq!(s.cap_lower_bound(), 50);

        for i in 0..50 {
            s.symbolize(&i.to_string());
        }
        assert_eq!(s.cap_lower_bound(), 50);

        s.symbolize("51st");
        assert_eq!(s.cap_lower_bound(), 51);
    }

    #[test]
    fn test_v2_common_symbols_layout() {
        let cs = v2_common_symbols();
        assert_eq!(cs.get_slice().len(), 36);
        assert!(cs.get_slice().len() < V2_RECORD_SYMBOL_OFFSET as usize);
        assert_eq!(cs.get_ref(""), Some(0));
        assert_eq!(cs.get_ref("__name__"), Some(1));
        assert_eq!(cs.get_ref("default/kubernetes"), Some(35));
    }
}
