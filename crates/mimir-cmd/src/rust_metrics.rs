//! Rust-runtime-specific operational metrics (`mimir_rs_*`).
//!
//! Because `mimir-rs` intentionally omits Go's `go_*` runtime/GC metrics (no Go runtime or GC
//! exists in Rust), this pull-on-scrape collector exposes the Rust-native equivalents needed to
//! diagnose memory growth, allocator fragmentation, page-cache pressure, async executor
//! saturation, and off-heap structures in production with zero hot-path overhead:
//!
//! - **jemalloc allocator** (`mimir_rs_jemalloc_*`): `allocated`, `active`, `resident`, `mapped`,
//!   `retained`, `metadata`, and explicit `arena_purges_total`.
//! - **Linux process memory/thread split** (`mimir_rs_process_*` from `/proc/self/status`):
//!   `rss_anon_bytes`, `rss_file_bytes`, `rss_shmem_bytes`, `threads`.
//! - **Tokio runtimes** (`mimir_rs_tokio_*`, labeled `runtime="main"|"rpc"`): `workers`,
//!   `alive_tasks`, `global_queue_depth`, `worker_busy_seconds_total`, `worker_park_total`.
//! - **Off-heap `NanosSlab`** (`mimir_rs_nanos_slab_*`): `mapped_bytes`, `pages`,
//!   `reclaimed_pages_total`.
//! - **Persistent `ScopedWorkerPool`** (`mimir_rs_scoped_worker_pool_*`): `threads`,
//!   `executions_total`.

use std::collections::HashMap;
use std::sync::Arc;

use mimir_common::JemallocStats;
use parking_lot::RwLock;
use prometheus::core::{Collector, Desc};
use prometheus::proto;

type JemallocReader = Box<dyn Fn() -> Option<JemallocStats> + Send + Sync>;

/// Shared handle allowing `app.rs` to register/unregister Tokio runtimes (e.g. `main` and `rpc`)
/// on the already-registered [`RustMetricsCollector`].
#[derive(Clone, Default)]
pub struct RustRuntimeMetricsHandle {
    runtimes: Arc<RwLock<Vec<(&'static str, tokio::runtime::Handle)>>>,
}

impl RustRuntimeMetricsHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) a named Tokio runtime handle for scrape-time metric collection.
    pub fn register_runtime(&self, name: &'static str, handle: tokio::runtime::Handle) {
        let mut rts = self.runtimes.write();
        if let Some(slot) = rts.iter_mut().find(|(n, _)| *n == name) {
            slot.1 = handle;
        } else {
            rts.push((name, handle));
        }
    }

    /// Removes a named Tokio runtime handle once that runtime is shutting down.
    pub fn unregister_runtime(&self, name: &'static str) {
        self.runtimes.write().retain(|(n, _)| *n != name);
    }
}

struct MetricSpec {
    name: &'static str,
    help: &'static str,
    mtype: proto::MetricType,
    labels: &'static [&'static str],
}

const JEMALLOC_SPECS: &[MetricSpec] = &[
    MetricSpec {
        name: "mimir_rs_jemalloc_allocated_bytes",
        help: "Total number of bytes allocated by the application in jemalloc.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_jemalloc_active_bytes",
        help: "Total number of bytes in active pages allocated by jemalloc.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_jemalloc_resident_bytes",
        help: "Maximum number of bytes in physically resident data pages mapped by jemalloc.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_jemalloc_mapped_bytes",
        help: "Total number of bytes in active extents mapped by jemalloc.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_jemalloc_retained_bytes",
        help: "Total number of bytes in virtual memory mappings retained by jemalloc.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_jemalloc_metadata_bytes",
        help: "Total number of bytes dedicated to jemalloc metadata.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_jemalloc_arena_purges_total",
        help: "Total number of explicit jemalloc arena purges triggered by mimir-rs.",
        mtype: proto::MetricType::COUNTER,
        labels: &[],
    },
];

const PROC_STATUS_SPECS: &[MetricSpec] = &[
    MetricSpec {
        name: "mimir_rs_process_rss_anon_bytes",
        help: "Anonymous resident memory (RssAnon) of the process in bytes.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_process_rss_file_bytes",
        help: "File-backed resident memory (RssFile / page cache) of the process in bytes.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_process_rss_shmem_bytes",
        help: "Shared resident memory (RssShmem) of the process in bytes.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_process_threads",
        help: "Number of OS threads currently in the process.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
];

const TOKIO_SPECS: &[MetricSpec] = &[
    MetricSpec {
        name: "mimir_rs_tokio_workers",
        help: "Number of worker threads in the Tokio runtime.",
        mtype: proto::MetricType::GAUGE,
        labels: &["runtime"],
    },
    MetricSpec {
        name: "mimir_rs_tokio_alive_tasks",
        help: "Current number of alive tasks in the Tokio runtime.",
        mtype: proto::MetricType::GAUGE,
        labels: &["runtime"],
    },
    MetricSpec {
        name: "mimir_rs_tokio_global_queue_depth",
        help: "Number of tasks currently scheduled in the Tokio runtime global injection queue.",
        mtype: proto::MetricType::GAUGE,
        labels: &["runtime"],
    },
    MetricSpec {
        name: "mimir_rs_tokio_worker_busy_seconds_total",
        help: "Cumulative seconds spent executing tasks across all Tokio worker threads.",
        mtype: proto::MetricType::COUNTER,
        labels: &["runtime"],
    },
    MetricSpec {
        name: "mimir_rs_tokio_worker_park_total",
        help: "Cumulative number of times Tokio worker threads parked.",
        mtype: proto::MetricType::COUNTER,
        labels: &["runtime"],
    },
];

const INTERNAL_SPECS: &[MetricSpec] = &[
    MetricSpec {
        name: "mimir_rs_nanos_slab_mapped_bytes",
        help: "Total bytes currently mapped by off-heap NanosSlab anonymous pages across all tenants.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_nanos_slab_pages",
        help: "Total number of off-heap NanosSlab pages currently mapped across all tenants.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_nanos_slab_reclaimed_pages_total",
        help: "Total number of off-heap NanosSlab pages physically reclaimed via madvise(MADV_DONTNEED).",
        mtype: proto::MetricType::COUNTER,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_scoped_worker_pool_threads",
        help: "Current number of persistent OS worker threads across ScopedWorkerPool instances.",
        mtype: proto::MetricType::GAUGE,
        labels: &[],
    },
    MetricSpec {
        name: "mimir_rs_scoped_worker_pool_executions_total",
        help: "Total number of parallel fork-join batches dispatched on ScopedWorkerPool instances.",
        mtype: proto::MetricType::COUNTER,
        labels: &[],
    },
];

pub struct RustMetricsCollector {
    proc_status_path: Option<String>,
    runtimes: Arc<RwLock<Vec<(&'static str, tokio::runtime::Handle)>>>,
    jemalloc_reader: JemallocReader,
    descs: Vec<Desc>,
}

impl RustMetricsCollector {
    /// Creates a collector for the current process and returns a handle for registering
    /// Tokio runtimes as they start/stop.
    pub fn for_self() -> (Self, RustRuntimeMetricsHandle) {
        let proc_status_path = if std::path::Path::new("/proc/self/status").exists() {
            Some("/proc/self/status".to_string())
        } else {
            None
        };
        Self::with_sources(proc_status_path, Box::new(mimir_common::read_jemalloc_stats))
    }

    fn with_sources(
        proc_status_path: Option<String>,
        jemalloc_reader: JemallocReader,
    ) -> (Self, RustRuntimeMetricsHandle) {
        let handle = RustRuntimeMetricsHandle::new();
        let mut descs = Vec::new();
        for group in [JEMALLOC_SPECS, PROC_STATUS_SPECS, TOKIO_SPECS, INTERNAL_SPECS] {
            for spec in group {
                let variable_labels: Vec<String> =
                    spec.labels.iter().map(|s| (*s).to_string()).collect();
                if let Ok(desc) = Desc::new(
                    spec.name.to_string(),
                    spec.help.to_string(),
                    variable_labels,
                    HashMap::new(),
                ) {
                    descs.push(desc);
                }
            }
        }
        let collector = Self {
            proc_status_path,
            runtimes: Arc::clone(&handle.runtimes),
            jemalloc_reader,
            descs,
        };
        (collector, handle)
    }
}

fn scalar_family(spec: &MetricSpec, value: f64) -> proto::MetricFamily {
    let mut m = proto::Metric::default();
    match spec.mtype {
        proto::MetricType::COUNTER => {
            let mut c = proto::Counter::default();
            c.set_value(value);
            m.set_counter(c);
        }
        _ => {
            let mut g = proto::Gauge::default();
            g.set_value(value);
            m.set_gauge(g);
        }
    }
    let mut mf = proto::MetricFamily::default();
    mf.set_name(spec.name.to_string());
    mf.set_help(spec.help.to_string());
    mf.set_field_type(spec.mtype);
    mf.set_metric(vec![m].into());
    mf
}

fn runtime_family(spec: &MetricSpec, samples: &[(&str, f64)]) -> proto::MetricFamily {
    let mut metrics = Vec::with_capacity(samples.len());
    for &(runtime_name, value) in samples {
        let mut m = proto::Metric::default();
        let mut lp = proto::LabelPair::default();
        lp.set_name("runtime".to_string());
        lp.set_value(runtime_name.to_string());
        m.set_label(vec![lp].into());
        match spec.mtype {
            proto::MetricType::COUNTER => {
                let mut c = proto::Counter::default();
                c.set_value(value);
                m.set_counter(c);
            }
            _ => {
                let mut g = proto::Gauge::default();
                g.set_value(value);
                m.set_gauge(g);
            }
        }
        metrics.push(m);
    }
    let mut mf = proto::MetricFamily::default();
    mf.set_name(spec.name.to_string());
    mf.set_help(spec.help.to_string());
    mf.set_field_type(spec.mtype);
    mf.set_metric(metrics.into());
    mf
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ProcStatusValues {
    rss_anon_bytes: Option<u64>,
    rss_file_bytes: Option<u64>,
    rss_shmem_bytes: Option<u64>,
    threads: Option<u64>,
}

fn parse_proc_status(text: &str) -> ProcStatusValues {
    let mut out = ProcStatusValues::default();
    for line in text.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        let rest = rest.trim();
        match key {
            "RssAnon" => {
                if let Some(kb) = parse_kb_field(rest) {
                    out.rss_anon_bytes = Some(kb.saturating_mul(1024));
                }
            }
            "RssFile" => {
                if let Some(kb) = parse_kb_field(rest) {
                    out.rss_file_bytes = Some(kb.saturating_mul(1024));
                }
            }
            "RssShmem" => {
                if let Some(kb) = parse_kb_field(rest) {
                    out.rss_shmem_bytes = Some(kb.saturating_mul(1024));
                }
            }
            "Threads" => {
                if let Some(tok) = rest.split_whitespace().next() {
                    if let Ok(n) = tok.parse::<u64>() {
                        out.threads = Some(n);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn parse_kb_field(s: &str) -> Option<u64> {
    s.split_whitespace().next()?.parse::<u64>().ok()
}

impl Collector for RustMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut out = Vec::with_capacity(self.descs.len());

        // 1. jemalloc stats (present when running the Linux binary with tikv-jemallocator).
        if let Some(stats) = (self.jemalloc_reader)() {
            out.push(scalar_family(&JEMALLOC_SPECS[0], stats.allocated as f64));
            out.push(scalar_family(&JEMALLOC_SPECS[1], stats.active as f64));
            out.push(scalar_family(&JEMALLOC_SPECS[2], stats.resident as f64));
            out.push(scalar_family(&JEMALLOC_SPECS[3], stats.mapped as f64));
            out.push(scalar_family(&JEMALLOC_SPECS[4], stats.retained as f64));
            out.push(scalar_family(&JEMALLOC_SPECS[5], stats.metadata as f64));
            out.push(scalar_family(
                &JEMALLOC_SPECS[6],
                mimir_common::allocator_arena_purges_total() as f64,
            ));
        }

        // 2. Linux /proc/self/status memory & thread breakdown.
        if let Some(ref path) = self.proc_status_path {
            if let Ok(text) = std::fs::read_to_string(path) {
                let parsed = parse_proc_status(&text);
                if let Some(v) = parsed.rss_anon_bytes {
                    out.push(scalar_family(&PROC_STATUS_SPECS[0], v as f64));
                }
                if let Some(v) = parsed.rss_file_bytes {
                    out.push(scalar_family(&PROC_STATUS_SPECS[1], v as f64));
                }
                if let Some(v) = parsed.rss_shmem_bytes {
                    out.push(scalar_family(&PROC_STATUS_SPECS[2], v as f64));
                }
                if let Some(v) = parsed.threads {
                    out.push(scalar_family(&PROC_STATUS_SPECS[3], v as f64));
                }
            }
        }

        // 3. Registered Tokio runtimes ("main", "rpc").
        let rts: Vec<(&'static str, tokio::runtime::Handle)> = self.runtimes.read().clone();
        if !rts.is_empty() {
            let mut workers = Vec::with_capacity(rts.len());
            let mut alive = Vec::with_capacity(rts.len());
            let mut qdepth = Vec::with_capacity(rts.len());
            let mut busy = Vec::with_capacity(rts.len());
            let mut parks = Vec::with_capacity(rts.len());

            for (name, handle) in &rts {
                let m = handle.metrics();
                let n = m.num_workers();
                workers.push((*name, n as f64));
                alive.push((*name, m.num_alive_tasks() as f64));
                qdepth.push((*name, m.global_queue_depth() as f64));
                let mut total_busy = 0.0f64;
                let mut total_parks = 0u64;
                for w in 0..n {
                    total_busy += m.worker_total_busy_duration(w).as_secs_f64();
                    total_parks = total_parks.saturating_add(m.worker_park_count(w));
                }
                busy.push((*name, total_busy));
                parks.push((*name, total_parks as f64));
            }

            out.push(runtime_family(&TOKIO_SPECS[0], &workers));
            out.push(runtime_family(&TOKIO_SPECS[1], &alive));
            out.push(runtime_family(&TOKIO_SPECS[2], &qdepth));
            out.push(runtime_family(&TOKIO_SPECS[3], &busy));
            out.push(runtime_family(&TOKIO_SPECS[4], &parks));
        }

        // 4. Off-heap NanosSlab & persistent ScopedWorkerPool stats.
        let slab = mimir_common::read_nanos_slab_stats();
        out.push(scalar_family(&INTERNAL_SPECS[0], slab.mapped_bytes as f64));
        out.push(scalar_family(&INTERNAL_SPECS[1], slab.pages as f64));
        out.push(scalar_family(
            &INTERNAL_SPECS[2],
            slab.reclaimed_pages_total as f64,
        ));

        let pool = mimir_common::read_scoped_worker_pool_stats();
        out.push(scalar_family(&INTERNAL_SPECS[3], pool.threads as f64));
        out.push(scalar_family(
            &INTERNAL_SPECS[4],
            pool.executions_total as f64,
        ));

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, Registry, TextEncoder};

    #[test]
    fn parse_proc_status_extracts_rss_split_and_threads() {
        let sample = "\
Name:\tmimir
State:\tS (sleeping)
Threads:\t42
VmRSS:\t2048000 kB
RssAnon:\t1536000 kB
RssFile:\t510000 kB
RssShmem:\t2000 kB
";
        let got = parse_proc_status(sample);
        assert_eq!(
            got,
            ProcStatusValues {
                rss_anon_bytes: Some(1_536_000 * 1024),
                rss_file_bytes: Some(510_000 * 1024),
                rss_shmem_bytes: Some(2_000 * 1024),
                threads: Some(42),
            }
        );
    }

    #[test]
    fn parse_proc_status_handles_missing_and_malformed_lines() {
        let sample = "MalformedLineWithoutColon\nRssAnon: notanumber kB\nThreads:\t7\n";
        let got = parse_proc_status(sample);
        assert_eq!(got.rss_anon_bytes, None);
        assert_eq!(got.rss_file_bytes, None);
        assert_eq!(got.rss_shmem_bytes, None);
        assert_eq!(got.threads, Some(7));
    }

    #[test]
    fn collector_emits_all_families_with_mock_jemalloc_and_live_runtimes() {
        let dir = tempfile::tempdir().unwrap();
        let status_path = dir.path().join("status");
        std::fs::write(
            &status_path,
            "Threads:\t12\nRssAnon:\t4096 kB\nRssFile:\t1024 kB\nRssShmem:\t128 kB\n",
        )
        .unwrap();

        let (collector, handle) = RustMetricsCollector::with_sources(
            Some(status_path.to_string_lossy().into_owned()),
            Box::new(|| {
                Some(JemallocStats {
                    allocated: 100_000,
                    active: 120_000,
                    resident: 140_000,
                    mapped: 200_000,
                    retained: 300_000,
                    metadata: 16_384,
                })
            }),
        );

        let rt_main = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let rt_rpc = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .enable_all()
            .build()
            .unwrap();

        handle.register_runtime("main", rt_main.handle().clone());
        handle.register_runtime("rpc", rt_rpc.handle().clone());

        let reg = Registry::new();
        reg.register(Box::new(collector)).unwrap();

        let mut buf = Vec::new();
        TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();

        for expected in [
            "mimir_rs_jemalloc_allocated_bytes 100000",
            "mimir_rs_jemalloc_active_bytes 120000",
            "mimir_rs_jemalloc_resident_bytes 140000",
            "mimir_rs_jemalloc_mapped_bytes 200000",
            "mimir_rs_jemalloc_retained_bytes 300000",
            "mimir_rs_jemalloc_metadata_bytes 16384",
            "mimir_rs_jemalloc_arena_purges_total ",
            "mimir_rs_process_rss_anon_bytes 4194304",
            "mimir_rs_process_rss_file_bytes 1048576",
            "mimir_rs_process_rss_shmem_bytes 131072",
            "mimir_rs_process_threads 12",
            "mimir_rs_tokio_workers{runtime=\"main\"} 2",
            "mimir_rs_tokio_workers{runtime=\"rpc\"} 3",
            "mimir_rs_tokio_alive_tasks{runtime=\"main\"}",
            "mimir_rs_tokio_global_queue_depth{runtime=\"rpc\"}",
            "mimir_rs_tokio_worker_busy_seconds_total{runtime=\"main\"}",
            "mimir_rs_tokio_worker_park_total{runtime=\"rpc\"}",
            "mimir_rs_nanos_slab_mapped_bytes ",
            "mimir_rs_nanos_slab_pages ",
            "mimir_rs_nanos_slab_reclaimed_pages_total ",
            "mimir_rs_scoped_worker_pool_threads ",
            "mimir_rs_scoped_worker_pool_executions_total ",
        ] {
            assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
        }

        handle.unregister_runtime("rpc");
        let mut buf2 = Vec::new();
        TextEncoder::new().encode(&reg.gather(), &mut buf2).unwrap();
        let text2 = String::from_utf8(buf2).unwrap();
        assert!(!text2.contains("runtime=\"rpc\""));
        assert!(text2.contains("runtime=\"main\""));
    }

    #[test]
    fn collector_omits_optional_groups_cleanly_when_unavailable() {
        let (collector, _handle) = RustMetricsCollector::with_sources(None, Box::new(|| None));
        let reg = Registry::new();
        reg.register(Box::new(collector)).unwrap();
        let names: Vec<String> = reg
            .gather()
            .into_iter()
            .map(|mf| mf.get_name().to_string())
            .collect();
        assert!(!names.iter().any(|n| n.starts_with("mimir_rs_jemalloc_")));
        assert!(!names.iter().any(|n| n.starts_with("mimir_rs_process_")));
        assert!(!names.iter().any(|n| n.starts_with("mimir_rs_tokio_")));
        assert!(names.iter().any(|n| n == "mimir_rs_nanos_slab_mapped_bytes"));
        assert!(names.iter().any(|n| n == "mimir_rs_scoped_worker_pool_threads"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn collects_live_linux_proc_status_metrics() {
        let (collector, _handle) = RustMetricsCollector::for_self();
        let fams = collector.collect();
        let map: HashMap<String, f64> = fams
            .into_iter()
            .filter_map(|mf| {
                let name = mf.get_name().to_string();
                let v = mf.get_metric().first()?.get_gauge().get_value();
                Some((name, v))
            })
            .collect();
        assert!(map.get("mimir_rs_process_rss_anon_bytes").copied().unwrap_or(0.0) > 0.0);
        assert!(map.get("mimir_rs_process_threads").copied().unwrap_or(0.0) >= 1.0);
    }
}
