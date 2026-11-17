//! `process_*` metrics, identical in names, help, and semantics to Go Mimir's registrations.
//!
//! Ported from:
//! - github.com/prometheus/client_golang/prometheus/process_collector.go and
//!   process_collector_procfsenabled.go (`collectors.NewProcessCollector`, Linux procfs path):
//!   `process_cpu_seconds_total`, `process_open_fds`, `process_max_fds`,
//!   `process_virtual_memory_bytes`, `process_virtual_memory_max_bytes`,
//!   `process_resident_memory_bytes`, `process_start_time_seconds`,
//!   `process_network_receive_bytes_total`, `process_network_transmit_bytes_total`.
//! - github.com/grafana/mimir/pkg/util/process/collector.go (`process_memory_map_areas`,
//!   `process_memory_map_areas_limit`), registered in pkg/mimir/mimir.go.
//!
//! As in Go, a value that cannot be read is omitted (Go reports an invalid metric for it, which
//! fails the scrape only for that collector); `go_*` runtime metrics have no Rust equivalent and
//! are intentionally not emitted (see PORT_STATUS.md).

use std::collections::HashMap;

use prometheus::core::{Collector, Desc};
use prometheus::proto;

/// procfs assumes `USER_HZ` = 100 (`userHZ` in github.com/prometheus/procfs).
const USER_HZ: f64 = 100.0;

pub struct ProcessCollector {
    proc_root: String,
    descs: Vec<Desc>,
}

const METRICS: &[(&str, &str, proto::MetricType)] = &[
    ("process_cpu_seconds_total", "Total user and system CPU time spent in seconds.", proto::MetricType::COUNTER),
    ("process_open_fds", "Number of open file descriptors.", proto::MetricType::GAUGE),
    ("process_max_fds", "Maximum number of open file descriptors.", proto::MetricType::GAUGE),
    ("process_virtual_memory_bytes", "Virtual memory size in bytes.", proto::MetricType::GAUGE),
    ("process_virtual_memory_max_bytes", "Maximum amount of virtual memory available in bytes.", proto::MetricType::GAUGE),
    ("process_resident_memory_bytes", "Resident memory size in bytes.", proto::MetricType::GAUGE),
    ("process_start_time_seconds", "Start time of the process since unix epoch in seconds.", proto::MetricType::GAUGE),
    ("process_network_receive_bytes_total", "Number of bytes received by the process over the network.", proto::MetricType::COUNTER),
    ("process_network_transmit_bytes_total", "Number of bytes sent by the process over the network.", proto::MetricType::COUNTER),
    ("process_memory_map_areas", "Number of memory map areas allocated by the process.", proto::MetricType::GAUGE),
    ("process_memory_map_areas_limit", "Maximum number of memory map ares the process can allocate.", proto::MetricType::GAUGE),
];

impl ProcessCollector {
    /// Collector for the current process using `/proc`. `None` when procfs is unavailable
    /// (Go's `canCollectProcess` / `isSupported`).
    pub fn for_self() -> Option<Self> {
        Self::new("/proc")
    }

    fn new(proc_root: &str) -> Option<Self> {
        if !std::path::Path::new(&format!("{proc_root}/stat")).exists() {
            return None;
        }
        let descs = METRICS
            .iter()
            .map(|(n, h, _)| {
                Desc::new(n.to_string(), h.to_string(), vec![], HashMap::new())
                    .expect("valid process metric desc")
            })
            .collect();
        Some(Self {
            proc_root: proc_root.to_string(),
            descs,
        })
    }

    fn read(&self, rel: &str) -> Option<String> {
        std::fs::read_to_string(format!("{}/{rel}", self.proc_root)).ok()
    }

    fn values(&self) -> HashMap<&'static str, f64> {
        let mut v = HashMap::new();
        if let Some(stat) = self.read("self/stat") {
            if let Some(fields) = parse_stat(&stat) {
                let page = page_size() as f64;
                v.insert("process_cpu_seconds_total", (fields.utime + fields.stime) as f64 / USER_HZ);
                v.insert("process_virtual_memory_bytes", fields.vsize as f64);
                v.insert("process_resident_memory_bytes", fields.rss as f64 * page);
                if let Some(btime) = self.read("stat").as_deref().and_then(parse_btime) {
                    v.insert(
                        "process_start_time_seconds",
                        btime as f64 + fields.starttime as f64 / USER_HZ,
                    );
                }
            }
        }
        if let Ok(rd) = std::fs::read_dir(format!("{}/self/fd", self.proc_root)) {
            v.insert("process_open_fds", rd.count() as f64);
        }
        if let Some(limits) = self.read("self/limits") {
            if let Some(l) = parse_limit(&limits, "Max open files") {
                v.insert("process_max_fds", l);
            }
            if let Some(l) = parse_limit(&limits, "Max address space") {
                v.insert("process_virtual_memory_max_bytes", l);
            }
        }
        if let Some(netstat) = self.read("self/net/netstat") {
            let ip_ext = parse_netstat_section(&netstat, "IpExt");
            v.insert(
                "process_network_receive_bytes_total",
                ip_ext.get("InOctets").copied().unwrap_or(0.0),
            );
            v.insert(
                "process_network_transmit_bytes_total",
                ip_ext.get("OutOctets").copied().unwrap_or(0.0),
            );
        }
        if let Some(maps) = self.read("self/maps") {
            v.insert("process_memory_map_areas", maps.lines().count() as f64);
        }
        if let Some(limit) = self.read("sys/vm/max_map_count") {
            if let Ok(n) = limit.trim().parse::<i64>() {
                v.insert("process_memory_map_areas_limit", n as f64);
            }
        }
        v
    }
}

struct StatFields {
    utime: u64,
    stime: u64,
    starttime: u64,
    vsize: u64,
    rss: i64,
}

/// Parses `/proc/<pid>/stat` (fields after the parenthesised command name).
fn parse_stat(s: &str) -> Option<StatFields> {
    let after = &s[s.rfind(')')? + 2..];
    let f: Vec<&str> = after.split_whitespace().collect();
    // `after` starts at field 3 (state); field N is f[N - 3].
    let get = |n: usize| f.get(n - 3).copied();
    Some(StatFields {
        utime: get(14)?.parse().ok()?,
        stime: get(15)?.parse().ok()?,
        starttime: get(22)?.parse().ok()?,
        vsize: get(23)?.parse().ok()?,
        rss: get(24)?.parse().ok()?,
    })
}

fn parse_btime(stat: &str) -> Option<u64> {
    stat.lines()
        .find_map(|l| l.strip_prefix("btime "))
        .and_then(|v| v.trim().parse().ok())
}

/// procfs `Limits`: soft limit column; `unlimited` is `^uint64(0)`.
fn parse_limit(limits: &str, name: &str) -> Option<f64> {
    let line = limits.lines().find(|l| l.starts_with(name))?;
    let soft = line[name.len()..].split_whitespace().next()?;
    if soft == "unlimited" {
        return Some(u64::MAX as f64);
    }
    soft.parse::<u64>().ok().map(|v| v as f64)
}

fn parse_netstat_section(s: &str, section: &str) -> HashMap<String, f64> {
    let prefix = format!("{section}:");
    let lines: Vec<&str> = s.lines().filter(|l| l.starts_with(&prefix)).collect();
    let mut out = HashMap::new();
    if lines.len() >= 2 {
        let names = lines[0].split_whitespace().skip(1);
        let values = lines[1].split_whitespace().skip(1);
        for (n, v) in names.zip(values) {
            if let Ok(v) = v.parse::<f64>() {
                out.insert(n.to_string(), v);
            }
        }
    }
    out
}

fn page_size() -> usize {
    // SAFETY: sysconf has no preconditions.
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if ps > 0 {
        ps as usize
    } else {
        4096
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let values = self.values();
        let mut out = Vec::new();
        for (name, help, typ) in METRICS {
            let Some(v) = values.get(name) else { continue };
            let mut m = proto::Metric::default();
            match typ {
                proto::MetricType::COUNTER => {
                    let mut c = proto::Counter::default();
                    c.set_value(*v);
                    m.set_counter(c);
                }
                _ => {
                    let mut g = proto::Gauge::default();
                    g.set_value(*v);
                    m.set_gauge(g);
                }
            }
            let mut mf = proto::MetricFamily::default();
            mf.set_name(name.to_string());
            mf.set_help(help.to_string());
            mf.set_field_type(*typ);
            mf.set_metric(vec![m].into());
            out.push(mf);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_files() {
        let stat = "1234 (my (weird) cmd) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 8 0 4242 1000000 300 18446744073709551615";
        let f = parse_stat(stat).unwrap();
        assert_eq!((f.utime, f.stime, f.starttime, f.vsize, f.rss), (250, 50, 4242, 1000000, 300));
        let limits = "Limit                     Soft Limit           Hard Limit           Units     \n\
                      Max open files            1024                 524288               files     \n\
                      Max address space         unlimited            unlimited            bytes     \n";
        assert_eq!(parse_limit(limits, "Max open files"), Some(1024.0));
        assert_eq!(parse_limit(limits, "Max address space"), Some(1.8446744073709552e19));
        let netstat = "TcpExt: A B\nTcpExt: 1 2\nIpExt: InNoRoutes InOctets OutOctets\nIpExt: 0 123 456\n";
        let ip = parse_netstat_section(netstat, "IpExt");
        assert_eq!(ip["InOctets"], 123.0);
        assert_eq!(ip["OutOctets"], 456.0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn collects_all_families_on_linux() {
        let c = ProcessCollector::for_self().unwrap();
        let names: Vec<String> = c.collect().iter().map(|m| m.get_name().to_string()).collect();
        for (n, _, _) in METRICS {
            assert!(names.iter().any(|x| x == n), "missing {n}");
        }
    }
}
