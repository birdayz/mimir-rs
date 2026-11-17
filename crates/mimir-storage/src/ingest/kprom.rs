//! franz-go `kprom` client hook metrics, as configured for the partition reader's Kafka client.
//!
//! Ported from:
//! - github.com/twmb/franz-go/plugin/kprom/kprom.go (`OnNewClient`, `OnBrokerConnect`,
//!   `OnBrokerDisconnect`, `OnBrokerE2E`, `OnFetchBatchRead`)
//! - github.com/twmb/franz-go/plugin/kprom/config.go (`newCfg` defaults, `FetchAndProduceDetail`)
//! - github.com/grafana/mimir/pkg/storage/ingest/reader_client.go (`NewKafkaReaderClientMetrics`)
//!
//! Mimir builds them with
//! `kprom.NewMetrics(prefix, Registerer(WrapRegistererWith({component}, reg)),
//! FetchAndProduceDetail(Batches, Records, CompressedBytes, UncompressedBytes))`, so:
//! - broker metrics carry the default `node_id` label (`kgo.NodeName`: the broker ID, or
//!   `seed_<i>` for the i-th seed broker);
//! - fetch / produce metrics carry NO label (`FetchAndProduceDetail` without `ByNode` / `ByTopic`
//!   resets the label list);
//! - no histogram is ever observed (no `kprom.Histograms` option), so the histogram vectors are
//!   never exposed and are not ported;
//! - every metric gets the `component` constant label.
//!
//! Vectors are only exposed once a child exists, exactly like client_golang: e.g.
//! `fetch_bytes_total` appears with the first fetched batch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{CounterVec, Opts, Registry};

/// `kgo.NodeName`: `seed_<i>` for seed brokers, the broker ID otherwise.
pub fn seed_node_name(seed_index: usize) -> String {
    format!("seed_{seed_index}")
}

/// kgo `FetchBatchMetrics` (the fields kprom uses).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchBatchMetrics {
    pub num_records: usize,
    pub uncompressed_bytes: usize,
    pub compressed_bytes: usize,
}

/// The `kprom.Metrics` hooks of a Kafka client.
pub struct KpromMetrics {
    connects_total: CounterVec,
    connect_errors_total: CounterVec,
    disconnects_total: CounterVec,
    write_bytes_total: CounterVec,
    write_errors_total: CounterVec,
    read_bytes_total: CounterVec,
    read_errors_total: CounterVec,
    fetch_compressed_bytes: CounterVec,
    fetch_uncompressed_bytes: CounterVec,
    fetch_batches_total: CounterVec,
    fetch_records_total: CounterVec,
    /// `client.BufferedFetchRecords()` / `client.BufferedFetchBytes()`.
    buffered_fetch_records: Arc<AtomicI64>,
    buffered_fetch_bytes: Arc<AtomicI64>,
}

impl KpromMetrics {
    /// `NewKafkaReaderClientMetrics(prefix, component, reg)`; `prefix` is the kprom namespace.
    pub fn new(prefix: &str, component: &str, reg: Option<&Registry>) -> Arc<Self> {
        let mut const_labels = HashMap::new();
        const_labels.insert("component".to_string(), component.to_string());
        let vec = |name: &str, help: &str, labels: &[&str]| {
            let opts = Opts::new(format!("{prefix}_{name}"), help).const_labels(const_labels.clone());
            let v = CounterVec::new(opts, labels).expect("valid metric");
            if let Some(r) = reg {
                let _ = r.register(Box::new(v.clone()));
            }
            v
        };
        const NODE: &[&str] = &["node_id"];
        let m = Self {
            connects_total: vec("connects_total", "Total number of connections opened", NODE),
            connect_errors_total: vec("connect_errors_total", "Total number of connection errors", NODE),
            disconnects_total: vec("disconnects_total", "Total number of connections closed", NODE),
            write_bytes_total: vec(
                "write_bytes_total",
                "Total number of bytes written to the TCP connection. The bytes count is tracked after compression (when used).",
                NODE,
            ),
            write_errors_total: vec("write_errors_total", "Total number of write errors", NODE),
            read_bytes_total: vec(
                "read_bytes_total",
                "Total number of bytes read from the TCP connection. The bytes count is tracked before uncompression (when used).",
                NODE,
            ),
            read_errors_total: vec("read_errors_total", "Total number of read errors", NODE),
            fetch_compressed_bytes: vec("fetch_compressed_bytes_total", "Total number of compressed bytes fetched", &[]),
            fetch_uncompressed_bytes: vec("fetch_bytes_total", "Total number of uncompressed bytes fetched", &[]),
            fetch_batches_total: vec("fetch_batches_total", "Total number of batches fetched", &[]),
            fetch_records_total: vec("fetch_records_total", "Total number of records fetched", &[]),
            buffered_fetch_records: Arc::new(AtomicI64::new(0)),
            buffered_fetch_bytes: Arc::new(AtomicI64::new(0)),
        };
        // The produce counters (`produce_*`) are registered by kprom but never get a child on a
        // reader client, so they are never exposed; the buffered gauges always are.
        if let Some(r) = reg {
            let gauges = BufferedGauges::new(
                prefix,
                &const_labels,
                Arc::clone(&m.buffered_fetch_records),
                Arc::clone(&m.buffered_fetch_bytes),
            );
            let _ = r.register(Box::new(gauges));
        }
        Arc::new(m)
    }

    /// `OnBrokerConnect`.
    pub fn on_broker_connect(&self, node: &str, ok: bool) {
        if ok {
            self.connects_total.with_label_values(&[node]).inc();
        } else {
            self.connect_errors_total.with_label_values(&[node]).inc();
        }
    }

    /// `OnBrokerDisconnect`.
    pub fn on_broker_disconnect(&self, node: &str) {
        self.disconnects_total.with_label_values(&[node]).inc();
    }

    /// `OnBrokerE2E` for a request whose write failed.
    pub fn on_write_error(&self, node: &str) {
        self.write_errors_total.with_label_values(&[node]).inc();
    }

    /// `OnBrokerE2E` for a request written successfully whose response could not be read.
    pub fn on_read_error(&self, node: &str, bytes_written: usize) {
        self.write_bytes_total
            .with_label_values(&[node])
            .inc_by(bytes_written as f64);
        self.read_errors_total.with_label_values(&[node]).inc();
    }

    /// `OnBrokerE2E` for a successful round trip.
    pub fn on_e2e(&self, node: &str, bytes_written: usize, bytes_read: usize) {
        self.write_bytes_total
            .with_label_values(&[node])
            .inc_by(bytes_written as f64);
        self.read_bytes_total
            .with_label_values(&[node])
            .inc_by(bytes_read as f64);
    }

    /// `OnFetchBatchRead`.
    pub fn on_fetch_batch_read(&self, m: FetchBatchMetrics) {
        self.fetch_uncompressed_bytes
            .with_label_values(&[])
            .inc_by(m.uncompressed_bytes as f64);
        self.fetch_compressed_bytes
            .with_label_values(&[])
            .inc_by(m.compressed_bytes as f64);
        self.fetch_batches_total.with_label_values(&[]).inc();
        self.fetch_records_total
            .with_label_values(&[])
            .inc_by(m.num_records as f64);
    }

    /// Records/bytes buffered within the client (consumer path, `fetch_concurrency_max = 0`).
    pub fn set_buffered_fetch(&self, records: i64, bytes: i64) {
        self.buffered_fetch_records.store(records, Ordering::Relaxed);
        self.buffered_fetch_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn buffered_fetch_records(&self) -> i64 {
        self.buffered_fetch_records.load(Ordering::Relaxed)
    }

    pub fn buffered_fetch_bytes(&self) -> i64 {
        self.buffered_fetch_bytes.load(Ordering::Relaxed)
    }
}

/// kprom's four `GaugeFunc`s (`buffered_produce_records_total`, `buffered_produce_bytes`,
/// `buffered_fetch_records_total`, `buffered_fetch_bytes`).
struct BufferedGauges {
    descs: Vec<Desc>,
    fetch_records: Arc<AtomicI64>,
    fetch_bytes: Arc<AtomicI64>,
}

impl BufferedGauges {
    fn new(
        prefix: &str,
        const_labels: &HashMap<String, String>,
        fetch_records: Arc<AtomicI64>,
        fetch_bytes: Arc<AtomicI64>,
    ) -> Self {
        let d = |name: &str, help: &str| {
            Desc::new(format!("{prefix}_{name}"), help.to_string(), vec![], const_labels.clone())
                .expect("valid desc")
        };
        Self {
            descs: vec![
                d(
                    "buffered_produce_records_total",
                    "Total number of records buffered within the client ready to be produced",
                ),
                d(
                    "buffered_produce_bytes",
                    "Total number of bytes buffered within the client ready to be produced",
                ),
                d(
                    "buffered_fetch_records_total",
                    "Total number of records buffered within the client ready to be consumed",
                ),
                d(
                    "buffered_fetch_bytes",
                    "Total number of bytes buffered within the client ready to be consumed",
                ),
            ],
            fetch_records,
            fetch_bytes,
        }
    }
}

impl Collector for BufferedGauges {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let values = [
            0.0,
            0.0,
            self.fetch_records.load(Ordering::Relaxed) as f64,
            self.fetch_bytes.load(Ordering::Relaxed) as f64,
        ];
        self.descs
            .iter()
            .zip(values)
            .map(|(desc, v)| {
                let mut g = prometheus::proto::Gauge::new();
                g.set_value(v);
                let mut m = prometheus::proto::Metric::new();
                m.set_label(desc.const_label_pairs.clone().into());
                m.set_gauge(g);
                let mut mf = MetricFamily::new();
                mf.set_name(desc.fq_name.clone());
                mf.set_help(desc.help.clone());
                mf.set_field_type(prometheus::proto::MetricType::GAUGE);
                mf.set_metric(vec![m].into());
                mf
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};

    fn render(reg: &Registry) -> String {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn exposes_like_kprom_reader_client_metrics() {
        let reg = Registry::new();
        let m = KpromMetrics::new("cortex_ingest_storage_reader", "partition-reader", Some(&reg));
        let out = render(&reg);
        // Only the GaugeFuncs are exposed before any event.
        assert!(out.contains(
            "cortex_ingest_storage_reader_buffered_fetch_records_total{component=\"partition-reader\"} 0"
        ));
        assert!(out.contains(
            "cortex_ingest_storage_reader_buffered_produce_bytes{component=\"partition-reader\"} 0"
        ));
        assert!(!out.contains("connects_total"));
        assert!(!out.contains("fetch_bytes_total"));

        m.on_broker_connect("seed_0", true);
        m.on_broker_connect("1", false);
        m.on_e2e("seed_0", 10, 20);
        m.on_read_error("2", 7);
        m.on_fetch_batch_read(FetchBatchMetrics {
            num_records: 3,
            uncompressed_bytes: 300,
            compressed_bytes: 100,
        });
        m.set_buffered_fetch(5, 50);
        let out = render(&reg);
        for want in [
            "cortex_ingest_storage_reader_connects_total{component=\"partition-reader\",node_id=\"seed_0\"} 1",
            "cortex_ingest_storage_reader_connect_errors_total{component=\"partition-reader\",node_id=\"1\"} 1",
            "cortex_ingest_storage_reader_write_bytes_total{component=\"partition-reader\",node_id=\"seed_0\"} 10",
            "cortex_ingest_storage_reader_read_bytes_total{component=\"partition-reader\",node_id=\"seed_0\"} 20",
            "cortex_ingest_storage_reader_read_errors_total{component=\"partition-reader\",node_id=\"2\"} 1",
            "cortex_ingest_storage_reader_fetch_bytes_total{component=\"partition-reader\"} 300",
            "cortex_ingest_storage_reader_fetch_compressed_bytes_total{component=\"partition-reader\"} 100",
            "cortex_ingest_storage_reader_fetch_batches_total{component=\"partition-reader\"} 1",
            "cortex_ingest_storage_reader_fetch_records_total{component=\"partition-reader\"} 3",
            "cortex_ingest_storage_reader_buffered_fetch_records_total{component=\"partition-reader\"} 5",
            "cortex_ingest_storage_reader_buffered_fetch_bytes{component=\"partition-reader\"} 50",
        ] {
            assert!(out.contains(want), "missing {want} in\n{out}");
        }
    }
}
