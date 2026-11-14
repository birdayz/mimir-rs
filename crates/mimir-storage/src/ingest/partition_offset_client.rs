//! Kafka partition offset lookup client and Prometheus metrics.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/partition_offset_client.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::error::ParseResponseErrorCode;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{
    ApiKey, BrokerId, ListOffsetsRequest, ListOffsetsResponse, RequestHeader, ResponseHeader,
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use mimir_config::KafkaConfig;
use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts, Registry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::kafka_client::{KafkaClient, KafkaClientError, KAFKA_OFFSET_END};

#[derive(Clone)]
pub struct PartitionOffsetClientMetrics {
    pub last_produced_offset_requests_total: CounterVec,
    pub last_produced_offset_failures_total: CounterVec,
    pub last_produced_offset_latency: HistogramVec,
    pub partition_start_offset_requests_total: CounterVec,
    pub partition_start_offset_failures_total: CounterVec,
    pub partition_start_offset_latency: HistogramVec,
}

impl PartitionOffsetClientMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let last_produced_offset_requests_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_produced_offset_requests_total",
                "Total number of requests issued to get the last produced offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let last_produced_offset_failures_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_produced_offset_failures_total",
                "Total number of failed requests to get the last produced offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let last_produced_offset_latency = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
                "The duration of requests to fetch the last produced offset of a given partition.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let partition_start_offset_requests_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_partition_start_offset_requests_total",
                "Total number of requests issued to get the partition start offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let partition_start_offset_failures_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_partition_start_offset_failures_total",
                "Total number of failed requests to get the partition start offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let partition_start_offset_latency = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_partition_start_offset_request_duration_seconds",
                "The duration of requests to fetch the start offset of a given partition.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(last_produced_offset_requests_total.clone()));
            let _ = reg.register(Box::new(last_produced_offset_failures_total.clone()));
            let _ = reg.register(Box::new(last_produced_offset_latency.clone()));
            let _ = reg.register(Box::new(partition_start_offset_requests_total.clone()));
            let _ = reg.register(Box::new(partition_start_offset_failures_total.clone()));
            let _ = reg.register(Box::new(partition_start_offset_latency.clone()));
        }

        Self {
            last_produced_offset_requests_total,
            last_produced_offset_failures_total,
            last_produced_offset_latency,
            partition_start_offset_requests_total,
            partition_start_offset_failures_total,
            partition_start_offset_latency,
        }
    }
}

struct BatchConn {
    client_id: String,
    next_corr_id: AtomicI32,
    write_tx: mpsc::Sender<Bytes>,
    pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>>,
    alive: Arc<AtomicBool>,
}

impl BatchConn {
    async fn connect(
        addr: &str,
        client_id: String,
        dial_timeout: Duration,
    ) -> Result<Arc<Self>, KafkaClientError> {
        let stream = tokio::time::timeout(dial_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| KafkaClientError::ConnectTimeout {
                addr: addr.to_string(),
            })??;
        let _ = stream.set_nodelay(true);

        let (mut reader, mut writer) = stream.into_split();
        let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(128);
        let pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>> =
            Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));

        let alive_w = Arc::clone(&alive);
        tokio::spawn(async move {
            while let Some(frame) = write_rx.recv().await {
                if writer.write_all(&frame).await.is_err() {
                    break;
                }
            }
            alive_w.store(false, Ordering::SeqCst);
        });

        let pending_r = Arc::clone(&pending);
        let alive_r = Arc::clone(&alive);
        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];
                if reader.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let frame_len = i32::from_be_bytes(len_buf) as usize;
                if !(4..=128 * 1024 * 1024).contains(&frame_len) {
                    break;
                }
                let mut body = vec![0u8; frame_len];
                if reader.read_exact(&mut body).await.is_err() {
                    break;
                }
                let corr_id = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let tx = pending_r.lock().remove(&corr_id);
                if let Some(tx) = tx {
                    let _ = tx.send(Bytes::from(body));
                }
            }
            alive_r.store(false, Ordering::SeqCst);
            let drained: Vec<_> = pending_r.lock().drain().map(|(_, v)| v).collect();
            for tx in drained {
                drop(tx);
            }
        });

        Ok(Arc::new(Self {
            client_id,
            next_corr_id: AtomicI32::new(1),
            write_tx,
            pending,
            alive,
        }))
    }

    async fn send_list_offsets(
        &self,
        req: &ListOffsetsRequest,
        timeout: Duration,
    ) -> Result<ListOffsetsResponse, KafkaClientError> {
        if !self.alive.load(Ordering::SeqCst) {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let ver = 2i16;
        let corr_id = self.next_corr_id.fetch_add(1, Ordering::SeqCst);
        let req_header_ver = ListOffsetsRequest::header_version(ver);
        let mut hdr = RequestHeader::default();
        hdr.request_api_key = ApiKey::ListOffsets as i16;
        hdr.request_api_version = ver;
        hdr.correlation_id = corr_id;
        hdr.client_id = Some(StrBytes::from_string(self.client_id.clone()));

        let mut payload = BytesMut::new();
        hdr.encode(&mut payload, req_header_ver)?;
        req.encode(&mut payload, ver)?;

        let mut frame = BytesMut::with_capacity(4 + payload.len());
        frame.put_i32(payload.len() as i32);
        frame.extend_from_slice(&payload);

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(corr_id, tx);
        if self.write_tx.send(frame.freeze()).await.is_err() {
            self.pending.lock().remove(&corr_id);
            return Err(KafkaClientError::ConnectionClosed);
        }

        let mut resp_buf = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                self.pending.lock().remove(&corr_id);
                KafkaClientError::RequestTimeout(timeout)
            })?
            .map_err(|_| KafkaClientError::ConnectionClosed)?;

        let resp_header_ver = ListOffsetsResponse::header_version(ver);
        let _resp_hdr = ResponseHeader::decode(&mut resp_buf, resp_header_ver)?;
        Ok(ListOffsetsResponse::decode(&mut resp_buf, ver)?)
    }
}

/// Topic-agnostic client to read partition start & last produced offsets.
#[derive(Clone)]
pub struct PartitionOffsetClient {
    client: Arc<KafkaClient>,
    metrics: PartitionOffsetClientMetrics,
    batch_addrs: Vec<String>,
    client_id: String,
    dial_timeout: Duration,
    request_timeout: Duration,
    batch_conn: Arc<Mutex<Option<Arc<BatchConn>>>>,
    closed: Arc<AtomicBool>,
}

impl PartitionOffsetClient {
    pub fn new(client: Arc<KafkaClient>, registry: Option<&Registry>) -> Self {
        Self {
            client,
            metrics: PartitionOffsetClientMetrics::new(registry),
            batch_addrs: Vec::new(),
            client_id: String::new(),
            dial_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
            batch_conn: Arc::new(Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn from_config(cfg: &KafkaConfig, registry: Option<&Registry>) -> Result<Self, KafkaClientError> {
        let client = Arc::new(KafkaClient::from_config(cfg)?);
        Ok(Self::with_config(client, cfg, registry))
    }

    pub fn with_config(
        client: Arc<KafkaClient>,
        cfg: &KafkaConfig,
        registry: Option<&Registry>,
    ) -> Self {
        let mut addrs = Vec::new();
        for raw in &cfg.address.0 {
            for part in raw.split(',') {
                let t = part.trim();
                if !t.is_empty() {
                    addrs.push(t.to_string());
                }
            }
        }
        let dial_timeout = Duration::from_nanos(cfg.dial_timeout.as_nanos().max(0) as u64)
            .max(Duration::from_millis(100));
        let retry_timeout =
            Duration::from_nanos(cfg.last_produced_offset_retry_timeout.as_nanos().max(0) as u64)
                .max(Duration::from_millis(100));
        Self {
            client,
            metrics: PartitionOffsetClientMetrics::new(registry),
            batch_addrs: addrs,
            client_id: cfg.client_id.clone(),
            dial_timeout,
            request_timeout: retry_timeout,
            batch_conn: Arc::new(Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Ok(mut guard) = self.batch_conn.try_lock() {
            if let Some(conn) = guard.take() {
                conn.alive.store(false, Ordering::SeqCst);
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn kafka_client(&self) -> &Arc<KafkaClient> {
        &self.client
    }

    /// Fetches the last produced offset for `(topic, partition_id)`, or `-1` if empty.
    pub async fn fetch_partition_last_produced_offset(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<i64, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let part_label = partition_id.to_string();
        let reqs = self
            .metrics
            .last_produced_offset_requests_total
            .with_label_values(&[topic, &part_label]);
        let fails = self
            .metrics
            .last_produced_offset_failures_total
            .with_label_values(&[topic, &part_label]);
        let lat = self
            .metrics
            .last_produced_offset_latency
            .with_label_values(&[topic, &part_label]);

        reqs.inc();
        let start = Instant::now();
        let res = self
            .client
            .fetch_partition_last_produced_offset(topic, partition_id)
            .await;
        lat.observe(start.elapsed().as_secs_f64());
        if res.is_err() {
            fails.inc();
        }
        res
    }

    /// Fetches the start offset (`ListOffsets -2`) for `(topic, partition_id)`.
    pub async fn fetch_partition_start_offset(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<i64, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let part_label = partition_id.to_string();
        let reqs = self
            .metrics
            .partition_start_offset_requests_total
            .with_label_values(&[topic, &part_label]);
        let fails = self
            .metrics
            .partition_start_offset_failures_total
            .with_label_values(&[topic, &part_label]);
        let lat = self
            .metrics
            .partition_start_offset_latency
            .with_label_values(&[topic, &part_label]);

        reqs.inc();
        let start = Instant::now();
        let res = self
            .client
            .fetch_partition_start_offset(topic, partition_id)
            .await;
        lat.observe(start.elapsed().as_secs_f64());
        if res.is_err() {
            fails.inc();
        }
        res
    }

    async fn get_batch_conn(&self) -> Result<Arc<BatchConn>, KafkaClientError> {
        let mut guard = self.batch_conn.lock().await;
        if let Some(ref c) = *guard {
            if c.alive.load(Ordering::SeqCst) {
                return Ok(Arc::clone(c));
            }
        }
        let mut last_err = None;
        for addr in &self.batch_addrs {
            match BatchConn::connect(addr, self.client_id.clone(), self.dial_timeout).await {
                Ok(c) => {
                    *guard = Some(Arc::clone(&c));
                    return Ok(c);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(KafkaClientError::NoSeedBrokers))
    }

    async fn fetch_partitions_end_offsets_single_request(
        &self,
        partitions_by_topic: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaClientError> {
        let conn = self.get_batch_conn().await?;
        let mut req = ListOffsetsRequest::default();
        req.replica_id = BrokerId(-1);
        req.isolation_level = 0;

        for (topic, pids) in partitions_by_topic {
            let mut lt = ListOffsetsTopic::default();
            lt.name = TopicName(StrBytes::from_string(topic.clone()));
            for &pid in pids {
                let mut lp = ListOffsetsPartition::default();
                lp.partition_index = pid;
                lp.current_leader_epoch = -1;
                lp.timestamp = KAFKA_OFFSET_END;
                lp.max_num_offsets = 1;
                lt.partitions.push(lp);
            }
            req.topics.push(lt);
        }

        let resp = conn.send_list_offsets(&req, self.request_timeout).await?;

        let mut out: HashMap<String, HashMap<i32, i64>> =
            HashMap::with_capacity(partitions_by_topic.len());
        for topic in partitions_by_topic.keys() {
            out.insert(topic.clone(), HashMap::new());
        }

        for rt in resp.topics {
            let topic_name = rt.name.0.as_str();
            let Some(pmap) = out.get_mut(topic_name) else {
                return Err(KafkaClientError::UnexpectedResponse(format!(
                    "unexpected topic in the response: {topic_name}"
                )));
            };
            for rp in rt.partitions {
                if let Some(err) = rp.error_code.err() {
                    return Err(KafkaClientError::Broker(err));
                }
                pmap.insert(rp.partition_index, rp.offset - 1);
            }
        }

        for (topic, pids) in partitions_by_topic {
            let pmap = out.get(topic);
            for &pid in pids {
                if pmap.and_then(|m| m.get(&pid)).is_none() {
                    return Err(KafkaClientError::UnexpectedResponse(format!(
                        "the offset for topic {topic:?} partition {pid} is missing in the response"
                    )));
                }
            }
        }

        Ok(out)
    }

    /// Fetches last produced offsets for all requested `(topic, partition_ids)`.
    pub async fn fetch_partitions_last_produced_offsets(
        &self,
        partitions_by_topic: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let num_partitions: usize = partitions_by_topic.values().map(|v| v.len()).sum();
        if num_partitions == 0 {
            return Ok(HashMap::new());
        }

        let topic_label = if partitions_by_topic.len() == 1 {
            partitions_by_topic
                .keys()
                .next()
                .map(String::as_str)
                .unwrap_or("mixed")
        } else {
            "mixed"
        };

        let reqs = self
            .metrics
            .last_produced_offset_requests_total
            .with_label_values(&[topic_label, "mixed"]);
        let fails = self
            .metrics
            .last_produced_offset_failures_total
            .with_label_values(&[topic_label, "mixed"]);
        let lat = self
            .metrics
            .last_produced_offset_latency
            .with_label_values(&[topic_label, "mixed"]);

        reqs.inc();
        let start = Instant::now();

        if !self.batch_addrs.is_empty() {
            let res = self
                .fetch_partitions_end_offsets_single_request(partitions_by_topic)
                .await;
            lat.observe(start.elapsed().as_secs_f64());
            if res.is_err() {
                fails.inc();
            }
            return res;
        }

        let mut out: HashMap<String, HashMap<i32, i64>> =
            HashMap::with_capacity(partitions_by_topic.len());
        for (topic, pids) in partitions_by_topic {
            let mut pmap = HashMap::with_capacity(pids.len());
            for &pid in pids {
                match self
                    .client
                    .fetch_partition_last_produced_offset(topic, pid)
                    .await
                {
                    Ok(off) => {
                        pmap.insert(pid, off);
                    }
                    Err(e) => {
                        lat.observe(start.elapsed().as_secs_f64());
                        fails.inc();
                        return Err(e);
                    }
                }
            }
            out.insert(topic.clone(), pmap);
        }

        lat.observe(start.elapsed().as_secs_f64());
        Ok(out)
    }

    /// Lists sorted partition IDs for `topic`.
    pub async fn list_topic_partition_ids(
        &self,
        topic: &str,
    ) -> Result<Vec<i32>, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        self.client.list_topic_partition_ids(topic).await
    }
}
