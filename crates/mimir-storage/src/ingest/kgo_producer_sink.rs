//! Kafka producer sink, batch/request wire encoders, promise ring flush, and txn coordinator
//! sharding ported from franz-go sink.go, producer.go, txn.go, and client.go.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use kafka_protocol::messages::write_txn_markers_request::{
    WritableTxnMarker, WritableTxnMarkerTopic,
};
use kafka_protocol::messages::WriteTxnMarkersRequest;
use kafka_protocol::protocol::StrBytes;

use super::compression::{compress_record_batch, KafkaCompression};
use super::ring::PromiseRing;

pub fn append_zigzag_i32(dst: &mut Vec<u8>, v: i32) {
    append_uvarint(dst, (((v << 1) ^ (v >> 31)) as u32) as u64);
}

pub fn append_zigzag_i64(dst: &mut Vec<u8>, v: i64) {
    append_uvarint(dst, ((v << 1) ^ (v >> 63)) as u64);
}

pub fn append_uvarint(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

#[derive(Debug, Clone)]
pub struct WirePromisedRec {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub timestamp_delta: i64,
}

impl WirePromisedRec {
    pub fn append_to(&self, dst: &mut Vec<u8>, offset_delta: i32) {
        let mut body = Vec::new();
        body.push(0);
        append_zigzag_i64(&mut body, self.timestamp_delta);
        append_zigzag_i32(&mut body, offset_delta);
        match &self.key {
            None => append_zigzag_i32(&mut body, -1),
            Some(k) => {
                append_zigzag_i32(&mut body, k.len() as i32);
                body.extend_from_slice(k);
            }
        }
        match &self.value {
            None => append_zigzag_i32(&mut body, -1),
            Some(v) => {
                append_zigzag_i32(&mut body, v.len() as i32);
                body.extend_from_slice(v);
            }
        }
        append_zigzag_i32(&mut body, self.headers.len() as i32);
        for (hk, hv) in &self.headers {
            append_zigzag_i32(&mut body, hk.len() as i32);
            body.extend_from_slice(hk.as_bytes());
            match hv {
                None => append_zigzag_i32(&mut body, -1),
                Some(val) => {
                    append_zigzag_i32(&mut body, val.len() as i32);
                    body.extend_from_slice(val);
                }
            }
        }
        append_zigzag_i32(dst, body.len() as i32);
        dst.extend_from_slice(&body);
    }
}

#[derive(Debug, Clone)]
pub struct WireRecBatch {
    pub seq: i32,
    pub first_timestamp: i64,
    pub max_timestamp_delta: i64,
    pub records: Vec<WirePromisedRec>,
}

impl WireRecBatch {
    pub fn append_to(
        &self,
        dst: &mut Vec<u8>,
        produce_version: i16,
        producer_id: i64,
        producer_epoch: i16,
        transactional: bool,
        compression: KafkaCompression,
    ) {
        let mut recs_raw = Vec::new();
        for (idx, r) in self.records.iter().enumerate() {
            r.append_to(&mut recs_raw, idx as i32);
        }
        let (payload, comp_bits) = if matches!(compression, KafkaCompression::None) {
            (recs_raw, 0i16)
        } else {
            let mut src = bytes::BytesMut::from(recs_raw.as_slice());
            let mut out = bytes::BytesMut::new();
            compress_record_batch(&mut src, &mut out, compression.to_protocol())
                .expect("compress record batch");
            let bits = match compression {
                KafkaCompression::None => 0,
                KafkaCompression::Gzip => 1,
                KafkaCompression::Snappy => 2,
                KafkaCompression::Lz4 => 3,
                KafkaCompression::Zstd => 4,
            };
            (out.to_vec(), bits)
        };

        let mut attrs = comp_bits;
        if transactional {
            attrs |= 0x0010;
        }
        let last_offset_delta = (self.records.len() as i32).saturating_sub(1);

        let mut batch = Vec::with_capacity(61 + payload.len());
        batch.extend_from_slice(&0i64.to_be_bytes());
        batch.extend_from_slice(&0i32.to_be_bytes());
        batch.extend_from_slice(&(-1i32).to_be_bytes());
        batch.push(2);
        batch.extend_from_slice(&0u32.to_be_bytes());
        batch.extend_from_slice(&attrs.to_be_bytes());
        batch.extend_from_slice(&last_offset_delta.to_be_bytes());
        batch.extend_from_slice(&self.first_timestamp.to_be_bytes());
        batch.extend_from_slice(&(self.first_timestamp + self.max_timestamp_delta).to_be_bytes());
        batch.extend_from_slice(&producer_id.to_be_bytes());
        batch.extend_from_slice(&producer_epoch.to_be_bytes());
        batch.extend_from_slice(&self.seq.to_be_bytes());
        batch.extend_from_slice(&(self.records.len() as i32).to_be_bytes());
        batch.extend_from_slice(&payload);

        let batch_len = (batch.len() - 12) as i32;
        batch[8..12].copy_from_slice(&batch_len.to_be_bytes());
        let crc = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());

        if produce_version >= 9 {
            append_uvarint(dst, (batch.len() + 1) as u64);
        } else {
            dst.extend_from_slice(&(batch.len() as i32).to_be_bytes());
        }
        dst.extend_from_slice(&batch);
    }
}

#[derive(Debug, Clone)]
pub struct NullBatchProduceRequest {
    pub version: i16,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<(String, Vec<i32>)>,
}

impl NullBatchProduceRequest {
    pub fn append_to(&self, dst: &mut Vec<u8>) {
        let flexible = self.version >= 9;
        if flexible {
            dst.push(0);
        } else {
            dst.extend_from_slice(&(-1i16).to_be_bytes());
        }
        dst.extend_from_slice(&self.acks.to_be_bytes());
        dst.extend_from_slice(&self.timeout_ms.to_be_bytes());

        if flexible {
            append_uvarint(dst, (self.topics.len() + 1) as u64);
        } else {
            dst.extend_from_slice(&(self.topics.len() as i32).to_be_bytes());
        }
        for (topic, parts) in &self.topics {
            if flexible {
                append_uvarint(dst, (topic.len() + 1) as u64);
                dst.extend_from_slice(topic.as_bytes());
                append_uvarint(dst, (parts.len() + 1) as u64);
            } else {
                dst.extend_from_slice(&(topic.len() as i16).to_be_bytes());
                dst.extend_from_slice(topic.as_bytes());
                dst.extend_from_slice(&(parts.len() as i32).to_be_bytes());
            }
            for &p in parts {
                dst.extend_from_slice(&p.to_be_bytes());
                if flexible {
                    dst.push(0);
                    dst.push(0);
                } else {
                    dst.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
            if flexible {
                dst.push(0);
            }
        }
        if flexible {
            dst.push(0);
        }
    }
}

pub fn append_legacy_message_set(
    dst: &mut Vec<u8>,
    magic: i8,
    recs: &[(&[u8], &[u8], i64)],
    gzip: bool,
) {
    fn crc32_ieee(data: &[u8]) -> u32 {
        let mut crc = 0xffff_ffffu32;
        for &b in data {
            crc ^= b as u32;
            for _ in 0..8 {
                crc = if (crc & 1) != 0 {
                    0xedb8_8320 ^ (crc >> 1)
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }
    let encode_one = |out: &mut Vec<u8>, offset: i64, attrs: i8, ts: i64, k: Option<&[u8]>, v: Option<&[u8]>| {
        let mut body = Vec::new();
        body.push(magic as u8);
        body.push(attrs as u8);
        if magic == 1 {
            body.extend_from_slice(&ts.to_be_bytes());
        }
        match k {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(kb) => {
                body.extend_from_slice(&(kb.len() as i32).to_be_bytes());
                body.extend_from_slice(kb);
            }
        }
        match v {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(vb) => {
                body.extend_from_slice(&(vb.len() as i32).to_be_bytes());
                body.extend_from_slice(vb);
            }
        }
        out.extend_from_slice(&offset.to_be_bytes());
        out.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
        out.extend_from_slice(&crc32_ieee(&body).to_be_bytes());
        out.extend_from_slice(&body);
    };

    if !gzip {
        for (i, &(k, v, ts)) in recs.iter().enumerate() {
            encode_one(dst, i as i64, 0, ts, Some(k), Some(v));
        }
    } else {
        let mut inner = Vec::new();
        for (i, &(k, v, ts)) in recs.iter().enumerate() {
            encode_one(&mut inner, i as i64, 0, ts, Some(k), Some(v));
        }
        use std::io::Write;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&inner).unwrap();
        let compressed = gz.finish().unwrap();
        let wrapper_ts = recs.last().map_or(0, |r| r.2);
        encode_one(dst, (recs.len() as i64) - 1, 0x01, wrapper_ts, None, Some(&compressed));
    }
}

#[derive(Debug, Clone)]
pub struct StagedBatch {
    pub seq: i32,
    pub failed: bool,
    pub rec_count: usize,
}

#[derive(Debug)]
pub struct RecBuf {
    pub topic: String,
    pub partition: i32,
    pub batches: Vec<StagedBatch>,
    pub batch_drain_idx: usize,
    pub inflight: usize,
    pub seq: i32,
    pub need_seq_reset: bool,
    pub added_to_txn: AtomicBool,
    pub ok_on_sink: bool,
}

impl RecBuf {
    pub fn new(topic: impl Into<String>, partition: i32) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            topic: topic.into(),
            partition,
            batches: Vec::new(),
            batch_drain_idx: 0,
            inflight: 0,
            seq: 0,
            need_seq_reset: false,
            added_to_txn: AtomicBool::new(false),
            ok_on_sink: false,
        }))
    }

    pub fn buffer_one(&mut self) {
        let seq = self.seq;
        self.batches.push(StagedBatch {
            seq,
            failed: false,
            rec_count: 1,
        });
    }
}

#[derive(Debug, Default)]
pub struct TxnReq {
    pub newly_added: Vec<(String, i32, Arc<Mutex<RecBuf>>)>,
}

impl TxnReq {
    pub fn contains(&self, topic: &str, partition: i32) -> bool {
        self.newly_added
            .iter()
            .any(|(t, p, _)| t == topic && *p == partition)
    }
}

#[derive(Debug)]
pub struct StagedProduceRequest {
    pub batches: Vec<(Arc<Mutex<RecBuf>>, usize)>,
    pub seq_reset_on_drain: Vec<Arc<Mutex<RecBuf>>>,
}

impl StagedProduceRequest {
    pub fn undo_staged_batches(&self, txn_req: Option<&TxnReq>) {
        for (rb_arc, _) in &self.batches {
            let mut rb = rb_arc.lock().unwrap();
            rb.batch_drain_idx = rb.batch_drain_idx.saturating_sub(1);
            rb.inflight = rb.inflight.saturating_sub(1);
            rb.seq -= 1;
        }
        if let Some(tr) = txn_req {
            for (_, _, rb_arc) in &tr.newly_added {
                let rb = rb_arc.lock().unwrap();
                rb.added_to_txn.store(false, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Sink {
    pub produce_version: AtomicI32,
    pub transactional: bool,
    pub rec_bufs: Mutex<Vec<Arc<Mutex<RecBuf>>>>,
}

impl Sink {
    pub fn new(produce_version: i32, transactional: bool) -> Self {
        Self {
            produce_version: AtomicI32::new(produce_version),
            transactional,
            rec_bufs: Mutex::new(Vec::new()),
        }
    }

    pub fn add_rec_buf(&self, rb: Arc<Mutex<RecBuf>>) {
        self.rec_bufs.lock().unwrap().push(rb);
    }

    pub fn create_req(&self, _pid: i64, _pepoch: i16) -> (StagedProduceRequest, Option<TxnReq>) {
        let rbs = self.rec_bufs.lock().unwrap().clone();
        let mut staged = Vec::new();
        let mut txn_req = TxnReq::default();
        let pv = self.produce_version.load(Ordering::Relaxed);

        for rb_arc in rbs {
            let mut rb = rb_arc.lock().unwrap();
            if rb.need_seq_reset {
                if rb.inflight > 0 {
                    continue;
                }
                rb.need_seq_reset = false;
                rb.seq = 0;
            }
            if rb.batch_drain_idx >= rb.batches.len() {
                continue;
            }
            if rb.inflight > 0 && !rb.ok_on_sink {
                continue;
            }
            if rb.batches[rb.batch_drain_idx].failed {
                continue;
            }
            let seq = rb.seq;
            let idx = rb.batch_drain_idx;
            rb.batches[idx].seq = seq;
            rb.batch_drain_idx += 1;
            rb.inflight += 1;
            rb.seq += 1;
            if self.transactional && pv < 12 && !rb.added_to_txn.swap(true, Ordering::SeqCst) {
                txn_req
                    .newly_added
                    .push((rb.topic.clone(), rb.partition, Arc::clone(&rb_arc)));
            }
            staged.push((Arc::clone(&rb_arc), seq as usize));
        }

        let tr_opt = if txn_req.newly_added.is_empty() {
            None
        } else {
            Some(txn_req)
        };
        (
            StagedProduceRequest {
                batches: staged,
                seq_reset_on_drain: Vec::new(),
            },
            tr_opt,
        )
    }

    pub fn finish_batch(rb: &mut RecBuf) {
        if !rb.batches.is_empty() {
            rb.batches.remove(0);
            if rb.batch_drain_idx > 0 {
                rb.batch_drain_idx -= 1;
            }
        }
        rb.inflight = rb.inflight.saturating_sub(1);
    }

    pub fn fail_all_records(rb: &mut RecBuf) {
        rb.batches.clear();
        rb.batch_drain_idx = 0;
        rb.need_seq_reset = true;
    }
}

#[derive(Debug, Clone)]
pub struct PromisedOutcome {
    pub offset: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub leader_epoch: i32,
}

impl Default for PromisedOutcome {
    fn default() -> Self {
        Self {
            offset: -1,
            producer_id: -1,
            producer_epoch: -1,
            leader_epoch: -1,
        }
    }
}

pub type BroadcastHook = Box<dyn FnMut(bool) + Send + Sync>;
pub type UnknownTopicWaiter = Box<dyn FnOnce(&str) + Send>;

pub struct PromiseChainFlushHarness {
    pub buffered_records: Mutex<i64>,
    pub blocked: AtomicI64,
    pub flushing: AtomicI64,
    pub flush_cond: Condvar,
    pub ring: Arc<PromiseRing<Arc<dyn Fn() + Send + Sync>>>,
    pub on_broadcast: Mutex<Option<BroadcastHook>>,
}

impl Default for PromiseChainFlushHarness {
    fn default() -> Self {
        Self::new(8192)
    }
}

impl PromiseChainFlushHarness {
    pub fn new(max_ring: usize) -> Self {
        Self {
            buffered_records: Mutex::new(0),
            blocked: AtomicI64::new(0),
            flushing: AtomicI64::new(0),
            flush_cond: Condvar::new(),
            ring: Arc::new(PromiseRing::with_max_len(max_ring)),
            on_broadcast: Mutex::new(None),
        }
    }

    pub fn flush(&self) {
        self.flushing.fetch_add(1, Ordering::SeqCst);
        let mut buf = self.buffered_records.lock().unwrap();
        while *buf + self.blocked.load(Ordering::SeqCst) > 0 {
            buf = self.flush_cond.wait(buf).unwrap();
        }
        self.flushing.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn finish_one_buffered(&self) {
        let mut buf = self.buffered_records.lock().unwrap();
        *buf = (*buf - 1).max(0);
        let more_queued = !self.ring.is_empty();
        drop(buf);
        if let Some(ref mut cb) = *self.on_broadcast.lock().unwrap() {
            cb(more_queued);
        }
        self.flush_cond.notify_all();
    }

    pub fn cancel_blocked_produce(&self) {
        let buf = self.buffered_records.lock().unwrap();
        self.blocked.fetch_sub(1, Ordering::SeqCst);
        self.flush_cond.notify_all();
        drop(buf);
    }
}

#[derive(Default)]
pub struct ProducerTopicsRegistry {
    pub topics_mu: Mutex<HashSet<String>>,
    pub unknown_topics_mu: Mutex<HashMap<String, Vec<UnknownTopicWaiter>>>,
    pub sink_rec_bufs_mu: Mutex<()>,
}

impl ProducerTopicsRegistry {
    pub fn produce_unknown(&self, topic: &str, cb: UnknownTopicWaiter) {
        let mut topics = self.topics_mu.lock().unwrap();
        let mut unk = self.unknown_topics_mu.lock().unwrap();
        topics.insert(topic.to_string());
        unk.entry(topic.to_string()).or_default().push(cb);
    }

    pub fn purge_topics(&self, to_purge: &[&str]) {
        let waiters: Vec<UnknownTopicWaiter> = {
            let mut topics = self.topics_mu.lock().unwrap();
            let mut unk = self.unknown_topics_mu.lock().unwrap();
            let mut swept = Vec::new();
            for t in to_purge {
                topics.remove(*t);
                if let Some(ws) = unk.remove(*t) {
                    swept.extend(ws);
                }
            }
            swept
        };
        for w in waiters {
            w("errPurged");
        }
        let _sink_guard = self.sink_rec_bufs_mu.lock().unwrap();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TxnMarkerBucketKey {
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
    coordinator_epoch: i32,
}

pub fn shard_write_txn_markers(
    req: &WriteTxnMarkersRequest,
    partition_leaders: &HashMap<(String, i32), i32>,
) -> BTreeMap<i32, WriteTxnMarkersRequest> {
    let mut by_broker: BTreeMap<i32, BTreeMap<TxnMarkerBucketKey, BTreeMap<String, Vec<i32>>>> =
        BTreeMap::new();

    for m in &req.markers {
        let key = TxnMarkerBucketKey {
            producer_id: m.producer_id.0,
            producer_epoch: m.producer_epoch,
            committed: m.transaction_result,
            coordinator_epoch: m.coordinator_epoch,
        };
        for mt in &m.topics {
            for &p in &mt.partition_indexes {
                let leader = partition_leaders
                    .get(&(mt.name.0.as_str().to_string(), p))
                    .copied()
                    .unwrap_or(-1);
                by_broker
                    .entry(leader)
                    .or_default()
                    .entry(key.clone())
                    .or_default()
                    .entry(mt.name.0.as_str().to_string())
                    .or_default()
                    .push(p);
            }
        }
    }

    let mut out = BTreeMap::new();
    for (broker_id, markers_map) in by_broker {
        let mut wreq = WriteTxnMarkersRequest::default();
        for (k, topics_map) in markers_map {
            let mut marker = WritableTxnMarker::default();
            marker.producer_id = kafka_protocol::messages::ProducerId(k.producer_id);
            marker.producer_epoch = k.producer_epoch;
            marker.transaction_result = k.committed;
            marker.coordinator_epoch = k.coordinator_epoch;
            for (tname, parts) in topics_map {
                let mut mt = WritableTxnMarkerTopic::default();
                mt.name = kafka_protocol::messages::TopicName(StrBytes::from_string(tname));
                mt.partition_indexes = parts;
                marker.topics.push(mt);
            }
            wreq.markers.push(marker);
        }
        out.insert(broker_id, wreq);
    }
    out
}

pub static PROMISE_CHAIN_TEST_COUNT: AtomicUsize = AtomicUsize::new(0);
