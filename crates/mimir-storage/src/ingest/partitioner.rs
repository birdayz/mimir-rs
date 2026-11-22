//! Kafka record partitioners and murmur2 hash matching franz-go `kgo/partitioner.go`.
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/partitioner.go

use rand::Rng;

#[derive(Debug, Clone, Default)]
pub struct PartitionerRecord {
    pub topic: String,
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

pub trait TopicBackupIter {
    fn next(&mut self) -> (usize, i64);
    fn rem(&self) -> usize;
}

pub trait TopicPartitioner: Send {
    fn requires_consistency(&self, rec: &PartitionerRecord) -> bool;
    fn partition(&mut self, rec: &PartitionerRecord, n: usize) -> i32;
    fn on_new_batch(&mut self) {}
    fn partition_by_backup(
        &mut self,
        rec: &PartitionerRecord,
        n: usize,
        _backup: &mut dyn TopicBackupIter,
    ) -> i32 {
        self.partition(rec, n)
    }
}

pub trait Partitioner: Send + Sync {
    fn for_topic(&self, topic: &str) -> Box<dyn TopicPartitioner>;
}

/// Kafka murmur2 hash (matches Apache Kafka `Utils.murmur2` and franz-go `murmur2`).
pub fn murmur2_kafka(b: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let mut h = SEED ^ (b.len() as u32);
    let mut rem = b;
    while rem.len() >= 4 {
        let mut k = u32::from_le_bytes([rem[0], rem[1], rem[2], rem[3]]);
        rem = &rem[4..];
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h = h.wrapping_mul(M);
        h ^= k;
    }
    match rem.len() {
        3 => {
            h ^= (rem[2] as u32) << 16;
            h ^= (rem[1] as u32) << 8;
            h ^= rem[0] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (rem[1] as u32) << 8;
            h ^= rem[0] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= rem[0] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

pub type PartitionerHasher = Box<dyn Fn(&[u8], usize) -> usize + Send + Sync>;

pub fn kafka_hasher<F>(hash_fn: F) -> PartitionerHasher
where
    F: Fn(&[u8]) -> u32 + Send + Sync + 'static,
{
    Box::new(move |key, n| ((hash_fn(key) & 0x7fff_ffff) as usize) % n)
}

/// Historical `SaramaHasher` matching Go `int(hashFn(key)) % n` (pointer/int-width dependent).
pub fn sarama_hasher<F>(hash_fn: F) -> PartitionerHasher
where
    F: Fn(&[u8]) -> u32 + Send + Sync + 'static,
{
    Box::new(move |key, n| {
        let p = (hash_fn(key) as isize) % (n as isize);
        p.unsigned_abs()
    })
}

pub struct ManualPartitioner;

struct ManualTopicPartitioner;

impl TopicPartitioner for ManualTopicPartitioner {
    fn requires_consistency(&self, _rec: &PartitionerRecord) -> bool {
        true
    }
    fn partition(&mut self, rec: &PartitionerRecord, _n: usize) -> i32 {
        rec.partition
    }
}

impl Partitioner for ManualPartitioner {
    fn for_topic(&self, _topic: &str) -> Box<dyn TopicPartitioner> {
        Box::new(ManualTopicPartitioner)
    }
}

pub struct LeastBackupPartitioner;

pub struct LeastBackupTopicPartitioner {
    on_part: isize,
}

impl LeastBackupTopicPartitioner {
    pub fn new() -> Self {
        Self { on_part: -1 }
    }
}

impl Default for LeastBackupTopicPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicPartitioner for LeastBackupTopicPartitioner {
    fn requires_consistency(&self, _rec: &PartitionerRecord) -> bool {
        false
    }
    fn partition(&mut self, _rec: &PartitionerRecord, _n: usize) -> i32 {
        0
    }
    fn on_new_batch(&mut self) {
        self.on_part = -1;
    }
    fn partition_by_backup(
        &mut self,
        _rec: &PartitionerRecord,
        n: usize,
        backup: &mut dyn TopicBackupIter,
    ) -> i32 {
        if self.on_part == -1 || self.on_part >= n as isize {
            let mut least_backup = i64::MAX;
            let mut npicked = 0usize;
            let mut rng = rand::thread_rng();
            for _ in 0..n {
                let (pick, b) = backup.next();
                if b < least_backup {
                    least_backup = b;
                    self.on_part = pick as isize;
                    npicked = 1;
                } else if b == least_backup {
                    npicked += 1;
                    if rng.gen_range(0..npicked) == 0 {
                        self.on_part = pick as isize;
                    }
                }
            }
        }
        self.on_part as i32
    }
}

impl Partitioner for LeastBackupPartitioner {
    fn for_topic(&self, _topic: &str) -> Box<dyn TopicPartitioner> {
        Box::new(LeastBackupTopicPartitioner::new())
    }
}

pub struct UniformBytesPartitioner {
    pub bytes: usize,
    pub adaptive: bool,
    pub keys: bool,
    pub hasher: PartitionerHasher,
}

impl UniformBytesPartitioner {
    pub fn new(bytes: usize, adaptive: bool, keys: bool, hasher: Option<PartitionerHasher>) -> Self {
        Self {
            bytes,
            adaptive,
            keys,
            hasher: hasher.unwrap_or_else(|| kafka_hasher(murmur2_kafka)),
        }
    }

    pub fn for_topic_concrete(&self, _topic: &str) -> UniformBytesTopicPartitioner {
        UniformBytesTopicPartitioner {
            threshold_bytes: self.bytes,
            adaptive: self.adaptive,
            keys: self.keys,
            bytes: 0,
            on_part: -1,
            calc: Vec::new(),
            hasher: if self.keys {
                Some(kafka_hasher(murmur2_kafka))
            } else {
                None
            },
        }
    }
}

pub struct UniformBytesTopicPartitioner {
    pub threshold_bytes: usize,
    pub adaptive: bool,
    pub keys: bool,
    pub bytes: usize,
    pub on_part: isize,
    calc: Vec<(f64, usize)>,
    hasher: Option<PartitionerHasher>,
}

fn varint_len(v: i32) -> usize {
    let mut u = ((v as u32) << 1) ^ ((v >> 31) as u32);
    let mut n = 1;
    while u >= 0x80 {
        u >>= 7;
        n += 1;
    }
    n
}

impl TopicPartitioner for UniformBytesTopicPartitioner {
    fn requires_consistency(&self, rec: &PartitionerRecord) -> bool {
        self.keys && rec.key.is_some()
    }

    fn partition(&mut self, _rec: &PartitionerRecord, _n: usize) -> i32 {
        0
    }

    fn partition_by_backup(
        &mut self,
        rec: &PartitionerRecord,
        n: usize,
        backup: &mut dyn TopicBackupIter,
    ) -> i32 {
        if self.keys {
            if let (Some(k), Some(h)) = (&rec.key, &self.hasher) {
                return h(k, n) as i32;
            }
        }
        let klen = rec.key.as_ref().map_or(0, |k| k.len());
        let vlen = rec.value.as_ref().map_or(0, |v| v.len());
        let mut l = 3
            + varint_len(klen as i32)
            + klen
            + varint_len(vlen as i32)
            + vlen
            + varint_len(rec.headers.len() as i32);
        for (hk, hv) in &rec.headers {
            l += varint_len(hk.len() as i32) + hk.len() + varint_len(hv.len() as i32) + hv.len();
        }
        self.bytes += l;
        if self.bytes >= self.threshold_bytes {
            self.bytes = l;
            self.on_part = -1;
        }
        if self.on_part >= 0 && self.on_part < n as isize {
            return self.on_part as i32;
        }
        let mut rng = rand::thread_rng();
        if !self.adaptive {
            self.on_part = rng.gen_range(0..n) as isize;
        } else {
            self.calc.clear();
            let mut t = 0.0f64;
            for _ in 0..n {
                let (part, b) = backup.next();
                let weight_denom = (b + 1) as f64;
                let f = 1.0 / weight_denom;
                t += f;
                self.calc.push((f, part));
            }
            let r: f64 = rng.gen();
            let mut pick = r * t;
            let mut picked = false;
            for &(f, part) in &self.calc {
                pick -= f;
                if pick <= 0.0 {
                    self.on_part = part as isize;
                    picked = true;
                    break;
                }
            }
            if !picked {
                if let Some(&(_, last_part)) = self.calc.last() {
                    self.on_part = last_part as isize;
                }
            }
        }
        self.on_part as i32
    }
}
