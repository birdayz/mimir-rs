//! Per-tenant TSDB block -> Kafka offset watermark catalogue.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/offset_catalogue.go
//! - github.com/grafana/mimir/pkg/storage/ingest/kmeta/partitions_offsets.go

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use prometheus::{Counter, Gauge, GaugeVec, Opts, Registry};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::shipper::atomic_write_file;

pub const OFFSET_CATALOGUE_VERSION: i32 = 2;
pub const OFFSET_CATALOGUE_FILENAME: &str = "offset-catalogue.json";

#[derive(Debug, Error)]
pub enum OffsetCatalogueError {
    #[error("offset catalogue has no offsets to sync")]
    EmptyOffsets,
    #[error("offset catalogue version is too old: expect version {expected}, got {got}")]
    VersionTooOld { expected: i32, got: i32 },
    #[error("unexpected offset catalogue version: expected {expected} got {got}")]
    UnexpectedVersion { expected: i32, got: i32 },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Ported from `kmeta.PartitionOffsets` in `pkg/storage/ingest/kmeta/partitions_offsets.go`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionOffsets(pub Vec<i64>);

impl PartitionOffsets {
    pub fn new_single_cluster(offset: i64) -> Self {
        Self(vec![offset])
    }

    pub fn new_multi_cluster(offsets: Vec<i64>) -> Self {
        Self(offsets)
    }

    pub fn num_kafka_clusters(&self) -> usize {
        self.0.len()
    }

    pub fn for_kafka_cluster(&self, cluster_id: usize) -> i64 {
        self.0.get(cluster_id).copied().unwrap_or(-1)
    }
}

impl fmt::Display for PartitionOffsets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub type PartitionsOffsets = HashMap<i32, PartitionOffsets>;
pub type TopicsPartitionsOffsets = HashMap<String, PartitionsOffsets>;

/// Merges per-cluster `topic -> partition -> offset` maps into `TopicsPartitionsOffsets`.
pub fn merge_topics_partitions_offsets(
    per_cluster: &[HashMap<String, HashMap<i32, i64>>],
) -> TopicsPartitionsOffsets {
    let num_clusters = per_cluster.len();
    let mut merged: HashMap<String, HashMap<i32, Vec<i64>>> = HashMap::new();

    for (cluster_id, by_topic) in per_cluster.iter().enumerate() {
        for (topic, by_partition) in by_topic {
            let topic_offsets = merged.entry(topic.clone()).or_default();
            for (&partition_id, &offset) in by_partition {
                let cluster_offsets = topic_offsets
                    .entry(partition_id)
                    .or_insert_with(|| vec![-1; num_clusters]);
                cluster_offsets[cluster_id] = offset;
            }
        }
    }

    merged
        .into_iter()
        .map(|(topic, by_part)| {
            let parts = by_part
                .into_iter()
                .map(|(pid, offs)| (pid, PartitionOffsets::new_multi_cluster(offs)))
                .collect();
            (topic, parts)
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetWatermark {
    #[serde(rename = "p")]
    pub partition: i32,
    #[serde(rename = "o")]
    pub offset: i64,
}

impl fmt::Display for OffsetWatermark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.partition, self.offset)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetCatalogueData {
    pub version: i32,
    pub updated_at: i64,
    pub data: BTreeMap<String, BTreeMap<usize, OffsetWatermark>>,
}

#[derive(Clone)]
pub struct OffsetCatalogueMetrics {
    pub syncs: Counter,
    pub sync_failures: Counter,
    pub last_sync_time: Gauge,
    pub last_synced_offset: GaugeVec,
}

impl OffsetCatalogueMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let syncs = Counter::with_opts(Opts::new(
            "cortex_ingester_offset_catalogue_syncs_total",
            "Total number of successful offset catalogue syncs.",
        ))
        .expect("valid metric");
        let sync_failures = Counter::with_opts(Opts::new(
            "cortex_ingester_offset_catalogue_sync_failures_total",
            "Total number of offset catalogue sync failures.",
        ))
        .expect("valid metric");
        let last_sync_time = Gauge::with_opts(Opts::new(
            "cortex_ingester_offset_catalogue_last_sync_timestamp_seconds",
            "Unix timestamp (in seconds) of the last successful offset catalogue sync.",
        ))
        .expect("valid metric");
        let last_synced_offset = GaugeVec::new(
            Opts::new(
                "cortex_ingester_offset_catalogue_last_synced_offset",
                "The last-seen offset used as the high-water mark in the most recent successful offset catalogue sync.",
            ),
            &["write_compartment"],
        )
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(syncs.clone()));
            let _ = reg.register(Box::new(sync_failures.clone()));
            let _ = reg.register(Box::new(last_sync_time.clone()));
            let _ = reg.register(Box::new(last_synced_offset.clone()));
        }

        Self {
            syncs,
            sync_failures,
            last_sync_time,
            last_synced_offset,
        }
    }
}

/// Ported from `offsetCatalogue` in `pkg/ingester/offset_catalogue.go`.
pub struct OffsetCatalogue {
    pub dir: PathBuf,
    pub user_id: String,
    pub partition: i32,
    pub metrics: OffsetCatalogueMetrics,
}

impl OffsetCatalogue {
    pub fn new(
        dir: impl AsRef<Path>,
        user_id: impl Into<String>,
        partition: i32,
        metrics: OffsetCatalogueMetrics,
    ) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            user_id: user_id.into(),
            partition,
            metrics,
        }
    }

    pub fn sync(&self, offset_hws: &PartitionOffsets) -> Result<(), OffsetCatalogueError> {
        let res = self.sync_inner(offset_hws);
        if res.is_err() {
            self.metrics.sync_failures.inc();
        }
        res
    }

    fn sync_inner(&self, offset_hws: &PartitionOffsets) -> Result<(), OffsetCatalogueError> {
        let mut old_data = match read_offset_catalogue_from_file(&self.dir) {
            Ok(d) => d.data,
            Err(err) => {
                let not_found = matches!(
                    &err,
                    OffsetCatalogueError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound
                );
                if !not_found {
                    warn!(error = %err, dir = %self.dir.display(), "reading offset catalogue failed, will override");
                }
                BTreeMap::new()
            }
        };

        let mut blocks = HashSet::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if Ulid::from_string(&name).is_err() {
                continue;
            }
            if entry.path().is_dir() {
                blocks.insert(name);
            }
        }

        let mut offsets = BTreeMap::new();
        for (cluster_id, &offset) in offset_hws.0.iter().enumerate() {
            if offset == -1 {
                continue;
            }
            offsets.insert(
                cluster_id,
                OffsetWatermark {
                    partition: self.partition,
                    offset,
                },
            );
        }

        if offsets.is_empty() {
            return Err(OffsetCatalogueError::EmptyOffsets);
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut data = OffsetCatalogueData {
            version: OFFSET_CATALOGUE_VERSION,
            updated_at: now_secs,
            data: BTreeMap::new(),
        };

        for id in blocks {
            if let Some(mut old_offsets) = old_data.remove(&id) {
                if old_offsets.len() < offsets.len() {
                    for (&cluster_id, &wm) in &offsets {
                        old_offsets.entry(cluster_id).or_insert(wm);
                    }
                }
                data.data.insert(id, old_offsets);
            } else {
                data.data.insert(id, offsets.clone());
            }
        }

        write_offset_catalogue_to_file(&self.dir, &data)?;

        self.metrics.syncs.inc();
        self.metrics.last_sync_time.set(now_secs as f64);
        for (cluster_id, wm) in &offsets {
            self.metrics
                .last_synced_offset
                .with_label_values(&[&cluster_id.to_string()])
                .set(wm.offset as f64);
        }

        Ok(())
    }
}

pub fn read_offset_catalogue_from_file(
    dir: impl AsRef<Path>,
) -> Result<OffsetCatalogueData, OffsetCatalogueError> {
    let file_path = dir.as_ref().join(OFFSET_CATALOGUE_FILENAME);
    let bytes = fs::read(&file_path)?;

    #[derive(Deserialize)]
    struct Header {
        version: i32,
    }
    if let Ok(hdr) = serde_json::from_slice::<Header>(&bytes) {
        if hdr.version < OFFSET_CATALOGUE_VERSION {
            return Err(OffsetCatalogueError::VersionTooOld {
                expected: OFFSET_CATALOGUE_VERSION,
                got: hdr.version,
            });
        }
        if hdr.version != OFFSET_CATALOGUE_VERSION {
            return Err(OffsetCatalogueError::UnexpectedVersion {
                expected: OFFSET_CATALOGUE_VERSION,
                got: hdr.version,
            });
        }
    }

    let data: OffsetCatalogueData = serde_json::from_slice(&bytes)?;
    if data.version != OFFSET_CATALOGUE_VERSION {
        return Err(OffsetCatalogueError::UnexpectedVersion {
            expected: OFFSET_CATALOGUE_VERSION,
            got: data.version,
        });
    }
    Ok(data)
}

pub fn write_offset_catalogue_to_file(
    dir: impl AsRef<Path>,
    data: &OffsetCatalogueData,
) -> Result<(), OffsetCatalogueError> {
    let file_path = dir.as_ref().join(OFFSET_CATALOGUE_FILENAME);
    let mut encoded = serde_json::to_vec_pretty(data)?;
    encoded.push(b'\n');
    atomic_write_file(&file_path, &encoded)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_dummy_block_dir(dir: &Path, id: Ulid) {
        fs::create_dir_all(dir.join(id.to_string())).unwrap();
    }

    #[test]
    fn test_offset_catalogue_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = Ulid::from_parts(1000, 1);
        let id2 = Ulid::from_parts(2000, 2);
        create_dummy_block_dir(dir.path(), id1);

        let cat = OffsetCatalogue::new(dir.path(), "user-1", 1, OffsetCatalogueMetrics::new(None));

        // Empty offsets (-1, -1) fails without creating file.
        assert!(matches!(
            cat.sync(&PartitionOffsets::new_multi_cluster(vec![-1, -1])),
            Err(OffsetCatalogueError::EmptyOffsets)
        ));
        assert!(!dir.path().join(OFFSET_CATALOGUE_FILENAME).exists());

        // Partial offset (50, -1) writes cluster 0.
        cat.sync(&PartitionOffsets::new_multi_cluster(vec![50, -1])).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(data.data[&id1.to_string()][&0], OffsetWatermark { partition: 1, offset: 50 });
        assert!(!data.data[&id1.to_string()].contains_key(&1));

        // New block id2 appears + cluster 1 gets offset 400; id1 preserves cluster 0=50 and adds cluster 1=400.
        create_dummy_block_dir(dir.path(), id2);
        cat.sync(&PartitionOffsets::new_multi_cluster(vec![200, 400])).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(data.data[&id1.to_string()][&0], OffsetWatermark { partition: 1, offset: 50 });
        assert_eq!(data.data[&id1.to_string()][&1], OffsetWatermark { partition: 1, offset: 400 });
        assert_eq!(data.data[&id2.to_string()][&0], OffsetWatermark { partition: 1, offset: 200 });
        assert_eq!(data.data[&id2.to_string()][&1], OffsetWatermark { partition: 1, offset: 400 });

        // Delete id1 -> dropped on next sync.
        fs::remove_dir_all(dir.path().join(id1.to_string())).unwrap();
        cat.sync(&PartitionOffsets::new_single_cluster(300)).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert!(!data.data.contains_key(&id1.to_string()));
        assert!(data.data.contains_key(&id2.to_string()));
    }

    #[test]
    fn test_offset_catalogue_rebuilds_old_or_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let id = Ulid::from_parts(1000, 1);
        create_dummy_block_dir(dir.path(), id);

        let old = format!(r#"{{"version":1,"updated_at":0,"data":{{"{id}":{{"p":1,"o":50}}}}}}"#);
        fs::write(dir.path().join(OFFSET_CATALOGUE_FILENAME), old).unwrap();
        assert!(matches!(
            read_offset_catalogue_from_file(dir.path()),
            Err(OffsetCatalogueError::VersionTooOld { expected: 2, got: 1 })
        ));

        let cat = OffsetCatalogue::new(dir.path(), "user-1", 1, OffsetCatalogueMetrics::new(None));
        cat.sync(&PartitionOffsets::new_multi_cluster(vec![100, 250])).unwrap();
        let data = read_offset_catalogue_from_file(dir.path()).unwrap();
        assert_eq!(data.version, 2);
        assert_eq!(data.data[&id.to_string()][&0], OffsetWatermark { partition: 1, offset: 100 });
        assert_eq!(data.data[&id.to_string()][&1], OffsetWatermark { partition: 1, offset: 250 });
    }
}
