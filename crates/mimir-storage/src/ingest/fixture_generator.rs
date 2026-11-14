//! Deterministic benchmark and test WriteRequest fixture generator.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/fixture_generator.go

use std::collections::BTreeMap;

use mimir_config::{KafkaConfig, StringSliceCSV};
use mimir_proto::cortexpb::{write_request::SourceEnum, LabelPair, Sample, TimeSeries, WriteRequest};
use mimir_proto::prost::Message;

use super::writer::IngestWriter;

#[derive(Debug, Clone, PartialEq)]
pub struct FixtureTenantClassConfig {
    pub tenant_percent: i64,
    pub timeseries_percent: i64,
    pub avg_timeseries_per_req: f64,
}

impl FixtureTenantClassConfig {
    pub fn validate(&self, class_name: &str) -> Result<(), String> {
        let mut errs = Vec::new();
        if self.tenant_percent < 0 {
            errs.push(format!(
                "{class_name}.TenantPercent must be non-negative, got {}",
                self.tenant_percent
            ));
        }
        if self.timeseries_percent < 0 {
            errs.push(format!(
                "{class_name}.TimeseriesPercent must be non-negative, got {}",
                self.timeseries_percent
            ));
        }
        if self.tenant_percent > 0 && self.avg_timeseries_per_req <= 0.0 {
            errs.push(format!(
                "{class_name}.AvgTimeseriesPerReq must be positive, got {}",
                self.avg_timeseries_per_req
            ));
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.join("; "))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FixtureConfig {
    pub total_unique_timeseries: usize,
    pub total_samples: usize,
    pub small_tenants: FixtureTenantClassConfig,
    pub medium_tenants: FixtureTenantClassConfig,
    pub large_tenants: FixtureTenantClassConfig,
    pub avg_labels_per_timeseries: usize,
    pub avg_label_name_length: usize,
    pub avg_label_value_length: usize,
    pub avg_metric_name_length: usize,
    pub num_unique_label_names: usize,
    pub num_unique_label_values: usize,
}

impl FixtureConfig {
    pub fn validate(&self) -> Result<(), String> {
        let mut errs = Vec::new();
        if self.total_unique_timeseries == 0 {
            errs.push(format!(
                "TotalUniqueTimeseries must be positive, got {}",
                self.total_unique_timeseries
            ));
        }
        if self.total_samples == 0 {
            errs.push(format!(
                "TotalSamples must be positive, got {}",
                self.total_samples
            ));
        }
        if self.total_samples < self.total_unique_timeseries {
            errs.push(format!(
                "TotalSamples ({}) must be >= TotalUniqueTimeseries ({})",
                self.total_samples, self.total_unique_timeseries
            ));
        }
        if let Err(e) = self.small_tenants.validate("SmallTenants") {
            errs.push(e);
        }
        if let Err(e) = self.medium_tenants.validate("MediumTenants") {
            errs.push(e);
        }
        if let Err(e) = self.large_tenants.validate("LargeTenants") {
            errs.push(e);
        }

        let tenant_pct_sum = self.small_tenants.tenant_percent
            + self.medium_tenants.tenant_percent
            + self.large_tenants.tenant_percent;
        if tenant_pct_sum != 100 {
            errs.push(format!(
                "TenantPercent values must sum to 100, got {} (small={}, medium={}, large={})",
                tenant_pct_sum,
                self.small_tenants.tenant_percent,
                self.medium_tenants.tenant_percent,
                self.large_tenants.tenant_percent
            ));
        }

        let ts_pct_sum = self.small_tenants.timeseries_percent
            + self.medium_tenants.timeseries_percent
            + self.large_tenants.timeseries_percent;
        if ts_pct_sum != 100 {
            errs.push(format!(
                "TimeseriesPercent values must sum to 100, got {} (small={}, medium={}, large={})",
                ts_pct_sum,
                self.small_tenants.timeseries_percent,
                self.medium_tenants.timeseries_percent,
                self.large_tenants.timeseries_percent
            ));
        }

        if self.avg_labels_per_timeseries == 0 {
            errs.push(format!(
                "AvgLabelsPerTimeseries must be positive, got {}",
                self.avg_labels_per_timeseries
            ));
        }
        if self.avg_label_name_length == 0 {
            errs.push(format!(
                "AvgLabelNameLength must be positive, got {}",
                self.avg_label_name_length
            ));
        }
        if self.avg_label_value_length == 0 {
            errs.push(format!(
                "AvgLabelValueLength must be positive, got {}",
                self.avg_label_value_length
            ));
        }
        if self.avg_metric_name_length == 0 {
            errs.push(format!(
                "AvgMetricNameLength must be positive, got {}",
                self.avg_metric_name_length
            ));
        }
        if self.num_unique_label_names == 0 {
            errs.push(format!(
                "NumUniqueLabelNames must be positive, got {}",
                self.num_unique_label_names
            ));
        }
        if self.num_unique_label_values == 0 {
            errs.push(format!(
                "NumUniqueLabelValues must be positive, got {}",
                self.num_unique_label_values
            ));
        }
        if self.num_unique_label_names < self.avg_labels_per_timeseries {
            errs.push(format!(
                "NumUniqueLabelNames ({}) must be >= AvgLabelsPerTimeseries ({}) to avoid duplicate label names",
                self.num_unique_label_names, self.avg_labels_per_timeseries
            ));
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.join("; "))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TenantSize {
    Small,
    Medium,
    Large,
}

#[derive(Debug, Clone)]
struct TenantProfile {
    id: String,
    _size: TenantSize,
    unique_series: usize,
    avg_series_per_req: f64,
    global_series_idx_offset: usize,
    next_series_idx: usize,
}

/// Simple deterministic SplitMix64 PRNG so fixture generation is 100% reproducible given `seed`.
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: i64) -> Self {
        Self {
            state: seed as u64,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn gen_range(&mut self, upper: usize) -> usize {
        (self.next_u64() % (upper as u64)) as usize
    }

    fn gen_f64(&mut self) -> f64 {
        // Uniform in [0.0, 1.0)
        ((self.next_u64() >> 11) as f64) * (1.0 / ((1u64 << 53) as f64))
    }
}

pub struct FixtureGenerator {
    cfg: FixtureConfig,
    rng: DeterministicRng,
    tenants: Vec<TenantProfile>,
    tenant_weights: Vec<usize>,
    total_weight: usize,
}

impl FixtureGenerator {
    pub fn new(cfg: FixtureConfig, num_tenants: usize, seed: i64) -> Result<Self, String> {
        if num_tenants == 0 {
            return Err("numTenants must be positive, got 0".to_string());
        }
        cfg.validate()
            .map_err(|e| format!("invalid fixture config: {e}"))?;

        let mut num_small = (num_tenants * cfg.small_tenants.tenant_percent as usize) / 100;
        let mut num_medium = (num_tenants * cfg.medium_tenants.tenant_percent as usize) / 100;
        let mut num_large = num_tenants.saturating_sub(num_small + num_medium);

        if num_small == 0 && cfg.small_tenants.tenant_percent > 0 {
            num_small = 1;
        }
        if num_medium == 0 && cfg.medium_tenants.tenant_percent > 0 {
            num_medium = 1;
        }
        if num_large == 0 && cfg.large_tenants.tenant_percent > 0 {
            num_large = 1;
        }

        let ts_small =
            (cfg.total_unique_timeseries * cfg.small_tenants.timeseries_percent as usize) / 100;
        let ts_medium =
            (cfg.total_unique_timeseries * cfg.medium_tenants.timeseries_percent as usize) / 100;
        let ts_large = cfg
            .total_unique_timeseries
            .saturating_sub(ts_small + ts_medium);

        let mut tenants = Vec::with_capacity(num_tenants);
        let mut series_idx = 0usize;

        let mut add_tenants = |count: usize,
                               num_series: usize,
                               size: TenantSize,
                               class_cfg: &FixtureTenantClassConfig| {
            if count == 0 {
                return;
            }
            let series_per_tenant = num_series / count;
            let remainder = num_series % count;
            for i in 0..count {
                let ts = series_per_tenant + usize::from(i < remainder);
                tenants.push(TenantProfile {
                    id: format!("tenant-{}", tenants.len()),
                    _size: size,
                    unique_series: ts,
                    avg_series_per_req: class_cfg.avg_timeseries_per_req,
                    global_series_idx_offset: series_idx,
                    next_series_idx: 0,
                });
                series_idx += ts;
            }
        };

        add_tenants(num_small, ts_small, TenantSize::Small, &cfg.small_tenants);
        add_tenants(
            num_medium,
            ts_medium,
            TenantSize::Medium,
            &cfg.medium_tenants,
        );
        add_tenants(num_large, ts_large, TenantSize::Large, &cfg.large_tenants);

        let mut tenant_weights = Vec::with_capacity(tenants.len());
        let mut cumulative = 0usize;
        for t in &tenants {
            cumulative += t.unique_series;
            tenant_weights.push(cumulative);
        }

        if cumulative == 0 {
            return Err("configuration results in zero total timeseries across all tenants".to_string());
        }

        Ok(Self {
            cfg,
            rng: DeterministicRng::new(seed),
            tenants,
            tenant_weights,
            total_weight: cumulative,
        })
    }

    pub fn num_tenants(&self) -> usize {
        self.tenants.len()
    }

    fn generate_padded_string(&self, prefix: &str, idx: usize, target_len: usize) -> String {
        let base = format!("{prefix}{idx}");
        if base.len() >= target_len {
            base
        } else {
            format!("{base}{}", "_".repeat(target_len - base.len()))
        }
    }

    pub fn generate_series_labels(&self, idx: usize) -> Vec<LabelPair> {
        let num_labels = self.cfg.avg_labels_per_timeseries;
        let metric_name =
            self.generate_padded_string("metric_", idx, self.cfg.avg_metric_name_length);

        let mut sorted: BTreeMap<String, String> = BTreeMap::new();
        sorted.insert("__name__".to_string(), metric_name);
        for i in 0..num_labels {
            let label_name_idx = (idx + i) % self.cfg.num_unique_label_names;
            let label_value_idx = (idx * num_labels + i) % self.cfg.num_unique_label_values;
            let label_name = self.generate_padded_string(
                "label_",
                label_name_idx,
                self.cfg.avg_label_name_length,
            );
            let label_value = self.generate_padded_string(
                "value_",
                label_value_idx,
                self.cfg.avg_label_value_length,
            );
            sorted.insert(label_name, label_value);
        }

        sorted
            .into_iter()
            .map(|(k, v)| LabelPair::from_strs(k, v))
            .collect()
    }

    pub fn select_next_tenant(&mut self) -> usize {
        let r = self.rng.gen_range(self.total_weight);
        self.tenant_weights.partition_point(|&w| w <= r)
    }

    pub fn generate_next_write_request(
        &mut self,
        timestamp_ms: i64,
    ) -> Result<(String, Option<WriteRequest>), String> {
        let tenant_idx = self.select_next_tenant();
        let tenant_id = self.tenants[tenant_idx].id.clone();
        let req = self.generate_write_request(tenant_idx, timestamp_ms)?;
        Ok((tenant_id, req))
    }

    fn generate_write_request(
        &mut self,
        tenant_idx: usize,
        timestamp_ms: i64,
    ) -> Result<Option<WriteRequest>, String> {
        let (unique_series, avg_series_per_req, global_offset, next_idx) = {
            let t = &self.tenants[tenant_idx];
            (
                t.unique_series,
                t.avg_series_per_req,
                t.global_series_idx_offset,
                t.next_series_idx,
            )
        };
        if unique_series == 0 {
            return Ok(None);
        }

        let r = self.rng.gen_f64();
        let num_series = ((avg_series_per_req * (0.8 + 0.4 * r)).round() as usize)
            .min(unique_series)
            .max(1);

        let mut timeseries = Vec::with_capacity(num_series);
        let mut cur_idx = next_idx;
        for _ in 0..num_series {
            let global_series_idx = global_offset + cur_idx;
            timeseries.push(TimeSeries {
                labels: self.generate_series_labels(global_series_idx),
                samples: vec![Sample {
                    timestamp_ms,
                    value: timestamp_ms as f64,
                }],
                exemplars: Vec::new(),
                histograms: Vec::new(),
                created_timestamp: 0,
            });
            cur_idx = (cur_idx + 1) % unique_series;
        }
        self.tenants[tenant_idx].next_series_idx = cur_idx;

        let req = WriteRequest {
            timeseries,
            source: SourceEnum::Api as i32,
            ..Default::default()
        };
        let encoded = req.encode_to_vec();
        let decoded = WriteRequest::decode(encoded.as_slice())
            .map_err(|e| format!("failed to unmarshal WriteRequest: {e}"))?;
        Ok(Some(decoded))
    }

    pub async fn produce_write_requests(
        &mut self,
        kafka_addresses: Vec<String>,
        topic: &str,
        partition_id: i32,
    ) -> Result<usize, String> {
        let cfg = KafkaConfig {
            address: StringSliceCSV(kafka_addresses),
            topic: topic.to_string(),
            producer_record_version: 1,
            ..Default::default()
        };
        let writer = IngestWriter::from_config(&cfg, None)
            .map_err(|e| format!("failed to create Kafka writer: {e}"))?;

        let mut total_samples = 0usize;
        let mut num_records = 0usize;
        while total_samples < self.cfg.total_samples {
            let (tenant_id, req_opt) = self.generate_next_write_request(total_samples as i64)?;
            let Some(req) = req_opt else {
                continue;
            };
            for ts in &req.timeseries {
                total_samples += ts.samples.len();
            }
            writer
                .write_sync(topic, partition_id, &tenant_id, &req)
                .await
                .map_err(|e| format!("failed to produce WriteRequest: {e}"))?;
            num_records += 1;
        }
        Ok(num_records)
    }
}
