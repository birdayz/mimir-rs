//! Helpers to locate, build, and invoke `target/go-oracle` and `target/go-mimir`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Returns the workspace root directory.
pub fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/mimir-conformance
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/mimir-conformance has workspace root two levels up")
        .to_path_buf()
}

/// Returns the path to `target/go-oracle`.
pub fn go_oracle_bin() -> PathBuf {
    workspace_root().join("target").join("go-oracle")
}

/// Returns the path to `target/go-mimir`.
pub fn go_mimir_bin() -> PathBuf {
    workspace_root().join("target").join("go-mimir")
}

/// Ensures `target/go-oracle` (and `target/go-mimir`) exist, running `scripts/build-go-oracle.sh` if missing.
pub fn ensure_go_oracle_built() -> Result<PathBuf> {
    let oracle = go_oracle_bin();
    let mimir = go_mimir_bin();
    if oracle.exists() && mimir.exists() {
        return Ok(oracle);
    }

    let script = workspace_root().join("scripts").join("build-go-oracle.sh");
    let status = Command::new(&script)
        .current_dir(workspace_root())
        .status()
        .with_context(|| format!("failed to execute {}", script.display()))?;
    if !status.success() {
        bail!("{} exited with status {}", script.display(), status);
    }
    Ok(oracle)
}

/// Invokes `target/go-oracle <subcommand>` with JSON on stdin and deserializes JSON from stdout.
pub fn invoke_oracle<Req: Serialize, Resp: DeserializeOwned>(
    subcommand: &str,
    req: &Req,
) -> Result<Resp> {
    let bin = ensure_go_oracle_built()?;
    let payload = serde_json::to_vec(req).context("serialize oracle request")?;

    let mut child = Command::new(&bin)
        .arg(subcommand)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn {} {}", bin.display(), subcommand))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&payload).context("write oracle stdin")?;
    }

    let output = child
        .wait_with_output()
        .context("wait for go-oracle output")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "go-oracle {} failed ({}): {}",
            subcommand,
            output.status,
            stderr.trim()
        );
    }

    serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "decode go-oracle {} stdout: {}",
            subcommand,
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

/// Invokes `target/go-mimir` with the given arguments and returns its combined output.
pub fn run_go_mimir(args: &[&str]) -> Result<std::process::Output> {
    let _ = ensure_go_oracle_built()?;
    let bin = go_mimir_bin();
    Command::new(&bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {}", bin.display()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelJson {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashSeriesRequest {
    pub user_id: String,
    pub metric_name: String,
    pub labels: Vec<LabelJson>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashSeriesResponse {
    pub shard_by_user: u32,
    pub shard_by_metric_name: u32,
    pub shard_by_all_labels: u32,
    pub prom_labels_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SampleJson {
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarJson {
    pub labels: Vec<LabelJson>,
    pub value: f64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanJson {
    pub offset: i32,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HistogramJson {
    pub timestamp_ms: i64,
    pub schema: i32,
    pub zero_threshold: f64,
    #[serde(default)]
    pub zero_count: u64,
    #[serde(default)]
    pub zero_count_float: f64,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub count_float: f64,
    pub sum: f64,
    pub reset_hint: u32,
    #[serde(default)]
    pub is_float: bool,
    #[serde(default)]
    pub positive_spans: Vec<SpanJson>,
    #[serde(default)]
    pub positive_deltas: Vec<i64>,
    #[serde(default)]
    pub positive_counts: Vec<f64>,
    #[serde(default)]
    pub negative_spans: Vec<SpanJson>,
    #[serde(default)]
    pub negative_deltas: Vec<i64>,
    #[serde(default)]
    pub negative_counts: Vec<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataJson {
    #[serde(rename = "type")]
    pub metric_type: String,
    pub metric_family_name: String,
    pub help: String,
    pub unit: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TimeSeriesJson {
    pub labels: Vec<LabelJson>,
    #[serde(default)]
    pub samples: Vec<SampleJson>,
    #[serde(default)]
    pub exemplars: Vec<ExemplarJson>,
    #[serde(default)]
    pub histograms: Vec<HistogramJson>,
    #[serde(default)]
    pub created_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WriteRequestJson {
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub skip_label_validation: bool,
    #[serde(default)]
    pub skip_label_count_validation: bool,
    #[serde(default)]
    pub timeseries: Vec<TimeSeriesJson>,
    #[serde(default)]
    pub metadata: Vec<MetadataJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BytesB64Response {
    pub bytes_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodeWriteRequest {
    pub mode: String,
    pub bytes_b64: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkSampleJson {
    #[serde(default)]
    pub st: i64,
    pub t: i64,
    pub v: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EncodeChunkRequest {
    pub encoding: u8,
    #[serde(default)]
    pub samples: Vec<ChunkSampleJson>,
    #[serde(default)]
    pub histograms: Vec<HistogramJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodeChunkRequest {
    pub encoding: u8,
    pub bytes_b64: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkResponse {
    pub encoding: u8,
    pub num_samples: usize,
    #[serde(default)]
    pub bytes_b64: String,
    #[serde(default)]
    pub samples: Vec<ChunkSampleJson>,
    #[serde(default)]
    pub histograms: Vec<HistogramJson>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TsdbSeriesFixtureJson {
    pub labels: Vec<LabelJson>,
    #[serde(default)]
    pub samples: Vec<ChunkSampleJson>,
    #[serde(default)]
    pub histograms: Vec<HistogramJson>,
    #[serde(default)]
    pub exemplars: Vec<ExemplarJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTsdbFixtureRequest {
    pub dir: String,
    #[serde(default)]
    pub compact: bool,
    #[serde(default)]
    pub out_of_order_time_window_ms: i64,
    pub series: Vec<TsdbSeriesFixtureJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTsdbFixtureResponse {
    pub status: String,
    #[serde(default)]
    pub blocks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyTsdbDirRequest {
    pub dir: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyTsdbDirResponse {
    pub block_ulids: Vec<String>,
    pub head_series: u64,
    pub series: Vec<TsdbSeriesFixtureJson>,
}
