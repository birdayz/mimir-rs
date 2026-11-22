//! Protobuf codec for Prometheus v1 query API responses (`application/vnd.grafana.mimir.query+protobuf`).
//!
//! Ported from github.com/grafana/mimir/pkg/api/protobuf_codec.go

use mimir_proto::cortexpb::{
    self, query_response, BucketSpan, FloatHistogram as PbFloatHistogram, FloatHistogramPair,
    FloatSample, MatrixData, MatrixSeries, QueryErrorType, QueryResponse, QueryStatus, ScalarData,
    StringData, VectorData, VectorHistogram, VectorSample,
};
use prost::Message as _;

pub const QUERY_RESPONSE_MIME_TYPE_TYPE: &str = "application";
pub const QUERY_RESPONSE_MIME_TYPE_SUBTYPE: &str = "vnd.grafana.mimir.query+protobuf";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterResetHint {
    Unknown = 0,
    Reset = 1,
    NotReset = 2,
    Gauge = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistSpan {
    pub offset: i32,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FloatHistogram {
    pub counter_reset_hint: CounterResetHint,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<HistSpan>,
    pub negative_spans: Vec<HistSpan>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub metric: Vec<Label>,
    pub t: i64,
    pub f: f64,
    pub h: Option<FloatHistogram>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FPoint {
    pub t: i64,
    pub f: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HPoint {
    pub t: i64,
    pub h: FloatHistogram,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub metric: Vec<Label>,
    pub floats: Vec<FPoint>,
    pub histograms: Vec<HPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PromqlResult {
    String { t: i64, v: String },
    Scalar { t: i64, v: f64 },
    Vector(Vec<Sample>),
    Matrix(Vec<Series>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryData {
    pub result: PromqlResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseData {
    QueryData(QueryData),
    Other(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct V1Response {
    pub status: String,
    pub error_type: String,
    pub error: String,
    pub warnings: Vec<String>,
    pub infos: Vec<String>,
    pub data: Option<ResponseData>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProtobufCodec;

impl ProtobufCodec {
    pub fn content_type(&self) -> (&'static str, &'static str) {
        (QUERY_RESPONSE_MIME_TYPE_TYPE, QUERY_RESPONSE_MIME_TYPE_SUBTYPE)
    }

    pub fn can_encode(&self, resp: &V1Response) -> bool {
        if resp.status == "error" && resp.data.is_none() {
            return true;
        }
        matches!(resp.data, Some(ResponseData::QueryData(_)))
    }

    pub fn encode(&self, resp: &V1Response) -> Result<Vec<u8>, String> {
        let status = QueryStatus::from_prometheus_string(&resp.status)?;
        let error_type = QueryErrorType::from_prometheus_string(&resp.error_type)?;

        let data = match &resp.data {
            Some(ResponseData::QueryData(qd)) => Some(match &qd.result {
                PromqlResult::String { t, v } => query_response::Data::String(StringData {
                    timestamp_ms: *t,
                    value: v.clone(),
                }),
                PromqlResult::Scalar { t, v } => query_response::Data::Scalar(ScalarData {
                    timestamp_ms: *t,
                    value: *v,
                }),
                PromqlResult::Vector(vec) => query_response::Data::Vector(encode_vector(vec)),
                PromqlResult::Matrix(mat) => query_response::Data::Matrix(encode_matrix(mat)),
            }),
            Some(ResponseData::Other(_)) => {
                return Err("cannot encode non-QueryData response".to_string());
            }
            None => None,
        };

        let pb = QueryResponse {
            status: status as i32,
            error_type: error_type as i32,
            error: resp.error.clone(),
            warnings: resp.warnings.clone(),
            infos: resp.infos.clone(),
            data,
        };
        Ok(pb.encode_to_vec())
    }
}

fn encode_vector(v: &[Sample]) -> VectorData {
    let histogram_count = v.iter().filter(|s| s.h.is_some()).count();
    let mut samples = Vec::with_capacity(v.len() - histogram_count);
    let mut histograms = Vec::with_capacity(histogram_count);

    for s in v {
        let metric = labels_to_string_array(&s.metric);
        match &s.h {
            None => samples.push(VectorSample {
                metric,
                timestamp_ms: s.t,
                value: s.f,
            }),
            Some(h) => histograms.push(VectorHistogram {
                metric,
                timestamp_ms: s.t,
                histogram: Some(float_histogram_to_pb(h)),
            }),
        }
    }
    VectorData {
        samples,
        histograms,
    }
}

fn encode_matrix(m: &[Series]) -> MatrixData {
    MatrixData {
        series: m.iter().map(encode_matrix_series).collect(),
    }
}

fn encode_matrix_series(s: &Series) -> MatrixSeries {
    let samples = s
        .floats
        .iter()
        .map(|p| FloatSample {
            timestamp_ms: p.t,
            value: p.f,
        })
        .collect();
    let histograms = s
        .histograms
        .iter()
        .map(|p| FloatHistogramPair {
            timestamp_ms: p.t,
            histogram: Some(float_histogram_to_pb(&p.h)),
        })
        .collect();
    MatrixSeries {
        metric: labels_to_string_array(&s.metric),
        samples,
        histograms,
    }
}

fn float_histogram_to_pb(h: &FloatHistogram) -> PbFloatHistogram {
    PbFloatHistogram {
        counter_reset_hint: h.counter_reset_hint as u32,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        count: h.count,
        sum: h.sum,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|sp| BucketSpan {
                offset: sp.offset,
                length: sp.length,
            })
            .collect(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|sp| BucketSpan {
                offset: sp.offset,
                length: sp.length,
            })
            .collect(),
        positive_buckets: h.positive_buckets.clone(),
        negative_buckets: h.negative_buckets.clone(),
        custom_values: Vec::new(),
    }
}

fn labels_to_string_array(l: &[Label]) -> Vec<String> {
    let mut out = Vec::with_capacity(2 * l.len());
    for pair in l {
        out.push(pair.name.clone());
        out.push(pair.value.clone());
    }
    out
}

pub use cortexpb::QueryResponse as PbQueryResponse;
