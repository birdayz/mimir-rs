//! Ported from github.com/grafana/mimir/pkg/mimirpb/mimir.proto and errors.go

use bytes::Bytes;

#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    #[prost(enumeration = "write_request::SourceEnum", tag = "2")]
    pub source: i32,
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadata>,
    #[prost(string, repeated, tag = "4")]
    pub symbols_rw2: Vec<String>,
    #[prost(message, repeated, tag = "5")]
    pub timeseries_rw2: Vec<TimeSeriesRw2>,
    #[prost(bool, tag = "1000")]
    pub skip_label_validation: bool,
    #[prost(bool, tag = "1001")]
    pub skip_label_count_validation: bool,
}

pub mod write_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum SourceEnum {
        Api = 0,
        Rule = 1,
        Otlp = 2,
    }

    impl SourceEnum {
        pub fn as_str_name(&self) -> &'static str {
            match self {
                SourceEnum::Api => "API",
                SourceEnum::Rule => "RULE",
                SourceEnum::Otlp => "OTLP",
            }
        }

        pub fn from_str_name(value: &str) -> Option<Self> {
            match value {
                "API" => Some(Self::Api),
                "RULE" => Some(Self::Rule),
                "OTLP" => Some(Self::Otlp),
                _ => None,
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct WriteResponse {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum ErrorCause {
    Unknown = 0,
    ReplicasDidNotMatch = 1,
    TooManyClusters = 2,
    BadData = 3,
    IngestionRateLimited = 4,
    RequestRateLimited = 5,
    InstanceLimit = 6,
    ServiceUnavailable = 7,
    TsdbUnavailable = 8,
    TooBusy = 9,
    CircuitBreakerOpen = 10,
    MethodNotAllowed = 11,
    TenantLimit = 12,
    ActiveSeriesLimited = 13,
}

impl ErrorCause {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ErrorCause::Unknown => "ERROR_CAUSE_UNKNOWN",
            ErrorCause::ReplicasDidNotMatch => "ERROR_CAUSE_REPLICAS_DID_NOT_MATCH",
            ErrorCause::TooManyClusters => "ERROR_CAUSE_TOO_MANY_CLUSTERS",
            ErrorCause::BadData => "ERROR_CAUSE_BAD_DATA",
            ErrorCause::IngestionRateLimited => "ERROR_CAUSE_INGESTION_RATE_LIMITED",
            ErrorCause::RequestRateLimited => "ERROR_CAUSE_REQUEST_RATE_LIMITED",
            ErrorCause::InstanceLimit => "ERROR_CAUSE_INSTANCE_LIMIT",
            ErrorCause::ServiceUnavailable => "ERROR_CAUSE_SERVICE_UNAVAILABLE",
            ErrorCause::TsdbUnavailable => "ERROR_CAUSE_TSDB_UNAVAILABLE",
            ErrorCause::TooBusy => "ERROR_CAUSE_TOO_BUSY",
            ErrorCause::CircuitBreakerOpen => "ERROR_CAUSE_CIRCUIT_BREAKER_OPEN",
            ErrorCause::MethodNotAllowed => "ERROR_CAUSE_METHOD_NOT_ALLOWED",
            ErrorCause::TenantLimit => "ERROR_CAUSE_TENANT_LIMIT",
            ErrorCause::ActiveSeriesLimited => "ERROR_CAUSE_ACTIVE_SERIES_LIMITED",
        }
    }

    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "ERROR_CAUSE_UNKNOWN" => Some(Self::Unknown),
            "ERROR_CAUSE_REPLICAS_DID_NOT_MATCH" => Some(Self::ReplicasDidNotMatch),
            "ERROR_CAUSE_TOO_MANY_CLUSTERS" => Some(Self::TooManyClusters),
            "ERROR_CAUSE_BAD_DATA" => Some(Self::BadData),
            "ERROR_CAUSE_INGESTION_RATE_LIMITED" => Some(Self::IngestionRateLimited),
            "ERROR_CAUSE_REQUEST_RATE_LIMITED" => Some(Self::RequestRateLimited),
            "ERROR_CAUSE_INSTANCE_LIMIT" => Some(Self::InstanceLimit),
            "ERROR_CAUSE_SERVICE_UNAVAILABLE" => Some(Self::ServiceUnavailable),
            "ERROR_CAUSE_TSDB_UNAVAILABLE" => Some(Self::TsdbUnavailable),
            "ERROR_CAUSE_TOO_BUSY" => Some(Self::TooBusy),
            "ERROR_CAUSE_CIRCUIT_BREAKER_OPEN" => Some(Self::CircuitBreakerOpen),
            "ERROR_CAUSE_METHOD_NOT_ALLOWED" => Some(Self::MethodNotAllowed),
            "ERROR_CAUSE_TENANT_LIMIT" => Some(Self::TenantLimit),
            "ERROR_CAUSE_ACTIVE_SERIES_LIMITED" => Some(Self::ActiveSeriesLimited),
            _ => None,
        }
    }
}

/// Ported from `IsClientErrorCause` in `pkg/mimirpb/errors.go`.
pub fn is_client_error_cause(cause: ErrorCause) -> bool {
    matches!(cause, ErrorCause::BadData | ErrorCause::TenantLimit)
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct ErrorDetails {
    #[prost(enumeration = "ErrorCause", tag = "1")]
    pub cause: i32,
    #[prost(bool, tag = "2")]
    pub soft: bool,
    #[prost(int64, tag = "3")]
    pub rejected_samples: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<LabelPair>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub exemplars: Vec<Exemplar>,
    #[prost(message, repeated, tag = "4")]
    pub histograms: Vec<Histogram>,
    #[prost(int64, tag = "6")]
    pub created_timestamp: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
pub struct LabelPair {
    #[prost(bytes = "bytes", tag = "1")]
    pub name: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub value: Bytes,
}

impl LabelPair {
    pub fn from_strs(name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        Self {
            name: Bytes::copy_from_slice(name.as_ref().as_bytes()),
            value: Bytes::copy_from_slice(value.as_ref().as_bytes()),
        }
    }

    pub fn from_static(name: &'static str, value: &'static str) -> Self {
        Self {
            name: Bytes::from_static(name.as_bytes()),
            value: Bytes::from_static(value.as_bytes()),
        }
    }

    pub fn name_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.name)
    }

    pub fn value_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.value)
    }
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct MetricMetadata {
    #[prost(enumeration = "metric_metadata::MetricType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "4")]
    pub help: String,
    #[prost(string, tag = "5")]
    pub unit: String,
}

pub mod metric_metadata {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum MetricType {
        Unknown = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        Gaugehistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }

    impl MetricType {
        pub fn as_str_name(&self) -> &'static str {
            match self {
                MetricType::Unknown => "UNKNOWN",
                MetricType::Counter => "COUNTER",
                MetricType::Gauge => "GAUGE",
                MetricType::Histogram => "HISTOGRAM",
                MetricType::Gaugehistogram => "GAUGEHISTOGRAM",
                MetricType::Summary => "SUMMARY",
                MetricType::Info => "INFO",
                MetricType::Stateset => "STATESET",
            }
        }

        pub fn from_str_name(value: &str) -> Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "COUNTER" => Some(Self::Counter),
                "GAUGE" => Some(Self::Gauge),
                "HISTOGRAM" => Some(Self::Histogram),
                "GAUGEHISTOGRAM" => Some(Self::Gaugehistogram),
                "SUMMARY" => Some(Self::Summary),
                "INFO" => Some(Self::Info),
                "STATESET" => Some(Self::Stateset),
                _ => None,
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Metric {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<LabelPair>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Exemplar {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<LabelPair>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Histogram {
    #[prost(double, tag = "3")]
    pub sum: f64,
    #[prost(sint32, tag = "4")]
    pub schema: i32,
    #[prost(double, tag = "5")]
    pub zero_threshold: f64,
    #[prost(message, repeated, tag = "8")]
    pub negative_spans: Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "9")]
    pub negative_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "10")]
    pub negative_counts: Vec<f64>,
    #[prost(message, repeated, tag = "11")]
    pub positive_spans: Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "12")]
    pub positive_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "13")]
    pub positive_counts: Vec<f64>,
    #[prost(enumeration = "histogram::ResetHint", tag = "14")]
    pub reset_hint: i32,
    #[prost(int64, tag = "15")]
    pub timestamp: i64,
    #[prost(double, repeated, tag = "16")]
    pub custom_values: Vec<f64>,
    #[prost(oneof = "histogram::Count", tags = "1, 2")]
    pub count: Option<histogram::Count>,
    #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
    pub zero_count: Option<histogram::ZeroCount>,
}

impl Histogram {
    pub fn is_float_histogram(&self) -> bool {
        matches!(self.count, Some(histogram::Count::CountFloat(_)))
    }

    pub fn is_gauge(&self) -> bool {
        self.reset_hint == histogram::ResetHint::Gauge as i32
    }
}

pub mod histogram {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum ResetHint {
        Unknown = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }

    impl ResetHint {
        pub fn as_str_name(&self) -> &'static str {
            match self {
                ResetHint::Unknown => "UNKNOWN",
                ResetHint::Yes => "YES",
                ResetHint::No => "NO",
                ResetHint::Gauge => "GAUGE",
            }
        }

        pub fn from_str_name(value: &str) -> Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "YES" => Some(Self::Yes),
                "NO" => Some(Self::No),
                "GAUGE" => Some(Self::Gauge),
                _ => None,
            }
        }
    }

    #[derive(Clone, Copy, PartialEq, prost::Oneof)]
    pub enum Count {
        #[prost(uint64, tag = "1")]
        CountInt(u64),
        #[prost(double, tag = "2")]
        CountFloat(f64),
    }

    #[derive(Clone, Copy, PartialEq, prost::Oneof)]
    pub enum ZeroCount {
        #[prost(uint64, tag = "6")]
        ZeroCountInt(u64),
        #[prost(double, tag = "7")]
        ZeroCountFloat(f64),
    }
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct FloatSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct FloatHistogram {
    #[prost(double, tag = "2")]
    pub count: f64,
    #[prost(double, tag = "3")]
    pub sum: f64,
    #[prost(sint32, tag = "4")]
    pub schema: i32,
    #[prost(double, tag = "5")]
    pub zero_threshold: f64,
    #[prost(double, tag = "7")]
    pub zero_count: f64,
    #[prost(message, repeated, tag = "8")]
    pub negative_spans: Vec<BucketSpan>,
    #[prost(double, repeated, tag = "10")]
    pub negative_buckets: Vec<f64>,
    #[prost(message, repeated, tag = "11")]
    pub positive_spans: Vec<BucketSpan>,
    #[prost(double, repeated, tag = "13")]
    pub positive_buckets: Vec<f64>,
    #[prost(uint32, tag = "14")]
    pub counter_reset_hint: u32,
    #[prost(double, repeated, tag = "16")]
    pub custom_values: Vec<f64>,
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct BucketSpan {
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    #[prost(uint32, tag = "2")]
    pub length: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct FloatHistogramPair {
    #[prost(message, optional, tag = "1")]
    pub histogram: Option<FloatHistogram>,
    #[prost(int64, tag = "2")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SampleHistogram {
    #[prost(double, tag = "1")]
    pub count: f64,
    #[prost(double, tag = "2")]
    pub sum: f64,
    #[prost(message, repeated, tag = "3")]
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct HistogramBucket {
    #[prost(int32, tag = "1")]
    pub boundaries: i32,
    #[prost(double, tag = "2")]
    pub lower: f64,
    #[prost(double, tag = "3")]
    pub upper: f64,
    #[prost(double, tag = "4")]
    pub count: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SampleHistogramPair {
    #[prost(message, optional, tag = "1")]
    pub histogram: Option<SampleHistogram>,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum QueryStatus {
    Error = 0,
    Success = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum QueryErrorType {
    None = 0,
    Timeout = 1,
    Canceled = 2,
    Execution = 3,
    BadData = 4,
    Internal = 5,
    Unavailable = 6,
    NotFound = 7,
    NotAcceptable = 8,
    TooManyRequests = 9,
    TooLargeEntry = 10,
    CostLimit = 11,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct QueryResponse {
    #[prost(enumeration = "QueryStatus", tag = "1")]
    pub status: i32,
    #[prost(enumeration = "QueryErrorType", tag = "2")]
    pub error_type: i32,
    #[prost(string, tag = "3")]
    pub error: String,
    #[prost(string, repeated, tag = "8")]
    pub warnings: Vec<String>,
    #[prost(string, repeated, tag = "9")]
    pub infos: Vec<String>,
    #[prost(oneof = "query_response::Data", tags = "4, 5, 6, 7")]
    pub data: Option<query_response::Data>,
}

pub mod query_response {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Data {
        #[prost(message, tag = "4")]
        String(super::StringData),
        #[prost(message, tag = "5")]
        Vector(super::VectorData),
        #[prost(message, tag = "6")]
        Scalar(super::ScalarData),
        #[prost(message, tag = "7")]
        Matrix(super::MatrixData),
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct StringData {
    #[prost(string, tag = "1")]
    pub value: String,
    #[prost(int64, tag = "2")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct VectorData {
    #[prost(message, repeated, tag = "1")]
    pub samples: Vec<VectorSample>,
    #[prost(message, repeated, tag = "2")]
    pub histograms: Vec<VectorHistogram>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct VectorSample {
    #[prost(string, repeated, tag = "1")]
    pub metric: Vec<String>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct VectorHistogram {
    #[prost(string, repeated, tag = "1")]
    pub metric: Vec<String>,
    #[prost(message, optional, tag = "2")]
    pub histogram: Option<FloatHistogram>,
    #[prost(int64, tag = "3")]
    pub timestamp_ms: i64,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct ScalarData {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MatrixData {
    #[prost(message, repeated, tag = "1")]
    pub series: Vec<MatrixSeries>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MatrixSeries {
    #[prost(string, repeated, tag = "1")]
    pub metric: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<FloatSample>,
    #[prost(message, repeated, tag = "3")]
    pub histograms: Vec<FloatHistogramPair>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteRequestRw2 {
    #[prost(string, repeated, tag = "4")]
    pub symbols: Vec<String>,
    #[prost(message, repeated, tag = "5")]
    pub timeseries: Vec<TimeSeriesRw2>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TimeSeriesRw2 {
    #[prost(uint32, repeated, tag = "1")]
    pub labels_refs: Vec<u32>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub histograms: Vec<Histogram>,
    #[prost(message, repeated, tag = "4")]
    pub exemplars: Vec<ExemplarRw2>,
    #[prost(message, optional, tag = "5")]
    pub metadata: Option<MetadataRw2>,
    #[prost(int64, tag = "6")]
    pub created_timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExemplarRw2 {
    #[prost(uint32, repeated, tag = "1")]
    pub labels_refs: Vec<u32>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct MetadataRw2 {
    #[prost(enumeration = "metadata_rw2::MetricType", tag = "1")]
    pub r#type: i32,
    #[prost(uint32, tag = "3")]
    pub help_ref: u32,
    #[prost(uint32, tag = "4")]
    pub unit_ref: u32,
}

pub mod metadata_rw2 {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum MetricType {
        Unspecified = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        Gaugehistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }

    impl MetricType {
        pub fn as_str_name(&self) -> &'static str {
            match self {
                MetricType::Unspecified => "METRIC_TYPE_UNSPECIFIED",
                MetricType::Counter => "METRIC_TYPE_COUNTER",
                MetricType::Gauge => "METRIC_TYPE_GAUGE",
                MetricType::Histogram => "METRIC_TYPE_HISTOGRAM",
                MetricType::Gaugehistogram => "METRIC_TYPE_GAUGEHISTOGRAM",
                MetricType::Summary => "METRIC_TYPE_SUMMARY",
                MetricType::Info => "METRIC_TYPE_INFO",
                MetricType::Stateset => "METRIC_TYPE_STATESET",
            }
        }

        pub fn from_str_name(value: &str) -> Option<Self> {
            match value {
                "METRIC_TYPE_UNSPECIFIED" => Some(Self::Unspecified),
                "METRIC_TYPE_COUNTER" => Some(Self::Counter),
                "METRIC_TYPE_GAUGE" => Some(Self::Gauge),
                "METRIC_TYPE_HISTOGRAM" => Some(Self::Histogram),
                "METRIC_TYPE_GAUGEHISTOGRAM" => Some(Self::Gaugehistogram),
                "METRIC_TYPE_SUMMARY" => Some(Self::Summary),
                "METRIC_TYPE_INFO" => Some(Self::Info),
                "METRIC_TYPE_STATESET" => Some(Self::Stateset),
                _ => None,
            }
        }
    }
}

pub type WriteRequestRW2 = WriteRequestRw2;
pub type TimeSeriesRW2 = TimeSeriesRw2;
pub type ExemplarRW2 = ExemplarRw2;
pub type MetadataRW2 = MetadataRw2;
