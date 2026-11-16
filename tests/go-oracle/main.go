// SPDX-License-Identifier: AGPL-3.0-only
// Tier-1 Go conformance oracle importing upstream Mimir & Prometheus packages directly.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: go-oracle <subcommand> [json-arg]")
	}
	cmd := os.Args[1]
	if cmd == "kfake-broker" {
		if err := cmdKfakeBroker(); err != nil {
			fatalf("kfake-broker: %v", err)
		}
		return
	}

	var input []byte
	var err error
	if len(os.Args) >= 3 {
		input = []byte(os.Args[2])
	} else {
		input, err = io.ReadAll(os.Stdin)
		if err != nil {
			fatalf("read stdin: %v", err)
		}
	}

	var out any
	switch cmd {
	case "hash-series":
		out, err = cmdHashSeries(input)
	case "encode-write-request":
		out, err = cmdEncodeWriteRequest(input)
	case "decode-write-request":
		out, err = cmdDecodeWriteRequest(input)
	case "encode-chunk":
		out, err = cmdEncodeChunk(input)
	case "decode-chunk":
		out, err = cmdDecodeChunk(input)
	case "verify-tsdb-dir":
		out, err = cmdVerifyTSDBDir(input)
	case "write-tsdb-fixture":
		out, err = cmdWriteTSDBFixture(input)
	case "partition-tokens":
		out, err = cmdPartitionTokens(input)
	case "ring-lookup":
		out, err = cmdRingLookup(input)
	case "ring-merge":
		out, err = cmdRingMerge(input)
	case "partition-ring-lookup":
		out, err = cmdPartitionRingLookup(input)
	case "httpgrpc-call":
		out, err = cmdHttpGrpcCall(input)
	case "gen-test-pki":
		out, err = cmdGenTestPKI(input)
	case "kafka-produce":
		out, err = cmdKafkaProduce(input)
	default:
		fatalf("unknown subcommand %q", cmd)
	}
	if err != nil {
		fatalf("%s: %v", cmd, err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		fatalf("encode stdout: %v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

type LabelJSON struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func toPromLabels(in []LabelJSON) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())
	for _, l := range in {
		b.Set(l.Name, l.Value)
	}
	return b.Labels()
}

func fromPromLabels(ls labels.Labels) []LabelJSON {
	out := make([]LabelJSON, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		out = append(out, LabelJSON{Name: l.Name, Value: l.Value})
	})
	return out
}

func toLabelAdapters(in []LabelJSON) []mimirpb.LabelAdapter {
	out := make([]mimirpb.LabelAdapter, len(in))
	for i, l := range in {
		out[i] = mimirpb.LabelAdapter{Name: l.Name, Value: l.Value}
	}
	return out
}

func fromLabelAdapters(in []mimirpb.LabelAdapter) []LabelJSON {
	out := make([]LabelJSON, len(in))
	for i, l := range in {
		out[i] = LabelJSON{Name: l.Name, Value: l.Value}
	}
	return out
}

// --- hash-series ---

type hashSeriesIn struct {
	UserID     string      `json:"user_id"`
	MetricName string      `json:"metric_name"`
	Labels     []LabelJSON `json:"labels"`
}

type hashSeriesOut struct {
	ShardByUser       uint32 `json:"shard_by_user"`
	ShardByMetricName uint32 `json:"shard_by_metric_name"`
	ShardByAllLabels  uint32 `json:"shard_by_all_labels"`
	PromLabelsHash    uint64 `json:"prom_labels_hash"`
}

func cmdHashSeries(input []byte) (*hashSeriesOut, error) {
	var in hashSeriesIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	ls := toPromLabels(in.Labels)
	return &hashSeriesOut{
		ShardByUser:       mimirpb.ShardByUser(in.UserID),
		ShardByMetricName: mimirpb.ShardByMetricName(in.UserID, in.MetricName),
		ShardByAllLabels:  mimirpb.ShardByAllLabels(in.UserID, ls),
		PromLabelsHash:    ls.Hash(),
	}, nil
}

// --- encode-write-request / decode-write-request ---

type SampleJSON struct {
	TimestampMs int64   `json:"timestamp_ms"`
	Value       float64 `json:"value"`
}

type ExemplarJSON struct {
	Labels      []LabelJSON `json:"labels"`
	Value       float64     `json:"value"`
	TimestampMs int64       `json:"timestamp_ms"`
}

type SpanJSON struct {
	Offset int32  `json:"offset"`
	Length uint32 `json:"length"`
}

type HistogramJSON struct {
	TimestampMs     int64      `json:"timestamp_ms"`
	Schema          int32      `json:"schema"`
	ZeroThreshold   float64    `json:"zero_threshold"`
	ZeroCount       uint64     `json:"zero_count,omitempty"`
	ZeroCountFloat  float64    `json:"zero_count_float,omitempty"`
	Count           uint64     `json:"count,omitempty"`
	CountFloat      float64    `json:"count_float,omitempty"`
	Sum             float64    `json:"sum"`
	ResetHint       uint32     `json:"reset_hint"`
	IsFloat         bool       `json:"is_float,omitempty"`
	PositiveSpans   []SpanJSON `json:"positive_spans,omitempty"`
	PositiveDeltas  []int64    `json:"positive_deltas,omitempty"`
	PositiveCounts  []float64  `json:"positive_counts,omitempty"`
	NegativeSpans   []SpanJSON `json:"negative_spans,omitempty"`
	NegativeDeltas  []int64    `json:"negative_deltas,omitempty"`
	NegativeCounts  []float64  `json:"negative_counts,omitempty"`
	CustomValues    []float64  `json:"custom_values,omitempty"`
}

type MetadataJSON struct {
	Type             string `json:"type"`
	MetricFamilyName string `json:"metric_family_name"`
	Help             string `json:"help"`
	Unit             string `json:"unit"`
}

type TimeSeriesJSON struct {
	Labels           []LabelJSON     `json:"labels"`
	Samples          []SampleJSON    `json:"samples,omitempty"`
	Exemplars        []ExemplarJSON  `json:"exemplars,omitempty"`
	Histograms       []HistogramJSON `json:"histograms,omitempty"`
	CreatedTimestamp int64           `json:"created_timestamp,omitempty"`
}

type WriteRequestJSON struct {
	Mode                     string           `json:"mode,omitempty"`
	Source                   string           `json:"source,omitempty"`
	SkipLabelValidation      bool             `json:"skip_label_validation,omitempty"`
	SkipLabelCountValidation bool             `json:"skip_label_count_validation,omitempty"`
	Timeseries               []TimeSeriesJSON `json:"timeseries,omitempty"`
	Metadata                 []MetadataJSON   `json:"metadata,omitempty"`
}

type bytesB64Out struct {
	BytesB64 string `json:"bytes_b64"`
}

type decodeWriteReqIn struct {
	Mode     string `json:"mode,omitempty"`
	BytesB64 string `json:"bytes_b64"`
}

func parseSource(s string) (mimirpb.WriteRequest_SourceEnum, error) {
	switch strings.ToUpper(s) {
	case "", "API":
		return mimirpb.API, nil
	case "RULE":
		return mimirpb.RULE, nil
	case "OTLP":
		return mimirpb.OTLP, nil
	default:
		return 0, fmt.Errorf("unknown source %q", s)
	}
}

func formatSource(s mimirpb.WriteRequest_SourceEnum) string {
	switch s {
	case mimirpb.API:
		return "API"
	case mimirpb.RULE:
		return "RULE"
	case mimirpb.OTLP:
		return "OTLP"
	default:
		return s.String()
	}
}

func parseMetricType(s string) (mimirpb.MetricMetadata_MetricType, error) {
	switch strings.ToUpper(s) {
	case "", "UNKNOWN":
		return mimirpb.UNKNOWN, nil
	case "COUNTER":
		return mimirpb.COUNTER, nil
	case "GAUGE":
		return mimirpb.GAUGE, nil
	case "HISTOGRAM":
		return mimirpb.HISTOGRAM, nil
	case "GAUGEHISTOGRAM":
		return mimirpb.GAUGEHISTOGRAM, nil
	case "SUMMARY":
		return mimirpb.SUMMARY, nil
	case "INFO":
		return mimirpb.INFO, nil
	case "STATESET":
		return mimirpb.STATESET, nil
	default:
		return 0, fmt.Errorf("unknown metric type %q", s)
	}
}

func toSpansPB(in []SpanJSON) []mimirpb.BucketSpan {
	if len(in) == 0 {
		return nil
	}
	out := make([]mimirpb.BucketSpan, len(in))
	for i, s := range in {
		out[i] = mimirpb.BucketSpan{Offset: s.Offset, Length: s.Length}
	}
	return out
}

func fromSpansPB(in []mimirpb.BucketSpan) []SpanJSON {
	if len(in) == 0 {
		return nil
	}
	out := make([]SpanJSON, len(in))
	for i, s := range in {
		out[i] = SpanJSON{Offset: s.Offset, Length: s.Length}
	}
	return out
}

func toSpansProm(in []SpanJSON) []histogram.Span {
	if len(in) == 0 {
		return nil
	}
	out := make([]histogram.Span, len(in))
	for i, s := range in {
		out[i] = histogram.Span{Offset: s.Offset, Length: s.Length}
	}
	return out
}

func fromSpansProm(in []histogram.Span) []SpanJSON {
	if len(in) == 0 {
		return nil
	}
	out := make([]SpanJSON, len(in))
	for i, s := range in {
		out[i] = SpanJSON{Offset: s.Offset, Length: s.Length}
	}
	return out
}

func toHistogramPB(h HistogramJSON) mimirpb.Histogram {
	out := mimirpb.Histogram{
		Sum:           h.Sum,
		Schema:        h.Schema,
		ZeroThreshold: h.ZeroThreshold,
		NegativeSpans: toSpansPB(h.NegativeSpans),
		PositiveSpans: toSpansPB(h.PositiveSpans),
		ResetHint:     mimirpb.Histogram_ResetHint(h.ResetHint),
		Timestamp:     h.TimestampMs,
		CustomValues:  h.CustomValues,
	}
	if h.IsFloat {
		out.Count = &mimirpb.Histogram_CountFloat{CountFloat: h.CountFloat}
		out.ZeroCount = &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: h.ZeroCountFloat}
		out.NegativeCounts = h.NegativeCounts
		out.PositiveCounts = h.PositiveCounts
	} else {
		out.Count = &mimirpb.Histogram_CountInt{CountInt: h.Count}
		out.ZeroCount = &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount}
		out.NegativeDeltas = h.NegativeDeltas
		out.PositiveDeltas = h.PositiveDeltas
	}
	return out
}

func fromHistogramPB(h mimirpb.Histogram) HistogramJSON {
	out := HistogramJSON{
		TimestampMs:   h.Timestamp,
		Schema:        h.Schema,
		ZeroThreshold: h.ZeroThreshold,
		Sum:           h.Sum,
		ResetHint:     uint32(h.ResetHint),
		IsFloat:       h.IsFloatHistogram(),
		PositiveSpans: fromSpansPB(h.PositiveSpans),
		NegativeSpans: fromSpansPB(h.NegativeSpans),
		CustomValues:  h.CustomValues,
	}
	if h.IsFloatHistogram() {
		out.CountFloat = h.GetCountFloat()
		out.ZeroCountFloat = h.GetZeroCountFloat()
		out.PositiveCounts = h.PositiveCounts
		out.NegativeCounts = h.NegativeCounts
	} else {
		out.Count = h.GetCountInt()
		out.ZeroCount = h.GetZeroCountInt()
		out.PositiveDeltas = h.PositiveDeltas
		out.NegativeDeltas = h.NegativeDeltas
	}
	return out
}

func buildWriteRequest(in WriteRequestJSON) (*mimirpb.WriteRequest, error) {
	src, err := parseSource(in.Source)
	if err != nil {
		return nil, err
	}

	req := &mimirpb.WriteRequest{
		Source:                   src,
		SkipLabelValidation:      in.SkipLabelValidation,
		SkipLabelCountValidation: in.SkipLabelCountValidation,
	}
	for _, m := range in.Metadata {
		mt, err := parseMetricType(m.Type)
		if err != nil {
			return nil, err
		}
		req.Metadata = append(req.Metadata, &mimirpb.MetricMetadata{
			Type:             mt,
			MetricFamilyName: m.MetricFamilyName,
			Help:             m.Help,
			Unit:             m.Unit,
		})
	}
	for _, ts := range in.Timeseries {
		pbTS := &mimirpb.TimeSeries{
			Labels:           toLabelAdapters(ts.Labels),
			CreatedTimestamp: ts.CreatedTimestamp,
		}
		for _, s := range ts.Samples {
			pbTS.Samples = append(pbTS.Samples, mimirpb.Sample{
				TimestampMs: s.TimestampMs,
				Value:       s.Value,
			})
		}
		for _, e := range ts.Exemplars {
			pbTS.Exemplars = append(pbTS.Exemplars, mimirpb.Exemplar{
				Labels:      toLabelAdapters(e.Labels),
				Value:       e.Value,
				TimestampMs: e.TimestampMs,
			})
		}
		for _, h := range ts.Histograms {
			pbTS.Histograms = append(pbTS.Histograms, toHistogramPB(h))
		}
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: pbTS})
	}
	return req, nil
}

func cmdEncodeWriteRequest(input []byte) (*bytesB64Out, error) {
	var in WriteRequestJSON
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	req, err := buildWriteRequest(in)
	if err != nil {
		return nil, err
	}

	var wire []byte
	switch in.Mode {
	case "", "classic":
		wire, err = req.Marshal()
	case "rw2":
		var rw2 *mimirpb.WriteRequest
		rw2, err = mimirpb.FromWriteRequestToRW2Request(req, nil, 0)
		if err == nil {
			defer mimirpb.ReuseRW2(rw2)
			wire, err = rw2.Marshal()
		}
	case "kafka-v2":
		var rw2 *mimirpb.WriteRequest
		rw2, err = mimirpb.FromWriteRequestToRW2Request(req, ingest.V2CommonSymbols, ingest.V2RecordSymbolOffset)
		if err == nil {
			defer mimirpb.ReuseRW2(rw2)
			wire, err = rw2.Marshal()
		}
	default:
		return nil, fmt.Errorf("unknown write-request mode %q", in.Mode)
	}
	if err != nil {
		return nil, err
	}
	return &bytesB64Out{BytesB64: base64.StdEncoding.EncodeToString(wire)}, nil
}

func cmdDecodeWriteRequest(input []byte) (*WriteRequestJSON, error) {
	var in decodeWriteReqIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(in.BytesB64)
	if err != nil {
		return nil, err
	}

	var pw mimirpb.PreallocWriteRequest
	switch in.Mode {
	case "", "classic":
		err = mimirpb.Unmarshal(raw, &pw)
	case "rw2":
		pw.UnmarshalFromRW2 = true
		err = mimirpb.Unmarshal(raw, &pw)
	case "kafka-v2":
		err = ingest.DeserializeRecordContent(raw, &pw, 2)
	default:
		return nil, fmt.Errorf("unknown write-request mode %q", in.Mode)
	}
	if err != nil {
		return nil, err
	}

	out := &WriteRequestJSON{
		Mode:                     in.Mode,
		Source:                   formatSource(pw.Source),
		SkipLabelValidation:      pw.SkipLabelValidation,
		SkipLabelCountValidation: pw.SkipLabelCountValidation,
	}
	for _, m := range pw.Metadata {
		if m == nil {
			continue
		}
		out.Metadata = append(out.Metadata, MetadataJSON{
			Type:             m.Type.String(),
			MetricFamilyName: m.MetricFamilyName,
			Help:             m.Help,
			Unit:             m.Unit,
		})
	}
	for _, pts := range pw.Timeseries {
		if pts.TimeSeries == nil {
			continue
		}
		ts := pts.TimeSeries
		tsJSON := TimeSeriesJSON{
			Labels:           fromLabelAdapters(ts.Labels),
			CreatedTimestamp: ts.CreatedTimestamp,
		}
		for _, s := range ts.Samples {
			tsJSON.Samples = append(tsJSON.Samples, SampleJSON{
				TimestampMs: s.TimestampMs,
				Value:       s.Value,
			})
		}
		for _, e := range ts.Exemplars {
			tsJSON.Exemplars = append(tsJSON.Exemplars, ExemplarJSON{
				Labels:      fromLabelAdapters(e.Labels),
				Value:       e.Value,
				TimestampMs: e.TimestampMs,
			})
		}
		for _, h := range ts.Histograms {
			tsJSON.Histograms = append(tsJSON.Histograms, fromHistogramPB(h))
		}
		out.Timeseries = append(out.Timeseries, tsJSON)
	}
	return out, nil
}

// --- encode-chunk / decode-chunk ---

type ChunkSampleJSON struct {
	ST int64   `json:"st,omitempty"`
	T  int64   `json:"t"`
	V  float64 `json:"v"`
}

type encodeChunkIn struct {
	Encoding   any             `json:"encoding"`
	Samples    []ChunkSampleJSON `json:"samples,omitempty"`
	Histograms []HistogramJSON `json:"histograms,omitempty"`
}

type chunkOut struct {
	Encoding   uint8             `json:"encoding"`
	NumSamples int               `json:"num_samples"`
	BytesB64   string            `json:"bytes_b64,omitempty"`
	Samples    []ChunkSampleJSON `json:"samples,omitempty"`
	Histograms []HistogramJSON   `json:"histograms,omitempty"`
}

type decodeChunkIn struct {
	Encoding any    `json:"encoding"`
	BytesB64 string `json:"bytes_b64"`
}

func parseEncoding(v any) (chunkenc.Encoding, error) {
	if v == nil {
		return chunkenc.EncXOR, nil
	}
	switch x := v.(type) {
	case float64:
		return chunkenc.Encoding(uint8(x)), nil
	case string:
		switch strings.ToLower(x) {
		case "xor":
			return chunkenc.EncXOR, nil
		case "xor2":
			return chunkenc.EncXOR2, nil
		case "histogram":
			return chunkenc.EncHistogram, nil
		case "floathistogram":
			return chunkenc.EncFloatHistogram, nil
		case "histogramst":
			return chunkenc.EncHistogramST, nil
		case "floathistogramst":
			return chunkenc.EncFloatHistogramST, nil
		default:
			return chunkenc.EncNone, fmt.Errorf("unknown chunk encoding %q", x)
		}
	default:
		return chunkenc.EncNone, fmt.Errorf("invalid chunk encoding type %T", v)
	}
}

func nilIfEmpty[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return s
}

func toPromHistogram(h HistogramJSON) *histogram.Histogram {
	return &histogram.Histogram{
		CounterResetHint: histogram.CounterResetHint(h.ResetHint),
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		ZeroCount:        h.ZeroCount,
		Count:            h.Count,
		Sum:              h.Sum,
		PositiveSpans:    toSpansProm(h.PositiveSpans),
		PositiveBuckets:  nilIfEmpty(h.PositiveDeltas),
		NegativeSpans:    toSpansProm(h.NegativeSpans),
		NegativeBuckets:  nilIfEmpty(h.NegativeDeltas),
		CustomValues:     nilIfEmpty(h.CustomValues),
	}
}

func toPromFloatHistogram(h HistogramJSON) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(h.ResetHint),
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		ZeroCount:        h.ZeroCountFloat,
		Count:            h.CountFloat,
		Sum:              h.Sum,
		PositiveSpans:    toSpansProm(h.PositiveSpans),
		PositiveBuckets:  nilIfEmpty(h.PositiveCounts),
		NegativeSpans:    toSpansProm(h.NegativeSpans),
		NegativeBuckets:  nilIfEmpty(h.NegativeCounts),
		CustomValues:     nilIfEmpty(h.CustomValues),
	}
}

func fromPromHistogram(t int64, h *histogram.Histogram) HistogramJSON {
	return HistogramJSON{
		TimestampMs:    t,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      h.ZeroCount,
		Count:          h.Count,
		Sum:            h.Sum,
		ResetHint:      uint32(h.CounterResetHint),
		IsFloat:        false,
		PositiveSpans:  fromSpansProm(h.PositiveSpans),
		PositiveDeltas: slicesClone(h.PositiveBuckets),
		NegativeSpans:  fromSpansProm(h.NegativeSpans),
		NegativeDeltas: slicesClone(h.NegativeBuckets),
		CustomValues:   slicesClone(h.CustomValues),
	}
}

func fromPromFloatHistogram(t int64, h *histogram.FloatHistogram) HistogramJSON {
	return HistogramJSON{
		TimestampMs:    t,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCountFloat: h.ZeroCount,
		CountFloat:     h.Count,
		Sum:            h.Sum,
		ResetHint:      uint32(h.CounterResetHint),
		IsFloat:        true,
		PositiveSpans:  fromSpansProm(h.PositiveSpans),
		PositiveCounts: slicesClone(h.PositiveBuckets),
		NegativeSpans:  fromSpansProm(h.NegativeSpans),
		NegativeCounts: slicesClone(h.NegativeBuckets),
		CustomValues:   slicesClone(h.CustomValues),
	}
}

func slicesClone[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	out := make([]T, len(s))
	copy(out, s)
	return out
}

func cmdEncodeChunk(input []byte) (*chunkOut, error) {
	var in encodeChunkIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	enc, err := parseEncoding(in.Encoding)
	if err != nil {
		return nil, err
	}
	c, err := chunkenc.NewEmptyChunk(enc)
	if err != nil {
		return nil, err
	}
	app, err := c.Appender()
	if err != nil {
		return nil, err
	}

	switch enc {
	case chunkenc.EncXOR, chunkenc.EncXOR2:
		for _, s := range in.Samples {
			app.Append(s.ST, s.T, s.V)
		}
	case chunkenc.EncHistogram, chunkenc.EncHistogramST:
		var prev chunkenc.Appender
		for _, h := range in.Histograms {
			newC, _, nextApp, err := app.AppendHistogram(prev, 0, h.TimestampMs, toPromHistogram(h), true)
			if err != nil {
				return nil, err
			}
			if newC != nil {
				return nil, fmt.Errorf("histogram did not fit into single chunk")
			}
			prev = app
			app = nextApp
		}
	case chunkenc.EncFloatHistogram, chunkenc.EncFloatHistogramST:
		var prev chunkenc.Appender
		for _, h := range in.Histograms {
			newC, _, nextApp, err := app.AppendFloatHistogram(prev, 0, h.TimestampMs, toPromFloatHistogram(h), true)
			if err != nil {
				return nil, err
			}
			if newC != nil {
				return nil, fmt.Errorf("float histogram did not fit into single chunk")
			}
			prev = app
			app = nextApp
		}
	default:
		return nil, fmt.Errorf("unsupported chunk encoding %v", enc)
	}

	c.Compact()
	return &chunkOut{
		Encoding:   uint8(c.Encoding()),
		NumSamples: c.NumSamples(),
		BytesB64:   base64.StdEncoding.EncodeToString(c.Bytes()),
	}, nil
}

func cmdDecodeChunk(input []byte) (*chunkOut, error) {
	var in decodeChunkIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	enc, err := parseEncoding(in.Encoding)
	if err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(in.BytesB64)
	if err != nil {
		return nil, err
	}
	c, err := chunkenc.FromData(enc, raw)
	if err != nil {
		return nil, err
	}
	out := &chunkOut{
		Encoding:   uint8(c.Encoding()),
		NumSamples: c.NumSamples(),
	}
	it := c.Iterator(nil)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		switch vt {
		case chunkenc.ValFloat:
			t, v := it.At()
			out.Samples = append(out.Samples, ChunkSampleJSON{ST: it.AtST(), T: t, V: v})
		case chunkenc.ValHistogram:
			t, h := it.AtHistogram(nil)
			out.Histograms = append(out.Histograms, fromPromHistogram(t, h))
		case chunkenc.ValFloatHistogram:
			t, fh := it.AtFloatHistogram(nil)
			out.Histograms = append(out.Histograms, fromPromFloatHistogram(t, fh))
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// --- write-tsdb-fixture / verify-tsdb-dir ---

type TSDBSeriesFixtureJSON struct {
	Labels     []LabelJSON       `json:"labels"`
	Samples    []ChunkSampleJSON `json:"samples,omitempty"`
	Histograms []HistogramJSON   `json:"histograms,omitempty"`
	Exemplars  []ExemplarJSON    `json:"exemplars,omitempty"`
}

type writeTSDBFixtureIn struct {
	Dir                    string                  `json:"dir"`
	Compact                bool                    `json:"compact,omitempty"`
	OutOfOrderTimeWindowMs int64                   `json:"out_of_order_time_window_ms,omitempty"`
	Series                 []TSDBSeriesFixtureJSON `json:"series"`
}

type statusOut struct {
	Status string   `json:"status"`
	Blocks []string `json:"blocks,omitempty"`
}

func cmdWriteTSDBFixture(input []byte) (*statusOut, error) {
	var in writeTSDBFixtureIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	opts := tsdb.DefaultOptions()
	opts.NoLockfile = true
	opts.EnableExemplarStorage = true
	opts.MaxExemplars = 100000
	opts.OutOfOrderTimeWindow = in.OutOfOrderTimeWindowMs

	nopLogger := slog.New(slog.DiscardHandler)
	db, err := tsdb.Open(in.Dir, nopLogger, nil, opts, nil)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	ctx := context.Background()
	app := db.Appender(ctx)
	for _, s := range in.Series {
		ls := toPromLabels(s.Labels)
		var ref storage.SeriesRef
		for _, sample := range s.Samples {
			ref, err = app.Append(ref, ls, sample.T, sample.V)
			if err != nil {
				_ = app.Rollback()
				return nil, err
			}
		}
		for _, h := range s.Histograms {
			if h.IsFloat {
				ref, err = app.AppendHistogram(ref, ls, h.TimestampMs, nil, toPromFloatHistogram(h))
			} else {
				ref, err = app.AppendHistogram(ref, ls, h.TimestampMs, toPromHistogram(h), nil)
			}
			if err != nil {
				_ = app.Rollback()
				return nil, err
			}
		}
		for _, ex := range s.Exemplars {
			_, err = app.AppendExemplar(ref, ls, exemplar.Exemplar{
				Labels: toPromLabels(ex.Labels),
				Value:  ex.Value,
				Ts:     ex.TimestampMs,
				HasTs:  true,
			})
			if err != nil {
				_ = app.Rollback()
				return nil, err
			}
		}
	}
	if err := app.Commit(); err != nil {
		return nil, err
	}

	if in.Compact {
		if err := db.Compact(ctx); err != nil {
			return nil, err
		}
		if len(db.Blocks()) == 0 && db.Head().NumSeries() > 0 {
			rh := tsdb.NewRangeHead(db.Head(), db.Head().MinTime(), db.Head().MaxTime())
			if err := db.CompactHead(rh); err != nil {
				return nil, err
			}
		}
	}

	var blockIDs []string
	for _, b := range db.Blocks() {
		blockIDs = append(blockIDs, b.Meta().ULID.String())
	}
	sort.Strings(blockIDs)

	return &statusOut{Status: "ok", Blocks: blockIDs}, nil
}

type verifyTSDBDirIn struct {
	Dir      string `json:"dir"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

type verifyTSDBDirOut struct {
	BlockULIDs []string                `json:"block_ulids"`
	HeadSeries uint64                  `json:"head_series"`
	Series     []TSDBSeriesFixtureJSON `json:"series"`
}

func cmdVerifyTSDBDir(input []byte) (*verifyTSDBDirOut, error) {
	var in verifyTSDBDirIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	ctx := context.Background()
	nopLogger := slog.New(slog.DiscardHandler)

	var blockULIDs []string
	var headSeries uint64
	var q storage.Querier
	var eq storage.ExemplarQuerier

	if in.ReadOnly {
		dbRO, err := tsdb.OpenDBReadOnly(in.Dir, "", nopLogger)
		if err != nil {
			return nil, err
		}
		defer dbRO.Close()
		blocks, err := dbRO.Blocks()
		if err != nil {
			return nil, err
		}
		for _, b := range blocks {
			blockULIDs = append(blockULIDs, b.Meta().ULID.String())
		}
		q, err = dbRO.Querier(math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, err
		}
		defer q.Close()
	} else {
		opts := tsdb.DefaultOptions()
		opts.NoLockfile = true
		opts.EnableExemplarStorage = true
		opts.MaxExemplars = 100000
		if _, err := os.Stat(filepath.Join(in.Dir, wlog.WblDirName)); err == nil {
			opts.OutOfOrderTimeWindow = 1 << 50
		}
		db, err := tsdb.Open(in.Dir, nopLogger, nil, opts, nil)
		if err != nil {
			return nil, err
		}
		defer db.Close()

		for _, b := range db.Blocks() {
			blockULIDs = append(blockULIDs, b.Meta().ULID.String())
		}
		headSeries = db.Head().NumSeries()
		q, err = db.Querier(math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, err
		}
		defer q.Close()
		eq, err = db.ExemplarQuerier(ctx)
		if err != nil {
			return nil, err
		}
	}

	sort.Strings(blockULIDs)

	allMatcher := labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*")
	exemplarsBySeries := make(map[uint64][]ExemplarJSON)
	if eq != nil {
		exRes, err := eq.Select(math.MinInt64, math.MaxInt64, []*labels.Matcher{allMatcher})
		if err != nil {
			return nil, err
		}
		for _, r := range exRes {
			h := r.SeriesLabels.Hash()
			for _, ex := range r.Exemplars {
				exemplarsBySeries[h] = append(exemplarsBySeries[h], ExemplarJSON{
					Labels:      fromPromLabels(ex.Labels),
					Value:       ex.Value,
					TimestampMs: ex.Ts,
				})
			}
		}
	}

	ss := q.Select(ctx, true, nil, allMatcher)
	var seriesOut []TSDBSeriesFixtureJSON
	for ss.Next() {
		s := ss.At()
		ls := s.Labels()
		entry := TSDBSeriesFixtureJSON{
			Labels:    fromPromLabels(ls),
			Exemplars: exemplarsBySeries[ls.Hash()],
		}
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				t, v := it.At()
				entry.Samples = append(entry.Samples, ChunkSampleJSON{ST: it.AtST(), T: t, V: v})
			case chunkenc.ValHistogram:
				t, h := it.AtHistogram(nil)
				entry.Histograms = append(entry.Histograms, fromPromHistogram(t, h))
			case chunkenc.ValFloatHistogram:
				t, fh := it.AtFloatHistogram(nil)
				entry.Histograms = append(entry.Histograms, fromPromFloatHistogram(t, fh))
			}
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		seriesOut = append(seriesOut, entry)
	}
	if err := ss.Err(); err != nil {
		return nil, err
	}

	// Ensure deterministic sort even if LabelNames fallback is ever needed.
	sort.Slice(seriesOut, func(i, j int) bool {
		return toPromLabels(seriesOut[i].Labels).String() < toPromLabels(seriesOut[j].Labels).String()
	})

	if blockULIDs == nil {
		blockULIDs = []string{}
	}
	if seriesOut == nil {
		seriesOut = []TSDBSeriesFixtureJSON{}
	}

	return &verifyTSDBDirOut{
		BlockULIDs: blockULIDs,
		HeadSeries: headSeries,
		Series:     seriesOut,
	}, nil
}
