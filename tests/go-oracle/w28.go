// SPDX-License-Identifier: AGPL-3.0-only
// Wave 28 Tier-1 Go conformance oracle subcommands for ST TSDB interop and PropagationDelayTracker.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/grafana/dskit/kv/memberlist"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// --- write-st-tsdb-fixture / read-st-tsdb ---

type STBatchFixtureJSON struct {
	Series []TSDBSeriesFixtureJSON `json:"series"`
}

type writeSTTSDBFixtureIn struct {
	Dir                       string                  `json:"dir"`
	EnableSTStorage           bool                    `json:"enable_st_storage,omitempty"`
	FloatChunkEncoding        string                  `json:"float_chunk_encoding,omitempty"`
	EnableHistogramSTEncoding bool                    `json:"enable_histogram_st_encoding,omitempty"`
	OutOfOrderTimeWindowMs    int64                   `json:"out_of_order_time_window_ms,omitempty"`
	OutOfOrderCapMax          int64                   `json:"out_of_order_cap_max,omitempty"`
	WALSegmentSize            int                     `json:"wal_segment_size,omitempty"`
	SamplesPerChunk           int                     `json:"samples_per_chunk,omitempty"`
	MinBlockDurationMs        int64                   `json:"min_block_duration_ms,omitempty"`
	MaxBlockDurationMs        int64                   `json:"max_block_duration_ms,omitempty"`
	Series                    []TSDBSeriesFixtureJSON `json:"series,omitempty"`
	Batches                   []STBatchFixtureJSON    `json:"batches,omitempty"`
	CompactHeadBeforeMs       *int64                  `json:"compact_head_before_ms,omitempty"`
	Compact                   bool                    `json:"compact,omitempty"`
	CompactOOO                bool                    `json:"compact_ooo,omitempty"`
	PostCompactBatches        []STBatchFixtureJSON    `json:"post_compact_batches,omitempty"`
}

type stFixtureStatusOut struct {
	Status string              `json:"status"`
	Blocks []string            `json:"blocks,omitempty"`
	Files  map[string][]string `json:"files,omitempty"`
}

func parseFloatChunkEncoding(s string) (chunkenc.Encoding, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "xor":
		return chunkenc.EncXOR, nil
	case "xor2":
		return chunkenc.EncXOR2, nil
	default:
		return chunkenc.EncNone, fmt.Errorf("unknown float_chunk_encoding %q", s)
	}
}

func appendSTBatch(ctx context.Context, db *tsdb.DB, series []TSDBSeriesFixtureJSON) error {
	if len(series) == 0 {
		return nil
	}
	app := db.AppenderV2(ctx)
	for _, s := range series {
		ls := toPromLabels(s.Labels)
		var ref storage.SeriesRef
		var err error
		for _, sample := range s.Samples {
			ref, err = app.Append(ref, ls, sample.ST, sample.T, sample.V, nil, nil, storage.AOptions{})
			if err != nil {
				_ = app.Rollback()
				return fmt.Errorf("append float %v t=%d st=%d: %w", ls, sample.T, sample.ST, err)
			}
		}
		for _, h := range s.Histograms {
			var ih *histogram.Histogram
			var fh *histogram.FloatHistogram
			if h.IsFloat {
				fh = toPromFloatHistogram(h)
			} else {
				ih = toPromHistogram(h)
			}
			ref, err = app.Append(ref, ls, h.ST, h.TimestampMs, 0, ih, fh, storage.AOptions{})
			if err != nil {
				_ = app.Rollback()
				return fmt.Errorf("append histogram %v t=%d st=%d: %w", ls, h.TimestampMs, h.ST, err)
			}
		}
		if len(s.Exemplars) > 0 {
			exs := make([]exemplar.Exemplar, 0, len(s.Exemplars))
			for _, ex := range s.Exemplars {
				exs = append(exs, exemplar.Exemplar{
					Labels: toPromLabels(ex.Labels),
					Value:  ex.Value,
					Ts:     ex.TimestampMs,
					HasTs:  true,
				})
			}
			ea, ok := app.(storage.ExemplarAppenderV2)
			if !ok {
				_ = app.Rollback()
				return fmt.Errorf("appender %T does not implement storage.ExemplarAppenderV2", app)
			}
			if _, err := ea.AppendExemplars(ref, ls, exs); err != nil {
				_ = app.Rollback()
				return fmt.Errorf("append exemplars %v: %w", ls, err)
			}
		}
	}
	return app.Commit()
}

func snapshotTSDBDirFiles(dir string) map[string][]string {
	out := map[string][]string{}
	for _, sub := range []string{"chunks_head", "wal", wlog.WblDirName} {
		out[sub] = listDirNames(filepath.Join(dir, sub))
	}
	return out
}

func cmdWriteSTTSDBFixture(input []byte) (*stFixtureStatusOut, error) {
	var in writeSTTSDBFixtureIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	floatEnc, err := parseFloatChunkEncoding(in.FloatChunkEncoding)
	if err != nil {
		return nil, err
	}

	opts := tsdb.DefaultOptions()
	opts.NoLockfile = true
	opts.EnableExemplarStorage = true
	opts.MaxExemplars = 100000
	opts.EnableSTStorage = in.EnableSTStorage
	opts.FloatChunkEncoding = floatEnc
	opts.EnableHistogramSTEncoding = in.EnableHistogramSTEncoding
	opts.OutOfOrderTimeWindow = in.OutOfOrderTimeWindowMs
	if in.OutOfOrderCapMax > 0 {
		opts.OutOfOrderCapMax = in.OutOfOrderCapMax
	}
	if in.WALSegmentSize != 0 {
		opts.WALSegmentSize = in.WALSegmentSize
	}
	if in.SamplesPerChunk > 0 {
		opts.SamplesPerChunk = in.SamplesPerChunk
	}
	if in.MinBlockDurationMs > 0 {
		opts.MinBlockDuration = in.MinBlockDurationMs
	}
	if in.MaxBlockDurationMs > 0 {
		opts.MaxBlockDuration = in.MaxBlockDurationMs
	}

	nopLogger := slog.New(slog.DiscardHandler)
	db, err := tsdb.Open(in.Dir, nopLogger, nil, opts, nil)
	if err != nil {
		return nil, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	ctx := context.Background()
	if len(in.Series) > 0 {
		if err := appendSTBatch(ctx, db, in.Series); err != nil {
			return nil, err
		}
	}
	for _, b := range in.Batches {
		if err := appendSTBatch(ctx, db, b.Series); err != nil {
			return nil, err
		}
	}

	if in.CompactHeadBeforeMs != nil {
		rh := tsdb.NewRangeHead(db.Head(), db.Head().MinTime(), *in.CompactHeadBeforeMs)
		if err := db.CompactHead(rh); err != nil {
			return nil, fmt.Errorf("compact head before %d: %w", *in.CompactHeadBeforeMs, err)
		}
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
	if in.CompactOOO {
		if err := db.CompactOOOHead(ctx); err != nil {
			return nil, err
		}
	}

	for _, b := range in.PostCompactBatches {
		if err := appendSTBatch(ctx, db, b.Series); err != nil {
			return nil, err
		}
	}

	var blockIDs []string
	for _, b := range db.Blocks() {
		blockIDs = append(blockIDs, b.Meta().ULID.String())
	}
	sort.Strings(blockIDs)

	if err := db.Close(); err != nil {
		return nil, err
	}
	closed = true

	return &stFixtureStatusOut{
		Status: "ok",
		Blocks: blockIDs,
		Files:  snapshotTSDBDirFiles(in.Dir),
	}, nil
}

type readSTTSDBIn struct {
	Dir                       string `json:"dir"`
	EnableSTStorage           bool   `json:"enable_st_storage,omitempty"`
	FloatChunkEncoding        string `json:"float_chunk_encoding,omitempty"`
	EnableHistogramSTEncoding bool   `json:"enable_histogram_st_encoding,omitempty"`
	OutOfOrderTimeWindowMs    int64  `json:"out_of_order_time_window_ms,omitempty"`
	ReadOnly                  bool   `json:"read_only,omitempty"`
}

type readSTTSDBOut struct {
	BlockULIDs []string                `json:"block_ulids"`
	HeadSeries uint64                  `json:"head_series"`
	Files      map[string][]string     `json:"files,omitempty"`
	Series     []TSDBSeriesFixtureJSON `json:"series"`
}

func cmdReadSTTSDB(input []byte) (*readSTTSDBOut, error) {
	var in readSTTSDBIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	floatEnc, err := parseFloatChunkEncoding(in.FloatChunkEncoding)
	if err != nil {
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
		opts.EnableSTStorage = in.EnableSTStorage
		opts.FloatChunkEncoding = floatEnc
		opts.EnableHistogramSTEncoding = in.EnableHistogramSTEncoding
		opts.OutOfOrderTimeWindow = in.OutOfOrderTimeWindowMs
		if opts.OutOfOrderTimeWindow == 0 {
			if _, err := os.Stat(filepath.Join(in.Dir, wlog.WblDirName)); err == nil {
				opts.OutOfOrderTimeWindow = 1 << 50
			}
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
				hj := fromPromHistogram(t, h)
				hj.ST = it.AtST()
				entry.Histograms = append(entry.Histograms, hj)
			case chunkenc.ValFloatHistogram:
				t, fh := it.AtFloatHistogram(nil)
				fhj := fromPromFloatHistogram(t, fh)
				fhj.ST = it.AtST()
				entry.Histograms = append(entry.Histograms, fhj)
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

	sort.Slice(seriesOut, func(i, j int) bool {
		return toPromLabels(seriesOut[i].Labels).String() < toPromLabels(seriesOut[j].Labels).String()
	})

	if blockULIDs == nil {
		blockULIDs = []string{}
	}
	if seriesOut == nil {
		seriesOut = []TSDBSeriesFixtureJSON{}
	}

	return &readSTTSDBOut{
		BlockULIDs: blockULIDs,
		HeadSeries: headSeries,
		Files:      snapshotTSDBDirFiles(in.Dir),
		Series:     seriesOut,
	}, nil
}

// --- format-propagation-tracker ---

type beaconInputJSON struct {
	PublishedBy string `json:"published_by"`
	PublishedAt int64  `json:"published_at"`
	DeletedAt   int64  `json:"deleted_at"`
}

type formatPropagationTrackerIn struct {
	Beacons  map[uint64]beaconInputJSON `json:"beacons,omitempty"`
	ValueB64 string                     `json:"value_b64,omitempty"`
}

type formatPropagationTrackerOut struct {
	GoString         string `json:"go_string"`
	JSON             string `json:"json"`
	JSONIndent       string `json:"json_indent"`
	JSONEncode       string `json:"json_encode"`
	JSONEncodeIndent string `json:"json_encode_indent"`
}

func cmdFormatPropagationTracker(input []byte) (*formatPropagationTrackerOut, error) {
	var in formatPropagationTrackerIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	var desc *memberlist.PropagationDelayTrackerDesc
	if in.ValueB64 != "" {
		raw, err := base64.StdEncoding.DecodeString(in.ValueB64)
		if err != nil {
			return nil, err
		}
		v, err := memberlist.GetPropagationDelayTrackerCodec().Decode(raw)
		if err != nil {
			return nil, err
		}
		var ok bool
		desc, ok = v.(*memberlist.PropagationDelayTrackerDesc)
		if !ok {
			return nil, fmt.Errorf("decoded %T, expected *memberlist.PropagationDelayTrackerDesc", v)
		}
	} else {
		desc = memberlist.NewPropagationDelayTrackerDesc()
		for id, b := range in.Beacons {
			desc.Beacons[id] = memberlist.BeaconDesc{
				PublishedBy: b.PublishedBy,
				PublishedAt: b.PublishedAt,
				DeletedAt:   b.DeletedAt,
			}
		}
	}

	jsonBytes, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}
	jsonIndentBytes, err := json.MarshalIndent(desc, "", "  ")
	if err != nil {
		return nil, err
	}
	var encBuf bytes.Buffer
	if err := json.NewEncoder(&encBuf).Encode(desc); err != nil {
		return nil, err
	}
	var encIndBuf bytes.Buffer
	enc := json.NewEncoder(&encIndBuf)
	enc.SetIndent("", "    ")
	if err := enc.Encode(desc); err != nil {
		return nil, err
	}

	return &formatPropagationTrackerOut{
		GoString:         fmt.Sprintf("%#v", desc),
		JSON:             string(jsonBytes),
		JSONIndent:       string(jsonIndentBytes),
		JSONEncode:       encBuf.String(),
		JSONEncodeIndent: encIndBuf.String(),
	}, nil
}
