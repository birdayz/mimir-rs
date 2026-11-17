// SPDX-License-Identifier: AGPL-3.0-only
// tsdb-open-report: opens a Prometheus TSDB directory exactly like Mimir's ingester does
// (tsdb.Open with a per-tenant registry) and reports whether it opened, the per-tenant
// prometheus_tsdb_* metric values, the on-disk files left after Open, and the queried data.
// Used for Go-vs-Rust differential tests of the startup repair paths (chunks_head / WAL /
// WBL corruption) and of the per-tenant TSDB metrics.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

type tsdbOpenReportIn struct {
	Dir                    string `json:"dir"`
	OutOfOrderTimeWindowMs int64  `json:"out_of_order_time_window_ms"`
	// Optional: call db.Compact() / head truncation after open (for metric differentials).
	Compact bool `json:"compact"`
}

type tsdbOpenReportSeries struct {
	Labels  []LabelJSON  `json:"labels"`
	Samples [][2]float64 `json:"samples"`
}

type tsdbOpenReportOut struct {
	OpenError  string                 `json:"open_error"`
	Metrics    map[string]float64     `json:"metrics"`
	Files      map[string][]string    `json:"files"`
	Records    map[string]int         `json:"records"`
	HeadSeries uint64                 `json:"head_series"`
	MinOOOTime int64                  `json:"min_ooo_time"`
	Series     []tsdbOpenReportSeries `json:"series"`
}

func listDirNames(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := []string{}
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func countWlogRecords(dir string) int {
	if _, err := os.Stat(dir); err != nil {
		return -1
	}
	sr, err := wlog.NewSegmentsReader(dir)
	if err != nil {
		return -1
	}
	defer sr.Close()
	r := wlog.NewReader(sr)
	n := 0
	for r.Next() {
		n++
	}
	return n
}

func gatherMetricValues(reg *prometheus.Registry) map[string]float64 {
	out := map[string]float64{}
	mfs, err := reg.Gather()
	if err != nil {
		return out
	}
	for _, mf := range mfs {
		var sum float64
		for _, m := range mf.GetMetric() {
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				sum += m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				sum += m.GetGauge().GetValue()
			case dto.MetricType_SUMMARY:
				sum += float64(m.GetSummary().GetSampleCount())
			case dto.MetricType_HISTOGRAM:
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
		out[mf.GetName()] = sum
	}
	return out
}

func cmdTSDBOpenReport(input []byte) (*tsdbOpenReportOut, error) {
	var in tsdbOpenReportIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	out := &tsdbOpenReportOut{Files: map[string][]string{}, Records: map[string]int{}}

	reg := prometheus.NewRegistry()
	opts := tsdb.DefaultOptions()
	opts.NoLockfile = true
	opts.OutOfOrderTimeWindow = in.OutOfOrderTimeWindowMs
	db, err := tsdb.Open(in.Dir, slog.New(slog.DiscardHandler), reg, opts, nil)

	snapshotFiles := func() {
		for _, sub := range []string{"chunks_head", "wal", wlog.WblDirName} {
			out.Files[sub] = listDirNames(filepath.Join(in.Dir, sub))
			if sub != "chunks_head" {
				out.Records[sub] = countWlogRecords(filepath.Join(in.Dir, sub))
			}
		}
	}

	if err != nil {
		out.OpenError = err.Error()
		out.Metrics = gatherMetricValues(reg)
		snapshotFiles()
		return out, nil
	}
	defer db.Close()

	if in.Compact {
		if err := db.Compact(context.Background()); err != nil {
			return nil, err
		}
	}

	out.Metrics = gatherMetricValues(reg)
	snapshotFiles()
	out.HeadSeries = db.Head().NumSeries()
	out.MinOOOTime = db.Head().MinOOOTime()

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	ss := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*"))
	for ss.Next() {
		s := ss.At()
		entry := tsdbOpenReportSeries{Labels: fromPromLabels(s.Labels()), Samples: [][2]float64{}}
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			if vt == chunkenc.ValFloat {
				t, v := it.At()
				entry.Samples = append(entry.Samples, [2]float64{float64(t), v})
			}
		}
		out.Series = append(out.Series, entry)
	}
	if err := ss.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
