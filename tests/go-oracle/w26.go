// SPDX-License-Identifier: AGPL-3.0-only
// Wave 26 Tier-1 Go conformance oracle subcommands.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdbencoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.yaml.in/yaml/v3"
)

// --- validate-config ---

type validateConfigIn struct {
	YAML string   `json:"yaml"`
	Args []string `json:"args"`
}

type validateConfigOut struct {
	OK    bool   `json:"ok"`
	Stage string `json:"stage"`
	Error string `json:"error"`
}

func cmdValidateConfig(input []byte) (*validateConfigOut, error) {
	var in validateConfigIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	logger := kitlog.NewNopLogger()
	fs := flag.NewFlagSet("mimir", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var cfg mimir.Config
	cfg.RegisterFlags(fs, logger)
	flagext.IgnoredFlag(fs, "config.file", "Configuration file to load.")
	_ = fs.Bool("config.expand-env", false, "Expands env vars.")

	if in.YAML != "" {
		dec := yaml.NewDecoder(strings.NewReader(in.YAML))
		dec.KnownFields(true)
		if err := dec.Decode((*mimir.ConfigWithCommon)(&cfg)); err != nil {
			return &validateConfigOut{
				OK:    false,
				Stage: "yaml",
				Error: err.Error(),
			}, nil
		}
	}

	if err := fs.Parse(in.Args); err != nil {
		return &validateConfigOut{
			OK:    false,
			Stage: "flag",
			Error: err.Error(),
		}, nil
	}

	if err := mimir.InheritCommonFlagValues(logger, fs, cfg.Common, &cfg); err != nil {
		return &validateConfigOut{
			OK:    false,
			Stage: "inherit",
			Error: err.Error(),
		}, nil
	}

	if err := cfg.Validate(logger); err != nil {
		return &validateConfigOut{
			OK:    false,
			Stage: "validate",
			Error: err.Error(),
		}, nil
	}

	return &validateConfigOut{
		OK:    true,
		Stage: "ok",
	}, nil
}

// --- load-runtime-config ---

type loadRuntimeConfigIn struct {
	DefaultLimitsYAML string   `json:"default_limits_yaml"`
	Files             []string `json:"files"`
	Loader            string   `json:"loader"`
	QueryStoreAfter   string   `json:"query_store_after"`
}

type loadRuntimeConfigOut struct {
	OK            bool            `json:"ok"`
	Error         string          `json:"error"`
	OverridesJSON json.RawMessage `json:"overrides_json,omitempty"`
}

func cmdLoadRuntimeConfig(input []byte) (*loadRuntimeConfigOut, error) {
	var in loadRuntimeConfigIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	logger := kitlog.NewNopLogger()
	fs := flag.NewFlagSet("mimir", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var cfg mimir.Config
	cfg.RegisterFlags(fs, logger)

	if strings.TrimSpace(in.DefaultLimitsYAML) != "" {
		dec := yaml.NewDecoder(strings.NewReader(in.DefaultLimitsYAML))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg.LimitsConfig); err != nil {
			return &loadRuntimeConfigOut{
				OK:    false,
				Error: fmt.Sprintf("default_limits_yaml: %v", err),
			}, nil
		}
	}

	if strings.TrimSpace(in.QueryStoreAfter) != "" {
		d, err := time.ParseDuration(in.QueryStoreAfter)
		if err != nil {
			return &loadRuntimeConfigOut{
				OK:    false,
				Error: fmt.Sprintf("query_store_after: %v", err),
			}, nil
		}
		cfg.Querier.QueryStoreAfter = d
	}

	cfg.RuntimeConfig.Loader = in.Loader
	cfg.RuntimeConfig.LoadPath = flagext.StringSliceCSV(in.Files)

	mgr, err := mimir.NewRuntimeManager(&cfg, "oracle", prometheus.NewRegistry(), logger)
	if err != nil {
		return &loadRuntimeConfigOut{
			OK:    false,
			Error: err.Error(),
		}, nil
	}

	if err := services.StartAndAwaitRunning(context.Background(), mgr); err != nil {
		return &loadRuntimeConfigOut{
			OK:    false,
			Error: err.Error(),
		}, nil
	}
	defer func() {
		_ = services.StopAndAwaitTerminated(context.Background(), mgr)
	}()

	overridesRaw := []byte("{}")
	if val := mgr.GetConfig(); val != nil {
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			field := rv.Elem().FieldByName("TenantLimits")
			if field.IsValid() && field.CanInterface() && !field.IsNil() {
				b, err := json.Marshal(field.Interface())
				if err != nil {
					return nil, fmt.Errorf("marshal overrides: %w", err)
				}
				overridesRaw = b
			}
		}
	}

	return &loadRuntimeConfigOut{
		OK:            true,
		OverridesJSON: overridesRaw,
	}, nil
}

// --- write-index-v1-fixture & verify-index-file ---

type writeIndexV1In struct {
	Path string `json:"path"`
}

type indexChunkMetaJSON struct {
	RefID   uint64 `json:"ref_id"`
	MinTime int64  `json:"min_time"`
	MaxTime int64  `json:"max_time"`
}

type indexSeriesJSON struct {
	RefID  uint64               `json:"ref_id"`
	Labels []LabelJSON          `json:"labels"`
	Chunks []indexChunkMetaJSON `json:"chunks"`
}

type writeIndexV1Out struct {
	Version           int                 `json:"version"`
	LabelNames        []string            `json:"label_names"`
	SortedLabelValues map[string][]string `json:"sorted_label_values"`
	LabelValues       map[string][]string `json:"label_values"`
	Postings          map[string][]uint64 `json:"postings"`
	AllPostings       []uint64            `json:"all_postings"`
	Series            []indexSeriesJSON   `json:"series"`
}

func appendUvarintBytes(dst []byte, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func appendLengthAndCRC32C(dst []byte, payload []byte, table *crc32.Table) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	dst = append(dst, lenBuf[:]...)
	dst = append(dst, payload...)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.Checksum(payload, table))
	return append(dst, crcBuf[:]...)
}

func cmdWriteIndexV1Fixture(input []byte) (*writeIndexV1Out, error) {
	var in writeIndexV1In
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(in.Path), 0o755); err != nil {
		return nil, err
	}

	castagnoli := crc32.MakeTable(crc32.Castagnoli)

	type fixtureSeries struct {
		lbls   []LabelJSON
		chunks []indexChunkMetaJSON
	}

	// Build >32 unique sorted symbols so symbolFactor=32 is exercised.
	seriesList := []fixtureSeries{
		{
			lbls: []LabelJSON{
				{Name: "__name__", Value: "alpha_metric"},
				{Name: "env", Value: "prod"},
				{Name: "instance", Value: "h01"},
				{Name: "job", Value: "zeta"},
			},
			chunks: []indexChunkMetaJSON{
				{RefID: 100, MinTime: 1000, MaxTime: 2000},
				{RefID: 250, MinTime: 2001, MaxTime: 3000},
			},
		},
		{
			lbls: []LabelJSON{
				{Name: "__name__", Value: "beta_metric"},
				{Name: "env", Value: "staging"},
				{Name: "instance", Value: "h02"},
				{Name: "job", Value: "alpha"},
			},
			chunks: []indexChunkMetaJSON{
				{RefID: 300, MinTime: 1500, MaxTime: 2500},
			},
		},
		{
			lbls: []LabelJSON{
				{Name: "__name__", Value: "gamma_metric"},
				{Name: "env", Value: "dev"},
				{Name: "instance", Value: "h03"},
				{Name: "job", Value: "alpha"},
				{Name: "region", Value: "us-east"},
			},
			chunks: []indexChunkMetaJSON{
				{RefID: 400, MinTime: 1000, MaxTime: 1999},
				{RefID: 555, MinTime: 2005, MaxTime: 3005},
			},
		},
		{
			lbls: []LabelJSON{
				{Name: "__name__", Value: "omega_metric"},
				{Name: "env", Value: "prod"},
				{Name: "instance", Value: "h04"},
				{Name: "job", Value: "middle"},
				{Name: "tier", Value: "core"},
			},
			chunks: []indexChunkMetaJSON{
				{RefID: 777, MinTime: 5000, MaxTime: 6000},
			},
		},
	}

	symSet := map[string]struct{}{"": {}}
	for _, s := range seriesList {
		for _, l := range s.lbls {
			symSet[l.Name] = struct{}{}
			symSet[l.Value] = struct{}{}
		}
	}
	// Pad out to >40 symbols so symbolFactor=32 chunking is exercised.
	for i := 0; i < 28; i++ {
		symSet[fmt.Sprintf("pad_sym_%02d", i)] = struct{}{}
	}
	symbols := make([]string, 0, len(symSet))
	for sym := range symSet {
		symbols = append(symbols, sym)
	}
	slices.Sort(symbols)

	var buf []byte
	// Header: MagicIndex + FormatV1
	buf = binary.BigEndian.AppendUint32(buf, index.MagicIndex)
	buf = append(buf, byte(index.FormatV1))

	// Symbols section
	tocSymbols := uint64(len(buf))
	var symPayload []byte
	symPayload = binary.BigEndian.AppendUint32(symPayload, uint32(len(symbols)))
	symByteOffsets := make(map[string]uint32, len(symbols))
	// File offset where symPayload starts is tocSymbols + 4 (4-byte length prefix).
	symPayloadFileBase := len(buf) + 4
	for _, sym := range symbols {
		symByteOffsets[sym] = uint32(symPayloadFileBase + len(symPayload))
		symPayload = appendUvarintBytes(symPayload, []byte(sym))
	}
	buf = appendLengthAndCRC32C(buf, symPayload, castagnoli)

	// Series section (explicitly non-16-aligned byte offsets!)
	tocSeries := uint64(len(buf))
	seriesOffsets := make([]uint32, len(seriesList))
	for i, s := range seriesList {
		// Ensure every series offset is NOT 16-byte aligned to test V1 exact byte offsets.
		if len(buf)%16 == 0 {
			buf = append(buf, 0x00)
		}
		seriesOffsets[i] = uint32(len(buf))

		var sb []byte
		sb = binary.AppendUvarint(sb, uint64(len(s.lbls)))
		for _, l := range s.lbls {
			sb = binary.AppendUvarint(sb, uint64(symByteOffsets[l.Name]))
			sb = binary.AppendUvarint(sb, uint64(symByteOffsets[l.Value]))
		}
		sb = binary.AppendUvarint(sb, uint64(len(s.chunks)))
		if len(s.chunks) > 0 {
			c0 := s.chunks[0]
			var eb tsdbencoding.Encbuf
			eb.PutVarint64(c0.MinTime)
			eb.PutUvarint64(uint64(c0.MaxTime - c0.MinTime))
			eb.PutUvarint64(c0.RefID)
			prevMaxT := c0.MaxTime
			prevRef := int64(c0.RefID)
			for _, c := range s.chunks[1:] {
				eb.PutUvarint64(uint64(c.MinTime - prevMaxT))
				eb.PutUvarint64(uint64(c.MaxTime - c.MinTime))
				prevMaxT = c.MaxTime
				eb.PutVarint64(int64(c.RefID) - prevRef)
				prevRef = int64(c.RefID)
			}
			sb = append(sb, eb.Get()...)
		}

		buf = binary.AppendUvarint(buf, uint64(len(sb)))
		buf = append(buf, sb...)
		buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(sb, castagnoli))
	}

	// Postings section
	for len(buf)%4 != 0 {
		buf = append(buf, 0x00)
	}
	tocLabelIndices := uint64(len(buf))
	tocPostings := uint64(len(buf))

	type poEntry struct {
		name string
		val  string
		off  uint64
	}
	postingsMap := map[string]map[string][]uint32{
		"": {"": slices.Clone(seriesOffsets)},
	}
	for i, s := range seriesList {
		for _, l := range s.lbls {
			if postingsMap[l.Name] == nil {
				postingsMap[l.Name] = map[string][]uint32{}
			}
			postingsMap[l.Name][l.Value] = append(postingsMap[l.Name][l.Value], seriesOffsets[i])
		}
	}

	var poEntries []poEntry
	// Emit postings lists, keeping poEntries intentionally UNSORTED in the offset table!
	var keys []struct{ n, v string }
	for n, vm := range postingsMap {
		for v := range vm {
			keys = append(keys, struct{ n, v string }{n: n, v: v})
		}
	}
	// Reverse sort so postings offset table is strictly descending (unsorted vs V2)!
	slices.SortFunc(keys, func(a, b struct{ n, v string }) int {
		if a.n != b.n {
			return strings.Compare(b.n, a.n)
		}
		return strings.Compare(b.v, a.v)
	})

	for _, kv := range keys {
		offs := postingsMap[kv.n][kv.v]
		for len(buf)%4 != 0 {
			buf = append(buf, 0x00)
		}
		postingOff := uint64(len(buf))
		var pp []byte
		pp = binary.BigEndian.AppendUint32(pp, uint32(len(offs)))
		for _, o := range offs {
			pp = binary.BigEndian.AppendUint32(pp, o)
		}
		buf = appendLengthAndCRC32C(buf, pp, castagnoli)
		poEntries = append(poEntries, poEntry{name: kv.n, val: kv.v, off: postingOff})
	}

	// Postings Offset Table (unsorted!)
	tocLabelIndicesTable := uint64(len(buf))
	tocPostingsTable := uint64(len(buf))
	var poTablePayload []byte
	poTablePayload = binary.BigEndian.AppendUint32(poTablePayload, uint32(len(poEntries)))
	for _, e := range poEntries {
		poTablePayload = binary.AppendUvarint(poTablePayload, 2)
		poTablePayload = appendUvarintBytes(poTablePayload, []byte(e.name))
		poTablePayload = appendUvarintBytes(poTablePayload, []byte(e.val))
		poTablePayload = binary.AppendUvarint(poTablePayload, e.off)
	}
	buf = appendLengthAndCRC32C(buf, poTablePayload, castagnoli)

	// TOC
	var tocBuf bytes.Buffer
	_ = binary.Write(&tocBuf, binary.BigEndian, tocSymbols)
	_ = binary.Write(&tocBuf, binary.BigEndian, tocSeries)
	_ = binary.Write(&tocBuf, binary.BigEndian, tocLabelIndices)
	_ = binary.Write(&tocBuf, binary.BigEndian, tocLabelIndicesTable)
	_ = binary.Write(&tocBuf, binary.BigEndian, tocPostings)
	_ = binary.Write(&tocBuf, binary.BigEndian, tocPostingsTable)
	tocBytes := tocBuf.Bytes()
	buf = append(buf, tocBytes...)
	buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(tocBytes, castagnoli))

	if err := os.WriteFile(in.Path, buf, 0o644); err != nil {
		return nil, err
	}

	// Verify and query via upstream Go tsdb/index.NewReader
	r, err := index.NewFileReader(in.Path, index.DecodePostingsRaw)
	if err != nil {
		return nil, fmt.Errorf("index.NewReader: %w", err)
	}
	defer r.Close()

	ctx := context.Background()
	labelNames, err := r.LabelNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("LabelNames: %w", err)
	}

	sortedLabelValues := make(map[string][]string, len(labelNames))
	labelValues := make(map[string][]string, len(labelNames))
	postingsOut := make(map[string][]uint64)

	for _, name := range labelNames {
		sv, err := r.SortedLabelValues(ctx, name, nil)
		if err != nil {
			return nil, fmt.Errorf("SortedLabelValues(%q): %w", name, err)
		}
		sortedLabelValues[name] = sv

		lv, err := r.LabelValues(ctx, name, nil)
		if err != nil {
			return nil, fmt.Errorf("LabelValues(%q): %w", name, err)
		}
		labelValues[name] = lv

		for _, v := range sv {
			p, err := r.Postings(ctx, name, v)
			if err != nil {
				return nil, fmt.Errorf("Postings(%q, %q): %w", name, v, err)
			}
			ids, err := index.ExpandPostings(p)
			if err != nil {
				return nil, fmt.Errorf("ExpandPostings(%q, %q): %w", name, v, err)
			}
			u64 := make([]uint64, len(ids))
			for i, id := range ids {
				u64[i] = uint64(id)
			}
			postingsOut[name+"="+v] = u64
		}
	}

	// Also query multi-value Postings("job", "alpha", "zeta")
	pMulti, err := r.Postings(ctx, "job", "alpha", "zeta")
	if err != nil {
		return nil, fmt.Errorf("Postings multi: %w", err)
	}
	multiIDs, err := index.ExpandPostings(pMulti)
	if err != nil {
		return nil, fmt.Errorf("ExpandPostings multi: %w", err)
	}
	multiU64 := make([]uint64, len(multiIDs))
	for i, id := range multiIDs {
		multiU64[i] = uint64(id)
	}
	postingsOut["job=alpha|zeta"] = multiU64

	pAll, err := r.Postings(ctx, "", "")
	if err != nil {
		return nil, fmt.Errorf("AllPostings: %w", err)
	}
	allIDs, err := index.ExpandPostings(pAll)
	if err != nil {
		return nil, fmt.Errorf("ExpandAllPostings: %w", err)
	}
	allU64 := make([]uint64, len(allIDs))
	var decodedSeries []indexSeriesJSON
	var sb labels.ScratchBuilder
	var chks []chunks.Meta
	for i, id := range allIDs {
		allU64[i] = uint64(id)
		chks = chks[:0]
		if err := r.Series(id, &sb, &chks); err != nil {
			return nil, fmt.Errorf("Series(%d): %w", id, err)
		}
		var chunkList []indexChunkMetaJSON
		for _, c := range chks {
			chunkList = append(chunkList, indexChunkMetaJSON{
				RefID:   uint64(c.Ref),
				MinTime: c.MinTime,
				MaxTime: c.MaxTime,
			})
		}
		decodedSeries = append(decodedSeries, indexSeriesJSON{
			RefID:  uint64(id),
			Labels: fromPromLabels(sb.Labels()),
			Chunks: chunkList,
		})
	}

	return &writeIndexV1Out{
		Version:           r.Version(),
		LabelNames:        labelNames,
		SortedLabelValues: sortedLabelValues,
		LabelValues:       labelValues,
		Postings:          postingsOut,
		AllPostings:       allU64,
		Series:            decodedSeries,
	}, nil
}

type verifyIndexFileIn struct {
	Path string `json:"path"`
}

type verifyIndexFileOut struct {
	OK          bool     `json:"ok"`
	Error       string   `json:"error"`
	Version     int      `json:"version"`
	LabelNames  []string `json:"label_names"`
	SeriesCount int      `json:"series_count"`
	QueryError  string   `json:"query_error,omitempty"`
}

func cmdVerifyIndexFile(input []byte) (*verifyIndexFileOut, error) {
	var in verifyIndexFileIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	r, err := index.NewFileReader(in.Path, index.DecodePostingsRaw)
	if err != nil {
		return &verifyIndexFileOut{
			OK:    false,
			Error: err.Error(),
		}, nil
	}
	defer r.Close()

	ctx := context.Background()
	ln, err := r.LabelNames(ctx)
	if err != nil {
		return &verifyIndexFileOut{
			OK:         true,
			Version:    r.Version(),
			QueryError: err.Error(),
		}, nil
	}

	pAll, err := r.Postings(ctx, "", "")
	if err != nil {
		return &verifyIndexFileOut{
			OK:         true,
			Version:    r.Version(),
			LabelNames: ln,
			QueryError: err.Error(),
		}, nil
	}
	allIDs, err := index.ExpandPostings(pAll)
	if err != nil {
		return &verifyIndexFileOut{
			OK:         true,
			Version:    r.Version(),
			LabelNames: ln,
			QueryError: err.Error(),
		}, nil
	}

	var sb labels.ScratchBuilder
	var chks []chunks.Meta
	for _, id := range allIDs {
		chks = chks[:0]
		if err := r.Series(id, &sb, &chks); err != nil {
			return &verifyIndexFileOut{
				OK:          true,
				Version:     r.Version(),
				LabelNames:  ln,
				SeriesCount: len(allIDs),
				QueryError:  err.Error(),
			}, nil
		}
	}

	return &verifyIndexFileOut{
		OK:          true,
		Version:     r.Version(),
		LabelNames:  ln,
		SeriesCount: len(allIDs),
	}, nil
}

// --- serialize-ingest-records ---

type serializeIngestRecordsIn struct {
	Version        int    `json:"version"`
	UserID         string `json:"user_id"`
	WriteReqB64    string `json:"write_req_b64"`
	MaxRecordBytes int    `json:"max_record_bytes"`
	Topic          string `json:"topic,omitempty"`
	Partition      int32  `json:"partition,omitempty"`
}

type serializeIngestRecordsOut struct {
	OK         bool     `json:"ok"`
	Error      string   `json:"error"`
	RecordsB64 []string `json:"records_b64"`
	InputSize  int      `json:"input_size"`
}

func cmdSerializeIngestRecords(input []byte) (*serializeIngestRecordsOut, error) {
	var in serializeIngestRecordsIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(in.WriteReqB64)
	if err != nil {
		return &serializeIngestRecordsOut{
			OK:    false,
			Error: err.Error(),
		}, nil
	}

	req := &mimirpb.WriteRequest{}
	if err := req.Unmarshal(raw); err != nil {
		return &serializeIngestRecordsOut{
			OK:    false,
			Error: err.Error(),
		}, nil
	}

	topic := in.Topic
	if topic == "" {
		topic = "ingest"
	}
	serializer := ingest.RecordSerializerFromVersion(in.Version)
	records, inputSize, err := serializer.ToRecords(topic, in.Partition, in.UserID, req, in.MaxRecordBytes)
	if err != nil {
		return &serializeIngestRecordsOut{
			OK:        false,
			Error:     err.Error(),
			InputSize: inputSize,
		}, nil
	}

	recsB64 := make([]string, len(records))
	for i, r := range records {
		recsB64[i] = base64.StdEncoding.EncodeToString(r.Value)
	}
	return &serializeIngestRecordsOut{
		OK:         true,
		RecordsB64: recsB64,
		InputSize:  inputSize,
	}, nil
}
