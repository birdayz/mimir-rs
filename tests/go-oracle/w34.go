// SPDX-License-Identifier: AGPL-3.0-only
// Wave 34 Tier-1 Go conformance oracle subcommands:
// fast-regex-check, yaml-v3-check, traceparent-check, ring-merge-check.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.yaml.in/yaml/v3"
)

// --- fast-regex-check ---

type fastRegexCaseIn struct {
	Pattern string   `json:"pattern"`
	Inputs  []string `json:"inputs"`
}

type fastRegexCheckIn struct {
	Cases []fastRegexCaseIn `json:"cases"`
}

type fastRegexCaseOut struct {
	Error      string   `json:"error"`
	SetMatches []string `json:"set_matches"`
	Matches    []bool   `json:"matches"`
}

type fastRegexCheckOut struct {
	Cases []fastRegexCaseOut `json:"cases"`
}

func cmdFastRegexCheck(input []byte) (*fastRegexCheckOut, error) {
	var in fastRegexCheckIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	out := &fastRegexCheckOut{
		Cases: make([]fastRegexCaseOut, len(in.Cases)),
	}
	for i, c := range in.Cases {
		m, err := labels.NewFastRegexMatcher(c.Pattern)
		if err != nil {
			out.Cases[i] = fastRegexCaseOut{
				Error:      err.Error(),
				SetMatches: []string{},
				Matches:    []bool{},
			}
			continue
		}
		setMatches := m.SetMatches()
		if setMatches == nil {
			setMatches = []string{}
		}
		matches := make([]bool, len(c.Inputs))
		for j, s := range c.Inputs {
			matches[j] = m.MatchString(s)
		}
		out.Cases[i] = fastRegexCaseOut{
			SetMatches: setMatches,
			Matches:    matches,
		}
	}
	return out, nil
}

// --- yaml-v3-check ---

type yamlV3CheckIn struct {
	Docs []string `json:"docs"`
}

type yamlV3DocOut struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

type yamlV3CheckOut struct {
	Docs []yamlV3DocOut `json:"docs"`
}

func cmdYamlV3Check(input []byte) (*yamlV3CheckOut, error) {
	var in yamlV3CheckIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	out := &yamlV3CheckOut{
		Docs: make([]yamlV3DocOut, len(in.Docs)),
	}
	for i, doc := range in.Docs {
		var v any
		if err := yaml.Unmarshal([]byte(doc), &v); err != nil {
			out.Docs[i] = yamlV3DocOut{
				OK:    false,
				Error: err.Error(),
			}
		} else {
			out.Docs[i] = yamlV3DocOut{
				OK: true,
			}
		}
	}
	return out, nil
}

// --- traceparent-check ---

type traceparentCheckIn struct {
	Headers []string `json:"headers"`
}

type traceparentHeaderOut struct {
	Valid      bool   `json:"valid"`
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`
	TraceFlags uint8  `json:"trace_flags"`
	Sampled    bool   `json:"sampled"`
}

type traceparentCheckOut struct {
	Headers []traceparentHeaderOut `json:"headers"`
}

func cmdTraceparentCheck(input []byte) (*traceparentCheckOut, error) {
	var in traceparentCheckIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	prop := propagation.TraceContext{}
	out := &traceparentCheckOut{
		Headers: make([]traceparentHeaderOut, len(in.Headers)),
	}
	for i, h := range in.Headers {
		ctx := prop.Extract(context.Background(), propagation.MapCarrier{"traceparent": h})
		sc := trace.SpanContextFromContext(ctx)
		if !sc.IsValid() {
			out.Headers[i] = traceparentHeaderOut{Valid: false}
			continue
		}
		out.Headers[i] = traceparentHeaderOut{
			Valid:      true,
			TraceID:    sc.TraceID().String(),
			SpanID:     sc.SpanID().String(),
			TraceFlags: uint8(sc.TraceFlags()),
			Sampled:    sc.IsSampled(),
		}
	}
	return out, nil
}

// --- ring-merge-check ---

type PartitionDescJSON struct {
	ID                         int32    `json:"id"`
	Tokens                     []uint32 `json:"tokens"`
	State                      int32    `json:"state"`
	StateTimestamp             int64    `json:"state_timestamp"`
	StateChangeLocked          bool     `json:"state_change_locked"`
	StateChangeLockedTimestamp int64    `json:"state_change_locked_timestamp"`
	TokenScheme                int32    `json:"token_scheme"`
}

type OwnerDescJSON struct {
	OwnedPartition   int32 `json:"owned_partition"`
	State            int32 `json:"state"`
	UpdatedTimestamp int64 `json:"updated_timestamp"`
}

type PartitionRingDescJSON struct {
	Partitions map[string]PartitionDescJSON `json:"partitions"`
	Owners     map[string]OwnerDescJSON     `json:"owners"`
}

type ringMergeCheckIn struct {
	LocalB64  string                 `json:"local_b64,omitempty"`
	RemoteB64 string                 `json:"remote_b64,omitempty"`
	Local     *PartitionRingDescJSON `json:"local,omitempty"`
	Remote    *PartitionRingDescJSON `json:"remote,omitempty"`
	LocalCAS  bool                   `json:"local_cas,omitempty"`
	Cas       bool                   `json:"cas,omitempty"`
	NowUnix   int64                  `json:"now_unix,omitempty"`
}

type ringMergeCheckOut struct {
	Changed    bool                         `json:"changed"`
	MergedB64  string                       `json:"merged_b64"`
	ChangeB64  *string                      `json:"change_b64,omitempty"`
	Partitions map[string]PartitionDescJSON `json:"partitions"`
	Owners     map[string]OwnerDescJSON     `json:"owners"`
}

func decodePartitionRingInput(b64 string, js *PartitionRingDescJSON, label string) (ring.PartitionRingDesc, error) {
	desc := ring.PartitionRingDesc{
		Partitions: map[int32]ring.PartitionDesc{},
		Owners:     map[string]ring.OwnerDesc{},
	}
	if b64 != "" {
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return desc, fmt.Errorf("%s base64: %w", label, err)
		}
		if err := desc.Unmarshal(raw); err != nil {
			return desc, fmt.Errorf("%s unmarshal: %w", label, err)
		}
		if desc.Partitions == nil {
			desc.Partitions = map[int32]ring.PartitionDesc{}
		}
		if desc.Owners == nil {
			desc.Owners = map[string]ring.OwnerDesc{}
		}
		return desc, nil
	}
	if js == nil {
		return desc, fmt.Errorf("missing %s_b64 or %s", label, label)
	}
	for k, p := range js.Partitions {
		pid64, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			return desc, fmt.Errorf("%s partition key %q: %w", label, k, err)
		}
		pid := int32(pid64)
		id := p.ID
		if id == 0 && pid != 0 {
			id = pid
		}
		tokens := append([]uint32(nil), p.Tokens...)
		desc.Partitions[pid] = ring.PartitionDesc{
			Id:                         id,
			Tokens:                     tokens,
			State:                      ring.PartitionState(p.State),
			StateTimestamp:             p.StateTimestamp,
			StateChangeLocked:          p.StateChangeLocked,
			StateChangeLockedTimestamp: p.StateChangeLockedTimestamp,
			TokenScheme:                ring.PartitionTokenScheme(p.TokenScheme),
		}
	}
	for k, o := range js.Owners {
		desc.Owners[k] = ring.OwnerDesc{
			OwnedPartition:   o.OwnedPartition,
			State:            ring.OwnerState(o.State),
			UpdatedTimestamp: o.UpdatedTimestamp,
		}
	}
	return desc, nil
}

func clonePartitionRingDesc(src ring.PartitionRingDesc) ring.PartitionRingDesc {
	out := ring.PartitionRingDesc{
		Partitions: make(map[int32]ring.PartitionDesc, len(src.Partitions)),
		Owners:     make(map[string]ring.OwnerDesc, len(src.Owners)),
	}
	for k, v := range src.Partitions {
		v.Tokens = append([]uint32(nil), v.Tokens...)
		out.Partitions[k] = v
	}
	for k, v := range src.Owners {
		out.Owners[k] = v
	}
	return out
}

func shiftTimestamp(ts, delta int64) int64 {
	if delta > 0 && ts > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && ts < math.MinInt64-delta {
		return math.MinInt64
	}
	return ts + delta
}

func unshiftTimestamp(ts, delta, origLocal, origRemote int64) int64 {
	if delta > 0 && ts == math.MaxInt64 {
		if origLocal > math.MaxInt64-delta {
			return origLocal
		}
		if origRemote > math.MaxInt64-delta {
			return origRemote
		}
	}
	if delta < 0 && ts == math.MinInt64 {
		if origLocal < math.MinInt64-delta {
			return origLocal
		}
		if origRemote < math.MinInt64-delta {
			return origRemote
		}
	}
	return ts - delta
}

func shiftPartitionRingDesc(d *ring.PartitionRingDesc, delta int64) {
	for k, p := range d.Partitions {
		p.StateTimestamp = shiftTimestamp(p.StateTimestamp, delta)
		p.StateChangeLockedTimestamp = shiftTimestamp(p.StateChangeLockedTimestamp, delta)
		d.Partitions[k] = p
	}
	for k, o := range d.Owners {
		o.UpdatedTimestamp = shiftTimestamp(o.UpdatedTimestamp, delta)
		d.Owners[k] = o
	}
}

func unshiftPartitionRingDesc(d *ring.PartitionRingDesc, delta int64, origLocal, origRemote ring.PartitionRingDesc) {
	for k, p := range d.Partitions {
		lp := origLocal.Partitions[k]
		rp := origRemote.Partitions[k]
		p.StateTimestamp = unshiftTimestamp(p.StateTimestamp, delta, lp.StateTimestamp, rp.StateTimestamp)
		p.StateChangeLockedTimestamp = unshiftTimestamp(p.StateChangeLockedTimestamp, delta, lp.StateChangeLockedTimestamp, rp.StateChangeLockedTimestamp)
		d.Partitions[k] = p
	}
	for k, o := range d.Owners {
		lo := origLocal.Owners[k]
		ro := origRemote.Owners[k]
		o.UpdatedTimestamp = unshiftTimestamp(o.UpdatedTimestamp, delta, lo.UpdatedTimestamp, ro.UpdatedTimestamp)
		d.Owners[k] = o
	}
}

func partitionRingDescToJSON(d ring.PartitionRingDesc) (map[string]PartitionDescJSON, map[string]OwnerDescJSON) {
	parts := make(map[string]PartitionDescJSON, len(d.Partitions))
	for id, p := range d.Partitions {
		tokens := append([]uint32(nil), p.Tokens...)
		if tokens == nil {
			tokens = []uint32{}
		}
		parts[strconv.FormatInt(int64(id), 10)] = PartitionDescJSON{
			ID:                         p.Id,
			Tokens:                     tokens,
			State:                      int32(p.State),
			StateTimestamp:             p.StateTimestamp,
			StateChangeLocked:          p.StateChangeLocked,
			StateChangeLockedTimestamp: p.StateChangeLockedTimestamp,
			TokenScheme:                int32(p.TokenScheme),
		}
	}
	owners := make(map[string]OwnerDescJSON, len(d.Owners))
	for id, o := range d.Owners {
		owners[id] = OwnerDescJSON{
			OwnedPartition:   o.OwnedPartition,
			State:            int32(o.State),
			UpdatedTimestamp: o.UpdatedTimestamp,
		}
	}
	return parts, owners
}

func cmdRingMergeCheck(input []byte) (*ringMergeCheckOut, error) {
	var in ringMergeCheckIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	local, err := decodePartitionRingInput(in.LocalB64, in.Local, "local")
	if err != nil {
		return nil, err
	}
	remote, err := decodePartitionRingInput(in.RemoteB64, in.Remote, "remote")
	if err != nil {
		return nil, err
	}

	cas := in.LocalCAS || in.Cas

	var merged ring.PartitionRingDesc
	var change memberlist.Mergeable
	for attempt := 0; attempt < 2; attempt++ {
		l := clonePartitionRingDesc(local)
		r := clonePartitionRingDesc(remote)
		before := time.Now().Unix()
		if in.NowUnix != 0 {
			delta := before - in.NowUnix
			shiftPartitionRingDesc(&l, delta)
			shiftPartitionRingDesc(&r, delta)
		}
		ch, err := l.Merge(&r, cas)
		if err != nil {
			return nil, err
		}
		after := time.Now().Unix()
		if in.NowUnix == 0 || after == before || attempt == 1 {
			if in.NowUnix != 0 {
				delta := before - in.NowUnix
				unshiftPartitionRingDesc(&l, delta, local, remote)
				if ch != nil {
					if cd, ok := ch.(*ring.PartitionRingDesc); ok && cd != nil {
						unshiftPartitionRingDesc(cd, delta, local, remote)
					}
				}
			}
			merged = l
			change = ch
			break
		}
	}

	mergedBytes, err := merged.Marshal()
	if err != nil {
		return nil, err
	}

	parts, owners := partitionRingDescToJSON(merged)
	out := &ringMergeCheckOut{
		Changed:    change != nil,
		MergedB64:  base64.StdEncoding.EncodeToString(mergedBytes),
		Partitions: parts,
		Owners:     owners,
	}

	if change != nil {
		if changeDesc, ok := change.(*ring.PartitionRingDesc); ok && changeDesc != nil {
			changeBytes, err := changeDesc.Marshal()
			if err != nil {
				return nil, err
			}
			out.ChangeB64 = ptrStr(base64.StdEncoding.EncodeToString(changeBytes))
		}
	}

	return out, nil
}
