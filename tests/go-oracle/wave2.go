// SPDX-License-Identifier: AGPL-3.0-only
// Wave 2 Tier-1 Go conformance oracle subcommands.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func ptrStr(s string) *string {
	return &s
}

// --- partition-tokens ---

type partitionTokensIn struct {
	Partitions int32 `json:"partitions"`
}

type partitionTokensOut struct {
	TokensByPartition [][]uint32 `json:"tokens_by_partition"`
}

func cmdPartitionTokens(input []byte) (*partitionTokensOut, error) {
	var in partitionTokensIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	table, err := ring.NewPartitionTokenTable(in.Partitions, kitlog.NewNopLogger(), prometheus.NewRegistry())
	if err != nil {
		return nil, err
	}

	out := make([][]uint32, in.Partitions)
	for id := int32(0); id < in.Partitions; id++ {
		toks, err := table.TokensFor(id)
		if err != nil {
			return nil, err
		}
		cp := make([]uint32, len(toks))
		copy(cp, toks)
		out[id] = cp
	}
	return &partitionTokensOut{TokensByPartition: out}, nil
}

// --- ring-lookup ---

type ringLookupIn struct {
	DescB64              string `json:"desc_b64"`
	ReplicationFactor    int    `json:"replication_factor"`
	HeartbeatTimeoutMs   int64  `json:"heartbeat_timeout_ms"`
	ZoneAwarenessEnabled bool   `json:"zone_awareness_enabled"`
	Op                   string `json:"op"`
	Token                uint32 `json:"token"`
	NowUnixSec           int64  `json:"now_unix_sec"`
}

type ringLookupOut struct {
	InstanceIDs         []string `json:"instance_ids"`
	MaxErrors           int      `json:"max_errors"`
	MaxUnavailableZones int      `json:"max_unavailable_zones"`
	Error               *string  `json:"error,omitempty"`
}

func parseRingOp(op string) (ring.Operation, error) {
	switch op {
	case "Write":
		return ring.Write, nil
	case "WriteNoExtend":
		return ring.WriteNoExtend, nil
	case "Read":
		return ring.Read, nil
	case "Reporting":
		return ring.Reporting, nil
	default:
		return 0, fmt.Errorf("unknown ring op %q", op)
	}
}

func cmdRingLookup(input []byte) (*ringLookupOut, error) {
	var in ringLookupIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	op, err := parseRingOp(in.Op)
	if err != nil {
		return nil, err
	}

	raw, err := base64.StdEncoding.DecodeString(in.DescB64)
	if err != nil {
		return nil, err
	}

	var desc ring.Desc
	if err := desc.Unmarshal(raw); err != nil {
		return nil, err
	}
	if desc.Ingesters == nil {
		desc.Ingesters = map[string]ring.InstanceDesc{}
	}

	if in.NowUnixSec != 0 {
		shift := time.Now().Unix() - in.NowUnixSec
		for id, inst := range desc.Ingesters {
			inst.Timestamp += shift
			desc.Ingesters[id] = inst
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), kitlog.NewNopLogger(), nil)
	defer func() { _ = closer.Close() }()

	if err := kvStore.CAS(ctx, "ring", func(_ any) (any, bool, error) {
		return &desc, true, nil
	}); err != nil {
		return nil, err
	}

	cfg := ring.Config{
		HeartbeatTimeout:     time.Duration(in.HeartbeatTimeoutMs) * time.Millisecond,
		ReplicationFactor:    in.ReplicationFactor,
		ZoneAwarenessEnabled: in.ZoneAwarenessEnabled,
	}

	r, err := ring.NewWithStoreClientAndStrategy(
		cfg,
		"oracle",
		"ring",
		kvStore,
		ring.NewDefaultReplicationStrategy(),
		prometheus.NewRegistry(),
		kitlog.NewNopLogger(),
	)
	if err != nil {
		return &ringLookupOut{
			InstanceIDs: []string{},
			Error:       ptrStr(err.Error()),
		}, nil
	}

	if err := services.StartAndAwaitRunning(ctx, r); err != nil {
		return nil, err
	}
	defer func() { _ = services.StopAndAwaitTerminated(context.Background(), r) }()

	rs, err := r.Get(in.Token, op, nil, nil, nil)
	if err != nil {
		return &ringLookupOut{
			InstanceIDs: []string{},
			Error:       ptrStr(err.Error()),
		}, nil
	}

	ids := make([]string, len(rs.Instances))
	for i, inst := range rs.Instances {
		ids[i] = inst.Id
	}

	return &ringLookupOut{
		InstanceIDs:         ids,
		MaxErrors:           rs.MaxErrors,
		MaxUnavailableZones: rs.MaxUnavailableZones,
	}, nil
}

// --- ring-merge ---

type ringMergeIn struct {
	LocalB64  string `json:"local_b64"`
	RemoteB64 string `json:"remote_b64"`
	Cas       bool   `json:"cas"`
}

type ringMergeOut struct {
	MergedB64 string  `json:"merged_b64"`
	ChangeB64 *string `json:"change_b64,omitempty"`
}

func cmdRingMerge(input []byte) (*ringMergeOut, error) {
	var in ringMergeIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	localRaw, err := base64.StdEncoding.DecodeString(in.LocalB64)
	if err != nil {
		return nil, err
	}
	remoteRaw, err := base64.StdEncoding.DecodeString(in.RemoteB64)
	if err != nil {
		return nil, err
	}

	var local ring.Desc
	if err := local.Unmarshal(localRaw); err != nil {
		return nil, err
	}
	if local.Ingesters == nil {
		local.Ingesters = map[string]ring.InstanceDesc{}
	}

	var remote ring.Desc
	if err := remote.Unmarshal(remoteRaw); err != nil {
		return nil, err
	}
	if remote.Ingesters == nil {
		remote.Ingesters = map[string]ring.InstanceDesc{}
	}

	change, err := local.Merge(&remote, in.Cas)
	if err != nil {
		return nil, err
	}

	mergedBytes, err := local.Marshal()
	if err != nil {
		return nil, err
	}

	out := &ringMergeOut{
		MergedB64: base64.StdEncoding.EncodeToString(mergedBytes),
	}

	if change != nil {
		if changeDesc, ok := change.(*ring.Desc); ok && changeDesc != nil {
			changeBytes, err := changeDesc.Marshal()
			if err != nil {
				return nil, err
			}
			out.ChangeB64 = ptrStr(base64.StdEncoding.EncodeToString(changeBytes))
		}
	}

	return out, nil
}

// --- partition-ring-lookup ---

type partitionRingLookupIn struct {
	DescB64                    string `json:"desc_b64"`
	MaxDerivedTokenPartitions  int32  `json:"max_derived_token_partitions"`
	Token                      uint32 `json:"token"`
	Op                         string `json:"op"`
}

type partitionRingLookupOut struct {
	PartitionID *int32   `json:"partition_id,omitempty"`
	Owners      []string `json:"owners"`
	Error       *string  `json:"error,omitempty"`
}

func cmdPartitionRingLookup(input []byte) (*partitionRingLookupOut, error) {
	var in partitionRingLookupIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	switch in.Op {
	case "", "Write", "Read":
	default:
		return nil, fmt.Errorf("unknown partition ring op %q", in.Op)
	}

	raw, err := base64.StdEncoding.DecodeString(in.DescB64)
	if err != nil {
		return nil, err
	}

	var desc ring.PartitionRingDesc
	if err := desc.Unmarshal(raw); err != nil {
		return nil, err
	}
	if desc.Partitions == nil {
		desc.Partitions = map[int32]ring.PartitionDesc{}
	}
	if desc.Owners == nil {
		desc.Owners = map[string]ring.OwnerDesc{}
	}

	opts := ring.DefaultPartitionRingOptions()
	if in.MaxDerivedTokenPartitions >= 0 {
		table, err := ring.NewPartitionTokenTable(in.MaxDerivedTokenPartitions, kitlog.NewNopLogger(), prometheus.NewRegistry())
		if err != nil {
			return &partitionRingLookupOut{
				Owners: []string{},
				Error:  ptrStr(err.Error()),
			}, nil
		}
		opts.TokenGenerator = table
	}

	pr, err := ring.NewPartitionRingWithOptions(desc, opts)
	if err != nil {
		return &partitionRingLookupOut{
			Owners: []string{},
			Error:  ptrStr(err.Error()),
		}, nil
	}

	pid, err := pr.ActivePartitionForKey(in.Token)
	if err != nil {
		return &partitionRingLookupOut{
			Owners: []string{},
			Error:  ptrStr(err.Error()),
		}, nil
	}

	owners := pr.PartitionOwnerIDsCopy(pid)
	if owners == nil {
		owners = []string{}
	}

	return &partitionRingLookupOut{
		PartitionID: &pid,
		Owners:      owners,
	}, nil
}

// --- kfake-broker ---

type kfakeRecordIn struct {
	Partition    int32            `json:"partition"`
	Tenant       string           `json:"tenant"`
	Version      int              `json:"version"`
	Compression  string           `json:"compression,omitempty"`
	WriteRequest WriteRequestJSON `json:"write_request"`
}

type kfakeBrokerIn struct {
	Topic       string          `json:"topic"`
	Partitions  int32           `json:"partitions"`
	Compression string          `json:"compression,omitempty"`
	Records     []kfakeRecordIn `json:"records"`
}

type kfakePartitionOffsetOut struct {
	Partition   int32 `json:"partition"`
	StartOffset int64 `json:"start_offset"`
	EndOffset   int64 `json:"end_offset"`
}

type kfakeBrokerReadyOut struct {
	BrokerAddr string                    `json:"broker_addr"`
	Topic      string                    `json:"topic"`
	Offsets    []kfakePartitionOffsetOut `json:"offsets"`
}

func parseKafkaCompression(name string) ([]kgo.CompressionCodec, error) {
	switch strings.ToLower(name) {
	case "":
		return nil, nil
	case "none":
		return []kgo.CompressionCodec{kgo.NoCompression()}, nil
	case "gzip":
		return []kgo.CompressionCodec{kgo.GzipCompression()}, nil
	case "snappy":
		return []kgo.CompressionCodec{kgo.SnappyCompression()}, nil
	case "lz4":
		return []kgo.CompressionCodec{kgo.Lz4Compression()}, nil
	case "zstd":
		return []kgo.CompressionCodec{kgo.ZstdCompression()}, nil
	default:
		return nil, fmt.Errorf("unknown compression codec %q", name)
	}
}

func produceBatchWithCompression(ctx context.Context, brokerAddr, compression string, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	codecs, err := parseKafkaCompression(compression)
	if err != nil {
		return err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DisableIdempotentWrite(),
	}
	if codecs != nil {
		opts = append(opts, kgo.ProducerBatchCompression(codecs...))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create kgo producer client: %w", err)
	}
	defer cl.Close()

	res := cl.ProduceSync(ctx, recs...)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("produce records: %w", err)
	}
	return nil
}

func cmdKfakeBroker() error {
	br := bufio.NewReader(os.Stdin)
	line, err := br.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("read config line from stdin: %w", err)
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return fmt.Errorf("empty config line on stdin")
	}

	var in kfakeBrokerIn
	if err := json.Unmarshal(line, &in); err != nil {
		return fmt.Errorf("decode config JSON: %w", err)
	}
	if in.Topic == "" {
		in.Topic = "ingest"
	}
	if in.Partitions <= 0 {
		in.Partitions = 1
	}

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(in.Partitions, in.Topic),
	)
	if err != nil {
		return fmt.Errorf("start kfake cluster: %w", err)
	}
	defer cluster.Close()

	addrs := cluster.ListenAddrs()
	if len(addrs) == 0 {
		return fmt.Errorf("kfake cluster returned no listen addresses")
	}
	brokerAddr := addrs[0]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Validate top-level compression early even if there are 0 records.
	if _, err := parseKafkaCompression(in.Compression); err != nil {
		return err
	}

	// Produce records preserving order while batching consecutive records with the same compression.
	var batch []*kgo.Record
	batchCompression := in.Compression
	for i, recIn := range in.Records {
		if err := ingest.ValidateRecordVersion(recIn.Version); err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
		wr, err := buildWriteRequest(recIn.WriteRequest)
		if err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
		recs, _, err := ingest.RecordSerializerFromVersion(recIn.Version).ToRecords(
			in.Topic,
			recIn.Partition,
			recIn.Tenant,
			wr,
			16*1024*1024,
		)
		if err != nil {
			return fmt.Errorf("record %d serialize: %w", i, err)
		}

		effCompression := in.Compression
		if recIn.Compression != "" {
			effCompression = recIn.Compression
		}
		if len(batch) > 0 && effCompression != batchCompression {
			if err := produceBatchWithCompression(ctx, brokerAddr, batchCompression, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		batchCompression = effCompression
		batch = append(batch, recs...)
	}
	if len(batch) > 0 {
		if err := produceBatchWithCompression(ctx, brokerAddr, batchCompression, batch); err != nil {
			return err
		}
	}

	admClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		return fmt.Errorf("create kadm client: %w", err)
	}
	defer admClient.Close()

	adm := kadm.NewClient(admClient)
	startOffsets, err := adm.ListStartOffsets(ctx, in.Topic)
	if err != nil {
		return fmt.Errorf("ListStartOffsets: %w", err)
	}
	endOffsets, err := adm.ListEndOffsets(ctx, in.Topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}

	offsets := make([]kfakePartitionOffsetOut, 0, in.Partitions)
	for p := int32(0); p < in.Partitions; p++ {
		sOff, ok := startOffsets.Lookup(in.Topic, p)
		if !ok {
			return fmt.Errorf("missing start offset for topic %q partition %d", in.Topic, p)
		}
		if sOff.Err != nil {
			return fmt.Errorf("start offset error for topic %q partition %d: %w", in.Topic, p, sOff.Err)
		}
		eOff, ok := endOffsets.Lookup(in.Topic, p)
		if !ok {
			return fmt.Errorf("missing end offset for topic %q partition %d", in.Topic, p)
		}
		if eOff.Err != nil {
			return fmt.Errorf("end offset error for topic %q partition %d: %w", in.Topic, p, eOff.Err)
		}
		offsets = append(offsets, kfakePartitionOffsetOut{
			Partition:   p,
			StartOffset: sOff.Offset,
			EndOffset:   eOff.Offset,
		})
	}

	ready := kfakeBrokerReadyOut{
		BrokerAddr: brokerAddr,
		Topic:      in.Topic,
		Offsets:    offsets,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(ready); err != nil {
		return fmt.Errorf("encode ready line: %w", err)
	}
	_ = os.Stdout.Sync()

	// Block reading stdin until EOF so the caller controls the broker lifetime.
	_, _ = io.Copy(io.Discard, br)
	return nil
}

// --- httpgrpc-call ---

type httpgrpcCallIn struct {
	GrpcAddr string              `json:"grpc_addr"`
	Method   string              `json:"method"`
	Url      string              `json:"url"`
	Headers  map[string][]string `json:"headers,omitempty"`
	BodyB64  string              `json:"body_b64,omitempty"`
	// OrgID is sent as x-scope-orgid gRPC metadata (required by multitenant servers).
	OrgID string `json:"org_id,omitempty"`
}

type httpgrpcCallOut struct {
	Code         int32               `json:"code"`
	Headers      map[string][]string `json:"headers"`
	BodyB64      string              `json:"body_b64"`
	RawGrpcError *string             `json:"raw_grpc_error,omitempty"`
}

func httpgrpcHeadersToMap(hs []*httpgrpc.Header) map[string][]string {
	out := make(map[string][]string, len(hs))
	for _, h := range hs {
		vals := make([]string, len(h.Values))
		copy(vals, h.Values)
		out[h.Key] = vals
	}
	return out
}

func cmdHttpGrpcCall(input []byte) (*httpgrpcCallOut, error) {
	var in httpgrpcCallIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	var body []byte
	if in.BodyB64 != "" {
		var err error
		body, err = base64.StdEncoding.DecodeString(in.BodyB64)
		if err != nil {
			return nil, fmt.Errorf("decode body_b64: %w", err)
		}
	}

	var hs []*httpgrpc.Header
	for k, vs := range in.Headers {
		hs = append(hs, &httpgrpc.Header{
			Key:    k,
			Values: vs,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if in.OrgID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-scope-orgid", in.OrgID)
	}

	conn, err := grpc.NewClient(in.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc NewClient %s: %w", in.GrpcAddr, err)
	}
	defer func() { _ = conn.Close() }()

	client := httpgrpc.NewHTTPClient(conn)
	resp, err := client.Handle(ctx, &httpgrpc.HTTPRequest{
		Method:  in.Method,
		Url:     in.Url,
		Headers: hs,
		Body:    body,
	})
	if err != nil {
		errStr := err.Error()
		if httpResp, ok := httpgrpc.HTTPResponseFromError(err); ok && httpResp != nil {
			return &httpgrpcCallOut{
				Code:         httpResp.Code,
				Headers:      httpgrpcHeadersToMap(httpResp.Headers),
				BodyB64:      base64.StdEncoding.EncodeToString(httpResp.Body),
				RawGrpcError: &errStr,
			}, nil
		}
		return &httpgrpcCallOut{
			Code:         0,
			Headers:      map[string][]string{},
			BodyB64:      "",
			RawGrpcError: &errStr,
		}, nil
	}

	return &httpgrpcCallOut{
		Code:    resp.Code,
		Headers: httpgrpcHeadersToMap(resp.Headers),
		BodyB64: base64.StdEncoding.EncodeToString(resp.Body),
	}, nil
}
