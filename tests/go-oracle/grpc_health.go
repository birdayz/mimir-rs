// SPDX-License-Identifier: AGPL-3.0-only
// Tier-1 Go conformance oracle for gRPC health checks (`dskit/ring/client/pool.go` +
// `pkg/ingester/client` + `grpc_health_v1`) with gzip/snappy/s2 compression, multitenancy,
// and cluster validation.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	_ "github.com/grafana/dskit/grpcencoding/snappy"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type grpcHealthProbeIn struct {
	GrpcAddr               string `json:"grpc_addr"`
	Compression            string `json:"compression"`
	ClusterValidationLabel string `json:"cluster_validation_label"`
}

type rpcOutcome struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Status  int32             `json:"status,omitempty"`
	List    map[string]int32  `json:"list,omitempty"`
	Extra   string            `json:"extra,omitempty"`
}

type grpcHealthProbeOut struct {
	PoolCountAfterChecks int        `json:"pool_count_after_checks"`
	PoolLogs             []string   `json:"pool_logs"`
	CheckEmptySvcWithOrg rpcOutcome `json:"check_empty_svc_with_org"`
	CheckNamedSvcWithOrg rpcOutcome `json:"check_named_svc_with_org"`
	CheckNoOrg           rpcOutcome `json:"check_no_org"`
	ListWithOrg          rpcOutcome `json:"list_with_org"`
	ListNoOrg            rpcOutcome `json:"list_no_org"`
	WatchWithOrg         rpcOutcome `json:"watch_with_org"`
	WatchNoOrg           rpcOutcome `json:"watch_no_org"`
	LabelNamesWithOrg    rpcOutcome `json:"label_names_with_org"`
	HttpGrpcReadyWithOrg rpcOutcome `json:"httpgrpc_ready_with_org"`
}

type syncLinesLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *syncLinesLogger) Log(keyvals ...any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var buf bytes.Buffer
	for i := 0; i < len(keyvals); i += 2 {
		if i > 0 {
			buf.WriteByte(' ')
		}
		k := fmt.Sprint(keyvals[i])
		var v any = ""
		if i+1 < len(keyvals) {
			v = keyvals[i+1]
		}
		fmt.Fprintf(&buf, "%s=%q", k, fmt.Sprint(v))
	}
	l.lines = append(l.lines, buf.String())
	return nil
}

func (l *syncLinesLogger) Snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out
}

func outcomeFromErr(err error) rpcOutcome {
	if err == nil {
		return rpcOutcome{Code: "OK"}
	}
	st, ok := status.FromError(err)
	if !ok {
		return rpcOutcome{Code: "NonStatusError", Message: err.Error()}
	}
	return rpcOutcome{Code: st.Code().String(), Message: st.Message()}
}

func cmdGrpcHealthProbe(input []byte) (*grpcHealthProbeOut, error) {
	var in grpcHealthProbeIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}

	var clientCfg ingester_client.Config
	clientCfg.GRPCClientConfig.MaxRecvMsgSize = 100 << 20
	clientCfg.GRPCClientConfig.MaxSendMsgSize = 100 << 20
	clientCfg.GRPCClientConfig.KeepaliveTime = 20 * time.Second
	clientCfg.GRPCClientConfig.KeepaliveTimeout = 10 * time.Second
	clientCfg.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	clientCfg.GRPCClientConfig.GRPCCompression = in.Compression
	clientCfg.GRPCClientConfig.ClusterValidation.Label = in.ClusterValidationLabel
	if err := clientCfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate client config: %w", err)
	}

	reg := prometheus.NewRegistry()
	metrics := ingester_client.NewMetrics(reg)
	poolLogger := &syncLinesLogger{}

	// Build a real inmemory ring containing the target ingester so distributor.NewPool
	// exercises both NewRingServiceDiscovery and cleanUnhealthy -> healthCheck.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), kitlog.NewNopLogger(), nil)
	defer func() { _ = closer.Close() }()

	inst := ring.InstanceDesc{
		Addr:      in.GrpcAddr,
		Timestamp: time.Now().Unix(),
		State:     ring.ACTIVE,
		Tokens:    []uint32{1},
	}
	if err := kvStore.CAS(ctx, "ring", func(_ any) (any, bool, error) {
		d := ring.NewDesc()
		d.Ingesters["ingester-0"] = inst
		return d, true, nil
	}); err != nil {
		return nil, err
	}

	r, err := ring.NewWithStoreClientAndStrategy(
		ring.Config{
			HeartbeatTimeout:  time.Minute,
			ReplicationFactor: 1,
		},
		"ingester",
		"ring",
		kvStore,
		ring.NewDefaultReplicationStrategy(),
		prometheus.NewRegistry(),
		kitlog.NewNopLogger(),
	)
	if err != nil {
		return nil, err
	}
	if err := services.StartAndAwaitRunning(ctx, r); err != nil {
		return nil, err
	}
	defer func() { _ = services.StopAndAwaitTerminated(context.Background(), r) }()

	factory := ring_client.PoolInstFunc(func(d ring.InstanceDesc) (ring_client.PoolClient, error) {
		return ingester_client.MakeIngesterClient(d, clientCfg, metrics, poolLogger)
	})
	poolCfg := distributor.PoolConfig{
		ClientCleanupPeriod:    40 * time.Millisecond,
		HealthCheckIngesters:   true,
		RemoteTimeout:          2 * time.Second,
		HealthCheckGracePeriod: 0,
	}
	pool := distributor.NewPool(poolCfg, r, factory, poolLogger)
	if _, err := pool.GetClientForInstance(inst); err != nil {
		return nil, fmt.Errorf("pool GetClientForInstance: %w", err)
	}
	if err := services.StartAndAwaitRunning(ctx, pool); err != nil {
		return nil, err
	}
	// Let multiple CheckInterval ticks run cleanUnhealthy -> healthCheck.
	time.Sleep(250 * time.Millisecond)
	poolCount := pool.Count()
	_ = services.StopAndAwaitTerminated(context.Background(), pool)

	client, err := ingester_client.MakeIngesterClient(inst, clientCfg, metrics, kitlog.NewNopLogger())
	if err != nil {
		return nil, fmt.Errorf("MakeIngesterClient: %w", err)
	}
	defer func() { _ = client.Close() }()

	// Raw grpc_health_v1 client (without dskit ClientUserHeaderInterceptor) so we actually send
	// Check/List/Watch without x-scope-orgid over the wire to verify server noGRPCAuthOn.
	rawOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if in.Compression != "" {
		rawOpts = append(rawOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(in.Compression)))
	}
	rawConn, err := grpc.Dial(in.GrpcAddr, rawOpts...) //nolint:staticcheck
	if err != nil {
		return nil, fmt.Errorf("raw grpc.Dial: %w", err)
	}
	defer func() { _ = rawConn.Close() }()
	rawHealth := grpc_health_v1.NewHealthClient(rawConn)

	callCheck := func(useOrg bool, service string) rpcOutcome {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		var (
			resp *grpc_health_v1.HealthCheckResponse
			err  error
		)
		if useOrg {
			cctx = user.InjectOrgID(cctx, "0")
			resp, err = client.Check(cctx, &grpc_health_v1.HealthCheckRequest{Service: service})
		} else {
			resp, err = rawHealth.Check(cctx, &grpc_health_v1.HealthCheckRequest{Service: service})
		}
		out := outcomeFromErr(err)
		if err == nil && resp != nil {
			out.Status = int32(resp.Status)
		}
		return out
	}

	callList := func(useOrg bool) rpcOutcome {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		var (
			resp *grpc_health_v1.HealthListResponse
			err  error
		)
		if useOrg {
			cctx = user.InjectOrgID(cctx, "0")
			resp, err = client.List(cctx, &grpc_health_v1.HealthListRequest{})
		} else {
			resp, err = rawHealth.List(cctx, &grpc_health_v1.HealthListRequest{})
		}
		out := outcomeFromErr(err)
		if err == nil && resp != nil {
			m := make(map[string]int32, len(resp.Statuses))
			for k, v := range resp.Statuses {
				m[k] = int32(v.Status)
			}
			out.List = m
		}
		return out
	}

	callWatch := func(useOrg bool) rpcOutcome {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		var (
			stream grpc_health_v1.Health_WatchClient
			err    error
		)
		if useOrg {
			cctx = user.InjectOrgID(cctx, "0")
			stream, err = client.Watch(cctx, &grpc_health_v1.HealthCheckRequest{})
		} else {
			stream, err = rawHealth.Watch(cctx, &grpc_health_v1.HealthCheckRequest{})
		}
		if err != nil {
			return outcomeFromErr(err)
		}
		resp, err := stream.Recv()
		out := outcomeFromErr(err)
		if err == nil && resp != nil {
			out.Status = int32(resp.Status)
		}
		return out
	}

	callLabelNames := func() rpcOutcome {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		cctx = user.InjectOrgID(cctx, "tenant-probe")
		resp, err := client.LabelNames(cctx, &ingester_client.LabelNamesRequest{})
		out := outcomeFromErr(err)
		if err == nil && resp != nil {
			names := slices.Clone(resp.LabelNames)
			slices.Sort(names)
			out.Extra = strings.Join(names, ",")
		}
		return out
	}

	callHttpGrpcReady := func() rpcOutcome {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		cctx = user.InjectOrgID(cctx, "0")
		cfgCopy := clientCfg.GRPCClientConfig
		opts, err := cfgCopy.DialOption(
			[]grpc.UnaryClientInterceptor{middleware.ClientUserHeaderInterceptor},
			nil,
			nil,
		)
		if err != nil {
			return rpcOutcome{Code: "DialOptError", Message: err.Error()}
		}
		conn, err := grpc.Dial(in.GrpcAddr, opts...) //nolint:staticcheck
		if err != nil {
			return rpcOutcome{Code: "DialError", Message: err.Error()}
		}
		defer func() { _ = conn.Close() }()
		hc := httpgrpc.NewHTTPClient(conn)
		// Repetitive query string (>1 KiB) ensures s2/snappy/gzip emit compressed blocks (chunk 0x00).
		resp, err := hc.Handle(cctx, &httpgrpc.HTTPRequest{Method: "GET", Url: "/ready?pad=" + strings.Repeat("abcd", 256)})
		if err != nil {
			if httpResp, ok := httpgrpc.HTTPResponseFromError(err); ok && httpResp != nil {
				return rpcOutcome{Code: fmt.Sprintf("HTTP_%d", httpResp.Code), Message: strings.TrimSpace(string(httpResp.Body))}
			}
			return outcomeFromErr(err)
		}
		return rpcOutcome{Code: fmt.Sprintf("HTTP_%d", resp.Code), Message: strings.TrimSpace(string(resp.Body))}
	}

	return &grpcHealthProbeOut{
		PoolCountAfterChecks: poolCount,
		PoolLogs:             poolLogger.Snapshot(),
		CheckEmptySvcWithOrg: callCheck(true, ""),
		CheckNamedSvcWithOrg: callCheck(true, "cortex.Ingester"),
		CheckNoOrg:           callCheck(false, ""),
		ListWithOrg:          callList(true),
		ListNoOrg:            callList(false),
		WatchWithOrg:         callWatch(true),
		WatchNoOrg:           callWatch(false),
		LabelNamesWithOrg:    callLabelNames(),
		HttpGrpcReadyWithOrg: callHttpGrpcReady(),
	}, nil
}
