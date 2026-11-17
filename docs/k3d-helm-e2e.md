# k3d + Helm end-to-end test

`scripts/k3d-helm-e2e.sh` deploys the **upstream `mimir-distributed` Helm chart**
(`.ref/mimir/operations/helm/charts/mimir-distributed`) into a throwaway
[k3d](https://k3d.io) cluster, the way users run it (`helm upgrade --install` and a
values file, no template edits). It then swaps in the mimir-rs ingester image and
checks the deployment from the outside. Its purpose is to catch bugs that only show
up in a real Kubernetes deployment: interface/address detection, DNS-based
memberlist join, Kafka settings, rollout-operator interaction, and similar.

## Run

```sh
just k3d-e2e go        # baseline: chart defaults, all Go (proves the harness)
just k3d-e2e mixed     # ingester zone-a + zone-b Rust, zone-c Go
just k3d-e2e rust      # all ingester zones Rust

# Use an already built image and keep the cluster for debugging:
RUST_IMAGE=mimir-rs:k3d-hotfix KEEP_CLUSTER=1 MODE=mixed ./scripts/k3d-helm-e2e.sh
export KUBECONFIG=target/k3d-e2e/kubeconfig-mimir-e2e-mixed   # then kubectl -n mimir ...
k3d cluster delete mimir-e2e-mixed
```

Requirements: docker, `k3d` (v5.9), `helm` (v3), `kubectl`, `jq`, `rsync`, network
access the first time (chart dependencies, images). Without `RUST_IMAGE`, the script
builds `mimir-rs:k3d-<shortsha>` with `just docker-build` if that image does not
exist yet. `CARGO_BUILD_JOBS` (default 6) caps cargo parallelism inside the
Docker build.

Other options: `CLUSTER`, `REUSE_CLUSTER=1`, `ROLLOUT_ZONE` (default `zone-a`),
`READY_TIMEOUT` (default 900 s), `OUT_DIR`. The exit code is non-zero if any check
fails.

## What gets deployed

- k3d: 1 server node and 3 agent nodes labelled
  `topology.kubernetes.io/zone=zone-a|b|c`. traefik and metrics-server are
  disabled. Kubelet disk eviction and image GC are disabled so that a nearly full
  host disk cannot evict pods in the middle of a run.
- Chart values: `scripts/k3d-helm-e2e/values.yaml`. Everything not set there is the
  chart default: ingest storage with the built-in Kafka, minio, rollout-operator,
  and zone-aware ingesters (one per zone, each zone pinned to its node). The
  following are disabled: ruler, alertmanager, overrides-exporter, gateway, and
  store-gateway zone awareness. Resource requests are reduced.
- MODE=rust and MODE=mixed: `--set ingester.image.*` selects the Rust image. The
  chart has no per-zone image setting, so mixed mode uses the Helm post-renderer
  `scripts/k3d-helm-e2e/post-render-mixed.sh` to switch zone-c back to the Go image.
- `scripts/k3d-helm-e2e/workload.yaml` contains:
  - a Prometheus in agent mode that scrapes every `http-metrics` port in the
    namespace and remote-writes to the distributor as tenant `e2e`
  - a curl toolbox pod used to reach pod IPs
- Images are saved as single-platform tarballs (`target/k3d-e2e/images`) and
  imported into every node. The script then verifies that each node has them.
  `k3d image import <ref>` reports success even when it silently fails with
  Docker's containerd image store.

## Checks

Each check prints PASS, FAIL, or INFO with its evidence. Everything is also saved
under `target/k3d-e2e/runs/<mode>-<ts>/`: `summary.txt`, ring, partition-ring and
memberlist JSON, query logs, metric dumps, and ingester logs.

| id | check |
|----|-------|
| a  | All pods Ready (or Completed) within `READY_TIMEOUT`. The ingester inventory (zone → Go/Rust image) matches MODE. Reports time from container start to Ready per ingester; the chart's `initialDelaySeconds: 60` sets a lower bound on that time. |
| b  | `GET /ingester/ring` (`Accept: application/json`) on a distributor and a querier: every ingester is `ACTIVE`, its address is `<pod IP>:9095`, it is in the right zone, it has tokens, and there are no extra entries. Fails on 127.0.0.1 registration. |
| c  | `GET /ingester/partition-ring` on the distributor: every partition is ACTIVE with an owner, and every ingester owns a partition. |
| d1 | `GET /memberlist` on every Mimir pod: all of them see the same full membership, and every member's address is its pod IP. Covers Rust↔Go in both directions. |
| d2 | The `/memberlist` JSON uses dskit's `StatusPageData` schema (`SortedMembers`, `Store`, ...). |
| d3 | Report only: memberlist incarnation numbers, when the JSON exposes them. |
| e1–e2 | Remote-written data becomes queryable through the query-frontend (`/prometheus/api/v1/query` and `query_range`). The data is recent, so only ingesters can serve it. |
| e3 | 90 instant queries at a fixed timestamp, eventual and `X-Read-Consistency: strong`. All succeed, are non-empty, and give identical answers. |
| e4 | Read-your-writes: an OTLP JSON push followed immediately by a strong-consistency read returns the pushed value. Repeated 5 times. |
| e5 | Every Rust ingester (every ingester in MODE=go) served queries: `cortex_ingester_queries_total` increased. |
| f1–f7 | `kubectl rollout restart` of the `ROLLOUT_ZONE` ingester StatefulSet; the rollout-operator deletes the pod. Checks that the pod comes back Ready, that queries running throughout see no errors, and that the pod keeps the same ring tokens and partition. Then re-runs the ring, partition-ring, and memberlist checks, and checks that the data is still queryable (the restarted ingester's `cortex_ingester_memory_series` must be ≥ 80% of its peers'). |
| g  | Report only: the metric names that the mimir-mixin dashboards (`operations/mimir-mixin-compiled/dashboards`) reference, that Go ingesters expose, and that a Rust ingester does not expose. The Go reference is saved by MODE=go and MODE=mixed to `target/k3d-e2e/go-ingester-metrics.txt`. |
