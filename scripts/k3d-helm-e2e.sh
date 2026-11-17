#!/usr/bin/env bash
# k3d + upstream mimir-distributed Helm chart end-to-end test for mimir-rs ingesters.
#
# Deploys the chart from .ref/mimir/operations/helm/charts/mimir-distributed the way
# users do (helm install + values file) into a throwaway k3d cluster, swaps the
# ingester image according to MODE, and checks the deployment from the outside:
# readiness, ingester ring, partition ring, memberlist, write+read path, rolling
# restart of an ingester zone, and metric coverage. See docs/k3d-helm-e2e.md.
#
# Env:
#   MODE=go|mixed|rust         go = chart defaults (baseline), mixed = ingester zone-a+b Rust
#                              and zone-c Go, rust = all ingester zones Rust. Default: go.
#   KV_MODE=memberlist|etcd    memberlist = chart default gossip ring, etcd = shared etcd KV
#                              with num_tokens=0, push_grpc_method_enabled=false, memberlist
#                              disabled (also honored via RING_STORE=etcd). Default: memberlist.
#   RUST_IMAGE=img:tag   Rust image to use. Default: build mimir-rs:k3d-<shortsha>
#                        from this worktree with `just docker-build` (if missing).
#   KEEP_CLUSTER=1       keep the k3d cluster on exit (default: delete it).
#   REUSE_CLUSTER=1      reuse an existing cluster of the same name instead of
#                        recreating it (helm upgrade --install is idempotent).
#   CLUSTER=name         k3d cluster name (default: mimir-e2e-$MODE).
#   ROLLOUT_ZONE=zone-a  ingester zone restarted by check f.
#   READY_TIMEOUT=900    seconds to wait for all pods to become Ready.
#   OUT_DIR=dir          evidence directory (default: target/k3d-e2e/runs/<mode>-<ts>).
#   CARGO_BUILD_JOBS=6   cargo parallelism for the image build.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HERE=$ROOT/scripts/k3d-helm-e2e
MODE=${MODE:-go}
KV_MODE=${KV_MODE:-${RING_STORE:-memberlist}}
CLUSTER=${CLUSTER:-mimir-e2e-$MODE}
NS=mimir
RELEASE=mimir
TENANT=e2e
ROLLOUT_ZONE=${ROLLOUT_ZONE:-zone-a}
READY_TIMEOUT=${READY_TIMEOUT:-900}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}
REUSE_CLUSTER=${REUSE_CLUSTER:-0}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-6}

K3D=${K3D:-$(command -v k3d || echo "$HOME/.local/bin/k3d")}
HELM=${HELM:-$(command -v helm || echo "$HOME/.local/bin/helm")}
KUBECTL=${KUBECTL:-$(command -v kubectl || echo "$HOME/google-cloud-sdk/bin/kubectl")}
K3S_IMAGE=${K3S_IMAGE:-rancher/k3s:v1.35.5-k3s1}

CHART_SRC=$ROOT/.ref/mimir/operations/helm/charts/mimir-distributed
MIXIN=$ROOT/.ref/mimir/operations/mimir-mixin-compiled
WORK=$ROOT/target/k3d-e2e
CHART=$WORK/chart
OUT_DIR=${OUT_DIR:-$WORK/runs/$MODE-$(date +%Y%m%d-%H%M%S)}
export KUBECONFIG=$WORK/kubeconfig-$CLUSTER
export HELM_CONFIG_HOME=$WORK/helm/config HELM_CACHE_HOME=$WORK/helm/cache HELM_DATA_HOME=$WORK/helm/data

case $MODE in go|mixed|rust) ;; *) echo "MODE must be go|mixed|rust, got '$MODE'" >&2; exit 2 ;; esac
case $KV_MODE in memberlist|etcd) ;; *) echo "KV_MODE must be memberlist|etcd, got '$KV_MODE'" >&2; exit 2 ;; esac
for t in "$K3D" "$HELM" "$KUBECTL" docker jq awk; do
  command -v "$t" >/dev/null || { echo "missing tool: $t" >&2; exit 2; }
done
[[ -d $CHART_SRC ]] || { echo "chart not found at $CHART_SRC (is .ref/mimir present?)" >&2; exit 2; }

mkdir -p "$OUT_DIR"
SUMMARY=$OUT_DIR/summary.txt
: >"$SUMMARY"
FAILS=0
BG_PIDS=()

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
pass() { printf 'PASS %s\n' "$*" | tee -a "$SUMMARY"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); }
info() { printf 'INFO %s\n' "$*" | tee -a "$SUMMARY"; }
# Evidence lines are indented under the PASS/FAIL line they belong to.
evidence() { sed 's/^/     /' | tee -a "$SUMMARY"; }

kc() { "$KUBECTL" -n "$NS" "$@"; }
# curl from inside the cluster (pod IPs are not reachable from the host).
# (exec into a Running toolbox pod: `exec deploy/...` may pick an evicted one).
incurl() {
  local p
  p=$(kc get pods -l app=e2e-toolbox --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}') || return 1
  [[ -n $p ]] || { echo "no Running e2e-toolbox pod" >&2; return 1; }
  kc exec "$p" -c curl -- curl -sS -m 20 "$@"
}

collect_debug() {
  [[ -f $KUBECONFIG ]] || return 0
  log "collecting debug output into $OUT_DIR"
  kc get pods -o wide >"$OUT_DIR/pods.txt" 2>&1 || true
  kc get events --sort-by=.lastTimestamp >"$OUT_DIR/events.txt" 2>&1 || true
  local p
  for p in $(kc get pods -l app.kubernetes.io/component=ingester -o name 2>/dev/null); do
    kc logs "$p" --all-containers >"$OUT_DIR/logs-${p#pod/}.txt" 2>&1 || true
    kc logs "$p" --all-containers --previous >"$OUT_DIR/logs-${p#pod/}.previous.txt" 2>/dev/null || true
  done
}

cleanup() {
  local rc=$?
  for pid in "${BG_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  collect_debug
  if [[ $KEEP_CLUSTER == 1 ]]; then
    log "KEEP_CLUSTER=1: keeping cluster $CLUSTER (KUBECONFIG=$KUBECONFIG)"
  else
    log "deleting k3d cluster $CLUSTER"
    "$K3D" cluster delete "$CLUSTER" >/dev/null 2>&1 || { sleep 5; "$K3D" cluster delete "$CLUSTER" >/dev/null 2>&1; } || true
    "$K3D" cluster get "$CLUSTER" >/dev/null 2>&1 && log "WARNING: cluster $CLUSTER still exists after delete"
    rm -f "$KUBECONFIG"
  fi
  exit "$rc"
}
trap cleanup EXIT

###############################################################################
# Images
###############################################################################
GO_IMAGE=docker.io/grafana/mimir:$(awk '/^appVersion:/{print $2}' "$CHART_SRC/Chart.yaml")
if [[ $MODE != go ]]; then
  if [[ -z ${RUST_IMAGE:-} ]]; then
    RUST_IMAGE=mimir-rs:k3d-$(git -C "$ROOT" rev-parse --short HEAD)
    if ! docker image inspect "$RUST_IMAGE" >/dev/null 2>&1; then
      log "building $RUST_IMAGE (CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS)"
      (cd "$ROOT" && just docker-build "$RUST_IMAGE")
    fi
  fi
  docker image inspect "$RUST_IMAGE" >/dev/null 2>&1 || docker pull "$RUST_IMAGE"
fi
info "MODE=$MODE KV_MODE=$KV_MODE cluster=$CLUSTER go_image=$GO_IMAGE rust_image=${RUST_IMAGE:-n/a} out=$OUT_DIR"

###############################################################################
# Chart (copied out of .ref so `helm dependency build` never writes into it)
###############################################################################
prepare_chart() {
  mkdir -p "$CHART"
  rsync -aL --delete --exclude charts/ --exclude Chart.lock.new "$CHART_SRC/" "$CHART/"
  if ! ls "$CHART"/charts/*.tgz >/dev/null 2>&1 || [[ $CHART_SRC/Chart.lock -nt $CHART/charts ]]; then
    "$HELM" repo add minio https://charts.min.io/ --force-update >/dev/null
    "$HELM" repo add grafana https://grafana.github.io/helm-charts --force-update >/dev/null
    (cd "$CHART" && "$HELM" dependency build >/dev/null)
  fi
}
prepare_chart

###############################################################################
# Cluster
###############################################################################
if [[ $REUSE_CLUSTER == 1 ]] && "$K3D" cluster get "$CLUSTER" >/dev/null 2>&1; then
  log "reusing cluster $CLUSTER"
else
  "$K3D" cluster delete "$CLUSTER" >/dev/null 2>&1 || true
  log "creating k3d cluster $CLUSTER (1 server, 3 agents in zone-a/b/c)"
  "$K3D" cluster create "$CLUSTER" \
    --image "$K3S_IMAGE" --servers 1 --agents 3 --no-lb \
    --kubeconfig-update-default=false --kubeconfig-switch-context=false \
    --k3s-arg '--disable=traefik@server:0' \
    --k3s-arg '--disable=metrics-server@server:0' \
    --k3s-node-label 'topology.kubernetes.io/zone=zone-a@agent:0' \
    --k3s-node-label 'topology.kubernetes.io/zone=zone-b@agent:1' \
    --k3s-node-label 'topology.kubernetes.io/zone=zone-c@agent:2' \
    --k3s-arg '--kubelet-arg=eviction-hard=imagefs.available<1%,nodefs.available<1%@all' \
    --k3s-arg '--kubelet-arg=eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%@all' \
    --k3s-arg '--kubelet-arg=image-gc-high-threshold=100@all' \
    --k3s-arg '--kubelet-arg=image-gc-low-threshold=99@all' \
    --wait --timeout 300s >"$OUT_DIR/k3d-create.log" 2>&1
fi
"$K3D" kubeconfig get "$CLUSTER" >"$KUBECONFIG"
chmod 600 "$KUBECONFIG"

IMAGES=(
  "$GO_IMAGE"
  docker.io/apache/kafka-native:4.1.0
  docker.io/grafana/rollout-operator:v0.41.1
  docker.io/pgsty/silo:RELEASE.2026-09-03T13-18-01Z
  docker.io/prom/prometheus:v3.5.0
  docker.io/curlimages/curl:8.15.0
)
if [[ $MODE != go ]]; then IMAGES+=("$RUST_IMAGE"); fi
if [[ $KV_MODE == etcd ]]; then IMAGES+=(quay.io/coreos/etcd:v3.5.17); fi
for img in "${IMAGES[@]}"; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img" >/dev/null
done
# `k3d image import <ref>` silently fails with docker's containerd image store
# (multi-platform indexes with missing blobs), so save single-platform tarballs
# and verify every node actually has every image afterwards.
PLATFORM=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
mkdir -p "$WORK/images"
TARS=()
for img in "${IMAGES[@]}"; do
  id=$(docker image inspect "$img" --format '{{.Id}}')
  tar=$WORK/images/$(tr '/:' '__' <<<"$img")-${id#sha256:}.tar
  tar=${tar:0:250}
  [[ -s $tar ]] || docker save --platform "$PLATFORM" -o "$tar" "$img"
  TARS+=("$tar")
done
log "importing ${#IMAGES[@]} images into $CLUSTER"
"$K3D" image import -c "$CLUSTER" "${TARS[@]}" >"$OUT_DIR/k3d-import.log" 2>&1
normref() { # docker reference -> containerd name
  local r=$1
  [[ $r == */* ]] || r=library/$r
  [[ ${r%%/*} == *.* ]] || r=docker.io/$r
  echo "$r"
}
for node in $("$K3D" node list -o json | jq -r --arg c "$CLUSTER" '.[] | select(.runtimeLabels["k3d.cluster"] == $c and (.role == "server" or .role == "agent")) | .name'); do
  have=$(docker exec "$node" ctr -n k8s.io images ls -q)
  for img in "${IMAGES[@]}"; do
    grep -qxF "$(normref "$img")" <<<"$have" || { echo "image $img missing on node $node after import" >&2; cat "$OUT_DIR/k3d-import.log" >&2; exit 1; }
  done
done

###############################################################################
# Deploy
###############################################################################
HELM_ARGS=(upgrade --install "$RELEASE" "$CHART" -n "$NS" --create-namespace
  -f "$HERE/values.yaml" --timeout 20m)
if [[ $KV_MODE == etcd ]]; then
  cat >"$WORK/values-etcd.yaml" <<EOF
mimir:
  structuredConfig:
    ingester:
      push_grpc_method_enabled: false
      ring:
        num_tokens: 0
        tokens_file_path: ""
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
      partition_ring:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
    distributor:
      ring:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
      ha_tracker:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
    compactor:
      sharding_ring:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
    store_gateway:
      sharding_ring:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
    querier:
      ring:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
    query_scheduler:
      ring:
        kvstore:
          store: etcd
          etcd:
            endpoints: ["$RELEASE-etcd.$NS.svc.cluster.local:2379"]
EOF
  HELM_ARGS+=(-f "$WORK/values-etcd.yaml")
fi
if [[ $MODE != go ]]; then
  HELM_ARGS+=(--set "ingester.image.repository=${RUST_IMAGE%:*}" --set "ingester.image.tag=${RUST_IMAGE##*:}")
fi
if [[ $MODE == mixed ]]; then
  export E2E_RUST_IMAGE=$RUST_IMAGE E2E_GO_IMAGE=$GO_IMAGE E2E_GO_STATEFULSETS="$RELEASE-ingester-zone-c"
  HELM_ARGS+=(--post-renderer "$HERE/post-render-mixed.sh")
fi
if [[ $KV_MODE == etcd ]]; then
  "$KUBECTL" create namespace "$NS" --dry-run=client -o yaml | "$KUBECTL" apply -f - >/dev/null
  kc apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $RELEASE-etcd
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $RELEASE-etcd
  template:
    metadata:
      labels:
        app: $RELEASE-etcd
    spec:
      containers:
      - name: etcd
        image: quay.io/coreos/etcd:v3.5.17
        command: ["etcd"]
        args:
        - "--name=etcd0"
        - "--data-dir=/tmp/etcd"
        - "--listen-client-urls=http://0.0.0.0:2379"
        - "--advertise-client-urls=http://$RELEASE-etcd.$NS.svc.cluster.local:2379"
        ports:
        - containerPort: 2379
---
apiVersion: v1
kind: Service
metadata:
  name: $RELEASE-etcd
spec:
  selector:
    app: $RELEASE-etcd
  ports:
  - port: 2379
    targetPort: 2379
EOF
  kc rollout status "deployment/$RELEASE-etcd" --timeout=120s >/dev/null
fi
log "helm ${HELM_ARGS[*]}"
"$HELM" "${HELM_ARGS[@]}" >"$OUT_DIR/helm.log" 2>&1 || { cat "$OUT_DIR/helm.log" >&2; exit 1; }
kc apply -f "$HERE/workload.yaml" >/dev/null

###############################################################################
# a. All pods Ready
###############################################################################
not_ready_pods() {
  kc get pods -o json | jq -r '.items[]
    | select(.status.phase != "Succeeded")
    | select(([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 0)
    | .metadata.name'
}
log "a. waiting up to ${READY_TIMEOUT}s for all pods to become Ready"
deadline=$((SECONDS + READY_TIMEOUT))
while :; do
  pending=$(not_ready_pods || echo "kubectl-error")
  [[ -z $pending ]] && break
  if ((SECONDS > deadline)); then break; fi
  sleep 10
done
if [[ -z $pending ]]; then
  pass "a. all $(kc get pods --no-headers | wc -l) pods Ready/Completed"
else
  fail "a. pods not Ready after ${READY_TIMEOUT}s: $(echo $pending)"
  kc get pods -o wide | evidence
fi

# Ingester inventory: name ip zone impl(go|rust) image
ingesters_tsv() {
  kc get pods -l app.kubernetes.io/component=ingester -o json | jq -r --arg go "$GO_IMAGE" '.items[]
    | [.metadata.name, (.status.podIP // "-"), .metadata.labels.zone,
       (if (.spec.containers[] | select(.name == "ingester") | .image) == $go then "go" else "rust" end),
       (.spec.containers[] | select(.name == "ingester") | .image)] | @tsv' | sort
}
INGESTERS=$(ingesters_tsv)
echo "$INGESTERS" >"$OUT_DIR/ingesters.tsv"
# Expected implementation per zone for this MODE.
expect_impl() {
  case $MODE in
    go) echo go ;;
    rust) echo rust ;;
    mixed) [[ $1 == zone-c ]] && echo go || echo rust ;;
  esac
}
want_inv=$(for z in zone-a zone-b zone-c; do echo "$RELEASE-ingester-$z-0 $z $(expect_impl $z)"; done)
have_inv=$(awk -F'\t' '{print $1, $3, $4}' <<<"$INGESTERS")
if [[ $want_inv == "$have_inv" ]]; then
  pass "a. ingester inventory matches MODE=$MODE (name zone impl ip image):"
else
  fail "a. ingester inventory does not match MODE=$MODE; want: $(echo $want_inv)"
fi
echo "$INGESTERS" | evidence

kc get pods -l app.kubernetes.io/component=ingester -o json | jq -r '.items[]
  | (.spec.containers[] | select(.name == "ingester") | .image) as $img
  | (.status.containerStatuses[]? | select(.name == "ingester") | .state.running.startedAt // empty) as $start
  | ([.status.conditions[]? | select(.type == "Ready" and .status == "True") | .lastTransitionTime][0] // empty) as $ready
  | [.metadata.name, $img,
     (if $start != "" and $ready != "" then (($ready | fromdateiso8601) - ($start | fromdateiso8601) | tostring) + "s" else "not-ready" end)]
  | @tsv' | sort >"$OUT_DIR/time-to-ready.tsv"
info "a. time from container start to Ready per ingester (readinessProbe initialDelaySeconds=60 is a lower bound):"
evidence <"$OUT_DIR/time-to-ready.tsv"

pod_ip() { kc get pod "$1" -o jsonpath='{.status.podIP}' 2>/dev/null || true; }
first_pod() { kc get pods -l "app.kubernetes.io/component=$1" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' || true; }
DISTRIBUTOR=$(first_pod distributor)
QUERIER=$(first_pod querier)

###############################################################################
# b. Ingester ring as seen by distributor and querier
###############################################################################
# check_ring <label> -> 0 if every ingester pod is ACTIVE at podIP:9095 in its zone,
# in both views, and the ring has no other members.
check_ring() {
  local label=$1 viewer ip ring bad=0 out
  for viewer in "$DISTRIBUTOR" "$QUERIER"; do
    ip=$(pod_ip "$viewer")
    ring=$(incurl -H 'Accept: application/json' "http://$ip:8080/ingester/ring" 2>&1) || true
    echo "$ring" >"$OUT_DIR/ring-$label-$viewer.json"
    if ! jq -e '.shards' >/dev/null 2>&1 <<<"$ring"; then
      echo "$viewer: /ingester/ring did not return JSON: $(head -c 300 <<<"$ring")"
      bad=1
      continue
    fi
    out=$(jq -r --arg ing "$INGESTERS" --arg kv_mode "$KV_MODE" '
      ($ing | split("\n") | map(select(length > 0) | split("\t") | {name: .[0], ip: .[1], zone: .[2], impl: .[3]})) as $want
      | (.shards | map({key: .id, value: .}) | from_entries) as $have
      | ($want[] | . as $w | $have[$w.name] as $s
          | if $s == null then "MISSING \($w.name) (\($w.impl)) not in ring"
            else
              ( (if $s.state != "ACTIVE" then ["state=\($s.state)"] else [] end)
              + (if $s.address != "\($w.ip):9095" then ["address=\($s.address) want \($w.ip):9095"] else [] end)
              + (if $s.zone != $w.zone then ["zone=\($s.zone) want \($w.zone)"] else [] end)
              + (if $kv_mode == "etcd" then
                   (if ($s.tokens | length) != 0 then ["expected 0 tokens in tokenless mode, got \($s.tokens | length)"] else [] end)
                 else
                   (if ($s.tokens | length) == 0 then ["no tokens"] else [] end)
                 end)
              ) as $errs
              | if ($errs | length) > 0 then "BAD \($w.name) (\($w.impl)): \($errs | join(", "))"
                else "OK \($w.name) (\($w.impl)) \($s.state) \($s.address) \($s.zone) tokens=\($s.tokens | length)" end
            end),
        (.shards[] | select(.id as $id | ($want | map(.name) | index($id)) == null)
          | "EXTRA \(.id) \(.state) \(.address) \(.zone)")' <<<"$ring")
    echo "$out" | sed "s|^|$viewer: |"
    if grep -qvE '^OK ' <<<"$out"; then bad=1; fi
  done
  return $bad
}
ring_with_retry() {
  local label=$1 i res
  for i in $(seq 1 12); do
    if res=$(check_ring "$label"); then echo "$res"; return 0; fi
    sleep 10
  done
  echo "$res"
  return 1
}
log "b. checking ingester ring"
if res=$(ring_with_retry initial); then
  pass "b. ingester ring (distributor+querier view): all ingesters ACTIVE at podIP:9095 in their zone"
else
  fail "b. ingester ring (distributor+querier view) mismatch"
fi
echo "$res" | evidence

###############################################################################
# c. Partition ring
###############################################################################
check_partition_ring() {
  local label=$1 ip pr out bad=0
  ip=$(pod_ip "$DISTRIBUTOR")
  pr=$(incurl -H 'Accept: application/json' "http://$ip:8080/ingester/partition-ring" 2>&1) || true
  echo "$pr" >"$OUT_DIR/partition-ring-$label.json"
  if ! jq -e '.partitions' >/dev/null 2>&1 <<<"$pr"; then
    echo "/ingester/partition-ring did not return JSON: $(head -c 300 <<<"$pr")"
    return 1
  fi
  out=$(jq -r --arg ing "$INGESTERS" '
    ($ing | split("\n") | map(select(length > 0) | split("\t")[0])) as $want
    | def active: (. == 2 or . == "PartitionActive" or . == "ACTIVE" or . == "Active");
    (.partitions[] | if (.state | active) and (.owner_ids | length) > 0
        then "OK partition \(.id) state=\(.state) owners=\(.owner_ids | join(","))"
        else "BAD partition \(.id) state=\(.state) owners=\(.owner_ids // [] | join(","))" end),
    ([.partitions[].owner_ids[]?] | unique) as $owners
    | ($want[] | select(. as $w | $owners | index($w) | not) | "BAD ingester \(.) owns no partition"),
    (if (.partitions | length) == 0 then "BAD no partitions" else empty end)' <<<"$pr")
  echo "$out"
  grep -qvE '^OK ' <<<"$out" && bad=1
  return $bad
}
log "c. checking partition ring"
ok=0
for i in $(seq 1 12); do if res=$(check_partition_ring initial); then ok=1; break; fi; sleep 10; done
if ((ok)); then
  pass "c. partition ring: every partition ACTIVE with an owner, every ingester owns a partition"
else
  fail "c. partition ring"
fi
echo "$res" | evidence

###############################################################################
# d. Memberlist
###############################################################################
# Normalised member list of one /memberlist JSON answer: "name<TAB>ip<TAB>incarnation".
# dskit (Go) answers {"SortedMembers":[{"Name","Addr",...}]}; other shapes are
# parsed best-effort so membership can still be compared (schema is checked by
# memberlist_schema).
memberlist_members() {
  jq -r 'if .SortedMembers then .SortedMembers[] | [.Name, .Addr, "-"]
    elif .members then .members[] | select(.state == null or .state == "Alive" or .state == "Suspect")
      | [.name, (.addr | sub(":[0-9]+$"; "") | ltrimstr("[") | rtrimstr("]") | sub("^::ffff:"; "")), (.incarnation // "-" | tostring)]
    else empty end | @tsv'
}
memberlist_schema() { jq -e '(.SortedMembers | type) == "array" and (.Store | type) == "object"' >/dev/null 2>&1; }

check_memberlist() {
  local label=$1 pods p ip ml bad=0 expected="" view users=""
  # Every Mimir process (all expose http-metrics:8080) is asked for its view;
  # processes that do not use memberlist (query-scheduler) answer in plain text.
  pods=$(kc get pods -l app.kubernetes.io/name=mimir --field-selector=status.phase=Running -o json | jq -r '.items[]
    | select([.spec.containers[] | select(.image | test("mimir")) | .ports[]? | select(.name == "http-metrics")] | length > 0)
    | [.metadata.name, .status.podIP, (.metadata.labels["app.kubernetes.io/part-of"] // "")] | @tsv' | sort)
  while IFS=$'\t' read -r p ip partof; do
    [[ -z $p ]] && continue
    ml=$(incurl -H 'Accept: application/json' "http://$ip:8080/memberlist" 2>&1) || true
    echo "$ml" >"$OUT_DIR/memberlist-$label-$p.json"
    if [[ -n $(memberlist_members <<<"$ml" 2>/dev/null) ]]; then
      users+="$p"$'\t'"$ip"$'\n'
    elif [[ $partof == memberlist ]]; then
      echo "BAD $p (gossip-ring member): /memberlist returned no member list: $(head -c 200 <<<"$ml")"
      bad=1
      users+="$p"$'\t'"$ip"$'\n'
    fi
  done <<<"$pods"
  expected=$(echo -n "$users" | cut -f1 | sort | tr '\n' ' ')
  while IFS=$'\t' read -r p ip; do
    [[ -z $p ]] && continue
    ml=$(memberlist_members <"$OUT_DIR/memberlist-$label-$p.json" 2>/dev/null) || true
    [[ -n $ml ]] || continue
    # Map every member back to a pod: memberlist node names are "<hostname>-<random>".
    view=$(jq -Rr --arg pods "$users" '
      ($pods | split("\n") | map(select(length > 0) | split("\t") | {name: .[0], ip: .[1]})) as $want
      | [inputs, . | select(length > 0) | split("\t") | {Name: .[0], Addr: .[1]} | . as $m
          | ([$want[] | select(.name as $n | ($m.Name == $n) or ($m.Name | startswith($n + "-")))] | first) as $pod
          | if $pod == null then "UNKNOWN:\($m.Name)@\($m.Addr)"
            elif $pod.ip != $m.Addr then "BADADDR:\($pod.name)@\($m.Addr)!=\($pod.ip)"
            else $pod.name end] | sort | join(" ")' <<<"$ml")
    if [[ "$view " != "$expected" ]]; then
      echo "BAD $p view: $view"
      bad=1
    else
      echo "OK $p sees all $(wc -w <<<"$view") members at their pod IPs"
    fi
  done <<<"$users"
  if [[ -n $expected ]]; then echo "expected members: $expected"; fi
  return $bad
}
if [[ $KV_MODE == etcd ]]; then
  check_no_memberlist() {
    local label=$1 pods p ip ml tb_pod rc m bad=0
    pods=$(kc get pods -l app.kubernetes.io/name=mimir --field-selector=status.phase=Running -o json | jq -r '.items[]
      | select([.spec.containers[] | select(.image | test("mimir")) | .ports[]? | select(.name == "http-metrics")] | length > 0)
      | [.metadata.name, .status.podIP] | @tsv' | sort)
    tb_pod=$(kc get pods -l app=e2e-toolbox --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
    while IFS=$'\t' read -r p ip; do
      [[ -z $p ]] && continue
      ml=$(incurl "http://$ip:8080/memberlist" 2>&1) || true
      echo "$ml" >"$OUT_DIR/memberlist-$label-$p.txt"
      if [[ $ml != "This instance doesn't use memberlist." ]]; then
        echo "BAD $p /memberlist returned: $(head -c 200 <<<"$ml")"
        bad=1
        continue
      fi
      rc=0
      kc exec "$tb_pod" -c curl -- curl -sS --connect-timeout 2 "http://$ip:7946/" >/dev/null 2>&1 || rc=$?
      if ((rc != 7)); then
        echo "BAD $p port 7946 probe returned curl exit=$rc (want 7 connection refused)"
        bad=1
        continue
      fi
      m=$(incurl "http://$ip:8080/metrics" 2>/dev/null | grep -E '^(cortex_)?memberlist_' || true)
      if [[ -n $m ]]; then
        echo "BAD $p exposes memberlist metrics: $(head -n 3 <<<"$m")"
        bad=1
        continue
      fi
      echo "OK $p /memberlist disabled, port 7946 closed, zero memberlist metrics"
    done <<<"$pods"
    return $bad
  }
  log "d. checking memberlist is disabled on every mimir pod (KV_MODE=etcd)"
  if res=$(check_no_memberlist initial); then
    pass "d1. memberlist disabled on every mimir pod"
    pass "d2. /memberlist disabled response verified (KV_MODE=etcd)"
  else
    fail "d1. some pod unexpectedly uses memberlist under KV_MODE=etcd"
  fi
  echo "$res" | evidence
else
  log "d. checking memberlist membership on every memberlist pod"
  ok=0
  for i in $(seq 1 12); do if res=$(check_memberlist initial); then ok=1; break; fi; sleep 10; done
  if ((ok)); then
    pass "d1. memberlist: every member pod sees the same full membership at the right pod IPs"
  else
    fail "d1. memberlist views differ / incomplete"
  fi
  echo "$res" | evidence
  # d2: GET /memberlist with Accept: application/json must have dskit's schema
  # (memberlist.StatusPageData: SortedMembers, Store, ...) on every pod.
  schema_bad=""
  for f in "$OUT_DIR"/memberlist-initial-*.json; do
    memberlist_members <"$f" >/dev/null 2>&1 && [[ -n $(memberlist_members <"$f" 2>/dev/null) ]] || continue
    memberlist_schema <"$f" || schema_bad+="$(basename "$f" .json | sed 's/^memberlist-initial-//') keys=$(jq -c 'keys' "$f" 2>/dev/null) "$'\n'
  done
  if [[ -z $schema_bad ]]; then
    pass "d2. /memberlist JSON has dskit's StatusPageData schema on every pod"
  else
    fail "d2. /memberlist JSON is not dskit's StatusPageData schema (want keys SortedMembers, Store, ...) on:"
    echo -n "$schema_bad" | evidence
  fi
  # d3 (report-only): incarnation numbers, where the JSON exposes them. Go nodes
  # only bump their incarnation to refute suspicion, so it stays 1 in a healthy cluster.
  for f in "$OUT_DIR"/memberlist-initial-*.json; do
    inc=$({ memberlist_members <"$f" 2>/dev/null || true; } | awk -F'\t' '$3 != "-" {printf "%s=%s ", $1, $3}')
    if [[ -n $inc ]]; then info "d3. incarnations seen by $(basename "$f" .json | sed 's/^memberlist-initial-//'): $inc"; fi
  done
fi

###############################################################################
# e. Write + read path
###############################################################################
QF_URL=http://$RELEASE-query-frontend.$NS.svc:8080/prometheus/api/v1
DIST_URL=http://$RELEASE-distributor.$NS.svc:8080
# promq <consistency:eventual|strong> <endpoint> <args...> : prints the JSON response.
promq() {
  local cons=$1 ep=$2; shift 2
  local hdr=(-H "X-Scope-OrgID: $TENANT")
  if [[ $cons == strong ]]; then hdr+=(-H "X-Read-Consistency: strong"); fi
  incurl "${hdr[@]}" -G "$QF_URL/$ep" "$@"
}
ingester_metric() { # <pod> <metric> -> sum of all series of that metric (0 if absent)
  local ip
  ip=$(pod_ip "$1")
  { incurl "http://$ip:8080/metrics" 2>/dev/null || true; } | awk -v m="$2" '$1 == m || index($1, m "{") == 1 {s += $NF} END {printf "%.0f\n", s + 0}'
}

log "e. waiting for remote-written samples of every scrape target to become queryable"
n_targets=$(kc get pods -o json | jq '[.items[] | select(.status.phase == "Running")
  | select([.spec.containers[].ports[]? | select(.name == "http-metrics")] | length > 0)] | length')
got=0
for i in $(seq 1 60); do
  r=$(promq eventual query --data-urlencode "query=count(up{namespace=\"$NS\"} == 1)" 2>/dev/null || true)
  got=$(jq -r '.data.result[0].value[1] // 0' <<<"$r" 2>/dev/null || echo 0)
  [[ $got == "$n_targets" ]] && break
  sleep 5
done
if [[ $got == "$n_targets" ]]; then
  pass "e1. query-frontend instant query: count(up==1) = $got = number of http-metrics targets"
else
  fail "e1. count(up==1) = $got, expected $n_targets scrape targets"
  head -c 1000 <<<"$r" | evidence
fi

# Range query over the last 3 minutes: every series must have >= 3 points.
end=$(date +%s); start=$((end - 180))
r=$(promq eventual query_range --data-urlencode "query=up{namespace=\"$NS\"}" \
  --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode step=15 || true)
echo "$r" >"$OUT_DIR/query-range.json"
nseries=$(jq '.data.result | length' <<<"$r" 2>/dev/null || echo 0)
minpts=$(jq '[.data.result[].values | length] | min // 0' <<<"$r" 2>/dev/null || echo 0)
if [[ $(jq -r .status <<<"$r" 2>/dev/null) == success && $nseries -ge $n_targets && $minpts -ge 3 ]]; then
  pass "e2. query_range up{} last 3m: $nseries series, min $minpts points/series"
else
  fail "e2. query_range up{} last 3m: status=$(jq -r .status <<<"$r" 2>/dev/null) series=$nseries minpts=$minpts"
  head -c 1000 <<<"$r" | evidence
fi

# Consistency: same instant queries at a fixed timestamp, eventual vs strong,
# repeated; every answer must be identical (queriers pick a random zone per query).
before=$(for p in $(echo "$INGESTERS" | cut -f1); do echo "$p $(ingester_metric "$p" cortex_ingester_queries_total)"; done)
ts=$(($(date +%s) - 60))
answers=$OUT_DIR/consistency.txt
: >"$answers"
qerr=0
for i in $(seq 1 15); do
  for cons in eventual strong; do
    for q in "sum(up{namespace=\"$NS\"})" "count({namespace=\"$NS\",__name__=~\"cortex_.+\"})" \
      "sum by (component) (count_over_time(up{namespace=\"$NS\"}[2m]))"; do
      r=$(promq "$cons" query --data-urlencode "query=$q" --data-urlencode "time=$ts" 2>&1 || true)
      if [[ $(jq -r .status <<<"$r" 2>/dev/null) != success ]]; then
        qerr=$((qerr + 1)); echo "ERROR $cons $q: $(head -c 300 <<<"$r")" >>"$answers"; continue
      fi
      echo "$q => $(jq -c '[.data.result[] | [.metric, .value[1]]] | sort' <<<"$r")" >>"$answers"
      echo "$cons $q => $(jq -c '[.data.result[] | [.metric, .value[1]]] | sort' <<<"$r")" >>"$answers.full"
    done
  done
done
distinct=$({ grep -v '^ERROR' "$answers" || true; } | sort -u | wc -l)
empty=$(grep -c '=> \[\]$' "$answers" || true)
if ((qerr == 0 && distinct == 3 && empty == 0)); then
  pass "e3. 90 instant queries (eventual+strong) at fixed time all succeeded, non-empty and identical per query"
else
  fail "e3. consistency: errors=$qerr distinct_answers=$distinct (want 3) empty=$empty"
  sort "$answers" | uniq -c >"$answers.counts"; head -c 3000 "$answers.counts" | evidence
fi

# Read-your-writes with strong consistency: push via OTLP JSON, read immediately.
ryw_fail=0
for i in $(seq 1 5); do
  val=$((RANDOM * 1000 + i)); run=r$RANDOM$i; now_ns=$(date +%s%N)
  body='{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"e2e"}}]},"scopeMetrics":[{"metrics":[{"name":"e2e_ryw_probe","gauge":{"dataPoints":[{"asDouble":'$val',"timeUnixNano":"'$now_ns'","attributes":[{"key":"run","value":{"stringValue":"'$run'"}}]}]}}]}]}]}'
  code=$(incurl -o /dev/null -w '%{http_code}' -H "X-Scope-OrgID: $TENANT" -H 'Content-Type: application/json' \
    --data-binary "$body" "$DIST_URL/otlp/v1/metrics" || true)
  r=$(promq strong query --data-urlencode "query=e2e_ryw_probe{run=\"$run\"}" || true)
  got=$(jq -r '.data.result[0].value[1] // "none"' <<<"$r" 2>/dev/null || echo none)
  if [[ $code != 200 || $got != "$val" ]]; then
    ryw_fail=$((ryw_fail + 1)); echo "push=$code run=$run want=$val got=$got resp=$(head -c 300 <<<"$r")"
  fi
done >"$OUT_DIR/read-your-writes.txt"
if ((ryw_fail == 0)); then
  pass "e4. read-your-writes: 5/5 OTLP pushes immediately visible with X-Read-Consistency: strong"
else
  fail "e4. read-your-writes: $ryw_fail/5 pushes not visible with strong consistency"
  evidence <"$OUT_DIR/read-your-writes.txt"
fi

# Did every ingester (in particular every Rust one) serve queries?
served_bad=0
served=$(while read -r p b; do
  impl=$(awk -v p="$p" '$1 == p {print $4}' <<<"$INGESTERS")
  a=$(ingester_metric "$p" cortex_ingester_queries_total)
  echo "$p ($impl) cortex_ingester_queries_total $b -> $a"
done <<<"$before")
while read -r p impl _ b _ a; do
  if ((a <= b)); then
    if [[ $impl == "(rust)" || $MODE == go ]]; then served_bad=1; fi
  fi
done <<<"$served"
if ((served_bad == 0)); then
  pass "e5. ingesters served queries (cortex_ingester_queries_total increased)"
else
  fail "e5. an ingester did not serve any of the queries"
fi
echo "$served" | evidence

###############################################################################
# f. Rolling restart of one ingester zone (rollout-operator managed)
###############################################################################
RPOD=$RELEASE-ingester-$ROLLOUT_ZONE-0
RSTS=$RELEASE-ingester-$ROLLOUT_ZONE
RIMPL=$(awk -v p="$RPOD" '$1 == p {print $4}' <<<"$INGESTERS")
log "f. rolling restart of $RSTS ($RIMPL)"
ring_json=$(incurl -H 'Accept: application/json' "http://$(pod_ip "$DISTRIBUTOR"):8080/ingester/ring" || true)
tokens_before=$(jq -c --arg id "$RPOD" '[.shards[] | select(.id == $id) | .tokens[]] | sort' <<<"$ring_json" || echo "[]")
parts_before=$(incurl -H 'Accept: application/json' "http://$(pod_ip "$DISTRIBUTOR"):8080/ingester/partition-ring" |
  jq -c --arg id "$RPOD" '[.partitions[] | select(.owner_ids | index($id)) | .id] | sort' || echo "[]")
uid_before=$(kc get pod "$RPOD" -o jsonpath='{.metadata.uid}' || true)
restart_ts=$(date +%s)

qlog=$OUT_DIR/rollout-queries.txt
: >"$qlog"
(
  while :; do
    for cons in eventual strong; do
      r=$(promq "$cons" query --data-urlencode "query=count(up{namespace=\"$NS\"})" 2>&1 || true)
      if [[ $(jq -r .status <<<"$r" 2>/dev/null) == success && $(jq '.data.result | length' <<<"$r") -gt 0 ]]; then
        echo "$(date +%T) ok $cons $(jq -r '.data.result[0].value[1]' <<<"$r")"
      else
        echo "$(date +%T) ERROR $cons $(head -c 300 <<<"$r" | tr '\n' ' ')"
      fi
    done
    sleep 1
  done >>"$qlog"
) &
BG_PIDS+=($!)
QPID=$!

kc rollout restart "statefulset/$RSTS" >/dev/null
deadline=$((SECONDS + 600))
restarted=0
while ((SECONDS < deadline)); do
  uid=$(kc get pod "$RPOD" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  ready=$(kc get pod "$RPOD" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  if [[ -n $uid && $uid != "$uid_before" && $ready == True ]]; then restarted=1; break; fi
  sleep 5
done
restart_secs=$(($(date +%s) - restart_ts))
sleep 20
kill "$QPID" 2>/dev/null || true
wait "$QPID" 2>/dev/null || true

if ((restarted)); then
  pass "f1. $RPOD ($RIMPL) recreated by rollout and Ready again after ${restart_secs}s"
else
  fail "f1. $RPOD ($RIMPL) not recreated+Ready within 600s (uid_before=$uid_before)"
  kc get pod "$RPOD" -o wide 2>&1 | evidence
  kc logs "$RPOD" --tail 50 2>&1 | evidence
fi
nq=$(wc -l <"$qlog"); nerr=$(grep -c ERROR "$qlog" || true)
if ((nerr == 0 && nq > 10)); then
  pass "f2. $nq queries during/after the rollout, 0 errors"
else
  fail "f2. $nerr/$nq queries failed during/after the rollout"
  awk '/ERROR/ && n++ < 20' "$qlog" | evidence
fi

INGESTERS=$(ingesters_tsv)
ring_json=$(incurl -H 'Accept: application/json' "http://$(pod_ip "$DISTRIBUTOR"):8080/ingester/ring" || true)
echo "$ring_json" >"$OUT_DIR/ring-after-rollout.json"
tokens_after=$(jq -c --arg id "$RPOD" '[.shards[] | select(.id == $id) | .tokens[]] | sort' <<<"$ring_json" || echo "[]")
parts_after=$(incurl -H 'Accept: application/json' "http://$(pod_ip "$DISTRIBUTOR"):8080/ingester/partition-ring" |
  jq -c --arg id "$RPOD" '[.partitions[] | select(.owner_ids | index($id)) | .id] | sort' || echo "[]")
if [[ $KV_MODE == etcd ]]; then
  if [[ $tokens_before == "[]" && $tokens_after == "[]" && $parts_before == "$parts_after" && $parts_before != "[]" ]]; then
    pass "f3. $RPOD kept 0 ring tokens (tokenless) and partitions $parts_after"
  else
    fail "f3. $RPOD ring identity changed: tokens $(jq length <<<"$tokens_before") -> $(jq length <<<"$tokens_after"), partitions $parts_before -> $parts_after"
  fi
else
  if [[ $tokens_before == "$tokens_after" && $tokens_before != "[]" && $parts_before == "$parts_after" && $parts_before != "[]" ]]; then
    pass "f3. $RPOD kept its $(jq length <<<"$tokens_before") ring tokens and partitions $parts_after"
  else
    fail "f3. $RPOD ring identity changed: tokens $(jq length <<<"$tokens_before") -> $(jq length <<<"$tokens_after") equal=$([[ $tokens_before == "$tokens_after" ]] && echo yes || echo no), partitions $parts_before -> $parts_after"
  fi
fi
if res=$(ring_with_retry after-rollout); then
  pass "f4. ingester ring after rollout: all ingesters ACTIVE at their (new) podIP:9095"
else
  fail "f4. ingester ring after rollout mismatch"
fi
echo "$res" | evidence

# Data still queryable: a window that starts before the restart must still have
# every target, and the restarted ingester must hold as many in-memory series as
# its peers (it replayed WAL / Kafka).
win=$(($(date +%s) - restart_ts + 120))
r=$(promq strong query --data-urlencode "query=count(count_over_time(up{namespace=\"$NS\"}[${win}s]) > 0)" || true)
cnt=$(jq -r '.data.result[0].value[1] // 0' <<<"$r" 2>/dev/null || echo 0)
mem=$(for p in $(echo "$INGESTERS" | cut -f1); do echo "$p $(ingester_metric "$p" cortex_ingester_memory_series)"; done)
rmem=$(awk -v p="$RPOD" '$1 == p {print $2}' <<<"$mem")
peermax=$(awk -v p="$RPOD" '$1 != p && $2 > m {m = $2} END {print m + 0}' <<<"$mem")
if ((cnt >= n_targets && rmem > 0 && rmem * 10 >= peermax * 8)); then
  pass "f5. data queryable after rollout: $cnt series over ${win}s window; memory_series $RPOD=$rmem peers max=$peermax"
else
  fail "f5. after rollout: series over ${win}s window=$cnt (want >= $n_targets); memory_series $RPOD=$rmem peers max=$peermax"
fi
echo "$mem" | evidence
if res=$(check_partition_ring after-rollout); then pass "f6. partition ring after rollout OK"; else fail "f6. partition ring after rollout"; fi
echo "$res" | evidence
if [[ $KV_MODE == etcd ]]; then
  if res=$(check_no_memberlist after-rollout); then pass "f7. memberlist disabled after rollout OK"; else fail "f7. memberlist enabled after rollout"; fi
else
  ok=0
  for i in $(seq 1 12); do if res=$(check_memberlist after-rollout); then ok=1; break; fi; sleep 10; done
  if ((ok)); then pass "f7. memberlist membership after rollout OK"; else fail "f7. memberlist after rollout"; fi
fi
echo "$res" | evidence

###############################################################################
# g. Metric coverage vs mixin dashboards (report-only)
###############################################################################
sample_names() { awk '!/^#/ && NF {n = $1; sub(/\{.*/, "", n); print n} /^# TYPE /{print $3}' | sort -u; }
mixin=$OUT_DIR/mixin-metrics.txt
grep -ohE '\b[a-z][a-z0-9_]*_[a-z0-9_]+' "$MIXIN"/dashboards/*.json "$MIXIN"/alerts.yaml "$MIXIN"/rules.yaml | sort -u >"$mixin"
GO_REF=$WORK/go-ingester-metrics.txt
while IFS=$'\t' read -r p ip zone impl img; do
  { incurl "http://$ip:8080/metrics" || true; } | sample_names >"$OUT_DIR/metrics-$p.txt"
  if [[ $impl == go ]]; then cp "$OUT_DIR/metrics-$p.txt" "$GO_REF.tmp"; fi
done <<<"$INGESTERS"
if [[ -f $GO_REF.tmp ]]; then mv "$GO_REF.tmp" "$GO_REF"; fi
if [[ $MODE == go ]]; then
  info "g. Go ingester metric names saved to $GO_REF ($(wc -l <"$GO_REF") names) as reference for mixed/rust"
elif [[ -f $GO_REF ]]; then
  while IFS=$'\t' read -r p ip zone impl img; do
    [[ $impl == rust ]] || continue
    comm -12 "$mixin" "$GO_REF" | comm -23 - "$OUT_DIR/metrics-$p.txt" >"$OUT_DIR/missing-mixin-metrics-$p.txt"
    info "g. $p (rust): $(wc -l <"$OUT_DIR/missing-mixin-metrics-$p.txt") mixin (dashboards+alerts+rules) metric names exposed by Go ingesters but not by this Rust ingester (report-only):"
    { tr '\n' ' ' <"$OUT_DIR/missing-mixin-metrics-$p.txt"; echo; } | fold -s -w 150 | evidence
    comm -23 "$GO_REF" "$OUT_DIR/metrics-$p.txt" >"$OUT_DIR/missing-all-metrics-$p.txt"
    info "g. $p (rust): $(wc -l <"$OUT_DIR/missing-all-metrics-$p.txt") metric names in total exposed by Go but not Rust ingesters (list: $OUT_DIR/missing-all-metrics-$p.txt)"
  done <<<"$INGESTERS"
else
  info "g. no Go ingester reference metrics ($GO_REF missing); run MODE=go or MODE=mixed first"
fi

###############################################################################
echo
echo "==== $MODE: $(grep -c '^PASS' "$SUMMARY") PASS, $FAILS FAIL (evidence: $OUT_DIR) ===="
((FAILS == 0))
