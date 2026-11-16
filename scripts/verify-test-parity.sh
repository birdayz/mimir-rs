#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REF="$ROOT/.ref/mimir"

if [[ ! -d "$REF" ]]; then
  echo "ERROR: $REF not found" >&2
  exit 1
fi

# Opt-in dependency scope: ./scripts/verify-test-parity.sh --deps [--list] [--strict] [FILTER]
# Reports upstream Test* functions of the dskit / memberlist / prometheus-tsdb packages that
# define ingester behaviour (vendor/ ships no *_test.go, so they are read from the Go module
# cache at the exact versions pinned by .ref/mimir/go.mod, honouring replace directives).
# A test counts as ported only via a marker
#   // Upstream: github.com/grafana/dskit/<pkg>/<file>_test.go::TestName
#   // Upstream: github.com/hashicorp/memberlist/<file>_test.go::TestName
#   // Upstream: github.com/prometheus/prometheus/tsdb/<pkg>/<file>_test.go::TestName
# "name-match" is a heuristic (a Rust fn named like the snake_cased Go test exists) shown for
# triage only. Exits 0 unless --strict is given and something is missing.
if [[ "${1:-}" == "--deps" ]]; then
  shift
  LIST=0
  STRICT=0
  DFILTER=""
  for a in "$@"; do
    case "$a" in
      --list) LIST=1 ;;
      --strict) STRICT=1 ;;
      *) DFILTER="$a" ;;
    esac
  done

  # module path (as in go.mod require) -> canonical marker prefix, packages (relative dirs)
  DSKIT_PKGS=(kv kv/codec kv/consul kv/etcd kv/memberlist kv/multi ring ring/shard ring/util
    dns dns/godns dns/miekgdns backoff flagext crypto/tls grpcclient grpcutil grpcencoding/snappy
    server middleware httpgrpc httpgrpc/server runtimeconfig limiter services modules user tenant
    netutil timeutil multierror concurrency instrument log)
  MEMBERLIST_PKGS=(.)
  PROM_PKGS=(tsdb tsdb/wlog tsdb/chunks tsdb/chunkenc tsdb/record tsdb/index tsdb/tombstones
    tsdb/fileutil tsdb/encoding model/labels model/histogram)

  modcache_dir() {
    (cd "$REF" && GOFLAGS=-mod=mod go mod download -json "$1" 2>/dev/null) \
      | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",*$/\1/p' | head -n 1
  }

  DMARKERS="$(mktemp)"
  RUSTFNS="$(mktemp)"
  trap 'rm -f "$DMARKERS" "$RUSTFNS"' EXIT
  rg --no-filename -o '// Upstream: [A-Za-z0-9_./-]+::Test[A-Za-z0-9_]+' "$ROOT/crates" \
    | sed 's|^// Upstream: ||' | sort -u > "$DMARKERS" || true
  rg --no-filename -o 'fn [a-z0-9_]+' "$ROOT/crates" | sed 's/^fn //' | sort -u > "$RUSTFNS" || true

  snake() {
    sed -e 's/^Test//' -e 's/\([a-z0-9]\)\([A-Z]\)/\1_\2/g' -e 's/\([A-Z]\+\)\([A-Z][a-z]\)/\1_\2/g' \
      <<<"$1" | tr 'A-Z' 'a-z' | sed -e 's/__*/_/g' -e 's/^_//' -e 's/_$//'
  }

  G_TOTAL=0
  G_PORTED=0
  G_NAME=0
  MISSING_ALL=()
  report_module() {
    local mod="$1" canon="$2"
    shift 2
    local dir
    dir="$(modcache_dir "$mod")"
    if [[ -z "$dir" || ! -d "$dir" ]]; then
      echo "ERROR: cannot locate $mod in the Go module cache (run: cd .ref/mimir && go mod download $mod)" >&2
      exit 2
    fi
    echo "== $canon ($(basename "$dir"))"
    local pkg
    for pkg in "$@"; do
      local key_pkg="$canon/$pkg"
      [[ "$pkg" == "." ]] && key_pkg="$canon"
      if [[ -n "$DFILTER" && "$key_pkg" != *"$DFILTER"* ]]; then
        continue
      fi
      [[ -d "$dir/$pkg" ]] || continue
      local total=0 ported=0 named=0 f fn key
      local missing=()
      for f in "$dir/$pkg"/*_test.go; do
        [[ -f "$f" ]] || continue
        while IFS= read -r fn; do
          [[ -z "$fn" || "$fn" == "TestMain" ]] && continue
          key="$key_pkg/$(basename "$f")::$fn"
          total=$((total + 1))
          if grep -Fxq "$key" "$DMARKERS"; then
            ported=$((ported + 1))
          else
            local s
            s="$(snake "$fn")"
            if grep -Fxq -e "$s" -e "test_$s" "$RUSTFNS"; then
              named=$((named + 1))
              missing+=("$key (name-match)")
            else
              missing+=("$key")
            fi
          fi
        done < <(sed -n 's/^func \(Test[A-Za-z0-9_]*\)(.*/\1/p' "$f")
      done
      [[ "$total" -eq 0 ]] && continue
      printf "  %-48s %4d / %-4d ported  (%d missing, %d name-match)\n" \
        "$key_pkg" "$ported" "$total" "$((total - ported))" "$named"
      G_TOTAL=$((G_TOTAL + total))
      G_PORTED=$((G_PORTED + ported))
      G_NAME=$((G_NAME + named))
      if [[ "$LIST" -eq 1 && "${#missing[@]}" -gt 0 ]]; then
        printf "      MISSING: %s\n" "${missing[@]}"
      fi
      MISSING_ALL+=("${missing[@]}")
    done
  }

  report_module github.com/grafana/dskit github.com/grafana/dskit "${DSKIT_PKGS[@]}"
  report_module github.com/hashicorp/memberlist github.com/hashicorp/memberlist "${MEMBERLIST_PKGS[@]}"
  report_module github.com/prometheus/prometheus github.com/prometheus/prometheus "${PROM_PKGS[@]}"

  echo "Upstream dependency test parity (${DFILTER:-all dependency scope}): $G_PORTED / $G_TOTAL ported ($((G_TOTAL - G_PORTED)) missing, $G_NAME heuristic name-match)"
  if [[ "$STRICT" -eq 1 && "$G_PORTED" -lt "$G_TOTAL" ]]; then
    exit 1
  fi
  exit 0
fi

FILTER="${1:-}"

mapfile -t GO_TEST_FILES < <(
  cd "$REF"
  {
    find cmd/mimir pkg/ingester pkg/storage/ingest pkg/mimirpb \
      pkg/util/activitytracker pkg/util/globalerror pkg/util/shutdownmarker \
      -name "*_test.go"
    printf "%s\n" \
      pkg/storage/tsdb/config_test.go \
      pkg/storage/tsdb/tenant_deletion_mark_test.go \
      pkg/storage/tsdb/tsdb_metrics_test.go \
      pkg/storage/tsdb/users_scanner_test.go \
      pkg/storage/tsdb/util_test.go \
      pkg/storage/tsdb/block/meta_test.go \
      pkg/storage/tsdb/block/block_test.go
  } | sort -u
)

# Collect all "// Upstream: <path>::<TestName>" markers present in Rust crates
MARKER_TMP="$(mktemp)"
trap 'rm -f "$MARKER_TMP"' EXIT
rg --no-filename -o '// Upstream: [A-Za-z0-9_./-]+::Test[A-Za-z0-9_]+' "$ROOT/crates" \
  | sed 's|^// Upstream: ||' | sort -u > "$MARKER_TMP" || true

# Forbid #[ignore] anywhere in crates/
IGNORED_COUNT="$(rg -n '#\[ignore' "$ROOT/crates" | wc -l || true)"
if [[ "$IGNORED_COUNT" -gt 0 ]]; then
  echo "ERROR: Found #[ignore] attributes in crates/ ($IGNORED_COUNT occurrences):" >&2
  rg -n '#\[ignore' "$ROOT/crates" >&2
  exit 1
fi

TOTAL=0
PORTED=0
MISSING=0
MISSING_LIST=()

for rel in "${GO_TEST_FILES[@]}"; do
  if [[ -n "$FILTER" && "$rel" != *"$FILTER"* ]]; then
    continue
  fi
  full="$REF/$rel"
  [[ -f "$full" ]] || continue
  while IFS= read -r fn; do
    [[ -z "$fn" ]] && continue
    [[ "$fn" == "TestMain" ]] && continue
    key="${rel}::${fn}"
    TOTAL=$((TOTAL + 1))
    if grep -Fxq "$key" "$MARKER_TMP"; then
      PORTED=$((PORTED + 1))
    else
      MISSING=$((MISSING + 1))
      MISSING_LIST+=("$key")
    fi
  done < <(sed -n 's/^func \(Test[A-Za-z0-9_]*\)(.*/\1/p' "$full")
done

echo "Upstream test parity (${FILTER:-all ingester scope}): $PORTED / $TOTAL ported ($MISSING missing)"
if [[ "$MISSING" -gt 0 ]]; then
  printf "  MISSING: %s\n" "${MISSING_LIST[@]}" | head -n 60
  if [[ "$MISSING" -gt 60 ]]; then
    echo "  ... and $((MISSING - 60)) more"
  fi
  exit 1
fi
