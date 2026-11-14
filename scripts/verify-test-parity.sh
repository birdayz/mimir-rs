#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REF="$ROOT/.ref/mimir"
FILTER="${1:-}"

if [[ ! -d "$REF" ]]; then
  echo "ERROR: $REF not found" >&2
  exit 1
fi

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
