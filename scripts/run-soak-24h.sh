#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-6}"

OUT_DIR=""
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir)
            OUT_DIR="${2:-}"
            ARGS+=("$1" "${2:-}")
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="$ROOT_DIR/target/soak-$(date -u +%Y%m%d-%H%M%S)"
    ARGS+=("--out-dir" "$OUT_DIR")
fi

mkdir -p "$OUT_DIR"

echo "[run-soak-24h] building target/go-mimir (if missing)..."
./scripts/build-go-oracle.sh

echo "[run-soak-24h] building release mimir + soak_vs_go (CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS)..."
cargo build --release -p mimir-cmd
cargo build --release -p mimir-conformance --example soak_vs_go

cleanup_stale_containers() {
    local ids
    ids="$(docker ps -aq --filter "label=org.mimir-rs.conformance=soak-vs-go" || true)"
    if [[ -n "$ids" ]]; then
        echo "[run-soak-24h] cleaning up soak containers..."
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null 2>&1 || true
    fi
}
trap cleanup_stale_containers EXIT

echo "[run-soak-24h] launching soak_vs_go -> $OUT_DIR"
set -o pipefail
"$ROOT_DIR/target/release/examples/soak_vs_go" --skip-build "${ARGS[@]}" 2>&1 | tee "$OUT_DIR/soak.log"
