#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

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

CLEANED_UP=0
SOAK_PID=""
cleanup_stale_containers() {
    if [[ "$CLEANED_UP" -eq 1 ]]; then
        return
    fi
    CLEANED_UP=1
    if [[ -n "${SOAK_PID:-}" ]] && kill -0 "$SOAK_PID" 2>/dev/null; then
        echo "[run-soak-24h] forwarding SIGTERM to soak_vs_go (pid=$SOAK_PID)..." >&2
        kill -TERM "$SOAK_PID" 2>/dev/null || true
        for _ in $(seq 1 30); do
            kill -0 "$SOAK_PID" 2>/dev/null || break
            sleep 0.5
        done
        kill -KILL "$SOAK_PID" 2>/dev/null || true
    fi
    local ids
    ids="$(docker ps -aq --filter "label=org.mimir-rs.conformance=soak-vs-go" || true)"
    if [[ -n "$ids" ]]; then
        echo "[run-soak-24h] cleaning up soak containers..." >&2
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null 2>&1 || true
    fi
}
trap cleanup_stale_containers EXIT INT TERM HUP

echo "[run-soak-24h] building target/go-mimir (if missing)..."
./scripts/build-go-oracle.sh

echo "[run-soak-24h] building release mimir + soak_vs_go (CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS)..."
cargo build --release -p mimir-cmd
cargo build --release -p mimir-conformance --example soak_vs_go

echo "[run-soak-24h] launching soak_vs_go -> $OUT_DIR"
exec > >(tee "$OUT_DIR/soak.log") 2>&1
"$ROOT_DIR/target/release/examples/soak_vs_go" --skip-build "${ARGS[@]}" &
SOAK_PID=$!
wait "$SOAK_PID"
