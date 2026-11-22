#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REF_DIR="$ROOT_DIR/.ref/mimir"
TARGET_DIR="$ROOT_DIR/target"
GO_MIMIR_BIN="$TARGET_DIR/go-mimir"
GO_ORACLE_BIN="$TARGET_DIR/go-oracle"
ORACLE_SRC_DIR="$ROOT_DIR/tests/go-oracle"
# Per-invocation directory: concurrent builds (worktrees sharing .ref) must not clobber each other.
ORACLE_CMD_DIR="$REF_DIR/cmd/mimir-oracle-$$"

REBUILD_MIMIR=false
for arg in "$@"; do
    case "$arg" in
        --rebuild-mimir)
            REBUILD_MIMIR=true
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

"$ROOT_DIR/scripts/fetch-upstream-mimir.sh" --check

mkdir -p "$TARGET_DIR"

if [[ ! -x "$GO_MIMIR_BIN" || "$REBUILD_MIMIR" == "true" ]]; then
    echo "Building $GO_MIMIR_BIN from .ref/mimir/cmd/mimir..."
    go -C "$REF_DIR" build -mod=vendor -o "$GO_MIMIR_BIN" ./cmd/mimir
fi

echo "Building $GO_ORACLE_BIN from tests/go-oracle..."
rm -rf "$ORACLE_CMD_DIR"
mkdir -p "$ORACLE_CMD_DIR"
cp "$ORACLE_SRC_DIR"/*.go "$ORACLE_CMD_DIR/"
trap 'rm -rf "$ORACLE_CMD_DIR"' EXIT

go -C "$REF_DIR" build -mod=vendor -o "$GO_ORACLE_BIN" "./cmd/$(basename "$ORACLE_CMD_DIR")"
echo "Done: $GO_MIMIR_BIN and $GO_ORACLE_BIN"
