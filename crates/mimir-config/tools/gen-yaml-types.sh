#!/usr/bin/env bash
# Regenerates crates/mimir-config/src/yaml_types.json from the upstream Go sources in .ref/mimir
# (go.yaml.in/yaml/v3 view of mimir.Config, see tools/yamltypes/main.go).
set -euo pipefail
CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$CRATE_DIR/../.." && pwd)"
REF_DIR="$ROOT_DIR/.ref/mimir"
TMP_CMD="$REF_DIR/cmd/mimir-rs-yamltypes-$$"
mkdir -p "$TMP_CMD"
trap 'rm -rf "$TMP_CMD"' EXIT
cp "$CRATE_DIR/tools/yamltypes/main.go" "$TMP_CMD/"
go -C "$REF_DIR" run -mod=vendor "./cmd/$(basename "$TMP_CMD")" > "$CRATE_DIR/src/yaml_types.json"
echo "wrote $CRATE_DIR/src/yaml_types.json"
