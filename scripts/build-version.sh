#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UPSTREAM_FILE="$ROOT/UPSTREAM_MIMIR"

if [[ ! -f "$UPSTREAM_FILE" ]]; then
  echo "ERROR: $UPSTREAM_FILE not found" >&2
  exit 1
fi

# shellcheck source=../UPSTREAM_MIMIR
source "$UPSTREAM_FILE"

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [sha]" >&2
  exit 2
fi

if [[ $# -eq 1 && -n "$1" ]]; then
  SHORT_SHA="${1:0:7}"
else
  SHORT_SHA="$(git -C "$ROOT" rev-parse --short=7 HEAD)"
fi

printf 'v%s-rs.%s\n' "$UPSTREAM_MIMIR_VERSION" "$SHORT_SHA"
