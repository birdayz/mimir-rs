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

if [[ -z "${UPSTREAM_MIMIR_REPO:-}" || -z "${UPSTREAM_MIMIR_VERSION:-}" || -z "${UPSTREAM_MIMIR_REF:-}" ]]; then
  echo "ERROR: UPSTREAM_MIMIR must define UPSTREAM_MIMIR_REPO, UPSTREAM_MIMIR_VERSION, and UPSTREAM_MIMIR_REF" >&2
  exit 1
fi

REF_DIR="$ROOT/.ref/mimir"

check_no_duplicated_pins() {
  local hits
  hits="$(grep -rnE '(^|[[:space:]])M''IMIR_REF[[:space:]]*[:=]' "$ROOT/.github/workflows" "$ROOT/scripts" || true)"
  if [[ -n "$hits" ]]; then
    echo "ERROR: found duplicated upstream ref variable in workflows/scripts (use UPSTREAM_MIMIR instead):" >&2
    echo "$hits" >&2
    exit 1
  fi
  hits="$(grep -rnE '(^|[^0-9a-fA-F])[0-9a-f]{40}([^0-9a-fA-F]|$)' "$ROOT/.github/workflows" "$ROOT/scripts" || true)"
  if [[ -n "$hits" ]]; then
    echo "ERROR: found hardcoded 40-char commit SHA pin in workflows/scripts (use UPSTREAM_MIMIR instead):" >&2
    echo "$hits" >&2
    exit 1
  fi
}

case "${1:-}" in
  --print-ref)
    printf '%s\n' "$UPSTREAM_MIMIR_REF"
    exit 0
    ;;
  --print-version)
    printf '%s\n' "$UPSTREAM_MIMIR_VERSION"
    exit 0
    ;;
  --check)
    if [[ ! -d "$REF_DIR" ]]; then
      echo "ERROR: $REF_DIR not found; run ./scripts/fetch-upstream-mimir.sh" >&2
      exit 1
    fi
    HEAD_SHA="$(git -C "$REF_DIR" rev-parse HEAD 2>/dev/null || true)"
    if [[ "$HEAD_SHA" != "$UPSTREAM_MIMIR_REF" ]]; then
      echo "ERROR: $REF_DIR HEAD ($HEAD_SHA) != UPSTREAM_MIMIR_REF ($UPSTREAM_MIMIR_REF); run ./scripts/fetch-upstream-mimir.sh" >&2
      exit 1
    fi
    check_no_duplicated_pins
    exit 0
    ;;
  "")
    ;;
  *)
    echo "Usage: $0 [--check | --print-ref | --print-version]" >&2
    exit 2
    ;;
esac

if [[ -d "$REF_DIR" ]]; then
  HEAD_SHA="$(git -C "$REF_DIR" rev-parse HEAD 2>/dev/null || true)"
  if [[ "$HEAD_SHA" == "$UPSTREAM_MIMIR_REF" ]]; then
    exit 0
  fi
else
  mkdir -p "$REF_DIR"
  git init -q "$REF_DIR"
fi

if git -C "$REF_DIR" remote get-url origin >/dev/null 2>&1; then
  git -C "$REF_DIR" remote set-url origin "$UPSTREAM_MIMIR_REPO"
else
  git -C "$REF_DIR" remote add origin "$UPSTREAM_MIMIR_REPO"
fi

if git -C "$REF_DIR" cat-file -e "${UPSTREAM_MIMIR_REF}^{commit}" 2>/dev/null; then
  git -C "$REF_DIR" checkout -q "$UPSTREAM_MIMIR_REF"
else
  git -C "$REF_DIR" fetch -q --depth 1 origin "$UPSTREAM_MIMIR_REF"
  git -C "$REF_DIR" checkout -q FETCH_HEAD
fi
