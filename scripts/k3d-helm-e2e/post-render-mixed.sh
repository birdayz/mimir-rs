#!/usr/bin/env bash
# Helm post-renderer used by scripts/k3d-helm-e2e.sh for MODE=mixed.
#
# The mimir-distributed chart only has a per-component image override
# (ingester.image), not a per-zone one. The script installs with the Rust image
# for all ingesters and this post-renderer switches the StatefulSets listed in
# E2E_GO_STATEFULSETS (space separated) back to E2E_GO_IMAGE.
set -euo pipefail
: "${E2E_RUST_IMAGE:?}" "${E2E_GO_IMAGE:?}" "${E2E_GO_STATEFULSETS:?}"

awk -v rust="$E2E_RUST_IMAGE" -v go="$E2E_GO_IMAGE" -v names="$E2E_GO_STATEFULSETS" '
function flush(   i, hit, n, want) {
  hit = 0
  if (doc ~ /(^|\n)kind: StatefulSet(\n|$)/) {
    n = split(names, want, " ")
    for (i = 1; i <= n; i++)
      if (index(doc, "\n  name: " want[i] "\n") > 0) hit = 1
  }
  if (hit) {
    replaced = 0
    m = split(doc, lines, "\n")
    doc = ""
    for (i = 1; i <= m; i++) {
      l = lines[i]
      if (l ~ /^ +image: / && index(l, rust) > 0) { sub(/image: .*/, "image: " go, l); replaced++ }
      doc = doc (i > 1 ? "\n" : "") l
    }
    if (replaced == 0) { print "post-render-mixed: no Rust image line in Go StatefulSet" > "/dev/stderr"; exit 1 }
  }
  if (doc != "") printf "%s\n", doc
  doc = ""
}
/^---/ { flush(); print; next }
{ doc = (doc == "" ? $0 : doc "\n" $0) }
END { flush() }
'
