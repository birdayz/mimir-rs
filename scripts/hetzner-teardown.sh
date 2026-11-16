#!/usr/bin/env bash
set -euo pipefail

# Safely tears down ONLY Hetzner Cloud resources labeled cluster=mimir-stress.
# Never touches pre-existing servers, networks, firewalls, or SSH keys.

if [[ -z "${HCLOUD_TOKEN:-}" ]]; then
  if [[ -f "${HOME}/hetzner_api_key" ]]; then
    HCLOUD_TOKEN="$(tr -d '[:space:]' < "${HOME}/hetzner_api_key")"
    export HCLOUD_TOKEN
  else
    echo "ERROR: HCLOUD_TOKEN not set and ~/hetzner_api_key not found" >&2
    exit 1
  fi
fi

LABEL="cluster=mimir-stress"

echo "Deleting servers with label ${LABEL}..."
mapfile -t SERVERS < <(hcloud server list -l "${LABEL}" -o noheader -o columns=id)
for id in "${SERVERS[@]}"; do
  [[ -n "${id}" ]] && hcloud server delete "${id}"
done

echo "Deleting firewalls with label ${LABEL}..."
mapfile -t FWS < <(hcloud firewall list -l "${LABEL}" -o noheader -o columns=id)
for id in "${FWS[@]}"; do
  [[ -n "${id}" ]] && hcloud firewall delete "${id}"
done

echo "Deleting networks with label ${LABEL}..."
mapfile -t NETS < <(hcloud network list -l "${LABEL}" -o noheader -o columns=id)
for id in "${NETS[@]}"; do
  [[ -n "${id}" ]] && hcloud network delete "${id}"
done

echo "Deleting SSH keys with label ${LABEL}..."
mapfile -t KEYS < <(hcloud ssh-key list -l "${LABEL}" -o noheader -o columns=id)
for id in "${KEYS[@]}"; do
  [[ -n "${id}" ]] && hcloud ssh-key delete "${id}"
done

echo "Teardown complete for label ${LABEL}."
