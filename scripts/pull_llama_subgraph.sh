#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-${LPSCAN_HOST:-134.199.135.19}}"
REMOTE_PATH="${2:-/root/llama/}"
DEST_DIR="${3:-subgraphs/llama/}"

if [[ -z "$HOST" ]]; then
  echo "Missing host. Usage: $0 <host> [remote_path] [dest_dir]" >&2
  exit 2
fi

mkdir -p "$DEST_DIR"

echo "Pulling llama subgraph from root@${HOST}:${REMOTE_PATH} -> ${DEST_DIR}"
rsync -az --delete \
  --exclude node_modules \
  --exclude build \
  --exclude .graphclient \
  "root@${HOST}:${REMOTE_PATH}" \
  "${DEST_DIR}"

echo "Done. Synced ${DEST_DIR}"
