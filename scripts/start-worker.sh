#!/bin/bash
# scripts/start-worker.sh — Chạy trên worker machine.

set -euo pipefail

cd "$(dirname "$0")/.."

load_env() {
  local raw line key value
  while IFS= read -r raw || [ -n "$raw" ]; do
    line="${raw%$'\r'}"
    [ -z "$line" ] && continue
    case "$line" in \#*) continue ;; esac
    [[ "$line" == *"="* ]] || continue
    key="${line%%=*}"
    value="${line#*=}"
    key="$(printf '%s' "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    export "$key=$value"
  done < ".env"
}

if [ -f ".env" ]; then
  load_env
fi

: "${SPARK_WORKER_HOST:?Set SPARK_WORKER_HOST in .env}"
: "${MASTER_IP:?Set MASTER_IP in .env}"
: "${WORKER_IP:?Set WORKER_IP in .env}"

echo "=== Starting Spark Worker ${SPARK_WORKER_HOST} (${WORKER_IP}) ==="
docker compose -f docker-compose.worker.yml up -d

echo "Worker ${SPARK_WORKER_HOST} started."
echo "Check on Master: http://${MASTER_IP}:8080"
