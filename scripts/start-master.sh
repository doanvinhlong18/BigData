#!/bin/bash
# scripts/start-master.sh - Run on the master machine.

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

tcp_probe() {
  local host="$1"
  local port="$2"
  python - "$host" "$port" <<'PY'
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
try:
    with socket.create_connection((host, port), timeout=3):
        pass
except OSError:
    sys.exit(1)
PY
}

wait_tcp() {
  local label="$1"
  local host="$2"
  local port="$3"
  local timeout="${4:-120}"
  local elapsed=0
  printf "Waiting %-22s" "$label..."
  while [ "$elapsed" -lt "$timeout" ]; do
    if tcp_probe "$host" "$port"; then
      echo " ${host}:${port} OK"
      return 0
    fi
    sleep 3
    elapsed=$((elapsed + 3))
    printf "."
  done
  echo " TIMEOUT ${host}:${port}"
  return 1
}

wait_http() {
  local label="$1"
  local url="$2"
  local timeout="${3:-120}"
  local elapsed=0
  printf "Waiting %-22s" "$label..."
  while [ "$elapsed" -lt "$timeout" ]; do
    if curl -fsS --connect-timeout 3 --max-time 5 "$url" >/dev/null 2>&1; then
      echo " OK"
      return 0
    fi
    sleep 3
    elapsed=$((elapsed + 3))
    printf "."
  done
  echo " TIMEOUT ${url}"
  return 1
}

load_env

: "${MASTER_IP:?Set MASTER_IP in .env}"
: "${WORKER_IP:?Set WORKER_IP in .env}"

MINIO_API_URL="${MINIO_ENDPOINT_EXTERNAL:-http://${WORKER_IP}:9000}"

echo "=== Starting BigData Master (${MASTER_IP}) ==="
echo "Worker endpoints: ${WORKER_IP}"
wait_tcp  "Zookeeper worker" "$WORKER_IP" 2181 60
wait_tcp  "Kafka worker"     "$WORKER_IP" 29092 120
wait_http "MinIO worker"     "${MINIO_API_URL}/minio/health/live" 60

echo ""
echo "Starting master-side services..."
docker compose -f docker-compose.master.yml up -d --build

echo ""
docker compose -f docker-compose.master.yml ps

echo ""
echo "=== Master ready ==="
echo "  Spark Master UI: http://${MASTER_IP}:8080"
echo "  MLflow:          http://${MASTER_IP}:5000"
echo "  Grafana:         http://${MASTER_IP}:3000"
echo "  MinIO Console:   http://${WORKER_IP}:9001"
echo ""
echo "Next: run Spark jobs with:"
echo "  docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh"
