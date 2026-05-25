#!/bin/bash
# scripts/start-worker.sh — Chạy trên worker machine.

set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f ".env" ]; then
  set -a
  . ./.env
  set +a
fi

: "${SPARK_WORKER_HOST:?Set SPARK_WORKER_HOST in .env}"
: "${MASTER_IP:?Set MASTER_IP in .env}"
: "${WORKER_IP:?Set WORKER_IP in .env}"

echo "=== Starting Spark Worker ${SPARK_WORKER_HOST} (${WORKER_IP}) ==="
docker compose -f docker-compose.worker.yml up -d

echo "Worker ${SPARK_WORKER_HOST} started."
echo "Check on Master: http://${MASTER_IP}:8080"
