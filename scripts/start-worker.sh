#!/bin/bash
# scripts/start-worker.sh — Chạy trên Machine 2 (WORKER_ID=A) và Machine 3 (WORKER_ID=B)

if [ -z "$WORKER_ID" ]; then
  echo "Usage: WORKER_ID=A bash scripts/start-worker.sh"
  echo "       WORKER_ID=B bash scripts/start-worker.sh"
  exit 1
fi

echo "=== Starting Spark Worker ${WORKER_ID} ==="
WORKER_ID=$WORKER_ID docker-compose -f docker-compose.worker.yml up -d

echo "Worker ${WORKER_ID} started."
echo "Check on Master: http://\${MASTER_IP}:8080"