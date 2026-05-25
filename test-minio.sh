#!/bin/bash
echo "=== Testing MinIO setup command ==="

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
DOCKER_NETWORK="${DOCKER_NETWORK:-bigdata-net}"

# Simulate the exact command
docker run --rm --network "${DOCKER_NETWORK}" \
  minio/mc:latest \
  /bin/sh -c "
    mc alias set local ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} &&
    mc mb -p local/bronze &&
    mc mb -p local/silver &&
    mc mb -p local/gold &&
    mc policy set public local/bronze &&
    mc policy set public local/silver &&
    echo '[SETUP] Buckets created: bronze, silver, gold'
  "
