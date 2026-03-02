#!/bin/bash
echo "=== Testing MinIO setup command ==="

# Simulate the exact command
docker run --rm --network bigdata-final-fixed_default \
  minio/mc:latest \
  /bin/sh -c "
    mc alias set local http://minio:9000 minioadmin minioadmin &&
    mc mb -p local/bronze &&
    mc mb -p local/silver &&
    mc mb -p local/gold &&
    mc policy set public local/bronze &&
    mc policy set public local/silver &&
    echo '[SETUP] Buckets created: bronze, silver, gold'
  "
