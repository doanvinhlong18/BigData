#!/bin/bash
# scripts/start-master.sh — Chạy trên Machine 1
set -e

echo "=== Starting BigData Master ==="

echo "[1/5] Starting Zookeeper..."
docker-compose -f docker-compose.master.yml up -d zookeeper
sleep 15

echo "[2/5] Starting Kafka..."
docker-compose -f docker-compose.master.yml up -d kafka
sleep 20

echo "[3/5] Starting MinIO + Spark Master..."
docker-compose -f docker-compose.master.yml up -d minio minio-setup kafka-setup spark-master
sleep 15

echo "[4/5] Starting Airflow + MLflow + Grafana..."
docker-compose -f docker-compose.master.yml up -d airflow mlflow grafana
sleep 10

echo "[5/5] Checking services..."
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "zookeeper|kafka|minio|spark-master|airflow|mlflow|grafana"

echo ""
echo "=== Master ready ==="
echo "  Spark Master UI: http://$(hostname -I | awk '{print $1}'):8080"
echo "  MinIO Console:   http://$(hostname -I | awk '{print $1}'):9001"
echo "  Airflow:         http://$(hostname -I | awk '{print $1}'):8888"
echo "  MLflow:          http://$(hostname -I | awk '{print $1}'):5000"
echo "  Grafana:         http://$(hostname -I | awk '{print $1}'):3000"
echo ""
echo "Next: Start workers on Machine 2 and 3, then run: bash scripts/submit-jobs.sh"