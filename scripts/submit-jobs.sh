#!/bin/bash
# scripts/submit-jobs.sh — Chạy trên Machine 1 sau khi workers đã up
set -e

source .env

MASTER="spark://${MASTER_IP}:7077"
MODE=${1:-all}

echo "=== Submitting Spark Jobs to $MASTER ==="

submit_bronze() {
  echo "[SUBMIT] taxi_kafka_to_bronze.py..."
  docker exec spark-master spark-submit \
    --master "$MASTER" \
    --executor-memory 8g \
    --executor-cores 6 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.sql.shuffle.partitions=6 \
    --conf spark.streaming.kafka.maxRatePerPartition=3000 \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    /opt/spark/app/spark/jobs/taxi_kafka_to_bronze.py &
  echo "  Bronze job submitted (background)"
}

submit_silver_request() {
  echo "[SUBMIT] request_bronze_to_silver.py..."
  docker exec spark-master spark-submit \
    --master "$MASTER" \
    --executor-memory 4g \
    --executor-cores 3 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    /opt/spark/app/spark/jobs/request_bronze_to_silver.py &
  echo "  Silver request job submitted (background)"
}

submit_silver_complete() {
  echo "[SUBMIT] complete_bronze_to_silver.py..."
  docker exec spark-master spark-submit \
    --master "$MASTER" \
    --executor-memory 6g \
    --executor-cores 5 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    /opt/spark/app/spark/jobs/complete_bronze_to_silver.py &
  echo "  Silver complete job submitted (background)"
}

submit_gold() {
  echo "[SUBMIT] silver_to_gold.py..."
  docker exec spark-master spark-submit \
    --master "$MASTER" \
    --executor-memory 4g \
    --executor-cores 3 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    /opt/spark/app/spark/jobs/silver_to_gold.py &
  echo "  Gold job submitted (background)"
}

case "$MODE" in
  bronze)         submit_bronze ;;
  silver-request) submit_silver_request ;;
  silver-complete) submit_silver_complete ;;
  gold)           submit_gold ;;
  all)
    submit_bronze
    sleep 5
    submit_silver_request
    submit_silver_complete
    sleep 5
    submit_gold
    ;;
  *)
    echo "Usage: $0 [bronze|silver-request|silver-complete|gold|all]"
    exit 1
    ;;
esac

# Đợi Spark jobs khởi động xong
echo ""
echo "Waiting 30s for Spark jobs to initialize before starting producers..."
sleep 30

echo "[START] Starting producers..."
docker-compose -f docker-compose.master.yml up -d request-producer response-producer

echo ""
echo "=== All jobs submitted ==="
echo "Monitor at: http://${MASTER_IP}:8080"
echo "Check Kafka lag:"
echo "  docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups"