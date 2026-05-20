#!/bin/bash
# scripts/submit-jobs.sh
# Run inside the spark-master container:
#   docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh
#
# For 2-machine setup, set MASTER_IP in the container environment
# (already done in docker-compose.master.yml via MASTER_IP: ${MASTER_IP}).
# The driver runs client-mode inside this container; remote executors on
# the worker machine reach the driver at MASTER_IP:4040 / MASTER_IP:7337.

set -e

# ── Paths ────────────────────────────────────────────────────────────────────
SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
# Must match the volume mount path in docker-compose.master.yml
JOBS_DIR="/opt/spark/app/spark/jobs"

# ── JARs baked into the bigdata-spark image ──────────────────────────────────
JARS="/opt/spark/jars/delta-spark_2.12-3.1.0.jar,\
/opt/spark/jars/delta-storage-3.1.0.jar,\
/opt/spark/jars/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/aws-java-sdk-bundle-1.12.517.jar"

# ── Kafka connector resolved at runtime ──────────────────────────────────────
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# ── Resolve MASTER_IP for driver advertisement ───────────────────────────────
# Remote executors need to reach the driver (running in this container) back at
# MASTER_IP. Docker Desktop forwards MASTER_IP:4040 and MASTER_IP:7337 here.
DRIVER_HOST="${MASTER_IP:-spark-master}"
MINIO_EP="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_AK="${AWS_ACCESS_KEY_ID:-minioadmin}"
MINIO_SK="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

# ── Common spark-submit flags ─────────────────────────────────────────────────
COMMON="spark-submit \
  --master ${SPARK_MASTER} \
  --deploy-mode client \
  --packages ${PACKAGES} \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --jars ${JARS} \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_EP} \
  --conf spark.hadoop.fs.s3a.access.key=${MINIO_AK} \
  --conf spark.hadoop.fs.s3a.secret.key=${MINIO_SK} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.driver.host=${DRIVER_HOST} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.blockManager.port=7337"

echo "=== Spark master : ${SPARK_MASTER} ==="
echo "=== Driver host  : ${DRIVER_HOST} (remote executors call back here) ==="
echo "=== MinIO        : ${MINIO_EP} ==="
echo ""

# ── Submit streaming jobs (all run concurrently) ─────────────────────────────

echo "[1/5] Kafka → Bronze"
${COMMON} ${JOBS_DIR}/taxi_kafka_to_bronze.py &
sleep 5

echo "[2/5] Bronze/request → Silver/request"
${COMMON} ${JOBS_DIR}/request_bronze_to_silver.py &
sleep 3

echo "[3/5] Silver/request ⋈ Bronze/pickup → Silver/response"
${COMMON} ${JOBS_DIR}/request_to_response_silver.py &
sleep 5

echo "[4/5] Silver/response ⋈ Bronze/dropoff → Silver/complete"
${COMMON} ${JOBS_DIR}/complete_bronze_to_silver.py &
sleep 5

echo "[5/5] Silver/complete → Gold (window aggregation)"
${COMMON} ${JOBS_DIR}/silver_to_gold.py &
sleep 3

echo ""
echo "=== All jobs submitted — monitoring links ==="
echo "  Spark UI  : http://${DRIVER_HOST}:8080"
echo "  Driver UI : http://${DRIVER_HOST}:4040"

wait
