#!/bin/bash
# scripts/submit-jobs.sh

set -e

# ================================
# Spark config
# ================================
SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
JOBS_DIR="/opt/spark/jobs"

# ================================
# External dependencies (NON-KAFKA)
# ================================
JARS="/opt/spark/jars/delta-spark_2.12-3.1.0.jar,\
/opt/spark/jars/delta-storage-3.1.0.jar,\
/opt/spark/jars/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/aws-java-sdk-bundle-1.12.517.jar"

# ================================
# Kafka dependency (auto resolve)
# ================================
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# ================================
# Common spark-submit config
# ================================
COMMON="spark-submit \
  --master $SPARK_MASTER \
  --deploy-mode client \
  --packages $PACKAGES \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --jars $JARS \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000} \
  --conf spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY:-minioadmin} \
  --conf spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY:-minioadmin} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"

echo "=== Spark master: $SPARK_MASTER ==="

# ================================
# Submit jobs (pipeline)
# ================================

echo "=== [1/5] Kafka → Bronze ==="
$COMMON $JOBS_DIR/taxi_kafka_to_bronze.py &
sleep 5

echo "=== [2/5] Bronze/request → Silver/request ==="
$COMMON $JOBS_DIR/request_bronze_to_silver.py &
sleep 3

echo "=== [3/5] Silver/request ⋈ Bronze/pickup → Silver/response ==="
$COMMON $JOBS_DIR/request_to_response_silver.py &
sleep 5

echo "=== [4/5] Silver/response ⋈ Bronze/dropoff → Silver/complete ==="
$COMMON $JOBS_DIR/complete_bronze_to_silver.py &
sleep 5

echo "=== [5/5] Silver/complete → Gold (window agg) ==="
$COMMON $JOBS_DIR/silver_to_gold.py &
sleep 3

echo "=== All Spark jobs submitted ==="
echo "Monitor UI: http://spark-master:8080"

wait