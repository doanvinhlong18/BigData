#!/bin/bash
# ============================================================
#  submit_bronze_jobs.sh
#  Máy: 16GB RAM / 12 cores
#  Worker Spark: 6GB RAM / 6 cores
#
#  Phân bổ:
#    Taxi job    → 4 cores, 4GB RAM
#    Weather job → 2 cores, 2GB RAM
#    Tổng        → 6 cores, 6GB RAM ✅
# ============================================================

set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

SPARK_SUBMIT_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
DRIVER_HOST="${MASTER_IP:-spark-master}"
KAFKA_EXECUTOR_BOOTSTRAP="${KAFKA_BOOTSTRAP_EXTERNAL:-${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}}"
TARGET="${1:-all}"

SPARK_SUBMIT_ARGS=(
    /opt/spark/bin/spark-submit
    --master "${SPARK_SUBMIT_MASTER}"
    --deploy-mode client
    --conf "spark.driver.host=${DRIVER_HOST}"
    --conf "spark.driver.bindAddress=0.0.0.0"
    --conf "spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_EXECUTOR_BOOTSTRAP}"
)

submit_weather() {
    echo "[SUBMIT] Submitting Weather Bronze streaming job..."
    docker exec spark-master \
        "${SPARK_SUBMIT_ARGS[@]}" \
        --driver-memory 512m \
        --executor-memory 2g \
        --executor-cores 2 \
        --total-executor-cores 2 \
        /opt/spark/app/spark/jobs/weather_kafka_to_bronze.py &
    echo "[SUBMIT] Weather job submitted (PID: $!)"
}

submit_taxi() {
    echo "[SUBMIT] Submitting Taxi Events Bronze streaming job..."
    docker exec spark-master \
        "${SPARK_SUBMIT_ARGS[@]}" \
        --driver-memory 1g \
        --executor-memory 4g \
        --executor-cores 4 \
        --total-executor-cores 4 \
        /opt/spark/app/spark/jobs/taxi_kafka_to_bronze.py &
    echo "[SUBMIT] Taxi job submitted (PID: $!)"
}

case $TARGET in
    weather) submit_weather ;;
    taxi)    submit_taxi ;;
    all)
        submit_weather
        submit_taxi
        echo ""
        echo "[SUBMIT] ✅ Both jobs submitted."
        echo "  Spark UI : http://localhost:8080"
        echo "  MinIO UI : http://localhost:9001  (minioadmin / minioadmin)"
        ;;
    *)
        echo "Usage: $0 [weather|taxi|all]"
        exit 1
        ;;
esac

wait
echo "[SUBMIT] Done."
