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

TARGET="${1:-all}"

submit_weather() {
    echo "[SUBMIT] Submitting Weather Bronze streaming job..."
    docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 512m \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 1 \
        /opt/spark/app/spark/jobs/weather_kafka_to_bronze.py &
    echo "[SUBMIT] Weather job submitted (PID: $!)"
}

submit_taxi() {
    echo "[SUBMIT] Submitting Taxi Events Bronze streaming job..."
    docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 1 \
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
