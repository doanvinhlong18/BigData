#!/bin/bash
# ============================================================
#  submit_bronze_jobs.sh
#  Submit cả 2 streaming jobs lên Spark cluster
#  Chạy từ máy host sau khi cluster đã up:
#    bash scripts/submit_bronze_jobs.sh [weather|taxi|all]
# ============================================================

TARGET="${1:-all}"

submit_weather() {
    echo "[SUBMIT] Submitting Weather Bronze streaming job..."
    docker exec spark-master \
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 512m \
        --executor-memory 1g \
        --executor-cores 1 \
        /opt/spark/app/spark/jobs/weather_kafka_to_bronze.py &
    echo "[SUBMIT] Weather job submitted (PID: $!)"
}

submit_taxi() {
    echo "[SUBMIT] Submitting Taxi Events Bronze streaming job..."
    docker exec spark-master \
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 512m \
        --executor-memory 1g \
        --executor-cores 2 \
        /opt/spark/app/spark/jobs/taxi_kafka_to_bronze.py &
    echo "[SUBMIT] Taxi job submitted (PID: $!)"
}

case $TARGET in
    weather) submit_weather ;;
    taxi)    submit_taxi ;;
    all)
        submit_weather
        submit_taxi
        echo "[SUBMIT] Both jobs submitted. Logs:"
        echo "  docker logs -f spark-weather-bronze"
        echo "  docker logs -f spark-taxi-bronze"
        ;;
    *)
        echo "Usage: $0 [weather|taxi|all]"
        exit 1
        ;;
esac

wait
echo "[SUBMIT] Done."