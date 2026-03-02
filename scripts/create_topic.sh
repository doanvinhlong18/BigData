#!/bin/bash
# ============================================================
#  create_topic.sh – Tạo Kafka topics thủ công
#
#  Chạy từ trong kafka container:
#    docker exec -it kafka bash /opt/spark/app/scripts/create_topic.sh
#
#  Hoặc từ máy host:
#    docker exec kafka bash -c "$(cat scripts/create_topic.sh)"
# ============================================================

BOOTSTRAP="kafka:9092"

echo "[TOPICS] Creating Kafka topics..."

# Topic weather – 1 partition (single hourly feed)
# Tên phải khớp với: TOPIC = "nyc_weather" trong weather_producer.py
kafka-topics \
  --bootstrap-server $BOOTSTRAP \
  --create \
  --if-not-exists \
  --topic nyc_weather \
  --partitions 1 \
  --replication-factor 1

# Topic taxi events – 3 partitions (request + response, high throughput)
# Tên phải khớp với: TOPIC = "nyc_taxi_events" trong request_producer.py / response_producer.py
kafka-topics \
  --bootstrap-server $BOOTSTRAP \
  --create \
  --if-not-exists \
  --topic nyc_taxi_events \
  --partitions 3 \
  --replication-factor 1

echo "[TOPICS] Done. Listing all topics:"
kafka-topics --bootstrap-server $BOOTSTRAP --list