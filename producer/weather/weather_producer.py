# kafka/producer/weather_producer.py

import csv
import time
import json
from datetime import datetime
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "nyc_weather_2018"
CSV_PATH = "../datasets/nyc_weather_2018.csv"

# 1 hour weather data = 1 second realtime
SPEED_FACTOR = 3600


def parse_time(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    prev_event_time = None

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            event_time = parse_time(row["datetime"])

            if prev_event_time:
                delta = (event_time - prev_event_time).total_seconds()
                if delta > 0:
                    time.sleep(delta / SPEED_FACTOR)

            event = {
                "location_id": int(row["LocationID"]),
                "datetime": row["datetime"],
                "temperature_2m": float(row["temperature_2m"]),
                "precipitation": float(row["precipitation"]),
                "rain": float(row["rain"]),
                "snowfall": float(row["snowfall"]),
                "cloud_cover": float(row["cloud_cover"]),
            }

            producer.send(TOPIC, event)
            producer.flush()

            print(
                f"[WEATHER] ingestion={datetime.now()} "
                f"event_time={event_time} "
                f"location={event['location_id']}"
            )

            prev_event_time = event_time


if __name__ == "__main__":
    main()
