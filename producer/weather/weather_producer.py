# kafka/producer/weather_producer.py

import csv
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "nyc_weather_2018"
CSV_PATH = "../datasets/nyc_weather_2018.csv"

# 1 hour weather data = 1 second realtime
SPEED_FACTOR = 3600


def to_int(value, default=None):
    try:
        if value is None or str(value).strip().lower() in ("", "null"):
            return default
        return int(float(value))
    except:
        return default


def to_float(value, default=None):
    try:
        if value is None or str(value).strip().lower() in ("", "null"):
            return default
        return float(value)
    except:
        return default


def parse_time(ts):
    try:
        if ts is None:
            return None

        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)

        if hasattr(ts, "to_pydatetime"):
            dt = ts.to_pydatetime()
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        s = str(ts).strip()
        if s == "" or s.lower() == "null":
            return None

        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except:
        return None


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
                "location_id": to_int(row["LocationID"]),
                "datetime": event_time.isoformat(),
                "temperature_2m": to_float(row["temperature_2m"]),
                "precipitation": to_float(row["precipitation"]),
                "rain": to_float(row["rain"]),
                "snowfall": to_float(row["snowfall"]),
                "cloud_cover": to_float(row["cloud_cover"]),
                "relative_humidity_2m": to_float(row["relative_humidity_2m"]),
                "surface_pressure": to_float(row["surface_pressure"]),
                "wind_speed_10m": to_float(row["wind_speed_10m"]),
                "wind_gusts_10m": to_float(row["wind_gusts_10m"]),
                "soil_temperature_0_to_7cm": to_float(row["soil_temperature_0_to_7cm"]),
                "weather_code": to_int(row["weather_code"]),
            }

            producer.send(TOPIC, event)

            print(
                f"[WEATHER] ingestion={datetime.now(timezone.utc)} "
                f"event_time={event_time} "
                f"location={event['location_id']}"
            )

            prev_event_time = event_time
        producer.flush()


if __name__ == "__main__":
    main()
