# kafka/producer/weather_producer.py

import csv
from base_producer import RealtimeReplayProducer
from config import KAFKA_BOOTSTRAP_SERVERS, WEATHER_TOPIC, SPEED_FACTOR


CSV_PATH = "/data/raw/nyc_weather_hourly.csv"


def run():
    producer = RealtimeReplayProducer(
        topic=WEATHER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        speed_factor=SPEED_FACTOR
    )

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            event = {
                "datetime": row["datetime"],      # hourly timestamp
                "location_id": int(row["location_id"]),
                "temperature": float(row["temp"]),
                "precipitation": float(row["rain"]),
                "wind_speed": float(row["wind"])
            }

            producer.send_event(event, "datetime")


if __name__ == "__main__":
    run()
