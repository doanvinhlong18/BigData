# kafka/producer/taxi_producer.py

import csv
import time
from datetime import datetime
from kafka import KafkaProducer
import json

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TAXI_TOPIC = "nyc_taxi_trips"
CSV_PATH = "../../datasets/2018_taxi_trips.csv"
SPEED_FACTOR = 3600  # 1 hour data = 1 second realtime


class RealtimeReplayProducer:
    def __init__(self, topic, bootstrap_servers, speed_factor=3600):
        self.topic = topic
        self.speed_factor = speed_factor

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.prev_event_time = None

    def parse_time(self, value):
        return datetime.fromisoformat(value)

    def send_event(self, event, event_time_field):
        event_time = self.parse_time(event[event_time_field])

        if self.prev_event_time:
            delta = (event_time - self.prev_event_time).total_seconds()
            if delta > 0:
                time.sleep(delta / self.speed_factor)

        self.producer.send(self.topic, value=event)
        self.prev_event_time = event_time


CSV_PATH = "../../../datasets/2018_taxi_trips.csv"


def run():
    producer = RealtimeReplayProducer(
        topic=TAXI_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        speed_factor=SPEED_FACTOR,
    )

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            event = {
                "vendor_id": row["VendorID"],
                "pickup_datetime": row["tpep_pickup_datetime"],
                "dropoff_datetime": row["tpep_dropoff_datetime"],
                "pu_location_id": int(row["PULocationID"]),
                "do_location_id": int(row["DOLocationID"]),
                "fare_amount": float(row["fare_amount"]),
                "trip_distance": float(row["trip_distance"]),
            }

            producer.send_event(event, "pickup_datetime")


if __name__ == "__main__":
    run()
