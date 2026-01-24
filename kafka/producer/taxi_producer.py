# kafka/producer/taxi_producer.py

import csv
from base_producer import RealtimeReplayProducer
from config_producer import KAFKA_BOOTSTRAP_SERVERS, TAXI_TOPIC, SPEED_FACTOR


CSV_PATH = "../../datasets/2018_taxi_trips.csv"


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
