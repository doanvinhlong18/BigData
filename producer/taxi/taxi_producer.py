import csv
import time
import json
from datetime import datetime
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "nyc_green_taxi_trips"
CSV_PATH = "../datasets/nyc_taxi_2018.csv"

# 1 hour event time = 1 second realtime
SPEED_FACTOR = 3600


def to_int(value, default=None):
    try:
        return int(float(value))
    except:
        return default


def to_float(value, default=0.0):
    try:
        return float(value)
    except:
        return default


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
            # Trip COMPLETED event
            event_time = parse_time(row["lpep_dropoff_datetime"])

            if prev_event_time:
                delta = (event_time - prev_event_time).total_seconds()
                if delta > 0:
                    time.sleep(delta / SPEED_FACTOR)

            event = {
                # ===== identifiers =====
                "vendor_id": to_int(row["VendorID"]),
                "ratecode_id": to_int(row["RatecodeID"]),
                # ===== time =====
                "pickup_datetime": row["lpep_pickup_datetime"],
                "dropoff_datetime": row["lpep_dropoff_datetime"],
                # ===== location =====
                "pu_location_id": to_int(row["PULocationID"]),
                "do_location_id": to_int(row["DOLocationID"]),
                # ===== trip =====
                "passenger_count": to_int(row["passenger_count"], 0),
                "trip_distance": to_float(row["trip_distance"]),
                "store_and_fwd_flag": row["store_and_fwd_flag"],
                "trip_type": to_int(row["trip_type"]),
                # ===== payment =====
                "payment_type": to_int(row["payment_type"]),
                # ===== fare =====
                "fare_amount": to_float(row["fare_amount"]),
                "extra": to_float(row["extra"]),
                "mta_tax": to_float(row["mta_tax"]),
                "tip_amount": to_float(row["tip_amount"]),
                "tolls_amount": to_float(row["tolls_amount"]),
                "improvement_surcharge": to_float(row["improvement_surcharge"]),
                "congestion_surcharge": to_float(row.get("congestion_surcharge")),
                "ehail_fee": to_float(row.get("ehail_fee")),
                "total_amount": to_float(row["total_amount"]),
            }

            producer.send(TOPIC, event)
            producer.flush()

            print(
                f"[SEND] ingestion={datetime.now()} " f"dropoff_event_time={event_time}"
            )

            prev_event_time = event_time


if __name__ == "__main__":
    main()
