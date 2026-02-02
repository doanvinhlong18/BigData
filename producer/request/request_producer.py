import pyarrow.parquet as pq
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "nyc_taxi_events"

# 1 hour event-time = 1 second realtime
SPEED_FACTOR = 3600

START_EVENT_TIME = datetime(2025, 1, 1, 0, 0, 0)


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


def sleep_by_event_time(prev_time, curr_time):
    if prev_time:
        delta = (curr_time - prev_time).total_seconds()
        if delta > 0:
            time.sleep(delta / SPEED_FACTOR)


def read_and_send(file_path, producer, start_time):
    prev_event_time = start_time
    current_event_time = None

    parquet_file = pq.ParquetFile(file_path)

    # đọc từng batch nhỏ, nhưng xử lý từng row
    for batch in parquet_file.iter_batches(batch_size=5_000):
        columns = batch.to_pydict()
        col_names = list(columns.keys())
        rows = zip(*columns.values())

        for values in rows:
            row = dict(zip(col_names, values))
            current_event_time = parse_time(row["request_datetime"])

            sleep_by_event_time(prev_event_time, current_event_time)

            event = {
                "event_type": "request",
                "event_time": current_event_time.isoformat(),
                "trip_id": row.get("trip_id"),
                "hvfhs_license_num": row.get("hvfhs_license_num"),
                "dispatching_base_num": row.get("dispatching_base_num"),
                "pu_location_id": to_int(row.get("PULocationID")),
                "do_location_id": to_int(row.get("DOLocationID")),
                "trip_miles": to_float(row.get("trip_miles")),
                "trip_time": to_int(row.get("trip_time")),
                "base_passenger_fare": to_float(row.get("base_passenger_fare")),
                "tips": to_float(row.get("tips")),
                "tolls": to_float(row.get("tolls")),
                "total_amount": to_float(row.get("total_amount")),
            }

            producer.send(TOPIC, key=row["trip_id"].encode(), value=event)

            print(f"[SEND] request, event_time={current_event_time}")

            prev_event_time = current_event_time

    producer.flush()

    return current_event_time


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    current_time = START_EVENT_TIME
    time.sleep(1)

    for i in range(1, 13):
        file_idx = f"{i:02d}"

        # ===== BOOKED EVENTS =====
        booked_file = (
            f"../../datasets/sorted_request_table/request_2025_{file_idx}.parquet"
        )
        current_time = read_and_send(
            booked_file,
            producer=producer,
            start_time=current_time,
        )


if __name__ == "__main__":
    main()
