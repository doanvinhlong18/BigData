"""
producer/unified_producer.py
─────────────────────────────
DESIGN:
  request  event → chứa: request_datetime, PULocationID, DOLocationID, flags
                   KHÔNG có: pickup_datetime, on_scene_datetime (chưa biết lúc này)
  response event → chứa: dropoff_datetime, pickup_datetime, on_scene_datetime,
                          trip_miles, financials (chỉ biết sau khi chuyến xong)

Silver/complete join 2 stream lại → mới có đầy đủ tất cả timestamps trong 1 row.
"""

import sys, os, json, time, gc
from datetime import datetime, timezone
import pyarrow.parquet as pq
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "nyc_taxi_events"
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "60"))
DATASET_BASE = os.getenv("DATASET_PATH", "/datasets")
BATCH_SIZE = 5_000

# Columns gửi theo từng loại event
REQUEST_COLS = [
    "trip_id",
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "PULocationID",
    "DOLocationID",
    "request_datetime",  # event_time của request
    "wav_request_flag",
    "access_a_ride_flag",
    "shared_request_flag",  # tên gốc Parquet, có "d"
]

RESPONSE_COLS = [
    "trip_id",
    "dropoff_datetime",  # event_time của response
    "pickup_datetime",  # chỉ có sau khi tài xế đón xong
    "on_scene_datetime",  # chỉ có ở response
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "driver_pay",
    "tips",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "shared_match_flag",
    "wav_match_flag",
]

CONFIG = {
    "request": {
        "time_field": "request_datetime",
        "event_type": "request",
        "folder": "sorted_request_table",
        "file_tmpl": "request_2025_{:02d}.parquet",
        "cols": REQUEST_COLS,
    },
    "response": {
        "time_field": "dropoff_datetime",
        "event_type": "response",
        "folder": "sorted_response_table",
        "file_tmpl": "response_2025_{:02d}.parquet",
        "cols": RESPONSE_COLS,
    },
}


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
        if not s or s.lower() in ("null", "nan", "none", "nat", ""):
            return None
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except:
        return None


def to_iso(v):
    dt = parse_time(v)
    return dt.isoformat() if dt else None


def to_int(v):
    try:
        return (
            None
            if v is None or str(v).strip().lower() in ("", "null", "nan", "none")
            else int(float(v))
        )
    except:
        return None


def to_float(v):
    try:
        return (
            None
            if v is None or str(v).strip().lower() in ("", "null", "nan", "none")
            else float(v)
        )
    except:
        return None


def to_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return None if s.lower() in ("null", "nan", "none", "") else s


TIMESTAMP_COLS = {
    "request_datetime",
    "pickup_datetime",
    "on_scene_datetime",
    "dropoff_datetime",
}
INT_COLS = {"PULocationID", "DOLocationID", "trip_time"}
FLOAT_COLS = {
    "trip_miles",
    "base_passenger_fare",
    "driver_pay",
    "tips",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
}


def cast_value(col_name, val):
    if col_name in TIMESTAMP_COLS:
        return to_iso(val)
    if col_name in INT_COLS:
        return to_int(val)
    if col_name in FLOAT_COLS:
        return to_float(val)
    return to_str(val)


def sleep_delta(prev, curr):
    if prev is None or curr is None:
        return
    delta = (curr - prev).total_seconds()
    if 0 < delta < 3600:
        time.sleep(delta / SPEED_FACTOR)


def read_and_send(file_path, producer, cfg, prev_time):
    time_field = cfg["time_field"]
    event_type = cfg["event_type"]
    send_cols = cfg["cols"]
    current_time = prev_time
    sent = 0
    errors = 0
    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        all_cols = batch.to_pydict()
        names = list(all_cols.keys())
        for values in zip(*all_cols.values()):
            row = dict(zip(names, values))
            current_time = parse_time(row.get(time_field))
            if current_time is None:
                errors += 1
                continue
            sleep_delta(prev_time, current_time)
            payload = {"event_type": event_type, "event_time": current_time.isoformat()}
            for c in send_cols:
                payload[c] = cast_value(c, row.get(c))
            trip_id = row.get("trip_id")
            key = str(trip_id).encode() if trip_id else None
            try:
                producer.send(TOPIC, key=key, value=payload)
                sent += 1
                prev_time = current_time
                if sent % 10_000 == 0:
                    producer.flush()
                    print(
                        f"[{event_type.upper()}] {sent:,} | {current_time:%Y-%m-%d %H:%M} | err={errors}"
                    )
            except KafkaError as e:
                errors += 1
                print(f"[ERROR] {e}")
        producer.flush()
        gc.collect()
    print(f"[{event_type.upper()}] done sent={sent:,} errors={errors}")
    return current_time


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in CONFIG:
        print("Usage: python unified_producer.py [request|response]")
        sys.exit(1)
    mode = sys.argv[1]
    cfg = CONFIG[mode]
    print(f"[INFO] mode={mode} SPEED_FACTOR={SPEED_FACTOR}")
    time.sleep(5)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        key_serializer=lambda k: k if isinstance(k, bytes) else k.encode(),
        acks=1,
        linger_ms=10,
        batch_size=65536,
        compression_type="snappy",
        retries=3,
        retry_backoff_ms=500,
    )
    folder = os.path.join(DATASET_BASE, cfg["folder"])
    current_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for month in range(1, 12):
        fp = os.path.join(folder, cfg["file_tmpl"].format(month))
        if not os.path.exists(fp):
            print(f"[WARN] {fp}")
            continue
        print(f"\n[{mode.upper()}] {cfg['file_tmpl'].format(month)}")
        current_time = read_and_send(fp, producer, cfg, current_time)
    producer.flush()
    producer.close()
    print(f"\n[{mode.upper()}] All done.")


if __name__ == "__main__":
    main()
