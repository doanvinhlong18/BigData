"""
producer/unified_producer.py  —  3 event types: request · pickup · dropoff
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

# ── Columns per event ────────────────────────────────────────────────────────
REQUEST_COLS = [
    "trip_id",
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "PULocationID",
    "DOLocationID",
    "request_datetime",
    "wav_request_flag",
    "access_a_ride_flag",
    "shared_request_flag",
]
PICKUP_COLS = [
    "trip_id",
    "pickup_datetime",
    "on_scene_datetime",
    "PULocationID",
    "shared_match_flag",
    "wav_match_flag",
]
DROPOFF_COLS = [
    "trip_id",
    "dropoff_datetime",
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
]

CONFIG = {
    "request": {
        "time_field": "request_datetime",
        "event_type": "request",
        "folder": "sorted_request_table",
        "file_tmpl": "request_2025_{:02d}.parquet",
        "cols": REQUEST_COLS,
        "single_file": False,
    },
    "pickup": {
        "time_field": "pickup_datetime",
        "event_type": "pickup",
        "folder": "sorted_pickup_table",
        "file_tmpl": "sorted_pickup.parquet",
        "cols": PICKUP_COLS,
        "single_file": True,
    },
    "dropoff": {
        "time_field": "dropoff_datetime",
        "event_type": "dropoff",
        "folder": "sorted_dropoff_table",
        "file_tmpl": "dropoff_2025_{:02d}.parquet",
        "cols": DROPOFF_COLS,
        "single_file": False,
    },
}

# ── Type helpers ─────────────────────────────────────────────────────────────
TS_COLS = {
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


def _parse_time(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if hasattr(v, "to_pydatetime"):
        dt = v.to_pydatetime()
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s or s.lower() in ("null", "nan", "none", "nat", ""):
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except:
        return None


def _cast(col, val):
    if col in TS_COLS:
        dt = _parse_time(val)
        return dt.isoformat() if dt else None
    if col in INT_COLS:
        try:
            return (
                int(float(val))
                if val not in (None, "", "null", "nan", "none")
                else None
            )
        except:
            return None
    if col in FLOAT_COLS:
        try:
            return float(val) if val not in (None, "", "null", "nan", "none") else None
        except:
            return None
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in ("null", "nan", "none", "") else s


def _sleep(prev, curr):
    if prev is None or curr is None:
        return
    d = (curr - prev).total_seconds()
    if 0 < d < 3600:
        time.sleep(d / SPEED_FACTOR)


# ── Send loop ─────────────────────────────────────────────────────────────────
def read_and_send(file_path, producer, cfg, prev_time):
    tf = cfg["time_field"]
    ev = cfg["event_type"]
    cols = cfg["cols"]
    cur = prev_time
    sent = errs = 0
    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=BATCH_SIZE):
        d = batch.to_pydict()
        names = list(d.keys())
        for vals in zip(*d.values()):
            row = dict(zip(names, vals))
            cur = _parse_time(row.get(tf))
            if cur is None:
                errs += 1
                continue
            _sleep(prev_time, cur)
            payload = {"event_type": ev, "event_time": cur.isoformat()}
            for c in cols:
                payload[c] = _cast(c, row.get(c))
            tid = row.get("trip_id")
            key = str(tid).encode() if tid else None
            try:
                producer.send(TOPIC, key=key, value=payload)
                sent += 1
                prev_time = cur
                if sent % 10_000 == 0:
                    producer.flush()
                    print(
                        f"[{ev.upper()}] {sent:,} | {cur:%Y-%m-%d %H:%M} | err={errs}"
                    )
            except KafkaError as e:
                errs += 1
                print(f"[ERR] {e}")
        producer.flush()
        gc.collect()
    print(f"[{ev.upper()}] done sent={sent:,} errors={errs}")
    return cur


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in CONFIG:
        print("Usage: python unified_producer.py [request|pickup|dropoff]")
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
    cur = datetime(2025, 1, 1, tzinfo=timezone.utc)
    if cfg["single_file"]:
        fp = os.path.join(DATASET_BASE, cfg["folder"], cfg["file_tmpl"])
        if os.path.exists(fp):
            cur = read_and_send(fp, producer, cfg, cur)
        else:
            print(f"[WARN] {fp} not found")
    else:
        folder = os.path.join(DATASET_BASE, cfg["folder"])
        for m in range(1, 12):
            fp = os.path.join(folder, cfg["file_tmpl"].format(m))
            if not os.path.exists(fp):
                print(f"[WARN] {fp}")
                continue
            print(f"\n[{mode.upper()}] month={m:02d}")
            cur = read_and_send(fp, producer, cfg, cur)
    producer.flush()
    producer.close()
    print(f"\n[{mode.upper()}] All done.")


if __name__ == "__main__":
    main()
