"""
predict_service/predict_service.py
────────────────────────────────────
Thay thế predict_dag trong Airflow.

Vòng lặp chính: mỗi POLL_INTERVAL giây wall clock:
  1. Đọc Delta stats gold/aggregated → lấy window_end mới nhất
  2. So sánh với slot đã predict trong PG
  3. Nếu có slot mới → build features → predict 263 zones → upsert PG
  4. Nếu chưa có slot mới → skip, sleep tiếp

Với SPEED_FACTOR=60 và gold trigger 15 giây:
  - Mỗi 15 giây wall clock gold có 1 slot mới (= 15 phút event_time)
  - POLL_INTERVAL=15 → predict ngay khi slot vừa sẵn sàng
  - Lag thực tế: 0–15 giây wall clock = 0–15 phút event_time
    (tốt hơn nhiều so với loop 4×15s trước đây có lag tối đa 60 giây)

Models được load 1 lần khi khởi động, reload mỗi MODEL_RELOAD_INTERVAL
giây để tự động nhận model mới sau khi monitoring_dag promote.
"""

import os
import sys
import time
import logging
import signal
import json
import copy
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pandas as pd
import numpy as np
import mlflow
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from deltalake import DeltaTable
import psycopg2
from psycopg2.extras import execute_values

os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("AWS_REGION", "us-east-1")

# ── Prometheus metrics export ─────────────────────────────────────────────────
# FIX: Alert "PredictServiceStale" dùng metric predict_service_last_prediction_timestamp
# nhưng metric này chưa được export. Thêm prometheus_client để expose /metrics.
from prometheus_client import Gauge, start_http_server

METRIC_LAST_PRED_TS = Gauge(
    "predict_service_last_prediction_timestamp",
    "Unix timestamp của lần cuối predict_service ghi thành công vào PostgreSQL",
)
METRIC_ZONES_PREDICTED = Gauge(
    "predict_service_zones_predicted_last_slot",
    "Số zones được predict trong slot gần nhất",
)
METRIC_MODEL_LOADED = Gauge(
    "predict_service_model_loaded",
    "1 nếu model/stage tương ứng đang được load",
    ["model", "stage"],
)
METRIC_MODEL_READY = Gauge(
    "predict_service_model_ready",
    "1 nếu predict_service có ít nhất một Production model để inference",
)
METRIC_MODELS_LOADED_TOTAL = Gauge(
    "predict_service_models_loaded_total",
    "Số Production models đang được load",
)
METRIC_MODEL_VERSION = Gauge(
    "predict_service_model_version",
    "MLflow model version predict_service thấy trong registry, 0 nếu chưa có",
    ["model", "stage"],
)

# ── Startup validation: feature_builder phải có trước khi import ──────────────
_FEATURE_BUILDER_PATH = "/opt/ml/feature_builder.py"
if not os.path.exists(_FEATURE_BUILDER_PATH):
    # In ra stderr để dễ thấy trong docker logs, rồi exit
    print(
        f"[FATAL] {_FEATURE_BUILDER_PATH} không tồn tại.\n"
        "  Kiểm tra volume mount trong docker-compose:\n"
        "    volumes:\n"
        "      - ./ml:/opt/ml\n"
        "  Không thể khởi động predict_service.",
        file=sys.stderr,
    )
    sys.exit(1)

sys.path.insert(0, "/opt/ml")
from feature_builder import (
    FeatureBuilder,
    ALL_FEATURE_COLS,
    NO_WEATHER_FEATURE_COLS,
    inject_weather_leads,
    LAG_STEPS,
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("predict_service")

# ── Config từ env ──────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
GOLD_AGG_PATH = os.getenv("GOLD_AGG_PATH", "s3://gold/aggregated")
WEATHER_PARQUET = os.getenv("WEATHER_PARQUET_PATH", "s3://weather/parquet")
WEATHER_PARQUET_LOCAL = os.getenv("WEATHER_PARQUET_LOCAL", "/datasets/parquet_by_day")
WEATHER_CSV = os.getenv("WEATHER_CSV_PATH", "/datasets/weather/2526.csv")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "bigdata")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
ZONES_GEOJSON_PATH = os.getenv("ZONES_GEOJSON_PATH", "/app/nyc_taxi_zones.geojson")

# Mỗi 15 giây poll 1 lần → khớp với gold trigger 15 giây
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_S", "15"))
# Reload model mỗi 5 phút để nhận promote mới từ monitoring_dag
MODEL_RELOAD_INTERVAL = int(os.getenv("MODEL_RELOAD_INTERVAL_S", "300"))

SLOT_MINUTES = 15

DEMAND_LABELS = {
    0: "0 - Không có",
    1: "1 - Rất thấp",
    2: "2 - Thấp",
    3: "3 - Trung bình",
    4: "4 - Cao",
    5: "5 - Rất cao",
}
DEMAND_COLORS = {
    0: "#F3F4F6",
    1: "#7DD3FC",
    2: "#2563EB",
    3: "#FACC15",
    4: "#F97316",
    5: "#B91C1C",
}
NO_PREDICTION_COLOR = "#D1D5DB"

STORAGE_OPTS = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_KEY,
    "aws_secret_access_key": MINIO_SECRET,
    "region_name": "us-east-1",
    "aws_allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

S3_FAST_CONFIG = Config(
    connect_timeout=3,
    read_timeout=10,
    retries={"max_attempts": 2, "mode": "standard"},
)


def _fast_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        config=S3_FAST_CONFIG,
    )


def _parse_s3_uri(uri: str):
    normalized = uri.replace("s3a://", "s3://", 1)
    parsed = urlparse(normalized)
    return parsed.netloc, parsed.path.strip("/")


def _s3_prefix_available(uri: str) -> bool:
    if not uri.startswith(("s3://", "s3a://")):
        return True
    bucket, prefix = _parse_s3_uri(uri)
    try:
        resp = _fast_s3_client().list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        return resp.get("KeyCount", 0) > 0
    except (BotoCoreError, ClientError, Exception) as e:
        log.warning(f"[S3] Prefix unavailable at {uri}: {e}")
        return False

# ── Graceful shutdown ──────────────────────────────────────────────────────────
_running = True


def _handle_signal(sig, frame):
    global _running
    log.info(f"Signal {sig} received, shutting down after current slot...")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── PostgreSQL helpers ─────────────────────────────────────────────────────────
def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


def _ensure_table():
    """
    Xác nhận bảng predictions_monitoring đã tồn tại.
    Schema được quản lý bởi postgres/init/02_create_schema.sql (single source of truth).
    Hàm này KHÔNG tạo lại bảng để tránh drift giữa 2 nơi định nghĩa schema.
    Nếu bảng chưa tồn tại → log lỗi rõ ràng và exit để người vận hành biết.
    """
    check_sql = """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = 'predictions_monitoring'
        LIMIT 1;
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(check_sql)
            row = cur.fetchone()

    if row is None:
        log.error(
            "[PG] Bảng predictions_monitoring KHÔNG tồn tại.\n"
            "  Schema phải được tạo bởi postgres/init/02_create_schema.sql.\n"
            "  Kiểm tra: docker logs postgres | grep 'predictions_monitoring'\n"
            "  Hoặc chạy thủ công:\n"
            "    docker exec -i postgres psql -U admin -d bigdata < postgres/init/02_create_schema.sql"
        )
        sys.exit(1)

    log.info("[PG] Table predictions_monitoring ready")


def _get_last_predicted_slot() -> pd.Timestamp | None:
    try:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(window_end) FROM predictions_monitoring")
                row = cur.fetchone()
        if row and row[0] is not None:
            ts = pd.Timestamp(row[0])
            return ts.tz_localize("UTC") if ts.tzinfo is None else ts
    except Exception as e:
        log.warning(f"[PG] get_last_predicted_slot failed: {e}")
    return None


def _restore_metrics_from_db():
    """
    Restore non-timestamp metrics after restart. The timestamp metric intentionally
    stays at 0 until this process writes a prediction, so Grafana can show no data
    instead of computing time() - 0 as ~56 years.
    """
    try:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM predictions_monitoring
                    WHERE window_end = (
                        SELECT MAX(window_end) FROM predictions_monitoring
                    )
                    """
                )
                zones = cur.fetchone()[0]
        METRIC_ZONES_PREDICTED.set(float(zones or 0))
        log.info(f"[METRICS] Restored zones_predicted_last_slot={zones or 0}")
    except Exception as e:
        log.warning(f"[METRICS] restore from DB failed: {e}")


# ── GeoJSON endpoint for Grafana zone map ─────────────────────────────────────
def _load_zone_geojson():
    with open(ZONES_GEOJSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _latest_predictions_by_zone():
    sql = """
        WITH latest AS (
            SELECT MAX(window_end) AS window_end
            FROM predictions_monitoring
        )
        SELECT
            zone_id,
            predicted_class,
            pred_confidence,
            used_model,
            model_version,
            window_end
        FROM predictions_monitoring
        JOIN latest USING (window_end)
    """
    rows = {}
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for (
                zone_id,
                predicted_class,
                pred_confidence,
                used_model,
                model_version,
                window_end,
            ) in cur.fetchall():
                rows[int(zone_id)] = {
                    "predicted_class": int(predicted_class),
                    "confidence": float(pred_confidence),
                    "model": used_model,
                    "model_version": model_version or "n/a",
                    "window_end": window_end.isoformat() if window_end else None,
                }
    return rows


def _prediction_style(predicted_class: int | None):
    if predicted_class is None:
        return NO_PREDICTION_COLOR, "Không có prediction"
    return (
        DEMAND_COLORS.get(predicted_class, NO_PREDICTION_COLOR),
        DEMAND_LABELS.get(predicted_class, "Không rõ"),
    )


def _build_prediction_geojson():
    geojson = copy.deepcopy(_load_zone_geojson())
    predictions = _latest_predictions_by_zone()

    for feature in geojson.get("features", []):
        props = feature.setdefault("properties", {})
        try:
            zone_id = int(props.get("LocationID"))
        except (TypeError, ValueError):
            zone_id = None

        if zone_id is not None:
            feature["id"] = zone_id

        pred = predictions.get(zone_id)
        if pred:
            predicted_class = pred["predicted_class"]
            fill, label = _prediction_style(predicted_class)
            props.update(
                {
                    "has_prediction": True,
                    "predicted_class": predicted_class,
                    "demand_label": label,
                    "confidence": round(pred["confidence"], 3),
                    "model": pred["model"],
                    "model_version": pred["model_version"],
                    "window_end": pred["window_end"],
                    "fill": fill,
                    "fill-opacity": 0.9,
                    "stroke": "#FFFFFF",
                    "stroke-width": 0.7,
                }
            )
        else:
            fill, label = _prediction_style(None)
            props.update(
                {
                    "has_prediction": False,
                    "predicted_class": None,
                    "demand_label": label,
                    "confidence": None,
                    "model": None,
                    "model_version": None,
                    "window_end": None,
                    "fill": fill,
                    "fill-opacity": 0.45,
                    "stroke": "#FFFFFF",
                    "stroke-width": 0.5,
                }
            )

    geojson.setdefault("properties", {})
    geojson["properties"].update(
        {
            "prediction_count": len(predictions),
            "latest_window": next(
                (p["window_end"] for p in predictions.values() if p["window_end"]),
                None,
            ),
        }
    )
    return geojson


class _ZonePredictionGeoJSONHandler(BaseHTTPRequestHandler):
    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/geo+json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/health"):
            self._send_json(200, {"status": "ok"})
            return
        if path != "/zone-predictions.geojson":
            self._send_json(404, {"error": "not found"})
            return
        try:
            self._send_json(200, _build_prediction_geojson())
        except Exception as e:
            log.error(f"[GEOJSON] build failed: {e}", exc_info=True)
            self._send_json(500, {"error": str(e)})

    def log_message(self, fmt, *args):
        log.debug("[GEOJSON] " + fmt, *args)


def _start_geojson_server(port: int):
    server = ThreadingHTTPServer(("0.0.0.0", port), _ZonePredictionGeoJSONHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info(
        f"[INIT] Zone GeoJSON endpoint: http://0.0.0.0:{port}/zone-predictions.geojson"
    )
    return server


# ── Gold helpers ───────────────────────────────────────────────────────────────
def _load_latest_gold():
    """
    Đọc 3 snapshot từ gold/aggregated:
      - current  (window_end mới nhất)
      - lag92    (window_end - 92 × 15 phút ≈ 23h)
      - lag668   (window_end - 668 × 15 phút ≈ 7 ngày)
    Dùng Delta stats để tìm latest window, sau đó đọc snapshot theo timestamp.
    """
    try:
        delta_log_uri = f"{GOLD_AGG_PATH.rstrip('/')}/_delta_log/"
        if not _s3_prefix_available(delta_log_uri):
            log.warning(f"[GOLD] Delta log unavailable at {delta_log_uri}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)
        add_actions = dt.get_add_actions(flatten=True).to_pydict()
        max_col = "max.window_end"
        full_df = None

        if max_col in add_actions and add_actions[max_col]:
            raw_max = max(v for v in add_actions[max_col] if v is not None)
            latest_we = pd.Timestamp(raw_max)
            if latest_we.tzinfo is None:
                latest_we = latest_we.tz_localize("UTC")
        else:
            log.warning("[GOLD] Delta stats unavailable, column-scan fallback")
            tmp = dt.to_pandas(columns=["window_end"])
            if tmp.empty:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            tmp["window_end"] = pd.to_datetime(tmp["window_end"])
            latest_we = tmp["window_end"].max()

        lag92_we = latest_we - pd.Timedelta(minutes=SLOT_MINUTES * LAG_STEPS[0])
        lag668_we = latest_we - pd.Timedelta(minutes=SLOT_MINUTES * LAG_STEPS[1])

        def snap(we):
            nonlocal full_df
            target = pd.Timestamp(we)
            if target.tzinfo is None:
                target = target.tz_localize("UTC")
            else:
                target = target.tz_convert("UTC")
            target = target.floor("us")

            try:
                return dt.to_pandas(
                    filters=[("window_end", "=", target.to_pydatetime())]
                )
            except Exception as filter_error:
                log.warning(
                    "[GOLD] timestamp pushdown failed for %s: %s; "
                    "falling back to pandas filter",
                    target,
                    filter_error,
                )
                if full_df is None:
                    full_df = dt.to_pandas()
                    if full_df.empty:
                        return full_df
                    full_df["_window_end_utc"] = (
                        pd.to_datetime(full_df["window_end"], utc=True).dt.floor("us")
                    )
                result = full_df.loc[full_df["_window_end_utc"].eq(target)].copy()
                return result.drop(columns=["_window_end_utc"], errors="ignore")

        return snap(latest_we), snap(lag92_we), snap(lag668_we)

    except Exception as e:
        log.warning(f"[GOLD] load failed: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ── Weather helpers ────────────────────────────────────────────────────────────
def _get_s3_fs():
    import pyarrow.fs as pafs

    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return pafs.S3FileSystem(
        endpoint_override=endpoint,
        access_key=MINIO_KEY,
        secret_key=MINIO_SECRET,
        scheme="http",
    )


def _load_weather(slot_end: pd.Timestamp) -> pd.DataFrame:
    needed: set[str] = set()
    for delta_min in [
        0,
        15,
        30,
        45,
        SLOT_MINUTES * LAG_STEPS[0],
        SLOT_MINUTES * LAG_STEPS[1],
    ]:
        pt = slot_end - pd.Timedelta(minutes=delta_min)
        needed.add(pt.strftime("%Y-%m-%d"))
        needed.add((pt + pd.Timedelta(days=1)).strftime("%Y-%m-%d"))

    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    frames = []

    # 1. Thử MinIO S3 (partition date=YYYY-MM-DD/)
    for d in sorted(needed):
        try:
            dataset = ds.dataset(
                f"{WEATHER_PARQUET}/date={d}/",
                format="parquet",
                filesystem=_get_s3_fs(),
            )
            part = dataset.to_table().to_pandas()
            if not part.empty:
                frames.append(part)
        except Exception:
            pass

    if frames:
        wdf = pd.concat(frames, ignore_index=True)
        wdf["window_end"] = pd.to_datetime(wdf["window_end"])
        return wdf

    # 2. Thử local parquet files: datasets/parquet_by_day/{date}.parquet
    local_frames = []
    for d in sorted(needed):
        fp = os.path.join(WEATHER_PARQUET_LOCAL, f"{d}.parquet")
        if os.path.exists(fp):
            try:
                part = pq.read_table(fp).to_pandas()
                if not part.empty:
                    local_frames.append(part)
                    log.debug(f"[WEATHER] Loaded local parquet {fp}")
            except Exception as e:
                log.warning(f"[WEATHER] Cannot read {fp}: {e}")

    if local_frames:
        wdf = pd.concat(local_frames, ignore_index=True)
        wdf["window_end"] = pd.to_datetime(wdf["window_end"])
        log.info(f"[WEATHER] Loaded {len(local_frames)} local parquet file(s) for {slot_end}")
        return wdf

    # 3. CSV fallback
    if os.path.exists(WEATHER_CSV):
        log.warning("[WEATHER] Parquet not found — CSV fallback")
        wdf = pd.read_csv(WEATHER_CSV, parse_dates=["window_end"])
        wdf["window_end"] = pd.to_datetime(wdf["window_end"])
        return wdf[wdf["window_end"].dt.strftime("%Y-%m-%d").isin(needed)]

    log.warning("[WEATHER] No weather data found (S3, local parquet, or CSV) — running without weather features")
    return pd.DataFrame()


# ── MLflow artifact helpers ───────────────────────────────────────────────────
def _s3_artifact_available(source: str) -> bool:
    if not source or not source.startswith("s3://"):
        return True

    parsed = urlparse(source)
    bucket = parsed.netloc
    prefix = parsed.path.strip("/")
    key = f"{prefix}/MLmodel" if prefix else "MLmodel"

    try:
        _fast_s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except (BotoCoreError, ClientError, Exception) as e:
        log.warning(f"[MODEL] Artifact unavailable at {source}: {e}")
        return False


# ── Model loader ───────────────────────────────────────────────────────────────
class ModelCache:
    """
    Load model từ MLflow 1 lần, tự reload sau MODEL_RELOAD_INTERVAL giây.
    Tách ra class để vòng lặp chính không gọi MLflow mỗi 15 giây.

    Shadow model = version đang ở stage Staging của model_a.
    monitoring_dag promote Staging → Production khi đủ điều kiện.
    predict_service predict bằng Production, ghi thêm shadow_predicted_class
    từ Staging để monitoring_dag có dữ liệu so sánh accuracy.
    """

    def __init__(self):
        self._model_a = None
        self._ver_a = None
        self._model_b = None
        self._ver_b = None
        self._shadow_a = None  # Staging version của model_a (nếu có)
        self._shadow_a_ver = None
        self._last_load = -float(MODEL_RELOAD_INTERVAL)
        mlflow.set_tracking_uri(MLFLOW_URI)
        self._client = mlflow.tracking.MlflowClient()
        self._publish_metrics()

    @staticmethod
    def _version_metric(version) -> float:
        try:
            return float(version)
        except (TypeError, ValueError):
            return 0.0

    def _publish_metrics(self):
        prod_a_loaded = 1.0 if self._model_a else 0.0
        prod_b_loaded = 1.0 if self._model_b else 0.0
        shadow_a_loaded = 1.0 if self._shadow_a else 0.0

        METRIC_MODEL_LOADED.labels("model_a", "Production").set(prod_a_loaded)
        METRIC_MODEL_LOADED.labels("model_b", "Production").set(prod_b_loaded)
        METRIC_MODEL_LOADED.labels("model_a", "Staging").set(shadow_a_loaded)

        METRIC_MODEL_VERSION.labels("model_a", "Production").set(
            self._version_metric(self._ver_a)
        )
        METRIC_MODEL_VERSION.labels("model_b", "Production").set(
            self._version_metric(self._ver_b)
        )
        METRIC_MODEL_VERSION.labels("model_a", "Staging").set(
            self._version_metric(self._shadow_a_ver)
        )

        METRIC_MODELS_LOADED_TOTAL.set(prod_a_loaded + prod_b_loaded)
        METRIC_MODEL_READY.set(1.0 if (self._model_a or self._model_b) else 0.0)

    def _load_one(self, name: str, stage: str = "Production"):
        try:
            vs = self._client.get_latest_versions(name, stages=[stage])
            if not vs:
                return None, None
            version = vs[0]
            if not _s3_artifact_available(version.source):
                return None, version.version
            return (
                mlflow.lightgbm.load_model(f"models:/{name}/{stage}"),
                version.version,
            )
        except Exception as e:
            log.warning(f"[MODEL] {name}/{stage} load failed: {e}")
            return None, None

    def refresh_if_needed(self):
        now = time.monotonic()
        if now - self._last_load < MODEL_RELOAD_INTERVAL:
            return
        log.info("[MODEL] Reloading models from MLflow...")
        self._model_a, self._ver_a = self._load_one(
            "demand_forecast_model_a", "Production"
        )
        self._model_b, self._ver_b = self._load_one(
            "demand_forecast_model_b", "Production"
        )
        # Shadow = Staging của model_a — đúng với logic monitoring_dag
        self._shadow_a, self._shadow_a_ver = self._load_one(
            "demand_forecast_model_a", "Staging"
        )
        self._last_load = now
        log.info(
            f"[MODEL] model_a={'OK v'+str(self._ver_a) if self._model_a else 'NONE'} "
            f"model_b={'OK v'+str(self._ver_b) if self._model_b else 'NONE'} "
            f"shadow_a={'OK v'+str(self._shadow_a_ver) if self._shadow_a else 'NONE'}"
        )
        self._publish_metrics()

    @property
    def model_a(self):
        return self._model_a

    @property
    def ver_a(self):
        return self._ver_a

    @property
    def model_b(self):
        return self._model_b

    @property
    def ver_b(self):
        return self._ver_b

    @property
    def shadow(self):
        return self._shadow_a  # alias dùng trong _predict_slot


# ── Predict 1 slot → upsert PG ────────────────────────────────────────────────
def _predict_slot(
    slot_end, cur_df, l92_df, l668_df, weather_df, cache: ModelCache
) -> int:
    # FeatureBuilder.build_inference_matrix_from_snapshots() already calls
    # inject_weather_leads() internally, so don't call it again here
    feat_df = FeatureBuilder.build_inference_matrix_from_snapshots(
        current_df=cur_df,
        lag92_df=l92_df,
        lag668_df=l668_df,
        weather_df=weather_df if not weather_df.empty else None,
    )

    rows = []
    predicted_at = pd.Timestamp.utcnow()
    for _, row in feat_df.iterrows():
        zone_id = int(row["zone_id"])
        has_wx = all(pd.notna(row.get(f"temperature_2m_lead{i}")) for i in [1, 2, 3])

        if cache.model_a and has_wx:
            x = row[ALL_FEATURE_COLS].values.reshape(1, -1)
            proba = cache.model_a.predict_proba(x)[0]
            pred, used, m_ver = int(np.argmax(proba)), "model_a", cache.ver_a
        elif cache.model_b:
            x = row[NO_WEATHER_FEATURE_COLS].values.reshape(1, -1)
            proba = cache.model_b.predict_proba(x)[0]
            pred, used, m_ver = int(np.argmax(proba)), "model_b", cache.ver_b
        else:
            proba = np.zeros(6)
            proba[0] = 1.0
            pred, used, m_ver = 0, "fallback", None

        shadow_pred, shadow_proba = None, [None] * 6
        if cache.shadow:
            try:
                sp = cache.shadow.predict_proba(
                    row[ALL_FEATURE_COLS].values.reshape(1, -1)
                )[0]
                shadow_pred = int(np.argmax(sp))
                shadow_proba = sp.tolist()
            except Exception:
                pass

        rows.append(
            {
                "zone_id": zone_id,
                "window_end": slot_end,
                "predicted_class": pred,
                "pred_confidence": float(proba[pred]),
                "used_model": used,
                "model_version": str(m_ver) if m_ver else None,
                "predicted_at": predicted_at,
                "shadow_predicted_class": shadow_pred,
                **{f"proba_{i}": float(proba[i]) for i in range(6)},
                **{
                    f"shadow_proba_{i}": (
                        float(shadow_proba[i]) if shadow_proba[i] is not None else None
                    )
                    for i in range(6)
                },
            }
        )

    if not rows:
        return 0

    cols = [
        "zone_id",
        "window_end",
        "predicted_class",
        "pred_confidence",
        "used_model",
        "model_version",
        "predicted_at",
        "shadow_predicted_class",
        "proba_0",
        "proba_1",
        "proba_2",
        "proba_3",
        "proba_4",
        "proba_5",
        "shadow_proba_0",
        "shadow_proba_1",
        "shadow_proba_2",
        "shadow_proba_3",
        "shadow_proba_4",
        "shadow_proba_5",
    ]
    update_cols = [c for c in cols if c not in ("zone_id", "window_end")]
    upsert_sql = f"""
        INSERT INTO predictions_monitoring ({', '.join(cols)}) VALUES %s
        ON CONFLICT (zone_id, window_end) DO UPDATE SET
        {', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)}
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                upsert_sql,
                [tuple(r[c] for c in cols) for r in rows],
                page_size=500,
            )
        conn.commit()

    return len(rows)


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    log.info(
        f"[INIT] predict_service starting | POLL_INTERVAL={POLL_INTERVAL}s "
        f"| MODEL_RELOAD_INTERVAL={MODEL_RELOAD_INTERVAL}s"
    )

    # FIX: Expose Prometheus metrics trên port 8001
    # (port 8000 đã dùng bởi kafka-lag-exporter)
    # Prometheus scrape: thêm job "predict-service" trong prometheus.yml
    METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
    start_http_server(METRICS_PORT)
    log.info(
        f"[INIT] Prometheus metrics endpoint: http://0.0.0.0:{METRICS_PORT}/metrics"
    )
    GEOJSON_PORT = int(os.getenv("GEOJSON_PORT", "8002"))
    _start_geojson_server(GEOJSON_PORT)

    # Chờ các service phụ thuộc sẵn sàng
    time.sleep(10)
    _ensure_table()
    _restore_metrics_from_db()

    cache = ModelCache()
    cache.refresh_if_needed()  # load ngay lúc khởi động

    while _running:
        tick_start = time.monotonic()

        try:
            # 1. Reload model nếu đến hạn
            cache.refresh_if_needed()

            # 2. Lấy slot đã predict gần nhất
            last_slot = _get_last_predicted_slot()

            # 3. Đọc gold
            cur_df, l92_df, l668_df = _load_latest_gold()

            if cur_df.empty:
                log.debug("[LOOP] gold empty, waiting for Spark...")
            else:
                cur_df["window_end"] = pd.to_datetime(cur_df["window_end"])
                slot_end = cur_df["window_end"].iloc[0]
                if slot_end.tzinfo is None:
                    slot_end = slot_end.tz_localize("UTC")

                if last_slot is not None and slot_end <= last_slot:
                    # Gold chưa có slot mới, Spark chưa trigger xong
                    log.debug(f"[LOOP] slot {slot_end} already predicted, skip")
                else:
                    # 4. Slot mới → predict ngay
                    weather_df = _load_weather(slot_end)
                    n = _predict_slot(
                        slot_end, cur_df, l92_df, l668_df, weather_df, cache
                    )
                    log.info(
                        f"[PREDICT] slot={slot_end} | zones={n} "
                        f"| model={'a' if cache.model_a else 'b/fallback'}"
                    )
                    # FIX: cập nhật Prometheus metrics sau mỗi lần predict thành công
                    # Alert "PredictServiceStale" đọc metric này để biết service còn sống.
                    if n > 0:
                        METRIC_LAST_PRED_TS.set(time.time())
                        METRIC_ZONES_PREDICTED.set(n)
                    cache._publish_metrics()

        except Exception as e:
            log.error(f"[LOOP] unexpected error: {e}", exc_info=True)

        # 5. Sleep phần còn lại của interval
        elapsed = time.monotonic() - tick_start
        sleep_s = max(0.0, POLL_INTERVAL - elapsed)
        if sleep_s > 0:
            time.sleep(sleep_s)

    log.info("[INIT] predict_service stopped cleanly")


if __name__ == "__main__":
    main()
