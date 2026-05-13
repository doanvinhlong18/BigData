"""
airflow/dags/predict_dag.py
Schedule: */15 * * * *

Hệ thống đang replay lịch sử x60 → mọi logic thời gian dùng
event_time (window_end từ gold), KHÔNG dùng now()/wall clock.

Thay đổi:
  - FIX Lỗi 1  : _load_latest_gold dùng get_add_actions() thay vì full scan
  - FIX Lỗi 4  : predicted_at = window_end (event_time), không dùng now()
  - WEATHER     : Parquet partitioned date=YYYY-MM-DD, filter theo event_time
  - OUTPUT      : Upsert vào PostgreSQL, ON CONFLICT DO UPDATE — idempotent
  - DEMO MODE   : bỏ alert/shadow logic phức tạp
"""

import os, sys, logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import mlflow
from deltalake import DeltaTable
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/ml")
from feature_builder import (
    FeatureBuilder,
    ALL_FEATURE_COLS,
    NO_WEATHER_FEATURE_COLS,
    inject_weather_leads,
    LAG_STEPS,
)

log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
GOLD_AGG_PATH = "s3://gold/aggregated"
WEATHER_PARQUET_BASE = os.getenv("WEATHER_PARQUET_PATH", "s3://weather/parquet")

STORAGE_OPTS = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_KEY,
    "aws_secret_access_key": MINIO_SECRET,
    "region_name": "us-east-1",
    "aws_allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "bigdata")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

SLOT_MINUTES = 15


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
    ddl = """
    CREATE TABLE IF NOT EXISTS predictions_monitoring (
        zone_id             INTEGER      NOT NULL,
        window_end          TIMESTAMPTZ  NOT NULL,
        predicted_class     SMALLINT     NOT NULL,
        pred_confidence     REAL         NOT NULL,
        used_model          VARCHAR(20)  NOT NULL,
        model_version       VARCHAR(20),
        predicted_at        TIMESTAMPTZ  NOT NULL,
        shadow_predicted_class SMALLINT,
        proba_0 REAL, proba_1 REAL, proba_2 REAL,
        proba_3 REAL, proba_4 REAL, proba_5 REAL,
        shadow_proba_0 REAL, shadow_proba_1 REAL, shadow_proba_2 REAL,
        shadow_proba_3 REAL, shadow_proba_4 REAL, shadow_proba_5 REAL,
        PRIMARY KEY (zone_id, window_end)
    );
    CREATE INDEX IF NOT EXISTS idx_pred_predicted_at
        ON predictions_monitoring (predicted_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pred_window_end
        ON predictions_monitoring (window_end DESC);
    CREATE INDEX IF NOT EXISTS idx_pred_zone_time
        ON predictions_monitoring (zone_id, window_end DESC);
    CREATE INDEX IF NOT EXISTS idx_pred_used_model
        ON predictions_monitoring (used_model, window_end DESC);
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def _load_latest_gold():
    """
    FIX Lỗi 1: get_add_actions() đọc Delta transaction log JSON (nhỏ),
    lấy max(window_end) từ file-level stats — O(n_files) không O(n_rows).
    Sau đó load đúng 3 snapshot qua predicate pushdown.
    """
    try:
        dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)
        add_actions = dt.get_add_actions(flatten=True).to_pydict()
        max_col = "max.window_end"

        if max_col in add_actions and add_actions[max_col]:
            raw_max = max(v for v in add_actions[max_col] if v is not None)
            latest_we = pd.Timestamp(raw_max)
            if latest_we.tzinfo is None:
                latest_we = latest_we.tz_localize("UTC")
            log.info(f"[GOLD] latest_we from Delta stats: {latest_we}")
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
            return dt.to_pandas(filters=[("window_end", "=", str(we))])

        cur_df = snap(latest_we)
        l92_df = snap(lag92_we)
        l668_df = snap(lag668_we)
        log.info(f"[GOLD] cur={len(cur_df)} lag92={len(l92_df)} lag668={len(l668_df)}")
        return cur_df, l92_df, l668_df

    except Exception as e:
        log.warning(f"[GOLD] load failed: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def _get_s3_fs():
    import pyarrow.fs as pafs

    return pafs.S3FileSystem(
        endpoint_override=MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_KEY,
        secret_key=MINIO_SECRET,
        scheme="http",
    )


def _load_weather(slot_end: pd.Timestamp) -> pd.DataFrame:
    """
    Tính partition ngày cần load dựa trên event_time (slot_end),
    KHÔNG dùng wall clock — hệ thống đang replay dữ liệu 2025.
    """
    import pyarrow.dataset as ds

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
        # edge case: slot cuối ngày + weather lead vượt sang ngày hôm sau
        needed.add((pt + pd.Timedelta(days=1)).strftime("%Y-%m-%d"))

    frames = []
    for d in sorted(needed):
        try:
            dataset = ds.dataset(
                f"{WEATHER_PARQUET_BASE}/date={d}/",
                format="parquet",
                filesystem=_get_s3_fs(),
            )
            part = dataset.to_table().to_pandas()
            if not part.empty:
                frames.append(part)
        except Exception:
            pass  # partition không tồn tại cho ngày đó — bình thường

    if frames:
        wdf = pd.concat(frames, ignore_index=True)
        wdf["window_end"] = pd.to_datetime(wdf["window_end"])
        log.info(f"[WEATHER] {len(wdf)} rows from {len(frames)} partitions")
        return wdf

    # Fallback CSV — filter đúng ngày event_time, không load toàn bộ
    csv_path = os.getenv("WEATHER_CSV_PATH", "/datasets/weather/2526.csv")
    if os.path.exists(csv_path):
        log.warning("[WEATHER] Parquet not found, CSV fallback")
        wdf = pd.read_csv(csv_path, parse_dates=["window_end"])
        wdf["window_end"] = pd.to_datetime(wdf["window_end"])
        wdf = wdf[wdf["window_end"].dt.strftime("%Y-%m-%d").isin(needed)]
        return wdf

    return pd.DataFrame()


def task_predict(**ctx):
    _ensure_table()

    cur_df, l92_df, l668_df = _load_latest_gold()
    if cur_df.empty:
        log.info("[PREDICT] No gold data, skip")
        return

    cur_df["window_end"] = pd.to_datetime(cur_df["window_end"])
    slot_end = cur_df["window_end"].iloc[0]
    log.info(f"[PREDICT] slot_end (event_time) = {slot_end}")

    weather_df = _load_weather(slot_end)

    feat_df = FeatureBuilder.build_inference_matrix_from_snapshots(
        current_df=cur_df,
        lag92_df=l92_df,
        lag668_df=l668_df,
        weather_df=weather_df if not weather_df.empty else None,
    )
    if not weather_df.empty:
        feat_df = inject_weather_leads(feat_df, weather_df)

    mlflow.set_tracking_uri(MLFLOW_URI)
    client = mlflow.tracking.MlflowClient()

    def load_model(name):
        try:
            vs = client.get_latest_versions(name, stages=["Production"])
            if not vs:
                return None, None
            return (
                mlflow.lightgbm.load_model(f"models:/{name}/Production"),
                vs[0].version,
            )
        except Exception as e:
            log.warning(f"[MODEL] {name}: {e}")
            return None, None

    model_a, ver_a = load_model("demand_forecast_model_a")
    model_b, ver_b = load_model("demand_forecast_model_b")
    shadow, _ = load_model("demand_forecast_shadow")

    # predicted_at = slot_end (event_time)
    # Wall clock now() sẽ sai vì hệ thống replay x60:
    # 1 giờ thực tế = 60 giờ event_time → Grafana time axis bị lệch hoàn toàn
    predicted_at = slot_end

    rows = []
    for _, row in feat_df.iterrows():
        zone_id = int(row["zone_id"])
        has_weather = all(
            pd.notna(row.get(f"temperature_2m_lead{i}")) for i in [1, 2, 3]
        )

        if model_a and has_weather:
            x = row[ALL_FEATURE_COLS].values.reshape(1, -1)
            proba = model_a.predict_proba(x)[0]
            pred, used, m_ver = int(np.argmax(proba)), "model_a", ver_a
        elif model_b:
            x = row[NO_WEATHER_FEATURE_COLS].values.reshape(1, -1)
            proba = model_b.predict_proba(x)[0]
            pred, used, m_ver = int(np.argmax(proba)), "model_b", ver_b
        else:
            proba = np.zeros(6)
            proba[0] = 1.0
            pred, used, m_ver = 0, "fallback", None

        shadow_pred, shadow_proba = None, [None] * 6
        if shadow:
            try:
                sx = row[ALL_FEATURE_COLS].values.reshape(1, -1)
                sp = shadow.predict_proba(sx)[0]
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
        return

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

    log.info(f"[PREDICT] upserted {len(rows)} zones → slot {slot_end}")


with DAG(
    dag_id="predict_demand",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ml", "inference"],
) as dag:
    PythonOperator(
        task_id="predict", python_callable=task_predict, provide_context=True
    )
