"""
airflow/dags/weather_and_predict_dag.py
────────────────────────────────────────
Schedule: */15 * * * *

Flow sau khi Gold đã có weather (join ở streaming):
  Task 1: load_weather_leads
    → Đọc silver/weather CHỈ 3 slots tương lai (T+15, T+30, T+45)
    → ~789 rows (263 zones × 3 slots) — rất nhỏ, phù hợp XCom

  Task 2: predict
    → Đọc Gold 7 ngày history (đã có weather)
    → feature_builder.build_inference_matrix() → lag weather từ Gold history
    → inject_weather_leads() → 3 slots tương lai từ Task 1
    → Predict Model A / Model B per zone
    → Ghi gold/predictions_monitoring (actual = NULL)

Quantile thresholds: không lưu per row, load từ MLflow 1 lần khi cần evaluate.
"""

import os, sys, logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "ml"))
from feature_builder import FeatureBuilder

log = logging.getLogger("weather_predict")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
SILVER_WEATHER = os.getenv("SILVER_WEATHER", "s3a://silver/weather")
GOLD_AGG_PATH = "s3://gold/aggregated"
GOLD_PRED_PATH = "s3://gold/predictions_monitoring"
HISTORY_HOURS = 168  # 7 ngày — đủ cho lag668

STORAGE_OPTS = {
    "aws_access_key_id": MINIO_ACCESS,
    "aws_secret_access_key": MINIO_SECRET,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "region_name": "us-east-1",
}

LAG_WEATHER_COLS = [
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "weather_code",
]


def task_load_weather_leads(**ctx):
    """
    Đọc silver/weather cho 3 slots tương lai: T+15, T+30, T+45.
    Gold đã có weather hiện tại và quá khứ — chỉ cần leads.
    ~789 rows (263 zones × 3 slots).
    """
    from deltalake import DeltaTable

    execution_date = ctx["execution_date"]
    window_end = pd.Timestamp(execution_date).floor("15min")

    lead_slots = [window_end + pd.Timedelta(minutes=15 * i) for i in [1, 2, 3]]

    try:
        dt = DeltaTable(SILVER_WEATHER, storage_options=STORAGE_OPTS)
        # Filter chỉ 3 slots tương lai
        leads_df = dt.to_pandas(
            filters=[("window_end", "in", [str(s) for s in lead_slots])]
        )[["zone_id", "window_end"] + LAG_WEATHER_COLS]
        leads_df["window_end"] = pd.to_datetime(leads_df["window_end"])
        log.info(f"Weather leads: {len(leads_df)} rows for {len(lead_slots)} slots")
    except Exception as e:
        log.warning(
            f"Cannot read silver/weather: {e} — leads = NaN → Model B fallback per zone"
        )
        leads_df = pd.DataFrame(columns=["zone_id", "window_end"] + LAG_WEATHER_COLS)

    ctx["ti"].xcom_push(
        "weather_leads", leads_df.to_json(orient="records", date_format="iso")
    )
    ctx["ti"].xcom_push("window_end", str(window_end))


def task_predict(**ctx):
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake
    import mlflow, mlflow.lightgbm

    mlflow.set_tracking_uri(MLFLOW_URI)

    ti = ctx["ti"]
    window_end = pd.Timestamp(
        ti.xcom_pull(key="window_end", task_ids="load_weather_leads")
    )
    leads_json = ti.xcom_pull(key="weather_leads", task_ids="load_weather_leads")
    leads_df = pd.read_json(leads_json, orient="records")
    if not leads_df.empty:
        leads_df["window_end"] = pd.to_datetime(leads_df["window_end"])

    # ── Đọc Gold 7 ngày (đã có weather từ streaming join) ─────────────────────
    since = window_end - pd.Timedelta(hours=HISTORY_HOURS)
    dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)
    gold_df = dt.to_pandas(
        filters=[
            ("window_end", ">=", str(since)),
            ("window_end", "<=", str(window_end)),
        ]
    )
    gold_df["window_end"] = pd.to_datetime(gold_df["window_end"])
    if gold_df.empty:
        raise ValueError(
            f"gold/aggregated trống đến {window_end}. Pipeline chưa có đủ data."
        )
    log.info(f"Gold history: {len(gold_df):,} rows")

    # ── Build feature matrix ──────────────────────────────────────────────────
    # Tự tính imbalance + temporal, lag weather từ Gold history
    X = FeatureBuilder.build_inference_matrix(gold_df)

    # Inject leads (T+15/30/45) từ silver/weather
    if not leads_df.empty:
        X = FeatureBuilder.inject_weather_leads(X, leads_df)

    # ── Load models ───────────────────────────────────────────────────────────
    def load_model(name, stage="Production"):
        try:
            m = mlflow.lightgbm.load_model(f"models:/{name}/{stage}")
            log.info(f"Loaded {name}/{stage}")
            return m
        except Exception as e:
            if stage == "Production":
                raise RuntimeError(
                    f"Cannot load {name}: {e}. Chạy retrain_models trước."
                )
            return None

    model_a = load_model("demand_forecast_with_weather")
    model_b = load_model("demand_forecast_no_weather")
    shadow_a = load_model("demand_forecast_with_weather", "Staging")
    shadow_b = load_model("demand_forecast_no_weather", "Staging")

    from mlflow import MlflowClient

    client = MlflowClient(tracking_uri=MLFLOW_URI)

    def get_run_id(name, stage="Production"):
        try:
            vs = client.get_latest_versions(name, stages=[stage])
            return vs[0].run_id if vs else "unknown"
        except:
            return "unknown"

    feat_cols = FeatureBuilder.get_feature_columns()

    # Xác định zones có đủ weather (dùng Model A)
    WEATHER_CURRENT_COLS = [
        "temperature_2m",
        "relative_humidity_2m",
        "surface_pressure",
        "precipitation",
        "rain",
        "snowfall",
        "cloud_cover",
        "weather_code",
        "wind_speed_10m",
        "wind_gusts_10m",
    ]
    weather_cols_in_X = [c for c in WEATHER_CURRENT_COLS if c in X.columns]

    results = []
    for _, row in X.iterrows():
        zone_id = int(row["zone_id"])
        has_weather = all(pd.notna(row.get(c)) for c in weather_cols_in_X)
        model = model_a if has_weather else model_b
        used = "model_a" if has_weather else "model_b_fallback"

        x_df = pd.DataFrame([row[feat_cols].values], columns=feat_cols)
        proba = model.predict(x_df)[0]
        pred_class = int(np.argmax(proba))

        rec = {
            "zone_id": zone_id,
            "predicted_class": pred_class,
            "pred_confidence": float(np.max(proba)),
            "used_model": used,
            "model_version": get_run_id(
                "demand_forecast_with_weather"
                if has_weather
                else "demand_forecast_no_weather"
            ),
        }
        for i, p in enumerate(proba):
            rec[f"proba_{i}"] = float(p)

        # Shadow prediction
        shm = shadow_a if (has_weather and shadow_a) else shadow_b
        if shm:
            sp = shm.predict(x_df)[0]
            rec["shadow_predicted_class"] = int(np.argmax(sp))
            for i, p in enumerate(sp):
                rec[f"shadow_proba_{i}"] = float(p)
        else:
            rec["shadow_predicted_class"] = None
            for i in range(6):
                rec[f"shadow_proba_{i}"] = None

        results.append(rec)

    pred_df = pd.DataFrame(results)
    pred_df["window_end"] = window_end
    pred_df["feature_schema_version"] = FeatureBuilder.get_schema_version()
    pred_df["predicted_at"] = pd.Timestamp.utcnow()
    # Actual = NULL, monitoring fill sau 60 phút
    pred_df["actual_imbalance"] = None
    pred_df["actual_label_class"] = None
    pred_df["is_correct"] = None
    pred_df["shadow_is_correct"] = None
    pred_df["evaluated_at"] = None
    # Không lưu Q1-Q5 per row — monitoring load từ MLflow khi evaluate

    n_a = (pred_df["used_model"] == "model_a").sum()
    n_b = (pred_df["used_model"] == "model_b_fallback").sum()
    log.info(f"Predicted {window_end}: {n_a} Model A, {n_b} Model B")

    write_deltalake(
        GOLD_PRED_PATH, pred_df, mode="append", storage_options=STORAGE_OPTS
    )
    log.info(f"Written {len(pred_df)} rows to predictions_monitoring")


default_args = {
    "owner": "bigdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="weather_and_predict",
    description="Predict demand mỗi 15 phút (weather đã có trong Gold)",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["prediction", "core"],
) as dag:
    t1 = PythonOperator(
        task_id="load_weather_leads", python_callable=task_load_weather_leads
    )
    t2 = PythonOperator(task_id="predict", python_callable=task_predict)
    t1 >> t2
