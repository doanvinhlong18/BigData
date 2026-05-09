"""
airflow/dags/weather_and_predict_dag.py
────────────────────────────────────────
Schedule: */15 * * * *

Weather không lưu trong Gold hay Silver — load từ CSV mỗi lần cần.
2526.csv: 263 zones × toàn bộ 2025-2026 × mỗi 15 phút (~9M rows).
Load toàn bộ lên RAM một lần rồi filter theo timestamp — nhanh hơn
query Delta nhiều lần cho các slot lẻ.

Flow:
  Task predict (single task):
    1. Load 2526.csv vào pandas (cache process-level nếu DAG run liên tiếp)
    2. Load Gold 7 ngày history từ Delta
    3. Merge weather vào gold_df theo (zone_id, window_end)
       — bao gồm cả 3 slot tương lai (T+15/30/45) để tính lead features
    4. build_inference_matrix() → tính imbalance, temporal, lag, lead weather
    5. inject_weather_leads() với 3 slots tương lai
    6. Predict Model A (có weather) / Model B (fallback không weather)
    7. Append 263 rows vào gold/predictions_monitoring
"""

import os, sys, logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "ml"))
from feature_builder import FeatureBuilder

log = logging.getLogger("predict")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
WEATHER_CSV_PATH = os.getenv("WEATHER_CSV_PATH", "/datasets/weather/2526.csv")
GOLD_AGG_PATH = "s3://gold/aggregated"
GOLD_PRED_PATH = "s3://gold/predictions_monitoring"
HISTORY_HOURS = 168  # 7 ngày — đủ cho lag668

STORAGE_OPTS = {
    "aws_access_key_id": MINIO_ACCESS,
    "aws_secret_access_key": MINIO_SECRET,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "region_name": "us-east-1",
}

WEATHER_COLS = [
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
LAG_WEATHER_COLS = [
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "weather_code",
]


def load_weather_csv() -> pd.DataFrame:
    """
    Load toàn bộ 2526.csv vào RAM.
    CSV columns thực tế: LocationID, datetime (theo notebook cell 23).
    Rename về zone_id, window_end để khớp với Gold schema.
    ~9M rows × 12 cols — load 1 lần mỗi DAG run, filter theo timestamp cần thiết.
    """
    df = pd.read_csv(
        WEATHER_CSV_PATH,
        parse_dates=["datetime"],
        usecols=["LocationID", "datetime"] + WEATHER_COLS,
    )
    df = df.rename(columns={"LocationID": "zone_id", "datetime": "window_end"})
    df["window_end"] = pd.to_datetime(df["window_end"])
    log.info(f"Weather CSV loaded: {len(df):,} rows")
    return df


def task_predict(**ctx):
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake
    import mlflow, mlflow.lightgbm
    from mlflow import MlflowClient

    mlflow.set_tracking_uri(MLFLOW_URI)

    exec_dt = ctx["execution_date"]
    window_end = pd.Timestamp(exec_dt).floor("15min")
    since = window_end - pd.Timedelta(hours=HISTORY_HOURS)
    lead_slots = [window_end + pd.Timedelta(minutes=15 * i) for i in [1, 2, 3]]

    # ── 1. Load Gold 7 ngày ───────────────────────────────────────────────────
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
            f"gold/aggregated trống đến {window_end}. Pipeline chưa đủ data."
        )
    log.info(f"Gold history: {len(gold_df):,} rows")

    # ── 2. Load weather CSV + merge vào Gold ─────────────────────────────────
    # Load toàn bộ CSV rồi filter — nhanh hơn query Delta nhiều lần
    weather_df = load_weather_csv()

    # Slots cần: 7 ngày lịch sử (để tính lag1-4) + 3 slots tương lai (để tính lead1-3)
    needed_slots = set(gold_df["window_end"].tolist()) | set(lead_slots)
    weather_slice = weather_df[weather_df["window_end"].isin(needed_slots)].copy()

    # Merge weather vào gold (chỉ slots lịch sử — các slot tương lai không có Gold row)
    gold_with_wx = gold_df.merge(
        weather_slice, on=["zone_id", "window_end"], how="left"
    )
    log.info(
        f"Weather merged: {gold_with_wx[WEATHER_COLS[0]].notna().sum()} rows có weather"
    )

    # ── 3. Build feature matrix ───────────────────────────────────────────────
    # build_inference_matrix tính: imbalance, temporal features, lag (bao gồm lag weather)
    X = FeatureBuilder.build_inference_matrix(gold_with_wx)

    # Inject lead weather từ CSV (T+15, T+30, T+45) — không có trong Gold history
    leads_df = weather_slice[weather_slice["window_end"].isin(lead_slots)][
        ["zone_id", "window_end"] + LAG_WEATHER_COLS
    ].copy()
    if not leads_df.empty:
        X = FeatureBuilder.inject_weather_leads(X, leads_df)

    # ── 4. Load models ────────────────────────────────────────────────────────
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

    client = MlflowClient(tracking_uri=MLFLOW_URI)

    def get_run_id(name, stage="Production"):
        try:
            vs = client.get_latest_versions(name, stages=[stage])
            return vs[0].run_id if vs else "unknown"
        except:
            return "unknown"

    feat_cols = FeatureBuilder.get_feature_columns()
    weather_check_cols = [c for c in WEATHER_COLS if c in X.columns]

    # ── 5. Predict per zone ───────────────────────────────────────────────────
    # Batch predict toàn bộ 263 zones cùng lúc — nhanh hơn loop per zone
    X_feat = X[feat_cols].copy()
    has_weather = X_feat[weather_check_cols].notna().all(axis=1)

    # Model A: zones có đủ weather
    results_a = pd.DataFrame(index=X.index)
    if has_weather.any():
        proba_a = model_a.predict(X_feat[has_weather])
        results_a.loc[has_weather, "predicted_class"] = proba_a.argmax(axis=1)
        results_a.loc[has_weather, "pred_confidence"] = proba_a.max(axis=1)
        results_a.loc[has_weather, "used_model"] = "model_a"
        for i in range(6):
            results_a.loc[has_weather, f"proba_{i}"] = proba_a[:, i]

    # Model B: zones thiếu weather
    if (~has_weather).any():
        feat_b = FeatureBuilder.get_no_weather_feature_columns()
        X_b = X_feat.loc[~has_weather, feat_b]
        proba_b = model_b.predict(X_b)
        results_a.loc[~has_weather, "predicted_class"] = proba_b.argmax(axis=1)
        results_a.loc[~has_weather, "pred_confidence"] = proba_b.max(axis=1)
        results_a.loc[~has_weather, "used_model"] = "model_b_fallback"
        for i in range(6):
            results_a.loc[~has_weather, f"proba_{i}"] = proba_b[:, i]

    pred_df = pd.concat([X[["zone_id"]], results_a], axis=1)
    pred_df["predicted_class"] = pred_df["predicted_class"].astype(int)
    pred_df["model_version"] = pred_df["used_model"].map(
        {
            "model_a": get_run_id("demand_forecast_with_weather"),
            "model_b_fallback": get_run_id("demand_forecast_no_weather"),
        }
    )

    # Shadow predictions
    if shadow_a or shadow_b:
        shad_feat_a = FeatureBuilder.get_feature_columns()
        shad_feat_b = FeatureBuilder.get_no_weather_feature_columns()
        shad_proba = np.zeros((len(X), 6))
        if shadow_a and has_weather.any():
            shad_proba[has_weather.values] = shadow_a.predict(X_feat[has_weather])
        if shadow_b and (~has_weather).any():
            shad_proba[~has_weather.values] = shadow_b.predict(
                X_feat.loc[~has_weather, shad_feat_b]
            )
        pred_df["shadow_predicted_class"] = shad_proba.argmax(axis=1)
        for i in range(6):
            pred_df[f"shadow_proba_{i}"] = shad_proba[:, i]
    else:
        pred_df["shadow_predicted_class"] = None
        for i in range(6):
            pred_df[f"shadow_proba_{i}"] = None

    # Metadata + null actuals
    pred_df["window_end"] = window_end
    pred_df["feature_schema_version"] = FeatureBuilder.get_schema_version()
    pred_df["predicted_at"] = pd.Timestamp.utcnow()
    pred_df["actual_label_class"] = None
    pred_df["is_correct"] = None
    pred_df["shadow_is_correct"] = None
    pred_df["evaluated_at"] = None

    n_a = (pred_df["used_model"] == "model_a").sum()
    n_b = (pred_df["used_model"] == "model_b_fallback").sum()
    log.info(f"Predicted {window_end}: {n_a} Model A, {n_b} Model B")

    write_deltalake(
        GOLD_PRED_PATH, pred_df, mode="append", storage_options=STORAGE_OPTS
    )
    log.info(f"Written {len(pred_df)} rows → predictions_monitoring")


default_args = {
    "owner": "bigdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="weather_and_predict",
    description="Predict demand mỗi 15 phút — weather từ CSV",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["prediction", "core"],
) as dag:
    PythonOperator(task_id="predict", python_callable=task_predict)
