"""
airflow/dags/retrain_dag.py
────────────────────────────
Schedule: 0 2 1 * * (02:00 ngày 1 hàng tháng) hoặc trigger từ monitoring alert.

Flow:
  load_and_prepare
    → [train_model_a ‖ train_model_b]
         → compare_and_stage

Thay đổi so với trước:
  - Không ghi temp xuống MinIO — Gold load vào pandas, tính feature,
    lưu /tmp local để 2 training task song song đọc.
  - Feature pipeline chạy đúng 1 lần trong load_and_prepare (không gọi lại
    compute_imbalance / _add_lag_features thủ công trong training task).
  - Weather không lưu trong Gold — merge từ 2526.csv trước khi tính feature.
  - Quantiles tính lại từ data thực tế, lưu vào MLflow, không hardcode.
"""

import os, sys, logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "ml"))
from feature_builder import (
    FeatureBuilder,
    ALL_FEATURE_COLS,
    LAG_WEATHER_COLS,
    WEATHER_LEAD_STEPS,
)

log = logging.getLogger("retrain")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
WEATHER_CSV_PATH = os.getenv("WEATHER_CSV_PATH", "/datasets/weather/2526.csv")
GOLD_AGG_PATH = "s3://gold/aggregated"

TMP_TRAIN = "/tmp/retrain_train.parquet"
TMP_VAL = "/tmp/retrain_val.parquet"

STORAGE_OPTS = {
    "aws_access_key_id": MINIO_ACCESS,
    "aws_secret_access_key": MINIO_SECRET,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "region_name": "us-east-1",
}

WEATHER_FEATURE_COLS = (
    [
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
    + [f"{c}_lag{s}" for c in LAG_WEATHER_COLS for s in [1, 2, 3, 4]]
    + [f"{c}_lead{s}" for c in LAG_WEATHER_COLS for s in WEATHER_LEAD_STEPS]
)
NO_WEATHER_FEATURES = [c for c in ALL_FEATURE_COLS if c not in WEATHER_FEATURE_COLS]

LGBM_PARAMS = {
    "objective": "multiclass",
    "num_class": 6,
    "metric": "multi_logloss",
    "boosting_type": "gbdt",
    "num_leaves": 127,
    "learning_rate": 0.05,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 50,
    "n_estimators": 500,
    "early_stopping_rounds": 30,
    "verbosity": -1,
    "n_jobs": -1,
    "seed": 42,
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


def task_load_and_prepare(**ctx):
    """
    1. Load Gold 12 tháng từ Delta (không ghi temp MinIO)
    2. Merge weather từ 2526.csv theo (zone_id, window_end)
       — training dùng toàn bộ lịch sử nên tất cả slots đều có weather,
         lead weather = shift(-1/-2/-3) trên lịch sử, không cần inject riêng
    3. Gọi FeatureBuilder.build_training_data() đúng 1 lần
       — trả về X, y, quantiles_used (không tự tính lại thủ công)
    4. Time-series split 90/10 theo index (đã sort by window_end trong build_training_data)
    5. Lưu train/val vào /tmp local — 2 training task đọc song song từ đây
    """
    from deltalake import DeltaTable

    exec_dt = ctx["execution_date"]
    since = pd.Timestamp(exec_dt) - pd.Timedelta(days=365)
    until = pd.Timestamp(exec_dt)

    log.info(f"Loading gold/aggregated: {since} → {until}")
    dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)
    gold = dt.to_pandas(
        filters=[
            ("window_end", ">=", str(since)),
            ("window_end", "<=", str(until)),
        ]
    )
    gold["window_end"] = pd.to_datetime(gold["window_end"])

    if gold.empty:
        raise ValueError("gold/aggregated trống — pipeline chưa đủ data để retrain")
    log.info(f"Gold loaded: {len(gold):,} rows, {gold['zone_id'].nunique()} zones")

    # Merge weather từ CSV — Gold không còn lưu weather
    # CSV columns: LocationID, datetime (theo notebook) → rename về zone_id, window_end
    log.info("Merging weather from CSV...")
    weather = pd.read_csv(
        WEATHER_CSV_PATH,
        parse_dates=["datetime"],
        usecols=["LocationID", "datetime"] + WEATHER_COLS,
    )
    weather = weather.rename(
        columns={"LocationID": "zone_id", "datetime": "window_end"}
    )
    weather["window_end"] = pd.to_datetime(weather["window_end"])
    gold = gold.merge(weather, on=["zone_id", "window_end"], how="left")

    missing_wx = gold[WEATHER_COLS[0]].isna().sum()
    log.info(
        f"Weather merged: {missing_wx:,} rows thiếu weather ({missing_wx/len(gold)*100:.1f}%)"
    )

    # Build features + label — 1 lần duy nhất
    # build_training_data: compute_imbalance → compute_temporal → _add_lag_features
    #   → label = lead(imbalance, 4) → drop NaN → time-series sort → trả về X, y, quantiles
    log.info("Building features...")
    X, y, quantiles_used = FeatureBuilder.build_training_data(gold, quantiles=None)
    log.info(
        f"Feature matrix: {X.shape}, label dist:\n{pd.Series(y).value_counts().sort_index()}"
    )
    log.info(f"Quantiles: {quantiles_used}")

    # Time-series split 90/10 — không shuffle (temporal dependency)
    # X đã sort theo window_end bên trong build_training_data
    cut = int(len(X) * 0.9)
    X_train = X.iloc[:cut]
    X_val = X.iloc[cut:]
    y_train = y.iloc[:cut]
    y_val = y.iloc[cut:]
    log.info(f"Split: train={len(X_train):,} val={len(X_val):,}")

    # Lưu /tmp local — nhanh, không cần MinIO, sẽ cleanup sau
    train_df = X_train.copy()
    train_df["_label"] = y_train.values
    val_df = X_val.copy()
    val_df["_label"] = y_val.values
    train_df.to_parquet(TMP_TRAIN, index=False)
    val_df.to_parquet(TMP_VAL, index=False)
    log.info(f"Saved to {TMP_TRAIN}, {TMP_VAL}")

    ctx["ti"].xcom_push("quantiles", quantiles_used)
    ctx["ti"].xcom_push("n_train", len(X_train))
    ctx["ti"].xcom_push("n_val", len(X_val))


def _train_model(model_name: str, feat_cols: list, quantiles: dict, ctx):
    import lightgbm as lgb
    import mlflow, mlflow.lightgbm
    from sklearn.metrics import accuracy_score, f1_score, classification_report
    from mlflow import MlflowClient

    mlflow.set_tracking_uri(MLFLOW_URI)

    train_df = pd.read_parquet(TMP_TRAIN)
    val_df = pd.read_parquet(TMP_VAL)

    # Chỉ giữ feature columns hợp lệ có trong data
    feat_cols = [c for c in feat_cols if c in train_df.columns]
    X_train, y_train = train_df[feat_cols], train_df["_label"].astype(int)
    X_val, y_val = val_df[feat_cols], val_df["_label"].astype(int)
    log.info(
        f"Training {model_name}: {len(X_train):,} rows × {len(feat_cols)} features"
    )

    with mlflow.start_run(
        run_name=f"{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}"
    ):
        mlflow.log_params(
            {
                **LGBM_PARAMS,
                "model_name": model_name,
                "n_train": len(X_train),
                "n_val": len(X_val),
                "n_features": len(feat_cols),
                "feature_schema_version": FeatureBuilder.get_schema_version(),
                # Quantiles lưu vào MLflow — monitoring DAG đọc từ đây, không hardcode
                "label_Q1": quantiles["Q1"],
                "label_Q2": quantiles["Q2"],
                "label_Q3": quantiles["Q3"],
                "label_Q4": quantiles["Q4"],
                "label_Q5": quantiles["Q5"],
            }
        )

        model = lgb.LGBMClassifier(**LGBM_PARAMS)
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            callbacks=[
                lgb.early_stopping(LGBM_PARAMS["early_stopping_rounds"], verbose=False)
            ],
        )

        y_pred = model.predict(X_val)
        val_acc = accuracy_score(y_val, y_pred)
        val_f1 = f1_score(y_val, y_pred, average="weighted")
        mlflow.log_metrics(
            {
                "val_accuracy": val_acc,
                "val_f1_weighted": val_f1,
                "best_iteration": model.best_iteration_,
            }
        )
        log.info(f"{model_name}: val_acc={val_acc:.4f} f1={val_f1:.4f}")
        log.info(f"\n{classification_report(y_val, y_pred)}")

        fi = pd.DataFrame(
            {"feature": feat_cols, "importance": model.feature_importances_}
        )
        fi = fi.sort_values("importance", ascending=False)
        fi.to_csv("/tmp/fi.csv", index=False)
        mlflow.log_artifact("/tmp/fi.csv", "feature_importance")

        mlflow.lightgbm.log_model(model, "model", registered_model_name=model_name)

    client = MlflowClient(tracking_uri=MLFLOW_URI)
    versions = client.get_latest_versions(model_name, stages=["None"])
    if versions:
        client.transition_model_version_stage(
            model_name, versions[0].version, "Staging"
        )
        log.info(
            f"{model_name} v{versions[0].version} → Staging (shadow period starts)"
        )
        ctx["ti"].xcom_push(f"{model_name}_val_acc", val_acc)
        ctx["ti"].xcom_push(f"{model_name}_version", versions[0].version)


def task_train_model_a(**ctx):
    quantiles = ctx["ti"].xcom_pull(key="quantiles", task_ids="load_and_prepare")
    _train_model("demand_forecast_with_weather", ALL_FEATURE_COLS, quantiles, ctx)


def task_train_model_b(**ctx):
    quantiles = ctx["ti"].xcom_pull(key="quantiles", task_ids="load_and_prepare")
    _train_model("demand_forecast_no_weather", NO_WEATHER_FEATURES, quantiles, ctx)


def task_compare_and_stage(**ctx):
    from mlflow import MlflowClient
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient(tracking_uri=MLFLOW_URI)
    ti = ctx["ti"]

    def get_prod_acc(name):
        try:
            vs = client.get_latest_versions(name, stages=["Production"])
            return (
                float(
                    client.get_run(vs[0].run_id).data.metrics.get("val_accuracy", 0.0)
                )
                if vs
                else 0.0
            )
        except:
            return 0.0

    for model_name in ["demand_forecast_with_weather", "demand_forecast_no_weather"]:
        new_acc = ti.xcom_pull(key=f"{model_name}_val_acc")
        new_ver = ti.xcom_pull(key=f"{model_name}_version")
        if new_acc is None or new_ver is None:
            continue
        prod_acc = get_prod_acc(model_name)
        log.info(f"{model_name}: new={new_acc:.4f} prod={prod_acc:.4f}")

        if prod_acc == 0.0:
            client.transition_model_version_stage(model_name, new_ver, "Production")
            log.info(f"{model_name} v{new_ver} → Production (first deploy)")
        elif new_acc >= prod_acc * 0.90:
            log.info(f"{model_name} v{new_ver} vào Staging — bắt đầu shadow period")
        else:
            client.transition_model_version_stage(model_name, new_ver, "Archived")
            log.warning(
                f"{model_name} v{new_ver} archived (new={new_acc:.4f} < {prod_acc*0.90:.4f})"
            )

    # Cleanup /tmp
    for f in [TMP_TRAIN, TMP_VAL, "/tmp/fi.csv"]:
        try:
            os.remove(f)
        except:
            pass
    log.info("Cleanup done")


default_args = {
    "owner": "bigdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="retrain_models",
    description="Retrain hàng tháng — weather từ CSV, quantiles tự tính",
    schedule_interval="0 2 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["training", "core"],
) as dag:
    t1 = PythonOperator(
        task_id="load_and_prepare", python_callable=task_load_and_prepare
    )
    t3a = PythonOperator(task_id="train_model_a", python_callable=task_train_model_a)
    t3b = PythonOperator(task_id="train_model_b", python_callable=task_train_model_b)
    t4 = PythonOperator(
        task_id="compare_and_stage", python_callable=task_compare_and_stage
    )
    t1 >> [t3a, t3b] >> t4
