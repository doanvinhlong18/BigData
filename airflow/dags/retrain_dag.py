"""
airflow/dags/retrain_dag.py
─────────────────────────────
Schedule: 0 2 28 * *  (02:00 ngày 1 hàng tháng)

Training flow:
  load_and_prepare → [train_model_a ‖ train_model_b] → compare_and_stage
"""

import os, sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
from deltalake import DeltaTable
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/ml")
from feature_builder import (
    FeatureBuilder,
    ALL_FEATURE_COLS,
    NO_WEATHER_FEATURE_COLS,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
WEATHER_CSV = os.getenv(
    "WEATHER_CSV_PATH", "/datasets/weather/2526.csv"
)  # FIX: path đúng
GOLD_AGG_PATH = "s3://gold/aggregated"
TRAIN_PATH = "/tmp/retrain_train.parquet"
VAL_PATH = "/tmp/retrain_val.parquet"

STORAGE_OPTS = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_KEY,
    "aws_secret_access_key": MINIO_SECRET,
    "region_name": "us-east-1",
    "aws_allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

MODEL_A_NAME = "demand_forecast_model_a"
MODEL_B_NAME = "demand_forecast_model_b"
TRAIN_RATIO = 0.9
N_CLASSES = 6


def task_load_and_prepare(**ctx):
    mlflow.set_tracking_uri(MLFLOW_URI)
    dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)
    gold = dt.to_pandas()
    gold["window_end"] = pd.to_datetime(gold["window_end"])

    wdf = pd.read_csv(WEATHER_CSV, parse_dates=["window_end"])
    wdf["window_end"] = pd.to_datetime(wdf["window_end"])

    feat_df, quantiles = FeatureBuilder.build_training_data(
        gold_df=gold,
        weather_df=wdf,
        label_shift=4,
    )
    feat_df = feat_df.sort_values(["window_end", "zone_id"]).reset_index(drop=True)
    n = len(feat_df)
    split = int(n * TRAIN_RATIO)
    feat_df.iloc[:split].to_parquet(TRAIN_PATH, index=False)
    feat_df.iloc[split:].to_parquet(VAL_PATH, index=False)

    ctx["ti"].xcom_push(key="quantiles", value=quantiles)
    ctx["ti"].xcom_push(key="n_train", value=split)
    ctx["ti"].xcom_push(key="n_val", value=n - split)
    print(f"[PREPARE] train={split:,}  val={n-split:,}  quantiles={quantiles}")


def _train(feature_cols, model_name, run_name, quantiles, **ctx):
    train_df = pd.read_parquet(TRAIN_PATH)
    val_df = pd.read_parquet(VAL_PATH)

    X_tr, y_tr = train_df[feature_cols].values, train_df["label_6class"].values
    X_va, y_va = val_df[feature_cols].values, val_df["label_6class"].values

    counts = np.bincount(y_tr.astype(int), minlength=N_CLASSES)
    weights_map = {i: len(y_tr) / (N_CLASSES * max(c, 1)) for i, c in enumerate(counts)}
    sample_w = np.array([weights_map[int(y)] for y in y_tr])

    params = dict(
        objective="multiclass",
        num_class=N_CLASSES,
        num_leaves=127,
        learning_rate=0.05,
        n_estimators=500,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=0.1,
        n_jobs=-1,
        random_state=42,
        verbose=-1,
    )

    mlflow.set_tracking_uri(MLFLOW_URI)
    with mlflow.start_run(run_name=run_name):
        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_tr,
            y_tr,
            sample_weight=sample_w,
            eval_set=[(X_va, y_va)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(100)],
        )
        y_pred = model.predict(X_va)
        val_acc = float((y_pred == y_va).mean())
        mlflow.log_params({**params, "n_features": len(feature_cols)})
        mlflow.log_params({f"q{i}": q for i, q in enumerate(quantiles)})
        mlflow.log_metric("val_accuracy", val_acc)
        mlflow.lightgbm.log_model(model, "model", registered_model_name=model_name)
        print(f"[TRAIN] {model_name}  val_acc={val_acc:.4f}")

    ctx["ti"].xcom_push(key=f"val_acc_{model_name}", value=val_acc)


def task_train_model_a(**ctx):
    quantiles = ctx["ti"].xcom_pull(key="quantiles", task_ids="load_and_prepare")
    _train(ALL_FEATURE_COLS, MODEL_A_NAME, "train_model_a", quantiles, **ctx)


def task_train_model_b(**ctx):
    quantiles = ctx["ti"].xcom_pull(key="quantiles", task_ids="load_and_prepare")
    _train(NO_WEATHER_FEATURE_COLS, MODEL_B_NAME, "train_model_b", quantiles, **ctx)


def task_compare_and_stage(**ctx):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = mlflow.tracking.MlflowClient()
    for model_name in [MODEL_A_NAME, MODEL_B_NAME]:
        try:
            versions = client.get_latest_versions(model_name, stages=["None"])
            if not versions:
                continue
            latest = max(versions, key=lambda v: int(v.version))
            client.transition_model_version_stage(
                name=model_name,
                version=latest.version,
                stage="Staging",
                archive_existing_versions=False,
            )
            print(f"[STAGE] {model_name} v{latest.version} → Staging")
        except Exception as e:
            print(f"[WARN] {model_name}: {e}")


with DAG(
    dag_id="retrain_models",
    schedule_interval="0 2 28 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ml", "training"],
) as dag:

    t_prepare = PythonOperator(
        task_id="load_and_prepare",
        python_callable=task_load_and_prepare,
        provide_context=True,
    )
    t_model_a = PythonOperator(
        task_id="train_model_a",
        python_callable=task_train_model_a,
        provide_context=True,
    )
    t_model_b = PythonOperator(
        task_id="train_model_b",
        python_callable=task_train_model_b,
        provide_context=True,
    )
    t_stage = PythonOperator(
        task_id="compare_and_stage",
        python_callable=task_compare_and_stage,
        provide_context=True,
    )

    t_prepare >> [t_model_a, t_model_b] >> t_stage
