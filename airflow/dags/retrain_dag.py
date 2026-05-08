"""
airflow/dags/retrain_dag.py
────────────────────────────
Schedule: 0 2 1 * * (02:00 ngày 1 hàng tháng) hoặc alert trigger.

Thay đổi quan trọng:
  - Quantile thresholds tính lại từ data thực tế mỗi lần retrain
  - Lưu quantiles vào MLflow params cùng với model
  - Monitoring DAG và predict DAG đọc quantiles từ MLflow → không hardcode
  - Weather merge từ 2526.csv (không phải từ Gold — Gold không lưu weather)
  - build_training_data() nhận quantiles=None → tự tính → trả về quantiles_used

Flow:
  load_gold_history → build_features_and_label
                       → [train_model_a ‖ train_model_b]
                              → compare_and_stage
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
TEMP_PATH = "s3://gold/_tmp/retrain"

STORAGE_OPTS = {
    "aws_access_key_id": MINIO_ACCESS,
    "aws_secret_access_key": MINIO_SECRET,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "region_name": "us-east-1",
}

# Weather feature cols — Model B không dùng
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


def task_load_gold_history(**ctx):
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake

    exec_dt = ctx["execution_date"]
    since = pd.Timestamp(exec_dt) - pd.Timedelta(days=365)
    until = pd.Timestamp(exec_dt)

    log.info(f"Loading gold/aggregated: {since} → {until}")
    dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)
    df = dt.to_pandas(
        filters=[("window_end", ">=", str(since)), ("window_end", "<=", str(until))]
    )
    df["window_end"] = pd.to_datetime(df["window_end"])
    if df.empty:
        raise ValueError(
            "gold/aggregated trống — cần ít nhất vài ngày data trước khi retrain"
        )
    log.info(f"Loaded {len(df):,} rows, {df['zone_id'].nunique()} zones")

    # Weather đã được join vào Gold khi streaming (silver_to_gold.py)
    # → không cần merge CSV ở đây
    # Weather leads (lead1/2/3) trong training data = shift(-1/-2/-3) trên lịch sử
    # vì training dùng toàn bộ historical Gold → tất cả slots đều đã có weather
    weather_cols = [
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
    missing = [c for c in weather_cols if c not in df.columns]
    if missing:
        log.warning(f"Weather cols missing in Gold: {missing}. Model A có thể kém hơn.")
    else:
        log.info(f"Weather available in Gold: {len(weather_cols)} cols")

    write_deltalake(TEMP_PATH, df, mode="overwrite", storage_options=STORAGE_OPTS)
    ctx["ti"].xcom_push("n_rows", len(df))
    log.info(f"Saved {len(df):,} rows to temp")


def task_build_features_and_label(**ctx):
    from deltalake import DeltaTable

    dt = DeltaTable(TEMP_PATH, storage_options=STORAGE_OPTS)
    df = dt.to_pandas()
    df["window_end"] = pd.to_datetime(df["window_end"])

    # build_training_data tính: imbalance, temporal, lags, label
    # quantiles=None → tự tính từ data thực tế → trả về quantiles_used
    X, y, quantiles_used = FeatureBuilder.build_training_data(df, quantiles=None)

    # Gắn window_end để time-series split
    df_clean = df.sort_values(["zone_id", "window_end"])
    df_clean["_label_raw"] = (
        df_clean.groupby("zone_id")["imbalance"].shift(-4)
        if "imbalance" in df_clean.columns
        else None
    )

    # Lấy window_end cho các rows tương ứng với X
    # build_training_data drop NaN rows, cần align lại
    from feature_builder import (
        compute_imbalance,
        compute_temporal,
        _add_lag_features,
        assign_label_class,
    )

    df2 = compute_imbalance(df.copy())
    df2 = compute_temporal(df2)
    df2 = df2.sort_values(["zone_id", "window_end"]).reset_index(drop=True)
    df2 = _add_lag_features(df2)
    df2["label_raw"] = df2.groupby("zone_id")["imbalance"].shift(-4)
    df2["label_6class"] = df2["label_raw"].apply(
        lambda x: assign_label_class(x, quantiles_used) if pd.notna(x) else np.nan
    )
    df2 = df2.dropna(subset=["label_6class"])
    df2["label_6class"] = df2["label_6class"].astype(int)

    for c in ALL_FEATURE_COLS:
        if c not in df2.columns:
            df2[c] = np.nan

    df2 = df2.sort_values("window_end").reset_index(drop=True)
    cut = int(len(df2) * 0.9)
    train_df = df2.iloc[:cut]
    val_df = df2.iloc[cut:]

    log.info(f"Split: train={len(train_df):,} val={len(val_df):,}")
    log.info(
        f"Train label dist:\n{train_df['label_6class'].value_counts().sort_index()}"
    )
    log.info(f"Quantiles used: {quantiles_used}")

    train_df[ALL_FEATURE_COLS + ["label_6class"]].to_parquet(
        "/tmp/train.parquet", index=False
    )
    val_df[ALL_FEATURE_COLS + ["label_6class"]].to_parquet(
        "/tmp/val.parquet", index=False
    )

    ctx["ti"].xcom_push("quantiles", quantiles_used)
    ctx["ti"].xcom_push("n_train", len(train_df))
    ctx["ti"].xcom_push("n_val", len(val_df))


def _train_model(model_name, feat_cols, quantiles, ctx):
    """Helper dùng chung cho train_model_a và train_model_b."""
    import lightgbm as lgb
    import mlflow, mlflow.lightgbm
    from sklearn.metrics import accuracy_score, f1_score, classification_report

    mlflow.set_tracking_uri(MLFLOW_URI)

    train_df = pd.read_parquet("/tmp/train.parquet")
    val_df = pd.read_parquet("/tmp/val.parquet")

    X_train = train_df[feat_cols]
    y_train = train_df["label_6class"]
    X_val = val_df[feat_cols]
    y_val = val_df["label_6class"]

    log.info(
        f"Training {model_name}: {len(X_train):,} rows × {len(X_train.columns)} features"
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
                "feature_schema_version": FeatureBuilder.get_schema_version(),
                # Lưu quantiles vào MLflow — predict + monitor đọc từ đây
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

        y_pred_val = model.predict(X_val)
        val_acc = accuracy_score(y_val, y_pred_val)
        val_f1 = f1_score(y_val, y_pred_val, average="weighted")

        mlflow.log_metrics(
            {
                "val_accuracy": val_acc,
                "val_f1_weighted": val_f1,
                "best_iteration": model.best_iteration_,
            }
        )
        log.info(f"{model_name}: val_acc={val_acc:.4f} val_f1={val_f1:.4f}")
        log.info(f"\n{classification_report(y_val, y_pred_val)}")

        # Feature importance top 20
        fi = pd.DataFrame(
            {"feature": X_train.columns, "importance": model.feature_importances_}
        ).sort_values("importance", ascending=False)
        fi.to_csv("/tmp/fi.csv", index=False)
        mlflow.log_artifact("/tmp/fi.csv", "feature_importance")
        log.info(f"Top 10:\n{fi.head(10).to_string()}")

        mlflow.lightgbm.log_model(model, "model", registered_model_name=model_name)

    from mlflow import MlflowClient

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
    quantiles = ctx["ti"].xcom_pull(
        key="quantiles", task_ids="build_features_and_label"
    )
    _train_model("demand_forecast_with_weather", ALL_FEATURE_COLS, quantiles, ctx)


def task_train_model_b(**ctx):
    quantiles = ctx["ti"].xcom_pull(
        key="quantiles", task_ids="build_features_and_label"
    )
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
            if not vs:
                return 0.0
            return float(
                client.get_run(vs[0].run_id).data.metrics.get("val_accuracy", 0.0)
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
            # Chưa có Production → promote ngay
            client.transition_model_version_stage(model_name, new_ver, "Production")
            log.info(f"✅ {model_name} v{new_ver} → Production (first time)")
        elif new_acc >= prod_acc * 0.90:
            log.info(f"✅ {model_name} v{new_ver} vào Staging, bắt đầu shadow period")
        else:
            client.transition_model_version_stage(model_name, new_ver, "Archived")
            log.warning(
                f"❌ {model_name} v{new_ver} quá tệ ({new_acc:.4f} < {prod_acc*0.90:.4f}) → Archived"
            )

    # Cleanup temp
    try:
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        r = s3.list_objects_v2(Bucket="gold", Prefix="_tmp/retrain/")
        for obj in r.get("Contents", []):
            s3.delete_object(Bucket="gold", Key=obj["Key"])
        log.info("Temp data cleaned up")
    except Exception as e:
        log.warning(f"Cleanup failed: {e}")


default_args = {
    "owner": "bigdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="retrain_models",
    description="Retrain hàng tháng, quantiles tự tính, lưu vào MLflow",
    schedule_interval="0 2 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["training", "core"],
) as dag:
    t1 = PythonOperator(
        task_id="load_gold_history", python_callable=task_load_gold_history
    )
    t2 = PythonOperator(
        task_id="build_features_and_label",
        python_callable=task_build_features_and_label,
    )
    t3a = PythonOperator(task_id="train_model_a", python_callable=task_train_model_a)
    t3b = PythonOperator(task_id="train_model_b", python_callable=task_train_model_b)
    t4 = PythonOperator(
        task_id="compare_and_stage", python_callable=task_compare_and_stage
    )
    t1 >> t2 >> [t3a, t3b] >> t4
