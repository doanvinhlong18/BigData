"""
airflow/dags/monitoring_dag.py
────────────────────────────────
Schedule: 5 * * * *

Quantile thresholds:
  Load TỪ MLFLOW 1 lần mỗi giờ khi evaluate — không đọc từ prediction rows.
  Lý do: prediction là classification (argmax proba), Q1-Q5 không cần lúc predict.
  Q1-Q5 chỉ cần khi: tính actual_label_class từ actual imbalance để so sánh.

Imbalance:
  Gold không lưu imbalance trực tiếp — tính lại từ (pickup_delay_mean, requests_60m).
  compute_imbalance() từ feature_builder, cùng formula với training.

Flow:
  evaluate_and_fill_actuals
    → Gold window_end + 60 phút → actual imbalance
    → compute_imbalance() → assign_label_class() với quantiles từ MLflow
    → fill actual_label_class, is_correct, shadow_is_correct

  check_shadow_promotion
    → So sánh shadow vs production accuracy
    → Promote / archive sau shadow period

  check_alerts
    → accuracy < 0.45 → trigger retrain
"""

import os, sys, logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "ml"))
from feature_builder import compute_imbalance, assign_label_class, DEFAULT_QUANTILES

log = logging.getLogger("monitoring")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
GOLD_AGG_PATH = "s3://gold/aggregated"
GOLD_PRED_PATH = "s3://gold/predictions_monitoring"

ALERT_THRESHOLD = 0.55
RETRIGGER_THRESHOLD = 0.45
SHADOW_EVAL_DAYS = 3
SHADOW_MAX_DAYS = 7
SHADOW_THRESHOLD = 0.97

STORAGE_OPTS = {
    "aws_access_key_id": MINIO_ACCESS,
    "aws_secret_access_key": MINIO_SECRET,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "region_name": "us-east-1",
}


def load_quantiles_from_mlflow() -> dict:
    """
    Load Q1-Q5 từ MLflow Production model params.
    Chỉ gọi 1 lần mỗi giờ khi evaluate — không lưu per-row.
    """
    try:
        import mlflow
        from mlflow import MlflowClient

        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient(tracking_uri=MLFLOW_URI)
        # Dùng model_a để lấy quantiles (cùng quantiles cho cả A và B)
        versions = client.get_latest_versions(
            "demand_forecast_with_weather", stages=["Production"]
        )
        if not versions:
            log.warning("No Production model — using default quantiles")
            return DEFAULT_QUANTILES
        params = client.get_run(versions[0].run_id).data.params
        q = {
            k: float(params.get(f"label_{k}", DEFAULT_QUANTILES[k]))
            for k in ["Q1", "Q2", "Q3", "Q4", "Q5"]
        }
        log.info(f"Quantiles from MLflow: {q}")
        return q
    except Exception as e:
        log.warning(f"Cannot load quantiles from MLflow: {e} — using defaults")
        return DEFAULT_QUANTILES


def task_evaluate_and_fill_actuals(**ctx):
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake

    exec_dt = ctx["execution_date"]
    eval_hour = pd.Timestamp(exec_dt).floor("h") - pd.Timedelta(hours=1)
    since_we = eval_hour - pd.Timedelta(hours=1)
    until_we = eval_hour
    log.info(f"Evaluating predictions: {since_we} → {until_we}")

    try:
        pred_df = DeltaTable(GOLD_PRED_PATH, storage_options=STORAGE_OPTS).to_pandas(
            filters=[
                ("window_end", ">=", str(since_we)),
                ("window_end", "<=", str(until_we)),
            ]
        )
    except Exception as e:
        log.warning(f"Cannot read predictions_monitoring: {e}")
        return

    if pred_df.empty:
        log.info("No predictions found")
        return
    unevaluated = pred_df[pred_df["evaluated_at"].isna()].copy()
    if unevaluated.empty:
        log.info("All already evaluated")
        return
    log.info(f"Unevaluated: {len(unevaluated)}")

    # Đọc Gold actuals (window_end + 60 phút)
    pred_wes = pd.to_datetime(unevaluated["window_end"].unique())
    actual_wes = pred_wes + pd.Timedelta(hours=1)
    try:
        agg_df = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS).to_pandas(
            filters=[
                ("window_end", ">=", str(actual_wes.min())),
                ("window_end", "<=", str(actual_wes.max())),
            ]
        )
    except Exception as e:
        log.warning(f"Cannot read gold/aggregated: {e}")
        return

    if agg_df.empty:
        log.info("No actual data yet — retry next hour")
        return

    # Tính imbalance từ Gold raw metrics
    agg_df = compute_imbalance(agg_df)
    agg_df["window_end"] = pd.to_datetime(agg_df["window_end"])
    agg_df["pred_window_end"] = agg_df["window_end"] - pd.Timedelta(hours=1)
    agg_slim = agg_df[["zone_id", "pred_window_end", "imbalance"]].rename(
        columns={"imbalance": "actual_imbalance"}
    )

    unevaluated["window_end"] = pd.to_datetime(unevaluated["window_end"])
    merged = unevaluated.merge(
        agg_slim,
        left_on=["zone_id", "window_end"],
        right_on=["zone_id", "pred_window_end"],
        how="left",
    ).drop(columns=["pred_window_end"])

    # Load quantiles từ MLflow 1 lần — dùng cho tất cả rows trong giờ này
    quantiles = load_quantiles_from_mlflow()

    merged["actual_label_class"] = merged["actual_imbalance"].apply(
        lambda x: assign_label_class(x, quantiles) if pd.notna(x) else None
    )

    merged["is_correct"] = np.where(
        merged["actual_label_class"].notna(),
        merged["predicted_class"] == merged["actual_label_class"],
        np.nan,
    )

    if "shadow_predicted_class" in merged.columns:
        merged["shadow_is_correct"] = np.where(
            merged["actual_label_class"].notna()
            & merged["shadow_predicted_class"].notna(),
            merged["shadow_predicted_class"] == merged["actual_label_class"],
            np.nan,
        )

    merged["evaluated_at"] = pd.Timestamp.utcnow()

    to_write = merged[merged["is_correct"].notna()].copy()
    if to_write.empty:
        log.info("No actual data available yet")
        return

    write_deltalake(
        GOLD_PRED_PATH,
        to_write,
        mode="overwrite",
        overwrite_schema=False,
        storage_options=STORAGE_OPTS,
        predicate=f"window_end >= '{since_we}' AND window_end <= '{until_we}'",
    )

    accuracy = float(to_write["is_correct"].mean())
    model_a_acc = float(
        to_write[to_write["used_model"] == "model_a"]["is_correct"].mean()
    )
    model_b_acc = float(
        to_write[to_write["used_model"] == "model_b_fallback"]["is_correct"].mean()
    )
    shadow_acc = (
        float(to_write["shadow_is_correct"].mean())
        if "shadow_is_correct" in to_write.columns
        and to_write["shadow_is_correct"].notna().any()
        else None
    )
    n_eval = int(len(to_write))

    log.info(
        f"Result: overall={accuracy:.4f} A={model_a_acc:.4f} B={model_b_acc:.4f} "
        f"shadow={shadow_acc} n={n_eval}"
    )

    ctx["ti"].xcom_push("accuracy", accuracy)
    ctx["ti"].xcom_push("model_a_acc", model_a_acc)
    ctx["ti"].xcom_push("model_b_acc", model_b_acc)
    ctx["ti"].xcom_push("shadow_acc", shadow_acc)
    ctx["ti"].xcom_push("n_evaluated", n_eval)


def task_check_shadow_promotion(**ctx):
    from mlflow import MlflowClient
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient(tracking_uri=MLFLOW_URI)

    from deltalake import DeltaTable

    since = pd.Timestamp.utcnow() - pd.Timedelta(days=SHADOW_MAX_DAYS)

    for model_name in ["demand_forecast_with_weather", "demand_forecast_no_weather"]:
        try:
            staging = client.get_latest_versions(model_name, stages=["Staging"])
            if not staging:
                continue
            shadow_ver = staging[0]
        except:
            continue

        try:
            df = DeltaTable(GOLD_PRED_PATH, storage_options=STORAGE_OPTS).to_pandas(
                filters=[("predicted_at", ">=", str(since))]
            )
        except:
            continue

        ev = df[df["evaluated_at"].notna()]
        if ev.empty:
            continue

        prod_acc = float(ev["is_correct"].mean())
        shadow_acc = (
            float(ev["shadow_is_correct"].mean())
            if "shadow_is_correct" in ev.columns
            and ev["shadow_is_correct"].notna().any()
            else None
        )
        if shadow_acc is None:
            continue

        shadow_start = pd.Timestamp(shadow_ver.creation_timestamp, unit="ms", tz="UTC")
        shadow_days = (pd.Timestamp.utcnow(tz="UTC") - shadow_start).days

        log.info(
            f"{model_name}: day={shadow_days} prod={prod_acc:.4f} shadow={shadow_acc:.4f}"
        )

        if (
            shadow_days >= SHADOW_EVAL_DAYS
            and shadow_acc >= prod_acc * SHADOW_THRESHOLD
        ):
            for v in client.get_latest_versions(model_name, stages=["Production"]):
                client.transition_model_version_stage(model_name, v.version, "Archived")
            client.transition_model_version_stage(
                model_name, shadow_ver.version, "Production"
            )
            log.info(f"✅ Promoted {model_name} v{shadow_ver.version} → Production")
        elif shadow_days >= SHADOW_MAX_DAYS:
            client.transition_model_version_stage(
                model_name, shadow_ver.version, "Archived"
            )
            log.warning(
                f"❌ {model_name} shadow archived (day={shadow_days}, not improving)"
            )


def task_check_alerts(**ctx):
    ti = ctx["ti"]
    accuracy = ti.xcom_pull(key="accuracy", task_ids="evaluate_and_fill_actuals")
    n_eval = ti.xcom_pull(key="n_evaluated", task_ids="evaluate_and_fill_actuals")

    if accuracy is None or n_eval is None or n_eval < 10:
        log.info("Not enough data to alert")
        return

    if accuracy < RETRIGGER_THRESHOLD:
        log.error(
            f"🚨 accuracy={accuracy:.4f} < {RETRIGGER_THRESHOLD} → triggering retrain"
        )
        try:
            trigger_dag(
                dag_id="retrain_models",
                run_id=f"alert_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                conf={"triggered_by": "monitoring", "accuracy": float(accuracy)},
            )
        except Exception as e:
            log.error(f"Trigger failed: {e}")
    elif accuracy < ALERT_THRESHOLD:
        log.warning(f"⚠️ accuracy={accuracy:.4f} < {ALERT_THRESHOLD} — model degrading")
        # TODO: Slack/email
    else:
        log.info(f"✅ accuracy={accuracy:.4f} — OK")


default_args = {
    "owner": "bigdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="model_monitoring",
    description="Evaluate actuals, shadow promotion, alerts",
    schedule_interval="5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["monitoring", "core"],
) as dag:
    t1 = PythonOperator(
        task_id="evaluate_and_fill_actuals",
        python_callable=task_evaluate_and_fill_actuals,
    )
    t2 = PythonOperator(
        task_id="check_shadow_promotion", python_callable=task_check_shadow_promotion
    )
    t3 = PythonOperator(task_id="check_alerts", python_callable=task_check_alerts)
    t1 >> [t2, t3]
