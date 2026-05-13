"""
airflow/dags/monitoring_dag.py
Schedule: 5 * * * *

Demo mode: chỉ giữ shadow promotion, bỏ toàn bộ alert.
FIX Lỗi 9: bỏ import trigger_dag không dùng.
"""

import os, logging
from datetime import datetime, timedelta, timezone

import mlflow
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
SHADOW_EVAL_DAYS = 3
SHADOW_MAX_DAYS = 14
SHADOW_ACC_RATIO = 0.97


def task_check_shadow_promotion(**ctx):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = mlflow.tracking.MlflowClient()
    now = datetime.now(tz=timezone.utc)

    def val_acc(name, stage):
        try:
            vs = client.get_latest_versions(name, stages=[stage])
            return (
                float(
                    client.get_run(vs[0].run_id).data.metrics.get("val_accuracy", 0.0)
                )
                if vs
                else None
            )
        except Exception:
            return None

    def created_at(name, stage):
        try:
            vs = client.get_latest_versions(name, stages=[stage])
            return (
                datetime.fromtimestamp(vs[0].creation_timestamp / 1000, tz=timezone.utc)
                if vs
                else None
            )
        except Exception:
            return None

    for model in ["demand_forecast_model_a", "demand_forecast_model_b"]:
        shadow_acc = val_acc(model, "Staging")
        if shadow_acc is None:
            continue
        created = created_at(model, "Staging")
        if created is None:
            continue

        days = (now - created).days
        prod_acc = val_acc(model, "Production") or 0.0
        log.info(
            f"[SHADOW] {model}: days={days} shadow={shadow_acc:.4f} prod={prod_acc:.4f}"
        )

        if days >= SHADOW_MAX_DAYS:
            for v in client.get_latest_versions(model, stages=["Staging"]):
                client.transition_model_version_stage(
                    name=model, version=v.version, stage="Archived"
                )
            log.warning(f"[SHADOW] {model} archived (>{SHADOW_MAX_DAYS}d)")
            continue

        if days >= SHADOW_EVAL_DAYS and shadow_acc >= prod_acc * SHADOW_ACC_RATIO:
            for v in client.get_latest_versions(model, stages=["Staging"]):
                client.transition_model_version_stage(
                    name=model,
                    version=v.version,
                    stage="Production",
                    archive_existing_versions=True,
                )
            log.info(f"[SHADOW] {model} → Production ✓")


with DAG(
    dag_id="pipeline_monitoring",
    schedule_interval="5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["monitoring"],
) as dag:
    PythonOperator(
        task_id="check_shadow_promotion",
        python_callable=task_check_shadow_promotion,
        provide_context=True,
    )
