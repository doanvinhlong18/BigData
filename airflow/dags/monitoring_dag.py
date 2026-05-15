"""
airflow/dags/monitoring_dag.py
Schedule: 5 * * * *

FIX: Shadow promotion dựa trên LIVE accuracy tính từ predictions_monitoring
và Gold data thực tế — thay vì val_accuracy tĩnh từ training set.

Logic mới:
  1. Query predictions_monitoring: lấy các row đã có shadow_predicted_class
  2. Tính target_we = window_end + 4 slots (1 giờ sau) — slot model đang dự báo
  3. Đọc Gold tại target_we → actual imbalance → actual_class
  4. So sánh predicted_class / shadow_predicted_class với actual_class
  5. Promote Shadow → Production nếu:
       - Đã chạy ≥ SHADOW_EVAL_DAYS ngày wall clock
       - shadow_live_acc ≥ prod_live_acc × SHADOW_ACC_RATIO
       - Có ≥ SHADOW_MIN_SAMPLES predictions để so sánh
  6. Archive nếu đã qua SHADOW_MAX_DAYS mà chưa đủ điều kiện

Fallback: nếu Gold không đọc được (MinIO down) hoặc quá ít samples,
dùng lại val_accuracy từ MLflow (hành vi cũ) và log warning rõ ràng.
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import mlflow
import numpy as np
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "bigdata")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
GOLD_AGG_PATH = os.getenv("GOLD_AGG_PATH", "s3://gold/aggregated")

STORAGE_OPTS = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

SHADOW_EVAL_DAYS = 3  # Phải chạy ≥ 3 ngày wall clock trước khi xét promote
SHADOW_MAX_DAYS = 14  # Sau 14 ngày vẫn không đủ → Archive
SHADOW_ACC_RATIO = 0.97  # shadow_live_acc ≥ prod_live_acc × 0.97
SHADOW_MIN_SAMPLES = 500  # Cần ≥ 500 predictions có shadow để tin live accuracy

LABEL_SHIFT = 4  # slots — prediction là cho T + 4×15min = T + 1h
SLOT_MINUTES = 15
EVAL_HOURS = 72  # Lấy 72h event time gần nhất để tính live accuracy

MODEL_NAMES = ["demand_forecast_model_a", "demand_forecast_model_b"]


# ── PostgreSQL helper ──────────────────────────────────────────────────────────
def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


# ── Gold reader ────────────────────────────────────────────────────────────────
def _read_gold_for_targets(target_window_ends: list) -> pd.DataFrame:
    """
    Đọc Gold/aggregated cho danh sách window_end cụ thể.
    Chỉ lấy các cột cần thiết để tính imbalance.
    Raise exception nếu không kết nối được MinIO — caller xử lý fallback.
    """
    from deltalake import DeltaTable

    dt = DeltaTable(GOLD_AGG_PATH, storage_options=STORAGE_OPTS)

    # deltalake filter nhận string ISO hoặc timestamp — dùng string để an toàn
    we_strs = [
        t.isoformat() if hasattr(t, "isoformat") else str(t) for t in target_window_ends
    ]

    df = dt.to_pandas(
        columns=["zone_id", "window_end", "pickup_delay_mean", "requests_60m"],
        filters=[("window_end", "in", we_strs)],
    )
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df


def _imbalance_to_class(gold_df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Tính imbalance từ Gold và phân thành 6 class bằng quantile động.

    Dùng cùng công thức với feature_builder.compute_imbalance():
        imbalance = (pickup_delay_mean^1.2 × requests_60m) / zone_area_km2

    Thresholds tính từ dữ liệu trong batch này (thay vì dùng thresholds cứng
    từ training) để phản ánh phân phối thực tế và tránh concept drift.

    Returns:
        actual_df   : DataFrame (zone_id, window_end, actual_class)
        qs          : list 5 thresholds đã dùng — để log/debug
    """
    try:
        sys.path.insert(0, "/opt/ml")
        from feature_builder import ZONE_AREAS_KM2
    except ImportError:
        log.warning(
            "[GOLD] Không import được feature_builder.ZONE_AREAS_KM2 — area = 1.0"
        )
        ZONE_AREAS_KM2 = {}

    df = gold_df.copy()
    area = df["zone_id"].map(ZONE_AREAS_KM2).fillna(1.0)
    valid = df["pickup_delay_mean"].notna() & df["requests_60m"].notna()
    df["imbalance"] = np.where(
        valid,
        (df["pickup_delay_mean"].clip(lower=0) ** 1.2) * df["requests_60m"] / area,
        0.0,
    )

    qs = df["imbalance"].quantile([0.2, 0.4, 0.6, 0.8, 0.95]).tolist()
    df["actual_class"] = pd.cut(
        df["imbalance"],
        bins=[-np.inf] + qs + [np.inf],
        labels=[0, 1, 2, 3, 4, 5],
    ).astype(int)

    return df[["zone_id", "window_end", "actual_class"]], qs


# ── Live accuracy ──────────────────────────────────────────────────────────────
def _compute_live_accuracy() -> tuple:
    """
    Tính live accuracy cho Production và Shadow model.

    Returns:
        (prod_acc, shadow_acc, n_samples)
        Trả về (None, None, 0) nếu không đủ điều kiện.
    """
    # ── 1. Query predictions có shadow từ PostgreSQL ───────────────────────────
    query = """
        SELECT
            zone_id,
            window_end,
            predicted_class,
            shadow_predicted_class
        FROM predictions_monitoring
        WHERE shadow_predicted_class IS NOT NULL
        ORDER BY window_end DESC
        LIMIT 50000
    """
    try:
        with _pg_conn() as conn:
            pred_df = pd.read_sql(query, conn, parse_dates=["window_end"])
    except Exception as exc:
        log.error(f"[LIVE_ACC] Lỗi query PostgreSQL: {exc}")
        return None, None, 0

    if pred_df.empty:
        log.info("[LIVE_ACC] Chưa có shadow predictions trong DB")
        return None, None, 0

    pred_df["window_end"] = pd.to_datetime(pred_df["window_end"], utc=True)

    # Giới hạn EVAL_HOURS gần nhất tính theo event time
    max_we = pred_df["window_end"].max()
    cutoff = max_we - pd.Timedelta(hours=EVAL_HOURS)
    pred_df = pred_df[pred_df["window_end"] >= cutoff].copy()

    n_raw = len(pred_df)
    if n_raw < SHADOW_MIN_SAMPLES:
        log.info(
            f"[LIVE_ACC] {n_raw} samples trong {EVAL_HOURS}h qua "
            f"(cần ≥ {SHADOW_MIN_SAMPLES}) — chưa đủ"
        )
        return None, None, n_raw

    # ── 2. target_we = window_end + 1h (slot model đang dự báo) ──────────────
    shift = pd.Timedelta(minutes=LABEL_SHIFT * SLOT_MINUTES)
    pred_df["target_we"] = pred_df["window_end"] + shift
    target_wes = pred_df["target_we"].dt.to_pydatetime().tolist()

    # ── 3. Đọc Gold tại target_we ─────────────────────────────────────────────
    try:
        gold_raw = _read_gold_for_targets(target_wes)
    except Exception as exc:
        log.warning(f"[LIVE_ACC] Không đọc được Gold: {exc} — fallback val_accuracy")
        return None, None, 0

    if gold_raw.empty:
        log.warning(
            "[LIVE_ACC] Gold trả về empty cho target slots — fallback val_accuracy"
        )
        return None, None, 0

    # ── 4. Tính actual_class từ imbalance ─────────────────────────────────────
    actual_df, qs = _imbalance_to_class(gold_raw)
    log.info(f"[LIVE_ACC] Quantile thresholds từ Gold: {[round(q, 3) for q in qs]}")

    # ── 5. Join predictions ↔ actual labels ───────────────────────────────────
    actual_df = actual_df.rename(columns={"window_end": "target_we"})
    merged = pred_df.merge(actual_df, on=["zone_id", "target_we"], how="inner")

    n = len(merged)
    if n < SHADOW_MIN_SAMPLES:
        log.info(
            f"[LIVE_ACC] Sau join còn {n} samples "
            f"(Gold có thể thiếu một số target slots)"
        )
        return None, None, n

    # ── 6. Accuracy ───────────────────────────────────────────────────────────
    prod_acc = (merged["predicted_class"] == merged["actual_class"]).mean()
    shadow_acc = (merged["shadow_predicted_class"] == merged["actual_class"]).mean()

    log.info(
        f"[LIVE_ACC] n={n} | prod={prod_acc:.4f} | shadow={shadow_acc:.4f} "
        f"| Δ={shadow_acc - prod_acc:+.4f}"
    )
    return float(prod_acc), float(shadow_acc), n


# ── MLflow helpers ─────────────────────────────────────────────────────────────
def _get_staging_info(client, model: str):
    """(version_str, created_utc) của Staging, hoặc (None, None) nếu không có."""
    try:
        vs = client.get_latest_versions(model, stages=["Staging"])
        if not vs:
            return None, None
        v = vs[0]
        created = datetime.fromtimestamp(v.creation_timestamp / 1000, tz=timezone.utc)
        return v.version, created
    except Exception:
        return None, None


def _get_val_accuracy(client, model: str, stage: str) -> float | None:
    """val_accuracy tĩnh từ MLflow run — dùng khi live accuracy không có."""
    try:
        vs = client.get_latest_versions(model, stages=[stage])
        if not vs:
            return None
        return float(client.get_run(vs[0].run_id).data.metrics.get("val_accuracy", 0.0))
    except Exception:
        return None


# ── Task chính ─────────────────────────────────────────────────────────────────
def task_check_shadow_promotion(**ctx):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = mlflow.tracking.MlflowClient()
    now = datetime.now(tz=timezone.utc)

    # Tính live accuracy một lần — dùng chung cho tất cả models
    # (predictions_monitoring không phân biệt model_a / model_b theo cột riêng)
    prod_live_acc, shadow_live_acc, n_samples = _compute_live_accuracy()
    use_live = prod_live_acc is not None and shadow_live_acc is not None

    if use_live:
        log.info(
            f"[SHADOW] Dùng LIVE accuracy | n={n_samples} | "
            f"prod={prod_live_acc:.4f} | shadow={shadow_live_acc:.4f}"
        )
    else:
        log.warning(
            f"[SHADOW] Live accuracy không khả dụng (n={n_samples}) — "
            "fallback sang val_accuracy từ MLflow"
        )

    for model in MODEL_NAMES:
        staging_ver, created = _get_staging_info(client, model)
        if staging_ver is None:
            log.info(f"[SHADOW] {model}: không có Staging version, skip")
            continue

        days = (now - created).days

        # Chọn accuracy source
        if use_live:
            s_acc = shadow_live_acc
            p_acc = prod_live_acc
            acc_source = "live"
        else:
            s_acc = _get_val_accuracy(client, model, "Staging")
            p_acc = _get_val_accuracy(client, model, "Production") or 0.0
            acc_source = "val_accuracy(fallback)"
            if s_acc is None:
                log.warning(f"[SHADOW] {model}: không lấy được accuracy nào, skip")
                continue

        log.info(
            f"[SHADOW] {model} v{staging_ver} | age={days}d | "
            f"[{acc_source}] shadow={s_acc:.4f} prod={p_acc:.4f}"
        )

        # ── Quá hạn → Archive ─────────────────────────────────────────────────
        if days >= SHADOW_MAX_DAYS:
            client.transition_model_version_stage(
                name=model, version=staging_ver, stage="Archived"
            )
            log.warning(
                f"[SHADOW] {model} v{staging_ver} → Archived "
                f"(quá {SHADOW_MAX_DAYS}d | {acc_source}: {s_acc:.4f})"
            )
            continue

        # ── Chưa đủ ngày ──────────────────────────────────────────────────────
        if days < SHADOW_EVAL_DAYS:
            log.info(
                f"[SHADOW] {model}: cần thêm {SHADOW_EVAL_DAYS - days}d "
                f"nữa để đủ {SHADOW_EVAL_DAYS}d eval"
            )
            continue

        # ── Đủ điều kiện promote? ─────────────────────────────────────────────
        threshold = p_acc * SHADOW_ACC_RATIO
        if s_acc >= threshold:
            # Bảo vệ thêm: với live accuracy, phải đủ sample size
            if use_live and n_samples < SHADOW_MIN_SAMPLES:
                log.warning(
                    f"[SHADOW] {model}: live acc đủ ({s_acc:.4f} ≥ {threshold:.4f}) "
                    f"nhưng n={n_samples} < {SHADOW_MIN_SAMPLES} — bỏ qua"
                )
                continue

            client.transition_model_version_stage(
                name=model,
                version=staging_ver,
                stage="Production",
                archive_existing_versions=True,
            )
            log.info(
                f"[SHADOW] {model} v{staging_ver} → Production ✓ "
                f"[{acc_source}: {s_acc:.4f} ≥ {threshold:.4f}]"
            )
        else:
            log.info(
                f"[SHADOW] {model}: chưa đủ "
                f"[{acc_source}: {s_acc:.4f} < {threshold:.4f}]"
            )


# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="pipeline_monitoring",
    schedule_interval="5 * * * *",
    start_date=datetime(2026, 2, 1),
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
