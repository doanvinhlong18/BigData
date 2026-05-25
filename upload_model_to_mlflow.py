"""
upload_model_to_mlflow.py
─────────────────────────
Chạy trên máy local (Windows) sau khi train_lgb.py xong.
Upload file .txt của LightGBM lên MLflow server trên máy master,
register và set stage Production — predict_service sẽ tự load.

CÁCH DÙNG:
    # Cài dependencies (1 lần)
    pip install mlflow lightgbm boto3

    # Chạy (docker compose phải đang up trên master)
    python upload_model_to_mlflow.py

CONFIG:
    Script tự đọc .env ở thư mục project. Có thể override bằng biến môi trường.
"""

import os
import sys
import logging

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
from mlflow.tracking import MlflowClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("upload_model")

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


def _load_dotenv(path: str = os.path.join(REPO_ROOT, ".env")) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()

# ── CONFIG — lấy từ .env / environment ────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MINIO_ENDPOINT = (
    os.getenv("MLFLOW_S3_ENDPOINT_URL")
    or os.getenv("MINIO_ENDPOINT_EXTERNAL")
    or "http://localhost:9000"
)
OUTPUT_DIR = (
    os.getenv("MODEL_OUTPUT_DIR")
    or os.getenv("OUTPUT_DIR")
    or os.path.join(REPO_ROOT, "datasets", "lgb_output")
)
# ──────────────────────────────────────────────────────────────────────────────

MODEL_A_FILE = os.getenv(
    "MODEL_A_PATH", os.path.join(OUTPUT_DIR, "lgb_final_model.txt")
)
# Model B = model KHÔNG dùng weather features (train với NO_WEATHER_FEATURE_COLS).
# PHẢI là file khác model_a — dùng cùng file sẽ gây mismatch features khi
# predict_service fallback sang model_b với NO_WEATHER_FEATURE_COLS.
# Cách train: chạy train_lgb.py với --no-weather, output vào lgb_model_b.txt.
# FIX: không dùng MODEL_A_FILE làm default — bắt buộc set MODEL_B_PATH.
MODEL_B_FILE = os.getenv(
    "MODEL_B_PATH", os.path.join(OUTPUT_DIR, "lgb_model_b.txt")
)

EXPERIMENT_NAME = "demand_forecast"

# MinIO credentials — phải khớp .env
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def _check_file(path: str, label: str):
    if not os.path.exists(path):
        log.error(f"[{label}] File không tồn tại: {path}")
        log.error("Chạy train_lgb.py trước, sau đó chạy lại script này.")
        sys.exit(1)
    size_mb = os.path.getsize(path) / 1024 / 1024
    log.info(f"[{label}] Found: {path}  ({size_mb:.1f} MB)")


def _register_model(
    booster: lgb.Booster,
    model_name: str,
    val_accuracy: float,
    params: dict,
    feature_names: list[str],
):
    """
    Log model vào MLflow experiment, register vào Model Registry,
    và set stage Production (archive phiên bản cũ).
    """
    with mlflow.start_run(run_name=f"upload_{model_name}") as run:
        # Log params để tracking
        mlflow.log_params(
            {
                "num_trees": booster.num_trees(),
                "num_features": booster.num_feature(),
                "best_iteration": booster.best_iteration,
                **{k: v for k, v in params.items() if k not in ("verbose",)},
            }
        )
        mlflow.log_metrics(
            {
                "val_accuracy": val_accuracy,
                "best_logloss": booster.best_score.get("valid", {}).get(
                    "multi_logloss", -1
                ),
            }
        )

        # Log feature list để retrain_dag tham chiếu
        mlflow.log_dict({"features": feature_names}, "feature_names.json")

        # Log model
        mlflow.lightgbm.log_model(
            lgb_model=booster,
            artifact_path="model",
            registered_model_name=model_name,
        )
        run_id = run.info.run_id

    log.info(f"[{model_name}] Logged run_id={run_id}")

    # Lấy version vừa tạo và promote lên Production
    client = MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    latest_ver = max(int(v.version) for v in versions)

    client.transition_model_version_stage(
        name=model_name,
        version=str(latest_ver),
        stage="Production",
        archive_existing_versions=True,  # tự archive version cũ
    )
    log.info(f"[{model_name}] version {latest_ver} → Production ✓")
    return latest_ver


def main():
    log.info(f"MLflow URI : {MLFLOW_TRACKING_URI}")
    log.info(f"MinIO      : {MINIO_ENDPOINT}")

    # ── Validate MODEL_B_FILE bắt buộc phải khác MODEL_A_FILE ─────────────────
    # Model B phải được train với NO_WEATHER_FEATURE_COLS (không có weather leads).
    # Nếu dùng cùng file: predict_service sẽ gọi model_b.predict_proba(x) với
    # NO_WEATHER_FEATURE_COLS (34 features) nhưng model thực ra có 46 features
    # → LightGBM raise ValueError hoặc cho kết quả hoàn toàn sai.
    if not MODEL_B_FILE:
        log.error(
            "[FATAL] MODEL_B_PATH chưa được set.\n"
            "  Model B phải train riêng với NO_WEATHER_FEATURE_COLS.\n"
            "  Ví dụ: set MODEL_B_PATH=C:\\...\\lgb_model_b.txt  (Windows)\n"
            "         export MODEL_B_PATH=/path/to/lgb_model_b.txt  (Linux)\n"
            "  Sau đó chạy lại upload_model_to_mlflow.py."
        )
        sys.exit(1)

    if os.path.abspath(MODEL_A_FILE) == os.path.abspath(MODEL_B_FILE):
        log.error(
            "[FATAL] MODEL_A_FILE và MODEL_B_FILE đang trỏ cùng một file.\n"
            f"  MODEL_A_FILE = {MODEL_A_FILE}\n"
            f"  MODEL_B_FILE = {MODEL_B_FILE}\n"
            "  Hãy train model_b riêng (không dùng weather features) và set MODEL_B_PATH."
        )
        sys.exit(1)

    # Validate số features khớp với feature_builder constants
    # model_a: ALL_FEATURE_COLS (~46), model_b: NO_WEATHER_FEATURE_COLS (~34)
    try:
        from feature_builder import ALL_FEATURE_COLS, NO_WEATHER_FEATURE_COLS

        _EXPECTED_A = len(ALL_FEATURE_COLS)
        _EXPECTED_B = len(NO_WEATHER_FEATURE_COLS)
    except ImportError:
        _EXPECTED_A = None
        _EXPECTED_B = None
        log.warning(
            "[WARN] Không import được feature_builder — bỏ qua kiểm tra feature count."
        )

    # Kiểm tra file tồn tại trước khi connect
    _check_file(MODEL_A_FILE, "model_a")
    _check_file(MODEL_B_FILE, "model_b")

    # Connect MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        mlflow.set_experiment(EXPERIMENT_NAME)
    except Exception as e:
        log.error(f"Không kết nối được MLflow tại {MLFLOW_TRACKING_URI}: {e}")
        log.error("Đảm bảo docker compose đang chạy trên máy master.")
        sys.exit(1)

    # Load model từ file
    log.info("[model_a] Loading LightGBM model from file...")
    booster_a = lgb.Booster(model_file=MODEL_A_FILE)
    feature_names_a = booster_a.feature_name()
    params_a = booster_a.params

    log.info("[model_b] Loading LightGBM model from file...")
    booster_b = lgb.Booster(model_file=MODEL_B_FILE)
    feature_names_b = booster_b.feature_name()

    # ── Validate feature count sau khi load ───────────────────────────────────
    n_a, n_b = booster_a.num_feature(), booster_b.num_feature()
    log.info(f"[model_a] num_features = {n_a}")
    log.info(f"[model_b] num_features = {n_b}")

    if n_a == n_b:
        log.error(
            f"[FATAL] model_a và model_b có cùng số features ({n_a}).\n"
            "  model_b phải ít features hơn (không có weather leads).\n"
            "  Kiểm tra lại file MODEL_B_PATH."
        )
        sys.exit(1)

    if _EXPECTED_A is not None:
        if n_a != _EXPECTED_A:
            log.warning(
                f"[WARN] model_a có {n_a} features, feature_builder kỳ vọng {_EXPECTED_A}. "
                "Kiểm tra feature_builder.ALL_FEATURE_COLS."
            )
        if n_b != _EXPECTED_B:
            log.warning(
                f"[WARN] model_b có {n_b} features, feature_builder kỳ vọng {_EXPECTED_B}. "
                "Kiểm tra feature_builder.NO_WEATHER_FEATURE_COLS."
            )

    # val_accuracy = 1 - best_error (nếu có trong best_score)
    def get_val_accuracy(booster: lgb.Booster) -> float:
        err = booster.best_score.get("valid", {}).get("multi_error", None)
        return round(1.0 - err, 6) if err is not None else 0.0

    val_acc_a = get_val_accuracy(booster_a)
    val_acc_b = get_val_accuracy(booster_b)
    log.info(f"[model_a] val_accuracy = {val_acc_a:.4f}")
    log.info(f"[model_b] val_accuracy = {val_acc_b:.4f}")

    # Register model_a (dùng weather features)
    _register_model(
        booster=booster_a,
        model_name="demand_forecast_model_a",
        val_accuracy=val_acc_a,
        params=params_a,
        feature_names=feature_names_a,
    )

    # Register model_b (không dùng weather — fallback khi thiếu weather data)
    _register_model(
        booster=booster_b,
        model_name="demand_forecast_model_b",
        val_accuracy=val_acc_b,
        params=booster_b.params,
        feature_names=feature_names_b,
    )

    log.info("=" * 55)
    log.info("Upload hoàn tất. predict_service sẽ tự load model trong")
    log.info(f"tối đa {300}s (MODEL_RELOAD_INTERVAL_S=300).")
    log.info(f"Kiểm tra tại: {MLFLOW_TRACKING_URI}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
