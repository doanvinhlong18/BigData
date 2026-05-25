#!/usr/bin/env bash
# =============================================================================
# start_pipeline_with_mlflow_upload.sh
#
# One-file pipeline runner:
#   1. Start master services with docker compose.
#   2. Wait for core services to become healthy.
#   3. Upload LightGBM text models from datasets/lgb_output to MLflow.
#   4. Register/promote:
#        lgb_final_model.txt -> demand_forecast_model_a/Production
#        lgb_model_b.txt     -> demand_forecast_model_b/Production
#   5. Restart predict-service so it reloads models.
#   6. Submit 5 Spark streaming jobs.
#
# Usage:
#   bash start_pipeline_with_mlflow_upload.sh
#
# Optional:
#   MODEL_OUTPUT_DIR=./datasets/lgb_output bash start_pipeline_with_mlflow_upload.sh
#
# Notes:
#   - This script does not use upload_model_to_mlflow.py because that script logs
#     LightGBM Booster directly. predict_service calls predict_proba(), so this
#     script wraps the Booster in LGBMClassifier before logging to MLflow.
# =============================================================================

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.master.yml}"
LOG_DIR="${LOG_DIR:-./logs}"
MODEL_OUTPUT_DIR="${MODEL_OUTPUT_DIR:-./datasets/lgb_output}"

mkdir -p "$LOG_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; }
step()    { echo -e "\n${BOLD}$*${NC}"; }

load_env() {
  if [ ! -f ".env" ]; then
    error ".env not found. Create it first."
    exit 1
  fi
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
}

require_file() {
  local path="$1"
  local label="$2"
  if [ ! -f "$path" ]; then
    error "$label not found: $path"
    exit 1
  fi
}

wait_healthy() {
  local container="$1"
  local timeout="${2:-120}"
  local elapsed=0

  printf "  Waiting %-25s" "$container..."
  while [ "$elapsed" -lt "$timeout" ]; do
    local status
    status="$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")"
    if [ "$status" = "healthy" ]; then
      echo -e " ${GREEN}healthy${NC}"
      return 0
    fi
    if [ "$status" = "unhealthy" ]; then
      echo -e " ${RED}UNHEALTHY${NC}"
      warn "Check logs: docker logs $container --tail 50"
      return 1
    fi
    sleep 3
    elapsed=$((elapsed + 3))
    printf "."
  done

  echo -e " ${RED}TIMEOUT${NC}"
  warn "Check logs: docker logs $container --tail 50"
  return 1
}

container_running() {
  docker ps --format '{{.Names}}' | grep -qx "$1"
}

stop_local_worker_stack_conflicts() {
  # This is a master-side runner. If docker-compose.worker.yml was started on
  # the same host by accident, it occupies ports that the master needs:
  #   spark-worker-*  -> 7337-7360, 8081, 8085
  #   cadvisor-worker -> 8090
  #
  # In the intended 2-machine setup the worker runs on another machine, so these
  # containers should not exist locally on the master.
  local workers
  workers="$(docker ps -a --format '{{.Names}}' | grep -E '^(spark-worker-|cadvisor-worker$)' || true)"
  if [ -z "$workers" ]; then
    return 0
  fi

  if [ "${KEEP_LOCAL_WORKER_STACK:-0}" = "1" ]; then
    warn "Local worker-side containers are running and may conflict:"
    echo "$workers" | sed 's/^/  - /'
    warn "KEEP_LOCAL_WORKER_STACK=1 is set, so the script will not stop them."
    return 0
  fi

  warn "Worker-side containers are running on this master host:"
  echo "$workers" | sed 's/^/  - /'
  warn "Stopping them to free master ports 7337-7360 and 8090..."

  # Do not call `docker compose -f docker-compose.worker.yml down` here.
  # The master and worker compose files share the default project name when run
  # from this directory, so compose may remove master services as "orphans".
  echo "$workers" | xargs -r docker rm -f
}

docker_volume_path() {
  local path="$1"
  local abs
  abs="$(cd "$path" && pwd)"
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$abs"
  else
    printf '%s' "$abs"
  fi
}

upload_models_to_mlflow() {
  local model_dir_abs
  model_dir_abs="$(docker_volume_path "$MODEL_OUTPUT_DIR")"

  info "Uploading LightGBM models to MLflow from: $MODEL_OUTPUT_DIR"
  info "Using Docker network: bigdata-net"

  if ! docker image inspect bigdata-predict-service:latest >/dev/null 2>&1; then
    info "Image bigdata-predict-service:latest not found; building predict-service..."
    docker compose -f "$COMPOSE_FILE" build predict-service
  fi

  # MSYS_NO_PATHCONV avoids Git Bash rewriting /models inside docker arguments.
  MSYS_NO_PATHCONV=1 docker run --rm -i \
    --network bigdata-net \
    -v "${model_dir_abs}:/models:ro" \
    -e MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI_INTERNAL:-http://mlflow:5000}" \
    -e MLFLOW_S3_ENDPOINT_URL="${MINIO_ENDPOINT:-http://minio:9000}" \
    -e AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY:-minioadmin}" \
    -e AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY:-minioadmin}" \
    -e GIT_PYTHON_REFRESH="quiet" \
    -e PYTHONWARNINGS="ignore" \
    bigdata-predict-service:latest \
    python - <<'PY'
import os
import json
import logging

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
from lightgbm import LGBMClassifier
from mlflow.tracking import MlflowClient
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("upload_models")

MODEL_DIR = "/models"
EXPERIMENT_NAME = "demand_forecast"
STAGE = "Production"

MODELS = [
    ("lgb_final_model.txt", "demand_forecast_model_a"),
    ("lgb_model_b.txt", "demand_forecast_model_b"),
]


def read_header(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("Tree="):
                break
            if "=" in line:
                key, value = line.split("=", 1)
                out[key] = value
    return out


def read_num_class(path: str) -> int:
    header = read_header(path)
    if "num_class" in header:
        return int(header["num_class"])
    objective = header.get("objective", "")
    marker = "num_class:"
    if marker in objective:
        return int(objective.split(marker, 1)[1].split()[0])
    return 6


def as_lgbm_classifier(path: str) -> tuple[LGBMClassifier, list[str], int]:
    booster = lgb.Booster(model_file=path)
    feature_names = booster.feature_name()
    n_features = booster.num_feature()
    num_class = read_num_class(path)

    clf = LGBMClassifier(
        objective="multiclass",
        num_class=num_class,
        n_estimators=booster.current_iteration(),
    )

    # Make the sklearn wrapper look fitted. This preserves predict_proba(),
    # which predict_service expects from mlflow.lightgbm.load_model().
    clf._Booster = booster
    clf._objective = "multiclass"
    clf._n_features = n_features
    clf._n_features_in = n_features
    clf._classes = np.arange(num_class)
    clf._n_classes = num_class
    clf._le = LabelEncoder().fit(clf._classes)
    clf._best_iteration = booster.best_iteration or booster.current_iteration()
    clf._best_score = booster.best_score or {}
    clf._evals_result = {}
    clf.fitted_ = True

    probe = clf.predict_proba(np.zeros((1, n_features), dtype=np.float32))
    if probe.shape != (1, num_class):
        raise RuntimeError(f"predict_proba returned shape {probe.shape}, expected {(1, num_class)}")

    return clf, feature_names, num_class


def latest_version_for_run(client: MlflowClient, model_name: str, run_id: str):
    versions = client.search_model_versions(f"name = '{model_name}'")
    matches = [v for v in versions if v.run_id == run_id]
    if not matches:
        raise RuntimeError(f"No model version found for {model_name}, run_id={run_id}")
    return max(matches, key=lambda v: int(v.version))


def main() -> None:
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    log.info("MLflow URI: %s", tracking_uri)
    log.info("MinIO endpoint: %s", os.environ.get("MLFLOW_S3_ENDPOINT_URL"))

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()

    for filename, model_name in MODELS:
        path = os.path.join(MODEL_DIR, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(path)

        size_mb = os.path.getsize(path) / 1024 / 1024
        model, feature_names, num_class = as_lgbm_classifier(path)
        n_features = len(feature_names)

        log.info("[%s] file=%s size=%.1fMB features=%s classes=%s", model_name, filename, size_mb, n_features, num_class)

        with mlflow.start_run(run_name=f"bootstrap_{model_name}") as run:
            mlflow.log_params({
                "source_file": filename,
                "n_features": n_features,
                "num_class": num_class,
                "source_format": "lightgbm_text",
            })
            mlflow.log_metric("val_accuracy", 0.0)
            mlflow.set_tags({
                "bootstrap_from": "datasets/lgb_output",
                "serving_contract": "predict_proba",
            })
            mlflow.log_dict({"features": feature_names}, "feature_names.json")
            mlflow.log_text(json.dumps(feature_names), "feature_names.txt")
            mlflow.lightgbm.log_model(
                lgb_model=model,
                artifact_path="model",
                registered_model_name=model_name,
                input_example=np.zeros((1, n_features), dtype=np.float32),
            )

            version = latest_version_for_run(client, model_name, run.info.run_id)
            client.transition_model_version_stage(
                name=model_name,
                version=version.version,
                stage=STAGE,
                archive_existing_versions=True,
            )
            log.info("[%s] version %s -> %s", model_name, version.version, STAGE)

    log.info("DONE")


if __name__ == "__main__":
    main()
PY
}

check_registered_models() {
  info "Checking MLflow model registry..."
  MSYS_NO_PATHCONV=1 docker run --rm -i \
    --network bigdata-net \
    -e MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI_INTERNAL:-http://mlflow:5000}" \
    -e PYTHONWARNINGS="ignore" \
    bigdata-predict-service:latest \
    python - <<'PY'
import os
import mlflow

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
client = mlflow.tracking.MlflowClient()

required = ["demand_forecast_model_a", "demand_forecast_model_b"]
missing = []
for name in required:
    versions = client.get_latest_versions(name, stages=["Production"])
    if versions:
        print(f"OK: {name}/Production v{versions[0].version}")
    else:
        print(f"MISSING: {name}/Production")
        missing.append(name)

if missing:
    raise SystemExit(1)
PY
}

main() {
  load_env

  step "STEP 1/6 - Check prerequisites"
  if ! docker info >/dev/null 2>&1; then
    error "Docker daemon is not running."
    exit 1
  fi
  success "Docker OK"

  stop_local_worker_stack_conflicts

  require_file "${MODEL_OUTPUT_DIR}/lgb_final_model.txt" "Model A"
  require_file "${MODEL_OUTPUT_DIR}/lgb_model_b.txt" "Model B"
  success "Model files OK: ${MODEL_OUTPUT_DIR}"

  if [ ! -d "${DATASET_PATH:-./datasets}" ]; then
    error "DATASET_PATH=${DATASET_PATH:-./datasets} does not exist."
    exit 1
  fi
  success "Dataset path OK: ${DATASET_PATH:-./datasets}"

  step "STEP 2/6 - Start master services"
  docker compose -f "$COMPOSE_FILE" up --build -d 2>&1 | tee "$LOG_DIR/compose_up.log"

  step "STEP 3/6 - Wait for healthy services"
  wait_healthy "zookeeper" 60
  wait_healthy "kafka" 120
  wait_healthy "minio" 60
  wait_healthy "postgres" 60
  wait_healthy "spark-master" 60
  wait_healthy "mlflow" 180
  wait_healthy "airflow" 180

  if container_running "predict-service"; then
    success "predict-service running"
  else
    warn "predict-service is not running yet. Check: docker logs predict-service"
  fi

  step "STEP 4/6 - Upload models to MLflow"
  upload_models_to_mlflow 2>&1 | tee "$LOG_DIR/mlflow_model_upload.log"
  check_registered_models
  success "MLflow models are ready"

  if container_running "predict-service"; then
    step "STEP 5/6 - Restart predict-service"
    docker restart predict-service >/dev/null
    success "predict-service restarted"
  else
    step "STEP 5/6 - Skip predict-service restart"
    warn "predict-service container not found"
  fi

  step "STEP 6/6 - Submit Spark streaming jobs"
  info "Waiting a little for Spark workers to register..."
  sleep 10
  MSYS_NO_PATHCONV=1 docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh 2>&1 | tee "$LOG_DIR/spark_submit.log"

  success "Pipeline submitted"
  echo ""
  echo -e "${GREEN}${BOLD}Pipeline is running.${NC}"
  echo "Spark UI  : http://${MASTER_IP}:8080"
  echo "Airflow   : http://${MASTER_IP}:8888"
  echo "MLflow    : http://${MASTER_IP}:5000"
  echo "MinIO     : http://${MASTER_IP}:9001"
  echo "Grafana   : http://${MASTER_IP}:3000"
  echo "Logs      : ${LOG_DIR}"
  echo ""
  echo "Follow predict-service:"
  echo "  docker logs -f predict-service"
}

case "${1:-}" in
  --upload-models-only)
    load_env
    require_file "${MODEL_OUTPUT_DIR}/lgb_final_model.txt" "Model A"
    require_file "${MODEL_OUTPUT_DIR}/lgb_model_b.txt" "Model B"
    upload_models_to_mlflow
    check_registered_models
    exit 0
    ;;
  --check-models-only)
    load_env
    check_registered_models
    exit 0
    ;;
esac

main "$@"
