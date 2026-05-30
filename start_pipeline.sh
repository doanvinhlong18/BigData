#!/bin/bash
# =============================================================================
# start_pipeline.sh
# Khởi động toàn bộ pipeline theo đúng thứ tự.
#
# CÁCH DÙNG:
#   chmod +x start_pipeline.sh
#   ./start_pipeline.sh
#
# YÊU CẦU TRƯỚC KHI CHẠY:
#   1. Đã chạy train_lgb.py trên máy local và có file lgb_final_model.txt
#   2. Dataset đã đặt đúng DATASET_PATH trong .env
#   3. Sửa MASTER_IP trong .env cho khớp máy thực tế
#
# THỨ TỰ THỰC HIỆN (script tự hướng dẫn):
#   [Script]  STEP 1 — Kiểm tra prerequisites
#   [Script]  STEP 2 — docker compose up --build
#   [Script]  STEP 3 — Chờ master services healthy và worker endpoints sẵn sàng
#   [Bạn]    STEP 4 — Chạy upload_model_to_mlflow.py trên máy Windows
#                     (script dừng lại, in hướng dẫn, chờ xác nhận)
#   [Script]  STEP 5 — Submit 5 Spark streaming jobs
# =============================================================================

set -euo pipefail

COMPOSE_FILE="docker-compose.master.yml"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; }
step()    { echo -e "\n${BOLD}$*${NC}"; }

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
    error ".env không tồn tại. Copy .env.example → .env và điền thông tin."
    exit 1
fi

load_env() {
    local raw line key value
    while IFS= read -r raw || [ -n "$raw" ]; do
        line="${raw%$'\r'}"
        [ -z "$line" ] && continue
        case "$line" in \#*) continue ;; esac
        [[ "$line" == *"="* ]] || continue
        key="${line%%=*}"
        value="${line#*=}"
        key="$(printf '%s' "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
        [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        export "$key=$value"
    done < ".env"
}

load_env

: "${MASTER_IP:?MASTER_IP chưa được cấu hình trong .env}"
: "${WORKER_IP:?WORKER_IP chưa được cấu hình trong .env}"
: "${DATASET_PATH:?DATASET_PATH chưa được cấu hình trong .env}"

MLFLOW_URL="http://${MASTER_IP}:5000"
SPARK_URL="http://${MASTER_IP}:8080"
MINIO_API_URL="${MINIO_ENDPOINT_EXTERNAL:-http://${WORKER_IP}:9000}"
MINIO_URL="http://${WORKER_IP}:9001"
GRAFANA_URL="http://${MASTER_IP}:3000"

# =============================================================================
# STEP 1 — Kiểm tra prerequisites
# =============================================================================
step "STEP 1/5 — Kiểm tra prerequisites"

if ! docker info &>/dev/null; then
    error "Docker daemon chưa chạy."
    exit 1
fi
success "Docker OK"

if [ ! -d "${DATASET_PATH}" ]; then
    error "DATASET_PATH=${DATASET_PATH} không tồn tại."
    error "Tạo thư mục và đặt dữ liệu vào đúng cấu trúc (xem README)."
    exit 1
fi
success "Dataset path OK: ${DATASET_PATH}"

# =============================================================================
# STEP 2 — Build và khởi động tất cả services
# =============================================================================
step "STEP 2/5 — Build images và khởi động services"
info "docker compose up --build -d ..."

docker compose -f "$COMPOSE_FILE" up --build -d 2>&1 | tee "$LOG_DIR/compose_up.log"

# =============================================================================
# STEP 3 — Chờ services healthy
# =============================================================================
step "STEP 3/5 — Chờ master services và worker endpoints"

wait_healthy() {
    local container="$1"
    local timeout="${2:-120}"
    local elapsed=0
    printf "  Waiting %-25s" "$container..."
    while [ $elapsed -lt $timeout ]; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            echo -e " ${GREEN}healthy${NC}"
            return 0
        fi
        if [ "$status" = "unhealthy" ]; then
            echo -e " ${RED}UNHEALTHY${NC}"
            warn "Xem logs: docker logs $container --tail 30"
            return 1
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        printf "."
    done
    echo -e " ${RED}TIMEOUT${NC}"
    return 1
}

tcp_probe() {
    local host="$1"
    local port="$2"
    python - "$host" "$port" <<'PY'
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
try:
    with socket.create_connection((host, port), timeout=3):
        pass
except OSError:
    sys.exit(1)
PY
}

wait_tcp() {
    local label="$1"
    local host="$2"
    local port="$3"
    local timeout="${4:-120}"
    local elapsed=0
    printf "  Waiting %-25s" "$label..."
    while [ $elapsed -lt $timeout ]; do
        if tcp_probe "$host" "$port"; then
            echo -e " ${GREEN}${host}:${port} OK${NC}"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        printf "."
    done
    echo -e " ${RED}TIMEOUT ${host}:${port}${NC}"
    return 1
}

wait_http() {
    local label="$1"
    local url="$2"
    local timeout="${3:-120}"
    local elapsed=0
    printf "  Waiting %-25s" "$label..."
    while [ $elapsed -lt $timeout ]; do
        if curl -fsS --connect-timeout 3 --max-time 5 "$url" >/dev/null 2>&1; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        printf "."
    done
    echo -e " ${RED}TIMEOUT ${url}${NC}"
    return 1
}

# Các service này chạy trên worker, không phải container trong compose master.
wait_tcp  "zookeeper(worker)" "$WORKER_IP" 2181 60
wait_tcp  "kafka(worker)"     "$WORKER_IP" 29092 120
wait_http "minio(worker)"     "${MINIO_API_URL}/minio/health/live" 60

wait_healthy "postgres"     60
wait_healthy "spark-master" 60
wait_healthy "mlflow"       180

# predict-service: chỉ check đang chạy, model chưa có nên sẽ log fallback — bình thường
sleep 5
if docker ps --format '{{.Names}}' | grep -q "predict-service"; then
    success "predict-service running (đang dùng fallback, chờ model upload xong)"
else
    warn "predict-service chưa thấy — kiểm tra: docker logs predict-service"
fi

# =============================================================================
# STEP 4 — Upload model lên MLflow (thao tác thủ công trên máy Windows)
# =============================================================================
step "STEP 4/5 — Upload model lên MLflow"

echo ""
echo -e "${YELLOW}${BOLD}  ╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}${BOLD}  ║           THAO TÁC THỦ CÔNG — ĐỌC KỸ TRƯỚC KHI TIẾP    ║${NC}"
echo -e "${YELLOW}${BOLD}  ╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  MLflow đang chạy tại: ${BOLD}${MLFLOW_URL}${NC}"
echo ""
echo -e "  Mở terminal trên ${BOLD}máy Windows${NC} và chạy:"
echo ""
echo -e "    ${CYAN}cd <thư mục dự án>${NC}"
echo -e "    ${CYAN}pip install mlflow lightgbm boto3   ${NC}${YELLOW}# bỏ qua nếu đã cài${NC}"
echo -e "    ${CYAN}python upload_model_to_mlflow.py${NC}"
echo ""
echo -e "  Script sẽ tự động:"
echo -e "    • Load ${BOLD}lgb_final_model.txt${NC} từ OUTPUT_DIR"
echo -e "    • Register ${BOLD}demand_forecast_model_a${NC} và ${BOLD}demand_forecast_model_b${NC}"
echo -e "    • Set stage ${BOLD}Production${NC}"
echo ""

# Vòng lặp kiểm tra — tự động tiếp tục khi model đã có, không cần nhấn Enter
check_model() {
    local model_name="$1"
    local result
    result=$(curl -s "${MLFLOW_URL}/api/2.0/mlflow/model-versions/search?filter=name%3D%27${model_name}%27%20AND%20current_stage%3D%27Production%27" 2>/dev/null || echo "")
    echo "$result" | grep -q '"version"'
}

info "Script đang tự kiểm tra MLflow mỗi 15 giây..."
info "Hoặc nhấn ${BOLD}Enter${NC} để bỏ qua và dùng fallback (predict class 0)."
echo ""

# Chạy check trong background, đồng thời chờ input người dùng
SKIP_MODEL=false
while true; do
    # Non-blocking read với timeout 15 giây
    if read -t 15 -r -p "  [Chờ model...] Nhấn Enter để bỏ qua → " INPUT 2>/dev/null; then
        warn "Bỏ qua upload model. predict-service dùng fallback."
        warn "Upload sau bằng: python upload_model_to_mlflow.py"
        warn "predict-service tự reload model sau tối đa 5 phút."
        SKIP_MODEL=true
        break
    fi

    # Timeout → kiểm tra model
    MODEL_A_OK=false
    MODEL_B_OK=false
    check_model "demand_forecast_model_a" && MODEL_A_OK=true || true
    check_model "demand_forecast_model_b" && MODEL_B_OK=true || true

    if [ "$MODEL_A_OK" = true ] && [ "$MODEL_B_OK" = true ]; then
        echo ""
        success "demand_forecast_model_a → Production ✓"
        success "demand_forecast_model_b → Production ✓"
        success "Model sẵn sàng, tiếp tục pipeline!"
        break
    else
        # In trạng thái từng model
        [ "$MODEL_A_OK" = true ] && echo -e "    model_a: ${GREEN}OK${NC}" || echo -e "    model_a: ${YELLOW}chờ...${NC}"
        [ "$MODEL_B_OK" = true ] && echo -e "    model_b: ${GREEN}OK${NC}" || echo -e "    model_b: ${YELLOW}chờ...${NC}"
    fi
done

# =============================================================================
# STEP 5 — Submit Spark streaming jobs
# =============================================================================
step "STEP 5/5 — Submit Spark streaming jobs"

info "Chờ spark-worker đăng ký với master..."
sleep 10

info "Submitting 5 Spark streaming jobs..."
docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh \
    2>&1 | tee "$LOG_DIR/spark_submit.log"

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    success "Tất cả Spark jobs đã submit"
else
    error "Submit Spark jobs thất bại. Xem $LOG_DIR/spark_submit.log"
    exit 1
fi

# =============================================================================
# DONE
# =============================================================================
echo ""
echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}${BOLD} Pipeline đang chạy!${NC}"
echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${BOLD}Spark UI${NC}       → ${SPARK_URL}"
echo -e "  ${BOLD}MLflow${NC}         → ${MLFLOW_URL}"
echo -e "  ${BOLD}MinIO${NC}          → ${MINIO_URL}   (minioadmin / minioadmin)"
echo -e "  ${BOLD}Grafana${NC}        → ${GRAFANA_URL}   (admin / admin)"
echo ""
echo -e "  ${BOLD}Logs${NC}           → ./logs/"
echo -e "  ${BOLD}Dừng pipeline${NC} → docker compose -f ${COMPOSE_FILE} down"
echo ""

if [ "$SKIP_MODEL" = true ]; then
    warn "predict-service đang dùng fallback. Upload model khi sẵn sàng:"
    warn "  python upload_model_to_mlflow.py"
    warn "predict-service tự reload sau tối đa 5 phút."
else
    info "predict-service sẽ bắt đầu predict sau khi gold/aggregated có data (~5-10 phút)"
fi
info "Theo dõi predict: docker logs -f predict-service"
