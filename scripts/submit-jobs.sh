#!/bin/bash
# scripts/submit-jobs.sh
# Run inside the spark-master container:
#   docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh

# ── Paths ────────────────────────────────────────────────────────────────────
SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
JOBS_DIR="/opt/spark/app/spark/jobs"
# s3_check.py: thay cho 'hadoop fs -test -e' (hadoop binary không có trong image)
S3CHECK="python3 /opt/spark/app/scripts/s3_check.py"

# ── Resolve env vars ──────────────────────────────────────────────────────────
DRIVER_HOST="${MASTER_IP:-spark-master}"
MINIO_EP="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_AK="${AWS_ACCESS_KEY_ID:-minioadmin}"
MINIO_SK="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
KAFKA_BS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
KAFKA_HOST="${KAFKA_BS%%:*}"
KAFKA_PORT="${KAFKA_BS##*:}"
KAFKA_BS_EXECUTOR="${KAFKA_BOOTSTRAP_SERVERS_EXECUTOR:-${KAFKA_BS}}"
CHECKPOINT_BASE="${STREAMING_CHECKPOINT_BASE:-s3a://checkpoints}"
STATEFUL_CHECKPOINT_BASE_EFFECTIVE="${STATEFUL_CHECKPOINT_BASE:-${CHECKPOINT_BASE}}"
STATEFUL_STREAM_MASTER="${STATEFUL_STREAM_MASTER:-${SPARK_MASTER}}"
LAUNCHER_POLL_INTERVAL_S="${LAUNCHER_POLL_INTERVAL_S:-30}"

min_driver_memory() {
  local value="${1:-512m}"
  case "${value}" in
    *m|*M)
      local mb="${value%[mM]}"
      if [[ "${mb}" =~ ^[0-9]+$ ]] && [ "${mb}" -lt 512 ]; then
        echo "512m"
      else
        echo "${value}"
      fi
      ;;
    "")
      echo "512m"
      ;;
    *)
      echo "${value}"
      ;;
  esac
}

JOB1_DRIVER_MEMORY_EFFECTIVE="$(min_driver_memory "${JOB1_DRIVER_MEMORY:-512m}")"
JOB2_DRIVER_MEMORY_EFFECTIVE="$(min_driver_memory "${JOB2_DRIVER_MEMORY:-512m}")"
JOB3_DRIVER_MEMORY_EFFECTIVE="$(min_driver_memory "${JOB3_DRIVER_MEMORY:-512m}")"
JOB4_DRIVER_MEMORY_EFFECTIVE="$(min_driver_memory "${JOB4_DRIVER_MEMORY:-512m}")"
JOB5_DRIVER_MEMORY_EFFECTIVE="$(min_driver_memory "${JOB5_DRIVER_MEMORY:-512m}")"

# ── Common spark-submit flags ─────────────────────────────────────────────────
COMMON_BASE="spark-submit \
  --deploy-mode client \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_EP} \
  --conf spark.hadoop.fs.s3a.access.key=${MINIO_AK} \
  --conf spark.hadoop.fs.s3a.secret.key=${MINIO_SK} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.pyspark.driver.python=/usr/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BS_EXECUTOR} \
  --conf spark.driver.host=${DRIVER_HOST} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.port.maxRetries=20 \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.rpc.askTimeout=300s \
  --conf spark.rpc.lookupTimeout=300s \
  --conf spark.shuffle.io.maxRetries=12 \
  --conf spark.shuffle.io.retryWait=5s \
  --conf spark.task.maxFailures=8 \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.streaming.backpressure.initialRate=500 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.s3a.connection.maximum=20 \
  --conf spark.hadoop.fs.s3a.threads.max=8 \
  --conf spark.hadoop.fs.s3a.multipart.size=67108864 \
  --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk \
  --conf spark.hadoop.fs.s3a.fast.upload.active.blocks=2"
COMMON="${COMMON_BASE} --master ${SPARK_MASTER}"
COMMON_STATEFUL="${COMMON_BASE} --master ${STATEFUL_STREAM_MASTER}"

# ── Tài nguyên mỗi job ────────────────────────────────────────────────────────
# Tổng executor RAM: 1.5+0.875+1.25+3.0+3.875 = 10.5g ≤ 12g worker ✅
# Mỗi job tự set memoryOverhead riêng (không dùng chung 512m nữa)
RES_JOB1="--conf spark.driver.port=4100 \
  --executor-cores ${JOB1_EXECUTOR_CORES:-2} \
  --total-executor-cores ${JOB1_TOTAL_CORES:-2} \
  --executor-memory ${JOB1_EXECUTOR_MEMORY:-1280m} \
  --driver-memory ${JOB1_DRIVER_MEMORY_EFFECTIVE} \
  --conf spark.executor.memoryOverhead=${JOB1_MEMORY_OVERHEAD:-256m} \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=${JOB1_SHUFFLE_PARTITIONS:-4}"

RES_JOB2="--conf spark.driver.port=4101 \
  --executor-cores ${JOB2_EXECUTOR_CORES:-1} \
  --total-executor-cores ${JOB2_TOTAL_CORES:-1} \
  --executor-memory ${JOB2_EXECUTOR_MEMORY:-640m} \
  --driver-memory ${JOB2_DRIVER_MEMORY_EFFECTIVE} \
  --conf spark.executor.memoryOverhead=${JOB2_MEMORY_OVERHEAD:-256m} \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=${JOB2_SHUFFLE_PARTITIONS:-4}"

RES_JOB3="--conf spark.driver.port=4102 \
  --executor-cores ${JOB3_EXECUTOR_CORES:-1} \
  --total-executor-cores ${JOB3_TOTAL_CORES:-1} \
  --executor-memory ${JOB3_EXECUTOR_MEMORY:-1024m} \
  --driver-memory ${JOB3_DRIVER_MEMORY_EFFECTIVE} \
  --conf spark.executor.memoryOverhead=${JOB3_MEMORY_OVERHEAD:-256m} \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=${JOB3_SHUFFLE_PARTITIONS:-4}"

RES_JOB4="--conf spark.driver.port=4103 \
  --executor-cores ${JOB4_EXECUTOR_CORES:-2} \
  --total-executor-cores ${JOB4_TOTAL_CORES:-2} \
  --executor-memory ${JOB4_EXECUTOR_MEMORY:-2816m} \
  --driver-memory ${JOB4_DRIVER_MEMORY_EFFECTIVE} \
  --conf spark.executor.memoryOverhead=${JOB4_MEMORY_OVERHEAD:-256m} \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=${JOB4_SHUFFLE_PARTITIONS:-4}"

# Job5: total-executor-cores=2 → 1 executor (foreachBatch serialized)
# storageFraction=0.5: storage pool lớn hơn cho 3× persist + batch read
RES_JOB5="--conf spark.driver.port=4104 \
  --executor-cores ${JOB5_EXECUTOR_CORES:-2} \
  --total-executor-cores ${JOB5_TOTAL_CORES:-2} \
  --executor-memory ${JOB5_EXECUTOR_MEMORY:-3584m} \
  --driver-memory ${JOB5_DRIVER_MEMORY_EFFECTIVE} \
  --conf spark.executor.memoryOverhead=${JOB5_MEMORY_OVERHEAD:-384m} \
  --conf spark.memory.storageFraction=${JOB5_STORAGE_FRACTION:-0.5} \
  --conf spark.sql.shuffle.partitions=${JOB5_SHUFFLE_PARTITIONS:-4}"


# ── check_delta: kiểm tra Delta table có commit đầu tiên chưa ─────────────────
# Dùng python3 s3_check.py thay 'hadoop fs -test -e' (hadoop ko có trong image)
# Usage: check_delta "s3a://bronze/request"
check_delta() {
  local s3a_url="$1"                         # e.g. s3a://bronze/request
  local path="${s3a_url#s3a://}"             # bronze/request
  local bucket="${path%%/*}"                 # bronze
  local prefix="${path#*/}"                  # request
  ${S3CHECK} "${bucket}" "${prefix}/_delta_log/00000000000000000000.json" 2>/dev/null
}

# ═══════════════════════════════════════════════════════════════════════════════
# PRE-FLIGHT: Kiểm tra Kafka topic tồn tại
# ═══════════════════════════════════════════════════════════════════════════════
wait_for_kafka_topic() {
  local topic="$1"
  local host="$2"
  local port="$3"
  local timeout="${4:-120}"
  local elapsed=0

  echo "⏳ Pre-flight: chờ Kafka topic '${topic}' tại ${host}:${port}..."

  cat > /tmp/_check_topic.py << 'PYEOF'
import socket, struct, sys

def kafka_topic_exists(host, port, topic):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, int(port)))
        t = topic.encode('utf-8')
        body = (struct.pack('>hhi', 3, 0, 1)
                + struct.pack('>h', -1)
                + struct.pack('>i', 1)
                + struct.pack('>h', len(t)) + t)
        s.send(struct.pack('>i', len(body)) + body)
        header = s.recv(4)
        if len(header) < 4:
            s.close(); return False
        length = struct.unpack('>i', header)[0]
        data = b''
        while len(data) < length:
            chunk = s.recv(length - len(data))
            if not chunk: break
            data += chunk
        s.close()
        return t in data
    except Exception:
        return False

host, port, topic = sys.argv[1], sys.argv[2], sys.argv[3]
sys.exit(0 if kafka_topic_exists(host, port, topic) else 1)
PYEOF

  while [ "${elapsed}" -lt "${timeout}" ]; do
    if python3 /tmp/_check_topic.py "${host}" "${port}" "${topic}" 2>/dev/null; then
      echo "  ✅ Kafka topic '${topic}' sẵn sàng (${elapsed}s)"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    echo "     ... ${elapsed}s / ${timeout}s"
  done

  echo "  ❌ Timeout — Kafka topic '${topic}' không tồn tại!"
  return 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# Kill zombie Spark apps
# ═══════════════════════════════════════════════════════════════════════════════
kill_zombie_apps() {
  local master_ui="http://localhost:8080"
  echo "🧹 Kiểm tra Spark apps zombie..."
  local app_ids
  app_ids=$(curl -s "${master_ui}/api/v1/applications" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    apps = json.load(sys.stdin)
    for a in apps:
        if a.get('state','') == 'RUNNING':
            print(a['id'])
except: pass
" 2>/dev/null)

  if [ -z "${app_ids}" ]; then
    echo "   ✅ Không có app zombie."
    return 0
  fi
  for app_id in ${app_ids}; do
    echo "   ⚠️  Kill zombie app: ${app_id}"
    curl -s -X POST "${master_ui}/app/kill/" -d "id=${app_id}&terminate=true" > /dev/null 2>&1 || true
  done
  sleep 5
}

# ── Trap Ctrl+C ────────────────────────────────────────────────────────────────
ALL_PIDS=""
cleanup() {
  echo ""
  echo "🛑 Đang dừng tất cả Spark jobs..."
  [ -n "${ALL_PIDS}" ] && kill ${ALL_PIDS} 2>/dev/null
  exit 0
}
trap cleanup INT TERM

# ═══════════════════════════════════════════════════════════════════════════════
echo "================================================================"
echo " NYC Taxi Streaming Pipeline — Lazy Launcher"
echo "================================================================"
echo " Spark Master : ${SPARK_MASTER}"
echo " Driver Host  : ${DRIVER_HOST}"
echo " MinIO        : ${MINIO_EP}"
echo " Kafka        : ${KAFKA_BS}"
echo " Checkpoints  : ${CHECKPOINT_BASE}"
echo " Stateful CP  : ${STATEFUL_CHECKPOINT_BASE_EFFECTIVE}"
echo " Stateful run : ${STATEFUL_STREAM_MASTER}"
echo " Launcher poll: ${LAUNCHER_POLL_INTERVAL_S}s"
echo "================================================================"
echo ""

wait_for_kafka_topic "nyc_taxi_events" "${KAFKA_HOST}" "${KAFKA_PORT}" 120 || exit 1
echo ""

kill_zombie_apps
echo ""

# ── [JOB 1] Kafka → Bronze ────────────────────────────────────────────────────
echo "[1/5] Kafka → Bronze (request / pickup / dropoff)"
echo "      Đọc: Kafka topic nyc_taxi_events"
echo "      Ghi: s3a://bronze/request, bronze/pickup, bronze/dropoff"
echo "      Tài nguyên: ${JOB1_TOTAL_CORES:-2} total cores, executor=${JOB1_EXECUTOR_CORES:-2} cores/${JOB1_EXECUTOR_MEMORY:-4g}, shuffle=${JOB1_SHUFFLE_PARTITIONS:-6}"
${COMMON} ${RES_JOB1} "${JOBS_DIR}/taxi_kafka_to_bronze.py" &
JOB1_PID=$!
ALL_PIDS="${JOB1_PID}"
echo "      PID: ${JOB1_PID}"

# ── PARALLEL LAUNCHER cho Jobs 2-5 ───────────────────────────────────────────
# Submit ngay từ đầu để Spark jobs tự wait cho source Delta.
# Mục tiêu: giảm độ trễ orchestration và cho phép silver/gold khởi động sớm,
# thay vì đợi shell launcher polling trước khi submit.
# ─────────────────────────────────────────────────────────────────────────────

(
  exec ${COMMON_STATEFUL} ${RES_JOB2} "${JOBS_DIR}/request_bronze_to_silver.py"
) &
JOB2_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB2_PID}"
echo "      [2/5] PID: ${JOB2_PID}"

(
  exec ${COMMON_STATEFUL} ${RES_JOB3} "${JOBS_DIR}/request_to_response_silver.py"
) &
JOB3_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB3_PID}"
echo "      [3/5] PID: ${JOB3_PID}"

(
  exec ${COMMON_STATEFUL} ${RES_JOB4} "${JOBS_DIR}/complete_bronze_to_silver.py"
) &
JOB4_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB4_PID}"
echo "      [4/5] PID: ${JOB4_PID}"

(
  exec ${COMMON} ${RES_JOB5} "${JOBS_DIR}/silver_to_gold.py"
) &
JOB5_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB5_PID}"
echo "      [5/5] PID: ${JOB5_PID}"

echo ""
echo "================================================================"
echo " ✅ Job1 đang chạy. Launchers 2-5 đang theo dõi source tables."
echo " Jobs tự khởi động khi pipeline sẵn sàng — không mất event"
echo " (Delta streaming đọc từ commit 0 dù start muộn)."
echo "================================================================"
echo "  Spark Master UI : http://${DRIVER_HOST}:8080"
echo "  Spark Driver UI : http://${DRIVER_HOST}:4040"
echo "  (Ctrl+C để dừng tất cả)"
echo "================================================================"
echo ""

wait
