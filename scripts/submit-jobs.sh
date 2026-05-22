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

# ── Common spark-submit flags ─────────────────────────────────────────────────
COMMON="spark-submit \
  --master ${SPARK_MASTER} \
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
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.executor.memoryOverhead=256m"

# ── Tài nguyên mỗi job ────────────────────────────────────────────────────────
RES_JOB1="--executor-cores 2 --total-executor-cores 2 --executor-memory 2g   --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4"
RES_JOB2="--executor-cores 1 --total-executor-cores 1 --executor-memory 1g   --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4"
RES_JOB3="--executor-cores 1 --total-executor-cores 1 --executor-memory 1g   --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4"
RES_JOB4="--executor-cores 1 --total-executor-cores 1 --executor-memory 1g   --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4"
RES_JOB5="--executor-cores 1 --total-executor-cores 1 --executor-memory 750m --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4"

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
echo "      Tài nguyên: 2 cores / 2GB executor"
${COMMON} ${RES_JOB1} "${JOBS_DIR}/taxi_kafka_to_bronze.py" &
JOB1_PID=$!
ALL_PIDS="${JOB1_PID}"
echo "      PID: ${JOB1_PID}"

# ── LAZY LAUNCHER cho Jobs 2-5 ────────────────────────────────────────────────
# Dùng python3 s3_check.py để kiểm tra source (hadoop binary không có trong image).
# spark-submit chỉ được gọi khi source Delta table đã tồn tại.
# Delta streaming đọc từ commit 0 → không mất dữ liệu dù start muộn.
# Executor cấp phát tuần tự → tránh OOM từ 5 JVM cùng lúc.
# ─────────────────────────────────────────────────────────────────────────────

# [2/5] chờ bronze/request rồi submit
(
  echo "[2/5-launcher] Chờ s3a://bronze/request ..."
  elapsed=0
  while true; do
    if ! kill -0 "${JOB1_PID}" 2>/dev/null; then
      echo "[2/5-launcher] ❌ Job1 chết — hủy Job2"; exit 1
    fi
    if check_delta "s3a://bronze/request"; then
      echo "[2/5-launcher] ✅ s3a://bronze/request sẵn sàng (${elapsed}s) → Submit Job2"
      break
    fi
    elapsed=$((elapsed + 10)); sleep 10
    echo "[2/5-launcher]   ... chưa sẵn sàng (${elapsed}s)"
  done
  exec ${COMMON} ${RES_JOB2} "${JOBS_DIR}/request_bronze_to_silver.py"
) &
JOB2_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB2_PID}"
echo "      [2/5] Launcher PID: ${JOB2_PID}"

# [3/5] chờ silver/request + bronze/pickup rồi submit
(
  echo "[3/5-launcher] Chờ s3a://silver/request + s3a://bronze/pickup ..."
  elapsed=0
  while true; do
    if ! kill -0 "${JOB2_PID}" 2>/dev/null; then
      echo "[3/5-launcher] ❌ Job2 chết — hủy Job3"; exit 1
    fi
    rs=false; rp=false
    check_delta "s3a://silver/request" && rs=true
    check_delta "s3a://bronze/pickup"  && rp=true
    if ${rs} && ${rp}; then
      echo "[3/5-launcher] ✅ silver/request + bronze/pickup sẵn sàng (${elapsed}s) → Submit Job3"
      break
    fi
    elapsed=$((elapsed + 10)); sleep 10
    echo "[3/5-launcher]   ... silver/request=${rs} bronze/pickup=${rp} (${elapsed}s)"
  done
  exec ${COMMON} ${RES_JOB3} "${JOBS_DIR}/request_to_response_silver.py"
) &
JOB3_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB3_PID}"
echo "      [3/5] Launcher PID: ${JOB3_PID}"

# [4/5] chờ silver/response + bronze/dropoff rồi submit
(
  echo "[4/5-launcher] Chờ s3a://silver/response + s3a://bronze/dropoff ..."
  elapsed=0
  while true; do
    if ! kill -0 "${JOB3_PID}" 2>/dev/null; then
      echo "[4/5-launcher] ❌ Job3 chết — hủy Job4"; exit 1
    fi
    rr=false; rd=false
    check_delta "s3a://silver/response"  && rr=true
    check_delta "s3a://bronze/dropoff"   && rd=true
    if ${rr} && ${rd}; then
      echo "[4/5-launcher] ✅ silver/response + bronze/dropoff sẵn sàng (${elapsed}s) → Submit Job4"
      break
    fi
    elapsed=$((elapsed + 10)); sleep 10
    echo "[4/5-launcher]   ... silver/response=${rr} bronze/dropoff=${rd} (${elapsed}s)"
  done
  exec ${COMMON} ${RES_JOB4} "${JOBS_DIR}/complete_bronze_to_silver.py"
) &
JOB4_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB4_PID}"
echo "      [4/5] Launcher PID: ${JOB4_PID}"

# [5/5] chờ silver/complete rồi submit
(
  echo "[5/5-launcher] Chờ s3a://silver/complete ..."
  elapsed=0
  while true; do
    if ! kill -0 "${JOB4_PID}" 2>/dev/null; then
      echo "[5/5-launcher] ❌ Job4 chết — hủy Job5"; exit 1
    fi
    if check_delta "s3a://silver/complete"; then
      echo "[5/5-launcher] ✅ s3a://silver/complete sẵn sàng (${elapsed}s) → Submit Job5"
      break
    fi
    elapsed=$((elapsed + 10)); sleep 10
    echo "[5/5-launcher]   ... chưa sẵn sàng (${elapsed}s)"
  done
  exec ${COMMON} ${RES_JOB5} "${JOBS_DIR}/silver_to_gold.py"
) &
JOB5_PID=$!
ALL_PIDS="${ALL_PIDS} ${JOB5_PID}"
echo "      [5/5] Launcher PID: ${JOB5_PID}"

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
