# 🚀 Hướng Dẫn Chạy Hệ Thống NYC Taxi — 2 Máy (Master + Server)

## 📋 Kiến Trúc 2 Máy

```
┌─────────────────────────────────────────┐    ┌──────────────────────────────────┐
│         MÁYA 1 — Data Processing       │    │   MÁY 2 — ML & Monitoring       │
│                                         │    │                                  │
│  • Kafka + Zookeeper                    │    │  • PostgreSQL (shared metadata)  │
│  • Spark Master/Worker                  │    │  • Airflow (Orchestration)      │
│  • MinIO (S3-compatible)                │    │  • MLflow (Model Registry)      │
│  • Delta Lake (Data Lakehouse)          │    │  • LightGBM (Model Training)    │
│  • 3x Producers (request/pickup/drop)   │◄──►│  • Prometheus (Metrics)         │
│                                         │    │  • Grafana (Dashboards)         │
│  16GB RAM × 12 cores                    │    │  • Statsd Exporter              │
│  IP: 192.168.1.100                      │    │  16GB RAM × 12 cores            │
│                                         │    │  IP: 192.168.1.101              │
└─────────────────────────────────────────┘    └──────────────────────────────────┘
        ▲                                              ▲
        │ Kafka: 9092                                │
        │ S3: 9000/9001                             │
        │ Spark: 7077, 8080                          │
        └─────────────────────────────────────────────┘
           REST APIs + Spark Cluster
```

---

## ✅ Prerequisites

### Trên cả Máy 1 & Máy 2:
1. ✅ **Docker Desktop** ≥ 24.0 (cài đặt & chạy)
2. ✅ **Docker Compose** ≥ 2.20
3. ✅ **Python 3.10+** (cho scripts)
4. ✅ **Git** (clone project)
5. ✅ **Network**: Hai máy phải nằm cùng network (hoặc VPN)

### Kiểm tra:
```bash
docker --version
docker compose version
python --version
git --version
```

### Network Setup:
- Nếu testing local: dùng `localhost` hoặc `127.0.0.1`
- Nếu multi-machine thực: dùng IP tĩnh, firewall allow ports:
  - 9092 (Kafka)
  - 9000/9001 (MinIO)
  - 7077/8080/8082 (Spark)
  - 5432 (PostgreSQL)
  - 5000 (MLflow)
  - 8888 (Airflow)
  - 3000 (Grafana)
  - 9090 (Prometheus)

---

## 📁 Chuẩn Bị Dữ Liệu

### Bước 1: Tải Dataset

**NYC HVFHV 2025 (Jan–Nov)** — ~200 GB:
```
sorted_request_table/    → 11 files request_2025_XX.parquet
sorted_pickup_table/     → 1 file sorted_pickup.parquet
sorted_dropoff_table/    → 11 files dropoff_2025_XX.parquet
weather_full.csv         → Weather data (~10 MB)
```

### Bước 2: Cấu trúc Thư Mục

**Máy 1** — Đặt dataset:
```
/data/bigdata/              # hoặc D:\data\BigData (Windows)
├── sorted_request_table/
│   ├── request_2025_01.parquet
│   ├── request_2025_02.parquet
│   └── ...request_2025_11.parquet
├── sorted_pickup_table/
│   └── sorted_pickup.parquet
├── sorted_dropoff_table/
│   ├── dropoff_2025_01.parquet
│   └── ...dropoff_2025_11.parquet
└── weather_full.csv
```

**Máy 2** — Copy project, không cần dataset (sẽ đọc qua mạng từ Máy 1)

### Bước 3: Sync Project

```bash
# Cả 2 máy
git clone <repo-url>
cd BigData
```

---

## ⚙️ Cấu Hình `.env`

### Tạo `.env` từ template:
```bash
# Cả 2 máy — tạo file .env
touch .env
```

### `.env` đầy đủ cho Máy 1 & Máy 2:

```env
# ============================================================
# NETWORK & MACHINES
# ============================================================
# Máy 1 IP (nơi chạy Kafka, Spark, MinIO, Producers)
MACHINE_1_IP=192.168.1.100

# Máy 2 IP (nơi chạy Airflow, MLflow, PostgreSQL, Grafana)
MACHINE_2_IP=192.168.1.101

# Dùng trong các config (localhost nếu testing local)
MASTER_IP=${MACHINE_1_IP}
MLFLOW_MACHINE_IP=${MACHINE_2_IP}

# ============================================================
# DATASET (chỉ Máy 1 cần)
# ============================================================
# Windows: D:\data\BigData
# Linux:   /mnt/data/bigdata
DATASET_PATH=D:\data\BigData

# ============================================================
# MINIO (S3-compatible Object Storage — Máy 1)
# ============================================================
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_ENDPOINT_EXTERNAL=http://${MACHINE_1_IP}:9000

# ============================================================
# KAFKA (Máy 1)
# ============================================================
# Tốc độ phát dữ liệu: 1 = real-time, 60 = 60x faster
SPEED_FACTOR=60
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_EXTERNAL_HOST=${MACHINE_1_IP}:29092

# ============================================================
# POSTGRESQL (shared metadata — nên đặt Máy 2)
# ============================================================
POSTGRES_USER=bigdata_user
POSTGRES_PASSWORD=secure_password_here
POSTGRES_DB=bigdata
POSTGRES_HOST=postgres      # Docker internal (Máy 2)
POSTGRES_PORT=5432
POSTGRES_EXTERNAL_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${MACHINE_2_IP}:5432/${POSTGRES_DB}

# ============================================================
# AIRFLOW (Máy 2)
# ============================================================
AIRFLOW_FERNET_KEY=your-32-char-fernet-key-base64-here
AIRFLOW_SECRET_KEY=your-secret-key-here
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW_UID=1000

# ============================================================
# SPARK (Máy 1)
# ============================================================
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=5g
SPARK_WORKER_CORES=4

# ============================================================
# MLFLOW (Máy 2)
# ============================================================
MLFLOW_TRACKING_URI=http://${MACHINE_2_IP}:5000

# ============================================================
# WEATHER DATA (Máy 1 mount)
# ============================================================
WEATHER_CSV_PATH=/datasets/weather_full.csv
WEATHER_PARQUET_PATH=s3://weather/parquet

# ============================================================
# SILVER & GOLD PATHS (MinIO S3 — Máy 1)
# ============================================================
SILVER_WEATHER=s3a://silver/weather
SILVER_RESPONSE=s3a://silver/response
SILVER_COMPLETE=s3a://silver/complete
GOLD_AGGREGATED=s3a://gold/aggregated

# ============================================================
# AIRFLOW ENVIRONMENT PATHS (Máy 2)
# ============================================================
# Reference Máy 1 services
MINIO_ENDPOINT_EXTERNAL=http://${MACHINE_1_IP}:9000
SPARK_MASTER_URL_FROM_AIRFLOW=spark://${MACHINE_1_IP}:7077
```

### Sinh Fernet Key (chạy bất kỳ đâu):
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Sao chép output vào `AIRFLOW_FERNET_KEY=...`

---

## 🚀 CHẠY HỆ THỐNG

### **BƯỚC 1: MÁY 1 — Setup Kafka, Spark, MinIO**

#### 1.1 Chuẩn bị

```bash
cd BigData

# Kiểm tra Docker chạy
docker ps

# Cấp quyền cho script
chmod +x start_pipeline.sh scripts/*.sh
```

#### 1.2 Build & Start Máy 1 Services

Tạo file `docker-compose.machine1.yml` từ `docker-compose.master.yml` (bỏ các services Máy 2):

```bash
# Nếu có sẵn, dùng:
docker compose -f docker-compose.master.yml up --build -d \
  zookeeper kafka kafka-setup kafka-lag-exporter \
  minio minio-setup \
  spark-master spark-worker \
  request-producer pickup-producer dropoff-producer
```

Hoặc chạy script:
```bash
./start_pipeline.sh  # Script tự start Máy 1
```

#### 1.3 Kiểm tra Máy 1 Healthy

```bash
# Kiểm tra từng service
docker logs zookeeper | grep "Started"
docker logs kafka | grep "started"
docker logs minio | grep "started"
docker logs spark-master | grep "started"

# Hoặc dùng health check
docker ps | grep -E "zookeeper|kafka|minio|spark"
# Tất cả phải là "healthy" hoặc "Up"
```

**Dấu hiệu OK:**
```
✅ Kafka broker started (port 9092)
✅ MinIO running (port 9000/9001)
✅ Spark Master UI accessible (port 8080)
✅ Producers sending data to Kafka
```

---

### **BƯỚC 2: MÁY 2 — Setup PostgreSQL, Airflow, MLflow, Grafana**

#### 2.1 Chuẩn bị

```bash
cd BigData

# Cấp quyền scripts
chmod +x scripts/*.sh
```

#### 2.2 Tạo docker-compose cho Máy 2

Tạo file `docker-compose.machine2.yml`:

```yaml
version: "3.8"

# ================================================================
# docker-compose.machine2.yml — Machine 2 (Airflow + MLflow)
# ================================================================

x-minio-env: &minio-env
  AWS_ACCESS_KEY_ID: ${MINIO_ACCESS_KEY}
  AWS_SECRET_ACCESS_KEY: ${MINIO_SECRET_KEY}
  MLFLOW_S3_ENDPOINT_URL: ${MINIO_ENDPOINT}

services:

  # ── POSTGRES (shared: Airflow + MLflow) ─────────────────────
  postgres:
    image: postgres:16-alpine
    container_name: postgres
    restart: unless-stopped
    mem_limit: 1g
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_INITDB_ARGS: "--data-checksums"
    command: >
      postgres
        -c shared_buffers=256MB
        -c work_mem=16MB
        -c maintenance_work_mem=128MB
        -c max_connections=200
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./postgres/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s

  # ── AIRFLOW ────────────────────────────────────────────────
  airflow:
    build:
      context: ./airflow
      dockerfile: Dockerfile
    image: bigdata-airflow:latest
    container_name: airflow
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY}
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
      AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 30
      AIRFLOW__CORE__PARALLELISM: 4
      AIRFLOW__SCHEDULER__MAX_THREADS: 2
      AIRFLOW__METRICS__STATSD_ON: "true"
      AIRFLOW__METRICS__STATSD_HOST: airflow-statsd-exporter
      AIRFLOW__METRICS__STATSD_PORT: 8125
      AIRFLOW__METRICS__STATSD_PREFIX: airflow
      # Point ke Máy 1
      MASTER_IP: ${MACHINE_1_IP}
      MINIO_ENDPOINT: ${MINIO_ENDPOINT_EXTERNAL}
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY}
      MLFLOW_TRACKING_URI: http://mlflow:5000
      POSTGRES_HOST: postgres
      POSTGRES_PORT: "5432"
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      SPARK_MASTER_URL: ${SPARK_MASTER_URL}
      SILVER_WEATHER: s3a://silver/weather
      SILVER_RESPONSE: s3a://silver/response
      SILVER_COMPLETE: s3a://silver/complete
      GOLD_AGGREGATED: s3a://gold/aggregated
      <<: *minio-env
    ports:
      - "8888:8080"
    volumes:
      - airflow-data:/opt/airflow
      - ./airflow/dags:/opt/airflow/dags
      - ./ml:/opt/airflow/ml
      - ./spark/jobs:/opt/airflow/spark_jobs
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create --username admin --password admin
          --firstname Admin --lastname Admin
          --role Admin --email admin@bigdata.com 2>/dev/null || true &&
        airflow webserver &
        airflow scheduler
      "
    healthcheck:
      test: curl -f http://localhost:8080/health
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 60s

  # ── AIRFLOW STATSD EXPORTER ────────────────────────────────
  airflow-statsd-exporter:
    image: prom/statsd-exporter:v0.26.0
    container_name: airflow-statsd-exporter
    restart: unless-stopped
    command:
      - "--statsd.listen-udp=:8125"
      - "--web.listen-address=:9102"
      - "--statsd.mapping-config=/etc/statsd/statsd_mapping.yml"
    ports:
      - "9102:9102"
    volumes:
      - ./monitoring/airflow-statsd/statsd_mapping.yml:/etc/statsd/statsd_mapping.yml:ro

  # ── MLFLOW ────────────────────────────────────────────────
  mlflow:
    image: python:3.11-slim
    container_name: mlflow
    restart: unless-stopped
    mem_limit: 800m
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      <<: *minio-env
    command: >
      bash -c "
        pip install --quiet mlflow boto3 psycopg2-binary &&
        mlflow server
          --backend-store-uri postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/mlflow
          --default-artifact-root s3://mlflow-artifacts/
          --host 0.0.0.0
          --port 5000
      "
    ports:
      - "5000:5000"
    healthcheck:
      test: curl -f http://localhost:5000/health
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s

  # ── PROMETHEUS ────────────────────────────────────────────
  prometheus:
    image: prom/prometheus:v2.48.0
    container_name: prometheus
    restart: unless-stopped
    mem_limit: 1g
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"
      - "--storage.tsdb.retention.time=30d"
      - "--web.enable-lifecycle"
      - "--web.enable-admin-api"
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/prometheus/alerts:/etc/prometheus/alerts:ro
      - prometheus-data:/prometheus

  # ── GRAFANA ────────────────────────────────────────────────
  grafana:
    image: grafana/grafana:10.2.0
    container_name: grafana
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
      prometheus:
        condition: service_started
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_INSTALL_PLUGINS: grafana-piechart-panel
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /etc/grafana/provisioning/dashboards/demand_forecast.json
      GF_UNIFIED_ALERTING_ENABLED: "true"
      GF_POSTGRES_USER: ${POSTGRES_USER}
      GF_POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./monitoring/grafana/datasources/datasources.yml:/etc/grafana/provisioning/datasources/datasources.yml:ro

  # ── NODE EXPORTER (infrastructure metrics) ──────────────────
  node-exporter:
    image: prom/node-exporter:v1.7.0
    container_name: node-exporter
    restart: unless-stopped
    network_mode: host
    pid: host
    command:
      - "--path.rootfs=/host"
      - "--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)"
    volumes:
      - /:/host:ro,rslave

  # ── BLACKBOX EXPORTER (endpoint monitoring) ────────────────
  blackbox-exporter:
    image: prom/blackbox-exporter:v0.24.0
    container_name: blackbox-exporter
    restart: unless-stopped
    ports:
      - "9115:9115"
    command:
      - "--config.file=/etc/blackbox/config.yml"
    volumes:
      - ./monitoring/prometheus/blackbox.yml:/etc/blackbox/config.yml:ro

volumes:
  postgres-data:
  airflow-data:
  grafana-data:
  prometheus-data:

networks:
  default:
    name: bigdata-net
    driver: bridge
```

#### 2.3 Start Máy 2

```bash
docker compose -f docker-compose.machine2.yml up --build -d
```

#### 2.4 Kiểm tra Máy 2 Healthy

```bash
# Kiểm tra PostgreSQL
docker logs postgres | grep "ready to accept"

# Kiểm tra Airflow
docker logs airflow | grep "Starting Airflow"

# Kiểm tra MLflow
docker logs mlflow | grep "started"

# Kiểm tra Grafana
docker logs grafana | grep "Listening"
```

**Dấu hiệu OK:**
```
✅ PostgreSQL ready (port 5432)
✅ Airflow running (port 8888, admin/admin)
✅ MLflow server started (port 5000)
✅ Prometheus scraping (port 9090)
✅ Grafana up (port 3000, admin/admin)
```

---

### **BƯỚC 3: Upload LightGBM Model lên MLflow**

Khi Máy 2 ready, upload model:

```bash
# Trên máy Windows hoặc lokal
pip install mlflow lightgbm boto3 psycopg2-binary

python upload_model_to_mlflow.py
```

**Script sẽ:**
- Load `lgb_final_model.txt` (trained trước)
- Register 2 models: `demand_forecast_model_a` & `demand_forecast_model_b`
- Set stage: **Production**

**Dấu hiệu thành công:**
```
✅ Model registered: demand_forecast_model_a → Production
✅ Model registered: demand_forecast_model_b → Production
```

---

### **BƯỚC 4: MÁY 1 — Submit Spark Streaming Jobs**

Sau khi Máy 2 ready, submit Spark jobs:

```bash
# Trên Máy 1
docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh
```

**5 jobs sẽ chạy:**
1. ✅ Kafka → Bronze
2. ✅ Bronze/request → Silver/request
3. ✅ Silver/request ⋈ Bronze/pickup → Silver/response
4. ✅ Silver/response ⋈ Bronze/dropoff → Silver/complete
5. ✅ Silver/complete → Gold (hourly aggregations)

**Monitor tại:** `http://<MACHINE_1_IP>:8080`

---

## 📊 Data Flow

```
MÁYA 1                              MÁY 2
─────────────────────────────────  ─────────────────────────────────

Producers (3x)
    ↓ Kafka topic: nyc_taxi_events
kafka-setup (auto create topic)
    ↓
Spark Job 1: taxi_kafka_to_bronze.py
    ↓
MinIO Bronze Layer
    ├── request_events
    ├── pickup_events
    └── dropoff_events
    ↓
[Jobs 2-5: Silver → Gold transformations]
    ↓
MinIO Gold Layer (hourly agg)
    ├── aggregated/
    └── (location, window_end, demand)
         ↓
         ├──→ PostgreSQL predictions_monitoring table ─→ Grafana 📊
         │
         ├──→ Airflow retrain_dag (daily) ────────→ Train & Promote Model
         │
         └──→ MLflow Model Registry (Production)

Prometheus ←─ Metrics from Kafka, Spark, Airflow
     ↓
Grafana ← Prometheus + PostgreSQL
```

---

## 🔍 Theo Dõi Pipeline

### URLs Chính

| Service | Máy | URL | Credentials |
|---------|-----|-----|-------------|
| **Spark Master** | 1 | http://MACHINE_1_IP:8080 | — |
| **MinIO Console** | 1 | http://MACHINE_1_IP:9001 | minioadmin/minioadmin |
| **Kafka LAG** | 1 | http://MACHINE_1_IP:8099 | — |
| **Airflow** | 2 | http://MACHINE_2_IP:8888 | admin/admin |
| **MLflow** | 2 | http://MACHINE_2_IP:5000 | — |
| **Prometheus** | 2 | http://MACHINE_2_IP:9090 | — |
| **Grafana** | 2 | http://MACHINE_2_IP:3000 | admin/admin |
| **PostgreSQL** | 2 | MACHINE_2_IP:5432 | bigdata_user/... |

### Logs Real-Time

```bash
# Máya 1 — Data Pipeline
docker logs -f request-producer | head -20
docker logs -f kafka | grep "started"
docker logs -f spark-master | grep "Batch"

# Máy 2 — ML & Monitoring
docker logs -f airflow | grep "DAG"
docker logs -f mlflow | grep "Registered"
docker logs -f grafana | tail -20
```

---

## ⚠️ Troubleshooting

### **Máy 1 Issues**

#### Kafka không start
```bash
docker logs kafka | tail -50
# Kiểm tra Zookeeper healthy
docker logs zookeeper | grep "Started"
```

#### Producers không gửi data
```bash
# Kiểm tra topic
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Kiểm tra data trong topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic nyc_taxi_events \
  --max-messages 5 \
  --from-beginning
```

#### MinIO permission denied
```bash
docker logs minio | grep "ERROR"
# Fix: kiểm tra MINIO_ACCESS_KEY, MINIO_SECRET_KEY trong .env
```

### **Máy 2 Issues**

#### PostgreSQL connection fail
```bash
docker logs postgres | tail -20

# Test connection
docker exec postgres psql -U bigdata_user -d bigdata -c "SELECT 1"
```

#### Airflow DAGs không show
```bash
# Restart scheduler
docker compose -f docker-compose.machine2.yml restart airflow

# Kiểm trace log
docker logs airflow | grep "ERROR"
```

#### MLflow model không register
```bash
# Kiểm tra MLflow connectivity
python -c "
import mlflow
mlflow.set_tracking_uri('http://MACHINE_2_IP:5000')
print(mlflow.get_tracking_uri())
"
```

#### Grafana datasource fail
```bash
# Kiểm tra PostgreSQL từ Grafana container
docker exec grafana psql -h postgres -U bigdata_user -d bigdata -c "SELECT 1"
```

---

## 🛑 Dừng Hệ Thống

### Dừng Máy 1
```bash
docker compose -f docker-compose.master.yml down

# Xóa volumes (BE CAREFUL!)
docker compose -f docker-compose.master.yml down -v
```

### Dừng Máy 2
```bash
docker compose -f docker-compose.machine2.yml down

# Xóa volumes
docker compose -f docker-compose.machine2.yml down -v
```

---

## ✅ Checklist Setup Hoàn Tất

### Máy 1
- [ ] Docker running
- [ ] Dataset ở đúng path với cấu trúc file đúng
- [ ] `.env` điền `MACHINE_1_IP`, `DATASET_PATH`
- [ ] Zookeeper healthy
- [ ] Kafka broker started & topic created
- [ ] MinIO buckets created (bronze, silver, gold, etc.)
- [ ] Spark Master UI accessible (8080)
- [ ] Producers sending data (request-producer logs show "Sent")
- [ ] Bronze layer có data (MinIO Console → bronze bucket)

### Máy 2
- [ ] `.env` điền `MACHINE_2_IP`, `MACHINE_1_IP`
- [ ] PostgreSQL healthy
- [ ] Airflow UI up (8888)
- [ ] MLflow server started (5000)
- [ ] Model registered in Production stage
- [ ] Prometheus scraping (9090)
- [ ] Grafana datasources configured (3000)
- [ ] Grafana PostgreSQL datasource connected

### Integration
- [ ] Máy 1 ↔ Máy 2 network connectivity OK (test `ping`)
- [ ] Airflow can reach Spark Master (SPARK_MASTER_URL resolves)
- [ ] Airflow can reach MinIO (MINIO_ENDPOINT_EXTERNAL valid)
- [ ] Spark jobs submitted & running
- [ ] Data flowing: Kafka → Bronze → Silver → Gold
- [ ] Predictions written to PostgreSQL
- [ ] Grafana dashboards showing data

---

## 📚 Quick Commands

```bash
# Máy 1
docker compose -f docker-compose.master.yml logs -f <service>
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec spark-master spark-submit --version

# Máy 2
docker compose -f docker-compose.machine2.yml logs -f <service>
docker exec postgres psql -U bigdata_user -d bigdata -c "SELECT COUNT(*) FROM predictions_monitoring;"
docker exec airflow airflow dags list

# General
docker ps
docker stats
docker exec <container> bash
```

---

## 📞 Support

- **Spark Issues**: Check logs → `docker logs spark-master | grep ERROR`
- **Airflow Issues**: Airflow UI → Admin → Logs
- **Database Issues**: `docker exec postgres psql -U bigdata_user -l`
- **Network Issues**: `ping <MACHINE_IP>` từ cả 2 máy

---

**Version**: 2.0 — 2 Machines Setup  
**Last Updated**: May 2026  
**Tested**: Docker Compose 3.8, Spark 3.5, Python 3.11

