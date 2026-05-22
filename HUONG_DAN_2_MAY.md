# Hướng Dẫn Chạy 2 Máy — Master + Worker
# NYC Taxi Big Data Pipeline

> **Setup**: Laptop 1 (Master) + Laptop 2 (Worker), kết nối qua Radmin VPN hoặc Tailscale VPN

---

## Phân Chia Vai Trò

| Máy | Vai trò | Chạy gì |
|-----|---------|---------|
| Laptop 1 — Master | Hạ tầng chính | Kafka, Zookeeper, MinIO, Spark Master, PostgreSQL, Airflow, MLflow, Prometheus, Grafana, Producers, Predict Service |
| Laptop 2 — Worker | Tính toán Spark | Spark Worker (nhận task từ Master, đọc Kafka, ghi MinIO) |

> spark-worker trong docker-compose.master.yml phải để COMMENT khi chạy 2 máy.

---

## BƯỚC 0 — Xác Định IP VPN

Chạy trên mỗi máy:
```powershell
ipconfig
```

Tìm adapter tên "Radmin VPN" hoặc "Tailscale".

| Máy | IP VPN (ví dụ) |
|-----|---------------|
| Laptop 1 (Master) | 26.x.x.1 |
| Laptop 2 (Worker) | 26.x.x.2 |

Kiểm tra 2 máy thấy nhau:
```powershell
# Trên Master
ping 26.x.x.2

# Trên Worker
ping 26.x.x.1
```

---

## BƯỚC 1 — Cấu Hình .env

### LAPTOP 1 (Master) — sửa file .env:

```env
MASTER_IP=26.x.x.1          # IP VPN của MÁY NÀY (Master)
WORKER_IP=26.x.x.2          # IP VPN của máy Worker

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_ENDPOINT_EXTERNAL=http://26.x.x.1:9000

KAFKA_BOOTSTRAP_INTERNAL=kafka:9092
KAFKA_BOOTSTRAP_EXTERNAL=26.x.x.1:29092

SPARK_MASTER_URL=spark://26.x.x.1:7077
SPARK_WORKER_MEMORY=12G
SPARK_WORKER_CORES=8

POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=bigdata

AIRFLOW_FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW_SECRET_KEY=bigdata-secret-key-2025

MLFLOW_TRACKING_URI=http://26.x.x.1:5000

SPEED_FACTOR=60
DATASET_PATH=D:/data/nyc-taxi    # THAY ĐỔI đường dẫn dataset

WORKER_ID=A

WEATHER_PARQUET_PATH=s3://weather/parquet
WEATHER_CSV_PATH=/datasets/weather/2526.csv
```

### LAPTOP 2 (Worker) — sau khi clone project, sửa .env:

```env
MASTER_IP=26.x.x.1          # IP VPN của máy MASTER (máy kia)
WORKER_IP=26.x.x.2          # IP VPN của MÁY NÀY (Worker)

SPARK_MASTER_URL=spark://26.x.x.1:7077
SPARK_WORKER_MEMORY=12G     # tuỳ RAM máy Worker (để lại ~4GB cho OS)
SPARK_WORKER_CORES=8        # tuỳ số cores
WORKER_ID=A

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=bigdata
AIRFLOW_FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW_SECRET_KEY=bigdata-secret-key-2025
```

---

## BƯỚC 2 — Mở Firewall (PowerShell Admin, từng máy)

### Máy Master:
```powershell
$ports = @(7077, 8080, 4040, 7337, 9092, 29092, 9000, 9001, 5432, 5000, 8888, 3000, 9090, 9115, 9102, 8090)
foreach ($p in $ports) {
    New-NetFirewallRule -DisplayName "BigData-Master-$p" -Direction Inbound `
        -Protocol TCP -LocalPort $p -Action Allow -Profile Any -ErrorAction SilentlyContinue
}
Write-Host "Master firewall OK"
```

### Máy Worker:
```powershell
$ports = @(8081, 8085, 7337, 9100, 8090)
foreach ($p in $ports) {
    New-NetFirewallRule -DisplayName "BigData-Worker-$p" -Direction Inbound `
        -Protocol TCP -LocalPort $p -Action Allow -Profile Any -ErrorAction SilentlyContinue
}
Write-Host "Worker firewall OK"
```

---

## BƯỚC 3 — Cập Nhật Prometheus (trên Master)

QUAN TRỌNG: File monitoring/prometheus/prometheus.yml dùng IP tĩnh, không đọc được biến .env.
Phải sửa thủ công 3 chỗ sau, thay 192.168.10.2 bằng WORKER_IP thực:

- Dòng 72:  targets: ["26.x.x.2:8081"]   (spark-worker-remote)
- Dòng 162: targets: ["26.x.x.2:9100"]   (node-exporter-worker)
- Dòng 170: targets: ["26.x.x.2:8090"]   (cadvisor-worker)

---

## BƯỚC 4 — Build Spark Image (TRÊN CẢ HAI MÁY)

```powershell
cd D:\Project\BigData
docker build -t bigdata-spark:latest ./spark
```

Lần đầu mất 5-10 phút. Cả hai máy phải có cùng image bigdata-spark:latest.

---

## BƯỚC 5 — Khởi Động Máy Master (TRƯỚC)

```powershell
cd D:\Project\BigData
docker compose -f docker-compose.master.yml up -d --build
```

Theo dõi:
```powershell
docker compose -f docker-compose.master.yml ps
docker compose -f docker-compose.master.yml logs -f --tail=20
```

Chờ ~2-3 phút, tất cả service phải hiển thị "(healthy)":
- kafka — chờ ~60-90 giây
- minio
- postgres
- spark-master

Xác nhận: http://localhost:8080 → Spark Master UI, thấy "Workers: 0"

---

## BƯỚC 5.5 — Xác Nhận Kafka Topic Đã Được Tạo

Sau khi Master up, container `kafka-setup` tự động tạo topic `nyc_taxi_events`.
Xác nhận topic đã tồn tại trước khi submit jobs:

```powershell
# Kiểm tra kafka-setup chạy thành công
docker logs kafka-setup
# Phải thấy:
#   Created topic nyc_taxi_events.
#   Topics OK
#   nyc_taxi_events
```

Nếu thấy đúng output trên → **bỏ qua phần tạo thủ công bên dưới**, sang Bước 6.

---

### Tạo Topic Thủ Công (chỉ làm khi kafka-setup bị lỗi)

Kiểm tra lý do kafka-setup lỗi:
```powershell
docker logs kafka-setup
docker inspect kafka-setup --format='{{.State.ExitCode}}'
```

Tạo topic trực tiếp trong container kafka:
```powershell
# Tạo topic nyc_taxi_events
docker exec kafka kafka-topics `
  --create `
  --if-not-exists `
  --bootstrap-server localhost:9092 `
  --topic nyc_taxi_events `
  --partitions 3 `
  --replication-factor 1 `
  --config retention.ms=172800000
```

Xác nhận topic đã tồn tại:
```powershell
# Liệt kê tất cả topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
# Phải thấy: nyc_taxi_events

# Xem chi tiết topic
docker exec kafka kafka-topics `
  --describe `
  --bootstrap-server localhost:9092 `
  --topic nyc_taxi_events
# Phải thấy: PartitionCount: 3, ReplicationFactor: 1
```

## BƯỚC 6 — Khởi Động Máy Worker (SAU KHI Master healthy)

```powershell
cd D:\Project\BigData
docker compose -f docker-compose.worker.yml up -d
```

Xem log:
```powershell
docker logs spark-worker-A -f --tail=50
```

Dấu hiệu đăng ký thành công:
```
INFO Worker: Starting Spark worker 26.x.x.2:8085 with 8 cores, 12.0 GiB RAM
INFO Worker: Successfully registered with master spark://26.x.x.1:7077
```

Xác nhận tại: http://26.x.x.1:8080 → phải thấy Workers: 1, spark-worker-A trạng thái ALIVE.

---

## BƯỚC 7 — Upload Model ML vào MLflow

```powershell
cd D:\Project\BigData
pip install mlflow lightgbm boto3 psycopg2-binary
$env:MLFLOW_TRACKING_URI = "http://localhost:5000"
python upload_model_to_mlflow.py
```

Kết quả: 2 model được register với stage Production.
Kiểm tra: http://localhost:5000 → tab Models.

---

## BƯỚC 8 — Submit Spark Streaming Jobs

```powershell
docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh
```

Monitor:
- http://26.x.x.1:8080 — Spark Master UI
- http://26.x.x.1:4040 — Spark Driver UI (streaming progress)

---

## THỨ TỰ CHẠY VÀ DEPENDENCY CÁC SPARK JOB

### Sơ Đồ Dependency

```
Kafka (topic: nyc_taxi_events)
    |
    v
[JOB 1] taxi_kafka_to_bronze.py
    |
    |-- ghi --> s3a://bronze/request/    --.
    |-- ghi --> s3a://bronze/pickup/     --|-- đọc bởi job 2, 3, 4
    '-- ghi --> s3a://bronze/dropoff/   --'
         |
         |
    .----'----------------------------.
    |                                 |
    v                                 v
[JOB 2]                          [JOB 3]
request_bronze_to_silver.py      request_to_response_silver.py
    |                                 |
    | đọc: bronze/request             | đọc: silver/request (từ Job 2)
    | ghi: silver/request             |       bronze/pickup  (từ Job 1)
    |                                 | ghi: silver/response
    |                                 |
    '---- cả 2 cần xong ---.          |
                            v          v
                        [JOB 4]
                        complete_bronze_to_silver.py
                            |
                            | đọc: silver/response (từ Job 3)
                            |       bronze/dropoff  (từ Job 1)
                            | ghi: silver/complete
                            |
                            v
                        [JOB 5]
                        silver_to_gold.py
                            |
                            | đọc: silver/complete (từ Job 4)  [stream]
                            |       silver/response (từ Job 3)  [batch read]
                            | ghi: gold/aggregated (UPSERT/MERGE)
                            |
                            v
                    Predict Service (poll mỗi 15 giây)
                            |
                            v
                    PostgreSQL predictions table
                            |
                            v
                        Grafana Dashboard
```

### Chi Tiết Từng Job

| # | File | Đọc từ | Ghi vào | Loại join | Trigger |
|---|------|--------|---------|-----------|---------|
| 1 | taxi_kafka_to_bronze.py | Kafka: nyc_taxi_events | bronze/request, bronze/pickup, bronze/dropoff | foreachBatch (split theo event_type) | 2 giây |
| 2 | request_bronze_to_silver.py | bronze/request | silver/request | Stream → Filter/Dedup | 1 giây |
| 3 | request_to_response_silver.py | silver/request + bronze/pickup | silver/response | Stream-Stream INNER JOIN (trip_id, window 2h) | 2 giây |
| 4 | complete_bronze_to_silver.py | silver/response + bronze/dropoff | silver/complete | Stream-Stream INNER JOIN (trip_id, window 2h) | 5 giây |
| 5 | silver_to_gold.py | silver/complete (stream) + silver/response (batch) | gold/aggregated | foreachBatch + sliding window 60min/15min + MERGE UPSERT | 15 giây |

### Vòng Đời Dữ Liệu Một Chuyến Đi

```
Thực tế:  [REQUEST] --> [PICKUP] --> [DROPOFF]
              |              |            |
              v              v            v
Kafka:    event_type=request  pickup   dropoff  (cùng topic, 3 partitions)
              |              |            |
              v              v            v
Bronze:   bronze/request  bronze/pickup  bronze/dropoff
              |              |            |
              |              |            |
Job 2 -->  silver/request    |            |
              |              |            |
              '----JOIN (trip_id)----'    |
Job 3 -->       silver/response          |
                      |                  |
                      '----JOIN (trip_id)--'
Job 4 -->               silver/complete
                               |
Job 5 -->          gold/aggregated (window agg 60min/15min)
                               |
Predict -->       predictions (PostgreSQL)
```

### Watermark và Late Data

| Job | Watermark | Ý nghĩa |
|-----|-----------|---------|
| Job 2 | request_datetime: 15 phút | Cho phép event đến muộn tối đa 15 phút |
| Job 3 | request: 30 phút, pickup: 15 phút | Pickup phải đến trong 2h sau request |
| Job 4 | pickup: 24 giờ, dropoff: 15 phút | Dropoff phải đến trong 2h sau pickup |
| Job 5 | dropoff: 15 phút | Window 60min/15min, UPSERT khi late data |

### Tại Sao Thứ Tự Quan Trọng?

- **Job 1 phải chạy đầu tiên**: Các job còn lại đọc Delta table từ Bronze.
  Nếu Job 1 chưa ghi data thì các job 2-4 sẽ lỗi `DELTA_SCHEMA_NOT_SET`.

- **Job 2 và Job 3 phải chạy trước Job 4**: Job 4 đọc cả silver/response (từ Job 3)
  lẫn bronze/dropoff (từ Job 1). Nếu silver/response chưa có schema thì Job 4 crash.

- **Job 5 phải chạy cuối**: Đọc silver/complete (từ Job 4) và silver/response (từ Job 3).

- **Trong thực tế**: `submit-jobs.sh` submit tất cả 5 jobs cùng lúc (sleep 3-5 giây giữa
  các lần submit). Điều này hoạt động vì Spark Structured Streaming tự retry khi
  Delta table chưa có schema — chỉ lần đầu startup có thể mất vài giây cho jobs
  sau "chờ" jobs trước ghi batch đầu tiên vào Bronze/Silver.

### Nếu Gặp Lỗi DELTA_SCHEMA_NOT_SET

Nguyên nhân: Job 2/3/4/5 start trước khi Job 1 ghi được batch đầu vào Bronze.
Giải pháp: Submit lần lượt thay vì đồng thời:

```powershell
# Submit Job 1 trước, chờ ~30 giây cho Bronze có data
docker exec spark-master bash -c "
  JOBS=/opt/spark/app/spark/jobs
  JARS='/opt/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/delta-storage-3.1.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.517.jar'
  BASE='spark-submit --master spark://spark-master:7077 --deploy-mode client --jars \$JARS \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.driver.host=\${MASTER_IP:-spark-master} \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.blockManager.port=7337'
  eval \"\$BASE \$JOBS/taxi_kafka_to_bronze.py\" &
  sleep 30
  eval \"\$BASE \$JOBS/request_bronze_to_silver.py\" &
  sleep 5
  eval \"\$BASE \$JOBS/request_to_response_silver.py\" &
  sleep 10
  eval \"\$BASE \$JOBS/complete_bronze_to_silver.py\" &
  sleep 10
  eval \"\$BASE \$JOBS/silver_to_gold.py\" &
  wait
"
```

---

## BƯỚC 9 — Bật Airflow DAGs

Truy cập http://26.x.x.1:8888 (admin/admin):
1. DAGs → monitoring_dag → bật toggle ON
2. retrain_dag → bật nếu muốn tự động re-train

DAGs chỉ có data để xử lý sau khi Gold layer được populate (~10-15 phút từ khi jobs chạy).

---

## Timeline Mong Đợi

```
T+00:00  Master up, Worker đăng ký vào Spark Master
T+02:00  Submit-jobs.sh: 5 jobs start, Producers gửi events vào Kafka
T+05:00  Bronze layer có data (MinIO: bronze/request, bronze/pickup, bronze/dropoff)
T+06:00  Silver/request populated (Job 2 chạy)
T+08:00  Silver/response populated (Job 3: request JOIN pickup)
T+10:00  Silver/complete populated (Job 4: response JOIN dropoff)
T+12:00  Gold/aggregated xuất hiện (Job 5: sliding window 60min, mỗi 15 giây)
T+12:15  Predict Service detect gold data mới → inference → ghi vào PostgreSQL
T+13:00  Grafana http://26.x.x.1:3000 hiển thị predictions đầu tiên
T+30:00  monitoring_dag chạy lần đầu → kiểm tra model performance
```

---

## Tất Cả URLs

Truy cập từ bất kỳ máy nào trong VPN (thay 26.x.x.1 = IP Master thực):

| Dịch vụ | URL | Login |
|---------|-----|-------|
| Spark Master UI | http://26.x.x.1:8080 | — |
| Spark Driver UI | http://26.x.x.1:4040 | — |
| Spark Worker UI | http://26.x.x.2:8081 | — |
| MinIO Console | http://26.x.x.1:9001 | minioadmin / minioadmin |
| Airflow | http://26.x.x.1:8888 | admin / admin |
| MLflow | http://26.x.x.1:5000 | — |
| Grafana | http://26.x.x.1:3000 | admin / admin |
| Prometheus | http://26.x.x.1:9090 | — |

---

## Dừng Hệ Thống

```powershell
# 1. Dừng Worker trước (trên máy Worker)
docker compose -f docker-compose.worker.yml down

# 2. Dừng Master (trên máy Master)
docker compose -f docker-compose.master.yml down

# 3. Reset hoàn toàn — XOÁ data (trên máy Master)
docker compose -f docker-compose.master.yml down -v
```

---

## Xử Lý Lỗi Thường Gặp

### Worker không đăng ký được với Master

```powershell
# Trên máy Worker — kiểm tra kết nối VPN
Test-NetConnection -ComputerName 26.x.x.1 -Port 7077
# TcpTestSucceeded phải là: True

# Kiểm tra WORKER_IP đúng chưa
docker exec spark-worker-A env | Select-String "SPARK_DAEMON_JAVA_OPTS"
# Phải thấy: -Dspark.local.hostname=26.x.x.2
```

Trong Spark UI, address của worker phải là 26.x.x.2:8085 (không phải 172.x.x.x).

### Spark Jobs không đọc được MinIO

```powershell
# Từ máy Worker
Invoke-WebRequest http://26.x.x.1:9000/minio/health/live

# Trong container Worker — minio phải resolve về MASTER_IP
docker exec spark-worker-A ping minio -c 3
```

Kiểm tra extra_hosts trong docker-compose.worker.yml — biến ${MASTER_IP} phải đúng.

### Executor không liên lạc được Driver (port 7337)

```powershell
# Trên máy Worker
Test-NetConnection -ComputerName 26.x.x.1 -Port 7337
# Trên máy Master
netstat -ano | findstr ":7337"
```

### Lỗi NoSuchMethodException: CheckpointFileManager

Nguyên nhân: Dòng sai trong spark-defaults.conf:
```
spark.sql.streaming.checkpointFileManagerClass  org.apache.spark.sql.execution.streaming.CheckpointFileManager
```
Xoá dòng này, rebuild image Spark và restart.

### Lỗi DELTA_SCHEMA_NOT_SET

Nguyên nhân: Job sau start trước khi Job trước ghi được batch đầu.
Giải pháp: Submit Job 1 trước, chờ 30 giây, rồi submit các job còn lại.

### Kafka từ Worker không kết nối được

```powershell
# Trên máy Worker
Test-NetConnection -ComputerName 26.x.x.1 -Port 29092
```

Kiểm tra KAFKA_ADVERTISED_LISTENERS trong docker-compose.master.yml:
```
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://${MASTER_IP}:29092
```

---

## Checklist Hoàn Chỉnh

### Trước khi bắt đầu:
- [ ] Cả 2 máy có Docker Desktop đang chạy
- [ ] VPN kết nối, 2 máy ping được nhau
- [ ] IP VPN đã ghi lại

### Máy Master:
- [ ] .env: MASTER_IP = IP VPN máy này, DATASET_PATH = đường dẫn dataset thực
- [ ] spark-worker block đang bị COMMENT trong docker-compose.master.yml
- [ ] prometheus.yml: 3 dòng WORKER_IP đã cập nhật (dòng 72, 162, 170)
- [ ] docker build -t bigdata-spark:latest ./spark OK
- [ ] docker compose -f docker-compose.master.yml up -d --build OK
- [ ] Kafka, MinIO, Postgres, Spark Master đều healthy

### Máy Worker:
- [ ] .env: MASTER_IP = IP VPN máy Master, WORKER_IP = IP VPN máy này
- [ ] Firewall ports mở (8081, 8085, 7337, 9100, 8090)
- [ ] docker build -t bigdata-spark:latest ./spark OK
- [ ] docker compose -f docker-compose.worker.yml up -d OK
- [ ] Worker xuất hiện trong Spark UI ALIVE, address = WORKER_IP:8085

### Pipeline:
- [ ] python upload_model_to_mlflow.py → 2 model Production
- [ ] docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh OK
- [ ] Spark UI: 5 Streaming apps đang Running
- [ ] MinIO bronze/silver/gold có data
- [ ] PostgreSQL predictions table có rows
- [ ] Grafana dashboard có data

---

*NYC Taxi Big Data — 2 Machine Distributed Setup*
*Spark 3.5.1 + Delta Lake 3.1.0 + Kafka 7.5.0 + MinIO*
