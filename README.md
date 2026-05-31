## Bài toán

Dự báo nhu cầu taxi theo từng NYC Taxi Zone bằng dữ liệu streaming.

Luồng chính:

```text
Dataset -> Producer -> Kafka -> Spark Bronze/Silver/Gold -> Predict Service -> PostgreSQL -> Grafana
```

Kết quả xem ở Grafana:

- Map demand theo zone.
- Metric Kafka/Spark/MinIO/Postgres/Predict.
- Trạng thái model MLflow.

## 1. Cài requirements

Chạy ở root project:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Nếu PowerShell chặn script:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## 2. Sửa `.env`

Sửa đúng IP trước khi chạy:

```env
MASTER_IP=IP_MAY_MASTER
WORKER_IP=IP_MAY_WORKER

SPARK_WORKER_A_IP=IP_MAY_WORKER
MINIO_ENDPOINT_EXTERNAL=http://IP_MAY_WORKER:9000
KAFKA_BOOTSTRAP_EXTERNAL=IP_MAY_WORKER:29092
SPARK_MASTER_URL=spark://IP_MAY_MASTER:7077
MLFLOW_TRACKING_URI=http://IP_MAY_MASTER:5000
```

## 3. Chạy Worker

Trên máy Worker:

```powershell
docker compose -f docker-compose.worker.yml up -d --build
```

Worker chạy:

- Kafka
- Zookeeper
- MinIO
- Spark Worker
- node-exporter/cAdvisor

## 4. Chạy Master

Trên máy Master:

```powershell
.\start_pipeline_with_mlflow_upload.ps1
```

Script này sẽ:

- Chạy `docker-compose.master.yml`.
- Upload model lên MLflow nếu thiếu.
- Submit 5 Spark streaming jobs.

Nếu chỉ muốn upload lại model:

```powershell
.\start_pipeline_with_mlflow_upload.ps1 -UploadOnly
```

## 5. Kiểm tra nhanh

```powershell
docker ps
docker logs -f predict-service
```

Kiểm tra Postgres có prediction:

```powershell
docker exec postgres psql -U admin -d bigdata -c "SELECT window_end, COUNT(*) FROM predictions_monitoring GROUP BY window_end ORDER BY window_end DESC LIMIT 5;"
```

Kiểm tra model loaded:

```powershell
docker exec predict-service curl -s http://localhost:8001/metrics
```

## 6. Link sử dụng

Thay `MASTER_IP`, `WORKER_IP` bằng IP trong `.env`.

```text
Grafana:      http://MASTER_IP:3000
Prometheus:   http://MASTER_IP:9090
Spark Master: http://MASTER_IP:8080
MLflow:       http://MASTER_IP:5000
MinIO:        http://WORKER_IP:9001
```

Grafana:

```text
user: admin
password: admin
```

MinIO:

```text
user: minioadmin
password: minioadmin
```

## 7. Dừng

Trên Master:

```powershell
docker compose -f docker-compose.master.yml down
```

Trên Worker:

```powershell
docker compose -f docker-compose.worker.yml down
```

