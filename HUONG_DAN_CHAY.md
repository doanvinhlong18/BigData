# Hướng Dẫn Chạy Hệ Thống NYC Taxi Big Data

Hai máy tính Windows kết nối qua **Radmin VPN** hoặc **Tailscale VPN**.

---

## Yêu Cầu

Cả hai máy cần có:
- Docker Desktop ≥ 24.0 (đang chạy)
- Git (để clone/sync project)
- VPN đã kết nối (Radmin hoặc Tailscale)

Kiểm tra nhanh:
```powershell
docker --version
docker compose version
```

---

## Bước 1 — Xác Định IP VPN

Mở PowerShell trên mỗi máy, chạy:
```powershell
ipconfig
```

Tìm adapter tên **Radmin VPN** hoặc **Tailscale**. Ghi lại IP của từng máy:

| Máy | Vai trò | IP ví dụ |
|-----|---------|----------|
| Laptop 1 | Master | `26.x.x.1` |
| Laptop 2 | Worker | `26.x.x.2` |

Ping kiểm tra kết nối VPN:
```powershell
# Trên máy Master, ping Worker
ping 26.x.x.2

# Trên máy Worker, ping Master
ping 26.x.x.1
```

---

## Bước 2 — Cấu Hình `.env`

### Máy Master

Sửa file `.env` ở thư mục gốc project:
```env
MASTER_IP=26.x.x.1       # IP VPN của máy Master
WORKER_IP=26.x.x.2       # IP VPN của máy Worker

MINIO_ENDPOINT_EXTERNAL=http://26.x.x.1:9000
KAFKA_BOOTSTRAP_EXTERNAL=26.x.x.1:29092
SPARK_MASTER_URL=spark://26.x.x.1:7077
MLFLOW_TRACKING_URI=http://26.x.x.1:5000

DATASET_PATH=D:/data/nyc-taxi   # Thư mục chứa dataset trên máy Master
```

> Giữ nguyên các dòng còn lại, chỉ thay IP.

### Máy Worker

Copy toàn bộ thư mục project sang máy Worker (git clone hoặc rsync), sau đó sửa `.env`:
```env
MASTER_IP=26.x.x.1       # IP VPN của máy Master (giống trên)
WORKER_IP=26.x.x.2       # IP VPN của MÁY WORKER (chính máy này)

SPARK_MASTER_URL=spark://26.x.x.1:7077
WORKER_ID=A
SPARK_WORKER_MEMORY=12G
SPARK_WORKER_CORES=8
```

---

## Bước 3 — Mở Firewall (Windows)

Chạy PowerShell **với quyền Administrator** trên từng máy.

### Máy Master
```powershell
$ports = @(7077,8080,4040,7337,9092,29092,9000,9001,5432,5000,8888,3000,9090,8081)
foreach ($p in $ports) {
    New-NetFirewallRule -DisplayName "BigData-$p" -Direction Inbound `
        -Protocol TCP -LocalPort $p -Action Allow -Profile Any
}
Write-Host "Firewall rules added for Master"
```

### Máy Worker
```powershell
$ports = @(8081,8085,7337,9100,8090)
foreach ($p in $ports) {
    New-NetFirewallRule -DisplayName "BigData-Worker-$p" -Direction Inbound `
        -Protocol TCP -LocalPort $p -Action Allow -Profile Any
}
Write-Host "Firewall rules added for Worker"
```

---

## Bước 4 — Khởi Động Máy Master

### 4a. Build image Spark (lần đầu)
```powershell
cd D:\Project\BigData
docker build -t bigdata-spark:latest ./spark
```

### 4b. Bật/tắt local worker

File `docker-compose.master.yml` có service `spark-worker` (local worker trên cùng máy Master):

- **Chạy 1 máy** (không có máy Worker riêng): giữ nguyên
- **Chạy 2 máy** (có máy Worker riêng): comment out hoặc xóa service `spark-worker` trong `docker-compose.master.yml` để tránh double-register

### 4c. Khởi động toàn bộ dịch vụ Master
```powershell
cd D:\Project\BigData
docker compose -f docker-compose.master.yml up -d --build
```

Theo dõi log để xem có lỗi không:
```powershell
docker compose -f docker-compose.master.yml logs -f --tail=50
```

### 4d. Kiểm tra health

```powershell
docker compose -f docker-compose.master.yml ps
```

Các service quan trọng cần `healthy`:
- `kafka` — chờ ~60 giây
- `minio`
- `postgres`
- `spark-master`

---

## Bước 5 — Khởi Động Máy Worker

Trên máy Worker:

```powershell
cd D:\Project\BigData

# Build Spark image (lần đầu)
docker build -t bigdata-spark:latest ./spark

# Khởi động Worker
docker compose -f docker-compose.worker.yml up -d
```

Xem log Worker:
```powershell
docker compose -f docker-compose.worker.yml logs -f spark-worker
```

Log thành công trông như thế này:
```
INFO Worker: Starting Spark worker 26.x.x.2:8085 with 8 cores, 12.0 GiB RAM
INFO Worker: Successfully registered with master spark://26.x.x.1:7077
```

Kiểm tra Worker đã đăng ký trên Spark UI:
```
http://26.x.x.1:8080
```
Phải thấy Worker `26.x.x.2` trong danh sách Workers.

---

## Bước 6 — Submit Spark Jobs

Sau khi Kafka, MinIO, và Spark Master đều healthy, chạy từ máy Master:

```powershell
docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh
```

Script sẽ submit 5 streaming jobs theo thứ tự:
1. `taxi_kafka_to_bronze.py` — Kafka → Bronze (Delta Lake)
2. `request_bronze_to_silver.py` — Bronze/request → Silver/request
3. `request_to_response_silver.py` — Silver/request ⋈ Bronze/pickup → Silver/response
4. `complete_bronze_to_silver.py` — Silver/response ⋈ Bronze/dropoff → Silver/complete
5. `silver_to_gold.py` — Silver/complete → Gold (window aggregation)

Theo dõi Spark Driver UI:
```
http://26.x.x.1:4040
```

---

## Bước 7 — Truy Cập Các Dịch Vụ

Truy cập từ bất kỳ máy nào trong VPN:

| Dịch vụ | URL | Tài khoản |
|---------|-----|-----------|
| **Spark Master UI** | `http://26.x.x.1:8080` | — |
| **Spark Driver UI** | `http://26.x.x.1:4040` | — |
| **MinIO Console** | `http://26.x.x.1:9001` | `minioadmin / minioadmin` |
| **Airflow** | `http://26.x.x.1:8888` | `admin / admin` |
| **MLflow** | `http://26.x.x.1:5000` | — |
| **Grafana** | `http://26.x.x.1:3000` | `admin / admin` |
| **Prometheus** | `http://26.x.x.1:9090` | — |
| **Spark Worker UI** | `http://26.x.x.2:8081` | — |

> Thay `26.x.x.1` và `26.x.x.2` bằng IP VPN thực của bạn.

---

## Bước 8 — Tắt Hệ Thống

### Tắt Worker trước
```powershell
# Trên máy Worker
docker compose -f docker-compose.worker.yml down
```

### Tắt Master
```powershell
# Trên máy Master
docker compose -f docker-compose.master.yml down
```

### Xóa toàn bộ data (reset hoàn toàn)
```powershell
# Trên máy Master — XÓA HẾT volumes!
docker compose -f docker-compose.master.yml down -v
```

---

## Xử Lý Lỗi Thường Gặp

### Worker không đăng ký được với Master

```
ERROR Worker: All masters are unresponsive
```

Nguyên nhân: Firewall chặn port 7077 hoặc VPN mất kết nối.

Kiểm tra:
```powershell
# Trên máy Worker, kiểm tra kết nối tới Master
Test-NetConnection -ComputerName 26.x.x.1 -Port 7077
# Kết quả phải là: TcpTestSucceeded: True
```

---

### Executor không connect được về Driver

```
ERROR BlockManager: Failed to connect to driver
```

Nguyên nhân: Port 7337 chưa được mở hoặc `MASTER_IP` chưa đúng trong `.env`.

Kiểm tra:
```powershell
# Trên máy Master, xem driver đang dùng IP nào
docker exec spark-master env | grep MASTER_IP
```

---

### Spark job không đọc được MinIO

```
ERROR S3AFileSystem: Unable to connect to http://minio:9000
```

Nguyên nhân: `extra_hosts` trong `docker-compose.worker.yml` chưa map đúng IP.

Kiểm tra từ container Worker:
```powershell
docker exec spark-worker-A ping minio
# Phải resolve về MASTER_IP (26.x.x.1)
```

---

### Kafka không nhận được message từ Worker

Worker cần kết nối tới Kafka qua external listener `MASTER_IP:29092`.

Kiểm tra:
```powershell
# Test từ máy Worker
Test-NetConnection -ComputerName 26.x.x.1 -Port 29092
```

---

### Xem log toàn bộ hệ thống

```powershell
# Master
docker compose -f docker-compose.master.yml logs -f

# Worker
docker compose -f docker-compose.worker.yml logs -f

# Một service cụ thể
docker logs spark-master -f --tail=100
docker logs kafka -f --tail=50
```

---

## Cập Nhật Code Spark Jobs

Khi sửa code trong `spark/jobs/`:

1. Dừng các streaming jobs đang chạy (Ctrl+C trong terminal submit-jobs.sh)
2. Không cần rebuild image (jobs được mount làm volume)
3. Submit lại:
```powershell
docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh
```

Nếu sửa Dockerfile hoặc dependencies trong `spark/`:
```powershell
# Build lại image trên CẢ HAI máy
docker build -t bigdata-spark:latest ./spark

# Khởi động lại service
docker compose -f docker-compose.master.yml up -d spark-master spark-worker
docker compose -f docker-compose.worker.yml up -d spark-worker
```
