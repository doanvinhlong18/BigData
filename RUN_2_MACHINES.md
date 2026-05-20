# Distributed 2-Machine Setup Guide

Two Windows laptops connected via **Radmin VPN** or **Tailscale VPN**.

Example IPs used throughout:
- **Master**: `192.168.10.1`
- **Worker**: `192.168.10.2`

Replace these with your actual VPN IPs everywhere you see them.

---

## Architecture: What Runs Where

### Master Machine
| Service | Purpose |
|---------|---------|
| Zookeeper | Kafka coordination |
| Kafka | Message broker (topics: `nyc_taxi_events`) |
| MinIO | S3-compatible object storage (bronze/silver/gold/mlflow-artifacts) |
| Spark Master | Cluster coordinator, accepts worker registrations |
| spark-worker-local | **Optional** local worker; disable if you have a dedicated worker machine |
| PostgreSQL | Shared backend for Airflow, MLflow, predictions |
| Airflow | DAG orchestration (`monitoring_dag`, `retrain_dag`) |
| predict-service | Real-time prediction loop (polls gold/ every 15 s) |
| MLflow | Experiment tracking + model registry |
| Prometheus | Metrics collection (scrapes master + remote worker) |
| Grafana | Dashboards |
| Producers | `request-producer`, `pickup-producer`, `dropoff-producer` → Kafka |

### Worker Machine
| Service | Purpose |
|---------|---------|
| spark-worker | Registers with master, runs executors that process Kafka streams |
| node-exporter | Host metrics scraped by Prometheus on master |
| cadvisor-worker | Container metrics scraped by Prometheus on master |

---

## How the Distributed System Works

### Kafka → Spark distribution
```
Producers (master) → Kafka topic (3 partitions) → Spark Structured Streaming
                                                         ↓
                                              Each partition = 1 Spark task
                                              Tasks run as executors on Worker machine
                                              Executors write to MinIO on master via S3A
```

Kafka has 3 partitions. Spark assigns one task per partition. With 1 worker running those tasks all go to the same worker. With 2+ workers, Spark distributes them across workers.

### How Spark jobs are distributed
```
Airflow (master)
  └─ spark-submit --master spark://spark-master:7077 --deploy-mode client
        │
        ├─ Driver runs in the spark-master container (client mode)
        │   advertises MASTER_IP:4040 to the cluster
        │
        └─ Spark Master assigns executors to Worker(s)
              └─ Executor on Worker reads Kafka at MASTER_IP:29092
              └─ Executor writes Parquet to MinIO at http://minio:9000
                 (minio resolves to MASTER_IP via extra_hosts)
```

### How networking works between machines
```
WORKER MACHINE (VPN: 192.168.10.2)
  └─ spark-worker container
       ├─ Connects OUT to: spark://192.168.10.1:7077  ← Worker registration
       ├─ Connects OUT to: 192.168.10.1:9092/29092    ← Kafka consumer
       ├─ Connects OUT to: 192.168.10.1:9000          ← MinIO write (via extra_hosts minio→IP)
       └─ Listens on:      WORKER_IP:8085             ← Master sends "launch executor" here
                           WORKER_IP:7337             ← Driver block manager communication
                           WORKER_IP:8081             ← Worker Web UI + Prometheus scrape

MASTER MACHINE (VPN: 192.168.10.1)
  ├─ Kafka:        0.0.0.0:9092 (internal) + 0.0.0.0:29092 (external)
  ├─ Spark Master: 0.0.0.0:7077  (workers connect here)
  ├─ Spark Driver: 0.0.0.0:4040 + 0.0.0.0:7337  (executors call back here)
  └─ MinIO:        0.0.0.0:9000
```

**Key networking trick for Windows Docker Desktop:**
Docker containers use an internal bridge IP (`172.x.x.x`) that is NOT routable via VPN.
The worker container advertises the **Windows host's VPN IP** (`WORKER_IP`) to the cluster
via `spark.local.hostname`, while binding to `0.0.0.0` inside the container.
Docker Desktop forwards `WORKER_IP:8085` and `WORKER_IP:7337` to the container.

---

## Port Reference

### Master machine — open these in Windows Firewall
```
7077   Spark Master RPC (workers register here)
8080   Spark Master Web UI
4040   Spark Driver Web UI (client-mode jobs)
7337   Driver Block Manager (executors contact driver here)
29092  Kafka external listener (worker's Kafka consumers)
9000   MinIO API (S3A writes from worker)
9001   MinIO Console
5432   PostgreSQL (optional, for remote tools)
5000   MLflow
8888   Airflow Web UI
9090   Prometheus
3000   Grafana
```

### Worker machine — open these in Windows Firewall
```
8081   Spark Worker Web UI + Prometheus scrape
8085   Spark Worker RPC (master sends executor launch here)
7337   Executor Block Manager (driver contacts executors here)
9100   Node Exporter (Prometheus scrape)
8090   cAdvisor (Prometheus scrape, optional)
```

---

## Step 1: Set your VPN IPs

Find your VPN IPs:
- **Radmin VPN**: Open Radmin VPN → your IP shows at the top
- **Tailscale**: `tailscale ip -4` in PowerShell

Edit `.env` on **both machines**. The file is in the project root.

On **master** machine `.env`:
```env
MASTER_IP=192.168.10.1    # ← this machine's VPN IP
WORKER_IP=192.168.10.2    # ← the other machine's VPN IP
```

On **worker** machine `.env`:
```env
MASTER_IP=192.168.10.1    # ← master machine's VPN IP
WORKER_IP=192.168.10.2    # ← this machine's VPN IP
WORKER_ID=A               # A, B, C... if you have multiple workers
SPARK_WORKER_MEMORY=12G   # leave ~4G for OS + Docker Desktop
SPARK_WORKER_CORES=8
```

---

## Step 2: Update Prometheus worker targets

`monitoring/prometheus/prometheus.yml` has static IPs (Prometheus can't read env vars).
Open the file and replace `192.168.10.2` with your actual `WORKER_IP`:

```yaml
# Find these three jobs and update the targets:
- job_name: "spark-worker-remote"
  static_configs:
    - targets: ["YOUR_WORKER_IP:8081"]   # ← change here

- job_name: "node-exporter-worker"
  static_configs:
    - targets: ["YOUR_WORKER_IP:9100"]   # ← change here

- job_name: "cadvisor-worker"
  static_configs:
    - targets: ["YOUR_WORKER_IP:8090"]   # ← change here
```

---

## Step 3: Open firewall ports (both machines)

Run in PowerShell **as Administrator** on both machines:

```powershell
# Master machine — run these
$masterPorts = @(7077, 8080, 4040, 7337, 29092, 9000, 9001, 5432, 5000, 8888, 9090, 3000)
foreach ($port in $masterPorts) {
    New-NetFirewallRule -DisplayName "BigData-$port" -Direction Inbound `
        -Protocol TCP -LocalPort $port -Action Allow -Profile Any
}

# Worker machine — run these
$workerPorts = @(8081, 8085, 7337, 9100, 8090)
foreach ($port in $workerPorts) {
    New-NetFirewallRule -DisplayName "BigData-$port" -Direction Inbound `
        -Protocol TCP -LocalPort $port -Action Allow -Profile Any
}
```

---

## Step 4: Start the Master

On the **master machine**:

```powershell
cd BigData

# If you have a dedicated worker machine, disable the local worker:
# In docker-compose.master.yml, comment out the spark-worker service block.

docker compose -f docker-compose.master.yml up --build -d
```

Check containers are running:
```powershell
docker ps --format "table {{.Names}}`t{{.Status}}"
```

Verify services:
```
Spark UI:   http://192.168.10.1:8080
MinIO:      http://192.168.10.1:9001  (minioadmin / minioadmin)
Airflow:    http://192.168.10.1:8888  (admin / admin)
MLflow:     http://192.168.10.1:5000
Prometheus: http://192.168.10.1:9090
Grafana:    http://192.168.10.1:3000  (admin / admin)
```

---

## Step 5: Build and start the Worker

On the **worker machine** (needs the same project source code — clone/copy the repo):

```powershell
cd BigData

# Build the custom Spark image (needed once, or after spark/Dockerfile changes)
docker build -t bigdata-spark:latest ./spark

# Start worker
docker compose -f docker-compose.worker.yml up -d
```

Check the worker started:
```powershell
docker logs spark-worker-A --tail 50
```

You should see output like:
```
Starting Spark worker ... with 12288 MB, 8 cores
Successfully registered with master spark://spark-master:7077
```

Open the Spark Master UI on the master machine:
```
http://192.168.10.1:8080
```

The **Workers** section should show `spark-worker-A` with status `ALIVE`.

---

## Step 6: Upload model to MLflow

On any machine with the model file `lgb_final_model.txt`:

```powershell
$env:MLFLOW_TRACKING_URI = "http://192.168.10.1:5000"
python upload_model_to_mlflow.py
```

---

## Step 7: Submit Spark streaming jobs

Run inside the `spark-master` container on the **master machine**:

```powershell
docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh
```

This submits 5 concurrent Structured Streaming jobs. Monitor them:
- **Spark UI**: `http://192.168.10.1:8080` → click on any running app
- **Driver UI**: `http://192.168.10.1:4040` → shows streaming micro-batch progress

---

## MinIO S3A Configuration

These settings are in `spark/conf/spark-defaults.conf` and are automatically applied:

```properties
spark.hadoop.fs.s3a.endpoint              http://minio:9000
spark.hadoop.fs.s3a.access.key            minioadmin
spark.hadoop.fs.s3a.secret.key            minioadmin
spark.hadoop.fs.s3a.path.style.access     true
spark.hadoop.fs.s3a.impl                  org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.fast.upload           true
spark.hadoop.fs.s3a.multipart.size        32m
spark.hadoop.fs.s3a.connection.maximum    100
spark.hadoop.fs.s3a.threads.max           20
```

`minio` resolves to `MASTER_IP` on the worker machine via `extra_hosts` in `docker-compose.worker.yml`.

For testing S3A access manually from the worker:
```powershell
# Test MinIO is reachable from worker machine
curl http://192.168.10.1:9000/minio/health/live
```

---

## Spark-submit Examples

### Example 1: Structured Streaming (client mode — driver on master)
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=${MASTER_IP} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.blockManager.port=7337 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --jars /opt/spark/jars/delta-spark_2.12-3.1.0.jar,\
/opt/spark/jars/delta-storage-3.1.0.jar,\
/opt/spark/jars/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/aws-java-sdk-bundle-1.12.517.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/app/spark/jobs/taxi_kafka_to_bronze.py
```

### Example 2: Batch job (cluster mode — driver on Spark cluster)
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --conf spark.blockManager.port=7337 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  /opt/spark/app/spark/jobs/silver_to_gold.py
```

### Spark Structured Streaming config reference
```python
# In your Spark job Python file:
spark = SparkSession.builder \
    .appName("NYC-Taxi-Streaming") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.blockManager.port", "7337") \
    .getOrCreate()

# Kafka source — use external bootstrap server so executors on worker
# connect directly to Kafka on the master
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nyc_taxi_events") \
    .option("startingOffsets", "latest") \
    .load()

# Write to MinIO (minio hostname resolves to MASTER_IP via extra_hosts)
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3a://checkpoints/bronze") \
    .outputMode("append") \
    .start("s3a://bronze/taxi_events")

query.awaitTermination()
```

---

## Troubleshooting Checklist

### Worker does not appear in Spark UI

1. **Verify VPN connectivity** (from worker machine):
   ```powershell
   ping 192.168.10.1
   Test-NetConnection -ComputerName 192.168.10.1 -Port 7077
   ```

2. **Verify WORKER_IP is set correctly** in `.env` on worker machine:
   ```powershell
   # In the project directory on the worker machine:
   type .env | Select-String "WORKER_IP"
   ```
   Should show your worker machine's actual VPN IP.

3. **Check worker logs** on worker machine:
   ```powershell
   docker logs spark-worker-A --tail 100
   ```
   Look for `Successfully registered with master` or any error about the master address.

4. **Verify published ports are accessible** from master:
   ```powershell
   # Run this on the MASTER machine:
   Test-NetConnection -ComputerName 192.168.10.2 -Port 8081
   Test-NetConnection -ComputerName 192.168.10.2 -Port 8085
   ```

5. **Check Docker Desktop is forwarding ports** on worker machine:
   ```powershell
   netstat -ano | findstr ":8081"
   netstat -ano | findstr ":8085"
   ```
   Should show `0.0.0.0:8081` LISTENING.

### Kafka consumer error from worker / executors

1. **Verify Kafka external listener** is working (from worker machine):
   ```powershell
   Test-NetConnection -ComputerName 192.168.10.1 -Port 29092
   ```

2. **Verify KAFKA_LISTENERS is set** in `docker-compose.master.yml` Kafka service:
   ```yaml
   KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
   ```
   Restart Kafka if you added this after first run: `docker restart kafka`

3. **Test Kafka from worker** using kafkacat or:
   ```bash
   docker exec spark-worker-A bash -c \
     "apt-get install -y kafkacat 2>/dev/null; \
      kafkacat -b 192.168.10.1:29092 -L"
   ```

### Worker cannot write to MinIO

1. **Test MinIO from worker machine**:
   ```powershell
   Invoke-WebRequest http://192.168.10.1:9000/minio/health/live
   ```

2. **Verify extra_hosts in docker-compose.worker.yml**:
   ```yaml
   extra_hosts:
     - "minio:192.168.10.1"   # MASTER_IP
   ```
   Inside the container: `docker exec spark-worker-A ping minio`

3. **Verify S3A credentials match** MinIO:
   ```
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin
   ```

### Executor tasks fail immediately after launch

This usually means the master cannot reach the executor/driver on the worker.

1. **Check blockManager.port is published** in `docker-compose.worker.yml`:
   ```yaml
   ports:
     - "7337:7337"
   ```

2. **Verify spark.local.hostname is being passed** to worker JVM:
   ```powershell
   docker exec spark-worker-A env | Select-String "SPARK"
   ```
   Should show `SPARK_DAEMON_JAVA_OPTS` with `-Dspark.local.hostname=192.168.10.2`.

3. **Verify the worker's advertised address** in Spark Master UI:
   Open `http://192.168.10.1:8080` → click on the worker → the address shown should be `192.168.10.2:8085`, not a `172.x.x.x` Docker bridge IP.

4. **Check firewall on worker machine** allows inbound on port 7337 and 8085.

### Prometheus cannot scrape worker metrics

1. Update `monitoring/prometheus/prometheus.yml` with the actual WORKER_IP (see Step 2).

2. Reload Prometheus without restart:
   ```powershell
   Invoke-WebRequest -Method POST http://192.168.10.1:9090/-/reload
   ```

3. Test scrape endpoint manually:
   ```powershell
   # From master machine:
   Invoke-WebRequest http://192.168.10.2:8081/metrics/worker/prometheus/
   Invoke-WebRequest http://192.168.10.2:9100/metrics
   ```

### MLflow model not found / predict-service fallback

Normal behavior before first model upload. After upload, predict-service reloads automatically within `MODEL_RELOAD_INTERVAL_S` (default 300 s). Force reload:
```powershell
docker restart predict-service
```

---

## Startup Order

```
1. [MASTER] docker compose -f docker-compose.master.yml up --build -d
   Wait for: spark-master healthy, kafka healthy, minio healthy

2. [WORKER] docker build -t bigdata-spark:latest ./spark
            docker compose -f docker-compose.worker.yml up -d
   Wait for: worker appears ALIVE in http://MASTER_IP:8080

3. [MASTER] Upload model:  python upload_model_to_mlflow.py

4. [MASTER] Submit jobs:   docker exec spark-master bash /opt/spark/app/scripts/submit-jobs.sh

5. [MASTER] Open Grafana:  http://MASTER_IP:3000
```

## Shutdown

```powershell
# Worker machine:
docker compose -f docker-compose.worker.yml down

# Master machine:
docker compose -f docker-compose.master.yml down

# Master — also delete all data volumes (careful: destroys Kafka/MinIO/PG data):
docker compose -f docker-compose.master.yml down -v
```
