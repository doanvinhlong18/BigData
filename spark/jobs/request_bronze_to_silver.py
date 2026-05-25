"""
spark/jobs/request_bronze_to_silver.py
────────────────────────────────────────
Bronze/request → Silver/request
- Filter null trip_id / PULocationID / DOLocationID
- Validate location range 1-263 (NYC TLC có 263 zones)
- Dedup theo trip_id trong watermark 15 phút
- KHÔNG có trip_miles / trip_time (đã chuyển sang dropoff event)
"""

import os
import time
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_REQUEST = "s3a://bronze/request"
SILVER_REQUEST = "s3a://silver/request"
CHECKPOINT_ROOT = (
    os.getenv("STATEFUL_CHECKPOINT_BASE")
    or os.getenv("STREAMING_CHECKPOINT_BASE")
    or "s3a://checkpoints"
).rstrip("/")
CHECKPOINT = f"{CHECKPOINT_ROOT}/silver/request"


def wait_for_source(spark, path, timeout=600):
    """Chờ Delta table tồn tại trước khi readStream.
    Dùng DeltaTable.isDeltaTable() thay vì spark.read — trả về True/False,
    không throw exception khi table chưa tồn tại."""
    print(f"[wait_for_source] Chờ {path} ...", flush=True)
    elapsed = 0
    while elapsed < timeout:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                print(f"[wait_for_source] ✅ {path} sẵn sàng ({elapsed}s)", flush=True)
                return
        except Exception as e:
            print(f"[wait_for_source]   ⚠️  check error: {e}", flush=True)
        time.sleep(10)
        elapsed += 10
        print(f"[wait_for_source]   ... {path} chưa sẵn sàng ({elapsed}s)", flush=True)
    raise TimeoutError(f"Source {path} không xuất hiện sau {timeout}s")


def main():
    spark = (
        SparkSession.builder.appName("request_bronze_to_silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Chờ source sẵn sàng (jobs submit đồng thời, bronze có thể chưa có)
    wait_for_source(spark, BRONZE_REQUEST)

    stream = (
        spark.readStream.format("delta")
        .load(BRONZE_REQUEST)
        .withColumn("request_datetime", to_timestamp(col("request_datetime")))
        .withWatermark("request_datetime", "15 minutes")
        .filter(
            col("trip_id").isNotNull()
            & col("PULocationID").isNotNull()
            & col("DOLocationID").isNotNull()
            & col("PULocationID").between(1, 263)
            & col("DOLocationID").between(1, 263)
        )
        .dropDuplicates(["trip_id"])
        .select(
            "trip_id",
            "hvfhs_license_num",
            "dispatching_base_num",
            "originating_base_num",
            "PULocationID",
            "DOLocationID",
            "request_datetime",
            "wav_request_flag",
            "access_a_ride_flag",
            "shared_request_flag",
        )
    )

    query = (
        stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start(SILVER_REQUEST)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
