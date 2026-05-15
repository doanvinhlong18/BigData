"""
spark/jobs/request_bronze_to_silver.py
────────────────────────────────────────
Bronze/request → Silver/request
- Filter null trip_id / PULocationID / DOLocationID
- Validate location range 1-265
- Dedup theo trip_id trong watermark 15 phút
- KHÔNG có trip_miles / trip_time (đã chuyển sang dropoff event)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_REQUEST = "s3a://bronze/request"
SILVER_REQUEST = "s3a://silver/request"
CHECKPOINT = "s3a://checkpoints/silver/request"


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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.format("delta")
        .load(BRONZE_REQUEST)
        .withColumn("request_datetime", to_timestamp(col("request_datetime")))
        .withWatermark("request_datetime", "15 minutes")
        .filter(
            col("trip_id").isNotNull()
            & col("PULocationID").isNotNull()
            & col("DOLocationID").isNotNull()
            & col("PULocationID").between(1, 265)
            & col("DOLocationID").between(1, 265)
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
        .trigger(processingTime="1 second")
        .start(SILVER_REQUEST)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
