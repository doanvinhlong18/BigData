"""
spark/jobs/request_to_response_silver.py
─────────────────────────────────────────
Silver/request (stream) ⋈ Bronze/pickup (stream) → Silver/response

Khi pickup event đến, request đã có sẵn trong silver/request.
Stream-stream join trên trip_id với watermark để handle late data.

Schema silver/response:
  trip_id, request_datetime, pickup_datetime, on_scene_datetime,
  PULocationID, DOLocationID, confirmed_PU,
  hvfhs_license_num, dispatching_base_num, originating_base_num,
  wav_request_flag, access_a_ride_flag, shared_request_flag,
  wav_match_flag, share_match_flag
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_REQUEST = "s3a://silver/request"
BRONZE_PICKUP = "s3a://bronze/pickup"
SILVER_RESPONSE = "s3a://silver/response"
CHECKPOINT = "s3a://checkpoints/silver/response"

WATERMARK_REQ = "30 minutes"
WATERMARK_PICKUP = "15 minutes"


def main():
    spark = (
        SparkSession.builder.appName("request_to_response_silver")
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
        # Stream-stream join cần shuffle partitions nhỏ để tránh spill
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Stream 1: Silver/request ──────────────────────────────────────────────
    req_stream = (
        spark.readStream.format("delta")
        .load(SILVER_REQUEST)
        .withWatermark("request_datetime", WATERMARK_REQ)
        .select(
            "trip_id",
            "request_datetime",
            "hvfhs_license_num",
            "dispatching_base_num",
            "originating_base_num",
            "PULocationID",
            "DOLocationID",
            "wav_request_flag",
            "access_a_ride_flag",
            "shared_request_flag",
        )
    )

    # ── Stream 2: Bronze/pickup ───────────────────────────────────────────────
    pickup_stream = (
        spark.readStream.format("delta")
        .load(BRONZE_PICKUP)
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
        .withColumn("on_scene_datetime", to_timestamp(col("on_scene_datetime")))
        .withWatermark("pickup_datetime", WATERMARK_PICKUP)
        .filter(
            col("trip_id").isNotNull()
            & col("pickup_datetime").isNotNull()
            & col("PULocationID").isNotNull()
            & col("PULocationID").between(1, 263)  # NYC có 263 zones
        )
        .dropDuplicates(["trip_id"])
        .select(
            col("trip_id").alias("pu_trip_id"),
            "pickup_datetime",
            "on_scene_datetime",
            col("PULocationID").alias("confirmed_PU"),
            # match flags từ bronze/pickup — cần cho silver_to_gold tính wav_match/share_match
            "wav_match_flag",
            "share_match_flag",
        )
    )

    # ── Stream-stream inner join trên trip_id ─────────────────────────────────
    # Spark stream-stream join BẮT BUỘC có range condition trên event time
    # để bound state size. Pickup phải đến trong [request_datetime, request_datetime + 2h].
    joined = req_stream.join(
        pickup_stream,
        (req_stream["trip_id"] == pickup_stream["pu_trip_id"])
        & (pickup_stream["pickup_datetime"] >= req_stream["request_datetime"])
        & (
            pickup_stream["pickup_datetime"]
            <= req_stream["request_datetime"] + F.expr("INTERVAL 2 HOURS")
        ),
        how="inner",
    ).select(
        req_stream["trip_id"],
        "request_datetime",
        "pickup_datetime",
        "on_scene_datetime",      # nullable
        "PULocationID",           # từ request (planned)
        "confirmed_PU",           # từ pickup (actual)
        "DOLocationID",
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        "wav_request_flag",
        "access_a_ride_flag",
        "shared_request_flag",
        # match flags từ bronze/pickup — dùng trong silver_to_gold để tính wav_match/share_match
        "wav_match_flag",
        "share_match_flag",
    )

    query = (
        joined.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema", "true")
        .trigger(processingTime="2 seconds")
        .start(SILVER_RESPONSE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
