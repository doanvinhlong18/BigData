"""
spark/jobs/complete_bronze_to_silver.py
────────────────────────────────────────
Silver/response (stream) ⋈ Bronze/dropoff (stream) → Silver/complete

Thiết kế:
- Stream–stream join thay vì foreachBatch
- Có watermark + event-time constraint để tránh state vô hạn
- Trigger theo dropoff (event cuối lifecycle)

Schema silver/complete:
  trip_id,
  request_datetime, pickup_datetime, on_scene_datetime, dropoff_datetime,
  PULocationID, confirmed_PU, DOLocationID,
  hvfhs_license_num, dispatching_base_num, originating_base_num,
  wav_request_flag, access_a_ride_flag, shared_request_flag,
  trip_miles, trip_time,
  base_passenger_fare, driver_pay, tips, tolls, bcf,
  sales_tax, congestion_surcharge, airport_fee, cbd_congestion_fee

Lưu ý: shared_match_flag và wav_match_flag KHÔNG có ở bronze/dropoff —
  chúng nằm ở bronze/pickup → silver/response. silver_to_gold đọc
  wav_match/share_match từ silver/response, không từ silver/complete.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr

# ── ENV ─────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_DROPOFF = "s3a://bronze/dropoff"
SILVER_RESPONSE = "s3a://silver/response"
SILVER_COMPLETE = "s3a://silver/complete"
CHECKPOINT = "s3a://checkpoints/silver/complete"

# ── WATERMARK CONFIG ───────────────────────────────
RESPONSE_WATERMARK = "24 hours"
DROPOFF_WATERMARK = "15 minutes"


# ── MAIN ──────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder.appName("complete_silver_stream_join")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3 / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ───────────────────────────────────────────────
    # Stream: Silver/response
    # ───────────────────────────────────────────────
    response_stream = (
        spark.readStream.format("delta")
        .load(SILVER_RESPONSE)
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
        .withColumn("request_datetime", to_timestamp(col("request_datetime")))
        .withColumn("on_scene_datetime", to_timestamp(col("on_scene_datetime")))
        .withWatermark("pickup_datetime", RESPONSE_WATERMARK)
        .filter(col("trip_id").isNotNull())
        .select(
            "trip_id",
            "request_datetime",
            "pickup_datetime",
            "on_scene_datetime",
            "PULocationID",
            "confirmed_PU",
            col("DOLocationID").alias("req_DOLocationID"),
            "hvfhs_license_num",
            "dispatching_base_num",
            "originating_base_num",
            "wav_request_flag",
            "access_a_ride_flag",
            "shared_request_flag",
        )
    )

    # ───────────────────────────────────────────────
    # Stream: Bronze/dropoff
    # ───────────────────────────────────────────────
    dropoff_stream = (
        spark.readStream.format("delta")
        .load(BRONZE_DROPOFF)
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
        .withWatermark("dropoff_datetime", DROPOFF_WATERMARK)
        .filter(
            col("trip_id").isNotNull()
            & col("dropoff_datetime").isNotNull()
            & col("trip_miles").between(0, 200)
            & col("trip_time").between(0, 86400)
        )
        .dropDuplicates(["trip_id"])
        .select(
            "trip_id",
            "dropoff_datetime",
            "DOLocationID",
            "trip_miles",
            "trip_time",
            "base_passenger_fare",
            "driver_pay",
            "tips",
            "tolls",
            "bcf",
            "sales_tax",
            "congestion_surcharge",
            "airport_fee",
            "cbd_congestion_fee",
        )
    )

    # ───────────────────────────────────────────────
    # Stream-Stream Join (CRITICAL PART)
    # ───────────────────────────────────────────────
    complete_stream = dropoff_stream.join(
        response_stream,
        on=[
            dropoff_stream.trip_id == response_stream.trip_id,
            # ⚠️ Event-time constraint để giới hạn state
            dropoff_stream.dropoff_datetime >= response_stream.pickup_datetime,
            dropoff_stream.dropoff_datetime
            <= response_stream.pickup_datetime + expr("INTERVAL 2 HOURS"),
        ],
        how="inner",
    ).select(
        dropoff_stream["trip_id"],
        # timestamps
        "request_datetime",
        "pickup_datetime",
        "on_scene_datetime",
        "dropoff_datetime",
        # locations
        "PULocationID",
        "confirmed_PU",
        dropoff_stream["DOLocationID"],
        # identity
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        # flags
        "wav_request_flag",
        "access_a_ride_flag",
        "shared_request_flag",
        # trip stats
        "trip_miles",
        "trip_time",
        # financials
        "base_passenger_fare",
        "driver_pay",
        "tips",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    )

    # ───────────────────────────────────────────────
    # Write stream
    # ───────────────────────────────────────────────
    query = (
        complete_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start(SILVER_COMPLETE)
    )

    query.awaitTermination()


# ───────────────────────────────────────────────
if __name__ == "__main__":
    main()
