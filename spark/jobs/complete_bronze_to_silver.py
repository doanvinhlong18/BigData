"""
spark/jobs/complete_bronze_to_silver.py
─────────────────────────────────────────
Silver request + Bronze response → Silver complete

Source:
  Stream 1: silver/request       — đã dedup + clean ở request_bronze_to_silver.py
  Stream 2: bronze/sorted_response_table — raw, filter + dedup inline ở đây

Lý do không dùng silver/response riêng:
  Chỉ có 2 silver tables: silver/request và silver/complete.
  Response không cần lưu riêng vì không có downstream nào đọc response
  độc lập (ngoài join này).

Watermark:
  Request side: 3 giờ — cần giữ state đủ lâu chờ response đến
  Response side: 15 phút — response vào gần như lúc nào request cũng đã có
                            trong state, không cần đợi lâu

Join condition:
  trip_id khớp + dropoff trong khoảng [request, request + 3h]
"""

import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    current_timestamp,
    to_timestamp,
    year,
    month,
    dayofmonth,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("complete_bronze_to_silver")


def get_spark():
    ep = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ak = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    sk = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName("complete-bronze-to-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", ep)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run(spark):
    # Stream 1: Silver request — đã dedup + clean
    # Watermark 3h: giữ state request đủ lâu chờ response đến (chuyến xa nhất ~2h)
    req = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load("s3a://silver/request")
        .withWatermark("request_datetime", "3 hours")
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
            "year",
            "month",
            "day",
        )
    )

    # Stream 2: Bronze response — chưa clean, xử lý inline
    # Watermark 15 phút: request luôn đến trước, response vào là khớp ngay
    # dropDuplicatesWithinWatermark: phòng producer retry gửi trùng
    res = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load("s3a://bronze/sorted_response_table")
        .filter(
            col("trip_id").isNotNull()
            & col("dropoff_datetime").isNotNull()
            & col("pickup_datetime").isNotNull()
            & (col("pickup_datetime") < col("dropoff_datetime"))
        )
        .withWatermark("dropoff_datetime", "15 minutes")
        .dropDuplicatesWithinWatermark(["trip_id"])
        .select(
            "trip_id",
            "dropoff_datetime",
            "pickup_datetime",
            "on_scene_datetime",
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
            "shared_match_flag",
            "wav_match_flag",
        )
    )

    # Stream-stream join: trip_id + time constraint
    # Request state (3h) đảm bảo Spark giữ đủ lâu để response kịp khớp
    complete = (
        req.alias("req")
        .join(
            res.alias("res"),
            expr("""
                req.trip_id = res.trip_id
                AND res.dropoff_datetime >= req.request_datetime
                AND res.dropoff_datetime <= req.request_datetime + INTERVAL 3 HOURS
            """),
            "inner",
        )
        .select(
            col("req.trip_id"),
            col("req.hvfhs_license_num"),
            col("req.dispatching_base_num"),
            col("req.originating_base_num"),
            col("req.PULocationID"),
            col("req.DOLocationID"),
            col("req.request_datetime"),
            col("res.pickup_datetime"),  # từ response — lúc tài xế đón
            col("res.on_scene_datetime"),
            col("res.dropoff_datetime"),
            col("res.trip_miles"),
            col("res.trip_time"),
            col("res.base_passenger_fare"),
            col("res.driver_pay"),
            col("res.tips"),
            col("res.tolls"),
            col("res.bcf"),
            col("res.sales_tax"),
            col("res.congestion_surcharge"),
            col("res.airport_fee"),
            col("res.cbd_congestion_fee"),
            col("req.wav_request_flag"),
            col("req.access_a_ride_flag"),
            col("req.shared_request_flag"),
            col("res.shared_match_flag"),
            col("res.wav_match_flag"),
            col("req.year"),
            col("req.month"),
            col("req.day"),
            current_timestamp().alias("silver_ingest_time"),
        )
    )

    (
        complete.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3a://silver/checkpoints/complete")
        .partitionBy("year", "month", "day")
        .trigger(processingTime="30 seconds")
        .start("s3a://silver/complete")
        .awaitTermination()
    )


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
