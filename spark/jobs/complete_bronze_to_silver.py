"""
spark/jobs/complete_bronze_to_silver.py
─────────────────────────────────────────
Bronze request + Bronze response → Silver complete

Request stream: trip_id, request_datetime, PULocationID, DOLocationID, flags
Response stream: trip_id, dropoff_datetime, pickup_datetime, on_scene_datetime,
                 trip_miles, trip_time, financials, match_flags

Join theo trip_id + time constraints → silver/complete có đầy đủ tất cả.
pickup_datetime chỉ xuất hiện ở đây sau khi join xong.
"""

import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp

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
    # Stream 1: Request — chỉ có request_datetime, locations, flags
    # KHÔNG có pickup_datetime vì lúc request chưa biết tài xế đến lúc nào
    req = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load("s3a://bronze/sorted_request_table")
        .filter(
            col("trip_id").isNotNull()
            & col("request_datetime").isNotNull()
            & col("PULocationID").isNotNull()
        )
        .withWatermark("request_datetime", "15 minutes")
        .select(
            "trip_id",
            "hvfhs_license_num",
            "dispatching_base_num",
            "originating_base_num",
            "PULocationID",
            "DOLocationID",
            "request_datetime",
            # Flags từ request
            "wav_request_flag",
            "access_a_ride_flag",
            "shared_request_flag",
            "year",
            "month",
            "day",
        )
    )

    # Stream 2: Response — có pickup_datetime, financials, match_flags
    # pickup_datetime chỉ biết sau khi tài xế đã đón khách → chỉ có ở response
    res = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load("s3a://bronze/sorted_response_table")
        .filter(
            col("trip_id").isNotNull()
            & col("dropoff_datetime").isNotNull()
            & col("pickup_datetime").isNotNull()
        )  # response phải có pickup_datetime
        .withWatermark("dropoff_datetime", "15 minutes")
        .select(
            "trip_id",
            "dropoff_datetime",
            "pickup_datetime",  # ← từ response, đây là lúc tài xế đón khách
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

    # Stream-stream join
    # Sau khi join: có đủ tất cả — request_datetime + pickup_datetime + dropoff_datetime
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
            # Tất cả timestamps — pickup_datetime đến từ response stream
            col("req.request_datetime"),
            col("res.pickup_datetime"),  # từ response
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
